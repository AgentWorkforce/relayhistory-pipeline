import { describe, expect, test } from "bun:test";
import type { AnalysisPacket } from "../../../packages/coordination-eval/src/analysis.ts";
import {
  createCoordinationTransport,
  COORDINATION_RESULT_SCHEMA,
  COORDINATION_PROMPT_VERSION,
  type CoordinationResponseMetadata,
} from "../transport.ts";

const packet = { packetId: "packet-test" } as AnalysisPacket;
describe("coordination Responses transport", () => {
  test("uses injected fetch, strict schema, memory credentials and output budget", async () => {
    let calls = 0;
    const metadata: CoordinationResponseMetadata[] = [];
    const transport = createCoordinationTransport({
      model: "test-model",
      apiKey: () => "test-key",
      maxOutputTokens: 8192,
      onResponse: (value) => {
        metadata.push(value);
      },
      fetch: (async (url, init) => {
        calls++;
        expect(String(url)).toBe("https://api.openai.com/v1/responses");
        expect(init?.redirect).toBe("manual");
        expect(new Headers(init?.headers).get("authorization")).toBe(
          "Bearer test-key",
        );
        const body = JSON.parse(String(init?.body));
        expect(body.store).toBe(false);
        expect(body.max_output_tokens).toBe(8192);
        expect(body.text.format.strict).toBe(true);
        expect(body.text.format.schema).toEqual(COORDINATION_RESULT_SCHEMA);
        expect(JSON.parse(body.input)).toEqual(packet);
        return Response.json({
          status: "completed",
          model: "reported-model",
          id: "resp_test",
          usage: { input_tokens: 42, output_tokens: 10 },
          output: [{ content: [{ type: "output_text", text: "{}" }] }],
        });
      }) as typeof fetch,
    });
    expect(
      await transport({
        packet,
        attempt: 1,
        signal: new AbortController().signal,
      }),
    ).toBe("{}");
    expect(calls).toBe(1);
    expect(metadata).toEqual([
      {
        packetId: "packet-test",
        attempt: 1,
        configuredModel: "test-model",
        reportedModel: "reported-model",
        promptVersion: COORDINATION_PROMPT_VERSION,
        inputTokens: 42,
        outputTokens: 10,
        providerRequestId: "resp_test",
      },
    ]);
  });

  test("forwards cancellation to the actual HTTP request", async () => {
    const controller = new AbortController();
    const transport = createCoordinationTransport({
      model: "test",
      apiKey: () => "test",
      fetch: (async (_url, init) => {
        controller.abort();
        expect(init?.signal?.aborted).toBe(true);
        throw new Error("cancelled");
      }) as typeof fetch,
    });
    await expect(
      transport({ packet, attempt: 1, signal: controller.signal }),
    ).rejects.toThrow();
  });

  test("rejects an aborted request without fetching", async () => {
    const transport = createCoordinationTransport({
      model: "test",
      apiKey: () => "test",
      fetch: (async () => {
        throw new Error("must not fetch");
      }) as typeof fetch,
    });
    await expect(
      transport({ packet, attempt: 1, signal: AbortSignal.abort() }),
    ).rejects.toThrow();
  });

  test("rejects unsafe endpoints and invalid budgets before executing", () => {
    const base = { model: "test", apiKey: () => "test" };
    for (const responsesUrl of [
      "http://example.com/v1/responses",
      "https://user:password@example.com/v1/responses",
      "https://example.com/responses?key=x",
      "https://example.com/completions",
    ])
      expect(() =>
        createCoordinationTransport({ ...base, responsesUrl }),
      ).toThrow();
    expect(() =>
      createCoordinationTransport({ ...base, maxOutputTokens: 0 }),
    ).toThrow();
    expect(() =>
      createCoordinationTransport({ ...base, timeoutMs: Infinity }),
    ).toThrow();
  });

  test("does not expose provider error bodies", async () => {
    const transport = createCoordinationTransport({
      model: "test",
      apiKey: () => "test",
      fetch: (async () =>
        new Response("sensitive-provider-body", {
          status: 429,
        })) as typeof fetch,
    });
    await expect(
      transport({ packet, attempt: 1, signal: new AbortController().signal }),
    ).rejects.toThrow("model_rate_limited");
  });
});
