/**
 * The Stage 2 Claude transport (src/session-intel/synthesis/claude-transport.ts) against an injected fake fetch: no
 * network, no environment, no real key (the key below is a made-up test string).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  ANTHROPIC_VERSION, claudeRequestBody, ClaudeTransportError, createClaudeTransport, parseClaudeResponse, retryAfterSeconds,
  stripUnsupportedSchema, UNSUPPORTED_SCHEMA_KEYWORDS,
} from "../synthesis/claude-transport.ts";
import { memoryBudgetLedger, runStage2, STAGE2_CAPS, STAGE2_INSTRUCTIONS, STAGE2_REPLY_SCHEMA, type Stage2ModelRequest } from "../synthesis/stage2.ts";
import { synthesize } from "../synthesis/synthesize.ts";

const KEY = "fx-test-key-not-a-real-credential-0123456789";
const FIXTURE_DIR = new URL("./fixtures/synthesis/", import.meta.url);
const SCHEMA_GOLDEN = JSON.parse(readFileSync(new URL("stage2-request-schema.json", FIXTURE_DIR), "utf8"));

interface Call { url: string; init: RequestInit }
function fakeFetch(responses: Array<Response | Error | ((init: RequestInit) => Promise<Response>)>) {
  const calls: Call[] = [];
  const fetch = (async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), init: init ?? {} });
    const next = responses.shift();
    if (!next) throw new Error("fx: no response queued");
    if (next instanceof Error) throw next;
    if (typeof next === "function") return next(init ?? {});
    return next;
  }) as unknown as typeof globalThis.fetch;
  return { fetch, calls };
}
const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
const message = (over: Record<string, unknown> = {}, usage: Record<string, unknown> = {}) => ({
  id: "msg_fx", type: "message", role: "assistant", model: "claude-sonnet-5-5", stop_reason: "end_turn", stop_details: null,
  content: [{ type: "thinking", thinking: "", signature: "fx" }, { type: "text", text: "{\"initiatives\":[],\"themes\":[],\"hypotheses\":[]}" }],
  usage: { input_tokens: 1_200, output_tokens: 300, cache_creation_input_tokens: 50, cache_read_input_tokens: 25, ...usage },
  ...over,
});
const request = (over: Partial<Stage2ModelRequest> = {}): Stage2ModelRequest => ({
  model: "claude-sonnet-5-5", instructions: STAGE2_INSTRUCTIONS, input: "{\"sessions\":[]}", schemaName: "workspace_synthesis_stage2",
  schema: STAGE2_REPLY_SCHEMA, timeoutMs: 120_000, maxOutputTokens: STAGE2_CAPS.maxOutputTokensPerRun, signal: new AbortController().signal, ...over,
});
const transport = (f: ReturnType<typeof fakeFetch>, sleeps: number[] = []) =>
  createClaudeTransport({ apiKey: KEY, fetch: f.fetch, sleep: async (ms) => { sleeps.push(ms); } });
async function failure(p: Promise<unknown>): Promise<ClaudeTransportError> {
  try { await p; } catch (e) { return e as ClaudeTransportError; }
  throw new Error("expected a failure");
}

const realFetch = globalThis.fetch;
afterEach(() => { globalThis.fetch = realFetch; });

describe("the request", () => {
  test("the exact body golden, headers and URL", async () => {
    const f = fakeFetch([json(200, message())]);
    await transport(f).complete(request());
    expect(f.calls.length).toBe(1);
    const { url, init } = f.calls[0]!;
    expect(url).toBe("https://api.anthropic.com/v1/messages");
    expect(init.method).toBe("POST");
    expect(init.redirect).toBe("manual");
    expect(init.headers).toEqual({ "content-type": "application/json", "x-api-key": KEY, "anthropic-version": ANTHROPIC_VERSION });
    expect(ANTHROPIC_VERSION).toBe("2026-10-21");
    const body = JSON.parse(init.body as string);
    expect(body).toEqual({
      model: "claude-sonnet-5-5",
      max_tokens: 10_000,
      system: STAGE2_INSTRUCTIONS,
      messages: [{ role: "user", content: "{\"sessions\":[]}" }],
      output_config: { effort: "low", format: { type: "json_schema", schema: SCHEMA_GOLDEN } },
    });
    // No forced tool choice, no tools, no thinking override, no sampling parameters, no fallbacks.
    for (const k of ["tool_choice", "tools", "thinking", "temperature", "top_p", "top_k", "fallbacks", "stream"]) expect(Object.hasOwn(body, k)).toBe(false);
    expect(Object.keys(body)).toEqual(["model", "max_tokens", "system", "messages", "output_config"]);
  });

  test("baseUrl: an https origin only", async () => {
    const f = fakeFetch([json(200, message())]);
    await createClaudeTransport({ apiKey: KEY, fetch: f.fetch, baseUrl: "https://proxy.example.test" }).complete(request());
    expect(f.calls[0]!.url).toBe("https://proxy.example.test/v1/messages");
    for (const bad of ["http://api.anthropic.com", "https://u:p@api.anthropic.com", "https://api.anthropic.com/v2", "https://api.anthropic.com?x=1"])
      expect(() => createClaudeTransport({ apiKey: KEY, fetch: f.fetch, baseUrl: bad })).toThrow("baseUrl");
  });

  test("only an approved model is requested; nothing is sent otherwise", async () => {
    const f = fakeFetch([]);
    for (const model of ["claude-opus-5-5", "claude-sonnet-5", "claude-sonnet-5-5-20260101", ""]) {
      const e = await failure(transport(f).complete(request({ model })));
      expect(e.code).toBe("model_not_approved");
    }
    expect(() => claudeRequestBody(request({ model: "claude-haiku-5-5" }))).toThrow(ClaudeTransportError);
    expect(f.calls).toEqual([]);
  });

  test("the abort signal is the request's, and an aborted call is a typed `aborted`", async () => {
    const ac = new AbortController();
    const f = fakeFetch([(init) => new Promise<Response>((_, reject) => {
      init.signal!.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    })]);
    const p = transport(f).complete(request({ signal: ac.signal }));
    await Promise.resolve();
    expect(f.calls[0]!.init.signal).toBe(ac.signal);
    ac.abort();
    expect((await failure(p)).code).toBe("aborted");
    // Already aborted: no request at all.
    const g = fakeFetch([]);
    expect((await failure(transport(g).complete(request({ signal: AbortSignal.abort() })))).code).toBe("aborted");
    expect(g.calls).toEqual([]);
  });

  test("the injected fetch is used, never the global one", async () => {
    let globalCalls = 0;
    globalThis.fetch = (async () => { globalCalls++; throw new Error("global fetch used"); }) as unknown as typeof fetch;
    const f = fakeFetch([json(200, message())]);
    await transport(f).complete(request());
    expect([globalCalls, f.calls.length]).toEqual([0, 1]);
    expect(() => createClaudeTransport({ apiKey: KEY } as never)).toThrow("fetch is required");
    const source = readFileSync(new URL("../synthesis/claude-transport.ts", import.meta.url), "utf8");
    const code = source.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gmu, "");
    expect(code.match(/globalThis\.fetch\s*\(|[^.\w]fetch\s*\(|process\.env|Bun\.env|readFileSync|console\./u)).toBeNull();
  });
});

describe("schema stripping", () => {
  test("every unsupported keyword is gone; types, enums, required and additionalProperties stay", () => {
    const out = stripUnsupportedSchema(STAGE2_REPLY_SCHEMA);
    expect(out).toEqual(SCHEMA_GOLDEN);
    const text = JSON.stringify(out);
    for (const k of UNSUPPORTED_SCHEMA_KEYWORDS) expect(text).not.toContain(`"${k}"`);
    // minItems survives only as 0 or 1 (the hypothesis basis); the 2-session minimum is the parser's.
    expect([...text.matchAll(/"minItems":(\d+)/gu)].map((m) => m[1])).toEqual(["1"]);
    expect(text).toContain("\"additionalProperties\":false");
    expect(text).toContain("\"enum\":[\"recurring_issue\",\"workflow\",\"topic\"]");
    // The source schema is untouched (the parser and the prompt still carry the bounds).
    expect(JSON.stringify(STAGE2_REPLY_SCHEMA)).toContain("\"maxLength\":80");
  });

  test("a property named like a keyword is kept; unsupported constructs fail closed", () => {
    const s = { type: "object", additionalProperties: false, required: ["maxLength"], properties: { maxLength: { type: "integer", minimum: 0 } } };
    expect(stripUnsupportedSchema(s)).toEqual({ type: "object", additionalProperties: false, required: ["maxLength"], properties: { maxLength: { type: "integer" } } });
    expect(() => stripUnsupportedSchema({ type: "object", properties: {} })).toThrow("additionalProperties");
    expect(() => stripUnsupportedSchema({ oneOf: [] })).toThrow("oneOf");
    expect(() => stripUnsupportedSchema({ type: "array", items: [] })).toThrow();
  });
});

describe("the response", () => {
  test("usage sums the cache tokens into input; output is output_tokens; the text block and the model are reported", async () => {
    const f = fakeFetch([json(200, message(), { "request-id": "req_fx_1" })]);
    const r = await transport(f).complete(request());
    expect(r).toEqual({
      outputText: "{\"initiatives\":[],\"themes\":[],\"hypotheses\":[]}", inputTokens: 1_200 + 50 + 25, outputTokens: 300,
      providerRequestId: "req_fx_1", model: "claude-sonnet-5-5",
    });
    // Absent or null cache fields count 0; a malformed one is not accepted.
    const absent = parseClaudeResponse(message({}, { cache_creation_input_tokens: undefined, cache_read_input_tokens: null }), null);
    expect([absent.inputTokens, absent.outputTokens]).toEqual([1_200, 300]);
    expect(() => parseClaudeResponse(message({}, { cache_read_input_tokens: -1 }), null)).toThrow("response_invalid");
    expect(() => parseClaudeResponse(message({}, { output_tokens: 1.5 }), null)).toThrow("response_invalid");
  });

  test("max_tokens and refusal are typed failures, never partial text; usage is on the error", async () => {
    for (const [stop, code] of [["max_tokens", "max_tokens"], ["refusal", "refusal"], ["tool_use", "unexpected_stop"], ["pause_turn", "unexpected_stop"], [null, "unexpected_stop"]] as const) {
      const partial = message({ stop_reason: stop, content: [{ type: "text", text: "{\"initiatives\":[{\"title\":\"cut" }] });
      const e = await failure(transport(fakeFetch([json(200, partial)])).complete(request()));
      expect([stop, e.code]).toEqual([stop, code]);
      expect(e.usage).toEqual({ inputTokens: 1_275, outputTokens: 300 });
      expect(e.message).not.toContain("cut");
    }
  });

  test("a 0-input success, no text, two texts, a non-message and a non-JSON body are refused", async () => {
    const cases: Array<[unknown, string]> = [
      [message({}, { input_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0 }), "zero_input"],
      [message({ content: [{ type: "thinking", thinking: "" }] }), "no_text"],
      [message({ content: [{ type: "text", text: "{}" }, { type: "text", text: "{}" }] }), "no_text"],
      [message({ content: [{ type: "text", text: "" }] }), "no_text"],
      [{ ...message(), type: "error" }, "response_invalid"],
      [message({ model: 7 }), "response_invalid"],
      [message({ usage: null }), "response_invalid"],
    ];
    for (const [body, code] of cases) expect((await failure(transport(fakeFetch([json(200, body)])).complete(request()))).code).toBe(code as never);
    expect((await failure(transport(fakeFetch([new Response("not json", { status: 200 })])).complete(request()))).code).toBe("response_invalid");
    expect((await failure(transport(fakeFetch([new Response("x".repeat(1024 * 1024 + 1), { status: 200 })])).complete(request()))).code).toBe("response_invalid");
  });
});

describe("HTTP errors and the retry", () => {
  test("4xx and 5xx are typed by status; none of them is retried except 429/529 with retry-after", async () => {
    const cases: Array<[number, string]> = [
      [400, "request_invalid"], [401, "auth_failed"], [402, "billing_error"], [403, "permission_denied"], [404, "not_found"],
      [413, "request_too_large"], [500, "server_error"], [503, "server_error"], [302, "http_error"], [418, "http_error"],
    ];
    for (const [status, code] of cases) {
      const f = fakeFetch([json(status, { type: "error", error: { type: "x", message: "fx detail" } }, { "retry-after": "1" })]);
      const e = await failure(transport(f).complete(request()));
      expect([status, e.code, e.status, e.attempts, f.calls.length]).toEqual([status, code, status, 1, 1]);
      expect(e.message).not.toContain("fx detail");
    }
    const net = await failure(transport(fakeFetch([new Error("connect ECONNREFUSED")])).complete(request()));
    expect([net.code, net.message.includes("ECONNREFUSED")]).toEqual(["network_error", false]);
  });

  test("429 or 529 with retry-after: exactly one retry after the wait; a second one fails", async () => {
    for (const [status, code] of [[429, "rate_limited"], [529, "overloaded"]] as const) {
      const sleeps: number[] = [];
      const ok = fakeFetch([json(status, {}, { "retry-after": "2" }), json(200, message())]);
      expect((await transport(ok, sleeps).complete(request())).outputTokens).toBe(300);
      expect([ok.calls.length, sleeps]).toEqual([2, [2_000]]);
      expect(ok.calls[1]!.init.body).toBe(ok.calls[0]!.init.body);
      const twice = fakeFetch([json(status, {}, { "retry-after": "0" }), json(status, {}, { "retry-after": "0" }), json(200, message())]);
      const e = await failure(transport(twice).complete(request()));
      expect([e.code, e.attempts, twice.calls.length]).toEqual([code, 2, 2]);
    }
  });

  test("no retry without retry-after (a spend cap), past the 30 s bound, an HTTP-date, or once aborted", async () => {
    for (const headers of [{}, { "retry-after": "31" }, { "retry-after": "Wed, 21 Oct 2026 07:28:00 GMT" }, { "retry-after": "-1" }]) {
      const f = fakeFetch([json(429, {}, headers), json(200, message())]);
      const e = await failure(transport(f).complete(request()));
      expect([e.code, f.calls.length]).toEqual(["rate_limited", 1]);
    }
    const ac = new AbortController();
    const f = fakeFetch([json(529, {}, { "retry-after": "1" }), json(200, message())]);
    const t = createClaudeTransport({ apiKey: KEY, fetch: f.fetch, sleep: async () => { ac.abort(); throw new Error("aborted"); } });
    expect((await failure(t.complete(request({ signal: ac.signal })))).code).toBe("aborted");
    expect(f.calls.length).toBe(1);
    expect([retryAfterSeconds("5"), retryAfterSeconds(" 30 "), retryAfterSeconds("30.5"), retryAfterSeconds(null)]).toEqual([5, 30, null, null]);
  });
});

describe("the key", () => {
  test("never in an error message, stack or serialization, on any failure path", async () => {
    const echo = (init: RequestInit) => Promise.reject(new Error(`boom ${(init.headers as Record<string, string>)["x-api-key"]}`));
    const paths: Array<Response | Error | ((init: RequestInit) => Promise<Response>)> = [
      echo, json(401, { error: { message: `bad key ${KEY}` } }), json(200, { ...message(), stop_reason: "refusal", content: [{ type: "text", text: KEY }] }),
      json(200, { type: KEY }), new Response(KEY, { status: 200 }), json(200, message({ content: [{ type: "text", text: "" }], model: KEY })),
    ];
    for (const p of paths) {
      const e = await failure(transport(fakeFetch([p])).complete(request()));
      expect(e).toBeInstanceOf(ClaudeTransportError);
      for (const s of [e.message, String(e.stack), JSON.stringify(e), String(e)]) expect(s).not.toContain(KEY);
    }
    // A bad key is refused at construction without echoing it.
    for (const bad of ["", "has space", "line\nbreak", 7]) {
      try { createClaudeTransport({ apiKey: bad as string, fetch: fakeFetch([]).fetch }); throw new Error("no throw"); } catch (e) {
        expect((e as Error).message).toBe("claude transport: apiKey must be a non-empty printable string");
      }
    }
  });
});

describe("through runStage2", () => {
  const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE_DIR), "utf8"));
  const reply = readFileSync(new URL("stage2-reply.json", FIXTURE_DIR), "utf8");
  const OPTS = { now: "2026-11-08T00:00:00.000Z" };
  const go = (body: unknown) => {
    const c = cohort();
    const f = fakeFetch([json(200, body)]);
    const ledger = memoryBudgetLedger();
    return {
      f, ledger,
      run: runStage2({ v1: synthesize(c, OPTS), cohort: c, options: OPTS, orgId: "fx-org", deps: {
        enabled: true, trigger: "manual", clock: () => new Date("2026-11-24T10:00:00.000Z"), ledger, transport: transport(f),
      } }),
    };
  };

  test("a complete reply settles input + cache tokens plus output and yields a v2 document", async () => {
    const { run, ledger, f } = go(message({ content: [{ type: "text", text: reply }] }, { input_tokens: 3_000, cache_read_input_tokens: 500, cache_creation_input_tokens: 100, output_tokens: 800 }));
    const r = await run;
    expect(r.document.stage2Reason).toBeNull();
    expect(r.usage).toEqual({ inputTokens: 3_600, outputTokens: 800 });
    expect([...ledger.reservations.values()][0]).toMatchObject({ settled: true, tokens: 4_400 });
    expect(JSON.parse(f.calls[0]!.init.body as string).output_config.format.schema).toEqual(SCHEMA_GOLDEN);
  });

  test("max_tokens is transport_failed with the reservation kept charged (never a partial parse)", async () => {
    const { run, ledger } = go(message({ stop_reason: "max_tokens", content: [{ type: "text", text: reply.slice(0, 200) }] }));
    const r = await run;
    expect([r.document.stage2, r.document.stage2Reason, r.called]).toEqual([null, "transport_failed", true]);
    expect([...ledger.reservations.values()][0]!.settled).toBe(false);
  });
});
