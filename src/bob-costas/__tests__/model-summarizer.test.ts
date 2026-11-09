import { describe, expect, test } from "bun:test";
import { logLine } from "../../shared/log.ts";
import {
  DailyAdmission,
  buildModelRequest,
  createModelSummarizer,
  modelSummarizerConfig,
  summarizerFromEnv,
} from "../model-summarizer.ts";
import { digestRecords, extractiveSummarizer, type SessionDigest, type SummaryContext } from "../summarizer.ts";

const AWS_KEY = "AKIAIOSFODNN7EXAMPLE";
const EMAIL = "someone@example.com";
const GH_TOKEN = `ghp_${"a1B2c3D4e5".repeat(4).slice(0, 36)}`;
const PROMPT_MARKER = "Please fix the heron login bug";
const OUTPUT_MARKER = "Fixed the egret login flow";

const lines = [
  JSON.stringify({ type: "user", message: { role: "user", content: `${PROMPT_MARKER}; key ${AWS_KEY}, mail ${EMAIL}, token ${GH_TOKEN}` }, timestamp: "2026-10-28T10:00:00Z" }),
  JSON.stringify({ type: "assistant", message: { role: "assistant", model: "claude-sonnet-4-6", content: [{ type: "text", text: "On it" }, { type: "tool_use", name: "Bash", input: {} }, { type: "tool_use", name: "Edit", input: {} }, { type: "tool_use", name: "Bash", input: {} }] }, timestamp: "2026-10-28T10:00:05Z" }),
  JSON.stringify({ type: "user", message: { role: "user", content: "now run the tests" }, timestamp: "2026-10-28T10:01:00Z" }),
];
const digest: SessionDigest = digestRecords("claude", lines);
const ctx: SummaryContext = { orgId: "org-1", sessionKey: '["w","claude","s1"]', traceId: "01a188e1-ef86-7b16-938a-e91b6b78643b" };
const KEY = "sk-test-only-not-a-real-key";
const env = (extra: Record<string, string> = {}) => ({ BOB_COSTAS_MODEL: "gpt-test-mini", OPENAI_API_KEY: KEY, BOB_COSTAS_RETRY_DELAY_MS: "0", ...extra });

function responsesBody(summary: string, opts: { model?: string; tokensIn?: number; tokensOut?: number } = {}) {
  return {
    id: "resp_1",
    status: "completed",
    model: opts.model ?? "gpt-test-mini-2026-10-01",
    output: [{ type: "message", content: [{ type: "output_text", text: JSON.stringify({ summary }) }] }],
    usage: { input_tokens: opts.tokensIn ?? 120, output_tokens: opts.tokensOut ?? 30 },
  };
}

interface Call { url: string; headers: Record<string, string>; body: Record<string, unknown> }

/** A fake fetch answering from a queue of responses (status + JSON body) and recording each request. */
function fakeFetch(answers: { status: number; body?: unknown }[]) {
  const calls: Call[] = [];
  const fetchFn = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(input), headers: init?.headers as Record<string, string>, body: JSON.parse(String(init?.body)) });
    const a = answers.shift() ?? { status: 500, body: { error: { message: "exhausted" } } };
    return new Response(JSON.stringify(a.body ?? {}), { status: a.status, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  return { fetchFn, calls };
}

/** Records the exact JSON lines the shared logger would emit (allowlist applied). */
function captureLog() {
  const lines: string[] = [];
  return {
    lines,
    log: {
      info: (m: string, f: Record<string, unknown> = {}) => void lines.push(logLine("info", m, f)),
      warn: (m: string, f: Record<string, unknown> = {}) => void lines.push(logLine("warn", m, f)),
    },
  };
}

function assertNoContentInLogs(logLines: string[], extra: string[] = []) {
  const joined = logLines.join("\n");
  for (const s of [PROMPT_MARKER, OUTPUT_MARKER, AWS_KEY, EMAIL, GH_TOKEN, KEY, "now run the tests", "summarise one coding-agent session", ...extra])
    expect(joined).not.toContain(s);
}

const parsed = (line: string) => JSON.parse(line) as Record<string, unknown>;

describe("summarizerFromEnv selection", () => {
  test("unset, empty, off and extractive select the extractive summariser without reading OPENAI_*", () => {
    for (const value of [undefined, "", "  ", "off", "extractive"]) {
      const e: Record<string, string> = { OPENAI_BASE_URL: "http://not-validated.example" };
      if (value !== undefined) e.BOB_COSTAS_MODEL = value;
      expect(summarizerFromEnv(e)).toBe(extractiveSummarizer);
    }
  });

  test("a valid model id selects the model summariser", () => {
    const s = summarizerFromEnv({ BOB_COSTAS_MODEL: "gpt-5.6-luna" });
    expect(s.id).toBe("model/gpt-5.6-luna");
  });

  test("an invalid model id fails startup without echoing the value", () => {
    expect(() => summarizerFromEnv({ BOB_COSTAS_MODEL: "bad model id!" })).toThrow(/BOB_COSTAS_MODEL is not a valid model id/);
    try {
      summarizerFromEnv({ BOB_COSTAS_MODEL: `${AWS_KEY} x` });
    } catch (err) {
      expect(String(err)).not.toContain(AWS_KEY);
    }
  });

  test("a bad OPENAI_BASE_URL or ALLOW_IN_CLUSTER_HTTP fails startup, even with no key", () => {
    expect(() => summarizerFromEnv({ BOB_COSTAS_MODEL: "m1", OPENAI_BASE_URL: "http://llm.example.com/v1" })).toThrow(/OPENAI_BASE_URL must use https/);
    expect(() => summarizerFromEnv({ BOB_COSTAS_MODEL: "m1", OPENAI_BASE_URL: "https://u:p@llm.example.com/v1" })).toThrow(/must not contain credentials/);
    expect(() => summarizerFromEnv({ BOB_COSTAS_MODEL: "m1", OPENAI_BASE_URL: "https://llm.example.com/v1", ALLOW_IN_CLUSTER_HTTP: "yes" })).toThrow(/ALLOW_IN_CLUSTER_HTTP/);
  });

  test("an in-cluster gateway with ALLOW_IN_CLUSTER_HTTP=true gets the request", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: responsesBody(OUTPUT_MARKER) }]);
    const s = summarizerFromEnv(env({ OPENAI_BASE_URL: "http://llm-gateway.ai.svc:4000/v1", ALLOW_IN_CLUSTER_HTTP: "true" }), { fetch: fetchFn, log: captureLog().log });
    await s.summarize("short", digest, ctx);
    expect(calls.map((c) => c.url)).toEqual(["http://llm-gateway.ai.svc:4000/v1/responses"]);
  });

  test("defaults match session-brief's conservative settings", () => {
    const c = modelSummarizerConfig("m1", {});
    expect(c).toMatchObject({
      backgroundDailyBudget: 5_000,
      sessionDailyCap: 30,
      globalDailyCap: 20_000,
      maxAttempts: 2,
      timeoutMs: 20_000,
      maxOutputTokensShort: 256,
      maxOutputTokensLong: 1_024,
      reasoningEffort: "none",
    });
    expect(modelSummarizerConfig("m1", { BOB_COSTAS_REASONING_EFFORT: "omit" }).reasoningEffort).toBeUndefined();
    expect(modelSummarizerConfig("m1", { BOB_COSTAS_MAX_ATTEMPTS: "99" }).maxAttempts).toBe(2);
  });
});

describe("prompts", () => {
  for (const kind of ["short", "long"] as const) {
    test(`${kind} prompt carries the scrubbed digest fields and no raw secrets`, () => {
      const req = buildModelRequest(kind, digest, modelSummarizerConfig("m1", {}));
      const input = JSON.parse(req.input) as Record<string, unknown>;
      expect(input.harness).toBe("claude");
      expect(input.user_turn_count).toBe(2);
      expect(input.assistant_turn_count).toBe(1);
      expect(input.tool_calls).toEqual([{ name: "Bash", count: 2 }, { name: "Edit", count: 1 }]);
      expect(input.models).toEqual(["claude-sonnet-4-6"]);
      expect(input.first_ts).toBe("2026-10-28T10:00:00Z");
      expect(input.last_ts).toBe("2026-10-28T10:01:00Z");
      const turns = input.user_turns as string[];
      expect(turns[0]).toContain(PROMPT_MARKER);
      expect(turns[1]).toBe("now run the tests");
      const whole = JSON.stringify(req);
      for (const secret of [AWS_KEY, EMAIL, GH_TOKEN]) expect(whole).not.toContain(secret);
      expect(req.store).toBe(false);
      expect(req.maxOutputTokens as number).toBe(kind === "short" ? 256 : 1_024);
    });
  }
});

describe("model summariser", () => {
  test("a successful response is parsed, scrubbed, and carries the actual model id and usage", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: responsesBody(`${OUTPUT_MARKER}; leaked ${AWS_KEY} to ${EMAIL}`, { tokensIn: 321, tokensOut: 45 }) }]);
    const cap = captureLog();
    const s = summarizerFromEnv(env(), { fetch: fetchFn, log: cap.log });
    const out = await s.summarize("long", digest, ctx);
    expect(out.text).toContain(OUTPUT_MARKER);
    expect(out.text).not.toContain(AWS_KEY);
    expect(out.text).not.toContain(EMAIL);
    expect(out.modelId).toBe("gpt-test-mini-2026-10-01");
    expect(out.tokensIn).toBe(321);
    expect(out.tokensOut).toBe(45);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe("https://api.openai.com/v1/responses");
    expect(calls[0]!.headers.Authorization).toBe(`Bearer ${KEY}`);
    expect(calls[0]!.body.model).toBe("gpt-test-mini");
    expect(calls[0]!.body.max_output_tokens).toBe(1_024);
    expect(calls[0]!.body.reasoning).toEqual({ effort: "none" });
    expect(cap.lines).toHaveLength(1);
    expect(parsed(cap.lines[0]!)).toMatchObject({ outcome: "model", model_id: "gpt-test-mini-2026-10-01", attempt: 1, tokens_in: 321, tokens_out: 45, kind: "long", stage: "summary-long" });
    assertNoContentInLogs(cap.lines);
  });

  test("the short summary is bounded to 280 characters", async () => {
    const { fetchFn } = fakeFetch([{ status: 200, body: responsesBody("x".repeat(1_000)) }]);
    const out = await summarizerFromEnv(env(), { fetch: fetchFn, log: captureLog().log }).summarize("short", digest, ctx);
    expect(out.text.length).toBeLessThanOrEqual(280);
  });

  test("no API key: extractive fallback with reason no_api_key, no request", async () => {
    const { fetchFn, calls } = fakeFetch([]);
    const cap = captureLog();
    const s = summarizerFromEnv({ BOB_COSTAS_MODEL: "gpt-test-mini", OPENAI_API_KEY: "sk-disabled" }, { fetch: fetchFn, log: cap.log });
    const out = await s.summarize("short", digest, ctx);
    expect(out).toEqual(await extractiveSummarizer.summarize("short", digest));
    expect(calls).toHaveLength(0);
    expect(parsed(cap.lines[0]!)).toMatchObject({ outcome: "fallback", reason: "no_api_key", attempt: 0 });
    assertNoContentInLogs(cap.lines);
  });

  test("no context: extractive fallback with reason no_context, no request", async () => {
    const { fetchFn, calls } = fakeFetch([]);
    const cap = captureLog();
    const out = await summarizerFromEnv(env(), { fetch: fetchFn, log: cap.log }).summarize("short", digest);
    expect(out.modelId).toBe("extractive/v1");
    expect(calls).toHaveLength(0);
    expect(parsed(cap.lines[0]!)).toMatchObject({ reason: "no_context" });
  });

  test("a retryable failure is retried once and then succeeds (attempt 2)", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 503, body: { error: { message: `upstream body ${AWS_KEY}` } } },
      { status: 200, body: responsesBody(OUTPUT_MARKER, { tokensIn: 10, tokensOut: 5 }) },
    ]);
    const cap = captureLog();
    const sleeps: number[] = [];
    const s = summarizerFromEnv(env({ BOB_COSTAS_RETRY_DELAY_MS: "250" }), { fetch: fetchFn, log: cap.log, sleep: async (ms) => void sleeps.push(ms) });
    const out = await s.summarize("short", digest, ctx);
    expect(out.text).toContain(OUTPUT_MARKER);
    expect(out.tokensIn).toBe(10);
    expect(calls).toHaveLength(2);
    expect(sleeps).toEqual([250]);
    expect(parsed(cap.lines[0]!)).toMatchObject({ error_code: "model_unavailable", retryable: true, attempt: 1 });
    expect(parsed(cap.lines[1]!)).toMatchObject({ outcome: "model", attempt: 2 });
    assertNoContentInLogs(cap.lines, ["upstream body"]);
  });

  test("retryable failures past BOB_COSTAS_MAX_ATTEMPTS fall back with model_failed", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 429 }, { status: 429 }, { status: 429 }]);
    const cap = captureLog();
    const out = await summarizerFromEnv(env(), { fetch: fetchFn, log: cap.log }).summarize("long", digest, ctx);
    expect(out.modelId).toBe("extractive/v1");
    expect(calls).toHaveLength(2);
    expect(parsed(cap.lines.at(-1)!)).toMatchObject({ outcome: "fallback", reason: "model_failed", error_code: "model_rate_limited", attempt: 2 });
  });

  test("a non-retryable failure falls back at once, without the error body", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 400, body: { error: { message: `bad request echoing ${PROMPT_MARKER}` } } }]);
    const cap = captureLog();
    const out = await summarizerFromEnv(env(), { fetch: fetchFn, log: cap.log }).summarize("short", digest, ctx);
    expect(out).toEqual(await extractiveSummarizer.summarize("short", digest));
    expect(calls).toHaveLength(1);
    expect(cap.lines).toHaveLength(1);
    expect(parsed(cap.lines[0]!)).toMatchObject({ outcome: "fallback", reason: "model_failed", error_code: "model_request_failed", retryable: false, attempt: 1, model_id: "gpt-test-mini" });
    assertNoContentInLogs(cap.lines, ["bad request echoing"]);
  });

  test("unparseable model output falls back (model_output_invalid), tokens still reported", async () => {
    const body = responsesBody("ignored", { tokensIn: 7, tokensOut: 3 });
    (body.output[0]!.content[0] as { text: string }).text = `not json ${OUTPUT_MARKER}`;
    const { fetchFn } = fakeFetch([{ status: 200, body }]);
    const cap = captureLog();
    const out = await summarizerFromEnv(env(), { fetch: fetchFn, log: cap.log }).summarize("short", digest, ctx);
    expect(out.modelId).toBe("extractive/v1");
    expect(parsed(cap.lines[0]!)).toMatchObject({ reason: "model_failed", error_code: "model_output_invalid", tokens_in: 7, tokens_out: 3 });
    assertNoContentInLogs(cap.lines);
  });

  test("an unexpected transport exception never escapes", async () => {
    const cap = captureLog();
    const s = createModelSummarizer({
      transport: { complete: async () => { throw new Error(`boom ${PROMPT_MARKER}`); } },
      config: modelSummarizerConfig("m1", {}),
      log: cap.log,
    });
    const out = await s.summarize("short", digest, ctx);
    expect(out.modelId).toBe("extractive/v1");
    expect(parsed(cap.lines[0]!)).toMatchObject({ reason: "model_failed", error_code: "model_processing_failed" });
    assertNoContentInLogs(cap.lines, ["boom"]);
  });

  test("org budget exhausted: other sessions fall back with reason budget; admitted session keeps going", async () => {
    const { fetchFn, calls } = fakeFetch([
      { status: 200, body: responsesBody(OUTPUT_MARKER) },
      { status: 200, body: responsesBody(OUTPUT_MARKER) },
    ]);
    const cap = captureLog();
    const s = summarizerFromEnv(env({ BOB_COSTAS_BACKGROUND_DAILY_BUDGET: "1" }), { fetch: fetchFn, log: cap.log });
    expect((await s.summarize("short", digest, ctx)).modelId).toBe("gpt-test-mini-2026-10-01");
    const other = await s.summarize("short", digest, { ...ctx, sessionKey: '["w","claude","s2"]' });
    expect(other.modelId).toBe("extractive/v1");
    expect(parsed(cap.lines[1]!)).toMatchObject({ outcome: "fallback", reason: "budget", attempt: 0 });
    // Another org has its own budget.
    expect((await s.summarize("long", digest, { ...ctx, orgId: "org-2" })).modelId).toBe("gpt-test-mini-2026-10-01");
    expect(calls).toHaveLength(2);
  });

  test("session daily cap: attempts past the cap fall back with reason session_cap, also mid-retry", async () => {
    const { fetchFn, calls } = fakeFetch([{ status: 200, body: responsesBody(OUTPUT_MARKER) }, { status: 503 }]);
    const cap = captureLog();
    const s = summarizerFromEnv(env({ BOB_COSTAS_SESSION_DAILY_CAP: "2" }), { fetch: fetchFn, log: cap.log });
    expect((await s.summarize("short", digest, ctx)).modelId).toBe("gpt-test-mini-2026-10-01");
    // Second call: attempt 1 fails retryably (503), the retry is refused by the cap.
    const second = await s.summarize("long", digest, ctx);
    expect(second.modelId).toBe("extractive/v1");
    expect(parsed(cap.lines.at(-1)!)).toMatchObject({ reason: "session_cap", attempt: 1, error_code: "model_unavailable" });
    const third = await s.summarize("short", digest, ctx);
    expect(third.modelId).toBe("extractive/v1");
    expect(parsed(cap.lines.at(-1)!)).toMatchObject({ reason: "session_cap", attempt: 0 });
    expect(calls).toHaveLength(2);
  });

  test("global daily cap 0 disables model calls; counters reset at the UTC day boundary", () => {
    let now = new Date("2026-11-08T23:59:59Z");
    const a = new DailyAdmission({ sessionDailyCap: 1, budget: 10, globalDailyCap: 1 }, () => now);
    expect(a.admit("o", "s1")).toEqual({ admitted: true });
    expect(a.admit("o", "s1")).toEqual({ admitted: false, reason: "session_cap" });
    expect(a.admit("o", "s2")).toEqual({ admitted: false, reason: "global_cap" });
    now = new Date("2026-11-09T00:00:00Z");
    expect(a.admit("o", "s1")).toEqual({ admitted: true });
    const off = new DailyAdmission({ sessionDailyCap: 30, budget: 10, globalDailyCap: 0 });
    expect(off.admit("o", "s1")).toEqual({ admitted: false, reason: "global_cap" });
  });
});
