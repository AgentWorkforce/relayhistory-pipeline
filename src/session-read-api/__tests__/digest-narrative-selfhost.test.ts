/**
 * The self-host digest narrative (digest-narrative-selfhost.ts): the OpenAI-compatible provider
 * against a fake fetch, env parsing, the separate writer pool and the read API service wiring.
 * No network and no database: pg.Pool is replaced by a recorder.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { generateNarrative, resolveDigestNarrative, UPSERT_SQL, type NarrativeRequest, type NarrativeRow } from "../digest-narrative.ts";
import type { DailyDigest } from "../digest-query.ts";
import {
  MAX_RESPONSE_BYTES,
  WRITER_LOGIN_SQL,
  assertNarrativeWriterRole,
  chatCompletionsUrl,
  digestNarrativeModel,
  inProcessNarrativeGenerator,
  narrativeWriterPoolTarget,
  openAiChatNarrativeProvider,
  selfhostNarrativeFromEnv,
} from "../digest-narrative-selfhost.ts";
import { readApiDepsFromSecret, READER_LOGIN_SQL } from "../lambda.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
import { selfhostSearchComposition } from "../server.ts";

const SECRET_KEY = "sk-test-narrative-key-0123456789abcdefghij";
const LEAK = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789";
const URL_OK = "https://api.openai.com/v1/chat/completions";

const digest = (events = 3, summary = "Fix the parser"): DailyDigest => ({
  stats: {
    sessionCount: events ? 1 : 0, eventCount: events, projectCount: 1, epicCount: 1, totalCostUsdMicros: 0, totalInputTokens: 0, totalOutputTokens: 0,
    models: ["m"], filesTouchedCount: 0, topFiles: [], outcomes: { committed: 1, reverted: 0, unknown: 0 },
  },
  projects: [{ projectId: "p", sessionCount: 1, eventCount: events, epicCount: 1, costUsdMicros: 0, firstTs: "2026-11-05T00:00:00.000Z", lastTs: "2026-11-05T00:00:00.000Z" }],
  epics: [],
  sessions: [{ projectId: "p", summary, eventCount: events, models: ["m"] }],
});
const req = (d = digest()): NarrativeRequest => ({
  orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null,
  scopeKey: 'workspace:["ws-a",null]', statsHash: "a".repeat(64), digest: d,
});
const chat = (content: unknown, status = 200) =>
  new Response(JSON.stringify({ choices: [{ message: { content: typeof content === "string" ? content : JSON.stringify(content) } }] }), { status, headers: { "content-type": "application/json" } });

type Call = { url: string; init: RequestInit };
const fakeFetch = (impl: (call: Call) => Promise<Response>) => {
  const calls: Call[] = [];
  const f = ((url: string, init: RequestInit) => { const c = { url: String(url), init }; calls.push(c); return impl(c); }) as unknown as typeof fetch;
  return { f, calls };
};

/** Captures every log line (stdout/stderr) during `work`. */
async function captureLogs<T>(work: () => Promise<T>): Promise<{ result: T; out: string }> {
  const lines: string[] = [];
  const orig = { log: console.log, error: console.error, warn: console.warn, info: console.info };
  const w1 = process.stdout.write.bind(process.stdout), w2 = process.stderr.write.bind(process.stderr);
  const sink = (...a: unknown[]) => { lines.push(a.map(String).join(" ")); };
  Object.assign(console, { log: sink, error: sink, warn: sink, info: sink });
  process.stdout.write = ((s: string) => (lines.push(String(s)), true)) as typeof process.stdout.write;
  process.stderr.write = ((s: string) => (lines.push(String(s)), true)) as typeof process.stderr.write;
  try { return { result: await work(), out: lines.join("\n") }; }
  finally { Object.assign(console, orig); process.stdout.write = w1; process.stderr.write = w2; }
}

describe("openAiChatNarrativeProvider", () => {
  test("posts the hosted Worker's chat-completions request with a scrubbed prompt; scrubs the narrative", async () => {
    const { f, calls } = fakeFetch(async () => chat({ narrative: `Shipped the parser fix. ${LEAK}` }));
    const provider = openAiChatNarrativeProvider({ model: "gpt-4o-mini", apiKey: SECRET_KEY, url: URL_OK, fetch: f });
    const rows: NarrativeRow[] = [];
    const r = await generateNarrative(req(digest(3, `rotated ${LEAK} today`)), { provider, upsert: async (row) => { rows.push(row); }, now: () => new Date("2026-11-06T00:00:00Z") });
    expect(r).toEqual({ text: "Shipped the parser fix. [REDACTED]", model: "gpt-4o-mini", generatedAt: "2026-11-06T00:00:00.000Z", cached: false, unavailable: null });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.text).not.toContain(LEAK);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe(URL_OK);
    expect(calls[0]!.init.method).toBe("POST");
    expect(calls[0]!.init.redirect).toBe("manual");
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe(`Bearer ${SECRET_KEY}`);
    const body = JSON.parse(String(calls[0]!.init.body));
    expect(body).toMatchObject({ model: "gpt-4o-mini", temperature: 0, response_format: { type: "json_object" } });
    expect(body.messages).toHaveLength(1);
    expect(body.messages[0].content).toContain("[REDACTED]");
    expect(body.messages[0].content).not.toContain(LEAK);
  });

  test("non-2xx: no narrative, nothing cached, body unread, and neither the body nor the key is logged", async () => {
    let bodyRead = false;
    const { f } = fakeFetch(async () => {
      const stream = new ReadableStream({ pull(c) { bodyRead = true; c.enqueue(new TextEncoder().encode(`provider says ${SECRET_KEY} secret-body`)); c.close(); } }, { highWaterMark: 0 });
      return new Response(stream, { status: 500 });
    });
    const provider = openAiChatNarrativeProvider({ model: "m", apiKey: SECRET_KEY, url: URL_OK, fetch: f });
    const rows: NarrativeRow[] = [];
    const { result, out } = await captureLogs(() => generateNarrative(req(), { provider, upsert: async (row) => { rows.push(row); } }));
    expect(result).toMatchObject({ text: null, unavailable: "provider_error" });
    expect(rows).toEqual([]);
    expect(bodyRead).toBe(false);
    expect(out).not.toContain("secret-body");
    expect(out).not.toContain(SECRET_KEY);
    // The error itself names the status only.
    await provider.extract("p", new AbortController().signal).then(() => { throw new Error("expected a throw"); }, (e: Error) => {
      expect(e.message).toBe("digest narrative provider: http_status 500");
    });
  });

  test("a redirect is refused like any non-2xx (the key never follows it)", async () => {
    const { f, calls } = fakeFetch(async () => new Response(null, { status: 307, headers: { location: "https://elsewhere.invalid/" } }));
    const provider = openAiChatNarrativeProvider({ model: "m", apiKey: SECRET_KEY, url: URL_OK, fetch: f });
    expect((await generateNarrative(req(), { provider, upsert: async () => {} })).unavailable).toBe("provider_error");
    expect(calls).toHaveLength(1);
  });

  test("timeout: the 10 s bound (shortened here) aborts the call; nothing cached", async () => {
    let aborted = false;
    const { f } = fakeFetch(({ init }) => new Promise((_, reject) => init.signal!.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); })));
    const provider = openAiChatNarrativeProvider({ model: "m", apiKey: SECRET_KEY, url: URL_OK, fetch: f });
    const rows: NarrativeRow[] = [];
    const r = await generateNarrative(req(), { provider, upsert: async (row) => { rows.push(row); }, timeoutMs: 20 });
    expect(r.unavailable).toBe("provider_error");
    expect(aborted).toBe(true);
    expect(rows).toEqual([]);
  });

  test("invalid output, oversized responses and transport failures are provider_error", async () => {
    for (const make of [
      async () => chat("not json"),
      async () => chat({ other: 1 }),
      async () => chat({ narrative: "   " }),
      async () => new Response("{", { status: 200 }),
      async () => new Response("x".repeat(MAX_RESPONSE_BYTES + 1), { status: 200 }),
      async () => new Response("{}", { status: 200, headers: { "content-length": String(MAX_RESPONSE_BYTES + 1) } }),
      async () => { throw new Error(`ECONNREFUSED ${SECRET_KEY}`); },
    ]) {
      const provider = openAiChatNarrativeProvider({ model: "m", apiKey: SECRET_KEY, url: URL_OK, fetch: fakeFetch(make).f });
      const error = await provider.extract("p", new AbortController().signal).then(() => null, (e: Error) => e);
      if (error) expect(error.message).not.toContain(SECRET_KEY);
      expect((await generateNarrative(req(), { provider, upsert: async () => {} })).unavailable).toBe("provider_error");
    }
  });
});

describe("env parsing", () => {
  test("READ_API_DIGEST_NARRATIVE_MODEL: unset/blank = not configured; invalid throws without echoing", () => {
    expect(digestNarrativeModel({})).toBeNull();
    expect(digestNarrativeModel({ READ_API_DIGEST_NARRATIVE_MODEL: "  " })).toBeNull();
    expect(digestNarrativeModel({ READ_API_DIGEST_NARRATIVE_MODEL: " gpt-4o-mini " })).toBe("gpt-4o-mini");
    expect(digestNarrativeModel({ READ_API_DIGEST_NARRATIVE_MODEL: "us.anthropic.claude:0" })).toBe("us.anthropic.claude:0");
    for (const bad of ["has space", `${SECRET_KEY}?x=1`, "x".repeat(200), "-leading"]) {
      const e = (() => { try { digestNarrativeModel({ READ_API_DIGEST_NARRATIVE_MODEL: bad }); return null; } catch (err) { return err as Error; } })();
      expect(e?.message).toBe("96bulls: READ_API_DIGEST_NARRATIVE_MODEL is not a valid model id");
    }
  });

  test("OPENAI_BASE_URL: default OpenAI; https or loopback; in-cluster http only with ALLOW_IN_CLUSTER_HTTP=true", () => {
    expect(chatCompletionsUrl({})).toBe(URL_OK);
    expect(chatCompletionsUrl({ OPENAI_BASE_URL: "https://gateway.example/v1/" })).toBe("https://gateway.example/v1/chat/completions");
    expect(chatCompletionsUrl({ OPENAI_BASE_URL: "http://127.0.0.1:4000/v1" })).toBe("http://127.0.0.1:4000/v1/chat/completions");
    const svc = "http://llm-gateway.models.svc.cluster.local:8080/v1";
    expect(() => chatCompletionsUrl({ OPENAI_BASE_URL: svc })).toThrow(/OPENAI_BASE_URL must use https/);
    expect(() => chatCompletionsUrl({ OPENAI_BASE_URL: svc, ALLOW_IN_CLUSTER_HTTP: "false" })).toThrow(/OPENAI_BASE_URL must use https/);
    expect(chatCompletionsUrl({ OPENAI_BASE_URL: svc, ALLOW_IN_CLUSTER_HTTP: "true" })).toBe(`${svc}/chat/completions`);
    for (const bad of ["http://gateway.example/v1", "http://10.0.0.5/v1", "https://user:pw@gateway.example/v1", "https://gateway.example/v1?key=x", "ftp://gateway.example"]) {
      const e = (() => { try { chatCompletionsUrl({ OPENAI_BASE_URL: bad, ALLOW_IN_CLUSTER_HTTP: "true" }); return null; } catch (err) { return err as Error; } })();
      expect(e?.message).toMatch(/^96bulls: OPENAI_BASE_URL /);
      expect(e?.message).not.toContain("gateway.example");
      expect(e?.message).not.toContain("10.0.0.5");
    }
    expect(() => chatCompletionsUrl({ ALLOW_IN_CLUSTER_HTTP: "yes" })).toThrow(/ALLOW_IN_CLUSTER_HTTP must be true or false/);
  });

  test("READ_API_DIGEST_NARRATIVE_DB_URL: discrete fields; refusals never echo the URL", () => {
    expect(narrativeWriterPoolTarget("postgresql://bulls96_digest_narrative:p%40ss@db.internal:6543/history?sslmode=disable&options=-c%20statement_timeout%3D0"))
      .toEqual({ host: "db.internal", port: 6543, user: "bulls96_digest_narrative", password: "p@ss", database: "history" });
    for (const bad of ["postgresql://u@db.internal/history", "postgresql://u:pw@db.internal/", "mysql://u:pw@db.internal/h", "postgresql://u:pw@db.internal/h?host=evil", "not a url"]) {
      const e = (() => { try { narrativeWriterPoolTarget(bad); return null; } catch (err) { return err as Error; } })();
      expect(e?.message).toBe("session read api: READ_API_DIGEST_NARRATIVE_DB_URL needs a postgres URL with a user, password and database");
    }
  });

  test("writer login: exactly bulls96_digest_narrative, not elevated", async () => {
    const ok = { session_user: "bulls96_digest_narrative", current_user: "bulls96_digest_narrative", role_setting: "none", rolsuper: false, rolbypassrls: false };
    const q = (row?: Record<string, unknown>) => ({ query: async <R>(text: string) => { expect(text).toBe(WRITER_LOGIN_SQL); return { rows: (row ? [row] : []) as R[] }; } });
    await assertNarrativeWriterRole(q(ok));
    await expect(assertNarrativeWriterRole(q())).rejects.toThrow(/role_unknown/);
    await expect(assertNarrativeWriterRole(q({ ...ok, session_user: "bulls96_read_api", current_user: "bulls96_read_api" }))).rejects.toThrow(/role_mismatch/);
    await expect(assertNarrativeWriterRole(q({ ...ok, session_user: "postgres", role_setting: "bulls96_digest_narrative" }))).rejects.toThrow(/role_mismatch/);
    await expect(assertNarrativeWriterRole(q({ ...ok, rolsuper: true }))).rejects.toThrow(/role_elevated/);
    await expect(assertNarrativeWriterRole(q({ ...ok, rolbypassrls: true }))).rejects.toThrow(/role_elevated/);
  });
});

describe("in-process generation", () => {
  test("the read gives up at its deadline; generation continues and caches; concurrent reads share one call", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let calls = 0;
    const provider = { model: "m", async extract() { calls++; await gate; return { narrative: "Late but cached." }; } };
    const rows: NarrativeRow[] = [];
    const generator = inProcessNarrativeGenerator({ provider, upsert: async (row) => { rows.push(row); }, log: () => {} });
    const params = { orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null, statsHash: "a".repeat(64), refresh: true };
    const [a, b] = await Promise.all([
      resolveDigestNarrative({ generator, deadlineMs: 20 }, params, digest()),
      resolveDigestNarrative({ generator, deadlineMs: 20 }, params, digest()),
    ]);
    expect(a.unavailable).toBe("provider_error");
    expect(b.unavailable).toBe("provider_error");
    expect(calls).toBe(1);
    release();
    await new Promise((r) => setTimeout(r, 10));
    expect(rows.map((r) => r.text)).toEqual(["Late but cached."]);
  });

  test("at most maxInflight distinct generations; the rest are provider_error without a model call", async () => {
    let calls = 0;
    const provider = { model: "m", extract: () => { calls++; return new Promise<unknown>(() => {}); } };
    const generator = inProcessNarrativeGenerator({ provider, upsert: async () => {}, maxInflight: 2, timeoutMs: 50, log: () => {} });
    const base = { orgId: "org-a", workspaceId: "ws-a", tz: "UTC", projectId: null, statsHash: "a".repeat(64), refresh: true };
    const results = await Promise.all(["2026-11-01", "2026-11-02", "2026-11-03"].map((day) => resolveDigestNarrative({ generator, deadlineMs: 10 }, { ...base, day }, digest())));
    expect(results.every((r) => r.unavailable === "provider_error")).toBe(true);
    expect(calls).toBe(2);
    await new Promise((r) => setTimeout(r, 80)); // the 50 ms generation timeout frees the slots
    await resolveDigestNarrative({ generator, deadlineMs: 10 }, { ...base, day: "2026-11-04" }, digest());
    expect(calls).toBe(3);
  });
});

// ---- the read API service wiring, with pg.Pool recorded ----

const PG_URL = "postgresql://reader:pw@db.selfhost.internal:6543/history";
const WRITER_URL = "postgresql://bulls96_digest_narrative:wpw@db.selfhost.internal:6543/history";
const ENV = {
  RELAYHISTORY_SELFHOST_READ_API: "true",
  RELAYHISTORY_READ_API_SECRET: JSON.stringify({ supabase_url: PG_URL }),
  READ_API_COMPLETE_SINCE: "epoch",
  SESSION_POSTGRES_TLS_PROFILE: "supabase",
  SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
} as const;
const NARRATIVE_ENV = { READ_API_DIGEST_ENABLED: "1", READ_API_DIGEST_NARRATIVE_MODEL: "gpt-4o-mini", OPENAI_API_KEY: SECRET_KEY, READ_API_DIGEST_NARRATIVE_DB_URL: WRITER_URL };
const WRITER_ROW = { session_user: "bulls96_digest_narrative", current_user: "bulls96_digest_narrative", role_setting: "none", rolsuper: false, rolbypassrls: false };

type Recorded = { pool: number; text: string; params?: unknown[] };
let pools: { config: pg.PoolConfig; ended: boolean }[] = [];
let sql: Recorded[] = [];
let writerRow: Record<string, unknown> = WRITER_ROW;
const RealPool = pg.Pool;
const answer = (text: string) => ({ rows: text === WRITER_LOGIN_SQL ? [writerRow] : text.includes("to_regprocedure") ? [{ installed: true }] : readerLoginAnswer(text) ?? [] });
class RecordingPool {
  private readonly id: number;
  constructor(config: pg.PoolConfig) { this.id = pools.length; pools.push({ config, ended: false }); }
  on() { return this; }
  async query(text: string, params?: unknown[]) { sql.push({ pool: this.id, text, params }); return answer(text); }
  async connect() { return { query: async (text: string, params?: unknown[]) => { sql.push({ pool: this.id, text, params }); return answer(text); }, release() {} }; }
  async end() { pools[this.id]!.ended = true; }
}
beforeEach(() => { pools = []; sql = []; writerRow = WRITER_ROW; pg.Pool = RecordingPool as unknown as typeof pg.Pool; });
afterEach(() => { pg.Pool = RealPool; });

const build = (env: NodeJS.ProcessEnv) => readApiDepsFromSecret(ENV.RELAYHISTORY_READ_API_SECRET, env, selfhostSearchComposition(env), { entry: "service" });
const shape = (deps: object) => Object.fromEntries(Object.entries(deps).map(([k, v]) => [k, typeof v === "function" ? "function" : v instanceof Set ? [...v] : typeof v === "object" && v ? Object.keys(v).sort() : v]));

describe("read API service wiring", () => {
  test("the new vars unset: deps are exactly today's, one pool, no narrative", async () => {
    const before = await build({ ...ENV });
    const queriesBefore = sql.map((s) => s.text);
    expect(before.digestNarrative).toBeUndefined();
    expect(Object.keys(before).sort()).toEqual(["authConfig", "close", "deps"]);
    expect(pools).toHaveLength(1);
    // Digest flag set but nothing else, and narrative vars set with the digest flag off: deps unchanged.
    for (const extra of [{ READ_API_DIGEST_ENABLED: "0" }, { ...NARRATIVE_ENV, READ_API_DIGEST_ENABLED: "" }, { READ_API_DIGEST_ENABLED: "true" }]) {
      pools = []; sql = [];
      const other = await build({ ...ENV, ...extra });
      expect(shape(other.deps)).toEqual(shape(before.deps));
      expect(other.digestNarrative).toBeUndefined();
      expect(pools).toHaveLength(1);
      expect(sql.map((s) => s.text)).toEqual(queriesBefore);
      await other.close();
    }
    await before.close();
  });

  test("an invalid model id refuses startup before any pool opens, without echoing it", async () => {
    await expect(build({ ...ENV, READ_API_DIGEST_ENABLED: "1", READ_API_DIGEST_NARRATIVE_MODEL: `${SECRET_KEY} x` })).rejects.toThrow("96bulls: READ_API_DIGEST_NARRATIVE_MODEL is not a valid model id");
    expect(pools).toEqual([]);
  });

  test("with the flag off an invalid model id is not read: startup unchanged", async () => {
    const built = await build({ ...ENV, READ_API_DIGEST_NARRATIVE_MODEL: `${SECRET_KEY} x` });
    expect(built.digestNarrative).toBeUndefined();
    expect(pools).toHaveLength(1);
    await built.close();
  });

  test("digest on, no model: cache only (provider_not_configured on a miss), no writer pool", async () => {
    const built = await build({ ...ENV, READ_API_DIGEST_ENABLED: "1", OPENAI_API_KEY: SECRET_KEY, READ_API_DIGEST_NARRATIVE_DB_URL: WRITER_URL });
    expect(built.digestNarrative?.cache).toBeDefined();
    expect(built.digestNarrative?.generator).toBeUndefined();
    expect(pools).toHaveLength(1);
    expect(built.deps).not.toHaveProperty("digest");
    await built.close();
  });

  test("digest on with a model but no writer URL (or no key): read-only from the cache, no model call", async () => {
    for (const env of [{ ...NARRATIVE_ENV, READ_API_DIGEST_NARRATIVE_DB_URL: "" }, { ...NARRATIVE_ENV, OPENAI_API_KEY: "sk-x-disabled" }]) {
      pools = [];
      const built = await build({ ...ENV, ...env });
      expect(built.digestNarrative?.generator).toBeUndefined();
      expect(pools).toHaveLength(1);
      const r = await resolveDigestNarrative(built.digestNarrative!, { orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null, statsHash: "a".repeat(64), refresh: false }, digest());
      expect(r.unavailable).toBe("provider_not_configured");
      await built.close();
    }
  });

  test("fully configured: the writer pool is separate (max 1, TLS, timeout) and is the only pool that sees the upsert", async () => {
    const built = await build({ ...ENV, ...NARRATIVE_ENV });
    expect(built.deps).not.toHaveProperty("digest"); // no digest backend selected (READ_API_DIGEST_BACKEND unset)
    expect(pools).toHaveLength(2);
    const [reader, writer] = pools;
    expect(writer!.config).toMatchObject({ host: "db.selfhost.internal", user: "bulls96_digest_narrative", password: "wpw", database: "history", max: 1, statement_timeout: 5000, options: "-c statement_timeout=5000" });
    expect(writer!.config.connectionString).toBeUndefined();
    expect(writer!.config.ssl).toMatchObject({ rejectUnauthorized: true });
    expect(reader!.config.user).toBe("reader");
    expect(sql.filter((s) => s.pool === 1).map((s) => s.text)).toEqual([WRITER_LOGIN_SQL]);

    // A cache miss through the real generator, with the model call faked at the transport.
    const realFetch = globalThis.fetch;
    const seen: string[] = [];
    globalThis.fetch = (async (url: string) => { seen.push(String(url)); return chat({ narrative: "A quiet, productive day." }); }) as unknown as typeof fetch;
    try {
      sql = [];
      const { result } = await captureLogs(() => resolveDigestNarrative(built.digestNarrative!, { orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null, statsHash: "a".repeat(64), refresh: false }, digest()));
      expect(result).toMatchObject({ text: "A quiet, productive day.", model: "gpt-4o-mini", cached: false, unavailable: null });
    } finally { globalThis.fetch = realFetch; }
    expect(seen).toEqual([URL_OK]);
    const upserts = sql.filter((s) => s.text === UPSERT_SQL);
    expect(upserts).toHaveLength(1);
    expect(upserts[0]!.pool).toBe(1);
    expect(upserts[0]!.params?.slice(0, 7)).toEqual(["org-a", "ws-a", "2026-11-05", "UTC", 'workspace:["ws-a",null]', "a".repeat(64), "A quiet, productive day."]);
    // The read pool saw only the read-only tenant transaction for the cache read.
    const readSql = sql.filter((s) => s.pool === 0).map((s) => s.text);
    expect(readSql[0]).toBe("BEGIN READ ONLY");
    expect(readSql.some((t) => t.includes("digest_narrative_read"))).toBe(true);
    expect(readSql.some((t) => t.includes("digest_narrative_upsert"))).toBe(false);
    await built.close();
    expect(pools.every((p) => p.ended)).toBe(true);
  });

  test("with READ_API_DIGEST_BACKEND=postgres the digest route gets these narrative deps (the generator included)", async () => {
    const built = await build({ ...ENV, ...NARRATIVE_ENV, READ_API_DIGEST_BACKEND: "postgres" });
    expect(built.deps.digest?.backend?.kind).toBe("postgres");
    expect(built.deps.digest?.generator).toBeDefined();
    expect(built.deps.digest?.generator).toBe(built.digestNarrative?.generator);
    expect(built.deps.digest?.cache).toBe(built.digestNarrative?.cache);
    expect(pools).toHaveLength(2);
    await built.close();
    expect(pools.every((p) => p.ended)).toBe(true);
    // No model: the route's narrative is the cache only (a miss is provider_not_configured).
    pools = [];
    const cacheOnly = await build({ ...ENV, READ_API_DIGEST_ENABLED: "1", READ_API_DIGEST_BACKEND: "postgres" });
    expect(cacheOnly.deps.digest?.cache).toBeDefined();
    expect(cacheOnly.deps.digest?.generator).toBeUndefined();
    expect(pools).toHaveLength(1);
    await cacheOnly.close();
  });

  test("a writer login that is not the narrative role refuses startup and closes both pools", async () => {
    writerRow = { ...WRITER_ROW, session_user: "postgres", current_user: "postgres", rolsuper: true };
    await expect(build({ ...ENV, ...NARRATIVE_ENV })).rejects.toThrow(/narrative writer role_mismatch/);
    expect(pools).toHaveLength(2);
    expect(pools.every((p) => p.ended)).toBe(true);
  });

  test("a bad OPENAI_BASE_URL refuses startup only when the provider would be built", async () => {
    await expect(build({ ...ENV, ...NARRATIVE_ENV, OPENAI_BASE_URL: "http://gateway.example/v1" })).rejects.toThrow(/OPENAI_BASE_URL must use https/);
    const off = await build({ ...ENV, OPENAI_BASE_URL: "http://gateway.example/v1" });
    expect(off.digestNarrative).toBeUndefined();
    await off.close();
  });

  test("selfhostNarrativeFromEnv is inert when READ_API_DIGEST_ENABLED is not exactly 1", async () => {
    const run = (async () => { throw new Error("must not run"); }) as never;
    for (const v of [undefined, "", "0", "true", "yes"]) expect(await selfhostNarrativeFromEnv({ ...NARRATIVE_ENV, READ_API_DIGEST_ENABLED: v }, run)).toBeUndefined();
    expect(pools).toEqual([]);
  });
});

// READER_LOGIN_SQL/ROW are imported so a change to the reader admission shape breaks this file loudly.
void READER_LOGIN_SQL; void READER_LOGIN_ROW;
