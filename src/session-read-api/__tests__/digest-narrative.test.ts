/**
 * The digest narrative off Neon (digest-narrative.ts): relayhistory's resolveNarrative
 * outcomes on the read side (cache through the read role, generation through the service-bound
 * Worker under a deadline), the Worker's generate-and-cache half, the request check, and the
 * omitted reverted count. Mocked model and cache only: no live model call.
 */
import { describe, expect, test } from "bun:test";
import {
  buildNarrativePrompt,
  extractionProviderFromEnv,
  generateNarrative,
  narrativeScopeKey,
  parseNarrative,
  parseNarrativeRequest,
  parseNarrativeResult,
  resolveDigestNarrative,
  type NarrativeGenerator,
  type NarrativeRequest,
  type NarrativeResult,
  type NarrativeRow,
} from "../digest-narrative.ts";
import { computeStatsHash, type DailyDigest } from "../digest-query.ts";

const digest = (events = 3): DailyDigest => ({
  stats: {
    sessionCount: events ? 1 : 0, eventCount: events, projectCount: 1, epicCount: 1, totalCostUsdMicros: 0, totalInputTokens: 0, totalOutputTokens: 0,
    models: ["m"], filesTouchedCount: 0, topFiles: [], outcomes: { committed: 1, reverted: 0, unknown: 0 },
  },
  projects: [{ projectId: "p", sessionCount: 1, eventCount: events, epicCount: 1, costUsdMicros: 0, firstTs: "2026-11-05T00:00:00.000Z", lastTs: "2026-11-05T00:00:00.000Z" }],
  epics: [],
  sessions: [{ projectId: "p", summary: "Fix the parser", eventCount: events, models: ["m"] }],
});
const params = { orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null, statsHash: "h1", refresh: false };
const generated: NarrativeResult = { text: "Fresh.", model: "m", generatedAt: "2026-11-06T00:00:00.000Z", cached: false, unavailable: null };
const generator = (impl: (req: NarrativeRequest, signal: AbortSignal) => Promise<NarrativeResult>) => {
  const calls: NarrativeRequest[] = [];
  const g: NarrativeGenerator = { generate: (req, signal) => (calls.push(req), impl(req, signal)) };
  return { g, calls };
};
const cache = (row: { statsHash: string } | null | Error) => ({
  reads: 0,
  async read() {
    this.reads++;
    if (row instanceof Error) throw row;
    return row ? { ...row, text: "Cached.", model: "m", generatedAt: "2026-11-05T23:00:00.000Z" } : null;
  },
});

describe("resolveDigestNarrative (read side)", () => {
  test("a quiet day is no_activity and calls nothing", async () => {
    const c = cache(null);
    const { g, calls } = generator(async () => generated);
    expect(await resolveDigestNarrative({ cache: c, generator: g }, params, digest(0))).toMatchObject({ unavailable: "no_activity", text: null });
    expect(c.reads + calls.length).toBe(0);
  });
  test("a cache hit whose stats hash matches is served cached; the Worker is not called", async () => {
    const { g, calls } = generator(async () => generated);
    expect(await resolveDigestNarrative({ cache: cache({ statsHash: "h1" }), generator: g }, params, digest())).toEqual({
      text: "Cached.", model: "m", generatedAt: "2026-11-05T23:00:00.000Z", cached: true, unavailable: null,
    });
    expect(calls).toHaveLength(0);
  });
  test("a stale hash or a miss generates through the Worker with relayhistory's workspace scope key", async () => {
    for (const row of [{ statsHash: "old" }, null]) {
      const { g, calls } = generator(async () => generated);
      expect(await resolveDigestNarrative({ cache: cache(row), generator: g }, params, digest())).toEqual(generated);
      expect(calls[0]!.scopeKey).toBe('workspace:["ws-a",null]');
    }
  });
  test("refresh=true skips the cache", async () => {
    const c = cache({ statsHash: "h1" });
    const { g, calls } = generator(async () => generated);
    await resolveDigestNarrative({ cache: c, generator: g }, { ...params, refresh: true }, digest());
    expect(c.reads).toBe(0);
    expect(calls).toHaveLength(1);
  });
  test("failures are 200-shaped narratives, never thrown: cache error, no Worker, deadline, bad result", async () => {
    expect((await resolveDigestNarrative({ cache: cache(new Error("pg")) }, params, digest())).unavailable).toBe("provider_error");
    expect((await resolveDigestNarrative({ cache: cache(null) }, params, digest())).unavailable).toBe("provider_not_configured");
    let aborted = false;
    const slow = generator((_r, signal) => new Promise((resolve) => signal.addEventListener("abort", () => ((aborted = true), resolve(generated)))));
    expect((await resolveDigestNarrative({ cache: cache(null), generator: slow.g, deadlineMs: 20 }, params, digest())).unavailable).toBe("provider_error");
    expect(aborted).toBe(true);
    const bad = generator(async () => ({ ...generated, extra: 1 }) as never);
    expect((await resolveDigestNarrative({ generator: bad.g }, params, digest())).unavailable).toBe("provider_error");
    const thrown = generator(async () => { throw new Error("binding"); });
    expect((await resolveDigestNarrative({ generator: thrown.g }, params, digest())).unavailable).toBe("provider_error");
  });
});

describe("generateNarrative (narrative Worker)", () => {
  const req = async (d = digest()): Promise<NarrativeRequest> => ({ ...params, scopeKey: narrativeScopeKey("ws-a", null), statsHash: await computeStatsHash(d, "ws-a"), digest: d });
  const provider = (extract: (prompt: string, signal: AbortSignal) => Promise<unknown>) => ({ model: "gpt-4o-mini", extract });
  test("generates, caches, then returns; the cached row is keyed by the request", async () => {
    const rows: NarrativeRow[] = [];
    const r = await generateNarrative(await req(), { provider: provider(async () => ({ narrative: "  A good day.  " })), upsert: async (row) => void rows.push(row), now: () => new Date("2026-11-06T01:00:00Z") });
    expect(r).toEqual({ text: "A good day.", model: "gpt-4o-mini", generatedAt: "2026-11-06T01:00:00.000Z", cached: false, unavailable: null });
    expect(rows[0]).toMatchObject({ orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", scopeKey: 'workspace:["ws-a",null]', text: "A good day." });
  });
  test("no key: provider_not_configured; timeout, unparseable, empty, oversized or upsert failure: provider_error (text never leaked)", async () => {
    expect((await generateNarrative(await req(), { provider: null, upsert: async () => {} })).unavailable).toBe("provider_not_configured");
    const slow = provider((_p, signal) => new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")))));
    expect((await generateNarrative(await req(), { provider: slow, upsert: async () => {}, timeoutMs: 20 })).unavailable).toBe("provider_error");
    for (const raw of [{ nope: 1 }, { narrative: "   " }, "text", { narrative: "x".repeat(16_385) }])
      expect((await generateNarrative(await req(), { provider: provider(async () => raw), upsert: async () => {} })).unavailable).toBe("provider_error");
    const r = await generateNarrative(await req(), { provider: provider(async () => ({ narrative: "secret prose" })), upsert: async () => { throw new Error("pg"); } });
    expect(r).toEqual({ text: null, model: null, generatedAt: null, cached: false, unavailable: "provider_error" });
  });
  test("the prompt omits outcomes.reverted and keeps relayhistory's bounds", () => {
    const d = digest();
    d.sessions = Array.from({ length: 30 }, () => ({ projectId: "p", summary: "s", eventCount: 1, models: [] }));
    const prompt = buildNarrativePrompt({ day: "2026-11-05", tz: "UTC", projectId: null, digest: d });
    const payload = JSON.parse(prompt.split("\n").at(-1)!);
    expect(payload.stats.outcomes).toEqual({ committed: 1, unknown: 0 });
    expect(prompt).not.toContain("reverted");
    expect(payload.sessions).toHaveLength(15);
    expect(prompt).toContain('Return ONLY raw JSON: {"narrative": "..."}');
  });
  test("parseNarrative is relayhistory's", () => {
    expect(parseNarrative({ narrative: " x " })).toBe("x");
    for (const v of [null, [], { narrative: 1 }, { narrative: "" }]) expect(parseNarrative(v)).toBeNull();
  });
});

describe("parseNarrativeRequest / parseNarrativeResult", () => {
  test("accepts a consistent request; refuses a stats hash that is not the digest's, or a scope key for another workspace", async () => {
    const d = digest();
    const ok = { ...params, scopeKey: narrativeScopeKey("ws-a", null), statsHash: await computeStatsHash(d, "ws-a"), digest: d };
    expect(await parseNarrativeRequest(ok, computeStatsHash)).not.toBeNull();
    expect(await parseNarrativeRequest({ ...ok, statsHash: "f".repeat(64) }, computeStatsHash)).toBeNull();
    expect(await parseNarrativeRequest({ ...ok, scopeKey: narrativeScopeKey("ws-b", null) }, computeStatsHash)).toBeNull();
    expect(await parseNarrativeRequest({ ...ok, orgId: "" }, computeStatsHash)).toBeNull();
    expect(await parseNarrativeRequest({ ...ok, day: "11/05/2026" }, computeStatsHash)).toBeNull();
  });
  test("results must be exactly the cloud-client narrativeResult shape", () => {
    expect(parseNarrativeResult(generated)).toEqual(generated);
    expect(parseNarrativeResult({ ...generated, unavailable: "nope" })).toBeNull();
    expect(parseNarrativeResult({ ...generated, generatedAt: "not a date" })).toBeNull();
  });
});

describe("extractionProviderFromEnv", () => {
  test("no key = null; a key posts relayhistory's JSON-mode request to the configured URL (mocked fetch)", async () => {
    expect(extractionProviderFromEnv({})).toBeNull();
    const seen: Array<{ url: string; body: any; auth: string | null }> = [];
    const fetcher = (async (url: string, init: RequestInit) => {
      seen.push({ url, body: JSON.parse(String(init.body)), auth: new Headers(init.headers).get("authorization") });
      return Response.json({ choices: [{ message: { content: '{"narrative":"ok"}' } }] });
    }) as unknown as typeof fetch;
    const p = extractionProviderFromEnv({ EXTRACTION_API_KEY: "k-test", EXTRACTION_API_URL: "https://model.invalid/v1", EXTRACTION_MODEL: "m-1" }, fetcher)!;
    expect(await p.extract("prompt", new AbortController().signal)).toEqual({ narrative: "ok" });
    expect(seen[0]).toMatchObject({ url: "https://model.invalid/v1", auth: "Bearer k-test", body: { model: "m-1", temperature: 0, response_format: { type: "json_object" } } });
  });
});
