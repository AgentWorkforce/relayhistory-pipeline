/**
 * The narrative Worker's handler (packages/session-read-worker/src/digest-narrative.ts):
 * POST /v1/narrative only, bounded and validated input, generation awaited AND kept alive in
 * waitUntil, no writer = no model call. Mocked model and upsert; no live call, no database.
 */
import { describe, expect, test } from "bun:test";
import { handleNarrative } from "../../../packages/session-read-worker/src/digest-narrative.ts";
import { narrativeScopeKey, type NarrativeRow } from "../digest-narrative.ts";
import { computeStatsHash, type DailyDigest } from "../digest-query.ts";

const digest: DailyDigest = {
  stats: { sessionCount: 1, eventCount: 2, projectCount: 1, epicCount: 1, totalCostUsdMicros: 0, totalInputTokens: 0, totalOutputTokens: 0, models: [], filesTouchedCount: 0, topFiles: [], outcomes: { committed: 0, reverted: 0, unknown: 1 } },
  projects: [], epics: [], sessions: [],
};
const body = async () => ({ orgId: "org-a", workspaceId: "ws-a", day: "2026-11-05", tz: "UTC", projectId: null, scopeKey: narrativeScopeKey("ws-a", null), statsHash: await computeStatsHash(digest, "ws-a"), digest });
const post = (b: unknown, path = "/v1/narrative") => new Request(`https://digest-narrative.internal${path}`, { method: "POST", body: typeof b === "string" ? b : JSON.stringify(b) });
const model = (content: string) => (async () => Response.json({ choices: [{ message: { content } }] })) as unknown as typeof fetch;

describe("digest narrative Worker", () => {
  test("generates, upserts, keeps the work alive in waitUntil, and answers the NarrativeResult", async () => {
    const rows: NarrativeRow[] = [];
    const waited: Promise<unknown>[] = [];
    const r = await handleNarrative(post(await body()), { EXTRACTION_API_KEY: "k-test" }, { waitUntil: (p) => void waited.push(p) }, {
      upsert: async (row) => void rows.push(row), fetcher: model('{"narrative":"Quiet but steady."}'),
    });
    expect(r.status).toBe(200);
    expect(await r.json()).toMatchObject({ text: "Quiet but steady.", cached: false, unavailable: null });
    expect(waited).toHaveLength(1);
    expect(rows).toHaveLength(1);
  });
  test("only POST /v1/narrative; malformed, inconsistent or oversized input is refused before any model call", async () => {
    let calls = 0;
    const fetcher = (async () => (calls++, Response.json({}))) as unknown as typeof fetch;
    const env = { EXTRACTION_API_KEY: "k-test" };
    const ctx = { waitUntil: () => {} };
    expect((await handleNarrative(new Request("https://x/v1/narrative"), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(404);
    expect((await handleNarrative(post(await body(), "/other"), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(404);
    expect((await handleNarrative(post("{not json"), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(400);
    expect((await handleNarrative(post({ ...(await body()), statsHash: "0".repeat(64) }), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(400);
    expect((await handleNarrative(post({ ...(await body()), scopeKey: narrativeScopeKey("ws-b", null) }), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(400);
    expect((await handleNarrative(post("x".repeat(1024 * 1024 + 1)), env, ctx, { upsert: async () => {}, fetcher })).status).toBe(413);
    expect(calls).toBe(0);
  });
  test("no writer (no NARRATIVE_DB) or no key: provider_not_configured, and the model is never called", async () => {
    let calls = 0;
    const fetcher = (async () => (calls++, Response.json({}))) as unknown as typeof fetch;
    const noWriter = await handleNarrative(post(await body()), { EXTRACTION_API_KEY: "k-test" }, { waitUntil: () => {} }, { fetcher });
    expect((await noWriter.json()).unavailable).toBe("provider_not_configured");
    const noKey = await handleNarrative(post(await body()), {}, { waitUntil: () => {} }, { upsert: async () => {}, fetcher });
    expect((await noKey.json()).unavailable).toBe("provider_not_configured");
    expect(calls).toBe(0);
  });
});
