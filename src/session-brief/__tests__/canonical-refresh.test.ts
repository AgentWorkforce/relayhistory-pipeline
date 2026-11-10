import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { database, hoopDb, pointer, KEY, FakeModel, testConfig } from "./harness.ts";
import { armWakeup, recordBrief, scrubBrief, sweepUnbriefed, admit, claimWakeups } from "../store.ts";
import { briefTick } from "../worker.ts";
import { briefInputFromCanonical } from "../canonical-input.ts";
import { SESSION_BRIEF_CANONICAL_ENGINE_VERSION, SESSION_BRIEF_ENGINE_VERSION, SESSION_BRIEF_PROMPT_VERSION } from "../versions.ts";

async function setup() {
  const pg = await database(), db = hoopDb(pg), model = new FakeModel();await pointer(pg);
  const snapshot = { exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: [
    { table: "convergence_events", row: { source: KEY.harness, session_id: KEY.sessionId, event_id: "h", kind: "history", type: "user", content: "Synthetic greeting", ts: "2026-11-09T01:00:00Z" } },
    { table: "convergence_events", row: { source: KEY.harness, session_id: KEY.sessionId, event_id: "u", kind: "session_event", type: "user", content: "Synthetic greeting", ts: "2026-11-09T01:00:00Z" } },
    { table: "convergence_events", row: { source: KEY.harness, session_id: KEY.sessionId, event_id: "a", kind: "session_event", type: "assistant", content: "Synthetic response", ts: "2026-11-09T01:00:01Z" } },
  ] };
  const canonicalInput = async (_db: unknown, key: typeof KEY, pointers: Parameters<typeof briefInputFromCanonical>[1]) => ({ ...briefInputFromCanonical(key, pointers, snapshot), canonicalSessionId: key.sessionId });
  const input = briefInputFromCanonical(KEY, [{ machineId: "m-1", manifestKey: "opaque", revision: 1 }], snapshot);if (input.kind !== "ready") throw Error("fixture not ready");
  const fingerprint = createHash("sha256").update(JSON.stringify(input.input)).digest("hex");
  await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text: {
    ...scrubBrief(model.reply), fingerprint, promptVersion: SESSION_BRIEF_PROMPT_VERSION,
    engineVersion: SESSION_BRIEF_ENGINE_VERSION, model: "gpt-5.6-luna", messageCount: 2,
    shortenedMessages: 0, inputTokens: 2, outputTokens: 2, durationMs: 1, generatedAt: new Date(),
  } });
  return { pg, db, model, canonicalInput };
}

test("normal canonical scheduler refreshes old ready input version once then stops, preserving legacy mode", async () => {
  const s = await setup();try {
    expect(await sweepUnbriefed(s.db, 100, { includeChanged: true, workspaceIds: [KEY.workspaceId] })).toBe(0);
    const tick = briefTick({ reader: {} as never, store: {} as never, transport: s.model, canonicalInput: s.canonicalInput, config: testConfig({ sweepSeconds: 1, throttleSeconds: 0, workspaceIds: [KEY.workspaceId] }) });
    expect(await tick(s.db, new Date())).toEqual({ generated: 1 });expect(s.model.requests).toHaveLength(1);
    expect((await s.pg.query<{ engine_version: string }>("SELECT engine_version FROM sessions.session_briefs")).rows[0]!.engine_version).toBe(SESSION_BRIEF_CANONICAL_ENGINE_VERSION);
    expect(await tick(s.db, new Date(Date.now() + 2000))).toEqual({});expect(s.model.requests).toHaveLength(1);
    expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true, includeChanged: true })).toBe(0);
  } finally { await s.pg.close(); }
}, 60000);

test("version refresh respects workspace scope, scheduler limit and existing leased wakeup", async () => {
  const s = await setup();try {
    expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true, workspaceIds: ["foreign"] })).toBe(0);
    expect(await sweepUnbriefed(s.db, 0, { refreshCanonicalInput: true })).toBe(0);
    expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true, workspaceIds: [KEY.workspaceId] })).toBe(1);
    expect(await claimWakeups(s.db, 1, 300, [KEY.workspaceId])).toHaveLength(1);
    expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true })).toBe(0);
    expect((await s.pg.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_brief_wakeups")).rows[0]!.n).toBe(1);
  } finally { await s.pg.close(); }
}, 60000);

test("version refresh follows the same throttle and admitted model cap", async () => {
  for (const capped of [false, true]) {
    const s = await setup();try {
      if (capped) { await armWakeup(s.db, KEY, { idleSeconds: 0, maxWaitSeconds: 0 });const w = (await claimWakeups(s.db, 1, 300))[0]!;expect(await admit(s.db, w, { sessionDailyCap: 1, budget: 20, globalDailyCap: 20 })).toEqual({ admitted: true });await s.pg.exec("DELETE FROM sessions.session_brief_wakeups"); }
      const tick = briefTick({ reader: {} as never, store: {} as never, transport: s.model, canonicalInput: s.canonicalInput, config: testConfig({ sweepSeconds: 1, throttleSeconds: capped ? 0 : 60, sessionDailyCap: 1, workspaceIds: [KEY.workspaceId] }) });
      expect(await tick(s.db, new Date())).toEqual(capped ? { rate_limited: 1 } : { throttled: 1 });expect(s.model.requests).toHaveLength(0);
    } finally { await s.pg.close(); }
  }
}, 60000);

test("canonical version refresh excludes non-native and failed output; current versions stay covered", async () => {
  const s = await setup();try {
    await s.pg.query("UPDATE sessions.session_briefs SET engine_version=$1", [SESSION_BRIEF_CANONICAL_ENGINE_VERSION]);expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true })).toBe(0);
    await s.pg.query("UPDATE sessions.session_briefs SET status='failed',engine_version=$1", [SESSION_BRIEF_ENGINE_VERSION]);expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true })).toBe(0);
    await s.pg.query("UPDATE sessions.session_briefs SET status='ready',origin='neon_backfill',engine_version='neon-backfill:test',legacy_job_id='synthetic-job'");expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true })).toBe(0);
    await s.pg.exec("UPDATE sessions.session_briefs SET origin='native'");expect(await sweepUnbriefed(s.db, 100, { refreshCanonicalInput: true })).toBe(0);
  } finally { await s.pg.close(); }
}, 60000);
