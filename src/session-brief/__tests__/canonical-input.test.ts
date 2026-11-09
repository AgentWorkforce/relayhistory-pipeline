import { expect, test } from "bun:test";
import { briefInputFromCanonical, canonicalBriefInputFromEnv } from "../canonical-input.ts";
import { briefSession } from "../worker.ts";
import { armWakeup, claimWakeups, sweepUnbriefed } from "../store.ts";
import { database, hoopDb, pointer, KEY, FakeModel, testConfig } from "./harness.ts";
import type { CanonicalRows } from "../../session-read-api/session-timing-source.ts";

const pointers = [{ machineId: "m-1", manifestKey: "opaque-pointer", revision: 1 }];
const scope = { ...KEY, harness: "codex" };
const snapshot = (): CanonicalRows & { exists: boolean; generation: string } => ({ exists: true, generation: "a".repeat(64), legacyCoverage: null, rows: [
  { table: "convergence_events", row: { session_id: scope.sessionId, source: "codex", event_id: "original-user", machine_id: "m-1", ts: "2026-11-07T01:00:00Z", kind: "history", type: "user", content: "Investigate the queue retry race." } },
  { table: "convergence_events", row: { session_id: scope.sessionId, source: "codex", event_id: "original-assistant", machine_id: "m-1", ts: "2026-11-07T01:00:01Z", kind: "history", actor_role: "assistant", content: "The retry completed after awaiting its lease." } },
  { table: "convergence_events", row: { session_id: scope.sessionId, source: "codex", event_id: "tool-output", machine_id: "m-1", ts: "2026-11-07T01:00:02Z", kind: "tool_result", content: "TOOL-NOT-MODEL-INPUT" } },
] });

test("native brief input uses full old canonical user/assistant records and existing model budget contract", () => {
  const input = briefInputFromCanonical(scope, pointers, snapshot()); expect(input.kind).toBe("ready");
  if (input.kind !== "ready") throw Error("not ready");
  expect(input.sourceRevisions).toEqual({ "m-1": 1 });
  expect(input.input.messages.map(m => m.role)).toEqual(["user", "assistant"]);
  expect(JSON.stringify(input.input)).not.toContain("TOOL-NOT-MODEL-INPUT");
  expect(input.input.messages[0]!.text).toBe("Investigate the queue retry race.");
});

test("missing, foreign and malformed canonical input cannot reach the model", () => {
  expect(() => briefInputFromCanonical(scope, pointers, { ...snapshot(), exists: false })).toThrow("canonical input is unavailable");
  expect(() => briefInputFromCanonical(scope, pointers, { ...snapshot(), generation: "not-a-hash" })).toThrow("canonical input is unavailable");
  for (const changed of [{ source: "claude" }, { session_id: "other" }, { workspace_id: "other" }, { org_id: "other" }]) {
    const s = snapshot();s.rows[0]!.row = { ...s.rows[0]!.row, ...changed };
    expect(() => briefInputFromCanonical(scope, pointers, s)).toThrow("canonical input scope refused");
  }
});

test("canonical source selection is explicit, strict and refuses incomplete configuration", () => {
  expect(canonicalBriefInputFromEnv({})).toBeUndefined();
  expect(canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "false" })).toBeUndefined();
  expect(() => canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "yes" })).toThrow("must be true or false");
  expect(() => canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "true" })).toThrow("requires");
  expect(canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "true", SESSION_BRIEF_ENABLED: "true", LOCKER_BUCKET: "owned-bucket", PIPELINE_ORG_FROM_PREFIX: "true" })).toBeFunction();
});

test("native changed-revision sweep re-arms the existing leased worker without touching consumer privileges", async () => {
  const pg = await database(); const db = hoopDb(pg);
  try {
    await pointer(pg, { machine: "m-1", revision: 1, manifestKey: "opaque-pointer" });
    // fixture pointer is KEY claude; keep native snapshot scoped to that owner.
    const ready = snapshot();ready.rows.forEach(r => { r.row.source = KEY.harness; });
    await armWakeup(db, KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
    const wakeup = (await claimWakeups(db, 1, 300))[0]!; const model = new FakeModel();
    const result = await briefSession({ db, reader: { read: async () => { throw Error("legacy reader must not run"); } } as never, store: {} as never, transport: model, config: testConfig({ throttleSeconds: 0 }), canonicalInput: async (_db, key, actual) => briefInputFromCanonical(key, actual, ready) }, wakeup);
    expect(result).toBe("generated"); expect(model.requests).toHaveLength(1);
    const saved = (await pg.query<{ status: string; description: string; model: string }>("SELECT status,description,model FROM sessions.session_briefs")).rows[0]!;
    expect(saved).toMatchObject({ status: "ready", description: model.reply.description, model: "gpt-5.6-luna" });
    expect(await sweepUnbriefed(db, 100, { includeChanged: true })).toBe(0);
    await pg.exec("UPDATE sessions.session_index SET revision=2");
    expect(await sweepUnbriefed(db, 100)).toBe(0); // legacy unchanged
    expect(await sweepUnbriefed(db, 100, { includeChanged: true })).toBe(1);
    expect(await sweepUnbriefed(db, 100, { includeChanged: true })).toBe(0); // existing wakeup preserved
  } finally { await pg.close(); }
}, 60000);
