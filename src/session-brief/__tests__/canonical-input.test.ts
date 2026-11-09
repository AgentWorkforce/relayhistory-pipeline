import { expect, spyOn, test } from "bun:test";
import { briefInputFromCanonical, canonicalBriefInputFromEnv, canonicalBriefScope, canonicalInputRefusal } from "../canonical-input.ts";
import { briefSession } from "../worker.ts";
import { armWakeup, claimWakeups, sweepUnbriefed, recordBrief } from "../store.ts";
import { database, hoopDb, pointer, KEY, FakeModel, testConfig } from "./harness.ts";
import { poolTenantRunner } from "../../session-read-api/liveness.ts";
import { sqlCanonicalPointers } from "../../session-read-api/session-timing-source.ts";
import { CanonicalReadError, canonicalSnapshot } from "../../session-read-api/canonical.ts";
import { PipelineError } from "../../shared/vendor.ts";
import { objectKey, manifestKey } from "../../../packages/object-store/src/index.ts";
import { zstdDecompressSync } from "node:zlib";
import { recordStream, tempStore, loc } from "./harness.ts";
import { briefConfig } from "../config.ts";
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

test("a canonical refusal logs its reason and projection reason codes (codes only), quarantine unchanged", () => {
  const lines: string[] = [];
  const spy = spyOn(process.stderr, "write").mockImplementation(((chunk: string) => { lines.push(String(chunk)); return true; }) as never);
  try {
    let thrown: unknown;
    try { canonicalInputRefusal(new CanonicalReadError("canonical_projection_incomplete", 503, ["scope_too_large", "sess:1/manifest.json"]), pointers); } catch (e) { thrown = e; }
    expect(thrown).toBeInstanceOf(PipelineError);
    expect(thrown).toMatchObject({ code: "canonical_input_refused", message: "canonical input refused", retryable: true });
    expect(() => canonicalInputRefusal(new CanonicalReadError("forbidden", 403), pointers)).toThrow("canonical input refused");
    const logged = lines.map(l => JSON.parse(l) as Record<string, unknown>).filter(l => l.error_code === "canonical_input_refused");
    expect(logged).toHaveLength(2);
    expect(logged[0]).toMatchObject({ level: "warn", stage: "session-brief", reason: "canonical_projection_incomplete", reason_codes: "scope_too_large", retryable: true });
    expect(JSON.stringify(logged[0])).not.toContain("manifest.json");
    expect(logged[1]).toMatchObject({ reason: "forbidden", retryable: false });
    expect(logged[1]).not.toHaveProperty("reason_codes");
    let jsonlThrown: unknown;
    try { canonicalInputRefusal(new CanonicalReadError("transcript_json_invalid", 501, ["bad/key"]), pointers, "canonical_jsonl_input_refused"); } catch (e) { jsonlThrown = e; }
    expect(jsonlThrown).toMatchObject({ code: "canonical_jsonl_input_refused", retryable: false });
    expect(JSON.parse(lines.at(-1)!)).toMatchObject({ error_code: "canonical_jsonl_input_refused", reason: "transcript_json_invalid", retryable: false });
    expect(lines.at(-1)).not.toContain("bad/key");
    lines.length = 0;
    expect(canonicalInputRefusal(new CanonicalReadError("canonical_projection_incomplete", 503, ["manifest_unscrubbed"]), pointers))
      .toEqual({ kind: "quarantined", error: "session_quarantined", sourceRevisions: { "m-1": 1 } });
    expect(lines).toHaveLength(0);
  } finally { spy.mockRestore(); }
});

test("canonical source selection is explicit, strict and refuses incomplete configuration", () => {
  expect(canonicalBriefInputFromEnv({})).toBeUndefined();
  expect(canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "false" })).toBeUndefined();
  expect(() => canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "yes" })).toThrow("must be true or false");
  expect(() => canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "true" })).toThrow("requires");
  expect(canonicalBriefInputFromEnv({ SESSION_BRIEF_CANONICAL_INPUT: "true", SESSION_BRIEF_ENABLED: "true", LOCKER_BUCKET: "owned-bucket", PIPELINE_ORG_FROM_PREFIX: "true", SESSION_BRIEF_WORKSPACE_IDS: "ws-a" })).toBeFunction();
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

test("actual SQL physical→logical mapping reads verified alias objects, rejects foreign/multiple sources before object IO", async () => {
  const pg = await database(); const db = hoopDb(pg); const store = tempStore();
  const physical = { ...KEY, sessionId: "sess_1" }, logical = { ...physical, sessionId: "sess:1" };
  try {
    const records = ["user", "assistant"].map((role, n) => ({ kind: "session_event", origin_id: "original", record_id: `r${n}`, revision: "1", operation: "upsert", received_at: "2026-10-30T12:00:00Z", payload: { ts_ms: Date.parse(`2026-10-29T10:00:0${n}Z`), role, text: `${role} canonical text` } }));
    const manifest = await recordStream(store, records, { key: physical });
    manifest.extensions!["96bulls"] = { user_id: "u-1", canonical_session_id: logical.sessionId };
    (manifest.extensions!["client.relayhistory_export"] as any).source_user_id = "u-1";
    const path = manifestKey(loc(physical)); await store.put(objectKey(path), new TextEncoder().encode(JSON.stringify(manifest)));
    await pointer(pg, { key: physical, manifestKey: path });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1,raw_format='ndjson',storage_provider='s3',content_sha256=decode($2,'hex') WHERE session_id=$3", [logical.sessionId, manifest.raw.content_sha256, physical.sessionId]);
    const run = poolTenantRunner(db.pool as never);
    expect(await canonicalBriefScope(run, physical)).toEqual(logical);
    await expect(canonicalBriefScope(run, { ...physical, workspaceId: "other" })).rejects.toThrow("canonical source unavailable");
    let objectReads = 0; const counted = new Proxy(store, { get(target, prop) {
      const value = Reflect.get(target, prop);
      if (prop === "getObject") return async (...args: Parameters<typeof store.getObject>) => { objectReads++; return target.getObject(...args); };
      return typeof value === "function" ? value.bind(target) : value;
    } });
    const snapshot = await canonicalSnapshot({ pointers: sqlCanonicalPointers(run).pointers, store: counted, provider: "s3", bucket: "bucket", prefix: {}, decompress: b => new Uint8Array(zstdDecompressSync(b)) }, { orgId: logical.orgId, workspaceId: logical.workspaceId, source: logical.harness, sessionId: logical.sessionId });
    const built = briefInputFromCanonical(logical, pointers, snapshot); expect(built.kind).toBe("ready");
    if (built.kind === "ready") { expect(built.input.sessionId).toBe(logical.sessionId); expect(built.input.messages.map(m => m.text)).toEqual(["user canonical text", "assistant canonical text"]); }
    expect(objectReads).toBeGreaterThan(0);
    const before = objectReads;
    await pointer(pg, { key: physical, machine: "m-2", manifestKey: "not-read" });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id='other-logical' WHERE machine_id='m-2'");
    await expect(canonicalBriefScope(run, physical)).rejects.toThrow("canonical source unavailable");
    expect(objectReads).toBe(before);
    await pg.exec("DELETE FROM sessions.session_index WHERE machine_id='m-2'");
    await pointer(pg, { key: { ...physical, sessionId: "another-physical" }, manifestKey: "not-read" });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1 WHERE session_id='another-physical'", [logical.sessionId]);
    await expect(canonicalBriefScope(run, physical)).rejects.toThrow("multiple physical sources are not qualified");
    expect(objectReads).toBe(before);
  } finally { await pg.close(); }
}, 60000);

test("workspace scope filters sweep and claim before any lease or model call", async () => {
  const pg = await database(); const db = hoopDb(pg);
  try {
    await pointer(pg);await pointer(pg, { key: { ...KEY, workspaceId: "other" } });
    expect(await sweepUnbriefed(db, 100, { includeChanged: true, workspaceIds: [KEY.workspaceId] })).toBe(1);
    await armWakeup(db, { ...KEY, workspaceId: "other" }, { idleSeconds: 0, maxWaitSeconds: 0 });
    const claimed = await claimWakeups(db, 10, 300, [KEY.workspaceId]);expect(claimed).toHaveLength(1);expect(claimed[0]!.workspaceId).toBe(KEY.workspaceId);
    expect((await pg.query<{ lease_id: string | null }>("SELECT lease_id::text FROM sessions.session_brief_wakeups WHERE workspace_id='other'")).rows[0]!.lease_id).toBeNull();
    expect(() => briefConfig({ SESSION_BRIEF_WORKSPACE_IDS: "*" })).toThrow("explicit workspace list");
  } finally { await pg.close(); }
}, 60000);

test("native publication rejects a second physical source introduced during model execution", async () => {
  const pg = await database(); const db = hoopDb(pg);
  try {
    await pointer(pg, { manifestKey: "opaque-pointer" });
    await armWakeup(db, KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
    const wakeup = (await claimWakeups(db, 1, 300))[0]!;
    const ready = snapshot();ready.rows.forEach(r => { r.row.source = KEY.harness; });
    const model = new FakeModel(); const original = model.complete.bind(model);
    model.complete = async request => {
      await pointer(pg, { key: { ...KEY, sessionId: "new-physical" }, manifestKey: "never-read" });
      await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1 WHERE session_id='new-physical'", [KEY.sessionId]);
      return original(request);
    };
    const result = await briefSession({ db, reader: {} as never, store: {} as never, transport: model, config: testConfig({ throttleSeconds: 0 }), canonicalInput: async (_db, key, actual) => ({ ...briefInputFromCanonical(key, actual, ready), canonicalSessionId: key.sessionId }) }, wakeup);
    expect(result).toBe("stale");expect(model.requests).toHaveLength(1);
    expect((await pg.query("SELECT 1 FROM sessions.session_briefs")).rows).toEqual([]);
    expect((await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_directory WHERE short_summary IS NOT NULL")).rows[0]!.n).toBe(0);
  } finally { await pg.close(); }
}, 60000);

test("native publication rejects an unchanged-revision machine remap during model execution", async () => {
  const pg = await database(); const db = hoopDb(pg);
  try {
    await pointer(pg, { manifestKey: "opaque-pointer" });await pointer(pg, { machine: "m-2", manifestKey: "opaque-second" });
    await armWakeup(db, KEY, { idleSeconds: 0, maxWaitSeconds: 0 });
    const wakeup = (await claimWakeups(db, 1, 300))[0]!;
    const ready = snapshot();ready.rows.forEach(r => { r.row.source = KEY.harness; });
    const model = new FakeModel(); const original = model.complete.bind(model);
    model.complete = async request => { await pg.exec("UPDATE sessions.session_index SET canonical_session_id='other-logical' WHERE machine_id='m-2'");return original(request); };
    const result = await briefSession({ db, reader: {} as never, store: {} as never, transport: model, config: testConfig({ throttleSeconds: 0 }), canonicalInput: async (_db, key, actual) => ({ ...briefInputFromCanonical(key, actual, ready), canonicalSessionId: key.sessionId }) }, wakeup);
    expect(result).toBe("stale");expect(model.requests).toHaveLength(1);
    expect((await pg.query("SELECT 1 FROM sessions.session_briefs")).rows).toEqual([]);
  } finally { await pg.close(); }
}, 60000);
