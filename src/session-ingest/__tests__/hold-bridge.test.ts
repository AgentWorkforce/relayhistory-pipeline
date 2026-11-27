/**
 * The admission hold's bridge side (src/session-ingest/hold-bridge.ts; docs/design/org-admission-hold.md section 6.1
 * and 6.2): the lifecycle adapter, the erasure set against the real schema (PGlite), the receipted replay, the
 * bridge_complete gate, waves and alarms. Each erase mapping is traced: operator_user_erasures -> the user's S3 fence
 * reset plus eraseLogPayloads; session_erasure_fences -> the session's S3 fence deletion for every owner.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import type { LedgerTenant, SelectionInput } from "../ledger.ts";
import {
  ERASURE_REREAD_LIMIT,
  ERASURE_SET_SQL,
  HOLD_ALARM_OWNER_VAR,
  holdAlarmOwner,
  HOLD_ATTEMPT_MAX_MS,
  HOLD_FUNCTIONS,
  HOLD_SEAL_MAX_MS,
  bridgeCompleteVerdict,
  erasureReceiptKey,
  holdAlarms,
  holdSql,
  parseErasures,
  parseHoldStatus,
  releaseAllowed,
  replayErasure,
  replayErasures,
  requestHash,
  waveOrgs,
  type Erasure,
  type Hold,
} from "../hold-bridge.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(300_000);
const ORG = "11111111-2222-4333-8444-555555555555", ORG2 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const E = "0f0e0d0c-0b0a-4908-8706-050403020100", E2 = "1f0e0d0c-0b0a-4908-8706-050403020100";
const SHA = "a".repeat(64), NOW = Date.parse("2026-11-27T06:00:00Z");
const code = async (fn: () => unknown) => { try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const codeSync = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const fresh = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };
const ROOT = "96bulls";

// ---- the lifecycle adapter ----

test("lifecycle adapter: the slot's function names and parameter shapes in one place; inputs validated", () => {
  expect(HOLD_FUNCTIONS).toEqual({ status: "sessions.ingest_org_admission_hold_status", install: "sessions.ingest_org_admission_hold_install",
    seal: "sessions.ingest_org_admission_hold_seal", markerAttempt: "sessions.ingest_org_admission_hold_marker_attempt", release: "sessions.ingest_org_admission_hold_release" });
  expect(holdSql.release(ORG, E, "bridge_complete", SHA, null, 3)).toEqual({
    text: "SELECT sessions.ingest_org_admission_hold_release($1::text, $2::uuid, $3::text, $4::text, $5::text, $6::bigint)", params: [ORG, E, "bridge_complete", SHA, null, "3"] });
  expect(holdSql.install(ORG, E).params).toEqual([ORG, E]);
  expect(holdSql.seal(ORG, E, SHA).text).toContain("ingest_org_admission_hold_seal($1::text, $2::uuid, $3::text)");
  expect(holdSql.markerAttempt(ORG, E, SHA).text).toContain("ingest_org_admission_hold_marker_attempt(");
  expect(holdSql.status(ORG)).toEqual({ text: "SELECT sessions.ingest_org_admission_hold_status($1::text) AS status", params: [ORG] });
  for (const bad of [() => holdSql.release("org-a", E, "bridge_complete", SHA, null, 0), () => holdSql.release(ORG, "x", "bridge_complete", SHA, null, 0),
    () => holdSql.release(ORG, E, "rollback" as never, SHA, null, 0), () => holdSql.release(ORG, E, "bridge_complete", "abc", null, 0),
    () => holdSql.release(ORG, E, "bridge_complete", SHA, "Bad Detail", 0), () => holdSql.release(ORG, E, "bridge_complete", SHA, null, -1),
    () => holdSql.install(ORG, "not-a-uuid"), () => holdSql.status("org-a")])
    expect(codeSync(bad)).toBe("hold_input");
});

const status = (hold: Record<string, unknown> | null, erasures = 0) => [{ status: { hold, erasures } }];
const iso = (ms: number) => new Date(ms).toISOString();

test("hold status: live hold shape-checked against the state machine; a released hold is no live hold", () => {
  expect(parseHoldStatus(status(null))).toEqual({ hold: null, erasures: 0 });
  expect(parseHoldStatus(status({ epoch: E, state: "released", installed_at: iso(NOW - 9e6) }, 4)).hold).toBeNull();
  const held = parseHoldStatus(status({ epoch: E, state: "held", installed_at: iso(NOW - 9e6), sealed_at: null, marker_attempted_at: null }, 2));
  expect(held).toEqual({ hold: { epoch: E, state: "held", installedMs: NOW - 9e6, sealedMs: null, attemptedMs: null, attemptEvent: false }, erasures: 2 });
  const att = parseHoldStatus([{ status: JSON.stringify({ hold: { epoch: E, state: "marker_attempted", installed_at: iso(NOW - 9e6), sealed_at: iso(NOW - 8e6),
    marker_attempted_at: iso(NOW - 7e6) }, erasures: 1 }) }]);
  expect(att.hold?.attemptEvent).toBe(true);
  for (const bad of [[], [{ status: null }], status({ epoch: "x", state: "held", installed_at: iso(NOW) }), status({ epoch: E, state: "frozen", installed_at: iso(NOW) }),
    status({ epoch: E, state: "held", installed_at: iso(NOW), sealed_at: iso(NOW) }), status({ epoch: E, state: "sealed", installed_at: iso(NOW) }),
    status({ epoch: E, state: "marker_attempted", installed_at: iso(NOW), sealed_at: iso(NOW) }), [{ status: { hold: null, erasures: -1 } }],
    status({ epoch: E, state: "held", installed_at: "yesterday" })])
    expect(codeSync(() => parseHoldStatus(bad as never))).toBe("hold_state_invalid");
});

const hold = (state: Hold["state"], over: Partial<Hold> = {}): Hold => ({ epoch: E, state, installedMs: NOW - 9e6,
  sealedMs: state === "held" ? null : NOW - 8e6, attemptedMs: state === "marker_attempted" ? NOW - 7e6 : null, attemptEvent: state === "marker_attempted", ...over });

test("release: after any marker attempt only bridge_complete; before, only verified_rollback (no rollback after an attempt)", () => {
  expect(releaseAllowed(hold("held"), "verified_rollback")).toBe(true);
  expect(releaseAllowed(hold("sealed"), "verified_rollback")).toBe(true);
  expect(releaseAllowed(hold("held"), "bridge_complete")).toBe(false);
  expect(releaseAllowed(hold("marker_attempted"), "bridge_complete")).toBe(true);
  expect(releaseAllowed(hold("marker_attempted"), "verified_rollback")).toBe(false);
  // An attempt recorded on a hold whatever its reported state still forbids a rollback.
  expect(releaseAllowed(hold("sealed", { attemptEvent: true }), "verified_rollback")).toBe(false);
});

// ---- the erasure set, against the real schema ----

async function schemaDb() {
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await applySupabaseSchema(pg, { until: null });
  return pg;
}
const at = (ms: number) => `to_timestamp(${ms} / 1000.0)`;

test("erasure set: operator_user_erasures and session_erasure_fences of the org since FULL minus the margin, oldest first (real schema)", async () => {
  const pg = await schemaDb();
  const since = NOW - 3600_000;
  await pg.exec(`
    INSERT INTO sessions.operator_user_erasures (request_id, org_id, workspace_id, user_id, state, origins, created_at, updated_at, metadata_purged_at, bytes_verified_at)
      VALUES ('req-old', '${ORG}', 'ws-1', 'user-old', 'purging', ARRAY['o1'], ${at(since - 1)}, ${at(since - 1)}, NULL, NULL),
             ('req-new', '${ORG}', 'ws-1', 'user-new', 'purging', ARRAY['o1'], ${at(since + 10)}, ${at(since + 10)}, NULL, NULL),
             ('req-done', '${ORG}', 'ws-3', 'user-done', 'bytes_verified', ARRAY[]::text[], ${at(since + 20)}, ${at(since + 20)}, ${at(since + 20)}, ${at(since + 20)}),
             ('req-other', '${ORG2}', 'ws-1', 'user-x', 'purging', ARRAY['o1'], ${at(since + 10)}, ${at(since + 10)}, NULL, NULL);
    INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id, created_at)
      VALUES ('${ORG}', 'ws-1', 'claude', 'sess-old', ${at(since - 5)}),
             ('${ORG}', 'ws-2', 'claude', 'sess-new', ${at(since + 5)}),
             ('${ORG2}', 'ws-2', 'claude', 'sess-x', ${at(since + 5)});
  `);
  const rows = (await pg.query<Record<string, unknown>>(ERASURE_SET_SQL, [ORG, String(since)])).rows;
  const set = parseErasures(rows);
  expect(set.map((e) => [e.kind, e.kind === "user" ? e.userId : e.sessionId])).toEqual([["session", "sess-new"], ["user", "user-new"], ["user", "user-done"]]);
  expect(set.filter((e) => e.kind === "user").map((e) => (e as { bytesVerified: boolean }).bytesVerified)).toEqual([false, true]);
  expect(set[0]).toEqual({ kind: "session", requestId: "session\nws-2\nclaude\nsess-new", workspaceId: "ws-2", source: "claude", sessionId: "sess-new", atMs: since + 5 });
  await pg.close();
});

test("erasure set rows are shape-checked", () => {
  for (const bad of [[{ kind: "user", request_id: "r", workspace_id: "w", user_id: null, at_ms: "1", bytes_verified: true }], [{ kind: "user", request_id: "r", workspace_id: "w", user_id: "u", at_ms: "1" }], [{ kind: "session", request_id: "r", workspace_id: "w", source: "c", session_id: "s", at_ms: "1" }],
    [{ kind: "other", request_id: "r", workspace_id: "w", at_ms: "1" }], [{ kind: "user", request_id: "r", workspace_id: "w", user_id: "u", at_ms: "x", bytes_verified: true }]])
    expect(codeSync(() => parseErasures(bad))).toBe("erasure_set_invalid");
});

// ---- the replay ----

const userErase: Erasure = { kind: "user", requestId: "req-1", workspaceId: "ws-1", userId: "user-1", atMs: NOW, bytesVerified: true };
const sessionErase: Erasure = { kind: "session", requestId: "session\nws-2\nclaude\nsess-1", workspaceId: "ws-2", source: "claude", sessionId: "sess-1", atMs: NOW };
function deps(store: ObjectStore, over: Record<string, unknown> = {}) {
  const calls: unknown[][] = [];
  return { calls, d: {
    store, keyRoot: ROOT, org: ORG, epoch: E, now: () => NOW,
    resetUploadedSessions: async (t: LedgerTenant, r: string) => { calls.push(["reset", t, r]); return 3; },
    eraseLogPayloads: async (t: LedgerTenant) => { calls.push(["logs", t]); return 2; },
    userGenerationBoundary: async () => true,
    setSessionUploads: async (i: SelectionInput) => { calls.push(["fence", i]); },
    resolveSessionTargets: async () => ({ complete: true, targets: [{ userId: "user-a", originId: "origin-a", source: "claude", sessionId: "sess-1" },
      { userId: "user-b", originId: "origin-b", source: "claude", sessionId: "sess-1" }] }),
    ...over,
  } };
}

test("replay, user erase -> the user's S3 fence reset (keyed by the request) and eraseLogPayloads; one create-only receipt", async () => {
  const store = fresh();
  const { d, calls } = deps(store);
  expect(await replayErasure(d, userErase)).toBe("replayed");
  const tenant = { orgId: ORG, workspaceId: "ws-1", userId: "user-1" };
  expect(calls).toEqual([["reset", tenant, "req-1"], ["logs", tenant]]);
  const receipt = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(erasureReceiptKey(ROOT, ORG, E, "req-1")))).body)));
  expect(receipt).toEqual({ v: 1, kind: "hold-bridge-erasure", epoch: E, request: requestHash("req-1"), erasure: "user", counts: { reset: 3, payloads: 2, owners: 0 }, appliedAtMs: NOW });
  expect(JSON.stringify(receipt)).not.toContain("user-1");
  // A retry: already receipted, nothing re-applied.
  expect(await replayErasure(d, userErase)).toBe("already");
  expect(calls.length).toBe(2);
  // The same request under another epoch is a separate replay.
  expect(await replayErasure({ ...d, epoch: E2 }, userErase)).toBe("replayed");
  expect(erasureReceiptKey(ROOT, ORG, E, "req-1")).toBe(`${ROOT}/.ledger/hold-bridge/${ORG}/${E}/erasures/${requestHash("req-1")}.json`);
  expect(erasureReceiptKey(ROOT, ORG, E, "req-1")).not.toContain("req-1");
});

test("replay, session erase -> the fence deletion for every target of a COMPLETE manifest; no manifest or partial coverage blocks", async () => {
  const store = fresh();
  const { d, calls } = deps(store);
  expect(await replayErasure(d, sessionErase)).toBe("replayed");
  expect(calls).toEqual([
    ["fence", { tenant: { orgId: ORG, workspaceId: "ws-2", userId: "user-a" }, originId: "origin-a", sessions: [["claude", "sess-1"]], enabled: false }],
    ["fence", { tenant: { orgId: ORG, workspaceId: "ws-2", userId: "user-b" }, originId: "origin-b", sessions: [["claude", "sess-1"]], enabled: false }],
  ]);
  // No manifest source at all (today's state): fail closed, nothing fenced, no receipt, bridge_complete refused.
  const none = deps(fresh(), { resolveSessionTargets: undefined });
  expect(await replayErasure(none.d, sessionErase)).toBe("session_manifest_missing");
  expect(none.calls).toEqual([]);
  expect(await bridgeCompleteVerdict(none.d.store, ROOT, ORG, hold("marker_attempted"), E, [sessionErase])).toEqual({ code: "erasure_unreplayed", erasures: 1, receipted: 0 });
  // Stubs that resolve some owners but cannot prove the set complete (a missing second owner): partial, blocked.
  for (const m of [{ complete: false, targets: [{ userId: "user-a", originId: "origin-a", source: "claude", sessionId: "sess-1" }] }, { complete: true, targets: [] }]) {
    const p = deps(fresh(), { resolveSessionTargets: async () => m });
    expect(await replayErasure(p.d, sessionErase)).toBe("session_coverage_partial");
    expect(p.calls).toEqual([]);
  }
  // A complete manifest's alias and turns-origin targets are each fenced, and nothing else.
  const alias = deps(fresh(), { resolveSessionTargets: async () => ({ complete: true, targets: [
    { userId: "user-a", originId: "origin-a", source: "claude", sessionId: "sess-1" },
    { userId: "user-a", originId: "origin-a", source: "claude", sessionId: "sess-1-alias" },
    { userId: "user-a", originId: "relayhistory:conversation_turns:v1", source: "claude", sessionId: "sess-1" }] }) });
  expect(await replayErasure(alias.d, sessionErase)).toBe("replayed");
  expect(alias.calls.map((c) => [(c[1] as SelectionInput).originId, (c[1] as SelectionInput).sessions])).toEqual([
    ["origin-a", [["claude", "sess-1"]]], ["origin-a", [["claude", "sess-1-alias"]]], ["relayhistory:conversation_turns:v1", [["claude", "sess-1"]]]]);
});

test("replay, user erase before the SQL erase reached bytes_verified: no effect, no receipt (the artifact lane's receipt is a prerequisite)", async () => {
  const { d, calls } = deps(fresh());
  expect(await replayErasure(d, { ...userErase, bytesVerified: false })).toBe("erasure_bytes_unverified");
  expect(calls).toEqual([]);
});

test("replay, user erase without a proven generation boundary (today: none exists): held even after bytes_verified", async () => {
  for (const over of [{ userGenerationBoundary: undefined }, { userGenerationBoundary: async () => false }, { userGenerationBoundary: async () => { throw new Error("x"); } }]) {
    const store = fresh();
    const { d, calls } = deps(store, over);
    const outcome = await replayErasure(d, userErase);
    expect(["erasure_generation_unproven", "replay_failed"]).toContain(outcome);
    expect(calls).toEqual([]);
    expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [userErase])).code).toBe("erasure_unreplayed");
  }
});

test("replay failures write no receipt (retried later); a corrupt receipt is never taken as done", async () => {
  for (const over of [{ resetUploadedSessions: async () => { throw new Error("s3 503 secret"); } }, { eraseLogPayloads: async () => { throw new Error("unverified"); } }]) {
    const store = fresh();
    const { d } = deps(store, over);
    expect(await replayErasure(d, userErase)).toBe("replay_failed");
    expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [userErase])).toMatchObject({ code: "erasure_unreplayed" });
    // The retry, once the fault clears, replays it.
    expect(await replayErasure(deps(store).d, userErase)).toBe("replayed");
  }
  const store = fresh();
  await store.put(objectKey(erasureReceiptKey(ROOT, ORG, E, "req-1")), new TextEncoder().encode("{\"v\":1}"), { contentType: "application/json" });
  expect(await replayErasure(deps(store).d, userErase)).toBe("replay_failed");
  // A concurrent replay that wrote the receipt first: `already`.
  const raced = fresh();
  const { d } = deps(raced);
  const realPut = raced.put.bind(raced);
  let first = true;
  raced.put = (async (...a: Parameters<ObjectStore["put"]>) => {
    if (first) { first = false; await realPut(a[0], a[1], { contentType: "application/json" }); throw new ObjectStoreError("precondition_failed", "s3", "exists", { status: 412 }); }
    return realPut(...a);
  }) as ObjectStore["put"];
  expect(await replayErasure(d, userErase)).toBe("already");
});

test("replay until settled: the erasure record count unchanged across a pass that found nothing new; the count is what release re-checks", async () => {
  const store = fresh();
  const { d } = deps(store);
  let set: Erasure[] = [userErase], count = 1, reads = 0;
  const readSet = async () => { reads++; if (reads === 2) { set = [userErase, sessionErase]; count = 2; } return set; };
  const r = await replayErasures(d, async () => count, readSet);
  expect(r).toMatchObject({ settled: true, erasureCount: 2, replayed: 2, already: 0 });
  // A set that keeps growing never settles.
  let n = 0;
  const growing = await replayErasures(deps(fresh()).d, async () => n++, async () => [{ ...userErase, requestId: `r${n}` }]);
  expect(growing).toMatchObject({ settled: false, rereads: ERASURE_REREAD_LIMIT, erasureCount: null });
});

// ---- the bridge_complete gate ----

test("bridge_complete: only with the hold marker_attempted at this epoch and every erasure of the re-read set receipted", async () => {
  const store = fresh();
  const { d } = deps(store);
  await replayErasure(d, userErase);
  expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [userErase])).toEqual({ code: "ok", erasures: 1, receipted: 1 });
  expect(await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [])).toEqual({ code: "ok", erasures: 0, receipted: 0 });
  // An erasure that arrived after the last replay (the re-read at release) refuses.
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [userErase, sessionErase])).code).toBe("erasure_unreplayed");
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, null, E, [])).code).toBe("hold_missing");
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted", { epoch: E2 }), E, [])).code).toBe("hold_epoch_mismatch");
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold("sealed"), E, [])).code).toBe("hold_not_attempted");
  // A receipt from another epoch never counts.
  await replayErasure({ ...d, epoch: E2 }, sessionErase);
  expect((await bridgeCompleteVerdict(store, ROOT, ORG, hold("marker_attempted"), E, [userErase, sessionErase])).code).toBe("erasure_unreplayed");
});

// ---- waves and alarms ----

test("waves: an org stuck in marker_attempted never joins a new wave; a missing state refuses", () => {
  const holds = new Map<string, Hold | null>([[ORG, hold("marker_attempted")], [ORG2, null]]);
  expect(waveOrgs([ORG, ORG2], holds)).toEqual({ wave: [ORG2], stuck: 1 });
  expect(waveOrgs([ORG], new Map([[ORG, hold("sealed", { attemptEvent: true })]]))).toEqual({ wave: [], stuck: 1 });
  expect(waveOrgs([ORG], new Map([[ORG, hold("held")]]))).toEqual({ wave: [ORG], stuck: 0 });
  expect(codeSync(() => waveOrgs([ORG, "33333333-2222-4333-8444-555555555555"], holds))).toBe("hold_state_missing");
});

const OWNER = "claude-cloud-cloud-b1";
test("alarm owner: configuration (BACKLOG_OPS_HOLD_ALARM_OWNER), required and shape-checked; the seal bound is 60 min", () => {
  expect(holdAlarmOwner({ [HOLD_ALARM_OWNER_VAR]: OWNER })).toBe(OWNER);
  for (const bad of [{}, { [HOLD_ALARM_OWNER_VAR]: "" }, { [HOLD_ALARM_OWNER_VAR]: "Has Spaces" }, { [HOLD_ALARM_OWNER_VAR]: "x".repeat(80) }])
    expect(codeSync(() => holdAlarmOwner(bad))).toBe("hold_alarm_owner");
  expect(HOLD_SEAL_MAX_MS).toBe(60 * 60_000);
  expect(HOLD_ATTEMPT_MAX_MS).toBe(24 * 3600_000);
  expect(codeSync(() => holdAlarms(new Map(), NOW, "bad owner"))).toBe("hold_alarm_owner");
});

test("alarms: a seal or an attempt held too long pages the named owner; hashes only", () => {
  const sealedLong = hold("sealed", { sealedMs: NOW - HOLD_SEAL_MAX_MS - 1 });
  const attemptedLong = hold("marker_attempted", { attemptedMs: NOW - HOLD_ATTEMPT_MAX_MS - 1 });
  const a = holdAlarms(new Map([[ORG, sealedLong], [ORG2, attemptedLong]]), NOW, OWNER);
  expect(a.map((x) => [x.alarm, x.owner])).toEqual([["hold_seal_overdue", OWNER], ["hold_attempt_overdue", OWNER]]);
  expect(JSON.stringify(a)).not.toContain(ORG);
  expect(a[0]!.org).toMatch(/^h_[0-9a-f]{32}$/u);
  expect(holdAlarms(new Map([[ORG, hold("sealed", { sealedMs: NOW - 60_000 })], [ORG2, null]]), NOW, OWNER)).toEqual([]);
  // 61 minutes sealed already alarms.
  expect(holdAlarms(new Map([[ORG, hold("sealed", { sealedMs: NOW - 61 * 60_000 })]]), NOW, OWNER).length).toBe(1);
});

void code;
