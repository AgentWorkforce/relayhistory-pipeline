/**
 * The S3 writer fence of a per-org cutover through a committed bridge_complete (org-bridge-gate.ts,
 * docs/design/org-bridge-gate.md).
 * - The direct writer is the S3 consumer: ledger-s3 claim and commit with `orgBridge: "required"`.
 * - Once the org is enrolled (before install), it admits only beside its per-org marker AND the bridge-complete record
 *   of its enrollment generation. That holds with a valid deployment-wide marker and with a warm positive cache.
 * - completeOrgBridge writes that record only after the SQL release of the enrolled request is committed and the
 *   marker has landed, in either commit order. It survives a crash between the two.
 * - Recovery: install failed; verified rollback; a later generation meeting old bytes.
 * The drain's side (the fallback writer) is in buffered-drain.test.ts. Synthetic ids only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { acquireDrainLock, drainLockKey, exportLockKey } from "../buffered-drain.ts";
import type { ClaimInput, CommitInput, LedgerRecord } from "../ledger.ts";
import {
  fenceAuthorityKey, fenceAuthorityStopKey, orgBridgeEpochHash, orgBridgeKey, orgEnrollmentKey, orgEnrollmentRequestHash, orgFenceAuthorityKey, s3Ledger, s3LedgerKeys, s3LedgerObjects, validOrgBridge,
} from "../ledger-s3.ts";
import { completeOrgBridge, enrollmentFor, enrollOrg, OrgBridgeRefusal, withdrawEnrollment, type BridgeSql } from "../org-bridge-gate.ts";
import { liveMachineId } from "../writer.ts";
import { TENANT, scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORIGIN = "origin-1";
const ORG = TENANT.orgId;
const EPOCH = "00000000-0000-4000-8000-000000000001";
const REQ = "00000000-0000-4000-8000-0000000000a1";
const REQ2 = "00000000-0000-4000-8000-0000000000a2";
const EVIDENCE = "e".repeat(64);
const hex = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
const enc = new TextEncoder();
const dec = new TextDecoder();
const FIELDS = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 };
const MARKER = enc.encode(JSON.stringify({ ...FIELDS, orgId: ORG }));
const OTHER = { ...TENANT, orgId: "org-other" };

const cleanups: (() => void)[] = [];
afterEach(() => { cleanups.splice(0).forEach((c) => c()); });
function fresh(): ObjectStore {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}
const putMarker = (store: ObjectStore, bytes = MARKER) => store.put(objectKey(orgFenceAuthorityKey(ROOT, ORG)), bytes, { contentType: "application/json" });
const putJson = (store: ObjectStore, key: string, v: unknown) => store.put(objectKey(key), enc.encode(typeof v === "string" ? v : JSON.stringify(v)), { contentType: "application/json" });
const readJson = async (store: ObjectStore, key: string) => JSON.parse(dec.decode(await readAll(await store.get(objectKey(key)))));
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const enrollment = (o: Record<string, unknown> = {}) => ({ v: 1, kind: "org-cutover-enrollment", orgId: ORG, generation: 1, requestSha256: orgEnrollmentRequestHash(REQ),
  state: "enrolled", withdrawn: null, writtenAtMs: 3, ...o });
const record = (o: Record<string, unknown> = {}) => ({ v: 1, kind: "org-bridge-complete", orgId: ORG, generation: 1, requestSha256: orgEnrollmentRequestHash(REQ),
  holdEpochSha256: orgBridgeEpochHash(EPOCH), markerSha256: hex(MARKER), releaseEvidenceSha256: EVIDENCE, writtenAtMs: 5, ...o });
const putEnrollment = (store: ObjectStore, o: Record<string, unknown> = {}) => putJson(store, orgEnrollmentKey(ROOT, ORG), enrollment(o));
const putRecord = (store: ObjectStore, o: Record<string, unknown> = {}, gen = 1) => putJson(store, orgBridgeKey(ROOT, ORG, gen), record(o));

function rec(recordId: string, revision: number): LedgerRecord {
  return { source: "claude", sessionId: "sess-1", recordId, revision, revisionId: `${recordId}@${revision}`, digest: hex(`${recordId}:${revision}`) };
}
function claimInput(batchId: string, tenant = TENANT): ClaimInput {
  return { tenant, originId: ORIGIN, batchId, batchDigest: hex(batchId), receiptId: `rhr_${hex(`receipt:${batchId}`)}`, sessions: [["claude", "sess-1"]], records: [rec("r1", 1)] } as ClaimInput;
}
const commitOf = (c: ClaimInput, claimToken: string): CommitInput => ({ tenant: c.tenant, originId: c.originId, batchId: c.batchId, batchDigest: c.batchDigest, claimToken,
  sessions: c.sessions, records: c.records, machineId: liveMachineId(c.tenant.workspaceId, c.originId, c.tenant.userId) });
const consumer = (store: ObjectStore, now: () => number = () => 0) =>
  s3Ledger(store, ROOT, { fenceAuthority: "required", orgBridge: "required", nowMs: now, sleep: async () => {} });
const code = (e: unknown) => String((e as { code?: unknown }).code);
/** The direct writer's verdict for one new batch: "claimed" or the refusal code. */
async function direct(store: ObjectStore, tenant = TENANT, ledger = consumer(store)): Promise<string> {
  try { return (await ledger.claim(claimInput(`b-${randomUUID()}`, tenant))).kind; } catch (e) { return code(e); }
}

/** Slot 062320's (062340's) status function, as the gate reads it: the latest hold of the org. */
function sqlWith(hold: Record<string, unknown> | null | (() => Record<string, unknown> | null)): BridgeSql {
  return {
    async query(text: string, params: unknown[]) {
      expect(text).toContain("sessions.ingest_org_admission_hold_status");
      expect(params).toEqual([ORG]);
      return { rows: [{ status: { hold: typeof hold === "function" ? hold() : hold, erasures: 0 } }] };
    },
  };
}
const held = (state: string, o: Record<string, unknown> = {}) => ({ epoch: EPOCH, request_id: REQ, held_org: ORG, state, release_reason: null, release_evidence_sha256: null, ...o });
const released = (o: Record<string, unknown> = {}) => held("released", { release_reason: "bridge_complete", release_evidence_sha256: EVIDENCE, ...o });
const complete = (store: ObjectStore, sql: BridgeSql, o: Partial<Parameters<typeof completeOrgBridge>[0]> = {}) =>
  completeOrgBridge({ sql, store, keyRoot: ROOT, orgId: ORG, epoch: EPOCH, holder: "bridge-test", now: () => 10, ...o });
const enroll = (store: ObjectStore, sql: BridgeSql, requestId = REQ) => enrollOrg({ sql, store, keyRoot: ROOT, orgId: ORG, requestId, now: () => 7 });
const refusal = async (p: Promise<unknown>) => { try { await p; return "ok"; } catch (e) { return e instanceof OrgBridgeRefusal ? e.code : `other:${String(e)}`; } };
/** A store whose `op` throws for `key` (a crash or a lost response), around the real one. */
function failing(base: ObjectStore, op: "put" | "getObject", key: string, landed = false): ObjectStore {
  return new Proxy(base, {
    get(target, prop) {
      if (prop === op) return async (...a: unknown[]) => {
        if (String(a[0]) !== key) return (target[op] as (...x: unknown[]) => unknown).apply(target, a);
        if (op === "put" && landed) { await (target.put as (...x: unknown[]) => unknown).apply(target, a); throw new Error("response lost"); }
        throw op === "getObject" ? new ObjectStoreError("unavailable", "down") : new Error("killed");
      };
      const v = (target as unknown as Record<PropertyKey, unknown>)[prop];
      return typeof v === "function" ? (v as (...x: unknown[]) => unknown).bind(target) : v;
    },
  });
}

describe("ledger-s3 orgBridge: an enrolled org is fenced from enrollment until its bridge-complete record", () => {
  test("enrolled, no marker yet: refused even with a valid deployment-wide marker and a warm positive cache; an unrelated org is unaffected", async () => {
    const store = fresh();
    let now = 0;
    await putJson(store, fenceAuthorityKey(ROOT), FIELDS);
    const ledger = consumer(store, () => now);
    // Warm: the deployment-wide marker admits the org (and the control org) and is cached for 60 s.
    expect(await direct(store, TENANT, ledger)).toBe("claimed");
    expect(await direct(store, OTHER, ledger)).toBe("claimed");
    await putEnrollment(store);
    now = 1; // well inside the warm window
    expect(await direct(store, TENANT, ledger)).toBe("ledger_fence_authority_bridge_pending");
    expect(await direct(store, OTHER, ledger)).toBe("claimed");
    // The marker alone does not open it; nor a later cache window.
    await putMarker(store);
    for (const t of [30_000, 61_000, 3_600_000]) { now = t; expect(await direct(store, TENANT, ledger)).toBe("ledger_fence_authority_bridge_pending"); }
    await putRecord(store);
    expect(await direct(store, TENANT, ledger)).toBe("claimed");
    expect(await direct(store, OTHER, ledger)).toBe("claimed");
  });

  test("the STOP keeps precedence for an enrolled and bridged org, and for the control org", async () => {
    const store = fresh();
    await putMarker(store);
    await putEnrollment(store);
    await putRecord(store);
    await putJson(store, fenceAuthorityKey(ROOT), FIELDS);
    expect(await direct(store)).toBe("claimed");
    await putJson(store, fenceAuthorityStopKey(ROOT), { v: 1, stop: "all", writtenAtMs: 1 });
    expect(await direct(store)).toBe("ledger_fence_authority_stopped");
    expect(await direct(store, OTHER)).toBe("ledger_fence_authority_stopped");
  });

  test("records and enrollments that do not bind refuse (bridge_invalid / enrollment_invalid), never admit", async () => {
    const bad: [Record<string, unknown> | string, string][] = [
      ["not json", "ledger_fence_authority_bridge_invalid"],
      [{ ...record(), extra: 1 }, "ledger_fence_authority_bridge_invalid"],
      [record({ orgId: "org-other" }), "ledger_fence_authority_bridge_invalid"],
      [record({ markerSha256: "0".repeat(64) }), "ledger_fence_authority_bridge_invalid"],
      [record({ requestSha256: orgEnrollmentRequestHash(REQ2) }), "ledger_fence_authority_bridge_invalid"],
      [record({ generation: 2 }), "ledger_fence_authority_bridge_invalid"],
    ];
    for (const [doc, want] of bad) {
      const store = fresh();
      await putMarker(store);
      await putEnrollment(store);
      await putJson(store, orgBridgeKey(ROOT, ORG, 1), doc);
      expect(await direct(store)).toBe(want);
    }
    for (const doc of ["not json", enrollment({ orgId: "org-other" }), enrollment({ state: "withdrawn" }), enrollment({ generation: 0 })]) {
      const store = fresh();
      await putJson(store, fenceAuthorityKey(ROOT), FIELDS);
      await putJson(store, orgEnrollmentKey(ROOT, ORG), doc);
      expect(await direct(store)).toBe("ledger_fence_authority_enrollment_invalid");
    }
    // The marker replaced by other bytes after the record: the record no longer binds it.
    const store = fresh();
    await putEnrollment(store);
    await putRecord(store);
    await putMarker(store, enc.encode(JSON.stringify({ ...FIELDS, orgId: ORG, writtenAtMs: 2 })));
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_invalid");
  });

  test("a claim acquired before install whose commit resumes while held stays pending, then commits after the record", async () => {
    const store = fresh();
    await putJson(store, fenceAuthorityKey(ROOT), FIELDS);
    const ledger = consumer(store);
    const c = claimInput("pre-install");
    const claimed = await ledger.claim(c);
    expect(claimed.kind).toBe("claimed");
    const token = (claimed as { claimToken: string }).claimToken;
    // Enrollment (and the hold) land; the paused writer resumes its commit.
    await putEnrollment(store);
    await expect(ledger.commit(commitOf(c, token))).rejects.toMatchObject({ code: "ledger_fence_authority_bridge_pending" });
    // Nothing of the commit's: no index entry for r1 and the batch still pending, so a retry of the same token works.
    const keys = s3LedgerKeys(ROOT, TENANT);
    expect((await readJson(store, keys.batch(ORIGIN, c.batchId))).state).toBe("pending");
    expect(await s3LedgerObjects(store, ROOT).readIndex(keys.revisions(ORIGIN, "r1"))).toBeFalsy();
    await putMarker(store);
    await putRecord(store);
    expect(await ledger.commit(commitOf(c, token))).toEqual({ rearmed: 0 });
    expect((await readJson(store, keys.batch(ORIGIN, c.batchId))).state).toBe("committed");
  });

  test("unset (self-host): enrollment is never read and a per-org marker alone admits; orgBridge without fenceAuthority refuses construction", async () => {
    const store = fresh();
    await putMarker(store);
    await putEnrollment(store);
    expect((await s3Ledger(store, ROOT, { fenceAuthority: "required", sleep: async () => {} }).claim(claimInput("s1"))).kind).toBe("claimed");
    expect(() => s3Ledger(store, ROOT, { orgBridge: "required" })).toThrow();
    expect(() => s3Ledger(store, ROOT, { fenceAuthority: "required", orgBridge: "on" as never })).toThrow();
  });

  test("not enrolled, or withdrawn: the markers decide exactly as before", async () => {
    const store = fresh();
    await putMarker(store);
    expect(await direct(store)).toBe("claimed");
    await putEnrollment(store, { state: "withdrawn", withdrawn: "install_failed" });
    expect(await direct(store)).toBe("claimed");
  });
});

describe("enrollOrg / enrollmentFor / withdrawEnrollment: bound to the install request, before install", () => {
  test("enrolls generation 1 before install, idempotently; the install tool's check binds the request", async () => {
    const store = fresh();
    expect(await enroll(store, sqlWith(null))).toEqual({ outcome: "enrolled", generation: 1 });
    expect(await enroll(store, sqlWith(null))).toEqual({ outcome: "adopted", generation: 1 });
    expect(await readJson(store, orgEnrollmentKey(ROOT, ORG))).toEqual(enrollment({ writtenAtMs: 7 }));
    expect(JSON.stringify(await readJson(store, orgEnrollmentKey(ROOT, ORG)))).not.toContain(REQ);
    expect(await enrollmentFor(store, ROOT, ORG, REQ)).toEqual({ generation: 1 });
    expect(await refusal(enrollmentFor(store, ROOT, ORG, REQ2))).toBe("bridge_request_mismatch");
    expect(await refusal(enrollmentFor(fresh(), ROOT, ORG, REQ))).toBe("bridge_not_enrolled");
    // Another request while this one is live and unbridged: refused.
    expect(await refusal(enroll(store, sqlWith(held("held")), REQ2))).toBe("enrollment_hold_present");
    expect(await refusal(enroll(store, sqlWith(null), REQ2))).toBe("enrollment_live");
  });

  test("an install that ran before its enrollment is refused (enrollment_after_install): fenced writers were never retired by install", async () => {
    const store = fresh();
    expect(await refusal(enroll(store, sqlWith(held("held"))))).toBe("enrollment_after_install");
    expect(await refusal(enroll(store, sqlWith(released({ release_reason: "verified_rollback" }))))).toBe("enrollment_after_install");
    expect(await exists(store, orgEnrollmentKey(ROOT, ORG))).toBe(false);
  });

  // install_failed now consumes the request in SQL (_install + verified_rollback) before its S3 CAS: proven against the
  // real 062320 functions in cutover-withdrawal.pglite.test.ts. Here, only what needs no SQL write:
  test("install_failed refuses while another request's hold is live, and when consuming the request fails", async () => {
    const store = fresh();
    await enroll(store, sqlWith(null));
    expect(await refusal(withdrawEnrollment({ sql: sqlWith(held("held", { request_id: REQ2 })), store, keyRoot: ROOT, orgId: ORG, requestId: REQ, reason: "install_failed" }))).toBe("enrollment_hold_present");
    expect(await refusal(withdrawEnrollment({ sql: sqlWith(null), store, keyRoot: ROOT, orgId: ORG, requestId: REQ, reason: "install_failed" }))).toBe("enrollment_consume_failed");
    expect((await readJson(store, orgEnrollmentKey(ROOT, ORG))).state).toBe("enrolled"); // nothing withdrawn: fenced
  });

  test("verified rollback: withdraw only when the latest hold is that request's, released with verified_rollback", async () => {
    const store = fresh();
    await enroll(store, sqlWith(null));
    const w = (sql: BridgeSql) => withdrawEnrollment({ sql, store, keyRoot: ROOT, orgId: ORG, requestId: REQ, reason: "verified_rollback" });
    for (const h of [null, held("held"), held("sealed"), released(), released({ request_id: REQ2, release_reason: "verified_rollback" })])
      expect(await refusal(w(sqlWith(h)))).toBe("enrollment_rollback_unverified");
    expect(await w(sqlWith(held("released", { release_reason: "verified_rollback", release_evidence_sha256: EVIDENCE })))).toEqual({ outcome: "withdrawn" });
    expect((await readJson(store, orgEnrollmentKey(ROOT, ORG))).withdrawn).toBe("verified_rollback");
  });

  test("a later cutover of the same org: generation 2, and generation 1's bridge bytes never satisfy it", async () => {
    const store = fresh();
    await putMarker(store);
    await enroll(store, sqlWith(null));
    expect((await complete(store, sqlWith(released()))).outcome).toBe("written");
    expect(await direct(store)).toBe("claimed");
    // Generation 1 is bridged, so a new request may enroll (after its own install is still to come).
    expect(await enroll(store, sqlWith(released()), REQ2)).toEqual({ outcome: "enrolled", generation: 2 });
    expect(await exists(store, orgBridgeKey(ROOT, ORG, 1))).toBe(true);
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    // A record of generation 1 copied to generation 2's key does not bind (generation and request differ).
    await putJson(store, orgBridgeKey(ROOT, ORG, 2), await readJson(store, orgBridgeKey(ROOT, ORG, 1)));
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_invalid");
  });
});

describe("completeOrgBridge: written only after the enrolled request's committed bridge_complete and a landed marker", () => {
  const ready = async (store: ObjectStore) => { await putMarker(store); await enroll(store, sqlWith(null)); };

  test("marker first, then bridge_complete: no record while the hold is live; the release writes it; the consumer opens only then", async () => {
    const store = fresh();
    await ready(store);
    for (const state of ["held", "sealed", "marker_attempted"]) {
      expect(await refusal(complete(store, sqlWith(held(state))))).toBe("bridge_hold_not_released");
      expect(await exists(store, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
      expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    }
    const r = await complete(store, sqlWith(released()));
    expect(r.outcome).toBe("written");
    const stored = await readJson(store, orgBridgeKey(ROOT, ORG, 1));
    expect(validOrgBridge(stored)).toBe(true);
    expect(stored).toEqual(record({ writtenAtMs: 10 }));
    expect(JSON.stringify(stored)).not.toContain(EPOCH); // hashes only
    expect(await direct(store)).toBe("claimed");
    expect(await complete(store, sqlWith(released()), { now: () => 99 })).toEqual({ outcome: "adopted", recordSha256: r.recordSha256 });
  });

  test("bridge_complete committed before the marker PUT lands: refused (bridge_marker_absent), fenced; the late marker alone does not open; a rerun does", async () => {
    const store = fresh();
    await enroll(store, sqlWith(null));
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_marker_absent");
    expect(await exists(store, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    await putMarker(store); // the paused marker PUT is delivered late
    expect(await direct(store)).toBe("ledger_fence_authority_bridge_pending");
    expect((await complete(store, sqlWith(released()))).outcome).toBe("written");
    expect(await direct(store)).toBe("claimed");
  });

  test("a crash between the SQL release and the record: fenced (fail closed), and the rerun completes it", async () => {
    const base = fresh();
    await ready(base);
    expect(await refusal(complete(failing(base, "put", orgBridgeKey(ROOT, ORG, 1)), sqlWith(released())))).toBe("bridge_put_unknown");
    expect(await exists(base, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
    expect(await direct(base)).toBe("ledger_fence_authority_bridge_pending");
    expect((await complete(base, sqlWith(released()))).outcome).toBe("written");
    expect(await direct(base)).toBe("claimed");
  });

  test("an ambiguous record PUT that did land is adopted by its read-back", async () => {
    const base = fresh();
    await ready(base);
    expect((await complete(failing(base, "put", orgBridgeKey(ROOT, ORG, 1), true), sqlWith(released()))).outcome).toBe("adopted");
    expect(await direct(base)).toBe("claimed");
  });

  test("anything but the enrolled request released with bridge_complete refuses, with nothing written", async () => {
    const cases: [Record<string, unknown> | null, string][] = [
      [null, "bridge_hold_absent"],
      [released({ epoch: "00000000-0000-4000-8000-000000000002" }), "bridge_hold_epoch_mismatch"],
      [released({ request_id: REQ2 }), "bridge_request_mismatch"],
      [held("released", { release_reason: "verified_rollback", release_evidence_sha256: EVIDENCE }), "bridge_release_not_bridge_complete"],
      [released({ release_evidence_sha256: "nope" }), "bridge_hold_unreadable"],
    ];
    for (const [h, want] of cases) {
      const store = fresh();
      await ready(store);
      expect(await refusal(complete(store, sqlWith(h)))).toBe(want);
      expect(await exists(store, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
    }
    const store = fresh();
    await putMarker(store);
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_not_enrolled");
    await enroll(store, sqlWith(null));
    expect(await refusal(complete(store, { query: async () => { throw new Error("db down"); } }))).toBe("bridge_hold_unreadable");
    expect(await refusal(complete(store, sqlWith(released()), { epoch: "not-a-uuid" }))).toBe("bridge_input");
    await putMarker(store, enc.encode(JSON.stringify({ ...FIELDS, orgId: "org-other" })));
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_marker_invalid");
  });

  test("a stored record of another release is a conflict and is never overwritten", async () => {
    const store = fresh();
    await ready(store);
    const other = record({ releaseEvidenceSha256: "f".repeat(64) });
    await putJson(store, orgBridgeKey(ROOT, ORG, 1), other);
    expect(await refusal(complete(store, sqlWith(released())))).toBe("bridge_record_conflict");
    expect(await readJson(store, orgBridgeKey(ROOT, ORG, 1))).toEqual(other);
  });

  test("no drain beside the switch: a live drain or export lease refuses; a stalled drain whose lease expired loses it to the bridge", async () => {
    const store = fresh();
    await ready(store);
    let now = 1_000;
    const drain = await acquireDrainLock(store, ROOT, { holder: "drain-1", ttlMs: 60_000, now: () => now });
    expect(await refusal(complete(store, sqlWith(released()), { now: () => now }))).toBe("bridge_drain_running");
    expect(await exists(store, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
    now = 1_000 + 61_000;
    expect((await complete(store, sqlWith(released()), { now: () => now })).outcome).toBe("written");
    expect(drain.alive()).toBe(false);
    await expect(drain.renew()).rejects.toMatchObject({ code: "drain_lock_lost" });
    const s2 = fresh();
    await ready(s2);
    const exp = await acquireDrainLock(s2, ROOT, { holder: "export-1", ttlMs: 60_000, now: () => 5, key: exportLockKey(ROOT) });
    expect(await refusal(complete(s2, sqlWith(released()), { now: () => 5 }))).toBe("bridge_drain_running");
    expect(await exists(s2, drainLockKey(ROOT))).toBe(true); // its own lease, expired in place
    await exp.release();
    expect((await complete(s2, sqlWith(released()), { now: () => 5 })).outcome).toBe("written");
    // A clock at zero would write a record the consumer refuses: refused before any PUT.
    const s3 = fresh();
    await ready(s3);
    expect(await refusal(complete(s3, sqlWith(released()), { now: () => 0 }))).toBe("bridge_input");
    expect(await exists(s3, orgBridgeKey(ROOT, ORG, 1))).toBe(false);
  });

  test("store errors are codes, never an admit", async () => {
    const base = fresh();
    await ready(base);
    const broken = failing(base, "getObject", orgBridgeKey(ROOT, ORG, 1));
    expect(await refusal(complete(broken, sqlWith(released())))).toBe("bridge_record_unreadable");
    expect(await direct(broken)).toMatch(/^ledger_store_/);
    expect(await direct(failing(base, "getObject", orgEnrollmentKey(ROOT, ORG)))).toMatch(/^ledger_store_/);
  });
});
