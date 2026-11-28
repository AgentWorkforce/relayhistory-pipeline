/**
 * The S3 writer fence of a per-org 0005 cutover, from hold install through a COMMITTED bridge_complete
 * (docs/design/org-bridge-gate.md). Slot 062320's hold fences the SQL writers; without this the S3 consumer reopened
 * an org as soon as its fence-authority marker existed (before the drain, the erasure replay and `bridge_complete`),
 * so the direct and the fallback writer could both land.
 *
 * Writers of one cut-over org O:
 *   direct    the S3 consumer (ledger-s3 claim + commit) for direct-to-S3 inbox uploads;
 *   fallback  the Worker's HTTP admission into SQL (the 062320 hold from install to release, then the marker's
 *             `workerHttpAdmission: "off"`) and the buffered drain (buffered-drain.ts).
 *
 * Two operator objects (ledger-s3 orgEnrollmentKey, orgBridgeKey):
 *   ENROLLMENT, generation G, bound to sha256 of the hold INSTALL's request id, written by enrollOrg BEFORE the SQL
 *     install (the install tool checks enrollmentFor first). From then on ledger-s3 `orgBridge: "required"` reads it
 *     on every claim and commit, uncached: O admits only beside its per-org marker AND the bridge record for G.
 *   BRIDGE-COMPLETE for G, create-only, written by completeOrgBridge only when, in order:
 *     1. O is enrolled at G and the hold status shows the hold of G's request RELEASED with `bridge_complete`;
 *     2. O's per-org marker is present and valid (a marker PUT still in flight just makes a rerun succeed);
 *     3. the drain's run lease is taken (no live drain or export), held over the PUT;
 *     4. create-only PUT, read back; a present record is adopted only when it binds the same release.
 *   The drain refuses O from the moment it exists.
 *
 * Recovery (withdrawEnrollment, CAS to `withdrawn`, the markers decide again):
 *   install_failed     the install of G's request never committed (status shows no hold of that request). The next
 *                      attempt enrolls G+1 with a NEW request id; a late commit of the old install is then a hold no
 *                      enrollment covers, which completeOrgBridge refuses, and its only exit is verified_rollback.
 *   verified_rollback  the hold of G's request released with `verified_rollback` (so no marker was ever attempted).
 * A later cutover of O enrolls G+1 (from withdrawn, or once G's bridge record exists): old bytes of G never satisfy it.
 *
 * Not here: wiring into the bridge's release (hold-bridge.ts) and the install tool (sessions-backlog-integration,
 * sessions-wave-tooling); paused-writer exclusion at the authoritative write (design doc, item C). Codes and hashes
 * only, never an org id, epoch, request id or key in an error.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore, type ObjectVersion } from "../shared/vendor.ts";
import { acquireExclusiveLock, DRAIN_LOCK_TTL_MS, exportLockKey, DrainLockError } from "./buffered-drain.ts";
import {
  orgBridgeEpochHash, orgBridgeKey, orgEnrollmentKey, orgEnrollmentRequestHash, orgFenceAuthorityKey, validOrgBridge, validOrgEnrollment,
  validOrgFenceAuthority, type OrgBridgeDoc, type OrgEnrollmentDoc, type OrgEnrollmentWithdrawal,
} from "./ledger-s3.ts";

export class OrgBridgeRefusal extends Error {
  constructor(readonly code: OrgBridgeRefusalCode) { super(code); this.name = "OrgBridgeRefusal"; }
}
export type OrgBridgeRefusalCode =
  | "bridge_input"
  | "bridge_hold_unreadable"
  | "bridge_hold_absent"
  | "bridge_hold_epoch_mismatch"
  | "bridge_hold_not_released"
  | "bridge_release_not_bridge_complete"
  | "bridge_marker_absent"
  | "bridge_marker_invalid"
  | "bridge_drain_running"
  | "bridge_record_conflict"
  | "bridge_record_unreadable"
  | "bridge_put_unknown"
  | "bridge_not_enrolled"
  | "bridge_request_mismatch"
  | "enrollment_invalid"
  | "enrollment_live"
  | "enrollment_after_install"
  | "enrollment_contended"
  | "enrollment_hold_present"
  | "enrollment_rollback_unverified";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const ORG = /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$/u;
const refuse = (code: OrgBridgeRefusalCode): never => { throw new OrgBridgeRefusal(code); };
const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** An owner SQL client (READ COMMITTED; the status function is owner-only and read-only). */
export interface BridgeSql { query(text: string, params: unknown[]): Promise<{ rows: readonly Record<string, unknown>[] }> }

export interface CompleteOrgBridgeInput {
  sql: BridgeSql;
  store: ObjectStore;
  keyRoot: string;
  orgId: string;
  /** The hold epoch whose release this projects. */
  epoch: string;
  /** This run's lease holder name (the drain lease, held over the PUT). */
  holder: string;
  now?: () => number;
}
export interface CompleteOrgBridgeResult { outcome: "written" | "adopted"; recordSha256: string }

/** The latest hold of the org, as slot 062320's (062340's) status function returns it: the live row, else the latest released. */
async function latestHold(sql: BridgeSql, orgId: string): Promise<Record<string, unknown> | null> {
  let st: unknown;
  try {
    const r = await sql.query("SELECT sessions.ingest_org_admission_hold_status($1::text) AS status", [orgId]);
    st = r.rows.length === 1 ? r.rows[0]!.status : undefined;
    if (typeof st === "string") st = JSON.parse(st);
  } catch {
    return refuse("bridge_hold_unreadable");
  }
  if (!st || typeof st !== "object" || Array.isArray(st) || !("hold" in st)) return refuse("bridge_hold_unreadable");
  const h = (st as { hold: unknown }).hold;
  if (h === null) return null;
  if (!h || typeof h !== "object" || Array.isArray(h)) return refuse("bridge_hold_unreadable");
  const row = h as Record<string, unknown>;
  if (typeof row.epoch !== "string" || typeof row.request_id !== "string" || typeof row.state !== "string") return refuse("bridge_hold_unreadable");
  return row;
}

async function releasedHold(sql: BridgeSql, orgId: string, epoch: string): Promise<{ evidence: string; requestSha256: string }> {
  const h = await latestHold(sql, orgId);
  if (h === null) return refuse("bridge_hold_absent");
  if (h.epoch !== epoch) return refuse("bridge_hold_epoch_mismatch");
  if (h.state !== "released") return refuse("bridge_hold_not_released");
  if (h.release_reason !== "bridge_complete") return refuse("bridge_release_not_bridge_complete");
  if (typeof h.release_evidence_sha256 !== "string" || !HEX64.test(h.release_evidence_sha256)) return refuse("bridge_hold_unreadable");
  return { evidence: h.release_evidence_sha256, requestSha256: orgEnrollmentRequestHash(h.request_id as string) };
}

/** The org's enrollment with its version, or null. Invalid (or another org's) is enrollment_invalid. */
async function readEnrollment(store: ObjectStore, keyRoot: string, orgId: string): Promise<{ doc: OrgEnrollmentDoc; version: ObjectVersion } | null> {
  let got;
  try { got = await store.getObject(objectKey(orgEnrollmentKey(keyRoot, orgId))); } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    return refuse("enrollment_invalid");
  }
  const v = parse(await readAll(got.body).catch(() => new Uint8Array()));
  if (!validOrgEnrollment(v) || v.orgId !== orgId) return refuse("enrollment_invalid");
  return { doc: v, version: got.meta.version };
}

/** Create-only (no version) or CAS. false: lost the race. */
async function casJson(store: ObjectStore, key: string, doc: unknown, version: ObjectVersion | undefined): Promise<boolean> {
  try {
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(doc)), version ? { ifMatch: version, contentType: "application/json" } : { ifNoneMatch: true, contentType: "application/json" });
    return true;
  } catch (error) {
    if (isObjectStoreError(error, "precondition_failed") || isObjectStoreError(error, "already_exists")) return false;
    throw error;
  }
}

export interface EnrollmentInput { sql: BridgeSql; store: ObjectStore; keyRoot: string; orgId: string; requestId: string; now?: () => number }

/**
 * Enroll O for the hold install whose request id is `requestId`, BEFORE that install. Idempotent for the same request.
 * Refuses while another request's enrollment is live and unbridged (`enrollment_live`), and when a live or latest hold
 * of this request already exists without an enrollment (`enrollment_after_install`: the install ran first, so admitted
 * direct writers were never fenced by install; recover by verified_rollback and a new request).
 */
export async function enrollOrg(input: EnrollmentInput): Promise<{ outcome: "enrolled" | "adopted"; generation: number }> {
  const { sql, store, keyRoot, orgId, requestId } = input;
  const now = input.now ?? Date.now;
  if (!ORG.test(orgId) || !UUID.test(requestId) || typeof keyRoot !== "string") refuse("bridge_input");
  const requestSha256 = orgEnrollmentRequestHash(requestId);
  for (let attempt = 0; attempt < 4; attempt++) {
    const cur = await readEnrollment(store, keyRoot, orgId);
    if (cur && cur.doc.state === "enrolled" && cur.doc.requestSha256 === requestSha256) return { outcome: "adopted", generation: cur.doc.generation };
    const hold = await latestHold(sql, orgId);
    if (hold && hold.request_id === requestId) return refuse("enrollment_after_install");
    if (hold && hold.state !== "released") return refuse("enrollment_hold_present");
    let generation = 1;
    if (cur) {
      if (cur.doc.state === "enrolled") {
        // Another request's cutover: only once its bridge record exists (that cutover completed).
        let bridged: Uint8Array | null;
        try { bridged = await readBytes(store, orgBridgeKey(keyRoot, orgId, cur.doc.generation)); } catch { return refuse("bridge_record_unreadable"); }
        if (!bridged) return refuse("enrollment_live");
      }
      generation = cur.doc.generation + 1;
    }
    const doc: OrgEnrollmentDoc = { v: 1, kind: "org-cutover-enrollment", orgId, generation, requestSha256, state: "enrolled", withdrawn: null, writtenAtMs: now() };
    if (!validOrgEnrollment(doc)) return refuse("bridge_input");
    if (await casJson(store, orgEnrollmentKey(keyRoot, orgId), doc, cur?.version)) return { outcome: "enrolled", generation };
  }
  return refuse("enrollment_contended");
}

/** For the install tool, right before `_install(org, requestId)`: O is enrolled for exactly this request. */
export async function enrollmentFor(store: ObjectStore, keyRoot: string, orgId: string, requestId: string): Promise<{ generation: number }> {
  if (!ORG.test(orgId) || !UUID.test(requestId)) refuse("bridge_input");
  const cur = await readEnrollment(store, keyRoot, orgId);
  if (!cur || cur.doc.state !== "enrolled") return refuse("bridge_not_enrolled");
  if (cur.doc.requestSha256 !== orgEnrollmentRequestHash(requestId)) return refuse("bridge_request_mismatch");
  return { generation: cur.doc.generation };
}

/**
 * Withdraw O's enrollment for `requestId` (CAS to `withdrawn`; the markers decide again). `install_failed`: the
 * status shows no hold of that request (its install never committed). `verified_rollback`: the latest hold is that
 * request's, released with verified_rollback (no marker was ever attempted). Idempotent.
 */
export async function withdrawEnrollment(input: EnrollmentInput & { reason: OrgEnrollmentWithdrawal }): Promise<{ outcome: "withdrawn" | "adopted" }> {
  const { sql, store, keyRoot, orgId, requestId, reason } = input;
  const now = input.now ?? Date.now;
  if (!ORG.test(orgId) || !UUID.test(requestId) || (reason !== "install_failed" && reason !== "verified_rollback")) refuse("bridge_input");
  const requestSha256 = orgEnrollmentRequestHash(requestId);
  for (let attempt = 0; attempt < 4; attempt++) {
    const cur = await readEnrollment(store, keyRoot, orgId);
    if (!cur) return refuse("bridge_not_enrolled");
    if (cur.doc.requestSha256 !== requestSha256) return refuse("bridge_request_mismatch");
    if (cur.doc.state === "withdrawn") return cur.doc.withdrawn === reason ? { outcome: "adopted" } : refuse("enrollment_invalid");
    const hold = await latestHold(sql, orgId);
    if (reason === "install_failed") {
      if (hold && hold.request_id === requestId) return refuse("enrollment_hold_present");
      if (hold && hold.state !== "released") return refuse("enrollment_hold_present");
    } else if (!hold || hold.request_id !== requestId || hold.state !== "released" || hold.release_reason !== "verified_rollback") {
      return refuse("enrollment_rollback_unverified");
    }
    const doc: OrgEnrollmentDoc = { ...cur.doc, state: "withdrawn", withdrawn: reason, writtenAtMs: Math.max(cur.doc.writtenAtMs, now()) };
    if (await casJson(store, orgEnrollmentKey(keyRoot, orgId), doc, cur.version)) return { outcome: "withdrawn" };
  }
  return refuse("enrollment_contended");
}

async function readBytes(store: ObjectStore, key: string): Promise<Uint8Array | null> {
  try {
    return await readAll((await store.getObject(objectKey(key))).body);
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
}
const parse = (b: Uint8Array): unknown => { try { return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(b)); } catch { return undefined; } };

/** A stored record is this release's when every bound field matches (writtenAtMs may differ: an earlier attempt's). */
function sameRelease(v: unknown, want: Omit<OrgBridgeDoc, "writtenAtMs">): boolean {
  return validOrgBridge(v) && v.orgId === want.orgId && v.generation === want.generation && v.requestSha256 === want.requestSha256 && v.holdEpochSha256 === want.holdEpochSha256
    && v.markerSha256 === want.markerSha256 && v.releaseEvidenceSha256 === want.releaseEvidenceSha256;
}

export async function completeOrgBridge(input: CompleteOrgBridgeInput): Promise<CompleteOrgBridgeResult> {
  const { sql, store, keyRoot, orgId, epoch, holder } = input;
  const now = input.now ?? Date.now;
  if (!ORG.test(orgId) || !UUID.test(epoch) || typeof holder !== "string" || holder.length === 0 || typeof keyRoot !== "string") refuse("bridge_input");
  // 1. The enrollment, and the committed release of exactly its request.
  const enrolled = await readEnrollment(store, keyRoot, orgId);
  if (!enrolled || enrolled.doc.state !== "enrolled") return refuse("bridge_not_enrolled");
  const { evidence, requestSha256 } = await releasedHold(sql, orgId, epoch);
  if (requestSha256 !== enrolled.doc.requestSha256) return refuse("bridge_request_mismatch");
  const generation = enrolled.doc.generation;
  // 2. The marker, by its stored bytes.
  let markerBytes: Uint8Array | null;
  try { markerBytes = await readBytes(store, orgFenceAuthorityKey(keyRoot, orgId)); } catch { return refuse("bridge_marker_invalid"); }
  if (!markerBytes) return refuse("bridge_marker_absent");
  const marker = parse(markerBytes);
  if (!validOrgFenceAuthority(marker) || marker.orgId !== orgId) return refuse("bridge_marker_invalid");
  const want = { v: 1 as const, kind: "org-bridge-complete" as const, orgId, generation, requestSha256, holdEpochSha256: orgBridgeEpochHash(epoch), markerSha256: sha256(markerBytes), releaseEvidenceSha256: evidence };
  const key = orgBridgeKey(keyRoot, orgId, generation);
  const existing = async (): Promise<CompleteOrgBridgeResult | null> => {
    let got: Uint8Array | null;
    try { got = await readBytes(store, key); } catch { return refuse("bridge_record_unreadable"); }
    if (!got) return null;
    if (!sameRelease(parse(got), want)) return refuse("bridge_record_conflict");
    return { outcome: "adopted", recordSha256: sha256(got) };
  };
  const already = await existing();
  if (already) return already;
  // 3. No drain beside the switch: take the drain's own lease (and refuse beside a live export).
  let lease;
  try {
    lease = await acquireExclusiveLock(store, keyRoot, { holder, ttlMs: DRAIN_LOCK_TTL_MS, now, peerKey: exportLockKey(keyRoot) });
  } catch (error) {
    if (error instanceof DrainLockError) return refuse("bridge_drain_running");
    throw error;
  }
  try {
    // 4. Create-only, then read back.
    const doc: OrgBridgeDoc = { ...want, writtenAtMs: now() };
    // Never PUT what the consumer would refuse (a clock at or before the epoch): it would block the switch for good.
    if (!validOrgBridge(doc)) return refuse("bridge_input");
    const body = new TextEncoder().encode(JSON.stringify(doc));
    try {
      await store.put(objectKey(key), body, { ifNoneMatch: true, contentType: "application/json" });
    } catch (error) {
      if (!(isObjectStoreError(error, "already_exists") || isObjectStoreError(error, "precondition_failed"))) {
        // Possibly landed: the read-back decides; absent stays fenced and a rerun retries.
        const after = await existing().catch(() => null);
        if (after) return after;
        return refuse("bridge_put_unknown");
      }
    }
    const after = await existing();
    if (!after) return refuse("bridge_put_unknown");
    return after.recordSha256 === sha256(body) ? { outcome: "written", recordSha256: after.recordSha256 } : after;
  } finally {
    await lease.release().catch(() => {});
  }
}
