/**
 * The S3 writer fence of a per-org 0005 cutover, from hold install through a COMMITTED bridge_complete
 * (docs/design/org-bridge-gate.md). Slot 062320's hold fences the SQL writers. Without this module, the S3 consumer
 * reopened an org as soon as its fence-authority marker existed. That was before the drain and the erasure replay,
 * and before `bridge_complete`. The direct writer and the fallback writer could then both land.
 *
 * The writers of one cut-over org O, hold epoch E:
 *   direct    the S3 consumer (ledger-s3 claim + commit) for direct-to-S3 inbox uploads;
 *   fallback  the Worker's HTTP admission into SQL (refused by the 062320 hold from install until release, then by
 *             the marker's `workerHttpAdmission: "off"` precondition) and the buffered drain (buffered-drain.ts), which
 *             writes the fallback-accepted backlog into the canonical layout.
 *
 * The switch is ONE create-only S3 object, the bridge-complete record (ledger-s3 orgBridgeKey, OrgBridgeDoc). It
 * binds the org, sha256 of the hold epoch, sha256 of the marker's stored bytes and the hold's release evidence:
 *   - before it exists, no direct claim of O admits (ledger-s3 `orgBridge: "required"`:
 *     `ledger_fence_authority_bridge_pending`), whatever the marker says and whatever an earlier read cached;
 *   - from the moment it exists, the drain writes nothing more of O (`bridge_complete_drain_refused`): it re-reads
 *     the record, uncached, before each batch's first canonical byte and again before its index merge.
 *
 * completeOrgBridge writes it, and only when all of these hold, in this order:
 *   1. the hold status shows E RELEASED with reason `bridge_complete` (a committed SQL release, never an intent);
 *   2. the org's per-org marker is present and valid for O, so bridge_complete can never open an org whose marker PUT
 *      has not landed (a paused marker PUT delivered later just makes a rerun succeed);
 *   3. the drain's run lease (buffered-drain.ts acquireExclusiveLock) is taken, so no drain process holds a live
 *      lease. It is held over the PUT: a stalled drain's lease is superseded (its fencing token is below this one),
 *      and the drain checks its lease before every mutation;
 *   4. create-only PUT, then read back. A present record is adopted when it is byte-for-byte this release's (a rerun
 *      after a crash or a lost answer), and anything else is `bridge_record_conflict`, never overwritten.
 * A crash anywhere before 4 leaves O fenced for direct writes (fail closed). Rerunning is safe at every step.
 *
 * Out of scope here: wiring this into the bridge's release (hold-bridge.ts, sessions-backlog-integration); SQL
 * admission after release (it rests on `workerHttpAdmission: "off"`, see the design doc's open item). Codes and
 * hashes only, never an org id, epoch or key in an error.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { acquireExclusiveLock, DRAIN_LOCK_TTL_MS, exportLockKey, DrainLockError } from "./buffered-drain.ts";
import { orgBridgeEpochHash, orgBridgeKey, orgFenceAuthorityKey, validOrgBridge, validOrgFenceAuthority, type OrgBridgeDoc } from "./ledger-s3.ts";

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
  | "bridge_put_unknown";

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

/** The latest hold of the org, as slot 062320's status function returns it (the live row, else the latest released). */
async function releasedHold(sql: BridgeSql, orgId: string, epoch: string): Promise<{ evidence: string }> {
  let st: unknown;
  try {
    const r = await sql.query("SELECT sessions.ingest_org_admission_hold_status($1::text) AS status", [orgId]);
    st = r.rows.length === 1 ? r.rows[0]!.status : undefined;
    if (typeof st === "string") st = JSON.parse(st);
  } catch {
    return refuse("bridge_hold_unreadable");
  }
  if (!st || typeof st !== "object" || Array.isArray(st) || !("hold" in st)) return refuse("bridge_hold_unreadable");
  const h = (st as { hold: unknown }).hold as Record<string, unknown> | null;
  if (h === null) return refuse("bridge_hold_absent");
  if (typeof h !== "object" || typeof h.epoch !== "string") return refuse("bridge_hold_unreadable");
  if (h.epoch !== epoch) return refuse("bridge_hold_epoch_mismatch");
  if (h.state !== "released") return refuse("bridge_hold_not_released");
  if (h.release_reason !== "bridge_complete") return refuse("bridge_release_not_bridge_complete");
  if (typeof h.release_evidence_sha256 !== "string" || !HEX64.test(h.release_evidence_sha256)) return refuse("bridge_hold_unreadable");
  return { evidence: h.release_evidence_sha256 };
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
  return validOrgBridge(v) && v.orgId === want.orgId && v.holdEpochSha256 === want.holdEpochSha256
    && v.markerSha256 === want.markerSha256 && v.releaseEvidenceSha256 === want.releaseEvidenceSha256;
}

export async function completeOrgBridge(input: CompleteOrgBridgeInput): Promise<CompleteOrgBridgeResult> {
  const { sql, store, keyRoot, orgId, epoch, holder } = input;
  const now = input.now ?? Date.now;
  if (!ORG.test(orgId) || !UUID.test(epoch) || typeof holder !== "string" || holder.length === 0 || typeof keyRoot !== "string") refuse("bridge_input");
  // 1. The committed release.
  const { evidence } = await releasedHold(sql, orgId, epoch);
  // 2. The marker, by its stored bytes.
  let markerBytes: Uint8Array | null;
  try { markerBytes = await readBytes(store, orgFenceAuthorityKey(keyRoot, orgId)); } catch { return refuse("bridge_marker_invalid"); }
  if (!markerBytes) return refuse("bridge_marker_absent");
  const marker = parse(markerBytes);
  if (!validOrgFenceAuthority(marker) || marker.orgId !== orgId) return refuse("bridge_marker_invalid");
  const want = { v: 1 as const, kind: "org-bridge-complete" as const, orgId, holdEpochSha256: orgBridgeEpochHash(epoch), markerSha256: sha256(markerBytes), releaseEvidenceSha256: evidence };
  const key = orgBridgeKey(keyRoot, orgId);
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
