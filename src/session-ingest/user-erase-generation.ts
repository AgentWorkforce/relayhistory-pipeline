/**
 * The USER-ERASE GENERATION PROOF for the hold bridge (hold-bridge.ts, `erasure_generation_unproven`): what lets an
 * operator user erase recorded after a FULL cutoff count toward the 062320 hold's `bridge_complete`. CODE ONLY and
 * DORMANT: no workflow mode wires it, nothing here applies a migration, and it reads and writes only the object store
 * it is given (the bridge passes the S3 ledger's).
 *
 * Contract (layered-sessions-pipeline, 2026-11-27, answering Q1-Q4):
 *
 *   Generation.  A user erase R's generation G(R) is the user's S3-fence reset ordinal that R's OWN begin CAS produced,
 *                recorded in the same CAS (ledger-s3.ts FenceDoc.resetOrdinals, `bindOrdinal`). It is the only
 *                durable request -> generation binding: resetRequests[R] is the count tombstoned, shared by every reset
 *                finished together, and resets / resetsCompleted move on. Every index entry and owner stub stamped
 *                below G(R) is forgotten once resetsCompleted >= G(R) (entryForgotten), and every log batch admitted
 *                below G(R) is never materialized (ledger-log-materialize.ts). A request recorded without an ordinal
 *                (a copy of SQL's reset by the backfill or the control mirror, or any reset begun before the field
 *                existed) has NO proven generation: refused (`generation_ordinal_missing`), however finished it is.
 *
 *   Split.       Two checks, never one proof feeding itself:
 *                  1. PRE-EFFECT (generationPreEffect): may the replay run at all? Read-only, before any write.
 *                  2. POST-EFFECT (generationEvidence -> the create-only receipt; verifyGeneration at the gate):
 *                     did the bytes of every generation below G(R) go, in every byte class in scope?
 *                The fence writer (applyReset) captures the ordinal; the bridge replay only assembles the receipt
 *                from what it reads afterwards, after SQL bytes_verified. A receipt found on a retry is re-derived
 *                and must match field for field.
 *
 *   Ordering.    The erase must be strictly AFTER the hold's install (062340's history fence; 062320 refuses SQL
 *                'admission' for any non-released hold from the install on, and the install waits out every writer
 *                holding the org key). Timestamps are used ONLY to refuse: an erase before the install is
 *                `generation_pre_install` (an exact historic boundary would be needed, and none exists), one within
 *                ERASE_ORDER_MARGIN_MS of it is `generation_ambiguous`. They never prove anything: that every
 *                admission and publication path (SQL, direct S3, fallback) was fenced before R must come from
 *                `admissionFencedBefore`, an evidence port. NOT BUILT: absent, every user erase stays
 *                `generation_admission_unproven`.
 *
 *   Bytes.       SQL `bytes_verified` is required, AND the byte phase's own receipt (user-erase-byte-receipt.ts),
 *                written by the erase job into the store it erased, must be found in the bridge's store, naming this
 *                destination and the exact prefixes; with a routed ledger bucket the ledger-home marker must name it.
 *                A declaration never stands in for that. Every read and write here takes the bridge's one store. Every other byte class of the user is checked here,
 *                generation-bound, read-only (BYTE_CLASSES below). A class that cannot be shown empty below G(R)
 *                refuses; a generic sweeper pass or a bare zero listing never stands in.
 *
 *   Payloads.    The replay's log-payload erase is eraseGenerationLogPayloads(G(R)): only batches admitted below G(R),
 *                in every stream of the user. Never the unconditional eraseLogPayloads: a known reset no-ops on a
 *                retry after release while an unconditional payload erase would still delete post-release uploads.
 *
 * Receipts and ids: the receipt carries hashes, counts, the key root and the hold epoch only; no org, workspace, user,
 * request or session id. Refusals are closed codes.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { component, orgPrefix } from "../migration/neon-s3-sync/export-session.ts";
import { LEGACY_EXPORT_ORIGIN_ID } from "../shared/legacy-export-machine.ts";
import type { LedgerTenant } from "./ledger.ts";
import { generationLogPayloadsUnproven } from "./ledger-log-erase.ts";
import { listKeysUnder } from "./ledger-log.ts";
import { fenceEpoch, hasPendingReset, isDeleted, s3LedgerKeys, s3LedgerObjects, sessionKeyHash, validOwner, validRearm, type FenceDoc, type FenceRead } from "./ledger-s3.ts";
import { INBOX_NAMESPACE } from "./async-admission.ts";
import { rearmDoneKey } from "./rearm-sweeper.ts";
import { liveMachineId } from "./writer.ts";
import { userErasePrefixes } from "../session-erase/user-erase.ts";
import { byteRequestHash, byteTenantHash, destinationText, prefixesDigest, readUserEraseByteReceipt, sameBucket, validStoreDestination, type StoreDestination,
  type UserEraseByteReceipt } from "../session-erase/user-erase-byte-receipt.ts";
import { ledgerHomeKey, validLedgerHome } from "./ledger-store.ts";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const str = (v: unknown): v is string => typeof v === "string" && v.length > 0 && v.length <= 512;
const sha = (...parts: string[]) => createHash("sha256").update(JSON.stringify(["user-erase-generation/v1", ...parts])).digest("hex");
const h32 = (...parts: string[]) => "h_" + sha(...parts).slice(0, 32);

/**
 * Every object name the ledger writes under a user's admission directory (ledger-s3.ts s3LedgerKeys, ledger-log.ts
 * logKeysAt and logUserIndex, async-admission.ts journalKey), exactly. Anything else there is unaccounted for.
 */
const H32 = "[0-9a-f]{32}";
export const OWN_ADMIN_OBJECTS = new RegExp("^(?:upload-fences\\.json|privacy-journal\\.json"
  + `|ledger/batches/o=${H32}/b=${H32}\\.json|ledger/revisions/o=${H32}/r=[0-9a-f]{2,3}\\.json|ledger/owners/o=${H32}/s=${H32}\\.json`
  + `|ledger/rearm/o=${H32}/${H32}(?:\\.(?:done|corrupt)/${H32})?\\.json|ledger/log-index/o=${H32}\\.json`
  + `|ledger/log/o=${H32}/(?:e/[0-9]{17}/[0-9]{20}\\.json|c/[0-9]{20}\\.json|m/[0-9]{20}\\.json|s/${H32}\\.json|p/[0-9a-f]{64}\\.json\\.zst`
  + "|head\\.json|closed\\.json|stream\\.json))$", "u");

/** An erase recorded within this of the hold's install is ambiguous (clock and commit order are not proof). */
export const ERASE_ORDER_MARGIN_MS = 5_000;

export type GenerationRefusal =
  | "erasure_bytes_unverified" | "generation_hold_invalid" | "generation_full_unbound" | "generation_pre_install" | "generation_ambiguous"
  | "generation_admission_unproven" | "generation_destination_mismatch" | "generation_fence_unreadable" | "generation_reset_pending"
  | "generation_ordinal_missing" | "generation_reset_unfinished" | "generation_owner_live" | "generation_payloads_remain"
  | "generation_rearm_pending" | "generation_bytes_remain" | "generation_machine_unattributed" | "generation_byte_receipt_missing" | "generation_receipt_mismatch" | "generation_unreadable";

export class GenerationRefused extends Error {
  constructor(readonly code: GenerationRefusal) { super(code); this.name = "GenerationRefused"; }
}

/** What the erase is bound to. `full`: the FULL 062340 bound to this epoch (run, start ms, the seal's evidence). */
export interface GenerationBinding {
  /**
   * The bridge store's full destination (hold-bridge.ts bridgeStore: the Locker bucket, the routed ledger bucket or
   * null, the key root), from the store's own construction config. Proven physically before any effect
   * (destinationProof): the ledger-home marker and the SQL byte phase's receipt, both read from the store itself.
   */
  destination: StoreDestination;
  keyRoot: string;
  tenant: LedgerTenant;
  requestId: string;
  epoch: string;
  hold: { state: "held" | "sealed" | "marker_attempted"; installedMs: number; sealedMs: number | null };
  full: { run: string; startedMs: number; evidenceSha256: string } | null;
  /** operator_user_erasures.created_at (ms) and its state. */
  erasure: { atMs: number; bytesVerified: boolean };
}

/** Per path: was it fenced for the org before R was recorded? Evidence, never a timestamp comparison. */
export interface AdmissionFencing { sql: boolean; direct: boolean; fallback: boolean }

/**
 * The ports. There is NO store here: every read and write takes the bridge's one store as an argument, so the reset,
 * the scoped payload erase, both receipts and every re-verification hit the same object store.
 */
export interface GenerationPorts {
  /**
   * Evidence that every admission and publication path for the org was fenced before R. NOT BUILT: absent,
   * generation_admission_unproven. A throw is the same refusal.
   */
  admissionFencedBefore?(b: GenerationBinding): Promise<AdmissionFencing>;
  /**
   * Attribution of machines under this workspace: `self` (this user's, beyond the derived ones) or `other` (provably
   * another user's; this includes other users' admission pseudo-machines, async-admission.ts admissionUserDir). Every machine found under ws=<ws> in the publication-generation and inbox roots must
   * be this user's (then empty) or attributed `other`. Absent: an empty map, so any other machine refuses.
   */
  machineOwners?(b: GenerationBinding): Promise<ReadonlyMap<string, "self" | "other">>;
  /** The SQL erase's origins (operator_user_erase_begin's view), unioned with the S3 owner stubs' origins. */
  userOrigins(b: GenerationBinding): Promise<readonly string[]>;
  now(): number;
}

const refuse = (code: GenerationRefusal): never => { throw new GenerationRefused(code); };

/** The receipt's key: next to the bridge's erasure receipts, per hold epoch. */
export const generationReceiptKey = (keyRoot: string, org: string, epoch: string, requestId: string) =>
  `${keyRoot}/.ledger/hold-bridge/${org}/${epoch}/generations/${h32("request", requestId)}.json`;

function checkBinding(b: GenerationBinding): void {
  if (!validStoreDestination(b.destination) || !str(b.keyRoot) || b.destination.keyRoot !== b.keyRoot || !str(b.tenant?.orgId) || !str(b.tenant.workspaceId) || !str(b.tenant.userId) || !str(b.requestId) || !UUID.test(b.epoch))
    refuse("generation_hold_invalid");
  const { hold, erasure } = b;
  if (!hold || !Number.isSafeInteger(hold.installedMs) || hold.installedMs <= 0) refuse("generation_hold_invalid");
  // The replay runs only under a sealed (or attempted) hold: publication for the org is refused from the seal on.
  if (hold.state !== "sealed" && hold.state !== "marker_attempted") refuse("generation_hold_invalid");
  if (!Number.isSafeInteger(hold.sealedMs) || (hold.sealedMs as number) < hold.installedMs) refuse("generation_hold_invalid");
  if (!erasure || !Number.isSafeInteger(erasure.atMs) || erasure.atMs <= 0 || typeof erasure.bytesVerified !== "boolean") refuse("generation_hold_invalid");
  // 062340: the bound FULL started strictly after the install and no later than the seal.
  const f = b.full;
  if (!f || !str(f.run) || !Number.isSafeInteger(f.startedMs) || f.startedMs <= hold.installedMs || f.startedMs > (hold.sealedMs as number) || !HEX64.test(f.evidenceSha256))
    refuse("generation_full_unbound");
}

async function readFenceOf(store: ObjectStore, b: GenerationBinding): Promise<FenceRead> {
  try { return await s3LedgerObjects(store, b.keyRoot).readFence(s3LedgerKeys(b.keyRoot, b.tenant).fence); }
  catch { return refuse("generation_fence_unreadable"); }
}

// ---- 1. pre-effect --------------------------------------------------------------------------------------------------

/**
 * May R's replay run? Read-only. `generation`: R's bound ordinal when it already has one (a retry), null when R has
 * not been begun in this fence (the replay's applyReset will begin it and bind the ordinal). Throws GenerationRefused.
 */
export async function generationPreEffect(store: ObjectStore, p: GenerationPorts, b: GenerationBinding): Promise<{ generation: number | null }> {
  checkBinding(b);
  if (!b.erasure.bytesVerified) refuse("erasure_bytes_unverified");
  if (b.erasure.atMs < b.hold.installedMs) refuse("generation_pre_install");
  if (b.erasure.atMs - b.hold.installedMs < ERASE_ORDER_MARGIN_MS) refuse("generation_ambiguous");
  const receipt = await destinationProof(store, b);
  // The byte phase verified exactly the prefixes this erase's SQL origins give, checked BEFORE any write (and again in
  // the post-effect evidence): a receipt for another origin set never lets a replay start.
  let sqlOrigins: readonly string[];
  try { sqlOrigins = await p.userOrigins(b); } catch { return refuse("generation_unreadable"); }
  if (!Array.isArray(sqlOrigins) || !sqlOrigins.every(str)) refuse("generation_unreadable");
  const t = b.tenant;
  if (receipt.prefixes !== prefixesDigest(userErasePrefixes(b.keyRoot, { requestId: b.requestId, orgId: t.orgId, workspaceId: t.workspaceId, userId: t.userId }, sqlOrigins)))
    refuse("generation_destination_mismatch");
  let fenced: AdmissionFencing | undefined;
  try { fenced = p.admissionFencedBefore ? await p.admissionFencedBefore(b) : undefined; } catch { fenced = undefined; }
  if (!fenced || fenced.sql !== true || fenced.direct !== true || fenced.fallback !== true) refuse("generation_admission_unproven");
  const read = await readFenceOf(store, b);
  if (read.state === "absent") return { generation: null };
  const f = read.fence;
  if (hasPendingReset(f)) refuse("generation_reset_pending");
  if (!Object.hasOwn(f.resetRequests, b.requestId)) return { generation: null };
  const g = ordinalOf(f, b.requestId);
  if (g === null) refuse("generation_ordinal_missing");
  return { generation: g };
}

/**
 * The destination, proven from the store itself, never from a declaration:
 *   - the ledger-home marker (ledger-store.ts): with a routed ledger bucket it must exist and name that bucket; without
 *     one, a marker present must name the Locker bucket (a ledger relocated away refuses);
 *   - the SQL byte phase's receipt (session-erase/user-erase-byte-receipt.ts), written by the erase job into the store
 *     it erased: present HERE, for this request and tenant, naming this Locker bucket, region, endpoint and key root.
 * Returns the byte receipt.
 */
async function destinationProof(store: ObjectStore, b: GenerationBinding): Promise<UserEraseByteReceipt> {
  const d = b.destination;
  const home = await readJson(store, ledgerHomeKey(b.keyRoot));
  if (home !== null && !validLedgerHome(home)) refuse("generation_unreadable");
  if (d.ledger ? !(home && (home as { home: string }).home === d.ledger.bucket) : (home !== null && (home as { home: string }).home !== d.main.bucket))
    refuse("generation_destination_mismatch");
  let r: UserEraseByteReceipt | null;
  try { r = await readUserEraseByteReceipt(store, b.keyRoot, b.requestId); } catch { return refuse("generation_unreadable"); }
  if (r === null) return refuse("generation_byte_receipt_missing");
  const t = b.tenant;
  if (r.request !== byteRequestHash(b.requestId) || r.tenant !== byteTenantHash(t.orgId, t.workspaceId, t.userId)) refuse("generation_destination_mismatch");
  if (r.destination.keyRoot !== b.keyRoot || !sameBucket({ bucket: r.destination.bucket, region: r.destination.region, endpoint: r.destination.endpoint }, d.main))
    refuse("generation_destination_mismatch");
  return r;
}

const ordinalOf = (f: FenceDoc, requestId: string): number | null =>
  f.resetOrdinals && Object.hasOwn(f.resetOrdinals, requestId) ? f.resetOrdinals[requestId]! : null;

// ---- 2. post-effect -------------------------------------------------------------------------------------------------

/**
 * The byte classes of one user, beyond SQL's byte phase. Each is read for this (org, workspace, user) and its origins
 * only, and must hold nothing at a generation below G(R):
 *   sql_byte_phase      userErasePrefixes (live objects, pointers, staged and buffered batches, the legacy export
 *                       machine): covered by SQL bytes_verified; its byte receipt's prefix digest must match.
 *   upload_generations  {org}/upload-generations/g=* /v1/ws=<ws>/m=*: publication generations, which the SQL byte
 *                       phase does not cover. EVERY machine under the workspace is judged (machineOwners): this
 *                       user's must hold nothing (admission is held, so anything is pre-R); another user's must be
 *                       attributed; an unattributed machine refuses. A key that does not parse refuses.
 *   admission_inbox     .admission-inbox/<org>/v1/ws=<ws>/m=*: request bodies, judged the same way (the install drains
 *                       the queue). Only bodies and outcome records parse; anything else refuses.
 *   log_payloads        EVERY present payload of every stream, classified: only one positively bound at >= G(R)
 *                       (an unapplied batch admitted at or above it, not shared with an older one) is kept; an
 *                       orphan, applied residue, an unsnapshotted batch's or an older batch's payload counts: must be 0.
 *   owner_stubs         sessions claimed below G(R) whose epoch has not moved and that are not tombstoned: must be 0
 *                       (each would be an old-generation session the reset never reached).
 *   rearm               every re-arm marker of the user (a write fenced at commit: a paused pre-reset claim resumed
 *                       after the reset) must have the sweeper's done record for its exact bytes.
 */
export const BYTE_CLASSES = ["sql_byte_phase", "upload_generations", "admission_inbox", "log_payloads", "owner_stubs", "rearm"] as const;

export interface GenerationEvidence {
  v: 1;
  kind: "user-erase-generation";
  epoch: string;
  request: string;
  tenant: string;
  keyRoot: string;
  /** sha of the full destination (Locker and ledger buckets with region and endpoint, key root), proven by destinationProof. */
  destination: string;
  full: string;
  fence: { instance: string; generation: number; resetsCompleted: number };
  bytes: { sqlPrefixes: string; origins: number; uploadGenerations: 0; inbox: 0; payloads: 0; ownersOld: 0; rearmMarkers: number };
}
export type GenerationReceipt = GenerationEvidence & { producedAtMs: number };

const decoder = new TextDecoder("utf-8", { fatal: true });
async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  try { return await listKeysUnder(store, prefix); } catch { return refuse("generation_unreadable"); }
}
async function readBytes(store: ObjectStore, key: string): Promise<Uint8Array | null> {
  try { return await readAll((await store.getObject(objectKey(key))).body); }
  catch (e) { if (isObjectStoreError(e, "not_found")) return null; return refuse("generation_unreadable"); }
}
async function readJson(store: ObjectStore, key: string): Promise<unknown | null> {
  const raw = await readBytes(store, key);
  if (raw === null) return null;
  try { return JSON.parse(decoder.decode(raw)); } catch { return refuse("generation_unreadable"); }
}

/**
 * Derive the post-effect evidence for R, read-only. Throws GenerationRefused when any class is not shown erased below
 * G(R). Run after the replay's effects (and again at the gate): never before, so it never proves its own precondition.
 */
export async function generationEvidence(store: ObjectStore, p: GenerationPorts, b: GenerationBinding): Promise<GenerationEvidence> {
  checkBinding(b);
  if (!b.erasure.bytesVerified) refuse("erasure_bytes_unverified");
  const byteReceipt = await destinationProof(store, b);
  const read = await readFenceOf(store, b);
  if (read.state === "absent") refuse("generation_ordinal_missing");
  const f = (read as Extract<FenceRead, { state: "present" }>).fence;
  if (hasPendingReset(f)) refuse("generation_reset_pending");
  const g = Object.hasOwn(f.resetRequests, b.requestId) ? ordinalOf(f, b.requestId) : null;
  if (g === null) refuse("generation_ordinal_missing");
  const G = g as number;
  if (typeof f.resetRequests[b.requestId] !== "number" || f.resetsCompleted < G) refuse("generation_reset_unfinished");

  const keys = s3LedgerKeys(b.keyRoot, b.tenant), t = b.tenant;
  // owner stubs: none claimed below G whose session the reset did not reach.
  const origins = new Set<string>();
  let ownersOld = 0;
  for (const k of await listAll(store, keys.ownersRoot + "/")) {
    if (!/\/ledger\/owners\/o=[0-9a-f]{32}\/s=[0-9a-f]{32}\.json$/u.test(k)) refuse("generation_unreadable");
    const d = await readJson(store, k);
    if (!validOwner(d)) refuse("generation_unreadable");
    const o = d as { originId: string; source: string; sessionId: string; epoch: number; resets: number };
    origins.add(o.originId);
    if (o.resets < G && o.epoch === fenceEpoch(f, o.originId, sessionKeyHash(o.source, o.sessionId)) && !isDeleted(f, o.originId, o.source, o.sessionId)) ownersOld++;
  }
  if (ownersOld > 0) refuse("generation_owner_live");
  let sqlOrigins: readonly string[];
  try { sqlOrigins = await p.userOrigins(b); } catch { return refuse("generation_unreadable"); }
  if (!Array.isArray(sqlOrigins) || !sqlOrigins.every(str)) refuse("generation_unreadable");
  for (const o of sqlOrigins) origins.add(o);
  origins.add(LEGACY_EXPORT_ORIGIN_ID);
  const sorted = [...origins].sort();

  // log payloads below G, every stream.
  let payloads: number;
  try { payloads = await generationLogPayloadsUnproven(store, b.keyRoot, t, G); } catch { return refuse("generation_unreadable"); }
  if (payloads > 0) refuse("generation_payloads_remain");

  // rearm: every marker of the user carries the sweeper's done record for its exact bytes.
  const rearmRoot = `${keys.dir}/ledger/rearm/`;
  let rearmMarkers = 0;
  const all = await listAll(store, rearmRoot);
  const done = new Set(all.filter((k) => /\.done\/[0-9a-f]{32}\.json$/u.test(k)));
  for (const k of all) {
    const rest = k.slice(rearmRoot.length);
    if (/^o=[0-9a-f]{32}\/[0-9a-f]{32}\.(?:done|corrupt)\/[0-9a-f]{32}\.json$/u.test(rest)) continue;
    if (!/^o=[0-9a-f]{32}\/[0-9a-f]{32}\.json$/u.test(rest)) refuse("generation_unreadable");
    const raw = await readBytes(store, k);
    if (raw === null) continue;
    let doc: unknown;
    try { doc = JSON.parse(decoder.decode(raw)); } catch { return refuse("generation_rearm_pending"); }
    if (!validRearm(doc)) refuse("generation_rearm_pending");
    rearmMarkers++;
    const digest = createHash("sha256").update(raw).digest("hex").slice(0, 32);
    if (!done.has(rearmDoneKey(k, digest))) refuse("generation_rearm_pending");
  }

  // Publication generations and the admission inbox: EVERY machine under this workspace, never only the machines the
  // known origins derive (an origin with bytes but no owner stub and no SQL row would otherwise pass unseen). A
  // machine of this user (derived, or attributed `self`) holding anything refuses; another user's must be attributed
  // `other` by machineOwners; any other machine is unattributed and refuses. Any key that does not parse refuses.
  let owners: ReadonlyMap<string, "self" | "other">;
  try { owners = p.machineOwners ? await p.machineOwners(b) : new Map(); } catch { return refuse("generation_unreadable"); }
  if (!(owners instanceof Map)) refuse("generation_unreadable");
  const mine = new Set(sorted.map((o) => liveMachineId(t.workspaceId, o, t.userId)));
  for (const [m, who] of owners) if (who === "self") mine.add(m); else if (who !== "other" || mine.has(m)) refuse("generation_unreadable");
  const judge = (m: string) => { if (mine.has(m)) refuse("generation_bytes_remain"); if (owners.get(m) !== "other") refuse("generation_machine_unattributed"); };
  const ws = component(t.workspaceId);
  const genRoot = `${orgPrefix(b.keyRoot, t.orgId)}/upload-generations/`;
  const genRe = /^g=[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\/v1\/ws=([A-Za-z0-9._-]{1,128})\/m=([A-Za-z0-9._-]{1,128})\/[^/]/u;
  for (const k of await listAll(store, genRoot)) {
    const m = genRe.exec(k.slice(genRoot.length));
    if (!m) refuse("generation_unreadable");
    if (m![1] === ws) judge(m![2]!);
  }
  const inboxRoot = `${b.keyRoot}/${INBOX_NAMESPACE}/${t.orgId}/v1/ws=${ws}/`;
  const inboxRe = /^m=([A-Za-z0-9._-]{1,128})\/r=[0-9a-f]{64}\/(?:a=[0-9]{15}-[0-9a-f]{16}\.ndjson\.zst|outcome-(?:admitted|refused)\.json)$/u;
  // The S3 ledger's per-user admission directories share this root (async-admission.ts admissionUserDir: a pseudo
  // machine srv_<hash of ("admission-user-v1", ws, user)>). This user's is keys.dir, covered by the classes above;
  // any other has another (ws, user) by construction. Only its own object names parse.
  // An admission-directory name passes unjudged ONLY under this user's own computed admission machine (keys.dir), or
  // under a machine that is not one of this user's upload machines (another user's admission directory). Under one of
  // this user's upload machines it is judged like any byte (a ledger-shaped object there is not the ledger's).
  const adminRe = /^m=([A-Za-z0-9._-]{1,128})\/(?:upload-fences\.json|privacy-journal\.json|ledger\/[^\s]+)$/u;
  if (!keys.dir.startsWith(inboxRoot)) refuse("generation_unreadable");
  const ownAdmin = keys.dir.slice(inboxRoot.length);
  if (!/^m=[A-Za-z0-9._-]{1,128}$/u.test(ownAdmin) || mine.has(ownAdmin.slice(2))) refuse("generation_unreadable");
  for (const k of await listAll(store, inboxRoot)) {
    const rest = k.slice(inboxRoot.length);
    const m = inboxRe.exec(rest);
    if (m) { if (!k.endsWith(".json")) judge(m[1]!); continue; }
    const a = adminRe.exec(rest);
    if (!a) refuse("generation_unreadable");
    if (`m=${a![1]}` === ownAdmin) {
      // This user's own admission directory: EVERY object must be one the ledger writes, by exact name. A payload in a
      // malformed stream root (which the log-stream discovery would skip) or any other unknown object refuses.
      if (!OWN_ADMIN_OBJECTS.test(rest.slice(ownAdmin.length + 1))) refuse("generation_unreadable");
      continue;
    }
    // Not this user's admission directory: one of this user's upload machines holds bytes; any other machine must be
    // authoritatively attributed to another user (machineOwners, which covers admission pseudo-machines too). An
    // admission-shaped name under an unattributed machine is never taken as someone else's directory.
    judge(a![1]!);
  }

  const prefixes = userErasePrefixes(b.keyRoot, { requestId: b.requestId, orgId: t.orgId, workspaceId: t.workspaceId, userId: t.userId }, sqlOrigins);
  // The byte phase verified exactly these prefixes (its receipt's digest), at this destination.
  if (byteReceipt.prefixes !== prefixesDigest(prefixes)) refuse("generation_destination_mismatch");
  return {
    v: 1, kind: "user-erase-generation", epoch: b.epoch, request: h32("request", b.requestId), tenant: h32("tenant", t.orgId, t.workspaceId, t.userId),
    keyRoot: b.keyRoot, destination: sha("destination", destinationText(b.destination)), full: sha("full", b.full!.run, String(b.full!.startedMs), b.full!.evidenceSha256),
    fence: { instance: h32("fence", f.instance), generation: G, resetsCompleted: f.resetsCompleted },
    bytes: { sqlPrefixes: byteReceipt.prefixes, origins: sorted.length, uploadGenerations: 0, inbox: 0, payloads: 0, ownersOld: 0, rearmMarkers },
  };
}

/**
 * The comparable part of a receipt: everything but the production time. `resetsCompleted` and `rearmMarkers` may only
 * grow between a receipt and a later re-derivation (later resets, later fenced writes all swept); the rest is exact.
 */
function sameEvidence(a: GenerationEvidence, now: GenerationEvidence): boolean {
  const strip = (e: GenerationEvidence) => JSON.stringify({ v: e.v, kind: e.kind, epoch: e.epoch, request: e.request, tenant: e.tenant, keyRoot: e.keyRoot,
    destination: e.destination, full: e.full,
    fence: { instance: e.fence.instance, generation: e.fence.generation }, bytes: [e.bytes.sqlPrefixes, e.bytes.origins, e.bytes.uploadGenerations, e.bytes.inbox, e.bytes.payloads, e.bytes.ownersOld] });
  return strip(a) === strip(now) && now.fence.resetsCompleted >= a.fence.resetsCompleted && now.bytes.rearmMarkers >= a.bytes.rearmMarkers;
}

export function validGenerationReceipt(v: unknown): v is GenerationReceipt {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const r = v as Record<string, any>;
  const n = (x: unknown) => Number.isSafeInteger(x) && (x as number) >= 0;
  return Object.keys(r).length === 11 && r.v === 1 && r.kind === "user-erase-generation" && UUID.test(r.epoch) && /^h_[0-9a-f]{32}$/u.test(r.request)
    && /^h_[0-9a-f]{32}$/u.test(r.tenant) && str(r.keyRoot) && HEX64.test(r.destination) && HEX64.test(r.full) && r.fence && Object.keys(r.fence).length === 3
    && /^h_[0-9a-f]{32}$/u.test(r.fence.instance) && n(r.fence.generation) && r.fence.generation >= 1 && n(r.fence.resetsCompleted)
    && r.fence.resetsCompleted >= r.fence.generation && r.bytes && Object.keys(r.bytes).length === 7 && HEX64.test(r.bytes.sqlPrefixes) && n(r.bytes.origins)
    && r.bytes.uploadGenerations === 0 && r.bytes.inbox === 0 && r.bytes.payloads === 0 && r.bytes.ownersOld === 0 && n(r.bytes.rearmMarkers)
    && Number.isSafeInteger(r.producedAtMs) && r.producedAtMs > 0;
}

/**
 * Producer: derive the evidence and write the receipt create-only. A receipt already there (a retry, or a concurrent
 * replay) must be valid and match the fresh derivation (sameEvidence), else generation_receipt_mismatch.
 */
export async function produceGenerationReceipt(store: ObjectStore, p: GenerationPorts, b: GenerationBinding): Promise<GenerationReceipt> {
  const ev = await generationEvidence(store, p, b);
  const key = generationReceiptKey(b.keyRoot, b.tenant.orgId, b.epoch, b.requestId);
  const check = async (): Promise<GenerationReceipt> => {
    const got = await readJson(store, key);
    if (!validGenerationReceipt(got) || !sameEvidence(got, ev)) refuse("generation_receipt_mismatch");
    return got as GenerationReceipt;
  };
  if ((await readJson(store, key)) !== null) return check();
  const receipt: GenerationReceipt = { ...ev, producedAtMs: p.now() };
  try {
    await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(receipt)), { contentType: "application/json", ifNoneMatch: true });
  } catch (e) {
    if (isObjectStoreError(e, "precondition_failed") || isObjectStoreError(e, "already_exists")) return check();
    return refuse("generation_unreadable");
  }
  return receipt;
}

/**
 * Verifier (the bridge_complete gate): the receipt exists, is valid, and a fresh derivation still matches it; and the
 * pre-effect ordering still holds (admission fenced before R, after the install). null when proven, else the code.
 */
export async function verifyGeneration(store: ObjectStore, p: GenerationPorts, b: GenerationBinding): Promise<GenerationRefusal | null> {
  try {
    const pre = await generationPreEffect(store, p, b);
    if (pre.generation === null) return "generation_ordinal_missing";
    const got = await readJson(store, generationReceiptKey(b.keyRoot, b.tenant.orgId, b.epoch, b.requestId));
    if (got === null) return "generation_ordinal_missing";
    if (!validGenerationReceipt(got)) return "generation_receipt_mismatch";
    const now = await generationEvidence(store, p, b);
    if (now.fence.generation !== pre.generation || !sameEvidence(got, now)) return "generation_receipt_mismatch";
    return null;
  } catch (e) {
    if (e instanceof GenerationRefused) return e.code;
    return "generation_unreadable";
  }
}
