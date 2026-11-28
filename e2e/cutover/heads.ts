/**
 * The integration points of the 0005 per-org cutover that are NOT on main yet (the "release-path heads"). The
 * qualification harness (harness.ts) calls the cutover only through these adapters, so integrating a head is one
 * edit here, and the harness can tell what it actually exercised:
 *
 *   landed    the adapter calls the head's own entry point, at a commit on main. Only this counts toward QUALIFIED.
 *   standin   the adapter follows the head's documented contract with main's own primitives (the 062320/062340 SQL
 *             lifecycle functions, the ledger-s3 marker shape). It proves the order and the SQL gates, not the head.
 *   pending   no stand-in is possible without the head (its proof does not exist yet): the adapter REFUSES with the
 *             head's own fail-closed code, so the run stops where production would.
 *   simulated (--simulate-pending only) a pending adapter replaced by the S3 effect the bridge describes, without the
 *             proof the head adds. For timing rehearsals only; a simulated run is never QUALIFIED.
 *
 * Heads (as layered-sessions-pipeline describes them; interfaces from review/0005-org-wave-tooling-minors @ 1bce290b
 * docs/backlog-ops-workflow.md and src/session-ingest/hold-bridge.ts):
 *   - sessions-wave-tooling: held install / seal / bind modes (hold-install-execute, hold-seal-execute + _bind) and the
 *     marker / release modes (marker-execute, bridge-execute). Today: standin over the SQL functions.
 *   - sessions-erase-generation-proof: the user-erase generation boundary (hold-bridge userGenerationBoundary). Today:
 *     pending, refuses erasure_generation_unproven.
 *   - hard-delete-owner: the 062360 session erase target manifest. Today: pending, refuses session_manifest_missing.
 *
 * Synthetic ids only. Nothing here reads the environment or a network endpoint.
 */
import { createHash } from "node:crypto";
import { objectKey, readAll, isObjectStoreError, type ObjectStore } from "../../src/shared/vendor.ts";
import { orgFenceAuthorityKey, s3Ledger, s3LedgerObjects, s3LedgerKeys, validOrgFenceAuthority, isDeleted, type OrgFenceAuthorityDoc } from "../../src/session-ingest/ledger-s3.ts";
import type { LedgerTenant } from "../../src/session-ingest/ledger.ts";

export type Integration = "landed" | "standin" | "pending" | "simulated";

/** The head's fail-closed refusal: the run stops here, exactly as the bridge would. */
export class HeadPending extends Error {
  constructor(readonly code: string, readonly head: string) { super(`${head}: ${code}`); this.name = "HeadPending"; }
}

/** Owner-session SQL (the lifecycle functions are owner-only; each call runs alone, READ COMMITTED). */
export interface OwnerSql { query<T = Record<string, any>>(text: string, params?: unknown[]): Promise<{ rows: T[] }> }

export interface CutoverContext {
  sql: OwnerSql;
  store: ObjectStore;
  keyRoot: string;
  org: string;
}

/** One erasure the bridge must replay into the S3 ledger before bridge_complete. */
export type ReplayItem =
  | { kind: "user_erase"; requestId: string; tenant: LedgerTenant }
  | { kind: "session_erase"; tenant: LedgerTenant; originIds: string[]; source: string; sessionId: string }
  | { kind: "selection_deletion"; tenant: LedgerTenant; originId: string; source: string; sessionId: string };

export interface ReplayReceipt { kind: ReplayItem["kind"]; outcome: "replayed" | "mirrored" | "simulated"; hash: string }

export interface Adapter<A extends unknown[], R> { head: string; integration: Integration; todo: string; run: (...a: A) => Promise<R> }

export interface CutoverHeads {
  holdInstall: Adapter<[CutoverContext, string], { epoch: string }>;
  holdSealBind: Adapter<[CutoverContext, string, { run: string; startedMs: number; receiptSha256: string }], { fenceAtMs: number }>;
  markerExecute: Adapter<[CutoverContext, string, { catchupReceiptSha256: string; writtenAtMs: number }], { markerKey: string }>;
  userEraseReplay: Adapter<[CutoverContext, string, Extract<ReplayItem, { kind: "user_erase" }>], ReplayReceipt>;
  sessionEraseReplay: Adapter<[CutoverContext, string, Extract<ReplayItem, { kind: "session_erase" }>], ReplayReceipt>;
  /** A user's own selection deletion: mirrored to the S3 fence at write time (controlMirror on); verified, not replayed. */
  selectionReplay: Adapter<[CutoverContext, string, Extract<ReplayItem, { kind: "selection_deletion" }>], ReplayReceipt>;
  bridgeRelease: Adapter<[CutoverContext, string, { erasures: number; receipts: ReplayReceipt[]; evidenceSha256: string; detail: "erasure_window_empty" | null }], void>;
  /**
   * The S3-side hold (layered: the direct S3 writer stays fenced through COMMITTED bridge_complete, in-flight requests
   * included): the exact refusal codes the S3 admission path answers for a held org. An S3 refusal with any other code
   * is not a hold refusal. Pending today: no head defines one, so the set is empty and every S3 probe fails.
   */
  s3HoldRefusal: Adapter<[], ReadonlySet<string>>;
  /** A real publication of drained sessions (claim, publish, search rows) after release. Pending: only the gate exists. */
  publication: Adapter<[CutoverContext, { sessions: string[] }], { published: number }>;
}

const sha = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");
const hashOf = (...parts: string[]) => "h_" + sha(["cutover-qualification/v1", ...parts].join("\n")).slice(0, 32);

const WAVE = "sessions-wave-tooling";
const GEN = "sessions-erase-generation-proof";
const MANIFEST = "hard-delete-owner (062360 session erase manifest)";
const S3HOLD = "release-path head: S3 admission fenced through bridge_complete (owner to be named by Pipeline)";
const PUBLICATION = "search publication (publication job / buffered search feed)";

/** The default adapters: stand-ins where main has the primitives, refusals where only the head can prove it. */
export function defaultHeads(opts: { simulatePending?: boolean } = {}): CutoverHeads {
  const sim = opts.simulatePending === true;
  return {
    holdInstall: {
      head: WAVE, integration: "standin",
      todo: "replace with the wave-tooling hold-install-execute mode (its receipt binds the request id and epoch hash)",
      run: async (c, requestId) => {
        const r = await c.sql.query<{ e: string }>("SELECT sessions.ingest_org_admission_hold_install($1, $2::uuid)::text AS e", [c.org, requestId]);
        return { epoch: r.rows[0]!.e };
      },
    },
    holdSealBind: {
      head: WAVE, integration: "standin",
      todo: "replace with hold-seal-execute + _bind (seal evidence = sha256 of the FULL receipt's downloaded bytes; bind full_started_ms = FULL created_at)",
      run: async (c, epoch, full) => {
        await c.sql.query("SELECT sessions.ingest_org_admission_hold_seal($1, $2::uuid, $3)", [c.org, epoch, full.receiptSha256]);
        const r = await c.sql.query<{ f: string }>("SELECT floor(extract(epoch FROM sessions.ingest_org_erasure_history_bind($1, $2::uuid, $3, $4::bigint, $5)) * 1000)::bigint::text AS f",
          [c.org, epoch, full.run, full.startedMs, full.receiptSha256]);
        return { fenceAtMs: Number(r.rows[0]!.f) };
      },
    },
    markerExecute: {
      head: WAVE, integration: "standin",
      todo: "replace with marker-execute (attestations, publication identity, the erasure-class SHARED key around the PUT, adoption on retry)",
      run: async (c, epoch, m) => {
        // Step 3 of marker-execute: the point of no return, committed before the PUT.
        await c.sql.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1, $2::uuid, $3)", [c.org, epoch, m.catchupReceiptSha256]);
        const doc: OrgFenceAuthorityDoc = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on",
          rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: m.writtenAtMs, orgId: c.org };
        const key = orgFenceAuthorityKey(c.keyRoot, c.org);
        await c.store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(doc)), { contentType: "application/json", ifNoneMatch: true });
        const back = JSON.parse(new TextDecoder().decode(await readAll(await c.store.get(objectKey(key)))));
        if (!validOrgFenceAuthority(back) || back.orgId !== c.org) throw new HeadPending("marker_readback_invalid", WAVE);
        return { markerKey: key };
      },
    },
    userEraseReplay: {
      head: GEN, integration: sim ? "simulated" : "pending",
      todo: "call the generation-proof head: bytes_verified + userGenerationBoundary, then resetUploadedSessions + eraseLogPayloads with a create-only receipt",
      run: async (c, epoch, it) => {
        if (!sim) throw new HeadPending("erasure_generation_unproven", GEN);
        await s3Ledger(c.store, c.keyRoot, { acceptanceMode: "canonical" }).resetUploadedSessions(it.tenant, it.requestId);
        return { kind: it.kind, outcome: "simulated", hash: hashOf("user", epoch, it.requestId) };
      },
    },
    sessionEraseReplay: {
      head: MANIFEST, integration: sim ? "simulated" : "pending",
      todo: "read the 062360 manifest (every alias x owner x origin, turns included) and fence-delete every target; refuse session_coverage_partial",
      run: async (c, epoch, it) => {
        if (!sim) throw new HeadPending("session_manifest_missing", MANIFEST);
        // Simulated: the fixture's own knowledge of the session's origins stands in for the manifest (NOT a coverage proof).
        const led = s3Ledger(c.store, c.keyRoot, { acceptanceMode: "canonical" });
        for (const originId of it.originIds)
          await led.setSessionUploads({ tenant: it.tenant, originId, enabled: false, sessions: [[it.source, it.sessionId]] });
        return { kind: it.kind, outcome: "simulated", hash: hashOf("session", epoch, it.source, it.sessionId) };
      },
    },
    selectionReplay: {
      head: WAVE, integration: "standin",
      todo: "the bridge's erasure set must include upload_deletions (062340 counts it); today hold-bridge ERASURE_SET_SQL reads only operator user erases and session fences",
      run: async (c, epoch, it) => {
        const io = s3LedgerObjects(c.store, c.keyRoot);
        let read;
        try { read = await io.readFence(s3LedgerKeys(c.keyRoot, it.tenant).fence); } catch (e) { if (isObjectStoreError(e)) throw new HeadPending("selection_fence_unreadable", WAVE); throw e; }
        if (read.state !== "present" || !isDeleted(read.fence, it.originId, it.source, it.sessionId)) throw new HeadPending("selection_not_mirrored", WAVE);
        return { kind: it.kind, outcome: "mirrored", hash: hashOf("selection", epoch, it.originId, it.source, it.sessionId) };
      },
    },
    s3HoldRefusal: {
      head: S3HOLD, integration: "pending",
      todo: "the S3 admission path must refuse a held org (held, sealed, marker_attempted) through committed bridge_complete, with a fixed code and no durable write; name the code here",
      run: async () => new Set<string>(),
    },
    publication: {
      head: PUBLICATION, integration: "pending",
      todo: "drive the real publication (ingest_publication_claim/publish or the buffered search feed) for drained sessions and read their search rows",
      run: async () => { throw new HeadPending("publication_not_exercised", PUBLICATION); },
    },
    bridgeRelease: {
      head: WAVE, integration: "standin",
      todo: "replace with bridge-execute (re-read the set, every item receipted for this epoch, _release bridge_complete with the settled count)",
      run: async (c, epoch, r) => {
        await c.sql.query("SELECT sessions.ingest_org_admission_hold_release($1, $2::uuid, 'bridge_complete', $3, $5, $4::bigint)", [c.org, epoch, r.evidenceSha256, r.erasures, r.detail]);
      },
    },
  };
}

export const integrationSummary = (h: CutoverHeads) =>
  Object.fromEntries(Object.entries(h).map(([k, a]) => [k, { head: a.head, integration: a.integration, todo: a.integration === "landed" ? null : a.todo }]));
export const allLanded = (h: CutoverHeads) => Object.values(h).every((a) => a.integration === "landed");
export { sha as sha256Hex };
