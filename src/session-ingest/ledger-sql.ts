import {validatedCustodyAck} from "./custody-ack.ts";
import type { PrivacyAuthorityHead, SourceControlHead } from "./ledger.ts";
import type { CurrentCustodyPacket } from "./custody-protocol.ts";
import {DeliveryError} from "./vendor/delivery/delivery-contracts.ts";
import {orderedControl,orderedControlResult} from "./control-protocol.ts";
import type { IngestAcceptanceMode } from "./buffered-protocol.ts";
import { dbErrorDiagnosisFrom, dbErrorSnapshot, type DbErrorSnapshot } from "./db-error-class.ts";
import { acquisitionRefusalFrom } from "./acquisition-refusal.ts";
/**
 * The ledger over SQL: one call per claim and per commit, each to its
 * SECURITY DEFINER function (supabase/migrations/20261104022000_session_ingest_ledger.sql),
 * as the `bulls96_session_ingest` login (supabase/bootstrap/session_ingest_role.sql).
 *
 * Failures become `LedgerUnavailableError` with a code only (the SQLSTATE
 * class, or a fixed name): driver and database messages can quote parameters,
 * so they are never carried, logged or returned. A result that does not have
 * the shape the function promises is `ledger_bad_result`.
 */
import type { StagedAttempt, LedgerRecord, SelectionInput } from "./ledger.ts";
import type { BabysitterRetentionRegistration, BabysitterWriteOutcome } from "./ledger.ts";
import {
  LedgerUnavailableError,
  type ClaimInput,
  type ClaimResult,
  type CommitInput,
  type CommitResult,
  type Ledger,
  type LedgerTenant,
  type RevisionConflict,
  type SlotRequest,
  type SlotResult,
} from "./ledger.ts";

export type SqlRun = (text: string, params: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>;

const SQLSTATE = /^[0-9A-Z]{5}$/;
const HEX64 = /^[0-9a-f]{64}$/;

/** A code that is safe to log: never a message. Reads only the guarded snapshot, so it never throws. */
export function ledgerErrorCode(error: unknown): string {
  return ledgerErrorCodeFrom(dbErrorSnapshot(error));
}

export function ledgerErrorCodeFrom(s: DbErrorSnapshot): string {
  const code = s.code;
  if (code && SQLSTATE.test(code)) {
    if (code === "22023") return "ledger_input_refused";
    if (code === "42501") return "ledger_caller_refused";
    if (code === "P0002") return "ledger_no_claim";
    if (code === "57014") return "ledger_timeout";
    if (code === "55P03" || code === "40P01") return "ledger_lock_timeout";
    if (code.startsWith("08")) return "ledger_connection";
    if (code.startsWith("53")) return "ledger_resources";
    return `ledger_sqlstate_${code}`;
  }
  if (code && /^E[A-Z]+$/.test(code)) return "ledger_connection"; // ECONNRESET, ETIMEDOUT, ...
  if (s.name !== undefined && /timeout/i.test(s.name)) return "ledger_timeout";
  return "ledger_error";
}

type LedgerFunction =
  | "ingest_claim_buffered_batch"
  | "ingest_prepare_buffer_stage"
  | "ingest_commit_buffered_batch"
  | "ingest_claim_batch"
  | "ingest_commit_batch"
  | "ingest_acquire_slot"
  | "ingest_release_slot"
  | "ingest_set_session_uploads"
  | "ingest_selfhost_set_session_uploads"
  | "ingest_reset_uploaded_sessions"
  | "ingest_fanout_fence"
  | "ingest_commit_revisions"
  | "ingest_babysitter_retention_register"
  | "ingest_babysitter_retention_written"
  | "ingest_frozen_source_witness"
  | "ingest_source_control_begin"
  | "ingest_source_control_complete"
  | "ingest_privacy_authority_begin"
  | "ingest_privacy_authority_complete"
  | "ingest_frozen_source_receipt";

async function call(run: SqlRun, fn: LedgerFunction, input: unknown): Promise<unknown> {
  let rows: Record<string, unknown>[];
  try {
    ({ rows } = await run(`SELECT sessions.${fn}($1::jsonb) AS result`, [JSON.stringify(input)]));
  } catch (error) {
    // One guarded snapshot for every consumer: a throwing getter/proxy cannot escape this path.
    const s = dbErrorSnapshot(error);
    if (s.code === "P0W01") throw new DeliveryError("forbidden", 403, "Delivery record belongs to another user");
    // Acquisition owner refusal (claim, stage, commit): the typed 409 of design §5.6, never a 503.
    const refusal = acquisitionRefusalFrom(error);
    if (refusal) throw refusal;
    throw new LedgerUnavailableError(ledgerErrorCodeFrom(s), dbErrorDiagnosisFrom(s));
  }
  if (rows.length !== 1) throw new LedgerUnavailableError("ledger_bad_result");
  let result = rows[0]!.result;
  // pg parses jsonb; a driver that returns text is accepted too.
  if (typeof result === "string") {
    try {
      result = JSON.parse(result);
    } catch {
      throw new LedgerUnavailableError("ledger_bad_result");
    }
  }
  if (!isObject(result)) throw new LedgerUnavailableError("ledger_bad_result");
  return result;
}

function isObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function pairs(v: unknown): [string, string][] {
  if (!Array.isArray(v)) throw new LedgerUnavailableError("ledger_bad_result");
  return v.map((p) => {
    if (!Array.isArray(p) || p.length !== 2 || typeof p[0] !== "string" || typeof p[1] !== "string")
      throw new LedgerUnavailableError("ledger_bad_result");
    return [p[0], p[1]];
  });
}

/** Strictly increasing record indexes (`ord` order, as the function returns them). */
function indexes(v: unknown): number[] {
  if (!Array.isArray(v)) throw new LedgerUnavailableError("ledger_bad_result");
  let last = -1;
  for (const i of v) {
    if (typeof i !== "number" || !Number.isSafeInteger(i) || i <= last) throw new LedgerUnavailableError("ledger_bad_result");
    last = i;
  }
  return v as number[];
}

const revision = (v: unknown): number => {
  if (typeof v !== "number" || !Number.isSafeInteger(v) || v < 1) throw new LedgerUnavailableError("ledger_bad_result");
  return v;
};
const text = (v: unknown): string => {
  if (typeof v !== "string" || v === "") throw new LedgerUnavailableError("ledger_bad_result");
  return v;
};
const digest = (v: unknown): string => {
  if (typeof v !== "string" || !HEX64.test(v)) throw new LedgerUnavailableError("ledger_bad_result");
  return v;
};

function conflict(v: unknown): RevisionConflict {
  if (!isObject(v)) throw new LedgerUnavailableError("ledger_bad_result");
  return {
    recordId: text(v.recordId),
    submittedRevisionId: text(v.submittedRevisionId),
    submittedRevision: revision(v.submittedRevision),
    submittedDigest: digest(v.submittedDigest),
    currentRevisionId: text(v.currentRevisionId),
    currentRevision: revision(v.currentRevision),
    currentDigest: digest(v.currentDigest),
  };
}

export function parseClaimResult(r: Record<string, unknown>): ClaimResult {
  switch (r.kind) {
    case "baseline_pending": throw new LedgerUnavailableError("ledger_baseline_pending");
    case "claimed":
    case "resume":
    case "replay_fast":
      if (typeof r.resets !== "number" || !Number.isSafeInteger(r.resets) || r.resets < 0)
        throw new LedgerUnavailableError("ledger_bad_result");
      if (r.kind === "replay_fast") return { kind: r.kind, deleted: pairs(r.deleted), stale: indexes(r.stale), resets: r.resets };
      if (typeof r.claimToken !== "string" || !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(r.claimToken))
        throw new LedgerUnavailableError("ledger_bad_result");
      return { kind: r.kind, claimToken: r.claimToken, deleted: pairs(r.deleted), stale: indexes(r.stale), resets: r.resets };
    case "replay":
      if (typeof r.receiptId !== "string" || !/^rhr_[0-9a-f]{64}$/.test(r.receiptId))
        throw new LedgerUnavailableError("ledger_bad_result");
      return { kind: "replay", receiptId: r.receiptId };
    case "batch_conflict":
      return { kind: "batch_conflict", currentDigest: digest(r.currentDigest) };
    case "revision_conflict": {
      if (!Array.isArray(r.conflicts) || typeof r.count !== "number" || !Number.isSafeInteger(r.count))
        throw new LedgerUnavailableError("ledger_bad_result");
      const conflicts = r.conflicts.map(conflict);
      if (conflicts.length === 0 || conflicts.length > 100 || r.count < conflicts.length)
        throw new LedgerUnavailableError("ledger_bad_result");
      return { kind: "revision_conflict", conflicts, count: r.count };
    }
    default:
      throw new LedgerUnavailableError("ledger_bad_result");
  }
}

export function sqlLedger(run: SqlRun, options: { acceptanceMode?: IngestAcceptanceMode;expectedGeneration?:string;selfhostSelectionErase?:boolean } = {}): Ledger {
  if(options.acceptanceMode==="buffered"&&(typeof options.expectedGeneration!=="string"||!/^[a-f0-9]{64}$/.test(options.expectedGeneration)))throw new LedgerUnavailableError("ledger_generation_required");
  const buffered=(input:unknown)=>({...input as Record<string,unknown>,generation:options.expectedGeneration});
  const hex64 = (v: unknown) => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
  return {
    async frozenWitness(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_frozen_source_witness", input)) as Record<string, unknown>;
      if (!isObject(r.witness) || !hex64(r.sourceDigest) || Object.keys(r).length !== 2) throw new LedgerUnavailableError("ledger_bad_result");
      return r as unknown as CurrentCustodyPacket;
    },
    async frozenReceipt(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_frozen_source_receipt", input)) as Record<string, unknown>;
      if (r.stored === null) return null;
      const s = r.stored as Record<string, unknown> | undefined;
      if (!isObject(s) || !hex64(s.digest) || !isObject(s.receipt)) throw new LedgerUnavailableError("ledger_bad_result");
      return { digest: s.digest as string, receipt: s.receipt as Record<string, unknown> };
    },
    async sourceControlBegin(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_source_control_begin", input)) as Record<string, unknown>;
      if (!hex64(r.epoch) || typeof r.version !== "string" || !/^[1-9][0-9]{0,18}$/.test(r.version) || !hex64(r.digest)
        || (r.kind !== "selection" && r.kind !== "reset") || r.pending !== true || typeof r.apply !== "boolean")
        throw new LedgerUnavailableError("ledger_bad_result");
      return r as unknown as SourceControlHead;
    },
    async sourceControlComplete(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_source_control_complete", input)) as Record<string, unknown>;
      if (r.completed !== true || typeof r.superseded !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
      return { completed: true, superseded: r.superseded };
    },
    async privacyAuthorityBegin(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_privacy_authority_begin", input)) as Record<string, unknown>;
      if (!hex64(r.epoch) || typeof r.version !== "string" || !/^[1-9][0-9]{0,18}$/.test(r.version) || !hex64(r.digest)
        || (r.kind !== "selection" && r.kind !== "reset") || r.pending !== true || typeof r.apply !== "boolean"
        || !(r.deleted === null || (typeof r.deleted === "string" && /^(0|[1-9][0-9]{0,18})$/.test(r.deleted)))
        || (r.kind === "selection" && r.deleted !== null) || (r.kind === "reset" && r.deleted === null)
        || Object.keys(r).length !== 7)
        throw new LedgerUnavailableError("ledger_bad_result");
      return r as unknown as PrivacyAuthorityHead;
    },
    async privacyAuthorityComplete(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = (await call(run, "ingest_privacy_authority_complete", input)) as Record<string, unknown>;
      if (r.completed !== true || typeof r.superseded !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
      return { completed: true, superseded: r.superseded };
    },
    async applyOrderedControl(input) {
      if(options.acceptanceMode!=="buffered")throw new LedgerUnavailableError("ledger_mode_refused");
      const control=orderedControl(input.control);
      const fn=control.kind==="selection"?"ingest_set_session_uploads":"ingest_reset_uploaded_sessions";
      const r=await call(run,fn,buffered(input));
      try{return orderedControlResult(r,control)}catch{throw new LedgerUnavailableError("ledger_control_ack_refused")}
    },
    async claim(input: ClaimInput): Promise<ClaimResult> {
      const raw=(await call(run,options.acceptanceMode==="buffered"?"ingest_claim_buffered_batch":"ingest_claim_batch",options.acceptanceMode==="buffered"?buffered(input):input)) as Record<string,unknown>;
      const result=parseClaimResult(raw);
      if(raw.custodyAck!==undefined){try{result.custodyAck=validatedCustodyAck(raw.custodyAck,input.custody)}catch{throw new LedgerUnavailableError("ledger_custody_ack_refused")}}
      if(input.custody&&!result.custodyAck)throw new LedgerUnavailableError("ledger_custody_ack_missing");
      return result;
    },
    async prepareBufferStage(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = await call(run, "ingest_prepare_buffer_stage", buffered(input)) as Record<string,unknown>;
      if (typeof r.stageToken !== "string" || !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(r.stageToken) ||
          typeof r.expiresAt !== "string" || !Number.isFinite(Date.parse(r.expiresAt))) throw new LedgerUnavailableError("ledger_bad_result");
      return {stageToken:r.stageToken,expiresAt:r.expiresAt};
    },
    async commitBuffered(input) {
      if (options.acceptanceMode !== "buffered") throw new LedgerUnavailableError("ledger_mode_refused");
      const r = await call(run,"ingest_commit_buffered_batch",buffered(input)) as Record<string,unknown>;
      if (r.kind === "intent_invalid") throw new LedgerUnavailableError("ledger_stage_invalidated");
      if (r.kind !== "committed") throw new LedgerUnavailableError("ledger_bad_result");
      if(r.custodyAck!==undefined){try{return validatedCustodyAck(r.custodyAck)}catch{throw new LedgerUnavailableError("ledger_custody_ack_refused")}}
    },
    async commit(input: CommitInput): Promise<CommitResult> {
      const r = (await call(run, "ingest_commit_batch", input)) as Record<string, unknown>;
      if (typeof r.rearmed !== "number" || !Number.isSafeInteger(r.rearmed) || r.rearmed < 0 || typeof r.invalidated !== "boolean")
        throw new LedgerUnavailableError("ledger_bad_result");
      if (r.invalidated === true) throw new LedgerUnavailableError("ledger_attempt_superseded");
      return { rearmed: r.rearmed };
    },
    async acquireSlot(request: SlotRequest): Promise<SlotResult> {
      const r = (await call(run, "ingest_acquire_slot", {
        org_id: request.tenant.orgId,
        workspace_id: request.tenant.workspaceId,
        user_id: request.tenant.userId,
        holder: request.holder,
        max_per_user: request.maxPerUser,
        max_per_org: request.maxPerOrg,
        ttl_seconds: request.ttlSeconds,
      })) as Record<string, unknown>;
      if (r.ok === true) return { ok: true };
      const after = r.retry_after_seconds;
      if (r.ok !== false || typeof after !== "number" || !Number.isSafeInteger(after) || after < 2 || after > 10)
        throw new LedgerUnavailableError("ledger_bad_result");
      return { ok: false, retryAfterSeconds: after };
    },
    async releaseSlot(tenant: LedgerTenant, holder: string): Promise<void> {
      const r = (await call(run, "ingest_release_slot", { org_id: tenant.orgId, user_id: tenant.userId, holder })) as Record<string, unknown>;
      if (typeof r.released !== "boolean") throw new LedgerUnavailableError("ledger_bad_result");
    },
    async setSessionUploads(input: SelectionInput): Promise<void> {
      if (options.selfhostSelectionErase) {
        // The self-host erase front door: fence and tombstone every origin of the caller's session in one transaction. Only a disable is served.
        if (input.enabled) throw new DeliveryError("unsupported_by_backend", 400, "Re-enabling an erased session is not served by the self-host session ingest");
        const erased = (await call(run, "ingest_selfhost_set_session_uploads", input)) as Record<string, unknown>;
        if (typeof erased.updated !== "number") throw new LedgerUnavailableError("ledger_bad_result");
        return;
      }
      const r = (await call(run, "ingest_set_session_uploads", options.acceptanceMode==="buffered"?buffered(input):input)) as Record<string, unknown>;
      if (typeof r.updated !== "number") throw new LedgerUnavailableError("ledger_bad_result");
    },
    async fanoutFence(tenant: LedgerTenant, originId: string, sessions: [string, string][], attempt: StagedAttempt): Promise<[string, string][]> {
      const r = (await call(run, "ingest_fanout_fence", { tenant, originId, sessions, batchDigest: attempt.batchDigest, claimToken: attempt.claimToken })) as Record<string, unknown>;
      if (!Array.isArray(r.deleted) || !r.deleted.every((p) => Array.isArray(p) && p.length === 2 && typeof p[0] === "string" && typeof p[1] === "string"))
        throw new LedgerUnavailableError("ledger_bad_result");
      return r.deleted as [string, string][];
    },
    async commitRevisions(tenant: LedgerTenant, originId: string, machineId: string, records: LedgerRecord[], attempt: StagedAttempt) {
      if (records.length === 0) return { committed: 0, rearmed: 0 };
      const r = (await call(run, "ingest_commit_revisions", { tenant, originId, machineId, records, batchDigest: attempt.batchDigest, claimToken: attempt.claimToken })) as Record<string, unknown>;
      if (typeof r.committed !== "number" || typeof r.rearmed !== "number") throw new LedgerUnavailableError("ledger_bad_result");
      return { committed: r.committed, rearmed: r.rearmed };
    },
    async registerBabysitterRetention(input: BabysitterRetentionRegistration) {
      const r = (await call(run, "ingest_babysitter_retention_register", input)) as Record<string, unknown>;
      if (!Array.isArray(r.objects) || typeof r.reopened !== "number" || r.objects.length !== input.buckets.length)
        throw new LedgerUnavailableError("ledger_bad_result");
      const objects = r.objects.map((o: unknown) => {
        const x = o as Record<string, unknown>;
        if (!x || typeof x.ref !== "string" || typeof x.source !== "string" || (x.sessionId !== null && typeof x.sessionId !== "string") || typeof x.eraseAfterMs !== "number")
          throw new LedgerUnavailableError("ledger_bad_result");
        return { ref: x.ref, source: x.source, sessionId: x.sessionId as string | null, eraseAfterMs: x.eraseAfterMs };
      });
      return { objects, reopened: r.reopened };
    },
    async babysitterRetentionWritten(input: BabysitterWriteOutcome) {
      const r = (await call(run, "ingest_babysitter_retention_written", input)) as Record<string, unknown>;
      if (typeof r.updated !== "number") throw new LedgerUnavailableError("ledger_bad_result");
      return r.updated;
    },
    async resetUploadedSessions(tenant: LedgerTenant, requestId: string): Promise<number> {
      const r = (await call(run, "ingest_reset_uploaded_sessions", options.acceptanceMode==="buffered"?buffered({tenant,requestId}):{tenant,requestId})) as Record<string, unknown>;
      if (typeof r.deleted !== "number" || !Number.isSafeInteger(r.deleted) || r.deleted < 0) throw new LedgerUnavailableError("ledger_bad_result");
      return r.deleted;
    },
  };
}
