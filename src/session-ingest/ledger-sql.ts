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

/** A code that is safe to log: never a message. */
export function ledgerErrorCode(error: unknown): string {
  const e = error as { code?: unknown; name?: unknown } | null;
  const code = typeof e?.code === "string" ? e.code : undefined;
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
  if (typeof e?.name === "string" && /timeout/i.test(e.name)) return "ledger_timeout";
  return "ledger_error";
}

type LedgerFunction =
  | "ingest_claim_batch"
  | "ingest_commit_batch"
  | "ingest_acquire_slot"
  | "ingest_release_slot"
  | "ingest_set_session_uploads"
  | "ingest_reset_uploaded_sessions"
  | "ingest_fanout_fence"
  | "ingest_commit_revisions";

async function call(run: SqlRun, fn: LedgerFunction, input: unknown): Promise<unknown> {
  let rows: Record<string, unknown>[];
  try {
    ({ rows } = await run(`SELECT sessions.${fn}($1::jsonb) AS result`, [JSON.stringify(input)]));
  } catch (error) {
    throw new LedgerUnavailableError(ledgerErrorCode(error));
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

export function sqlLedger(run: SqlRun): Ledger {
  return {
    async claim(input: ClaimInput): Promise<ClaimResult> {
      return parseClaimResult((await call(run, "ingest_claim_batch", input)) as Record<string, unknown>);
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
      const r = (await call(run, "ingest_set_session_uploads", input)) as Record<string, unknown>;
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
    async resetUploadedSessions(tenant: LedgerTenant, requestId: string): Promise<number> {
      const r = (await call(run, "ingest_reset_uploaded_sessions", { tenant, requestId })) as Record<string, unknown>;
      if (typeof r.deleted !== "number" || !Number.isSafeInteger(r.deleted) || r.deleted < 0) throw new LedgerUnavailableError("ledger_bad_result");
      return r.deleted;
    },
  };
}
