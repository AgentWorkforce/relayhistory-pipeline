/**
 * Operator per-user erase of buffered desktop uploads (data-subject deletion), DEFAULT OFF.
 * SQL: supabase/migrations/20261105058010_operator_user_erase.sql. Runbook: docs/operator-user-erase.md.
 *
 *   1. begin     ledger row, the user's tokens held in this database revoked (auth_sessions revoked and kept,
 *                read_api_tokens deleted; Neon-held tokens are revoked out of band BEFORE this), the user's
 *                publication frontiers fenced, the user's device origins captured
 *   2. step*     restartable metadata purge: while slots remain each step deletes at most `stepLimit` of them; the
 *                last step is the final purge of every other per-user row in ONE transaction, sized by the user's
 *                batches/claims/stages and bounded only by USER_ERASE_STATEMENT_TIMEOUT_MS (a timeout rolls it
 *                back; the request stays purging and a rerun resumes). Waits out a live buffered-GC lease.
 *   3. bytes     every version and delete marker under each of the user's machine prefixes, for every captured
 *                origin: buffered batches, staged live batches, transcript objects and live pointers
 *   4. settle    wait, then a fresh strict listing of every prefix; anything found (a late in-flight PUT) is
 *                erased and the wait/verify repeats, up to `maxByteRounds`
 *   5. complete  only after every prefix verified empty; re-runs the "rows of the user remain" probe (a late row
 *                refuses with user_rows_remain_at_completion and nothing is reported verified), then the ledger
 *                drops the origins and keeps counts. Uploads of the user are refused (RH041) from begin until here.
 *                A seat naming the user that another user's slot still references is kept, counted in the ledger's
 *                `retained`, and refuses completion with user_seat_ambiguity until an operator repairs it.
 *
 * The machine id is per (workspace, origin, user) (`liveMachineId`), so a prefix never holds another user's
 * objects. The buffered GC owns buffered objects only while their stage rows exist: the bytes phase starts
 * after the metadata purge removed them, and every step refused to run while a GC lease was live.
 *
 * Logs and results carry counts and codes only, never an id or a key.
 */
import { createHash } from "node:crypto";
import type { EraseCheckpoint, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { liveBufferedPrefix, liveObjectPrefix, livePointerPrefix, liveStagedPrefix } from "../migration/neon-s3-erasure/live.ts";

export class UserEraseRefused extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "UserEraseRefused";
  }
}
const refuse = (code: string): never => {
  throw new UserEraseRefused(code);
};

export const CONFIRMATION = "ERASE ALL UPLOADS OF THIS USER";
// eslint-disable-next-line no-control-regex
const ID = /^[^\u0000-\u001f\u007f]{1,512}$/;

export interface UserEraseRequest {
  requestId: string;
  orgId: string;
  workspaceId: string;
  userId: string;
}

/** Exact keys; the confirmation phrase must match. Values are never echoed in a refusal. */
export function parseUserEraseRequest(text: string): UserEraseRequest {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return refuse("request_invalid");
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) refuse("request_invalid");
  const o = raw as Record<string, unknown>;
  const keys = ["request_id", "org_id", "workspace_id", "user_id", "confirmation"];
  if (Object.keys(o).sort().join() !== [...keys].sort().join()) refuse("request_invalid");
  if (o.confirmation !== CONFIRMATION) refuse("confirmation_required");
  for (const k of ["request_id", "org_id", "user_id"]) if (typeof o[k] !== "string" || !ID.test(o[k] as string)) refuse("request_invalid");
  if (typeof o.workspace_id !== "string" || (o.workspace_id !== "" && !ID.test(o.workspace_id))) refuse("request_invalid");
  return { requestId: o.request_id as string, orgId: o.org_id as string, workspaceId: o.workspace_id as string, userId: o.user_id as string };
}

/** One SQL call in its own read-committed transaction (a pg Client or a test double). */
export interface UserEraseDb {
  query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

interface LedgerView {
  state: "purging" | "metadata_purged" | "bytes_verified";
  origins: string[];
  tokens_revoked: number;
  deleted: Record<string, number>;
  /** Material naming the user that was kept, never counted as deleted (user_seats_referenced_by_other_slots). */
  retained?: Record<string, number>;
  done: boolean;
  phase?: string;
  retry_after_ms?: number;
}

/**
 * Per-transaction bounds, set with SET LOCAL inside every call's transaction (they end with it). Lock waits are on
 * the gate, the ledger row and the user's fence/frontier rows, whose other holders (another erase, the buffered
 * GC, a publication claim) are short transactions: past 30 s something is stuck and the call fails (55P03)
 * instead of queueing behind it. A statement is one begin, one step or the final purge; 15 min bounds the final
 * purge of a very large user (57014 rolls it back, the request stays purging, a rerun resumes).
 */
export const USER_ERASE_LOCK_TIMEOUT_MS = 30_000;
export const USER_ERASE_STATEMENT_TIMEOUT_MS = 15 * 60_000;

async function call(db: UserEraseDb, sql: string, params: unknown[]): Promise<LedgerView> {
  await db.query("BEGIN ISOLATION LEVEL READ COMMITTED");
  try {
    await db.query(`SET LOCAL lock_timeout = ${USER_ERASE_LOCK_TIMEOUT_MS}`);
    await db.query(`SET LOCAL statement_timeout = ${USER_ERASE_STATEMENT_TIMEOUT_MS}`);
    const r = await db.query(sql, params);
    await db.query("COMMIT");
    return r.rows[0]!.r as LedgerView;
  } catch (e) {
    await db.query("ROLLBACK").catch(() => {});
    throw e;
  }
}

export interface UserEraseOptions {
  keyRoot: string;
  eraser: PagedEraser;
  /** Slots per metadata step (1..20000). */
  stepLimit?: number;
  /** Upper bound on metadata steps in one run. */
  maxSteps?: number;
  /** Longest wait honoured for a buffered-GC lease (ms). */
  maxLeaseWaitMs?: number;
  /** Wait between the byte erase and its verification, so an in-flight PUT lands first (ms). */
  settleMs?: number;
  maxByteRounds?: number;
  sleep?: (ms: number) => Promise<void>;
  log?: (event: Record<string, unknown>) => void;
}

export interface UserEraseResult {
  state: LedgerView["state"];
  verified: boolean;
  tokensRevoked: number;
  deleted: Record<string, number>;
  steps: number;
  leaseWaits: number;
  prefixes: number;
  versionsDeleted: number;
  byteRounds: number;
}

/** Every prefix a captured origin of this user can have written to. */
export function userErasePrefixes(keyRoot: string, req: UserEraseRequest, origins: readonly string[]): string[] {
  const out: string[] = [];
  for (const origin_id of origins) {
    const g = { org_id: req.orgId, workspace_id: req.workspaceId, origin_id, user_id: req.userId };
    out.push(liveBufferedPrefix(keyRoot, g), liveStagedPrefix(keyRoot, g), liveObjectPrefix(keyRoot, g), livePointerPrefix(keyRoot, g));
  }
  for (const p of out) if (!/\/m=srv_[0-9a-f]{32}\/$/.test(p)) refuse("prefix_invalid");
  return [...new Set(out)];
}

const sleeper = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function runUserErase(db: UserEraseDb, req: UserEraseRequest, opts: UserEraseOptions): Promise<UserEraseResult> {
  const stepLimit = opts.stepLimit ?? 2000, maxSteps = opts.maxSteps ?? 100_000;
  const maxLeaseWaitMs = opts.maxLeaseWaitMs ?? 6 * 60_000, settleMs = opts.settleMs ?? 60_000, maxByteRounds = opts.maxByteRounds ?? 3;
  if (!Number.isInteger(stepLimit) || stepLimit < 1 || stepLimit > 20_000) refuse("config_invalid");
  const sleep = opts.sleep ?? sleeper, log = opts.log ?? (() => {});

  let view = await call(db, "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) AS r", [req.requestId, req.orgId, req.workspaceId, req.userId]);
  log({ event: "user_erase_begin", state: view.state, origins: view.origins.length, tokens_revoked: view.tokens_revoked });
  const origins = view.origins;
  let steps = 0, leaseWaits = 0;
  while (view.state === "purging") {
    if (steps >= maxSteps) refuse("step_budget_exhausted");
    view = await call(db, "SELECT sessions.operator_user_erase_step($1,$2) AS r", [req.requestId, stepLimit]);
    steps++;
    if (view.phase === "gc_lease_active") {
      const wait = Math.min(Math.max(view.retry_after_ms ?? 1000, 1000), maxLeaseWaitMs);
      leaseWaits++;
      log({ event: "user_erase_gc_lease_wait", wait_ms: wait });
      await sleep(wait);
    }
  }
  log({ event: "user_erase_metadata", state: view.state, steps });
  const base = { tokensRevoked: view.tokens_revoked, steps, leaseWaits };
  // Only a verified erase short-circuits. A rerun of a metadata_purged request runs the byte phase and complete's
  // recheck again, whatever an earlier run reported.
  if (view.state === "bytes_verified") {
    return { ...base, state: view.state, verified: true, deleted: view.deleted, prefixes: 0, versionsDeleted: 0, byteRounds: 0 };
  }

  const prefixes = userErasePrefixes(opts.keyRoot, req, origins);
  const scope = "operator-user-erase:" + createHash("sha256").update(JSON.stringify([req.requestId, req.orgId, req.workspaceId, req.userId])).digest("hex");
  // Each page re-checks that the ledger still says the metadata is purged.
  const authorize = async () => {
    const r = await db.query("SELECT state FROM sessions.operator_user_erasures WHERE request_id=$1", [req.requestId]);
    if (r.rows[0]?.state !== "metadata_purged") refuse("ledger_not_purged");
  };
  let versionsDeleted = 0, byteRounds = 0, verified = false;
  while (!verified && byteRounds < maxByteRounds) {
    byteRounds++;
    for (const prefix of prefixes) {
      let checkpoint: EraseCheckpoint | null = null;
      for (;;) {
        const res = await opts.eraser.eraseStep(prefix, { scope, checkpoint, authorize });
        versionsDeleted += res.deleted;
        if (res.status === "traversed") break;
        checkpoint = res.checkpoint;
      }
    }
    await sleep(settleMs);
    // A fresh, read-only, strict full pass per prefix (select deletes nothing): a prefix may exceed
    // verifyErased's single-invocation budget, and zero is never inferred from a partial listing.
    let remaining = 0;
    for (const prefix of prefixes) {
      let checkpoint: EraseCheckpoint | null = null;
      for (;;) {
        const pass = await opts.eraser.eraseStep(prefix, { scope: `${scope}:verify`, checkpoint, authorize, select: () => { remaining++; return false; } });
        if (pass.status === "traversed") break;
        checkpoint = pass.checkpoint;
      }
    }
    verified = remaining === 0;
    log({ event: "user_erase_bytes", round: byteRounds, prefixes: prefixes.length, versions_deleted: versionsDeleted, remaining });
  }
  if (verified) {
    try {
      view = await call(db, "SELECT sessions.operator_user_erase_complete($1) AS r", [req.requestId]);
    } catch (e) {
      // A row of the user landed after the metadata purge (a writer the ingest fence does not cover): the request
      // stays metadata_purged and is never reported verified. Only the code is surfaced, never the message.
      const err = e as { code?: unknown; message?: unknown };
      // A seat naming the user that another user's slot references: kept, and completion refuses until repaired
      // (docs/operator-user-erase.md). A distinct reason, so it is not retried as a late row.
      if (err?.code === "55000" && typeof err.message === "string" && err.message.includes("seats naming the user remain referenced by other slots")) {
        log({ event: "user_erase_complete_refused", code: "user_seat_ambiguity" });
        refuse("user_seat_ambiguity");
      }
      if (err?.code === "55000" && typeof err.message === "string" && err.message.includes("remain at completion")) {
        log({ event: "user_erase_complete_refused", code: "user_rows_remain_at_completion" });
        refuse("user_rows_remain_at_completion");
      }
      throw e;
    }
  }
  return { ...base, state: view.state, verified, deleted: view.deleted, prefixes: prefixes.length, versionsDeleted, byteRounds };
}
