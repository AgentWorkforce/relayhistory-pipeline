/**
 * Bounded, resumable job envelope of the P1 frozen-source copy (scripts/frozen-source-copy.ts is the thin driver).
 *
 * Durable cursor. The ONLY cursor is an owner-only progress table in the TARGET Supabase, PROGRESS_TABLE, whose DDL
 * (PROGRESS_TABLE_DDL) is a proposed schema-release migration for a follow-up slot; this module never creates it.
 * When it is absent (or not owner-only) the operator refuses: frozen_copy_progress_table_missing / _unsafe. There is
 * no local-file, CI-artifact, log or receipt cursor, and no fallback.
 *
 * One page = one target transaction: the page INSERT (ON CONFLICT DO NOTHING) and its checkpoint row (binding, raw
 * keyset cursor, page_seq + 1, cumulative rows, status) commit together or not at all, and the checkpoint only
 * advances from exactly the previous page_seq under the same binding (otherwise frozen_copy_progress_conflict).
 * The in-memory cursor moves only after COMMIT returns. So the checkpoint row IS the readback for an in-doubt commit:
 * a page whose COMMIT answer was lost either committed together with its checkpoint (resume continues after it) or
 * did not (resume re-reads it). Nothing is ever skipped and nothing is blindly retried in-process.
 *
 * Binding = generation, source identity, control epoch, privacy baseline (the live target policy, equal to the Neon
 * cutover descriptor) and the operator SHA (sha256 of the operator's source files). A resume under any other binding
 * refuses; there is no delete/truncate/reset recovery path in the operator. Exclusive ownership is a session-level
 * try-lock on COPY_OWNERSHIP_LOCK on the one owner connection (copy and seal both take it).
 *
 * Raw tenant/key values live only in the progress table. The receipt is sanitized: counts, hashes, timestamps, codes.
 */
import { createHash } from "node:crypto";
import {
  FrozenCopyRefusal, FrozenCopyUnknownCommit, hasSqlState, policyRefusal, type CutoverStatus, type InDoubt, type Queryable,
  type SealResult,
} from "./frozen-source-copy-seal.ts";

const refuse = (code: string, detail?: Record<string, string>): never => {
  throw new FrozenCopyRefusal(code, detail);
};
const sha = (t: string) => createHash("sha256").update(t).digest("hex");

// ---------------------------------------------------------------------------------------------------------------
// 1. Progress table contract
// ---------------------------------------------------------------------------------------------------------------

export const PROGRESS_TABLE = "sessions.__96bulls_frozen_copy_progress";
export const PROGRESS_SCHEMA_VERSION = 1;
export type Dataset = "records" | "receipts";
export const DATASETS: readonly Dataset[] = ["records", "receipts"];

/** Proposed schema-release migration (NOT applied by this PR or by the operator; tests execute it in PGlite only). */
export const PROGRESS_TABLE_DDL = `CREATE TABLE sessions.__96bulls_frozen_copy_progress(
 org_id text NOT NULL,
 dataset text NOT NULL CHECK(dataset IN('records','receipts')),
 binding_sha256 text NOT NULL CHECK(binding_sha256~'^[0-9a-f]{64}$'),
 generation text NOT NULL CHECK(generation~'^[0-9a-f]{64}$'),
 source_identity_sha256 text NOT NULL CHECK(source_identity_sha256~'^[0-9a-f]{64}$'),
 control_epoch text NOT NULL CHECK(control_epoch~'^[0-9a-f]{64}$'),
 privacy_baseline_sha256 text NOT NULL CHECK(privacy_baseline_sha256~'^[0-9a-f]{64}$'),
 operator_sha256 text NOT NULL CHECK(operator_sha256~'^[0-9a-f]{64}$'),
 cursor jsonb NOT NULL CHECK(jsonb_typeof(cursor)='array'),
 page_seq bigint NOT NULL CHECK(page_seq>=1),
 rows_copied bigint NOT NULL CHECK(rows_copied>=0),
 status text NOT NULL CHECK(status IN('in_progress','complete')),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,dataset)
);
ALTER TABLE sessions.__96bulls_frozen_copy_progress ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.__96bulls_frozen_copy_progress
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;`;

export const PROGRESS_COLUMNS = [
  "binding_sha256", "control_epoch", "cursor", "dataset", "generation", "operator_sha256", "org_id", "page_seq", "privacy_baseline_sha256", "rows_copied",
  "source_identity_sha256", "status", "updated_at",
] as const;

const PROGRESS_SHAPE = `SELECT c.relrowsecurity AS rls, pg_get_userbyid(c.relowner)=current_user AS owned,
 (SELECT count(*) FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE a.grantee<>c.relowner)::int AS foreign_grants,
 (SELECT array_agg(a.attname::text ORDER BY a.attname::text) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS cols
 FROM pg_class c WHERE c.oid=to_regclass('${PROGRESS_TABLE}')`;

/** The table must exist, be owned by this (owner) session, have RLS on, grant nothing to anyone else, and have the contract's columns. */
export async function requireProgressTable(c: Queryable): Promise<void> {
  const row = (await c.query(PROGRESS_SHAPE)).rows[0] as { rls: boolean; owned: boolean; foreign_grants: number; cols: string[] } | undefined;
  if (!row) refuse("frozen_copy_progress_table_missing", { table: PROGRESS_TABLE });
  const cols = [...(row!.cols ?? [])].sort();
  if (row!.rls !== true || row!.owned !== true || row!.foreign_grants !== 0 || cols.join(",") !== [...PROGRESS_COLUMNS].sort().join(","))
    refuse("frozen_copy_progress_table_unsafe", { table: PROGRESS_TABLE });
}

// ---------------------------------------------------------------------------------------------------------------
// 2. Binding, exclusive ownership, resume proof
// ---------------------------------------------------------------------------------------------------------------

export interface CopyBinding {
  generation: string;
  sourceIdentitySha256: string;
  controlEpoch: string;
  privacyBaselineSha256: string;
  operatorSha256: string;
}
export const bindingSha = (b: CopyBinding) =>
  sha(JSON.stringify(["frozen-copy-binding-v1", b.generation, b.sourceIdentitySha256, b.controlEpoch, b.privacyBaselineSha256, b.operatorSha256]));

/** sha256 over the operator's own source files (name + bytes), in the given order. */
export function operatorSha256(files: Array<{ name: string; bytes: string | Uint8Array }>): string {
  const h = createHash("sha256");
  for (const f of files) h.update(`${f.name}\n${typeof f.bytes === "string" ? Buffer.byteLength(f.bytes) : f.bytes.length}\n`).update(f.bytes);
  return h.digest("hex");
}

const BINDING_POLICY = "SELECT generation,source_identity_sha256,control_epoch,privacy_baseline_sha256 FROM sessions.ingest_lazy_policy WHERE singleton";
/** The binding of this run: the live target policy, which must equal the frozen Neon descriptor (epoch, privacy baseline). */
export async function copyBinding(target: Queryable, status: CutoverStatus, operator: string): Promise<CopyBinding> {
  const p = (await target.query(BINDING_POLICY)).rows[0] as
    | { generation: string; source_identity_sha256: string; control_epoch: string; privacy_baseline_sha256: string }
    | undefined;
  const bad = policyRefusal(p, status);
  if (bad) refuse(bad);
  return { generation: p!.generation, sourceIdentitySha256: p!.source_identity_sha256, controlEpoch: p!.control_epoch, privacyBaselineSha256: p!.privacy_baseline_sha256, operatorSha256: operator };
}

/** Session-level exclusive key for "one copy/seal operator at a time" (distinct from the authority keys 96/97). */
export const COPY_OWNERSHIP_LOCK = [1919249529, 98] as const;
export async function takeCopyOwnership(c: Queryable): Promise<void> {
  const ok = (await c.query("SELECT pg_try_advisory_lock($1,$2) AS ok", [...COPY_OWNERSHIP_LOCK])).rows[0]?.ok;
  if (ok !== true) refuse("frozen_copy_operator_busy");
}

/** Every existing checkpoint must carry this run's exact binding (resume re-proof). Returns the number of checkpoints. */
export async function requireResumeBinding(c: Queryable, b: CopyBinding): Promise<number> {
  const rows = (await c.query(`SELECT binding_sha256,generation,source_identity_sha256,control_epoch,privacy_baseline_sha256,operator_sha256,count(*)::int AS n
    FROM ${PROGRESS_TABLE} GROUP BY 1,2,3,4,5,6`)).rows as Array<Record<string, string> & { n: number }>;
  const want = bindingSha(b);
  let n = 0;
  for (const r of rows) {
    const fieldsMatch = r.generation === b.generation && r.source_identity_sha256 === b.sourceIdentitySha256 && r.control_epoch === b.controlEpoch && r.privacy_baseline_sha256 === b.privacyBaselineSha256;
    if (fieldsMatch && r.operator_sha256 !== b.operatorSha256) refuse("frozen_copy_resume_operator_mismatch");
    if (!fieldsMatch || r.binding_sha256 !== want) refuse("frozen_copy_resume_binding_mismatch");
    n += r.n;
  }
  return n;
}

// ---------------------------------------------------------------------------------------------------------------
// 3. Checkpoints: one page and its checkpoint per target transaction
// ---------------------------------------------------------------------------------------------------------------

export interface Checkpoint {
  cursor: string[];
  pageSeq: number;
  rows: number;
  status: "in_progress" | "complete";
  updatedAt: string | null;
}

export async function loadCheckpoint(c: Queryable, org: string, dataset: Dataset): Promise<Checkpoint | null> {
  const r = (await c.query(`SELECT cursor,page_seq::text AS seq,rows_copied::text AS rows,status,updated_at::text AS at FROM ${PROGRESS_TABLE} WHERE org_id=$1 AND dataset=$2`, [org, dataset])).rows[0];
  if (!r) return null;
  const cursor = typeof r.cursor === "string" ? JSON.parse(r.cursor) : r.cursor;
  return { cursor: cursor as string[], pageSeq: Number(r.seq), rows: Number(r.rows), status: r.status, updatedAt: r.at ?? null };
}

const CHECKPOINT = `INSERT INTO ${PROGRESS_TABLE} AS p(org_id,dataset,binding_sha256,generation,source_identity_sha256,control_epoch,privacy_baseline_sha256,operator_sha256,cursor,page_seq,rows_copied,status)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12)
 ON CONFLICT (org_id,dataset) DO UPDATE SET cursor=EXCLUDED.cursor,page_seq=EXCLUDED.page_seq,rows_copied=EXCLUDED.rows_copied,status=EXCLUDED.status,updated_at=clock_timestamp()
 WHERE p.page_seq=EXCLUDED.page_seq-1 AND p.binding_sha256=EXCLUDED.binding_sha256 AND p.status='in_progress'
 RETURNING page_seq::text AS seq,updated_at::text AS at`;

/**
 * BEGIN; insert(); checkpoint (page_seq = from + 1, only from exactly `from`); COMMIT. Before COMMIT is sent, any
 * failure rolls back and nothing committed (frozen_copy_page_not_committed unless the server gave a SQLSTATE). A COMMIT
 * answered with a SQLSTATE did not commit. A COMMIT without an answer is in doubt: FrozenCopyUnknownCommit (no retry).
 */
export async function commitPage(
  target: Queryable,
  p: { binding: CopyBinding; org: string; dataset: Dataset; from: Checkpoint | null; cursor: string[]; rows: number; last: boolean },
  insert: () => Promise<unknown>,
  now: () => number = Date.now,
): Promise<Checkpoint> {
  const seq = (p.from?.pageSeq ?? 0) + 1, total = (p.from?.rows ?? 0) + p.rows, status = p.last ? "complete" : "in_progress";
  if (p.from?.status === "complete") refuse("frozen_copy_progress_conflict", { dataset: p.dataset });
  let at: string | null = null;
  await target.query("BEGIN");
  try {
    if (p.rows > 0) await insert();
    const b = p.binding;
    const r = await target.query(CHECKPOINT, [p.org, p.dataset, bindingSha(b), b.generation, b.sourceIdentitySha256, b.controlEpoch, b.privacyBaselineSha256, b.operatorSha256, JSON.stringify(p.cursor), seq, total, status]);
    if (r.rows.length !== 1 || Number(r.rows[0].seq) !== seq) refuse("frozen_copy_progress_conflict", { dataset: p.dataset, checkpointSeq: String(seq) });
    at = r.rows[0].at ?? null;
  } catch (error) {
    await target.query("ROLLBACK").catch(() => {});
    if (error instanceof FrozenCopyRefusal || hasSqlState(error)) throw error;
    throw new FrozenCopyRefusal("frozen_copy_page_not_committed", { dataset: p.dataset, checkpointSeq: String(seq) });
  }
  try {
    await target.query("COMMIT");
  } catch (error) {
    if (hasSqlState(error)) {
      await target.query("ROLLBACK").catch(() => {});
      throw error;
    }
    throw new FrozenCopyUnknownCommit({ phase: "copy", dataset: p.dataset, checkpointSeq: seq, org: p.org, detectedAt: now() });
  }
  return { cursor: p.cursor, pageSeq: seq, rows: total, status, updatedAt: at };
}

export type Reconciliation = "NOT_REQUIRED" | "PENDING" | "COMMITTED" | "NOT_COMMITTED" | "REFUSED";
/**
 * Readback of an in-doubt COMMIT from the target itself. copy: the checkpoint row reached `checkpointSeq` (COMMITTED)
 * or not (NOT_COMMITTED). seal: the completeness row carries this manifest (COMMITTED), is absent (NOT_COMMITTED) or
 * carries another manifest (REFUSED). A failed readback stays PENDING; the next run's resume is then the readback.
 * An in-doubt autocommit privacy insert (checkpointSeq null) stays PENDING: the next --copy re-runs the idempotent
 * insert after reading the target, and --seal refuses unless the privacy rows equal Neon's.
 */
export async function reconcileInDoubt(c: Queryable, d: InDoubt): Promise<Reconciliation> {
  // An autocommit privacy insert (no checkpoint): nothing to read back here; the seal's privacy parity proves it.
  if (d.phase === "copy" && d.checkpointSeq === null) return "PENDING";
  try {
    if (d.phase === "copy") {
      const r = (await c.query(`SELECT page_seq::text AS seq FROM ${PROGRESS_TABLE} WHERE org_id=$1 AND dataset=$2`, [d.org, d.dataset])).rows[0];
      return r && Number(r.seq) >= (d.checkpointSeq ?? Infinity) ? "COMMITTED" : "NOT_COMMITTED";
    }
    const r = (await c.query("SELECT manifest_sha256 FROM sessions.ingest_frozen_source_completeness WHERE singleton")).rows[0];
    return !r ? "NOT_COMMITTED" : r.manifest_sha256 === d.manifestSha256 ? "COMMITTED" : "REFUSED";
  } catch {
    return "PENDING";
  }
}

/** Seal precondition: every org has a COMPLETE checkpoint for every dataset under exactly this binding. */
export async function requireCopyComplete(c: Queryable, b: CopyBinding, orgs: string[]): Promise<void> {
  const rows = (await c.query(`SELECT org_id,dataset FROM ${PROGRESS_TABLE} WHERE binding_sha256=$1 AND status='complete'`, [bindingSha(b)])).rows;
  const done = new Set(rows.map((r) => JSON.stringify([r.org_id, r.dataset])));
  const missing = orgs.flatMap((o) => DATASETS.filter((d) => !done.has(JSON.stringify([o, d])))).length;
  if (missing) refuse("frozen_copy_progress_incomplete", { missingCheckpoints: String(missing) });
}

/** Serializes target transactions on the ONE owner connection (concurrent source readers share it). */
export function serialQueue() {
  let tail: Promise<unknown> = Promise.resolve();
  return <T>(fn: () => Promise<T>): Promise<T> => {
    const run = tail.then(fn, fn);
    tail = run.then(() => {}, () => {});
    return run;
  };
}

export interface CopyDatasetDeps {
  target: Queryable;
  binding: CopyBinding;
  org: string;
  dataset: Dataset;
  pageSize: number;
  /** Next source page strictly after `cursor` (keyset), at most `limit` rows. */
  readPage(cursor: string[], limit: number): Promise<Record<string, unknown>[]>;
  keyOf(row: Record<string, unknown>): string[];
  /** Runs inside the page transaction on `target`. */
  insert(rows: Record<string, unknown>[]): Promise<unknown>;
  /** Budget + abort; called before every read and before every write. */
  check(): void;
  serialize: <T>(fn: () => Promise<T>) => Promise<T>;
  progress?: ProgressTracker;
}

/** Copies one (org, dataset) from its checkpoint to the end. Returns the final checkpoint. */
export async function copyDataset(d: CopyDatasetDeps): Promise<Checkpoint> {
  let cp = await d.serialize(() => loadCheckpoint(d.target, d.org, d.dataset));
  if (cp?.status === "complete") return cp;
  if (cp) d.progress?.resumed();
  let cursor = cp?.cursor ?? ["", "", ""];
  for (;;) {
    d.check();
    const rows = await d.readPage(cursor, d.pageSize);
    d.check();
    const last = rows.length < d.pageSize;
    const next = rows.length ? d.keyOf(rows[rows.length - 1]!) : cursor;
    const from: Checkpoint | null = cp;
    d.progress?.begin(d.dataset, (from?.pageSeq ?? 0) + 1);
    try {
      // Re-check inside the queue: a page that waited behind another transaction must not START after an abort/deadline.
      cp = await d.serialize(() => (d.check(), commitPage(d.target, { binding: d.binding, org: d.org, dataset: d.dataset, from, cursor: next, rows: rows.length, last }, () => d.insert(rows))));
    } finally {
      d.progress?.end();
    }
    d.progress?.page(d.dataset, cp, rows.length);
    if (last) return cp;
    cursor = next;
  }
}

// ---------------------------------------------------------------------------------------------------------------
// 4. Abort, progress tracking and the sanitized receipt
// ---------------------------------------------------------------------------------------------------------------

/** Operator abort (SIGINT/SIGTERM): stop before the next read/write; open transactions roll back; receipt ABORTED. */
export function abortController(now: () => number = Date.now) {
  const s = { requested: false, observed: false, reason: null as string | null, requestedAt: null as number | null };
  return {
    state: s,
    request(reason: string) {
      if (s.requested) return;
      s.requested = true;
      s.reason = reason;
      s.requestedAt = now();
    },
    check() {
      if (!s.requested) return;
      s.observed = true;
      refuse("frozen_copy_aborted", { reason: s.reason ?? "abort" });
    },
  };
}

export type ProgressTracker = ReturnType<typeof progressTracker>;
export function progressTracker(totalOrgs: number) {
  const s = {
    bindingSha: null as string | null,
    totalOrgs,
    completedOrgs: 0,
    dataset: null as Dataset | null,
    pages: 0,
    rows: 0,
    resumeCount: 0,
    lastCheckpointAt: null as string | null,
    cursorSha256: null as string | null,
    inFlight: 0,
  };
  return {
    state: s,
    bind(b: CopyBinding) {
      s.bindingSha = bindingSha(b);
    },
    resumed() {
      s.resumeCount++;
    },
    begin(dataset: Dataset, _seq: number) {
      s.dataset = dataset;
      s.inFlight++;
    },
    end() {
      s.inFlight--;
    },
    page(dataset: Dataset, cp: Checkpoint, rows: number) {
      s.dataset = dataset;
      s.pages++;
      s.rows += rows;
      s.lastCheckpointAt = cp.updatedAt;
      s.cursorSha256 = sha(JSON.stringify(cp.cursor));
    },
    orgDone() {
      s.completedOrgs++;
    },
  };
}

export type JobOutcome = "pass" | "refused" | "aborted" | "unknown-commit";
export type JobStatus = "COPIED" | "SEALED" | "REFUSED" | "ABORTED" | "UNKNOWN_COMMIT";

export function classifyJobOutcome(error: unknown): { outcome: JobOutcome; code: string | null } {
  if (error === undefined) return { outcome: "pass", code: null };
  if (error instanceof FrozenCopyUnknownCommit) return { outcome: "unknown-commit", code: error.code };
  if (error instanceof FrozenCopyRefusal) return { outcome: error.code === "frozen_copy_aborted" ? "aborted" : "refused", code: error.code };
  const sqlstate = hasSqlState(error) ? (error as { code: string }).code : null;
  return { outcome: "refused", code: sqlstate ? `frozen_copy_sql_${sqlstate}` : "frozen_copy_job_failed" };
}

export interface JobEnvelope {
  startedAt: number;
  finishedAt: number;
  budget: { maxWallMs: number; deadline: number; pageSizes: { records: number; receipts: number }; concurrency: number; maxPageMs: number; statementTimeoutMs: number; lockTimeoutMs: number };
  progress: ProgressTracker["state"];
  abort: ReturnType<typeof abortController>["state"] & { completedAt: number | null; sourceClosed: boolean; targetClosed: boolean; writesStopped: boolean };
  reconciliation: { result: Reconciliation; readbackAt: number | null };
}

const iso = (t: number | null) => (t === null ? null : new Date(t).toISOString());

const NEXT: Record<JobStatus, Record<"copy" | "seal", string>> = {
  COPIED: { copy: "run --seal", seal: "run --seal" },
  SEALED: { copy: "none", seal: "none; the completeness seal is committed" },
  REFUSED: {
    copy: "fix the refusal, then rerun --copy (resumes from the owner-only checkpoints; never delete them)",
    seal: "no seal row was written; fix the refusal, then rerun --seal",
  },
  ABORTED: {
    copy: "rerun --copy when ready (resumes from the owner-only checkpoints; nothing in flight was committed without its checkpoint)",
    seal: "no seal row was written; rerun --seal when ready",
  },
  UNKNOWN_COMMIT: {
    copy: "do not retry blindly: rerun --copy, whose start-up reads the checkpoint row (the readback) and resumes after a committed page or re-reads an uncommitted one",
    seal: "do not retry blindly: read sessions.ingest_frozen_source_completeness; this run's manifest means sealed (rerun --seal confirms with inserted=false), no row means rerun --seal",
  },
};

export interface JobReceipt {
  receipt: "frozen-source-copy-job-v2";
  mode: "copy" | "seal";
  outcome: JobOutcome;
  status: JobStatus;
  code: string | null;
  startedAt: string;
  finishedAt: string;
  progress: { schemaVersion: number; stateTable: string; bindingSha: string | null; totalOrgs: number; completedOrgs: number; dataset: Dataset | null; pages: number; rows: number; resumeCount: number; lastCheckpointAt: string | null; cursorSha256: string | null };
  budget: { maxWallMs: number; elapsedWallMs: number; deadline: string; pageSizes: { records: number; receipts: number }; concurrency: number; maxPageMs: number; statementTimeoutMs: number; lockTimeoutMs: number; exceeded: boolean };
  abort: { requested: boolean; observed: boolean; boundedReason: string | null; requestedAt: string | null; completedAt: string | null; inFlightResolved: boolean; sourceClosed: boolean; targetClosed: boolean; writesStopped: boolean };
  unknownCommit: { detected: boolean; phase: "copy" | "seal" | null; dataset: Dataset | null; checkpointSeq: number | null; detectedAt: string | null; reconciliation: Reconciliation; readbackAt: string | null; resolvedAt: string | null; noBlindRetry: true };
  seal: SealResult | null;
  next: string;
}

/** The one sanitized receipt: no credential, no org id, no raw key (cursor only as sha256). */
export function jobReceipt(mode: "copy" | "seal", error: unknown, env: JobEnvelope, seal: SealResult | null = null): JobReceipt {
  const { outcome, code } = classifyJobOutcome(error);
  const doubt = error instanceof FrozenCopyUnknownCommit ? error.context : null;
  const status: JobStatus =
    outcome === "unknown-commit" ? "UNKNOWN_COMMIT"
    : outcome === "aborted" ? "ABORTED"
    : outcome === "refused" ? "REFUSED"
    : mode === "seal" ? (seal?.manifestSha256 ? "SEALED" : "REFUSED")
    : "COPIED";
  const reconciliation: Reconciliation = doubt ? env.reconciliation.result : "NOT_REQUIRED";
  const p = env.progress;
  return {
    receipt: "frozen-source-copy-job-v2",
    mode,
    outcome: status === "REFUSED" && outcome === "pass" ? "refused" : outcome,
    status,
    code: status === "REFUSED" && outcome === "pass" ? "frozen_copy_seal_missing" : code,
    startedAt: new Date(env.startedAt).toISOString(),
    finishedAt: new Date(env.finishedAt).toISOString(),
    progress: { schemaVersion: PROGRESS_SCHEMA_VERSION, stateTable: PROGRESS_TABLE, bindingSha: p.bindingSha, totalOrgs: p.totalOrgs, completedOrgs: p.completedOrgs, dataset: p.dataset, pages: p.pages, rows: p.rows, resumeCount: p.resumeCount, lastCheckpointAt: p.lastCheckpointAt, cursorSha256: p.cursorSha256 },
    budget: { ...env.budget, deadline: new Date(env.budget.deadline).toISOString(), elapsedWallMs: env.finishedAt - env.startedAt, exceeded: code === "frozen_copy_budget_exhausted" },
    abort: {
      requested: env.abort.requested,
      observed: env.abort.observed,
      boundedReason: env.abort.reason,
      requestedAt: iso(env.abort.requestedAt),
      completedAt: iso(env.abort.completedAt),
      inFlightResolved: p.inFlight === 0,
      sourceClosed: env.abort.sourceClosed,
      targetClosed: env.abort.targetClosed,
      writesStopped: env.abort.writesStopped,
    },
    unknownCommit: {
      detected: !!doubt,
      phase: doubt?.phase ?? null,
      dataset: doubt?.dataset ?? null,
      checkpointSeq: doubt?.checkpointSeq ?? null,
      detectedAt: iso(doubt?.detectedAt ?? null),
      reconciliation,
      readbackAt: doubt ? iso(env.reconciliation.readbackAt) : null,
      resolvedAt: doubt && (reconciliation === "COMMITTED" || reconciliation === "NOT_COMMITTED") ? iso(env.reconciliation.readbackAt) : null,
      noBlindRetry: true,
    },
    seal,
    next: NEXT[status][mode],
  };
}
