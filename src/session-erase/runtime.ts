/** Runtime ports for the default-off self-host operator erase. Provider messages never leave here. */
import type { SqlRun } from "../session-ingest/ledger-sql.ts";
import type { SessionEraseRequest, SessionEraseService } from "../session-ingest/session-erase.ts";
import { dbErrorSnapshot } from "../session-ingest/db-error-class.ts";
import { IngestUnavailable } from "../session-ingest/deliver.ts";
import { erasureReceipt, SessionEraseServiceError, type ArtifactKey, type EraseKey, type ErasureReceipt } from "./receipt.ts";
import type { SessionEraseBytesResult } from "./selfhost-eraser.ts";

export type SessionWriteGuard = <T>(tenant: { orgId: string; workspaceId: string }, sessions: readonly [string, string][], work: () => Promise<T>) => Promise<T>;
export interface EraseStoreIdentity { bucket: string; keyRoot: string; storeRef: string }
export interface EraseWork extends EraseKey, EraseStoreIdentity { sessionIds: string[] }
export interface EraseLedger extends SessionEraseService {
  next(lease: string): Promise<EraseWork | null>;
  authorize(work: EraseWork, lease: string): Promise<boolean>;
  release(work: EraseWork, lease: string): Promise<void>;
  purge(work: EraseWork, lease: string): Promise<void>;
  step(work: EraseWork, lease: string, artifact: ArtifactKey, state: "done" | "pending" | "not_applicable" | "failed_retryable", count: number, code?: string): Promise<void>;
}
const params = (key: EraseKey) => [key.orgId, key.workspaceId, key.source, key.sessionId];
const fn = {
  begin: "ingest_operator_runtime_begin", read: "ingest_operator_erase_receipt", next: "ingest_operator_runtime_next",
  authorize: "ingest_operator_runtime_authorize", release: "ingest_operator_runtime_release", purge: "ingest_operator_runtime_purge",
  step: "ingest_operator_runtime_step", writeBegin: "ingest_operator_runtime_write_begin", writeFinish: "ingest_operator_runtime_write_finish",
} as const;
async function call(run: SqlRun, name: typeof fn[keyof typeof fn], values: unknown[]): Promise<unknown> {
  try {
    const { rows } = await run(`SELECT sessions.${name}(${values.map((_, i) => `$${i + 1}`).join(",")}) AS result`, values);
    if (rows.length !== 1) throw new SessionEraseServiceError();
    let value = rows[0]!.result;
    if (typeof value === "string" && /^[{[]/.test(value)) value = JSON.parse(value);
    return value;
  } catch (error) {
    if (error instanceof SessionEraseServiceError) throw error;
    const code = dbErrorSnapshot(error).code;
    throw new SessionEraseServiceError(code === "RH010" ? "too_many_owners" : code === "RH011" || code === "RH013" ? "unsupported_storage" : "unavailable");
  }
}

/** Successful writes close their obligation only after all object writes and the commit resolve.
 * Failed/ambiguous requests remain durable blockers, even across a process restart. */
export function sqlSessionWriteGuard(run: SqlRun): SessionWriteGuard {
  return async (tenant, sessions, work) => {
    if (sessions.length === 0) return work();
    const writerId = crypto.randomUUID();
    const allowed = await call(run, fn.writeBegin, [JSON.stringify({ orgId: tenant.orgId, workspaceId: tenant.workspaceId, writerId, sessions })]);
    if (allowed !== true) throw new IngestUnavailable("objects", "operator_session_erased");
    const result = await work();
    await call(run, fn.writeFinish, [tenant.orgId, tenant.workspaceId, writerId]);
    return result;
  };
}

export function sqlEraseLedger(run: SqlRun, identity: EraseStoreIdentity): EraseLedger {
  return {
    async erase(key: EraseKey, request: SessionEraseRequest) {
      const r = erasureReceipt(await call(run, fn.begin, [...params(key), request.requestId, request.actor.kind, request.actor.id,
        identity.bucket, identity.keyRoot, identity.storeRef]), key);
      if (r.failure?.step === "fence" && r.failure.code === "RH010") throw new SessionEraseServiceError("too_many_owners");
      return r;
    },
    async receipt(key) {
      const value = await call(run, fn.read, params(key));
      return value === null ? null : erasureReceipt(value, key);
    },
    async next(lease) {
      const value = await call(run, fn.next, [lease]);
      if (value === null) return null;
      if (!value || typeof value !== "object") throw new SessionEraseServiceError();
      const work = value as EraseWork;
      for (const field of ["orgId", "workspaceId", "source", "sessionId", "bucket", "keyRoot", "storeRef"] as const)
        if (typeof work[field] !== "string" || !work[field] || work[field].length > 512) throw new SessionEraseServiceError();
      if (!Array.isArray(work.sessionIds) || !work.sessionIds.length || work.sessionIds.length > 256
        || work.sessionIds.some(id => typeof id !== "string" || !id || id.length > 512) || !work.sessionIds.includes(work.sessionId)) throw new SessionEraseServiceError();
      return work;
    },
    async authorize(work, lease) { return await call(run, fn.authorize, [...params(work), lease]) === true; },
    async release(work, lease) { await call(run, fn.release, [...params(work), lease]); },
    async purge(work, lease) { await call(run, fn.purge, [...params(work), lease]); },
    async step(work, lease, artifact, state, count, code) { await call(run, fn.step, [...params(work), lease, artifact, state, count, code ?? null]); },
  };
}

export interface EraseBytesPort {
  erase(key: EraseKey, authorize: () => Promise<void>): Promise<SessionEraseBytesResult>;
  /** Absence of mixed-session staged/buffered/inbox payloads must be proven independently. */
  stagesAbsent(key: EraseKey, authorize: () => Promise<void>): Promise<boolean>;
}
export interface EraseSearchPort {
  /** Return only after a synchronous, tenant-scoped delete and its verification. */
  erase(key: EraseKey, sessionIds: readonly string[]): Promise<void>;
}
export interface EraseWorkerOptions {
  ledger: EraseLedger; identity: EraseStoreIdentity; bytes: EraseBytesPort; search: EraseSearchPort;
  /** Same metadata-DB search advisory locks the consumer uses, held across external erasure. */
  searchLock<T>(work: EraseWork, run: () => Promise<T>): Promise<T>;
  log?: (fields: Record<string, unknown>) => void;
  intervalMs?: number;
}
class ErasePending extends Error {}

export function sessionEraseWorker(options: EraseWorkerOptions) {
  const { ledger, identity } = options;
  const log = options.log ?? (() => {});
  let timer: ReturnType<typeof setInterval> | undefined;
  let running: Promise<boolean> | undefined;
  const runOnce = async (): Promise<boolean> => {
    const lease = crypto.randomUUID();
    const work = await ledger.next(lease);
    if (!work) return false;
    let artifact: ArtifactKey = "transcriptObjects";
    const authorize = async () => { if (!await ledger.authorize(work, lease)) throw new ErasePending(); };
    try {
      await authorize();
      if (work.bucket !== identity.bucket || work.keyRoot !== identity.keyRoot || work.storeRef !== identity.storeRef)
        throw new SessionEraseServiceError("unsupported_storage");
      if (!await options.bytes.stagesAbsent(work, authorize)) {
        await ledger.step(work, lease, "stagedObjects", "pending", 0);
        return true;
      }
      let objects = 0;
      for (const sessionId of work.sessionIds) {
        await authorize();
        const result = await options.bytes.erase({ orgId: work.orgId, workspaceId: work.workspaceId, source: work.source, sessionId }, authorize);
        objects += result.transcriptObjects + result.pointerObjects;
        if (!result.verified) { await ledger.step(work, lease, artifact, "pending", objects); return true; }
      }
      await ledger.step(work, lease, artifact, "done", objects);
      artifact = "searchDocuments";
      await options.searchLock(work, async () => {
        await authorize();
        await options.search.erase(work, work.sessionIds);
        await authorize();
        // Purge verifies metadata and records its counts in one transaction. The search lock
        // drains a pre-fence indexer and prevents it from restoring documents behind our delete.
        await ledger.purge(work, lease);
      });
      // SQL admission proved these independent pipelines absent. Sync self-host creates no stage,
      // buffered rows or history authority; the object-store absence check also passed above.
      for (const name of ["stagedObjects", "bufferedBatches", "bufferedRows", "historyDerivedObjects"] as const)
        await ledger.step(work, lease, name, "not_applicable", 0);
      log({ event: "operator_session_erase_pass", outcome: "verified" });
      return true;
    } catch (error) {
      if (error instanceof ErasePending) { log({ event: "operator_session_erase_pass", outcome: "writes_pending" }); return true; }
      const code = error instanceof SessionEraseServiceError && error.code === "unsupported_storage" ? "unsupported_storage" : "erase_step_failed";
      await ledger.step(work, lease, artifact, "failed_retryable", 0, code).catch(() => {});
      log({ event: "operator_session_erase_pass", outcome: "failed", step: artifact, code });
      return true;
    } finally { await ledger.release(work, lease).catch(() => {}); }
  };
  const scan = (): Promise<boolean> => {
    if (running) return running;
    running = runOnce().catch(() => { log({ event: "operator_session_erase_tick", outcome: "unavailable" }); return false; })
      .finally(() => { running = undefined; });
    return running;
  };
  return {
    scan,
    start() { if (!timer) { timer = setInterval(() => { void scan(); }, options.intervalMs ?? 1_000); timer.unref?.(); void scan(); } },
    async stop() { if (timer) clearInterval(timer); timer = undefined; await running; },
  };
}
