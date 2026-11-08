/**
 * Self-host search-index writer (SERVICE=session-pipeline-consumer).
 *
 * Runs the existing session-pipeline consumer as a Node process. Each poll:
 * loads due retries from the durable pending table, lists recent manifests
 * with the same `reconcileRecent` the Lambda schedule uses, compares
 * projection_state freshness against the pointer (reconcile bodies for stale
 * sessions), feeds every body to the same `processBatch`, persists failures
 * back to the pending table, and advances the persisted high-water mark only
 * on a fully complete tick. There is no SQS hop in self-host (MinIO sends no
 * bucket notifications, so nothing could publish to it); the reconcile
 * bodies are the queue items, and at-least-once comes from the durable
 * pending table plus the freshness comparison, never from process memory.
 *
 * Identity: this Deployment holds ONLY the dedicated `search_consumer_runtime`
 * database URL. It never holds owner, migrator or reader credentials. Schema,
 * grants and RLS policies come exclusively from the one-shot owner bootstrap
 * (SERVICE=session-pipeline-consumer-bootstrap); startup refuses to run as
 * any other session_user (owner, superuser, reader), and refuses to start
 * when the bootstrap tables are absent.
 *
 * Fail-closed contract (mirrors indexSearchScope, which does the real work):
 * an incomplete read, a stale write or a changed expected source epoch never
 * becomes complete and is retried; a session with no live manifests or
 * siblings left is erased (every row deleted) and records complete with a
 * zero row count; a successful reconciliation of a new revision completes
 * normally; lock contention returns typed busy/incomplete and retries via
 * the pending table. Nothing here seeds state: rows come only from
 * reconciling real canonical events.
 */
import { createServer } from "node:http";
import { unlinkSync, writeFileSync } from "node:fs";
import { log } from "../shared/log.ts";
import { integer } from "../shared/env.ts";
import { inTransaction, type HoopDb } from "../shared/db.ts";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { runWorker } from "../shared/worker.ts";
import { lockerManifestReader, lockerStore } from "../shared/locker.ts";
import {
  objectKey,
  prefixAllowed,
  prefixOrgResolver,
} from "../shared/vendor.ts";
import { postgresTlsOptions } from "../shared/postgres-tls.ts";
import {
  composeSearchService,
  createPostgresSearchComposition,
  searchSecret,
  type SearchComposition,
} from "../shared/search-composition.ts";
import type { SearchProjectionState } from "./search-adapter.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { writerTransactionSql } from "./transaction-sql.ts";
import {
  droppedFanout,
  scopeLockKey,
  type PointerDeps,
  type ScopeLock,
} from "./pointer.ts";
import { processBatch, type ConsumerDeps, type QueueRecord } from "./consumer.ts";
import { RECONCILE_BODY_KEY } from "./s3-events.ts";
import { reconcileRecent, supabasePool, transaction } from "./lambda.ts";
import {
  CONSUMER_BACKEND_ID_VAR,
  consumerServiceConfig,
} from "./consumer-config.ts";

/**
 * The postgres projection state behind our writer composition, which carries
 * the explicit operator step `ensureSchema`. The cast is safe because this
 * service only ever builds `createPostgresSearchComposition`, whose state is
 * always `postgresSearchState`.
 */
export type WriterProjectionState = SearchProjectionState & { ensureSchema(owner: SqlExecutor): Promise<void> };

/** Attempts past this mark the pending row dead (dead-letter state) instead of retrying it. */
export const MAX_ATTEMPTS = 24;
/** Pending rows are bounded per tick; the reconcile window re-discovers the rest. */
export const MAX_PENDING_ROWS = 10_000;
/** High-water margin: re-list this far before the last fully complete tick. */
export const HIGH_WATER_MARGIN_MS = 5 * 60_000;
/** Dead-letter backoff ceiling. */
const MAX_DELAY_MS = 60 * 60_000;

export interface ConsumerTickIo {
  /** List recent manifests and hand each reconcile body to `send`. */
  reconcile: (send: (body: string) => Promise<void>, since: Date) => Promise<{ enqueued: number; complete: boolean; nextCursor?: string }>;
  /** Check projection_state freshness against the pointer; send bodies for stale sessions. */
  checkFreshness: (send: (body: string) => Promise<void>) => Promise<{ stale: number; tombstoned: number; overflow: boolean }>;
  /**
   * Drop bodies that must not be consumed this tick (durable dead letters with
   * an unchanged revision). Runs after collection, before consume.
   */
  dedupe?: (bodies: string[]) => Promise<string[]>;
  /** Run one batch through the real consumer. */
  consume: (records: QueueRecord[]) => Promise<{ batchItemFailures: { itemIdentifier: string }[] }>;
  /** In-tick carry only; the service persists it to Postgres after the tick. */
  pending: string[];
  requeued: string[];
  now?: () => number;
}

export interface ConsumerTickSummary {
  collected: number;
  failed: number;
  requeued: number;
  pending: number;
  stale: number;
  tombstoned: number;
  overflow: boolean;
  complete: boolean;
  /** Set when the whole batch threw: every body counts as failed, and the
   * service persists attempts before rethrowing for its failure counter. */
  batchError?: unknown;
  failedKeys: string[];
  requeuedKeys: string[];
  failedItems: { key: string; etag?: string; version?: string }[];
  /**
   * Keys actually sent to consume this tick (post-dedupe). Success is computed
   * over exactly these: a body dedupe skipped was never consumed and must not
   * be deleted as succeeded.
   */
  consumedKeys: string[];
  /**
   * Requeued keys with the revision actually consumed (carried from the
   * consumed body, not the etag-less requeue callback body), so busy-retry
   * accounting never nulls a known revision.
   */
  requeuedItems: { key: string; etag?: string; version?: string }[];
  /** Listing resume point when the reconcile link did not complete. */
  nextCursor?: string;
}

/** Manifest key carried by a reconcile body, or null when the body is not one. */
export function pendingKey(body: string): string | null {
  return reconcileMeta(body)?.key ?? null;
}

/** Manifest key plus revision markers carried by a reconcile body, or null when the body is not one. */
export function reconcileMeta(body: string): { key: string; etag?: string; version?: string } | null {
  try {
    const value = JSON.parse(body) as { reconcile?: { key?: unknown; etag?: unknown; version?: unknown } };
    if (typeof value?.reconcile?.key !== "string") return null;
    const out: { key: string; etag?: string; version?: string } = { key: value.reconcile.key };
    if (typeof value.reconcile.etag === "string") out.etag = value.reconcile.etag;
    if (typeof value.reconcile.version === "string") out.version = value.reconcile.version;
    return out;
  } catch {
    return null;
  }
}

/**
 * One poll: collect carry-over plus reconcile bodies, consume them once, and
 * park failures and busy requeues back on pending for the service to persist.
 * A whole-batch throw counts every body as failed (so the service persists an
 * attempt for each, and poison keys still dead-letter) and is reported via
 * `batchError` for the service to count and rethrow. A reconcile listing
 * error propagates so the run is visible and retried next tick, with pending
 * left intact. `complete` additionally requires the freshness check to find
 * nothing stale: per-scope incomplete/partial/delegated outcomes are logged
 * per scope by processBatch, and any session they leave behind is re-sent by
 * the next freshness check instead of being trusted here.
 */
export async function runConsumerTick(io: ConsumerTickIo, since: Date): Promise<ConsumerTickSummary> {
  const fresh: string[] = [];
  const link = await io.reconcile(
    async (body) => {
      fresh.push(body);
    },
    since,
  );
  // Pending is cleared only after a successful listing, so a listing error
  // never drops retries; the durable table is the backstop regardless.
  const bodies = [...io.pending, ...fresh];
  io.pending.length = 0;
  const freshness = await io.checkFreshness(async (body) => {
    bodies.push(body);
  });
  const consumable = io.dedupe ? await io.dedupe(bodies) : bodies;
  // Identities actually consumed this tick (post-dedupe), with the revision
  // carried on each body. A requeue callback only hands back the key, so the
  // map below restores the consumed revision for busy-retry accounting.
  const consumedKeys: string[] = [];
  const consumedMetas = new Map<string, { etag?: string; version?: string }>();
  for (const body of consumable) {
    const meta = reconcileMeta(body);
    if (meta === null) continue;
    consumedKeys.push(meta.key);
    if (!consumedMetas.has(meta.key)) consumedMetas.set(meta.key, { etag: meta.etag, version: meta.version });
  }
  const stamp = io.now?.() ?? Date.now();
  const records: QueueRecord[] = consumable.map((body, i) => ({ messageId: `reconcile-${stamp}-${i}`, body }));
  const bodyById = new Map(records.map((record, i) => [record.messageId, consumable[i]!]));
  let batchError: unknown;
  let failed: Set<string>;
  try {
    const response = await io.consume(records);
    failed = new Set(response.batchItemFailures.map((f) => f.itemIdentifier));
  } catch (error) {
    // The whole batch errored (e.g. store down): every body counts as failed
    // so the service persists an attempt for each instead of parking them in
    // memory with no accounting (which retried poison keys forever).
    batchError = error;
    failed = new Set(records.map((record) => record.messageId));
  }
  const failedKeys: string[] = [];
  const failedItems: { key: string; etag?: string; version?: string }[] = [];
  const requeuedKeys: string[] = [];
  let failedCount = 0;
  records.forEach((record) => {
    if (!failed.has(record.messageId)) return;
    failedCount += 1;
    const meta = reconcileMeta(bodyById.get(record.messageId)!);
    if (meta !== null) {
      failedKeys.push(meta.key);
      failedItems.push(meta);
    }
    if (io.pending.length < MAX_PENDING_ROWS) io.pending.push(bodyById.get(record.messageId)!);
  });
  const requeuedItems: { key: string; etag?: string; version?: string }[] = [];
  for (const body of io.requeued) {
    const meta = reconcileMeta(body);
    if (meta !== null) {
      requeuedKeys.push(meta.key);
      // The requeue callback hands back an etag-less body; carry the revision
      // actually consumed so busy-retry accounting never nulls a known one.
      const carried = consumedMetas.get(meta.key);
      requeuedItems.push({
        key: meta.key,
        ...(carried?.etag !== undefined ? { etag: carried.etag } : {}),
        ...(carried?.version !== undefined ? { version: carried.version } : {}),
      });
      if (io.pending.length < MAX_PENDING_ROWS) {
        // Park the consumed (revision-carrying) body, not the etag-less
        // requeue callback body, so the next tick still sees the revision.
        const carriedBody = consumable.find((b) => reconcileMeta(b)?.key === meta.key);
        io.pending.push(carriedBody ?? body);
      }
    } else if (io.pending.length < MAX_PENDING_ROWS) {
      io.pending.push(body);
    }
  }
  io.requeued.length = 0;
  if (io.pending.length >= MAX_PENDING_ROWS)
    log.warn("pipeline consumer pending overflow; reconcile window re-discovers", { pending: io.pending.length });
  return {
    collected: bodies.length,
    failed: failedCount,
    requeued: requeuedKeys.length,
    pending: io.pending.length,
    stale: freshness.stale,
    tombstoned: freshness.tombstoned,
    overflow: freshness.overflow || !link.complete,
    complete:
      batchError === undefined &&
      link.complete &&
      !freshness.overflow &&
      freshness.stale === 0 &&
      freshness.tombstoned === 0 &&
      failedCount === 0 &&
      io.pending.length === 0,
    ...(batchError === undefined ? {} : { batchError }),
    failedKeys,
    requeuedKeys,
    failedItems,
    consumedKeys,
    requeuedItems,
    ...(link.nextCursor === undefined ? {} : { nextCursor: link.nextCursor }),
  };
}

/**
 * The writer composition for this backend id and coverage floor. The floor
 * MUST equal the reader's READ_API_COMPLETE_SINCE floor: partials treats any
 * other coverage_from_ms as stale, so a mismatched writer never converges.
 * Throws "search state configuration invalid" on a bad id.
 */
export function writerComposition(backendId: string, fromMs: number): SearchComposition {
  return createPostgresSearchComposition({ backendId, fromMs });
}

/** Tenant-scoped runner for the writer's own freshness self-check: sets the
 * RLS GUCs and runs as the same runtime role, no privilege change. */
export function writerTenantRunner(db: HoopDb): TenantRunner {
  return async (orgId, workspaceId, work) =>
    inTransaction(db, async (tx) => {
      await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [orgId, workspaceId]);
      return work(tx);
    });
}

/** Exponential backoff with a one-hour ceiling. */
export function retryDelayMs(attempts: number): number {
  return Math.min(30_000 * 2 ** Math.min(Math.max(attempts, 0), 10), MAX_DELAY_MS);
}

/** Listing lower bound: the older of the lookback window and the high-water mark minus margin. */
export function computeSince(nowMs: number, lookbackMs: number, lastCompleteAt: Date | null): Date {
  if (!lastCompleteAt) return new Date(nowMs - lookbackMs);
  return new Date(Math.min(nowMs - lookbackMs, lastCompleteAt.getTime() - HIGH_WATER_MARGIN_MS));
}

export interface PendingRow {
  manifestKey: string;
  manifestEtag: string | null;
  manifestVersion: string | null;
  attempts: number;
  nextAt: Date;
  lastErrorCode: string;
}

const reconcileBody = (key: string, etag?: string | null, version?: string | null) =>
  JSON.stringify({ [RECONCILE_BODY_KEY]: { key, ...(etag ? { etag } : {}), ...(version ? { version } : {}) } });

export interface AttemptScore {
  attempts: number;
  dead: boolean;
}

/**
 * Failure accounting for one pending row. A new cycle starts (attempts 1,
 * alive) only when the failure carries a revision that differs from the
 * recorded one; etag-less bodies never revive a dead row. Otherwise attempts
 * increment and the row dead-letters past MAX_ATTEMPTS.
 */
export function scoreFailedAttempt(
  prev: { attempts: number; etag: string | null; version: string | null } | undefined,
  item: { etag?: string; version?: string },
): AttemptScore {
  const carried = item.etag !== undefined || item.version !== undefined;
  const revised =
    carried &&
    prev !== undefined &&
    ((prev.etag ?? null) !== (item.etag ?? null) || (prev.version ?? null) !== (item.version ?? null));
  const attempts = revised ? 1 : (prev?.attempts ?? 0) + 1;
  return { attempts, dead: attempts > MAX_ATTEMPTS };
}

/**
 * Requeue accounting: a busy scope retries with backoff, but a perpetually
 * busy key is poison and dead-letters instead of retrying forever. Same
 * carried-revision revival rule as failures.
 */
export function scoreRequeuedAttempt(
  prev: { attempts: number; dead: boolean; etag: string | null; version: string | null } | undefined,
  meta: { etag?: string; version?: string } | undefined,
): AttemptScore {
  const carried = meta?.etag !== undefined || meta?.version !== undefined;
  const revised =
    carried &&
    prev !== undefined &&
    prev.dead &&
    ((prev.etag ?? null) !== (meta?.etag ?? null) || (prev.version ?? null) !== (meta?.version ?? null));
  const attempts = revised ? 0 : (prev?.attempts ?? 0) + 1;
  return { attempts, dead: attempts > MAX_ATTEMPTS };
}

/** Due, non-dead retries for this backend, oldest first. */
export async function loadPendingDue(db: SqlExecutor, backendId: string, limit: number, now: Date): Promise<PendingRow[]> {
  const { rows } = await db.query<PendingRow>(
    `SELECT manifest_key AS "manifestKey", manifest_etag AS "manifestEtag", manifest_version AS "manifestVersion",
       attempts AS "attempts", next_at AS "nextAt", last_error_code AS "lastErrorCode"
     FROM sessions.search_consumer_pending
     WHERE backend_id=$1 AND dead IS NOT TRUE AND next_at<=$2::timestamptz ORDER BY next_at LIMIT $3::int`,
    [backendId, now.toISOString(), limit],
  );
  return rows;
}

export interface PendingOutcome {
  manifestKey: string;
  manifestEtag?: string | null;
  manifestVersion?: string | null;
  attempts: number;
  nextAt: Date;
  lastErrorCode: string;
  dead: boolean;
}

/**
 * Upsert pending outcomes. The service decides dead/alive per row; the table
 * only records it. A dead row is never resurrected by this statement: revival
 * is an explicit store with dead=false for a new etag/version.
 */
export async function storePending(
  db: SqlExecutor,
  backendId: string,
  rows: PendingOutcome[],
  now: Date,
): Promise<void> {
  for (const row of rows) {
    await db.query(
      // A failure body without a revision never nulls the recorded one: the
      // etag/version columns keep the previously carried revision so the next
      // revive comparison still sees the real last revision.
      `INSERT INTO sessions.search_consumer_pending(backend_id,manifest_key,manifest_etag,manifest_version,attempts,next_at,last_error_code,dead,updated_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT(backend_id,manifest_key) DO UPDATE SET
         manifest_etag=COALESCE(excluded.manifest_etag,sessions.search_consumer_pending.manifest_etag),
         manifest_version=COALESCE(excluded.manifest_version,sessions.search_consumer_pending.manifest_version),
         attempts=excluded.attempts,next_at=excluded.next_at,last_error_code=excluded.last_error_code,dead=excluded.dead,updated_at=excluded.updated_at`,
      [backendId, row.manifestKey, row.manifestEtag ?? null, row.manifestVersion ?? null, row.attempts, row.nextAt.toISOString(), row.lastErrorCode, row.dead, now.toISOString()],
    );
    if (row.dead)
      log.error("pipeline consumer pending dead-lettered", { manifest: row.manifestKey, attempts: row.attempts, error_code: row.lastErrorCode });
  }
}

export interface KnownPending {
  attempts: number;
  dead: boolean;
  etag: string | null;
  version: string | null;
}

/** All known pending rows for these keys, dead or alive, for skip/revive decisions. */
export async function loadPendingMap(db: SqlExecutor, backendId: string, keys: string[]): Promise<Map<string, KnownPending>> {
  const out = new Map<string, KnownPending>();
  if (!keys.length) return out;
  const { rows } = await db.query<{ k: string; attempts: number; dead: boolean; etag: string | null; version: string | null }>(
    `SELECT manifest_key AS k, attempts, dead, manifest_etag AS etag, manifest_version AS version
     FROM sessions.search_consumer_pending WHERE backend_id=$1 AND manifest_key = ANY($2::text[])`,
    [backendId, keys],
  );
  for (const row of rows) out.set(row.k, { attempts: row.attempts, dead: row.dead, etag: row.etag, version: row.version });
  return out;
}

/**
 * Dead-letter skip rule: a dead row is skipped unless the body carries a NEW
 * revision (a genuinely new manifest revives the key). An etag-less body
 * never revives: without a revision it cannot prove it is newer, so it stays
 * skipped instead of resurrecting the dead row.
 */
export function shouldSkipDeadLetter(
  row: KnownPending | undefined,
  meta: { key: string; etag?: string; version?: string } | null,
): boolean {
  if (meta === null || !row?.dead) return false;
  const carried = meta.etag !== undefined || meta.version !== undefined;
  if (!carried) return true;
  return (row.etag ?? null) === (meta.etag ?? null) && (row.version ?? null) === (meta.version ?? null);
}

/**
 * Freshness/tombstone backoff gate: bodies whose pending row has not come
 * due yet are not re-sent this tick (their next_at backoff keeps working).
 * Returns the gated send plus a skipped count for logging. Skipped bodies
 * still count in the stale/tombstoned totals, so a backing-off session
 * blocks completion exactly like a failure does.
 */
export function gateDeferredSends(
  deferred: Set<string>,
  send: (body: string) => Promise<void>,
): { send: (body: string) => Promise<void>; skipped: () => number } {
  let skipped = 0;
  return {
    send: async (body) => {
      const meta = reconcileMeta(body);
      if (meta && deferred.has(meta.key)) {
        skipped += 1;
        return;
      }
      await send(body);
    },
    skipped: () => skipped,
  };
}

/**
 * The tick's durable dead-letter filter: drop bodies this tick must not
 * consume (dead rows with an unchanged or absent revision). Runs after
 * collection, before consume, so a skipped body is neither consumed nor
 * deleted as succeeded.
 */
export function makeDedupe(db: SqlExecutor, backendId: string): (bodies: string[]) => Promise<string[]> {
  return async (bodies: string[]): Promise<string[]> => {
    const keys = [...new Set(bodies.map((b) => reconcileMeta(b)?.key).filter((k): k is string => k !== null))];
    const known = await loadPendingMap(db, backendId, keys);
    let skipped = 0;
    const survivors = bodies.filter((body) => {
      if (shouldSkipDeadLetter(known.get(reconcileMeta(body)?.key ?? ""), reconcileMeta(body))) {
        skipped += 1;
        return false;
      }
      return true;
    });
    if (skipped) log.info("pipeline consumer tick skips dead letters", { skipped });
    return survivors;
  };
}

/**
 * Pending keys whose backoff has not elapsed yet. Freshness and tombstone
 * re-sends skip these: without the gate every tick re-sent a failing key and
 * its next_at backoff never slowed the retry loop. Bounded: past the cap the
 * tail re-sends early (no worse than today), and the cap is far above any
 * real backlog.
 */
export const MAX_DEFERRED_KEYS = 100_000;

export async function loadPendingDeferred(db: SqlExecutor, backendId: string, now: Date): Promise<Set<string>> {
  const { rows } = await db.query<{ manifest_key: string }>(
    `SELECT manifest_key FROM sessions.search_consumer_pending
     WHERE backend_id=$1 AND next_at>$2::timestamptz ORDER BY next_at LIMIT $3::int`,
    [backendId, now.toISOString(), MAX_DEFERRED_KEYS],
  );
  return new Set(rows.map((r) => r.manifest_key));
}

/** A persisted reconcile-listing resume point, bound to the fixed since/window that produced it. */
export interface ReconcileCursor {
  cursor: string;
  since: Date;
  lookbackMinutes: number;
}

export async function loadReconcileCursor(db: SqlExecutor, backendId: string): Promise<ReconcileCursor | null> {
  const { rows } = await db.query<{ cursor: string | null; since: string | null; window: number | null }>(
    `SELECT reconcile_cursor AS cursor, reconcile_since AS since, reconcile_window_minutes AS window
     FROM sessions.search_consumer_progress WHERE backend_id=$1`,
    [backendId],
  );
  const row = rows[0];
  if (!row?.cursor || !row.since || row.window === null) return null;
  return { cursor: row.cursor, since: new Date(row.since), lookbackMinutes: Number(row.window) };
}

export async function storeReconcileCursor(
  db: SqlExecutor,
  backendId: string,
  state: ReconcileCursor | null,
  at: Date,
): Promise<void> {
  await db.query(
    `INSERT INTO sessions.search_consumer_progress(backend_id,last_complete_at,reconcile_cursor,reconcile_since,reconcile_window_minutes,updated_at)
     VALUES($1,NULL,$2,$3,$4,$5)
     ON CONFLICT(backend_id) DO UPDATE SET reconcile_cursor=excluded.reconcile_cursor,
       reconcile_since=excluded.reconcile_since,reconcile_window_minutes=excluded.reconcile_window_minutes,updated_at=excluded.updated_at`,
    [backendId, state?.cursor ?? null, state?.since.toISOString() ?? null, state?.lookbackMinutes ?? null, at.toISOString()],
  );
}

/** A healthy start clears a stale UNHEALTHY marker (missing file is not an error). */
export function clearUnhealthyFile(path: string): void {
  try {
    unlinkSync(path);
  } catch (error) {
    if ((error as { code?: string })?.code !== "ENOENT") throw error;
  }
}

export async function deletePending(db: SqlExecutor, backendId: string, manifestKeys: string[]): Promise<void> {
  for (const key of manifestKeys)
    await db.query(`DELETE FROM sessions.search_consumer_pending WHERE backend_id=$1 AND manifest_key=$2`, [backendId, key]);
}

export async function loadHighWaterMark(db: SqlExecutor, backendId: string): Promise<Date | null> {
  const { rows } = await db.query<{ mark: string | null }>(
    `SELECT last_complete_at AS mark FROM sessions.search_consumer_progress WHERE backend_id=$1`, [backendId]);
  const mark = rows[0]?.mark;
  return mark ? new Date(mark) : null;
}

export async function storeHighWaterMark(db: SqlExecutor, backendId: string, at: Date): Promise<void> {
  await db.query(
    `INSERT INTO sessions.search_consumer_progress(backend_id,last_complete_at,updated_at) VALUES($1,$2,$2)
     ON CONFLICT(backend_id) DO UPDATE SET last_complete_at=excluded.last_complete_at,updated_at=excluded.updated_at`,
    [backendId, at.toISOString()],
  );
}

const likeEscape = (value: string) => value.replace(/[\\%_]/g, (c) => `\\${c}`);

/**
 * Freshness self-check: reuse the exact staleness definition the reader uses
 * (projection_state vs pointer/directory), and re-send reconcile bodies for
 * every live manifest of a stale session. This is what rediscovers a search
 * write lost to a crash after the pointer succeeded.
 */
/**
 * Cap on stale pages per tenant per tick: progress is guaranteed (the cursor
 * advances past every returned session) but a pathological tenant reports
 * overflow instead of stalling the tick.
 */
export const MAX_STALE_PAGES = 64;

export async function reconcileStaleProjections(
  db: SqlExecutor,
  partials: (tenant: { orgId: string; workspaceId: string }, after?: string) => Promise<{ sessions: readonly string[]; overflow: boolean }>,
  keyPrefix: string,
  send: (body: string) => Promise<void>,
): Promise<{ stale: number; overflow: boolean }> {
  const { rows: tenants } = await db.query<{ orgId: string; workspaceId: string }>(
    `SELECT DISTINCT org_id AS "orgId", workspace_id AS "workspaceId" FROM sessions.session_index WHERE deleted_at IS NULL`);
  let stale = 0;
  let overflow = false;
  for (const tenant of tenants) {
    // Keyset pages over the tenant's stale sessions: without the cursor, a
    // tenant with more stale sessions than the page limit is skipped every
    // tick while later tenants starve behind it.
    let after: string | undefined;
    for (let page = 0; ; page += 1) {
      const partial = await partials(tenant, after);
      for (const session of partial.sessions) {
        after = session;
        const { rows: keys } = await db.query<{ manifest_key: string }>(
          `SELECT manifest_key FROM sessions.session_index
           WHERE org_id=$1 AND workspace_id=$2 AND (session_id=$3 OR canonical_session_id=$3) AND deleted_at IS NULL AND manifest_key LIKE $4 ESCAPE '\\'`,
          [tenant.orgId, tenant.workspaceId, session, `${likeEscape(keyPrefix)}/%`],
        );
        stale += keys.length;
        for (const { manifest_key } of keys) await send(reconcileBody(manifest_key));
      }
      if (!partial.overflow) break;
      if (page + 1 >= MAX_STALE_PAGES) {
        overflow = true;
        // The stale cursor is tick-local by design: next tick restarts the
        // listing from scratch (bounded and idempotent) instead of resuming.
        log.warn("pipeline consumer stale backlog overflow; restarts from scratch next tick", { after });
        break;
      }
    }
  }
  return { stale, overflow };
}

/**
 * Cap on tombstone pages per tick: the backstop always makes progress (the
 * cursor advances) but a pathological volume reports overflow instead of
 * stalling the tick. 64 pages cover far more than any real backlog.
 */
export const MAX_TOMBSTONE_PAGES = 64;

/**
 * Tombstone backstop: a pointer tombstone commits before indexSearchScope runs,
 * so a crash between them leaves erased content searchable with no live
 * pointer row for reconcileRecent or partials to find.
 *
 * Tenant GUCs, not a writer RLS policy, gate the search tables: every pass
 * runs inside the tenant's transaction (exactly as #330's writer path does),
 * so #330's exact-one-policy verify keeps passing and the writer can never
 * write across tenants. The tenant enumeration itself is cross-tenant and
 * uses the sessions-table writer policy (no GUCs involved).
 *
 * Three behaviors keep this convergent instead of a permanent resend loop:
 * 1. Erasure residue retires atomically per tenant: a tombstone whose
 *    projection state is complete with row_count 0 and whose scope holds no
 *    transcript rows is fully erased, so its residue state row is deleted in
 *    one statement (never re-sent). The row has no reader: partials only
 *    lists live pointers.
 * 2. The re-send set filters that residue out: only tombstones with an
 *    incomplete state, a state that still counts rows, or leftover transcript
 *    rows are re-sent, while the transcript EXISTS check is kept.
 * 3. Re-sends page by manifest-key cursor per tenant with an overflow
 *    contract, so the same first LIMIT rows are not selected forever while
 *    later scopes starve and the high-water mark never advances. The cursor
 *    is per-tick (not persisted): past MAX_TOMBSTONE_PAGES in one tenant the
 *    tick reports overflow and resumes from scratch next tick, which is
 *    bounded and idempotent (documented follow-up if a tenant ever exceeds
 *    pages-per-tick backlog in production).
 */
export async function reconcileTombstonedScopes(
  db: SqlExecutor,
  runInTenant: TenantRunner,
  backendId: string,
  keyPrefix: string,
  limit: number,
  send: (body: string) => Promise<void>,
): Promise<{ tombstoned: number; overflow: boolean }> {
  const { rows: tenants } = await db.query<{ orgId: string; workspaceId: string }>(
    `SELECT DISTINCT org_id AS "orgId", workspace_id AS "workspaceId"
     FROM sessions.session_index WHERE deleted_at IS NOT NULL`,
  );
  let tombstoned = 0;
  let overflow = false;
  for (const tenant of tenants) {
    const out = await runInTenant(tenant.orgId, tenant.workspaceId, async (tx) => {
      const retired = await tx.query<{ n: string }>(
        `WITH retired AS (
           DELETE FROM search_index.projection_state s
           USING sessions.session_index p
           WHERE s.backend_id = $1
             AND s.org_id = $2 AND s.workspace_id = $3
             AND s.org_id = p.org_id AND s.workspace_id = p.workspace_id
             AND s.source = p.harness AND s.session_id = coalesce(p.canonical_session_id, p.session_id)
             AND p.deleted_at IS NOT NULL AND p.manifest_key LIKE $4 ESCAPE '\\'
             AND s.complete AND s.row_count = 0
             AND NOT EXISTS (SELECT 1 FROM search_index.transcript_events t
               WHERE t.org_id = p.org_id AND t.workspace_id = p.workspace_id
                 AND t.source = p.harness AND t.session_id = coalesce(p.canonical_session_id, p.session_id))
           RETURNING 1)
         SELECT count(*) AS n FROM retired`,
        [backendId, tenant.orgId, tenant.workspaceId, `${likeEscape(keyPrefix)}/%`],
      );
      let sent = 0;
      let capped = false;
      let cursor: string | null = null;
      for (let page = 0; ; page += 1) {
        const pageRows: { rows: { manifest_key: string | null }[] } = await tx.query(
          `SELECT DISTINCT p.manifest_key FROM sessions.session_index p
           WHERE p.deleted_at IS NOT NULL AND p.org_id = $5 AND p.workspace_id = $6
             AND p.manifest_key LIKE $3 ESCAPE '\\'
             AND ($4::text IS NULL OR p.manifest_key > $4)
             AND (EXISTS (SELECT 1 FROM search_index.projection_state s
                    WHERE s.backend_id=$1 AND s.org_id=p.org_id AND s.workspace_id=p.workspace_id
                      AND s.source=p.harness AND s.session_id=coalesce(p.canonical_session_id,p.session_id)
                      AND (NOT s.complete OR s.row_count > 0))
               OR EXISTS (SELECT 1 FROM search_index.transcript_events t
                    WHERE t.org_id=p.org_id AND t.workspace_id=p.workspace_id
                      AND t.source=p.harness AND t.session_id=coalesce(p.canonical_session_id,p.session_id)))
           ORDER BY p.manifest_key LIMIT $2`,
          [backendId, limit, `${likeEscape(keyPrefix)}/%`, cursor, tenant.orgId, tenant.workspaceId],
        );
        const rows = pageRows.rows;
        if (rows.length === 0) break;
        let last: string | null = null;
        for (const { manifest_key } of rows) {
          if (!manifest_key) continue;
          last = manifest_key;
          sent += 1;
          await send(reconcileBody(manifest_key));
        }
        if (rows.length < limit) break;
        // A null key can never advance the cursor; without a non-null key the
        // next page would repeat this one, so stop instead of looping.
        if (last === null) break;
        cursor = last;
        if (page + 1 >= MAX_TOMBSTONE_PAGES) {
          capped = true;
          break;
        }
      }
      return { retired: Number(retired.rows[0]?.n ?? 0), sent, capped, cursor };
    });
    if (out.retired > 0) log.info("pipeline consumer tombstone residue retired", { retired: out.retired });
    tombstoned += out.sent;
    if (out.capped) {
      overflow = true;
      // The tombstone cursor is tick-local by design: next tick restarts the
      // listing from scratch (bounded and idempotent) instead of resuming.
      log.warn("pipeline consumer tombstone backlog overflow; restarts from scratch next tick", { cursor: out.cursor, tombstoned });
    }
  }
  return { tombstoned, overflow };
}

/** Consecutive failing ticks before the process exits non-zero (visible restart, not silent stall). */
export const MAX_CONSECUTIVE_TICK_FAILURES = 5;

/** Consecutive-failure accounting: reset on success, increment on failure. */
export function nextFailureCount(previous: number, failed: boolean): number {
  return failed ? previous + 1 : 0;
}

/** Fail-closed startup gate: the owner bootstrap must have created the writer's tables. */
export async function assertServiceSchema(db: SqlExecutor): Promise<void> {
  for (const table of ["sessions.search_consumer_pending", "sessions.search_consumer_progress"]) {
    const { rows } = await db.query<{ present: boolean }>(`SELECT to_regclass($1) IS NOT NULL AS present`, [table]);
    if (!rows[0]?.present)
      throw new Error(
        `pipeline consumer: ${table} is missing; run SERVICE=session-pipeline-consumer-bootstrap first`,
      );
  }
}

/**
 * The Deployment must hold the dedicated writer URL: an owner, superuser or
 * reader session_user fails closed here, before any read or write. Exported
 * so the refusal runs against real Postgres in tests.
 */
export async function assertRuntimeIdentity(db: SqlExecutor): Promise<void> {
  const { rows: who } = await db.query<{ user: string; super: boolean }>(
    `SELECT session_user AS user, (SELECT rolsuper FROM pg_roles WHERE rolname = session_user) AS super`,
  );
  if (who[0]?.user !== "search_consumer_runtime" || who[0]?.super)
    throw new Error(
      `pipeline consumer: refusing to run as session_user '${who[0]?.user ?? "unknown"}'; use the search_consumer_runtime URL`,
    );
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<never> {
  const config = consumerServiceConfig(env);
  if (!config.enabled) {
    // Dormant but observable, like the read API: no database, no polling, a
    // plain health endpoint only. /healthz answers 200 so the process is not
    // restart-looped by a tick clock; everything else is 503 (including
    // /readyz) so a misconfigured deployment stays out of rotation.
    // The chart does not deploy this SERVICE while dormant.
    const port = integer("WORKER_HEALTH_PORT", 8081);
    const server = createServer((req, res) => {
      if (req.url === "/healthz") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, dormant: true, service: "session-pipeline-consumer" }));
        return;
      }
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, dormant: true }));
    });
    server.listen(port, "0.0.0.0", () => log.info("pipeline consumer dormant", { service: "session-pipeline-consumer", port }));
    await new Promise(() => {});
    throw new Error("unreachable");
  }
  // runWorker opens its own pool from HOOPDB_URL for the live gates: it must
  // be the same identity that writes, never a second role or database.
  if (env.HOOPDB_URL && env.HOOPDB_URL !== config.secret.hoopdb_url)
    throw new Error("pipeline consumer: HOOPDB_URL must match the consumer secret hoopdb_url");
  env.HOOPDB_URL ??= config.secret.hoopdb_url;

  const expected = await expectedSchema();
  const tls = postgresTlsOptions();
  const db = supabasePool(config.secret.hoopdb_url, 4, tls);
  await assertRuntimeIdentity(db);
  await assertServiceSchema(db);
  // A healthy start clears a stale UNHEALTHY marker from an earlier crash
  // loop; without this the marker is sticky across restarts.
  clearUnhealthyFile(config.unhealthyFile);
  const composition = writerComposition(config.backendId, config.floorMs);
  const writerSql = writerTransactionSql(db);
  const tenantRunner = writerTenantRunner(db);
  const binding = composeSearchService(
    composition,
    {
      env: { SEARCH_INDEX_BACKEND: "postgres", [CONSUMER_BACKEND_ID_VAR]: config.backendId },
      sql: writerSql.sql,
      secrets: searchSecret(config.secret.search),
    },
    "writer",
    tenantRunner,
  );

  const raw = lockerStore();
  const reader = lockerManifestReader();
  const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: config.keyPrefix };
  const lock: ScopeLock = (scope, work) =>
    transaction(db, async (tx) => {
      await tx.query("SET LOCAL lock_timeout = '30s'");
      await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [scopeLockKey(scope)]);
      return work();
    });
  const pointer: PointerDeps = {
    db,
    lock,
    reader,
    exists: async (key) => (await raw.store.head(objectKey(key))) !== undefined,
    orgs: prefixOrgResolver,
    allowPrefix: (prefix, orgId) => prefixAllowed(bindings, prefix, orgId),
    // Terminal consumer: fanout is always dropped (config refuses postgres).
    fanout: droppedFanout,
    publication: {
      schemaGeneration: expected.generation,
      transact: (work) =>
        inTransaction(db, async (tx) => {
          await tx.query("SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s'");
          return work(tx);
        }),
    },
  };
  const search: ConsumerDeps["search"] = {
    transact: (work) => transaction(db, (tx) => writerSql.run(tx, () => work(tx))),
    reader,
    store: raw.store,
    adapter: binding,
    prefixFor: (orgId) => `${config.keyPrefix}/${orgId}`,
  };

  let lastRun = 0;
  let consecutiveFailures = 0;
  log.info("pipeline consumer starting", { service: "session-pipeline-consumer", generation: expected.generation });

  let exitCode = 0;
  try {
    await runWorker({
      service: "session-pipeline-consumer",
      stages: [],
      handlers: () => ({}),
      tick: async () => {
        const now = Date.now();
        if (now - lastRun < config.everyMinutes * 60_000) return;
        lastRun = now;
        const lookbackMs = config.lookbackMinutes * 60_000;
        try {
          const mark = await loadHighWaterMark(db, config.backendId);
          // An interrupted listing resumes its own fixed since/window instead
          // of restarting: the stored cursor is only reused while the window
          // matches, so a config change restarts the listing from scratch.
          const stored = await loadReconcileCursor(db, config.backendId);
          const resume = stored !== null && stored.lookbackMinutes === config.lookbackMinutes;
          const since = resume && stored !== null ? stored.since : computeSince(now, lookbackMs, mark);
          const due = await loadPendingDue(db, config.backendId, MAX_PENDING_ROWS, new Date(now));
          // Freshness and tombstone re-sends skip keys still inside their
          // backoff: without the gate every tick re-sent a failing key and
          // next_at never slowed the retry loop.
          const deferred = await loadPendingDeferred(db, config.backendId, new Date(now));
          const pending: string[] = [];
          const requeued: string[] = [];
          // Dead letters stay dead unless a NEW etag/version arrives (a genuinely
          // new revision revives the key; the same revision never resurrects,
          // and an etag-less body never revives at all).
          const dedupe = makeDedupe(db, config.backendId);
          const summary = await runConsumerTick(
            {
              reconcile: async (send, _since) => {
                for (const row of due) {
                  await send(reconcileBody(row.manifestKey, row.manifestEtag, row.manifestVersion));
                }
                const link = await reconcileRecent({
                  db,
                  store: raw.store,
                  keyPrefix: config.keyPrefix,
                  send,
                  since,
                  ...(resume && stored !== null ? { cursor: stored.cursor } : {}),
                });
                return { enqueued: link.enqueued + due.length, complete: link.complete, ...(link.nextCursor === undefined ? {} : { nextCursor: link.nextCursor }) };
              },
              checkFreshness: async (send) => {
                const staleGated = gateDeferredSends(deferred, send);
                const freshness = await reconcileStaleProjections(
                  db,
                  (tenant, after) => binding.state.partials(tenant, { limit: 1000, ...(after === undefined ? {} : { after }) }),
                  config.keyPrefix,
                  staleGated.send,
                );
                const tombGated = gateDeferredSends(deferred, send);
                const tombstones = await reconcileTombstonedScopes(
                  db,
                  tenantRunner,
                  config.backendId,
                  config.keyPrefix,
                  MAX_PENDING_ROWS,
                  tombGated.send,
                );
                const deferredSkipped = staleGated.skipped() + tombGated.skipped();
                if (deferredSkipped) log.info("pipeline consumer tick defers backoff retries", { deferredSkipped });
                return { stale: freshness.stale, tombstoned: tombstones.tombstoned, overflow: freshness.overflow || tombstones.overflow };
              },
              dedupe,
              consume: (records) =>
                processBatch(records, {
                  pointer,
                  search,
                  requeue: async (key) => {
                    requeued.push(JSON.stringify({ [RECONCILE_BODY_KEY]: { key } }));
                  },
                }),
              pending,
              requeued,
              now: () => now,
            },
            since,
          );
          const at = new Date(now);
          const failedKeys = new Set(summary.failedKeys);
          const requeuedKeys = new Set(summary.requeuedKeys);
          const known = await loadPendingMap(
            db,
            config.backendId,
            [...new Set([...summary.failedKeys, ...summary.requeuedKeys])],
          );
          // Success is computed over exactly the keys actually consumed this
          // tick: a body dedupe skipped was never consumed, so it stays (a
          // successful consume is the only thing that may clear a row,
          // including a dead letter).
          const succeeded = summary.consumedKeys.filter((k) => !failedKeys.has(k) && !requeuedKeys.has(k));
          await deletePending(db, config.backendId, succeeded);
          const store: PendingOutcome[] = [];
          for (const item of summary.failedItems) {
            const prev = known.get(item.key);
            const { attempts, dead } = scoreFailedAttempt(
              prev === undefined ? undefined : { attempts: prev.attempts, etag: prev.etag, version: prev.version },
              { etag: item.etag, version: item.version },
            );
            store.push({
              manifestKey: item.key,
              manifestEtag: item.etag,
              manifestVersion: item.version,
              attempts,
              nextAt: new Date(now + retryDelayMs(attempts)),
              lastErrorCode: "consume_failed",
              dead: attempts > MAX_ATTEMPTS,
            });
          }
          for (const item of summary.requeuedItems) {
            if (failedKeys.has(item.key)) continue; // failure accounting already stored it
            const prev = known.get(item.key);
            const { attempts: requeuedAttempts, dead: requeuedDead } = scoreRequeuedAttempt(
              prev === undefined
                ? undefined
                : { attempts: prev.attempts, dead: prev.dead, etag: prev.etag, version: prev.version },
              { etag: item.etag, version: item.version },
            );
            store.push({
              manifestKey: item.key,
              manifestEtag: item.etag,
              manifestVersion: item.version,
              attempts: requeuedAttempts,
              nextAt: new Date(now + 60_000),
              lastErrorCode: "scope_busy",
              dead: requeuedDead,
            });
          }
          await storePending(db, config.backendId, store, at);
          if (summary.complete) {
            await storeHighWaterMark(db, config.backendId, at);
            await storeReconcileCursor(db, config.backendId, null, at);
          } else if (summary.nextCursor !== undefined) {
            await storeReconcileCursor(
              db,
              config.backendId,
              { cursor: summary.nextCursor, since, lookbackMinutes: config.lookbackMinutes },
              at,
            );
          }
          // Attempts are persisted above, so rethrowing still converges: the
          // failure counter below counts this tick, and five in a row exit.
          if (summary.batchError !== undefined) throw summary.batchError;
          consecutiveFailures = nextFailureCount(consecutiveFailures, false);
          log.info("pipeline consumer tick", {
            collected: summary.collected,
            failed: summary.failed,
            requeued: summary.requeued,
            stale: summary.stale,
            tombstoned: summary.tombstoned,
            overflow: summary.overflow,
            pending: summary.pending,
            complete: summary.complete,
          });
        } catch (error) {
          consecutiveFailures = nextFailureCount(consecutiveFailures, true);
          log.error("pipeline consumer tick failed; next poll retries", {
            error_code: (error as { code?: string })?.code ?? "tick_failed",
            consecutive_failures: consecutiveFailures,
          });
          // A tick that keeps failing is a stuck worker, not a healthy idle
          // one. runWorker swallows side-tick throws and health only tracks
          // DB probes, so give up loudly here: mark the UNHEALTHY file for
          // the compose healthcheck/watcher, close the pool, and exit
          // non-zero so the restart is visible instead of looping forever.
          if (consecutiveFailures >= MAX_CONSECUTIVE_TICK_FAILURES) {
            try {
              writeFileSync(config.unhealthyFile, `${new Date(now).toISOString()} consecutive_failures=${consecutiveFailures}\n`);
            } catch {}
            log.error("pipeline consumer giving up after consecutive tick failures", {
              consecutive_failures: consecutiveFailures,
              unhealthy_file: config.unhealthyFile,
            });
            try {
              await db.end();
            } catch {}
            process.exit(1);
          }
        }
      },
    });
  } catch (error) {
    log.error("pipeline consumer stopping with error", {
      error_code: (error as { code?: string })?.code ?? "fatal",
    });
    exitCode = 1;
  } finally {
    try {
      await db.end();
    } catch {}
  }
  process.exit(exitCode);
}

if (import.meta.main) await serve();
