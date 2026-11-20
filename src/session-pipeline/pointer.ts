/**
 * Stage 1 for an S3 event: the pipeline's `object-landed` handler, then
 * `session-indexed` through the HoopDb pointer index, run in process instead
 * of across two Postgres queue hops. The handler code is the same the
 * hoopdb-indexer runs (packages/pipeline stages.ts + src/hoopdb/index.ts):
 * the Supabase row is written only by `sessions.upsert_session_index`
 * (revision-fenced: a late, duplicated or replayed event never rolls a row
 * back), then `session_index_ext` and the logical `session_directory` row.
 *
 * Serialization. The read and the upsert run under the session's pointer lock
 * (a transaction-scoped advisory lock in Supabase), so a put
 * whose read raced an erasure cannot upsert after the delete's tombstone: the
 * second holder re-reads S3 and sees what the first left. Without it, a
 * revision-2 upsert that read before the delete would land after the
 * tombstone and bring the row back for good (`upsert_session_index` lets a
 * newer revision undelete).
 *
 * The event is a hint. The manifest is always re-read, so:
 *   - a put whose manifest is gone again is handled as a delete;
 *   - a delete whose key still has a current manifest (an old version deleted
 *     by id, or a re-upload) indexes the current manifest instead.
 */
import {
  buildObjectDeletedMessage,
  buildObjectLandedMessage,
  objectLandedHandler,
  PipelineError,
  PipelineRecorder,
  logOnlyPipelineEventSink,
  manifestKey,
  parseSessionKey,
  type ManifestReader,
  type OrgResolver,
  type PipelineQueue,
  type StageMessage,
} from "../shared/vendor.ts";
import { RETENTION_BUCKET_HARNESS, type ObjectLanded } from "@rhc/lib/pipeline/adapter.ts";
import type { HoopDb,Transact } from "../shared/db.ts";
import { enrichMessage } from "../shared/enrich.ts";
import { hoopDbSessionIndex } from "../hoopdb/index.ts";

export interface SessionScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}

export type PointerOutcome =
  | { status: "ignored"; reason: string }
  | { status: "indexed" | "unchanged" | "deleted" | "moved"; scope: SessionScope; manifestKey: string; revision?: number };

/**
 * The per-session lock keys. The pointer stage and the search stage each
 * serialize among themselves. They need not exclude each other, because a
 * search pass takes its lock after its own event's pointer committed. Keeping
 * them apart means a long search pass never holds up a pointer write.
 */
export function scopeLockKey(scope: SessionScope, stage: "pointer" | "search" = "pointer"): string {
  return [`96bulls:${stage}`, scope.orgId, scope.workspaceId, scope.harness, scope.sessionId].join("\u001f");
}

/** Run `work` while holding the session's lock (lambda.ts: pg_advisory_xact_lock in Supabase). */
export type ScopeLock = <T>(scope: SessionScope, work: () => Promise<T>) => Promise<T>;

export interface PointerDeps {
  db: HoopDb;
  lock: ScopeLock;
  reader: ManifestReader;
  /** HEAD: whether a manifest key has a current object (the move probe). */
  exists(key: string): Promise<boolean>;
  orgs: OrgResolver;
  allowPrefix(prefix: string | undefined, orgId: string): boolean;
  /** Where the index's fan-out (summary-short, summary-long, status-classify) goes. */
  fanout: PipelineQueue;
  publication?:{schemaGeneration:string;transact:Transact};
  now?: () => Date;
  /**
   * SESSION_POINTER_PREREAD_ENABLED=true only (the hosted Lambda): every object-store call of the stage runs
   * BEFORE the scope lock, and the lock transaction holds SQL only (indexPreRead). Absent: as before, the
   * manifest is re-read under the lock.
   */
  preRead?: boolean;
}

/** A fan-out queue that drops messages: no summary or state consumer runs in production yet. */
export const droppedFanout: PipelineQueue = {
  backend: "memory",
  async send() {},
  async sendBatch() {},
};

export async function indexPointer(landed: ObjectLanded, deps: PointerDeps): Promise<PointerOutcome> {
  const location = parseSessionKey(landed.key);
  if (!location) return { status: "ignored", reason: "not_a_session_key" };
  if(location.prefix?.includes('/upload-generations/'))return{status:"ignored",reason:"managed_publication_hint"};
  // Babysitter retention buckets (src/session-ingest/babysitter-retention.ts) are not sessions.
  if (location.harness === RETENTION_BUCKET_HARNESS) return { status: "ignored", reason: "retention_bucket" };
  if (location.rest !== "manifest.json") return { status: "ignored", reason: "not_a_manifest" };
  if (landed.bucket && deps.reader.bucket && landed.bucket !== deps.reader.bucket)
    return { status: "ignored", reason: "other_bucket" };
  const orgId = await deps.orgs.orgFor(location);
  // A key under the deployment prefix always names its org; no org means a key
  // this deployment does not own.
  if (!orgId) return { status: "ignored", reason: "org_unresolved" };
  if (!deps.allowPrefix(location.prefix, orgId)) return { status: "ignored", reason: "outside_prefix" };
  // A legacy key of an adopted managed carrier must not update ext/fanout either.
  if(location.machineId.startsWith('srv_')){
    const managed=await deps.db.query<{managed:boolean}>("SELECT sessions.session_publication_is_managed($1,$2,$3,$4,$5) AS managed",
      [orgId,location.workspaceId,location.harness,location.sessionId,location.machineId]);
    if(managed.rows[0]?.managed===true)return{status:"ignored",reason:"managed_publication_hint"};
  }
  const scope: SessionScope = {
    orgId,
    workspaceId: location.workspaceId,
    harness: location.harness,
    sessionId: location.sessionId,
  };

  if (deps.preRead) return indexPreRead(landed, orgId, scope, deps);
  return deps.lock(scope, () => indexLocked(landed, orgId, scope, deps));
}

/**
 * The scope's pointer rows as one comparable value: every pointer write to the scope (an insert, a newer
 * revision, a tombstone, a rekey) changes it, because each moves indexed_at (and revision, deleted_at or
 * manifest_key). Read before the object-store reads and again under the lock.
 */
async function scopeStamp(db: HoopDb, scope: SessionScope): Promise<string> {
  const { rows } = await db.query<{ stamp: string }>(
    `SELECT coalesce(jsonb_agg(jsonb_build_array(machine_id, manifest_key, revision::text, manifest_etag, deleted_at, indexed_at)
              ORDER BY machine_id), '[]'::jsonb)::text AS stamp
       FROM sessions.session_index WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
  return rows[0]?.stamp ?? "[]";
}

/** The object-store results of one attempt, by key: a manifest read (null: gone) and a HEAD. */
interface PreReadCache {
  reads: Map<string, Awaited<ReturnType<ManifestReader["read"]>>>;
  heads: Map<string, boolean>;
}

/**
 * Every object-store call indexLocked would make for this event, made now (outside the lock) in the same
 * order: the manifest; if it is gone, the move probe and the sibling's manifest; then, for a live manifest,
 * the stored row of its machine and, when that row names another live key, that key's manifest. The
 * enrich and upsert reads are of the same keys.
 */
async function preReadEvent(landed: ObjectLanded, orgId: string, scope: SessionScope, deps: PointerDeps): Promise<PreReadCache> {
  const cache: PreReadCache = { reads: new Map(), heads: new Map() };
  const read = async (key: string) => {
    if (!cache.reads.has(key)) cache.reads.set(key, await deps.reader.read(key));
    return cache.reads.get(key)!;
  };
  const exists = async (key: string) => {
    if (!cache.heads.has(key)) cache.heads.set(key, await deps.exists(key));
    return cache.heads.get(key)!;
  };
  let key = landed.key;
  let got = await read(key);
  if (!got) {
    const sibling = await findMovedSibling(key, exists);
    if (sibling) {
      key = sibling;
      got = await read(sibling);
    }
  }
  if (got) {
    const stored = (
      await deps.db.query<{ manifest_key: string; deleted: boolean }>(
        `SELECT manifest_key, deleted_at IS NOT NULL AS deleted FROM sessions.session_index
          WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
        [orgId, scope.workspaceId, scope.harness, scope.sessionId, got.manifest.session.machine_id],
      )
    ).rows[0];
    if (stored && stored.manifest_key !== key && !stored.deleted) await read(stored.manifest_key);
  }
  return cache;
}

/** Under the lock, object-store calls answer only from the attempt's pre-read; anything else is a bug. */
function replayDeps(deps: PointerDeps, cache: PreReadCache): PointerDeps {
  const miss = (key: string) => new PipelineError("pointer_preread_miss", `no pre-read for ${key}`, true);
  return {
    ...deps,
    reader: {
      ...deps.reader,
      read: async (key: string) => {
        if (!cache.reads.has(key)) throw miss(key);
        return cache.reads.get(key)!;
      },
    },
    exists: async (key: string) => {
      if (!cache.heads.has(key)) throw miss(key);
      return cache.heads.get(key)!;
    },
  };
}

const SCOPE_CHANGED = Symbol("scope_changed");

/**
 * SESSION_POINTER_PREREAD_ENABLED: the object-store reads before the scope lock, a short lock that holds
 * SQL only. The scope's pointer rows are stamped before the reads and re-read under the lock: unchanged
 * means no other pointer stage wrote this scope since the reads began, so indexLocked runs exactly as
 * before against the pre-read (the revision fence still answers a non-newer revision); changed means the
 * reads may be stale (the put whose read raced an erasure), so they are made again, once. A second change
 * falls back to the original locked read: contention can make the optimization miss, but cannot send an
 * otherwise valid event toward the dead-letter queue.
 */
async function indexPreRead(landed: ObjectLanded, orgId: string, scope: SessionScope, deps: PointerDeps): Promise<PointerOutcome> {
  for (let attempt = 0; attempt < 2; attempt++) {
    const stamp = await scopeStamp(deps.db, scope);
    const cache = await preReadEvent(landed, orgId, scope, deps);
    const out = await deps.lock(scope, async () => {
      if ((await scopeStamp(deps.db, scope)) !== stamp) return SCOPE_CHANGED;
      return indexLocked(landed, orgId, scope, replayDeps(deps, cache));
    });
    if (out !== SCOPE_CHANGED) return out;
  }
  // The flag is an optimization. Under sustained churn, preserve the base path's liveness instead of
  // inventing a new retry/DLQ route. This rare fallback deliberately reads under the original lock.
  return deps.lock(scope, () => indexLocked(landed, orgId, scope, deps));
}

/** How far either side of a deleted key's date the same session is looked for (an exporter date move). */
export const MOVE_PROBE_DAYS = 45;

/**
 * The same session (same machine) live under another start date, if any. The
 * exporter re-keys a session whose first record moved earlier, writes the new
 * key, then erases the old prefix; the pointer row (whose identity has no date)
 * must follow the new key, not be tombstoned with the old one. Returns the
 * nearest live key, or undefined.
 */
export async function findMovedSibling(key: string, exists: (key: string) => Promise<boolean>): Promise<string | undefined> {
  const loc = parseSessionKey(key);
  if (!loc) return undefined;
  const base = Date.parse(`${loc.startDate}T00:00:00Z`);
  const offsets: number[] = [];
  for (let d = 1; d <= MOVE_PROBE_DAYS; d++) offsets.push(-d, d);
  for (let i = 0; i < offsets.length; i += 10) {
    const batch = offsets.slice(i, i + 10).map((d) => manifestKey({ ...loc, startDate: new Date(base + d * 86_400_000).toISOString().slice(0, 10) }));
    const found = await Promise.all(batch.map(async (k) => ((await exists(k)) ? k : undefined)));
    const hit = found.find((k) => k !== undefined);
    if (hit) return hit;
  }
  return undefined;
}

async function indexLocked(landed: ObjectLanded, orgId: string, scope: SessionScope, deps: PointerDeps, followed = false): Promise<PointerOutcome> {
  // Canonical state, not the event: re-read the manifest, under the lock.
  const read = await deps.reader.read(landed.key);
  if (!read && !followed) {
    // Gone. Before tombstoning, look for the same session under another date.
    const sibling = await findMovedSibling(landed.key, deps.exists);
    if (sibling) {
      const { etag: _e, version: _v, size: _s, deleted: _d, ...rest } = landed;
      return indexLocked({ ...rest, key: sibling }, orgId, scope, deps, true);
    }
  }
  const built = read
    ? buildObjectLandedMessage({
        landed: { ...landed, deleted: false },
        orgId,
        manifest: read.manifest,
        read: { ...(read.etag ? { etag: read.etag } : {}), ...(read.version ? { version: read.version } : {}) },
      })
    : buildObjectDeletedMessage({
        landed: { ...landed, deleted: true },
        orgId,
        ...(deps.now ? { now: deps.now() } : {}),
      });
  if (built.status === "ignored") return { status: "ignored", reason: built.reason };
  const landedMessage = await enrichMessage(built.message, deps.reader);

  let indexed: StageMessage | undefined;
  const recorder = new PipelineRecorder(landedMessage.ctx, { sink: logOnlyPipelineEventSink });
  await objectLandedHandler({
    senders: {
      async send(stage, message) {
        if (stage === "session-indexed") indexed = message;
      },
    },
    quarantine: async () => {
      throw new PipelineError("transform_disabled", "file-normalize is not enabled here", false);
    },
    transformEnabled: false,
  })(landedMessage, recorder, 1);
  if (!indexed) return { status: "ignored", reason: "no_pointer" };
  const message = await enrichMessage(indexed, deps.reader);

  const index = hoopDbSessionIndex({ db: deps.db, reader: deps.reader, queue: deps.fanout, current: () => message,publication:deps.publication });
  const upsert = () => index.upsertRawPointer(message.payload as Parameters<typeof index.upsertRawPointer>[0], message.ctx);
  const result = await upsert();
  if(result?.status==="skipped"&&result.reason==="publication_retired")return{status:"ignored",reason:"managed_publication_hint"};
  const revision = message.revision;
  if (!read) return { status: "deleted", scope, manifestKey: landed.key };

  // A moved session: the row names another key of the same identity (the start
  // date, and so `d=`, changed), and the revision fence kept this key out,
  // because a new key restarts its revision count. The key that is live in S3
  // wins; while both are live the move is in progress and the event retries.
  const pk = [orgId, scope.workspaceId, scope.harness, scope.sessionId, read.manifest.session.machine_id];
  const stored = (
    await deps.db.query<{ manifest_key: string; deleted: boolean }>(
      `SELECT manifest_key, deleted_at IS NOT NULL AS deleted FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
      pk,
    )
  ).rows[0];
  if (stored && stored.manifest_key !== landed.key) {
    const otherLive = !stored.deleted && (await deps.reader.read(stored.manifest_key)) !== null;
    if (otherLive)
      throw new PipelineError("pointer_move_pending", "the session's other key is still live; retry after its erasure", true);
    // The stored key is gone (or tombstoned): re-key the row to this one. The
    // row is derived, and the upsert rebuilds it from this manifest. The delete
    // and the re-insert run inside the upsert's transaction (the publication
    // transaction, or a pooled one; a test double without a pool runs them in
    // sequence), and the new row keeps least(old, new) created_at (first-seen
    // survives the move: the analysis cutover proof, 20261105062120). Every
    // outcome that inserts no new row throws inside that transaction, so the
    // delete rolls back and the old row stays.
    let moved: Awaited<ReturnType<typeof index.upsertRawPointer>>;
    try {
      moved = await index.upsertRawPointer(message.payload as Parameters<typeof index.upsertRawPointer>[0], message.ctx, { rekeyFrom: stored.manifest_key });
    } catch (error) {
      // An erased (fenced) or guarded session: terminal. The old row (its tombstone) stays; retrying
      // would only repeat the refusal until the dead-letter queue.
      if ((error as { code?: unknown }).code === "pointer_move_fenced") {
        const why = (error as { reason?: unknown }).reason;
        return { status: "ignored", reason: why === "operator_erased" ? "operator_erased" : "move_guarded" };
      }
      throw error;
    }
    // Skipped before the delete ran (nothing changed). A retired managed publication is terminal, as
    // on the in-place path; anything else (a manifest read that vanished) is retried.
    if (moved && moved.status === "skipped") {
      if (moved.reason === "publication_retired") return { status: "ignored", reason: "managed_publication_hint" };
      throw new PipelineError("pointer_move_skipped", `the session's move was not applied (${moved.reason}); retry`, true);
    }
    return { status: "moved", scope, manifestKey: landed.key, ...(revision !== undefined ? { revision } : {}) };
  }
  return {
    status: result && result.status === "skipped" ? "unchanged" : "indexed",
    scope,
    manifestKey: landed.key,
    ...(revision !== undefined ? { revision } : {}),
  };
}
