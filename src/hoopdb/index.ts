/**
 * The session pointer index for HoopDb (upstream has only test fakes): reads the
 * manifest a `session-indexed` message points at, upserts the row through the
 * vendored `sessions.upsert_session_index`, records the session uuid in
 * `session_index_ext`, then fans out to Bob Costas (summaries) and the Referee
 * (state) with the same trace and session ids.
 */
import {
  pointerFields,
  postgresPipelineQueue,
  type ManifestReader,
  type PipelineQueue,
  type RawPointer,
  type SessionPointerIndex,
  type StageMessage,
  type TraceContext,
} from "../shared/vendor.ts";
import { createHash } from "node:crypto";
import { pipelineEventKey } from "@rhc/lib/pipeline/events.ts";
import type { HoopDb,Transact } from "../shared/db.ts";
import {publicationEvidence,withPublicationAuthority,type PublicationEvidence}from "../shared/publication-authority.ts";
import { ctxOf, enrichMessage, sessionExt } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { uuidv7 } from "../shared/ids.ts";
import { propagatePointerTombstone } from "./session-directory-summary.ts";
import { projectSessionDirectory } from "./session-directory-projector.ts";
import { briefConfig } from "../session-brief/config.ts";
import { armWakeup } from "../session-brief/store.ts";
import { ERASE_FENCE_FLAG, eraseRuntimeFlag } from "../session-erase/flags.ts";
import {
  canonicalManifestIdentity,
  manifestUserId,
} from "../shared/manifest-identity.ts";

export const FAN_OUT_STAGES = [
  "summary-short",
  "summary-long",
  "status-classify",
] as const;

export interface IndexDeps {
  db: HoopDb;
  reader: ManifestReader;
  queue: PipelineQueue;
  /** The message being handled, so fan-out continues its trace and ids. */
  current: () => StageMessage | undefined;
  /** Compiled schema identity and one short final metadata transaction. */
  /**
   * `enqueueSearchWake`: write the managed `search-index` wake inside the authority transaction
   * (docs/handoffs/incremental-search-indexing.md §2; needs migration 20261105062230). Absent means on, so
   * every production caller passes it explicitly from its own flag (lambda.ts SEARCH_WAKE_ENQUEUE, indexer.ts
   * PIPELINE_MANAGED_SEARCH_WAKE_ENABLED, consumer-service.ts SEARCH_WAKE_ENQUEUE; each default off).
   */
  publication?:{schemaGeneration:string;transact:Transact; enqueueSearchWake?:boolean};
  /**
   * Session briefs (src/session-brief): the wakeup debounce, or null when briefs are off.
   * Defaults to SESSION_BRIEF_ENABLED and its timings from env.
   */
  brief?: { idleSeconds: number; maxWaitSeconds: number } | null;
}

/**
 * A pointer move (src/session-pipeline/pointer.ts): the row still names `rekeyFrom`, an older key of
 * the same identity. The upsert deletes that row and inserts the new one in ONE transaction, keeping
 * least(old, new) created_at, so a failure in between leaves the old row and its first-seen intact.
 */
export interface UpsertOptions { rekeyFrom?: string }
export type HoopDbSessionPointerIndex = SessionPointerIndex & {
  upsertRawPointer(pointer: RawPointer, ctx: TraceContext, opts?: UpsertOptions): ReturnType<SessionPointerIndex["upsertRawPointer"]>;
};

/**
 * One transaction on one pooled connection (shared/db.ts inTransaction, inlined: importing db.ts
 * as a value pulls its schema-asset reads into the large-read worker bundle).
 */
async function moveTransaction<T>(db: HoopDb, work: (tx: { query: HoopDb["query"] }) => Promise<T>): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const result = await work({
      query: (async (text: string, params?: unknown[]) => ({ rows: (await client.query(text, params as unknown[])).rows })) as HoopDb["query"],
    });
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/** A move the erase fence (or a guard) refuses: terminal, mapped to an ignored outcome by pointer.ts. */
const movedFenced = (reason: "operator_erased" | "move_guarded") =>
  Object.assign(new Error(`pointer move refused (${reason})`), { code: "pointer_move_fenced", reason, retryable: false });

const ROW_IDENTITY=(p:{org_id:string;workspace_id:string;harness:string;session_id:string;machine_id:string})=>JSON.stringify([p.org_id,p.workspace_id,p.harness,p.session_id,p.machine_id]);
export function hoopDbSessionIndex(deps: IndexDeps): HoopDbSessionPointerIndex {return sessionIndex(deps,false);}
type ManagedSearchWake={evidence:PublicationEvidence;generation:string};
function sessionIndex(deps:IndexDeps,authorized:boolean,managedSearch?:ManagedSearchWake):HoopDbSessionPointerIndex{
  const operatorErase = eraseRuntimeFlag(process.env, ERASE_FENCE_FLAG);
  const fromEnv = briefConfig();
  const brief =
    deps.brief !== undefined ? deps.brief : fromEnv.enabled ? fromEnv : null;
  return {
    async upsertRawPointer(pointer: RawPointer, ctx: TraceContext, opts: UpsertOptions = {}) {
      const pk = {
        org_id: ctx.orgId,
        workspace_id: pointer.workspaceId,
        harness: pointer.harness ?? ctx.harness ?? "unknown",
        session_id: pointer.sessionId ?? ctx.sessionId ?? "unknown",
        machine_id: pointer.machineId ?? ctx.machineId ?? "unknown",
      };
      if (pointer.deleted) {
        // Fenced on the deleted key: one row (org, ws, harness, session, machine)
        // can be fed by two keys when a session's start date moves (the d=
        // segment is in the key, not in the row's identity). Deleting the old
        // key must not tombstone a row that already names the new one.
        const { rows: tombstoned } = await deps.db.query(
          `UPDATE sessions.session_index SET deleted_at = now(), indexed_at = now()
            WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5
              AND manifest_key = $6
            RETURNING 1`,
          [
            pk.org_id,
            pk.workspace_id,
            pk.harness,
            pk.session_id,
            pk.machine_id,
            pointer.manifestKey,
          ],
        );
        if (tombstoned.length === 0) {
          log.info("pointer tombstone skipped", { stage: "session-indexed", reason: "row_names_another_key" });
          return { status: "skipped", reason: "row_names_another_key" };
        }
        // Propagate to the logical directory: clear the summary now (it may include this
        // machine's content), then re-project. With machines still live the projector
        // rebuilds metadata and memberships; with none left it tombstones the row and
        // deletes its memberships.
        const logical = {
          orgId: pk.org_id,
          workspaceId: pk.workspace_id,
          harness: pk.harness,
          sessionId: pk.session_id,
        };
        await propagatePointerTombstone(deps.db, logical);
        await projectSessionDirectory(deps.db, logical);
        return;
      }
      const read = await deps.reader.read(pointer.manifestKey);
      if (!read) return { status: "skipped", reason: "manifest_missing" };
      const m = read.manifest;
      if(!authorized&&deps.publication){
        const config=deps.publication;
        if(ROW_IDENTITY(pk)!==ROW_IDENTITY({org_id:ctx.orgId,workspace_id:m.session.workspace_id,harness:m.session.harness,session_id:m.session.session_id,machine_id:m.session.machine_id}))
          throw Object.assign(Error("publication_pointer_identity"),{code:"publication_pointer_identity"});
        const evidence=publicationEvidence(ctx.orgId,pointer.manifestKey,m,config.schemaGeneration);
        // Hydration above; all later reads use the same immutable RAM manifest.
        const cached:ManifestReader={provider:deps.reader.provider,bucket:deps.reader.bucket,async read(key){
          if(key!==pointer.manifestKey)throw new Error("publication_cached_manifest_key");return read;
        }};
        const final=await withPublicationAuthority(config.transact,evidence,async (tx,authority)=>{
          const db={...deps.db,query:tx.query.bind(tx)}as HoopDb;
          return sessionIndex({...deps,db,reader:cached,queue:postgresPipelineQueue(tx)},true,authority.managed && config.enqueueSearchWake !== false && authority.generation ? {evidence,generation:authority.generation} : undefined).upsertRawPointer(pointer,ctx,opts);
        });
        return final.current?final.value:{status:"skipped",reason:"publication_retired"};
      }
      if(!authorized&&pointer.manifestKey.includes('/upload-generations/'))
        throw Object.assign(Error("publication_authority_unconfigured"),{code:"publication_authority_unconfigured"});
      // A move without the publication transaction (no publication config): the delete and the
      // re-insert still commit together. (A test double without a pool runs them in sequence.)
      if(!authorized&&opts.rekeyFrom&&deps.db.pool)
        return moveTransaction(deps.db,tx=>sessionIndex({...deps,db:{...deps.db,query:tx.query}as HoopDb},true).upsertRawPointer(pointer,ctx,opts));
      const f = pointerFields(m);
      const ext = sessionExt(m);
      const canonical = canonicalManifestIdentity(m);
      const ectx = ctx as unknown as {
        user_id?: string;
        session_uuid?: string;
      };
      // Prefer identity from the authenticated request. Reconcile has no request identity,
      // so retain the server-side value, then use only validated server-admitted manifest
      // metadata. Never let the placeholder replace a real identity.
      const existing = (
        await deps.db.query<{
          session_uuid: string;
          user_id: string | null;
          fanout_revision: string;
        }>(
          `SELECT session_uuid::text, user_id, fanout_revision::text FROM sessions.session_index_ext
          WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5`,
          [
            pk.org_id,
            pk.workspace_id,
            pk.harness,
            pk.session_id,
            pk.machine_id,
          ],
        )
      ).rows[0];
      const recordedUser =
        existing?.user_id && existing.user_id !== "unknown"
          ? existing.user_id
          : undefined;
      const requestUser =
        ectx.user_id && ectx.user_id !== "unknown" ? ectx.user_id : undefined;
      const realUserId = requestUser ?? recordedUser ?? manifestUserId(m);
      const userId = realUserId ?? "unknown";
      const row = {
        ...pk,
        user_id: userId,
        project_id: null,
        title: null,
        state: null,
        state_classified_at: null,
        created_at: f.startedAt,
        last_activity_at: f.endedAt ?? m.raw.updated_at,
        git_repo: f.gitRemoteUrl,
        git_branch: null,
        git_commit: null,
        model: null,
        event_count: m.raw.chunks.reduce(
          (n: number, c: { records?: number }) => n + (c.records ?? 0),
          0,
        ),
        storage_provider: pointer.provider ?? deps.reader.provider ?? "s3",
        bucket: pointer.bucket ?? deps.reader.bucket ?? "",
        manifest_key: pointer.manifestKey,
        content_sha256: `\\x${f.contentSha256}`,
        raw_bytes: f.rawBytes,
        chunk_count: f.chunkCount,
        raw_format: m.raw.format,
        format_version: 1,
        normalized_version: f.normalizedVersion,
        scrubbed: f.scrubbed,
        manifest_etag: read.etag ?? pointer.manifestVersion ?? null,
        revision: f.manifestRevision,
        trace_id: f.traceId,
        deleted_at: null,
      };
      if (opts.rekeyFrom) {
        // The move's delete, in this upsert's transaction; the new row keeps the older first-seen.
        const removed = await deps.db.query<{ created_at: unknown }>(
          `DELETE FROM sessions.session_index
            WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5
              AND manifest_key = $6
            RETURNING created_at`,
          [pk.org_id, pk.workspace_id, pk.harness, pk.session_id, pk.machine_id, opts.rekeyFrom],
        );
        const before = removed.rows[0]?.created_at;
        const prior = before instanceof Date ? before.getTime() : before == null ? Number.NaN : Date.parse(String(before));
        if (Number.isFinite(prior) && (row.created_at == null || prior < Date.parse(String(row.created_at))))
          row.created_at = new Date(prior).toISOString();
      }
      const { rows } = await deps.db.query<{
        applied: boolean;
        revision: string;
        fenced?: boolean;
      }>(
        operatorErase ? "SELECT applied,revision,fenced FROM sessions.ingest_operator_runtime_upsert($1::jsonb,$2,$3)"
          : "SELECT applied, revision FROM sessions.upsert_session_index($1::jsonb)",
        operatorErase ? [JSON.stringify([row]), canonical.sessionId, canonical.machineId] : [JSON.stringify([row])],
      );
      // A move (opts.rekeyFrom) has already deleted the old-key row in this transaction. Every outcome
      // that does not insert the new row must THROW, so the transaction rolls the delete back and the old
      // row (often a tombstone) stays: returning would commit the delete with no new row.
      if (rows[0]?.fenced === true) {
        if (opts.rekeyFrom) throw movedFenced("operator_erased");
        return { status: "skipped", reason: "operator_erased" };
      }
      const applied = rows[0]?.applied ?? false;
      if (opts.rekeyFrom && !applied) {
        // The old row is gone, so nothing could fence the insert by revision: a missing row means a
        // BEFORE INSERT guard suppressed it (20261105050010's erase-fence guard with the fence flag off,
        // or the publication pointer guard). That is terminal: retrying cannot change it.
        const present = await deps.db.query(
          `SELECT 1 FROM sessions.session_index
            WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
          [pk.org_id, pk.workspace_id, pk.harness, pk.session_id, pk.machine_id],
        );
        if (present.rows.length === 0) throw movedFenced("move_guarded");
        throw Object.assign(new Error("pointer move: the re-keyed row was not applied; retry"), { code: "pointer_move_not_applied", retryable: true });
      }
      // The customer-level id. Minted client-side; if an older client sent no
      // uuid we derive one here ONCE (reusing the stored one on later revisions) and say so.
      const sessionUuid =
        ext?.session_uuid ?? existing?.session_uuid ?? uuidv7();
      const source = ext ? ext.session_uuid_source : "derived";
      await deps.db.query(
        `INSERT INTO sessions.session_index_ext
           (org_id, workspace_id, harness, session_id, machine_id, session_uuid, session_uuid_source,
            tenant_id, user_id, first_trace_id, last_trace_id, last_revision)
         VALUES ($1,$2,$3,$4,$5,$6::uuid,$7,$8,$9,$10::uuid,$10::uuid,$11)
         ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
           last_trace_id = excluded.last_trace_id,
           last_revision = greatest(sessions.session_index_ext.last_revision, excluded.last_revision),
           user_id = coalesce(excluded.user_id, sessions.session_index_ext.user_id),
           updated_at = now()`,
        [
          pk.org_id,
          pk.workspace_id,
          pk.harness,
          pk.session_id,
          pk.machine_id,
          sessionUuid,
          source,
          ctx.orgId,
          realUserId ?? null,
          f.traceId,
          f.manifestRevision,
        ],
      );
      // `upsert_session_index` is revision-fenced, so an equal-revision S3
      // notification may have won the race with the authenticated manifest POST and
      // stored `unknown`. Repair identity separately, but only while this physical row
      // still names the manifest we just read; a newer pointer is never overwritten.
      await deps.db.query(
        `UPDATE sessions.session_index
            SET user_id = CASE WHEN $7::text IS NULL THEN user_id ELSE $7 END,
                canonical_session_id = $8,
                canonical_machine_id = $9
          WHERE org_id = $1 AND workspace_id = $2 AND harness = $3
            AND session_id = $4 AND machine_id = $5
            AND manifest_key = $6 AND deleted_at IS NULL`,
        [
          pk.org_id,
          pk.workspace_id,
          pk.harness,
          pk.session_id,
          pk.machine_id,
          pointer.manifestKey,
          realUserId ?? null,
          canonical.sessionId,
          canonical.machineId,
        ],
      );
      // Logical-session directory projection (idempotent; resurrects a tombstone only for
      // a live pointer indexed after it, per the V1 precedence contract).
      await projectSessionDirectory(deps.db, {
        orgId: pk.org_id,
        workspaceId: pk.workspace_id,
        harness: pk.harness,
        sessionId: pk.session_id,
      });
      if (managedSearch) await enqueueManagedSearchWake(deps.db, managedSearch, ctx.traceId);
      const fanoutDone = existing
        ? Number(existing.fanout_revision) >= f.manifestRevision
        : false;
      if (!applied && fanoutDone) {
        log.info("session index unchanged", {
          revision: f.manifestRevision,
          reason: "stale_or_equal_revision",
        });
        return { status: "skipped", reason: "stale_revision" };
      }
      if (fanoutDone) return;
      // Fan out, then record it. A crash in between repeats the fan-out on retry
      // (consumers are idempotent by revision / content hash); nothing is lost.
      const current = deps.current();
      const base: StageMessage = {
        v: 1,
        stage: "session-indexed",
        ctx: {
          ...(current?.ctx ?? ctx),
          session_uuid: sessionUuid,
          session_uuid_source: source,
        } as StageMessage["ctx"],
        workKey:
          current?.workKey ??
          `${pointer.manifestKey}@${read.etag ?? read.version ?? ""}`,
        revision: f.manifestRevision,
        payload: pointer,
      };
      for (const stage of FAN_OUT_STAGES) {
        const enriched = await enrichMessage({ ...base, stage }, deps.reader);
        await deps.queue.send(enriched);
      }
      // The model brief is debounced per logical session rather than queued per revision:
      // each new revision pushes the wakeup back (src/session-brief/store.ts armWakeup).
      if (brief)
        await armWakeup(
          deps.db,
          {
            orgId: pk.org_id,
            workspaceId: pk.workspace_id,
            harness: pk.harness,
            sessionId: pk.session_id,
          },
          brief,
        );
      await deps.db.query(
        `UPDATE sessions.session_index_ext SET fanout_revision = greatest(fanout_revision, $6)
          WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5`,
        [
          pk.org_id,
          pk.workspace_id,
          pk.harness,
          pk.session_id,
          pk.machine_id,
          f.manifestRevision,
        ],
      );
      log.info("session indexed", {
        stage: "session-indexed",
        revision: f.manifestRevision,
        session_uuid: sessionUuid,
        session_uuid_source: source,
        chunk_count: f.chunkCount,
        bytes_raw: f.rawBytes,
        trace_id: f.traceId,
        traceparent: ctxOf(base).traceparent,
      });
      return;
    },
  };
}


/**
 * The publish clock of a managed head, proven the way sessions.enqueue_managed_search_wake_v1 proves it: the
 * head's own published attempt, and a published_at on every live sibling of its canonical scope. `unprovable`
 * only when the head and its canonical scope resolve but a clock does not; anything else is left to the
 * function, which raises (and rolls the authority transaction back) exactly as before.
 */
const MANAGED_SEARCH_CLOCK_SQL = `
WITH h AS (
  SELECT * FROM sessions.ingest_publication_heads
   WHERE generation = $2::uuid AND current_key = $1 AND state = 'live'
), canon AS (
  SELECT coalesce(p.canonical_session_id, p.session_id) AS canonical_id
    FROM h JOIN sessions.session_index p
      ON (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id) = (h.org_id, h.workspace_id, h.physical_source, h.physical_session, h.machine_id)
     AND p.manifest_key = h.current_key AND p.revision = h.pointer_revision AND p.deleted_at IS NULL
), own AS (
  SELECT count(*) AS n FROM h
    JOIN sessions.ingest_publication_images i
      ON i.manifest_key = h.current_key AND i.generation = h.generation AND i.pointer_revision = h.pointer_revision AND i.raw_sha256 = h.current_raw_sha256
    JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
    JOIN sessions.ingest_stage_intents s ON s.stage_token = a.stage_token
   WHERE a.schema_generation = $3
     AND (a.org_id, a.workspace_id, a.user_id, a.origin_id) = (h.org_id, h.workspace_id, h.user_id, h.origin_id)
     AND (s.org_id, s.workspace_id, s.user_id, s.origin_id, s.batch_id, s.digest) = (a.org_id, a.workspace_id, a.user_id, a.origin_id, a.batch_id, a.digest)
     AND a.state = 'published' AND a.published_at IS NOT NULL
), sib AS (
  SELECT count(*) AS n, count(a.published_at) AS clocked
    FROM h CROSS JOIN canon
    JOIN sessions.ingest_publication_heads sibling ON true
    JOIN sessions.session_index p ON
      (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.manifest_key, p.revision) =
      (sibling.org_id, sibling.workspace_id, sibling.physical_source, sibling.physical_session, sibling.machine_id, sibling.current_key, sibling.pointer_revision)
    LEFT JOIN sessions.ingest_publication_images i ON
      (i.manifest_key, i.generation, i.pointer_revision, i.raw_sha256) = (sibling.current_key, sibling.generation, sibling.pointer_revision, sibling.current_raw_sha256)
    LEFT JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token AND a.state = 'published'
      AND (a.org_id, a.workspace_id, a.user_id) = (sibling.org_id, sibling.workspace_id, sibling.user_id)
   WHERE sibling.state = 'live' AND p.deleted_at IS NULL AND p.scrubbed IS TRUE AND NOT p.scrub_quarantined
     AND (p.org_id, p.workspace_id, p.harness, coalesce(p.canonical_session_id, p.session_id)) = (h.org_id, h.workspace_id, h.physical_source, canon.canonical_id)
)
SELECT h.org_id, h.workspace_id, h.physical_source AS harness, (h.source = '_none' AND h.session_id = '_origin') AS origin_bucket,
       (SELECT canonical_id FROM canon LIMIT 1) AS canonical_id,
       (SELECT n FROM own)::int AS own, (SELECT n FROM sib)::int AS siblings, (SELECT clocked FROM sib)::int AS clocked
  FROM h`;

/**
 * §2: the managed wake, inside the authority transaction. When the publish clock is unprovable it fails closed FOR
 * SEARCH ONLY [rev 7]: no job, never a pointer clock, and one durable refusal (pipeline_events search-index /
 * skipped / clock_unprovable) committed with the pointer, so the session stays readable.
 */
async function enqueueManagedSearchWake(db: HoopDb, wake: ManagedSearchWake, traceId: string): Promise<void> {
  const { evidence } = wake;
  const probe = (await db.query<{ org_id: string; workspace_id: string; harness: string; origin_bucket: boolean; canonical_id: string | null; own: number; siblings: number; clocked: number }>(
    MANAGED_SEARCH_CLOCK_SQL, [evidence.manifestKey, wake.generation, evidence.schemaGeneration])).rows[0];
  const unprovable = probe && !probe.origin_bucket && probe.canonical_id !== null &&
    (Number(probe.own) === 0 || Number(probe.siblings) === 0 || Number(probe.clocked) < Number(probe.siblings));
  if (probe && unprovable) {
    await db.query(
      `INSERT INTO sessions.pipeline_events (trace_id, stage, status, error, attrs, attempt, idempotency_key, org_id, workspace_id, harness, session_id)
       VALUES ($1::uuid, 'search-index', 'skipped', $2::jsonb, $3::jsonb, 1, $4, $5, $6, $7, $8)`,
      [traceId, JSON.stringify({ code: "clock_unprovable", name: "SearchWakeRefused", retryable: false }),
        JSON.stringify({ manifest_key_sha256: createHash("sha256").update(evidence.manifestKey).digest("hex") }),
        pipelineEventKey(traceId, "search-index", "skipped", 1, evidence.manifestKey),
        probe.org_id, probe.workspace_id, probe.harness, probe.canonical_id]);
    log.error("search wake refused", { stage: "search-index", outcome: "skipped", reason: "clock_unprovable", error_code: "clock_unprovable",
      org_id: probe.org_id, workspace_id: probe.workspace_id, harness: probe.harness, session_id: probe.canonical_id ?? undefined, trace_id: traceId });
    return;
  }
  await db.query("SELECT sessions.enqueue_managed_search_wake_v1($1::jsonb,$2::uuid)", [JSON.stringify(evidence), traceId]);
}
