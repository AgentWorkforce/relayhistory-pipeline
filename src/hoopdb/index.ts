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
import type { HoopDb,Transact } from "../shared/db.ts";
import {publicationEvidence,withPublicationAuthority}from "../shared/publication-authority.ts";
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
  publication?:{schemaGeneration:string;transact:Transact};
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
export interface UpsertOptions {
  rekeyFrom?: string;
  /**
   * The pointer stage's scope lock is held on another connection (pointer.ts ScopeLock): a
   * synchronous check that throws once that connection has dropped. Given, this call runs
   * `ensureLive()` before every statement it sends (the main pool, the publication transaction,
   * the move transaction), before each fan-out send and before the brief wakeup. The check is
   * this call's only: nothing is stored on the index object. Absent: as before.
   */
  ensureLive?: () => void;
}
export type HoopDbSessionPointerIndex = SessionPointerIndex & {
  upsertRawPointer(pointer: RawPointer, ctx: TraceContext, opts?: UpsertOptions): ReturnType<SessionPointerIndex["upsertRawPointer"]>;
};

/**
 * One transaction on one pooled connection (shared/db.ts inTransaction, inlined: importing db.ts
 * as a value pulls its schema-asset reads into the large-read worker bundle). `ensureLive`
 * (UpsertOptions) runs before BEGIN, each statement and COMMIT; absent, nothing changes.
 */
async function moveTransaction<T>(db: HoopDb, work: (tx: { query: HoopDb["query"] }) => Promise<T>, ensureLive?: () => void): Promise<T> {
  const client = await db.pool.connect();
  try {
    ensureLive?.();
    await client.query("BEGIN");
    const result = await work({
      query: (async (text: string, params?: unknown[]) => {
        ensureLive?.();
        return { rows: (await client.query(text, params as unknown[])).rows };
      }) as HoopDb["query"],
    });
    ensureLive?.();
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

/** A query function that runs `ensureLive()` before each statement. */
function checkedQuery(query: HoopDb["query"], ensureLive: () => void): HoopDb["query"] {
  return (async (text: string, params?: unknown[]) => {
    ensureLive();
    return query(text, params);
  }) as HoopDb["query"];
}

/**
 * `deps` for one upsertRawPointer call whose scope lock is held elsewhere (UpsertOptions.ensureLive):
 * the main pool's queries, the fan-out sends and the publication transaction (before it starts,
 * before each of its statements and before its COMMIT) run `ensureLive()` first. Same objects underneath, same statements.
 */
function checkedIndexDeps(deps: IndexDeps, ensureLive: () => void): IndexDeps {
  const queue = deps.queue;
  return {
    ...deps,
    db: { ...deps.db, query: checkedQuery(deps.db.query.bind(deps.db), ensureLive) } as HoopDb,
    queue: {
      backend: queue.backend,
      send: (message, options) => {
        ensureLive();
        return queue.send(message, options);
      },
      sendBatch: (messages, options) => {
        ensureLive();
        return queue.sendBatch(messages, options);
      },
    },
    ...(deps.publication
      ? {
          publication: {
            schemaGeneration: deps.publication.schemaGeneration,
            transact: (work) => {
              ensureLive();
              // The check after `work` runs before the transaction's COMMIT: a lost scope lock rolls it back.
              return deps.publication!.transact(async (tx) => {
                const result = await work({ ...tx, query: checkedQuery(tx.query.bind(tx), ensureLive) });
                ensureLive();
                return result;
              });
            },
          } satisfies NonNullable<IndexDeps["publication"]>,
        }
      : {}),
  };
}

function sessionIndex(given:IndexDeps,authorized:boolean):HoopDbSessionPointerIndex{
  const operatorErase = eraseRuntimeFlag(process.env, ERASE_FENCE_FLAG);
  const fromEnv = briefConfig();
  const brief =
    given.brief !== undefined ? given.brief : fromEnv.enabled ? fromEnv : null;
  return {
    async upsertRawPointer(pointer: RawPointer, ctx: TraceContext, opts: UpsertOptions = {}) {
      // The outer call wraps its deps once; the inner authorized calls run on the transaction it
      // opened, whose statements are checked already.
      const deps = !authorized && opts.ensureLive ? checkedIndexDeps(given, opts.ensureLive) : given;
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
        const final=await withPublicationAuthority(config.transact,evidence,async tx=>{
          const db={...deps.db,query:tx.query.bind(tx)}as HoopDb;
          return sessionIndex({...deps,db,reader:cached,queue:postgresPipelineQueue(tx)},true).upsertRawPointer(pointer,ctx,opts);
        });
        return final.current?final.value:{status:"skipped",reason:"publication_retired"};
      }
      if(!authorized&&pointer.manifestKey.includes('/upload-generations/'))
        throw Object.assign(Error("publication_authority_unconfigured"),{code:"publication_authority_unconfigured"});
      // A move without the publication transaction (no publication config): the delete and the
      // re-insert still commit together. (A test double without a pool runs them in sequence.)
      if(!authorized&&opts.rekeyFrom&&deps.db.pool)
        return moveTransaction(deps.db,tx=>sessionIndex({...deps,db:{...deps.db,query:tx.query}as HoopDb},true).upsertRawPointer(pointer,ctx,opts),opts.ensureLive);
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
