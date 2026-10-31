/**
 * The session pointer index for HoopDb (upstream has only test fakes): reads the
 * manifest a `session-indexed` message points at, upserts the row through the
 * vendored `sessions.upsert_session_index`, records the session uuid in
 * `session_index_ext`, then fans out to Bob Costas (summaries) and the Referee
 * (state) with the same trace and session ids.
 */
import {
  pointerFields,
  type ManifestReader,
  type PipelineQueue,
  type RawPointer,
  type SessionPointerIndex,
  type StageMessage,
  type TraceContext,
} from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf, enrichMessage, sessionExt } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { uuidv7 } from "../shared/ids.ts";
import { propagatePointerTombstone } from "./session-directory-summary.ts";
import { projectSessionDirectory } from "./session-directory-projector.ts";

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
}

export function hoopDbSessionIndex(deps: IndexDeps): SessionPointerIndex {
  return {
    async upsertRawPointer(pointer: RawPointer, ctx: TraceContext) {
      const pk = {
        org_id: ctx.orgId,
        workspace_id: pointer.workspaceId,
        harness: pointer.harness ?? ctx.harness ?? "unknown",
        session_id: pointer.sessionId ?? ctx.sessionId ?? "unknown",
        machine_id: pointer.machineId ?? ctx.machineId ?? "unknown",
      };
      if (pointer.deleted) {
        await deps.db.query(
          `UPDATE sessions.session_index SET deleted_at = now(), indexed_at = now()
            WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
          [
            pk.org_id,
            pk.workspace_id,
            pk.harness,
            pk.session_id,
            pk.machine_id,
          ],
        );
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
      const f = pointerFields(m);
      const ext = sessionExt(m);
      const ectx = ctx as unknown as {
        user_id?: string;
        session_uuid?: string;
      };
      // Identity comes from the authenticated request (ctx), never from the manifest body.
      // With no request identity (reconcile), keep whatever an authenticated upload recorded.
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
      const userId = ectx.user_id ?? existing?.user_id ?? "unknown";
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
        storage_provider: pointer.provider ?? "s3",
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
      const { rows } = await deps.db.query<{
        applied: boolean;
        revision: string;
      }>(
        "SELECT applied, revision FROM sessions.upsert_session_index($1::jsonb)",
        [JSON.stringify([row])],
      );
      const applied = rows[0]?.applied ?? false;
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
          ectx.user_id ?? null,
          f.traceId,
          f.manifestRevision,
        ],
      );
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
