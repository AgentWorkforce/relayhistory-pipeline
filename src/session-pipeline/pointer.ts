/**
 * Stage 1 for an S3 event: the pipeline's `object-landed` handler, then
 * `session-indexed` through the HoopDb pointer index, run in process instead
 * of across two Postgres queue hops. The handler code is the same the
 * hoopdb-indexer runs (packages/pipeline stages.ts + src/hoopdb/index.ts):
 * the Supabase row is written only by `sessions.upsert_session_index`
 * (revision-fenced: a late, duplicated or replayed event never rolls a row
 * back), then `session_index_ext` and the logical `session_directory` row.
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
  parseSessionKey,
  type ManifestReader,
  type OrgResolver,
  type PipelineQueue,
  type StageMessage,
} from "../shared/vendor.ts";
import type { ObjectLanded } from "@rhc/lib/pipeline/adapter.ts";
import type { HoopDb } from "../shared/db.ts";
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
  | { status: "indexed" | "unchanged" | "deleted"; scope: SessionScope; manifestKey: string; revision?: number };

export interface PointerDeps {
  db: HoopDb;
  reader: ManifestReader;
  orgs: OrgResolver;
  allowPrefix(prefix: string | undefined, orgId: string): boolean;
  /** Where the index's fan-out (summary-short, summary-long, status-classify) goes. */
  fanout: PipelineQueue;
  now?: () => Date;
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
  if (location.rest !== "manifest.json") return { status: "ignored", reason: "not_a_manifest" };
  if (landed.bucket && deps.reader.bucket && landed.bucket !== deps.reader.bucket)
    return { status: "ignored", reason: "other_bucket" };
  const orgId = await deps.orgs.orgFor(location);
  // A key under the deployment prefix always names its org; no org means a key
  // this deployment does not own.
  if (!orgId) return { status: "ignored", reason: "org_unresolved" };
  if (!deps.allowPrefix(location.prefix, orgId)) return { status: "ignored", reason: "outside_prefix" };
  const scope: SessionScope = {
    orgId,
    workspaceId: location.workspaceId,
    harness: location.harness,
    sessionId: location.sessionId,
  };

  // Canonical state, not the event: re-read the manifest.
  const read = await deps.reader.read(landed.key);
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

  const index = hoopDbSessionIndex({ db: deps.db, reader: deps.reader, queue: deps.fanout, current: () => message });
  const result = await index.upsertRawPointer(message.payload as Parameters<typeof index.upsertRawPointer>[0], message.ctx);
  const revision = message.revision;
  if (!read) return { status: "deleted", scope, manifestKey: landed.key };
  return {
    status: result && result.status === "skipped" ? "unchanged" : "indexed",
    scope,
    manifestKey: landed.key,
    ...(revision !== undefined ? { revision } : {}),
  };
}
