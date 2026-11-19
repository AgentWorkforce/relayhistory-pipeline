/**
 * Stage 2 for an S3 event: make ClickHouse's `transcript_events` for one
 * session scope equal what the session's manifests in S3 hold. This replaces
 * the relayhistory-cloud minute tail, which reads the same rows from Neon.
 *
 * Scope. A session (org, workspace, harness, session id) can have several
 * manifests: one per machine (`session_index` keys rows by machine), and the
 * Neon exporter (`m=delivery_…`) and the relayhistory-api live writer
 * (`m=srv_…`) each write their own copy of the same delivery records. The
 * scope's truth is every live pointer row's manifest, read from S3 (canonical)
 * and folded together by the pipeline's own transform:
 * `normalizeRelayhistoryRecords` keeps the newest revision of each
 * (origin, record) across all copies, and `projectRelayhistorySession` turns
 * the survivors into the `convergence_events` rows the Neon triggers would
 * write (identity included: a delivery event's machine is derived from its
 * record's origin, never from the manifest that carried it), so the same
 * event from two manifests is one ClickHouse row.
 *
 * Reconcile. As in the Neon tail (vendor/search-index/reconcile.ts): compare
 * the scope's identities and fingerprints, insert what is new or changed,
 * then remove ClickHouse rows S3 no longer has (erasure, retraction, a moved
 * ts). An incomplete read (a manifest missing, a chunk unreadable, a
 * truncated ClickHouse read) never deletes. With no live pointer left the
 * session is erased and every row of the scope goes, inside the window or not.
 *
 * Serialization. Two events for one scope run one at a time: the pointer
 * listing and the ClickHouse write happen under a transaction-scoped advisory
 * lock in Supabase, taken after the event's own pointer upsert committed, so
 * a reconcile never acts on a pointer set older than one already applied.
 *
 * Fingerprints. Rows from S3 carry `s3`-prefixed fingerprints, which never
 * equal a Neon-tail fingerprint, so the first S3 pass over a session rewrites
 * its rows once (ReplacingMergeTree keeps the newer `version`). Do not run the
 * Neon tail and this indexer at the same time: each would keep rewriting the
 * other's rows (correct, but wasted work).
 *
 * Nothing here logs content; errors carry codes only.
 */
import { ARTIFACT_MAX_SOURCE_BYTES, ARTIFACT_MAX_SOURCE_RECORDS } from "../session-read-api/canonical-artifact.ts";
import { readCanonicalScope } from "../session-read-api/canonical-projection.ts";
import {
  STREAM_DEADLINE,
  STREAM_KEYED_RECORDS_CAP,
  STREAM_READ_BYTES_BUDGET,
  STREAM_SOURCE_CHANGED,
  STREAM_PASSES,
  STREAM_READ_EVENTS_BUDGET,
  StreamBudgetError,
  streamCanonicalScope,
  streamDeadlinePassed,
  streamIndexKey,
  type StreamDeadline,
  type StreamReadLimits,
} from "../session-read-api/canonical-stream-projection.ts";
import { nodeReadBudget } from "../session-read-api/node-read-profile.ts";

/** The largest streaming read budget: the large-read profile's source bound. */
export const STREAM_READ_CEILING: Readonly<StreamReadLimits> = Object.freeze({
  maxReadBytes: nodeReadBudget("large").sourceBytes,
  maxReadEvents: nodeReadBudget("large").sourceRecords,
});
export { manifestUserId, readManifestRecords, projectedToNeonRow } from "../session-read-api/canonical-projection.ts";
import * as zlib from "node:zlib";
import { layoutComponent } from "../shared/layout-component.ts";
import { errorFields, log } from "../shared/log.ts";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { tsMillis } from "./vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "./vendor/search-index/schema.ts";
import { chNow, deleteRows, windowStart, type PendingDelete } from "./vendor/search-index/reconcile.ts";
import { scopeLockKey, type SessionScope } from "./pointer.ts";
import { directoryCatalogWriter, type writeDirectoryCatalog } from "../hoopdb/session-directory-catalog.ts";
import { writeDirectoryEvents, invalidateDirectoryEvents, completeErasedDirectoryEvents } from "../hoopdb/session-directory-events.ts";
import { outputFactsComplete, outputFactsFromScope, passVersion, type OutputFactsScope } from "./output-facts.ts";
import {
  clickhouseOutputFactsStore,
  finishStagedOutputFacts,
  MAX_FACTS_CHUNK_INPUTS,
  OUTPUT_FACTS_CHUNK_UNSTAGEABLE,
  isDeterministicRefusal,
  sealOutputFactsWant,
  stageOutputFactsChunk,
  type OutputFactsStore,
} from "./output-facts-store.ts";
import { adapterProjectionIo, adapterScanPage, type SearchProjectionIo, type SearchServiceBinding } from "./search-adapter.ts";
import {
  STAGED_RECONCILE_STALE_LEASE,
  admittedManifestFingerprint,
  eraseStagedReconcileRuns,
  runStagedReconcile,
  type SourceStamp,
  type StagedIdentity,
  type StagedReconcileDeps,
  type StagedReconcileHooks,
  type StagedReconcileOps,
} from "./staged-reconcile.ts";
import { MAX_SCAN_PAGE, type SearchPosition } from "@rhc/search-index/contracts.ts";
/** Kept in sync with buffered-search-feed.ts (not imported: that module pulls in the ingest stage reader). */
const BUFFERED_FINGERPRINT_PREFIX = "bf";

export type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;

export interface SearchIndexDeps {
  readerMaxRuleset?: number;
  /** One Supabase transaction on one connection (for the advisory lock). */
  transact: Transact;
  reader: ManifestReader;
  store: ObjectStore;
  ch?: ClickHouseClient;
  /** Explicit registered service composition; mutually exclusive with legacy CH. */
  adapter?: SearchServiceBinding;
  /** The deployment prefix of an org's keys, e.g. `96bulls/<org>`. */
  prefixFor(orgId: string): string;
  decompress?: (stored: Uint8Array, maxOutputLength: number) => Uint8Array;
  now?: () => Date;
  limits?: Partial<SearchIndexLimits>;
  /**
   * SESSION_DESKTOP_CATALOG_ENABLED=1 only: write the complete desktop catalog (migration
   * 20261105022000 columns). Absent or false: the pre-#182 catalog writer, exactly as before;
   * SEARCH_INDEX_ENABLED alone never selects the desktop writer
   * (docs/session-desktop-catalog.md, Activation blockers).
   */
  desktopCatalog?: boolean;
  /**
   * MESSAGE_ORIGIN_LABELS_ENABLED=true only: index rows carry who wrote each event in
   * actor_name / subagent_id (message-origin.ts). Absent or false: rows exactly as before.
   * Turning it on changes the fingerprint of every labelled row, so each scope's next
   * pass rewrites its rows once.
   */
  originLabels?: boolean;
  /** Test seam (replaces the writer `desktopCatalog` selects); production writes the catalog in the same Supabase transaction. */
  writeCatalog?: typeof writeDirectoryCatalog;
  writeEvents?: typeof writeDirectoryEvents;
  /**
   * SEARCH_INDEX_OUTPUT_FACTS=1 only: also reconcile `transcript_event_facts` and
   * `session_output_facts` (output-facts.ts) in this pass, under the same lock.
   * Needs the owner-applied output-facts DDL. Every pass records the scope's facts
   * state (complete or not) in `output_facts_session_state`, separate from the search
   * state, so the Output route answers 501 for an inexact scope rather than a stale
   * count, and a facts problem never marks the session `needs_full` for search.
   * Absent or false: no fact table is touched.
   */
  outputFacts?: boolean;
  /**
   * RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true only (the self-host consumer, no ClickHouse): the
   * Postgres store of migration 20261105045040 on this pass's transaction
   * (output-facts-store.ts postgresOutputFactsStore). A complete pass reconciles the facts and
   * records the state after the directory events, in the same transaction; a partial read
   * records the state incomplete; erasure deletes the scope's facts and state. The source
   * version is the directory event_projection_epoch the pass read: an older pass is a no-op.
   * Absent: no output facts function is called.
   */
  outputFactsStore?: (tx: SqlExecutor) => OutputFactsStore;
  /**
   * SEARCH_INDEX_STREAMING_ENABLED on (the default) only, in the self-host consumer (adapter
   * binding, no output facts), or SEARCH_INDEX_STREAMING_CH=1 (the hosted consumer Lambda,
   * ClickHouse binding), never with output facts: a scope past the whole-read bound
   * (`scope_too_large` alone) is reconciled
   * by the streaming reader (canonical-stream-projection.ts) within these read budgets,
   * summed over its passes, instead of being marked partial. On the ClickHouse binding the
   * index rows are read by keyset pages, at most `maxReadEvents` of them. Absent: such a
   * scope is marked partial exactly as before, and every other scope never reaches the
   * streaming reader.
   */
  stream?: StreamReadLimits;
  /**
   * Only with `stream` (the hosted Lambda sets it per invocation from its remaining time less
   * SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS): checked between chunks, between index scan pages,
   * before the regenerate (pass 4) and before any delete. Once crossed, the scope stops
   * there, deletes nothing, keeps what pass 4 already wrote, and is recorded partial
   * (`scope_too_large`, `stream_deadline`, needs_full=1); the pass does not throw. Absent:
   * no deadline.
   */
  streamDeadline?: StreamDeadline;
  /**
   * Only with `stream` (the hosted Lambda sets one per invocation, with the deadline): the
   * invocation's single streamed scope. The first scope that streams takes it; a later scope
   * of the same invocation that would stream returns `deferred` instead, before any write,
   * and the consumer requeues it (as `busy`), so one streamed scope's run up to the deadline
   * never leaves several more to finish inside the margin. Absent: no such limit.
   */
  streamSlot?: { used: boolean };
  /** Tests only: how often the streaming reader keeps its transaction busy (default 20 s). */
  streamKeepaliveMs?: number;
  /**
   * The staged reconcile (the self-host consumer only, with `stream`; searchIndexStagedStartup, on by
   * default since Phase 3 slice 4; with output facts their hooks stage them), Phase 3 slice 1: the index scan of every pass
   * walks the adapter's keyset pages (scanFingerprintsPage) instead of one read bounded by
   * SEARCH_INDEX_MAX_ROWS, so there is no `search_index_truncated`; the streaming budgets bound
   * each pass, not their sum, so there is no four-times refusal; and the records one pass keys
   * in memory, and the index rows one scan holds (at least SEARCH_INDEX_MAX_ROWS), are capped at
   * `keyedRecordsCap` (`stream_keyed_records_cap`, partial). Absent: every pass exactly as before.
   */
  staged?: StagedReconcile;
  /**
   * SEARCH_INDEX_SKIP_CURRENT_ENABLED=true only (the hosted Lambda, lambda.ts): a pass returns at once,
   * before any object read or index query, when the session directory proves that a COMPLETE pass already
   * covered exactly the scope's live pointer set at its current revisions (alreadyCurrent): result
   * `skipped`, reason `already_current`, nothing written. Absent: every pass exactly as before.
   */
  skipCurrent?: boolean;
}

/** SearchIndexDeps.staged: the staged reconcile's interim bound. */
export interface StagedReconcile {
  /** The most records one streamed pass may key in memory, and the floor of the index-scan cap. */
  keyedRecordsCap: number;
  /** Tests only: the staged run's batch sizes, lease and phase-boundary seam (staged-reconcile.ts). */
  run?: Partial<Pick<StagedReconcileDeps, "leaseMs" | "stageBatch" | "scanPage" | "writeKeys" | "deletePage" | "boundary" | "factsChunk" | "hooks" | "releaseOnError" | "heartbeatMs" | "attemptsCap">>;
  /** Tests only: the facts pass's record bytes bound (STAGED_FACTS_RECORD_BYTES_CAP in production). */
  factsRecordBytesCap?: number;
  /**
   * This indexer instance's cached size refusals (StagedRefusals): searchIndexStagedStartup gives each
   * start a fresh one. Absent: one per StagedReconcile object.
   */
  refusals?: StagedRefusals;
}

/**
 * The staged reconcile's interim cap on keyed records (about 3 KB of peak memory each, measured
 * with scripts/bench/staged-stream-rss.ts: +0.6 GB at 100000 records, +3.3 GB at 1000000): the
 * largest scope the default streaming budgets stream today (2,000,000 / STREAM_PASSES), so turning
 * the flag on never asks a pod for more memory than streaming already does (about 2 GB at the
 * cap). A later slice replaces the in-memory keys and removes it.
 */
export const STAGED_KEYED_RECORDS_CAP = 500_000;

/**
 * The most weighted bytes of record items (legacy outcomes and turns, live conversation turns: their
 * JSON text, STAGED_FACTS_WIDE_TEXT_WEIGHT applied) one staged run's facts pass may stage per attempt.
 * The final step reads them all back and folds them in memory. Measured with
 * scripts/bench/staged-facts-rss.ts --whole-run (one process, Bun 1.4.2, 100000 legacy turns):
 *   300 ASCII characters (762 staged bytes each)          peak 0.66 GB
 *   3000 ASCII characters (3462 bytes each)               peak 1.22 GB   -> about 2.09 B of RSS per staged byte
 *   3000 characters with one above U+00FF (3464 bytes)    peak 1.89-1.94 GB -> about 4.6 B per staged byte
 * Model: peak = a(records) + slope x staged bytes, with a(500000 records) = 1.61 GB (500000 short ASCII turns
 * peak 2.41 GB). At both bounds: ASCII 1.61 + 2.09 x 0.537 = 2.73 GB; wide text, counted 2.5 x,
 * 1.61 + 4.6 x 0.215 = 2.60 GB; both under the chart's 3Gi. Over it the run abandons as
 * output_facts_chunk_unstageable, as for an unstageable chunk. Only with output facts on.
 */
export const STAGED_FACTS_RECORD_BYTES_CAP = 512 * 1024 * 1024;
/**
 * A staged record whose JSON text holds any code unit above 0xFF counts this many times its UTF-8 bytes
 * toward STAGED_FACTS_RECORD_BYTES_CAP: the runtime then keeps the whole text as a 16-bit string, about
 * 4.6 B of peak RSS per staged byte instead of 2.09 (the model above).
 */
export const STAGED_FACTS_WIDE_TEXT_WEIGHT = 2.5;

export interface SearchIndexLimits {
  /** Largest scope read into memory, summed over its manifests' raw bytes. */
  maxScopeBytes: number;
  /** Largest scope read, summed over its manifests' chunk record counts. */
  maxScopeEvents: number;
  /** Largest number of ClickHouse rows compared for one scope. */
  maxRows: number;
  /** Rows per insert. */
  insertBatch: number;
}

/** The search stage's own limits: the default path, live with SEARCH_INDEX_ENABLED=1. */
export const SEARCH_INDEX_DEFAULT_LIMITS: Readonly<SearchIndexLimits> = Object.freeze({
  // Raw text is held several times over (strings, records, projected and index
  // rows), and an out-of-memory kill fails the whole batch: stay well inside 3 GB.
  maxScopeBytes: 64 * 1024 * 1024,
  maxScopeEvents: 100_000,
  maxRows: 500_000,
  insertBatch: 5_000,
});

/**
 * Only with the replay stage (SESSION_REPLAY_PAGES_ENABLED=1): the index pass
 * must cover the same source the replay producer reads, so it shares the
 * producer's 256 MiB / 1M-record bound. This does not lift Worker limits or
 * cover production cohorts above the 256 MiB ceiling, and it is not yet proven
 * to fit the 3008 MiB Lambda (see docs/canonical-replay-pages.md).
 */
export const REPLAY_SEARCH_INDEX_LIMITS: Readonly<SearchIndexLimits> = Object.freeze({
  maxScopeBytes: ARTIFACT_MAX_SOURCE_BYTES,
  maxScopeEvents: ARTIFACT_MAX_SOURCE_RECORDS,
  maxRows: 1_000_000,
  insertBatch: 5_000,
});

export function searchIndexLimits(replayPagesEnabled: boolean): SearchIndexLimits {
  return { ...(replayPagesEnabled ? REPLAY_SEARCH_INDEX_LIMITS : SEARCH_INDEX_DEFAULT_LIMITS) };
}

/**
 * Operator overrides for the indexer SERVICE (consumer-service.ts serve() only; the hosted
 * Lambda never reads them), which has no large-read handoff: a scope above `maxScopeBytes` /
 * `maxScopeEvents` is marked partial (`scope_too_large`), and one partial session makes the
 * read API refuse every workspace search (501 `session_partial`) with no fallback on a
 * self-host install. Raising the limits lets an operator who sized the pod's memory for it
 * index such a session on its next index pass.
 *
 *   SEARCH_INDEX_MAX_SCOPE_BYTES   raw bytes of one scope: a whole number, or one with a MiB / GiB
 *                                  suffix (as READ_API_CANONICAL_SCOPE_BYTES); default 64 MiB,
 *                                  accepted 64 MiB..256 MiB
 *   SEARCH_INDEX_MAX_SCOPE_EVENTS  records of one scope, a whole number; default 100000, accepted
 *                                  100000..1000000
 *   SEARCH_INDEX_MAX_ROWS          index rows compared for one scope, a whole number; default 500000,
 *                                  accepted 500000..1000000
 *
 * The floor is SEARCH_INDEX_DEFAULT_LIMITS (a lower limit only marks more sessions partial)
 * and the ceiling REPLAY_SEARCH_INDEX_LIMITS, the largest scope the index pass is written
 * for. Unset or empty keeps the default, and an all-unset environment returns `{}` so the
 * pass is byte-identical to before. Anything else fails startup naming the variable and its
 * range, never the value, and is never clamped: a typo must not silently mean the default.
 */
export function searchIndexLimitsFromEnv(env: Record<string, string | undefined>): Partial<SearchIndexLimits> {
  const MiB = 1024 * 1024;
  const fmt = (bytes: boolean, n: number) => (bytes ? `${n / MiB}MiB` : String(n));
  const read = (name: string, bytes: boolean, min: number, max: number): number | undefined => {
    const raw = env[name];
    if (raw === undefined || raw === "") return undefined;
    const m = (bytes ? /^([1-9][0-9]{0,12})(MiB|GiB)?$/u : /^([1-9][0-9]{0,12})()$/u).exec(raw);
    const value = m ? Number(m[1]) * (m[2] === "GiB" ? 1024 * MiB : m[2] === "MiB" ? MiB : 1) : NaN;
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new Error(`pipeline consumer: ${name} must be ${bytes ? "a whole number of bytes (optional MiB or GiB suffix)" : "a whole number"} in ${fmt(bytes, min)}..${fmt(bytes, max)}`);
    return value;
  };
  const lo = SEARCH_INDEX_DEFAULT_LIMITS, hi = REPLAY_SEARCH_INDEX_LIMITS;
  const maxScopeBytes = read("SEARCH_INDEX_MAX_SCOPE_BYTES", true, lo.maxScopeBytes, hi.maxScopeBytes);
  const maxScopeEvents = read("SEARCH_INDEX_MAX_SCOPE_EVENTS", false, lo.maxScopeEvents, hi.maxScopeEvents);
  const maxRows = read("SEARCH_INDEX_MAX_ROWS", false, lo.maxRows, hi.maxRows);
  return {
    ...(maxScopeBytes === undefined ? {} : { maxScopeBytes }),
    ...(maxScopeEvents === undefined ? {} : { maxScopeEvents }),
    ...(maxRows === undefined ? {} : { maxRows }),
  };
}

export interface SearchIndexResult {
  manifests: number;
  rows: number;
  inserted: number;
  deleted: number;
  /** False when something could not be read: nothing was deleted. */
  complete: boolean;
  /** Why the read was incomplete (codes only). */
  reasons: string[];
  /** Another pass holds this session: nothing was done, re-enqueue the event. */
  busy?: boolean;
  /** `skipCurrent` only: a complete pass already covers the live source set; nothing was read or written. */
  skipped?: boolean;
  /** Would stream, but this invocation already streamed a scope (`streamSlot`): nothing was written, re-enqueue the event. */
  deferred?: boolean;
  /** Too large to index whole: no rows written, its index state marked partial. */
  partial?: boolean;
  /** Present only with `outputFacts`: fact rows inserted and deleted, and whether they are exact. */
  outputFacts?: { inserted: number; deleted: number; complete: boolean; reasons: string[] };
  /** Present only when the streaming reader ran: what it read, summed over its passes. */
  stream?: { passes: number; chunks: number; bytes: number; events: number };
}

interface ScopeIdentity {
  machineId: string;
  sessionId: string;
  source: string;
  kind: string;
  eventId: string;
  workspaceId: string;
  tsMs: number;
  fingerprint: string;
}

/** Kinds projectRelayhistorySession always derives from exported delivery records (its ACTIVITY_KINDS). */
export const S3_ACTIVITY_KINDS = ["history", "session_event", "tool_call", "file_edit", "trajectory"] as const;

const SEP = "\u001f";
const keyOf = (i: Pick<ScopeIdentity, "machineId" | "source" | "kind" | "eventId">) =>
  [i.machineId, i.source, i.kind, i.eventId].join(SEP);

function defaultDecompress(stored: Uint8Array, maxOutputLength: number): Uint8Array {
  if (typeof zlib.zstdDecompressSync !== "function")
    throw new PipelineError("zstd_unavailable", "this runtime cannot zstd-decompress raw chunks", false);
  return new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength }));
}


async function clickhouseScope(
  ch: ClickHouseClient,
  scope: SessionScope,
  fromMs: number,
  limit: number,
  canonicalSessionId = scope.sessionId,
): Promise<ScopeIdentity[]> {
  const rows = await ch.select<{
    workspace_id: string;
    session_id: string;
    machine_id: string;
    source: string;
    kind: string;
    event_id: string;
    ts_ms: number;
    fingerprint: string;
  }>(
    `SELECT workspace_id, session_id, machine_id, source, kind, event_id,
        toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint
     FROM ${EVENTS_TABLE} FINAL
     WHERE org_id = {org:String} AND (session_id = {session:String} OR session_id = {canonical:String})
       AND workspace_id = {workspace:String} AND source = {source:String}
       AND toUnixTimestamp64Milli(ts) >= {from:Int64}
     LIMIT {limit:UInt32}`,
    {
      params: { org: scope.orgId, session: scope.sessionId, canonical: canonicalSessionId, workspace: scope.workspaceId, source: scope.harness, from: fromMs, limit },
      timeoutMs: 20_000,
    },
  );
  return rows.map((r) => ({
    workspaceId: r.workspace_id,
    sessionId: r.session_id,
    machineId: r.machine_id,
    source: r.source,
    kind: r.kind,
    eventId: r.event_id,
    tsMs: Number(r.ts_ms),
    fingerprint: r.fingerprint,
  }));
}

/**
 * The session's index state in `search_ingest_session_state` (vendored
 * schema.ts): `needs_full = 1` marks a session whose S3 pass was partial or
 * incomplete, so readers never take its rows as current; a complete pass
 * writes 0. `revision` is the highest manifest revision of the scope.
 */
export async function recordState(ch: ClickHouseClient, scope: SessionScope, revision: number, rows: number, complete: boolean, at: Date, canonicalSessionId = scope.sessionId) {
  const ts = chNow(at);
  await ch.insert(SESSION_STATE_TABLE, [...new Set([scope.sessionId, canonicalSessionId])].map(sessionId => ({
      org_id: scope.orgId,
      session_id: sessionId,
      revision,
      synced_at: ts,
      full_synced_at: complete ? ts : "1970-01-01 00:00:00.000",
      scanned_rows: Math.min(rows, 4_294_967_295),
      needs_full: complete ? 0 : 1,
    })));
}

/** Live manifests of a scope, from the pointer index. */
export async function livePointerKeys(tx: SqlExecutor, scope: SessionScope): Promise<string[]> {
  return (await livePointerSnapshot(tx, scope)).keys;
}

/** The source set and directory epoch share one MVCC snapshot. Pointer changes
 * increment the epoch; the final UPDATE compares it after locking the current row. */
async function livePointerSnapshot(tx: SqlExecutor, scope: SessionScope): Promise<{ keys: string[]; epoch: string; canonicalId:string }> {
  const { rows } = await tx.query<{ manifest_key: string; event_epoch: string; canonical_id:string }>(
    `SELECT p.manifest_key, coalesce(p.canonical_session_id,p.session_id) AS canonical_id, coalesce(d.event_projection_epoch, 0)::text AS event_epoch
       FROM sessions.session_index p LEFT JOIN LATERAL (
         SELECT d.event_projection_epoch FROM sessions.session_directory d
          WHERE d.org_id=p.org_id AND d.workspace_id=p.workspace_id AND d.harness=p.harness
            AND d.session_id IN(coalesce(p.canonical_session_id,p.session_id),p.session_id)
          ORDER BY (d.session_id=coalesce(p.canonical_session_id,p.session_id)) DESC LIMIT 1
       ) d ON true
      WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3 AND p.session_id=$4 AND p.deleted_at IS NULL
      ORDER BY p.machine_id`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
  let canonicalId=rows[0]?.canonical_id;
  if(!rows.length){const previous=await tx.query<{canonical_id:string}>(`SELECT coalesce(canonical_session_id,session_id) AS canonical_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 ORDER BY revision DESC LIMIT 1`,[scope.orgId,scope.workspaceId,scope.harness,scope.sessionId]);canonicalId=previous.rows[0]?.canonical_id;}
  return { keys: rows.map((r) => r.manifest_key), epoch: rows[0]?.event_epoch ?? "0", canonicalId:canonicalId??scope.sessionId };
}

export async function indexSearchScope(scope: SessionScope, deps: SearchIndexDeps): Promise<SearchIndexResult> {
  if (Boolean(deps.ch) === Boolean(deps.adapter)) throw new Error("search requires exactly one service binding");
  // The Output/digest facts are ClickHouse tables: a portable adapter has no home for them.
  if (deps.outputFacts && !deps.ch) throw new Error("output facts require the ClickHouse search binding");
  if (deps.outputFacts && deps.outputFactsStore) throw new Error("output facts: choose the ClickHouse or the Postgres store, not both");
  if (deps.outputFacts && deps.staged) throw new Error("output facts: the staged reconcile stages the Postgres store's facts only");
  const limits = { ...SEARCH_INDEX_DEFAULT_LIMITS, ...deps.limits };
  const now = deps.now ?? (() => new Date());
  // Staged (SEARCH_INDEX_STAGED_RECONCILE_ENABLED, adapter binding): a scope that would stream
  // leaves this transaction without a write and runs as a staged run, outside it, in its own
  // committed batches (stagedSearchScope).
  let handoff: StagedHandoff | undefined;
  const first = await deps.transact(async (tx): Promise<SearchIndexResult> => {
    const io = deps.adapter
      ? deps.staged
        ? adapterProjectionIo(deps.adapter, tx, { pagedScan: true })
        : adapterProjectionIo(deps.adapter, tx)
      : legacyProjectionIo(deps.ch!);
    const chFacts = deps.ch ? clickhouseOutputFactsStore(deps.ch, { maxRows: limits.maxRows, insertBatch: limits.insertBatch }) : undefined;
    const pgFacts = deps.outputFactsStore?.(tx);
    // Non-blocking: a pass already running for this session is busy for as long
    // as its ClickHouse work takes. The caller re-enqueues this event with a
    // delay instead of waiting (and timing out into the DLQ behind a hot session).
    const { rows: got } = await tx.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [
      scopeLockKey(scope, "search"),
    ]);
    if (!got[0]?.ok) return { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, busy: true, reasons: ["scope_busy"] };
    const pointerSnapshot = await livePointerSnapshot(tx, scope);
    const keys = pointerSnapshot.keys;
    const { rows: canonicalLock } = await tx.query<{ok:boolean}>("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS ok", [["96bulls:directory-events",scope.orgId,scope.workspaceId,scope.harness,pointerSnapshot.canonicalId].join("\u001f")]);
    if (!canonicalLock[0]?.ok) return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,busy:true,reasons:["canonical_scope_busy"]};
    const at = now();
    const version = at.getTime();
    // A stale DB alias is not authority to delete another canonical session.
    if(layoutComponent(pointerSnapshot.canonicalId)!==scope.sessionId){
      await invalidateDirectoryEvents(tx,scope);
      await io.record(scope,0,0,false,at,pointerSnapshot.canonicalId);
      return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,reasons:["canonical_session_mismatch"]};
    }
    const {rows:siblings}=await tx.query<{manifest_key:string}>(`SELECT manifest_key FROM sessions.session_index
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id,session_id)=$4 AND deleted_at IS NULL ORDER BY session_id,machine_id`,[scope.orgId,scope.workspaceId,scope.harness,pointerSnapshot.canonicalId]);
    const siblingKeys=siblings.map(r=>r.manifest_key);
    if(!keys.length&&siblingKeys.length){
      // Erasing one alias must never erase a live canonical sibling.
      await invalidateDirectoryEvents(tx,scope);
      await io.record(scope,0,0,false,at,pointerSnapshot.canonicalId);
      return {manifests:0,rows:0,inserted:0,deleted:0,complete:false,reasons:["canonical_scope_still_live"]};
    }
    if (keys.length === 0) {
      // Erased: no live manifest is left, so every row of the scope goes, in or
      // out of the window. One predicate delete, no listing, so no size can
      // truncate it. It throws on failure and the message retries.
      // Staged: the scope's staged runs (and their stage rows) go FIRST, so no write page of a
      // run already past its scope-lock check can put erased rows back after the index erase
      // (a write page takes the same scope lock this transaction holds).
      if (deps.staged && deps.adapter) await eraseStagedReconcileRuns(tx, scope, [...new Set([scope.sessionId, pointerSnapshot.canonicalId])]);
      await io.erase(scope,pointerSnapshot.canonicalId);
      // Erasure purges the fact tables whenever they exist, flag or not: turning the producer
      // off must never leave an erased session's facts behind.
      if (chFacts) {
        const schema = await chFacts.schemaReady();
        if (schema.present) {
          const fScope = factsScope(scope, pointerSnapshot.canonicalId);
          await chFacts.erase(fScope);
          if (schema.ready) {
            const prior = await chFacts.previousState(fScope);
            await chFacts.recordState(fScope, { complete: true, at, range: null, phase: "final", sourceVersion: passVersion(at, prior.version) });
          }
        }
      }
      // Postgres (flag on): the scope's facts and state rows go in this transaction; a re-upload
      // starts from no state, so its fresh epoch is never refused as stale.
      if (pgFacts) await pgFacts.erase(factsScope(scope, pointerSnapshot.canonicalId));
      await io.record(scope,0,0,true,at,pointerSnapshot.canonicalId);
      await completeErasedDirectoryEvents(tx,scope);
      return { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: true, reasons: ["erased"] };
    }
    // SEARCH_INDEX_SKIP_CURRENT_ENABLED: under both locks, after every branch that must run (canonical
    // mismatch, alias still live, erase), before any object read or index query.
    if (deps.skipCurrent && (await alreadyCurrent(tx, scope, pointerSnapshot, keys.length, deps.outputFacts ? deps.ch : undefined)))
      return { manifests: keys.length, rows: 0, inserted: 0, deleted: 0, complete: true, skipped: true, reasons: [ALREADY_CURRENT] };
    const fromMs = await io.fromMs(at);
    const s3 = await readCanonicalScope(scope, siblingKeys, { ...deps, decompress: deps.decompress ?? defaultDecompress }, limits, version, pointerSnapshot.canonicalId);
    // Output facts need the whole read, or the staged run's facts hooks (Postgres store only): with a
    // facts store and no staged run, a too-large scope stays incomplete (scope_too_large) as before.
    const stagedFacts = !!deps.outputFactsStore && !!deps.staged && !!deps.adapter;
    if (
      deps.stream && !deps.outputFacts && (!deps.outputFactsStore || stagedFacts) && siblingKeys.length &&
      s3.partial && s3.admitted && s3.reasons.length === 1 && s3.reasons[0] === "scope_too_large"
    ) {
      if (deps.streamSlot?.used)
        return { manifests: s3.manifests, rows: 0, inserted: 0, deleted: 0, complete: false, deferred: true, reasons: ["stream_deferred"] };
      if (deps.streamSlot) deps.streamSlot.used = true;
      if (deps.staged && deps.adapter) {
        handoff = {
          admitted: s3.admitted, revision: s3.revision, version, fromMs, at, manifests: s3.manifests, epoch: pointerSnapshot.epoch,
          stamp: { canonicalId: pointerSnapshot.canonicalId, epoch: pointerSnapshot.epoch, manifestFingerprint: await sourceFingerprint(tx, scope, pointerSnapshot.canonicalId) },
        };
        return { manifests: s3.manifests, rows: 0, inserted: 0, deleted: 0, complete: false, reasons: [] };
      }
      // Never with a facts store: the in-transaction streamed reconcile builds no facts.
      if (deps.outputFactsStore) throw new Error("search index: the streamed reconcile cannot carry output facts");
      return streamedReconcile(tx, io, scope, deps, deps.stream, limits, pointerSnapshot, s3.admitted, s3.revision, version, fromMs, at);
    }
    if(!siblingKeys.length||!s3.complete||s3.directorySessionId!==pointerSnapshot.canonicalId||s3.events===null){
      await invalidateDirectoryEvents(tx,scope);
      await io.record(scope,s3.revision,0,false,at,pointerSnapshot.canonicalId);
      // The facts are not rewritten from a partial read: they now owe a pass too (over the
      // span they last covered, so only windows that span overlaps are refused).
      if (deps.outputFacts && (await chFacts!.schemaReady()).ready) {
        const fScope = factsScope(scope, pointerSnapshot.canonicalId);
        const prior = await chFacts!.previousState(fScope);
        await chFacts!.recordState(fScope, { complete: false, at, range: prior.range, phase: "final", sourceVersion: passVersion(at, prior.version) });
      }
      if (pgFacts) {
        const fScope = factsScope(scope, pointerSnapshot.canonicalId);
        const prior = await pgFacts.previousState(fScope);
        await pgFacts.recordState(fScope, { complete: false, at, range: prior.range, sourceVersion: Number(pointerSnapshot.epoch) });
      }
      return {manifests:s3.manifests,rows:0,inserted:0,deleted:0,complete:false,...(s3.partial?{partial:true}:{}),reasons:s3.reasons.length?s3.reasons:["canonical_metadata_incomplete"]};
    }
    const inWindow = s3.rows.filter((r) => tsMillis(`${r.ts.replace(" ", "T")}Z`) >= fromMs);
    // Staged: keyset pages up to the keyed-records cap (never below SEARCH_INDEX_MAX_ROWS).
    const stagedScan = deps.staged && io.scanPages ? stagedScanBounds(deps.staged, limits) : undefined;
    let scan: Awaited<ReturnType<SearchProjectionIo["scan"]>>;
    if (stagedScan) {
      // Between pages, as the streamed scan: the deadline (if any) and a throttled keepalive, so a
      // long walk neither outlives the deadline nor idles the transaction past the pool's timeout.
      const everyMs = deps.streamKeepaliveMs ?? STREAM_KEEPALIVE_MS;
      let pinged = Date.now();
      try {
        scan = await io.scanPages!(scope, pointerSnapshot.canonicalId, fromMs, stagedScan.pageSize, stagedScan.maxRows, async () => {
          if (streamDeadlinePassed(deps.streamDeadline)) throw new StreamBudgetError(STREAM_DEADLINE);
          if (Date.now() - pinged < everyMs) return;
          await tx.query("SELECT 1 AS stream_keepalive");
          pinged = Date.now();
        });
      } catch (error) {
        if (!(error instanceof StreamBudgetError)) throw error;
        // Past the deadline: nothing written or deleted, the scope owes a pass.
        await invalidateDirectoryEvents(tx, scope);
        await io.record(scope, s3.revision, 0, false, at, pointerSnapshot.canonicalId);
        return { manifests: s3.manifests, rows: 0, inserted: 0, deleted: 0, complete: false, reasons: [...s3.reasons, error.budget] };
      }
    } else scan = await io.scan(scope, pointerSnapshot.canonicalId, fromMs, limits.maxRows);
    const chRows = scan.rows;
    const truncated = scan.truncated;
    const existing = new Map<string, ScopeIdentity[]>();
    for (const r of chRows) existing.set(keyOf(r), [...(existing.get(keyOf(r)) ?? []), r]);

    const toInsert = inWindow.filter((r) => {
      const tsMs = tsMillis(`${r.ts.replace(" ", "T")}Z`);
      const same = existing.get(keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id })) ?? [];
      return !same.some((c) => c.sessionId === r.session_id && c.workspaceId === r.workspace_id && c.tsMs === tsMs && c.fingerprint === r.fingerprint);
    });
    let inserted = 0;
    for (let i = 0; i < toInsert.length; i += limits.insertBatch)
      inserted += await io.upsert(toInsert.slice(i, i + limits.insertBatch), scope, pointerSnapshot.canonicalId);

    const deletes: ScopeIdentity[] = [];
    let deletedCount = 0;
    // A version-guarded adapter may refuse a stale write. Never delete against
    // that older snapshot or mark its projection complete.
    const staleWrite = inserted !== toInsert.length;
    let complete = s3.complete && !truncated && !staleWrite;
    if (!complete) await invalidateDirectoryEvents(tx, scope);
    if (complete) {
      const wanted = new Map(inWindow.map((r) => [keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id }), r]));
      // Only kinds this pass can regenerate from S3 are diffed. ClickHouse also holds rows of
      // kinds the exported delivery stream never carries (e.g. tool_result/user/assistant from
      // other ingest paths); absent from S3 is not "retracted" for those, so they are kept.
      // Erasure (no live pointer, above) still removes every kind of the scope.
      const regenerable = new Set<string>([...S3_ACTIVITY_KINDS, ...inWindow.map((r) => r.kind)]);
      for (const [key, list] of existing) {
        const current = wanted.get(key);
        for (const c of list) {
          if (!regenerable.has(c.kind)) continue;
          // A buffered-origin row (buffered-search-feed.ts) holds a record S3 manifests do not carry
          // yet: absent from S3 is not "retracted". It is replaced once canonical carries the same
          // event; erasure of the session (no live pointer, above) still removes it.
          if (!current && c.fingerprint.startsWith(BUFFERED_FINGERPRINT_PREFIX)) continue;
          if (!current || current.session_id !== c.sessionId || current.workspace_id !== c.workspaceId || tsMillis(`${current.ts.replace(" ", "T")}Z`) !== c.tsMs)
            deletes.push(c);
        }
      }
      deletedCount = await io.remove(scope, deletes);
    }
    // Metadata and CH describe the same validated canonical source union.
    if(complete){
      const directoryKey={orgId:scope.orgId,workspaceId:scope.workspaceId,harness:scope.harness,sessionId:pointerSnapshot.canonicalId};
      const written=await (deps.writeEvents??writeDirectoryEvents)(tx,directoryKey,{sourceSessionId:scope.sessionId,expectedEpoch:pointerSnapshot.epoch,sources:s3.sources,events:s3.events});
      if(written.written)await (deps.writeCatalog??directoryCatalogWriter(deps.desktopCatalog===true))(tx,directoryKey,s3.catalog,{expectedEpoch:pointerSnapshot.epoch,sourceSessionId:scope.sessionId});
      else {complete=false;s3.reasons.push("directory_event_sources_changed");await invalidateDirectoryEvents(tx,scope);}
    }
    let outputFacts: SearchIndexResult["outputFacts"];
    if (deps.outputFacts) {
      // The whole scope (not the transcript window), from the same validated read. An
      // incomplete pass inserts what it read and deletes nothing.
      // Every session id this pass saw (scope, canonical, and any owner/alias id a projected event
      // carries): the facts are all written under the canonical id, and the state rows, the delete
      // filter and the route's readiness join all use this same set.
      const fScope = factsScope(scope, pointerSnapshot.canonicalId, s3.projectionRows);
      if (!(await chFacts!.schemaReady()).ready) {
        // Flag on before the owner applied the DDL (or an older skeleton): write nothing, say why.
        // Search is unaffected; the Output route has no state rows and refuses.
        outputFacts = { inserted: 0, deleted: 0, complete: false, reasons: ["output_facts_schema_missing"] };
      } else {
        // Strictly increasing per scope, even for two passes in one ms (fact rows and state alike).
        const prev = await chFacts!.previousState(fScope);
        const base = passVersion(at, prev.version);
        const facts = outputFactsFromScope(fScope, { projectionRows: s3.projectionRows, records: s3.records, complete }, base);
        const range = facts.range ?? null;
        // Pending first: the ClickHouse inserts and deletes are not transactional, so a pass that
        // dies mid-reconcile must leave the scope incomplete, never complete over torn facts.
        const prior = prev.range;
        const span = prior && range ? { min: prior.min < range.min ? prior.min : range.min, max: prior.max > range.max ? prior.max : range.max } : (range ?? prior);
        await chFacts!.recordState(fScope, { complete: false, at, range: span, phase: "pending", sourceVersion: base });
        const r = await chFacts!.reconcile(fScope, { eventFacts: facts.events, outputFacts: facts.facts, complete: facts.complete, reasons: facts.reasons }, base);
        outputFacts = { inserted: r.inserted, deleted: r.deleted, complete: r.complete, reasons: r.reasons };
        await chFacts!.recordState(fScope, { complete: r.complete, at, range: r.complete ? range : span, phase: "final", sourceVersion: base });
      }
    } else if (pgFacts) {
      // Self-host: the same builder over the same validated read, written after the directory
      // events on this transaction. Versioned by the directory epoch this pass read.
      const fScope = factsScope(scope, pointerSnapshot.canonicalId, s3.projectionRows);
      const sourceVersion = Number(pointerSnapshot.epoch);
      const prev = await pgFacts.previousState(fScope);
      const facts = outputFactsFromScope(fScope, { projectionRows: s3.projectionRows, records: s3.records, complete }, sourceVersion);
      const range = facts.range ?? null;
      const r = await pgFacts.reconcile(fScope, { eventFacts: facts.events, outputFacts: facts.facts, complete: facts.complete, reasons: facts.reasons }, sourceVersion);
      outputFacts = { inserted: r.inserted, deleted: r.deleted, complete: r.complete, reasons: r.reasons };
      if (r.applied) {
        const prior = prev.range;
        const span = prior && range ? { min: prior.min < range.min ? prior.min : range.min, max: prior.max > range.max ? prior.max : range.max } : (range ?? prior);
        await pgFacts.recordState(fScope, { complete: r.complete, at, range: r.complete ? range : span, sourceVersion });
      }
    }
    // Only a complete pass makes the session current; any other leaves it owing one.
    await io.record(scope, s3.revision, inWindow.length, complete, at, pointerSnapshot.canonicalId);
    return {
      manifests: s3.manifests,
      rows: inWindow.length,
      inserted,
      deleted: deletedCount,
      complete,
      reasons: [...s3.reasons, ...(truncated ? [stagedScan ? STREAM_KEYED_RECORDS_CAP : deps.adapter ? "search_index_truncated" : "clickhouse_truncated"] : []), ...(staleWrite ? ["search_version_conflict"] : [])],
      ...(outputFacts ? { outputFacts } : {}),
    };
  });
  return handoff ? stagedSearchScope(scope, deps, limits, handoff) : first;
}

/** What the index transaction hands a staged run: the admitted read and the source stamp it saw. */
interface StagedHandoff {
  admitted: NonNullable<Awaited<ReturnType<typeof readCanonicalScope>>["admitted"]>;
  revision: number;
  version: number;
  fromMs: number;
  at: Date;
  manifests: number;
  /** The directory epoch the pointer snapshot read: the directory write's expected epoch. */
  epoch: string;
  stamp: SourceStamp;
}

/**
 * The staged run's manifest fingerprint: the scope's live pointer rows (manifest key and
 * revision) under its canonical id, from the database, so the final step re-checks it without a
 * source read.
 */
async function sourceFingerprint(tx: SqlExecutor, scope: SessionScope, canonicalId: string): Promise<string> {
  const { rows } = await tx.query<{ manifest_key: string; revision: string }>(
    `SELECT manifest_key, revision::text AS revision FROM sessions.session_index
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id,session_id)=$4 AND deleted_at IS NULL`,
    [scope.orgId, scope.workspaceId, scope.harness, canonicalId],
  );
  return admittedManifestFingerprint(rows.map((r) => ({ manifestKey: r.manifest_key, revision: r.revision })));
}

/** Index scan cursors: page number (so cursors only grow) and the position to continue from. */
const STAGED_CURSOR_MAX = 480;
function encodeScanCursor(page: number, next: SearchPosition): string {
  const head = String(page).padStart(12, "0");
  const json = JSON.stringify(next);
  // A position too long for the run's cursor column resumes the scan from its start instead
  // (staging is idempotent per identity); the page number still grows.
  return head.length + 1 + json.length <= STAGED_CURSOR_MAX ? `${head}|${json}` : `${head}~`;
}
function decodeScanCursor(cursor: string | null): { page: number; after?: SearchPosition } {
  if (cursor === null) return { page: 0 };
  const page = Number(cursor.slice(0, 12));
  return cursor[12] === "|" ? { page, after: JSON.parse(cursor.slice(13)) as SearchPosition } : { page };
}

const STAGED_BUDGETS = new Set<string>([STREAM_READ_BYTES_BUDGET, STREAM_READ_EVENTS_BUDGET, STREAM_DEADLINE, STREAM_KEYED_RECORDS_CAP]);

/**
 * SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true, adapter binding: the streamed reconcile of a scope
 * the whole read refused as too large (`scope_too_large` alone), as a staged run
 * (staged-reconcile.ts): passes 1-3 and pass 4 read the source outside any transaction, the
 * diff state lives in the 062110 stage table, every batch commits on its own, and only the final
 * step holds the scope and canonical locks. Same rows, deletes, directory events, catalog and
 * state as streamedReconcile.
 *
 * Resume is the reconcile tick's: a successful claim records the scope as owing a pass (state
 * partial), so an interrupted run is listed by the tick and resumes at its committed cursor once
 * its lease expires. Nothing here answers `busy` (the queue's requeue and dead-letter path):
 * Every outcome after a successful claim that is not done leaves the scope owing (directory events
 * invalidated, state incomplete), except staged_lease_lost (the taker or the erase owns the state);
 * staged_run_leased claimed nothing and writes nothing.
 *   staged_run_leased       another tick holds the run's live lease; nothing written
 *   scope_busy / canonical_scope_busy   a lock was held at a write page or the final step; lease released
 *   stream_source_changed   the stamp moved; the run was abandoned and restarts on the tick
 *   output_facts_stage_incomplete   a hook sent the run round again (StagedRunRestart)
 *   staged_run_failed       past the attempts cap (not a slow-retry claim): abandoned before any read
 *   staged_lease_lost       the run was taken over or erased under this tick
 *   staged_run_interrupted  a step failed after the claim (logged: class and code); lease released
 */
async function stagedSearchScopeRun(
  scope: SessionScope, deps: SearchIndexDeps, limits: SearchIndexLimits, h: StagedHandoff, seen: { attempts?: number; cachedRefusal?: boolean },
): Promise<SearchIndexResult> {
  const binding = deps.adapter!;
  const budget = deps.stream!;
  const staged = deps.staged!;
  const canonicalId = h.stamp.canonicalId;
  let read: Awaited<ReturnType<typeof streamCanonicalScope>> | undefined;
  let claimed = false;
  const stream = () => (read ? { stream: { passes: read.meter.passes, chunks: read.meter.chunks, bytes: read.meter.bytes, events: read.meter.events } } : {});
  const result = (reasons: string[], extra: Partial<SearchIndexResult> = {}): SearchIndexResult => ({
    manifests: h.manifests, rows: 0, inserted: 0, deleted: 0, complete: false, reasons, ...stream(), ...extra,
  });
  const ops: StagedReconcileOps<NonNullable<typeof read>> = {
    async readSource() {
      read = await streamCanonicalScope(
        scope, h.admitted,
        {
          readerMaxRuleset: deps.readerMaxRuleset, store: deps.store, prefixFor: deps.prefixFor.bind(deps), decompress: deps.decompress ?? defaultDecompress,
          ...(deps.originLabels ? { originLabels: true as const } : {}),
          // The same deadline as the in-transaction stream (between chunks of every pass, the facts pass included).
          ...(deps.streamDeadline ? { deadline: deps.streamDeadline } : {}),
          perPass: { keyedRecordsCap: staged.keyedRecordsCap },
        },
        budget, h.version, h.fromMs,
      );
      return read.complete && read.directorySessionId === canonicalId && read.events !== null ? read : null;
    },
    wanted: (r) => [...r.wanted].map(([key, w]) => ({ key, ...r.owners[w.owner]!, tsMs: w.tsMs, fingerprint: w.fingerprint })),
    regenerableKinds: (r) => [...S3_ACTIVITY_KINDS, ...r.kinds],
    // The facts pass (output-facts hooks only): the whole read's facts input, chunked, from one more pass.
    factsPass: (r, chunk, emit) => r.facts(chunk, emit),
    sourceComplete: (r) => r.complete,
    async scanPage(_tx, after, limit) {
      const at = decodeScanCursor(after);
      const page = await adapterScanPage(binding, scope, canonicalId, h.fromMs, limit, at.after);
      return {
        rows: page.rows.map((r) => ({ key: streamIndexKey(r), machineId: r.machineId, sessionId: r.sessionId, source: r.source, kind: r.kind, eventId: r.eventId, workspaceId: r.workspaceId, tsMs: r.tsMs, fingerprint: r.fingerprint })),
        next: page.next ? encodeScanCursor(at.page + 1, page.next) : null,
      };
    },
    async regenerate(tx, r, keys, emit) {
      const io = adapterProjectionIo(binding, tx);
      let written = 0;
      await r.regenerate(keys, limits.insertBatch, async (rows) => {
        written += await io.upsert(rows, scope, canonicalId);
        emit?.(rows);
      });
      return written;
    },
    async currentSource(tx) {
      const snapshot = await livePointerSnapshot(tx, scope);
      return { canonicalId: snapshot.canonicalId, epoch: snapshot.epoch, manifestFingerprint: await sourceFingerprint(tx, scope, snapshot.canonicalId) };
    },
    async remove(tx, identities) {
      return adapterProjectionIo(binding, tx).remove(scope, identities.map(({ key: _key, ...position }: StagedIdentity) => position));
    },
    async commit(tx, r, pass) {
      const io = adapterProjectionIo(binding, tx);
      let complete = pass.complete;
      const reasons: string[] = [];
      if (!complete) await invalidateDirectoryEvents(tx, scope);
      else {
        const directoryKey = { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: canonicalId };
        const written = await (deps.writeEvents ?? writeDirectoryEvents)(tx, directoryKey, { sourceSessionId: scope.sessionId, expectedEpoch: h.epoch, sources: r.sources, events: r.events! });
        if (written.written) await (deps.writeCatalog ?? directoryCatalogWriter(deps.desktopCatalog === true))(tx, directoryKey, r.catalog, { expectedEpoch: h.epoch, sourceSessionId: scope.sessionId });
        else {
          complete = false;
          reasons.push("directory_event_sources_changed");
          await invalidateDirectoryEvents(tx, scope);
        }
      }
      await io.record(scope, h.revision, r.inWindow, complete, h.at, canonicalId);
      return { complete, reasons };
    },
    // Without the scope locks, on purpose: it only ever marks the scope owing. Racing a final step
    // that recorded the scope current, it can at worst leave a current scope owing one more pass.
    async claimed(tx, run) {
      seen.attempts = run.attempts;
      await adapterProjectionIo(binding, tx).record(scope, h.revision, 0, false, h.at, canonicalId);
      // The Postgres facts owe a pass too, in the claim's own transaction (so a run killed after the claim
      // is covered), exactly as the whole read's partial branch records them: complete false over the span
      // they last covered, versioned by the directory epoch the run started from. The store's version rule
      // refuses it against a newer epoch (that state is newer than this run). Racing a final step that
      // just recorded the facts current at the same epoch, it can at worst leave them owing one more pass,
      // as `owing` can for the search state; only this run's own final step records them complete again.
      if (deps.outputFactsStore) {
        const store = deps.outputFactsStore(tx);
        const fScope = factsScope(scope, canonicalId);
        const prior = await store.previousState(fScope);
        await store.recordState(fScope, { complete: false, at: h.at, range: prior.range, sourceVersion: Number(h.epoch) });
      }
    },
  };
  // Without the scope locks, as `claimed`: it only invalidates and marks owing, so a race with a
  // final step that recorded the scope current errs toward one more pass, never toward stale data.
  const owing = async (reasons: string[], partial: boolean): Promise<SearchIndexResult> => {
    await deps.transact(async (tx) => {
      await invalidateDirectoryEvents(tx, scope);
      await adapterProjectionIo(binding, tx).record(scope, h.revision, 0, false, h.at, canonicalId);
    });
    return result(reasons, partial ? { partial: true } : {});
  };
  // Self-host output facts (the Postgres store): staged through the run's hooks.
  let facts: SearchIndexResult["outputFacts"];
  const factsHooks = deps.outputFactsStore
    ? stagedOutputFactsHooks(scope, canonicalId, h, staged.factsRecordBytesCap ?? STAGED_FACTS_RECORD_BYTES_CAP, (r) => (facts = r))
    : undefined;
  // A size refusal this instance saw for the same source stamp: skip the claim and the source read (G2
  // fast-fail; StagedRefusals). The earlier run's claim already recorded the search and facts state
  // owing, so owing() answers as that abandon did.
  const refusals = stagedRefusalsOf(staged);
  const refusalKey = stagedRefusalKey(scope, canonicalId);
  const refusalStamp = stagedRefusalStamp(h.stamp);
  const nowMs = (deps.now?.() ?? new Date()).getTime();
  const refused = refusals.hit(refusalKey, refusalStamp, nowMs);
  if (refused !== null) {
    seen.cachedRefusal = true;
    // owing() writes the scope's search state owing and invalidates its directory events, as the abandoned
    // run's pass did. Equivalent, except while a peer holds a live lease on the run: the old path then
    // answered staged_run_leased and wrote nothing; this one marks the scope owing (the peer's final step
    // records it current again).
    return owing([refused], false);
  }
  let deterministicAbandon = false;
  let outcome: Awaited<ReturnType<typeof runStagedReconcile>>;
  try {
    outcome = await runStagedReconcile(scope, h.stamp, ops, {
      transact: deps.transact,
      scanPage: Math.min(limits.insertBatch, MAX_SCAN_PAGE),
      deletePage: Math.min(limits.insertBatch, MAX_SCAN_PAGE),
      keepFingerprintPrefix: BUFFERED_FINGERPRINT_PREFIX,
      abandonOn: (error) => {
        // The G2 fast-fail caches only a refusal that recurs for the same source (not 53200 out_of_memory).
        if (isDeterministicRefusal(error)) deterministicAbandon = true;
        return error instanceof StreamBudgetError ? error.budget : error instanceof PipelineError && !error.retryable ? error.code : null;
      },
      releaseOnError: true,
      attemptsCap: { cap: STAGED_RUN_MAX_ATTEMPTS, retryEvery: STAGED_RUN_RETRY_EVERY },
      onHeartbeatError: (error) =>
        log.warn("search index staged lease renewal failed", {
          stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId,
          ...(error instanceof Error ? { error_name: error.name } : {}), ...errorFields(error),
        }),
      ...(factsHooks ? { hooks: factsHooks } : {}),
      ...staged.run,
      // Never more inputs per facts chunk than one staged call takes (output-facts-store.ts).
      factsChunk: Math.min(staged.run?.factsChunk ?? STAGED_FACTS_CHUNK, MAX_FACTS_CHUNK_INPUTS),
      // `claimed` only once the claim committed (a failed claim commit is the caller's error).
      boundary: async (point) => {
        if (point === "claimed") claimed = true;
        await staged.run?.boundary?.(point);
      },
    });
  } catch (error) {
    if (!claimed) throw error;
    // RH062 happens only after a takeover (the taker's claim already recorded the scope owing) or
    // when the run row is gone (erased, or the taker finished and recorded the scope current): this
    // pass writes nothing, or it would rewrite state with an older revision after an erase, or flip a
    // scope the taker just finished back to owing.
    if ((error as { code?: unknown })?.code === STAGED_RECONCILE_STALE_LEASE) return result(["staged_lease_lost"]);
    // Every other outcome after the claim but done leaves the scope owing a pass: the tick resumes it.
    // A run that keeps failing is capped by the attempts cap (attemptsCap below, staged_run_failed).
    log.warn("search index staged run interrupted", {
      stage: "search-index", org_id: scope.orgId, session_id: scope.sessionId,
      ...(error instanceof Error ? { error_name: error.name } : {}), ...errorFields(error),
    });
    return owing(["staged_run_interrupted"], true);
  }
  switch (outcome.status) {
    case "busy":
      // Not claimed: the holder of the live lease is responsible for the scope.
      if (outcome.reason === "run_leased") return result(["staged_run_leased"]);
      return owing([outcome.reason], false);
    case "restart":
      return owing([outcome.reason === "source_changed" ? STREAM_SOURCE_CHANGED : outcome.reason], false);
    case "incomplete": {
      const [reason] = outcome.reasons;
      if (reason === "source_incomplete") {
        const r = read!;
        return owing(r.reasons.length ? r.reasons : ["canonical_metadata_incomplete"], r.partial);
      }
      if (reason && STAGED_BUDGETS.has(reason)) return owing(["scope_too_large", reason], true);
      // A deterministic size refusal (a chunk, or the run's record bytes): the next pass of the same
      // source skips the claim and the read until it expires (StagedRefusals).
      if (reason === OUTPUT_FACTS_CHUNK_UNSTAGEABLE && deterministicAbandon) refusals.set(refusalKey, refusalStamp, reason, nowMs);
      return owing(outcome.reasons, false);
    }
    case "failed":
      // Past the attempts cap: abandoned before any read; owing, and retried every Nth claim only.
      return owing([STAGED_RUN_FAILED], false);
    case "done":
      refusals.clear(refusalKey);
      return {
        manifests: h.manifests,
        rows: read?.inWindow ?? 0,
        inserted: outcome.inserted,
        deleted: outcome.deleted,
        complete: outcome.complete,
        reasons: [...(read?.reasons ?? []), ...outcome.reasons],
        ...stream(),
        ...(facts ? { outputFacts: facts } : {}),
      };
  }
}

/** A run claimed more than this many times since it last finished or was reset is failed (staged_run_failed). */
export const STAGED_RUN_MAX_ATTEMPTS = 24;
/** Past the cap, every this-many-th claim still runs (about 6 hours at the default 5-minute tick). */
export const STAGED_RUN_RETRY_EVERY = 72;
export const STAGED_RUN_FAILED = "staged_run_failed";

/** The summary line's outcome, from the pass result. */
function stagedOutcome(r: SearchIndexResult): "done" | "busy" | "restart" | "failed" | "incomplete" {
  if (r.complete) return "done";
  const [first] = r.reasons;
  if (first === "staged_run_leased" || first === "scope_busy" || first === "canonical_scope_busy") return "busy";
  if (first === STREAM_SOURCE_CHANGED || first === "output_facts_stage_incomplete") return "restart";
  if (first === STAGED_RUN_FAILED) return "failed";
  return "incomplete";
}

/**
 * The staged run of one scope, with ONE summary line per pass ("search index staged run", fields
 * only: the scope, the outcome, the first reason, the claim count, rows, inserted, deleted, the
 * duration). Warn for incomplete and failed outcomes, info otherwise. Never on the flag-off path.
 */
async function stagedSearchScope(scope: SessionScope, deps: SearchIndexDeps, limits: SearchIndexLimits, h: StagedHandoff): Promise<SearchIndexResult> {
  const started = Date.now();
  const seen: { attempts?: number; cachedRefusal?: boolean } = {};
  const scopeFields = { stage: "search-index", org_id: scope.orgId, workspace_id: scope.workspaceId, harness: scope.harness, session_id: scope.sessionId };
  let out: SearchIndexResult;
  try {
    out = await stagedSearchScopeRun(scope, deps, limits, h, seen);
  } catch (error) {
    // Thrown (before the claim, in the reset, in owing()): still one line, then the error goes on.
    log.warn("search index staged run", {
      ...scopeFields, outcome: "incomplete", ...(seen.attempts === undefined ? {} : { attempt: seen.attempts }),
      ...(seen.cachedRefusal ? { cached_refusal: true } : {}), duration_ms: Date.now() - started,
    });
    throw error;
  }
  const outcome = stagedOutcome(out);
  log[outcome === "incomplete" || outcome === "failed" ? "warn" : "info"]("search index staged run", {
    ...scopeFields,
    outcome, ...(out.reasons[0] ? { reason: out.reasons[0] } : {}), ...(seen.attempts === undefined ? {} : { attempt: seen.attempts }),
    ...(seen.cachedRefusal ? { cached_refusal: true } : {}),
    count: out.rows, inserted: out.inserted, deleted: out.deleted, duration_ms: Date.now() - started,
  });
  return out;
}

/** How long a cached size refusal holds before the scope is claimed and run again (StagedRefusals). */
export const STAGED_REFUSAL_TTL_MS = 24 * 60 * 60 * 1000;
/** The most scopes one instance keeps a size refusal for; past it the oldest entry goes (it then costs one more pass). */
export const STAGED_REFUSAL_MAX_ENTRIES = 10_000;

/**
 * The G2 fast-fail: per indexer instance (in memory, lost on restart, each replica its own), the scopes
 * whose staged run was abandoned as output_facts_chunk_unstageable, with the source stamp it ran on. A
 * later pass of the same scope and stamp skips the claim and the source read and answers as that abandon
 * did, until the entry is STAGED_REFUSAL_TTL_MS old (a hedge against a refusal that was not deterministic:
 * a finished session never changes its stamp) or the stamp changes. A finished run clears the scope's
 * entry. Not durable on purpose: the refusal depends on this process's bounds, so a restart or an
 * upgrade that raises them retries by itself.
 */
export class StagedRefusals {
  private readonly entries = new Map<string, { stamp: string; reason: string; at: number }>();
  readonly maxEntries: number;
  constructor(maxEntries = STAGED_REFUSAL_MAX_ENTRIES, readonly ttlMs = STAGED_REFUSAL_TTL_MS) {
    // At least one entry: set() evicts until there is room.
    this.maxEntries = Math.max(1, Math.floor(maxEntries));
  }
  /** The cached reason for this scope and stamp, or null (no entry, another stamp, or expired: then dropped). */
  hit(key: string, stamp: string, now: number): string | null {
    const e = this.entries.get(key);
    if (!e) return null;
    if (e.stamp !== stamp || now - e.at >= this.ttlMs) {
      this.entries.delete(key);
      return null;
    }
    return e.reason;
  }
  set(key: string, stamp: string, reason: string, now: number): void {
    this.entries.delete(key);
    while (this.entries.size > 0 && this.entries.size >= this.maxEntries) this.entries.delete(this.entries.keys().next().value!);
    this.entries.set(key, { stamp, reason, at: now });
  }
  clear(key: string): void {
    this.entries.delete(key);
  }
  get size(): number {
    return this.entries.size;
  }
}
const stagedRefusalsByStaged = new WeakMap<StagedReconcile, StagedRefusals>();
function stagedRefusalsOf(staged: StagedReconcile): StagedRefusals {
  if (staged.refusals) return staged.refusals;
  let r = stagedRefusalsByStaged.get(staged);
  if (!r) stagedRefusalsByStaged.set(staged, (r = new StagedRefusals()));
  return r;
}
/** The cache key of a scope (exported for tests). */
export const stagedRefusalKey = (scope: SessionScope, canonicalId: string) => JSON.stringify([scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, canonicalId]);
const stagedRefusalStamp = (stamp: SourceStamp) => JSON.stringify([stamp.canonicalId, stamp.epoch, stamp.manifestFingerprint]);

/** Inputs per facts chunk of a staged run (the module's default; clamped to MAX_FACTS_CHUNK_INPUTS). */
export const STAGED_FACTS_CHUNK = 5000;

/**
 * The staged run's output-facts hooks (slice 2c, migration 20261105062140; RELAYHISTORY_SELFHOST_OUTPUT_FACTS):
 * every facts chunk is staged in its own transaction, the want -> have advance seals the stage, and the
 * final transaction (after `commit`, before the run finishes) reconciles the facts from the stage and
 * records their state. A stage that does not hold the whole facts pass throws a StagedRunRestart (the
 * run goes round again, the facts tables untouched); a chunk that cannot be staged throws a
 * non-retryable PipelineError (abandonOn: the run abandons with output_facts_chunk_unstageable).
 * Versioned by the directory epoch the run started from, as the whole-set path is.
 */
function stagedOutputFactsHooks(
  scope: SessionScope, canonicalId: string, h: StagedHandoff, recordBytesCap: number,
  done: (r: NonNullable<SearchIndexResult["outputFacts"]>) => void,
): StagedReconcileHooks {
  const fScope = { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: scope.sessionId, canonicalId };
  const sourceVersion = Number(h.epoch);
  // The record bytes this attempt's facts pass staged (STAGED_FACTS_RECORD_BYTES_CAP); chunk 0 starts it over.
  const recordBytes = { used: 0, counted: 0, cap: recordBytesCap, wideWeight: STAGED_FACTS_WIDE_TEXT_WEIGHT };
  return {
    async onBatch(tx, run, batch) {
      if (batch.phase !== "facts") return;
      if (batch.chunk.seq === 0) recordBytes.used = recordBytes.counted = 0;
      await stageOutputFactsChunk(tx, run, fScope, batch.events, batch.chunk, sourceVersion, recordBytes);
    },
    async onWantDone(tx, run) {
      await sealOutputFactsWant(tx, run);
    },
    async onFinish(tx, run, pass) {
      // The whole-set path builds from the read's completeness, the pass's (truncation, stale write) and the
      // directory write's (directory_event_sources_changed): any of them false inserts what was read and deletes nothing.
      const r = await finishStagedOutputFacts(tx, run, fScope, {
        sourceComplete: pass.sourceComplete && pass.complete && pass.committedComplete, sourceVersion, at: h.at,
      });
      done({ inserted: r.inserted, deleted: r.deleted, complete: r.complete, reasons: r.reasons });
    },
  };
}

export const ALREADY_CURRENT = "already_current";

/**
 * Does a complete pass already cover this scope's live source set? Proven by the session directory row
 * of the canonical id (the row writeDirectoryEvents writes, picked as livePointerSnapshot picks it):
 *   - event_projection_ready: set only by writeDirectoryEvents of a COMPLETE pass, in the pass's own
 *     transaction, and only when the live pointer set equals the pass's sources; reset by every
 *     incomplete pass (invalidateDirectoryEvents) and by the session_index trigger on ANY pointer insert,
 *     update or delete of the scope or its canonical id (a new revision, a sibling added or removed, a
 *     canonical change, a tombstone), which also bumps the epoch;
 *   - event_projection_sources equal to the live manifest_key -> revision map of the canonical id, the
 *     same expression writeDirectoryEvents compares;
 *   - event_projection_version 1 and the row not deleted; the scope has live pointers.
 * (The directory epoch is not compared: this pass's snapshot reads it from the same row, and the trigger
 * that moves it on a pointer change resets readiness in the same statement.)
 * With the ClickHouse output facts on (`factsCh`), the facts state of the canonical id must be complete
 * too: a complete search pass can record its facts incomplete. Not detected: a change of reader ruleset,
 * origin labels or projection code that re-fingerprints rows without a pointer change.
 */
async function alreadyCurrent(
  tx: SqlExecutor,
  scope: SessionScope,
  snapshot: { canonicalId: string },
  liveKeys: number,
  factsCh?: ClickHouseClient,
): Promise<boolean> {
  if (liveKeys === 0) return false;
  const { rows } = await tx.query<{ current: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM (
         SELECT d.deleted_at, d.event_projection_ready, d.event_projection_version, d.event_projection_epoch, d.event_projection_sources
           FROM sessions.session_directory d
          WHERE d.org_id=$1 AND d.workspace_id=$2 AND d.harness=$3 AND d.session_id IN ($4,$5)
          ORDER BY (d.session_id=$4) DESC LIMIT 1
       ) d
       WHERE d.deleted_at IS NULL AND d.event_projection_ready AND d.event_projection_version=1
         AND d.event_projection_sources=(
           SELECT jsonb_object_agg(p.manifest_key,p.revision::text) FROM sessions.session_index p
            WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.harness=$3
              AND coalesce(p.canonical_session_id,p.session_id)=$4 AND p.deleted_at IS NULL)
     ) AS current`,
    [scope.orgId, scope.workspaceId, scope.harness, snapshot.canonicalId, scope.sessionId],
  );
  if (rows[0]?.current !== true) return false;
  if (!factsCh) return true;
  return outputFactsComplete(factsCh, { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: snapshot.canonicalId });
}

/** The streaming reader's keepalive interval: a third of the consumer pool's 60 s idle timeout. */
const STREAM_KEEPALIVE_MS = 20_000;

/**
 * The reconcile of `indexSearchScope` for a scope the whole read refused as too large, from
 * the streaming reader: the same rows, deletes, directory metadata, catalog and state, with
 * the rows regenerated only for the identities the index lacks. Nothing is deleted or
 * marked current unless the streamed read was complete; a failed regeneration keeps the
 * rows it wrote, deletes nothing and leaves the scope owing a pass.
 */
async function streamedReconcile(
  tx: SqlExecutor,
  io: SearchProjectionIo & Partial<Pick<LegacyProjectionIo, "scanPages">>,
  scope: SessionScope,
  deps: SearchIndexDeps,
  budget: StreamReadLimits,
  limits: SearchIndexLimits,
  pointerSnapshot: { epoch: string; canonicalId: string },
  admitted: NonNullable<Awaited<ReturnType<typeof readCanonicalScope>>["admitted"]>,
  revision: number,
  version: number,
  fromMs: number,
  at: Date,
): Promise<SearchIndexResult> {
  // The consumer pool ends a transaction idle for 60 s (lambda.ts). Passes 1 to 3 read S3, and
  // the index scan and deletes run on the search backend, without querying `tx`: keep it busy
  // well inside that, at most every STREAM_KEEPALIVE_MS, or now when `force`d.
  const everyMs = deps.streamKeepaliveMs ?? STREAM_KEEPALIVE_MS;
  let pinged = Number.NEGATIVE_INFINITY;
  const keepalive = async (force = false): Promise<void> => {
    if (!force && Date.now() - pinged < everyMs) return;
    await tx.query("SELECT 1 AS stream_keepalive");
    pinged = Date.now();
  };
  const read = await streamCanonicalScope(
    scope, admitted,
    {
      readerMaxRuleset: deps.readerMaxRuleset, store: deps.store, prefixFor: deps.prefixFor.bind(deps), decompress: deps.decompress ?? defaultDecompress,
      ...(deps.streamDeadline ? { deadline: deps.streamDeadline } : {}), // between chunks of every pass
      keepalive: { everyMs: 0, ping: () => keepalive() }, // throttled here, shared with the scan and deletes
      // The same labels as the whole read (readCanonicalScope gets them through `deps`).
      ...(deps.originLabels ? { originLabels: true as const } : {}),
      // Staged: each pass on its own budget, its keyed records capped.
      ...(deps.staged ? { perPass: { keyedRecordsCap: deps.staged.keyedRecordsCap } } : {}),
    },
    budget, version, fromMs,
  );
  const stream = () => ({ passes: read.meter.passes, chunks: read.meter.chunks, bytes: read.meter.bytes, events: read.meter.events });
  const owing = async (reasons: string[], partial: boolean, inserted = 0): Promise<SearchIndexResult> => {
    await invalidateDirectoryEvents(tx, scope);
    await io.record(scope, revision, 0, false, at, pointerSnapshot.canonicalId);
    return { manifests: read.manifests, rows: 0, inserted, deleted: 0, complete: false, ...(partial ? { partial: true } : {}), reasons, stream: stream() };
  };
  if (!read.complete || read.directorySessionId !== pointerSnapshot.canonicalId || read.events === null)
    return owing(read.reasons.length ? read.reasons : ["canonical_metadata_incomplete"], read.partial);

  const deadline = deps.streamDeadline;
  const atDeadline = () => {
    if (streamDeadlinePassed(deadline)) throw new StreamBudgetError(STREAM_DEADLINE);
  };
  // A large scan may take a while: start it with a fresh transaction and ping when it ends.
  let scan: Awaited<ReturnType<SearchProjectionIo["scan"]>>;
  try {
    atDeadline();
    await keepalive(true);
    // ClickHouse: keyset pages of one insert batch each, never the single LIMIT maxRows+1
    // read; past the events budget the scope is partial, never a delete. The adapter binding
    // keeps its own bounded scan.
    const between = async () => {
      atDeadline();
      await keepalive();
    };
    // Staged: keyset pages on every binding, up to the keyed-records cap, never SEARCH_INDEX_MAX_ROWS.
    const stagedScan = deps.staged && io.scanPages ? stagedScanBounds(deps.staged, limits) : undefined;
    scan = stagedScan
      ? await io.scanPages!(scope, pointerSnapshot.canonicalId, fromMs, stagedScan.pageSize, stagedScan.maxRows, between)
      : io.scanPages
      ? await io.scanPages(scope, pointerSnapshot.canonicalId, fromMs, limits.insertBatch, budget.maxReadEvents, async () => {
          atDeadline();
          await keepalive();
        })
      : await io.scan(scope, pointerSnapshot.canonicalId, fromMs, limits.maxRows);
    await keepalive(true);
  } catch (error) {
    if (error instanceof StreamBudgetError) return owing(["scope_too_large", error.budget], true);
    throw error;
  }
  if (deps.staged && io.scanPages && scan.truncated) return owing(["scope_too_large", STREAM_KEYED_RECORDS_CAP], true);
  if (io.scanPages && scan.truncated) return owing(["scope_too_large", STREAM_READ_EVENTS_BUDGET], true);
  const truncated = scan.truncated;
  const existing = new Map<string, ScopeIdentity[]>();
  for (const r of scan.rows) {
    const key = streamIndexKey(r);
    existing.set(key, [...(existing.get(key) ?? []), r]);
  }
  const toInsert = new Set<string>();
  for (const [key, w] of read.wanted) {
    const owner = read.owners[w.owner]!;
    const same = existing.get(key) ?? [];
    if (!same.some((c) => c.sessionId === owner.sessionId && c.workspaceId === owner.workspaceId && c.tsMs === w.tsMs && c.fingerprint === w.fingerprint))
      toInsert.add(key);
  }
  // Before pass 4: past the deadline nothing is written or deleted.
  if (streamDeadlinePassed(deadline)) return owing(["scope_too_large", STREAM_DEADLINE], true);
  let inserted = 0;
  try {
    await read.regenerate(toInsert, limits.insertBatch, async (rows) => {
      inserted += await io.upsert(rows, scope, pointerSnapshot.canonicalId);
    });
  } catch (error) {
    if (error instanceof StreamBudgetError) return owing(["scope_too_large", error.budget], true, inserted);
    if (!(error instanceof PipelineError) || error.retryable) throw error;
    return owing([error.code], false, inserted);
  }

  const staleWrite = inserted !== toInsert.size;
  let complete = !truncated && !staleWrite;
  // Before any delete: what pass 4 wrote stays, nothing is deleted, the scope still owes a pass.
  if (complete && streamDeadlinePassed(deadline)) return owing(["scope_too_large", STREAM_DEADLINE], true, inserted);
  const reasons: string[] = [];
  let deletedCount = 0;
  if (!complete) await invalidateDirectoryEvents(tx, scope);
  else {
    // As the whole read: only kinds this pass can regenerate are diffed, and a buffered-origin
    // row with no current canonical row is kept.
    const regenerable = new Set<string>([...S3_ACTIVITY_KINDS, ...read.kinds]);
    const deletes: ScopeIdentity[] = [];
    for (const [key, list] of existing) {
      const current = read.wanted.get(key);
      const owner = current ? read.owners[current.owner]! : undefined;
      for (const c of list) {
        if (!regenerable.has(c.kind)) continue;
        if (!current && c.fingerprint.startsWith(BUFFERED_FINGERPRINT_PREFIX)) continue;
        if (!current || owner!.sessionId !== c.sessionId || owner!.workspaceId !== c.workspaceId || current.tsMs !== c.tsMs)
          deletes.push(c);
      }
    }
    // In insert-sized batches, keeping the transaction busy between them.
    for (let i = 0; i < deletes.length; i += limits.insertBatch) {
      await keepalive();
      deletedCount += await io.remove(scope, deletes.slice(i, i + limits.insertBatch));
    }
    const directoryKey = { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: pointerSnapshot.canonicalId };
    const written = await (deps.writeEvents ?? writeDirectoryEvents)(tx, directoryKey, { sourceSessionId: scope.sessionId, expectedEpoch: pointerSnapshot.epoch, sources: read.sources, events: read.events });
    if (written.written) await (deps.writeCatalog ?? directoryCatalogWriter(deps.desktopCatalog === true))(tx, directoryKey, read.catalog, { expectedEpoch: pointerSnapshot.epoch, sourceSessionId: scope.sessionId });
    else {
      complete = false;
      reasons.push("directory_event_sources_changed");
      await invalidateDirectoryEvents(tx, scope);
    }
  }
  await io.record(scope, revision, read.inWindow, complete, at, pointerSnapshot.canonicalId);
  return {
    manifests: read.manifests,
    rows: read.inWindow,
    inserted,
    deleted: deletedCount,
    complete,
    reasons: [...read.reasons, ...reasons, ...(truncated ? ["search_index_truncated"] : []), ...(staleWrite ? ["search_version_conflict"] : [])],
    stream: stream(),
  };
}

/**
 * How SEARCH_INDEX_STREAMING_ENABLED resolved for the indexer SERVICE (searchIndexStreamStartup).
 * Unset and "" are the same: the code default, which is on unless
 * RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true (the facts are built from a whole read), where it is
 * off without failing startup. "true" is an explicit on and, with output facts, still fails
 * startup; "false" is an explicit off. Any other value fails startup (strict flag grammar:
 * "1", "0", "TRUE" and padded values are refused, never guessed). With the staged reconcile on
 * (searchIndexStagedOn), output facts do not change the answer: the staged hooks carry them.
 */
export type SearchIndexStreamFlag = "explicit_on" | "explicit_off" | "default_on" | "default_off_output_facts";

export function searchIndexStreamFlag(env: Record<string, string | undefined>, outputFacts: boolean): SearchIndexStreamFlag {
  // With the staged reconcile on, the self-host output facts are staged by its hooks (slice 2c), so they
  // no longer hold streaming off; with it off, every answer below is today's.
  if (outputFacts && searchIndexStagedOn(env)) outputFacts = false;
  const flag = env.SEARCH_INDEX_STREAMING_ENABLED;
  if (flag === undefined || flag === "") return outputFacts ? "default_off_output_facts" : "default_on";
  if (flag === "false") return "explicit_off";
  if (flag !== "true") throw new Error("pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED must be true or false");
  if (outputFacts)
    throw new Error("pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true");
  return "explicit_on";
}

/**
 * Streaming reconcile budgets for the indexer SERVICE (consumer-service.ts serve() only),
 * from the environment, given the effective whole-read `limits`:
 *
 *   SEARCH_INDEX_STREAMING_ENABLED      default on (unset or ""), except off when
 *                                       RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true; "true" is an
 *                                       explicit on (fails with output facts), "false" an
 *                                       explicit off (returns undefined); anything else fails
 *   SEARCH_INDEX_STREAM_MAX_READ_BYTES  bytes read, summed over every pass of one scope (a whole
 *                                       number, optional MiB / GiB suffix)
 *   SEARCH_INDEX_STREAM_MAX_READ_EVENTS records read, summed over every pass of one scope
 *
 * Unset, each budget is the large-read profile's bound, STREAM_READ_CEILING (2 GiB /
 * 2,000,000): a scope streams only when four times its size fits both budgets, so the
 * default streams scopes up to 512 MiB and 500,000 records. Set, a budget must lie between
 * its floor (searchIndexStreamFloor: four times the effective whole-read bound, capped at the
 * ceiling; at or below it no scope past that bound could stream) and the ceiling; lower it to
 * shrink the largest streamed scope and the pod memory it needs (one chunk plus about 3 KB
 * per record read, measured). Off reads neither budget. Errors name the variable and its
 * range, never the value.
 */
export function searchIndexStreamFromEnv(
  env: Record<string, string | undefined>,
  limits: Partial<SearchIndexLimits>,
  outputFacts: boolean,
): StreamReadLimits | undefined {
  const flag = searchIndexStreamFlag(env, outputFacts);
  if (flag !== "explicit_on" && flag !== "default_on") return undefined;
  const cap = STREAM_READ_CEILING;
  const floor = searchIndexStreamFloor(limits);
  const read = (name: string, bytes: boolean, min: number, max: number) =>
    readStreamBudget(env, "pipeline consumer", name, bytes, max, min, max);
  return {
    maxReadBytes: read("SEARCH_INDEX_STREAM_MAX_READ_BYTES", true, floor.maxReadBytes, cap.maxReadBytes),
    maxReadEvents: read("SEARCH_INDEX_STREAM_MAX_READ_EVENTS", false, floor.maxReadEvents, cap.maxReadEvents),
  };
}

const MiB = 1024 * 1024;

/** One streaming budget variable: unset or "" is `fallback`; set, a whole number in min..max. */
function readStreamBudget(
  env: Record<string, string | undefined>,
  prefix: string,
  name: string,
  bytes: boolean,
  fallback: number,
  min: number,
  max: number,
): number {
  const fmt = (n: number) => (bytes ? `${n / MiB}MiB` : String(n));
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const m = (bytes ? /^([1-9][0-9]{0,12})(MiB|GiB)?$/u : /^([1-9][0-9]{0,12})()$/u).exec(raw);
  const value = m ? Number(m[1]) * (m[2] === "GiB" ? 1024 * MiB : m[2] === "MiB" ? MiB : 1) : NaN;
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new Error(`${prefix}: ${name} must be ${bytes ? "a whole number of bytes (optional MiB or GiB suffix)" : "a whole number"} in ${fmt(min)}..${fmt(max)}`);
  return value;
}

/** The hosted Lambda's streaming budgets when SEARCH_INDEX_STREAMING_CH=1 sets none. */
export const SEARCH_INDEX_STREAM_CH_DEFAULTS: Readonly<StreamReadLimits> = Object.freeze({
  maxReadBytes: 1024 * MiB,
  maxReadEvents: 1_000_000,
});
/**
 * The hosted Lambda's cap: the self-host STREAM_READ_CEILING bytes (streamed a chunk at a time,
 * never held), but 1,000,000 events. On the ClickHouse binding the keyset scan holds up to
 * maxReadEvents index identities, each with its hashed key and the reader's wanted entry:
 * about 0.8 KB per identity, measured on Node 24 (V8). That is ~0.75 GiB at 1,000,000 against the
 * ~1.65 GiB default V8 heap of the 3008 MiB function (stacks/aws/session-pipeline.ts
 * memorySize), leaving room for chunk buffers and directory events; 2,000,000 (~1.5 GiB) would not.
 */
export const SEARCH_INDEX_STREAM_CH_CEILING: Readonly<StreamReadLimits> = Object.freeze({
  maxReadBytes: STREAM_READ_CEILING.maxReadBytes,
  maxReadEvents: 1_000_000,
});
export const SEARCH_INDEX_STREAM_DEADLINE_MARGIN_DEFAULT_MS = 60_000;
export const SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MIN_MS = 10_000;
export const SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MAX_MS = 240_000;

/** The hosted Lambda's streaming settings: the read budgets and the deadline margin. */
export interface SearchIndexStreamCh extends StreamReadLimits {
  deadlineMarginMs: number;
}

/**
 * Streaming reconcile for the hosted consumer LAMBDA (lambda.ts only; ClickHouse binding),
 * from the environment, given the effective whole-read `limits`:
 *
 *   SEARCH_INDEX_STREAMING_CH              exactly "1" turns it on; unset, "" or "0" leave it
 *                                          off (returns undefined, the knobs below are not
 *                                          read); anything else fails
 *   SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES  default 1 GiB (optional MiB / GiB suffix)
 *   SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS default 1,000,000
 *   SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS default 60000, in 10000..240000
 *
 * Each budget must lie between four times the effective whole-read bound (at most the cap)
 * and the cap SEARCH_INDEX_STREAM_CH_CEILING (2 GiB / 1,000,000, sized to the function's
 * memory); a default under that floor is raised to it (the replay profile's events floor is
 * the cap itself). The self-host SEARCH_INDEX_STREAMING_ENABLED and
 * SEARCH_INDEX_STREAM_MAX_READ_* are not read here. The caller checks the dependencies
 * (SEARCH_INDEX_ENABLED=1, the clickhouse backend, no SEARCH_INDEX_OUTPUT_FACTS=1).
 * Errors name the variable and its range, never the value.
 */
export function searchIndexStreamChFromEnv(
  env: Record<string, string | undefined>,
  limits: Partial<SearchIndexLimits>,
): SearchIndexStreamCh | undefined {
  const flag = env.SEARCH_INDEX_STREAMING_CH;
  if (flag === undefined || flag === "" || flag === "0") return undefined;
  if (flag !== "1") throw new Error("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
  const prefix = "session pipeline";
  const cap = SEARCH_INDEX_STREAM_CH_CEILING;
  const defaults = searchIndexStreamFloor(limits);
  const floor = { maxReadBytes: Math.min(defaults.maxReadBytes, cap.maxReadBytes), maxReadEvents: Math.min(defaults.maxReadEvents, cap.maxReadEvents) };
  const budget = (name: string, bytes: boolean, fallback: number, min: number, max: number) =>
    readStreamBudget(env, prefix, name, bytes, Math.min(Math.max(fallback, min), max), min, max);
  const marginRaw = env.SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS;
  let deadlineMarginMs = SEARCH_INDEX_STREAM_DEADLINE_MARGIN_DEFAULT_MS;
  if (marginRaw !== undefined && marginRaw !== "") {
    deadlineMarginMs = /^[1-9][0-9]{0,8}$/u.test(marginRaw) ? Number(marginRaw) : NaN;
    if (
      !Number.isSafeInteger(deadlineMarginMs) ||
      deadlineMarginMs < SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MIN_MS ||
      deadlineMarginMs > SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MAX_MS
    )
      throw new Error(
        `${prefix}: SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS must be a whole number of milliseconds in ${SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MIN_MS}..${SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MAX_MS}`,
      );
  }
  return {
    maxReadBytes: budget("SEARCH_INDEX_STREAM_CH_MAX_READ_BYTES", true, SEARCH_INDEX_STREAM_CH_DEFAULTS.maxReadBytes, floor.maxReadBytes, cap.maxReadBytes),
    maxReadEvents: budget("SEARCH_INDEX_STREAM_CH_MAX_READ_EVENTS", false, SEARCH_INDEX_STREAM_CH_DEFAULTS.maxReadEvents, floor.maxReadEvents, cap.maxReadEvents),
    deadlineMarginMs,
  };
}

/**
 * The smallest accepted streaming budgets for the effective whole-read `limits`: four whole
 * reads, capped at STREAM_READ_CEILING. A scope streams only when it is strictly past the
 * whole-read bound (canonical-projection.ts `bytes > maxScopeBytes || events > maxScopeEvents`),
 * so the smallest one needs four times (bound + 1): a budget from the floor up to
 * 4 * bound + 3 accepts no scope, and with both budgets there startup warns
 * (searchIndexStreamBudgetWarning). The floor stays four whole reads so values accepted
 * before stay valid. The defaults are the ceiling itself.
 */
export function searchIndexStreamFloor(limits: Partial<SearchIndexLimits>): StreamReadLimits {
  const effective = { ...SEARCH_INDEX_DEFAULT_LIMITS, ...limits };
  return {
    maxReadBytes: Math.min(STREAM_PASSES * effective.maxScopeBytes, STREAM_READ_CEILING.maxReadBytes),
    maxReadEvents: Math.min(STREAM_PASSES * effective.maxScopeEvents, STREAM_READ_CEILING.maxReadEvents),
  };
}

/**
 * The startup warning for streaming on with budgets no scope can use, or undefined. Every
 * scope the streaming reader takes is past the whole-read bound in bytes or in records, and
 * it needs four times its size within each budget. Past the bound is strictly above it
 * (canonical-projection.ts `bytes > limits.maxScopeBytes || events > limits.maxScopeEvents`;
 * canonical-stream-projection.ts refuses `size * STREAM_PASSES > budget`), so the smallest
 * scope that can stream on one axis needs STREAM_PASSES * (bound + 1) of that budget: when
 * neither budget reaches that, no scope fits and none streams (a scope whose chunks do not
 * declare their record counts is refused only after a full first pass under the scope's
 * locks). Never at the default budgets (the ceiling is above four times the largest accepted
 * bound in bytes); only when an operator lowers both below four times (bound + 1), the floor
 * included. Raise SEARCH_INDEX_STREAM_MAX_READ_BYTES and/or _EVENTS toward the ceiling, or
 * set SEARCH_INDEX_STREAMING_ENABLED=false.
 */
export function searchIndexStreamBudgetWarning(
  stream: StreamReadLimits | undefined,
  limits: Partial<SearchIndexLimits>,
): Record<string, number> | undefined {
  if (!stream) return undefined;
  const effective = { ...SEARCH_INDEX_DEFAULT_LIMITS, ...limits };
  if (
    stream.maxReadBytes >= STREAM_PASSES * (effective.maxScopeBytes + 1) ||
    stream.maxReadEvents >= STREAM_PASSES * (effective.maxScopeEvents + 1)
  )
    return undefined;
  return {
    max_read_bytes: stream.maxReadBytes,
    max_read_events: stream.maxReadEvents,
    largest_streamed_scope_bytes: Math.floor(stream.maxReadBytes / STREAM_PASSES),
    largest_streamed_scope_events: Math.floor(stream.maxReadEvents / STREAM_PASSES),
  };
}

/** One startup log line about the streaming reconcile, for serve() to emit. */
export interface SearchIndexStreamNotice {
  level: "info" | "warn";
  message: string;
  fields: Record<string, string | number>;
}

/** The streaming-off line output facts give while the staged reconcile is off (searchIndexStreamStartup). */
const STREAMING_OFF_FOR_FACTS: SearchIndexStreamNotice = {
  level: "info",
  message: "pipeline consumer streaming reconcile off: RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true builds facts from a whole read",
  fields: { streaming: "off", reason: "output_facts" },
};

/**
 * The streaming reconcile as the indexer SERVICE starts it: the budgets (undefined when off)
 * and at most one log line. Off by default because RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true
 * logs that reason (info) instead of failing; budgets no scope can use log a warning.
 * Throws exactly as searchIndexStreamFlag / searchIndexStreamFromEnv do.
 */
export function searchIndexStreamStartup(
  env: Record<string, string | undefined>,
  limits: Partial<SearchIndexLimits>,
  outputFacts: boolean,
): { stream: StreamReadLimits | undefined; notice: SearchIndexStreamNotice | undefined } {
  const flag = searchIndexStreamFlag(env, outputFacts);
  const stream = searchIndexStreamFromEnv(env, limits, outputFacts);
  if (flag === "default_off_output_facts") return { stream, notice: STREAMING_OFF_FOR_FACTS };
  const budget = searchIndexStreamBudgetWarning(stream, limits);
  return {
    stream,
    notice: budget
      ? { level: "warn", message: "pipeline consumer streaming reconcile on with read budgets no scope past the whole-read bound fits", fields: budget }
      : undefined,
  };
}

/**
 * The staged reconcile walks the index in keyset pages: serve() refuses an index without
 * scanFingerprintsPage at startup (not on the first scope).
 */
export function assertStagedSearchIndex(staged: StagedReconcile | undefined, index: { scanFingerprintsPage?: unknown }): void {
  if (staged && typeof index.scanFingerprintsPage !== "function")
    throw new Error("pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires a search index with paged scans");
}

/**
 * The staged index scan's bounds: pages of one insert batch (at most MAX_SCAN_PAGE), up to the
 * keyed-records cap but never below SEARCH_INDEX_MAX_ROWS, so turning the flag on never
 * truncates a scan that the bounded read would have taken whole.
 */
function stagedScanBounds(staged: StagedReconcile, limits: SearchIndexLimits): { pageSize: number; maxRows: number } {
  return { pageSize: Math.min(limits.insertBatch, MAX_SCAN_PAGE), maxRows: Math.max(staged.keyedRecordsCap, limits.maxRows) };
}

/**
 * How SEARCH_INDEX_STAGED_RECONCILE_ENABLED resolved for the indexer SERVICE (consumer-service.ts
 * serve() only; the hosted Lambda and the large-read worker never read it). Phase 3 slice 4: default on.
 *
 *   "true"            explicit_on: on; fails startup unless the streaming reconcile resolved on
 *   "false"           explicit_off: off, exactly the pre-staged path (no log line, no query)
 *   unset or ""       default_on: on, as explicit_on, unless SEARCH_INDEX_STREAMING_ENABLED is
 *                     exactly "false": default_off_streaming (off, one info line, no failure)
 *   anything else     fails startup ("1", "TRUE" and padded values included)
 *
 * The one place that decides it. It reads only the staged value and whether streaming is exactly
 * "false", so it never depends on how streaming itself resolved: searchIndexStreamFlag follows it
 * (with the staged reconcile on, output facts no longer hold streaming off), and
 * searchIndexStagedStartup returns it. Errors name the variable, never a value.
 */
export type SearchIndexStagedState = "explicit_on" | "explicit_off" | "default_on" | "default_off_streaming";

export function searchIndexStagedState(env: Record<string, string | undefined>): SearchIndexStagedState {
  const flag = env.SEARCH_INDEX_STAGED_RECONCILE_ENABLED;
  if (flag === "true") return "explicit_on";
  if (flag === "false") return "explicit_off";
  if (flag === undefined || flag === "") return env.SEARCH_INDEX_STREAMING_ENABLED === "false" ? "default_off_streaming" : "default_on";
  throw new Error("pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED must be true or false");
}

/** Whether the staged reconcile is on (searchIndexStagedState); an invalid value is not on (startup refuses it). */
export function searchIndexStagedOn(env: Record<string, string | undefined>): boolean {
  try {
    const state = searchIndexStagedState(env);
    return state === "explicit_on" || state === "default_on";
  } catch {
    return false;
  }
}

/**
 * serve()'s staged deps for the resolved state: `staged` when on, and at most one startup log line
 * (`notice`) for every state but explicit_off. On (explicit or default) needs the streaming reconcile
 * resolved on: explicitly "true" without it fails startup; by default it cannot happen (streaming
 * "false" is default_off_streaming, and output facts no longer hold streaming off). With
 * RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true the facts are staged (slice 2c, migration 20261105062140).
 */
export function searchIndexStagedStartup(
  env: Record<string, string | undefined>,
  stream: StreamReadLimits | undefined,
  _outputFacts: boolean,
): { state: SearchIndexStagedState; staged?: StagedReconcile; notice?: SearchIndexStreamNotice; streamingExplicitOn?: true } {
  const state = searchIndexStagedState(env);
  if (state === "explicit_off") return { state };
  if (state === "default_off_streaming")
    return {
      state,
      notice: {
        level: "info",
        message: "pipeline consumer staged reconcile off: SEARCH_INDEX_STREAMING_ENABLED=false (it runs only with streaming)",
        fields: { state: "off", reason: "streaming_off" },
      },
    };
  if (!stream)
    throw new Error("pipeline consumer: SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true requires the streaming reconcile on (SEARCH_INDEX_STREAMING_ENABLED)");
  return {
    state,
    // Kept for searchIndexStagedForIndex: an explicit streaming "true" with output facts cannot fall back.
    ...(env.SEARCH_INDEX_STREAMING_ENABLED === "true" ? { streamingExplicitOn: true as const } : {}),
    staged: { keyedRecordsCap: STAGED_KEYED_RECORDS_CAP, refusals: new StagedRefusals() },
    notice: {
      level: "info",
      // Allowlisted log fields only (src/shared/log.ts): the cap is a constant, so it rides in the message.
      message: state === "explicit_on"
        ? `pipeline consumer staged reconcile on (keyed records cap ${STAGED_KEYED_RECORDS_CAP})`
        : `pipeline consumer staged reconcile on by default (keyed records cap ${STAGED_KEYED_RECORDS_CAP}; SEARCH_INDEX_STAGED_RECONCILE_ENABLED=false turns it off)`,
      fields: { state: "on" },
    },
  };
}

/**
 * The staged deps for the composed search index. An index without keyset pages (scanFingerprintsPage):
 * explicit_on refuses to start (assertStagedSearchIndex); by default the staged reconcile is off with a
 * warn line, never a failure (both shipped backends have them, so the service never gets here). With
 * output facts on, that fallback also turns streaming off again (`streamOff`, with today's streaming-off
 * line): streaming resolved on only because the staged run would carry the facts. The consumer then
 * reconciles as with the staged reconcile off, plus the startup lines and the schema asserts already
 * run. An explicit SEARCH_INDEX_STREAMING_ENABLED=true with output facts is never turned off silently:
 * it fails startup with the error it gives while the staged reconcile is off.
 */
export function searchIndexStagedForIndex(
  started: ReturnType<typeof searchIndexStagedStartup>,
  index: { scanFingerprintsPage?: unknown },
  outputFacts = false,
): { staged?: StagedReconcile; notice?: SearchIndexStreamNotice; streamOff?: SearchIndexStreamNotice } {
  if (!started.staged || typeof index.scanFingerprintsPage === "function") return started.staged ? { staged: started.staged } : {};
  if (started.state === "explicit_on") assertStagedSearchIndex(started.staged, index);
  if (outputFacts && started.streamingExplicitOn)
    throw new Error("pipeline consumer: SEARCH_INDEX_STREAMING_ENABLED=true cannot be combined with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true");
  return {
    notice: {
      level: "warn",
      message: "pipeline consumer staged reconcile off: the search index has no paged scans (SEARCH_INDEX_STAGED_RECONCILE_ENABLED=true would refuse to start)",
      fields: { state: "off", reason: "no_paged_scan" },
    },
    ...(outputFacts ? { streamOff: STREAMING_OFF_FOR_FACTS } : {}),
  };
}

function factsScope(scope: SessionScope, canonicalId: string, projectionRows: Array<{ table: string; row: Record<string, unknown> }> = []): OutputFactsScope {
  const seen = projectionRows.filter((r) => r.table === "convergence_events" && typeof r.row.session_id === "string").map((r) => r.row.session_id as string);
  return {
    orgId: scope.orgId,
    workspaceId: scope.workspaceId,
    harness: scope.harness,
    sessionIds: [...new Set([scope.sessionId, canonicalId, ...seen])],
    canonicalId,
  };
}

/** The ClickHouse io, plus the streamed reconcile's keyset-paged scan. */
interface LegacyProjectionIo extends SearchProjectionIo {
  /**
   * The scope's in-window index identities in keyset pages of `pageSize` (ORDER BY the sort
   * key's scope-varying suffix), `between` awaited before each page after the first. Stops
   * at `maxRows + 1` rows: `truncated` then, and the caller must not delete.
   */
  scanPages(
    scope: SessionScope,
    canonicalId: string,
    fromMs: number,
    pageSize: number,
    maxRows: number,
    between: () => Promise<void>,
  ): Promise<{ rows: ScopeIdentity[]; truncated: boolean }>;
}

/**
 * One keyset page of a scope's index identities. Keyed on the vendored sort key
 * (org_id, workspace_id, ts, session_id, source, machine_id, kind, event_id): with org,
 * workspace and source fixed by the scope, (ts, session_id, machine_id, kind, event_id) is
 * the ReplacingMergeTree identity, unique under FINAL, so `WHERE tuple > last ORDER BY tuple`
 * visits every row once. A later page also carries `ts >= last.ts`, implied by the tuple
 * comparison but a plain range on the sort key's third column, so ClickHouse prunes the
 * granules before it instead of rescanning the scope's prefix on every page. There is no
 * row_id column; no DDL is needed for this order.
 */
export async function clickhouseScopePage(
  ch: ClickHouseClient,
  scope: SessionScope,
  canonicalSessionId: string,
  fromMs: number,
  limit: number,
  after?: Pick<ScopeIdentity, "tsMs" | "sessionId" | "machineId" | "kind" | "eventId">,
): Promise<ScopeIdentity[]> {
  const rows = await ch.select<{
    workspace_id: string;
    session_id: string;
    machine_id: string;
    source: string;
    kind: string;
    event_id: string;
    ts_ms: number;
    fingerprint: string;
  }>(
    `SELECT workspace_id, session_id, machine_id, source, kind, event_id,
        toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint
     FROM ${EVENTS_TABLE} FINAL
     WHERE org_id = {org:String} AND (session_id = {session:String} OR session_id = {canonical:String})
       AND workspace_id = {workspace:String} AND source = {source:String}
       AND toUnixTimestamp64Milli(ts) >= {from:Int64}${after ? `
       AND ts >= fromUnixTimestamp64Milli({after_ts:Int64}, 'UTC')
       AND (ts, session_id, machine_id, kind, event_id) >
           (fromUnixTimestamp64Milli({after_ts:Int64}, 'UTC'), {after_session:String}, {after_machine:String}, {after_kind:String}, {after_event:String})` : ""}
     ORDER BY ts, session_id, machine_id, kind, event_id
     LIMIT {limit:UInt32}`,
    {
      params: {
        org: scope.orgId, session: scope.sessionId, canonical: canonicalSessionId, workspace: scope.workspaceId, source: scope.harness, from: fromMs, limit,
        ...(after ? { after_ts: after.tsMs, after_session: after.sessionId, after_machine: after.machineId, after_kind: after.kind, after_event: after.eventId } : {}),
      },
      timeoutMs: 20_000,
    },
  );
  return rows.map((r) => ({
    workspaceId: r.workspace_id,
    sessionId: r.session_id,
    machineId: r.machine_id,
    source: r.source,
    kind: r.kind,
    eventId: r.event_id,
    tsMs: Number(r.ts_ms),
    fingerprint: r.fingerprint,
  }));
}

/** Preserve the existing SQL, retention and state behavior for an unselected
 * legacy deployment. Portable adapters never implement a fake SQL client. */
export function legacyProjectionIo(ch: ClickHouseClient): LegacyProjectionIo {
  return {
    async scanPages(scope, canonicalId, fromMs, pageSize, maxRows, between) {
      if (!Number.isSafeInteger(pageSize) || pageSize < 1 || !Number.isSafeInteger(maxRows) || maxRows < 0)
        throw new Error("search keyset scan bounds invalid");
      const rows: ScopeIdentity[] = [];
      let after: ScopeIdentity | undefined;
      for (;;) {
        if (after) await between();
        const want = Math.min(pageSize, maxRows + 1 - rows.length);
        const page = await clickhouseScopePage(ch, scope, canonicalId, fromMs, want, after);
        for (const r of page) rows.push(r);
        if (rows.length > maxRows) return { rows, truncated: true };
        if (page.length < want) return { rows, truncated: false };
        after = page[page.length - 1];
      }
    },
    async fromMs(at) { return Date.parse(await windowStart(ch, at)); },
    async scan(scope, canonicalId, fromMs, limit) {
      const rows = await clickhouseScope(ch, scope, fromMs, limit + 1, canonicalId);
      return { rows, truncated: rows.length > limit };
    },
    async upsert(rows) { await ch.insert(EVENTS_TABLE, [...rows], { timeoutMs: 60_000 }); return rows.length; },
    async remove(scope, positions) {
      const deletes: PendingDelete[] = positions.map(identity => ({ orgId: scope.orgId, sessionId: identity.sessionId, identity }));
      await deleteRows(ch, deletes);
      return deletes.length;
    },
    async erase(scope, canonical) {
      await ch.command(`DELETE FROM ${EVENTS_TABLE}
         WHERE org_id = {org:String} AND workspace_id = {workspace:String}
           AND source = {source:String} AND (session_id = {session:String} OR session_id = {canonical:String})`, {
        params: { org: scope.orgId, workspace: scope.workspaceId, source: scope.harness, session: scope.sessionId, canonical },
        settings: { lightweight_deletes_sync: 2 }, timeoutMs: 120_000,
      });
    },
    record: (scope, revision, rows, complete, at, canonicalId) => recordState(ch, scope, revision, rows, complete, at, canonicalId),
  };
}
