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
export { manifestUserId, readManifestRecords, projectedToNeonRow } from "../session-read-api/canonical-projection.ts";
import * as zlib from "node:zlib";
import { layoutComponent } from "../shared/layout-component.ts";
import { PipelineError, type ManifestReader, type ObjectStore } from "../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { tsMillis } from "./vendor/search-index/rows.ts";
import { EVENTS_TABLE, SESSION_STATE_TABLE } from "./vendor/search-index/schema.ts";
import { chNow, deleteRows, windowStart, type PendingDelete } from "./vendor/search-index/reconcile.ts";
import { scopeLockKey, type SessionScope } from "./pointer.ts";
import { directoryCatalogWriter, type writeDirectoryCatalog } from "../hoopdb/session-directory-catalog.ts";
import { writeDirectoryEvents, invalidateDirectoryEvents, completeErasedDirectoryEvents } from "../hoopdb/session-directory-events.ts";
import { outputFactsFromScope, passVersion, type OutputFactsScope } from "./output-facts.ts";
import { clickhouseOutputFactsStore, type OutputFactsStore } from "./output-facts-store.ts";
import { adapterProjectionIo, type SearchProjectionIo, type SearchServiceBinding } from "./search-adapter.ts";
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
}

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
  /** Too large to index whole: no rows written, its index state marked partial. */
  partial?: boolean;
  /** Present only with `outputFacts`: fact rows inserted and deleted, and whether they are exact. */
  outputFacts?: { inserted: number; deleted: number; complete: boolean; reasons: string[] };
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
  const limits = { ...SEARCH_INDEX_DEFAULT_LIMITS, ...deps.limits };
  const now = deps.now ?? (() => new Date());
  return deps.transact(async (tx) => {
    const io = deps.adapter ? adapterProjectionIo(deps.adapter, tx) : legacyProjectionIo(deps.ch!);
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
    const fromMs = await io.fromMs(at);
    const s3 = await readCanonicalScope(scope, siblingKeys, { ...deps, decompress: deps.decompress ?? defaultDecompress }, limits, version, pointerSnapshot.canonicalId);
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
    const scan = await io.scan(scope, pointerSnapshot.canonicalId, fromMs, limits.maxRows);
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
      reasons: [...s3.reasons, ...(truncated ? [deps.adapter ? "search_index_truncated" : "clickhouse_truncated"] : []), ...(staleWrite ? ["search_version_conflict"] : [])],
      ...(outputFacts ? { outputFacts } : {}),
    };
  });
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

/** Preserve the existing SQL, retention and state behavior for an unselected
 * legacy deployment. Portable adapters never implement a fake SQL client. */
function legacyProjectionIo(ch: ClickHouseClient): SearchProjectionIo {
  return {
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
