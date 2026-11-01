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
import { createHash } from "node:crypto";
import * as zlib from "node:zlib";
import {
  CONVERGENCE_EVENT_KIND,
  PipelineError,
  chunkKeys,
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  projectRelayhistorySession,
  readCapped,
  relayhistoryExportProvenance,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type RelayhistoryStreamRecord,
} from "../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { toIndexRow, tsMillis, type IndexRow, type NeonEventRow } from "./vendor/search-index/rows.ts";
import { EVENTS_TABLE } from "./vendor/search-index/schema.ts";
import { deleteRows, windowStart, type PendingDelete } from "./vendor/search-index/reconcile.ts";
import type { SessionScope } from "./pointer.ts";

export type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;

export interface SearchIndexDeps {
  /** One Supabase transaction on one connection (for the advisory lock). */
  transact: Transact;
  reader: ManifestReader;
  store: ObjectStore;
  ch: ClickHouseClient;
  /** The deployment prefix of an org's keys, e.g. `96bulls/<org>`. */
  prefixFor(orgId: string): string;
  decompress?: (stored: Uint8Array, maxOutputLength: number) => Uint8Array;
  now?: () => Date;
  limits?: Partial<SearchIndexLimits>;
}

export interface SearchIndexLimits {
  /** Largest scope read into memory, summed over its manifests' raw bytes. */
  maxScopeBytes: number;
  /** Largest number of ClickHouse rows compared for one scope. */
  maxRows: number;
  /** Rows per insert. */
  insertBatch: number;
}

const DEFAULT_LIMITS: SearchIndexLimits = {
  maxScopeBytes: 256 * 1024 * 1024,
  maxRows: 500_000,
  insertBatch: 5_000,
};
const MAX_CHUNK_BYTES = 16 * 1024 * 1024;

export interface SearchIndexResult {
  manifests: number;
  rows: number;
  inserted: number;
  deleted: number;
  /** False when something could not be read: nothing was deleted. */
  complete: boolean;
  /** Why the read was incomplete (codes only). */
  reasons: string[];
}

interface ScopeIdentity {
  machineId: string;
  source: string;
  kind: string;
  eventId: string;
  workspaceId: string;
  tsMs: number;
  fingerprint: string;
}

const SEP = "\u001f";
const keyOf = (i: Pick<ScopeIdentity, "machineId" | "source" | "kind" | "eventId">) =>
  [i.machineId, i.source, i.kind, i.eventId].join(SEP);

function lockKey(scope: SessionScope): string {
  return ["96bulls:search-index", scope.orgId, scope.workspaceId, scope.harness, scope.sessionId].join(SEP);
}

function defaultDecompress(stored: Uint8Array, maxOutputLength: number): Uint8Array {
  if (typeof zlib.zstdDecompressSync !== "function")
    throw new PipelineError("zstd_unavailable", "this runtime cannot zstd-decompress raw chunks", false);
  return new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength }));
}

const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/** The user a manifest's events belong to: the export grant's source user, else the 96bulls extension. */
export function manifestUserId(manifest: ManifestV1): string {
  try {
    return relayhistoryExportProvenance(manifest).sourceUserId;
  } catch {
    const ext = manifest.extensions?.["96bulls"] as { user_id?: unknown } | undefined;
    return typeof ext?.user_id === "string" ? ext.user_id : "";
  }
}

/** Every canonical record of one manifest, digests verified. Throws a PipelineError with a code. */
export async function readManifestRecords(
  manifest: ManifestV1,
  store: ObjectStore,
  prefix: string,
  decompress: (stored: Uint8Array, maxOutputLength: number) => Uint8Array,
): Promise<RelayhistoryStreamRecord[]> {
  if (manifest.raw.format !== "ndjson")
    throw new PipelineError("raw_format_unsupported", "only ndjson raw is indexed", false);
  const records: RelayhistoryStreamRecord[] = [];
  for (const { entry, key } of chunkKeys(manifest, { prefix })) {
    if (entry.stored_bytes > MAX_CHUNK_BYTES || (entry.stored_raw_bytes ?? entry.bytes) > MAX_CHUNK_BYTES)
      throw new PipelineError("chunk_too_large", "a raw chunk exceeds the index limit", false);
    let stored: Uint8Array | null;
    try {
      stored = await readCapped(await store.get(key), entry.stored_bytes);
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === "not_found")
        throw new PipelineError("chunk_missing", "a listed raw chunk is missing", true);
      throw new PipelineError("chunk_unavailable", "a raw chunk read failed", true);
    }
    if (!stored || stored.byteLength !== entry.stored_bytes || sha256(stored) !== entry.stored_sha256)
      throw new PipelineError("chunk_digest_mismatch", "a raw chunk has the wrong stored digest", false);
    let raw: Uint8Array;
    try {
      raw = decompress(stored, entry.stored_raw_bytes ?? MAX_CHUNK_BYTES);
    } catch (error) {
      if (error instanceof PipelineError) throw error;
      throw new PipelineError("chunk_decompress_failed", "a raw chunk is not valid zstd", false);
    }
    if (entry.raw_sha256 && sha256(raw) !== entry.raw_sha256)
      throw new PipelineError("chunk_digest_mismatch", "a raw chunk has the wrong raw digest", false);
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw);
    } catch {
      throw new PipelineError("chunk_not_utf8", "a raw chunk is not UTF-8", false);
    }
    for (const line of text.split("\n")) if (line) records.push(parseRelayhistoryRecord(line));
  }
  return records;
}

type Json = Record<string, unknown>;

const str = (v: unknown): string | null => (typeof v === "string" ? v : v === null || v === undefined ? null : String(v));

/** A projected `convergence_events` row as the Neon reader's row shape, so `toIndexRow` maps it. */
export function projectedToNeonRow(row: Json, scope: SessionScope, userId: string): NeonEventRow {
  const taskRef = row.task_ref && typeof row.task_ref === "object" ? (row.task_ref as Json) : {};
  const base: Omit<NeonEventRow, "fingerprint"> = {
    org_id: scope.orgId,
    workspace_id: scope.workspaceId,
    machine_id: String(row.machine_id),
    user_id: str(row.user_id) ?? userId,
    source: String(row.source),
    session_id: String(row.session_id),
    event_id: String(row.event_id),
    kind: String(row.kind),
    type: str(row.type) ?? String(row.kind),
    ts_text: String(row.ts),
    actor_name: str(row.actor_name),
    actor_role: str(row.actor_role),
    subagent_id: str(row.subagent_id),
    project_id: str(row.project_id),
    task_title: str(row.task_title),
    task_ref: JSON.stringify(taskRef),
    task_ref_id: str(taskRef.id),
    tags: JSON.stringify(Array.isArray(row.tags) ? row.tags : []),
    files_touched: JSON.stringify(Array.isArray(row.files_touched) ? row.files_touched : []),
    tool_name: str(row.tool_name),
    tool_status: str(row.tool_status),
    delivery_record_id: str(row.delivery_record_id),
    content: str(row.content),
    ingested_text: String(row.ingested_at ?? row.ts),
  };
  // Covers every stored column but ts and workspace_id (compared separately,
  // as in the Neon fingerprint). The `s3` prefix keeps it apart from Neon's.
  const { ts_text: _ts, workspace_id: _ws, ...covered } = base;
  const fingerprint = `s3${createHash("sha256").update(JSON.stringify(covered)).digest("hex").slice(0, 14)}`;
  return { ...base, fingerprint };
}

interface ScopeRead {
  rows: IndexRow[];
  manifests: number;
  complete: boolean;
  reasons: string[];
}

async function readScope(
  scope: SessionScope,
  manifestKeys: string[],
  deps: SearchIndexDeps,
  limits: SearchIndexLimits,
  version: number,
): Promise<ScopeRead> {
  const decompress = deps.decompress ?? defaultDecompress;
  const prefix = deps.prefixFor(scope.orgId);
  const reasons: string[] = [];
  let complete = true;
  let bytes = 0;
  type Part = { manifest: ManifestV1; records: RelayhistoryStreamRecord[]; userId: string };
  const parts: Part[] = [];
  for (const key of manifestKeys) {
    const read = await deps.reader.read(key);
    if (!read) {
      // A live pointer whose manifest is gone: its delete event is in flight.
      complete = false;
      reasons.push("manifest_missing");
      continue;
    }
    const manifest = read.manifest;
    // Unscrubbed raw is quarantined and never read (manifest spec, "Scrubbing").
    if (!manifest.raw.scrub) {
      reasons.push("manifest_unscrubbed");
      continue;
    }
    bytes += manifest.raw.chunks.reduce((n, c) => n + Math.max(c.bytes, c.stored_raw_bytes ?? 0), 0);
    if (bytes > limits.maxScopeBytes) {
      complete = false;
      reasons.push("scope_too_large");
      break;
    }
    try {
      parts.push({ manifest, records: await readManifestRecords(manifest, deps.store, prefix, decompress), userId: manifestUserId(manifest) });
    } catch (error) {
      if (error instanceof PipelineError && error.retryable) throw error;
      complete = false;
      reasons.push(error instanceof PipelineError ? error.code : "manifest_unreadable");
    }
  }

  // Fold every copy together; a conflicting copy (same revision, different
  // bytes) falls back to folding each manifest on its own, without deletes.
  let groups: { records: RelayhistoryStreamRecord[]; part: Part }[];
  try {
    const all = normalizeRelayhistoryRecords(parts.flatMap((p) => p.records));
    const owner = new Map<string, Part>();
    for (const part of parts) for (const r of part.records) if (!owner.has(`${r.origin_id}${SEP}${r.record_id}`)) owner.set(`${r.origin_id}${SEP}${r.record_id}`, part);
    // Direct convergence_event lines are their machine's own rows: project them
    // with the manifest that carried them. Delivery records fold across copies.
    const direct = new Map<Part, RelayhistoryStreamRecord[]>();
    const activity: RelayhistoryStreamRecord[] = [];
    for (const r of all) {
      if (r.kind === CONVERGENCE_EVENT_KIND) {
        const part = owner.get(`${r.origin_id}${SEP}${r.record_id}`)!;
        direct.set(part, [...(direct.get(part) ?? []), r]);
      } else activity.push(r);
    }
    groups = [...direct].map(([part, records]) => ({ records, part }));
    if (activity.length > 0 && parts[0]) groups.push({ records: activity, part: parts[0] });
  } catch (error) {
    complete = false;
    reasons.push(error instanceof PipelineError ? error.code : "fold_failed");
    groups = [];
    for (const part of parts) {
      try {
        groups.push({ records: normalizeRelayhistoryRecords(part.records), part });
      } catch {
        reasons.push("manifest_fold_failed");
      }
    }
  }

  const byKey = new Map<string, IndexRow>();
  for (const { records, part } of groups) {
    let projected;
    try {
      projected = projectRelayhistorySession(records, {
        workspaceId: scope.workspaceId,
        source: scope.harness,
        sessionId: scope.sessionId,
        machineId: part.manifest.session.machine_id,
        userId: part.userId,
      });
    } catch (error) {
      complete = false;
      reasons.push(error instanceof PipelineError ? error.code : "projection_failed");
      continue;
    }
    for (const { table, row } of projected.rows) {
      if (table !== "convergence_events") continue;
      const indexRow = toIndexRow(projectedToNeonRow(row, scope, part.userId), version);
      byKey.set(keyOf({ machineId: indexRow.machine_id, source: indexRow.source, kind: indexRow.kind, eventId: indexRow.event_id }), indexRow);
    }
  }
  return { rows: [...byKey.values()], manifests: parts.length, complete, reasons: [...new Set(reasons)] };
}

async function clickhouseScope(
  ch: ClickHouseClient,
  scope: SessionScope,
  fromMs: number,
  limit: number,
): Promise<ScopeIdentity[]> {
  const rows = await ch.select<{
    workspace_id: string;
    machine_id: string;
    source: string;
    kind: string;
    event_id: string;
    ts_ms: number;
    fingerprint: string;
  }>(
    `SELECT workspace_id, machine_id, source, kind, event_id,
        toUnixTimestamp64Milli(ts) AS ts_ms, fingerprint
     FROM ${EVENTS_TABLE} FINAL
     WHERE org_id = {org:String} AND session_id = {session:String}
       AND workspace_id = {workspace:String} AND source = {source:String}
       AND toUnixTimestamp64Milli(ts) >= {from:Int64}
     LIMIT {limit:UInt32}`,
    {
      params: { org: scope.orgId, session: scope.sessionId, workspace: scope.workspaceId, source: scope.harness, from: fromMs, limit },
      timeoutMs: 20_000,
    },
  );
  return rows.map((r) => ({
    workspaceId: r.workspace_id,
    machineId: r.machine_id,
    source: r.source,
    kind: r.kind,
    eventId: r.event_id,
    tsMs: Number(r.ts_ms),
    fingerprint: r.fingerprint,
  }));
}

/** Live manifests of a scope, from the pointer index. */
export async function livePointerKeys(tx: SqlExecutor, scope: SessionScope): Promise<string[]> {
  const { rows } = await tx.query<{ manifest_key: string }>(
    `SELECT manifest_key FROM sessions.session_index
      WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND deleted_at IS NULL
      ORDER BY machine_id`,
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId],
  );
  return rows.map((r) => r.manifest_key);
}

export async function indexSearchScope(scope: SessionScope, deps: SearchIndexDeps): Promise<SearchIndexResult> {
  const limits = { ...DEFAULT_LIMITS, ...deps.limits };
  const now = deps.now ?? (() => new Date());
  return deps.transact(async (tx) => {
    await tx.query("SET LOCAL lock_timeout = '30s'");
    await tx.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [lockKey(scope)]);
    const keys = await livePointerKeys(tx, scope);
    const at = now();
    const version = at.getTime();
    const erased = keys.length === 0;
    // An erased session loses every row; otherwise the index window applies.
    const fromMs = erased ? 0 : Date.parse(await windowStart(deps.ch, at));
    const s3 = erased
      ? { rows: [], manifests: 0, complete: true, reasons: [] as string[] }
      : await readScope(scope, keys, deps, limits, version);
    const inWindow = s3.rows.filter((r) => tsMillis(`${r.ts.replace(" ", "T")}Z`) >= fromMs);
    const chRows = await clickhouseScope(deps.ch, scope, fromMs, limits.maxRows + 1);
    const truncated = chRows.length > limits.maxRows;
    const existing = new Map<string, ScopeIdentity[]>();
    for (const r of chRows) existing.set(keyOf(r), [...(existing.get(keyOf(r)) ?? []), r]);

    const toInsert = inWindow.filter((r) => {
      const tsMs = tsMillis(`${r.ts.replace(" ", "T")}Z`);
      const same = existing.get(keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id })) ?? [];
      return !same.some((c) => c.workspaceId === r.workspace_id && c.tsMs === tsMs && c.fingerprint === r.fingerprint);
    });
    for (let i = 0; i < toInsert.length; i += limits.insertBatch)
      await deps.ch.insert(EVENTS_TABLE, toInsert.slice(i, i + limits.insertBatch), { timeoutMs: 60_000 });

    const deletes: PendingDelete[] = [];
    const complete = s3.complete && !truncated;
    if (complete) {
      const wanted = new Map(inWindow.map((r) => [keyOf({ machineId: r.machine_id, source: r.source, kind: r.kind, eventId: r.event_id }), r]));
      for (const [key, list] of existing) {
        const current = wanted.get(key);
        for (const c of list) {
          if (!current || current.workspace_id !== c.workspaceId || tsMillis(`${current.ts.replace(" ", "T")}Z`) !== c.tsMs)
            deletes.push({ orgId: scope.orgId, sessionId: scope.sessionId, identity: c } as PendingDelete);
        }
      }
      await deleteRows(deps.ch, deletes);
    }
    return {
      manifests: s3.manifests,
      rows: inWindow.length,
      inserted: toInsert.length,
      deleted: deletes.length,
      complete,
      reasons: truncated ? [...s3.reasons, "clickhouse_truncated"] : s3.reasons,
    };
  });
}
