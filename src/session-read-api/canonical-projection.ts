/** Shared canonical S3 reader/projection. The live CH writer and read Worker
 * invoke this same implementation; neither maintains a shadow event mapping. */
import { assertReaderScrubMarker } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { canonicalDirectoryValues } from "./canonical-directory-values.ts";
import { createHash } from "node:crypto";
import {
  CONVERGENCE_EVENT_KIND,
  projectRelayhistorySession,
  deliveryMachine,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  normalizeRelayhistoryRecords,
  isLegacyRelayhistoryRecord,
  parseRelayhistoryRecord,
  relayhistoryExportProvenance,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { legacyMachineId } from "../shared/legacy-export-machine.ts";
import { PipelineError } from "../../packages/pipeline/src/lib/pipeline/errors.ts";
import { labelledActorName, messageSubagent } from "../../packages/pipeline/src/lib/pipeline/message-origin.ts";
import {
  chunkKeys,
  parseSessionKey,
  isManifestRest,
  isPublicationManifestRest,
  parseUploadGenerationPrefix,
  type ManifestV1,
  type ObjectStore,
} from "../../packages/object-store/src/index.ts";
import { type ManifestReader } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { boundedPrefetch, readChunkBody } from "./bounded-prefetch.ts";
import {
  toIndexRow,
  type IndexRow,
  type NeonEventRow,
} from "../session-pipeline/vendor/search-index/rows.ts";
import {
  directoryCatalogFromProjection,
  type DirectoryCatalog,
  type CatalogProjectionRow,
} from "../hoopdb/session-directory-catalog.ts";
import {
  directoryEventsFromProjection,
  type DirectoryEvents,
  type DirectoryEventInput,
} from "../hoopdb/session-directory-events.ts";
import {
  canonicalManifestIdentity,
  manifestUserId as admittedManifestUserId,
} from "../shared/manifest-identity.ts";
export interface SessionScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}
interface SearchIndexDeps {
  readerMaxRuleset?: number;
  reader: ManifestReader;
  store: ObjectStore;
  prefixFor: (orgId: string) => string;
  decompress: (stored: Uint8Array, maxBytes: number) => Uint8Array;
  /** Self-host READ_API_CANONICAL_FETCH_CONCURRENCY (CanonicalDeps.fetchConcurrency); absent: 1, sequential. */
  fetchConcurrency?: number;
  /** MESSAGE_ORIGIN_LABELS_ENABLED on an index writer (projectedToNeonRow). Absent: off. */
  originLabels?: boolean;
}
interface SearchIndexLimits {
  maxScopeBytes: number;
  maxScopeEvents: number;
  maxRows: number;
  insertBatch: number;
  /**
   * Read-artifact projection only; legacy CH writer does not construct display values.
   * "required" (artifact producer): a directory_display_* refusal makes the projection
   * incomplete with that reason code. "optional" (live raw read): the display values are
   * omitted and the code is reported in `displayOmitted`; the read itself still succeeds.
   */
  directoryValues?: "required" | "optional";
}
const MAX_CHUNK_BYTES = 16 * 1024 * 1024;
const SEP = "\u001f";
const keyOf = (i: {
  machineId: string;
  source: string;
  kind: string;
  eventId: string;
}) => [i.machineId, i.source, i.kind, i.eventId].join(SEP);
const sha256 = (bytes: Uint8Array) =>
  createHash("sha256").update(bytes).digest("hex");

/** The user a manifest's events belong to: the export grant's source user, else the 96bulls extension. */
export function manifestUserId(manifest: ManifestV1): string {
  try {
    return relayhistoryExportProvenance(manifest).sourceUserId;
  } catch {
    return admittedManifestUserId(manifest) ?? "";
  }
}

/** Every canonical record of one manifest, digests verified. Throws a PipelineError with a code. */
export async function readManifestRecords(
  manifest: ManifestV1,
  store: ObjectStore,
  prefix: string,
  decompress: (stored: Uint8Array, maxOutputLength: number) => Uint8Array,
  maxRecords = Number.POSITIVE_INFINITY,
  maximum?: number,
  fetchConcurrency = 1,
): Promise<RelayhistoryStreamRecord[]> {
  const rows: RelayhistoryStreamRecord[] = [];
  for await (const row of streamManifestRecords(manifest, store, prefix, decompress, maxRecords, maximum, fetchConcurrency))
    rows.push(row);
  return rows;
}

/**
 * Watches a chunk-by-chunk read (the streaming reconcile's read budget, issue 610
 * Phase 2): `chunk` runs before a chunk's object is fetched, `decompressed` after
 * it is inflated. Either may throw to stop the read. Both run in manifest order at
 * the chunk's position in the read loop; with fetchConcurrency > 1 later chunk
 * bodies may already be prefetched when `chunk` runs (the streaming reconcile
 * reads sequentially, so for it `chunk` precedes the fetch).
 */
export interface ChunkReadObserver {
  chunk(entry: Readonly<{ bytes: number; stored_bytes: number; stored_raw_bytes?: number; records?: number }>): void;
  decompressed(bytes: number): void;
}

/**
 * Bounded chunk-by-chunk reader; proofs need not retain full session payloads.
 * fetchConcurrency (self-host READ_API_CANONICAL_FETCH_CONCURRENCY via
 * SearchIndexDeps; default 1 = strictly sequential, as before) prefetches up to
 * that many chunk bodies (bounded-prefetch.ts); every check below still runs per
 * chunk in manifest order, so the first failing chunk refuses with the same code.
 */
export async function* streamManifestRecords(
  manifest: ManifestV1,
  store: ObjectStore,
  prefix: string,
  decompress: (stored: Uint8Array, maxOutputLength: number) => Uint8Array,
  maxRecords = Number.POSITIVE_INFINITY,
  maximum?: number,
  fetchConcurrency = 1,
  observe?: ChunkReadObserver,
): AsyncGenerator<RelayhistoryStreamRecord> {
  assertReaderScrubMarker(manifest.raw.scrub, maximum);
  if (manifest.raw.format !== "ndjson")
    throw new PipelineError(
      "raw_format_unsupported",
      "only ndjson raw is indexed",
      false,
    );
  let records = 0;
  const chunks = chunkKeys(manifest, { prefix });
  const tooLarge = (entry: (typeof chunks)[number]["entry"]) =>
    entry.stored_bytes > MAX_CHUNK_BYTES ||
    (entry.stored_raw_bytes ?? entry.bytes) > MAX_CHUNK_BYTES;
  // An oversized chunk is never fetched (null); the loop refuses it at its position.
  const bodies = boundedPrefetch(chunks, fetchConcurrency, ({ entry, key }, stop) =>
    tooLarge(entry) ? Promise.resolve(null) : readChunkBody(store, key, manifest.raw.scrub, maximum, entry.stored_bytes, stop));
  try {
    for (const { entry } of chunks) {
      if (tooLarge(entry))
        throw new PipelineError(
          "chunk_too_large",
          "a raw chunk exceeds the index limit",
          false,
        );
      observe?.chunk(entry);
      let stored: Uint8Array | null;
      try {
        stored = (await bodies.next()).value ?? null;
      } catch (error) {
        if (error instanceof PipelineError) throw error;
        if ((error as { code?: unknown } | null)?.code === "not_found")
          throw new PipelineError(
            "chunk_missing",
            "a listed raw chunk is missing",
            true,
          );
        throw new PipelineError(
          "chunk_unavailable",
          "a raw chunk read failed",
          true,
        );
      }
      if (
        !stored ||
        stored.byteLength !== entry.stored_bytes ||
        sha256(stored) !== entry.stored_sha256
      )
        throw new PipelineError(
          "chunk_digest_mismatch",
          "a raw chunk has the wrong stored digest",
          false,
        );
      let raw: Uint8Array;
      try {
        raw = decompress(stored, entry.stored_raw_bytes ?? MAX_CHUNK_BYTES);
      } catch (error) {
        if (error instanceof PipelineError) throw error;
        throw new PipelineError(
          "chunk_decompress_failed",
          "a raw chunk is not valid zstd",
          false,
        );
      }
      observe?.decompressed(raw.byteLength);
      if (entry.raw_sha256 && sha256(raw) !== entry.raw_sha256)
        throw new PipelineError(
          "chunk_digest_mismatch",
          "a raw chunk has the wrong raw digest",
          false,
        );
      let text: string;
      try {
        text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
          raw,
        );
      } catch {
        throw new PipelineError(
          "chunk_not_utf8",
          "a raw chunk is not UTF-8",
          false,
        );
      }
      for (const line of text.split("\n"))
        if (line) {
          if (records >= maxRecords)
            throw new PipelineError(
              "scope_too_large",
              "actual canonical record count exceeds scope limit",
              false,
            );
          records += 1;
          yield parseRelayhistoryRecord(line);
        }
    }
  } finally {
    // Stops the prefetch on refusal or an early consumer stop (no unhandled sibling rejections).
    await bodies.return();
  }
}

type Json = Record<string, unknown>;

const str = (v: unknown): string | null =>
  typeof v === "string" ? v : v === null || v === undefined ? null : String(v);

/** A projected `convergence_events` row as the Neon reader's row shape, so `toIndexRow` maps it. */
export function projectedToNeonRow(
  row: Json,
  scope: SessionScope,
  userId: string,
  /** MESSAGE_ORIGIN_LABELS_ENABLED on the writer: label who wrote the event (message-origin.ts). Off: unchanged. */
  originLabels = false,
): NeonEventRow {
  const taskRef =
    row.task_ref && typeof row.task_ref === "object"
      ? (row.task_ref as Json)
      : {};
  // Who wrote the event (message-origin.ts), only with the flag on. The parity
  // projection leaves both NULL or as relayhistory sent them.
  const record = row.record && typeof row.record === "object" ? (row.record as { payload?: unknown }) : null;
  const payload = record?.payload && typeof record.payload === "object" ? (record.payload as Json) : null;
  const kind = String(row.kind);
  const base: Omit<NeonEventRow, "fingerprint"> = {
    org_id: scope.orgId,
    workspace_id: scope.workspaceId,
    machine_id: String(row.machine_id),
    user_id: str(row.user_id) ?? userId,
    source: String(row.source),
    session_id: String(row.session_id),
    event_id: String(row.event_id),
    kind,
    type: str(row.type) ?? kind,
    ts_text: String(row.ts),
    actor_name: originLabels ? labelledActorName(kind, payload, str(row.actor_name)) : str(row.actor_name),
    actor_role: str(row.actor_role),
    subagent_id: originLabels ? (str(row.subagent_id) ?? messageSubagent(kind, payload)) : str(row.subagent_id),
    project_id: str(row.project_id),
    task_title: str(row.task_title),
    task_ref: JSON.stringify(taskRef),
    task_ref_id: str(taskRef.id),
    tags: JSON.stringify(Array.isArray(row.tags) ? row.tags : []),
    files_touched: JSON.stringify(
      Array.isArray(row.files_touched) ? row.files_touched : [],
    ),
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

/** The projection identity of one group of records: whose session, machine and user they are. */
export interface EventProjectionIdentity {
  sessionId: string;
  machineId: string;
  userId: string;
}

/** A projected event row's admitted owner (the canonical reader's multi-manifest map). */
export type EventRowOwner = (
  row: Json,
) => { userId: string; sessionId: string } | undefined;

/** The generic delivery projection uses role as type. Codex tool-use captures
 * carry an explicit non-chat kind; restore it only with a same-origin, same
 * physical-session/message-id tool-call witness from the verified record union.
 * This changes classification only: content, identities, ordering and counts stay.
 */
function codexToolCaptures(records: readonly RelayhistoryStreamRecord[], scope: SessionScope) {
  const calls = new Map<string, RelayhistoryStreamRecord | null>();
  const key = (record: RelayhistoryStreamRecord): string | null => {
    const p = record.payload;
    return p && p.source === "codex" && typeof p.session_id === "string" && p.session_id.trim() &&
      typeof p.message_id === "string" && p.message_id.trim()
      ? JSON.stringify([record.origin_id, p.session_id, p.message_id]) : null;
  };
  if (scope.harness !== "codex") return new Map<string, string>();
  for (const record of records) {
    if (record.kind !== "tool_call") continue;
    const k = key(record);
    if (!k || typeof record.payload?.name !== "string" || !record.payload.name.trim()) continue;
    calls.set(k, calls.has(k) ? null : record);
  }
  const captures = new Map<string, string>();
  for (const record of records) {
    if (record.kind !== "session_event" || record.payload?.kind !== "tool_use" || record.payload.role !== "assistant") continue;
    const k = key(record), call = k ? calls.get(k) : null;
    if (call) captures.set(JSON.stringify([deliveryMachine(scope.workspaceId, record.origin_id), record.record_id]), call.payload!.name as string);
  }
  return captures;
}

/**
 * The one record-to-event projection both readers run (readCanonicalScope per manifest
 * group, owner-buffered.ts per buffered origin): projectRelayhistorySession, then each
 * `convergence_events` row with its owner restored and mapped to the index row shape.
 * Pure. A projection failure is returned as `{ error }` (the canonical reader records the
 * reason and continues; the buffered reader hides the session); a failure mapping a
 * projected row throws, as it always has. Catalog rows are returned in projection order.
 */
export function projectEventRows(
  records: RelayhistoryStreamRecord[],
  scope: SessionScope,
  identity: EventProjectionIdentity,
  version: number,
  owner?: EventRowOwner,
  /** MESSAGE_ORIGIN_LABELS_ENABLED (projectedToNeonRow). Off by default. */
  originLabels = false,
):
  | { error: unknown }
  | {
      catalogRows: CatalogProjectionRow[];
      events: Array<{ row: Json; indexRow: IndexRow }>;
    } {
  let projected;
  try {
    projected = projectRelayhistorySession(records, {
      workspaceId: scope.workspaceId,
      source: scope.harness,
      sessionId: identity.sessionId,
      machineId: identity.machineId,
      userId: identity.userId,
    });
  } catch (error) {
    return { error };
  }
  const codexTools = codexToolCaptures(records, scope);
  const catalogRows: CatalogProjectionRow[] = [];
  const events: Array<{ row: Json; indexRow: IndexRow }> = [];
  for (const { table, row } of projected.rows) {
    if (table !== "convergence_events") {
      catalogRows.push({ table, row });
      continue;
    }
    const found = owner?.(row);
    const toolName = row.kind === "session_event" && typeof row.delivery_record_id === "string"
      ? codexTools.get(JSON.stringify([row.machine_id, row.delivery_record_id])) : undefined;
    const canonicalRow = {
      ...row,
      ...(toolName ? { type: "tool_use", tool_name: toolName } : {}),
      session_id: row.delivery_record_id
        ? (found?.sessionId ?? row.session_id)
        : row.session_id,
      user_id: row.user_id ?? found?.userId ?? identity.userId,
    };
    const indexRow = toIndexRow(
      projectedToNeonRow(canonicalRow, scope, identity.userId, originLabels),
      version,
    );
    events.push({ row: canonicalRow, indexRow });
  }
  return { catalogRows, events };
}

export interface ScopeRead {
  records: RelayhistoryStreamRecord[];
  projectionRows: Array<{ table: string; row: Record<string, unknown> }>;
  rows: IndexRow[];
  manifests: number;
  complete: boolean;
  /** Too large to index whole: nothing was read. */
  partial: boolean;
  /** Highest manifest revision of the scope, recorded with its index state. */
  revision: number;
  reasons: string[];
  catalog: DirectoryCatalog;
  directorySessionId: string | null;
  events: DirectoryEvents | null;
  /** directory_display_* code when optional display values were omitted. */
  displayOmitted?: string | null;
  sources: DirectoryEventInput["sources"];
  /**
   * Only on a `partial` result: the manifests admitted before sizing, in read
   * order, so the streaming reconcile (canonical-stream-projection.ts) reads
   * exactly what this read would have.
   */
  admitted?: Array<{ manifest: ManifestV1; key: string }>;
}

export async function readCanonicalScope(
  scope: SessionScope,
  manifestKeys: string[],
  deps: SearchIndexDeps,
  limits: SearchIndexLimits,
  version: number,
  expectedCanonicalId?: string,
): Promise<ScopeRead> {
  const decompress = deps.decompress;
  const prefix = deps.prefixFor(scope.orgId);
  const reasons: string[] = [];
  let complete = true;
  type Part = {
    manifest: ManifestV1;
    records: RelayhistoryStreamRecord[];
    userId: string;
  };
  const parts: Part[] = [];
  const readable: ManifestV1[] = [];
  const manifestKeysByBody = new Map<ManifestV1, string>();
  for (const key of manifestKeys) {
    const read = await deps.reader.read(key);
    if (!read) {
      // A live pointer whose manifest is gone: its delete event is in flight.
      complete = false;
      reasons.push("manifest_missing");
      continue;
    }
    const manifest = read.manifest;
    // The body must describe the key it was read from (as the pointer stage
    // requires): chunk keys are built from the body, rows are stamped with the scope.
    const s = manifest.session;
    const at = parseSessionKey(key);
    if (
      !at || !isManifestRest(at.rest) ||
      ((parseUploadGenerationPrefix(at.prefix)?.base ?? at.prefix ?? "") !== prefix) ||
      (isPublicationManifestRest(at.rest) && !parseUploadGenerationPrefix(at.prefix)) ||
      s.workspace_id !== scope.workspaceId ||
      s.harness !== scope.harness ||
      s.session_id !== at.sessionId ||
      (!expectedCanonicalId && s.session_id !== scope.sessionId) ||
      s.machine_id !== at.machineId
    ) {
      complete = false;
      reasons.push("manifest_key_mismatch");
      continue;
    }
    // Unscrubbed raw is quarantined and never read (manifest spec, "Scrubbing").
    // Not reading it is not knowing it: the pass deletes nothing.
    if (!manifest.raw.scrub) {
      complete = false;
      reasons.push("manifest_unscrubbed");
      continue;
    }
    if (
      expectedCanonicalId &&
      canonicalManifestIdentity(manifest).sessionId !== expectedCanonicalId
    ) {
      complete = false;
      reasons.push("canonical_session_mismatch");
      continue;
    }
    try { assertReaderScrubMarker(manifest.raw.scrub, deps.readerMaxRuleset); }
    catch (error) {
      if (!(error instanceof PipelineError) || error.code !== "scrub_ruleset_unsupported") throw error;
      complete = false; reasons.push(error.code); continue;
    }
    readable.push(manifest);
    manifestKeysByBody.set(manifest, key);
  }

  // Size the whole session from its manifests before reading a chunk. A
  // session past either bound is not read at all: it is marked partial (see
  // indexSearchScope), never folded from a subset of its records, which could
  // index a record's older revision whose newer one sits in an unread chunk.
  const revision = Math.max(0, ...readable.map((m) => m.revision));
  const bytes = readable.reduce(
    (n, m) =>
      n +
      m.raw.chunks.reduce(
        (b, c) => b + Math.max(c.bytes, c.stored_raw_bytes ?? 0),
        0,
      ),
    0,
  );
  const events = readable.reduce(
    (n, m) => n + m.raw.chunks.reduce((b, c) => b + (c.records ?? 0), 0),
    0,
  );
  if (bytes > limits.maxScopeBytes || events > limits.maxScopeEvents)
    return {
      rows: [],
      manifests: readable.length,
      complete: false,
      partial: true,
      revision,
      reasons: [...new Set([...reasons, "scope_too_large"])],
      catalog: directoryCatalogFromProjection([]),
      directorySessionId: null,
      events: null,
      sources: [],
      projectionRows: [],
      records: [],
      admitted: readable.map((m) => ({ manifest: m, key: manifestKeysByBody.get(m)! })),
    };

  let parsedRecords = 0;
  for (const manifest of readable) {
    try {
      const records = await readManifestRecords(
        manifest,
        deps.store,
        parseSessionKey(manifestKeysByBody.get(manifest)!)?.prefix ?? prefix,
        decompress,
        limits.maxScopeEvents - parsedRecords,
        deps.readerMaxRuleset,
        deps.fetchConcurrency,
      );
      parsedRecords += records.length;
      parts.push({ manifest, records, userId: manifestUserId(manifest) });
    } catch (error) {
      if (error instanceof PipelineError && error.code === "scope_too_large")
        return {
          rows: [],
          manifests: parts.length,
          complete: false,
          partial: true,
          revision,
          reasons: [...new Set([...reasons, "scope_too_large"])],
          catalog: directoryCatalogFromProjection([]),
          directorySessionId: null,
          events: null,
          sources: [],
          projectionRows: [],
          records: [],
          admitted: readable.map((m) => ({ manifest: m, key: manifestKeysByBody.get(m)! })),
        };
      if (error instanceof PipelineError && error.retryable) throw error;
      complete = false;
      reasons.push(
        error instanceof PipelineError ? error.code : "manifest_unreadable",
      );
    }
  }

  // Legacy evidence provenance (activation gate 4). Legacy records skip delivery
  // acceptance contention and owner binding, so they are trusted only from ONE
  // manifest per scope that the legacy exporter wrote itself: the export marker,
  // raw owner `migration:{grant}`, a single `initial` epoch (no adopted or client
  // courier chunks), the dedicated legacy machine, and nothing but legacy records.
  // Anything else drops only legacy evidence. Live canonical rows remain complete;
  // a forged or stale legacy copy can neither replace content nor make the whole
  // session unavailable.
  {
    const legacyParts = parts.filter((p) =>
      p.records.some(isLegacyRelayhistoryRecord),
    );
    const owned = (m: ManifestV1) => {
      const marker = m.extensions?.["client.relayhistory_export"] as
        | { grant_id?: unknown }
        | undefined;
      return (
        typeof marker?.grant_id === "string" &&
        m.raw.owner === `migration:${marker.grant_id}` &&
        m.raw.epochs.length === 1 &&
        m.raw.epochs[0]!.reason === "initial" &&
        canonicalManifestIdentity(m).machineId ===
          legacyMachineId(m.session.workspace_id, manifestUserId(m))
      );
    };
    const inScope = (r: RelayhistoryStreamRecord) => {
      const p = r.payload;
      return (
        p?.org_id === scope.orgId &&
        p.workspace_id === scope.workspaceId &&
        p.source === scope.harness &&
        p.session_id === scope.sessionId
      );
    };
    if (
      legacyParts.length > 1 ||
      legacyParts.some(
        (p) =>
          !owned(p.manifest) ||
          !p.records.every(
            (r) => isLegacyRelayhistoryRecord(r) && inScope(r),
          ),
      )
    ) {
      reasons.push("legacy_provenance_invalid");
      for (const p of legacyParts)
        p.records = p.records.filter((r) => !isLegacyRelayhistoryRecord(r));
    }
  }
  // Fold every copy together; a conflicting copy (same revision, different
  // bytes) falls back to folding each manifest on its own, without deletes.
  let canonicalRecords: RelayhistoryStreamRecord[] = [];
  let groups: { records: RelayhistoryStreamRecord[]; part: Part }[];
  try {
    const all = normalizeRelayhistoryRecords(parts.flatMap((p) => p.records));
    canonicalRecords = all;
    const owner = new Map<string, Part>();
    for (const part of parts)
      for (const r of part.records)
        if (!owner.has(`${r.origin_id}${SEP}${r.record_id}`))
          owner.set(`${r.origin_id}${SEP}${r.record_id}`, part);
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
    if (activity.length > 0 && parts[0])
      groups.push({ records: activity, part: parts[0] });
  } catch (error) {
    complete = false;
    reasons.push(error instanceof PipelineError ? error.code : "fold_failed");
    groups = [];
    for (const part of parts) {
      try {
        groups.push({
          records: normalizeRelayhistoryRecords(part.records),
          part,
        });
      } catch {
        reasons.push("manifest_fold_failed");
      }
    }
  }

  const ownerByEvent = new Map<string, { userId: string; sessionId: string }>();
  for (const part of parts)
    for (const record of part.records) {
      if (
        record.kind === CONVERGENCE_EVENT_KIND ||
        isLegacyRelayhistoryRecord(record)
      )
        continue;
      const identity = JSON.stringify([
        deliveryMachine(scope.workspaceId, record.origin_id),
        record.record_id,
      ]);
      const prior = ownerByEvent.get(identity);
      if (prior?.userId && part.userId && prior.userId !== part.userId) {
        complete = false;
        reasons.push("event_owner_conflict");
      } else if (part.userId || prior === undefined)
        ownerByEvent.set(identity, {
          userId: part.userId,
          sessionId: canonicalManifestIdentity(part.manifest).sessionId,
        });
    }
  const byKey = new Map<string, IndexRow>();
  const metadataByKey = new Map<string, Json>();
  const catalogRows: CatalogProjectionRow[] = [];
  for (const { records, part } of groups) {
    const projected = projectEventRows(
      records,
      scope,
      {
        sessionId: canonicalManifestIdentity(part.manifest).sessionId,
        machineId: canonicalManifestIdentity(part.manifest).machineId,
        userId: part.userId,
      },
      version,
      // Delivery projection is tenant-free; restore the record's admitted owner,
      // not the uploader/first-manifest owner of a multi-machine logical scope.
      (row) =>
        ownerByEvent.get(JSON.stringify([row.machine_id, row.delivery_record_id])),
      deps.originLabels === true,
    );
    if ("error" in projected) {
      complete = false;
      reasons.push(
        projected.error instanceof PipelineError
          ? projected.error.code
          : "projection_failed",
      );
      continue;
    }
    catalogRows.push(...projected.catalogRows);
    for (const { row: canonicalRow, indexRow } of projected.events) {
      const identity = keyOf({
        machineId: indexRow.machine_id,
        source: indexRow.source,
        kind: indexRow.kind,
        eventId: indexRow.event_id,
      });
      byKey.set(identity, indexRow);
      metadataByKey.set(identity, {
        ...canonicalRow,
        _index_fingerprint: indexRow.fingerprint,
      });
    }
  }
  const directoryIds = new Set(
    parts.map(({ manifest }) => canonicalManifestIdentity(manifest).sessionId),
  );
  if (directoryIds.size > 1) {
    complete = false;
    reasons.push("canonical_session_mismatch");
  }
  let metadata: DirectoryEvents | null = null;
  try {
    metadata = directoryEventsFromProjection([...metadataByKey.values()]);
  } catch {
    complete = false;
    reasons.push("directory_event_metadata_invalid");
  }
  // Both locators were selected from metadataByKey. Fetch only those rows:
  // display materialization never scans/copies the full transcript a second time.
  let displayValues: ReturnType<typeof canonicalDirectoryValues> = [];
  let displayOmitted: string | null = null;
  if (limits.directoryValues && metadata) {
    try {
      displayValues = canonicalDirectoryValues({orgId:scope.orgId,workspaceId:scope.workspaceId},metadata,
        [...new Set([metadata.openingEvent,metadata.projectEvent].flatMap(event=>event ? [keyOf({machineId:event.machine_id,source:event.source,kind:event.kind,eventId:event.event_id})] : []))]
          .map(key=>metadataByKey.get(key)!));
    } catch (error) {
      const code = error instanceof Error && /^directory_display_[a-z_]+$/.test(error.message) ? error.message : null;
      if (!code) throw error;
      reasons.push(code);
      if (limits.directoryValues === "required") complete = false;
      else displayOmitted = code;
    }
  }
  return {
    records: canonicalRecords,
    projectionRows: [
      ...catalogRows,
      ...[...metadataByKey.values()].map(
        ({ _index_fingerprint: _fingerprint, ...row }) => ({
          table: "convergence_events",
          row,
        }),
      ),
      ...displayValues,
    ],
    rows: [...byKey.values()],
    manifests: parts.length,
    complete,
    partial: false,
    revision,
    reasons: [...new Set(reasons)],
    catalog: directoryCatalogFromProjection(catalogRows),
    directorySessionId: directoryIds.size === 1 ? [...directoryIds][0]! : null,
    events: metadata,
    displayOmitted,
    sources: parts.map(({ manifest }) => ({
      manifestKey: manifestKeysByBody.get(manifest)!,
      revision: String(manifest.revision),
    })),
  };
}
