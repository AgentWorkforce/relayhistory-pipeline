/**
 * The streamed S3 read of one search scope too large to read whole
 * (SEARCH_INDEX_STREAMING_CH=1 only; search-index.ts streamedReconcile). It
 * computes what readScope computes for the same admitted manifests, the same
 * index rows, the same directory catalog, without holding the session's
 * records: every pass reads the manifests' chunks again, one chunk at a time.
 *
 *   1. fold    payload-free seats: per (origin, record) the revision
 *              normalizeRelayhistoryRecords keeps, its first manifest, then the
 *              newest across origins (lastAccepted), as the whole fold does;
 *   2. facts   the catalog winners (small, held whole) and every tool use's
 *              earliest parent time (settleToolTimes over the whole session);
 *   3. verify  every winner projected, settled and mapped to its index row,
 *              and the row's identity kept (hashed key, time, fingerprint);
 *   4. write   only the rows the index lacks, regenerated and checked
 *              against pass 3 (`regenerate`).
 *
 * Read budgets are summed over every pass; past one, or past the deadline
 * (checked before every chunk), the read stops with StreamStop: partial, and
 * nothing more is read or written. A read, fold or projection error stops it
 * with that code (not partial), where the whole read would go on without
 * deletes. A retryable read error (chunk missing or unavailable) is thrown as
 * it is, as in the whole read.
 */
import { createHash } from "node:crypto";
import {
  CONVERGENCE_EVENT_KIND,
  PipelineError,
  compareRelayhistoryTimes,
  isLegacyRelayhistoryRecord,
  lastAccepted,
  normalizeRelayhistoryRecords,
  projectRelayhistorySession,
  receivedFirst,
  relayhistoryRevisionContent,
  relayhistoryToolChild,
  relayhistoryToolParent,
  type ChunkEntry,
  type ManifestV1,
  type ProjectionContext,
  type RelayhistoryStreamRecord,
} from "../shared/vendor.ts";
import { toIndexRow, tsMillis, type IndexRow } from "./vendor/search-index/rows.ts";
import {
  directoryCatalogFromProjection,
  type CatalogProjectionRow,
  type DirectoryCatalog,
} from "../hoopdb/session-directory-catalog.ts";
import type { SessionScope } from "./pointer.ts";
import { ACCEPTANCE_MISSING, seatsOfGroups, type ContenderSeats } from "./contender-seats.ts";

/** The most passes a scope is read in: a scope streams only if this many reads fit the budgets. */
export const STREAM_PASSES = 4;
export const STREAM_READ_BYTES_BUDGET = "stream_read_bytes_budget";
export const STREAM_READ_EVENTS_BUDGET = "stream_read_events_budget";
/** The invocation's deadline passed: partial like a spent budget. */
export const STREAM_DEADLINE = "stream_deadline";
/** Two surviving events share an index key the whole read would resolve by list order. */
export const STREAM_DUPLICATE_EVENT = "stream_duplicate_event";
/** A regenerated row differs from what pass 3 recorded. */
export const STREAM_SOURCE_CHANGED = "stream_source_changed";

/** Read budgets summed over every pass of one scope. */
export interface StreamReadLimits {
  maxReadBytes: number;
  maxReadEvents: number;
}

/** A wall-clock bound on one streamed scope. */
export interface StreamDeadline {
  /** Stop once `now()` reaches this instant (epoch ms). */
  at: number;
  /** Injectable clock; default Date.now. */
  now?: () => number;
}

export function streamDeadlinePassed(deadline: StreamDeadline | undefined): boolean {
  return deadline !== undefined && (deadline.now ?? Date.now)() >= deadline.at;
}

/**
 * The streamed read stopped: nothing more is read or written. `partial`: a budget or the deadline.
 * `contenderSeats`: only on an ACCEPTANCE_MISSING stop, which fold refused and how (contender-seats.ts).
 */
export class StreamStop extends Error {
  constructor(readonly code: string, readonly partial: boolean, readonly contenderSeats?: ContenderSeats) {
    super(code);
  }
}

/** Raw bytes a manifest's chunks declare, as readScope sizes a scope. */
export const chunkBytes = (c: ChunkEntry) => Math.max(c.bytes, c.stored_raw_bytes ?? 0);

/** What the reader needs from the caller: the admitted manifests and how to read one chunk. */
export interface StreamSource {
  scope: SessionScope;
  /** The admitted manifests, in readScope's order. */
  manifests: readonly ManifestV1[];
  /** manifestUserId of each manifest. */
  userIds: readonly string[];
  chunks(manifest: ManifestV1): Iterable<{ entry: ChunkEntry; key: string }>;
  /** One chunk's records, digests verified (readManifestRecords' per-chunk read). Throws a PipelineError. */
  readChunk(entry: ChunkEntry, key: string): Promise<RelayhistoryStreamRecord[]>;
  /** Awaited before every chunk of every pass: the deadline and the transaction keepalive. */
  beforeChunk(): Promise<void>;
  budget: StreamReadLimits;
  version: number;
  fromMs: number;
}

export interface StreamIdentity {
  workspaceId: string;
  kind: string;
  tsMs: number;
  fingerprint: string;
  inWindow: boolean;
  direct: boolean;
}

export interface StreamRead {
  /** Every projected event's identity by hashed search key (streamKey). */
  identities: Map<string, StreamIdentity>;
  /** Rows inside the window. */
  rows: number;
  /** Kinds of the rows inside the window. */
  kinds: Set<string>;
  catalog: DirectoryCatalog;
  /** Pass 4: the rows whose key is in `keys`, in batches of `batch`, through `write`; returns how many were written. */
  regenerate(keys: ReadonlySet<string>, batch: number, write: (rows: IndexRow[]) => Promise<void>): Promise<number>;
  /** Bytes and events read so far, summed over the passes. */
  read: { bytes: number; events: number };
}

const SEP = "\u001f";

/** The search key of an index identity (search-index.ts keyOf), hashed so a pass can hold one per row. */
export function streamKey(i: { machineId: string; source: string; kind: string; eventId: string }): string {
  return createHash("sha256").update([i.machineId, i.source, i.kind, i.eventId].join(SEP)).digest("base64").slice(0, 22);
}

const ACTIVITY = new Set(["history", "session_event", "tool_call", "file_edit", "trajectory"]);

interface Seat {
  kind: string;
  recordId: string;
  revision: bigint;
  receivedAt: string;
  content: string;
  acceptedSeq: string | undefined;
  live: boolean;
  /** The first manifest that carried this (origin, record), any revision (readScope's `owner`). */
  owner: number;
  winner: boolean;
  /** The last pass that took this seat's record, so a second copy in the same pass is skipped. */
  pass: number;
}

type Json = Record<string, unknown>;

const contentOf = (r: RelayhistoryStreamRecord) => createHash("sha256").update(relayhistoryRevisionContent(r)).digest("base64");

/** Stops the stream with a fold/projection code; retryable read errors pass through. */
function asStop(error: unknown, fallback: string): never {
  if (error instanceof StreamStop) throw error;
  if (error instanceof PipelineError && error.retryable) throw error;
  throw new StreamStop(error instanceof PipelineError ? error.code : fallback, false);
}

export async function streamScope(
  src: StreamSource,
  projectedToRow: (row: Json, scope: SessionScope, userId: string) => Parameters<typeof toIndexRow>[0],
): Promise<StreamRead> {
  const { scope, manifests, userIds, budget } = src;
  const read = { bytes: 0, events: 0 };

  /** Every chunk of every admitted manifest, metered and deadline-checked. */
  async function walk(onChunk: (records: RelayhistoryStreamRecord[], manifest: number) => void | Promise<void>) {
    for (const [index, manifest] of manifests.entries()) {
      for (const { entry, key } of src.chunks(manifest)) {
        await src.beforeChunk();
        read.bytes += chunkBytes(entry);
        if (read.bytes > budget.maxReadBytes) throw new StreamStop(STREAM_READ_BYTES_BUDGET, true);
        let records: RelayhistoryStreamRecord[];
        try {
          records = await src.readChunk(entry, key);
        } catch (error) {
          asStop(error, "manifest_unreadable");
        }
        read.events += records.length;
        if (read.events > budget.maxReadEvents) throw new StreamStop(STREAM_READ_EVENTS_BUDGET, true);
        await onChunk(records, index);
      }
    }
  }

  // Pass 1: the fold, payload-free (normalizeRelayhistoryRecords on seats).
  const seats = new Map<string, Seat>();
  // The fold's recordKey: legacy kinds are seated per (origin, record, kind).
  const seatKey = (r: RelayhistoryStreamRecord) =>
    JSON.stringify(isLegacyRelayhistoryRecord(r) ? [r.origin_id, r.record_id, r.kind] : [r.origin_id, r.record_id]);
  // readScope's `owner` is the first manifest carrying (origin, record) in any kind; a legacy
  // record has its own seat, so the first manifest carrying one is kept here for direct lines.
  const legacyFirst = new Map<string, number>();
  await walk((records, index) => {
    for (const r of records) {
      if (isLegacyRelayhistoryRecord(r)) {
        const at = JSON.stringify([r.origin_id, r.record_id]);
        if (!legacyFirst.has(at)) legacyFirst.set(at, index);
      }
      const key = seatKey(r);
      const held = seats.get(key);
      const revision = BigInt(r.revision);
      if (!held || revision > held.revision) {
        const seat: Seat = {
          kind: r.kind, recordId: r.record_id, revision, receivedAt: r.received_at, content: contentOf(r),
          acceptedSeq: r.accepted_seq, live: r.operation === "upsert" && r.payload !== null,
          owner: held ? held.owner : index, winner: false, pass: 0,
        };
        seats.set(key, seat);
        continue;
      }
      if (revision !== held.revision) continue;
      if (contentOf(r) !== held.content) throw new StreamStop("relayhistory_revision_conflict", false);
      if (r.accepted_seq !== undefined && held.acceptedSeq !== undefined && BigInt(r.accepted_seq) !== BigInt(held.acceptedSeq))
        throw new StreamStop("relayhistory_acceptance_conflict", false);
      if (receivedFirst(r, { received_at: held.receivedAt } as RelayhistoryStreamRecord)) held.receivedAt = r.received_at;
      held.acceptedSeq = r.accepted_seq ?? held.acceptedSeq;
    }
  });
  // newestAcrossOrigins: a direct line and a legacy record are their own; other kinds keep the
  // last accepted per record id.
  const contenders = new Map<string, Seat[]>();
  for (const [key, seat] of seats) {
    if (seat.kind === CONVERGENCE_EVENT_KIND) {
      seat.owner = Math.min(seat.owner, legacyFirst.get(key) ?? Infinity);
      seat.winner = seat.live;
      continue;
    }
    if (isLegacyRelayhistoryRecord(seat)) {
      seat.winner = seat.live;
      continue;
    }
    const group = contenders.get(seat.recordId);
    if (group) group.push(seat);
    else contenders.set(seat.recordId, [seat]);
  }
  for (const group of contenders.values()) {
    // `group.reduce(lastAccepted)` as the whole fold runs it, on stand-ins that point back at their seat.
    type Contender = RelayhistoryStreamRecord & { seat: Seat };
    const stand = group.map((seat) => ({ accepted_seq: seat.acceptedSeq, seat }) as unknown as Contender);
    let won: Contender;
    try {
      won = stand.reduce((left, right) => lastAccepted(left, right) as Contender);
    } catch (error) {
      // Failure path only: every record id group is already in `contenders`.
      if (error instanceof PipelineError && error.code === ACCEPTANCE_MISSING)
        throw new StreamStop(error.code, false, seatsOfGroups([...contenders.values()].map((g) => g.map((seat) => seat.acceptedSeq))));
      asStop(error, "fold_failed");
    }
    won.seat.winner = won.seat.live;
  }

  /** The surviving records of one chunk, each once per pass, with the fold's merged acceptance seat. */
  const winners = (records: RelayhistoryStreamRecord[], pass: number) => {
    const out: { record: RelayhistoryStreamRecord; seat: Seat }[] = [];
    for (const r of records) {
      const seat = seats.get(seatKey(r));
      if (!seat?.winner || seat.pass === pass || BigInt(r.revision) !== seat.revision || r.received_at !== seat.receivedAt) continue;
      seat.pass = pass;
      out.push({ record: seat.acceptedSeq === undefined ? r : { ...r, accepted_seq: seat.acceptedSeq }, seat });
    }
    return out;
  };
  const context = (index: number): ProjectionContext => ({
    workspaceId: scope.workspaceId,
    source: scope.harness,
    sessionId: scope.sessionId,
    machineId: manifests[index]!.session.machine_id,
    userId: userIds[index]!,
  });
  const project = (records: RelayhistoryStreamRecord[], index: number) => {
    try {
      return projectRelayhistorySession(records, context(index)).rows;
    } catch (error) {
      asStop(error, "projection_failed");
    }
  };

  // Pass 2: catalog winners and tool parent times.
  const catalogRecords: RelayhistoryStreamRecord[] = [];
  const parents = new Map<string, string>();
  await walk((records) => {
    const activity: RelayhistoryStreamRecord[] = [];
    for (const { record } of winners(records, 2)) {
      if (record.kind === CONVERGENCE_EVENT_KIND) continue;
      if (ACTIVITY.has(record.kind)) activity.push(record);
      else catalogRecords.push(record);
    }
    for (const { table, row } of project(activity, 0)) {
      if (table !== "convergence_events") continue;
      const parent = relayhistoryToolParent(row);
      if (!parent) continue;
      const seen = parents.get(parent.toolUse);
      if (seen === undefined || compareRelayhistoryTimes(parent.ts, seen) < 0) parents.set(parent.toolUse, parent.ts);
    }
  });
  // The catalog rows in the whole fold's order (a subset of a total order keeps its order).
  let catalogRows: CatalogProjectionRow[];
  try {
    catalogRows = project(normalizeRelayhistoryRecords(catalogRecords), 0).filter((r) => r.table !== "convergence_events");
  } catch (error) {
    // The record id fold already passed: this refusal is the projection's catalog-key fold.
    if (error instanceof StreamStop && error.code === ACCEPTANCE_MISSING) throw new StreamStop(error.code, false, "catalog_key");
    asStop(error, "fold_failed");
  }

  /** One chunk's winners as index rows: activity under the first manifest, direct lines under their owner. */
  const indexRows = (records: RelayhistoryStreamRecord[], pass: number) => {
    const activity: RelayhistoryStreamRecord[] = [];
    const direct = new Map<number, RelayhistoryStreamRecord[]>();
    for (const { record, seat } of winners(records, pass)) {
      if (record.kind === CONVERGENCE_EVENT_KIND) direct.set(seat.owner, [...(direct.get(seat.owner) ?? []), record]);
      else if (ACTIVITY.has(record.kind)) activity.push(record);
    }
    const out: { row: IndexRow; key: string; direct: boolean }[] = [];
    const add = (rows: ReturnType<typeof project>, index: number, isDirect: boolean) => {
      for (const { table, row } of rows) {
        if (table !== "convergence_events") continue;
        const child = isDirect ? null : relayhistoryToolChild(row);
        if (child !== null) row.ts = parents.get(child) ?? row.ingested_at;
        const indexRow = toIndexRow(projectedToRow(row, scope, userIds[index]!), src.version);
        out.push({ row: indexRow, key: streamKey({ machineId: indexRow.machine_id, source: indexRow.source, kind: indexRow.kind, eventId: indexRow.event_id }), direct: isDirect });
      }
    };
    for (const [index, list] of direct) add(project(list, index), index, true);
    if (activity.length) add(project(activity, 0), 0, false);
    return out;
  };
  const rowMs = (row: IndexRow) => tsMillis(`${row.ts.replace(" ", "T")}Z`);

  // Pass 3: every row's identity.
  const identities = new Map<string, StreamIdentity>();
  await walk((records) => {
    for (const { row, key, direct } of indexRows(records, 3)) {
      const tsMs = rowMs(row);
      const identity: StreamIdentity = { workspaceId: row.workspace_id, kind: row.kind, tsMs, fingerprint: row.fingerprint, inWindow: tsMs >= src.fromMs, direct };
      const held = identities.get(key);
      if (held) {
        // The whole read keeps the activity group's row over a direct line's (it is folded last).
        if (held.direct && !direct) identities.set(key, identity);
        else if (!held.direct && direct) continue;
        else throw new StreamStop(STREAM_DUPLICATE_EVENT, false);
        continue;
      }
      identities.set(key, identity);
    }
  });
  let rows = 0;
  const kinds = new Set<string>();
  for (const i of identities.values())
    if (i.inWindow) {
      rows++;
      kinds.add(i.kind);
    }

  return {
    identities,
    rows,
    kinds,
    catalog: directoryCatalogFromProjection(catalogRows),
    read,
    async regenerate(keys, batch, write) {
      let written = 0;
      let buffer: IndexRow[] = [];
      const flush = async () => {
        if (!buffer.length) return;
        const rows = buffer;
        buffer = [];
        await write(rows);
        written += rows.length;
      };
      const done = new Set<string>();
      await walk(async (records) => {
        for (const { row, key, direct } of indexRows(records, 4)) {
          if (!keys.has(key) || done.has(key)) continue;
          const want = identities.get(key);
          // Only the row pass 3 kept for this key (the activity row over a direct line).
          if (want && want.direct !== direct) continue;
          if (!want || want.tsMs !== rowMs(row) || want.fingerprint !== row.fingerprint || want.workspaceId !== row.workspace_id)
            throw new StreamStop(STREAM_SOURCE_CHANGED, false);
          done.add(key);
          buffer.push(row);
          if (buffer.length >= batch) await flush();
        }
      });
      await flush();
      return written;
    },
  };
}
