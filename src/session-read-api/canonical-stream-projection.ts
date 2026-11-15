/**
 * The streaming reconcile (issue 610 Phase 2): the canonical read of one session
 * too large to read whole (`readCanonicalScope` returned `partial` with only
 * `scope_too_large`), done in passes over the same admitted manifests instead of
 * holding the session's records.
 *
 *   1. fold    payload-free seat summaries: the winning revision of every record
 *              (`chooseRelayhistoryRevision`, `relayhistoryNewestAcrossOrigins`),
 *              each record's owning manifest, and the event-owner map;
 *   2. facts   the settlement facts, codex tool calls, catalog winners and the
 *              projected event keys;
 *   3. verify  every event projected, settled and mapped to its index row, the
 *              directory metadata built row by row, and the in-window index
 *              identities (hashed key, session, time, fingerprint) kept;
 *   4. write   only when the index lacks rows: the rows it lacks, regenerated
 *              (`regenerate`) and checked against pass 3.
 *
 * Every pass after the first keeps exactly the records the fold chose, by
 * revision, `received_at` and content digest, so the result is the one
 * `readCanonicalScope` would compute: the same rows, the same directory metadata
 * (rows tie-broken in the whole read's order: group, then (ts, event_id), then
 * fold position) and the same catalog. Memory is one chunk plus a few keys per
 * record. A read or fold error stops the stream as incomplete with that one
 * code, where the whole read may go on and list more codes; a source that
 * changes between passes stops it with `stream_source_changed`.
 *
 * Reads are metered across all passes (`StreamReadLimits`); past either budget
 * the scope stays partial with `scope_too_large` and the budget's code.
 */
import { createHash } from "node:crypto";
import {
  CONVERGENCE_EVENT_KIND,
  ProjectionSettlement,
  RELAYHISTORY_ACTIVITY_KINDS,
  deliveryMachine,
  relayhistoryActivityEvent,
  relayhistoryCatalogKey,
  relayhistoryCatalogRows,
  relayhistoryDirectEvent,
  type ProjectionContext,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  chooseRelayhistoryRevision,
  compareRelayhistoryOrder,
  isLegacyRelayhistoryRecord,
  relayhistoryEventTime,
  relayhistoryNewestAcrossOrigins,
  relayhistoryRecordKey,
  relayhistoryRevisionContent,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { PipelineError } from "../../packages/pipeline/src/lib/pipeline/errors.ts";
import { parseSessionKey, type ManifestV1, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { toIndexRow, tsMillis, type IndexRow } from "../session-pipeline/vendor/search-index/rows.ts";
import {
  directoryCatalogFromProjection,
  type CatalogProjectionRow,
  type DirectoryCatalog,
} from "../hoopdb/session-directory-catalog.ts";
import {
  DirectoryEventsBuilder,
  type DirectoryEventInput,
  type DirectoryEvents,
} from "../hoopdb/session-directory-events.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
import {
  manifestUserId,
  projectedToNeonRow,
  streamManifestRecords,
  type ChunkReadObserver,
  type SessionScope,
} from "./canonical-projection.ts";

type Json = Record<string, unknown>;
const SEP = "\u001f";

/** Read budgets summed over every pass of one scope. */
export interface StreamReadLimits {
  maxReadBytes: number;
  maxReadEvents: number;
}

export interface StreamDeps {
  readerMaxRuleset?: number;
  store: ObjectStore;
  prefixFor: (orgId: string) => string;
  decompress: (stored: Uint8Array, maxBytes: number) => Uint8Array;
  /**
   * Run between records at most every `everyMs` (by `now`, default Date.now): the caller's
   * transaction stays busy while passes 1 to 3 read without querying it, so an
   * idle_in_transaction_session_timeout cannot end it. A failure ends the read with that
   * error, unchanged.
   */
  keepalive?: { everyMs: number; ping: () => Promise<void>; now?: () => number };
}

export const STREAM_READ_BYTES_BUDGET = "stream_read_bytes_budget";
export const STREAM_READ_EVENTS_BUDGET = "stream_read_events_budget";
export const STREAM_SOURCE_CHANGED = "stream_source_changed";
/**
 * Two surviving events share an index key (other than an activity event
 * replacing a direct line): the whole read keeps the later one in its list, which
 * a streamed read cannot tell without another pass, so it writes nothing.
 */
export const STREAM_DUPLICATE_EVENT = "stream_duplicate_event";
/** The most passes a scope is read in: the budget must cover this many reads. */
export const STREAM_PASSES = 4;

/** A read budget ran out: the scope stays `scope_too_large`, `budget` says which. */
export class StreamBudgetError extends PipelineError {
  constructor(readonly budget: typeof STREAM_READ_BYTES_BUDGET | typeof STREAM_READ_EVENTS_BUDGET) {
    super("scope_too_large", `the streaming read budget ran out (${budget})`, false);
  }
}

/**
 * A callback of the caller (the keepalive ping, pass 4's write) failed: its error ends the
 * read as it is, never as a read code.
 */
class CallerFailed extends Error {
  constructor(readonly failure: unknown) {
    super("stream caller callback failed");
  }
}

/** The streamed read stopped: nothing may be written from it. */
class StreamStop extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

/** The search key of an index identity, hashed so a pass can hold one per row. */
export function streamIndexKey(i: { machineId: string; source: string; kind: string; eventId: string }): string {
  return createHash("sha256")
    .update([i.machineId, i.source, i.kind, i.eventId].join(SEP))
    .digest("base64")
    .slice(0, 22);
}

/** Meters the chunk reads of every pass against the budgets. */
export class StreamReadMeter implements ChunkReadObserver {
  bytes = 0;
  events = 0;
  chunks = 0;
  passes = 0;
  /**
   * The most any chunk check has asked for so far: bytes read before it plus its declared
   * size. After pass 1 this bounds what the same chunk checks need in a later pass.
   */
  peak = 0;
  constructor(readonly limits: StreamReadLimits) {}
  chunk(entry: Readonly<{ bytes: number; stored_raw_bytes?: number }>): void {
    const need = this.bytes + Math.max(entry.bytes, entry.stored_raw_bytes ?? 0);
    if (need > this.limits.maxReadBytes) throw new StreamBudgetError(STREAM_READ_BYTES_BUDGET);
    if (need > this.peak) this.peak = need;
    this.chunks += 1;
  }
  decompressed(bytes: number): void {
    this.bytes += bytes;
    if (this.bytes > this.limits.maxReadBytes) throw new StreamBudgetError(STREAM_READ_BYTES_BUDGET);
  }
  record(): void {
    this.events += 1;
    if (this.events > this.limits.maxReadEvents) throw new StreamBudgetError(STREAM_READ_EVENTS_BUDGET);
  }
}

/** One in-window index identity the verify pass produced. */
export interface StreamWanted {
  /** Index into `StreamedScope.owners`: the row's session and workspace. */
  owner: number;
  tsMs: number;
  fingerprint: string;
}

export interface StreamedScope {
  complete: boolean;
  /** A read budget stopped the stream. */
  partial: boolean;
  reasons: string[];
  manifests: number;
  catalog: DirectoryCatalog;
  directorySessionId: string | null;
  events: DirectoryEvents | null;
  sources: DirectoryEventInput["sources"];
  /** Only on a complete read: the in-window rows by `streamIndexKey`. */
  wanted: Map<string, StreamWanted>;
  owners: Array<{ sessionId: string; workspaceId: string }>;
  /** Kinds of the in-window rows. */
  kinds: Set<string>;
  inWindow: number;
  meter: StreamReadMeter;
  /**
   * Pass 4: the index rows whose keys are given, regenerated and checked against
   * pass 3, handed over in batches of at most `batch`. Throws StreamBudgetError,
   * or a PipelineError (`stream_source_changed`, or a read code) on a changed or
   * unreadable source.
   */
  regenerate(keys: ReadonlySet<string>, batch: number, write: (rows: IndexRow[]) => Promise<void>): Promise<void>;
}

/** The fold's summary of one record copy: no payload. */
interface Seat {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  received_at: string;
  accepted_seq?: string;
  digest: string;
  live: boolean;
  time: number;
  /** The first manifest (admitted index) that carried this seat. */
  firstPart: number;
}

const digestOf = (record: RelayhistoryStreamRecord) =>
  createHash("sha256").update(relayhistoryRevisionContent(record)).digest("base64");

const codeOf = (error: unknown, fallback: string) => (error instanceof PipelineError ? error.code : fallback);

/**
 * Reads one partial scope in passes. `admitted` is `ScopeRead.admitted`;
 * `revision` and the scope's other read state come from that same read.
 */
export async function streamCanonicalScope(
  scope: SessionScope,
  admitted: ReadonlyArray<{ manifest: ManifestV1; key: string }>,
  deps: StreamDeps,
  limits: StreamReadLimits,
  version: number,
  fromMs: number,
): Promise<StreamedScope> {
  const meter = new StreamReadMeter(limits);
  const prefix = deps.prefixFor(scope.orgId);
  const userIds = admitted.map(({ manifest }) => manifestUserId(manifest));
  const identities = admitted.map(({ manifest }) => canonicalManifestIdentity(manifest));
  const directoryIds = new Set(identities.map((i) => i.sessionId));
  const sources = admitted.map(({ manifest, key }) => ({ manifestKey: key, revision: String(manifest.revision) }));
  const result = (fields: Partial<StreamedScope> & { reasons: string[] }): StreamedScope => ({
    complete: false,
    partial: false,
    manifests: admitted.length,
    catalog: directoryCatalogFromProjection([]),
    directorySessionId: directoryIds.size === 1 ? [...directoryIds][0]! : null,
    events: null,
    sources,
    wanted: new Map(),
    owners: [],
    kinds: new Set(),
    inWindow: 0,
    meter,
    regenerate: async () => {
      throw new PipelineError("stream_incomplete", "an incomplete streamed read has nothing to write", false);
    },
    ...fields,
  });

  // The whole read refuses before reading what it cannot afford; so does this:
  // every pass may read every chunk once.
  const declaredBytes = admitted.reduce(
    (n, { manifest }) => n + manifest.raw.chunks.reduce((b, c) => b + Math.max(c.bytes, c.stored_raw_bytes ?? 0), 0),
    0,
  );
  const declaredEvents = admitted.reduce(
    (n, { manifest }) => n + manifest.raw.chunks.reduce((b, c) => b + (c.records ?? 0), 0),
    0,
  );
  const overBudget = (bytes: number, events: number) =>
    bytes * STREAM_PASSES > limits.maxReadBytes
      ? STREAM_READ_BYTES_BUDGET
      : events * STREAM_PASSES > limits.maxReadEvents
        ? STREAM_READ_EVENTS_BUDGET
        : null;
  const budgetStop = (budget: string) =>
    result({ partial: true, reasons: ["scope_too_large", budget] });
  const upFront = overBudget(declaredBytes, declaredEvents);
  if (upFront) return budgetStop(upFront);

  const keep = deps.keepalive;
  const clock = keep?.now ?? Date.now;
  let pinged = Number.NEGATIVE_INFINITY; // the first record pings: the read before this one may have been long
  async function keepalive(): Promise<void> {
    if (!keep || clock() - pinged < keep.everyMs) return;
    try {
      await keep.ping();
    } catch (error) {
      throw new CallerFailed(error);
    }
    pinged = clock();
  }

  async function* read(): AsyncGenerator<{ part: number; record: RelayhistoryStreamRecord }> {
    meter.passes += 1;
    for (let part = 0; part < admitted.length; part++) {
      const { manifest, key } = admitted[part]!;
      for await (const record of streamManifestRecords(
        manifest,
        deps.store,
        parseSessionKey(key)?.prefix ?? prefix,
        deps.decompress,
        Number.POSITIVE_INFINITY,
        deps.readerMaxRuleset,
        meter,
      )) {
        meter.record();
        await keepalive();
        yield { part, record };
      }
    }
  }
  /** Runs one pass; maps what stops it to a StreamStop, a budget, or a retry. */
  async function pass<T>(run: () => Promise<T>): Promise<T> {
    try {
      return await run();
    } catch (error) {
      if (error instanceof CallerFailed) throw error.failure;
      if (error instanceof StreamBudgetError || error instanceof StreamStop) throw error;
      if (error instanceof PipelineError && error.retryable) throw error;
      throw new StreamStop(codeOf(error, "manifest_unreadable"));
    }
  }

  // Pass 1: fold.
  const reasons: string[] = [];
  let winners: Seat[];
  const ownerByEvent = new Map<string, number>();
  const legacyFirst = new Map<string, number>();
  try {
    const seats = new Map<string, Seat>();
    await pass(async () => {
      for await (const { part, record } of read()) {
        const key = relayhistoryRecordKey(record);
        const previous = seats.get(key);
        const seat: Seat = {
          kind: record.kind,
          origin_id: record.origin_id,
          record_id: record.record_id,
          revision: record.revision,
          received_at: record.received_at,
          ...(record.accepted_seq === undefined ? {} : { accepted_seq: record.accepted_seq }),
          digest: digestOf(record),
          live: record.operation === "upsert" && record.payload !== null,
          time: relayhistoryEventTime(record),
          firstPart: part,
        };
        let chosen: Seat;
        try {
          chosen = chooseRelayhistoryRevision(previous, seat, (a, b) => a.digest === b.digest);
        } catch (error) {
          throw new StreamStop(codeOf(error, "fold_failed"));
        }
        // The owning manifest is the first one that carried the record, whichever copy wins.
        seats.set(key, previous && chosen.firstPart !== previous.firstPart ? { ...chosen, firstPart: previous.firstPart } : chosen);
        if (isLegacyRelayhistoryRecord(record)) {
          const at = `${record.origin_id}${SEP}${record.record_id}`;
          if (!legacyFirst.has(at)) legacyFirst.set(at, part);
          continue;
        }
        if (record.kind === CONVERGENCE_EVENT_KIND) continue;
        const identity = JSON.stringify([deliveryMachine(scope.workspaceId, record.origin_id), record.record_id]);
        const prior = ownerByEvent.get(identity);
        const priorUser = prior === undefined ? undefined : userIds[prior];
        if (priorUser && userIds[part] && priorUser !== userIds[part]) {
          if (!reasons.includes("event_owner_conflict")) reasons.push("event_owner_conflict");
        } else if (userIds[part] || prior === undefined) ownerByEvent.set(identity, part);
      }
    });
    // The declared sizes may undercount (records are optional): check what pass 1 read. Each
    // later pass checks every chunk's declared size against what was read before it, so the
    // last pass needs three passes' bytes plus pass 1's peak: a chunk declared larger than it
    // reads must not pass here and then stop pass 4 after some rows were written.
    const actual =
      meter.bytes * (STREAM_PASSES - 1) + Math.max(meter.bytes, meter.peak) > limits.maxReadBytes
        ? STREAM_READ_BYTES_BUDGET
        : overBudget(0, meter.events);
    if (actual) return budgetStop(actual);
    try {
      winners = relayhistoryNewestAcrossOrigins(seats.values())
        .filter((seat) => seat.live)
        .sort((a, b) => compareRelayhistoryOrder(a.time, a, b.time, b));
    } catch (error) {
      throw new StreamStop(codeOf(error, "fold_failed"));
    }
  } catch (error) {
    if (error instanceof StreamBudgetError) return budgetStop(error.budget);
    if (error instanceof StreamStop) return result({ reasons: [...new Set([error.code, ...reasons])] });
    throw error;
  }

  const W = winners.length;
  const winnerPos = new Map<string, number>();
  winners.forEach((seat, pos) => winnerPos.set(relayhistoryRecordKey(seat), pos));
  // Groups, as the whole read forms them: one per manifest that owns direct
  // convergence lines (ranked by first appearance in fold order), then one
  // activity group projected with the first admitted manifest.
  const rankOfPart = new Map<number, number>();
  const rankOf = new Int32Array(W);
  const directPos: number[] = [];
  for (let pos = 0; pos < W; pos++) {
    const seat = winners[pos]!;
    if (seat.kind !== CONVERGENCE_EVENT_KIND) continue;
    directPos.push(pos);
    const owner = Math.min(seat.firstPart, legacyFirst.get(`${seat.origin_id}${SEP}${seat.record_id}`) ?? Infinity);
    if (!rankOfPart.has(owner)) rankOfPart.set(owner, rankOfPart.size);
    rankOf[pos] = rankOfPart.get(owner)!;
  }
  legacyFirst.clear();
  const D = rankOfPart.size;
  for (let pos = 0; pos < W; pos++) if (winners[pos]!.kind !== CONVERGENCE_EVENT_KIND) rankOf[pos] = D;
  const partOfRank = [...rankOfPart.keys()];
  partOfRank.push(0);
  const contexts: ProjectionContext[] = partOfRank.map((part) => ({
    workspaceId: scope.workspaceId,
    source: scope.harness,
    sessionId: identities[part]!.sessionId,
    machineId: identities[part]!.machineId,
    userId: userIds[part]!,
  }));

  /** Pass 2+: the fold's chosen records, in read order, with their fold position and group. */
  async function* kept(): AsyncGenerator<{ pos: number; rank: number; record: RelayhistoryStreamRecord }> {
    const seen = new Uint8Array(W);
    let count = 0;
    for await (const { record } of read()) {
      const pos = winnerPos.get(relayhistoryRecordKey(record));
      if (pos === undefined) continue;
      const seat = winners[pos]!;
      if (seen[pos] || record.revision !== seat.revision || record.received_at !== seat.received_at) continue;
      if (digestOf(record) !== seat.digest) throw new StreamStop(STREAM_SOURCE_CHANGED);
      seen[pos] = 1;
      count += 1;
      const { accepted_seq: _seq, ...rest } = record;
      yield { pos, rank: rankOf[pos]!, record: seat.accepted_seq === undefined ? rest : { ...rest, accepted_seq: seat.accepted_seq } };
    }
    if (count !== W) throw new StreamStop(STREAM_SOURCE_CHANGED);
  }

  // A group's projection error, as the whole read reports it: the first one in
  // its projection order (records in fold order, then settling, then catalog rows).
  const projErr = new Map<number, { at: number; code: string }>();
  const failGroup = (rank: number, at: number, error: unknown) => {
    const prior = projErr.get(rank);
    if (!prior || at < prior.at) projErr.set(rank, { at, code: codeOf(error, "projection_failed") });
  };
  const rowKey = (row: Json) =>
    [String(row.machine_id), String(row.source), String(row.kind), String(row.event_id)].join(SEP);
  const codexKey = (record: RelayhistoryStreamRecord): string | null => {
    const p = record.payload;
    return p && p.source === "codex" && typeof p.session_id === "string" && p.session_id.trim() &&
      typeof p.message_id === "string" && p.message_id.trim()
      ? JSON.stringify([record.origin_id, p.session_id, p.message_id])
      : null;
  };

  // Pass 2: facts.
  const settlement = new ProjectionSettlement(scope.harness);
  /** Direct rows' keys and their list slots: an activity row with the same key takes the slot. */
  const directSlot = new Map<string, number>();
  const activityKeys = new Set<string>();
  const calls = new Map<string, string | null>();
  /** Codex tool_use rows by (delivery machine, record id) -> their call key. */
  const toolUses = new Map<string, string>();
  let duplicate = false;
  type CatalogEntry = { best: RelayhistoryStreamRecord; bestPos: number; minPos: number; secondPos: number; missingPos: number; count: number };
  const catalog = new Map<string, CatalogEntry>();
  let catalogRows: CatalogProjectionRow[] = [];
  const activityCtx = contexts[D]!;
  try {
    await pass(async () => {
      for await (const { pos, rank, record } of kept()) {
        if (record.kind === CONVERGENCE_EVENT_KIND) {
          try {
            const row = relayhistoryDirectEvent(record, contexts[rank]!);
            const key = rowKey(row);
            if (directSlot.has(key)) duplicate = true;
            directSlot.set(key, rank * W + pos);
          } catch (error) {
            failGroup(rank, pos, error);
          }
        } else if (RELAYHISTORY_ACTIVITY_KINDS.has(record.kind)) {
          try {
            const event = relayhistoryActivityEvent(record, activityCtx);
            if (event) {
              // Ordinals count adds in read order, as pass 3 counts them. A request's
              // carrier is its lowest delivery record id, and surviving activity
              // records have distinct record ids (contenders fold to one), so the
              // carrier does not depend on the order of adds.
              settlement.add(event);
              activityKeys.add(rowKey(event));
            }
          } catch (error) {
            failGroup(D, pos, error);
          }
          if (scope.harness === "codex" && record.kind === "tool_call") {
            const k = codexKey(record);
            if (k && typeof record.payload?.name === "string" && record.payload.name.trim())
              calls.set(k, calls.has(k) ? null : record.payload.name);
          }
          if (
            scope.harness === "codex" && record.kind === "session_event" &&
            record.payload?.kind === "tool_use" && record.payload.role === "assistant"
          ) {
            const k = codexKey(record);
            if (k) toolUses.set(JSON.stringify([deliveryMachine(scope.workspaceId, record.origin_id), record.record_id]), k);
          }
        } else {
          let key: string | null;
          try {
            key = relayhistoryCatalogKey(record);
          } catch (error) {
            failGroup(D, pos, error);
            continue;
          }
          if (key === null) continue;
          const seq = record.accepted_seq;
          const held = catalog.get(key);
          if (!held) {
            catalog.set(key, { best: record, bestPos: pos, minPos: pos, secondPos: Infinity, missingPos: seq === undefined ? pos : Infinity, count: 1 });
            continue;
          }
          held.count += 1;
          if (seq === undefined) held.missingPos = Math.min(held.missingPos, pos);
          if (pos < held.minPos) {
            held.secondPos = held.minPos;
            held.minPos = pos;
          } else if (pos < held.secondPos) held.secondPos = pos;
          // lastAccepted in fold order: the higher seat wins, a tie keeps the earlier.
          if (seq !== undefined && held.best.accepted_seq !== undefined) {
            const a = BigInt(seq), b = BigInt(held.best.accepted_seq);
            if (a > b || (a === b && pos < held.bestPos)) {
              held.best = record;
              held.bestPos = pos;
            }
          } else if (seq !== undefined && held.best.accepted_seq === undefined) {
            held.best = record;
            held.bestPos = pos;
          }
        }
      }
    });
  } catch (error) {
    if (error instanceof StreamBudgetError) return budgetStop(error.budget);
    if (error instanceof StreamStop) return result({ reasons: [...new Set([error.code, ...reasons])] });
    throw error;
  }
  const captures = new Map<string, string>();
  for (const [at, k] of toolUses) {
    const name = calls.get(k);
    if (name) captures.set(at, name);
  }
  toolUses.clear();
  calls.clear();
  // The fold compares each catalog record with the one held: with a seat missing,
  // the first comparison that reaches it fails.
  for (const held of catalog.values())
    if (held.count > 1 && held.missingPos !== Infinity)
      failGroup(D, Math.max(held.secondPos, held.missingPos), new PipelineError("relayhistory_acceptance_missing", "contending RelayHistory records have no acceptance order", false));
  if (!projErr.has(D)) {
    try {
      catalogRows = [...catalog.values()]
        .sort((a, b) => a.minPos - b.minPos)
        .flatMap((held) => relayhistoryCatalogRows(held.best, activityCtx));
    } catch (error) {
      failGroup(D, 2 * W, error);
    }
  }
  catalog.clear();

  /**
   * Passes 3 and 4: every surviving event, projected, settled and mapped as
   * `projectEventRows` maps it. `slot` is the row's place in the whole read's
   * list; `shadowed` marks a direct row an activity row replaces.
   */
  type Projected =
    | { rank: number; key: string; slot: number; shadowed: boolean; row: Json; indexRow: IndexRow }
    | { rank: number; mapError: unknown };
  // Only a loop failure (before settling) leaves the activity group nothing to settle.
  const activityLoopFailed = () => (projErr.get(D)?.at ?? Infinity) < W;
  async function* projected(): AsyncGenerator<Projected> {
    let added = 0;
    for await (const { pos, rank, record } of kept()) {
      let row: Json;
      let slot: number;
      let shadowed = false;
      if (record.kind === CONVERGENCE_EVENT_KIND) {
        if (projErr.has(rank)) continue;
        row = relayhistoryDirectEvent(record, contexts[rank]!);
        slot = rank * W + pos;
        shadowed = activityKeys.has(rowKey(row));
      } else if (RELAYHISTORY_ACTIVITY_KINDS.has(record.kind)) {
        if (activityLoopFailed()) continue;
        const event = relayhistoryActivityEvent(record, activityCtx);
        if (!event) continue;
        const ordinal = added++;
        try {
          settlement.settle(event, ordinal);
        } catch (error) {
          failGroup(D, W + pos, error);
          continue;
        }
        row = event;
        slot = directSlot.get(rowKey(row)) ?? D * W + pos;
      } else continue;
      const ctx = contexts[rank]!;
      try {
        const owner = ownerByEvent.get(JSON.stringify([row.machine_id, row.delivery_record_id]));
        const found = owner === undefined ? undefined : { userId: userIds[owner]!, sessionId: identities[owner]!.sessionId };
        // The whole read takes the captures from each group's own records: only the
        // activity group holds tool calls, so a direct group's rows never take one.
        const toolName = rank === D && row.kind === "session_event" && typeof row.delivery_record_id === "string"
          ? captures.get(JSON.stringify([row.machine_id, row.delivery_record_id])) : undefined;
        const canonicalRow = {
          ...row,
          ...(toolName ? { type: "tool_use", tool_name: toolName } : {}),
          session_id: row.delivery_record_id ? (found?.sessionId ?? row.session_id) : row.session_id,
          user_id: row.user_id ?? found?.userId ?? ctx.userId,
        };
        const indexRow = toIndexRow(projectedToNeonRow(canonicalRow, scope, ctx.userId), version);
        yield {
          rank,
          key: rowKey({ machine_id: indexRow.machine_id, source: indexRow.source, kind: indexRow.kind, event_id: indexRow.event_id }),
          slot,
          shadowed,
          row: canonicalRow,
          indexRow,
        };
      } catch (mapError) {
        yield { rank, mapError };
      }
    }
  }

  // Pass 3: verify.
  const builder = new DirectoryEventsBuilder();
  let builderFailed = false;
  const mapErrors = new Map<number, unknown>();
  const wanted = new Map<string, StreamWanted>();
  const ownerIndex = new Map<string, number>();
  const owners: StreamedScope["owners"] = [];
  const kinds = new Set<string>();
  const seenKeys = new Set<string>();
  let inWindow = 0;
  try {
    await pass(async () => {
      for await (const p of projected()) {
        if ("mapError" in p) {
          if (!mapErrors.has(p.rank)) mapErrors.set(p.rank, p.mapError);
          continue;
        }
        if (p.shadowed) continue;
        const key = streamIndexKey({ machineId: p.indexRow.machine_id, source: p.indexRow.source, kind: p.indexRow.kind, eventId: p.indexRow.event_id });
        if (seenKeys.has(key)) duplicate = true;
        seenKeys.add(key);
        if (!builderFailed)
          try {
            builder.add({ ...p.row, _index_fingerprint: p.indexRow.fingerprint }, p.slot);
          } catch {
            builderFailed = true;
          }
        const tsMs = tsMillis(`${p.indexRow.ts.replace(" ", "T")}Z`);
        if (tsMs < fromMs) continue;
        inWindow += 1;
        kinds.add(p.indexRow.kind);
        const ownerKey = JSON.stringify([p.indexRow.session_id, p.indexRow.workspace_id]);
        let owner = ownerIndex.get(ownerKey);
        if (owner === undefined) {
          owner = owners.length;
          ownerIndex.set(ownerKey, owner);
          owners.push({ sessionId: p.indexRow.session_id, workspaceId: p.indexRow.workspace_id });
        }
        wanted.set(key, {
          owner,
          tsMs,
          fingerprint: p.indexRow.fingerprint,
        });
      }
    });
  } catch (error) {
    if (error instanceof StreamBudgetError) return budgetStop(error.budget);
    if (error instanceof StreamStop) return result({ reasons: [...new Set([error.code, ...reasons])] });
    throw error;
  }
  seenKeys.clear();
  // The whole read throws the first mapping failure of the first group it maps.
  for (let rank = 0; rank <= D; rank++)
    if (mapErrors.has(rank) && !projErr.has(rank)) throw mapErrors.get(rank);

  for (let rank = 0; rank <= D; rank++) {
    const failed = projErr.get(rank);
    if (failed) reasons.push(failed.code);
  }
  if (directoryIds.size > 1) reasons.push("canonical_session_mismatch");
  let events: DirectoryEvents | null = null;
  if (duplicate) reasons.push(STREAM_DUPLICATE_EVENT);
  else if (builderFailed) reasons.push("directory_event_metadata_invalid");
  else
    try {
      events = builder.finish();
    } catch {
      reasons.push("directory_event_metadata_invalid");
    }
  const catalogOut = directoryCatalogFromProjection(projErr.has(D) ? [] : catalogRows);
  const complete = reasons.length === 0;
  const unique = [...new Set(reasons)];
  if (!complete) return result({ reasons: unique, catalog: catalogOut, events });

  return result({
    complete: true,
    reasons: [],
    catalog: catalogOut,
    events,
    wanted,
    owners,
    kinds,
    inWindow,
    async regenerate(keys, batch, write) {
      if (keys.size === 0) return;
      let rows: IndexRow[] = [];
      let found = 0;
      try {
        await pass(async () => {
          for await (const p of projected()) {
            if ("mapError" in p) throw new StreamStop(STREAM_SOURCE_CHANGED);
            if (p.shadowed) continue;
            const key = streamIndexKey({ machineId: p.indexRow.machine_id, source: p.indexRow.source, kind: p.indexRow.kind, eventId: p.indexRow.event_id });
            if (!keys.has(key)) continue;
            if (wanted.get(key)?.fingerprint !== p.indexRow.fingerprint) throw new StreamStop(STREAM_SOURCE_CHANGED);
            found += 1;
            rows.push(p.indexRow);
            if (rows.length >= batch) {
              const out = rows;
              rows = [];
              // A store error is the caller's, not the source's: it must not read as manifest_unreadable.
              await write(out).catch((error: unknown) => { throw new CallerFailed(error); });
            }
          }
        });
      } catch (error) {
        if (error instanceof StreamStop) throw new PipelineError(error.code, "the streamed source changed or could not be read", false);
        throw error;
      }
      if (found !== keys.size) throw new PipelineError(STREAM_SOURCE_CHANGED, "the streamed source changed", false);
      if (rows.length) await write(rows);
    },
  });
}
