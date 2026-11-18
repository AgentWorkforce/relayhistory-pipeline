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
 * the scope stays partial with `scope_too_large` and the budget's code. The staged
 * reconcile (`StreamDeps.perPass`, SEARCH_INDEX_STAGED_RECONCILE_ENABLED) meters each
 * pass on its own instead, and caps the records one pass keys in memory.
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
import {
  LEGACY_PROVENANCE_INVALID,
  legacyProvenanceGate,
} from "./legacy-provenance.ts";

type Json = Record<string, unknown>;
const SEP = "\u001f";

/** Read budgets summed over every pass of one scope (per pass with `StreamDeps.perPass`). */
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
  /**
   * MESSAGE_ORIGIN_LABELS_ENABLED=true only: label each row's actor_name / subagent_id exactly
   * as the whole read does (canonical-projection.ts deps.originLabels, message-origin.ts). Absent or false:
   * rows exactly as before.
   */
  originLabels?: boolean;
  /**
   * A wall-clock deadline (epoch ms by `now`, default Date.now): checked before every chunk
   * of every pass, so once crossed the read stops between chunks with StreamBudgetError
   * (STREAM_DEADLINE), as a spent budget does: partial, nothing more read or written.
   * Absent: no deadline, exactly as before.
   */
  deadline?: StreamDeadline;
  /**
   * The staged reconcile only (SEARCH_INDEX_STAGED_RECONCILE_ENABLED): each budget bounds one
   * pass, not the sum of every pass, so a scope is refused when its size exceeds a budget
   * (never four times its size), and a pass that reads more than `keyedRecordsCap` records
   * (each keyed in memory, about 3 KB apiece measured) stops the stream with
   * STREAM_KEYED_RECORDS_CAP. Absent: summed budgets, exactly as before.
   */
  perPass?: StreamPerPass;
}

/** StreamDeps.perPass: the staged reconcile's per-pass metering. */
export interface StreamPerPass {
  /** The most records one pass may read (and so key in memory). */
  keyedRecordsCap: number;
}

/** A wall-clock bound on one streamed scope (SearchIndexDeps.streamDeadline). */
export interface StreamDeadline {
  /** Stop once `now()` reaches this instant (epoch ms). */
  at: number;
  /** Injectable clock; default Date.now. */
  now?: () => number;
}

/** True once `deadline` is crossed (absent: never). */
export function streamDeadlinePassed(deadline: StreamDeadline | undefined): boolean {
  return deadline !== undefined && (deadline.now ?? Date.now)() >= deadline.at;
}

export const STREAM_READ_BYTES_BUDGET = "stream_read_bytes_budget";
export const STREAM_READ_EVENTS_BUDGET = "stream_read_events_budget";
/** The scope's wall-clock deadline passed (StreamDeps.deadline): partial like a spent budget. */
export const STREAM_DEADLINE = "stream_deadline";
export const STREAM_SOURCE_CHANGED = "stream_source_changed";
/** The staged reconcile only: one pass reads more records than StreamPerPass.keyedRecordsCap. */
export const STREAM_KEYED_RECORDS_CAP = "stream_keyed_records_cap";
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
  constructor(
    readonly budget: typeof STREAM_READ_BYTES_BUDGET | typeof STREAM_READ_EVENTS_BUDGET | typeof STREAM_DEADLINE | typeof STREAM_KEYED_RECORDS_CAP,
  ) {
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

/**
 * Meters the chunk reads of every pass against the budgets. `bytes`, `events`, `chunks` and
 * `passes` are always summed over every pass (what the stream reports). Summed budgets by
 * default; with `perPass` each budget is checked against the current pass's reads only
 * (`passBytes`, `passEvents`), and a pass's records against the keyed-records cap.
 */
export class StreamReadMeter implements ChunkReadObserver {
  bytes = 0;
  events = 0;
  chunks = 0;
  passes = 0;
  /** `perPass` only: what the current pass has read. */
  passBytes = 0;
  passEvents = 0;
  /**
   * The most any chunk check has asked for so far: bytes read before it plus its declared
   * size (before it in the same pass, with `perPass`). After pass 1 this bounds what the
   * same chunk checks need in a later pass.
   */
  peak = 0;
  constructor(readonly limits: StreamReadLimits, readonly deadline?: StreamDeadline, readonly perPass?: StreamPerPass) {}
  /** A pass starts: counted, and with `perPass` its own reads start from zero. */
  startPass(): void {
    this.passes += 1;
    if (this.perPass) {
      this.passBytes = 0;
      this.passEvents = 0;
    }
  }
  chunk(entry: Readonly<{ bytes: number; stored_raw_bytes?: number }>): void {
    // Between chunks, before this one is fetched: nothing of it is read or written.
    if (streamDeadlinePassed(this.deadline)) throw new StreamBudgetError(STREAM_DEADLINE);
    const need = (this.perPass ? this.passBytes : this.bytes) + Math.max(entry.bytes, entry.stored_raw_bytes ?? 0);
    if (need > this.limits.maxReadBytes) throw new StreamBudgetError(STREAM_READ_BYTES_BUDGET);
    if (need > this.peak) this.peak = need;
    this.chunks += 1;
  }
  decompressed(bytes: number): void {
    this.bytes += bytes;
    if (this.perPass) {
      this.passBytes += bytes;
      if (this.passBytes > this.limits.maxReadBytes) throw new StreamBudgetError(STREAM_READ_BYTES_BUDGET);
      return;
    }
    if (this.bytes > this.limits.maxReadBytes) throw new StreamBudgetError(STREAM_READ_BYTES_BUDGET);
  }
  record(): void {
    this.events += 1;
    if (this.perPass) {
      this.passEvents += 1;
      if (this.passEvents > this.limits.maxReadEvents) throw new StreamBudgetError(STREAM_READ_EVENTS_BUDGET);
      if (this.passEvents > this.perPass.keyedRecordsCap) throw new StreamBudgetError(STREAM_KEYED_RECORDS_CAP);
      return;
    }
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
  /**
   * The staged reconcile's facts pass (only on a complete read): one more pass handing the whole read's
   * facts input to `emit` in chunks of at most `chunk` inputs and about `maxBytes` of JSON text (one input
   * over that is a chunk alone):
   * the catalog rows, then every post-fold record and every convergence_events row the whole read produces
   * (in and out of the window, shadowed direct rows excluded), in manifest read order. Over all chunks
   * they equal readCanonicalScope's `records` and `projectionRows` as multisets; the whole read lists them
   * in fold/slot order, so a consumer must not depend on the order.
   * Throws as `regenerate` does on a changed or unreadable source.
   */
  facts(
    chunk: number,
    emit: (events: { projectionRows: Array<{ table: string; row: Json }>; records: RelayhistoryStreamRecord[] }) => Promise<void>,
    /** A chunk also closes once its inputs' JSON text reaches this many characters (default FACTS_CHUNK_MAX_BYTES). */
    maxBytes?: number,
  ): Promise<void>;
}

/**
 * The facts pass's byte bound per chunk: the inputs' JSON text. The staged writer sends a chunk as one
 * jsonb parameter, which PostgreSQL refuses above about 256 MB (54000) and which the writer holds
 * several times over (entries plus their JSON text), so 32 MiB keeps a wide margin.
 */
export const FACTS_CHUNK_MAX_BYTES = 32 * 1024 * 1024;

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
  const perPass = deps.perPass;
  const meter = perPass ? new StreamReadMeter(limits, deps.deadline, perPass) : new StreamReadMeter(limits, deps.deadline);
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
    facts: async () => {
      throw new PipelineError("stream_incomplete", "an incomplete streamed read has no facts input", false);
    },
    ...fields,
  });

  // The whole read refuses before reading what it cannot afford; so does this:
  // every pass may read every chunk once (with `perPass`, each pass on its own budget).
  const declaredBytes = admitted.reduce(
    (n, { manifest }) => n + manifest.raw.chunks.reduce((b, c) => b + Math.max(c.bytes, c.stored_raw_bytes ?? 0), 0),
    0,
  );
  const declaredEvents = admitted.reduce(
    (n, { manifest }) => n + manifest.raw.chunks.reduce((b, c) => b + (c.records ?? 0), 0),
    0,
  );
  const overBudget = perPass
    ? (bytes: number, events: number) =>
        bytes > limits.maxReadBytes
          ? STREAM_READ_BYTES_BUDGET
          : events > limits.maxReadEvents
            ? STREAM_READ_EVENTS_BUDGET
            : events > perPass.keyedRecordsCap
              ? STREAM_KEYED_RECORDS_CAP
              : null
    : (bytes: number, events: number) =>
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
    meter.startPass();
    for (let part = 0; part < admitted.length; part++) {
      const { manifest, key } = admitted[part]!;
      for await (const record of streamManifestRecords(
        manifest,
        deps.store,
        parseSessionKey(key)?.prefix ?? prefix,
        deps.decompress,
        Number.POSITIVE_INFINITY,
        deps.readerMaxRuleset,
        1, // fetchConcurrency: sequential, so the meter's chunk check runs before each fetch
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
  let legacyFoldFailure: string | null = null;
  const legacyGate = legacyProvenanceGate(
    scope,
    admitted.map(({ manifest }, part) => ({ manifest, userId: userIds[part]! })),
  );
  try {
    const seats = new Map<string, Seat>();
    // Keep legacy seats separate until the provenance gate has observed the
    // whole pass. In particular, do not let conflicts between legacy copies
    // stop the stream before the gate can reject their manifests: the whole
    // read drops invalid legacy evidence before it folds those records.
    const legacySeats = new Map<string, Seat>();
    await pass(async () => {
      for await (const { part, record } of read()) {
        legacyGate.observe(part, record);
        const key = relayhistoryRecordKey(record);
        const legacy = isLegacyRelayhistoryRecord(record);
        const target = legacy ? legacySeats : seats;
        const previous = target.get(key);
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
          if (!legacy) throw new StreamStop(codeOf(error, "fold_failed"));
          legacyFoldFailure ??= codeOf(error, "fold_failed");
          chosen = previous ?? seat;
        }
        // The owning manifest is the first one that carried the record, whichever copy wins.
        target.set(key, previous && chosen.firstPart !== previous.firstPart ? { ...chosen, firstPart: previous.firstPart } : chosen);
        if (legacy) {
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
    if (legacyGate.invalid) {
      // The whole read records provenance before fold/owner errors.
      reasons.unshift(LEGACY_PROVENANCE_INVALID);
      legacyFirst.clear();
    } else {
      // One valid legacy manifest with internally conflicting copies fails the
      // all-record fold and its per-manifest fallback in the whole reader.
      if (legacyFoldFailure)
        return result({
          reasons: [legacyFoldFailure, "manifest_fold_failed", ...reasons],
          events: new DirectoryEventsBuilder().finish(),
        });
      for (const [key, candidate] of legacySeats) {
        const previous = seats.get(key);
        let chosen: Seat;
        try {
          chosen = chooseRelayhistoryRevision(previous, candidate, (a, b) => a.digest === b.digest);
        } catch (error) {
          throw new StreamStop(codeOf(error, "fold_failed"));
        }
        seats.set(key, previous && chosen.firstPart !== previous.firstPart ? { ...chosen, firstPart: previous.firstPart } : chosen);
      }
    }
    // The declared sizes may undercount (records are optional): check what pass 1 read. Each
    // later pass checks every chunk's declared size against what was read before it, so the
    // last pass needs three passes' bytes plus pass 1's peak: a chunk declared larger than it
    // reads must not pass here and then stop pass 4 after some rows were written.
    // With `perPass` every later pass reads what pass 1 read, checked chunk by chunk against
    // the same per-pass budget pass 1 already fit (its peak included), so pass 1's own reads
    // decide it.
    const actual = perPass
      ? Math.max(meter.passBytes, meter.peak) > limits.maxReadBytes
        ? STREAM_READ_BYTES_BUDGET
        : overBudget(0, meter.passEvents)
      : meter.bytes * (STREAM_PASSES - 1) + Math.max(meter.bytes, meter.peak) > limits.maxReadBytes
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
    if (error instanceof StreamStop) {
      // The whole read evaluates legacy provenance before folding. Preserve
      // that reason order even when another pass-1 failure stops the stream.
      const ordered = legacyGate.invalid
        ? [LEGACY_PROVENANCE_INVALID, error.code, ...reasons.filter((reason) => reason !== LEGACY_PROVENANCE_INVALID)]
        : [error.code, ...reasons];
      return result({ reasons: [...new Set(ordered)] });
    }
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
  async function* projected(onRecord?: (record: RelayhistoryStreamRecord) => Promise<void>): AsyncGenerator<Projected> {
    let added = 0;
    for await (const { pos, rank, record } of kept()) {
      if (onRecord) await onRecord(record);
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
        const indexRow = toIndexRow(projectedToNeonRow(canonicalRow, scope, ctx.userId, deps.originLabels === true), version);
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
  const unique = [...new Set(reasons)];
  // Invalid legacy evidence is dropped while valid live canonical rows remain
  // complete, matching readCanonicalScope. The reason is retained for the
  // operator warn log and read-path response marker.
  const complete = unique.every((reason) => reason === LEGACY_PROVENANCE_INVALID);
  if (!complete) return result({ reasons: unique, catalog: catalogOut, events });

  return result({
    complete: true,
    reasons: unique,
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
    async facts(chunk, emit, maxBytes = FACTS_CHUNK_MAX_BYTES) {
      let projectionRows: Array<{ table: string; row: Json }> = [];
      let records: RelayhistoryStreamRecord[] = [];
      // A chunk closes at `chunk` inputs or at `maxBytes` of their JSON text, whichever comes first, so
      // its one staged call stays far below the database's jsonb limit (a record alone can be MiBs). A
      // single input over `maxBytes` is a chunk of its own.
      let bytes = 0;
      const take = () => {
        const out = { projectionRows, records };
        projectionRows = [];
        records = [];
        bytes = 0;
        return out;
      };
      const full = () => projectionRows.length + records.length >= chunk || bytes >= maxBytes;
      const size = (item: unknown) => JSON.stringify(item)?.length ?? 0;
      // The whole read's catalog rows come first (readCanonicalScope: catalogRows, then convergence_events),
      // counted and sized like every other input; emitted raw, before the pass, as the last chunk is.
      for (const r of projErr.has(D) ? [] : catalogRows) {
        projectionRows.push({ table: r.table, row: r.row as Json });
        bytes += size(r.row);
        if (full()) await emit(take());
      }
      // Inside the pass: a hook error is the caller's, not the source's (pass() unwraps it).
      const flush = async () => {
        if (!full()) return;
        await emit(take()).catch((error: unknown) => { throw new CallerFailed(error); });
      };
      try {
        await pass(async () => {
          const onRecord = async (record: RelayhistoryStreamRecord) => {
            records.push(record);
            bytes += size(record);
            await flush();
          };
          for await (const p of projected(onRecord)) {
            if ("mapError" in p) throw new StreamStop(STREAM_SOURCE_CHANGED);
            if (p.shadowed) continue;
            projectionRows.push({ table: "convergence_events", row: p.row });
            bytes += size(p.row);
            await flush();
          }
        });
      } catch (error) {
        if (error instanceof StreamStop) throw new PipelineError(error.code, "the streamed source changed or could not be read", false);
        throw error;
      }
      // The last chunk outside the pass: emitted raw, as regenerate's last write, so a hook error
      // (a restart, a lost lease) reaches the caller as itself.
      if (projectionRows.length || records.length) await emit(take());
    },
  });
}
