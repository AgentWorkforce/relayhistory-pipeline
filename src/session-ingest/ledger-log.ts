/**
 * The numbered-log-entry commit for the S3 ledger (docs/design/s3-ledger-log-commit.md), behind
 * INGEST_LEDGER_LOG_COMMIT (default off). FIRST SLICE: the per-stream log writer and reader only. Nothing in the
 * admission path calls this module yet, and `on` is refused at startup until that wiring lands, so with the flag
 * at its default every byte the ingest writes is unchanged.
 *
 * One STREAM per (org, workspace, user, origin): ordering across a user's machines is not required
 * (Khaliq, 2026-11-16), so each machine's stream commits on its own. A stream is a dense sequence of immutable
 * entries `e/<seq>.json`, each written ONCE with create-if-absent. That is the only coordination primitive used,
 * and it behaves the same on S3 (If-None-Match), GCS (ifGenerationMatch=0) and Azure (If-None-Match: *); the
 * object-store conformance cases `create_is_idempotent_for_same_content` and `create_conflicts_on_different_content`
 * pin it per backend. No object is ever compare-and-swapped on the commit path (GCS allows about one write per
 * second per object name).
 *
 *   commit  = payload PUTs (content addressed, idempotent, in parallel) + ONE create-if-absent of entry head+1
 *   fencing = an `epoch` entry. A writer takes the stream by appending one; a stale writer's next append targets
 *             a sequence number that is already taken, gets 412, reads the entry back and stops (LogFenced).
 *   dedupe  = a remembered-batches window (batchId -> digest), carried in checkpoints and replayed from entries.
 *
 * Entry 0 (`kind: "open"`) is the durable per-stream MODE MARKER: its existence means the stream is in log mode.
 * Every entry carries the sha256 of the previous entry's exact bytes (`prev`), so a reader detects a gap, a
 * reorder or a torn write and fails closed instead of guessing.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { admissionUserDir } from "./async-admission.ts";
import type { LedgerTenant } from "./ledger.ts";

export const INGEST_LEDGER_LOG_COMMIT_VAR = "INGEST_LEDGER_LOG_COMMIT";
export type LedgerLogCommitMode = "off" | "on";

/** Unset, "" or "off": off (today, byte for byte). Exactly "on" otherwise; anything else throws. */
export function ledgerLogCommitMode(raw: string | undefined): LedgerLogCommitMode {
  const v = raw?.trim();
  if (v === undefined || v === "" || v === "off") return "off";
  if (v === "on") return "on";
  throw new Error(`session ingest: ${INGEST_LEDGER_LOG_COMMIT_VAR} must be off or on`);
}

/** Entries between checkpoints: bounds how many entries an open replays. */
export const LOG_CHECKPOINT_EVERY = 64;
/** Batches remembered for dedupe (the legacy manifest's REMEMBERED_BATCHES is 64 per session; this is per stream). */
export const LOG_REMEMBERED_BATCHES = 4096;
/** Attempts of one entry PUT on a transient failure before an ambiguous result is returned to the caller. */
export const LOG_PUT_ATTEMPTS = 4;

const SEQ_DIGITS = 20;
/**
 * Entries live in DIRECTORIES of 1000 seqs (`e/<first 17 digits>/<20 digits>.json`), so the "nothing past head"
 * proof lists only the directory holding head+1 and the next one: at most two pages, never the whole `e/` prefix
 * (an ever-growing stream would otherwise cost O(N/1000) LIST pages per open). `list` works on `/` boundaries on every
 * backend (S3, GCS, Azure, fs), so this needs no start-after support in the ObjectStore interface.
 */
const BUCKET_DIGITS = SEQ_DIGITS - 3;
export const LOG_ENTRIES_PER_DIR = 1000;
const sha256 = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
const h = (v: string) => sha256(v).slice(0, 32);
const pad = (seq: number) => {
  if (!Number.isSafeInteger(seq) || seq < 0) throw new LogCorrupt("log_seq_invalid");
  return String(seq).padStart(SEQ_DIGITS, "0");
};

export function logKeys(keyRoot: string, tenant: LedgerTenant, originId: string) {
  const root = `${admissionUserDir(keyRoot, tenant)}/ledger/log/o=${h(originId)}`;
  return {
    root,
    entry: (seq: number) => { const p = pad(seq); return `${root}/e/${p.slice(0, BUCKET_DIGITS)}/${p}.json`; },
    /** The directory holding `seq` (a list prefix). */
    entryDir: (seq: number) => `${root}/e/${pad(seq).slice(0, BUCKET_DIGITS)}`,
    checkpoint: (seq: number) => `${root}/c/${pad(seq)}.json`,
    /** A HINT only (the newest checkpoint seq); overwritten without CAS, may lag, never trusted alone. */
    head: `${root}/head.json`,
    /** Content-addressed payloads: the same bytes always land at the same key. */
    payload: (sha: string) => `${root}/p/${sha}.json.zst`,
    /** The whole entry prefix: listed (first page only) to tell "no entries at all" from "entry 0 lost". */
    entries: `${root}/e`,
  };
}
export type LogKeys = ReturnType<typeof logKeys>;

export interface LogPayloadRef { key: string; sha256: string; size: number }
export interface LogBatch {
  batchId: string;
  digest: string;
  receiptId: string;
  /** The sessions the batch writes, as [source, sessionId]: privacy gates and readers key on these. */
  sessions: [string, string][];
  payloads: LogPayloadRef[];
}
/** `stream`: the stream's key root, so an entry copied to another stream never validates there. */
interface EntryBase { v: 1; stream: string; seq: number; prev: string | null; epoch: number; writer: string; atMs: number }
export type LogEntry =
  | (EntryBase & { kind: "open"; legacyCutover: string | null })
  | (EntryBase & { kind: "epoch" })
  | (EntryBase & { kind: "batches"; batches: LogBatch[]; inboxCursor: string | null });
export interface LogCheckpoint {
  v: 1;
  seq: number;
  /** sha256 of entry `seq`'s exact bytes: a checkpoint is valid only if it matches the entry it names. */
  hash: string;
  epoch: number;
  /** The writer whose open or epoch entry set `epoch`: the only one that may append batches. */
  epochWriter: string;
  legacyCutover: string | null;
  inboxCursor: string | null;
  /** Oldest first: [batchId, digest]. */
  remembered: [string, string][];
}

/** The reader found something the protocol cannot produce (a gap, a bad hash, a stale epoch): fail closed. */
export class LogCorrupt extends Error {
  constructor(readonly code: string) { super(code); this.name = "LogCorrupt"; }
}
/** Another writer owns the stream now (a newer epoch, or a different entry at our seq). Stop writing. */
export class LogFenced extends Error {
  constructor(readonly code: string, readonly seq: number) { super(code); this.name = "LogFenced"; }
}
/**
 * The store did not answer whether the entry landed. The caller MUST retry the same `pending` entry (same bytes,
 * same seq) until it resolves; writing different content at that seq could conflict with a late landing.
 */
export class LogUnavailable extends Error {
  constructor(readonly code: string) { super(code); this.name = "LogUnavailable"; }
}

export interface LogState {
  keys: LogKeys;
  head: number;
  headHash: string;
  epoch: number;
  /** The writer that holds `epoch` (its open or epoch entry landed): appendBatches refuses any other writer. */
  epochWriter: string;
  /** Entry 0's legacy cutover: the last legacy batch committed before the stream switched. */
  legacyCutover: string | null;
  inboxCursor: string | null;
  remembered: Map<string, string>;
  /** An entry whose landing is unresolved: the next append must resend exactly these bytes first. */
  pending?: { seq: number; bytes: Uint8Array; entry: LogEntry };
}

// ---- validation ----

const isStr = (v: unknown): v is string => typeof v === "string";
const isNat = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) >= 0;
function validBatch(b: unknown): b is LogBatch {
  const x = b as LogBatch;
  return !!x && isStr(x.batchId) && x.batchId !== "" && isStr(x.digest) && isStr(x.receiptId) &&
    Array.isArray(x.sessions) && x.sessions.every((s) => Array.isArray(s) && s.length === 2 && isStr(s[0]) && isStr(s[1])) &&
    Array.isArray(x.payloads) && x.payloads.every((p) => !!p && isStr(p.key) && isStr(p.sha256) && isNat(p.size));
}
export function validEntry(v: unknown): v is LogEntry {
  const e = v as LogEntry;
  if (!e || e.v !== 1 || !isStr(e.stream) || !isNat(e.seq) || !(e.prev === null || isStr(e.prev)) || !isNat(e.epoch) || !isStr(e.writer) || !isNat(e.atMs)) return false;
  if (e.kind === "open") return e.seq === 0 && e.prev === null && e.epoch === 1 && (e.legacyCutover === null || isStr(e.legacyCutover));
  if (e.kind === "epoch") return e.seq > 0 && isStr(e.prev);
  if (e.kind === "batches") return e.seq > 0 && isStr(e.prev) && Array.isArray(e.batches) && e.batches.length > 0 && e.batches.every(validBatch) &&
    (e.inboxCursor === null || isStr(e.inboxCursor));
  return false;
}
function validCheckpoint(v: unknown): v is LogCheckpoint {
  const c = v as LogCheckpoint;
  return !!c && c.v === 1 && isNat(c.seq) && isStr(c.hash) && isNat(c.epoch) && isStr(c.epochWriter) && (c.legacyCutover === null || isStr(c.legacyCutover)) &&
    (c.inboxCursor === null || isStr(c.inboxCursor)) &&
    Array.isArray(c.remembered) && c.remembered.every((r) => Array.isArray(r) && r.length === 2 && isStr(r[0]) && isStr(r[1]));
}

// ---- store primitives ----

const encode = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
async function readBytes(store: ObjectStore, key: string): Promise<Uint8Array | undefined> {
  try { return await readAll((await store.getObject(objectKey(key))).body); }
  catch (error) { if (isObjectStoreError(error) && error.code === "not_found") return undefined; throw error; }
}
const parse = (bytes: Uint8Array): unknown => { try { return JSON.parse(new TextDecoder().decode(bytes)); } catch { return undefined; } };
const equalBytes = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((x, i) => x === b[i]);
/** Retried: only the store's own `transient` (network, 408, 429, 5xx). Any other error is not a store fault: rethrown. */
const transient = (error: unknown) => isObjectStoreError(error) && error.code === "transient";
/** A create found a different object at the key ("already_exists"; some backends answer a 412 for it). */
const taken = (error: unknown) => isObjectStoreError(error) && (error.code === "already_exists" || error.code === "precondition_failed");

/** Write `bytes` at a content-addressed payload key. Idempotent: the same bytes always succeed. */
export async function putPayload(store: ObjectStore, keys: LogKeys, bytes: Uint8Array): Promise<LogPayloadRef> {
  const sha = sha256(bytes), key = keys.payload(sha);
  await store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: sha });
  return { key, sha256: sha, size: bytes.length };
}

type PutOutcome = "landed" | "taken";
/**
 * Create entry `seq` with exactly `bytes`. landed: ours is there (first write, or an idempotent re-put of the same
 * bytes, or a readback after a create conflict finding our bytes). taken: a different entry holds `seq`. Throws LogUnavailable
 * when no attempt or readback resolved it.
 */
async function createEntry(store: ObjectStore, keys: LogKeys, seq: number, bytes: Uint8Array, sleep: (ms: number) => Promise<void>): Promise<PutOutcome> {
  const key = keys.entry(seq);
  for (let attempt = 1; attempt <= LOG_PUT_ATTEMPTS; attempt++) {
    try {
      await store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: sha256(bytes), contentType: "application/json" });
      return "landed";
    } catch (error) {
      if (taken(error)) {
        let there: Uint8Array | undefined;
        try { there = await readBytes(store, key); }
        catch (readError) { if (transient(readError)) throw new LogUnavailable("log_entry_conflict_unreadable"); throw readError; }
        if (there === undefined) throw new LogUnavailable("log_entry_conflict_unreadable");
        return equalBytes(there, bytes) ? "landed" : "taken";
      }
      if (!transient(error)) throw error;
      if (attempt < LOG_PUT_ATTEMPTS) await sleep(25 * 2 ** attempt);
    }
  }
  // A timed-out or 5xx PUT may still have landed: read it back, never assume failure.
  let there: Uint8Array | undefined;
  try { there = await readBytes(store, key); } catch { throw new LogUnavailable("log_entry_ambiguous"); }
  if (there === undefined) throw new LogUnavailable("log_entry_ambiguous");
  return equalBytes(there, bytes) ? "landed" : "taken";
}

/** Seqs stored in one entry directory (at most LOG_ENTRIES_PER_DIR keys). A store fault is LogUnavailable. */
async function seqsIn(store: ObjectStore, prefix: string): Promise<number[]> {
  const out: number[] = [];
  let cursor: string | undefined;
  try {
    do {
      const page = await store.list(objectKey(prefix), cursor);
      for (const item of page.items) {
        const m = /\/(\d{20})\.json$/.exec(String(item.key));
        if (m) out.push(Number(m[1]));
      }
      cursor = page.nextCursor;
    } while (cursor);
  } catch (error) {
    if (transient(error)) throw new LogUnavailable("log_list_unavailable");
    throw error;
  }
  return out;
}
/**
 * Seqs stored past `head` in the two directories that could hold head+1 and its successors. A real gap (an entry
 * deleted or lost in the middle) leaves head+2.. in place, so it is found unless 1000+ CONSECUTIVE entries were lost.
 * A deleted TAIL (the newest entries, nothing after them) is indistinguishable from a shorter log: see the design.
 */
async function seqsPast(store: ObjectStore, keys: LogKeys, head: number): Promise<number[]> {
  const [a, b] = await Promise.all([seqsIn(store, keys.entryDir(head + 1)), seqsIn(store, keys.entryDir(head + 1 + LOG_ENTRIES_PER_DIR))]);
  return [...a, ...b].filter((x) => x > head);
}
/** Whether any entry exists at all (the first page of `e/` is enough). */
async function anyEntry(store: ObjectStore, keys: LogKeys): Promise<boolean> {
  try {
    const page = await store.list(objectKey(keys.entries));
    return page.items.some((item) => /\/(\d{20})\.json$/.test(String(item.key)));
  } catch (error) {
    if (transient(error)) throw new LogUnavailable("log_list_unavailable");
    throw error;
  }
}

/**
 * Entry 0's bytes, or undefined for a legacy stream. Concurrency-safe: when entry 0 reads missing but entries exist,
 * entry 0 is read AGAIN before declaring it lost (a concurrent enableLogMode may have created it between the read
 * and the list). Only a direct re-read still missing, with other entries present, is log_open_missing.
 */
async function readZero(store: ObjectStore, keys: LogKeys): Promise<Uint8Array | undefined> {
  const zero = await readBytes(store, keys.entry(0));
  if (zero !== undefined) return zero;
  if (!(await anyEntry(store, keys))) return undefined;
  const again = await readBytes(store, keys.entry(0));
  if (again !== undefined) return again;
  throw new LogCorrupt("log_open_missing");
}

/**
 * Replay entries after state.head, applying each, to the end of the chain. Concurrency-safe: a stream being appended
 * to keeps growing, so an entry listed past head is not a gap until head+1 is read DIRECTLY again and is still
 * missing. Only then: LogCorrupt("log_entry_gap").
 */
async function* replay(store: ObjectStore, state: LogState): AsyncGenerator<LogEntry> {
  for (;;) {
    const bytes = await readBytes(store, state.keys.entry(state.head + 1));
    if (bytes !== undefined) { yield applyEntry(state, bytes); continue; }
    const past = await seqsPast(store, state.keys, state.head);
    if (past.length === 0) return;
    const again = await readBytes(store, state.keys.entry(state.head + 1));
    if (again !== undefined) { yield applyEntry(state, again); continue; }
    throw new LogCorrupt("log_entry_gap");
  }
}

// ---- open / replay ----

function opened(keys: LogKeys, zero: Uint8Array): LogState {
  const open = parse(zero);
  if (!validEntry(open) || open.kind !== "open" || open.stream !== keys.root) throw new LogCorrupt("log_open_invalid");
  return { keys, head: 0, headHash: sha256(zero), epoch: open.epoch, epochWriter: open.writer, legacyCutover: open.legacyCutover, inboxCursor: null, remembered: new Map() };
}

/** `undefined`: the stream is not in log mode (no entry 0, and no entry at all), so it is a legacy stream. */
export async function openStream(store: ObjectStore, keys: LogKeys): Promise<LogState | undefined> {
  const zero = await readZero(store, keys);
  if (zero === undefined) return undefined;
  let state = opened(keys, zero);
  // Start from the newest checkpoint the hint names, if it verifies against its entry; else from entry 0. The hint
  // and the checkpoint are optional: a missing, malformed or unreadable one (a transient store error) falls back.
  try {
    const hint = parse((await readBytes(store, keys.head)) ?? new Uint8Array()) as { seq?: unknown } | undefined;
    if (hint && isNat(hint.seq) && hint.seq > 0) {
      const cp = parse((await readBytes(store, keys.checkpoint(hint.seq))) ?? new Uint8Array());
      const at = await readBytes(store, keys.entry(hint.seq));
      // Bounded on read: a checkpoint is trusted only as far as its hash, and an oversized window is ignored.
      if (validCheckpoint(cp) && cp.seq === hint.seq && cp.remembered.length <= LOG_REMEMBERED_BATCHES && cp.epoch >= 1 &&
        at !== undefined && sha256(at) === cp.hash && cp.legacyCutover === state.legacyCutover)
        state = { keys, head: cp.seq, headHash: cp.hash, epoch: cp.epoch, epochWriter: cp.epochWriter, legacyCutover: cp.legacyCutover,
          inboxCursor: cp.inboxCursor, remembered: new Map(cp.remembered) };
    }
  } catch (error) {
    if (!isObjectStoreError(error)) throw error;
  }
  for await (const _ of replay(store, state)) { /* applied in order */ }
  return state;
}

/** Advance `state` over the next entry's exact bytes, or throw LogCorrupt. */
export function applyEntry(state: LogState, bytes: Uint8Array): LogEntry {
  const e = parse(bytes);
  if (!validEntry(e)) throw new LogCorrupt("log_entry_invalid");
  if (e.stream !== state.keys.root) throw new LogCorrupt("log_entry_other_stream");
  if (e.seq !== state.head + 1) throw new LogCorrupt("log_entry_gap");
  if (e.prev !== state.headHash) throw new LogCorrupt("log_entry_chain");
  if (e.kind === "epoch") {
    if (e.epoch !== state.epoch + 1) throw new LogCorrupt("log_epoch_not_next");
    state.epoch = e.epoch;
    state.epochWriter = e.writer;
  } else if (e.kind === "batches") {
    if (e.epoch !== state.epoch || e.writer !== state.epochWriter) throw new LogCorrupt("log_entry_stale_epoch");
    // A payload is named by its content and lives in this stream: never another stream's or an arbitrary key.
    if (!e.batches.every((b) => b.payloads.every((p) => p.key === state.keys.payload(p.sha256)))) throw new LogCorrupt("log_payload_foreign");
    for (const b of e.batches) remember(state.remembered, b.batchId, b.digest);
    state.inboxCursor = e.inboxCursor;
  } else throw new LogCorrupt("log_open_repeated");
  state.head = e.seq;
  state.headHash = sha256(bytes);
  return e;
}

function remember(m: Map<string, string>, batchId: string, digest: string) {
  m.delete(batchId);
  m.set(batchId, digest);
  while (m.size > LOG_REMEMBERED_BATCHES) m.delete(m.keys().next().value as string);
}

/**
 * Every entry after `fromSeq`, in order, validated as a chain (for consumers that materialize from the log). It
 * replays from entry 0 (checkpoints are not a starting point here yet), so compaction must not delete any entry a
 * reader may still replay through; entry 0 is never deleted.
 */
export async function* readEntries(store: ObjectStore, keys: LogKeys, fromSeq = 0): AsyncGenerator<LogEntry> {
  const zero = await readZero(store, keys);
  if (zero === undefined) return;
  const state = opened(keys, zero);
  if (fromSeq === 0) yield parse(zero) as LogEntry;
  for await (const e of replay(store, state)) if (e.seq > fromSeq) yield e;
}

// ---- writing ----

/**
 * `writer` IS the ownership identity: the same writer id is the same owner (a restart reusing it regains the stream
 * without a take, and two processes sharing one would interleave appends; the chain stays linear, but the inbox cursor
 * could move backwards). The wiring MUST use a per-process-lifetime id (minted at process start, never configured).
 */
export interface WriteDeps { writer: string; now?: () => number; sleep?: (ms: number) => Promise<void> }
const deps = (d: WriteDeps) => ({ now: d.now ?? Date.now, sleep: d.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms))) });

/**
 * Put a stream into log mode (entry 0). Idempotent for the same `legacyCutover`. The writer whose entry 0 landed
 * holds epoch 1; every other opener gets the same state but does not own it (appendBatches refuses it until it
 * takes the stream). A stream already opened with a DIFFERENT legacyCutover is refused (LogFenced): two operators
 * disagreeing about the cutover is never silently resolved.
 */
export async function enableLogMode(store: ObjectStore, keys: LogKeys, legacyCutover: string | null, d: WriteDeps): Promise<LogState> {
  const { now, sleep } = deps(d);
  const existing = await openStream(store, keys);
  if (existing) {
    if (existing.legacyCutover !== legacyCutover) throw new LogFenced("log_open_other_cutover", 0);
    return existing;
  }
  const entry: LogEntry = { v: 1, stream: keys.root, seq: 0, prev: null, kind: "open", epoch: 1, writer: d.writer, atMs: now(), legacyCutover };
  await createEntry(store, keys, 0, encode(entry), sleep); // taken: another opener won; read it either way
  const state = await openStream(store, keys);
  if (!state) throw new LogUnavailable("log_open_unreadable");
  if (state.legacyCutover !== legacyCutover) throw new LogFenced("log_open_other_cutover", 0);
  return state;
}

/**
 * Take ownership: append an epoch entry. Any unresolved entry is resolved first, and the epoch entry itself is kept
 * pending until it resolves, exactly as a batches entry. Losing the race (another writer appended first) does NOT
 * retry: the state is caught up and LogFenced("log_take_lost") is thrown, so two takers back off (the caller's
 * retry decides) instead of fencing each other in a loop. The previous owner's next append is refused.
 */
export async function takeStream(store: ObjectStore, state: LogState, d: WriteDeps): Promise<LogState> {
  const { now, sleep } = deps(d);
  if (state.pending) await resolvePending(store, state, sleep);
  const entry: LogEntry = { v: 1, stream: state.keys.root, seq: state.head + 1, prev: state.headHash, kind: "epoch", epoch: state.epoch + 1, writer: d.writer, atMs: now() };
  state.pending = { seq: entry.seq, bytes: encode(entry), entry };
  try {
    await resolvePending(store, state, sleep);
  } catch (error) {
    if (!(error instanceof LogFenced)) throw error;
    const fresh = await openStream(store, state.keys);
    if (!fresh) throw new LogCorrupt("log_open_vanished");
    Object.assign(state, fresh, { pending: undefined });
    throw new LogFenced("log_take_lost", entry.seq);
  }
  return state;
}

export interface AppendResult {
  seq: number | null;
  admitted: LogBatch[];
  /** Same batchId and digest already committed (or repeated in this call): nothing written for it. */
  replays: string[];
  /** Same batchId with a different digest: refused, as the legacy batch_conflict. */
  conflicts: string[];
}

/**
 * Commit `batches` (whose payloads the caller already wrote with putPayload) as ONE entry at head+1. Only the
 * writer that holds the current epoch may append (LogFenced "log_not_owner" otherwise). On LogFenced the caller
 * stops; on LogUnavailable it retries appendBatches later with the same state, which resends the pending entry first.
 */
export async function appendBatches(store: ObjectStore, state: LogState, batches: LogBatch[], inboxCursor: string | null, d: WriteDeps): Promise<AppendResult> {
  const { now, sleep } = deps(d);
  if (state.pending) await resolvePending(store, state, sleep);
  if (state.epochWriter !== d.writer) throw new LogFenced("log_not_owner", state.head + 1);
  const admitted: LogBatch[] = [], replays: string[] = [], conflicts: string[] = [];
  // Dedupe against the window plus this call's own batches (only the call's ids are added, not a window copy).
  const inCall = new Map<string, string>();
  for (const b of batches) {
    if (!validBatch(b)) throw new LogCorrupt("log_batch_invalid");
    if (!b.payloads.every((p) => p.key === state.keys.payload(p.sha256))) throw new LogCorrupt("log_payload_foreign");
    const known = inCall.get(b.batchId) ?? state.remembered.get(b.batchId);
    if (known === undefined) { admitted.push(b); inCall.set(b.batchId, b.digest); }
    else if (known === b.digest) replays.push(b.batchId);
    else conflicts.push(b.batchId);
  }
  if (admitted.length === 0) return { seq: null, admitted, replays, conflicts };
  const entry: LogEntry = { v: 1, stream: state.keys.root, seq: state.head + 1, prev: state.headHash, kind: "batches", epoch: state.epoch, writer: d.writer, atMs: now(), batches: admitted, inboxCursor };
  const bytes = encode(entry);
  state.pending = { seq: entry.seq, bytes, entry };
  await resolvePending(store, state, sleep);
  await maybeCheckpoint(store, state);
  return { seq: entry.seq, admitted, replays, conflicts };
}

async function resolvePending(store: ObjectStore, state: LogState, sleep: (ms: number) => Promise<void>) {
  const p = state.pending!;
  const outcome = await createEntry(store, state.keys, p.seq, p.bytes, sleep); // LogUnavailable keeps `pending`
  state.pending = undefined;
  if (outcome === "taken") throw new LogFenced("log_entry_taken", p.seq);
  applyEntry(state, p.bytes);
}

async function maybeCheckpoint(store: ObjectStore, state: LogState) {
  if (state.head % LOG_CHECKPOINT_EVERY !== 0) return;
  const cp: LogCheckpoint = { v: 1, seq: state.head, hash: state.headHash, epoch: state.epoch, epochWriter: state.epochWriter, legacyCutover: state.legacyCutover,
    inboxCursor: state.inboxCursor, remembered: [...state.remembered] };
  try {
    await store.put(objectKey(state.keys.checkpoint(state.head)), encode(cp), { ifNoneMatch: true, contentType: "application/json" });
    await store.put(objectKey(state.keys.head), encode({ v: 1, seq: state.head }), { contentType: "application/json" });
  } catch {
    // A checkpoint is an optimisation: a missing or stale one only makes the next open replay more entries.
  }
}
