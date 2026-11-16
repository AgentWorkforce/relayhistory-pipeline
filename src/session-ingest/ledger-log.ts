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
    entry: (seq: number) => `${root}/e/${pad(seq)}.json`,
    checkpoint: (seq: number) => `${root}/c/${pad(seq)}.json`,
    /** A HINT only (the newest checkpoint seq); overwritten without CAS, may lag, never trusted alone. */
    head: `${root}/head.json`,
    /** Content-addressed payloads: the same bytes always land at the same key. */
    payload: (sha: string) => `${root}/p/${sha}.json.zst`,
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
interface EntryBase { v: 1; seq: number; prev: string | null; epoch: number; writer: string; atMs: number }
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
  if (!e || e.v !== 1 || !isNat(e.seq) || !(e.prev === null || isStr(e.prev)) || !isNat(e.epoch) || !isStr(e.writer) || !isNat(e.atMs)) return false;
  if (e.kind === "open") return e.seq === 0 && e.prev === null && (e.legacyCutover === null || isStr(e.legacyCutover));
  if (e.kind === "epoch") return e.seq > 0 && isStr(e.prev);
  if (e.kind === "batches") return e.seq > 0 && isStr(e.prev) && Array.isArray(e.batches) && e.batches.length > 0 && e.batches.every(validBatch) &&
    (e.inboxCursor === null || isStr(e.inboxCursor));
  return false;
}
function validCheckpoint(v: unknown): v is LogCheckpoint {
  const c = v as LogCheckpoint;
  return !!c && c.v === 1 && isNat(c.seq) && isStr(c.hash) && isNat(c.epoch) && (c.inboxCursor === null || isStr(c.inboxCursor)) &&
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
/** Retried: the store's own `transient` (network, 408, 429, 5xx) and anything thrown below the store (an abort). */
const transient = (error: unknown) => !isObjectStoreError(error) || error.code === "transient";
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
        const there = await readBytes(store, key);
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

// ---- open / replay ----

/** `undefined`: the stream is not in log mode (no entry 0), so it is a legacy stream. */
export async function openStream(store: ObjectStore, keys: LogKeys): Promise<LogState | undefined> {
  const zero = await readBytes(store, keys.entry(0));
  if (zero === undefined) return undefined;
  const open = parse(zero);
  if (!validEntry(open) || open.kind !== "open") throw new LogCorrupt("log_open_invalid");
  let state: LogState = { keys, head: 0, headHash: sha256(zero), epoch: open.epoch, inboxCursor: null, remembered: new Map() };
  // Start from the newest checkpoint the hint names, if it verifies against its entry; else from entry 0.
  const hint = parse((await readBytes(store, keys.head)) ?? new Uint8Array()) as { seq?: unknown } | undefined;
  if (hint && isNat(hint.seq) && hint.seq > 0) {
    const cp = parse((await readBytes(store, keys.checkpoint(hint.seq))) ?? new Uint8Array());
    const at = await readBytes(store, keys.entry(hint.seq));
    if (validCheckpoint(cp) && cp.seq === hint.seq && at !== undefined && sha256(at) === cp.hash)
      state = { keys, head: cp.seq, headHash: cp.hash, epoch: cp.epoch, inboxCursor: cp.inboxCursor, remembered: new Map(cp.remembered) };
  }
  for (;;) {
    const bytes = await readBytes(store, keys.entry(state.head + 1));
    if (bytes === undefined) return state;
    applyEntry(state, bytes);
  }
}

/** Advance `state` over the next entry's exact bytes, or throw LogCorrupt. */
export function applyEntry(state: LogState, bytes: Uint8Array): LogEntry {
  const e = parse(bytes);
  if (!validEntry(e)) throw new LogCorrupt("log_entry_invalid");
  if (e.seq !== state.head + 1) throw new LogCorrupt("log_entry_gap");
  if (e.prev !== state.headHash) throw new LogCorrupt("log_entry_chain");
  if (e.kind === "epoch") {
    if (e.epoch <= state.epoch) throw new LogCorrupt("log_epoch_not_increasing");
    state.epoch = e.epoch;
  } else if (e.kind === "batches") {
    if (e.epoch !== state.epoch) throw new LogCorrupt("log_entry_stale_epoch");
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

/** Every entry after `fromSeq`, in order, validated as a chain (for consumers that materialize from the log). */
export async function* readEntries(store: ObjectStore, keys: LogKeys, fromSeq = 0): AsyncGenerator<LogEntry> {
  const zero = await readBytes(store, keys.entry(0));
  if (zero === undefined) return;
  const open = parse(zero);
  if (!validEntry(open) || open.kind !== "open") throw new LogCorrupt("log_open_invalid");
  const state: LogState = { keys, head: 0, headHash: sha256(zero), epoch: open.epoch, inboxCursor: null, remembered: new Map() };
  if (fromSeq === 0) yield open;
  for (;;) {
    const bytes = await readBytes(store, keys.entry(state.head + 1));
    if (bytes === undefined) return;
    const e = applyEntry(state, bytes);
    if (e.seq > fromSeq) yield e;
  }
}

// ---- writing ----

export interface WriteDeps { writer: string; now?: () => number; sleep?: (ms: number) => Promise<void> }
const deps = (d: WriteDeps) => ({ now: d.now ?? Date.now, sleep: d.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms))) });

/**
 * Put a stream into log mode (entry 0). Idempotent for the same `legacyCutover` and writer. Returns the opened
 * state, with the writer holding epoch 1 (entry 0 carries it, so the opener is the first owner).
 */
export async function enableLogMode(store: ObjectStore, keys: LogKeys, legacyCutover: string | null, d: WriteDeps): Promise<LogState> {
  const { now, sleep } = deps(d);
  const existing = await openStream(store, keys);
  if (existing) return existing;
  const entry: LogEntry = { v: 1, seq: 0, prev: null, kind: "open", epoch: 1, writer: d.writer, atMs: now(), legacyCutover };
  await createEntry(store, keys, 0, encode(entry), sleep); // taken: another opener won; read it either way
  const state = await openStream(store, keys);
  if (!state) throw new LogUnavailable("log_open_unreadable");
  return state;
}

/** Take ownership: append an epoch entry. The previous owner's next append is refused (LogFenced). */
export async function takeStream(store: ObjectStore, state: LogState, d: WriteDeps): Promise<LogState> {
  const { now } = deps(d);
  for (let attempt = 0; attempt < 8; attempt++) {
    const entry: LogEntry = { v: 1, seq: state.head + 1, prev: state.headHash, kind: "epoch", epoch: state.epoch + 1, writer: d.writer, atMs: now() };
    const bytes = encode(entry);
    if (await createEntry(store, state.keys, entry.seq, bytes, deps(d).sleep) === "landed") { applyEntry(state, bytes); return state; }
    // Someone appended first: catch up and try again on top of it.
    const fresh = await openStream(store, state.keys);
    if (!fresh) throw new LogCorrupt("log_open_vanished");
    Object.assign(state, fresh, { pending: undefined });
  }
  throw new LogUnavailable("log_take_contended");
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
 * Commit `batches` (whose payloads the caller already wrote with putPayload) as ONE entry at head+1. The caller
 * must hold the stream (takeStream or enableLogMode). On LogFenced the caller stops; on LogUnavailable it retries
 * appendBatches later with the same state, which resends the pending entry first.
 */
export async function appendBatches(store: ObjectStore, state: LogState, batches: LogBatch[], inboxCursor: string | null, d: WriteDeps): Promise<AppendResult> {
  const { now, sleep } = deps(d);
  if (state.pending) await resolvePending(store, state, sleep);
  const admitted: LogBatch[] = [], replays: string[] = [], conflicts: string[] = [];
  const seen = new Map(state.remembered);
  for (const b of batches) {
    if (!validBatch(b)) throw new LogCorrupt("log_batch_invalid");
    const known = seen.get(b.batchId);
    if (known === undefined) { admitted.push(b); seen.set(b.batchId, b.digest); }
    else if (known === b.digest) replays.push(b.batchId);
    else conflicts.push(b.batchId);
  }
  if (admitted.length === 0) return { seq: null, admitted, replays, conflicts };
  const entry: LogEntry = { v: 1, seq: state.head + 1, prev: state.headHash, kind: "batches", epoch: state.epoch, writer: d.writer, atMs: now(), batches: admitted, inboxCursor };
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
  const cp: LogCheckpoint = { v: 1, seq: state.head, hash: state.headHash, epoch: state.epoch, inboxCursor: state.inboxCursor, remembered: [...state.remembered] };
  try {
    await store.put(objectKey(state.keys.checkpoint(state.head)), encode(cp), { ifNoneMatch: true, contentType: "application/json" });
    await store.put(objectKey(state.keys.head), encode({ v: 1, seq: state.head }), { contentType: "application/json" });
  } catch {
    // A checkpoint is an optimisation: a missing or stale one only makes the next open replay more entries.
  }
}
