/**
 * Where a pass's output facts (output-facts.ts outputFactsFromScope) are kept.
 *
 * - clickhouseOutputFactsStore: today's ClickHouse functions, called exactly as before
 *   (SEARCH_INDEX_OUTPUT_FACTS=1 with the ClickHouse search binding).
 * - postgresOutputFactsStore: the self-host Postgres tables of migration 20261105045040,
 *   written through its SECURITY DEFINER functions on the consumer's transaction
 *   (RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true, consumer-config.ts). One transaction holds
 *   the directory events and the facts, so a failed pass leaves neither.
 *
 * Ranges are ClickHouse DateTime64(6) text ("YYYY-MM-DD HH:MM:SS.ffffff", UTC) on both
 * sides of the interface, as the builder emits them.
 */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { PipelineError } from "../shared/vendor.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { StagedRunRestart, type StagedFactsChunk, type StagedFactsEvents, type StagedRunRef } from "./staged-reconcile.ts";
import {
  callMessageIds,
  OUTPUT_FACTS_MAX_ITEMS_PER_INPUT,
  outputFactsBatchStep,
  outputFactsFinishStep,
  type OutputFactsStagedCandidate,
  type OutputFactsStagedItem,
  type OutputFactsStagedScope,
  eraseOutputFacts,
  outputFactsSchema,
  previousOutputFactsState,
  reconcileOutputFacts,
  recordOutputFactsState,
  type EventFactRow,
  type FactsRange,
  type OutputFactRow,
  type OutputFactsScope,
  type StatePhase,
} from "./output-facts.ts";
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

export interface OutputFactsReconcileInput {
  eventFacts: EventFactRow[];
  outputFacts: OutputFactRow[];
  /** False when a source could not be read exactly: the reconcile then never deletes. */
  complete: boolean;
  /** The builder's reasons (OutputFacts.reasons). */
  reasons: string[];
}

export interface OutputFactsReconcileResult {
  inserted: number;
  deleted: number;
  complete: boolean;
  reasons: string[];
  /** False when the store refused the pass as stale (an older source version): nothing changed. */
  applied: boolean;
}

export interface OutputFactsStore {
  /** The scope's recorded span and highest recorded version (0 when none). */
  previousState(scope: OutputFactsScope): Promise<{ range: FactsRange; version: number }>;
  /** Make the scope's facts equal the input; deletes only when complete. */
  reconcile(scope: OutputFactsScope, input: OutputFactsReconcileInput, sourceVersion: number): Promise<OutputFactsReconcileResult>;
  /** Record the scope's facts state; false when refused as stale. */
  recordState(
    scope: OutputFactsScope,
    state: { complete: boolean; at: Date; range: FactsRange; phase?: StatePhase; sourceVersion: number },
  ): Promise<boolean>;
  /** Erasure: every fact (and, where the store keeps one per scope, state row) of the scope goes. */
  erase(scope: OutputFactsScope): Promise<void>;
  /** `present`: the fact tables exist (erasure must purge them); `ready`: a pass may write. */
  schemaReady(): Promise<{ ready: boolean; present: boolean }>;
}

/** Today's ClickHouse functions behind the interface, unchanged. */
export function clickhouseOutputFactsStore(ch: ClickHouseClient, opts: { maxRows: number; insertBatch: number }): OutputFactsStore {
  return {
    previousState: (scope) => previousOutputFactsState(ch, scope),
    async reconcile(scope, input) {
      // The ClickHouse rows carry their ReplacingMergeTree version already (the pass base).
      const r = await reconcileOutputFacts(ch, scope, { events: input.eventFacts, facts: input.outputFacts, complete: input.complete, reasons: input.reasons }, opts);
      return { ...r, applied: true };
    },
    async recordState(scope, s) {
      await recordOutputFactsState(ch, scope, s.complete, s.at, s.range, s.phase ?? "final", s.sourceVersion);
      return true;
    },
    erase: (scope) => eraseOutputFacts(ch, scope),
    schemaReady: () => outputFactsSchema(ch),
  };
}

/** DateTime64(6) text (UTC, no zone) -> an ISO instant Postgres reads the same in any TimeZone. */
export function pgInstant(chText: string): string {
  if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$/.test(chText)) throw new Error("output_facts_invalid_time");
  return `${chText.replace(" ", "T")}Z`;
}

const CH_TEXT = (column: string) => `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')`;
const scopeArgs = (scope: OutputFactsScope) => [scope.orgId, scope.workspaceId, scope.harness, scope.sessionIds];

function sourceVersionOf(v: number): number {
  if (!Number.isSafeInteger(v) || v < 0) throw new Error("output_facts_invalid_source_version");
  return v;
}

/**
 * The self-host Postgres store on one transaction (migration 20261105045040). The source
 * version is the session directory event_projection_epoch the pass read: a pass older than the
 * stored one is a no-op in the database. There is no pending phase: the writes are
 * transactional, so a dead pass leaves the prior state intact.
 */
export function postgresOutputFactsStore(tx: SqlExecutor): OutputFactsStore {
  return {
    async previousState(scope) {
      const { rows } = await tx.query<{ lo: string | null; hi: string | null; v: string | number | null }>(
        `SELECT ${CH_TEXT("s.min_ts")} AS lo, ${CH_TEXT("s.max_ts")} AS hi, s.source_version AS v
           FROM sessions.output_facts_state($1, $2, $3, $4::text[], $5) AS s`,
        [...scopeArgs(scope), scope.canonicalId],
      );
      const r = rows[0];
      return { range: r?.lo && r.hi ? { min: r.lo, max: r.hi } : null, version: Number(r?.v ?? 0) };
    },
    async reconcile(scope, input, sourceVersion) {
      const v = sourceVersionOf(sourceVersion);
      // `version` is the ClickHouse row version: the Postgres rows carry source_version instead.
      const events = input.eventFacts.map(({ version: _v, ...row }) => ({ ...row, ts: pgInstant(row.ts) }));
      const facts = input.outputFacts.map(({ version: _v, ...row }) => ({ ...row, ts: pgInstant(row.ts) }));
      const { rows } = await tx.query<{ applied: boolean; inserted: string | number; deleted: string | number }>(
        `SELECT applied, inserted, deleted FROM sessions.output_facts_reconcile($1, $2, $3, $4::text[], $5, $6::jsonb, $7::jsonb, $8, $9::bigint)`,
        [...scopeArgs(scope), scope.canonicalId, JSON.stringify(events), JSON.stringify(facts), input.complete, v],
      );
      const r = rows[0];
      const applied = r?.applied === true;
      return {
        inserted: Number(r?.inserted ?? 0),
        deleted: Number(r?.deleted ?? 0),
        complete: applied && input.complete,
        reasons: applied ? [...new Set(input.reasons)] : [...new Set([...input.reasons, "output_facts_stale_source_version"])],
        applied,
      };
    },
    async recordState(scope, s) {
      const v = sourceVersionOf(s.sourceVersion);
      const { rows } = await tx.query<{ ok: boolean }>(
        `SELECT sessions.output_facts_record_state($1, $2, $3, $4::text[], $5, $6, $7::timestamptz, $8::timestamptz, $9::bigint, $10::timestamptz) AS ok`,
        [...scopeArgs(scope), scope.canonicalId, s.complete && (s.phase ?? "final") === "final",
          s.range ? pgInstant(s.range.min) : null, s.range ? pgInstant(s.range.max) : null, v, s.at.toISOString()],
      );
      return rows[0]?.ok === true;
    },
    async erase(scope) {
      await tx.query(`SELECT sessions.output_facts_erase($1, $2, $3, $4::text[])`, scopeArgs(scope));
    },
    // The tables come with the migration the consumer requires before it serves.
    schemaReady: async () => ({ ready: true, present: true }),
  };
}

// ---- the staged writer (staged reconcile, slice 2c; migration 20261105062140) ------------------
//
// A staged run's facts, on the run's own transactions and lease: stageOutputFactsChunk in the
// transaction of each chunk of the facts pass (the whole read's facts input, delivered once per
// attempt at the end of the want phase, numbered from 0, the last one flagged),
// sealOutputFactsWant in the transaction that ends the want phase, and
// finishStagedOutputFacts in the final transaction BEFORE search_reconcile_finish_v1 (which drops
// the stage). The whole-set path (postgresOutputFactsStore.reconcile) is unchanged.

/** The scope a staged run's facts belong to: the run's scope and its canonical id. */
export interface StagedOutputFactsScope extends OutputFactsStagedScope {
  /** The scope's own session id (with the canonical id and every projected one, the facts' session id set). */
  sessionId: string;
}

/**
 * The stage does not cover the run's whole want pass (facts were off for part of it, or the hook
 * was attached late): nothing was written. The run must go round again from its first batch: as a
 * StagedRunRestart thrown from onFinish, the module rolls the final transaction back and abandons
 * the run with this reason, which drops the run's facts stage (the reset trigger); the next attempt
 * has the hook from its first batch.
 */
export class StagedOutputFactsIncompleteError extends StagedRunRestart {
  constructor() {
    super("output_facts_stage_incomplete");
    this.name = "StagedOutputFactsIncompleteError";
  }
}

const hex = (v: string) => createHash("sha256").update(v).digest("hex");
/** Items one facts chunk may stage (staged_facts_stage_v1 takes at most this many per call). */
export const MAX_CHUNK_ITEMS = 100_000;
/**
 * The largest facts chunk (projection rows plus records) that always fits one staged_facts_stage_v1
 * call by item count. The staged run clamps its chunk size to it (search-index.ts, Math.min).
 */
export const MAX_FACTS_CHUNK_INPUTS = Math.floor(MAX_CHUNK_ITEMS / OUTPUT_FACTS_MAX_ITEMS_PER_INPUT);
/**
 * The most UTF-8 bytes one staged_facts_stage_v1 parameter may hold: half of PostgreSQL's jsonb limit
 * (about 256 MB, 54000 above it), so a chunk never reaches the database's own refusal. A chunk holds at
 * most FACTS_CHUNK_MAX_BYTES (16 MiB) of its inputs' UTF-8 JSON text and MAX_FACTS_CHUNK_INPUTS inputs;
 * its entries are at most 2 x the input bytes (an input's items' bodies: the event row plus at most one
 * call, parent, candidate or record, each a subset of the input) x 2 (a body is JSON text inside JSON: at
 * worst every character is a quote or backslash and is escaped; characters above ASCII are not escaped)
 * = 64 MiB, plus about 2 KB of fixed part per input (field names, hashes, keys; 1.8 KB measured) x 25000 =
 * about 49 MiB: about 113 MiB, under this limit. Only an input larger than FACTS_CHUNK_MAX_BYTES by itself,
 * sent as a chunk alone, can pass it; it then fails by name (output_facts_chunk_too_large ->
 * OUTPUT_FACTS_CHUNK_UNSTAGEABLE), never by the database's limit.
 */
export const MAX_STAGE_CALL_BYTES = 128 * 1024 * 1024;
/** Tests only: the send limit stageEntries checks (MAX_STAGE_CALL_BYTES in production). */
export const stagedFactsSendLimit = { bytes: MAX_STAGE_CALL_BYTES };
/** Items per call for the final phase's own facts. */
const FINAL_CALL = 500;
const READ_PAGE = 1000;

type StageEntry = { k: string; i?: string; g?: string; m?: string; b: string };

/** The Postgres row of an event or fact (no ClickHouse version, a Postgres instant), as postgresOutputFactsStore.reconcile sends it. */
const pgRow = <T extends { version: number; ts: string }>({ version: _v, ...row }: T) => ({ ...row, ts: pgInstant(row.ts) });

function stageEntry(item: OutputFactsStagedItem): StageEntry {
  switch (item.kind) {
    case "event":
    case "fact":
      return { k: item.kind, b: JSON.stringify(pgRow(item.row)) };
    case "candidate": {
      const b = JSON.stringify({ group: item.group, match: item.match, ...item.body });
      return { k: "candidate", i: hex(b), g: hex(item.group), m: hex(item.match), b };
    }
    case "call":
    case "parent": {
      const b = JSON.stringify(item.body);
      return { k: item.kind, i: hex(b), g: hex(item.group), m: hex(item.match), b };
    }
    case "record": {
      const b = JSON.stringify(item.body);
      return { k: "record", i: hex(b), b };
    }
    case "session":
      return { k: "session", i: hex(item.sessionId), b: JSON.stringify({ session_id: item.sessionId }) };
    case "note":
      return { k: "note", i: hex(item.reason), b: JSON.stringify({ reason: item.reason }) };
  }
}


async function stageEntries(tx: SqlExecutor, run: StagedRunRef, entries: StageEntry[], chunk: StagedFactsChunk | null): Promise<number> {
  const call = async (batch: StageEntry[], seq: number | null, last: boolean | null) => {
    const text = JSON.stringify(batch);
    // The authoritative size check, before anything is sent (see MAX_STAGE_CALL_BYTES).
    if (Buffer.byteLength(text, "utf8") > stagedFactsSendLimit.bytes) throw new Error("output_facts_chunk_too_large");
    const { rows } = await tx.query<{ n: number | string }>(
      "SELECT sessions.staged_facts_stage_v1($1::bigint, $2::uuid, $3::jsonb, $4::integer, $5::boolean) AS n",
      [run.runId, run.nonce, text, seq, last],
    );
    return Number(rows[0]?.n ?? 0);
  };
  // A facts chunk is exactly one call, items or not: the call is the chain's step (the function takes up
  // to MAX_CHUNK_ITEMS; the wiring's chunk size bounds it, a few items per projection row).
  if (chunk) {
    if (!Number.isSafeInteger(chunk.seq) || chunk.seq < 0) throw new Error("output_facts_invalid_chunk");
    if (entries.length > MAX_CHUNK_ITEMS) throw new Error("output_facts_chunk_too_large");
    return call(entries, chunk.seq, chunk.last);
  }
  // The final phase's own facts: in calls of FINAL_CALL items, outside the chain.
  let staged = 0;
  for (let i = 0; i < entries.length; i += FINAL_CALL) staged += await call(entries.slice(i, i + FINAL_CALL), null, null);
  return staged;
}

/**
 * One chunk of the facts pass: its facts and the inputs the finish step needs, staged on the
 * chunk's own transaction. Chunk 0 drops what the run had staged and starts over (a facts pass is
 * delivered whole on each attempt); a later chunk must be the next number. Idempotent per run.
 */
export async function stageOutputFactsChunk(
  tx: SqlExecutor,
  run: StagedRunRef,
  scope: StagedOutputFactsScope,
  events: StagedFactsEvents,
  chunk: StagedFactsChunk,
  sourceVersion: number,
  /**
   * The record bytes staged so far this attempt (`used` weighted, `counted` as UTF-8), their bound and the
   * weight of a record with any code unit above 0xFF; the caller resets both sums at chunk 0.
   */
  recordBytes?: { used: number; counted: number; cap: number; wideWeight: number },
): Promise<number> {
  const items = outputFactsBatchStep(scope, events, sourceVersionOf(sourceVersion));
  const entries = items.map(stageEntry);
  if (recordBytes) {
    // Counted before anything is sent, so a run past the bound stops before the stage holds its records.
    for (const e of entries) {
      if (e.k !== "record") continue;
      const bytes = Buffer.byteLength(e.b, "utf8");
      recordBytes.counted += bytes;
      recordBytes.used += WIDE_TEXT.test(e.b) ? bytes * recordBytes.wideWeight : bytes;
    }
    if (recordBytes.used > recordBytes.cap)
      throw deterministic(new PipelineError(
        OUTPUT_FACTS_CHUNK_UNSTAGEABLE,
        `staged output facts: run bytes bound: ${recordBytes.counted} record bytes (${Math.ceil(recordBytes.used)} weighted) staged this attempt, bound ${recordBytes.cap} (chunk ${chunk.seq})`,
        false,
      ));
  }
  try {
    return await stageEntries(tx, run, entries, chunk);
  } catch (error) {
    throw unstageable(error, { seq: chunk.seq, items: entries.length }) ?? error;
  }
}

/** Any UTF-16 code unit above 0xFF: the runtime keeps such a string 16-bit (twice the heap per character). */
const WIDE_TEXT = /[^\u0000-\u00ff]/;

/** The reason a staged run abandons with when a facts chunk cannot be staged (see unstageable). */
export const OUTPUT_FACTS_CHUNK_UNSTAGEABLE = "output_facts_chunk_unstageable";

/**
 * The size refusals that will recur for the same source and this process's bounds (54000, 54001, a
 * RangeError, the send limit, the run's record bytes): marked where they are built, so the G2 fast-fail
 * (search-index.ts StagedRefusals) caches only these. 53200 out_of_memory abandons the run the same way
 * but is not marked: the database may have room on the next tick.
 */
const deterministicRefusals = new WeakSet<object>();
function deterministic<E extends object>(error: E): E {
  deterministicRefusals.add(error);
  return error;
}
/** Whether this abandon reason's error is a refusal that recurs for the same source (see deterministic). */
export function isDeterministicRefusal(error: unknown): boolean {
  return typeof error === "object" && error !== null && deterministicRefusals.has(error);
}

/**
 * A chunk that cannot be staged as sent: the database refuses its size (54000 program_limit_exceeded,
 * 54001 statement_too_complex, 53200 out_of_memory), the client cannot build it (a RangeError: string or
 * array length), or it is over the send limit. As a non-retryable PipelineError the staged run abandons
 * with OUTPUT_FACTS_CHUNK_UNSTAGEABLE (stagedSearchScope's abandonOn) instead of failing the same chunk on
 * every tick; all but 53200 are also marked deterministic (cached by the G2 fast-fail). Anything else (a
 * lost lease, a connection error) is returned as null and rethrown as it is.
 */
export function unstageable(error: unknown, at: { seq: number; items: number } = { seq: -1, items: -1 }): PipelineError | null {
  const code = (error as { code?: unknown } | null)?.code;
  const sized = code === "54000" || code === "54001" || code === "53200";
  const local = error instanceof RangeError || (error instanceof Error && error.message === "output_facts_chunk_too_large");
  if (!sized && !local) return null;
  // Counts and codes only, never content: which bound (item size), where, and why.
  const why = sized ? `database ${String(code)}` : error instanceof RangeError ? "client RangeError" : `send limit ${stagedFactsSendLimit.bytes} bytes`;
  const where = at.seq < 0 ? "final step" : `chunk ${at.seq}, ${at.items} items`;
  const refusal = new PipelineError(OUTPUT_FACTS_CHUNK_UNSTAGEABLE, `staged output facts: item size bound: ${where}, ${why}`, false);
  return code === "53200" ? refusal : deterministic(refusal);
}

/** The want phase ended (onWantDone, in the advance transaction). Returns whether the stage is whole. */
export async function sealOutputFactsWant(tx: SqlExecutor, run: StagedRunRef): Promise<boolean> {
  const { rows } = await tx.query<{ whole: boolean }>("SELECT sessions.staged_facts_seal_v1($1::bigint, $2::uuid) AS whole", [run.runId, run.nonce]);
  return rows[0]?.whole === true;
}

async function readStaged<T>(tx: SqlExecutor, run: StagedRunRef, kind: string, group: string | null = null, match: string[] | null = null): Promise<T[]> {
  const out: T[] = [];
  let after: string | null = null;
  for (;;) {
    const { rows }: { rows: Array<{ seq: string | number; body: string }> } = await tx.query<{ seq: string | number; body: string }>(
      "SELECT seq, body FROM sessions.staged_facts_read_v1($1::bigint, $2::uuid, $3, $4, $5::text[], $6::bigint, $7)",
      [run.runId, run.nonce, kind, group, match, after, READ_PAGE],
    );
    for (const r of rows) out.push(JSON.parse(r.body) as T);
    if (rows.length < READ_PAGE) return out;
    after = String(rows.at(-1)!.seq);
  }
}

export interface StagedOutputFactsResult extends OutputFactsReconcileResult {
  /** The scope the facts were reconciled for (the session id set includes every projected id). */
  scope: OutputFactsScope;
  range: FactsRange;
}

/**
 * The final transaction's facts step, before search_reconcile_finish_v1: decide what needs the
 * whole session, stage it, then reconcile from the stage with output_facts_reconcile's version
 * rule and result, and record the scope's state as indexSearchScope does for the whole-set path.
 * Throws StagedOutputFactsIncompleteError, before writing anything, when the stage does not cover
 * the whole want pass: a partial stage is never read as the session's facts.
 */
export async function finishStagedOutputFacts(
  tx: SqlExecutor,
  run: StagedRunRef,
  scope: StagedOutputFactsScope,
  pass: { sourceComplete: boolean; sourceVersion: number; at: Date },
): Promise<StagedOutputFactsResult> {
  const v = sourceVersionOf(pass.sourceVersion);
  const status = async () => (await tx.query<{ whole: boolean; lo: string | null; hi: string | null }>(
    `SELECT s.whole, ${CH_TEXT("s.min_ts")} AS lo, ${CH_TEXT("s.max_ts")} AS hi FROM sessions.staged_facts_status_v1($1::bigint, $2::uuid) AS s`,
    [run.runId, run.nonce],
  )).rows[0];
  if ((await status())?.whole !== true) throw new StagedOutputFactsIncompleteError();

  // The read-back, the decision and the final staging: a size failure there (the final call over the send
  // limit, the database refusing it, a RangeError building or parsing it) is as deterministic as a chunk's,
  // so it abandons the run by name (unstageable) instead of releasing it to fail the same way next tick.
  const decided = await (async () => {
    try {
      const seen = await readStaged<{ session_id: string }>(tx, run, "session");
      const fScope: OutputFactsScope = {
        orgId: scope.orgId,
        workspaceId: scope.workspaceId,
        harness: scope.harness,
        sessionIds: [...new Set([scope.sessionId, scope.canonicalId, ...seen.map((s) => s.session_id)])],
        canonicalId: scope.canonicalId,
      };
      const notes = (await readStaged<{ reason: string }>(tx, run, "note")).map((n) => n.reason);
      const candidates: OutputFactsStagedCandidate[] = [];
      for (const c of await readStaged<{ group: string; match: string; result: Record<string, unknown>; facts: OutputFactRow[] }>(tx, run, "candidate")) {
        const g = hex(c.group);
        const calls = await readStaged<Record<string, unknown>>(tx, run, "call", g, [hex(c.match)]);
        const messages = callMessageIds(calls);
        const parents = messages.length ? await readStaged<Record<string, unknown>>(tx, run, "parent", g, messages.map(hex)) : [];
        candidates.push({ result: c.result, facts: c.facts, sessionEvents: [...calls, ...parents] });
      }
      const records = await readStaged<RelayhistoryStreamRecord>(tx, run, "record");
      const finish = outputFactsFinishStep(fScope, { candidates, records, notes, sourceComplete: pass.sourceComplete }, v);
      if (finish.facts.length) await stageEntries(tx, run, finish.facts.map((row) => stageEntry({ kind: "fact", row })), null);
      return { fScope, finish };
    } catch (error) {
      throw unstageable(error) ?? error;
    }
  })();
  const { fScope, finish } = decided;

  const store = postgresOutputFactsStore(tx);
  const prev = await store.previousState(fScope);
  const st = await status();
  const range: FactsRange = st?.lo && st.hi ? { min: st.lo, max: st.hi } : null;
  const { rows } = await tx.query<{ applied: boolean; inserted: string | number; deleted: string | number }>(
    `SELECT applied, inserted, deleted FROM sessions.staged_facts_reconcile_v1($1::bigint, $2::uuid, $3, $4, $5, $6::text[], $7, $8, $9::bigint)`,
    [run.runId, run.nonce, ...scopeArgs(fScope), fScope.canonicalId, finish.complete, v],
  );
  const r = rows[0];
  const applied = r?.applied === true;
  const result: StagedOutputFactsResult = {
    inserted: Number(r?.inserted ?? 0),
    deleted: Number(r?.deleted ?? 0),
    complete: applied && finish.complete,
    reasons: applied ? [...new Set(finish.reasons)] : [...new Set([...finish.reasons, "output_facts_stale_source_version"])],
    applied,
    scope: fScope,
    range,
  };
  if (applied) {
    // indexSearchScope's Postgres branch, unchanged: a complete pass records its own span, any other the union with the prior one.
    const prior = prev.range;
    const span = prior && range ? { min: prior.min < range.min ? prior.min : range.min, max: prior.max > range.max ? prior.max : range.max } : (range ?? prior);
    await store.recordState(fScope, { complete: result.complete, at: pass.at, range: result.complete ? range : span, sourceVersion: v });
  }
  return result;
}

/** The 062140 writers the staged facts hooks call. */
export const STAGED_OUTPUT_FACTS_FUNCTIONS = [
  "sessions.staged_facts_stage_v1(bigint,uuid,jsonb,integer,boolean)",
  "sessions.staged_facts_seal_v1(bigint,uuid)",
  "sessions.staged_facts_read_v1(bigint,uuid,text,text,text[],bigint,integer)",
  "sessions.staged_facts_status_v1(bigint,uuid)",
  "sessions.staged_facts_reconcile_v1(bigint,uuid,text,text,text,text[],text,boolean,bigint)",
] as const;

/**
 * Startup with the staged reconcile and RELAYHISTORY_SELFHOST_OUTPUT_FACTS both on: every 062140 writer
 * exists, or the consumer refuses to start naming the migration (never on the first large scope).
 */
export async function assertStagedOutputFactsSchema(db: SqlExecutor): Promise<void> {
  const { rows } = await db.query<{ fn: string }>(
    "SELECT fn FROM unnest($1::text[]) AS fn WHERE to_regprocedure(fn) IS NULL",
    [[...STAGED_OUTPUT_FACTS_FUNCTIONS]],
  );
  if (rows.length)
    throw new Error(
      "pipeline consumer: the staged reconcile is on (the default; SEARCH_INDEX_STAGED_RECONCILE_ENABLED=false turns it off) and with RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true requires migration 20261105062140_staged_output_facts.sql (missing sessions.staged_facts_* functions)",
    );
}
