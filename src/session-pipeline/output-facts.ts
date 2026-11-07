/**
 * Producer for the Output and daily-digest facts (docs/output-digest-clickhouse-design.md
 * §3): one session scope's canonical S3 read, as `indexSearchScope` already holds it,
 * becomes the scope's rows in `transcript_event_facts` and `session_output_facts`
 * (output-facts-schema.ts). Default off: `indexSearchScope` calls this only with
 * `deps.outputFacts === true` (Lambda env SEARCH_INDEX_OUTPUT_FACTS=1).
 *
 * Exactness. Every per-row decision relayhistory's `getSessionOutput`
 * (lib/session-output.ts @ c7de2163) makes inside one session is made here, with
 * the whole session in memory:
 *   - message qualification and the message id COALESCE;
 *   - the Git success-header + shortstat regex over tool results, and the
 *     "exactly one logical tool_call, all of whose calls run `git commit`" check;
 *   - numstat normalization (arrays, single entries, objects of entries, binary rows).
 * What depends on the query scope (dedup across sessions, prefix resolution, the
 * window) stays at read time (session-read-api/output-query.ts).
 *
 * Coverage. Facts are the WHOLE scope, not the transcript_events window: commit
 * candidates from links and outcomes are unwindowed in relayhistory. Sources:
 *   convergence_events projection rows   message, tool_result commit, event facts
 *   session_commit_links projection rows commit (origin commit_link)
 *   legacy_session_outcome records       outcome + commit (origin outcome)
 *   legacy_conversation_turn + live      legacy_turn_message
 *   conversation_turn records
 *
 * Reconcile: ReplacingMergeTree(version) per identity, `row_id` = sha256 of the
 * identity. A complete pass inserts new or changed rows and deletes identities S3
 * no longer yields; an incomplete pass inserts only and never deletes. Erasure
 * (no live pointer) deletes the scope from both tables.
 *
 * Nothing here logs content; errors carry codes only.
 */
import { createHash } from "node:crypto";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { arrayLiteral } from "./vendor/search-index/clickhouse.ts";
import { EVENT_FACTS_TABLE, OUTPUT_FACTS_SESSION_STATE_TABLE, OUTPUT_FACTS_TABLE } from "./output-facts-schema.ts";
import { canonicalLegacy } from "../session-read-api/canonical-legacy.ts";
import { canonicalLiveTurns, mergeTurnRows } from "../session-read-api/canonical-turns.ts";
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

type Json = Record<string, unknown>;

export interface OutputFactsScope {
  orgId: string;
  workspaceId: string;
  /** The harness: every fact of the scope carries it as `scope_source`. */
  harness: string;
  /** Session ids the scope may hold rows under (the scope's and its canonical id). */
  sessionIds: string[];
  /** The canonical session id facts are written under. */
  canonicalId: string;
}

export interface EventFactRow {
  org_id: string; workspace_id: string; ts: string; session_id: string; source: string;
  machine_id: string; kind: string; event_id: string;
  project_id: string | null; task_ref_id: string | null; task_ref_empty: number; branch: string | null;
  model: string | null; provider: string | null;
  input_tokens: number; output_tokens: number; reasoning_tokens: number; cache_read_tokens: number; cache_create_tokens: number;
  cache_create_5m_tokens: number | null; cache_create_1h_tokens: number | null; cost_usd_micros: number | null;
  files_touched: Array<string | null>;
  row_id: string; fingerprint: string; version: number;
}

export interface OutputFactRow {
  org_id: string; workspace_id: string; fact_kind: string; ts: string; scope_source: string; source: string; session_id: string;
  fact_key: string; origin: string; row_key: string;
  added: number | null; removed: number | null; reverted: number;
  row_id: string; fingerprint: string; version: number;
}

export interface OutputFacts {
  events: EventFactRow[];
  facts: OutputFactRow[];
  /** False when a source could not be read exactly: the reconcile then never deletes. */
  complete: boolean;
  reasons: string[];
  /** The facts' time span (ClickHouse DateTime64(6) text); null when the scope has none. */
  range?: { min: string; max: string } | null;
}

/** Postgres `->>` on a jsonb value: strings as is, scalars as text, objects as JSON, null/absent as NULL. */
export function jtext(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") return value;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
const nullIfEmpty = (v: string | null) => (v === "" ? null : v);
/** Postgres btrim(x) with no characters argument trims spaces only. */
const btrimSpaces = (v: string) => v.replace(/^ +| +$/g, "");
const payloadOf = (row: Json): Json => {
  const record = row.record;
  if (!record || typeof record !== "object" || Array.isArray(record)) return {};
  const payload = (record as Json).payload;
  return payload && typeof payload === "object" && !Array.isArray(payload) ? (payload as Json) : {};
};
const ptext = (row: Json, key: string) => jtext(payloadOf(row)[key]);

/** An ISO instant with six fractional digits ("…T…Z") as ClickHouse DateTime64(6) text, UTC. */
export function chTime(iso: string): string {
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) throw new Error("output_facts_invalid_time");
  const fraction = /:\d{2}\.(\d+)/.exec(iso)?.[1] ?? "";
  const seconds = new Date(ms).toISOString().slice(0, 19).replace("T", " ");
  return `${seconds}.${fraction.slice(0, 6).padEnd(6, "0")}`;
}
/** Microseconds since the epoch, for ordering two instants of chTime precision. */
const micros = (iso: string) => BigInt(Math.floor(Date.parse(iso) / 1000)) * 1_000_000n + BigInt((/:\d{2}\.(\d+)/.exec(iso)?.[1] ?? "").slice(0, 6).padEnd(6, "0"));

// relayhistory lib/session-output.ts, verbatim patterns. Postgres ARE `\m`/`\M` are
// word starts/ends; JS `\b` over the same [A-Za-z0-9_] word class.
const COMMIT_OUTPUT = /(?:^|\n)\[[^\]\n]+ ([0-9a-f]{7,40})\][^\n]*\n[ \t]*[0-9]+ files? changed(?:, ([0-9]+) insertions?\(\+\))?(?:, ([0-9]+) deletions?\(-\))?/g;
const COMMIT_COMMAND = /\bgit\b[^\n]*\bcommit\b/;
const SHELL_TOOLS = new Set(["exec_command", "exec", "Bash", "bash", "shell", "run_shell_command"]);
const DIGITS = /^[0-9]{1,12}$/;

/** Every `[<branch> <sha>] …\n N files changed, …` match: sha, insertions, deletions (as text, as the SQL does). */
export function commitMatches(content: string): Array<{ sha: string; added: string | null; deleted: string | null }> {
  return [...content.matchAll(COMMIT_OUTPUT)].map((m) => ({ sha: m[1]!, added: m[2] ?? null, deleted: m[3] ?? null }));
}

const isObject = (v: unknown): v is Json => typeof v === "object" && v !== null && !Array.isArray(v);
const STAT_FIELDS = new Set(["added", "deleted", "removed", "additions", "deletions"]);

/**
 * The `stat_entries` + `normalized_stats` CTEs: additions and deletions summed
 * separately; a side is NULL when any entry on it is neither digits, `-`, nor a
 * binary entry (path set, both counts JSON null), or when there are no entries.
 */
export function normalizeStats(stats: unknown): { added: number | null; removed: number | null } {
  let entries: unknown[] | null;
  if (Array.isArray(stats)) entries = stats;
  else if (isObject(stats)) {
    const single = Object.entries(stats).some(([k, v]) => STAT_FIELDS.has(k) && !(typeof v === "object" && v !== null));
    if (single) entries = [stats];
    else {
      const values = Object.values(stats);
      // jsonb_agg over zero rows is NULL.
      entries = values.length ? values : null;
    }
  } else entries = null;
  if (entries === null) return { added: null, removed: null };
  const side = (pick: (e: Json) => string | null) => {
    let ok = true;
    let sum = 0;
    for (const raw of entries!) {
      const e = isObject(raw) ? raw : null;
      const v = e ? pick(e) : null;
      const binary = !!e && typeof e.path === "string" && e.additions === null && e.deletions === null;
      const fine = (v !== null && (DIGITS.test(v) || v === "-")) || binary;
      if (!fine) ok = false;
      if (v !== null && DIGITS.test(v)) sum += Number(v);
    }
    return ok ? sum : null;
  };
  return {
    added: side((e) => jtext(e.additions) ?? jtext(e.added)),
    removed: side((e) => jtext(e.deletions) ?? jtext(e.removed) ?? jtext(e.deleted)),
  };
}

/** relayhistory rollups.ts branchSql: NULL for `{}` or no `@`, else the text after the first `@`. */
export function branchOf(taskRef: unknown): string | null {
  if (isObject(taskRef) && Object.keys(taskRef).length === 0) return null;
  const id = isObject(taskRef) ? jtext(taskRef.id) : null;
  if (id === null) return null;
  const at = id.indexOf("@");
  return at < 0 ? null : id.slice(at + 1);
}

const sha256 = (parts: unknown[]) => createHash("sha256").update(JSON.stringify(parts)).digest("hex");
const int = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? Math.trunc(v) : typeof v === "string" && /^-?\d+$/.test(v) ? Number(v) : 0);
const nint = (v: unknown) => (v === null || v === undefined ? null : int(v));

/** `call.ts <= e.ts` etc. need microsecond order: compare canonical instants. */
const le = (a: string, b: string) => micros(a) <= micros(b);

/**
 * The tool_result → tool_call match of `getSessionOutput`: some calls of this
 * session match by tool_use_id (and parent/message id, and turn through a parent
 * session_event); together they must be ONE logical call, and every one must be a
 * shell tool whose args run `git commit`.
 */
function resultHasCommitCall(result: Json, sessionEvents: Json[]): boolean {
  const toolUse = nullIfEmpty(ptext(result, "tool_use_id"));
  if (toolUse === null) return false;
  const parentId = nullIfEmpty(ptext(result, "parent_id"));
  const messageId = nullIfEmpty(ptext(result, "message_id"));
  const turnId = nullIfEmpty(ptext(result, "turn_id"));
  const rawTurn = ptext(result, "turn_id");
  const calls = sessionEvents.filter((call) => {
    if (call.kind !== "tool_call" || !le(String(call.ts), String(result.ts))) return false;
    if (nullIfEmpty(ptext(call, "tool_use_id")) !== toolUse) return false;
    const callMessage = ptext(call, "message_id");
    if (!(parentId === null || parentId === callMessage || (messageId !== null && messageId === nullIfEmpty(callMessage)))) return false;
    if (turnId === null) return true;
    const callMessageId = nullIfEmpty(callMessage);
    if (callMessageId === null) return false;
    return sessionEvents.some((parent) =>
      parent.kind === "session_event" && nullIfEmpty(ptext(parent, "message_id")) === callMessageId && ptext(parent, "turn_id") === rawTurn);
  });
  if (!calls.length) return false;
  const logical = new Set(calls.map((call) => {
    const m = ptext(call, "message_id");
    return JSON.stringify([m, call.tool_name ?? null, ptext(call, "args_json"), nullIfEmpty(m) === null ? micros(String(call.ts)).toString() : null]);
  }));
  if (logical.size !== 1) return false;
  return calls.every((call) => {
    const args = ptext(call, "args_json");
    return typeof call.tool_name === "string" && SHELL_TOOLS.has(call.tool_name) && args !== null && COMMIT_COMMAND.test(args);
  });
}

/** Outcome and legacy-turn sources of the scope, grouped per session id they name. */
function legacyAndTurns(records: RelayhistoryStreamRecord[], scope: OutputFactsScope): { rows: Array<{ table: string; row: Json }>; ok: boolean } {
  const bySession = new Map<string, RelayhistoryStreamRecord[]>();
  const relevant = new Set(["legacy_conversation_turn", "legacy_session_outcome", "legacy_session_link", "legacy_scope", "conversation_turn"]);
  for (const r of records) {
    if (!relevant.has(r.kind)) continue;
    const sid = jtext(r.payload?.session_id);
    if (sid === null || !scope.sessionIds.includes(sid)) return { rows: [], ok: false };
    bySession.set(sid, [...(bySession.get(sid) ?? []), r]);
  }
  const rows: Array<{ table: string; row: Json }> = [];
  try {
    for (const [sessionId, list] of bySession) {
      const identity = { orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.harness, sessionId };
      const legacy = canonicalLegacy(list, identity);
      rows.push(...mergeTurnRows(legacy.rows, canonicalLiveTurns(list, identity)));
    }
  } catch {
    return { rows: [], ok: false };
  }
  return { rows, ok: true };
}

/**
 * The scope's facts. `projectionRows` and `records` are `readCanonicalScope`'s
 * (canonical-projection.ts ScopeRead); `scopeComplete` is that read's `complete`.
 */
export function outputFactsFromScope(
  scope: OutputFactsScope,
  read: { projectionRows: Array<{ table: string; row: Json }>; records: RelayhistoryStreamRecord[]; complete: boolean },
  version: number,
): OutputFacts {
  const reasons: string[] = [];
  let complete = read.complete;
  const base = { org_id: scope.orgId, workspace_id: scope.workspaceId, version };
  const facts: OutputFactRow[] = [];
  const pushFact = (f: Omit<OutputFactRow, "org_id" | "workspace_id" | "version" | "row_id" | "fingerprint" | "scope_source">) => {
    const row = { ...base, scope_source: scope.harness, ...f };
    const identity = [row.fact_kind, row.ts, row.source, row.session_id, row.fact_key, row.origin, row.row_key];
    facts.push({ ...row, row_id: sha256(identity), fingerprint: sha256([...identity, row.added, row.removed, row.reverted]).slice(0, 16) });
  };
  const events = read.projectionRows.filter((r) => r.table === "convergence_events").map((r) => r.row);
  const eventRows: EventFactRow[] = [];
  const bySession = new Map<string, Json[]>();
  for (const e of events) {
    const key = JSON.stringify([e.source, e.session_id]);
    bySession.set(key, [...(bySession.get(key) ?? []), e]);
  }
  try {
    for (const e of events) {
      const ts = chTime(String(e.ts));
      const taskRef = isObject(e.task_ref) ? e.task_ref : {};
      const files = Array.isArray(e.files_touched) ? e.files_touched.map(jtext) : [];
      const identity = [ts, e.session_id, e.source, e.machine_id, e.kind, e.event_id];
      const row = {
        ...base,
        ts,
        session_id: String(e.session_id),
        source: String(e.source),
        machine_id: String(e.machine_id),
        kind: String(e.kind),
        event_id: String(e.event_id),
        project_id: jtext(e.project_id),
        task_ref_id: jtext(taskRef.id),
        task_ref_empty: Object.keys(taskRef).length === 0 ? 1 : 0,
        branch: branchOf(taskRef),
        model: jtext(e.model),
        provider: jtext(e.provider),
        input_tokens: int(e.input_tokens),
        output_tokens: int(e.output_tokens),
        reasoning_tokens: int(e.reasoning_tokens),
        cache_read_tokens: int(e.cache_read_tokens),
        cache_create_tokens: int(e.cache_create_tokens),
        cache_create_5m_tokens: nint(e.cache_create_5m_tokens),
        cache_create_1h_tokens: nint(e.cache_create_1h_tokens),
        cost_usd_micros: nint(e.cost_usd_micros),
        files_touched: files,
      };
      const { version: _v, ...covered } = row;
      eventRows.push({ ...row, row_id: sha256(identity), fingerprint: sha256([covered]).slice(0, 16) });

      // messages CTE
      const content = jtext(e.content);
      const payloadKind = ptext(e, "kind");
      if (
        e.actor_role === "assistant" && btrimSpaces(content ?? "") !== "" &&
        ["session_event", "assistant", "text"].includes(String(e.kind)) &&
        (payloadKind === "text" || (payloadKind === null && ["assistant", "text"].includes(String(e.type))))
      ) {
        const messageId = nullIfEmpty(jtext(e.request_key)) ?? nullIfEmpty(ptext(e, "message_id")) ?? jtext(e.delivery_record_id) ?? `${e.machine_id}:${e.event_id}`;
        pushFact({ fact_kind: "message", ts, source: String(e.source), session_id: String(e.session_id), fact_key: messageId, origin: "", row_key: `${e.machine_id}\u001f${e.kind}\u001f${e.event_id}`, added: null, removed: null, reverted: 0 });
      }
      // commit_candidates, third branch
      if (e.actor_role === "tool_result" && content !== null) {
        const matches = commitMatches(content);
        if (matches.length && resultHasCommitCall(e, bySession.get(JSON.stringify([e.source, e.session_id])) ?? [])) {
          matches.forEach((m, i) => {
            const stats = normalizeStats({ added: m.added ?? "0", deleted: m.deleted ?? "0" });
            pushFact({ fact_kind: "commit", ts, source: String(e.source), session_id: String(e.session_id), fact_key: m.sha, origin: "tool_result", row_key: `${e.machine_id}\u001f${e.event_id}\u001f${i}`, added: stats.added, removed: stats.removed, reverted: 0 });
          });
        }
      }
    }
    // commit_candidates, first branch: session_commit_links
    for (const { table, row } of read.projectionRows) {
      if (table !== "session_commit_links") continue;
      const sha = jtext(row.commit_sha);
      const at = jtext(row.linked_at) ?? jtext(row.received_at);
      if (sha === null || at === null) { complete = false; reasons.push("output_facts_commit_link_invalid"); continue; }
      const stats = normalizeStats(row.numstat);
      pushFact({ fact_kind: "commit", ts: chTime(at), source: scope.harness, session_id: scope.canonicalId, fact_key: sha.toLowerCase(), origin: "commit_link", row_key: String(row.record_id ?? sha), ...stats, reverted: 0 });
    }
  } catch (error) {
    complete = false;
    reasons.push(error instanceof Error && error.message.startsWith("output_facts_") ? error.message : "output_facts_projection_invalid");
  }

  // Legacy outcomes/turns are contained like the projection: a malformed record (a missing
  // timestamp, an unparseable clock) marks the facts inexact; it never throws out of the pass,
  // which would abort indexSearchScope after its transcript writes.
  try {
    const legacy = legacyAndTurns(read.records, scope);
    if (!legacy.ok) { complete = false; reasons.push("output_facts_legacy_invalid"); }
    for (const { table, row } of legacy.rows) {
      if (table === "session_outcomes") {
        const sha = String(row.commit_sha).toLowerCase();
        const at = jtext(row.shipped_at) ?? String(row.created_at);
        const ts = chTime(at);
        const stats = normalizeStats(row.numstat_json);
        pushFact({ fact_kind: "commit", ts, source: scope.harness, session_id: scope.canonicalId, fact_key: sha, origin: "outcome", row_key: sha, ...stats, reverted: 0 });
        pushFact({ fact_kind: "outcome", ts, source: scope.harness, session_id: scope.canonicalId, fact_key: sha, origin: "", row_key: sha, added: null, removed: null, reverted: row.reverted === true ? 1 : 0 });
      } else if (table === "conversation_turns") {
        const content = typeof row.content === "string" ? row.content : "";
        if (row.role !== "assistant" || btrimSpaces(content) === "") continue;
        // NOT EXISTS (… m.source = t.source …) compares the STORED turn source.
        const stored = jtext(row.legacy_stored_source) ?? jtext(row.source) ?? scope.harness;
        const at = String(row.ts);
        const ts = chTime(at);
        const key = jtext(row.id) ?? JSON.stringify([row.workspace_id, stored, Number(row.turn_index)]);
        pushFact({ fact_kind: "legacy_turn_message", ts, source: stored, session_id: scope.canonicalId, fact_key: key, origin: "", row_key: key, added: null, removed: null, reverted: 0 });
      }
    }
  } catch (error) {
    complete = false;
    reasons.push(error instanceof Error && error.message.startsWith("output_facts_") ? error.message : "output_facts_legacy_invalid");
  }
  const times = [...eventRows.map((r) => r.ts), ...facts.map((f) => f.ts)].sort();
  return { events: eventRows, facts, complete, reasons: [...new Set(reasons)], range: times.length ? { min: times[0]!, max: times.at(-1)! } : null };
}

export interface ReconcileOutputFactsResult {
  inserted: number;
  deleted: number;
  complete: boolean;
  reasons: string[];
}

const scopeWhere = `org_id = {org:String} AND workspace_id = {workspace:String} AND session_id IN {sessions:Array(String)}`;

async function existing(ch: ClickHouseClient, table: string, sourceColumn: string, scope: OutputFactsScope, limit: number) {
  return ch.select<{ row_id: string; fingerprint: string }>(
    `SELECT row_id, fingerprint FROM ${table} FINAL WHERE ${scopeWhere} AND ${sourceColumn} = {source:String} LIMIT {limit:UInt32}`,
    { params: { org: scope.orgId, workspace: scope.workspaceId, sessions: scope.sessionIds, source: scope.harness, limit }, timeoutMs: 20_000 },
  );
}

async function deleteIds(ch: ClickHouseClient, table: string, sourceColumn: string, scope: OutputFactsScope, ids: string[], batch = 500) {
  for (let i = 0; i < ids.length; i += batch)
    await ch.command(
      `DELETE FROM ${table} WHERE ${scopeWhere} AND ${sourceColumn} = {source:String} AND row_id IN ${arrayLiteral(ids.slice(i, i + batch))}`,
      { params: { org: scope.orgId, workspace: scope.workspaceId, sessions: scope.sessionIds, source: scope.harness }, settings: { lightweight_deletes_sync: 2, max_query_size: 1_048_576 }, timeoutMs: 60_000 },
    );
}

/** Make both fact tables equal `facts` for the scope. Deletes only when `facts.complete` and nothing was truncated. */
export async function reconcileOutputFacts(
  ch: ClickHouseClient,
  scope: OutputFactsScope,
  facts: OutputFacts,
  opts: { maxRows: number; insertBatch: number },
): Promise<ReconcileOutputFactsResult> {
  let inserted = 0;
  let deleted = 0;
  let complete = facts.complete;
  const reasons = [...facts.reasons];
  for (const [table, sourceColumn, rows] of [
    [EVENT_FACTS_TABLE, "source", facts.events],
    [OUTPUT_FACTS_TABLE, "scope_source", facts.facts],
  ] as const) {
    const have = await existing(ch, table, sourceColumn, scope, opts.maxRows + 1);
    const truncated = have.length > opts.maxRows;
    if (truncated) { complete = false; reasons.push("output_facts_clickhouse_truncated"); }
    const known = new Map(have.map((r) => [r.row_id, r.fingerprint]));
    const toInsert = (rows as Array<{ row_id: string; fingerprint: string }>).filter((r) => known.get(r.row_id) !== r.fingerprint);
    for (let i = 0; i < toInsert.length; i += opts.insertBatch)
      await ch.insert(table, toInsert.slice(i, i + opts.insertBatch) as unknown as Record<string, unknown>[], { timeoutMs: 60_000 });
    inserted += toInsert.length;
    if (complete) {
      const wanted = new Set((rows as Array<{ row_id: string }>).map((r) => r.row_id));
      const gone = [...known.keys()].filter((id) => !wanted.has(id));
      await deleteIds(ch, table, sourceColumn, scope, gone);
      deleted += gone.length;
    }
  }
  return { inserted, deleted, complete, reasons: [...new Set(reasons)] };
}

/**
 * The scope's facts state (output-facts-schema.ts OUTPUT_FACTS_SESSION_STATE_TABLE), one row
 * per session id of the scope. Written on every pass with the flag on, complete or not, and
 * by erasure (complete: nothing is owed).
 */
export type FactsRange = { min: string; max: string } | null;

/** The scope's last recorded facts span, for a pass that cannot compute one (a partial read). */
export async function previousOutputFactsRange(ch: ClickHouseClient, scope: OutputFactsScope): Promise<FactsRange> {
  // Aliases must not shadow the columns: WHERE would then name the aggregate (code 184).
  const rows = await ch.select<{ lo: string | null; hi: string | null; n: string | number }>(
    `SELECT toString(min(min_ts)) AS lo, toString(max(max_ts)) AS hi, count() AS n FROM ${OUTPUT_FACTS_SESSION_STATE_TABLE} FINAL
      WHERE ${scopeWhere} AND source = {source:String} AND isNotNull(min_ts) AND isNotNull(max_ts)`,
    { params: { org: scope.orgId, workspace: scope.workspaceId, sessions: scope.sessionIds, source: scope.harness }, timeoutMs: 20_000 },
  );
  const r = rows[0];
  return r && Number(r.n) > 0 && r.lo && r.hi ? { min: r.lo, max: r.hi } : null;
}

/**
 * The phase of a pass's state write. Within one pass (`at`), `pending` (written BEFORE the
 * ClickHouse insert/delete) sorts below `final`, so a pass that dies between its fact writes
 * and its final state leaves the scope incomplete, never a torn reconcile read as complete.
 */
export type StatePhase = "pending" | "final";

export async function recordOutputFactsState(
  ch: ClickHouseClient,
  scope: OutputFactsScope,
  complete: boolean,
  at: Date,
  range: FactsRange,
  phase: StatePhase = "final",
): Promise<void> {
  const synced = at.toISOString().replace("T", " ").replace("Z", "");
  await ch.insert(OUTPUT_FACTS_SESSION_STATE_TABLE, scope.sessionIds.map((session_id) => ({
    org_id: scope.orgId, workspace_id: scope.workspaceId, source: scope.harness, session_id,
    complete: complete && phase === "final" ? 1 : 0, synced_at: synced,
    min_ts: range?.min ?? null, max_ts: range?.max ?? null,
    version: at.getTime() * 2 + (phase === "final" ? 1 : 0),
  })));
}

/** Columns each output-facts table must have before anything writes or reads it. */
const REQUIRED_COLUMNS: Record<string, readonly string[]> = {
  [EVENT_FACTS_TABLE]: ["row_id", "task_ref_empty", "files_touched", "fingerprint", "version"],
  [OUTPUT_FACTS_TABLE]: ["row_id", "scope_source", "fact_kind", "origin", "fingerprint", "version"],
  [OUTPUT_FACTS_SESSION_STATE_TABLE]: ["complete", "min_ts", "max_ts", "version"],
};
const schemaProbes = new WeakMap<ClickHouseClient, { at: number; ready: boolean; present: boolean }>();

/**
 * Whether the owner-applied output-facts DDL is present (`present`: any fact table exists, so
 * erasure must purge it) and complete at this producer's schema (`ready`: every table with every
 * required column, so a pass may write). An older skeleton (CREATE TABLE IF NOT EXISTS never
 * alters it) is present but not ready. Cached for a minute per client.
 */
export async function outputFactsSchema(ch: ClickHouseClient, now = Date.now()): Promise<{ ready: boolean; present: boolean }> {
  const cached = schemaProbes.get(ch);
  if (cached && now - cached.at < 60_000) return cached;
  const rows = await ch.select<{ table: string; name: string }>(
    `SELECT table, name FROM system.columns WHERE database = currentDatabase() AND table IN {tables:Array(String)}`,
    { params: { tables: Object.keys(REQUIRED_COLUMNS) }, timeoutMs: 10_000 },
  );
  const have = new Map<string, Set<string>>();
  for (const r of rows) have.set(r.table, (have.get(r.table) ?? new Set()).add(r.name));
  const ready = Object.entries(REQUIRED_COLUMNS).every(([t, cols]) => cols.every((c) => have.get(t)?.has(c)));
  const probe = { at: now, ready, present: have.has(EVENT_FACTS_TABLE) || have.has(OUTPUT_FACTS_TABLE) };
  schemaProbes.set(ch, probe);
  return probe;
}
/** Tests: forget a client's cached probe. */
export function resetOutputFactsSchemaProbe(ch: ClickHouseClient): void {
  schemaProbes.delete(ch);
}

/** Erasure: no live pointer is left, so every fact of the scope goes. */
export async function eraseOutputFacts(ch: ClickHouseClient, scope: OutputFactsScope): Promise<void> {
  for (const [table, sourceColumn] of [[EVENT_FACTS_TABLE, "source"], [OUTPUT_FACTS_TABLE, "scope_source"]] as const)
    await ch.command(`DELETE FROM ${table} WHERE ${scopeWhere} AND ${sourceColumn} = {source:String}`, {
      params: { org: scope.orgId, workspace: scope.workspaceId, sessions: scope.sessionIds, source: scope.harness },
      settings: { lightweight_deletes_sync: 2 },
      timeoutMs: 120_000,
    });
}
