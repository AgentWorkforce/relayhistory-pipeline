/**
 * Counts-only presence of ONE session across the ingest, buffer, custody and publication tables, for support
 * diagnosis of a dashboard that shows "No conversation recorded yet" (backlog-ops mode presence-dry,
 * scripts/session-presence.ts, docs/backlog-ops-workflow.md "Session presence (presence-dry)").
 *
 * One transaction: BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY, SHOW transaction_read_only must be `on`, a
 * bounded SET LOCAL statement_timeout and SET LOCAL lock_timeout, then one parameterised COUNT per entry of
 * SESSION_PRESENCE_COUNTS (the session is $1, the optional org scope $2::text[]) plus the distinct-org check, each in
 * its own savepoint, and ROLLBACK always. Every count is one snapshot. A count cancelled by the statement timeout
 * (sqlstate 57014) is rolled back to its savepoint and reported as null with complete=false; any other error
 * (including a lock_timeout, 55P03) refuses the run. The whole transaction also has a total budget (totalBudgetMs):
 * a statement is started only while the budget has at least SESSION_PRESENCE_MIN_TIMEOUT_MS left, and its statement
 * timeout is lowered to what is left, so the snapshot is held for at most about the budget. A statement skipped or
 * cut short by the budget is null with budget_exhausted. The publication generation marker
 * (SESSION_PRESENCE_GENERATION_MARKER) is read first and last; generation_stable reports whether they matched, and
 * complete=false with a fixed incomplete_reason code fails closed on an unobserved or unstable marker, more than one
 * ownership row or org, a timed-out count, or an exhausted budget. Counts are still emitted.
 *
 * Index use. Most tables are keyed tenant-first (org_id, workspace_id, user_id, ...) and have no session-first
 * index, so an UNSCOPED count of them is a sequential scan: ownership, claim_sessions, lazy_slots, revisions,
 * heads, generations and canonical pointers. That is accepted here because the tool runs on demand for one session,
 * read-only, in one REPEATABLE READ snapshot, and every count is bounded by the per-statement timeout (a count that
 * runs over is null, never a hung run) and the whole transaction by the total budget. With BACKLOG_OPS_ORGS set the same counts use the org-leading index
 * (named per count below). The claim-driven counts (claims, buffered_stages, outbox, outbox_buffered) use the
 * jsonb_path_ops GIN ingest_claim_attempts_records_gin_idx (044040) in either scope.
 *
 * The output never carries the session id, an org id, a key or a URL: numbers, booleans and the scope code only,
 * and the caller still passes it through countsOnly().
 */
import { isPresenceSession } from "./backlog-ops.ts";

export interface PresenceSql {
  query(text: string, params?: unknown[]): Promise<{ rows: Array<Record<string, unknown>> }>;
}

const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
export const SESSION_PRESENCE_MAX_ORGS = 64;
export const SESSION_PRESENCE_MIN_TIMEOUT_MS = 1000;
export const SESSION_PRESENCE_MAX_TIMEOUT_MS = 600000;
/** Bounds of the total transaction budget (--total-budget-ms): the same range as the per-statement timeout. */
export const SESSION_PRESENCE_MIN_BUDGET_MS = 1000;
export const SESSION_PRESENCE_MAX_BUDGET_MS = 600000;
/** SET LOCAL lock_timeout: a count never queues behind DDL for long (55P03 refuses the run). */
export const SESSION_PRESENCE_LOCK_TIMEOUT_MS = 5000;

/** `AND <alias>.org_id = ANY($2::text[])` when scoped; fixed text, never a value. */
const org = (alias: string, scoped: boolean) => (scoped ? ` AND ${alias}.org_id = ANY($2::text[])` : "");
/** The claims whose records name the session (any source): ingest_claim_attempts_records_gin_idx. */
const claimsOf = (scoped: boolean) =>
  `SELECT a.claim_token, a.org_id, a.workspace_id, a.origin_id, a.batch_id
     FROM sessions.ingest_claim_attempts AS a
    WHERE jsonb_typeof(a.records) = 'array'
      AND a.records @> jsonb_build_array(jsonb_build_object('session_id', $1::text))${org("a", scoped)}`;

export interface PresenceCount { columns: readonly string[]; sql: (scoped: boolean) => string }

/**
 * Each count, its table and its index (unscoped / scoped). Columns are the summary keys it fills.
 */
export const SESSION_PRESENCE_COUNTS: readonly PresenceCount[] = [
  // ingest_session_ownership: rows naming the session (one per org/ws/user/origin/source; 1 is the normal case).
  // PK (org_id, workspace_id, user_id, origin_id, source, session_id): unscoped = scan; scoped = PK org prefix.
  { columns: ["ownership"], sql: (s) =>
    `SELECT count(*)::int AS ownership FROM sessions.ingest_session_ownership AS w WHERE w.session_id = $1::text${org("w", s)}` },
  // ingest_claim_attempts: accepted upload claims whose records name the session. GIN (records jsonb_path_ops).
  { columns: ["claims"], sql: (s) => `SELECT count(*)::int AS claims FROM (${claimsOf(s)}) AS c` },
  // ingest_claim_sessions (044080, the v2 reader's session index): ingest_claim_sessions_session_idx
  // (org_id, workspace_id, user_id, source, session_id): unscoped = scan; scoped = index org prefix.
  { columns: ["claim_sessions"], sql: (s) =>
    `SELECT count(*)::int AS claim_sessions FROM sessions.ingest_claim_sessions AS cs WHERE cs.session_id = $1::text${org("cs", s)}` },
  // Owner-buffered stage rows: ingest_stage_intents of those claims (UNIQUE (claim_token, stage_key)) that are the
  // active stage of a buffered ingest_buffer_outbox row (ingest_buffer_outbox_active_stage_token_idx) of a committed,
  // still-buffered ingest_batches row (PK). The structural predicates of the 047010 reader's candidate set; the
  // reader's runtime gates (owner-buffered flag and workspace allowlist, admission epoch, resets, deletions, ancestor
  // checks) are not evaluated, so this is an upper bound on what the reader can serve.
  { columns: ["buffered_stages"], sql: (s) =>
    `SELECT count(DISTINCT st.stage_token)::int AS buffered_stages
       FROM (${claimsOf(s)}) AS c
       JOIN sessions.ingest_stage_intents AS st ON st.claim_token = c.claim_token
       JOIN sessions.ingest_buffer_outbox AS o ON o.active_stage_token = st.stage_token
       JOIN sessions.ingest_batches AS b
         ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
      WHERE st.receipt_state = 'acknowledged' AND st.cleanup_state <> 'redacted'
        AND o.state = 'buffered' AND o.user_id = st.user_id
        AND b.state = 'committed' AND b.publication_state = 'buffered' AND b.committed_at IS NOT NULL` },
  // ingest_buffer_outbox rows (publication outbox, any state, and those still `buffered`) of the batches those claims
  // name: outbox PK (org_id, workspace_id, origin_id, batch_id).
  { columns: ["outbox", "outbox_buffered"], sql: (s) =>
    `SELECT count(*)::int AS outbox, (count(*) FILTER (WHERE o.state = 'buffered'))::int AS outbox_buffered
       FROM sessions.ingest_buffer_outbox AS o
      WHERE (o.org_id, o.workspace_id, o.origin_id, o.batch_id) IN (SELECT c.org_id, c.workspace_id, c.origin_id, c.batch_id FROM (${claimsOf(s)}) AS c)` },
  // ingest_lazy_native_slots currently owned by the session. No index on current_session: unscoped = scan (the
  // largest table; bounded by the timeout, null if it runs over); scoped = ingest_lazy_native_slots_org_id_idx (org_id).
  { columns: ["lazy_slots"], sql: (s) =>
    `SELECT count(*)::int AS lazy_slots FROM sessions.ingest_lazy_native_slots AS l
      WHERE l.current_owner IS NOT NULL AND l.current_session = $1::text${org("l", s)}` },
  // ingest_record_revisions of the session. ingest_record_revisions_session_idx
  // (org_id, workspace_id, user_id, origin_id, source, session_id): unscoped = scan; scoped = index org prefix.
  { columns: ["revisions"], sql: (s) =>
    `SELECT count(*)::int AS revisions FROM sessions.ingest_record_revisions AS r WHERE r.session_id = $1::text${org("r", s)}` },
  // ingest_publication_heads: every head, and the published ones (state live or a current pointer key), as the
  // reader's published exclusion counts them. PK (org_id, ws, user, origin, source, session_id): unscoped = scan.
  // The three heads_<state> columns are the `dispositions` leaf: ingest_publication_heads.state is the fixed CHECK
  // enum building|live|retired (20261104060000_ingest_canonical_publication, unchanged since), so each is a bounded count.
  { columns: ["heads", "published_heads", "heads_building", "heads_live", "heads_retired"], sql: (s) =>
    `SELECT count(*)::int AS heads, (count(*) FILTER (WHERE h.state = 'live' OR h.current_key IS NOT NULL))::int AS published_heads,
            (count(*) FILTER (WHERE h.state = 'building'))::int AS heads_building,
            (count(*) FILTER (WHERE h.state = 'live'))::int AS heads_live,
            (count(*) FILTER (WHERE h.state = 'retired'))::int AS heads_retired
       FROM sessions.ingest_publication_heads AS h WHERE h.session_id = $1::text${org("h", s)}` },
  // ingest_publication_objects still live in a live ingest_publication_generations row of the session. Generations:
  // unscoped = scan, scoped = ingest_publication_generations_org_id_idx; objects by ingest_publication_objects_cleanup_idx
  // (generation, cleanup_state, object_key).
  { columns: ["published_objects"], sql: (s) =>
    `SELECT count(*)::int AS published_objects
       FROM sessions.ingest_publication_generations AS g
       JOIN sessions.ingest_publication_objects AS po ON po.generation = g.generation AND po.cleanup_state = 'live'
      WHERE g.session_id = $1::text AND g.state = 'live'${org("g", s)}` },
  // sessions.session_index: the canonical pointers the read API's turns/metadata routes look for first
  // (session-turns-source.ts SESSION_HARNESSES_SQL: session_id or canonical_session_id), and the live ones
  // (deleted_at IS NULL). si_session (org_id, session_id): unscoped = scan; scoped = si_session for session_id, the
  // canonical_session_id arm filtered within the org (bounded by the timeout).
  { columns: ["canonical_pointers", "canonical_pointers_live"], sql: (s) =>
    `SELECT count(*)::int AS canonical_pointers, (count(*) FILTER (WHERE p.deleted_at IS NULL))::int AS canonical_pointers_live
       FROM sessions.session_index AS p
      WHERE (coalesce(p.canonical_session_id, p.session_id) = $1::text OR p.session_id = $1::text)${org("p", s)}` },
];

/**
 * The publication generation marker, read as the FIRST and the LAST statement of the transaction and compared
 * in-process (generation_stable). It is what the publisher advances and the reader pins:
 *  - every ingest_publication_heads row of the session: (generation, pointer_revision, row_version, state,
 *    current_key), the publisher's per-session head (20261104060000), which the deliver path binds a claim to;
 *  - every sessions.session_index pointer of the session (manifest_key, revision, content_sha256, deleted_at), the
 *    inputs of the read API's canonicalSourceGeneration (session-read-api/canonical.ts), which the reader compares
 *    before and after a read (source_generation_changed).
 * The rows are folded to ONE md5 in SQL; the digest is compared and dropped, never returned, logged or written (it
 * is derived from keys). Under REPEATABLE READ both reads see one snapshot, so a mismatch means the counts did not
 * come from one snapshot (for example a pooler that does not hold the transaction): fail closed with
 * complete=false. Same index use as the heads and canonical_pointers counts (unscoped = scan, bounded by the timeout).
 */
export const SESSION_PRESENCE_GENERATION_MARKER = (s: boolean) =>
  `SELECT count(*)::int AS rows, coalesce(md5(string_agg(m.x, ',' ORDER BY m.x)), '') AS marker FROM (
     SELECT 'h' || (h.org_id, h.workspace_id, h.user_id, h.origin_id, h.source, h.generation, h.pointer_revision,
                    h.row_version, h.state, h.current_key)::text AS x
       FROM sessions.ingest_publication_heads AS h WHERE h.session_id = $1::text${org("h", s)}
     UNION ALL
     SELECT 'p' || (p.org_id, p.workspace_id, p.harness, p.session_id, p.machine_id, p.manifest_key, p.revision,
                    p.content_sha256, p.deleted_at)::text AS x
       FROM sessions.session_index AS p
      WHERE (coalesce(p.canonical_session_id, p.session_id) = $1::text OR p.session_id = $1::text)${org("p", s)}
   ) AS m`;

/**
 * The distinct-org check: how many DISTINCT org ids name the session across the org-bearing tables the counts read
 * (ownership, the claims' records, claim sessions, lazy slots, revisions, heads, generations and canonical
 * pointers). More than one means the session id is used in more than one org and the counts mix tenants: it widens
 * multi_ownership (with ownership rows > 1). The number itself is not emitted (the receipt shape is unchanged).
 * Same index use as those counts (unscoped = scans, bounded by the timeout and the budget).
 */
export const SESSION_PRESENCE_DISTINCT_ORGS = (s: boolean) =>
  `SELECT count(DISTINCT x.org_id)::int AS distinct_orgs FROM (
     SELECT w.org_id FROM sessions.ingest_session_ownership AS w WHERE w.session_id = $1::text${org("w", s)}
     UNION ALL SELECT c.org_id FROM (${claimsOf(s)}) AS c
     UNION ALL SELECT cs.org_id FROM sessions.ingest_claim_sessions AS cs WHERE cs.session_id = $1::text${org("cs", s)}
     UNION ALL SELECT l.org_id FROM sessions.ingest_lazy_native_slots AS l
                WHERE l.current_owner IS NOT NULL AND l.current_session = $1::text${org("l", s)}
     UNION ALL SELECT r.org_id FROM sessions.ingest_record_revisions AS r WHERE r.session_id = $1::text${org("r", s)}
     UNION ALL SELECT h.org_id FROM sessions.ingest_publication_heads AS h WHERE h.session_id = $1::text${org("h", s)}
     UNION ALL SELECT g.org_id FROM sessions.ingest_publication_generations AS g WHERE g.session_id = $1::text${org("g", s)}
     UNION ALL SELECT p.org_id FROM sessions.session_index AS p
                WHERE (coalesce(p.canonical_session_id, p.session_id) = $1::text OR p.session_id = $1::text)${org("p", s)}
   ) AS x`;

/** heads_<state> columns that move under `dispositions` (fixed enum codes, bounded counts). */
const DISPOSITION_COLUMNS = ["heads_building", "heads_live", "heads_retired"] as const;

/**
 * complete=false reasons, one fixed code, most serious first: generation_unobserved (a marker read timed out or was
 * skipped by the budget), generation_unstable (the marker differed between the first and last read; the two are
 * exclusive), multi_ownership (more than one ingest_session_ownership row, or the session id under more than one
 * org: the counts mix owners), count_timeout (a count hit the per-statement timeout and is null), budget_exhausted
 * (the total budget ran out, so a count was skipped or cut short and is null). `none` when complete.
 */
export const SESSION_PRESENCE_INCOMPLETE_REASONS = ["none", "generation_unobserved", "generation_unstable", "multi_ownership", "count_timeout", "budget_exhausted"] as const;

export type SessionPresenceSummary = Record<string, number | null | boolean | string | Record<string, number | null>>;

export interface SessionPresenceOptions {
  session: string;
  /** Lowercase org UUIDs (BACKLOG_OPS_ORGS) or null/empty for every org. */
  orgs?: readonly string[] | null;
  statementTimeoutMs: number;
  /** The whole transaction's budget in ms (SESSION_PRESENCE_MIN_BUDGET_MS..SESSION_PRESENCE_MAX_BUDGET_MS). */
  totalBudgetMs: number;
  /** Monotonic-enough clock in ms, injectable for tests (default Date.now). */
  now?: () => number;
}

const fail = (code: string): never => { throw new Error(code); };

export async function sessionPresence(sql: PresenceSql, o: SessionPresenceOptions): Promise<SessionPresenceSummary> {
  if (typeof o.session !== "string" || !isPresenceSession(o.session)) fail("session_presence_session_invalid");
  const orgs = o.orgs && o.orgs.length ? [...o.orgs] : null;
  if (orgs && (orgs.length > SESSION_PRESENCE_MAX_ORGS || new Set(orgs).size !== orgs.length || !orgs.every((x) => ORG_UUID.test(x))))
    fail("session_presence_scope");
  const ms = o.statementTimeoutMs;
  if (!Number.isInteger(ms) || ms < SESSION_PRESENCE_MIN_TIMEOUT_MS || ms > SESSION_PRESENCE_MAX_TIMEOUT_MS) fail("session_presence_timeout");
  const budget = o.totalBudgetMs;
  if (!Number.isInteger(budget) || budget < SESSION_PRESENCE_MIN_BUDGET_MS || budget > SESSION_PRESENCE_MAX_BUDGET_MS) fail("session_presence_budget");
  const now = o.now ?? Date.now;
  const scoped = orgs !== null, params: unknown[] = scoped ? [o.session, orgs] : [o.session];
  const out: SessionPresenceSummary = {};
  let timedOut = false, exhausted = false, generationStable = false, distinctOrgs: number | null = null;
  let reason: (typeof SESSION_PRESENCE_INCOMPLETE_REASONS)[number] = "none";
  const start = now();
  await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const ro = await sql.query("SHOW transaction_read_only");
    if (ro.rows[0]?.transaction_read_only !== "on") fail("session_presence_not_read_only");
    await sql.query(`SET LOCAL statement_timeout = ${ms}`);
    await sql.query(`SET LOCAL lock_timeout = ${SESSION_PRESENCE_LOCK_TIMEOUT_MS}`);
    let current = ms;
    /**
     * One statement in its own savepoint. null when the statement timeout (57014) cancels it (count_timeout, or
     * budget_exhausted when the timeout had been lowered to the budget's remainder) or when the budget has less than
     * SESSION_PRESENCE_MIN_TIMEOUT_MS left (skipped, budget_exhausted); other errors refuse.
     */
    const guarded = async (text: string): Promise<Record<string, unknown> | null> => {
      const left = budget - (now() - start);
      if (!(left >= SESSION_PRESENCE_MIN_TIMEOUT_MS)) { exhausted = true; return null; }
      const limit = Math.min(ms, Math.floor(left));
      // Outside the savepoint, so a rollback to it never undoes the lowered timeout.
      if (limit !== current) { await sql.query(`SET LOCAL statement_timeout = ${limit}`); current = limit; }
      await sql.query("SAVEPOINT session_presence_count");
      try {
        const row = (await sql.query(text, params)).rows[0] ?? {};
        await sql.query("RELEASE SAVEPOINT session_presence_count");
        return row;
      } catch (e) {
        if ((e as { code?: unknown } | null)?.code !== "57014") throw e;
        await sql.query("ROLLBACK TO SAVEPOINT session_presence_count");
        if (limit < ms) exhausted = true; else timedOut = true;
        return null;
      }
    };
    const marker = async (): Promise<string | null> => {
      const row = await guarded(SESSION_PRESENCE_GENERATION_MARKER(scoped));
      if (row === null) return null;
      const n = Number(row.rows), m = row.marker;
      if (!Number.isSafeInteger(n) || n < 0 || typeof m !== "string" || !(n === 0 ? m === "" : /^[0-9a-f]{32}$/u.test(m)))
        fail("session_presence_marker_shape");
      return `${n}:${m}`;
    };
    const first = await marker();
    for (const c of SESSION_PRESENCE_COUNTS) {
      const row = await guarded(c.sql(scoped));
      if (row === null) {
        for (const k of c.columns) out[k] = null;
        continue;
      }
      for (const k of c.columns) {
        const n = Number(row[k]);
        if (!Number.isSafeInteger(n) || n < 0) fail("session_presence_count_shape");
        out[k] = n;
      }
    }
    const orgsRow = await guarded(SESSION_PRESENCE_DISTINCT_ORGS(scoped));
    if (orgsRow !== null) {
      const n = Number(orgsRow.distinct_orgs);
      if (!Number.isSafeInteger(n) || n < 0) fail("session_presence_count_shape");
      distinctOrgs = n;
    }
    const last = await marker();
    if (first === null || last === null) reason = "generation_unobserved";
    else if (first !== last) reason = "generation_unstable";
    generationStable = first !== null && first === last;
  } finally {
    await sql.query("ROLLBACK").catch(() => undefined);
  }
  if (reason === "none" && ((typeof out.ownership === "number" && out.ownership > 1) || (distinctOrgs !== null && distinctOrgs > 1)))
    reason = "multi_ownership";
  if (reason === "none" && timedOut) reason = "count_timeout";
  if (reason === "none" && exhausted) reason = "budget_exhausted";
  const dispositions: Record<string, number | null> = {};
  for (const k of DISPOSITION_COLUMNS) { dispositions[k] = out[k] as number | null; delete out[k]; }
  out.dispositions = dispositions;
  out.scope = scoped ? `orgs_${orgs!.length}` : "all";
  out.generation_stable = generationStable;
  out.complete = reason === "none";
  out.incomplete_reason = reason;
  return out;
}

/**
 * The tool's argv: `--dry-run` (required; there is no execute), `--statement-timeout-ms n`, `--total-budget-ms n`
 * (both required, 1000..600000), `--org <uuid>` ...
 */
export interface SessionPresenceArgs { orgs: string[]; statementTimeoutMs: number; totalBudgetMs: number }
export function parseSessionPresenceArgs(argv: readonly string[]): SessionPresenceArgs {
  let dry = false, timeout: number | null = null, budget: number | null = null;
  const orgs: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === "--dry-run" && !dry) { dry = true; continue; }
    const v = argv[i + 1];
    if (a === "--statement-timeout-ms" && timeout === null && v !== undefined && /^[1-9][0-9]{3,5}$/u.test(v)) { timeout = Number(v); i++; continue; }
    if (a === "--total-budget-ms" && budget === null && v !== undefined && /^[1-9][0-9]{3,5}$/u.test(v)) { budget = Number(v); i++; continue; }
    if (a === "--org" && v !== undefined && ORG_UUID.test(v) && !orgs.includes(v)) { orgs.push(v); i++; continue; }
    fail("session_presence_args");
  }
  if (!dry || timeout === null || timeout < SESSION_PRESENCE_MIN_TIMEOUT_MS || timeout > SESSION_PRESENCE_MAX_TIMEOUT_MS ||
      budget === null || budget < SESSION_PRESENCE_MIN_BUDGET_MS || budget > SESSION_PRESENCE_MAX_BUDGET_MS ||
      orgs.length > SESSION_PRESENCE_MAX_ORGS) fail("session_presence_args");
  return { orgs: orgs.sort(), statementTimeoutMs: timeout!, totalBudgetMs: budget! };
}

/** stdin: exactly {"dbUrl": "...", "session": "..."} on one line (at most 64 KiB). */
export function parseSessionPresenceInput(text: string): { dbUrl: string; session: string } {
  const line = text.split("\n")[0] ?? "";
  if (line.length > 64 * 1024) fail("session_presence_stdin_too_large");
  let parsed: unknown;
  try { parsed = JSON.parse(line); } catch { fail("session_presence_stdin_invalid"); }
  const p = parsed as { dbUrl?: unknown; session?: unknown } | null;
  if (!p || typeof p !== "object" || Array.isArray(p) || Object.keys(p).sort().join(",") !== "dbUrl,session") fail("session_presence_stdin_invalid");
  if (typeof p!.dbUrl !== "string" || !p!.dbUrl) fail("session_presence_db_url_required");
  if (typeof p!.session !== "string" || p!.session === "") fail("session_presence_session_missing");
  if (!isPresenceSession(p!.session as string)) fail("session_presence_session_invalid");
  return { dbUrl: p!.dbUrl as string, session: p!.session as string };
}

/**
 * The codeless messages that pass through as a refusal code: this tool's own session_presence_* codes, a
 * backlog_ops_* code, or an already formed sqlstate_<5>. Any other message (a library's lowercase message can still
 * be a value, such as a lowercase session token) is internal_error. At most 40 characters, the receipt code limit.
 */
const PRESENCE_REASON = /^(?:session_presence_[a-z0-9_]{1,23}|backlog_ops_[a-z0-9_]{1,28}|sqlstate_[0-9a-z]{5})$/u;
/** A closed, redacted reason only: a pg error message can carry a host, a value or a tenant id. */
export const presenceRedactedReason = (e: unknown): string => {
  const code = (e as { code?: unknown } | null)?.code;
  // Lowercased: the receipt admits only lowercase codes, and SQLSTATEs such as XX000 or 42P01 carry letters.
  if (typeof code === "string" && /^[0-9A-Z]{5}$/u.test(code)) return "sqlstate_" + code.toLowerCase();
  if ((e as { name?: unknown })?.name === "OwnerUrlError" && typeof code === "string" && /^[a-z0-9_]{1,32}$/u.test(code)) return "db_url_" + code;
  const m = (e as Error)?.message;
  return typeof m === "string" && PRESENCE_REASON.test(m) ? m : "internal_error";
};

/**
 * The pg `options` of the presence client: the owner config's own options (idle_in_transaction_session_timeout)
 * kept, with default_transaction_read_only=on appended. Never replaces them.
 */
export function presenceClientOptions(baseOptions: string | undefined): string {
  const base = (baseOptions ?? "").trim();
  return (base ? base + " " : "") + "-c default_transaction_read_only=on";
}

/**
 * The tool body, injectable for tests: parse argv and stdin, connect, count, and return the one stdout line
 * (the summary, or {"refused": <code>}) and the exit code. Nothing else is written.
 */
export async function sessionPresenceMain(
  argv: readonly string[],
  stdin: string,
  connect: (dbUrl: string) => Promise<PresenceSql & { end?: () => Promise<unknown> }>,
): Promise<{ line: string; code: number }> {
  let client: (PresenceSql & { end?: () => Promise<unknown> }) | undefined;
  try {
    const args = parseSessionPresenceArgs(argv);
    const input = parseSessionPresenceInput(stdin);
    client = await connect(input.dbUrl);
    const summary = await sessionPresence(client, { session: input.session, orgs: args.orgs, statementTimeoutMs: args.statementTimeoutMs, totalBudgetMs: args.totalBudgetMs });
    return { line: JSON.stringify(summary), code: 0 };
  } catch (e) {
    return { line: JSON.stringify({ refused: presenceRedactedReason(e) }), code: 1 };
  } finally {
    await client?.end?.().catch(() => undefined);
  }
}
