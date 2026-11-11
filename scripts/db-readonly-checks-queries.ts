/**
 * Fixed query set for the read-only production database checks
 * (.github/workflows/db-readonly-checks.yml, runbooks/sessions-prod-db-readonly-checks.md).
 *
 * Every statement here is a static string built once from the constant name lists below. There is no
 * free-form SQL path: the runner (scripts/db-readonly-checks.ts) only executes entries of this module,
 * and src/hoopdb/__tests__/db-readonly-checks.test.ts pins the shape:
 *  - every statement starts with SELECT, EXPLAIN or SHOW;
 *  - only the EXPLAIN shapes carry `$n` placeholders, bound to the synthetic values below;
 *  - every name is a lower-case SQL identifier from these lists.
 *
 * The role is stats-only (scripts/sql/db-readonly-role.sql): it reads statistics and catalog views and has
 * no privilege on any sessions table. Statements that need table data (TABLE_DATA_PENDING_COUNT_FUNCTION:
 * the policy booleans, the reconcile counts and the EXPLAIN shapes) are kept here as the specification for
 * a later count-only SECURITY DEFINER function and are never executed by the runner; the artifact reports
 * them as TABLE_DATA_STATUS. ingest_claim_sessions_policy.backfill_cursor is never named.
 */

export const READONLY_ROLE = "sessions_readonly_stats";
export const SCHEMA = "sessions";

/** statement_timeout is a dispatch input (seconds); lock_timeout is fixed at 2s. */
export const STATEMENT_TIMEOUT_MIN = 5;
export const STATEMENT_TIMEOUT_MAX = 60;
export const STATEMENT_TIMEOUT_DEFAULT = 15;

/** Session settings the runner sends as startup options and then verifies with SHOW. */
export function sessionSettings(statementTimeoutSeconds: number) {
  checkStatementTimeout(statementTimeoutSeconds);
  return {
    default_transaction_read_only: "on",
    // SHOW renders 60000ms as "1min", anything below as "<n>s".
    statement_timeout: statementTimeoutSeconds === 60 ? "1min" : `${statementTimeoutSeconds}s`,
    lock_timeout: "2s",
    idle_in_transaction_session_timeout: "1min",
  } as const;
}
export function sessionOptions(statementTimeoutSeconds: number): string {
  checkStatementTimeout(statementTimeoutSeconds);
  return `-c default_transaction_read_only=on -c statement_timeout=${statementTimeoutSeconds * 1000} -c lock_timeout=2000 -c idle_in_transaction_session_timeout=60000`;
}
function checkStatementTimeout(s: number): void {
  if (!Number.isInteger(s) || s < STATEMENT_TIMEOUT_MIN || s > STATEMENT_TIMEOUT_MAX) throw new Error("statement_timeout_range");
}

/** Artifact status of every table-data section while the role has no table privilege (always, today). */
export const TABLE_DATA_STATUS = "not_permitted_pending_count_function";

/** The only roles sessions_readonly_stats may be a direct member of (scripts/sql/db-readonly-role.sql). */
export const ALLOWED_MEMBERSHIPS = ["pg_monitor", "pg_read_all_stats"] as const;

/** Ingest tables named in lock samples and in the not-executed table-data specification. No grant on any. */
export const NAMED_TABLES = [
  "ingest_claim_sessions_policy",
  "ingest_claim_sessions",
  "ingest_claim_attempts",
  "ingest_batches",
  "ingest_buffer_outbox",
] as const;

/** Indexes whose pg_stat_user_indexes counters are snapshotted twice and reported as deltas. */
export const NAMED_INDEXES = [
  "ingest_claim_attempts_records_gin_idx", // 044040 detail fallback (records @> ...)
  "ingest_claim_attempts_batch_idx",
  "ingest_claim_attempts_pkey",
  "ingest_claim_sessions_pkey",
  "ingest_claim_sessions_session_idx", // 044090 v2 detail
  "ingest_claim_sessions_accepted_idx", // 044090 v2 list walk
  "ingest_buffer_outbox_pkey",
  "ingest_buffer_outbox_user_buffered_idx", // out-of-band CONCURRENTLY index (043700 header)
  "ingest_batches_pkey",
] as const;

/** sessions.* functions whose pg_stat_statements entries are aggregated (matched as `%sessions.<fn>(%`). */
export const NAMED_FUNCTIONS = [
  "ingest_claim_batch",
  "ingest_claim_buffered_batch",
  "ingest_claim_batch_mode", // internal: only visible with pg_stat_statements.track = all
  "ingest_prepare_buffer_stage",
  "ingest_commit_batch",
  "ingest_commit_buffered_batch",
  "ingest_lazy_begin",
  "ingest_lazy_records",
  "ingest_lazy_ack_batch",
  "buffered_sessions_for",
  "buffered_session_stages_for",
  "buffered_search_gate_for",
] as const;

/** Schemas pg_stat_statements may live in; one fixed statement variant per schema. */
export const PGSS_SCHEMAS = ["extensions", "public"] as const;
export type PgssSchema = (typeof PGSS_SCHEMAS)[number];

const IDENT = /^[a-z_][a-z0-9_]{0,62}$/;
function list(names: readonly string[]): string {
  for (const n of names) if (!IDENT.test(n)) throw new Error("query_set_identifier");
  return names.map((n) => `'${n}'`).join(", ");
}
const TABLES = list(NAMED_TABLES);
const INDEXES = list(NAMED_INDEXES);
const FUNCTIONS = list(NAMED_FUNCTIONS);

/** Fail-closed checks run first; any mismatch refuses the whole run. */
export const PRECHECK = {
  readOnly: "SHOW transaction_read_only",
  defaultReadOnly: "SHOW default_transaction_read_only",
  statementTimeout: "SHOW statement_timeout",
  lockTimeout: "SHOW lock_timeout",
  serverVersion: "SHOW server_version_num",
  identity:
    "SELECT current_user AS me, session_user AS session, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolbypassrls, r.rolreplication, pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS read_all_stats FROM pg_roles r WHERE r.rolname = current_user",
  /** Direct role memberships; must be a subset of ALLOWED_MEMBERSHIPS. */
  memberships:
    "SELECT coalesce(array_agg(r.rolname::text ORDER BY r.rolname), '{}'::text[]) AS roles FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = current_user)",
  /** Any write-capable privilege on any table in schema sessions refuses the run. */
  schemaWritable:
    "SELECT count(*)::int AS writable FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm') AND (has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') OR has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES'))",
  /** Stats-only: any SELECT reach (table or column) on any table in schema sessions refuses the run. */
  schemaSelectable:
    "SELECT count(*)::int AS selectable FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm') AND (has_table_privilege(c.oid, 'SELECT') OR has_any_column_privilege(c.oid, 'SELECT'))",
} as const;

/** Snapshot statements (run at the start and the end of the interval). */
export const SNAPSHOT = {
  statsReset:
    "SELECT d.stats_reset, clock_timestamp() AS at FROM pg_stat_database d WHERE d.datname = current_database()",
  indexes: `SELECT s.relname, s.indexrelname, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch, i.indisvalid AS valid, clock_timestamp() AS at FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid WHERE s.schemaname = 'sessions' AND s.indexrelname IN (${INDEXES}) ORDER BY s.indexrelname`,
  pgssLocation:
    "SELECT n.nspname AS schema, e.extversion AS version FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_stat_statements'",
  pgssTrack: "SHOW pg_stat_statements.track",
} as const;

function pgssAggregate(schema: PgssSchema): string {
  return `SELECT f.fn, count(s.queryid)::int AS entries, coalesce(sum(s.calls), 0)::bigint AS calls, coalesce(sum(s.total_exec_time), 0)::float8 AS total_exec_ms, coalesce(max(s.max_exec_time), 0)::float8 AS max_exec_ms, coalesce(sum(s.rows), 0)::bigint AS rows, coalesce(sum(s.shared_blks_hit), 0)::bigint AS shared_blks_hit, coalesce(sum(s.shared_blks_read), 0)::bigint AS shared_blks_read, coalesce(sum(s.temp_blks_written), 0)::bigint AS temp_blks_written, clock_timestamp() AS at FROM unnest(ARRAY[${FUNCTIONS}]::text[]) AS f(fn) LEFT JOIN ${schema}.pg_stat_statements s ON s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND s.query ILIKE '%sessions.' || replace(f.fn, '_', '\\_') || '(%' GROUP BY f.fn ORDER BY f.fn`;
}
function pgssInfo(schema: PgssSchema): string {
  return `SELECT i.dealloc, i.stats_reset FROM ${schema}.pg_stat_statements_info i`;
}
export const PGSS = Object.fromEntries(
  PGSS_SCHEMAS.map((s) => [s, { aggregate: pgssAggregate(s), info: pgssInfo(s) }]),
) as Record<PgssSchema, { aggregate: string; info: string }>;

/** Sampled during the interval; aggregated client side. No pids, no query text, no user/client names. */
export const SAMPLE = {
  waits:
    "SELECT coalesce(a.wait_event_type, 'none') AS wait_event_type, coalesce(a.wait_event, 'none') AS wait_event, coalesce(a.state, 'none') AS state, count(*)::int AS n, count(*) FILTER (WHERE cardinality(pg_blocking_pids(a.pid)) > 0)::int AS blocked FROM pg_stat_activity a WHERE a.datname = current_database() AND a.backend_type = 'client backend' AND a.pid <> pg_backend_pid() GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
  locks: `SELECT l.locktype, l.mode, l.granted, coalesce(c.relname, '') AS relname, count(*)::int AS n FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation AND c.relnamespace = 'sessions'::regnamespace AND c.relname IN (${TABLES}, ${INDEXES}) WHERE (l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) OR l.database IS NULL) AND l.pid <> pg_backend_pid() GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4`,
} as const;

// ---------------------------------------------------------------------------------------------------
// TABLE DATA, PENDING A COUNT FUNCTION. Not executed: the stats-only role cannot read these tables. Kept as
// the specification of what a later count-only SECURITY DEFINER function must return.
// ---------------------------------------------------------------------------------------------------

/** (a) gate booleans. Never selects backfill_cursor. */
export const POLICY =
  "SELECT p.write_enabled, p.read_enabled, p.backfill_complete_at IS NOT NULL AS backfill_complete FROM sessions.ingest_claim_sessions_policy p WHERE p.singleton";

/**
 * (b) reconcile counts, the shapes of docs/handoffs/ingest-claim-sessions-044080/operator-packet.md
 * steps 5 (expected_pairs/side_rows/stale_rows) and the drift check (null_accepted/drifted).
 */
export const RECONCILE = {
  pairs:
    "SELECT (SELECT count(*) FROM (SELECT DISTINCT a.claim_token, e->>'source', e->>'session_id' FROM sessions.ingest_claim_attempts a CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.records) = 'array' THEN a.records ELSE '[]'::jsonb END) e WHERE jsonb_typeof(e) = 'object' AND jsonb_typeof(e->'source') = 'string' AND jsonb_typeof(e->'session_id') = 'string') x)::bigint AS expected_pairs, (SELECT count(*) FROM sessions.ingest_claim_sessions)::bigint AS side_rows",
  stale:
    "SELECT count(*)::bigint AS stale_rows FROM sessions.ingest_claim_sessions s JOIN sessions.ingest_claim_attempts a USING (claim_token) WHERE a.records IS NULL",
  nullAccepted:
    "SELECT count(*)::bigint AS null_accepted FROM sessions.ingest_claim_sessions s JOIN sessions.ingest_batches b USING (org_id, workspace_id, origin_id, batch_id) WHERE b.state = 'committed' AND s.accepted_at IS NULL",
  drifted:
    "SELECT count(*)::bigint AS drifted FROM sessions.ingest_claim_sessions s JOIN sessions.ingest_batches b USING (org_id, workspace_id, origin_id, batch_id) WHERE s.accepted_at IS NOT NULL AND s.accepted_at IS DISTINCT FROM b.committed_at",
} as const;

/** Synthetic parameter values for EXPLAIN. They match no real row; only the plan shape is reported. */
export const SYNTHETIC = {
  org: "synthetic-org",
  workspace: "synthetic-workspace",
  user: "synthetic-user",
  origin: "synthetic-origin",
  batch: "synthetic-batch",
  source: "synthetic-source",
  session: "synthetic-session",
  ts: "2000-01-01T00:00:00Z",
} as const;

export type ExplainShape = { sql: string; params: readonly string[] };
const S = SYNTHETIC;
const EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON, COSTS true) ";
/** (g) EXPLAIN only (never ANALYZE): the statements are planned, not executed. */
export const EXPLAIN: Record<string, ExplainShape> = {
  reader_list_v2: {
    sql: `${EXPLAIN_PREFIX}SELECT cs.accepted_at FROM sessions.ingest_claim_sessions AS cs WHERE cs.org_id = $1 AND cs.workspace_id = $2 AND cs.user_id = $3 AND cs.accepted_at <= $4::timestamptz ORDER BY cs.accepted_at DESC NULLS LAST LIMIT 64`,
    params: [S.org, S.workspace, S.user, S.ts],
  },
  reader_detail_v2: {
    sql: `${EXPLAIN_PREFIX}SELECT cs.claim_token FROM sessions.ingest_claim_sessions AS cs WHERE cs.org_id = $1 AND cs.workspace_id = $2 AND cs.user_id = $3 AND cs.source = $4 AND cs.session_id = $5`,
    params: [S.org, S.workspace, S.user, S.source, S.session],
  },
  reader_detail_044040_gin: {
    sql: `${EXPLAIN_PREFIX}SELECT a.claim_token FROM sessions.ingest_claim_attempts AS a WHERE a.org_id = $1 AND a.workspace_id = $2 AND a.user_id = $3 AND jsonb_typeof(a.records) = 'array' AND a.records @> jsonb_build_array(jsonb_build_object('source', $4::text, 'session_id', $5::text))`,
    params: [S.org, S.workspace, S.user, S.source, S.session],
  },
  reader_list_outbox: {
    sql: `${EXPLAIN_PREFIX}SELECT o.batch_id FROM sessions.ingest_buffer_outbox AS o WHERE o.org_id = $1 AND o.workspace_id = $2 AND o.user_id = $3 AND o.state = 'buffered' ORDER BY o.created_at DESC LIMIT 64`,
    params: [S.org, S.workspace, S.user],
  },
  writer_refresh_claims: {
    sql: `${EXPLAIN_PREFIX}SELECT s.claim_token, s.source, s.session_id FROM sessions.ingest_claim_attempts AS a JOIN sessions.ingest_claim_sessions AS s ON s.claim_token = a.claim_token WHERE a.org_id = $1 AND a.workspace_id = $2 AND a.origin_id = $3 AND a.batch_id = $4`,
    params: [S.org, S.workspace, S.origin, S.batch],
  },
  writer_batch_committed_lookup: {
    sql: `${EXPLAIN_PREFIX}SELECT b.committed_at FROM sessions.ingest_batches AS b WHERE b.org_id = $1 AND b.workspace_id = $2 AND b.origin_id = $3 AND b.batch_id = $4 AND b.state = 'committed'`,
    params: [S.org, S.workspace, S.origin, S.batch],
  },
};

/** The table-data specification, grouped as the artifact reports it. Never executed by the runner. */
export const TABLE_DATA_PENDING_COUNT_FUNCTION = { policy: POLICY, reconcile: RECONCILE, explain: EXPLAIN } as const;

type Stmt = { name: string; sql: string; params: readonly string[] };
const flat = (prefix: string, rec: Record<string, string>): Stmt[] =>
  Object.entries(rec).map(([k, sql]) => ({ name: `${prefix}.${k}`, sql, params: [] }));

/** Every statement the runner may execute (stats and catalog views only); the test pins this list. */
export function executedStatements(): Stmt[] {
  return [
    ...flat("precheck", PRECHECK),
    ...flat("snapshot", SNAPSHOT),
    ...PGSS_SCHEMAS.flatMap((s) => flat(`pgss.${s}`, PGSS[s])),
    ...flat("sample", SAMPLE),
  ];
}
/** The not-executed table-data specification, flattened (shape-checked by the same tests). */
export function tableDataStatements(): Stmt[] {
  return [
    ...flat("policy", { policy: POLICY }),
    ...flat("reconcile", RECONCILE),
    ...Object.entries(EXPLAIN).map(([k, v]) => ({ name: `explain.${k}`, sql: v.sql, params: v.params })),
  ];
}
export function allStatements(): Stmt[] {
  return [...executedStatements(), ...tableDataStatements()];
}
