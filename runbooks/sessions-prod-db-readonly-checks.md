# Production DB read-only checks (`sessions_readonly_stats`)

## Purpose

Answer a fixed set of operational questions about the production sessions database without a
credentialed operator session and without any write:

- whether the 044040 records-GIN detail fallback is still being read
  (`ingest_claim_attempts_records_gin_idx` scans over a window) after the 044090 claim-sessions reader;
- per-function call counts and timings of the ingest writer/reader functions (`pg_stat_statements`);
- wait events, lock pressure and blocked backends on the ingest tables during the window;
- not yet: the claim-sessions policy switches, the operator-packet reconcile counts and plan-only EXPLAIN
  shapes. These need table data, which this stats-only role cannot read; see "Table data" below.

Pieces:

| Piece | Path |
| --- | --- |
| Workflow (dispatch only) | `.github/workflows/db-readonly-checks.yml` |
| Runner | `scripts/db-readonly-checks.ts` |
| Fixed query set | `scripts/db-readonly-checks-queries.ts` |
| Artifact sanitizer | `scripts/db-readonly-checks-sanitize.ts` |
| Environment protection gate | `scripts/db-readonly-environment-gate.ts` |
| Role, step 1 (NOLOGIN, idempotent) | `scripts/sql/db-readonly-role.sql` |
| Role, step 2 (LOGIN, manual psql template) | `scripts/sql/db-readonly-role-login.sql.tmpl` |
| Role rollback | `scripts/sql/db-readonly-role-nologin.sql` |
| Tests (pure, no database) | `src/hoopdb/__tests__/db-readonly-checks.test.ts` |

## Hard rule: the workflow never writes

- The statements it executes are the constants returned by `executedStatements()` in
  `scripts/db-readonly-checks-queries.ts`: `SELECT` and `SHOW` against statistics and catalog views only.
  The test file refuses `ANALYZE`, `BUFFERS`, `TIMING`, `WAL`, `SETTINGS`, any DML/DDL keyword, `;`, and
  comments in every statement, and pins that no executed statement names a `sessions.` table.
- The connection sets `default_transaction_read_only=on`, `statement_timeout=<statement_timeout_seconds>`
  (5 to 60, default 15), `lock_timeout=2s`, `idle_in_transaction_session_timeout=60s` as startup options;
  the role carries `read_only`, 15s, 2s and 60s as `ALTER ROLE ... SET` defaults, a second layer.
- Before reading anything the runner refuses (exit 2, no artifact) unless `SHOW transaction_read_only`
  is `on`, the timeouts are exactly the requested values, the user is exactly `sessions_readonly_stats`,
  its only role memberships are `pg_monitor` and `pg_read_all_stats`, it holds no INSERT/UPDATE/DELETE/
  TRUNCATE/REFERENCES/TRIGGER privilege on any `sessions` table or column, and
  `pg_catalog.has_table_privilege(..., 'SELECT')` (and `has_any_column_privilege`) is false for every
  `sessions` table (code `table_privilege_present`).
- The role is stats-only: `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, memberships
  `pg_read_all_stats` and `pg_monitor`, and nothing else on `sessions`: no schema `USAGE`, no table or
  column `SELECT`, no RLS policies, no function `EXECUTE`. The one other grant is `USAGE` on schema
  `extensions` when `pg_stat_statements` lives there, so the view name resolves. Step 1 ends with an
  assertion block that raises if any of this does not hold.
- Nothing in this repository applies the role SQL; the workflow only connects.

## Owner gates (Khaliq's own go, in his own words, each one separately)

None of these is covered by the AGENTS.md code-integration standing order:

1. Applying `scripts/sql/db-readonly-role.sql` to production (creates a NOLOGIN role and two memberships).
2. Making the role a LOGIN role in production (`ALTER ROLE ... LOGIN PASSWORD`, step 2, run by hand from
   `scripts/sql/db-readonly-role-login.sql.tmpl`).
3. Creating the `production-db-readonly` environment and its secret/variable.
4. Each dispatch of `db-readonly-checks.yml` that runs the SQL against production (environment approval).
5. Any `ALTER ROLE` on the role afterwards, including the rollback.

Agents prepare and review; they do not create the environments, write the secrets, apply the SQL or
approve a run.

## One-time setup: GitHub environment

Create it before anything is dispatched. The gate job refuses unless the environment exists with at
least one required **User** reviewer and a deployment-branch policy of exactly one branch, `main`
(GitHub would otherwise auto-create a missing environment without protection).

| Environment | Reviewer | Branches | Secret | Variable |
| --- | --- | --- | --- | --- |
| `production-db-readonly` | Khaliq | `main` only | `DB_READONLY_URL` (set by hand in step 2) | `SUPABASE_PROJECT_REF` |

Secret values come from 1Password; nothing is saved to disk on any machine. No workflow creates roles,
generates passwords or writes GitHub secrets.

## Two-step bootstrap

**Step 1, NOLOGIN role (owner gate 1).** Apply `scripts/sql/db-readonly-role.sql` as the migration owner
in one transaction, by the owner's usual operator path, for example
`psql "$OWNER_URL" -v ON_ERROR_STOP=1 --single-transaction -f scripts/sql/db-readonly-role.sql`.
It is idempotent (re-run after any migration that adds a named table, or to repair drift) and ends with
an assertion block. This is not a migration and takes no registry slot. It needs `GRANT pg_read_all_stats, pg_monitor` by the
applying role (PG16+: that role needs ADMIN OPTION on them; unverified on Supabase, see below).

The role stays NOLOGIN: nothing can connect as it yet.

**Step 2, LOGIN (owner gates 2 and 3), by hand.** The owner:

1. creates the password in 1Password (never on disk, never on a command line, never in the repository);
2. computes a SCRAM-SHA-256 verifier from it on the operator machine, so only the verifier reaches the
   server and a statement log discloses no usable password;
3. runs `psql "$OWNER_URL" --single-transaction -v ON_ERROR_STOP=1 -v login_verifier="$VERIFIER" -f
   scripts/sql/db-readonly-role-login.sql.tmpl`; the template refuses unless step 1 holds (role safe,
   read-only default, no `sessions` table privilege);
4. sets the `production-db-readonly` environment secret `DB_READONLY_URL` by hand from the 1Password
   item: user `sessions_readonly_stats` (pooler: `sessions_readonly_stats.<ref>`), `?sslmode=verify-full`.

Repeating step 2 with a new password rotates it (update the secret afterwards).

**Rollback (owner gate 5).** `scripts/sql/db-readonly-role-nologin.sql`: `NOLOGIN PASSWORD NULL`, revokes any
table grant, schema usage and the memberships. Delete `DB_READONLY_URL` afterwards. `DROP ROLE` is left to
the owner (it needs no dependent objects in any database).

## Dispatch

Actions, "production DB read-only checks", branch `main`:

- `interval_seconds`: measurement window, 60 to 1800, default 900;
- `statement_timeout_seconds`: server `statement_timeout`, integer 5 to 60, default 15, validated in the
  gate job and again in the runner; `lock_timeout` stays 2s. If a pooler drops the startup options, the
  role default (15s) applies and the precheck refuses any other requested value (fails closed);
- `explain`: default `false`; currently always skipped (`not_permitted_pending_count_function`, below).

Then approve the `production-db-readonly` deployment. The gate job runs first without the environment
and refuses a non-`main` ref or an unprotected environment. One run at a time (concurrency group, no
cancel). Exit codes of the run step: 0 every section ok; 1 a section failed (artifact still uploaded);
2 refused before or during the run (no artifact; the log shows one code, never a message, host or URL).

Run order: precheck, then the window (index/statement snapshot, wait/lock samples every 30s, snapshot
again). Only statistics and catalog views are read, so the run's own reads do not show up in the index
deltas.

## Table data: skipped until a count function exists

The policy switches, reconcile counts and EXPLAIN shapes are kept in a clearly named, not-executed
section of `scripts/db-readonly-checks-queries.ts` (`TABLE_DATA_PENDING_COUNT_FUNCTION`). Because
`has_table_privilege` is false for every `sessions` table (and the precheck refuses otherwise), the run
skips them and reports `table_data.status = "not_permitted_pending_count_function"` with the skipped
names; this is not a failure (exit 0). Counts arrive via a count-only `SECURITY DEFINER` function in a
later migration (not in this PR, no placeholder slot).

## Artifact `db-readonly-checks-<run id>` (`db-readonly-checks.json`, schema `db-readonly-checks/v1`)

Counts, booleans, timestamps and object names only. No row data, no ids, no query text.
Before writing, the sanitizer walks every key and value and refuses (exit 2, no file) on any UUID,
32-hex string, JWT, URL, e-mail address, Supabase hostname, project-ref-shaped string, any fragment of
the password, host or ref, strings longer than 256 characters, or non-finite numbers.

| Key | Content |
| --- | --- |
| `window` | start, end, `interval_seconds` |
| `server_version_num` | PostgreSQL version number |
| `role` | name, memberships, `read_only: true`, `table_select_privilege: false` |
| `session` | `statement_timeout_seconds`, `lock_timeout_seconds: 2`, `read_only: true` |
| `stats_reset` | `pg_stat_database.stats_reset` before/after; `reset_during_window` invalidates the deltas |
| `index_usage` | per named index: `idx_scan`, `idx_tup_read`, `idx_tup_fetch` before/after/delta, `valid`; `missing` lists named indexes not found |
| `statements` | `pg_stat_statements` location, `track`, info (`dealloc`, `stats_reset`) and per function: entries, calls, total/max exec ms, rows, shared blocks hit/read, temp blocks written, as deltas |
| `activity` | per sample: wait event type/name/state counts, blocked backend count, lock mode/granted counts on the named relations |
| `table_data` | `status: "not_permitted_pending_count_function"`, `table_select_privilege: false`, `skipped` (policy, reconcile names, explain names when requested) |

Named tables (no grant on any of them; their indexes and locks are read through statistics views):
`ingest_claim_sessions_policy`, `ingest_claim_sessions`, `ingest_claim_attempts`,
`ingest_batches`, `ingest_buffer_outbox`. Named indexes and functions: see
`scripts/db-readonly-checks-queries.ts` (`NAMED_INDEXES`, `NAMED_FUNCTIONS`).

## Interpretation

- **`ingest_claim_attempts_records_gin_idx` `idx_scan` delta > 0**: something probed the records GIN in
  the window, which means the 044040 detail fallback is still being read (044090's v2 reader off, or
  write/backfill incomplete). With the v2 reader on, expect 0. The policy switches that would confirm
  this are unavailable until the count function exists; read them by the owner's operator path if
  needed. Caveat: `buffered_search_gate_for` (043890) also uses `records @>` per claim; the planner
  normally answers that through the primary key, so if `statements.rows` shows gate calls and no
  detail-reader calls, do not conclude from the GIN delta alone.
- **`ingest_claim_sessions_session_idx` / `_accepted_idx` scans > 0**: the v2 reader is in use.
- **`stats_reset.reset_during_window` true** or `info_before.stats_reset` != `info_after.stats_reset`:
  discard the deltas and rerun.
- **`statements.info` `dealloc` delta > 0**: pg_stat_statements evicted entries in the window; counts are
  lower bounds. `track = top` hides nested calls (for example `ingest_claim_batch_mode` called from
  another function); missing rows do not mean zero calls.
- **`statements.installed` false**: the extension is not installed; the statements section is empty.
- **`activity` blocked > 0 or ungranted locks on the named relations**: lock contention on the ingest
  path during the window; correlate with the function timings.
- **`table_data.status` `not_permitted_pending_count_function`**: expected; see "Table data" above.

## Known gaps / unverified

Not yet exercised against Supabase: ADMIN-less `GRANT pg_read_all_stats, pg_monitor`; Supavisor pooler
login for a new role (`<role>.<ref>` user name is assumed); the schema holding `pg_stat_statements`
(`extensions` then `public` are tried). Cloudflare Worker Observability is a TODO (see the runner header).
