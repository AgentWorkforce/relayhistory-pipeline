/**
 * Read-only production database checks (sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-db-readonly-checks.md).
 *
 * Run only by .github/workflows/db-readonly-checks.yml in the approval-gated `production-db-readonly`
 * environment. It connects as sessions_readonly_stats (scripts/sql/db-readonly-role.sql), executes only the
 * fixed statements in scripts/db-readonly-checks-queries.ts, and writes one sanitized JSON artifact.
 *
 * It never writes: the session starts with default_transaction_read_only=on and the run refuses unless
 * SHOW transaction_read_only is on, the login is exactly sessions_readonly_stats with no elevated
 * attributes, its memberships are only pg_monitor/pg_read_all_stats, and it holds no privilege at all
 * (not even SELECT) on any table in schema sessions: the role is stats-only. Only statistics and catalog
 * views are read. The table-data sections (policy booleans, reconcile counts, EXPLAIN shapes) are reported
 * as "not_permitted_pending_count_function" until a later count-only SECURITY DEFINER function exists.
 *
 * Inputs (environment): DB_READONLY_URL (secret, postgresql://...?sslmode=verify-full),
 * SUPABASE_PROJECT_REF (environment variable), INTERVAL_SECONDS (60..1800, default 900),
 * STATEMENT_TIMEOUT_SECONDS (5..60, default 15), EXPLAIN ("true"/"false"; recorded, the shapes are
 * part of the skipped table-data section), OUTPUT (artifact file name, default db-readonly-checks.json).
 * Exit codes: 0 all sections ok, 1 a section failed (artifact still written), 2 refused (no artifact).
 *
 * TODO(cloudflare-observability-join): join the pg_stat_statements per-function deltas with the
 * Cloudflare Workers Observability aggregates for the ingest/read Workers over the same window. Not
 * implemented. It would need a read-only Cloudflare API token scoped to Workers Observability (account
 * analytics read) held as a separate environment secret, plus the account id and Worker script names as
 * environment variables, and a fixed aggregate query (requests, errors, CPU/wall p50/p99 per script)
 * bounded to [window.start, window.end]. Numbers only, through the same sanitizer.
 */
import { writeFileSync } from "node:fs";
import pg from "pg";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { requiredSupabaseProjectRef } from "../src/shared/deploy-target.ts";
import {
  ALLOWED_MEMBERSHIPS,
  EXPLAIN,
  NAMED_INDEXES,
  NAMED_TABLES,
  PGSS,
  PGSS_SCHEMAS,
  PRECHECK,
  READONLY_ROLE,
  RECONCILE,
  SAMPLE,
  SNAPSHOT,
  STATEMENT_TIMEOUT_DEFAULT,
  STATEMENT_TIMEOUT_MAX,
  STATEMENT_TIMEOUT_MIN,
  TABLE_DATA_STATUS,
  sessionOptions,
  sessionSettings,
  type PgssSchema,
} from "./db-readonly-checks-queries.ts";
import { sanitizeArtifact, SanitizeError } from "./db-readonly-checks-sanitize.ts";

export const INTERVAL_MIN = 60;
export const INTERVAL_MAX = 1800;
export const INTERVAL_DEFAULT = 900;
export const SAMPLE_EVERY_SECONDS = 30;
const APPLICATION_NAME = "96bulls-db-readonly-checks";

export class Refusal extends Error {
  override name = "Refusal";
  constructor(readonly code: string) {
    super(`refused: ${code}`);
  }
}
const refuse = (code: string): never => {
  throw new Refusal(code);
};

export type Inputs = { intervalSeconds: number; statementTimeoutSeconds: number; explain: boolean; output: string };
export function parseInputs(env: Record<string, string | undefined>): Inputs {
  const raw = env.INTERVAL_SECONDS ?? "";
  const intervalSeconds = raw === "" ? INTERVAL_DEFAULT : /^[0-9]{1,5}$/.test(raw) ? Number(raw) : NaN;
  if (!Number.isInteger(intervalSeconds) || intervalSeconds < INTERVAL_MIN || intervalSeconds > INTERVAL_MAX)
    refuse("interval_seconds_invalid");
  const rawTimeout = env.STATEMENT_TIMEOUT_SECONDS ?? "";
  const statementTimeoutSeconds =
    rawTimeout === "" ? STATEMENT_TIMEOUT_DEFAULT : /^[0-9]{1,2}$/.test(rawTimeout) ? Number(rawTimeout) : NaN;
  if (
    !Number.isInteger(statementTimeoutSeconds) ||
    statementTimeoutSeconds < STATEMENT_TIMEOUT_MIN ||
    statementTimeoutSeconds > STATEMENT_TIMEOUT_MAX
  )
    refuse("statement_timeout_seconds_invalid");
  const ex = env.EXPLAIN ?? "false";
  if (ex !== "true" && ex !== "false") refuse("explain_invalid");
  const output = env.OUTPUT ?? "db-readonly-checks.json";
  if (!/^[a-z0-9][a-z0-9._-]{0,63}\.json$/.test(output)) refuse("output_invalid");
  return { intervalSeconds, statementTimeoutSeconds, explain: ex === "true", output };
}

/** Discrete pg config for the read-only login. Never a connectionString; TLS pinned (owner-pg-config.ts). */
export function readonlyClientConfig(
  url: string | undefined,
  projectRef: string,
  statementTimeoutSeconds: number = STATEMENT_TIMEOUT_DEFAULT,
): pg.ClientConfig {
  if (!url) refuse("db_readonly_url_required");
  let base: pg.ClientConfig;
  try {
    base = ownerClientConfig(url!, APPLICATION_NAME);
  } catch (e) {
    return refuse(`db_readonly_url_${(e as { code?: string }).code ?? "invalid"}`);
  }
  if (base.user !== READONLY_ROLE && base.user !== `${READONLY_ROLE}.${projectRef}`) refuse("db_readonly_user");
  return {
    ...base,
    options: sessionOptions(statementTimeoutSeconds),
    connectionTimeoutMillis: 10000,
    // Client-side backstop, above the server statement_timeout.
    query_timeout: (statementTimeoutSeconds + 15) * 1000,
  };
}

export function errorCode(error: unknown): string {
  if (error instanceof Refusal) return error.code;
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[a-zA-Z0-9_]{1,64}$/.test(code) ? code : "query_failed";
}

/** int8 arrives as a string, timestamptz as Date: integers become numbers, dates ISO strings. Other strings stay. */
export function normalize(v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
  if (typeof v === "string" && /^-?[0-9]{1,15}$/.test(v)) return Number(v);
  if (Array.isArray(v)) return v.map(normalize);
  if (typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, x] of Object.entries(v as Record<string, unknown>)) out[k] = normalize(x);
    return out;
  }
  return v;
}

type Row = Record<string, unknown>;
type Q = (sql: string, params?: readonly string[]) => Promise<Row[]>;
type Section = { ok: true; [k: string]: unknown } | { ok: false; error: string };

async function section(fn: () => Promise<Record<string, unknown>>): Promise<Section> {
  try {
    return { ok: true, ...(await fn()) };
  } catch (e) {
    if (e instanceof Refusal) throw e;
    return { ok: false, error: errorCode(e) };
  }
}

export async function precheck(q: Q, statementTimeoutSeconds: number) {
  const expected = sessionSettings(statementTimeoutSeconds);
  const show = async (sql: string) => {
    const v = Object.values((await q(sql))[0] ?? {})[0];
    return v === null || v === undefined ? "" : String(v);
  };
  if ((await show(PRECHECK.readOnly)) !== "on") refuse("transaction_not_read_only");
  if ((await show(PRECHECK.defaultReadOnly)) !== expected.default_transaction_read_only)
    refuse("default_transaction_read_only");
  if ((await show(PRECHECK.statementTimeout)) !== expected.statement_timeout) refuse("statement_timeout");
  if ((await show(PRECHECK.lockTimeout)) !== expected.lock_timeout) refuse("lock_timeout");
  const serverVersion = Number(await show(PRECHECK.serverVersion));
  const me = (await q(PRECHECK.identity))[0];
  if (
    !me ||
    me.me !== READONLY_ROLE ||
    me.session !== READONLY_ROLE ||
    me.rolsuper !== false ||
    me.rolcreaterole !== false ||
    me.rolcreatedb !== false ||
    me.rolbypassrls !== false ||
    me.rolreplication !== false
  )
    refuse("identity");
  if (me!.read_all_stats !== true) refuse("pg_read_all_stats_missing");
  const roles = ((await q(PRECHECK.memberships))[0]?.roles ?? null) as unknown;
  if (!Array.isArray(roles) || roles.some((r) => !(ALLOWED_MEMBERSHIPS as readonly string[]).includes(String(r))))
    refuse("memberships");
  const writable = Number((await q(PRECHECK.schemaWritable))[0]?.writable ?? -1);
  if (writable !== 0) refuse("write_privilege_present");
  const selectable = Number((await q(PRECHECK.schemaSelectable))[0]?.selectable ?? -1);
  if (selectable !== 0) refuse("table_privilege_present");
  return { serverVersion, memberships: roles as string[], tableSelectPrivilege: false as const };
}

async function pgssLocation(q: Q): Promise<{ schema: PgssSchema | null; installed: boolean }> {
  const row = (await q(SNAPSHOT.pgssLocation))[0];
  if (!row) return { schema: null, installed: false };
  const schema = (PGSS_SCHEMAS as readonly string[]).includes(String(row.schema)) ? (row.schema as PgssSchema) : null;
  return { schema, installed: true };
}

async function snapshot(q: Q, pgss: PgssSchema | null) {
  const statsReset = await section(async () => ({ ...(normalize((await q(SNAPSHOT.statsReset))[0]) as Row) }));
  const indexes = await section(async () => ({ rows: normalize(await q(SNAPSHOT.indexes)) as Row[] }));
  const statements = await section(async () => {
    if (!pgss) return { available: false };
    const rows = normalize(await q(PGSS[pgss].aggregate)) as Row[];
    let info: unknown = null;
    try {
      info = normalize((await q(PGSS[pgss].info))[0] ?? null);
    } catch (e) {
      info = { error: errorCode(e) };
    }
    return { available: true, rows, info };
  });
  return { statsReset, indexes, statements };
}

const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : null);
function delta(a: Row[] | undefined, b: Row[] | undefined, key: string, fields: string[]) {
  const before = new Map((a ?? []).map((r) => [String(r[key]), r]));
  return (b ?? []).map((r) => {
    const p = before.get(String(r[key]));
    const out: Row = { [key]: r[key] };
    for (const f of fields) {
      const x = num(r[f]), y = num(p?.[f]);
      out[f] = x;
      out[`${f}_delta`] = x !== null && y !== null ? Math.round((x - y) * 1000) / 1000 : null;
    }
    if ("relname" in r) out.relname = r.relname;
    if ("valid" in r) out.valid = r.valid;
    return out;
  });
}

type Agg = Map<string, { key: Row; samples: number; max: number; sum: number; blocked_max: number }>;
function accumulate(agg: Agg, rows: Row[], keys: string[]) {
  for (const r of rows) {
    const key: Row = Object.fromEntries(keys.map((k) => [k, r[k]]));
    const id = JSON.stringify(key);
    const n = Number(r.n) || 0;
    const blocked = Number(r.blocked) || 0;
    const cur = agg.get(id) ?? { key, samples: 0, max: 0, sum: 0, blocked_max: 0 };
    cur.samples += 1;
    cur.max = Math.max(cur.max, n);
    cur.sum += n;
    cur.blocked_max = Math.max(cur.blocked_max, blocked);
    agg.set(id, cur);
  }
}
const flatten = (agg: Agg) =>
  [...agg.values()].map((v) => ({ ...v.key, samples_present: v.samples, max: v.max, sum: v.sum, blocked_max: v.blocked_max }));

export async function sampleInterval(q: Q, seconds: number, sleep: (ms: number) => Promise<void>) {
  const waits: Agg = new Map(), locks: Agg = new Map();
  let samples = 0, errors = 0;
  const steps = Math.max(1, Math.ceil(seconds / SAMPLE_EVERY_SECONDS));
  for (let i = 0; i < steps; i++) {
    try {
      accumulate(waits, (await q(SAMPLE.waits)) as Row[], ["wait_event_type", "wait_event", "state"]);
      accumulate(locks, (await q(SAMPLE.locks)) as Row[], ["locktype", "mode", "granted", "relname"]);
      samples += 1;
    } catch {
      errors += 1;
    }
    // Sleeps sum to exactly `seconds`; the second snapshot follows the last sleep.
    await sleep(Math.min(SAMPLE_EVERY_SECONDS, seconds - i * SAMPLE_EVERY_SECONDS) * 1000);
  }
  return { samples, errors, every_seconds: SAMPLE_EVERY_SECONDS, waits: flatten(waits), locks: flatten(locks) };
}

export async function runChecks(
  q: Q,
  inputs: Inputs,
  sleep: (ms: number) => Promise<void>,
  now: () => Date = () => new Date(),
) {
  const pre = await precheck(q, inputs.statementTimeoutSeconds);
  const loc = await pgssLocation(q);
  let track: string | null = null;
  try {
    track = loc.installed ? String(Object.values((await q(SNAPSHOT.pgssTrack))[0] ?? {})[0] ?? "") : null;
  } catch (e) {
    track = `error:${errorCode(e)}`;
  }
  // Only catalog/statistics views are ever read, so this run's own reads never land in the index deltas.
  const start = now().toISOString();
  const s1 = await snapshot(q, loc.schema);
  const activity = await sampleInterval(q, inputs.intervalSeconds, sleep);
  const s2 = await snapshot(q, loc.schema);
  const end = now().toISOString();

  const rows = (s: Section, k: string) => (s.ok ? ((s as Row)[k] as Row[] | undefined) : undefined);
  const resetBefore = s1.statsReset.ok ? (s1.statsReset as Row).stats_reset : null;
  const resetAfter = s2.statsReset.ok ? (s2.statsReset as Row).stats_reset : null;
  const missingIndexes = NAMED_INDEXES.filter((n) => !(rows(s2.indexes, "rows") ?? []).some((r) => r.indexrelname === n));

  // Table data (policy booleans, reconcile counts, EXPLAIN shapes) needs table privilege, which the
  // stats-only role never has (precheck refuses otherwise). Skipped, not failed; never reported as zero.
  const tableData = {
    status: TABLE_DATA_STATUS,
    table_select_privilege: pre.tableSelectPrivilege,
    skipped: { policy: true, reconcile: Object.keys(RECONCILE), explain: inputs.explain ? Object.keys(EXPLAIN) : [] },
  };

  return {
    schema: "db-readonly-checks/v1",
    window: { start, end, interval_seconds: inputs.intervalSeconds },
    session: { statement_timeout_seconds: inputs.statementTimeoutSeconds, lock_timeout_seconds: 2, read_only: true },
    server_version_num: pre.serverVersion,
    role: { name: READONLY_ROLE, memberships: pre.memberships, read_only: true, table_select_privilege: false },
    stats_reset: {
      before: resetBefore ?? null,
      after: resetAfter ?? null,
      reset_during_window: resetBefore !== resetAfter,
    },
    index_usage: {
      ok: s1.indexes.ok && s2.indexes.ok,
      ...(s1.indexes.ok ? {} : { error_before: (s1.indexes as { error: string }).error }),
      ...(s2.indexes.ok ? {} : { error_after: (s2.indexes as { error: string }).error }),
      before_at: rows(s1.indexes, "rows")?.[0]?.at ?? null,
      after_at: rows(s2.indexes, "rows")?.[0]?.at ?? null,
      missing: missingIndexes,
      rows: delta(rows(s1.indexes, "rows"), rows(s2.indexes, "rows"), "indexrelname", ["idx_scan", "idx_tup_read", "idx_tup_fetch"]),
    },
    statements: {
      installed: loc.installed,
      schema: loc.schema,
      track,
      ok: s1.statements.ok && s2.statements.ok,
      info_before: s1.statements.ok ? ((s1.statements as Row).info ?? null) : null,
      info_after: s2.statements.ok ? ((s2.statements as Row).info ?? null) : null,
      rows: delta(rows(s1.statements, "rows"), rows(s2.statements, "rows"), "fn", [
        "entries",
        "calls",
        "total_exec_ms",
        "max_exec_ms",
        "rows",
        "shared_blks_hit",
        "shared_blks_read",
        "temp_blks_written",
      ]),
    },
    activity,
    table_data: tableData,
    named: { tables: NAMED_TABLES, indexes: NAMED_INDEXES },
  };
}

export function artifactFailed(a: Awaited<ReturnType<typeof runChecks>>): boolean {
  return (
    !a.index_usage.ok ||
    !a.statements.ok ||
    a.activity.samples === 0
  );
}

async function main(): Promise<number> {
  let client: pg.Client | null = null;
  try {
    const inputs = parseInputs(process.env);
    const ref = (() => {
      try {
        return requiredSupabaseProjectRef(process.env);
      } catch (e) {
        return refuse(errorCode(e));
      }
    })();
    const config = readonlyClientConfig(process.env.DB_READONLY_URL, ref, inputs.statementTimeoutSeconds);
    const secrets = [String(config.password ?? ""), String(config.host ?? ""), ref];
    client = new pg.Client(config);
    client.on("error", () => {}); // Never dump driver/connection errors to logs.
    await client.connect();
    const c = client;
    const q: Q = async (sql, params) => (await c.query({ text: sql, values: params ? [...params] : undefined })).rows as Row[];
    const artifact = await runChecks(q, inputs, (ms) => new Promise((r) => setTimeout(r, ms)));
    sanitizeArtifact(artifact, secrets);
    writeFileSync(inputs.output, JSON.stringify(artifact, null, 2) + "\n", { mode: 0o644 });
    const failed = artifactFailed(artifact);
    console.log(JSON.stringify({ result: failed ? "section_failed" : "ok", output: inputs.output }));
    return failed ? 1 : 0;
  } catch (e) {
    const code = e instanceof SanitizeError ? `sanitize_${e.code}` : errorCode(e);
    console.log(JSON.stringify({ result: "refused", code }));
    return 2;
  } finally {
    await client?.end().catch(() => {});
  }
}

if (import.meta.main) process.exitCode = await main();
