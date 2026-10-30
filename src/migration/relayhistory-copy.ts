/**
 * Row copy from RelayHistory's Neon `sessions` schema into Supabase, for the T0
 * cutover (docs/relayhistory-supabase-migration.md, docs/relayhistory-t0-write-hold.md).
 *
 *   mirror   T0 copy set (tokens, deletion tombstones, cursors). Source wins:
 *            changed rows are updated, and target rows whose key is gone from
 *            the source are deleted. Only before or during the hold, while
 *            Supabase has taken no writes of its own.
 *   backfill History after T0. Missing rows are inserted, newest key first;
 *            rows that already exist (written after T0) are never touched.
 *   check    Read-only: count source rows whose key Supabase lacks. Zero for
 *            every table is the criterion for removing the Neon read fallback
 *            (exit status 2 otherwise).
 *
 * Rows move as JSON through jsonb_populate_recordset, so every column type
 * (jsonb, arrays, timestamps, bytea, vector) is converted by Postgres itself.
 * Generated columns are skipped. The target session runs with triggers off
 * (prepareTargetSession), so rows land exactly as Neon has them. Keyset pages on the primary key make each run
 * resumable from a checkpoint. Nothing here prints row contents or URLs.
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... BULLS_TARGET_URL=... \
 *   bun run migration:copy --mode=mirror|backfill|check --tables=a,b [--chunk-size=500]
 *   [--checkpoint=/secure/path.json] [--keep-target-only=machines]
 *   [--execute --expect-supabase-ref=<ref>]   (without --execute: a read-only dry run)
 */
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pg from "pg";
import { guardFromArgv } from "./target-guard.js";

export type CopyMode = "mirror" | "backfill" | "check";

/** The query surface shared by node-postgres and PGlite. */
export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

export interface TablePlan {
  table: string;
  /** Conflict target: the primary key, or a unique key that treats NULLs as equal. */
  key: string[];
  /**
   * Keyset order as SQL expressions. A nullable key column orders as a text
   * NULL flag then `coalesce(col::text, '')`, because a plain tuple comparison
   * would silently skip rows whose key holds a NULL.
   */
  order: string[];
  nullableKey: boolean;
  columns: string[];
}

export interface TableResult {
  table: string;
  read: number;
  written: number;
  deleted: number;
  /** check mode: source rows whose key the target lacks. */
  missing: number;
  pages: number;
  after: unknown[] | null;
  complete: boolean;
}

const SCHEMA = "sessions";
const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;
const qualified = (table: string) => `${quoteIdent(SCHEMA)}.${quoteIdent(table)}`;

interface KeyInfo {
  columns: string[];
  nullable: boolean[];
}

/**
 * The primary key, else one unique, non-partial, column-only index that
 * either has all key columns NOT NULL or treats NULLs as equal (NULLS NOT
 * DISTINCT). Derived tables such as session_rollups have only the latter.
 */
async function keyInfo(db: Queryable, table: string): Promise<KeyInfo> {
  const { rows } = await db.query<{ index: string; name: string; nullable: boolean }>(
    `WITH candidate AS (
       SELECT i.indexrelid, i.indisprimary, i.indkey, i.indnkeyatts, c.oid AS relid
         FROM pg_index i
         JOIN pg_class c ON c.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indisvalid
          AND i.indpred IS NULL AND i.indexprs IS NULL
          AND (i.indisprimary OR i.indnullsnotdistinct OR NOT EXISTS (
                SELECT 1 FROM unnest(i.indkey) AS k(attnum)
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                 WHERE NOT a.attnotnull))
        ORDER BY i.indisprimary DESC, i.indnkeyatts, i.indexrelid
        LIMIT 1)
     SELECT candidate.indexrelid::text AS index, a.attname AS name, NOT a.attnotnull AS nullable
       FROM candidate
       JOIN unnest(candidate.indkey) WITH ORDINALITY AS k(attnum, position) ON true
       JOIN pg_attribute a ON a.attrelid = candidate.relid AND a.attnum = k.attnum
      WHERE k.position <= candidate.indnkeyatts
      ORDER BY k.position`,
    [SCHEMA, table],
  );
  return { columns: rows.map((row) => row.name), nullable: rows.map((row) => row.nullable) };
}

/** Writable (non-generated, non-dropped) columns, in table order. */
async function writableColumns(db: Queryable, table: string): Promise<string[]> {
  const { rows } = await db.query<{ name: string }>(
    `SELECT a.attname AS name
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0
        AND NOT a.attisdropped AND a.attgenerated = ''
      ORDER BY a.attnum`,
    [SCHEMA, table],
  );
  return rows.map((row) => row.name);
}

/**
 * Copy the source's columns that the target also has. The same primary key is
 * required on both sides; anything else fails closed before a row moves.
 */
export async function planTable(
  source: Queryable,
  target: Queryable,
  table: string,
): Promise<TablePlan> {
  const [sourceKey, targetKey, sourceColumns, targetColumns] = await Promise.all([
    keyInfo(source, table),
    keyInfo(target, table),
    writableColumns(source, table),
    writableColumns(target, table),
  ]);
  if (sourceKey.columns.length === 0)
    throw new Error(`${table}: no primary or usable unique key on the source`);
  if (sourceKey.columns.join("\0") !== targetKey.columns.join("\0"))
    throw new Error(`${table}: keys differ between source and target`);
  const available = new Set(targetColumns);
  const missing = sourceColumns.filter((column) => !available.has(column));
  if (missing.length)
    throw new Error(`${table}: target lacks ${missing.length} source column(s)`);
  const order = sourceKey.columns.flatMap((column, index) =>
    sourceKey.nullable[index]
      ? [
          `(CASE WHEN ${quoteIdent(column)} IS NULL THEN '1' ELSE '0' END)`,
          `coalesce(${quoteIdent(column)}::text, '')`,
        ]
      : [quoteIdent(column)],
  );
  return {
    table,
    key: sourceKey.columns,
    order,
    nullableKey: sourceKey.nullable.some(Boolean),
    columns: sourceColumns,
  };
}

function keyTuple(columns: string[]): string {
  const list = columns.map(quoteIdent).join(", ");
  return columns.length === 1 ? list : `(${list})`;
}

function orderTuple(order: string[]): string {
  return order.length === 1 ? order[0]! : `(${order.join(", ")})`;
}

function keyParams(columns: string[], first: number): string {
  const list = columns.map((_, index) => `$${first + index}`).join(", ");
  return columns.length === 1 ? list : `(${list})`;
}

/**
 * One keyset page, serialized by Postgres: `rows` is JSON text that goes
 * straight into the target, and `last` is the page's final key as text values.
 * Node never parses row values, so bytea, timestamp without time zone, bigint
 * and vector survive unchanged.
 */
export function pageSql(plan: TablePlan, mode: CopyMode, hasAfter: boolean): string {
  const projection = plan.columns.map(quoteIdent).join(", ");
  const forward = mode === "mirror";
  const where = hasAfter
    ? ` WHERE ${orderTuple(plan.order)} ${forward ? ">" : "<"} ${keyParams(plan.order, 2)}`
    : "";
  const order = (ascending: boolean) =>
    plan.order.map((expression) => `${expression} ${ascending ? "ASC" : "DESC"}`).join(", ");
  const lastKey = `json_build_array(${plan.order.map((expression) => `${expression}::text`).join(", ")})::text`;
  return `WITH page AS (
    SELECT ${projection} FROM ${qualified(plan.table)}${where} ORDER BY ${order(forward)} LIMIT $1
  )
  SELECT coalesce((SELECT json_agg(page)::text FROM page), '[]') AS rows,
         (SELECT count(*)::int FROM page) AS n,
         (SELECT ${lastKey} FROM page ORDER BY ${order(!forward)} LIMIT 1) AS last`;
}

export function writeSql(plan: TablePlan, mode: CopyMode, fenced = false): string {
  const columns = plan.columns.map(quoteIdent).join(", ");
  const insert = `INSERT INTO ${qualified(plan.table)} (${columns})
    SELECT ${columns} FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb) s${
      mode === "backfill" && fenced ? tombstoneFilter(plan) : ""
    }`;
  const conflict = `ON CONFLICT (${plan.key.map(quoteIdent).join(", ")})`;
  if (mode === "backfill") return `${insert} ${conflict} DO NOTHING`;
  const updates = plan.columns.filter((column) => !plan.key.includes(column));
  if (updates.length === 0) return `${insert} ${conflict} DO NOTHING`;
  const set = updates
    .map((column) => `${quoteIdent(column)} = EXCLUDED.${quoteIdent(column)}`)
    .join(", ");
  // Skip identical rows so a catch-up pass rewrites only what changed.
  const changed = `(${updates.map((column) => `${qualified(plan.table)}.${quoteIdent(column)}`).join(", ")}) IS DISTINCT FROM (${updates.map((column) => `EXCLUDED.${quoteIdent(column)}`).join(", ")})`;
  return `${insert} ${conflict} DO UPDATE SET ${set} WHERE ${changed}`;
}

/** Mirror only: target keys within a source page's key range that the source no longer has. */
export function staleKeysSql(plan: TablePlan, hasAfter: boolean): string {
  const keys = plan.key.map(quoteIdent).join(", ");
  const lower = hasAfter ? `${keyTuple(plan.key)} > ${keyParams(plan.key, 1)} AND ` : "";
  const upperStart = hasAfter ? plan.key.length + 1 : 1;
  return `SELECT ${keys} FROM ${qualified(plan.table)}
    WHERE ${lower}${keyTuple(plan.key)} <= ${keyParams(plan.key, upperStart)}
      AND NOT (${keyTuple(plan.key)} IN (
        SELECT ${keys} FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $${upperStart + plan.key.length}::jsonb)))`;
}

const TOMBSTONE_COLUMNS = ["org_id", "workspace_id", "user_id", "origin_id", "source", "session_id"];

/**
 * Backfill must not resurrect what users deleted on Supabase after T0. A hard
 * delete leaves no key to conflict with, so each row is checked against:
 *   - sessions.deleted_upload_sessions, matched on every tombstone column the
 *     table has (it needs at least org_id and session_id);
 *   - resets recorded on Supabase after T0 (pg_temp.backfill_resets): a reset
 *     deletes everything the user had, so none of the user's history returns.
 *     Tables without user columns match the reset users' sessions instead.
 */
export function tombstoneFilter(plan: TablePlan): string {
  const has = (column: string) => plan.columns.includes(column);
  const parts: string[] = [];
  if (has("org_id") && has("session_id")) {
    const match = TOMBSTONE_COLUMNS.filter(has)
      .map((column) => `d.${quoteIdent(column)} = s.${quoteIdent(column)}`)
      .join(" AND ");
    parts.push(`NOT EXISTS (SELECT 1 FROM sessions.deleted_upload_sessions d WHERE ${match})`);
  }
  if (has("org_id") && has("workspace_id") && has("user_id"))
    parts.push(
      "NOT EXISTS (SELECT 1 FROM pg_temp.backfill_resets r WHERE r.org_id = s.org_id AND r.workspace_id = s.workspace_id AND r.user_id = s.user_id)",
    );
  else if (has("org_id") && has("session_id"))
    parts.push(
      "NOT EXISTS (SELECT 1 FROM pg_temp.backfill_reset_sessions r WHERE r.org_id = s.org_id AND r.session_id = s.session_id)",
    );
  return parts.length ? ` WHERE ${parts.join(" AND ")}` : "";
}

/**
 * The same per-user lock the deletion functions take
 * (set_session_uploads, reset_uploaded_sessions), so a deletion either commits
 * before this page's insert sees the tombstone, or waits and deletes it.
 */
export function deletionLockSql(plan: TablePlan): string | null {
  if (!["org_id", "workspace_id", "user_id"].every((column) => plan.columns.includes(column)))
    return null;
  return `SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(org_id, workspace_id, user_id)::text, 0))
    FROM (SELECT DISTINCT org_id, workspace_id, user_id
            FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb)
           ORDER BY 1, 2, 3) AS users`;
}

/**
 * Load the backfill's deletion fences into this target session: resets that
 * exist on Supabase but not on Neon were made after T0. Fails closed if the
 * tombstone tables are missing.
 */
export async function prepareBackfillFence(source: Queryable, target: Queryable): Promise<void> {
  const sourceResets = await source.query<{ keys: string }>(
    `SELECT coalesce(json_agg(json_build_array(org_id, workspace_id, user_id, request_id))::text, '[]') AS keys
       FROM sessions.session_resets`,
  );
  await target.query(`CREATE TEMP TABLE IF NOT EXISTS backfill_resets (
    org_id text, workspace_id text, user_id text, PRIMARY KEY (org_id, workspace_id, user_id))`);
  await target.query(`CREATE TEMP TABLE IF NOT EXISTS backfill_reset_sessions (
    org_id text, session_id text, PRIMARY KEY (org_id, session_id))`);
  await target.query("TRUNCATE pg_temp.backfill_resets, pg_temp.backfill_reset_sessions");
  const resets = await target.query<{ users: string }>(
    `WITH neon AS (
       SELECT k->>0 AS org_id, k->>1 AS workspace_id, k->>2 AS user_id, k->>3 AS request_id
         FROM jsonb_array_elements($1::jsonb) AS k),
     after_t0 AS (
       SELECT DISTINCT r.org_id, r.workspace_id, r.user_id FROM sessions.session_resets r
        WHERE NOT EXISTS (SELECT 1 FROM neon n WHERE n.org_id = r.org_id
          AND n.workspace_id = r.workspace_id AND n.user_id = r.user_id AND n.request_id = r.request_id)),
     stored AS (INSERT INTO pg_temp.backfill_resets SELECT * FROM after_t0 ON CONFLICT DO NOTHING RETURNING *)
     SELECT coalesce(json_agg(json_build_array(org_id, workspace_id, user_id))::text, '[]') AS users FROM stored`,
    [sourceResets.rows[0]?.keys ?? "[]"],
  );
  // Sessions those users owned on Neon, for tables without user columns.
  const sessions = await source.query<{ keys: string }>(
    `SELECT coalesce(json_agg(DISTINCT jsonb_build_array(e.org_id, e.session_id))::text, '[]') AS keys
       FROM sessions.convergence_events e
       JOIN jsonb_array_elements($1::jsonb) AS u
         ON e.org_id = u->>0 AND e.workspace_id = u->>1 AND e.user_id = u->>2`,
    [resets.rows[0]?.users ?? "[]"],
  );
  await target.query(
    `INSERT INTO pg_temp.backfill_reset_sessions
     SELECT k->>0, k->>1 FROM jsonb_array_elements($1::jsonb) AS k ON CONFLICT DO NOTHING`,
    [sessions.rows[0]?.keys ?? "[]"],
  );
}

/** check mode: rows of a source page whose key is absent on the target (NULL-safe). */
export function missingSql(plan: TablePlan): string {
  const match = plan.key
    .map((column) => `t.${quoteIdent(column)} IS NOT DISTINCT FROM s.${quoteIdent(column)}`)
    .join(" AND ");
  return `SELECT count(*)::int AS n
    FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb) s
   WHERE NOT EXISTS (SELECT 1 FROM ${qualified(plan.table)} t WHERE ${match})`;
}

export interface CopyOptions {
  mode: CopyMode;
  chunkSize: number;
  dryRun?: boolean;
  /**
   * Mirror only: keep target rows the source lacks. For tables another writer
   * shares on Supabase (sessions.machines: pipeline-only machines).
   */
  keepTargetOnly?: boolean;
  /** backfill: honour deletion tombstones and resets (prepareBackfillFence first). */
  fenced?: boolean;
  /** Resume point from a checkpoint; null starts from the first key. */
  after?: unknown[] | null;
  onPage?: (result: TableResult) => Promise<void>;
}

export async function copyTable(
  source: Queryable,
  target: Queryable,
  plan: TablePlan,
  options: CopyOptions,
): Promise<TableResult> {
  if (options.mode === "mirror" && plan.nullableKey)
    throw new Error(`${plan.table}: mirror needs a key without NULLs`);
  const result: TableResult = {
    table: plan.table,
    read: 0,
    written: 0,
    deleted: 0,
    missing: 0,
    pages: 0,
    after: options.after ?? null,
    complete: false,
  };
  for (;;) {
    const hasAfter = result.after !== null;
    const page = (
      await source.query<{ rows: string; n: number; last: string | null }>(
        pageSql(plan, options.mode, hasAfter),
        hasAfter ? [options.chunkSize, ...result.after!] : [options.chunkSize],
      )
    ).rows[0]!;
    const upper = page.last === null ? null : (JSON.parse(page.last) as string[]);
    if (options.mode === "mirror" && !options.keepTargetOnly) {
      // Delete target rows in this key range the source no longer has. With no
      // source page left, the open-ended tail of target-only keys goes too.
      const stale = upper
        ? await target.query(staleKeysSql(plan, hasAfter), [
            ...(hasAfter ? result.after! : []),
            ...upper,
            page.rows,
          ])
        : await target.query(
            `SELECT ${plan.key.map(quoteIdent).join(", ")} FROM ${qualified(plan.table)}${
              hasAfter ? ` WHERE ${keyTuple(plan.key)} > ${keyParams(plan.key, 1)}` : ""
            }`,
            hasAfter ? result.after! : [],
          );
      if (stale.rows.length && !options.dryRun) {
        const keys = plan.key.map(quoteIdent).join(", ");
        await target.query(
          `DELETE FROM ${qualified(plan.table)} WHERE ${keyTuple(plan.key)} IN (
             SELECT ${keys} FROM ${qualified(plan.table)}
              WHERE ${keyTuple(plan.key)} IN (${staleSubquery(plan, hasAfter, upper !== null)}))`,
          [...(hasAfter ? result.after! : []), ...(upper ?? []), ...(upper ? [page.rows] : [])],
        );
      }
      result.deleted += stale.rows.length;
    }
    if (upper === null) {
      result.complete = true;
      await options.onPage?.(result);
      return result;
    }
    if (options.mode === "check") {
      const absent = await target.query<{ n: number }>(missingSql(plan), [page.rows]);
      result.missing += absent.rows[0]?.n ?? 0;
    } else if (!options.dryRun) {
      const fenced = options.mode === "backfill" && options.fenced === true;
      const lock = fenced ? deletionLockSql(plan) : null;
      if (fenced) await target.query("BEGIN");
      try {
        if (lock) await target.query(lock, [page.rows]);
        const written = await target.query<{ n: number }>(
          `WITH written AS (${writeSql(plan, options.mode, fenced)} RETURNING 1) SELECT count(*)::int AS n FROM written`,
          [page.rows],
        );
        if (fenced) await target.query("COMMIT");
        result.written += written.rows[0]?.n ?? 0;
      } catch (error) {
        if (fenced) await target.query("ROLLBACK").catch(() => {});
        throw error;
      }
    }
    result.read += page.n;
    result.pages += 1;
    result.after = upper;
    await options.onPage?.(result);
    if (page.n < options.chunkSize) {
      result.complete = true;
      // Backfill is done; mirror makes one more pass to clear trailing target-only keys.
      if (options.mode !== "mirror" || options.keepTargetOnly) return result;
    }
  }
}

/** The key subquery behind staleKeysSql, or the open tail when the source has no page left. */
function staleSubquery(plan: TablePlan, hasAfter: boolean, bounded: boolean): string {
  if (bounded) {
    const select = staleKeysSql(plan, hasAfter);
    return select;
  }
  const keys = plan.key.map(quoteIdent).join(", ");
  return `SELECT ${keys} FROM ${qualified(plan.table)}${
    hasAfter ? ` WHERE ${keyTuple(plan.key)} > ${keyParams(plan.key, 1)}` : ""
  }`;
}

/**
 * Copies replicate rows as they are: user triggers (projections, rollups,
 * revision feed, analysis wakeups) must not fire, or a backfill would replay
 * every historical session into the outbox, the brief queue and Cloud's feed.
 * `replica` disables them for this session only; live traffic keeps them.
 * Fails closed if the role may not set it.
 */
export async function prepareTargetSession(target: Queryable): Promise<void> {
  await target.query("SET session_replication_role = replica");
  const { rows } = await target.query<{ session_replication_role: string }>(
    "SHOW session_replication_role",
  );
  if (rows[0]?.session_replication_role !== "replica")
    throw new Error("target session could not disable triggers");
}

interface Checkpoint {
  version: 1;
  mode: CopyMode;
  tables: Record<string, TableResult>;
}

async function loadCheckpoint(path: string | undefined, mode: CopyMode): Promise<Checkpoint> {
  const empty: Checkpoint = { version: 1, mode, tables: {} };
  if (!path) return empty;
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as Checkpoint;
    if (parsed.version !== 1 || parsed.mode !== mode)
      throw new Error("checkpoint is for a different mode");
    return parsed;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty;
    throw error;
  }
}

async function saveCheckpoint(path: string | undefined, checkpoint: Checkpoint) {
  if (!path) return;
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}`;
  await writeFile(temporary, `${JSON.stringify(checkpoint, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporary, path);
}

function argument(name: string): string | undefined {
  return process.argv.find((value) => value.startsWith(`--${name}=`))?.slice(name.length + 3);
}

export async function main() {
  const sourceUrl = process.env.RELAYHISTORY_SOURCE_URL;
  const targetUrl = process.env.BULLS_TARGET_URL;
  if (!sourceUrl || !targetUrl)
    throw new Error("RELAYHISTORY_SOURCE_URL and BULLS_TARGET_URL are required");
  const mode = argument("mode");
  if (mode !== "mirror" && mode !== "backfill" && mode !== "check")
    throw new Error("--mode must be mirror, backfill or check");
  const tables = (argument("tables") ?? "").split(",").filter(Boolean);
  if (tables.length === 0 || tables.some((table) => !/^[a-z_][a-z0-9_]*$/.test(table)))
    throw new Error("--tables must list sessions table names");
  const chunkSize = Number(argument("chunk-size") ?? 500);
  if (!Number.isInteger(chunkSize) || chunkSize < 1)
    throw new Error("--chunk-size must be a positive integer");
  // Writes need --execute and a positively identified target; check never writes.
  const dryRun =
    mode === "check" || guardFromArgv(process.argv, targetUrl, sourceUrl) === "dry-run";
  const keepTargetOnly = new Set(
    (argument("keep-target-only") ?? "").split(",").filter(Boolean),
  );
  const checkpointPath = argument("checkpoint");
  const checkpoint = await loadCheckpoint(checkpointPath, mode);
  // One connection per side for the whole run, so session settings hold.
  const source = new pg.Client({ connectionString: sourceUrl, application_name: "96bulls-copy-source" });
  const target = new pg.Client({ connectionString: targetUrl, application_name: "96bulls-copy-target" });
  try {
    await Promise.all([source.connect(), target.connect()]);
    // The source is only ever read.
    await source.query("SET default_transaction_read_only = on");
    // Dry runs and checks only read; executed copies land with triggers off.
    if (dryRun) await target.query("SET default_transaction_read_only = on");
    else await prepareTargetSession(target);
    // Deletion fences need temp tables, which a read-only dry run cannot create.
    if (mode === "backfill" && !dryRun) await prepareBackfillFence(source, target);
    const results: TableResult[] = [];
    for (const table of tables) {
      const previous = checkpoint.tables[table];
      if (previous?.complete && mode === "backfill") {
        results.push(previous);
        continue;
      }
      const plan = await planTable(source, target, table);
      const result = await copyTable(source, target, plan, {
        mode,
        chunkSize,
        dryRun,
        keepTargetOnly: keepTargetOnly.has(table),
        fenced: mode === "backfill" && !dryRun,
        after: mode === "backfill" ? (previous?.after ?? null) : null,
        onPage: async (progress) => {
          checkpoint.tables[table] = { ...progress };
          if (!dryRun) await saveCheckpoint(checkpointPath, checkpoint);
        },
      });
      results.push(result);
    }
    process.stdout.write(
      `${JSON.stringify({ generatedAt: new Date().toISOString(), mode, dryRun, tables: results.map(({ after: _after, ...rest }) => rest) }, null, 2)}\n`,
    );
    if (mode === "check" && results.some((result) => result.missing > 0)) process.exitCode = 2;
  } finally {
    await Promise.all([source.end(), target.end()]);
  }
}

if (import.meta.main)
  main().catch((error) => {
    console.error(
      `96bulls copy failed: ${error instanceof Error ? error.message : "unknown error"}`,
    );
    process.exit(1);
  });
