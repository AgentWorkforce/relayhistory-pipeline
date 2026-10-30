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
 *
 * Rows move as JSON through jsonb_populate_recordset, so every column type
 * (jsonb, arrays, timestamps, bytea, vector) is converted by Postgres itself.
 * Generated columns are skipped. The target session runs with triggers off
 * (prepareTargetSession), so rows land exactly as Neon has them. Keyset pages on the primary key make each run
 * resumable from a checkpoint. Nothing here prints row contents or URLs.
 *
 * Usage: RELAYHISTORY_SOURCE_URL=... BULLS_TARGET_URL=... \
 *   bun run migration:copy --mode=mirror|backfill --tables=a,b [--chunk-size=500]
 *   [--checkpoint=/secure/path.json] [--keep-target-only=machines] [--dry-run]
 */
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pg from "pg";

export type CopyMode = "mirror" | "backfill";

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
  const forward = mode !== "backfill";
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

export function writeSql(plan: TablePlan, mode: CopyMode): string {
  const columns = plan.columns.map(quoteIdent).join(", ");
  const insert = `INSERT INTO ${qualified(plan.table)} (${columns})
    SELECT ${columns} FROM jsonb_populate_recordset(NULL::${qualified(plan.table)}, $1::jsonb)`;
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

export interface CopyOptions {
  mode: CopyMode;
  chunkSize: number;
  dryRun?: boolean;
  /**
   * Mirror only: keep target rows the source lacks. For tables another writer
   * shares on Supabase (sessions.machines: pipeline-only machines).
   */
  keepTargetOnly?: boolean;
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
    if (!options.dryRun) {
      const written = await target.query<{ n: number }>(
        `WITH written AS (${writeSql(plan, options.mode)} RETURNING 1) SELECT count(*)::int AS n FROM written`,
        [page.rows],
      );
      result.written += written.rows[0]?.n ?? 0;
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
  if (mode !== "mirror" && mode !== "backfill")
    throw new Error("--mode must be mirror or backfill");
  const tables = (argument("tables") ?? "").split(",").filter(Boolean);
  if (tables.length === 0 || tables.some((table) => !/^[a-z_][a-z0-9_]*$/.test(table)))
    throw new Error("--tables must list sessions table names");
  const chunkSize = Number(argument("chunk-size") ?? 500);
  if (!Number.isInteger(chunkSize) || chunkSize < 1)
    throw new Error("--chunk-size must be a positive integer");
  const dryRun = process.argv.includes("--dry-run");
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
    await prepareTargetSession(target);
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
