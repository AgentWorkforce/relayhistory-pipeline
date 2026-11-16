/**
 * Feature detection for the regeneration schema (supabase 20261105062040, hoopdb 0006): the code can
 * ship before the migrations are applied. With a flag on and the columns or table missing, the
 * worker logs once and behaves as if the flag were off; it never crashes. The answer is cached per
 * process and database handle (as brief-summary.ts caches its to_regprocedure probe); a positive
 * answer is final, a negative one is re-probed at most every `recheckMs` so applying the migration
 * later takes effect without a restart.
 */
import { log } from "../shared/log.ts";

export interface Queryable {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

interface Entry { ok: boolean; at: number; pending?: Promise<boolean> }
const cache = new WeakMap<object, Map<string, Entry>>();
const warned = new Set<string>();

export const COLUMNS_SQL =
  "SELECT count(*)::int AS n FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = ANY($3::text[])";
export const RELATION_SQL = "SELECT to_regclass($1) IS NOT NULL AS ok";

async function probe(db: Queryable, id: string, check: () => Promise<boolean>, recheckMs: number, now: number): Promise<boolean> {
  let map = cache.get(db);
  if (!map) cache.set(db, (map = new Map()));
  const hit = map.get(id);
  if (hit?.ok) return true;
  if (hit && !hit.pending && now - hit.at < recheckMs) return false;
  if (hit?.pending) return hit.pending;
  const pending = check().catch(() => false);
  map.set(id, { ok: hit?.ok ?? false, at: hit?.at ?? 0, pending });
  const ok = await pending;
  map.set(id, { ok, at: now });
  if (!ok && !warned.has(id)) {
    warned.add(id);
    log.warn("summary regeneration schema missing; flags behave as off", { reason: "schema_missing", kind: id.split(":")[0] });
  }
  return ok;
}

/** True when every named column exists on schema.table. */
export function hasColumns(db: Queryable, table: string, columns: readonly string[], opts: { recheckMs?: number; now?: number } = {}): Promise<boolean> {
  const [schema, name] = table.split(".") as [string, string];
  return probe(db, `columns:${table}:${columns.join(",")}`, async () => {
    const r = await db.query<{ n: number }>(COLUMNS_SQL, [schema, name, [...columns]]);
    return Number(r.rows[0]?.n ?? 0) === columns.length;
  }, opts.recheckMs ?? 300_000, opts.now ?? Date.now());
}

export function hasRelation(db: Queryable, relation: string, opts: { recheckMs?: number; now?: number } = {}): Promise<boolean> {
  return probe(db, `relation:${relation}`, async () => {
    const r = await db.query<{ ok: boolean }>(RELATION_SQL, [relation]);
    return r.rows[0]?.ok === true;
  }, opts.recheckMs ?? 300_000, opts.now ?? Date.now());
}

/** Test seam: forget the once-per-process warnings (answers are cached per database handle). */
export function resetDetection(): void {
  warned.clear();
}

export const SUMMARY_COVERAGE_COLUMNS = ["covered_turns", "failure_code", "degraded", "attempts", "next_retry_at", "config_hash", "pipeline_version"] as const;
export const BRIEF_COVERAGE_COLUMNS = ["covered_turns", "retry_attempts", "next_retry_at", "config_hash", "pipeline_version"] as const;
export const AUDIT_RELATION = "sessions.summary_regeneration_audit";
