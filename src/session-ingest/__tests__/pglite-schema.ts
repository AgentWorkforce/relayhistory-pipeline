/**
 * TEST-ONLY schema helper for the PGlite suites: the Supabase shim, the four role bootstraps
 * and the sorted supabase/migrations, optionally cut off at one migration.
 *
 * HOOPDB_MIGRATE_UNTIL=<migration filename> (or `{ until }`) applies the migrations up to and
 * including that file and nothing after it. It exists to prove that the session-ingest Worker
 * runs on a database that has not yet applied later migrations (the G4 set:
 * 20261105043800_lazy_ack_clock_no_key_update.sql, see g4-only-schema.pglite.test.ts).
 * Unset: every migration, exactly as the existing suites apply them. An unknown filename throws,
 * so a typo can never silently run the full set.
 *
 * Production never reads it: src/hoopdb/migrate.ts has no cutoff, and its plan / generation are
 * unchanged. Nothing outside src/session-ingest/__tests__ imports this file.
 */
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import type { PGlite } from "@electric-sql/pglite";

export const REPO_ROOT = join(import.meta.dir, "../../..");
/** The last migration of the G4 set (55 files plus bootstrap). */
export const G4_LAST_MIGRATION = "20261105043800_lazy_ack_clock_no_key_update.sql";
/** The migrations G4 excludes: #332, #338, #339 and #327 (in that file order). */
export const POST_G4_MIGRATIONS = [
  "20261105043850_ingest_acquisition_owner.sql",
  "20261105043860_buffered_search_gate.sql",
  "20261105043870_buffered_search_feed_cursor.sql",
  "20261105043880_ingest_bookkeeping_cleanup.sql",
] as const;
export const BOOTSTRAPS = ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"] as const;

/**
 * The sorted migration filenames, cut off (inclusive) at `until`. `until` omitted (undefined): the
 * HOOPDB_MIGRATE_UNTIL environment variable, if set. `null`: every migration, whatever the environment.
 */
export function migrationFiles(root = REPO_ROOT, until?: string | null): string[] {
  const all = readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort();
  const cut = until === undefined ? process.env.HOOPDB_MIGRATE_UNTIL || null : until;
  if (cut === null) return all;
  const at = all.indexOf(cut);
  if (at < 0) throw new Error(`HOOPDB_MIGRATE_UNTIL names no migration: ${cut}`);
  return all.slice(0, at + 1);
}

/** Shim, bootstraps, then the (optionally cut-off) migrations. Returns the applied filenames. */
export async function applySupabaseSchema(db: Pick<PGlite, "exec">, options: { until?: string | null; root?: string } = {}): Promise<string[]> {
  const root = options.root ?? REPO_ROOT;
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of BOOTSTRAPS) await db.exec(read(`supabase/bootstrap/${f}.sql`));
  const files = migrationFiles(root, options.until);
  for (const f of files) await db.exec(read(`supabase/migrations/${f}`));
  return files;
}

/** undefined_function, undefined_table, undefined_column, undefined_object. */
export const UNDEFINED_OBJECT_SQLSTATES: ReadonlySet<string> = new Set(["42883", "42P01", "42703", "42704"]);
export interface UndefinedObjectError { code: string; message: string; sql: string }

/**
 * Wraps `db.query` / `db.exec` so every statement that fails with an undefined-object SQLSTATE is
 * recorded (and still thrown). Every caller of the instance (sqlLedger's run, the fixtures) goes
 * through it. A plpgsql EXCEPTION block that swallows the error is invisible here; the G4 proof
 * covers that with a pg_proc source scan.
 */
export function guardUndefinedObjects(db: PGlite): UndefinedObjectError[] {
  const seen: UndefinedObjectError[] = [];
  for (const method of ["query", "exec"] as const) {
    const original = (db[method] as (...a: unknown[]) => Promise<unknown>).bind(db);
    (db as unknown as Record<string, unknown>)[method] = async (...args: unknown[]) => {
      try {
        return await original(...args);
      } catch (error) {
        const code = (error as { code?: unknown })?.code;
        if (typeof code === "string" && UNDEFINED_OBJECT_SQLSTATES.has(code))
          seen.push({ code, message: String((error as { message?: unknown }).message), sql: String(args[0]).slice(0, 200) });
        throw error;
      }
    };
  }
  return seen;
}
