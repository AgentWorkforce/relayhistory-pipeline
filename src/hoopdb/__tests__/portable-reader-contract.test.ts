/** 20261105048000 reader contract on the FULL migration set: both verifiers
 * (production session_read_api_role_verify.sql and the portable storage verifier) refuse
 * a directory policy widened TO PUBLIC USING (true) under the approved name, directory RLS
 * disabled, and an extra PUBLIC policy, each with its own attributable message. */
import { afterEach, expect, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const sql = (p: string) => read(p).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const READ_VERIFY = sql("supabase/bootstrap/session_read_api_role_verify.sql");
const STORAGE_VERIFY = sql("supabase/bootstrap/portable_search_storage_verify.sql");

const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((db) => db.close())); });

async function migrated(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  for (const f of ["supabase/tests/supabase_shim.sql", "supabase/bootstrap/session_directory_search_worker.sql",
    "supabase/bootstrap/session_token_verifier.sql", "supabase/bootstrap/session_ingest_roles.sql",
    "supabase/bootstrap/session_read_api_roles.sql"]) await db.exec(read(f));
  // The ledger table identifies the migration owner for the storage verifier.
  await db.exec("CREATE SCHEMA IF NOT EXISTS sessions; CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL)");
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sql("supabase/bootstrap/session_read_api_role.sql"));
  return db;
}

/** The storage verifier as phase 2 runs it: local owner-run (PGlite's postgres owns everything). */
const storage = (db: PGlite) => db.transaction(async (tx) => {
  await tx.query("SELECT set_config('hoopdb.runtime_role', 'postgres', true), set_config('hoopdb.db_mode', 'local', true)");
  await tx.exec(STORAGE_VERIFY);
});
const reader = (db: PGlite) => db.exec(READ_VERIFY);

test("the full migration set passes both verifiers with the exact 048000 reader contract", async () => {
  const db = await migrated();
  await reader(db);
  await storage(db);
}, 120_000);

const cases: [string, string, RegExp, RegExp][] = [
  ["directory policy widened TO PUBLIC USING (true) under the approved name",
    `DROP POLICY session_directory_portable_read_api ON sessions.session_directory;
     CREATE POLICY session_directory_portable_read_api ON sessions.session_directory AS PERMISSIVE FOR SELECT TO PUBLIC USING (true)`,
    /policy session_directory_portable_read_api on sessions\.session_directory is missing or not exactly/,
    /directory policy session_directory_portable_read_api is missing or not exact/],
  ["directory RLS disabled",
    "ALTER TABLE sessions.session_directory DISABLE ROW LEVEL SECURITY",
    /RLS is off on sessions\.session_directory/,
    /RLS is off on sessions\.session_directory/],
  ["an extra permissive PUBLIC policy on the directory",
    "CREATE POLICY extra_public ON sessions.session_directory AS PERMISSIVE FOR SELECT TO PUBLIC USING (true)",
    /unapproved policy extra_public on sessions\.session_directory/,
    /unapproved directory policy extra_public applies to PUBLIC or the reader/],
  ["an extra directory column granted to the reader",
    "GRANT SELECT (search_text) ON sessions.session_directory TO bulls96_read_api",
    /sessions\.session_directory column search_text: SELECT is granted/,
    /reader directory column SELECT differs from the six contract columns/],
  ["the directory policy removed while the state policy stays (partial)",
    "DROP POLICY session_directory_portable_read_api ON sessions.session_directory",
    /policy session_directory_portable_read_api on sessions\.session_directory is missing or not exactly/,
    /directory policy session_directory_portable_read_api is missing or not exact/],
  ["a directory contract column revoked (partial)",
    "REVOKE SELECT (event_projection_ready) ON sessions.session_directory FROM bulls96_read_api",
    /sessions\.session_directory column event_projection_ready: SELECT is missing/,
    /reader directory column SELECT differs from the six contract columns/],
  ["a directory contract column re-granted WITH GRANT OPTION",
    "GRANT SELECT (org_id) ON sessions.session_directory TO bulls96_read_api WITH GRANT OPTION",
    /reader column SELECT WITH GRANT OPTION on the portable relations/,
    /reader column SELECT WITH GRANT OPTION on the portable relations/],
];

for (const [name, drift, readerMessage, storageMessage] of cases)
  test(`both verifiers refuse: ${name}`, async () => {
    const db = await migrated();
    await db.exec(drift);
    await expect(reader(db)).rejects.toThrow(readerMessage);
    await expect(storage(db)).rejects.toThrow(storageMessage);
  }, 120_000);
