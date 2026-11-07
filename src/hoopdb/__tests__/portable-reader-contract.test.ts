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

/** The storage verifier as phase 2 runs it: local owner-run (PGlite's postgres owns
 * everything). `searchPath` makes sessions visible to prove rendering independence. */
const storage = (db: PGlite, searchPath?: string) => db.transaction(async (tx) => {
  if (searchPath) await tx.query("SELECT set_config('search_path', $1, true)", [searchPath]);
  await tx.query("SELECT set_config('hoopdb.runtime_role', 'postgres', true), set_config('hoopdb.db_mode', 'local', true)");
  await tx.exec(STORAGE_VERIFY);
});
const reader = (db: PGlite, searchPath?: string) => db.transaction(async (tx) => {
  if (searchPath) await tx.query("SELECT set_config('search_path', $1, true)", [searchPath]);
  await tx.exec(READ_VERIFY);
});

test("the full migration set passes both verifiers with the exact 048000 reader contract, with sessions on or off search_path", async () => {
  const db = await migrated();
  await reader(db);
  await storage(db);
  // sessions visible: regclass would render unqualified; both verifiers must still pass.
  await reader(db, "sessions, public");
  await storage(db, "sessions, public");
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
  ["a required directory contract column dropped (absent attribute, not a revoked grant)",
    "ALTER TABLE sessions.session_directory DROP COLUMN event_projection_ready CASCADE",
    /sessions\.session_directory required column event_projection_ready is missing/,
    /directory required column event_projection_ready is missing/],
  ["readiness RPC granted to PUBLIC",
    "GRANT EXECUTE ON FUNCTION sessions.portable_reader_readiness(text[], text) TO PUBLIC",
    /readiness function EXECUTE grants are not exactly the owner and bulls96_read_api/,
    /readiness function EXECUTE grants are not exactly the owner and bulls96_read_api/],
  ["readiness RPC granted to another role",
    "GRANT EXECUTE ON FUNCTION sessions.portable_reader_readiness(text[], text) TO authenticated",
    /readiness function EXECUTE grants are not exactly the owner and bulls96_read_api/,
    /readiness function EXECUTE grants are not exactly the owner and bulls96_read_api/],
  ["readiness RPC made SECURITY INVOKER",
    "ALTER FUNCTION sessions.portable_reader_readiness(text[], text) SECURITY INVOKER",
    /readiness function is not owner-owned VOLATILE SECURITY DEFINER with the pinned search_path/,
    /readiness function is not owner-owned VOLATILE SECURITY DEFINER with the pinned search_path/],
  ["readiness RPC search_path changed",
    "ALTER FUNCTION sessions.portable_reader_readiness(text[], text) SET search_path = public, pg_catalog",
    /readiness function is not owner-owned VOLATILE SECURITY DEFINER with the pinned search_path/,
    /readiness function is not owner-owned VOLATILE SECURITY DEFINER with the pinned search_path/],
  ["readiness RPC body replaced with an always-pass (same signature, flags, ACL)",
    "CREATE OR REPLACE FUNCTION sessions.portable_reader_readiness(p_names text[], p_generation text) RETURNS text LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $b$ BEGIN RETURN 'pass'; END $b$",
    /readiness function body differs from the reviewed definition/,
    /readiness function body differs from the reviewed definition/],
  ["readiness RPC dropped while 048000 stays",
    "DROP FUNCTION sessions.portable_reader_readiness(text[], text)",
    /readiness function sessions\.portable_reader_readiness\(text\[\],text\) is missing/,
    /readiness function sessions\.portable_reader_readiness\(text\[\],text\) is missing/],
];

for (const [name, drift, readerMessage, storageMessage] of cases)
  test(`both verifiers refuse: ${name}`, async () => {
    const db = await migrated();
    await db.exec(drift);
    await expect(reader(db)).rejects.toThrow(readerMessage);
    await expect(storage(db)).rejects.toThrow(storageMessage);
  }, 120_000);

test("readiness RPC present without the rest of 048000 is refused by both verifiers (all-or-nothing)", async () => {
  const db = await migrated();
  // Remove everything 048000 creates EXCEPT the function.
  await db.exec(`DROP POLICY session_directory_portable_read_api ON sessions.session_directory;
    DROP POLICY search_projection_state_read_api ON sessions.search_projection_state;
    REVOKE SELECT (org_id, workspace_id, harness, session_id, event_projection_epoch, event_projection_ready) ON sessions.session_directory FROM bulls96_read_api;
    REVOKE SELECT (backend_id, org_id, workspace_id, source, session_id, canonical_session_id, source_epoch, coverage_from_ms, complete) ON sessions.search_projection_state FROM bulls96_read_api;`);
  await expect(reader(db)).rejects.toThrow(/readiness function exists without the 048000 reader contract/);
  await expect(storage(db)).rejects.toThrow(/readiness function exists without the 048000 reader contract/);
}, 120_000);

test("the readiness RPC refuses any caller that is not the fixed reader on its own login (42501), including SET ROLE", async () => {
  const db = await migrated();
  const call = () => db.query("SELECT sessions.portable_reader_readiness(ARRAY['upstream:x.sql'], repeat('a', 64)) AS v");
  await expect(call()).rejects.toMatchObject({ code: "42501" }); // postgres (owner/superuser)
  await db.exec("SET ROLE bulls96_read_api");
  try { await expect(call()).rejects.toMatchObject({ code: "42501" }); } // session_user is still postgres
  finally { await db.exec("RESET ROLE"); }
}, 120_000);

test("an identical RPC grant set in a different physical ACL order passes both verifiers", async () => {
  const db = await migrated();
  // REVOKE then re-GRANT the owner's EXECUTE: the same set, but the reader entry is now first.
  await db.exec(`REVOKE EXECUTE ON FUNCTION sessions.portable_reader_readiness(text[], text) FROM postgres;
    GRANT EXECUTE ON FUNCTION sessions.portable_reader_readiness(text[], text) TO postgres;`);
  const order = (await db.query<{ k: string }>(`SELECT x.grantee::regrole::text AS k
      FROM pg_proc p, aclexplode(p.proacl) x WHERE p.oid = 'sessions.portable_reader_readiness(text[],text)'::regprocedure`)).rows.map(r => r.k);
  expect(order[0]).toBe("bulls96_read_api"); // physically reordered
  await reader(db);
  await storage(db);
}, 120_000);

test("048000 fails closed (55000) when the owner's function default privileges would leak EXECUTE on the readiness RPC", async () => {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  for (const f of ["supabase/tests/supabase_shim.sql", "supabase/bootstrap/session_directory_search_worker.sql",
    "supabase/bootstrap/session_token_verifier.sql", "supabase/bootstrap/session_ingest_roles.sql",
    "supabase/bootstrap/session_read_api_roles.sql"]) await db.exec(read(f));
  await db.exec("CREATE SCHEMA IF NOT EXISTS sessions; CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL)");
  const files = readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort();
  for (const file of files.filter(f => f < "20261105048000")) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec("ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT EXECUTE ON FUNCTIONS TO session_token_verifier");
  await expect(db.exec(read("supabase/migrations/20261105048000_portable_search_reader_contract.sql"))).rejects.toMatchObject({ code: "55000" });
}, 120_000);
