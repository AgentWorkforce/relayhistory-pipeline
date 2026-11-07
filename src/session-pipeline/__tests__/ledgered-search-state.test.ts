/** The ledgered self-host state (migration 20261105047000) under real role
 * separation: the real migration and verifier files run as a non-superuser
 * migration owner, and the writer is a NOSUPERUSER NOBYPASSRLS non-owner role
 * holding only what the migrator's installRuntimeApplicationAcl grants. PGlite is
 * in-process PostgreSQL; the PG17 server proof is separate. */
import { afterEach, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { ledgeredSearchState, postgresSearchState, reservedSearchSchema } from "../postgres-search-state.ts";
import { admitPortableConnection } from "../../shared/portable-connection-identity.ts";
import { createLedgeredSearchComposition } from "../../shared/search-composition.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { RUNTIME_APPLICATION_POLICY, RUNTIME_APPLICATION_TABLE_PRIVILEGES } from "../../hoopdb/migrate-guards.ts";

const root = (p: string) => fileURLToPath(new URL(`../../../${p}`, import.meta.url));
const MIGRATION = readFileSync(root("supabase/migrations/20261105047000_portable_search_storage.sql"), "utf8");
const READER = readFileSync(root("supabase/migrations/20261105048000_portable_search_reader_contract.sql"), "utf8");
const READER_DOWN = readFileSync(root("supabase/rollbacks/20261105048000_portable_search_reader_contract.down.sql"), "utf8");
// psql meta-commands are not SQL.
const VERIFY = readFileSync(root("supabase/bootstrap/portable_search_storage_verify.sql"), "utf8").replace(/^\\.*$/gm, "");

const open: PGlite[] = [];
afterEach(async () => { while (open.length) await open.pop()!.close(); });

/** The verifier as the migrator runs it: transaction-local expected runtime role and mode. */
function verify(db: PGlite, role: string | null = "rt", mode = "selfhosted", searchPath?: string) {
  return db.transaction(async (tx) => {
    if (searchPath) await tx.query("SELECT set_config('search_path', $1, true)", [searchPath]);
    if (role !== null) await tx.query("SELECT set_config('hoopdb.runtime_role', $1, true)", [role]);
    await tx.query("SELECT set_config('hoopdb.db_mode', $1, true)", [mode]);
    await tx.exec(VERIFY);
  });
}

async function as<T>(db: PGlite, role: string, work: () => Promise<T>): Promise<T> {
  await db.exec(`SET ROLE ${role}`);
  try { return await work(); } finally { await db.exec("RESET ROLE"); }
}

async function fixture(before?: (db: PGlite) => Promise<void>) {
  const db = new PGlite();
  open.push(db);
  await db.exec(`
    CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN; CREATE ROLE service_role NOLOGIN;
    CREATE ROLE bulls96_read_api NOLOGIN NOINHERIT;
    CREATE ROLE mig_owner NOLOGIN NOSUPERUSER NOBYPASSRLS;
    CREATE ROLE rt NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
    CREATE ROLE bypass NOLOGIN NOSUPERUSER BYPASSRLS;
    CREATE ROLE reporting NOLOGIN;
    GRANT CREATE ON DATABASE postgres TO mig_owner;`);
  await as(db, "mig_owner", () => db.exec(`
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL);
    CREATE TABLE sessions.session_index(org_id text, workspace_id text, harness text, session_id text, canonical_session_id text, deleted_at timestamptz);
    CREATE TABLE sessions.session_directory(org_id text, workspace_id text, harness text, session_id text, event_projection_epoch bigint, event_projection_ready boolean);
    GRANT USAGE ON SCHEMA sessions TO rt, bypass, bulls96_read_api;
    GRANT SELECT, UPDATE ON sessions.session_directory TO rt;`));
  if (before) await before(db);
  await as(db, "mig_owner", () => db.exec(MIGRATION));
  // Exactly what src/hoopdb/migrate.ts installRuntimeApplicationAcl does for this table.
  const privileges = RUNTIME_APPLICATION_TABLE_PRIVILEGES["sessions.search_projection_state"].join(", ");
  await as(db, "mig_owner", () => db.exec(`
    GRANT ${privileges} ON TABLE sessions.search_projection_state TO rt;
    CREATE POLICY ${RUNTIME_APPLICATION_POLICY} ON sessions.search_projection_state AS PERMISSIVE FOR ALL TO rt USING (true) WITH CHECK (true);`));
  return db;
}

const scope = { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s1" };
const value = (complete: boolean) => ({ revision: 1, rows: 1, complete, at: new Date(0), canonicalSessionId: "s1" });

test("the real migration and verifier: non-owner NOBYPASSRLS writer records; owner sees nothing (FORCE RLS)", async () => {
  const db = await fixture();
  await verify(db); // PASS, sessions not on search_path
  await verify(db, "rt", "selfhosted", "sessions, public"); // PASS, sessions visible
  await as(db, "mig_owner", () => db.exec("INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s1',7,true)"));
  const state = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
  await as(db, "rt", async () => {
    await admitPortableConnection(db, "writer");
    await db.transaction(tx => state.record(tx, scope, value(true)));
  });
  const stored = await as(db, "rt", () => db.query<{ source_epoch: string; complete: boolean }>("SELECT source_epoch, complete FROM sessions.search_projection_state"));
  expect(stored.rows.map(r => [Number(r.source_epoch), r.complete])).toEqual([[7, true]]);
  // FORCE RLS: the owner has no policy, so it reads and writes nothing.
  expect((await as(db, "mig_owner", () => db.query("SELECT 1 FROM sessions.search_projection_state"))).rows).toEqual([]);
  // No reader surface in 047000: the read role is refused, so completeness fails closed.
  await expect(as(db, "bulls96_read_api", () => db.query("SELECT 1 FROM sessions.search_projection_state"))).rejects.toThrow(/permission denied/);
}, 60_000);

test("admission refuses superuser, BYPASSRLS, storage owner and a write-capable reader", async () => {
  const db = await fixture();
  await expect(admitPortableConnection(db, "writer")).rejects.toThrow("superuser"); // PGlite's postgres
  await expect(as(db, "bypass", () => admitPortableConnection(db, "writer"))).rejects.toThrow("bypassrls");
  await expect(as(db, "mig_owner", () => admitPortableConnection(db, "writer"))).rejects.toThrow("storage_owner");
  await expect(as(db, "rt", () => admitPortableConnection(db, "reader"))).rejects.toThrow("reader_can_write");
  await as(db, "bulls96_read_api", () => admitPortableConnection(db, "reader")); // no write, not privileged
}, 60_000);

test("verifier refuses drift: PUBLIC/extra grant, grant option, reader surface, trigger, FORCE off, extra policy, column change", async () => {
  const drifts: [string, RegExp][] = [
    ["GRANT SELECT ON sessions.search_projection_state TO PUBLIC", /PUBLIC/],
    ["GRANT SELECT ON sessions.search_projection_state TO reporting", /reporting/],
    ["GRANT SELECT (org_id) ON sessions.search_projection_state TO bulls96_read_api", /column ACL/],
    ["CREATE POLICY extra ON sessions.search_projection_state FOR SELECT TO bulls96_read_api USING (true)", /unapproved policy extra/],
    ["ALTER TABLE sessions.search_projection_state NO FORCE ROW LEVEL SECURITY", /not FORCEd/],
    ["ALTER TABLE sessions.search_projection_state ADD COLUMN note text", /column set differs/],
    ["CREATE FUNCTION sessions.t() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$; CREATE TRIGGER t BEFORE INSERT ON sessions.search_projection_state FOR EACH ROW EXECUTE FUNCTION sessions.t()", /trigger/],
    ["CREATE INDEX extra_idx ON sessions.search_projection_state(updated_at)", /index set differs/],
  ];
  for (const [drift, message] of drifts) {
    const db = await fixture();
    await as(db, "mig_owner", () => db.exec(drift));
    await expect(verify(db)).rejects.toThrow(message);
    await db.close(); open.pop();
  }
  const db = await fixture();
  await as(db, "mig_owner", () => db.exec("GRANT SELECT ON sessions.search_projection_state TO rt WITH GRANT OPTION"));
  await expect(verify(db)).rejects.toThrow(/GRANT OPTION/);
}, 240_000);

test("verifier binds identity to configuration: unbound, another role, or a moved policy+grant are refused", async () => {
  const db = await fixture();
  await expect(verify(db, null)).rejects.toThrow(/not bound/);
  await expect(verify(db, "no_such_role")).rejects.toThrow(/not bound/);
  // The policy and grant still name rt, so binding another ordinary role fails.
  await db.exec("CREATE ROLE other NOLOGIN NOSUPERUSER NOBYPASSRLS");
  await expect(verify(db, "other")).rejects.toThrow(/missing|granted to rt/);
  // Move BOTH the policy and the grant from approved rt to other: still refused for rt.
  await as(db, "mig_owner", () => db.exec(`
    REVOKE ALL ON sessions.search_projection_state FROM rt;
    DROP POLICY bulls96_trusted_backend ON sessions.search_projection_state;
    GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.search_projection_state TO other;
    CREATE POLICY bulls96_trusted_backend ON sessions.search_projection_state AS PERMISSIVE FOR ALL TO other USING (true) WITH CHECK (true);`));
  await expect(verify(db, "rt")).rejects.toThrow(/unapproved policy bulls96_trusted_backend/);
  // The owner as runtime is accepted only in local owner-run mode, with no runtime ACL.
  await expect(verify(db, "mig_owner", "selfhosted")).rejects.toThrow(/owner outside local mode/);
}, 120_000);

test("verifier pins constraint definitions, validation and index properties", async () => {
  const drifts: [string, RegExp][] = [
    ["ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_counts, ADD CONSTRAINT search_projection_state_counts CHECK (revision >= 0)", /search_projection_state_counts definition differs/],
    ["ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_ids, ADD CONSTRAINT search_projection_state_ids CHECK (backend_id ~ '^[a-zA-Z0-9._-]{1,128}$' AND org_id <> '' AND workspace_id <> '') NOT VALID", /validation or deferrability differs/],
    ["ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_pkey, ADD CONSTRAINT search_projection_state_pkey PRIMARY KEY (backend_id, org_id, workspace_id, source, session_id) DEFERRABLE", /deferrability|index search_projection_state_pkey/],
    ["ALTER TABLE sessions.search_projection_state DROP CONSTRAINT search_projection_state_pkey, ADD CONSTRAINT search_projection_state_pkey PRIMARY KEY (backend_id, org_id)", /pkey definition differs/],
  ];
  for (const [drift, message] of drifts) {
    const db = await fixture();
    await as(db, "mig_owner", () => db.exec(drift));
    await expect(verify(db)).rejects.toThrow(message);
    await db.close(); open.pop();
  }
}, 240_000);

test("local owner-run profile: owner as runtime with no runtime ACL passes only in local mode", async () => {
  const db = new PGlite();
  open.push(db);
  await db.exec(`CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN; CREATE ROLE service_role NOLOGIN;
    CREATE ROLE mig_owner NOLOGIN NOSUPERUSER NOBYPASSRLS; GRANT CREATE ON DATABASE postgres TO mig_owner;`);
  await as(db, "mig_owner", () => db.exec(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL);
    CREATE TABLE sessions.session_directory(org_id text);`));
  await as(db, "mig_owner", () => db.exec(MIGRATION));
  await verify(db, "mig_owner", "local");
  await expect(verify(db, "mig_owner", "rds")).rejects.toThrow(/owner outside local mode/);
}, 60_000);

test("the migration fails closed when the owner's default privileges would leak a grant", async () => {
  await expect(fixture(db => as(db, "mig_owner", () => db.exec("ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT SELECT ON TABLES TO reporting")))).rejects.toThrow(/inherited default privileges/);
}, 60_000);

test("ledgered composition refuses env-selected schemas and the Postgres content backend; v1 refuses reserved schemas", () => {
  const composition = createLedgeredSearchComposition({ backendId: "opensearch-v1", fromMs: 0, registry: createSearchIndexRegistry() });
  const input = { env: {}, backend: "opensearch" as never, role: "writer" as const };
  expect(() => composition.state({ ...input, env: { SEARCH_INDEX_POSTGRES_SCHEMA: "search_index" } })).toThrow("SEARCH_INDEX_POSTGRES_SCHEMA");
  expect(() => composition.state({ ...input, backend: "postgres" })).toThrow("no Postgres content index");
  expect(typeof composition.admit).toBe("function");
  for (const schema of ["public", "sessions", "pg_catalog", "information_schema", "auth", "storage", "rds_tools"]) {
    expect(reservedSearchSchema(schema)).toBe(true);
    expect(() => postgresSearchState({ backendId: "b", fromMs: 0, schema })).toThrow(/reserved|invalid/);
  }
  expect(reservedSearchSchema("search_index")).toBe(false);
});

/** 047000 plus the 048000 reader contract, ledgered, with the session_index read grant
 * and tenant policy that supabase/bootstrap/session_read_api_role.sql installs. */
async function readerFixture() {
  const db = await fixture();
  await as(db, "mig_owner", () => db.exec(`
    CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY, status text, updated_at timestamptz);
    INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','pass',now());
    INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20261105047000_portable_search_storage.sql','x');
    ALTER TABLE sessions.session_index ENABLE ROW LEVEL SECURITY;
    ALTER TABLE sessions.session_directory ENABLE ROW LEVEL SECURITY;
    GRANT SELECT (org_id, workspace_id, harness, session_id, canonical_session_id, deleted_at) ON sessions.session_index TO bulls96_read_api;
    CREATE POLICY session_index_read_api ON sessions.session_index AS PERMISSIVE FOR SELECT TO bulls96_read_api
      USING (org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true));
    CREATE POLICY owner_all ON sessions.session_directory TO rt USING (true) WITH CHECK (true);`));
  await as(db, "mig_owner", () => db.exec(READER));
  await as(db, "mig_owner", () => db.exec("INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20261105048000_portable_search_reader_contract.sql','x')"));
  return db;
}

const tenantReader = (db: PGlite) => (org: string, workspace: string, work: (tx: never) => Promise<unknown>) =>
  db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE bulls96_read_api");
    await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [org, workspace]);
    return work(tx as never);
  }) as never;

test("048000 reader contract: tenant-bound completeness through the real read role; cross-tenant rows invisible", async () => {
  const db = await readerFixture();
  await verify(db); // storage verifier accepts exactly the reader surface once 048000 is ledgered
  await db.exec(`INSERT INTO sessions.session_index VALUES('org-a','ws-a','codex','s1',NULL,NULL),('org-b','ws-a','codex','s1',NULL,NULL);
    INSERT INTO sessions.session_directory VALUES('org-a','ws-a','codex','s1',7,true),('org-b','ws-a','codex','s1',7,true);`);
  const writer = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0 });
  await as(db, "rt", () => db.transaction(tx => writer.record(tx, scope, value(true))));
  const reader = ledgeredSearchState({ backendId: "opensearch-v1", fromMs: 0, reader: tenantReader(db) });
  await as(db, "bulls96_read_api", () => admitPortableConnection(db, "reader"));
  expect(await reader.partials({ orgId: "org-a", workspaceId: "ws-a" }, { limit: 10 })).toEqual({ sessions: [], overflow: false });
  // org-b has a directory row but no state: incomplete, and org-a's state is invisible to it.
  expect(await reader.partials({ orgId: "org-b", workspaceId: "ws-a" }, { limit: 10 })).toEqual({ sessions: ["s1"], overflow: false });
  const seen = await tenantReader(db)("org-b", "ws-a", (tx: any) => tx.query("SELECT org_id FROM sessions.search_projection_state")) as { rows: unknown[] };
  expect(seen.rows).toEqual([]);
  const dir = await tenantReader(db)("org-b", "ws-a", (tx: any) => tx.query("SELECT org_id FROM sessions.session_directory")) as { rows: { org_id: string }[] };
  expect(dir.rows.map(r => r.org_id)).toEqual(["org-b"]);
  // Columns outside the contract stay refused.
  await expect(tenantReader(db)("org-a", "ws-a", (tx: any) => tx.query("SELECT revision FROM sessions.search_projection_state"))).rejects.toThrow(/permission denied/);
  // The read role cannot write.
  await expect(tenantReader(db)("org-a", "ws-a", (tx: any) => tx.query("DELETE FROM sessions.search_projection_state"))).rejects.toThrow(/permission denied/);
}, 120_000);

test("048000 storage verifier drift: extra or whole-table reader grant, loosened reader policy, reader surface without the ledger row", async () => {
  const drifts: [string, RegExp][] = [
    ["GRANT SELECT (revision) ON sessions.search_projection_state TO bulls96_read_api", /column ACL is present: SELECT on revision/],
    ["GRANT SELECT ON sessions.search_projection_state TO bulls96_read_api", /ACL: SELECT granted to bulls96_read_api/],
    ["DROP POLICY search_projection_state_read_api ON sessions.search_projection_state; CREATE POLICY search_projection_state_read_api ON sessions.search_projection_state FOR SELECT TO bulls96_read_api USING (true)", /unapproved policy search_projection_state_read_api/],
    // Partial surface: grants without the policies are refused (all-or-nothing).
    ["DROP POLICY search_projection_state_read_api ON sessions.search_projection_state; DROP POLICY session_directory_portable_read_api ON sessions.session_directory", /column ACL is present/],
    ["REVOKE SELECT (complete) ON sessions.search_projection_state FROM bulls96_read_api", /reader column SELECT is missing/],
  ];
  for (const [drift, message] of drifts) {
    const db = await readerFixture();
    await as(db, "mig_owner", () => db.exec(drift));
    await expect(verify(db)).rejects.toThrow(message);
    await db.close(); open.pop();
  }
}, 240_000);

test("048000 rollback removes exactly its surface and the reader fails closed again", async () => {
  const db = await readerFixture();
  await as(db, "mig_owner", () => db.exec(READER_DOWN));
  await verify(db); // back to the 047000-only shape
  await expect(tenantReader(db)("org-a", "ws-a", (tx: any) => tx.query("SELECT 1 FROM sessions.search_projection_state"))).rejects.toThrow(/permission denied/);
}, 120_000);

test("before 048000 the standalone storage verifier refuses any directory reader surface; 047-only without a reader stays passing", async () => {
  const clean = await fixture();
  await verify(clean); // 047-only, no reader surface: PASS
  const drifts: [string, RegExp][] = [
    ["GRANT SELECT (org_id) ON sessions.session_directory TO bulls96_read_api", /reader directory SELECT exists without the 048000 reader contract/],
    ["ALTER TABLE sessions.session_directory ENABLE ROW LEVEL SECURITY; CREATE POLICY reader_dir ON sessions.session_directory FOR SELECT TO bulls96_read_api USING (true)", /directory policy naming the reader exists without the 048000 reader contract/],
  ];
  for (const [drift, message] of drifts) {
    const db = await fixture();
    await as(db, "mig_owner", () => db.exec(drift));
    await expect(verify(db)).rejects.toThrow(message);
    await db.close(); open.pop();
  }
}, 120_000);
