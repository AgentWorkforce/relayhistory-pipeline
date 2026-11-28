/**
 * 20261105062380 the session binding on a REAL PostgreSQL server (PGlite cannot run two sessions):
 * - two genuinely concurrent first projections of one key (two connections, both open, one blocked on the other's
 *   primary-key insert) issue exactly one binding;
 * - the projector lifecycle (mint, keep across retries, tombstone retires, fenced re-send stays retired, a new revision
 *   mints a new id with a greater revision) as on PGlite;
 * - the revision sequence stays owner-only despite a pre-existing default sequence grant (no setval by another role);
 * - the reader under the real bulls96_read_api role: a read that starts after a tombstone commits returns nothing,
 *   while a transaction that read before keeps its snapshot (the read's linearization point is its own statement).
 *
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, a superuser URL of a DISPOSABLE server: pg-proof's brief-postgres
 * service container in CI, trust auth, no credential). It creates and drops its own database; skipped otherwise.
 */
import { afterAll, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { projectSessionDirectory } from "../session-directory-projector.ts";

setDefaultTimeout(300_000);
const URL_ENV = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const sqlFile = (p: string) => read(p).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const DB = `session_directory_binding_${process.pid}`;
const clients: pg.Client[] = [];
const maybe = URL_ENV ? test : test.skip;

function urlFor(database: string): string {
  const u = new URL(URL_ENV!);
  u.pathname = "/" + database;
  return u.toString();
}
async function connect(database = DB): Promise<pg.Client> {
  const c = new pg.Client({ connectionString: urlFor(database) });
  c.on("error", () => {});
  await c.connect();
  clients.push(c);
  return c;
}
const exec = (c: pg.Client) => ({ query: async <R>(sql: string, params?: unknown[]) => (await c.query(sql, params)) as unknown as { rows: R[] } });

beforeAll(async () => {
  if (!URL_ENV) return;
  const admin = await connect("postgres");
  await admin.query(`DROP DATABASE IF EXISTS ${DB}`);
  await admin.query(`CREATE DATABASE ${DB}`);
  const c = await connect();
  await c.query(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await c.query(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) {
    // A synthetic pre-existing default sequence grant: 062380 must still leave its sequence owner-only.
    if (f === "20261105062380_session_directory_binding.sql")
      await c.query("ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT ALL ON SEQUENCES TO service_role, PUBLIC");
    await c.query(read("supabase/migrations/" + f));
  }
  await c.query(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
});
afterAll(async () => {
  if (!URL_ENV) return;
  for (const c of clients.splice(0).reverse()) await c.end().catch(() => {});
  const admin = new pg.Client({ connectionString: urlFor("postgres") });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`).catch(() => {});
  await admin.end();
});

const upsertPointer = (c: pg.Client, session: string, machine: string, revision: number, deleted = false) =>
  c.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        title, state, event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id, revision, indexed_at, deleted_at)
     VALUES ('o', 'w', 'codex', $1, $2, 'u', '2026-10-01T10:00:00Z', '2026-10-01T12:00:00Z'::timestamptz + make_interval(mins => $3),
             't', 'active', 1, 0, 's3', 'bucket', 'key/' || $2, '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', $3, now(),
             CASE WHEN $4::boolean THEN now() END)
     ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
       last_activity_at = excluded.last_activity_at, revision = excluded.revision, indexed_at = now(), deleted_at = excluded.deleted_at`,
    [session, machine, revision, deleted],
  );
const binding = async (c: pg.Client, session: string) =>
  (await c.query<{ id: string | null; revision: string | null; deleted: boolean }>(
    `SELECT session_binding_id::text AS id, session_binding_revision::text AS revision, deleted_at IS NOT NULL AS deleted
       FROM sessions.session_directory WHERE org_id = 'o' AND workspace_id = 'w' AND harness = 'codex' AND session_id = $1`, [session])).rows[0];
const readerSql = "SELECT session_binding_id::text AS id FROM sessions.session_directory_bindings_for('w', ARRAY['codex'], ARRAY[$1::text])";
async function beginReader(c: pg.Client, isolation = "READ COMMITTED") {
  await c.query(`BEGIN ISOLATION LEVEL ${isolation}`);
  await c.query("SET LOCAL ROLE bulls96_read_api");
  await c.query("SELECT set_config('bulls96_read.org_id','o',true), set_config('bulls96_read.workspace_id','w',true)");
}

maybe("two concurrent first projections of one key issue exactly one binding", async () => {
  const a = await connect();
  const b = await connect();
  const insert = (c: pg.Client) =>
    c.query<{ id: string; revision: string }>(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
       VALUES ('o','w','codex','race', now(), now())
       ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET last_activity_at = excluded.last_activity_at
       RETURNING session_binding_id::text AS id, session_binding_revision::text AS revision`);
  await a.query("BEGIN");
  await b.query("BEGIN");
  const first = (await insert(a)).rows[0]!;
  // b's insert blocks on a's uncommitted primary-key entry; once a commits it takes the ON CONFLICT path.
  const pending = insert(b);
  const blocked = await Promise.race([pending.then(() => false), new Promise<boolean>((r) => setTimeout(() => r(true), 300))]);
  expect(blocked).toBe(true);
  await a.query("COMMIT");
  const second = (await pending).rows[0]!;
  await b.query("COMMIT");
  expect(second).toEqual(first);
  expect(await binding(a, "race")).toEqual({ ...first, deleted: false });
});

maybe("the projector lifecycle: mint, keep, retire, fenced re-send stays retired, a new revision mints a greater one", async () => {
  const c = await connect();
  const key = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "life" };
  await upsertPointer(c, "life", "m1", 3);
  await projectSessionDirectory(exec(c), key);
  const live = (await binding(c, "life"))!;
  await projectSessionDirectory(exec(c), key);
  expect(await binding(c, "life")).toEqual(live);
  await upsertPointer(c, "life", "m1", 3, true);
  await projectSessionDirectory(exec(c), key);
  expect(await binding(c, "life")).toEqual({ ...live, deleted: true });
  await upsertPointer(c, "life", "m1", 3);
  await projectSessionDirectory(exec(c), key);
  expect(await binding(c, "life")).toEqual({ ...live, deleted: true });
  await upsertPointer(c, "life", "m1", 4);
  await projectSessionDirectory(exec(c), key);
  const revived = (await binding(c, "life"))!;
  expect(revived.deleted).toBe(false);
  expect(revived.id).not.toBe(live.id);
  expect(BigInt(revived.revision!)).toBeGreaterThan(BigInt(live.revision!));
});

maybe("the reader under bulls96_read_api: a read that starts after a tombstone commits refuses; a prior snapshot is its own", async () => {
  const writer = await connect();
  const key = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "lin" };
  await upsertPointer(writer, "lin", "m1", 1);
  await projectSessionDirectory(exec(writer), key);
  const live = (await binding(writer, "lin"))!;
  const early = await connect();
  await beginReader(early, "REPEATABLE READ");
  expect((await early.query(readerSql, ["lin"])).rows).toEqual([{ id: live.id }]);
  // The tombstone commits while `early` is open.
  await upsertPointer(writer, "lin", "m1", 1, true);
  await projectSessionDirectory(exec(writer), key);
  const late = await connect();
  await beginReader(late);
  expect((await late.query(readerSql, ["lin"])).rows).toEqual([]);
  await late.query("COMMIT");
  // The overlapping read keeps the snapshot it took before the erase committed.
  expect((await early.query(readerSql, ["lin"])).rows).toEqual([{ id: live.id }]);
  await early.query("COMMIT");
  // Another role is refused before any value is read.
  await writer.query("BEGIN");
  await writer.query("SET LOCAL ROLE service_role");
  await expect(writer.query(readerSql, ["lin"])).rejects.toMatchObject({ code: "42501" });
  await writer.query("ROLLBACK");
});

maybe("the revision sequence is owner-only despite a default sequence grant present before the migration", async () => {
  const c = await connect();
  const grantees = (await c.query<{ g: string }>(
    `SELECT coalesce(r.rolname, 'PUBLIC') AS g FROM pg_class k
      CROSS JOIN LATERAL aclexplode(coalesce(k.relacl, acldefault('S', k.relowner))) a
      LEFT JOIN pg_roles r ON r.oid = a.grantee
     WHERE k.oid = 'sessions.session_binding_revision'::regclass AND a.grantee <> k.relowner`)).rows;
  expect(grantees).toEqual([]);
  await c.query("BEGIN");
  await c.query("SET LOCAL ROLE service_role");
  await expect(c.query("SELECT setval('sessions.session_binding_revision', 1)")).rejects.toMatchObject({ code: "42501" });
  await c.query("ROLLBACK");
});
