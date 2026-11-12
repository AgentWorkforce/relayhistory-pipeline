/**
 * #566 (20261105057030): the self-host per-session erase (sessions.ingest_selfhost_set_session_uploads) removes the
 * erased copy's directory membership, as the projector (src/hoopdb/session-directory-projector.ts) keeps it. PGlite with
 * every Supabase migration and the ingest role, the same setup as selfhost-server.pglite.test.ts; the erase runs as
 * bulls96_session_ingest (EXECUTE only), the membership is written by the real projector and checked with the real
 * native catalog activation audit (orphanMachines).
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { nativeAgentCatalogActivation } from "../../hoopdb/native-agent-catalog-activation.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { TENANT } from "./support.ts";
import { liveMachineId } from "../writer.ts";

setDefaultTimeout(120_000);

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
function sql(path: string): string {
  return read(path).split("\n").map((line) => (line.startsWith("\\ir ") ? sql(join(dirname(path), line.slice(4).trim())) : line.startsWith("\\") ? "" : line)).join("\n");
}

const ORIGIN_ID = "origin-1";
const machine = liveMachineId(TENANT.workspaceId, ORIGIN_ID, TENANT.userId);
const otherMachine = liveMachineId(TENANT.workspaceId, "origin-2", TENANT.userId);
const generation = "a".repeat(64);
const KEY = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, harness: "claude", sessionId: "sess-1" };

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

async function database() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  cleanups.push(() => db.close());
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sql("supabase/bootstrap/session_ingest_role.sql"));
  // The audit's schema-validation precondition (native-agent-catalog-activation.test.ts does the same).
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text)");
  await db.query("INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker',$1,'pass')", [generation]);
  return db;
}

const pointer = (db: PGlite, machineId: string, user = TENANT.userId, canonical: { session?: string; machine?: string } = {}) =>
  db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision,
        canonical_session_id, canonical_machine_id)
     VALUES ($1, $2, 'claude', 'sess-1', $3, $4, now(), now(), 's3', 'bucket', 'k/sess-1' || $3,
             decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2, $5, $6)`,
    [TENANT.orgId, TENANT.workspaceId, machineId, user, canonical.session ?? null, canonical.machine ?? null],
  );

/** The ingest login's erase of sess-1 for origin-1 (ledger-sql.ts setSessionUploads, selfhost front door). */
async function erase(db: PGlite) {
  await db.exec("SET ROLE bulls96_session_ingest");
  try {
    const r = await db.query<{ r: { updated: number } }>("SELECT sessions.ingest_selfhost_set_session_uploads($1::jsonb) AS r", [
      JSON.stringify({ tenant: TENANT, originId: ORIGIN_ID, sessions: [["claude", "sess-1"]], enabled: false }),
    ]);
    expect(typeof r.rows[0]!.r.updated).toBe("number");
  } finally {
    await db.exec("RESET ROLE");
  }
}

async function membership(db: PGlite) {
  const dir = await db.query<{ session_id: string; deleted: boolean; machine_ids: string[]; user_ids: string[] }>(
    "SELECT session_id, deleted_at IS NOT NULL AS deleted, machine_ids, user_ids FROM sessions.session_directory ORDER BY session_id");
  const machines = await db.query<{ session_id: string; machine_id: string }>(
    "SELECT session_id, machine_id FROM sessions.session_directory_machines ORDER BY session_id, machine_id");
  const users = await db.query<{ session_id: string; user_id: string }>(
    "SELECT session_id, user_id FROM sessions.session_directory_users ORDER BY session_id, user_id");
  return { directory: dir.rows, machines: machines.rows, users: users.rows };
}

async function orphanMachines(db: PGlite): Promise<number> {
  const transact = async <T>(work: (tx: SqlExecutor) => Promise<T>) => {
    await db.exec("BEGIN");
    try { const result = await work(db as never); await db.exec("COMMIT"); return result; }
    catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  return (await nativeAgentCatalogActivation(transact, { mode: "selfhosted", generation })).counts.orphanMachines!;
}

const erased = async (db: PGlite, machineId: string) =>
  (await db.query<{ deleted: boolean }>("SELECT deleted_at IS NOT NULL AS deleted FROM sessions.session_index WHERE session_id = 'sess-1' AND machine_id = $1", [machineId])).rows[0]!.deleted;

test("(a) one machine: pointer and directory tombstoned, no membership rows left, orphanMachines 0", async () => {
  const db = await database();
  await pointer(db, machine);
  await projectSessionDirectory(db as never, KEY);
  expect(await membership(db)).toEqual({
    directory: [{ session_id: "sess-1", deleted: false, machine_ids: [machine], user_ids: [TENANT.userId] }],
    machines: [{ session_id: "sess-1", machine_id: machine }],
    users: [{ session_id: "sess-1", user_id: TENANT.userId }],
  });
  await erase(db);
  expect(await erased(db, machine)).toBe(true);
  expect(await membership(db)).toEqual({
    directory: [{ session_id: "sess-1", deleted: true, machine_ids: [], user_ids: [] }],
    machines: [],
    users: [],
  });
  expect(await orphanMachines(db)).toBe(0);
  // The projector re-run on the same event changes nothing: the erase left exactly its tombstone state.
  await projectSessionDirectory(db as never, KEY);
  expect((await membership(db)).machines).toEqual([]);
  expect((await membership(db)).users).toEqual([]);
});

test("(b) two srv_ machines, one erased: directory stays live, only the erased machine's row goes, orphanMachines 0", async () => {
  const db = await database();
  await pointer(db, machine);
  await pointer(db, otherMachine);
  await projectSessionDirectory(db as never, KEY);
  expect((await membership(db)).machines.map((m) => m.machine_id).sort()).toEqual([machine, otherMachine].sort());
  await erase(db);
  expect(await erased(db, machine)).toBe(true);
  expect(await erased(db, otherMachine)).toBe(false);
  const after = await membership(db);
  expect(after).toEqual({
    directory: [{ session_id: "sess-1", deleted: false, machine_ids: [otherMachine], user_ids: [TENANT.userId] }],
    machines: [{ session_id: "sess-1", machine_id: otherMachine }],
    // The caller still has a live pointer on the other machine, so their user row stays.
    users: [{ session_id: "sess-1", user_id: TENANT.userId }],
  });
  expect(await orphanMachines(db)).toBe(0);
  // Identical to what the projector produces for the same pointer state.
  await projectSessionDirectory(db as never, KEY);
  expect(await membership(db)).toEqual(after);
});

test("the other live machine is another user's: the caller's user row goes, theirs stays", async () => {
  const db = await database();
  const theirs = liveMachineId(TENANT.workspaceId, "origin-3", "user-2");
  await pointer(db, machine);
  await pointer(db, theirs, "user-2");
  await projectSessionDirectory(db as never, KEY);
  await erase(db);
  const after = await membership(db);
  expect(after).toEqual({
    directory: [{ session_id: "sess-1", deleted: false, machine_ids: [theirs], user_ids: ["user-2"] }],
    machines: [{ session_id: "sess-1", machine_id: theirs }],
    users: [{ session_id: "sess-1", user_id: "user-2" }],
  });
  expect(await orphanMachines(db)).toBe(0);
  await projectSessionDirectory(db as never, KEY);
  expect(await membership(db)).toEqual(after);
});

test("canonical ids: the row keyed by the canonical session loses the erased pointer's canonical machine", async () => {
  const db = await database();
  await pointer(db, machine, TENANT.userId, { session: "Sess:1", machine: "src-machine-1" });
  await pointer(db, otherMachine, TENANT.userId, { session: "Sess:1", machine: "src-machine-2" });
  await projectSessionDirectory(db as never, KEY);
  expect((await membership(db)).directory.map((d) => d.session_id)).toEqual(["Sess:1"]);
  await erase(db);
  const after = await membership(db);
  expect(after).toEqual({
    directory: [{ session_id: "Sess:1", deleted: false, machine_ids: ["src-machine-2"], user_ids: [TENANT.userId] }],
    machines: [{ session_id: "Sess:1", machine_id: "src-machine-2" }],
    users: [{ session_id: "Sess:1", user_id: TENANT.userId }],
  });
  expect(await orphanMachines(db)).toBe(0);
  await projectSessionDirectory(db as never, KEY);
  expect(await membership(db)).toEqual(after);
});
