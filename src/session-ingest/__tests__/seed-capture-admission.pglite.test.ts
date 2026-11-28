/**
 * 20261105062390: seed capture lease issuance is admission-class under the per-org admission hold (062320). Single
 * connection on PGlite with every migration, calling the real RPCs as the seeder role. The two-connection orderings
 * (a capture in flight blocks an install, an install in flight makes a capture wait then refuse) are in
 * seed-capture-admission.postgres.test.ts.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { TENANT } from "./support.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64), head = "b".repeat(40);
const read = (p: string) => readFileSync(join(root, p), "utf8");
const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const c of cleanups.splice(0)) await c(); });
const OTHER = "org-free";
const args = (org = TENANT.orgId, mode = "lease") => ({ org_id: org, workspace_id: TENANT.workspaceId, source: "claude", session_id: "sess-1", generation: gen, mode });

async function fixture() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); cleanups.push(() => db.close());
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now());
    INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','${gen}','pass')
    ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status=excluded.status;
    CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text);
    INSERT INTO sessions.__96bulls_migrations(name) VALUES('upstream:20261105062320_ingest_org_admission_fences.sql') ON CONFLICT DO NOTHING;`);
  // The scope's source epoch row (written by 040000's source-change triggers in production): without it the capture
  // records epoch 0 and the import's locked read finds none, so no import could ever match.
  for (const org of [TENANT.orgId, OTHER])
    await db.query("INSERT INTO sessions.ingest_seed_source_epochs VALUES($1,$2,'claude','sess-1',1) ON CONFLICT DO NOTHING", [org, TENANT.workspaceId]);
  /** One RPC as the seeder, in its own transaction (optionally at another isolation level). */
  const rpc = async (name: string, p: unknown, iso?: string) => {
    await db.exec(`BEGIN${iso ? " ISOLATION LEVEL " + iso : ""};SET LOCAL ROLE bulls96_session_ingest_seeder`);
    try { const r = (await db.query<{ r: any }>(`SELECT sessions.${name}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0]!.r; await db.exec("COMMIT"); return r; }
    catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  const code = (p: Promise<unknown>) => p.then(() => "ok", (e: { code?: string }) => String(e.code));
  const captures = async (org = TENANT.orgId) => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1", [org])).rows[0]!.n;
  const install = async (org = TENANT.orgId) => (await db.query<{ e: string }>("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [org])).rows[0]!.e;
  const rollbackRelease = (org: string, e: string) => db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'verified_rollback',$3,NULL,NULL)", [org, e, "e".repeat(64)]);
  const importRecords = (capture: any) => rpc("ingest_seed_proven_records", { capture_id: capture.capture_id, fingerprint: capture.fingerprint, source_head: head, records: [] });
  return { db, rpc, code, captures, install, rollbackRelease, importRecords };
}

test("the marked check is present in lease mode only, once, and the body is otherwise 20261104040000's", async () => {
  const up = read("supabase/migrations/20261105062390_ingest_seed_capture_admission.sql");
  const prior = read("supabase/migrations/20261104040000_ingest_s3_proof_seed.sql");
  const body = (s: string) => s.slice(s.indexOf("FUNCTION sessions.ingest_seed_scope_capture(p jsonb)"), s.indexOf("END $capture$;", s.indexOf("FUNCTION sessions.ingest_seed_scope_capture(p jsonb)")));
  const mark = " IF v_mode='lease' THEN PERFORM sessions.ingest_org_admission_hold_check(v_org,'admission'); END IF; -- 062390 org admission hold\n";
  expect(up.split(mark).length).toBe(2);
  expect(body(up).replace(mark, "")).toBe(body(prior));
});

test("while held: a lease capture is refused RH042 with zero writes; preview still answers; another org is unaffected", async () => {
  const f = await fixture();
  const e = await f.install();
  expect(await f.code(f.rpc("ingest_seed_scope_capture", args()))).toBe("RH042");
  expect(await f.captures()).toBe(0);
  const preview = await f.rpc("ingest_seed_scope_capture", args(TENANT.orgId, "preview"));
  expect(preview.capture_id).toBeNull();
  expect(await f.captures()).toBe(0);
  expect((await f.rpc("ingest_seed_scope_capture", args(OTHER))).capture_id).toBeString();
  expect(await f.captures(OTHER)).toBe(1);
  // Released (verified rollback, no attempt): lease issuance resumes.
  await f.rollbackRelease(TENANT.orgId, e);
  expect((await f.rpc("ingest_seed_scope_capture", args())).capture_id).toBeString();
  expect(await f.captures()).toBe(1);
});

test("a lease capture outside READ COMMITTED is refused 40001 (the check's own rule); preview is not", async () => {
  const f = await fixture();
  for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
    expect(await f.code(f.rpc("ingest_seed_scope_capture", args(), iso))).toBe("40001");
    expect((await f.rpc("ingest_seed_scope_capture", args(TENANT.orgId, "preview"), iso)).capture_id).toBeNull();
  }
  expect(await f.captures()).toBe(0);
});

test("partial acknowledgement and re-entry: a scope imported across batches is refused mid-hold and completes on a fresh capture after release", async () => {
  const f = await fixture();
  const first = await f.rpc("ingest_seed_scope_capture", args());
  expect(await f.importRecords(first)).toMatchObject({ accepted: true }); // batch 1 acknowledged before the hold
  const e = await f.install();
  // The pre-slot-style lease still exists (unexpired): the installer's post-key active-lease check sees it.
  expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_seed_captures WHERE org_id=$1 AND expires_at>clock_timestamp()", [TENANT.orgId])).rows[0]!.n).toBe(1);
  expect(await f.code(f.importRecords(first))).toBe("RH042"); // batch 2 while held: the import is admission-class (062320)
  expect(await f.code(f.rpc("ingest_seed_scope_capture", args()))).toBe("RH042"); // and no new lease can be issued (062390)
  await f.rollbackRelease(TENANT.orgId, e);
  const again = await f.rpc("ingest_seed_scope_capture", args()); // re-entry: a fresh capture
  expect(again.capture_id).not.toBe(first.capture_id);
  expect(await f.importRecords(again)).toMatchObject({ accepted: true });
});
