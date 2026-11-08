/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): two-connection proof of
 * 20261105043920. The self-host erase takes the user's fence lock before reading ownership, so a claim holding
 * that lock finishes first and the origin it adds is erased with the rest. PGlite has one connection and cannot
 * show this.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { TURN_ORIGIN_ID } from "../turns.ts";
import { liveMachineId } from "../writer.ts";
import { TENANT } from "./support.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const ERASE = "SELECT sessions.ingest_selfhost_set_session_uploads($1::jsonb) AS result";
const SESSION = "sess-lock";
const { orgId, workspaceId, userId } = TENANT;
const machine = (origin: string) => liveMachineId(workspaceId, origin, userId);

describe.skipIf(!url)("043920 self-host erase takes the fence lock (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string;
  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url });
    await admin.connect();
    name = "erase_lock_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!);
    target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 6 });
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect();
    try { await runMigrations(c, { profile }); } finally { c.release(); }
  }, 180000);
  afterAll(async () => {
    await pool?.end();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 30000);

  const owned = (q: pg.ClientBase | pg.Pool, origin: string) => q.query(
    "INSERT INTO sessions.ingest_session_ownership (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1, $2, $3, $4, 'claude', $5)",
    [orgId, workspaceId, userId, origin, SESSION]);
  const pointer = (origin: string) => pool.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision)
     VALUES ($1, $2, 'claude', $3, $4, $5, now(), now(), 's3', 'bucket', 'k/' || $4,
             decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 2)`,
    [orgId, workspaceId, SESSION, machine(origin), userId]);

  test("an origin claimed under the lock while the erase waits is fenced and tombstoned", async () => {
    await owned(pool, "origin-1");
    for (const origin of ["origin-1", "origin-late", TURN_ORIGIN_ID]) await pointer(origin);

    const claim = await pool.connect(), erase = await pool.connect();
    try {
      await claim.query("BEGIN");
      await claim.query("SELECT sessions.ingest_lock_fences($1, $2, $3)", [orgId, workspaceId, userId]);

      await erase.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
      const pid = (await erase.query("SELECT pg_backend_pid() AS pid")).rows[0].pid as number;
      const done = erase.query(ERASE, [JSON.stringify({
        tenant: { orgId, workspaceId, userId }, originId: "origin-1", sessions: [["claude", SESSION]], enabled: false,
      })]);

      let waiting = false;
      for (let i = 0; i < 200 && !waiting; i++) {
        const r = await pool.query("SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1", [pid]);
        waiting = r.rows[0]?.wait_event_type === "Lock";
        if (!waiting) await new Promise((resolve) => setTimeout(resolve, 25));
      }
      expect(waiting).toBe(true);

      // The claim's ownership row for an origin the erase has not seen, committed while the erase waits.
      await owned(claim, "origin-late");
      await claim.query("COMMIT");
      await done;
      await erase.query("COMMIT");
    } finally {
      await claim.query("ROLLBACK").catch(() => {});
      await erase.query("ROLLBACK").catch(() => {});
      claim.release();
      erase.release();
    }

    const fences = await pool.query<{ origin_id: string }>("SELECT origin_id FROM sessions.upload_deletions WHERE session_id = $1", [SESSION]);
    expect(fences.rows.map((r) => r.origin_id).sort()).toEqual(["origin-1", "origin-late", TURN_ORIGIN_ID].sort());
    const live = await pool.query("SELECT machine_id FROM sessions.session_index WHERE session_id = $1 AND deleted_at IS NULL", [SESSION]);
    expect(live.rows).toEqual([]);
  }, 60000);
});
