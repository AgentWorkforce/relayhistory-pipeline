/** Real two-connection turn registration/erasure races on an owned disposable PostgreSQL fixture. */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (
    !["postgres:", "postgresql:"].includes(u.protocol) ||
    !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) ||
    u.username !== "postgres" ||
    u.password ||
    u.pathname !== "/postgres" ||
    u.search ||
    u.hash
  )
    throw Error("owned_loopback_fixture_required");
}

const root = join(import.meta.dir, "../../..");
const read = (path: string) =>
  readFileSync(join(root, path), "utf8")
    .split("\n")
    .filter((line) => !line.startsWith("\\"))
    .join("\n");
const tenant = { orgId: "turn-race-org", workspaceId: "turn-race-ws", userId: "turn-race-user" };
const registerInput = (sessionId: string) =>
  JSON.stringify({ tenant, sessions: [["claude", sessionId]] });

describe.skipIf(!url)("turn fence registration on real PostgreSQL", () => {
  test("writer/erase races materialize the fence and reversed imports acquire locks in one order", async () => {
    const admin = new pg.Client({ connectionString: url });
    await admin.connect();
    const name = `turn_fence_${crypto.randomUUID().replaceAll("-", "")}`;
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(url!);
    target.pathname = `/${name}`;
    const pool = new pg.Pool({ connectionString: target.href, max: 5 });
    const held: pg.PoolClient[] = [];
    try {
      await pool.query(read("supabase/tests/supabase_shim.sql"));
      await pool.query("CREATE EXTENSION btree_gin;CREATE EXTENSION pg_trgm");
      for (const file of SESSION_BOOTSTRAP_FILES) await pool.query(read(file));
      for (const file of readdirSync(join(root, "supabase/migrations")).sort())
        await pool.query(read(`supabase/migrations/${file}`));

      const connection = async (role = false) => {
        const client = await pool.connect();
        held.push(client);
        await client.query(
          `BEGIN;${role ? "SET LOCAL ROLE bulls96_session_ingest;" : ""}SET LOCAL statement_timeout='10s'`,
        );
        return client;
      };
      const pid = async (client: pg.PoolClient) =>
        (await client.query<{ pid: number }>("SELECT pg_backend_pid() pid")).rows[0]!.pid;
      const waitForLock = async (waiter: number, blocker: number) => {
        const deadline = Date.now() + 5_000;
        while (Date.now() < deadline) {
          const blocked = (
            await pool.query<{ pids: number[] }>("SELECT pg_blocking_pids($1) pids", [waiter])
          ).rows[0]!.pids;
          if (blocked.includes(blocker)) return;
          await Bun.sleep(10);
        }
        throw Error("fixture_expected_lock_not_observed");
      };
      const register = (client: pg.PoolClient, sessionId: string) =>
        client.query<{ result: { fenced: [string, string][] } }>(
          "SELECT sessions.ingest_turn_register($1::jsonb) result",
          [registerInput(sessionId)],
        );
      const erase = (client: pg.PoolClient, sessionId: string) =>
        client.query(
          `INSERT INTO sessions.upload_deletions
             (org_id,workspace_id,user_id,origin_id,source,session_id)
           VALUES($1,$2,'other-user','desktop-origin','claude',$3)`,
          [tenant.orgId, tenant.workspaceId, sessionId],
        );
      const materialized = (sessionId: string) =>
        pool.query<{ count: string }>(
          `SELECT count(*) count FROM sessions.upload_deletions
            WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
              AND origin_id='relayhistory:conversation_turns:v1'
              AND source='claude' AND session_id=$4`,
          [tenant.orgId, tenant.workspaceId, tenant.userId, sessionId],
        );

      // Registration owns the logical-session lock. Erasure blocks, then observes the committed writer.
      const writer = await connection(true);
      const eraser = await connection();
      const writerPid = await pid(writer);
      const eraserPid = await pid(eraser);
      expect((await register(writer, "writer-first")).rows[0]!.result.fenced).toEqual([]);
      const pendingErase = erase(eraser, "writer-first");
      await waitForLock(eraserPid, writerPid);
      await writer.query("COMMIT");
      await pendingErase;
      await eraser.query("COMMIT");
      expect((await materialized("writer-first")).rows[0]!.count).toBe("1");

      // Erasure owns the same lock. Registration blocks, then observes the committed fence.
      await eraser.query("BEGIN;SET LOCAL statement_timeout='10s'");
      await writer.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest;SET LOCAL statement_timeout='10s'");
      await erase(eraser, "erase-first");
      const pendingRegister = register(writer, "erase-first");
      await waitForLock(writerPid, eraserPid);
      await eraser.query("COMMIT");
      expect((await pendingRegister).rows[0]!.result.fenced).toEqual([["claude", "erase-first"]]);
      await writer.query("COMMIT");
      expect((await materialized("erase-first")).rows[0]!.count).toBe("1");

      // Hold the first logical-session lock. A reversed input must sort before
      // locking, so it blocks on this lock without first taking the second.
      // The holder can therefore import both rows and release; caller-order
      // locking deadlocks here (the reversed caller would already own B).
      const a = "import-a", b = "import-b";
      const rows = (ids: string[]) => JSON.stringify(ids.map((session_id) => ({
        org_id: tenant.orgId, workspace_id: tenant.workspaceId, source: "claude", session_id,
      })));
      await writer.query("BEGIN;SET LOCAL ROLE service_role;SET LOCAL statement_timeout='10s'");
      await eraser.query("BEGIN;SET LOCAL ROLE service_role;SET LOCAL statement_timeout='10s'");
      await writer.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1||chr(31)||$2||chr(31)||'claude'||chr(31)||$3,0))",
        [tenant.orgId, tenant.workspaceId, a],
      );
      const pendingReversed = eraser.query(
        "SELECT sessions.ingest_turn_fences_import($1::jsonb)",
        [rows([b, a])],
      );
      await waitForLock(eraserPid, writerPid);
      await writer.query("SELECT sessions.ingest_turn_fences_import($1::jsonb)", [rows([a, b])]);
      await writer.query("COMMIT");
      await pendingReversed;
      await eraser.query("COMMIT");
      expect((await pool.query<{ session_id: string }>(
        "SELECT session_id FROM sessions.turn_session_fences WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3::text[]) ORDER BY session_id",
        [tenant.orgId, tenant.workspaceId, [a, b]],
      )).rows.map((row) => row.session_id)).toEqual([a, b]);
    } finally {
      for (const client of held) {
        await client.query("ROLLBACK").catch(() => {});
        client.release();
      }
      await pool.end();
      await admin.query(`DROP DATABASE ${name}`);
      await admin.end();
    }
  }, 120_000);
});
