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
  test("writer-first and erase-first both materialize the permanent turn-origin fence", async () => {
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
