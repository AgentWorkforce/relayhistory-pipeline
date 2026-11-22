/** Real two-connection races on an owned disposable PostgreSQL fixture only. */
import { describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgres:", "postgresql:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname)
    || u.username !== "postgres" || u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter(l => !l.startsWith("\\")).join("\n");
describe.skipIf(!url)("operator erase write barrier on real PostgreSQL", () => {
  test("writer-first drains durably; erase-first prevents a new write (both actual lock orders)", async () => {
    const admin = new pg.Client({ connectionString: url }); await admin.connect();
    const name = `erase_runtime_${crypto.randomUUID().replaceAll("-", "")}`;
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(url!); target.pathname = `/${name}`;
    const pool = new pg.Pool({ connectionString: target.href, max: 6 });
    const held: pg.PoolClient[] = [];
    try {
      await pool.query(read("supabase/tests/supabase_shim.sql"));
      await pool.query("CREATE EXTENSION btree_gin;CREATE EXTENSION pg_trgm");
      for (const f of SESSION_BOOTSTRAP_FILES) await pool.query(read(f));
      for (const f of readdirSync(join(root, "supabase/migrations")).sort()) await pool.query(read(`supabase/migrations/${f}`));
      for (const f of readdirSync(join(root, "src/hoopdb/migrations")).sort()) await pool.query(read(`src/hoopdb/migrations/${f}`));
      const transaction = async () => {
        const c = await pool.connect(); held.push(c);
        await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest;SET LOCAL statement_timeout='10s'");
        return c;
      };
      const waitForLock = async (waiter: number, blocker: number) => {
        const until = Date.now() + 5_000;
        while (Date.now() < until) {
          const row = (await pool.query<{ p: number[] }>("SELECT pg_blocking_pids($1) p", [waiter])).rows[0]!;
          if (row.p.includes(blocker)) return;
          await Bun.sleep(10);
        }
        throw Error("fixture_expected_lock_not_observed");
      };
      const pid = async (c: pg.PoolClient) => (await c.query<{ p: number }>("SELECT pg_backend_pid() p")).rows[0]!.p;
      const writerId = crypto.randomUUID();
      const write = (c: pg.PoolClient, session: string) => c.query<{ r: boolean }>("SELECT sessions.ingest_operator_runtime_write_begin($1::jsonb) r", [
        JSON.stringify({ orgId: "org", workspaceId: "ws", writerId, sessions: [["claude", session]] }),
      ]);
      const erase = (c: pg.PoolClient, session: string) => c.query<{ r: { uploadsFenced: boolean } }>(
        "SELECT sessions.ingest_operator_runtime_begin('org','ws','claude',$1,$2,'user','operator','bucket','96bulls',$3) r",
        [session, crypto.randomUUID(), "a".repeat(64)]);
      // Writer registers while holding its transaction; erase waits for the same advisory key.
      const writer = await transaction(), eraser = await transaction();
      const writerPid = await pid(writer), eraserPid = await pid(eraser);
      expect((await write(writer, "writer-first")).rows[0]!.r).toBe(true);
      const pendingErase = erase(eraser, "writer-first");
      await waitForLock(eraserPid, writerPid);
      await writer.query("COMMIT");
      expect((await pendingErase).rows[0]!.r.uploadsFenced).toBe(true);
      await eraser.query("COMMIT");
      const lease = crypto.randomUUID();
      await writer.query("SET ROLE bulls96_session_ingest");
      await writer.query("SELECT sessions.ingest_operator_runtime_next($1)", [lease]);
      const allowed = () => writer.query<{ r: boolean }>("SELECT sessions.ingest_operator_runtime_authorize('org','ws','claude','writer-first',$1) r", [lease]);
      expect((await allowed()).rows[0]!.r).toBe(false);
      await writer.query("SELECT sessions.ingest_operator_runtime_write_finish('org','ws',$1)", [writerId]);
      expect((await allowed()).rows[0]!.r).toBe(true);
      await writer.query("RESET ROLE");
      // Reverse order: the writer waits for a pending fence, then sees it and registers nothing.
      await eraser.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest;SET LOCAL statement_timeout='10s'");
      await writer.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest;SET LOCAL statement_timeout='10s'");
      await erase(eraser, "erase-first");
      const pendingWrite = write(writer, "erase-first");
      await waitForLock(writerPid, eraserPid);
      await eraser.query("COMMIT");
      expect((await pendingWrite).rows[0]!.r).toBe(false);
      await writer.query("COMMIT");
      expect((await pool.query("SELECT 1 FROM sessions.session_erasure_writes WHERE session_id='erase-first'")).rows).toEqual([]);
    } finally {
      for (const c of held) { await c.query("ROLLBACK").catch(() => {}); c.release(); }
      await pool.end(); await admin.query(`DROP DATABASE ${name}`); await admin.end();
    }
  }, 120_000);
});
