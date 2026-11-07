/**
 * Final review P1 (#296): request-history admission vs a C3 purge, with a REAL overlap on owned
 * PG17 (two connections). The purge holds its transaction open; the backfill (its own
 * connection, READ COMMITTED) starts its admission meanwhile; then the purge commits. The shared
 * per-scope lock must make the backfill block and then refuse: zero request rows survive.
 * Opt-in: SESSION_INGEST_TEST_PG_URL (an owned loopback postgres superuser URL).
 */
import { afterAll, beforeAll, expect, test, describe } from "bun:test";
import pg from "pg";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { storeEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { applyBackfill, planBackfill, type NeonV1Payload } from "../backfill.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const SCOPE = { orgId: "org-race", workspaceId: "ws-race", sessionId: "s-race" };
const NEON: NeonV1Payload = {
  outcome: "ok", sessionId: SCOPE.sessionId, analyses: [],
  requests: [{ id: "33333333-3333-4333-8333-333333333333", sessionId: SCOPE.sessionId, status: "failed", analysisId: null,
    error: "session_evidence_incomplete", createdAt: "2026-09-30T00:00:00+00:00", updatedAt: "2026-09-30T00:00:01+00:00" }],
};

describe.skipIf(!url)("backfill request history vs a C3 purge (real overlap, two connections)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string;
  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    name = "f1_race_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 6 });
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect();
    try { await runMigrations(c as never, { profile }); } finally { c.release(); }
    await pool.query(`CREATE TABLE IF NOT EXISTS sessions.ingest_frozen_source_owned_sessions(
      org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
      source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id))`);
    await pool.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES ($1, $2, 'u-owner', 'o', 'claude', $3)", [SCOPE.orgId, SCOPE.workspaceId, SCOPE.sessionId]);
    await pool.query(
      `INSERT INTO sessions.session_index (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
         storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title)
       VALUES ($1, $2, 'claude', $3, 'm-1', 'u-owner', now(), now(), 'fs', 'b', $4, decode(repeat('ab', 32), 'hex'), 'jsonl',
               '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 't')`,
      [SCOPE.orgId, SCOPE.workspaceId, SCOPE.sessionId, `${SCOPE.orgId}/${SCOPE.workspaceId}/m-1/claude/2026-10-29/${SCOPE.sessionId}/manifest.json`],
    );
  }, 300_000);
  afterAll(async () => {
    await pool?.end();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"); await admin.end(); }
  });

  test("the purge commits while the backfill's admission is in flight: the backfill blocks, then refuses; zero request rows", async () => {
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "f1-race-")) });
    const purger = await pool.connect(), writer = await pool.connect();
    try {
      // The purge's transaction is open (fence written, scope locked), not yet committed.
      await purger.query("BEGIN");
      await purger.query("SELECT sessions.purge_session_analysis_for_owned($1, $2, 'u-owner')", [SCOPE.orgId, SCOPE.workspaceId]);
      // The backfill starts on its own connection (its generation captured before the commit).
      const db = { query: (t: string, p?: unknown[]) => writer.query(t, p as never) };
      const pid = (await writer.query<{ p: number }>("SELECT pg_backend_pid() AS p")).rows[0]!.p;
      const run = applyBackfill(db as never, store, SCOPE, planBackfill(SCOPE, NEON), { eraser: storeEraser(store) }).then(
        () => "ok", (e: Error) => e.message);
      // It must be waiting on the purge (the shared per-scope lock), not racing past it.
      const purgerPid = (await purger.query<{ p: number }>("SELECT pg_backend_pid() AS p")).rows[0]!.p;
      const until = Date.now() + 10_000;
      let blocked = false;
      while (Date.now() < until && !blocked) {
        const pids = (await pool.query<{ pids: number[] }>("SELECT pg_blocking_pids($1) AS pids", [pid])).rows[0]!.pids;
        blocked = pids.includes(purgerPid);
        if (!blocked) await Bun.sleep(20);
      }
      await purger.query("COMMIT");
      expect(blocked).toBe(true);
      expect(await run).toContain("backfill_generation_changed");
      expect((await pool.query("SELECT count(*)::int n FROM sessions.session_analysis_requests_v2 WHERE session_id = $1", [SCOPE.sessionId])).rows[0].n).toBe(0);
      expect((await pool.query("SELECT count(*)::int n FROM sessions.session_analysis_purge_fences WHERE session_id = $1", [SCOPE.sessionId])).rows[0].n).toBe(1);
    } finally {
      await purger.query("ROLLBACK").catch(() => {});
      await writer.query("ROLLBACK").catch(() => {});
      purger.release(); writer.release();
    }
  }, 120_000);
});
