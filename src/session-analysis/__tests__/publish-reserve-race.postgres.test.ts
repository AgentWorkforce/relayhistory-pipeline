/**
 * write-obligation v3 caller proof #1 on owned PG17, two connections: the sweep's `reserve` and
 * the worker's publish (COMPLETE_SQL) take the SAME ticket row lock; whichever commits first
 * wins and the other refuses: publish-before-reserve -> the reservation is refused (the version
 * is owned, nothing deleted); reserve-before-publish -> the publish is refused (the reserved
 * version never becomes owned). Opt-in: SESSION_INGEST_TEST_PG_URL.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { analysisObligationWriterFor, postgresAnalysisJobStore, ticketObligationStore, workerFence } from "../store.ts";
import { encodeAnalysisResult } from "../results.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const ORG = "org-pr", WS = "ws-pr";
const ANCHOR = (s: string) => `${ORG}/${WS}/m-1/claude/2026-10-29/${s}/manifest.json`;

describe.skipIf(!url)("publish vs reserve on the real ticket row lock (two connections)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string;
  const exec = (c: pg.PoolClient | pg.Pool) => ({ query: (t: string, p?: unknown[]) => c.query(t, p as never) });
  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    name = "f1_pubres_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const target = new URL(url!); target.pathname = "/" + name;
    pool = new pg.Pool({ connectionString: target.href, max: 6 });
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect();
    try { await runMigrations(c as never, { profile }); } finally { c.release(); }
  }, 300_000);
  afterAll(async () => {
    await pool?.end();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"); await admin.end(); }
  });

  /** A running job with a reserved key and a second (dispatched) invocation so the ticket outlives the publish. */
  async function inflight(session: string) {
    await pool.query(
      `INSERT INTO sessions.session_index (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
         storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title)
       VALUES ($1, $2, 'claude', $3, 'm-1', 'u', now(), now(), 'fs', 'b', $4, decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 't')`,
      [ORG, WS, session, ANCHOR(session)],
    );
    await pool.query(
      `INSERT INTO sessions.session_analysis_jobs_v2 (org_id, workspace_id, session_id, analysis_version, schema_version, model_version, summary_version, summary_model_version)
       VALUES ($1, $2, $3, 'session-analysis-v2.4:96bulls-s3-v1', 2, 'gpt-5.4', 'session-summary-v3:workspace-output-v2', 'gpt-5.6-luna')`,
      [ORG, WS, session],
    );
    const js = postgresAnalysisJobStore({ db: exec(pool) as never, store: createFsStore({ root: mkdtempSync(join(tmpdir(), "f1-pr-")) }), anchorManifestKey: async () => ANCHOR(session) });
    const job = (await js.claim({ leaseSeconds: 600 }))!;
    const output = { harness: "claude", sourceFingerprint: "f".repeat(64), sourceGeneration: "e".repeat(64), inputTokens: 1, outputTokens: 1, result: { a: session } as never };
    const { bytes, sha256 } = encodeAnalysisResult(output.result);
    const key = (await js.reserve(job, output, { sha256, bytes: bytes.length }))!;
    await analysisObligationWriterFor(workerFence(job)).dispatch(exec(pool) as never, key, [ORG, WS, session, job.id], crypto.randomUUID());
    return { js, job, output, bytes, sha256, key };
  }
  const blockedBy = async (waiter: number, blocker: number) => {
    const until = Date.now() + 10_000;
    while (Date.now() < until) {
      if ((await pool.query<{ p: number[] }>("SELECT pg_blocking_pids($1) AS p", [waiter])).rows[0]!.p.includes(blocker)) return true;
      await Bun.sleep(20);
    }
    return false;
  };
  const pid = async (c: pg.PoolClient) => (await c.query<{ p: number }>("SELECT pg_backend_pid() AS p")).rows[0]!.p;

  const onConn = (c: pg.PoolClient, gate?: Promise<void>) => async <T,>(work: (tx: never) => Promise<T>): Promise<T> => {
    await c.query("BEGIN");
    try {
      const r = await work(exec(c) as never);
      if (gate) await gate;
      await c.query("COMMIT");
      return r;
    } catch (e) {
      await c.query("ROLLBACK");
      throw e;
    }
  };
  const storeOn = (c: pg.PoolClient, session: string) =>
    postgresAnalysisJobStore({ db: exec(c) as never, store: createFsStore({ root: mkdtempSync(join(tmpdir(), "f1-pr-")) }), anchorManifestKey: async () => ANCHOR(session) });

  test("publish holds the row lock first: the waiting reserve then refuses (owned); the version stays owned, nothing reserved", async () => {
    const { job, output, bytes, sha256, key } = await inflight("s-pub-first");
    const P = await pool.connect(), R = await pool.connect();
    const pPid = await pid(P), rPid = await pid(R);
    try {
      const store = ticketObligationStore(onConn(R), key);
      const state = (await store.load())!;
      await P.query("BEGIN");
      expect(await storeOn(P, "s-pub-first").complete({ job, output, stored: { key, sha256, bytes: bytes.length, versionId: "v-race" } })).toBe(true);
      const reserve = store.reserve(state.rev, ["v-race"]);
      expect(await blockedBy(rPid, pPid)).toBe(true);
      await P.query("COMMIT");
      expect(await reserve).toEqual({ ok: false });
      expect((await pool.query("SELECT count(*)::int n FROM sessions.session_analysis_ticket_reservations WHERE result_key = $1", [key])).rows[0].n).toBe(0);
      expect((await pool.query("SELECT result_version_id v FROM sessions.session_analysis_jobs_v2 WHERE id = $1", [job.id])).rows[0].v).toBe("v-race");
    } finally {
      await P.query("ROLLBACK").catch(() => {}); P.release(); R.release();
    }
  }, 60_000);

  test("reserve holds the row lock first: the waiting publish then refuses; the reserved version never becomes owned", async () => {
    const { job, output, bytes, sha256, key } = await inflight("s-res-first");
    const P = await pool.connect(), R = await pool.connect();
    const pPid = await pid(P), rPid = await pid(R);
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    try {
      const state = (await ticketObligationStore(onConn(R), key).load())!;
      // The reserve's transaction: row locked, reservation written, held open until the gate.
      const reserve = ticketObligationStore(onConn(R, gate), key).reserve(state.rev, ["v-doomed"]);
      await Bun.sleep(200);
      const publish = storeOn(P, "s-res-first").complete({ job, output, stored: { key, sha256, bytes: bytes.length, versionId: "v-doomed" } });
      expect(await blockedBy(pPid, rPid)).toBe(true);
      release();
      expect(await reserve).toMatchObject({ ok: true });
      expect(await publish).toBe(false);
      expect((await pool.query("SELECT result_version_id v FROM sessions.session_analysis_jobs_v2 WHERE id = $1", [job.id])).rows[0].v).toBeNull();
      expect((await pool.query("SELECT version_id v FROM sessions.session_analysis_ticket_reservations WHERE result_key = $1", [key])).rows).toEqual([{ v: "v-doomed" }]);
    } finally {
      release?.(); P.release(); R.release();
    }
  }, 60_000);
});
