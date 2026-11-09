/**
 * Opt-in, owned PostgreSQL latency proof for 20261105044040 (session-first buffered stages).
 * It never connects unless SESSION_INGEST_TEST_PG_URL names a local disposable server whose role can
 * create a database (and set session_replication_role: the bulk clone bypasses the ingest triggers).
 * It creates and drops its own database; no deployed database or migration apply is involved.
 *
 * Shape (the production backlog that timed out): ONE user with up to 167,000 buffered batches over 1,000
 * sessions, one claim per batch, five records each. One batch is delivered through the real ingest app;
 * the rest are generate_series clones of its rows (batch, claim, stage, outbox, native slots, ACK
 * decisions with the recomputed candidate key), so every clone is visible to the readers. The clone runs
 * in chunks and stops early once ~60 s are spent; the N actually reached is logged.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { sqlLedger } from "../../session-ingest/ledger-sql.ts";
import { deliveryAccount } from "../../session-ingest/vendor/delivery/prepare.ts";
import { TENANT, scratchStore, record, batch } from "../../session-ingest/__tests__/support.ts";
import { createIngestApp } from "../../session-ingest/app.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const sql = (path: string) => readFileSync(join(root, path), "utf8");
const TARGET_N = 167_000;
const SESSIONS = 1_000;
const CHUNK = 16_700;
const CLONE_BUDGET_MS = 60_000;
const GIN = "ingest_claim_attempts_records_gin_idx";
const generation = "4".repeat(64), epoch = "1".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64);

/** Clones the template batch 'tpl' as batches lo..hi (session 'sess-' || i % SESSIONS). */
const cloneSql = (lo: number, hi: number) => `
CREATE TEMP TABLE g AS
SELECT i, 'sess-' || (i % ${SESSIONS}) AS sid, 'lat-' || i AS bid,
       md5('claim-' || i)::uuid AS claim, md5('stage-' || i)::uuid AS stage
  FROM generate_series(${lo}, ${hi}) AS i;
INSERT INTO sessions.ingest_batches
SELECT (jsonb_populate_record(b, to_jsonb(b) || jsonb_build_object('batch_id', g.bid,
         'receipt_id', 'rhr_' || encode(sha256(convert_to(g.bid, 'UTF8')), 'hex')))).*
  FROM sessions.ingest_batches AS b CROSS JOIN g WHERE b.batch_id = 'tpl';
INSERT INTO sessions.ingest_claim_attempts
SELECT (jsonb_populate_record(a, to_jsonb(a) || jsonb_build_object('batch_id', g.bid, 'claim_token', g.claim,
         'epochs', (SELECT jsonb_agg(e || jsonb_build_object('session_id', g.sid)) FROM jsonb_array_elements(a.epochs) AS e),
         'records', (SELECT jsonb_agg(r || jsonb_build_object('session_id', g.sid,
                        'record_id', g.bid || '-' || (r->>'ord'), 'revision_id', g.bid || '-' || (r->>'ord') || ':r7'))
                       FROM jsonb_array_elements(a.records) AS r)))).*
  FROM sessions.ingest_claim_attempts AS a CROSS JOIN g WHERE a.batch_id = 'tpl';
INSERT INTO sessions.ingest_stage_intents
SELECT (jsonb_populate_record(s, to_jsonb(s) || jsonb_build_object('batch_id', g.bid, 'claim_token', g.claim,
         'stage_token', g.stage, 'stage_key', s.stage_key || '/' || g.bid))).*
  FROM sessions.ingest_stage_intents AS s CROSS JOIN g WHERE s.batch_id = 'tpl';
INSERT INTO sessions.ingest_buffer_outbox
SELECT (jsonb_populate_record(o, to_jsonb(o) || jsonb_build_object('batch_id', g.bid, 'active_stage_token', g.stage))).*
  FROM sessions.ingest_buffer_outbox AS o CROSS JOIN g WHERE o.batch_id = 'tpl';
INSERT INTO sessions.ingest_lazy_native_slots
SELECT (jsonb_populate_record(ns, to_jsonb(ns) || jsonb_build_object(
         'key_sha256', encode(sha256(convert_to(g.bid || '-' || ad.ord, 'UTF8')), 'hex'),
         'key_material', jsonb_build_array('global-native-custody-v1', ns.org_id, ns.workspace_id, ns.origin_id, g.bid || '-' || ad.ord),
         'record_id', g.bid || '-' || ad.ord, 'current_session', g.sid))).*
  FROM sessions.ingest_lazy_ack_decisions AS ad
  JOIN sessions.ingest_lazy_native_slots AS ns ON ns.key_sha256 = ad.native_key_sha256
  JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = ad.claim_token AND a.batch_id = 'tpl'
 CROSS JOIN g;
INSERT INTO sessions.ingest_lazy_ack_decisions
SELECT (jsonb_populate_record(ad, to_jsonb(ad) || jsonb_build_object('claim_token', g.claim,
         'native_key_sha256', encode(sha256(convert_to(g.bid || '-' || ad.ord, 'UTF8')), 'hex'),
         'candidate_key_sha256', encode(sha256(convert_to(jsonb_build_array('buffer-first-ack-seat-v1',
            ck.control_epoch, ck.source_identity_sha256, a.org_id, a.workspace_id, a.user_id, a.origin_id,
            g.bid || '-' || ad.ord, (r->>'revision'), a.reset_epoch::text, r->>'source', g.sid,
            (a.epochs->0->>'epoch'))::text, 'UTF8')), 'hex')))).*
  FROM sessions.ingest_lazy_ack_decisions AS ad
  JOIN sessions.ingest_claim_attempts AS a ON a.claim_token = ad.claim_token AND a.batch_id = 'tpl'
 CROSS JOIN LATERAL (SELECT x FROM jsonb_array_elements(a.records) AS x WHERE (x->>'ord')::int = ad.ord) AS rr(r)
 CROSS JOIN sessions.ingest_receipt_seat_clock AS ck
 CROSS JOIN g
 WHERE ck.singleton;
DROP TABLE g;`;

describe.skipIf(!url)("044040 buffered stages latency on actual PostgreSQL", () => {
  let admin: pg.Client;
  let db: pg.Client;
  let name = "";
  let n = 0;

  const owner = async (fn: string, p: unknown) =>
    (await db.query(`SELECT sessions.${fn}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0];

  let targetUrl = "";
  async function asReader<R extends pg.QueryResultRow>(q: string, params: unknown[], conn: pg.Client = db): Promise<R[]> {
    await conn.query("BEGIN READ ONLY");
    try {
      await conn.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",
        [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
      await conn.query("SET LOCAL ROLE bulls96_read_api");
      const r = await conn.query<R>(q, params);
      await conn.query("COMMIT");
      return r.rows;
    } catch (error) {
      await conn.query("ROLLBACK").catch(() => undefined);
      throw error;
    }
  }
  // No head: the G2 proof below is written with no_head.
  const head = [epoch, null, null, null, null, false, baseline];
  const pct = (xs: number[], p: number) => {
    const a = [...xs].sort((x, y) => x - y);
    return a[Math.min(a.length - 1, Math.ceil((p / 100) * a.length) - 1)]!;
  };
  /**
   * The full function call, timed in ONE fresh session (empty PL/pgSQL plan cache): call 1 is cold; the
   * rest are warm, and past the fifth execution PL/pgSQL's saved plans may switch to the generic plan
   * (the cached plan a long-lived production connection keeps). `generic` forces the generic plan for
   * the whole series. Stops early once budgetMs is spent; returns cold, the warm samples and the last rows.
   */
  async function series<R extends pg.QueryResultRow>(q: string, params: unknown[], calls: number, generic: boolean, budgetMs: number) {
    const conn = new pg.Client({ connectionString: targetUrl });
    await conn.connect();
    try {
      await conn.query("SET TIME ZONE 'UTC'");
      if (generic) await conn.query("SET plan_cache_mode = force_generic_plan");
      const t0 = performance.now();
      const warm: number[] = [];
      let cold = 0;
      let rows: R[] = [];
      for (let i = 0; i < calls && (i === 0 || performance.now() - t0 < budgetMs); i++) {
        const t = performance.now();
        rows = await asReader<R>(q, params, conn);
        const ms = performance.now() - t;
        if (i === 0) cold = ms; else warm.push(ms);
      }
      return { cold: Math.round(cold), warm, p50: warm.length ? Math.round(pct(warm, 50)) : NaN,
        p95: warm.length ? Math.round(pct(warm, 95)) : NaN, rows };
    } finally {
      await conn.end();
    }
  }

  beforeAll(async () => {
    const configured = new URL(url!);
    if (!new Set(["127.0.0.1", "localhost", "::1", "postgres"]).has(configured.hostname))
      throw new Error("044040 latency proof refuses a non-local database host");
    admin = new pg.Client({ connectionString: url });
    await admin.connect();
    name = `stg_latency_${crypto.randomUUID().replaceAll("-", "")}`;
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(url!);
    target.pathname = `/${name}`;
    targetUrl = target.toString();
    db = new pg.Client({ connectionString: target.toString() });
    await db.connect();
    await db.query("SET TIME ZONE 'UTC'; CREATE SCHEMA sessions; CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
    await db.query(sql("supabase/tests/supabase_shim.sql"));
    for (const file of SESSION_BOOTSTRAP_FILES) await db.query(sql(file));
    for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
      await db.query(sql(`supabase/migrations/${file}`));
    await db.query(sql("supabase/bootstrap/session_read_api_role.sql").split("\n")
      .filter((line) => !line.startsWith("\\")).join("\n"));
    // The migrator's readiness marker (created outside the migration set), PASS at the test generation.
    await db.query(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text);
      INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${generation}','pass')`);
    await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity,
      controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64),
      sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3_600_000).toISOString(), issuer: "relayhistory-cloud" });
    await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z",
      sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"]
        .map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993",
          isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
    await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",
      [identity, epoch, baseline, "e".repeat(64)]);
    const run = async (statement: string, params: unknown[] = []) => {
      await db.query("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
      try {
        const r = await db.query(statement, params);
        await db.query("COMMIT");
        return r as never;
      } catch (error) {
        await db.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
    };
    const fs = scratchStore();
    const app = createIngestApp({
      identify: () => ({ ...TENANT, scopes: ["rth:sync"] }),
      deliver: { ledger: sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation }),
        acceptanceMode: "buffered", target: { store: fs.store, keyRoot: "96bulls" } },
      log: () => {}, custodySource: "frozen-supabase",
    });
    const records = ["t-0", "t-1", "t-2", "t-3", "t-4"].map((id) =>
      record({ record_id: id, revision: 7, revision_id: `${id}:r7`, source: "claude", session_id: "sess-template" }));
    const b = await batch(records, { batch_id: "tpl", account_id: await deliveryAccount(TENANT) });
    const res = await app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ protocolVersion: 1, batch: b }) });
    expect(res.status).toBe(200);
    // The G2 proof at the current generation (no head).
    await db.query(`INSERT INTO sessions.owner_buffered_parity(org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation)
      SELECT $1,$2,$3,$4,NULL,NULL,NULL,NULL,true,$5,e.deletion_set_digest,e.reset_set_digest,e.target_lineage_digest,e.target_generation
        FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,NULL::bigint,NULL,NULL::text) AS e`,
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId, epoch, baseline]);
    // Clone in chunks; stop at the target or once the budget is spent.
    const t0 = performance.now();
    while (n < TARGET_N && performance.now() - t0 < CLONE_BUDGET_MS) {
      const hi = Math.min(TARGET_N, n + CHUNK);
      await db.query(`BEGIN; SET LOCAL session_replication_role = replica; ${cloneSql(n + 1, hi)} COMMIT;`);
      n = hi;
    }
    const cloneMs = Math.round(performance.now() - t0);
    await db.query("ANALYZE");
    console.log(`[044040 latency] cloned ${n} batches (target ${TARGET_N}) over ${SESSIONS} sessions in ${cloneMs} ms` +
      (n < TARGET_N ? `; stopped at the ${CLONE_BUDGET_MS} ms clone budget` : ""));
  }, 900_000);

  afterAll(async () => {
    if (db) await db.end();
    if (admin && name) await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    if (admin) await admin.end();
  }, 60_000);

  test("the backlog is one user's outbox with five records per claim", async () => {
    const { rows } = await db.query<{ outbox: number; claims: number }>(`
      SELECT (SELECT count(*)::int FROM sessions.ingest_buffer_outbox WHERE user_id = $1 AND state = 'buffered') AS outbox,
             (SELECT count(*)::int FROM sessions.ingest_claim_attempts WHERE jsonb_array_length(records) = 5) AS claims`,
      [TENANT.userId]);
    expect(rows[0]).toEqual({ outbox: n + 1, claims: n + 1 });
  });

  test("the session-first stg drives from the records GIN", async () => {
    const { rows } = await db.query<{ "QUERY PLAN": unknown }>(`EXPLAIN (FORMAT JSON)
      SELECT s.stage_token
        FROM sessions.ingest_claim_attempts AS a
        JOIN sessions.ingest_stage_intents AS s ON a.claim_token = s.claim_token
        JOIN sessions.ingest_buffer_outbox AS o ON s.stage_token = o.active_stage_token
        JOIN sessions.ingest_batches AS b
          ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
       WHERE jsonb_typeof(a.records) = 'array'
         AND a.records @> jsonb_build_array(jsonb_build_object('source', 'claude', 'session_id', 'sess-7'))
         AND o.org_id = '${TENANT.orgId}' AND o.workspace_id = '${TENANT.workspaceId}'
         AND o.user_id = '${TENANT.userId}' AND o.state = 'buffered'`);
    const plan = JSON.stringify(rows[0]!["QUERY PLAN"]);
    console.log(`[044040 latency] stg plan uses ${GIN}: ${plan.includes(GIN)}`);
    expect(plan).toContain(GIN);
  });

  // The full call (not an EXPLAIN of a reduced query): the function's own cached plan is what is timed.
  const STAGES_SQL = "SELECT batch_id, kept_ordinals FROM sessions.buffered_session_stages_for($1,'claude',$2,$3,$4,$5,$6,$7,$8,$9)";
  const LIST_SQL = "SELECT session_id FROM sessions.buffered_sessions_for($1,NULL,NULL,NULL,NULL,$2,$3,$4,$5,$6,$7,$8)";
  const CALLS = 12; // 1 cold + 11 warm: more than 5 executions, so PL/pgSQL's plan cache considers the generic plan

  for (const generic of [false, true])
    test(`one session's stages: full call, cold + ${CALLS - 1} warm (${generic ? "plan_cache_mode = force_generic_plan" : "default plan cache"}); warm p50/p95 under 1,000 ms`, async () => {
      for (const session of ["sess-7", "sess-0"]) {
        const want = Array.from({ length: n }, (_, i) => i + 1).filter((i) => i % SESSIONS === Number(session.slice(5))).length;
        const r = await series<{ batch_id: string; kept_ordinals: number[] }>(STAGES_SQL, [TENANT.workspaceId, session, ...head], CALLS, generic, 240_000);
        console.log(`[044040 latency] buffered_session_stages_for ${session} ${generic ? "generic" : "default"}: ${r.rows.length} stages; ` +
          `cold ${r.cold} ms; warm n=${r.warm.length} p50 ${r.p50} ms p95 ${r.p95} ms max ${Math.round(Math.max(...r.warm))} ms (backlog ${n + 1})`);
        expect(want).toBeGreaterThan(0);
        expect(r.rows.length).toBe(want);
        expect(r.rows.every((x) => x.kept_ordinals.length === 5)).toBe(true);
        expect(r.warm.length).toBe(CALLS - 1);
        expect(r.p50).toBeLessThan(1_000);
        expect(r.p95).toBeLessThan(1_000);
      }
    }, 600_000);

  for (const generic of [false, true])
    test(`the list: full call, cold + warm (${generic ? "force_generic_plan" : "default plan cache"}); linear in the backlog, logged, not asserted`, async () => {
      // Every list call scans the whole backlog; the series stops once ~150 s are spent (after the call in flight).
      const r = await series<{ session_id: string }>(LIST_SQL, [TENANT.workspaceId, ...head], CALLS, generic, 150_000);
      console.log(`[044040 latency] buffered_sessions_for ${generic ? "generic" : "default"}: ${r.rows.length} sessions; ` +
        `cold ${r.cold} ms; warm n=${r.warm.length} p50 ${r.p50} ms p95 ${r.p95} ms (backlog ${n + 1})`);
      expect(r.rows.length).toBeGreaterThan(0);
    }, 900_000);
});
