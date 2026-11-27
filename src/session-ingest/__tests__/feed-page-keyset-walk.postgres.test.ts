/**
 * 062350 deep-page proof on real PostgreSQL (SESSION_INGEST_TEST_PG_URL): sessions.buffered_search_feed_page walks
 * ingest_stage_intents_cleanup_idx in (created_at, stage_token) order from a deep cursor and stops at the limit,
 * probing the outbox once per visited row through ingest_buffer_outbox_active_stage_token_idx (the planner's choice
 * for the scalar subquery). It never hash-joins the two tables, sorts or gathers, including after the 5-execution
 * generic-plan point.
 *
 * Fixture (synthetic ids only): one heavy tenant with FEED_PROOF_ROWS (default 750,000) acknowledged live stage
 * intents, one per second, each with its own batch. Every intent before the cursor has an erased outbox row. After
 * the cursor, 1 in 10 intents is the batch's active buffered stage, so a 200-row page has to skip about 1,800 rows.
 * Rows are bulk-loaded as the fixture superuser with session_replication_role=replica (FK and trigger checks off).
 * The claim/batch parent chain is irrelevant to the plan and is not built. Statistics come from ANALYZE.
 *
 * The plan is read with auto_explain (log_nested_statements, analyze, buffers, JSON) at NOTICE on the runtime
 * login. The RPC requires session_user = buffered_search_feed_runtime, which this fixture's trust auth allows. That
 * role is cluster-global: it is switched to LOGIN only for this proof and switched back in afterAll, and a failed
 * restore fails the run, so a reused fixture never keeps a changed role silently.
 * The same fixture then runs with 062350 rolled back (its .down.sql) to record the prior body's plan for
 * comparison. That part is evidence only and is not asserted.
 */
import { afterAll, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, writeFileSync } from "node:fs";
import { fixturePgUrl, migrateAsOwner, scratchDatabase, serverMajor, type ScratchDatabase } from "./hold-harness/pg.ts";

setDefaultTimeout(1_800_000);
const url = fixturePgUrl();
const ROWS = Number(process.env.FEED_PROOF_ROWS ?? 750_000);
const TAIL = Math.min(50_000, Math.floor(ROWS / 10));
const CURSOR_AT = ROWS - TAIL; // the cursor sits after intent #CURSOR_AT (1-based)
const ORG = "org_feedproof_heavy";
const RUNTIME = "buffered_search_feed_runtime";
const DOWN_062350 = "supabase/rollbacks/20261105062350_buffered_search_feed_page_keyset.down.sql";
const OUT = process.env.FEED_PROOF_OUT;

let db: ScratchDatabase | undefined;
let restoreLogin: (() => Promise<void>) | undefined;
afterAll(async () => {
  await db?.drop().catch(() => {});
  if (!restoreLogin) return;
  await restoreLogin(); // fail-visible: a throw here fails the run
  const a = await client(url!);
  try { expect((await a.query("SELECT rolcanlogin FROM pg_roles WHERE rolname=$1", [RUNTIME])).rows[0].rolcanlogin).toBe(false); }
  finally { await a.end(); }
}, 120_000);

type PlanNode = { "Node Type": string; "Index Name"?: string; "Relation Name"?: string; "Actual Rows"?: number;
  "Rows Removed by Filter"?: number; "Shared Hit Blocks"?: number; "Shared Read Blocks"?: number; "Actual Loops"?: number;
  "Parent Relationship"?: string; "Subplan Name"?: string; Plans?: PlanNode[] };
const walk = (n: PlanNode, f: (n: PlanNode) => void) => { f(n); for (const c of n.Plans ?? []) walk(c, f); };
const nodeTypes = (n: PlanNode) => { const t: string[] = []; walk(n, (x) => t.push(x["Node Type"] + (x["Index Name"] ? `(${x["Index Name"]})` : ""))); return t; };

async function client(u: string) { const c = new pg.Client({ connectionString: u }); await c.connect(); return c; }
function runtimeUrl(d: ScratchDatabase) { const u = new URL(d.fixtureUrl); u.username = RUNTIME; u.password = ""; return u.href; }

async function seed(su: pg.Client) {
  await su.query("SET session_replication_role = replica");
  await su.query(`INSERT INTO sessions.ingest_stage_intents
    (stage_token, claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, stage_key, stage_sha256, stage_bytes,
     group_ordinals, reset_epoch, receipt_state, cleanup_state, created_at, expires_at)
    SELECT md5('st'||i)::uuid, md5('cl'||i)::uuid, $1, 'ws_feedproof', 'user_feedproof', 'origin_feedproof', 'batch_'||i, repeat('d',64),
      'stage/'||i, repeat('a',64), 1, '[]'::jsonb, 0, 'acknowledged', 'live',
      timestamptz '2026-11-01 00:00:00+00' + i * interval '1 second', timestamptz '2027-01-01 00:00:00+00'
    FROM generate_series(1, $2::int) i`, [ORG, ROWS]);
  // Before the cursor: erased (no active stage). After it: 1 in 10 is the batch's active buffered stage.
  await su.query(`INSERT INTO sessions.ingest_buffer_outbox (org_id, workspace_id, origin_id, batch_id, user_id, digest, active_stage_token, state, created_at)
    SELECT $1, 'ws_feedproof', 'origin_feedproof', 'batch_'||i, 'user_feedproof', repeat('d',64),
      CASE WHEN i > $3 AND i % 10 = 0 THEN md5('st'||i)::uuid END,
      CASE WHEN i > $3 AND i % 10 = 0 THEN 'buffered' ELSE 'erased' END,
      timestamptz '2026-11-01 00:00:00+00' + i * interval '1 second'
    FROM generate_series(1, $2::int) i`, [ORG, ROWS, CURSOR_AT]);
  await su.query("SET session_replication_role = origin");
  await su.query("ANALYZE sessions.ingest_stage_intents"); await su.query("ANALYZE sessions.ingest_buffer_outbox");
}

/** Calls the page RPC `times` times as the runtime login and returns each call's auto_explain plan for the stage walk. */
async function pages(d: ScratchDatabase, cursor: string, times: number) {
  const c = await client(runtimeUrl(d));
  await c.query("SET statement_timeout = '5s'"); // the production feed login's startup statement_timeout
  const plans: { plan: PlanNode; ms: number }[] = []; const results: { stages: { stageToken: string; createdAt: string }[] }[] = [];
  c.on("notice", (m) => {
    const text = String(m.message ?? "");
    const i = text.indexOf("{"); if (i < 0) return;
    try {
      const j = JSON.parse(text.slice(i)) as { "Query Text"?: string; Plan: PlanNode & { "Actual Total Time"?: number } };
      if (j["Query Text"]?.includes("ingest_stage_intents") && j["Query Text"]?.includes("ORDER BY s.created_at")) plans.push({ plan: j.Plan, ms: j.Plan["Actual Total Time"] ?? NaN });
    } catch { /* not a plan */ }
  });
  try {
    for (let k = 0; k < times; k++)
      results.push((await c.query("SELECT sessions.buffered_search_feed_page($1::jsonb) AS r", [JSON.stringify({ cursor, since: "2026-11-01T00:00:00Z", limit: 200 })])).rows[0].r);
  } finally { await c.end(); }
  return { plans, results, last: results[results.length - 1]! };
}

async function armAutoExplain(su: pg.Client) {
  for (const s of ["session_preload_libraries = 'auto_explain'", "auto_explain.log_min_duration = 0", "auto_explain.log_analyze = on",
    "auto_explain.log_buffers = on", "auto_explain.log_nested_statements = on", "auto_explain.log_format = json", "auto_explain.log_level = notice",
    "auto_explain.log_timing = on"])
    await su.query(`ALTER ROLE ${RUNTIME} IN DATABASE ${db!.name} SET ${s}`);
}

async function setCursor(su: pg.Client, name: string) {
  await su.query(`INSERT INTO sessions.buffered_search_feed_cursors(name, since, after_created_at, after_stage_token)
    VALUES ($1, '2026-11-01T00:00:00Z', timestamptz '2026-11-01 00:00:00+00' + $2::int * interval '1 second', md5('st'||$2::int)::uuid)
    ON CONFLICT (name) DO UPDATE SET after_created_at = EXCLUDED.after_created_at, after_stage_token = EXCLUDED.after_stage_token`, [name, CURSOR_AT]);
}

function summary(p: PlanNode) {
  let scan: PlanNode | undefined; let probe: PlanNode | undefined;
  walk(p, (n) => { if (n["Relation Name"] === "ingest_stage_intents" && !scan) scan = n; if (n["Relation Name"] === "ingest_buffer_outbox" && !probe) probe = n; });
  let hit = 0, read = 0; walk(p, (n) => { hit = Math.max(hit, n["Shared Hit Blocks"] ?? 0); read = Math.max(read, n["Shared Read Blocks"] ?? 0); });
  return { nodes: nodeTypes(p), stageScan: scan && { type: scan["Node Type"], index: scan["Index Name"], rows: scan["Actual Rows"], removedByFilter: scan["Rows Removed by Filter"] ?? 0 },
    // Actual Rows on the probe is per-loop (rounded), so 200 hits over ~2,000 loops reads 0.
    outboxProbe: probe && { type: probe["Node Type"], index: probe["Index Name"], loops: probe["Actual Loops"], rowsPerLoop: probe["Actual Rows"] },
    sharedHitBlocks: p["Shared Hit Blocks"] ?? hit, sharedReadBlocks: p["Shared Read Blocks"] ?? read };
}

test.skipIf(!url)("062350: a deep page walks the cleanup index in order and stops at the limit, before and after the generic-plan point", async () => {
  const major = await serverMajor(url!);
  db = await scratchDatabase(url!, { bootstrap: true, prefix: "feedwalk" });
  await migrateAsOwner(db.ownerUrl);
  const su = await client(db.fixtureUrl);
  try {
    const role = (await su.query("SELECT rolcanlogin FROM pg_roles WHERE rolname=$1", [RUNTIME])).rows[0];
    expect(role).toBeDefined();
    // Roles are cluster-global and the bootstrap verifier checks this one's profile: LOGIN only for this proof, then back.
    if (!role.rolcanlogin) {
      await su.query(`ALTER ROLE ${RUNTIME} LOGIN`);
      restoreLogin = async () => { const a = await client(url!); try { await a.query(`ALTER ROLE ${RUNTIME} NOLOGIN`); } finally { await a.end(); } };
    }
    await su.query(`GRANT CONNECT ON DATABASE ${db.name} TO ${RUNTIME}`);
    await seed(su);
    await armAutoExplain(su);
    await setCursor(su, "proof-keyset");
    const t0 = Date.now();
    const after = await pages(db, "proof-keyset", 8);
    const wallMs = Date.now() - t0;

    // Correctness on every call (the page never advances the cursor): exactly the 200 active stages after it, in order.
    const firstActive = Math.ceil((CURSOR_AT + 1) / 10) * 10;
    const want = (await su.query("SELECT array_agg(md5('st'||i)::uuid::text ORDER BY i) t FROM generate_series($1::int, $1::int + 1990, 10) i", [firstActive])).rows[0].t as string[];
    expect(after.results).toHaveLength(8);
    for (const r of after.results) {
      expect(r.stages.map((s) => s.stageToken)).toEqual(want);
      expect(r.stages.map((s) => s.createdAt)).toEqual([...r.stages.map((s) => s.createdAt)].sort());
    }

    expect(after.plans).toHaveLength(8);
    const summaries = after.plans.map((p) => ({ ...summary(p.plan), ms: p.ms }));
    for (const s of summaries) {
      expect(s.nodes.some((n) => n.startsWith("Hash Join") || n.startsWith("Sort") || n.startsWith("Gather"))).toBe(false);
      expect(s.stageScan?.type).toBe("Index Scan");
      expect(s.stageScan?.index).toBe("ingest_stage_intents_cleanup_idx");
      // The walk visits only the rows between the cursor and the 200th match, not the table, and probes each once.
      const visited = (s.stageScan?.rows ?? 0) + (s.stageScan?.removedByFilter ?? 0);
      expect(s.stageScan?.rows).toBe(200);
      expect(visited).toBeLessThanOrEqual(2_100);
      expect(s.outboxProbe?.type).toBe("Index Scan");
      expect(s.outboxProbe?.index).toBe("ingest_buffer_outbox_active_stage_token_idx");
      expect(s.outboxProbe?.loops).toBe(visited);
    }

    // Evidence only: the same fixture under the prior page body (062350 rolled back).
    const own = await client(db.ownerUrl);
    let prior: unknown = null;
    try { await own.query(readFileSync(DOWN_062350, "utf8")); } finally { await own.end(); }
    try {
      await setCursor(su, "proof-prior");
      const p = await pages(db, "proof-prior", 8);
      prior = { plans: p.plans.map((x) => ({ ...summary(x.plan), ms: x.ms })), returned: p.last.stages.length };
    } catch (e) { prior = { error: String((e as { code?: string }).code ?? e) }; }

    const receipt = { proof: "062350 buffered_search_feed_page deep keyset walk", serverMajor: major,
      serverVersion: (await su.query("SHOW server_version")).rows[0].server_version,
      settings: (await su.query("SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers','work_mem','random_page_cost','effective_cache_size','jit') ORDER BY name")).rows,
      rows: ROWS, cursorAfterRow: CURSOR_AT, activeEvery: 10, limit: 200, wallMsFor8Calls: wallMs,
      tableBlocks: (await su.query("SELECT relpages FROM pg_class WHERE oid='sessions.ingest_stage_intents'::regclass")).rows[0].relpages,
      keyset062350: summaries, prior043870_045010: prior };
    console.log("FEED_PAGE_WALK_RECEIPT " + JSON.stringify(receipt));
    if (OUT) writeFileSync(OUT, JSON.stringify(receipt, null, 2) + "\n");
  } finally { await su.end(); }
});
