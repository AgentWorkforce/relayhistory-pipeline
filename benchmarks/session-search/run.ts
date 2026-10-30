#!/usr/bin/env bun
/**
 * bun benchmarks/session-search/run.ts
 *
 *   BENCH_SCALE       S | M | L | test (default test)
 *   BENCH_PG_URL      local Postgres; a database named search_bench is dropped and
 *                     recreated. Unset: in-process PGlite, no throughput figures.
 *   BENCH_SHAPE_FILE  JSON of CorpusParams overrides (aggregate counts only)
 *   BENCH_OUT         report path (default benchmarks/session-search/results/<scale>.json)
 *   BENCH_REPEATS     warm samples per query (default 30)
 *
 * Exits 1 on any isolation or Lane A correctness failure: those are not metrics.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Client } from "pg";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { writeShortSummary } from "../../src/hoopdb/session-directory-summary.ts";
import {
  claims, directoryPlanSql, directorySearch, installSchema, likeTerm, loadBatch, loadNeonBatch,
  neonBriefSearch, neonEventSearch, NEON_BRIEF_SQL, NEON_EVENTS_SQL, type Db, type Hit,
} from "./arms.ts";
import { generateCorpus, SCALES, TERMS, tenantCanary, type CorpusParams, type CorpusSession } from "./corpus.ts";
import { assertLocalTarget } from "./guard.ts";
import { identity, listQueries, oracleLaneA, oracleLaneB, SMALL, textQueries, WHALE, type Tenant } from "./queries.ts";
import { planNodes, rowAmplification, summarize, type PlanNode } from "./stats.ts";

const scale = process.env.BENCH_SCALE ?? "test";
const base = SCALES[scale];
if (!base) throw new Error(`unknown BENCH_SCALE ${scale}`);
const shape = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) as Partial<CorpusParams> : {};
const params: CorpusParams = { ...base, ...shape };
const repeats = Number(process.env.BENCH_REPEATS ?? 30);
const out = process.env.BENCH_OUT ?? join(import.meta.dir, `results/${scale}.json`);
const pgUrl = process.env.BENCH_PG_URL;
const BATCH = 2_000;

const pgDb = (c: Client): Db => ({
  query: async (s, a) => ({ rows: (await c.query(s, a as never[])).rows }),
  exec: async (s) => void (await c.query(s)),
});
let target = "";
async function connect(): Promise<{ db: Db; close(): Promise<void> }> {
  const c = new Client({ connectionString: target });
  await c.connect();
  return { db: pgDb(c), close: () => c.end() };
}

async function open(): Promise<Db> {
  if (!pgUrl) {
    const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
    return { query: (s, a) => pg.query(s, a as never) as never, exec: async (s) => void (await pg.exec(s)) };
  }
  assertLocalTarget(pgUrl);
  const admin = new Client({ connectionString: pgUrl });
  await admin.connect();
  await admin.query("DROP DATABASE IF EXISTS search_bench");
  await admin.query("CREATE DATABASE search_bench");
  await admin.end();
  const u = new URL(pgUrl);
  u.pathname = "/search_bench";
  target = u.toString();
  return (await connect()).db;
}

const now = () => performance.now();
const failures: string[] = [];
const hitIds = (hits: Hit[]) => hits.map((h) => identity({ harness: h.harness, sessionId: h.session_id }));
const tenants = [WHALE, SMALL];

async function timed(run: () => Promise<unknown>) {
  const start = now();
  try { await run(); return { ms: now() - start, error: null as string | null }; }
  catch (e) { return { ms: now() - start, error: (e as { code?: string }).code ?? "error" }; }
}

async function sample(run: (db: Db) => Promise<unknown>, db: Db) {
  const cold: number[] = [];
  const errors: string[] = [];
  // Connection-cold: a new backend's first run. The server's buffers stay warm.
  for (let i = 0; i < (pgUrl ? 5 : 1); i++) {
    const c = pgUrl ? await connect() : { db, close: async () => {} };
    const r = await timed(() => run(c.db));
    cold.push(r.ms);
    if (r.error) errors.push(r.error);
    await c.close();
  }
  const warm: number[] = [];
  for (let i = 0; i < repeats; i++) {
    const r = await timed(() => run(db));
    warm.push(r.ms);
    if (r.error) errors.push(r.error);
  }
  return { cold: summarize(cold), warm: summarize(warm), errors: [...new Set(errors)], errorCount: errors.length };
}

async function explain(db: Db, sql: string, args: unknown[]) {
  const { rows } = await db.query<{ "QUERY PLAN": [{ Plan: PlanNode; "Execution Time": number }] }>(
    `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, args);
  const doc = rows[0]!["QUERY PLAN"][0];
  const nodes = planNodes(doc.Plan);
  return {
    indexes: [...new Set(nodes.flatMap((n) => n["Index Name"] ?? []))],
    seqScan: nodes.some((n) => n["Node Type"] === "Seq Scan"),
    executionMs: doc["Execution Time"],
    rowAmplification: rowAmplification(doc.Plan),
    plan: doc.Plan,
  };
}

/** Tenant rows kept for the oracle, events cut to those Lane B needs. */
function retain(s: CorpusSession, kept: CorpusSession[]) {
  if (!tenants.some((t) => t.orgId === s.orgId && t.workspaceId === s.workspaceId)) return;
  kept.push({ ...s, events: s.events.filter((e) => e.content.includes(TERMS.transcriptOnly) || e.content.includes(TERMS.oldTranscriptOnly)) });
}

async function loadDirectory(db: Db, withSummary: boolean, kept?: CorpusSession[], neon = false) {
  const start = now();
  let batch: CorpusSession[] = [];
  const flush = async () => {
    if (!batch.length) return;
    await loadBatch(db, batch, withSummary);
    if (neon) await loadNeonBatch(db, batch);
    batch = [];
  };
  for (const s of generateCorpus(params)) {
    if (kept) retain(s, kept);
    batch.push(s);
    if (batch.length >= BATCH) await flush();
  }
  await flush();
  await db.exec("ANALYZE sessions.session_directory; ANALYZE sessions.session_directory_users");
  return now() - start;
}

async function laneA(db: Db, kept: CorpusSession[], withSummary: boolean, arm: string) {
  const results: Record<string, unknown>[] = [];
  for (const t of tenants) {
    for (const q of textQueries(t).filter((x) => x.lane === "A")) {
      const want = oracleLaneA(kept, t, q.term, withSummary);
      let got: Hit[] = [];
      try { got = await directorySearch(db, t, q.term); } catch (e) { failures.push(`${arm} ${t.workspaceId} ${q.id}: ${(e as Error).message}`); }
      const exact = JSON.stringify(hitIds(got).slice(0, 50)) === JSON.stringify(want.page) && got.length > 50 === want.total > 50;
      if (!exact) failures.push(`${arm} ${t.workspaceId} ${q.id}: result differs from oracle`);
      if (got.some((h) => h.org_id !== t.orgId || h.workspace_id !== t.workspaceId)) failures.push(`${arm} ${q.id}: cross-tenant row`);
      results.push({
        tenant: t.workspaceId, query: q.id, oracleTotal: want.total, exact,
        latency: await sample((c) => directorySearch(c, t, q.term), db),
        plan: await explain(db, directoryPlanSql, [t.orgId, t.workspaceId, likeTerm(q.term)]),
      });
    }
  }
  return results;
}

async function laneC(db: Db) {
  const rlsRun = async (c: Db, t: Tenant, sql: string, args: unknown[]) => {
    await c.query("SELECT set_config('request.jwt.claims', $1, false)", [JSON.stringify({ role: "authenticated", app_metadata: { org_id: t.orgId, workspace_ids: [t.workspaceId] } })]);
    await c.exec("SET ROLE authenticated");
    try { return await c.query(sql, args); } finally { await c.exec("RESET ROLE"); }
  };
  const results: Record<string, unknown>[] = [];
  for (const t of tenants) {
    for (const q of listQueries(t)) {
      const plan = await (async () => {
        await db.query("SELECT set_config('request.jwt.claims', $1, false)", [JSON.stringify({ role: "authenticated", app_metadata: { org_id: t.orgId, workspace_ids: [t.workspaceId] } })]);
        await db.exec("SET ROLE authenticated");
        try { return await explain(db, q.sql, q.args); } finally { await db.exec("RESET ROLE"); }
      })();
      const agentGate = q.agent ? (plan.rowAmplification ?? 0) <= 100 : null;
      results.push({ tenant: t.workspaceId, query: q.id, agentGate, latency: await sample((c) => rlsRun(c, t, q.sql, q.args), db), plan });
    }
  }
  return results;
}

async function neonLanes(db: Db, kept: CorpusSession[]) {
  await db.exec("ANALYZE neon_baseline.convergence_events; ANALYZE neon_baseline.session_brief_search");
  const results: Record<string, unknown>[] = [];
  for (const t of tenants) {
    for (const q of textQueries(t)) {
      const brief = q.lane === "A";
      const hits = brief ? await neonBriefSearch(db, t, q.term) : await neonEventSearch(db, t, q.term);
      if (hits.length && brief && (await db.query<{ n: number }>(
        "SELECT count(*)::int AS n FROM neon_baseline.session_brief_search WHERE workspace_id <> $1 AND session_id = ANY($2)",
        [t.workspaceId, hits.map((h) => h.session_id)])).rows[0]!.n > 0) failures.push(`neon ${q.id}: cross-tenant row`);
      const wanted = brief ? new Set(oracleLaneA(kept, t, q.term).page) : oracleLaneB(kept, t, q.term);
      const got = new Set(hitIds(hits));
      const found = [...wanted].filter((w) => got.has(w)).length;
      // Lane B returns one 21-row page, so more matches than that is not a miss.
      const reachable = brief ? wanted.size : Math.min(wanted.size, 21);
      results.push({
        lane: q.lane, tenant: t.workspaceId, query: q.id, oracle: wanted.size, returned: got.size,
        recall: reachable ? Math.round((found / reachable) * 1000) / 1000 : null,
        latency: await sample((c) => (brief ? neonBriefSearch(c, t, q.term) : neonEventSearch(c, t, q.term)), db),
        plan: await explain(db, brief ? NEON_BRIEF_SQL : NEON_EVENTS_SQL, [t.orgId, t.workspaceId, `%${q.term}%`]),
      });
    }
  }
  return results;
}

/** Writer call to first RPC hit, through the real short-summary writer. Database only. */
async function freshness(db: Db, kept: CorpusSession[]) {
  const live = kept.filter((s) => s.workspaceId === WHALE.workspaceId && !s.deleted).slice(0, 20);
  const lags: number[] = [];
  for (const [i, s] of live.entries()) {
    const term = `freshterm${String.fromCharCode(97 + i)}${scale.toLowerCase()}`;
    const start = now();
    await writeShortSummary(db as never, {
      orgId: s.orgId, workspaceId: s.workspaceId, harness: s.harness, sessionId: s.sessionId,
      summary: `benchmark freshness ${term}`, source: "benchmark/synthetic/v1", generatedAt: new Date(),
    });
    while ((await directorySearch(db, WHALE, term)).length === 0) {
      if (now() - start > 10_000) { failures.push(`freshness: ${term} not searchable in 10 s`); break; }
    }
    lags.push(now() - start);
  }
  return summarize(lags);
}

async function throughput(concurrency: number, seconds: number) {
  if (!pgUrl) return null;
  const terms = [TERMS.common, TERMS.selective, TERMS.phrase, tenantCanary(WHALE.orgId, WHALE.workspaceId)];
  const clients = await Promise.all(Array.from({ length: concurrency }, connect));
  const stop = now() + seconds * 1000;
  let ok = 0, errors = 0;
  await Promise.all(clients.map(async (c, i) => {
    for (let n = i; now() < stop; n++) {
      try { await directorySearch(c.db, WHALE, terms[n % terms.length]!); ok++; } catch { errors++; }
    }
  }));
  await Promise.all(clients.map((c) => c.close()));
  return { concurrency, seconds, queriesPerSecond: Math.round(ok / seconds), errors };
}

async function sizes(db: Db) {
  const { rows } = await db.query(
    `SELECT c.relname AS relation, pg_total_relation_size(c.oid)::bigint AS total_bytes, pg_relation_size(c.oid)::bigint AS main_bytes
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r', 'i') AND n.nspname IN ('sessions', 'neon_baseline')
        AND (c.relname LIKE 'session_directory%' OR c.relname LIKE 'convergence_events%' OR c.relname LIKE 'session_brief_search%')
      ORDER BY 2 DESC`);
  return rows;
}

const db = await open();
await installSchema(db);
const kept: CorpusSession[] = [];
const version = (await db.query<{ v: string }>("SHOW server_version")).rows[0] as unknown as { server_version: string };

// Arm 2b is what #48 specifies; 2a is the same directory with no summary text.
const load2b = await loadDirectory(db, true, kept, true);
const report = {
  scale, params, engine: pgUrl ? `postgres ${version.server_version}` : "pglite",
  warning: "Relative numbers from a local stand-in. Not Neon, not Supabase, not an e2e proof.",
  baseline: await neonLanes(db, kept),
  arm2b: {
    loadMsIncludingBaseline: Math.round(load2b), laneA: await laneA(db, kept, true, "2b"), laneC: await laneC(db),
    laneB: "UNSUPPORTED by contract: docs/session-search-index-contract.md",
    sizes: await sizes(db),
    throughput: await throughput(8, 10),
    freshnessDatabaseOnly: await freshness(db, kept),
    tenantIndexReindexMs: (await timed(() => db.exec("REINDEX INDEX sessions.session_directory_tenant_search_trgm_idx"))).ms,
  },
  arm2a: {} as Record<string, unknown>,
  notMeasured: [
    "cache-cold latency (server restart between samples)",
    "landed-to-searchable freshness through the pipeline (needs a proof stack)",
    "failure recovery and replay from manifests",
    "Neon filter-only listing (session_rollups is not in the stand-in)",
    "ClickHouse/ObsessionDB in this report; see clickhouse-gates.ts for vanilla ClickHouse",
    "dollar cost (sizes are the only resource input)",
    ...(pgUrl ? [] : ["throughput (PGlite has one connection)"]),
  ],
  failures,
};
await db.exec("TRUNCATE sessions.session_directory CASCADE");
const load2a = await loadDirectory(db, false);
report.arm2a = { loadMs: Math.round(load2a), laneA: await laneA(db, kept, false, "2a"), sizes: await sizes(db) };

mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(report, null, 2));
const row = (r: Record<string, any>) => `${r.tenant} ${r.query}${r.lane ? ` [${r.lane}]` : ""}: cold p50 ${r.latency.cold.p50} warm p50/95/99 ${r.latency.warm.p50}/${r.latency.warm.p95}/${r.latency.warm.p99} ms errors ${r.latency.errorCount}${r.recall === undefined ? "" : ` recall ${r.recall}`} index ${r.plan.indexes.join(",") || "-"}${r.plan.seqScan ? " SEQSCAN" : ""}`;
console.log(`engine ${report.engine}, ${params.sessions} sessions, report ${out}`);
for (const [name, rows] of [["baseline", report.baseline], ["2b Lane A", report.arm2b.laneA], ["2b Lane C", report.arm2b.laneC], ["2a Lane A", (report.arm2a as any).laneA]] as const) {
  console.log(`\n${name}`);
  for (const r of rows as Record<string, any>[]) console.log(row(r));
}
console.log(`\nthroughput ${JSON.stringify(report.arm2b.throughput)}\nfreshness (database only) ${JSON.stringify(report.arm2b.freshnessDatabaseOnly)}`);
if (failures.length) {
  console.error(`\nFAILED\n${failures.join("\n")}`);
  process.exit(1);
}
process.exit(0);
