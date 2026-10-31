#!/usr/bin/env bun
/**
 * Same-run A/B of layout and search strategy, each with and without concurrent ingest, in
 * two passes so runner drift shows. Answers why mixed load is slower than the sweep, why
 * freshness moved, and whether a probe repairs terms with few or no hits.
 *
 * probe: count candidates from the index first, then use one query when they are few.
 *
 *   BENCH_AB_S  seconds per cell (default 15)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { SCALES } from "../corpus.ts";
import { percentile } from "../stats.ts";
import { applyShape } from "./shape.ts";
import { ClickHouseHttp, installSchema, schemaSql, SessionSearchStore } from "./store.ts";
import { NEEDLES, Oracle, oracleKey, transcriptBatches } from "./transcripts.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
const pw = "synthetic-proof-only";
const base = SCALES[process.env.BENCH_SCALE ?? "M"]!;
const shape = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) : null;
const params = shape ? applyShape(base, shape) : base;
const seconds = Number(process.env.BENCH_AB_S ?? 15);
const admin = new ClickHouseHttp(url, process.env.BENCH_CH_USER ?? "default", process.env.BENCH_CH_PASSWORD ?? "");
const reader = new ClickHouseHttp(url, "search_reader", pw);
const store = new SessionSearchStore(reader, new ClickHouseHttp(url, "search_writer", pw));
const LIMIT = 10, DAY = 86_400_000, PROBE_MAX = 200;
const ORGS = [0, 1].map((o) => ({ org: `org-${o}`, ws: `['ws-${o}-0','ws-${o}-1','ws-${o}-2']` }));
type Org = (typeof ORGS)[number];
const scan = "positionCaseInsensitiveUTF8(text, {q:String}) > 0";
const settings = (org: string, tag: string) => ({ SQL_relay_org: org, max_execution_time: 5, log_comment: tag });

async function page(table: string, o: Org, q: string, tag: string, range?: [number | null, number | null]) {
  const f = [`org_id = '${o.org}'`, `workspace_id IN ${o.ws}`, scan];
  if ([...q].length >= 3) f.push("hasAllTokens(text, {q:String})");
  if (range?.[0] != null) f.push(`ts >= fromUnixTimestamp64Milli(toInt64(${range[0]}))`);
  if (range?.[1] != null) f.push(`ts < fromUnixTimestamp64Milli(toInt64(${range[1]}))`);
  return reader.rows<{ event_id: string }>(`SELECT event_id, substringUTF8(text, greatest(positionCaseInsensitiveUTF8(text, {q:String}) - 80, 1), 320) AS snippet
    FROM ${table} WHERE ${f.join(" AND ")} ORDER BY ts DESC, event_id DESC LIMIT ${LIMIT + 1}`, { q }, settings(o.org, tag));
}
async function windowed(table: string, o: Org, q: string, tag: string) {
  const now = Date.now();
  const e = [1, 7, 30].map((d) => now - d * DAY);
  const ranges: Array<[number | null, number | null]> = [[e[0]!, null], [e[1]!, e[0]!], [e[2]!, e[1]!], [null, e[2]!]];
  const rows: unknown[] = [];
  for (const r of ranges) { rows.push(...await page(table, o, q, tag, r)); if (rows.length > LIMIT) break; }
  return rows;
}
async function probed(table: string, o: Org, q: string, tag: string) {
  if ([...q].length < 3) return windowed(table, o, q, tag);
  const n = Number((await reader.run(`SELECT count() FROM ${table} WHERE org_id = '${o.org}' AND workspace_id IN ${o.ws} AND hasAllTokens(text, {q:String})`, { q }, { ...settings(o.org, tag), query_plan_direct_read_from_text_index: 1, query_plan_optimize_count_from_text_index: 1 })).trim());
  return n <= PROBE_MAX ? page(table, o, q, tag) : windowed(table, o, q, tag);
}
const VARIANTS = {
  single_8192: { table: "t_8192", run: page },
  windowed_8192: { table: "t_8192", run: windowed },
  windowed_256: { table: "t_256", run: windowed },
  probed_256: { table: "t_256", run: probed },
} as const;

await installSchema(admin, pw);
const oracle = new Oracle();
let loaded = 0;
for (const batch of transcriptBatches(params, oracle)) { await store.insert(batch); loaded += batch.length; }
const tables = {
  t_8192: schemaSql().replace("session_search_records", "t_8192").replace("index_granularity = 256", "index_granularity = 8192"),
  t_256: schemaSql().replace("session_search_records", "t_256"),
};
for (const [t, ddl] of Object.entries(tables)) {
  await admin.run(`DROP TABLE IF EXISTS ${t} SYNC`);
  await admin.run(ddl);
  const s = performance.now();
  await admin.run(`INSERT INTO ${t} SELECT * FROM session_search_records`, {}, { max_execution_time: 900 });
  console.log(t, "bulk insert ms", Math.round(performance.now() - s));
  await admin.run(`GRANT SELECT ON ${t} TO search_reader`);
  await admin.run(`DROP ROW POLICY IF EXISTS p_${t} ON ${t}`);
  await admin.run(`CREATE ROW POLICY p_${t} ON ${t} USING org_id = getSetting('SQL_relay_org') TO search_reader`);
}
await Bun.sleep(20_000);

const parts = async (t: string) => Number((await admin.run(`SELECT count() FROM system.parts WHERE table = '${t}' AND active`)).trim());
const mix = NEEDLES.flatMap((n) => ORGS.map((o) => ({ n, o })));
const pct = (xs: number[]) => { const s = [...xs].sort((a, b) => a - b); return { n: s.length, p50: Math.round(percentile(s, 50) ?? 0), p95: Math.round(percentile(s, 95) ?? 0), p99: Math.round(percentile(s, 99) ?? 0) }; };

async function cell(name: keyof typeof VARIANTS, clients: number, ingest: boolean, pass: number) {
  const { table, run } = VARIANTS[name];
  const tag = `ab:${name}:${clients}:${ingest}:${pass}`;
  const byClass: Record<string, number[]> = {};
  const all: number[] = [], fresh: number[] = [], insertMs: number[] = [];
  const errors: Record<string, number> = {};
  let stop = false;
  const worker = async (id: number) => {
    for (let i = id; !stop; i++) {
      const m = mix[i % mix.length]!;
      const s = performance.now();
      try {
        await run(table, m.o, m.n.query, tag);
        const d = performance.now() - s;
        all.push(d); (byClass[m.n.id] ??= []).push(d);
      } catch (e) { const c = /Code: (\d+)|clickhouse (\d+)/.exec(String(e)); const k = c?.[1] ?? c?.[2] ?? "other"; errors[k] = (errors[k] ?? 0) + 1; }
    }
  };
  const writer = async () => {
    for (let i = 0; !stop; i++) {
      const term = `abfresh${pass}x${clients}x${i}x${name.length}${ingest ? 1 : 0}`;
      const row = JSON.stringify({ org_id: "org-0", workspace_id: "ws-0-0", harness: "claude", session_id: `s-${term}`, machine_id: "m1", event_id: term, revision: 1, chunk_seq: 0, record_seq: 0, ts: new Date().toISOString().replace("T", " ").replace("Z", ""), kind: "message", scrub_version: 1, text: term });
      const s = performance.now();
      await admin.run(`INSERT INTO ${table} FORMAT JSONEachRow`, {}, { async_insert: 1, wait_for_async_insert: 1, async_insert_busy_timeout_max_ms: 200 }, row);
      insertMs.push(performance.now() - s);
      while (!stop && !(await page(table, ORGS[0]!, term, `${tag}:fresh`, [Date.now() - DAY, null])).length) await Bun.sleep(5);
      fresh.push(performance.now() - s);
      await Bun.sleep(200);
    }
  };
  const started = performance.now();
  const running = [...Array.from({ length: clients }, (_, i) => worker(i)), ...(ingest ? [writer()] : [])];
  await Bun.sleep(seconds * 1000);
  stop = true;
  await Promise.all(running);
  const qps = all.length / ((performance.now() - started) / 1000);
  await admin.run("SYSTEM FLUSH LOGS");
  const [c] = await admin.rows<Record<string, number>>(`SELECT count() AS queries, sum(read_bytes) AS bytes, sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = {tag:String}`, { tag });
  const row = {
    variant: name, clients, ingest, pass, qps: Math.round(qps * 10) / 10, qpsPerCore: Math.round(qps / cpus().length * 10) / 10, ...pct(all), errors,
    mbPerSearch: Math.round((c?.bytes ?? 0) / 1e6 / Math.max(1, all.length)), cpuMsPerSearch: Math.round((c?.cpu ?? 0) / Math.max(1, all.length)),
    parts: await parts(table), insertMs: pct(insertMs), freshnessMs: pct(fresh),
    classes: Object.fromEntries(Object.entries(byClass).map(([k, v]) => [k, pct(v).p95])),
  };
  console.log(JSON.stringify({ ...row, classes: undefined }));
  return row;
}

const results = [];
for (const pass of [1, 2]) for (const clients of [8, 32]) for (const ingest of [false, true]) for (const name of Object.keys(VARIANTS) as Array<keyof typeof VARIANTS>)
  results.push(await cell(name, clients, ingest, pass));

// Recall for the probe path, against the oracle, whale org.
const recall: Record<string, number> = {};
for (const n of NEEDLES) {
  const truth = oracle.newest(oracleKey("org-0", n.id)).map((h) => h.eventId).slice(0, LIMIT);
  const got = (await probed("t_256", ORGS[0]!, n.query, "ab:recall") as Array<{ event_id: string }>).slice(0, LIMIT).map((r) => r.event_id);
  recall[n.id] = truth.length ? truth.filter((e, i) => got[i] === e).length / truth.length : got.length ? 0 : 1;
}
writeFileSync(process.env.BENCH_OUT ?? "ab-report.json", JSON.stringify({
  shapeFidelity: "PARTIAL: sanitized aggregate anchors only, not a fresh production-derived shape artifact. Not production-scale parity; cannot close T0.",
  verdict: "INCOMPLETE/HOLD", runner: { cores: cpus().length }, loaded, recall, results,
}, null, 2));
process.exit(0);
