#!/usr/bin/env bun
/**
 * Within-run comparison on one runner and one corpus: recent-first time windows, then
 * index granularity and sort order. Windows are disjoint and newest first, so a page is
 * the same page a single query returns. Every variant is scored on the generator oracle.
 * Ages are uniform over the window, so recency gains here are not production gains.
 *
 *   BENCH_COMPARE_S  seconds per level (default 6)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { SCALES } from "../corpus.ts";
import { percentile } from "../stats.ts";
import { applyShape } from "./shape.ts";
import { ClickHouseHttp, DEFAULT_GRANULARITY, installSchema, schemaSql, SessionSearchStore } from "./store.ts";
import { NEEDLES, Oracle, oracleKey, transcriptBatches } from "./transcripts.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
const pw = "synthetic-proof-only";
const base = SCALES[process.env.BENCH_SCALE ?? "M"]!;
const shape = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) : null;
const params = shape ? applyShape(base, shape) : base;
const seconds = Number(process.env.BENCH_COMPARE_S ?? 6);
const admin = new ClickHouseHttp(url, process.env.BENCH_CH_USER ?? "default", process.env.BENCH_CH_PASSWORD ?? "");
const reader = new ClickHouseHttp(url, "search_reader", pw);
const store = new SessionSearchStore(reader, new ClickHouseHttp(url, "search_writer", pw));
const ws = `['ws-0-0','ws-0-1','ws-0-2']`;
const LIMIT = 10;
const DAY = 86_400_000;
const now = Date.parse(params.now);
const WINDOW_DAYS = [1, 7, 30];

type Hit = { event_id: string };
async function page(table: string, q: string, tag: string, range?: [number | null, number | null]) {
  const filters = ["org_id = 'org-0'", `workspace_id IN ${ws}`, "positionCaseInsensitiveUTF8(text, {q:String}) > 0"];
  if ([...q].length >= 3) filters.push("hasAllTokens(text, {q:String})");
  if (range?.[0] != null) filters.push(`ts >= fromUnixTimestamp64Milli(toInt64(${range[0]}))`);
  if (range?.[1] != null) filters.push(`ts < fromUnixTimestamp64Milli(toInt64(${range[1]}))`);
  return reader.rows<Hit>(`SELECT event_id, toString(ts) AS at,
      substringUTF8(text, greatest(positionCaseInsensitiveUTF8(text, {q:String}) - 80, 1), 320) AS snippet
    FROM ${table} WHERE ${filters.join(" AND ")} ORDER BY ts DESC, event_id DESC LIMIT ${LIMIT + 1}`,
  { q }, { SQL_relay_org: "org-0", max_execution_time: 5, log_comment: tag });
}
const single = (table: string) => async (q: string, tag: string) => ({ rows: await page(table, q, tag), rounds: 1 });
const windowedOn = (table: string) => async (q: string, tag: string) => {
  const edges = [null, ...WINDOW_DAYS.map((d) => now - d * DAY)];
  const ranges: Array<[number | null, number | null]> = WINDOW_DAYS.map((_, i) => [edges[i + 1]!, edges[i] ?? null]);
  ranges.push([null, now - WINDOW_DAYS.at(-1)! * DAY]);
  const rows: Hit[] = [];
  let rounds = 0;
  for (const r of ranges) {
    rounds++;
    rows.push(...await page(table, q, tag, r));
    if (rows.length > LIMIT) break;
  }
  return { rows, rounds };
};

await installSchema(admin, pw);
const oracle = new Oracle();
let loaded = 0;
for (const batch of transcriptBatches(params, oracle)) { await store.insert(batch); loaded += batch.length; }
const copies: Record<string, string> = {
  base: schemaSql(undefined, DEFAULT_GRANULARITY).replace("session_search_records", "t_base"),
  gran_256: schemaSql().replace("session_search_records", "t_gran_256"),
};
const tableOf = (name: string) => `t_${name}`;
for (const [name, ddl] of Object.entries(copies)) {
  const t = tableOf(name);
  await admin.run(`DROP TABLE IF EXISTS ${t} SYNC`);
  await admin.run(ddl);
  await admin.run(`INSERT INTO ${t} SELECT * FROM session_search_records`, {}, { max_execution_time: 900 });
  await admin.run(`GRANT SELECT ON ${t} TO search_reader`);
  await admin.run(`DROP ROW POLICY IF EXISTS p_${name} ON ${t}`);
  await admin.run(`CREATE ROW POLICY p_${name} ON ${t} USING org_id = getSetting('SQL_relay_org') TO search_reader`);
}
await admin.run("OPTIMIZE TABLE t_base FINAL", {}, { max_execution_time: 900 });
await admin.run("OPTIMIZE TABLE t_gran_256 FINAL", {}, { max_execution_time: 900 });
await Bun.sleep(30_000);
const sizes = await admin.rows("SELECT table, count() AS parts, sum(rows) AS rows, sum(data_compressed_bytes) AS data, sum(secondary_indices_compressed_bytes) AS indexes FROM system.parts WHERE active AND (table = 'session_search_records' OR table LIKE 't\\_%') GROUP BY table");

const VARIANTS: Record<string, (q: string, tag: string) => Promise<{ rows: Hit[]; rounds: number }>> = {
  baseline: single(tableOf("base")),
  windowed: windowedOn(tableOf("base")),
  gran_256: single(tableOf("gran_256")),
  windowed_gran_256: windowedOn(tableOf("gran_256")),
};
const CLASSES = ["ultra", "absent", "pr", "phrase", "short", "common"];

async function counters(tag: string) {
  await admin.run("SYSTEM FLUSH LOGS");
  const [c] = await admin.rows<Record<string, number>>(`SELECT count() AS queries, sum(read_bytes) AS bytes, sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms
    FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = {tag:String}`, { tag });
  return c;
}

const results: unknown[] = [];
for (const id of CLASSES) {
  const n = NEEDLES.find((x) => x.id === id)!;
  const truth = oracle.newest(oracleKey("org-0", id)).map((h) => h.eventId);
  for (const [name, run] of Object.entries(VARIANTS)) {
    const got = (await run(n.query, `cmp:${id}:${name}:recall`)).rows.slice(0, LIMIT).map((r) => r.event_id);
    const recall = truth.length ? truth.slice(0, LIMIT).filter((e, i) => got[i] === e).length / Math.min(LIMIT, truth.length) : got.length === 0 ? 1 : 0;
    for (const clients of [1, 8, 32]) {
      const tag = `cmp:${id}:${name}:${clients}`;
      const lat: number[] = [];
      const errors: Record<string, number> = {};
      let stop = false, rounds = 0;
      const started = performance.now();
      const worker = async () => {
        while (!stop) {
          const s = performance.now();
          try { const r = await run(n.query, tag); rounds += r.rounds; lat.push(performance.now() - s); }
          catch (e) { const c = /Code: (\d+)|clickhouse (\d+)/.exec(String(e)); const k = c?.[1] ?? c?.[2] ?? "other"; errors[k] = (errors[k] ?? 0) + 1; }
        }
      };
      const running = Array.from({ length: clients }, worker);
      await Bun.sleep(seconds * 1000);
      stop = true;
      await Promise.all(running);
      const qps = lat.length / ((performance.now() - started) / 1000);
      lat.sort((a, b) => a - b);
      const c = await counters(tag);
      const row = {
        class: id, variant: name, clients, recallOrdered: recall, qps: Math.round(qps * 10) / 10, qpsPerCore: Math.round(qps / cpus().length * 10) / 10,
        p50: Math.round(percentile(lat, 50) ?? 0), p95: Math.round(percentile(lat, 95) ?? 0), p99: Math.round(percentile(lat, 99) ?? 0), errors,
        roundsPerSearch: Math.round(rounds / Math.max(1, lat.length) * 10) / 10,
        mbPerSearch: Math.round((c?.bytes ?? 0) / 1e6 / Math.max(1, lat.length)), cpuMsPerSearch: Math.round((c?.cpu_ms ?? 0) / Math.max(1, lat.length)),
      };
      results.push(row);
      console.log(JSON.stringify(row));
    }
  }
}
writeFileSync(process.env.BENCH_OUT ?? "compare-report.json", JSON.stringify({
  shapeFidelity: "PARTIAL: sanitized aggregate anchors only, not a fresh production-derived shape artifact. Not production-scale parity; cannot close T0.",
  verdict: "INCOMPLETE/HOLD", runner: { cores: cpus().length }, loaded, sizes, windowDays: WINDOW_DAYS, results,
}, null, 2));
process.exit(0);
