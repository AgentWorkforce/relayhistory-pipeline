#!/usr/bin/env bun
/**
 * Saturation diagnosis: each query class alone at 1, 2, 4, 8, 16 and 32 clients, with the
 * server's own counters. No retries: a failed search is counted by its error code, the
 * class stops at the first level with errors, and the run exits 1. Same synthetic corpus
 * and shape limits as proof.ts; timings are relative.
 *
 *   BENCH_DIAG_S  seconds per level (default 8)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { SCALES } from "../corpus.ts";
import { percentile } from "../stats.ts";
import { applyShape } from "./shape.ts";
import { ClickHouseHttp, installSchema, SessionSearchStore, type Scope } from "./store.ts";
import { NEEDLES, Oracle, transcriptBatches } from "./transcripts.ts";

const url = process.env.BENCH_CH_URL ?? "http://127.0.0.1:8123";
const pw = "synthetic-proof-only";
const base = SCALES[process.env.BENCH_SCALE ?? "M"]!;
const shape = process.env.BENCH_SHAPE_FILE ? JSON.parse(readFileSync(process.env.BENCH_SHAPE_FILE, "utf8")) : null;
const params = shape ? applyShape(base, shape) : base;
const seconds = Number(process.env.BENCH_DIAG_S ?? 8);
const out = process.env.BENCH_OUT ?? "diagnose-report.json";
const admin = new ClickHouseHttp(url, process.env.BENCH_CH_USER ?? "default", process.env.BENCH_CH_PASSWORD ?? "");
const search = new SessionSearchStore(new ClickHouseHttp(url, "search_reader", pw), new ClickHouseHttp(url, "search_writer", pw));
const whale: Scope = { orgId: "org-0", workspaceIds: Array.from({ length: params.workspacesPerOrg }, (_, w) => `ws-0-${w}`) };

const pct = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const p = (q: number) => Math.round((percentile(s, q) ?? 0) * 10) / 10;
  return { n: s.length, p50: p(50), p95: p(95), p99: p(99) };
};
const errorCode = (e: unknown) => /Code: (\d+)/.exec(String(e))?.[1] ?? String(e).slice(0, 60);

async function serverCounters(tag: string) {
  await admin.run("SYSTEM FLUSH LOGS");
  const [row] = await admin.rows<Record<string, number>>(`
    SELECT count() AS queries, avg(read_rows) AS read_rows, avg(read_bytes) AS read_bytes, max(peak_threads_usage) AS peak_threads,
      max(memory_usage) AS peak_memory, avg(ProfileEvents['SelectedParts']) AS parts, avg(ProfileEvents['SelectedMarks']) AS marks,
      avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms, avg(ProfileEvents['DiskReadElapsedMicroseconds']) / 1000 AS disk_ms
    FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = {tag:String}`, { tag });
  return row;
}

async function level(needle: (typeof NEEDLES)[number], clients: number) {
  const tag = `diag:${needle.id}:${clients}`;
  const lat: number[] = [];
  const errors: Record<string, number> = {};
  const work = { candidates: 0, falseCandidates: 0, rounds: 0, searches: 0 };
  let stop = false;
  const started = performance.now();
  const worker = async () => {
    while (!stop) {
      const s = performance.now();
      try {
        const { stats } = await search.search(whale, { query: needle.query, tag });
        lat.push(performance.now() - s);
        work.candidates += stats.candidates; work.falseCandidates += stats.falseCandidates; work.rounds += stats.rounds; work.searches++;
      }
      catch (e) { const c = errorCode(e); errors[c] = (errors[c] ?? 0) + 1; }
    }
  };
  const run = Array.from({ length: clients }, worker);
  await Bun.sleep(seconds * 1000);
  stop = true;
  await Promise.all(run);
  const elapsed = (performance.now() - started) / 1000;
  const qps = Math.round(lat.length / elapsed * 10) / 10;
  const per = (n: number) => Math.round(n / Math.max(1, work.searches) * 10) / 10;
  return {
    clients, qps, qpsPerCore: Math.round(qps / cpus().length * 10) / 10, errors, ...pct(lat), server: await serverCounters(tag),
    perSearch: { candidates: per(work.candidates), falseCandidates: per(work.falseCandidates), rounds: per(work.rounds) },
  };
}

await installSchema(admin, pw);
const oracle = new Oracle();
let loaded = 0;
for (const batch of transcriptBatches(params, oracle)) { await search.insert(batch); loaded += batch.length; }
const cores = await admin.rows("SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('NumberOfCPUCores', 'OSMemoryTotal')");
const classes = [];
let failed = false;
for (const n of NEEDLES) {
  const plan = await admin.run(`EXPLAIN indexes = 1 SELECT event_id FROM session_search_records WHERE org_id = 'org-0' AND positionCaseInsensitiveUTF8(text, {q:String}) > 0${[...n.query].length >= 3 ? " AND hasAllTokens(text, {q:String})" : ""} ORDER BY ts DESC LIMIT 11`, { q: n.query });
  await admin.run("SYSTEM DROP MARK CACHE"); await admin.run("SYSTEM DROP UNCOMPRESSED CACHE");
  const t0 = performance.now();
  await search.search(whale, { query: n.query });
  const coldMs = Math.round(performance.now() - t0);
  const t1 = performance.now();
  await search.search(whale, { query: n.query });
  const warmMs = Math.round(performance.now() - t1);
  const levels = [];
  for (const clients of [1, 2, 4, 8, 16, 32]) {
    const l = await level(n, clients);
    levels.push(l);
    console.log(n.id, JSON.stringify({ clients, qps: l.qps, p95: l.p95, errors: l.errors, read_rows: l.server?.read_rows, cpu_ms: l.server?.cpu_ms }));
    if (Object.keys(l.errors).length) { failed = true; break; }
  }
  classes.push({ id: n.id, cls: n.cls, query: n.query, truthWhale: oracle.counts.get(`org-0|${n.id}`) ?? 0, coldMs, warmMs, plan, levels });
}
const report = {
  shapeFidelity: "PARTIAL: sanitized aggregate anchors only, not a fresh production-derived shape artifact. Not production-scale parity; cannot close T0.",
  loaded, cores, runnerCores: cpus().length, activeParts: (await admin.run("SELECT count() FROM system.parts WHERE table = 'session_search_records' AND active")).trim(), classes,
};
writeFileSync(out, JSON.stringify(report, null, 2));
if (failed) { console.error("failed: a class recorded search errors; see report"); process.exit(1); }
process.exit(0);
