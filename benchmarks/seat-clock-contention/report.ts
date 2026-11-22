/**
 * Renders results/<runId>/summary.json (from bench.ts) as markdown tables. Usage: bun benchmarks/seat-clock-contention/report.ts <runId>
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

const runId = process.argv[2];
if (!runId) throw Error("usage: report.ts <runId>");
const { meta, rows } = JSON.parse(readFileSync(join(import.meta.dir, "results", runId, "summary.json"), "utf8")) as { meta: any; rows: any[] };
const f = (x: number | null | undefined, d = 0) => (x === null || x === undefined ? "-" : x.toFixed(d));
const d3 = (x: any) => (x && x.n ? `${f(x.p50)} / ${f(x.p95)} / ${f(x.p99)} / ${f(x.max)}` : "-");
const cell = (r: any) => `${r.cell.workload}${r.cell.label ? ` (${r.cell.label}${r.bufferLockBusy ? ", busy path on" : ""})` : ""} | ${r.cell.n} | ${r.cell.c}`;
const fails = (r: any) => Object.entries(r.stageFailures as Record<string, number>).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k} x${v}`).join(", ") || "none";
const out: string[] = [];
out.push(`## ${runId}`, "", `PostgreSQL: ${meta.serverVersion.split(" on ")[0]}; history switch live: ${meta.template.historyLive}; origin claim fence: ${meta.plan.originClaimFence}; records per run: ${meta.plan.totalRecords}; control every ${meta.plan.controlEveryMs} ms; loopback RTT p50 ${meta.rtt.p50} ms.`, "");
out.push("Tables only. Definitions, the qualified records/s reading (250 vs 500 is about equal only where retries are rare; the retry-heavy C64 cells differ), lock evidence and evidence gaps: ../comparison-runs-1-3.md.", "");
out.push("### Throughput and failures", "", "| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |", "|---|---|---|---|---|---|---|---|---|");
for (const r of rows) out.push(`| ${cell(r)} | ${r.drained ? "yes" : `no (${r.undrainedBatches} left)`} | ${f(r.drainSeconds, 1)} | ${r.durableRecordsPerSec} | ${f(r.commitsPerSec, 2)} | ${f(r.retriesPerBatch, 2)} | ${fails(r)} |`);
out.push("", "### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)", "",
  "Sampled: hold start and wait come from the lock sampler (period p50 per cell in the last table, about 0.6-2.8 ms); hold end is the COMMIT completion. Coverage = observed holds (commit transactions of any outcome, incl. ones that later failed 57014) / committed commits.", "", "| workload | batch | C | hold | wait for clock | commit stage latency (ok) | observed holds / committed | sample period p50 |", "|---|---|---|---|---|---|---|---|");
for (const r of rows) out.push(`| ${cell(r)} | ${d3(r.clockHoldMs.commit)} | ${d3(r.clockWaitMs.commit)} | ${d3(r.stageLatencyMs.commit)} | ${r.clockHoldMs.commit?.n ?? 0}/${r.stageLatencyMs.commit.n} | ${r.sampler.periodMs.p50} ms |`);
out.push("", "### Controls during load (fixed rate, one at a time; latency and hold in ms)", "",
  "C1 (diagnostic) = sessions.ingest_lazy_clear(reset) as owner, the clock FOR UPDATE step alone. C2 (acceptance) = ledger.applyOrderedControl -> ingest_set_session_uploads -> ingest_apply_ordered_control, runtime role, 2 s / 5 s. One control kind per run.",
  "Completed during load = finished before the last load worker stopped; after load end = finished only once uploads had stopped (drain-only).", "",
  "| workload | batch | C | control | slots scheduled | missed slots | started | completed during load | completed after load end | failed (SQLSTATE) | latency p50 / p95 / p99 / max | clock hold p50 / max |", "|---|---|---|---|---|---|---|---|---|---|---|---|");
for (const r of rows) {
  const cs: Record<string, any> = r.controls.attempted !== undefined ? { control: { ...r.controls } } : r.controls;
  for (const [st, c] of Object.entries(cs)) {
    if (!c.attempted) continue;
    const h = r.clockHoldMs[st];
    out.push(`| ${cell(r)} | ${st === "control" ? "C1" : "C2"} | ${c.scheduledSlots ?? c.attempted + c.missedSlots} | ${c.missedSlots} | ${c.attempted} | ${c.completedDuringLoad ?? c.completed} | ${c.completedAfterLoadEnd ?? "-"} | ${JSON.stringify(c.failed)} | ${d3(c.latencyMs)} | ${h ? `${f(h.p50, 1)} / ${f(h.max, 1)}` : "-"} |`);
  }
}
if (rows.some((r) => r.callTimingMs)) {
  out.push("", "### Call timing, measured per call (client clock; ms p50 / p95 / max)", "",
    "Whole-call age = BEGIN sent -> COMMIT returned. Statement = the stage function's SELECT, including every lock wait inside it. COMMIT response = COMMIT sent -> returned. Lock-wait time (the sections above and below) is sampled; these are measured.", "",
    "| workload | batch | C | stage | n | whole call | statement | COMMIT response |", "|---|---|---|---|---|---|---|---|");
  const d2 = (x: any) => (x && x.n ? `${f(x.p50, 1)} / ${f(x.p95, 1)} / ${f(x.max, 1)}` : "-");
  for (const r of rows) for (const [st, v] of Object.entries((r.callTimingMs ?? {}) as Record<string, any>)) out.push(`| ${cell(r)} | ${st} | ${v.wholeCall.n} | ${d2(v.wholeCall)} | ${d2(v.statement)} | ${d2(v.commitResponse)} |`);
  out.push("", "### Tuples and wrapper-lock evidence", "",
    "Tuples = distinct (org/workspace/user/origin) of the load streams. Held = transactions seen holding each granted advisory lock in at least one sample, out of the stage's transactions (sampled, so short holds can be missed). Queued commits = samples with two or more commits queued on the clock: how many origin-claim keys they held, and in how many samples two of them held the same key.", "");
  for (const r of rows) {
    if (!r.tuples) continue;
    const t = r.tuples, q = r.commitsQueuedOnClock, a = r.advisoryHeldByStage;
    out.push(`**${cell(r).replaceAll(" | ", " ")}**: ${t.distinct.length} tuples (${t.distinct.map((x: any) => `${x.tuple} x${x.workers}w`).join(", ")}); origin ids shared across users: ${t.sharedOriginIdAcrossUsers.length ? t.sharedOriginIdAcrossUsers.join(", ") : "none"}; control user: ${t.controlUsers.join(", ") || "none"}.`);
    out.push(`- held: ${Object.entries(a.held as Record<string, Record<string, number>>).map(([st, m]) => `${st} (${a.transactions[st]} txns): ${Object.entries(m).map(([k, v]) => `${k} ${v}`).join("; ")}`).join(" | ")}`);
    out.push(`- queued commits: ${q.samplesWithTwoOrMore} samples with 2+ commits queued on the clock (max ${q.waitersMax}); distinct origin-claim keys among them p50 ${q.distinctOriginClaimKeysAmongQueued.p50}, max ${q.distinctOriginClaimKeysAmongQueued.max}; samples where two queued commits held the same key: ${q.samplesWhereTwoQueuedCommitsHeldTheSameOriginClaimKey}`, "");
  }
}
out.push("", "### Lock waits by stage and lock class (ms per transaction that waited, p50 / p95 / p99 / max; n = transactions)", "");
for (const r of rows) {
  out.push(`**${r.cell.workload} n=${r.cell.n} C=${r.cell.c}**`, "", "| stage: lock class | n | p50 / p95 / p99 / max |", "|---|---|---|");
  for (const [k, v] of Object.entries(r.lockWaitMsByStageClass as Record<string, any>)) out.push(`| ${k} | ${v.n} | ${d3(v)} |`);
  out.push("");
}
out.push("### Top wait-graph edges (samples: waiter stage -> lock [mode] <- blocker stages)", "");
for (const r of rows) {
  out.push(`**${r.cell.workload} n=${r.cell.n} C=${r.cell.c}** (sampler ${r.sampler.samples} samples, period p50 ${r.sampler.periodMs.p50} ms)`, "");
  for (const [k, v] of Object.entries(r.waitGraphSamples as Record<string, number>).slice(0, 8)) out.push(`- ${v}: ${k}`);
  out.push("");
}
out.push("### Client checks", "", "| workload | batch | C | load threads | client CPU s | event-loop lag ms p50 / p95 / p99 / max | sampler period ms p50 / p95 / p99 / max |", "|---|---|---|---|---|---|---|");
for (const r of rows) out.push(`| ${cell(r)} | ${r.threads} | ${r.client.processCpuS} | ${d3(r.client.eventLoopLagMs)} | ${r.sampler.periodMs.p50} / ${r.sampler.periodMs.p95} / ${r.sampler.periodMs.p99} / ${r.sampler.periodMs.max} |`);
console.log(out.join("\n"));
