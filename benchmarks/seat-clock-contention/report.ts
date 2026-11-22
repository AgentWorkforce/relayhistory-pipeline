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
const cell = (r: any) => `${r.cell.workload} | ${r.cell.n} | ${r.cell.c}`;
const fails = (r: any) => Object.entries(r.stageFailures as Record<string, number>).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k} x${v}`).join(", ") || "none";
const out: string[] = [];
out.push(`## ${runId}`, "", `PostgreSQL: ${meta.serverVersion.split(" on ")[0]}; history switch live: ${meta.template.historyLive}; origin claim fence: ${meta.plan.originClaimFence}; records per run: ${meta.plan.totalRecords}; control every ${meta.plan.controlEveryMs} ms; loopback RTT p50 ${meta.rtt.p50} ms.`, "");
out.push("### Throughput and failures", "", "| workload | batch | C | drained | drain s | durable rec/s | commits/s | retries/batch | stage failures (stage:SQLSTATE) |", "|---|---|---|---|---|---|---|---|---|");
for (const r of rows) out.push(`| ${cell(r)} | ${r.drained ? "yes" : `no (${r.undrainedBatches} left)`} | ${f(r.drainSeconds, 1)} | ${r.durableRecordsPerSec} | ${f(r.commitsPerSec, 2)} | ${f(r.retriesPerBatch, 2)} | ${fails(r)} |`);
out.push("", "### Clock hold vs wait, commit transactions (ms, p50 / p95 / p99 / max)", "", "| workload | batch | C | hold | wait for clock | commit stage latency (ok) | holds unobserved |", "|---|---|---|---|---|---|---|");
for (const r of rows) out.push(`| ${cell(r)} | ${d3(r.clockHoldMs.commit)} | ${d3(r.clockWaitMs.commit)} | ${d3(r.stageLatencyMs.commit)} | ${r.unobservedHolds} |`);
out.push("", "### Controls during load (fixed rate, one at a time each; latency and hold in ms)", "",
  "C1 = sessions.ingest_lazy_clear(reset) as owner (the clock FOR UPDATE step). C2 = sessions.ingest_reset_uploaded_sessions as the runtime role (full ingest_apply_ordered_control).", "",
  "| workload | batch | C | control | attempted | completed | failed (SQLSTATE) | missed slots | latency p50 / p95 / p99 / max | clock hold p50 / max |", "|---|---|---|---|---|---|---|---|---|---|");
for (const r of rows) {
  const cs: Record<string, any> = r.controls.attempted !== undefined ? { control: { ...r.controls, what: "C1" } } : r.controls;
  for (const [st, c] of Object.entries(cs)) { const h = r.clockHoldMs[st]; out.push(`| ${cell(r)} | ${st === "control" ? "C1" : "C2"} | ${c.attempted} | ${c.completed} | ${JSON.stringify(c.failed)} | ${c.missedSlots} | ${d3(c.latencyMs)} | ${h ? `${f(h.p50, 1)} / ${f(h.max, 1)}` : "-"} |`); }
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
