/**
 * Re-analysis of retained results only (no database): per cell, hold coverage over successful commits, and the clock wait
 * split into (a) successful commits seen waiting, (b) commit attempts that failed while waiting on the clock, (c) commits
 * never seen waiting. Reads results/<runId>/summary.json and txns-*.jsonl.gz.
 * Usage: bun benchmarks/seat-clock-contention/analyze.ts <runId>... > results/coverage-and-wait-split.md
 *
 * Clock wait per transaction here = clock-wait sample count x the cell's sample period p50 (the retained fields keep the
 * count, not the first wait timestamp), so it is a sampled estimate, not the first-sample-to-holder interval of summary.json.
 */
import { readFileSync, readdirSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { join } from "node:path";

const runs = process.argv.slice(2);
if (!runs.length) throw Error("usage: analyze.ts <runId>...");
const pct = (xs: number[], p: number) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * s.length))]!; };
const d = (xs: number[]) => (xs.length ? `${xs.length}: ${[0.5, 0.95, 0.99, 1].map((p) => Math.round(pct(xs, p)!)).join(" / ")}` : "0: -");
const out: string[] = ["# Hold coverage and clock-wait split (from retained files)", "",
  "Hold coverage = successful commits with at least one clock-holder sample / successful commits.",
  "Clock wait groups, in ms, as `n: p50 / p95 / p99 / max`. Each value is the clock-wait sample count x the cell's sample period p50, so it is a sampled estimate:",
  "- (a) successful commits seen waiting on the clock;",
  "- (b) commit attempts that failed (any SQLSTATE) after being seen waiting on the clock, i.e. timed out in the queue;",
  "- (c) successful commits never seen waiting (count only).",
  "Failed commit attempts never seen waiting on the clock are counted separately.", ""];
for (const run of runs) {
  const dir = join(import.meta.dir, "results", run);
  const { rows } = JSON.parse(readFileSync(join(dir, "summary.json"), "utf8")) as { rows: any[] };
  const files = readdirSync(dir).filter((f) => f.startsWith("txns-"));
  out.push(`## ${run}`, "", "| cell | sample period p50 ms | hold coverage (ok commits) | (a) ok, waited | (b) failed while waiting on clock | (c) ok, never seen waiting | failed, not seen on clock |", "|---|---|---|---|---|---|---|");
  rows.forEach((r, i) => {
    const f = files.find((x) => x.startsWith(`txns-${i}-`));
    const label = `${r.cell.workload}${r.cell.label ? ` (${r.cell.label})` : ""} n=${r.cell.n} C=${r.cell.c}`;
    if (!f) { out.push(`| ${label} | - | unavailable | unavailable | unavailable | unavailable | unavailable |`); return; }
    const period = r.sampler.periodMs.p50 as number;
    const txns = gunzipSync(readFileSync(join(dir, f))).toString("utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)).filter((t) => t.stage === "commit");
    const ok = txns.filter((t) => t.ok), bad = txns.filter((t) => !t.ok);
    const clk = (t: any) => (t.waits?.clock ?? 0) as number;
    const cov = ok.length ? `${ok.filter((t) => t.holdFirst !== null).length}/${ok.length}` : "-";
    const a = ok.filter((t) => clk(t) > 0).map((t) => clk(t) * period);
    const b = bad.filter((t) => clk(t) > 0).map((t) => clk(t) * period);
    const c = ok.filter((t) => clk(t) === 0).length;
    const bn = bad.filter((t) => clk(t) === 0).length;
    const codes = Object.entries(bad.filter((t) => clk(t) > 0).reduce((m: Record<string, number>, t) => ({ ...m, [t.code]: (m[t.code] ?? 0) + 1 }), {})).map(([k, v]) => `${k} x${v}`).join(", ");
    out.push(`| ${label} | ${period} | ${cov} | ${d(a)} | ${d(b)}${codes ? ` (${codes})` : ""} | ${c} | ${bn} |`);
  });
  out.push("");
}
console.log(out.join("\n").trimEnd());
