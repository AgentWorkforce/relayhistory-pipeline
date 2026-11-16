/**
 * Turns the lanes' result lines into the write-up's tables and the plan's threshold outcomes.
 *
 *   bun benchmarks/ledger-bench/report.ts <results dir> > summary.md
 *
 * Each repeat is one run id; a figure reported here is the median over the repeats of that cell's own p50
 * or p99, as the plan asks. A threshold with a missing input is `inconclusive`, never a pass.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { median } from "./stats.ts";

export interface Record_ {
  run: string;
  lane: string;
  event?: string;
  step?: number;
  cell?: string;
  target?: string;
  params?: Record<string, string | number | boolean>;
  result?: Record<string, any>;
  [key: string]: unknown;
}

export interface Merged {
  step: number;
  cell: string;
  target: string;
  params: Record<string, string | number | boolean>;
  repeats: number;
  p50_ms: number | null;
  p90_ms: number | null;
  p99_ms: number | null;
  max_ms: number | null;
  ops_per_sec: number | null;
  /** ops/s times the batches one operation commits. */
  batches_per_sec: number | null;
  commit_p50_ms: number | null;
  commit_p99_ms: number | null;
  samples: number;
  errors: Record<string, number>;
  outcomes: Record<string, number>;
  violations: number | null;
  results: Record<string, any>[];
}

export function parseLines(text: string): Record_[] {
  return text
    .split("\n")
    .filter((l) => l.startsWith("{"))
    .map((l) => JSON.parse(l) as Record_)
    .filter((r) => (r as { ledger_bench?: number }).ledger_bench === 1);
}

const sumInto = (into: Record<string, number>, from: Record<string, number> | undefined) => {
  for (const [k, v] of Object.entries(from ?? {})) into[k] = (into[k] ?? 0) + v;
};

const paramKey = (p: Record<string, unknown> | undefined) => JSON.stringify(Object.entries(p ?? {}).sort(([a], [b]) => (a < b ? -1 : 1)));

/** One row per (step, cell, target, params), merged over the repeats. */
export function merge(records: Record_[]): Merged[] {
  const groups = new Map<string, Record_[]>();
  for (const r of records) {
    if (r.step === undefined || !r.cell || !r.result) continue;
    const k = `${r.step}|${r.cell}|${r.target}|${paramKey(r.params)}`;
    groups.set(k, [...(groups.get(k) ?? []), r]);
  }
  return [...groups.values()].map((rs) => {
    const first = rs[0]!;
    const results = rs.map((r) => r.result!);
    const errors: Record<string, number> = {};
    const outcomes: Record<string, number> = {};
    for (const r of results) {
      sumInto(errors, r.errors);
      sumInto(outcomes, r.outcomes);
    }
    const lat = (name: string) => median(results.map((r) => r.latency?.[name] as number | undefined));
    const commit = (name: string) => median(results.map((r) => r.commit_latency?.[name] as number | undefined));
    // A cell with no steady-state sample measured nothing: its rate is unknown, not zero.
    const ops = median(results.map((r) => (Number(r.latency?.n ?? 0) > 0 ? (r.ops_per_sec as number | undefined) : undefined)));
    const perOp = Number(results[0]!.batches_per_op ?? 1);
    const withViolations = results.filter((r) => typeof r.violations === "number");
    return {
      step: first.step!,
      cell: first.cell!,
      target: String(first.target),
      params: first.params ?? {},
      repeats: rs.length,
      p50_ms: lat("p50_ms"),
      p90_ms: lat("p90_ms"),
      p99_ms: lat("p99_ms"),
      max_ms: results.reduce<number | null>((a, r) => (typeof r.latency?.max_ms === "number" ? Math.max(a ?? 0, r.latency.max_ms) : a), null),
      ops_per_sec: ops,
      batches_per_sec: ops === null ? null : Math.round(ops * perOp * 100) / 100,
      commit_p50_ms: commit("p50_ms"),
      commit_p99_ms: commit("p99_ms"),
      samples: results.reduce((a, r) => a + Number(r.latency?.n ?? 0), 0),
      errors,
      outcomes,
      violations: withViolations.length === 0 ? null : withViolations.reduce((a, r) => a + (r.violations as number), 0),
      results,
    };
  });
}

export type Verdict = "pass" | "fail" | "inconclusive";

export interface Threshold {
  option: string;
  check: string;
  verdict: Verdict;
  detail: string;
}

/** Production's arrival rate for the hot user on 2026-11-15/16, in batches per second. */
export const OBSERVED_PEAK_BATCHES_PER_SEC = 140 / 60;

function ratioCheck(option: string, check: string, value: number | null | undefined, reference: number | null | undefined, pass: (ratio: number) => boolean, describe: string): Threshold {
  if (value === null || value === undefined || reference === null || reference === undefined || reference === 0) return { option, check, verdict: "inconclusive", detail: "not measured" };
  const ratio = value / reference;
  return { option, check, verdict: pass(ratio) ? "pass" : "fail", detail: `${value} against ${reference} (${Math.round(ratio * 1000) / 1000}x); ${describe}` };
}

/** The plan's "Success thresholds" table, evaluated on the merged cells. */
export function thresholds(cells: Merged[]): Threshold[] {
  const find = (step: number, cell: string, target: string, params: Record<string, unknown> = {}) =>
    cells.find((c) => c.step === step && c.cell === cell && c.target === target && Object.entries(params).every(([k, v]) => c.params[k] === v));
  /**
   * The violations of the named cells on a target; `null` when any of them did not run (in every repeat, with
   * a count), so a skipped or failed stage can never read as a pass.
   */
  const violationsOf = (step: number, target: string, required: string[]): number | null => {
    let total = 0;
    for (const name of required) {
      const found = cells.filter((c) => c.step === step && c.cell === name && c.target === target);
      if (found.length === 0 || found.some((c) => c.violations === null || c.results.some((r) => typeof r.violations !== "number"))) return null;
      total += found.reduce((a, c) => a + c.violations!, 0);
    }
    return total;
  };
  /** Torn or lost-acknowledged writes; `null` when no write was ever caught in flight (nothing was proven). */
  const ambiguous = (target: string): number | null => {
    const c = find(2, "ambiguous_commit", target);
    if (!c || c.results.reduce((a, r) => a + Number(r.in_flight ?? 0), 0) === 0) return null;
    return c.results.reduce((a, r) => a + Number(r.torn) + Number(r.acknowledged_lost), 0);
  };
  /** A general-purpose bucket must honour If-Match: anything but `supported` in any repeat is a violation. */
  const ifMatchSupported = (target: string): number | null => {
    const c = find(2, "if_match_support", target);
    return c ? c.results.filter((r) => r.support !== "supported").length : null;
  };
  const STEP2 = ["race_create", "race_cas", "race_delete"];
  const step2 = (target: string) => [violationsOf(2, target, STEP2), ifMatchSupported(target), ambiguous(target)];
  const correctness = (option: string, check: string, counts: (number | null)[]): Threshold => {
    const total = counts.reduce<number>((a, c) => a + (c ?? 0), 0);
    // A found violation fails even if another cell is missing; with none found, a missing cell is inconclusive.
    if (total > 0) return { option, check, verdict: "fail", detail: `${total} violations` };
    if (counts.some((c) => c === null)) return { option, check, verdict: "inconclusive", detail: "a correctness cell did not run, or caught nothing" };
    return { option, check, verdict: "pass", detail: "0 violations" };
  };
  const out: Threshold[] = [];
  const p13 = (target: string) => find(3, "p13", target, { streams: 1 });
  const plog = (target: string, group: number) => find(3, "plog", target, { streams: 1, group });
  const pddb = (group: number) => find(3, "pddb", "R2", { streams: 1, group });

  // A1: the in-region chain against the cross-region chain.
  out.push(ratioCheck("A1", "P13 p50 on R2 at most 1/8 of R1", p13("R2")?.p50_ms, p13("R1")?.p50_ms, (r) => r <= 1 / 8, "selected at 0.125x or less, rejected above 0.25x"));
  out.push(ratioCheck("A1", "P13 p99 on R2 at most 1/4 of R1", p13("R2")?.p99_ms, p13("R1")?.p99_ms, (r) => r <= 1 / 4, "needs 0.25x or less"));
  out.push(correctness("A1", "every step 2 cell passes on R2", step2("R2")));

  // B: Bucket Keys.
  for (const op of ["get", "put"]) out.push(ratioCheck("B", `${op.toUpperCase()} 64 KiB p50 on R3 at most 50% of R2 (concurrency 8)`, find(1, op, "R3", { concurrency: 8, kib: 64 })?.p50_ms, find(1, op, "R2", { concurrency: 8, kib: 64, sse: "kms" })?.p50_ms, (r) => r <= 0.5, "needs 0.5x or less"));
  out.push(ratioCheck("B", "P13 batches/s on R3 at least 1.3x R2", p13("R3")?.batches_per_sec, p13("R2")?.batches_per_sec, (r) => r >= 1.3, "selected at 1.3x or more, rejected under 1.15x"));
  out.push(correctness("B", "conditional cells behave the same on R3", step2("R3")));

  // C: the log commit.
  out.push(ratioCheck("C", "PLOG G=16 on R2 at least 10x P13 on R2 in batches/s, 1 stream", plog("R2", 16)?.batches_per_sec, p13("R2")?.batches_per_sec, (r) => r >= 10, "needs 10x or more"));
  out.push(correctness("C", "racing-writer, readback and ambiguous-commit checks on R2", [violationsOf(3, "R2", ["racing_writers", "plog_readback"]), violationsOf(2, "R2", ["race_create"]), ambiguous("R2")]));
  out.push(ratioCheck("C", "PLOG G=16 on R2 clears 10x the observed peak for 1 stream", plog("R2", 16)?.batches_per_sec, OBSERVED_PEAK_BATCHES_PER_SEC, (r) => r >= 10, "needs 23.3 batches/s or more"));

  // G: S3 Express.
  out.push(ratioCheck("G", "create-if-absent p99 on R4 at most 1/3 of R2", find(2, "create_if_absent", "R4")?.p99_ms, find(2, "create_if_absent", "R2")?.p99_ms, (r) => r <= 1 / 3, "needs 0.333x or less"));
  out.push(correctness("G", "If-None-Match correct under races on R4 and R5", [violationsOf(2, "R4", ["race_create"]), violationsOf(2, "R5", ["race_create"]), violationsOf(3, "R4", ["racing_writers", "plog_readback"])]));
  out.push(correctness("G", "the bucket's single KMS key is enforced on R4", [violationsOf(4, "R4", ["single_kms_key"])]));
  const soak = find(4, "session_refresh_soak", "R4");
  out.push(
    soak
      ? { option: "G", check: "session refresh causes 0 failed requests", verdict: Object.keys(soak.errors).length === 0 && soak.results.every((r) => r.create_session_failures === 0) ? "pass" : "fail", detail: `errors ${JSON.stringify(soak.errors)}; ${soak.results.map((r) => r.create_session_calls).join("/")} CreateSession calls` }
      : { option: "G", check: "session refresh causes 0 failed requests", verdict: "inconclusive", detail: "not measured" },
  );
  const single = find(4, "single_az_write", "R4", { kib: 1 });
  const dual = find(4, "dual_az_write", "R4+R5", { kib: 1 });
  const r2Put = find(1, "put", "R2", { concurrency: 8, kib: 1, sse: "kms" });
  out.push(
    single?.p50_ms != null && dual?.p50_ms != null && r2Put?.p50_ms != null
      ? { option: "G", check: "dual-AZ write adds no more than R2's own p50", verdict: dual.p50_ms - single.p50_ms <= r2Put.p50_ms ? "pass" : "fail", detail: `adds ${Math.round((dual.p50_ms - single.p50_ms) * 1000) / 1000} ms; R2 PUT p50 is ${r2Put.p50_ms} ms` }
      : { option: "G", check: "dual-AZ write adds no more than R2's own p50", verdict: "inconclusive", detail: "not measured" },
  );
  const ifMatch = ["R4", "R5"].map((t) => `${t}: ${find(2, "if_match_support", t)?.results.map((r) => r.support).join("/") ?? "not measured"}`);
  out.push({ option: "G", check: "If-Match on directory buckets (reported, not a gate for option C)", verdict: "inconclusive", detail: ifMatch.join("; ") });

  // E: DynamoDB.
  for (const group of [1, 4, 16, 30]) out.push(ratioCheck("E", `PDDB commit p99 below PLOG commit p99 on R2 at G=${group}`, pddb(group)?.commit_p99_ms, plog("R2", group)?.commit_p99_ms, (r) => r < 1, "needs under 1x"));
  out.push(correctness("E", "0 lost or duplicate commits", [violationsOf(5, "R6", ["transact_readback", "hot_head_readback", "idempotency_replay"]), violationsOf(3, "R2", ["pddb_readback"])]));
  const hot = cells.filter((c) => c.step === 5 && c.cell === "hot_head");
  const hotBest = hot.reduce<number | null>((a, c) => (c.ops_per_sec === null ? a : Math.max(a ?? 0, c.ops_per_sec)), null);
  out.push(ratioCheck("E", "one hot head sustains at least 2x the observed peak per-stream commit rate", hotBest, OBSERVED_PEAK_BATCHES_PER_SEC, (r) => r >= 2, "needs 4.7 writes/s or more"));
  return out;
}

const n = (v: number | null) => (v === null ? "-" : String(v));
const compact = (o: Record<string, number>) => (Object.keys(o).length === 0 ? "-" : Object.entries(o).map(([k, v]) => `${k}: ${v}`).join(", "));

/** Drops every record of a run that did not use `profile` (a smoke run must not count as a repeat). */
export function onlyProfile(records: Record_[], profile: string): Record_[] {
  // A run id is used with one profile (ops.ts refuses to reuse a run id for a lane).
  const runs = new Set(records.filter((r) => r.event === "start" && r.profile_name === profile).map((r) => r.run));
  return records.filter((r) => runs.has(r.run));
}

export function markdown(all: Record_[], profile = "standard"): string {
  const records = onlyProfile(all, profile);
  const cells = merge(records).sort((a, b) => a.step - b.step || a.cell.localeCompare(b.cell) || a.target.localeCompare(b.target) || paramKey(a.params).localeCompare(paramKey(b.params)));
  const runs = [...new Set(records.map((r) => r.run))].sort();
  const ends = records.filter((r) => r.event === "end");
  const lines: string[] = [`Profile: ${profile}. Repeats (run ids): ${runs.join(", ") || "none"}.`, ""];
  const throttled = ends.filter((e) => Number((e.cpu_stat as { nr_throttled?: number } | null)?.nr_throttled ?? 0) > 0);
  if (throttled.length > 0) lines.push(`The client was CPU-throttled in ${throttled.length} lane run(s): treat the high-concurrency cells there as client-bound.`, "");
  lines.push("## Lanes", "", "| Run | Lane | Complete | Requests | Budget | GiB | GiB budget | CPU throttled periods | Peak RSS KiB |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
  const gib = (v: unknown) => (typeof v === "number" ? String(Math.round((v / 1024 ** 3) * 100) / 100) : "-");
  for (const e of ends) lines.push(`| ${e.run} | ${e.lane} | ${e.complete ? "yes" : e.stopped_by ? `NO (${String(e.stopped_by)})` : "NO"} | ${e.requests} | ${e.request_budget} | ${gib(e.bytes)} | ${gib(e.byte_budget)} | ${(e.cpu_stat as { nr_throttled?: number } | null)?.nr_throttled ?? "-"} | ${e.max_rss_kib ?? "-"} |`);
  for (const f of records.filter((r) => r.event === "stage_failed" || r.event === "skipped")) lines.push("", `- ${f.run} ${f.lane}: ${f.event} ${String(f.stage)}${f.error ? `: ${String(f.error)}` : ""}`);
  lines.push("", "## Threshold outcomes", "", "| Option | Check | Outcome | Detail |", "| --- | --- | --- | --- |");
  for (const t of thresholds(cells)) lines.push(`| ${t.option} | ${t.check} | ${t.verdict} | ${t.detail} |`);
  for (const step of [1, 2, 3, 4, 5, 6]) {
    const latency = cells.filter((c) => c.step === step && c.samples > 0);
    const checks = cells.filter((c) => c.step === step && c.samples === 0);
    if (latency.length + checks.length === 0) continue;
    lines.push("", `## Step ${step}`, "");
    if (latency.length > 0) {
      lines.push("| Cell | Target | Parameters | Repeats | p50 ms | p90 ms | p99 ms | max ms | ops/s | batches/s | commit p50 / p99 ms | Errors | Outcomes |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
      for (const c of latency) lines.push(`| ${c.cell} | ${c.target} | ${compact(c.params as Record<string, number>)} | ${c.repeats} | ${n(c.p50_ms)} | ${n(c.p90_ms)} | ${n(c.p99_ms)} | ${n(c.max_ms)} | ${n(c.ops_per_sec)} | ${c.step === 3 ? n(c.batches_per_sec) : "-"} | ${c.commit_p50_ms === null ? "-" : `${c.commit_p50_ms} / ${n(c.commit_p99_ms)}`} | ${compact(c.errors)} | ${compact(c.outcomes)} |`);
    }
    if (checks.length > 0) {
      lines.push("", "| Check | Target | Parameters | Violations | Result per repeat |", "| --- | --- | --- | --- | --- |");
      for (const c of checks) lines.push(`| ${c.cell} | ${c.target} | ${compact(c.params as Record<string, number>)} | ${n(c.violations)} | ${c.results.map((r) => `\`${JSON.stringify(r)}\``).join(" ")} |`);
    }
  }
  return `${lines.join("\n")}\n`;
}

if (import.meta.main) {
  const dir = process.argv[2];
  if (!dir) {
    console.error("usage: report.ts <results dir>");
    process.exit(2);
  }
  const records = readdirSync(dir).filter((f) => f.endsWith(".jsonl")).sort().flatMap((f) => parseLines(readFileSync(join(dir, f), "utf8")));
  process.stdout.write(markdown(records, process.argv[3] ?? "standard"));
}
