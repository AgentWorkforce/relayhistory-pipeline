export function percentile(sorted: readonly number[], p: number): number | null {
  if (sorted.length === 0) return null;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

export function summarize(samples: readonly number[]) {
  const s = [...samples].sort((a, b) => a - b);
  const round = (v: number | null) => (v === null ? null : Math.round(v * 100) / 100);
  return { n: s.length, p50: round(percentile(s, 50)), p95: round(percentile(s, 95)), p99: round(percentile(s, 99)) };
}

export interface PlanNode { "Node Type": string; "Index Name"?: string; Plans?: PlanNode[]; "Actual Rows"?: number; "Rows Removed by Filter"?: number; "Actual Loops"?: number; "Rows Removed by Index Recheck"?: number }
export const planNodes = (n: PlanNode): PlanNode[] => [n, ...(n.Plans ?? []).flatMap(planNodes)];

/** The contract's portable gate: rows read per row returned, at most 100. */
export function rowAmplification(root: PlanNode): number | null {
  const returned = root["Actual Rows"] ?? 0;
  const scans = planNodes(root).filter((n) => n["Node Type"].includes("Scan"));
  const read = scans.reduce((sum, n) => sum + ((n["Actual Rows"] ?? 0) + (n["Rows Removed by Filter"] ?? 0)) * (n["Actual Loops"] ?? 1), 0);
  return returned === 0 ? null : read / returned;
}
