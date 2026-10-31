export function percentile(sorted: readonly number[], p: number): number | null {
  if (sorted.length === 0) return null;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

export function summarize(samples: readonly number[]) {
  const s = [...samples].sort((a, b) => a - b);
  const round = (v: number | null) => (v === null ? null : Math.round(v * 100) / 100);
  return { n: s.length, p50: round(percentile(s, 50)), p95: round(percentile(s, 95)), p99: round(percentile(s, 99)) };
}

export interface PlanNode { "Node Type": string; "Index Name"?: string; Plans?: PlanNode[]; "Actual Rows"?: number; "Rows Removed by Filter"?: number; "Actual Loops"?: number; "Rows Removed by Index Recheck"?: number; "Index Cond"?: string; "Recheck Cond"?: string }
export const planNodes = (n: PlanNode): PlanNode[] => [n, ...(n.Plans ?? []).flatMap(planNodes)];

/** The contract's portable gate: rows read per row returned, at most 100. */
export function rowAmplification(root: PlanNode): number | null {
  const returned = root["Actual Rows"] ?? 0;
  const scans = planNodes(root).filter((n) => n["Node Type"].includes("Scan"));
  const read = scans.reduce((sum, n) => sum + ((n["Actual Rows"] ?? 0) + (n["Rows Removed by Filter"] ?? 0)) * (n["Actual Loops"] ?? 1), 0);
  return returned === 0 ? null : read / returned;
}

export type GateVerdict = "PASS" | "FAIL" | "NOT MEASURED";

/**
 * The contract's agent gate. Exact and selective prefixes must constrain the name in an
 * index condition. A broad prefix may instead walk recency, within 100 rows read per row
 * returned. A plan that returned nothing proves nothing, so it never passes.
 */
export function agentGate(kind: "exact" | "selective-prefix" | "broad-prefix", root: PlanNode | null | undefined): { verdict: GateVerdict; reason: string } {
  if (!root || !root["Actual Rows"]) return { verdict: "NOT MEASURED", reason: "plan returned no rows" };
  const amplification = rowAmplification(root);
  if (amplification === null) return { verdict: "NOT MEASURED", reason: "no amplification" };
  const constrained = planNodes(root).some((n) => (n["Index Cond"] ?? "").includes("agent_name_normalized"));
  if (kind !== "broad-prefix" && !constrained) return { verdict: "FAIL", reason: `name is not in an Index Cond; ${amplification}x` };
  if (amplification > 100) return { verdict: "FAIL", reason: `${amplification}x exceeds 100x` };
  return { verdict: "PASS", reason: `${amplification}x` };
}
