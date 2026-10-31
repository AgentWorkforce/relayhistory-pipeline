/**
 * Latency and error-rate math for the proof report.
 *
 * Percentiles are nearest-rank, the same definition as the session search
 * benchmark (#59, benchmarks/session-search/stats.ts), so a p95 here and a p95
 * there mean the same thing. When #59 lands, import it from there instead.
 */

/** Nearest-rank percentile over an ascending-sorted array; null when empty. */
export function percentile(sorted: readonly number[], p: number): number | null {
  if (!(p > 0 && p <= 100)) throw new RangeError(`percentile p must be in (0, 100], got ${p}`);
  if (sorted.length === 0) return null;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

export interface LatencySummary {
  /** Samples that produced a latency. */
  n: number;
  /** Attempts that failed or never reached the stage before the deadline. */
  errors: number;
  /** errors / (n + errors); null when there were no attempts. */
  errorRate: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
}

const round = (v: number | null) => (v === null ? null : Math.round(v * 100) / 100);

/** Summarize latency samples (ms). Non-finite or negative samples are a harness bug, not data. */
export function summarize(samples: readonly number[], errors = 0): LatencySummary {
  for (const s of samples)
    if (!Number.isFinite(s) || s < 0) throw new RangeError(`latency sample must be a finite non-negative number, got ${s}`);
  if (!Number.isInteger(errors) || errors < 0) throw new RangeError(`errors must be a non-negative integer, got ${errors}`);
  const sorted = [...samples].sort((a, b) => a - b);
  const total = sorted.length + errors;
  return {
    n: sorted.length,
    errors,
    // Not rounded: 1 error in 1000 must not read as 0.
    errorRate: total === 0 ? null : errors / total,
    p50: round(percentile(sorted, 50)),
    p95: round(percentile(sorted, 95)),
    p99: round(percentile(sorted, 99)),
    max: round(sorted.at(-1) ?? null),
  };
}

/**
 * Pairwise stage latency: b - a for every item that has both. Items missing
 * either end, or whose end precedes its start (clock skew between sources),
 * count as errors rather than being clamped into a flattering zero.
 */
export function stageLatency<T>(items: readonly T[], from: (t: T) => number | undefined, to: (t: T) => number | undefined): LatencySummary {
  const samples: number[] = [];
  let errors = 0;
  for (const it of items) {
    const a = from(it);
    const b = to(it);
    if (a === undefined || b === undefined || b < a) errors += 1;
    else samples.push(b - a);
  }
  return summarize(samples, errors);
}
