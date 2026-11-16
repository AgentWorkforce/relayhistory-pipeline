/**
 * Latency recording for the hosted-ledger benchmark (docs/design/hosted-ledger-benchmark-plan.md).
 *
 * A cell can make millions of requests, so timings go into a log-bucketed histogram (each bucket is
 * {@link BUCKET_GROWTH} wider than the last, so a reported percentile is within 2% of the true one) instead
 * of an array. The histogram itself is written to the results file: it is the per-request record.
 */
export const BUCKET_GROWTH = 1.02;
const LOG_GROWTH = Math.log(BUCKET_GROWTH);
/** Timings below this are clamped into the first bucket (10 microseconds). */
const FLOOR_MS = 0.01;

export interface LatencySummary {
  n: number;
  min_ms: number | null;
  p50_ms: number | null;
  p90_ms: number | null;
  p99_ms: number | null;
  max_ms: number | null;
  mean_ms: number | null;
}

const round = (v: number) => Math.round(v * 1000) / 1000;

export class Histogram {
  #counts = new Map<number, number>();
  #n = 0;
  #sum = 0;
  #min = Infinity;
  #max = -Infinity;

  record(ms: number): void {
    if (!Number.isFinite(ms) || ms < 0) throw new Error(`ledger-bench: not a latency: ${ms}`);
    const bucket = Math.floor(Math.log(Math.max(ms, FLOOR_MS)) / LOG_GROWTH);
    this.#counts.set(bucket, (this.#counts.get(bucket) ?? 0) + 1);
    this.#n++;
    this.#sum += ms;
    if (ms < this.#min) this.#min = ms;
    if (ms > this.#max) this.#max = ms;
  }

  get count(): number {
    return this.#n;
  }

  /** The upper bound of the bucket holding the p-th percentile, clamped to the observed range. */
  percentile(p: number): number | null {
    if (this.#n === 0) return null;
    const rank = Math.max(1, Math.ceil((p / 100) * this.#n));
    let seen = 0;
    for (const bucket of [...this.#counts.keys()].sort((a, b) => a - b)) {
      seen += this.#counts.get(bucket)!;
      if (seen >= rank) return Math.min(this.#max, Math.max(this.#min, Math.exp((bucket + 1) * LOG_GROWTH)));
    }
    return this.#max;
  }

  summary(): LatencySummary {
    if (this.#n === 0) return { n: 0, min_ms: null, p50_ms: null, p90_ms: null, p99_ms: null, max_ms: null, mean_ms: null };
    return {
      n: this.#n,
      min_ms: round(this.#min),
      p50_ms: round(this.percentile(50)!),
      p90_ms: round(this.percentile(90)!),
      p99_ms: round(this.percentile(99)!),
      max_ms: round(this.#max),
      mean_ms: round(this.#sum / this.#n),
    };
  }

  /** `[bucket lower bound in ms, count]`, ascending: the raw record kept in the results file. */
  buckets(): [number, number][] {
    return [...this.#counts.entries()].sort(([a], [b]) => a - b).map(([b, c]) => [round(Math.exp(b * LOG_GROWTH)), c]);
  }
}

/** Counts by label (outcomes, error classes, HTTP statuses). */
export class Tally {
  #counts = new Map<string, number>();

  add(label: string, by = 1): void {
    this.#counts.set(label, (this.#counts.get(label) ?? 0) + by);
  }

  get(label: string): number {
    return this.#counts.get(label) ?? 0;
  }

  total(): number {
    let t = 0;
    for (const c of this.#counts.values()) t += c;
    return t;
  }

  toJSON(): Record<string, number> {
    return Object.fromEntries([...this.#counts.entries()].sort(([a], [b]) => (a < b ? -1 : 1)));
  }
}

/** The median of the repeats' values; `null` when no repeat measured it. */
export function median(values: readonly (number | null | undefined)[]): number | null {
  const v = values.filter((x): x is number => typeof x === "number" && Number.isFinite(x)).sort((a, b) => a - b);
  if (v.length === 0) return null;
  const mid = Math.floor(v.length / 2);
  return v.length % 2 === 1 ? v[mid]! : round((v[mid - 1]! + v[mid]!) / 2);
}
