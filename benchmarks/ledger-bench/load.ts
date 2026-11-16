/**
 * Closed-loop load: `concurrency` workers each run the operation back to back for `durationMs`, after a
 * warm-up whose samples are dropped. An operation's latency is the whole call as the ledger would see it
 * (hashing, signing, the round trip, reading the body).
 */
import { isObjectStoreError } from "../../packages/object-store/src/errors.ts";
import { Histogram, Tally, type LatencySummary } from "./stats.ts";

export interface CellResult {
  concurrency: number;
  duration_ms: number;
  /** Operations that returned, per second of steady state. */
  ops_per_sec: number;
  latency: LatencySummary;
  /** What the operations returned (`ok` unless the operation names its outcome). */
  outcomes: Record<string, number>;
  /** Thrown errors by class: `http_<status>`, `store_cause_<class>` for failures below HTTP, else the code. */
  errors: Record<string, number>;
  histogram: [number, number][];
  /** True when the run stopped early (the request budget ran out or the deadline passed). */
  truncated?: boolean;
  /** Set when the cell ended at its operation cap before its time was up (a cost bound, not a failure). */
  capped_at_ops?: number;
}

export interface LoadOptions {
  concurrency: number;
  warmupMs: number;
  durationMs: number;
  /** Returns an outcome label, or nothing for `ok`. `worker` is 0..concurrency-1, `i` counts per worker. */
  op: (worker: number, i: number) => Promise<string | void>;
  /** Checked before every operation; a true result ends the cell early and marks it truncated. */
  stop?: () => boolean;
  /** End the cell once this many operations have been started (warm-up included): bounds a fast cell's cost. */
  maxOps?: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

/** The closed class of a thrown error, safe to print: never a message, a key or a URL. */
export function errorClass(error: unknown): string {
  if (isObjectStoreError(error)) {
    if (error.status !== undefined) return `http_${error.status}`;
    if (error.transportCause !== undefined) return `store_cause_${error.transportCause}`;
    return error.code;
  }
  const o = error as { errorClass?: unknown; name?: unknown } | null;
  if (o && typeof o.errorClass === "string" && /^[A-Za-z0-9_.#-]{1,96}$/.test(o.errorClass)) return o.errorClass;
  if (o && typeof o.name === "string" && /^[A-Za-z0-9_]{1,64}$/.test(o.name)) return `thrown_${o.name}`;
  return "thrown_other";
}

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function runLoad(options: LoadOptions): Promise<CellResult> {
  const { concurrency, warmupMs, durationMs, op } = options;
  if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("ledger-bench: concurrency must be a positive integer");
  const now = options.now ?? (() => performance.now());
  const sleep = options.sleep ?? defaultSleep;
  const histogram = new Histogram();
  const outcomes = new Tally();
  const errors = new Tally();
  const start = now();
  const steadyFrom = start + warmupMs;
  const end = steadyFrom + durationMs;
  let truncated = false;
  let lastSteady = steadyFrom;
  let started = 0;
  let capped = false;

  const worker = async (id: number) => {
    for (let i = 0; ; i++) {
      const t0 = now();
      if (t0 >= end) return;
      if (options.stop?.()) {
        truncated = true;
        return;
      }
      if (options.maxOps !== undefined && started >= options.maxOps) {
        capped = true;
        return;
      }
      started++;
      let label: string | undefined;
      let failed: string | undefined;
      try {
        label = (await op(id, i)) ?? "ok";
      } catch (error) {
        failed = errorClass(error);
      }
      const t1 = now();
      // A sample counts when its operation started in the steady state.
      if (t0 >= steadyFrom) {
        if (failed === undefined) {
          histogram.record(t1 - t0);
          outcomes.add(label!);
        } else errors.add(failed);
        if (t1 > lastSteady) lastSteady = t1;
      }
      // A failing target must not turn the loop into a busy spin.
      if (failed !== undefined) await sleep(25);
    }
  };
  await Promise.all(Array.from({ length: concurrency }, (_, id) => worker(id)));
  const measured = Math.max(1, Math.min(lastSteady, end) - steadyFrom);
  return {
    concurrency,
    duration_ms: Math.round(measured),
    ops_per_sec: Math.round((histogram.count / measured) * 1000 * 100) / 100,
    latency: histogram.summary(),
    outcomes: outcomes.toJSON(),
    errors: errors.toJSON(),
    histogram: histogram.buckets(),
    ...(truncated ? { truncated: true } : {}),
    ...(capped ? { capped_at_ops: options.maxOps } : {}),
  };
}

/** Run `tasks` with at most `limit` in flight; results keep the input order. */
export async function mapLimit<T, R>(items: readonly T[], limit: number, work: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, async () => {
      for (;;) {
        const i = next++;
        if (i >= items.length) return;
        out[i] = await work(items[i]!, i);
      }
    }),
  );
  return out;
}
