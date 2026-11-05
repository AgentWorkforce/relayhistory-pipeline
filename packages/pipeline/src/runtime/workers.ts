/**
 * Default PipelineRuntimeAdapter: long-running worker pods (today's
 * hoopdb-indexer / referee / bob-costas tick loops, src/shared/worker.ts).
 *
 * A poll loop claims work from a `WorkSource` (the HoopDb `pipeline_jobs`
 * queue in production), settles each item, and fires schedules in-process on
 * wall-clock boundaries, like the indexer's RECONCILE_EVERY_MINUTES tick.
 * Several replicas may run: the queue's claim is the lease, and schedule
 * work must be safe to run concurrently (it only enqueues idempotent work).
 */
import {
  assertSchedules,
  scheduleItem,
  settle,
  type PipelineRuntimeAdapter,
  type PipelineWork,
  type RuntimeCapabilities,
  type WorkItem,
  type WorkOutcome,
} from "./contracts.js";

export interface WorkSource {
  /** Claim up to `max` items (lease them); empty when idle. */
  claim(max: number): Promise<readonly WorkItem[]>;
  /** Ack (done), release with a delay (retry) or park (dead). */
  settle(item: WorkItem, outcome: WorkOutcome): Promise<void>;
}

export interface WorkerRuntimeOptions {
  readonly source: WorkSource;
  readonly batchSize?: number;
  readonly idleMs?: number;
  readonly now?: () => number;
  readonly sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

export interface WorkerBinding {
  /** One loop iteration: fire due schedules, claim and settle one batch. Returns items handled. */
  tick(): Promise<number>;
  /** Loop until the signal aborts. */
  run(signal: AbortSignal): Promise<void>;
}

const defaultSleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const finish = () => {
      clearTimeout(t);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    const t = setTimeout(finish, ms);
    signal?.addEventListener("abort", finish, { once: true });
  });

export function createWorkerRuntime(options: WorkerRuntimeOptions): PipelineRuntimeAdapter<WorkerBinding> {
  const batchSize = options.batchSize ?? 10;
  const idleMs = options.idleMs ?? 1000;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? defaultSleep;
  const capabilities: RuntimeCapabilities = Object.freeze({
    kind: "workers",
    trigger: "poll",
    maxBatch: batchSize,
    perItemDelay: true,
    partialBatchFailure: true,
    scheduler: "in-process",
    scopeSerialization: "advisory-lock",
    deadLetter: "queue-table",
  });
  return {
    kind: "workers",
    capabilities: () => capabilities,
    bind(work: PipelineWork): WorkerBinding {
      assertSchedules(work.schedules);
      // Last fired boundary per job: fire once per `everySeconds` window.
      const lastWindow = new Map<string, number>();
      const context = { runtime: "workers" };
      async function tick(): Promise<number> {
        const at = now();
        for (const s of work.schedules) {
          const window = Math.floor(at / (s.everySeconds * 1000));
          if (lastWindow.get(s.job) === window) continue;
          lastWindow.set(s.job, window);
          // A failed schedule run is not retried: the next window runs it again.
          await settle(work, [scheduleItem(s.job, window * s.everySeconds * 1000)], context);
        }
        const items = await options.source.claim(batchSize);
        if (items.length === 0) return 0;
        const outcomes = await settle(work, items, context);
        for (const item of items) await options.source.settle(item, outcomes.get(item.id)!);
        return items.length;
      }
      return {
        tick,
        async run(signal) {
          while (!signal.aborted) {
            const handled = await tick();
            if (handled === 0) await sleep(idleMs, signal);
          }
        },
      };
    },
  };
}
