/**
 * Portable pipeline runtime contract (PipelineRuntimeAdapter): HOW stage work
 * is triggered and executed, separated from WHAT the work is.
 *
 * The engine supplies `PipelineWork`: an idempotent batch handler for work
 * items (a manifest landed; a schedule fired) plus the schedules it needs
 * (manifest reconcile, briefs, summary sweep). A runtime adapter translates
 * its platform's triggers into work items and the handler's outcomes back
 * into the platform's ack / retry / dead-letter mechanics:
 *
 *   workers   long-running pods polling the HoopDb queue; schedules in-process (OSS default)
 *   lambda    S3 -> SQS -> Lambda partial batch response; EventBridge schedules (cloud)
 *   cloudrun  GCS -> Pub/Sub push -> Cloud Run; Cloud Scheduler (cloud)
 *   terse     durable actors: one actor per session scope, alarms for schedules (cloud, stub)
 *
 * Delivery is at-least-once everywhere, so the handler must be idempotent and
 * serialize per session scope itself (advisory lock today) unless the runtime
 * declares `scopeSerialization: "actor"`.
 */

export type WorkItem =
  | {
      readonly type: "manifest-landed";
      /** The runtime's delivery id (SQS messageId, Pub/Sub messageId, queue row id). */
      readonly id: string;
      /** Object key of the landed manifest. */
      readonly key: string;
      /** Ordering token when the platform has one (S3 sequencer, GCS generation). */
      readonly sequencer?: string;
      /** 1 on first delivery. */
      readonly attempt: number;
    }
  | {
      readonly type: "schedule";
      readonly id: string;
      /** A job named in PipelineWork.schedules. */
      readonly job: string;
      readonly firedAtMs: number;
      readonly attempt: number;
    };

export type WorkOutcome =
  | { readonly status: "done" }
  /** Try again no sooner than delaySeconds (a busy scope, a transient backend error). */
  | { readonly status: "retry"; readonly delaySeconds: number; readonly code: string }
  /** Will never succeed (unparseable input): park it for an operator. */
  | { readonly status: "dead"; readonly code: string };

export interface ScheduleSpec {
  readonly job: string;
  readonly everySeconds: number;
}

export interface WorkContext {
  readonly runtime: string;
  readonly signal?: AbortSignal;
}

export interface PipelineWork {
  /**
   * Handle one delivery batch. Return an outcome per item id; a missing id is
   * treated as retry. Throwing retries the whole batch. Never throws for one
   * bad item: return `dead` for it instead.
   */
  handle(items: readonly WorkItem[], context: WorkContext): Promise<ReadonlyMap<string, WorkOutcome>>;
  readonly schedules: readonly ScheduleSpec[];
}

export interface RuntimeCapabilities {
  readonly kind: string;
  /** poll: the runtime claims work; push: the platform invokes the runtime. */
  readonly trigger: "poll" | "push";
  readonly maxBatch: number;
  /** A retry can carry its own delay (SQS DelaySeconds, queue visibility). */
  readonly perItemDelay: boolean;
  /** One batch can ack some items and retry others. */
  readonly partialBatchFailure: boolean;
  /** Who fires schedules. */
  readonly scheduler: "in-process" | "platform" | "actor-alarm";
  /** advisory-lock: the handler must serialize per scope; actor: the runtime does. */
  readonly scopeSerialization: "advisory-lock" | "actor";
  /** Where `dead` items go. */
  readonly deadLetter: "queue-table" | "platform-dlq" | "actor-state";
}

/**
 * A runtime adapter. `Binding` is platform-shaped: a `run(signal)` loop for
 * workers, a Lambda `handler`, a Cloud Run `fetch`, actor classes for Terse.
 */
export interface PipelineRuntimeAdapter<Binding = unknown> {
  readonly kind: string;
  capabilities(): RuntimeCapabilities;
  bind(work: PipelineWork): Binding;
}

export const DEFAULT_RETRY_DELAY_SECONDS = 30;

/**
 * Call the handler and complete its answer: a thrown handler or a missing
 * outcome becomes `retry`. Every runtime uses this so the failure semantics
 * are identical across platforms.
 */
export async function settle(
  work: PipelineWork,
  items: readonly WorkItem[],
  context: WorkContext,
): Promise<Map<string, WorkOutcome>> {
  const out = new Map<string, WorkOutcome>();
  if (items.length === 0) return out;
  let answered: ReadonlyMap<string, WorkOutcome> | undefined;
  try {
    answered = await work.handle(items, context);
  } catch {
    answered = undefined;
  }
  for (const item of items) {
    const outcome = answered?.get(item.id);
    out.set(
      item.id,
      outcome && ["done", "retry", "dead"].includes(outcome.status)
        ? outcome
        : { status: "retry", delaySeconds: DEFAULT_RETRY_DELAY_SECONDS, code: answered ? "no_outcome" : "handler_threw" },
    );
  }
  return out;
}

export function scheduleItem(job: string, firedAtMs: number, attempt = 1, id = `schedule:${job}:${firedAtMs}`): WorkItem {
  return { type: "schedule", id, job, firedAtMs, attempt };
}

export class RuntimeConfigError extends Error {
  constructor(readonly code: "invalid_argument" | "unsupported") {
    super(`pipeline runtime: ${code}`);
    this.name = "RuntimeConfigError";
  }
}

export function assertSchedules(schedules: readonly ScheduleSpec[]): void {
  const seen = new Set<string>();
  for (const s of schedules) {
    if (!/^[a-z][a-z0-9-]{0,62}$/.test(s.job) || seen.has(s.job) || !Number.isInteger(s.everySeconds) || s.everySeconds < 1) {
      throw new RuntimeConfigError("invalid_argument");
    }
    seen.add(s.job);
  }
}
