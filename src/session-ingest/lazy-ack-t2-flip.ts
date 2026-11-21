/**
 * Operator flip of the 062210 T2 staged-ACK mode (sessions.lazy_ack_t2_mode), held candidate. Flipping on any real
 * database is Khaliq's own decision; nothing in the product calls this.
 *
 * sessions.lazy_ack_t2_set_mode only TRY-locks the (961996,50000) barrier, so it never queues an exclusive request.
 * When the barrier is busy it raises 55P03 at once and changes nothing. Each attempt here is ONE autocommit statement:
 * the server ends that statement's transaction (commit or rollback) before the result reaches this process, so no
 * backoff, logging or other round trip ever happens inside a transaction that holds the barrier. When the budget runs
 * out the result is { result: "no_switch" }: the mode is unchanged and nothing failed.
 */
export type FlipQuery = (sql: string, params: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
export type FlipMode = "main" | "t2";
export type FlipOptions = {
  /** Total wall-clock budget, from the first attempt (default 10 s). */
  budgetMs?: number;
  /** Maximum number of attempts (default 200). */
  maxAttempts?: number;
  /** First backoff; doubled per busy attempt up to maxBackoffMs, with full jitter (defaults 5 ms and 200 ms). */
  backoffMs?: number;
  maxBackoffMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  random?: () => number;
};
export type FlipResult =
  | { result: "switched"; from: FlipMode; to: FlipMode; attempts: number; elapsedMs: number }
  | { result: "no_switch"; to: FlipMode; attempts: number; elapsedMs: number; reason: "barrier_busy_budget_exhausted" };

/** The single statement each attempt runs (autocommit: never send it inside BEGIN). */
export const FLIP_STATEMENT = "SELECT sessions.lazy_ack_t2_set_mode($1) AS old";
const BUSY_MESSAGE = "T2 staged ACK: flip barrier busy";

/** True only for the function's own busy refusal or a NOWAIT on the mode row (both 55P03). */
export function isFlipBusy(e: unknown): boolean {
  const x = e as { code?: unknown; message?: unknown } | null;
  return x?.code === "55P03" && typeof x.message === "string" &&
    (x.message.startsWith(BUSY_MESSAGE) || x.message.startsWith("could not obtain lock on row in relation \"lazy_ack_t2_mode\""));
}

export async function flipLazyAckT2Mode(query: FlipQuery, to: FlipMode, opts: FlipOptions = {}): Promise<FlipResult> {
  if (to !== "main" && to !== "t2") throw new TypeError("lazy_ack_t2 flip: mode must be main or t2");
  const budgetMs = opts.budgetMs ?? 10_000, maxAttempts = opts.maxAttempts ?? 200;
  const base = opts.backoffMs ?? 5, cap = opts.maxBackoffMs ?? 200;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = opts.now ?? (() => performance.now()), random = opts.random ?? Math.random;
  const start = now();
  let attempts = 0, backoff = base;
  for (;;) {
    attempts++;
    try {
      const from = (await query(FLIP_STATEMENT, [to])).rows[0]?.old;
      if (from !== "main" && from !== "t2") throw Error("lazy_ack_t2 flip: unexpected previous mode");
      return { result: "switched", from, to, attempts, elapsedMs: now() - start };
    } catch (e) {
      if (!isFlipBusy(e)) throw e;
    }
    const elapsed = now() - start;
    if (attempts >= maxAttempts || elapsed >= budgetMs)
      return { result: "no_switch", to, attempts, elapsedMs: elapsed, reason: "barrier_busy_budget_exhausted" };
    await sleep(Math.min(Math.floor(random() * backoff), Math.max(0, budgetMs - elapsed)));
    backoff = Math.min(cap, backoff * 2);
  }
}
