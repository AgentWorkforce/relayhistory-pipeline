/**
 * Operator flip of the 062210 T2 staged-ACK mode (sessions.lazy_ack_t2_mode), held candidate. Flipping on any real
 * database is Khaliq's own decision; nothing in the product calls this.
 *
 * sessions.lazy_ack_t2_set_mode only TRY-locks the (961996,50000) barrier, so it never queues an exclusive request.
 * When the barrier is busy it raises 55P03 at once and changes nothing. Each attempt here is ONE autocommit statement:
 * the server ends that statement's transaction (commit or rollback) before the result reaches this process, so no
 * backoff, logging or other round trip ever happens inside a transaction that holds the barrier. When the budget runs
 * out the outcome is "no_switch": the mode is unchanged and nothing failed. flipLazyAckT2Mode resolves ONLY on
 * "switched" and throws LazyAckT2FlipNotSwitched (carrying the outcome) otherwise; lazyAckT2FlipOutcome returns it.
 * The caller passes the query function (its own connection); this module opens nothing and logs nothing.
 *
 * Operator procedure (062210 review): ONE bounded attempt sequence per operator action, in a naturally quiet window.
 *   - no_switch: no switch; nothing changed. Do not loop on this helper; decide again later.
 *   - indeterminate: reconcile first (read the mode, confirm no other flip ran) before any further attempt.
 *   There is no queued fallback, no admission stop and no automatic repeat-until-success. Flipping back to main uses
 *   the same try-lock and can starve under load too: a rollback of the mode is not immediate.
 * The gate (ALTER FUNCTION ... SET bulls96.lazy_ack_t2) is not barriered: change it only while the mode is 'main', and
 * never RESET it (set 'off'). lazy_ack_t2_set_mode refuses 't2' (55000, not retried here) unless the gate is 'fenced'.
 * Busy is recognised by SQLSTATE 55P03 AND the server's English message text (the function's own message, or the row
 * NOWAIT message); a server with translated messages (lc_messages other than English) makes every busy a thrown error,
 * never a retry or a false "switched".
 * Options must be finite and bounded: budgetMs > 0, maxAttempts an integer >= 1, backoffs >= 0 (TypeError otherwise).
 * The first attempt is always dispatched (a valid budget is > 0, so it starts before the deadline).
 *
 * budgetMs is a RETRY-START deadline: no attempt starts at or after start + budgetMs (it is checked after every
 * backoff sleep, so a late timer cannot add an attempt). An attempt already sent before the deadline is allowed to
 * finish; it never waits on a lock (try-lock and NOWAIT), so it ends within one round trip.
 * If the connection fails after an attempt was sent (no reply), the outcome is unknown: the mode is read back on a
 * new statement and the result is { result: "indeterminate", observedMode } (observedMode null if the read-back fails
 * too). The operator must not assume either outcome without it.
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
  | { result: "no_switch"; to: FlipMode; attempts: number; elapsedMs: number; reason: "barrier_busy_budget_exhausted" }
  | { result: "indeterminate"; to: FlipMode; attempts: number; elapsedMs: number; observedMode: FlipMode | null; error: string };

/** The single statement each attempt runs (autocommit: never send it inside BEGIN). */
export const FLIP_STATEMENT = "SELECT sessions.lazy_ack_t2_set_mode($1) AS old";
const BUSY_MESSAGE = "T2 staged ACK: flip barrier busy";

/** True only for the function's own busy refusal or a NOWAIT on the mode row (both 55P03). */
export function isFlipBusy(e: unknown): boolean {
  const x = e as { code?: unknown; message?: unknown } | null;
  return x?.code === "55P03" && typeof x.message === "string" &&
    (x.message.startsWith(BUSY_MESSAGE) || x.message.startsWith("could not obtain lock on row in relation \"lazy_ack_t2_mode\""));
}

/** No SQLSTATE from the server (socket error, timeout) or a connection/shutdown class: the reply was lost. */
export function isConnectionLoss(e: unknown): boolean {
  const code = (e as { code?: unknown } | null)?.code;
  if (typeof code !== "string" || !/^[0-9A-Z]{5}$/.test(code)) return true;
  return code.startsWith("08") || code === "57P01" || code === "57P02" || code === "57P03";
}
export const MODE_READBACK = "SELECT mode FROM sessions.lazy_ack_t2_mode WHERE singleton";

export type FlipSwitched = Extract<FlipResult, { result: "switched" }>;
export type FlipNotSwitched = Exclude<FlipResult, { result: "switched" }>;

/**
 * Thrown by flipLazyAckT2Mode when the mode was NOT (or not provably) switched, so a caller that only checks for a
 * thrown error can never read "no_switch" as success. It is not a failed apply: `outcome` says exactly what happened
 * ("no_switch": barrier busy for the whole budget, mode unchanged; "indeterminate": reply lost, see observedMode).
 */
export class LazyAckT2FlipNotSwitched extends Error {
  constructor(readonly outcome: FlipNotSwitched) {
    super(outcome.result === "no_switch"
      ? `lazy_ack_t2 flip: no switch to ${outcome.to} (barrier busy for the whole budget, ${outcome.attempts} attempts); mode unchanged`
      : `lazy_ack_t2 flip: outcome unknown (reply lost: ${outcome.error}); mode read back: ${outcome.observedMode ?? "unavailable"}`);
    this.name = outcome.result === "no_switch" ? "LazyAckT2NoSwitch" : "LazyAckT2FlipIndeterminate";
  }
}

/** Resolves only when switched; otherwise throws LazyAckT2FlipNotSwitched (see lazyAckT2FlipOutcome for the raw outcome). */
export async function flipLazyAckT2Mode(query: FlipQuery, to: FlipMode, opts: FlipOptions = {}): Promise<FlipSwitched> {
  const r = await lazyAckT2FlipOutcome(query, to, opts);
  if (r.result !== "switched") throw new LazyAckT2FlipNotSwitched(r);
  return r;
}

/** The raw outcome: switched, no_switch or indeterminate (never throws for those three). */
export async function lazyAckT2FlipOutcome(query: FlipQuery, to: FlipMode, opts: FlipOptions = {}): Promise<FlipResult> {
  if (to !== "main" && to !== "t2") throw new TypeError("lazy_ack_t2 flip: mode must be main or t2");
  const budgetMs = opts.budgetMs ?? 10_000, maxAttempts = opts.maxAttempts ?? 200;
  const base = opts.backoffMs ?? 5, cap = opts.maxBackoffMs ?? 200;
  if (!Number.isFinite(budgetMs) || budgetMs <= 0) throw new TypeError("lazy_ack_t2 flip: budgetMs must be a finite number > 0");
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new TypeError("lazy_ack_t2 flip: maxAttempts must be an integer >= 1");
  if (!Number.isFinite(base) || base < 0 || !Number.isFinite(cap) || cap < 0) throw new TypeError("lazy_ack_t2 flip: backoffMs and maxBackoffMs must be finite numbers >= 0");
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = opts.now ?? (() => performance.now()), random = opts.random ?? Math.random;
  const start = now();
  let attempts = 0, backoff = base;
  for (;;) {
    const before = now() - start;
    if (attempts > 0 && (attempts >= maxAttempts || before >= budgetMs))
      return { result: "no_switch", to, attempts, elapsedMs: before, reason: "barrier_busy_budget_exhausted" };
    attempts++;
    let reply: { rows: Array<Record<string, unknown>> } | undefined;
    try {
      reply = await query(FLIP_STATEMENT, [to]);
    } catch (e) {
      if (isFlipBusy(e)) { /* nothing changed: retry below */ }
      else if (isConnectionLoss(e)) {
        let observedMode: FlipMode | null = null;
        try { const m = (await query(MODE_READBACK, [])).rows[0]?.mode; observedMode = m === "main" || m === "t2" ? m : null; } catch { observedMode = null; }
        return { result: "indeterminate", to, attempts, elapsedMs: now() - start, observedMode, error: String((e as { code?: unknown })?.code ?? (e as Error)?.message ?? e) };
      } else throw e;
    }
    if (reply) {
      const from = reply.rows[0]?.old;
      if (from !== "main" && from !== "t2") throw Error("lazy_ack_t2 flip: unexpected previous mode");
      return { result: "switched", from, to, attempts, elapsedMs: now() - start };
    }
    const elapsed = now() - start;
    if (attempts >= maxAttempts || elapsed >= budgetMs)
      return { result: "no_switch", to, attempts, elapsedMs: elapsed, reason: "barrier_busy_budget_exhausted" };
    await sleep(Math.min(Math.floor(random() * backoff), budgetMs - elapsed));
    backoff = Math.min(cap, backoff * 2);
  }
}
