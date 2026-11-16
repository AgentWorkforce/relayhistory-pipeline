/**
 * Retry backoff ladder. Failure n (1-based) of L = backoff.length waits backoff[n-1] seconds;
 * failure L+1 and later is terminal (null): only a config change or an operator requeue revives
 * the row. The due predicate is `attempts <= L AND next_retry_at <= now`, so every rung is used.
 */
export function nextRetryAt(attempts: number, backoffSeconds: readonly number[], now: Date): Date | null {
  if (!Number.isInteger(attempts) || attempts < 1 || attempts > backoffSeconds.length) return null;
  return new Date(now.getTime() + backoffSeconds[attempts - 1]! * 1000);
}

/** The highest attempts value that is still due (the ladder length). */
export const maxDueAttempts = (backoffSeconds: readonly number[]) => backoffSeconds.length;

/** smallint ceiling for the stored counter. */
export const clampAttempts = (n: number) => Math.min(Math.max(0, n), 32_767);
