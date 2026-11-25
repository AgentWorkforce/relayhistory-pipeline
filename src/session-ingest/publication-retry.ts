import { initialReconciliationState, reconcilePublicationPage, type ReconciliationSource, type ReconciliationState } from "./publication-reconciliation.ts";

/** The scope key is a digest of exact tenant, generation and physical target. */
export interface RetryCheckpoint {
  version: number;
  scan: ReconciliationState;
  retryTurn: boolean;
}
export const initialRetryCheckpoint = (): RetryCheckpoint => ({
  version: 0, scan: initialReconciliationState(), retryTurn: false,
});

export interface ReceiptRetry {
  token: string;
  attempts: number;
  state: "retry" | "exhausted" | "audit";
  due: number;
}

type StoreResult<T> = T | Promise<T>;

export interface ReceiptRetryStore {
  load(scope: string): StoreResult<RetryCheckpoint>;
  lookup(scope: string, token: string): StoreResult<ReceiptRetry | null>;
  /** Sorted by (due, token), only retry rows where due <= now. */
  due(scope: string, now: number, limit: number): StoreResult<ReceiptRetry[]>;
  /** One transaction: CAS scope/version, checkpoint and every upsert/deletion. */
  commit(scope: string, version: number, next: RetryCheckpoint, changes: ReadonlyMap<string, ReceiptRetry | null>): StoreResult<boolean>;
}

/** Throw only after establishing the operation had no ambiguous side effect (or can safely be retried). */
export class RetryablePublicationError extends Error {}
const maxAttempts = 6;
const tokenShape = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;

export function validateRetry(row: ReceiptRetry, token: string) {
  if (row.token !== token || !tokenShape.test(token) || !Number.isSafeInteger(row.attempts) ||
      row.attempts < 1 || row.attempts > maxAttempts || !Number.isSafeInteger(row.due) || row.due < 0 ||
      !(row.state === "audit" || (row.state === "exhausted" && row.attempts === maxAttempts) ||
      (row.state === "retry" && row.attempts < maxAttempts))) throw Error("invalid receipt retry");
}

/** Unwired scheduler for a single authenticated source. The store is scheduling-only:
 * each actual attempt still uses source.reconcile's idempotent, fenced claim path. */
export async function runPublicationRetryTurn(
  scope: string, source: ReconciliationSource & { scope: string }, store: ReceiptRetryStore,
  limit: number, now: number,
): Promise<"committed" | "conflict"> {
  if (!/^[a-f0-9]{64}$/.test(scope) || source.scope !== scope) throw Error("receipt scope mismatch");
  if (!Number.isInteger(limit) || limit < 1 || limit > 20 || !Number.isSafeInteger(now) ||
      now < 0 || now > Number.MAX_SAFE_INTEGER - 60_000) throw Error("invalid retry budget");
  const checkpoint = await store.load(scope);
  if (!Number.isSafeInteger(checkpoint.version) || checkpoint.version < 0 ||
      checkpoint.version >= Number.MAX_SAFE_INTEGER || typeof checkpoint.retryTurn !== "boolean")
    throw Error("invalid retry checkpoint");
  const changes = new Map<string, ReceiptRetry | null>();
  async function attempt(token: string, attempts: number) {
    let outcome;
    try {
      outcome = await source.reconcile(token);
    } catch (error) {
      const n = attempts + 1;
      const state = error instanceof RetryablePublicationError ? (n === maxAttempts ? "exhausted" : "retry") : "audit";
      changes.set(token, { token, attempts: n, state, due: now + Math.min(60_000, 5_000 * 2 ** (n - 1)) });
      return;
    }
    if (!["published", "already_published", "held", "missing"].includes(outcome)) throw Error("unknown receipt outcome");
    // Held/missing return to ordinary inventory rechecks; they confer no authority.
    if (attempts > 0) changes.set(token, null);
  }
  let scan = checkpoint.scan;
  if (checkpoint.retryTurn) {
    const due = await store.due(scope, now, limit);
    if (due.length > limit) throw Error("oversized retry page");
    const tokens = new Set<string>();
    for (const row of due) {
      validateRetry(row, row.token);
      if (tokens.has(row.token) || row.state !== "retry" || row.due > now) throw Error("invalid due page");
      tokens.add(row.token);
    }
    for (const row of due) await attempt(row.token, row.attempts);
  } else {
    const page = await reconcilePublicationPage(scan, {
      highest: () => source.highest(), page: (after, through, n) => source.page(after, through, n),
      reconcile: async token => {
        const row = await store.lookup(scope, token);
        if (row) validateRetry(row, token);
        else await attempt(token, 0);
        // The legacy page's visit counts are deliberately discarded.
        return "held";
      },
    }, limit);
    scan = page.next;
  }
  const next = { version: checkpoint.version + 1, scan, retryTurn: !checkpoint.retryTurn };
  // Never publish an in-memory advancement. If commit throws, reload by scope before any further attempt.
  return await store.commit(scope, checkpoint.version, next, changes) ? "committed" : "conflict";
}
