import { AsyncLocalStorage } from "node:async_hooks";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";

/** One writer runtime's SQL binding. An index operation inside a search pass
 * uses that pass's checked-out client, rather than waiting for another pool
 * slot while holding the first. Explicit calls outside a pass retain the pool.
 * Each async callback chain has its own binding; no global current transaction.
 */
export function writerTransactionSql(pool: SqlExecutor) {
  const current = new AsyncLocalStorage<{ sql: SqlExecutor; active: boolean }>();
  const sql: SqlExecutor = {
    query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const context = current.getStore();
      if (context && !context.active) throw new Error("Writer transaction is closed");
      return (context?.sql ?? pool).query<R>(text, params);
    },
  };
  return {
    sql,
    run<T>(tx: SqlExecutor, work: () => Promise<T>): Promise<T> {
      const context = { sql: tx, active: true };
      return current.run(context, async () => {
        try { return await work(); }
        finally { context.active = false; }
      });
    },
  };
}
