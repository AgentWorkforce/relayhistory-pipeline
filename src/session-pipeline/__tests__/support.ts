import { EventEmitter } from "node:events";
import type { HoopDb } from "../../shared/db.ts";
import { scopeLockKey, type ScopeLock } from "../pointer.ts";

/** An in-process stand-in for the Supabase advisory lock: one holder per scope at a time. */
export function memoryLock(): ScopeLock {
  const tails = new Map<string, Promise<unknown>>();
  return async (scope, work) => {
    const key = scopeLockKey(scope);
    const prior = tails.get(key) ?? Promise.resolve();
    const next = () => work(() => {});
    const run = prior.then(next, next);
    tails.set(key, run.catch(() => {}));
    return run;
  };
}

/**
 * A fake pg pool: one client for every checkout (as a pool reuses an idle client), recording
 * statements, releases and the 'error' listener count at each statement. `answer` gives the rows
 * of each statement (none by default).
 */
export function fakePool(answer: (text: string, params?: unknown[]) => unknown[] = () => []) {
  const client = Object.assign(new EventEmitter(), {
    statements: [] as string[],
    released: [] as unknown[],
    listenersSeen: [] as number[],
    async query(text: string, params?: unknown[]) {
      client.statements.push(text);
      client.listenersSeen.push(client.listenerCount("error"));
      return { rows: answer(text, params) };
    },
    release(error?: unknown) {
      client.released.push(error === undefined ? "release()" : error);
    },
  });
  const db = { pool: { connect: async () => client } } as unknown as HoopDb;
  return { client, db };
}

export const dropped = () => Object.assign(new Error("Connection terminated unexpectedly"), { code: "ECONNRESET" });

/**
 * What pg does when the server ends an idle session: 'error' from its own macrotask, outside any
 * query. With no listener on the client the emit throws there, outside the batch's promise chain;
 * the test runner reports that as an unhandled error and the test fails (checked by hand with
 * scopeLock and searchTransaction unguarded).
 */
export async function dropConnection(client: EventEmitter, error: Error) {
  await new Promise<void>((resolve) =>
    setImmediate(() => {
      try {
        client.emit("error", error);
      } finally {
        resolve();
      }
    }),
  );
}
