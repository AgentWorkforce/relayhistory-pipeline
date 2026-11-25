/**
 * The pipeline pool's checkout timeout: a stalled pooler fails a checkout after HOOPDB_CONNECT_TIMEOUT_MS instead of
 * holding the invocation until the Lambda timeout. Synthetic URLs only; no real database.
 */
import { expect, test } from "bun:test";
import { createServer } from "node:net";
import { HOOPDB_CONNECT_TIMEOUT_MS, supabasePool } from "../lambda.ts";

test("the pool is built with a 10 s checkout timeout and the given max", async () => {
  expect(HOOPDB_CONNECT_TIMEOUT_MS).toBe(10_000);
  const db = supabasePool("postgresql://u:p@127.0.0.1:1/db", 3);
  try {
    const o = (db.pool as unknown as { options: Record<string, unknown> }).options;
    expect(o.connectionTimeoutMillis).toBe(10_000);
    expect(o.max).toBe(3);
  } finally {
    await db.end();
  }
});

test("a server that accepts TCP but never answers fails the checkout instead of hanging", async () => {
  // Accepts the socket and stays silent, like a stalled pooler.
  const held: import("node:net").Socket[] = [];
  const server = createServer((s) => { held.push(s); });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  const db = supabasePool(`postgresql://u:p@127.0.0.1:${port}/db`, 1);
  // Shorten the wait for the test only; the production value is pinned above.
  (db.pool as unknown as { options: { connectionTimeoutMillis: number } }).options.connectionTimeoutMillis = 300;
  const started = Date.now();
  try {
    await expect(db.pool.connect()).rejects.toThrow();
    expect(Date.now() - started).toBeLessThan(5_000);
  } finally {
    for (const s of held) s.destroy();
    await db.end().catch(() => {});
    server.close();
  }
});
