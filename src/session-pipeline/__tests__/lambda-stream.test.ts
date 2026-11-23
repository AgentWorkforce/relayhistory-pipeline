/**
 * SEARCH_INDEX_STREAMING_CH in the Lambda: parsed strictly, refused without its
 * dependencies, and per invocation only a deadline and a one-scope slot. Unset, the
 * runtime is exactly as before.
 */
import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { handler, invocationDeps, lambdaEnv, searchTransaction, transaction } from "../lambda.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { ConsumerDeps } from "../consumer.ts";
import { SEARCH_INDEX_STREAM_CH_BUDGET, SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS, type SearchIndexDeps } from "../search-index.ts";

const base = {
  AWS_REGION: "us-east-1",
  PIPELINE_RAW_S3_BUCKET: "b",
  PIPELINE_KEY_PREFIX: "/96bulls/",
  PIPELINE_ORG_FROM_PREFIX: "true",
  SESSION_PIPELINE_SECRET_ARN: "arn",
  SESSION_PIPELINE_QUEUE_URL: "https://sqs",
};

describe("SEARCH_INDEX_STREAMING_CH", () => {
  test("unset, empty or 0: the env is exactly as without it", () => {
    const plain = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" });
    expect(Object.keys(plain)).not.toContain("searchStreamCh");
    for (const value of ["", "0"]) expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: value })).toEqual(plain);
  });

  test("1 with SEARCH_INDEX_ENABLED=1 arms it", () => {
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1" })).toEqual({ ...lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" }), searchStreamCh: true });
  });

  test("refuses any other value, and 1 without SEARCH_INDEX_ENABLED=1", () => {
    for (const value of ["true", "2", "on", " 1"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: value })).toThrow("session pipeline: SEARCH_INDEX_STREAMING_CH must be 1 or 0");
    for (const enabled of [undefined, "0", "true"])
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: enabled, SEARCH_INDEX_STREAMING_CH: "1" })).toThrow("session pipeline: SEARCH_INDEX_STREAMING_CH=1 needs SEARCH_INDEX_ENABLED=1");
  });

  test("armed, init refuses a secret without the clickhouse block; nothing in the batch is processed", async () => {
    const saved = { ...process.env };
    const realFetch = globalThis.fetch;
    let calls = 0;
    try {
      Object.assign(process.env, base, { SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1", AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test" });
      // The only call init makes before the check: GetSecretValue, answered here (no network).
      globalThis.fetch = (async () => {
        calls++;
        return new Response(JSON.stringify({ SecretString: JSON.stringify({ hoopdb_url: "postgresql://u:p@127.0.0.1:1/db" }) }), { status: 200 });
      }) as unknown as typeof fetch;
      const out = await handler({ Records: [{ messageId: "m1", body: "{}" }, { messageId: "m2", body: "{}" }] }, { getRemainingTimeInMillis: () => 300_000 });
      expect(out).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }, { itemIdentifier: "m2" }] });
      expect(calls).toBe(1);
    } finally {
      globalThis.fetch = realFetch;
      for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
      Object.assign(process.env, saved);
    }
  });
});

describe("invocationDeps", () => {
  const search = { ch: {} } as unknown as SearchIndexDeps;
  const deps = (s?: SearchIndexDeps): ConsumerDeps => ({ pointer: {} as ConsumerDeps["pointer"], ...(s ? { search: s } : {}) });

  test("unarmed: the runtime deps themselves, untouched", () => {
    const d = deps(search);
    expect(invocationDeps(d, { getRemainingTimeInMillis: () => 1 })).toBe(d);
    const none = deps();
    expect(invocationDeps(none)).toBe(none);
  });

  test("armed: a fresh slot per invocation and the deadline at remaining time less the margin", () => {
    const d = deps({ ...search, stream: { ...SEARCH_INDEX_STREAM_CH_BUDGET } });
    const one = invocationDeps(d, { getRemainingTimeInMillis: () => 300_000 }, () => 1_000);
    expect(one.search).toMatchObject({ streamSlot: { used: false }, streamDeadline: { at: 1_000 + 300_000 - SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS } });
    const two = invocationDeps(d, { getRemainingTimeInMillis: () => 300_000 }, () => 1_000);
    expect(two.search!.streamSlot).not.toBe(one.search!.streamSlot);
    expect(d.search).not.toHaveProperty("streamSlot");
    // No context: the slot still holds, no deadline.
    const bare = invocationDeps(d);
    expect(bare.search).toMatchObject({ streamSlot: { used: false } });
    expect(bare.search).not.toHaveProperty("streamDeadline");
  });

  test("the budgets: four passes over at most 256 MiB / 250,000 records", () => {
    expect(SEARCH_INDEX_STREAM_CH_BUDGET).toEqual({ maxReadBytes: 1024 * 1024 * 1024, maxReadEvents: 1_000_000 });
    expect(SEARCH_INDEX_STREAM_DEADLINE_MARGIN_MS).toBe(60_000);
  });
});

describe("transaction()", () => {
  /** A pg client stand-in: records statements, fails the ones named, records release(). */
  function fakeClient(fail: Record<string, Error> = {}) {
    const client = Object.assign(new EventEmitter(), {
      statements: [] as string[],
      released: [] as unknown[],
      async query(text: string) {
        client.statements.push(text);
        const f = Object.entries(fail).find(([prefix]) => text.startsWith(prefix));
        if (f) throw f[1];
        return { rows: [{ text }] };
      },
      release(error?: unknown) {
        client.released.push(error === undefined ? "release()" : error);
      },
    });
    const db = { pool: { connect: async () => client } } as unknown as HoopDb;
    return { client, db };
  }

  for (const guarded of [false, true]) {
    test(`${guarded ? "guarded" : "unguarded"}: BEGIN, the statement timeout, the work, COMMIT, release() once`, async () => {
      const { client, db } = fakeClient();
      let listeners = -1;
      const out = await transaction(db, async (tx) => {
        listeners = client.listenerCount("error");
        return (await tx.query("SELECT 1")).rows;
      }, guarded);
      expect(out).toEqual([{ text: "SELECT 1" }]);
      expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SELECT 1", "COMMIT"]);
      expect(client.released).toEqual(["release()"]);
      expect(listeners).toBe(guarded ? 1 : 0);
      expect(client.listenerCount("error")).toBe(0);
    });

    test(`${guarded ? "guarded" : "unguarded"}: a failed work rolls back, rethrows its own error, and a failed ROLLBACK is swallowed`, async () => {
      const boom = Object.assign(new Error("boom"), { code: "B" });
      const { client, db } = fakeClient();
      await expect(transaction(db, async () => { throw boom; }, guarded)).rejects.toBe(boom);
      expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "ROLLBACK"]);
      expect(client.released).toEqual(["release()"]);
      const rb = fakeClient({ ROLLBACK: new Error("rollback failed") });
      await expect(transaction(rb.db, async () => { throw boom; }, guarded)).rejects.toBe(boom);
      expect(rb.client.statements.at(-1)).toBe("ROLLBACK");
      expect(rb.client.released).toEqual(["release()"]);
    });

    test(`${guarded ? "guarded" : "unguarded"}: a failed COMMIT rolls back and rejects`, async () => {
      const failed = new Error("commit failed");
      const { client, db } = fakeClient({ COMMIT: failed });
      await expect(transaction(db, async () => 1, guarded)).rejects.toBe(failed);
      expect(client.statements.slice(-2)).toEqual(["COMMIT", "ROLLBACK"]);
    });
  }

  test("guarded: an 'error' emitted while idle in the transaction is kept; the next statement rejects with it and the client is destroyed", async () => {
    const { client, db } = fakeClient();
    const dropped = Object.assign(new Error("Connection terminated unexpectedly"), { code: "ECONNRESET" });
    const uncaught: unknown[] = [];
    const onUncaught = (e: unknown) => void uncaught.push(e);
    process.on("uncaughtException", onUncaught);
    try {
      await expect(transaction(db, async (tx) => {
        await tx.query("SELECT 1");
        // Fails fast if no listener kept the error: emit would throw here instead of the test hanging.
        await new Promise<void>((resolve, reject) =>
          setImmediate(() => {
            try {
              client.emit("error", dropped);
              resolve();
            } catch {
              reject(new Error("the client's 'error' had no listener: emit threw"));
            }
          }),
        );
        await tx.query("SELECT 2");
      }, true)).rejects.toBe(dropped);
    } finally {
      process.off("uncaughtException", onUncaught);
    }
    expect(uncaught).toEqual([]);
    // SELECT 2 and ROLLBACK never reach the dead connection.
    expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SELECT 1"]);
    expect(client.released).toEqual([dropped]);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("unguarded: no listener, so the same emit throws as it always did", async () => {
    const { client, db } = fakeClient();
    await transaction(db, async () => {
      expect(client.listenerCount("error")).toBe(0);
      expect(() => client.emit("error", new Error("x"))).toThrow("x");
    });
  });

  test("the search stage's transaction is guarded exactly when SEARCH_INDEX_STREAMING_CH=1 armed it", async () => {
    for (const [env, guarded] of [[{ searchStreamCh: true as const }, true], [{}, false]] as const) {
      const { client, db } = fakeClient();
      let listeners = -1;
      await searchTransaction(db, env)(async () => {
        listeners = client.listenerCount("error");
      });
      expect(listeners).toBe(guarded ? 1 : 0);
    }
    // And lambdaEnv only ever produces searchStreamCh: true from SEARCH_INDEX_STREAMING_CH=1.
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_STREAMING_CH: "1" }).searchStreamCh).toBe(true);
    expect(lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" }).searchStreamCh).toBeUndefined();
  });
});

describe("init's search transaction (source check)", () => {
  // init() is not exported and needs the secret, a pg pool and S3 to run; driving it end to end
  // would change src or leave a cached runtime behind for other test files. This pins the one
  // line that wires it instead: the search deps take searchTransaction(db, env), whose guard is
  // tested above, and no unguarded transaction(db, work) of their own.
  test("the search deps use searchTransaction(db, env)", () => {
    const source = readFileSync(fileURLToPath(new URL("../lambda.ts", import.meta.url)), "utf8");
    const search = source.slice(source.indexOf("deps.search = {"), source.indexOf("};", source.indexOf("deps.search = {")));
    expect(search).toContain("transact: searchTransaction(db, env),");
    expect(search).not.toMatch(/transaction\(db, work/);
  });
});
