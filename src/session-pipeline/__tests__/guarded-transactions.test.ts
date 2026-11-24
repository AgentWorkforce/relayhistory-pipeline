/**
 * The scope lock (scopeLock) and the unstreamed search transaction (searchTransaction) run guarded:
 * an 'error' emitted on the checked-out client between statements, as pg does when the connection
 * ends while no query is in flight, fails that key or scope through the consumer instead of
 * escaping as an uncaught exception, and the listener is gone once the client is released.
 *
 * The client and the pool are fakes (an EventEmitter with query/release, handed out by a
 * connect() that reuses one client); what pg does on a dropped connection is reproduced by
 * emitting 'error' from a separate macrotask, not by a real server. The pointer and search stages
 * run for real through processBatch, except that the search test stubs indexPointer to hand it
 * a scope.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import type { HoopDb } from "../../shared/db.ts";
import { manifestKey, prefixAllowed, prefixOrgResolver, type ManifestReader, type ObjectStore } from "../../shared/vendor.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { scopeLock, searchTransaction } from "../lambda.ts";
import * as pointerModule from "../pointer.ts";
import { droppedFanout, type SessionScope } from "../pointer.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { dropConnection, dropped, fakePool as fakePoolWith } from "./support.ts";

const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const key = manifestKey({ prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_1", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" });
const put = JSON.stringify({
  Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: "2026-11-01T10:00:00.000Z", s3: { bucket: { name: "bucket" }, object: { key, eTag: "e", sequencer: "01" } } }],
});
const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
const fakePool = () => fakePoolWith((text) => (text.startsWith("SELECT pg_try_advisory_xact_lock") ? [{ ok: true }] : []));


/** Uncaught exceptions while `run` runs (the emit's throw when no listener holds it). */
async function catchingUncaught<T>(run: () => Promise<T>): Promise<{ result: T; uncaught: unknown[] }> {
  const uncaught: unknown[] = [];
  const onUncaught = (e: unknown) => void uncaught.push(e);
  process.on("uncaughtException", onUncaught);
  try {
    const result = await run();
    await new Promise((r) => setImmediate(r));
    return { result, uncaught };
  } finally {
    process.off("uncaughtException", onUncaught);
  }
}

/**
 * Pointer deps with the scope lock on the fake pool; the manifest re-read is where the connection
 * drops. The manifest is gone, so the work's one write is the tombstone; `writes` records the
 * statements sent on the pointer stage's other connection.
 */
function pointerDeps(lockDb: HoopDb, onRead: () => Promise<void>, writes: string[] = []): ConsumerDeps["pointer"] {
  const reader: ManifestReader = {
    provider: "s3",
    bucket: "bucket",
    async read() {
      await onRead();
      return null;
    },
  } as unknown as ManifestReader;
  const rows = {
    async query<R>(text: string) {
      writes.push(text.trim().split(/\s+/).slice(0, 3).join(" "));
      return { rows: [] as R[] };
    },
  };
  return {
    db: { ...rows, pool: undefined, end: async () => {} } as unknown as HoopDb,
    lock: scopeLock(lockDb),
    reader,
    exists: async () => false,
    orgs: prefixOrgResolver,
    allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org),
    fanout: droppedFanout,
  };
}

describe("scope lock (pointer stage)", () => {
  test("no error: the same statements and release() as before, the listener only while checked out", async () => {
    const { client, db } = fakePool();
    const writes: string[] = [];
    const out = await processBatch([{ messageId: "m1", body: put }], { pointer: pointerDeps(db, async () => {}, writes) });
    expect(out.batchItemFailures).toEqual([]);
    expect(writes).toEqual(["UPDATE sessions.session_index SET"]);
    expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SET LOCAL lock_timeout = '30s'", "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "COMMIT"]);
    expect(client.released).toEqual(["release()"]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("a dropped connection while the lock is held fails the key, not the process; the client is destroyed and no listener is left", async () => {
    const { client, db } = fakePool();
    const error = dropped();
    let emitted = 0;
    const writes: string[] = [];
    const deps = pointerDeps(db, async () => {
      if (emitted++ === 0) await dropConnection(client, error);
    }, writes);
    const { result, uncaught } = await catchingUncaught(() => processBatch([{ messageId: "m1", body: put }], { pointer: deps }));
    expect(uncaught).toEqual([]);
    expect(result.batchItemFailures).toEqual([{ itemIdentifier: "m1" }]);
    // The work checks the lock before its tombstone: the write is not made.
    expect(writes).toEqual([]);
    // COMMIT and ROLLBACK never reach the dead connection.
    expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SET LOCAL lock_timeout = '30s'", "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"]);
    expect(client.released).toEqual([error]);
    expect(client.listenerCount("error")).toBe(0);

    // The next checkout of the same client starts with one listener (its own), not two, and succeeds.
    client.statements.length = 0;
    client.listenersSeen.length = 0;
    const next = await processBatch([{ messageId: "m2", body: put }], { pointer: deps });
    expect(next.batchItemFailures).toEqual([]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.released).toEqual([error, "release()"]);
    expect(client.listenerCount("error")).toBe(0);
  });
});

describe("unstreamed search transaction", () => {
  afterEach(() => {
    (pointerModule.indexPointer as unknown as { mockRestore?: () => void }).mockRestore?.();
  });

  /** Search deps with no stream budget (SEARCH_INDEX_STREAMING_CH unset) on the fake pool. */
  function searchDeps(transact: NonNullable<ConsumerDeps["search"]>["transact"], onClickHouse: () => Promise<void>): NonNullable<ConsumerDeps["search"]> {
    const ch = {
      async command() {
        await onClickHouse();
      },
      async select() {
        await onClickHouse();
        return [];
      },
      async insert() {},
    } as unknown as ClickHouseClient;
    return { transact, reader: {} as ManifestReader, store: {} as ObjectStore, ch, prefixFor: (org) => `96bulls/${org}` };
  }

  function stubPointer() {
    spyOn(pointerModule, "indexPointer").mockResolvedValue({ status: "indexed", scope, manifestKey: key } as Awaited<ReturnType<typeof pointerModule.indexPointer>>);
  }
  const pointer = {} as ConsumerDeps["pointer"];

  test("no error: the pass completes and releases with release(), the listener only while checked out", async () => {
    stubPointer();
    const { client, db } = fakePool();
    const out = await processBatch([{ messageId: "m1", body: put }], { pointer, search: searchDeps(searchTransaction(db), async () => {}) });
    expect(out.batchItemFailures).toEqual([]);
    expect(client.statements[0]).toBe("BEGIN");
    expect(client.statements.at(-1)).toBe("COMMIT");
    expect(client.released).toEqual(["release()"]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("a dropped connection during the pass fails the scope, not the process; the client is destroyed and no listener is left", async () => {
    stubPointer();
    const { client, db } = fakePool();
    const error = dropped();
    let emitted = 0;
    const search = searchDeps(searchTransaction(db), async () => {
      if (emitted++ === 0) await dropConnection(client, error);
    });
    const { result, uncaught } = await catchingUncaught(() => processBatch([{ messageId: "m1", body: put }], { pointer, search }));
    expect(emitted).toBeGreaterThan(0);
    expect(uncaught).toEqual([]);
    expect(result.batchItemFailures).toEqual([{ itemIdentifier: "m1" }]);
    expect(client.statements).not.toContain("COMMIT");
    expect(client.statements).not.toContain("ROLLBACK");
    expect(client.released).toEqual([error]);
    expect(client.listenerCount("error")).toBe(0);

    // The next checkout of the same client starts with one listener (its own), not two, and succeeds.
    client.listenersSeen.length = 0;
    const next = await processBatch([{ messageId: "m2", body: put }], { pointer, search });
    expect(next.batchItemFailures).toEqual([]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.released).toEqual([error, "release()"]);
    expect(client.listenerCount("error")).toBe(0);
  });
});
