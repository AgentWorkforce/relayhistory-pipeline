/**
 * The scope lock (scopeLock) and the search transaction (searchTransaction) run guarded: an 'error'
 * emitted on the checked-out client between statements, as pg does when the connection ends while
 * no query is in flight, fails that key or scope through the consumer instead of escaping as an
 * uncaught exception, and the listener is gone once the client is released. The transaction hands
 * its work `ensureLive`; the pointer work runs it before each statement these fakes record on the
 * main pool, the publication transaction and the move transaction, before each fan-out send and
 * before the brief wakeup; the search pass runs it before each index and facts call. Without a check, or with a check that never throws, the same statements are sent.
 *
 * The client and the pool are fakes (an EventEmitter with query/release, handed out by a connect()
 * that reuses one client); what pg does on a dropped connection is reproduced by emitting 'error'
 * from a separate macrotask, not by a real server.
 */
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { zstdCompressSync } from "node:zlib";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import type { HoopDb } from "../../shared/db.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  prefixAllowed,
  prefixOrgResolver,
  rawChunkKey,
  type ChunkEntry,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type PipelineQueue,
} from "../../shared/vendor.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { scopeLock, searchTransaction, transaction } from "../lambda.ts";
import * as pointerModule from "../pointer.ts";
import { droppedFanout, indexPointer, type PointerDeps, type ScopeLock, type SessionScope } from "../pointer.ts";
import { checkedCalls } from "../search-index.ts";
import { checkedDb } from "../../hoopdb/index.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { dropConnection, dropped, fakePool as fakePoolWith, memoryLock } from "./support.ts";

const scope: SessionScope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_1", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
const key = manifestKey(loc);
const AT = "2026-11-01T10:00:00.000Z";
const put = (k = key) =>
  JSON.stringify({
    Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", eventTime: AT, s3: { bucket: { name: "bucket" }, object: { key: k, eTag: "e", sequencer: "01" } } }],
  });
const bindings = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls" };
const fakePool = () => fakePoolWith((text) => (text.startsWith("SELECT pg_try_advisory_xact_lock") ? [{ ok: true }] : []));
const LOCK_STATEMENTS = ["BEGIN", "SET LOCAL statement_timeout = '60s'", "SET LOCAL lock_timeout = '30s'", "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"];

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

describe("transaction()", () => {
  test("unguarded: no listener, and ensureLive never throws even after an 'error'", async () => {
    const { client, db } = fakePool();
    let checks = 0;
    await transaction(db, async (tx, ensureLive) => {
      expect(client.listenerCount("error")).toBe(0);
      ensureLive();
      checks++;
      await tx.query("SELECT 1");
    });
    expect(checks).toBe(1);
    expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SELECT 1", "COMMIT"]);
    expect(client.released).toEqual(["release()"]);
  });

  test("guarded: an 'error' between statements makes ensureLive and the next statement throw it; no COMMIT or ROLLBACK reaches the client", async () => {
    const { client, db } = fakePool();
    const error = dropped();
    const seen: unknown[] = [];
    const out = await transaction(
      db,
      async (tx, ensureLive) => {
        await tx.query("SELECT 1");
        await dropConnection(client, error);
        try {
          ensureLive();
        } catch (e) {
          seen.push(e);
        }
        await tx.query("SELECT 2");
      },
      true,
    ).catch((e: unknown) => e);
    expect(seen).toEqual([error]);
    expect(out).toBe(error);
    expect(client.statements).toEqual(["BEGIN", "SET LOCAL statement_timeout = '60s'", "SELECT 1"]);
    expect(client.released).toEqual([error]);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("guarded without an 'error': the same statements and release() as unguarded, one listener only while checked out", async () => {
    const run = async (guarded: boolean) => {
      const { client, db } = fakePool();
      await transaction(db, async (tx, ensureLive) => {
        ensureLive();
        await tx.query("SELECT $1", [1]);
      }, guarded);
      return { statements: client.statements, released: client.released, seen: client.listenersSeen, after: client.listenerCount("error") };
    };
    const plain = await run(false);
    const guarded = await run(true);
    expect(guarded.statements).toEqual(plain.statements);
    expect(guarded.released).toEqual(plain.released);
    expect(plain.seen.every((n) => n === 0)).toBe(true);
    expect(guarded.seen.every((n) => n === 1)).toBe(true);
    expect(guarded.after).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------------
// The pointer stage: a real manifest in a filesystem bucket, a recording fake main pool, fan-out
// queue and publication transaction.

let root: string;
let store: ObjectStore;
let reader: ManifestReader;
beforeEach(async () => {
  const base = join(homedir(), ".agent-tmp");
  await mkdir(base, { recursive: true });
  root = await mkdtemp(join(base, "96bulls-guarded-"));
  store = createFsStore({ root });
  reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function writeManifest(revision: number, at: typeof loc = loc) {
  const source = new TextEncoder().encode(
    JSON.stringify({ kind: "session_event", origin_id: "o", record_id: `r${revision}`, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: 1, text: "x" } }) + "\n",
  );
  const stored = new Uint8Array(zstdCompressSync(source));
  const sha = await sha256Hex(source);
  const chunk: ChunkEntry = { seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength, raw_sha256: sha, compression: "zstd", records: 1 };
  await store.put(rawChunkKey(at, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision, created_at: AT, updated_at: AT,
    session: { workspace_id: "ws_1", machine_id: "srv_1", harness: "claude", session_id: "sess_1", started_at: `${at.startDate}T10:00:00.000Z` },
    writer: { delivery_instance_id: "srv_1", plugin_version: "t" },
    raw: { owner: "srv_1", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1, raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0, epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: "srv_1" }], next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 } },
    extensions: { "96bulls": { user_id: "user_1" } },
  } as unknown as ManifestV1;
  const k = manifestKey(at);
  await store.put(objectKey(k), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return k;
}

type World = {
  /** Every statement, send, transaction boundary and check, in order. */
  log: string[];
  deps: PointerDeps;
};

const words = (text: string) => text.trim().split(/\s+/).slice(0, 4).join(" ");

/**
 * Recording fakes. `stored`: the session_index row the stage reads back (absent: none). `pooled`:
 * no publication config, and a pool for the move transaction. `ensureLive`: the lock's check
 * (absent: a lock that passes none, like the self-host consumer's).
 */
function world(opts: { stored?: { manifest_key: string; deleted: boolean }; pooled?: boolean; ensureLive?: () => void; preRead?: boolean }): World {
  const log: string[] = [];
  const answer = (text: string): unknown[] => {
    if (text.includes("session_publication_lock")) return [{ result: { managed: false, current: true, generation: null, revision: "0" } }];
    if (/upsert_session_index|ingest_operator_runtime_upsert/.test(text)) return [{ applied: true, revision: "1" }];
    if (text.includes("deleted_at IS NOT NULL AS deleted")) return opts.stored ? [opts.stored] : [];
    if (text.startsWith("DELETE FROM sessions.session_index")) return [{ created_at: AT }];
    if (text.trim().startsWith("SELECT 1 FROM sessions.session_index")) return [{}];
    return [];
  };
  const record = (where: string) => async (text: string, params?: unknown[]) => {
    // Derived session uuids carry the clock and span ids are random; everything else is compared as sent.
    log.push(`${where}: ${words(text)} ${JSON.stringify(params ?? []).replace(/[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}/g, (u) => (u.startsWith("019a1f3c") ? u : "<uuid>")).replace(/(00-[0-9a-f]{32}-)[0-9a-f]{16}(-01)/g, "$1<span>$2")}`);
    return { rows: answer(text) as never[] };
  };
  const client = { query: record("move"), release() {} };
  const db = {
    query: record("pool"),
    end: async () => {},
    pool: opts.pooled ? { connect: async () => client } : undefined,
  } as unknown as HoopDb;
  const fanout: PipelineQueue = {
    backend: "memory",
    async send(message) {
      log.push(`send: ${message.stage}`);
    },
    async sendBatch(messages) {
      for (const m of messages) log.push(`send: ${m.stage}`);
    },
  };
  const lock: ScopeLock = async (_scope, work) => {
    log.push("lock");
    if (!opts.ensureLive) return work();
    const check = opts.ensureLive;
    return work(() => {
      log.push("check");
      check();
    });
  };
  const deps: PointerDeps = {
    db,
    lock,
    reader,
    exists: async (k) => (await store.head(objectKey(k))) !== undefined,
    orgs: prefixOrgResolver,
    allowPrefix: (p, o) => prefixAllowed(bindings, p, o),
    fanout,
    ...(opts.preRead ? { preRead: true } : {}),
    ...(opts.pooled
      ? {}
      : {
          publication: {
            schemaGeneration: "a".repeat(64),
            transact: async <T,>(work: (tx: { query: HoopDb["query"] }) => Promise<T>) => {
              log.push("publication: BEGIN");
              const value = await work({ query: record("publication") as HoopDb["query"] });
              log.push("publication: COMMIT");
              return value;
            },
          },
        }),
  } as PointerDeps;
  return { log, deps };
}

const landed = (k: string) => ({ provider: "s3" as const, key: k, etag: "e", eventTime: AT });
const sent = (line: string) => /^(pool|publication|move): /.test(line) || line.startsWith("send: ");

describe("scope lock check (pointer stage)", () => {
  // Briefs on, so the wakeup is armed under the lock as well.
  let prior: string | undefined;
  beforeEach(() => {
    prior = process.env.SESSION_BRIEF_ENABLED;
    process.env.SESSION_BRIEF_ENABLED = "true";
  });
  afterEach(() => {
    if (prior === undefined) delete process.env.SESSION_BRIEF_ENABLED;
    else process.env.SESSION_BRIEF_ENABLED = prior;
  });
  const cases: Array<[string, () => Promise<{ stored?: { manifest_key: string; deleted: boolean }; pooled?: boolean; preRead?: boolean; k: string }>]> = [
    ["a new pointer through the publication transaction", async () => ({ k: await writeManifest(1) })],
    ["a new pointer, pre-read", async () => ({ k: await writeManifest(1), preRead: true })],
    ["a moved session through the publication transaction", async () => {
      const old = manifestKey({ ...loc, startDate: "2026-10-31" });
      return { k: await writeManifest(1), stored: { manifest_key: old, deleted: true } };
    }],
    ["a moved session through the move transaction (no publication config)", async () => {
      const old = manifestKey({ ...loc, startDate: "2026-10-31" });
      return { k: await writeManifest(1), stored: { manifest_key: old, deleted: true }, pooled: true };
    }],
    ["a tombstone", async () => ({ k: key })],
  ];

  for (const [name, setup] of cases) {
    test(`${name}: a check before every statement, send and COMMIT; without one, the same log`, async () => {
      const c = await setup();
      const plain = world({ ...c });
      const checked = world({ ...c, ensureLive: () => {} });
      const a = await indexPointer(landed(c.k), plain.deps);
      const b = await indexPointer(landed(c.k), checked.deps);
      expect(b).toEqual(a);
      // The work under the lock: everything after the (last) lock.
      const under = checked.log.slice(checked.log.lastIndexOf("lock") + 1);
      expect(under.filter(sent).length).toBeGreaterThan(0);
      // Every case but the tombstone fans out and arms the brief wakeup under the lock.
      if (name !== "a tombstone") {
        expect(under.some((l) => l.includes("pipeline_jobs") || l.startsWith("send: "))).toBe(true);
        expect(under.some((l) => l.includes("session_brief_wakeups"))).toBe(true);
      }
      for (let i = 0; i < under.length; i++) {
        if (sent(under[i]!) || under[i] === "publication: COMMIT") expect([i, under[i - 1]]).toEqual([i, "check"]);
      }
      // Remove the checks: the same statements, parameters, sends and order.
      expect(checked.log.filter((l) => l !== "check")).toEqual(plain.log);
    });

    test(`${name}: once the check throws, nothing more is sent and the call rejects with it`, async () => {
      const c = await setup();
      const count = world({ ...c, ensureLive: () => {} });
      await indexPointer(landed(c.k), count.deps);
      const total = count.log.filter((l) => l === "check").length;
      expect(total).toBeGreaterThan(0);
      for (let n = 0; n < total; n++) {
        const error = dropped();
        let calls = 0;
        const w = world({ ...c, ensureLive: () => {
          if (calls++ >= n) throw error;
        } });
        const out = await indexPointer(landed(c.k), w.deps).catch((e: unknown) => e);
        expect([n, out]).toEqual([n, error]);
        const failed = w.log.findLastIndex((l) => l === "check");
        // The move transaction's own ROLLBACK (cleanup on its connection) is the only statement allowed.
        expect([n, w.log.slice(failed + 1).filter(sent).filter((l) => l !== "move: ROLLBACK []")]).toEqual([n, []]);
        expect(w.log.slice(failed + 1)).not.toContain("publication: COMMIT");
      }
    });
  }

  test("a lock that passes no check (in-memory, self-host): no check, the work runs as before", async () => {
    const k = await writeManifest(1);
    const w = world({});
    const out = await indexPointer(landed(k), { ...w.deps, lock: memoryLock() });
    expect(out).toMatchObject({ status: "indexed" });
    expect(w.log).not.toContain("check");
  });
});

/**
 * Pointer deps with the real scope lock on the fake pool; the manifest re-read is where the
 * connection drops. The manifest is gone, so the work's one write is the tombstone; `writes` records
 * the statements sent on the pointer stage's other connection.
 */
function droppingDeps(lockDb: HoopDb, onRead: () => Promise<void>, writes: string[] = []): ConsumerDeps["pointer"] {
  const gone: ManifestReader = {
    provider: "s3",
    bucket: "bucket",
    async read() {
      await onRead();
      return null;
    },
  } as unknown as ManifestReader;
  return {
    db: {
      async query<R>(text: string) {
        writes.push(text.trim().split(/\s+/).slice(0, 2).join(" "));
        return { rows: [] as R[] };
      },
      pool: undefined,
      end: async () => {},
    } as unknown as HoopDb,
    lock: scopeLock(lockDb),
    reader: gone,
    exists: async () => false,
    orgs: prefixOrgResolver,
    allowPrefix: (prefix, org) => prefixAllowed(bindings, prefix, org),
    fanout: droppedFanout,
  };
}

describe("scopeLock through processBatch", () => {
  test("no error: the same lock statements and release() as before, the listener only while checked out", async () => {
    const { client, db } = fakePool();
    const writes: string[] = [];
    const out = await processBatch([{ messageId: "m1", body: put() }], { pointer: droppingDeps(db, async () => {}, writes) });
    expect(out.batchItemFailures).toEqual([]);
    expect(writes).toEqual(["SELECT sessions.session_publication_is_managed($1,$2,$3,$4,$5)", "UPDATE sessions.session_index"]);
    expect(client.statements).toEqual([...LOCK_STATEMENTS, "COMMIT"]);
    expect(client.released).toEqual(["release()"]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("a dropped connection while the lock is held fails the key, not the process; the tombstone is not written; the client is destroyed", async () => {
    const { client, db } = fakePool();
    const error = dropped();
    let emitted = 0;
    const writes: string[] = [];
    const deps = droppingDeps(db, async () => {
      if (emitted++ === 0) await dropConnection(client, error);
    }, writes);
    const { result, uncaught } = await catchingUncaught(() => processBatch([{ messageId: "m1", body: put() }], { pointer: deps }));
    expect(uncaught).toEqual([]);
    expect(result.batchItemFailures).toEqual([{ itemIdentifier: "m1" }]);
    // Only the managed-publication read before the lock; the tombstone under the lock is not sent.
    expect(writes).toEqual(["SELECT sessions.session_publication_is_managed($1,$2,$3,$4,$5)"]);
    expect(client.statements).toEqual(LOCK_STATEMENTS);
    expect(client.released).toEqual([error]);
    expect(client.listenerCount("error")).toBe(0);

    // The next checkout of the same client starts with one listener (its own), not two, and succeeds.
    client.statements.length = 0;
    client.listenersSeen.length = 0;
    const next = await processBatch([{ messageId: "m2", body: put() }], { pointer: deps });
    expect(next.batchItemFailures).toEqual([]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.released).toEqual([error, "release()"]);
    expect(client.listenerCount("error")).toBe(0);
  });
});

describe("searchTransaction through processBatch", () => {
  afterEach(() => {
    (pointerModule.indexPointer as unknown as { mockRestore?: () => void }).mockRestore?.();
  });

  /** ClickHouse binding; `onClickHouse` runs before each call and `calls` records them. */
  function searchDeps(transact: NonNullable<ConsumerDeps["search"]>["transact"], onClickHouse: () => Promise<void>, calls: string[] = []): NonNullable<ConsumerDeps["search"]> {
    const ch = {
      async command(q: { query?: string }) {
        calls.push(`command ${words(q?.query ?? "")}`);
        await onClickHouse();
      },
      async select(q: unknown) {
        calls.push(`select ${words(typeof q === "string" ? q : ((q as { query?: string })?.query ?? ""))}`);
        await onClickHouse();
        return [];
      },
      async insert() {
        calls.push("insert");
      },
    } as unknown as ClickHouseClient;
    return { transact, reader: {} as ManifestReader, store: {} as ObjectStore, ch, prefixFor: (org) => `96bulls/${org}` };
  }

  function stubPointer() {
    spyOn(pointerModule, "indexPointer").mockResolvedValue({ status: "indexed", scope, manifestKey: key } as Awaited<ReturnType<typeof pointerModule.indexPointer>>);
  }
  const pointer = { reader: { provider: "s3" } } as unknown as ConsumerDeps["pointer"];

  test("no error: the same transaction statements and ClickHouse calls as an unguarded transaction, release(), the listener only while checked out", async () => {
    stubPointer();
    const run = async (guarded: boolean) => {
      const { client, db } = fakePool();
      const calls: string[] = [];
      const transact: NonNullable<ConsumerDeps["search"]>["transact"] = guarded ? searchTransaction(db) : (work) => transaction(db, (tx) => work(tx));
      const out = await processBatch([{ messageId: "m1", body: put() }], { pointer, search: searchDeps(transact, async () => {}, calls) });
      return { out, client, calls };
    };
    const plain = await run(false);
    const guarded = await run(true);
    expect(guarded.out).toEqual(plain.out);
    expect(guarded.out.batchItemFailures).toEqual([]);
    expect(guarded.calls).toEqual(plain.calls);
    expect(guarded.calls.length).toBeGreaterThan(0);
    expect(guarded.client.statements).toEqual(plain.client.statements);
    expect(guarded.client.statements[0]).toBe("BEGIN");
    expect(guarded.client.statements.at(-1)).toBe("COMMIT");
    expect(guarded.client.released).toEqual(["release()"]);
    expect(guarded.client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(guarded.client.listenerCount("error")).toBe(0);
  });

  test("a dropped connection during the pass fails the scope, not the process; no ClickHouse call follows; the client is destroyed", async () => {
    stubPointer();
    const { client, db } = fakePool();
    const error = dropped();
    let emitted = 0;
    const calls: string[] = [];
    const search = searchDeps(searchTransaction(db), async () => {
      if (emitted++ === 0) await dropConnection(client, error);
    }, calls);
    const { result, uncaught } = await catchingUncaught(() => processBatch([{ messageId: "m1", body: put() }], { pointer, search }));
    expect(emitted).toBeGreaterThan(0);
    expect(uncaught).toEqual([]);
    expect(result.batchItemFailures).toEqual([{ itemIdentifier: "m1" }]);
    // The call during which the connection dropped is the last one: the next index call checks first.
    expect(calls).toHaveLength(1);
    expect(client.statements).not.toContain("COMMIT");
    expect(client.statements).not.toContain("ROLLBACK");
    expect(client.released).toEqual([error]);
    expect(client.listenerCount("error")).toBe(0);

    client.listenersSeen.length = 0;
    const next = await processBatch([{ messageId: "m2", body: put() }], { pointer, search });
    expect(next.batchItemFailures).toEqual([]);
    expect(client.listenersSeen.every((n) => n === 1)).toBe(true);
    expect(client.released).toEqual([error, "release()"]);
    expect(client.listenerCount("error")).toBe(0);
  });

  test("the pass checks before every ClickHouse call it makes in the transaction; without a check, the same statements and calls", async () => {
    stubPointer();
    const run = async (withCheck: boolean) => {
      const log: string[] = [];
      const transact: NonNullable<ConsumerDeps["search"]>["transact"] = (work) => {
        const tx = {
          async query<R>(text: string) {
            log.push(`tx: ${words(text)}`);
            return { rows: (text.startsWith("SELECT pg_try_advisory_xact_lock") ? [{ ok: true }] : []) as R[] };
          },
        };
        return withCheck ? work(tx, () => void log.push("check")) : work(tx);
      };
      const calls: string[] = [];
      const search = searchDeps(transact, async () => {
        log.push(`ch: ${calls.at(-1)}`);
      }, calls);
      const out = await processBatch([{ messageId: "m1", body: put() }], { pointer, search });
      return { out, log };
    };
    const plain = await run(false);
    const checked = await run(true);
    expect(checked.out).toEqual(plain.out);
    const chAt = checked.log.flatMap((l, i) => (l.startsWith("ch: ") ? [i] : []));
    expect(chAt.length).toBeGreaterThan(0);
    for (const i of chAt) expect([i, checked.log[i - 1]]).toEqual([i, "check"]);
    expect(checked.log.filter((l) => l !== "check")).toEqual(plain.log);
  });
});

describe("checkedCalls", () => {
  test("no check: the same object; a check: runs before each method, keeps `this` and non-function fields", async () => {
    const target = {
      kind: "x",
      n: 0,
      async bump(by: number) {
        this.n += by;
        return this.n;
      },
    };
    expect(checkedCalls(target, undefined)).toBe(target);
    const log: string[] = [];
    const checked = checkedCalls(target, () => void log.push("check"));
    expect(checked.kind).toBe("x");
    expect(await checked.bump(2)).toBe(2);
    expect(log).toEqual(["check"]);
    const error = dropped();
    const failing = checkedCalls(target, () => {
      throw error;
    });
    expect(() => failing.bump(1)).toThrow(error);
    expect(target.n).toBe(2);
  });
});

describe("checkedDb", () => {
  test("a check before each query with the same text and parameters; the pool is the same object; a throwing check sends nothing", async () => {
    const sentTexts: Array<[string, unknown[] | undefined]> = [];
    const pool = { connect: async () => ({}) };
    const db = {
      async query<R>(text: string, params?: unknown[]) {
        sentTexts.push([text, params]);
        return { rows: [] as R[] };
      },
      pool,
      end: async () => {},
    } as unknown as HoopDb;
    const log: string[] = [];
    const checked = checkedDb(db, () => void log.push("check"));
    expect(checked.pool).toBe(db.pool);
    await checked.query("SELECT 1", [1]);
    expect(log).toEqual(["check"]);
    expect(sentTexts).toEqual([["SELECT 1", [1]]]);
    const error = dropped();
    const failing = checkedDb(db, () => {
      throw error;
    });
    expect(await failing.query("SELECT 2").catch((e: unknown) => e)).toBe(error);
    expect(sentTexts).toEqual([["SELECT 1", [1]]]);
  });
});
