import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { composeSearchService } from "../../shared/search-composition.ts";
import { postgresSearchState } from "../postgres-search-state.ts";
import { writerTransactionSql } from "../transaction-sql.ts";
import {
  consumerServiceConfig,
  consumerServiceEnabled,
  parseConsumerSecret,
  strictMinutes,
} from "../consumer-config.ts";
import {
  bootstrapCheckOwner,
  bootstrapLock,
  ensureServiceTables,
  runBootstrap,
  stripPsqlMeta,
} from "../consumer-bootstrap.ts";
import {
  assertRuntimeIdentity,
  assertSearchTenantGate,
  assertServiceSchema,
  clearUnhealthyFile,
  collectFreshnessSend,
  computeSince,
  deletePending,
  gateDeferredSends,
  HIGH_WATER_MARGIN_MS,
  loadHighWaterMark,
  loadPendingDeferred,
  loadPendingDue,
  loadPendingMap,
  loadReconcileCursor,
  makeDedupe,
  MAX_ATTEMPTS,
  MAX_CONSECUTIVE_TICK_FAILURES,
  nextFailureCount,
  pendingKey,
  reconcileMeta,
  reconcileStaleProjections,
  reconcileTombstonedScopes,
  refuseStartupOptions,
  retryDelayMs,
  runConsumerTick,
  selectResumeCursor,
  shouldSkipDeadLetter,
  storeHighWaterMark,
  storePending,
  storeReconcileCursor,
  systemUserIdentifies,
  scoreFailedAttempt,
  scoreRequeuedAttempt,
  writerComposition,
  type ConsumerTickIo,
} from "../consumer-service.ts";
import { refuseEnvOptions, reconcileRecent, supabasePool } from "../lambda.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { TenantRunner } from "../../session-read-api/liveness.ts";

const SECRET = JSON.stringify({ hoopdb_url: "postgresql://owner:secret@postgres:5432/hoopdb" });

function env(overrides: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  return {
    RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true",
    RELAYHISTORY_PIPELINE_CONSUMER_SECRET: SECRET,
    SESSION_POSTGRES_TLS_PROFILE: "custom-ca",
    SESSION_POSTGRES_CA_FILE: "/run/tls/ca.crt",
    SESSION_POSTGRES_CA_SHA256: "ab".repeat(32),
    SEARCH_INDEX_BACKEND: "postgres",
    SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
    READ_API_COMPLETE_SINCE: "epoch",
    PIPELINE_KEY_PREFIX: "96bulls/org_1",
    ...overrides,
  };
}

const asDb = (pg: PGlite): SqlExecutor => ({
  query: async <R>(text: string, params?: unknown[]): Promise<{ rows: R[] }> => {
    const r = await pg.query(text, params as unknown[]);
    return { rows: r.rows as R[] };
  },
});

describe("consumer service flag", () => {
  for (const raw of [undefined, "", "false"]) {
    test(`dormant for ${JSON.stringify(raw)} without reading anything else`, () => {
      expect(consumerServiceEnabled({ RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: raw })).toBe(false);
      expect(consumerServiceConfig({ RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: raw })).toEqual({ enabled: false });
    });
  }
  test("anything but exactly true fails loudly", () => {
    expect(() => consumerServiceEnabled({ RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: " True " })).toThrow(
      "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER must be true or false",
    );
    expect(() => consumerServiceEnabled({ RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "1" })).toThrow(
      "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER must be true or false",
    );
  });
});

describe("consumer service config", () => {
  test("minimal valid config enables with the reader floor", () => {
    const config = consumerServiceConfig(env());
    expect(config).toMatchObject({ enabled: true, backendId: "selfhost-pg", keyPrefix: "96bulls/org_1", floorMs: 0 });
  });
  test("floor follows READ_API_COMPLETE_SINCE and refuses junk", () => {
    expect(consumerServiceConfig(env({ READ_API_COMPLETE_SINCE: "2026-01-01T00:00:00Z" }))).toMatchObject({
      floorMs: Date.parse("2026-01-01T00:00:00Z"),
    });
    expect(() => consumerServiceConfig(env({ READ_API_COMPLETE_SINCE: undefined }))).toThrow(
      "READ_API_COMPLETE_SINCE",
    );
    expect(() => consumerServiceConfig(env({ READ_API_COMPLETE_SINCE: "yesterday" }))).toThrow(
      "READ_API_COMPLETE_SINCE",
    );
  });
  test("TLS profile is required explicitly (no silent supabase pin)", () => {
    expect(() => consumerServiceConfig(env({ SESSION_POSTGRES_TLS_PROFILE: undefined }))).toThrow(
      "SESSION_POSTGRES_TLS_PROFILE",
    );
    expect(consumerServiceConfig(env({ SESSION_POSTGRES_TLS_PROFILE: "supabase" }))).toMatchObject({ enabled: true });
  });
  test("secret is required and named", () => {
    expect(() => consumerServiceConfig(env({ RELAYHISTORY_PIPELINE_CONSUMER_SECRET: undefined }))).toThrow(
      "RELAYHISTORY_PIPELINE_CONSUMER_SECRET",
    );
    expect(() => consumerServiceConfig(env({ RELAYHISTORY_PIPELINE_CONSUMER_SECRET: "nope" }))).toThrow(
      "pipeline consumer secret is not JSON",
    );
    expect(() => consumerServiceConfig(env({ RELAYHISTORY_PIPELINE_CONSUMER_SECRET: "{}" }))).toThrow(
      "pipeline consumer secret has no hoopdb_url",
    );
  });
  test("hosted clickhouse block is refused", () => {
    const secret = JSON.stringify({ hoopdb_url: "postgresql://o@h/db", clickhouse: { url: "http://ch", user: "u", password: "p", database: "d" } });
    expect(() => parseConsumerSecret(secret)).toThrow("must not carry a clickhouse block");
  });
  test("only the postgres backend is accepted", () => {
    expect(() => consumerServiceConfig(env({ SEARCH_INDEX_BACKEND: undefined }))).toThrow(
      "SEARCH_INDEX_BACKEND must be postgres",
    );
    expect(() => consumerServiceConfig(env({ SEARCH_INDEX_BACKEND: "clickhouse" }))).toThrow(
      "SEARCH_INDEX_BACKEND must be postgres",
    );
  });
  test("backend id and key prefix are required and named", () => {
    expect(() => consumerServiceConfig(env({ SEARCH_INDEX_POSTGRES_BACKEND_ID: undefined }))).toThrow(
      "SEARCH_INDEX_POSTGRES_BACKEND_ID",
    );
    expect(() => consumerServiceConfig(env({ PIPELINE_KEY_PREFIX: undefined }))).toThrow("PIPELINE_KEY_PREFIX");
  });
  test("fanout postgres is refused: the consumer is terminal", () => {
    expect(consumerServiceConfig(env())).toMatchObject({ enabled: true, fanout: "off" });
    expect(() => consumerServiceConfig(env({ PIPELINE_FANOUT: "postgres" }))).toThrow(
      "PIPELINE_FANOUT must be off",
    );
    expect(() => consumerServiceConfig(env({ PIPELINE_FANOUT: "sqs" }))).toThrow(
      "PIPELINE_FANOUT must be off",
    );
  });
  test("knobs are strict positive integers", () => {
    expect(strictMinutes({}, "X", 30)).toBe(30);
    expect(strictMinutes({ X: "" }, "X", 30)).toBe(30);
    expect(strictMinutes({ X: "5" }, "X", 30)).toBe(5);
    for (const bad of ["abc", "0", "-1", "5m", "1.5"]) {
      expect(() => strictMinutes({ X: bad }, "X", 30)).toThrow("X must be an integer number of minutes");
    }
    expect(() => consumerServiceConfig(env({ PIPELINE_RECONCILE_LOOKBACK_MINUTES: "nope" }))).toThrow(
      "PIPELINE_RECONCILE_LOOKBACK_MINUTES",
    );
  });
  test("lookback must exceed the poll cadence", () => {
    expect(() =>
      consumerServiceConfig(env({ PIPELINE_RECONCILE_LOOKBACK_MINUTES: "5", CONSUMER_RECONCILE_EVERY_MINUTES: "5" })),
    ).toThrow("must be greater than");
  });
});

describe("writer composition", () => {
  test("valid backend id composes with the shared floor", () => {
    expect(writerComposition("selfhost-pg", 0).registry.registered()).toContain("postgres");
  });
  test("malformed backend id fails at binding time, before any write", () => {
    const sql = { query: async () => ({ rows: [] }) };
    expect(() =>
      composeSearchService(
        writerComposition("bad id;--", 0),
        { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql, secrets: undefined },
        "writer",
      ),
    ).toThrow("search state configuration invalid");
  });
});

function tickIo(
  consume: ConsumerTickIo["consume"],
  reconcileBodies: string[] = [],
  staleBodies: string[] = [],
): ConsumerTickIo & { calls: string[][] } {
  const calls: string[][] = [];
  return {
    calls,
    pending: [],
    requeued: [],
    reconcile: async (send, since) => {
      expect(since).toBeInstanceOf(Date);
      for (const body of reconcileBodies) await send(body);
      return { enqueued: reconcileBodies.length, complete: true };
    },
    checkFreshness: async (send) => {
      for (const body of staleBodies) await send(body);
      return { stale: staleBodies.length, tombstoned: 0, overflow: false };
    },
    consume: async (records) => {
      calls.push(records.map((r) => r.body));
      return consume(records);
    },
  };
}

const ok = () => Promise.resolve({ batchItemFailures: [] as { itemIdentifier: string }[] });

describe("consumer tick", () => {
  test("collects reconcile bodies and consumes them once", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"a"}}`, `{"reconcile":{"key":"b"}}`]);
    const summary = await runConsumerTick(io, new Date("2026-11-01T00:00:00Z"));
    expect(io.calls).toHaveLength(1);
    expect(io.calls[0]).toHaveLength(2);
    expect(summary).toMatchObject({ collected: 2, failed: 0, requeued: 0, pending: 0, stale: 0, tombstoned: 0, complete: true });
    expect(summary.failedItems).toEqual([]);
  });
  test("stale sessions join the same batch and block completion", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"a"}}`], [`{"reconcile":{"key":"stale"}}`]);
    const summary = await runConsumerTick(io, new Date());
    expect(summary).toMatchObject({ collected: 2, stale: 1, complete: false });
    expect(io.calls[0]).toContain(`{"reconcile":{"key":"stale"}}`);
  });
  test("failures return to pending and are retried next tick", async () => {
    let failOpen = true;
    const io = tickIo(
      async (records) => {
        if (failOpen && records[0]?.body.includes("flaky")) {
          failOpen = false;
          return { batchItemFailures: [{ itemIdentifier: records[0]!.messageId }] };
        }
        return ok();
      },
      [`{"reconcile":{"key":"flaky"}}`],
    );
    const first = await runConsumerTick(io, new Date());
    expect(first).toMatchObject({ collected: 1, failed: 1, pending: 1, complete: false });
    expect(first.failedKeys).toEqual(["flaky"]);
    expect(first.failedItems).toEqual([{ key: "flaky" }]);
    const second = await runConsumerTick({ ...io, reconcile: async () => ({ enqueued: 0, complete: true }) }, new Date());
    expect(second).toMatchObject({ collected: 1, failed: 0, pending: 0, complete: true });
    expect(io.calls).toHaveLength(2);
    expect(io.calls[1]).toEqual([`{"reconcile":{"key":"flaky"}}`]);
  });
  test("busy requeues are tracked separately and block completion", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"hot"}}`]);
    io.consume = async (records) => {
      for (const record of records) io.requeued.push(record.body);
      return ok();
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary).toMatchObject({ requeued: 1, pending: 1, complete: false });
    expect(summary.requeuedKeys).toEqual(["hot"]);
  });
  test("a newer successful revision beats an older failed one for the same key", async () => {
    // Pre-consume collapse keeps only the last invocation per key, so the
    // failure below names the consumed (newer) revision only: per-key outcome
    // is the final revision's, and the tick completes.
    const io = tickIo(async (records) => {
      const failures = records
        .filter((r) => r.body.includes('"e1"'))
        .map((r) => ({ itemIdentifier: r.messageId }));
      return { batchItemFailures: failures };
    }, [`{"reconcile":{"key":"k","etag":"e1"}}`, `{"reconcile":{"key":"k","etag":"e2"}}`]);
    const summary = await runConsumerTick(io, new Date());
    expect(summary.failedKeys).toEqual([]);
    expect(summary.consumedKeys).toEqual(["k"]);
    expect(summary).toMatchObject({ failed: 0, complete: true });
    expect(io.pending).toEqual([]);
  });
  test("dedupe drops dead bodies before consume", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"dead"}}`, `{"reconcile":{"key":"live"}}`]);
    io.dedupe = async (bodies) => bodies.filter((b) => !b.includes("dead"));
    const summary = await runConsumerTick(io, new Date());
    expect(io.calls[0]).toEqual([`{"reconcile":{"key":"live"}}`]);
    expect(summary).toMatchObject({ collected: 2, complete: true });
  });
  test("a batch error fails every body and reports for rethrow", async () => {
    const io = tickIo(
      async () => {
        throw Object.assign(new Error("store down"), { code: "store_unavailable" });
      },
      [`{"reconcile":{"key":"a"}}`],
    );
    const summary = await runConsumerTick(io, new Date());
    // Every body counts as failed (so the service persists an attempt for
    // each) and the error is reported for the service to count and rethrow.
    expect(summary).toMatchObject({ collected: 1, failed: 1, complete: false });
    expect((summary.batchError as Error).message).toBe("store down");
    expect(summary.failedKeys).toEqual(["a"]);
    expect(summary.failedItems).toEqual([{ key: "a" }]);
    expect(io.pending).toEqual([`{"reconcile":{"key":"a"}}`]);
  });
  test("a listing error propagates and keeps pending intact", async () => {
    const io = tickIo(ok);
    io.pending.push(`{"reconcile":{"key":"kept"}}`);
    io.reconcile = async () => {
      throw Object.assign(new Error("locker down"), { code: "locker_unavailable" });
    };
    await expect(runConsumerTick(io, new Date())).rejects.toThrow("locker down");
    expect(io.pending).toEqual([`{"reconcile":{"key":"kept"}}`]);
    expect(io.calls).toHaveLength(0);
  });
  test("an incomplete reconcile chain is reported, not completed", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"a"}}`]);
    io.reconcile = async (send) => {
      await send(`{"reconcile":{"key":"a"}}`);
      return { enqueued: 1, complete: false };
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary).toMatchObject({ collected: 1, failed: 0, complete: false, overflow: true });
  });
  test("freshness overflow blocks completion", async () => {
    const io = tickIo(ok);
    io.checkFreshness = async () => ({ stale: 0, tombstoned: 0, overflow: true });
    const summary = await runConsumerTick(io, new Date());
    expect(summary).toMatchObject({ overflow: true, complete: false });
  });
  test("tombstoned sessions block completion until erased", async () => {
    const io = tickIo(ok);
    io.checkFreshness = async (send) => {
      await send(`{"reconcile":{"key":"tombed"}}`);
      return { stale: 0, tombstoned: 1, overflow: false };
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary).toMatchObject({ tombstoned: 1, complete: false });
    expect(io.calls[0]).toContain(`{"reconcile":{"key":"tombed"}}`);
  });
  test("succeeded identities cover only consumed bodies, never skipped ones", async () => {
    const io = tickIo(ok, [
      `{"reconcile":{"key":"skipped"}}`,
      `{"reconcile":{"key":"consumed","etag":"e1"}}`,
    ]);
    // The skipped body was never consumed, so it must not count as succeeded.
    io.dedupe = async (bodies) => bodies.filter((b) => !b.includes("skipped"));
    const summary = await runConsumerTick(io, new Date());
    expect(summary.consumedKeys).toEqual(["consumed"]);
    expect(summary).toMatchObject({ collected: 2, failed: 0, complete: true });
  });
  test("requeues carry the consumed revision and park the revision body", async () => {
    const io = tickIo(ok, [`{"reconcile":{"key":"hot","etag":"e3"}}`]);
    io.consume = async (records) => {
      for (const record of records) io.requeued.push(JSON.stringify({ reconcile: { key: "hot" } }));
      return ok();
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary.requeuedKeys).toEqual(["hot"]);
    // The etag-less callback body is replaced by the consumed revision for
    // both accounting and the parked retry.
    expect(summary.requeuedItems).toEqual([{ key: "hot", etag: "e3" }]);
    expect(io.pending).toEqual([`{"reconcile":{"key":"hot","etag":"e3"}}`]);
  });
  test("reconcile cursor passes through the tick summary", async () => {
    const io = tickIo(ok);
    io.reconcile = async (send) => {
      await send(`{"reconcile":{"key":"a"}}`);
      return { enqueued: 1, complete: false, nextCursor: "c1" };
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary.nextCursor).toBe("c1");
    expect(summary).toMatchObject({ complete: false, overflow: true });
  });
  test("a duplicate key scores and parks once, on its newest revision", async () => {
    // A due retry (etag e1) arrives first, then the listing's current
    // revision (e2). An etag-less busy requeue for the e2 invocation must
    // carry e2, not the stale first-seen e1.
    const io = tickIo(ok, [
      `{"reconcile":{"key":"hot","etag":"e1"}}`,
      `{"reconcile":{"key":"hot","etag":"e2"}}`,
    ]);
    io.consume = async (records) => {
      for (const record of records) io.requeued.push(JSON.stringify({ reconcile: { key: "hot" } }));
      return ok();
    };
    const summary = await runConsumerTick(io, new Date());
    expect(summary.requeuedKeys).toEqual(["hot"]);
    expect(summary.requeuedItems).toEqual([{ key: "hot", etag: "e2" }]);
    expect(io.pending).toEqual([`{"reconcile":{"key":"hot","etag":"e2"}}`]);
    expect(summary).toMatchObject({ failed: 0, requeued: 1, complete: false });
  });
  test("an unversioned freshness re-send never discards the listing revision", async () => {
    // Production order per tick: due, listing, then freshness. A due retry
    // (e1), the listing's current revision (e2), then a revision-less
    // freshness re-send of the same key: the collapse must keep e2 (a busy
    // or failure parks and scores e2), not the last-positioned unversioned
    // body.
    const io = tickIo(
      ok,
      [
        `{"reconcile":{"key":"hot","etag":"e1"}}`,
        `{"reconcile":{"key":"hot","etag":"e2"}}`,
      ],
      [`{"reconcile":{"key":"hot"}}`],
    );
    const consumed: string[][] = [];
    io.consume = async (records) => {
      consumed.push(records.map((r) => r.body));
      for (const record of records) io.requeued.push(JSON.stringify({ reconcile: { key: "hot" } }));
      return ok();
    };
    const summary = await runConsumerTick(io, new Date());
    expect(consumed).toEqual([[`{"reconcile":{"key":"hot","etag":"e2"}}`]]);
    expect(summary.requeuedKeys).toEqual(["hot"]);
    expect(summary.requeuedItems).toEqual([{ key: "hot", etag: "e2" }]);
    expect(io.pending).toEqual([`{"reconcile":{"key":"hot","etag":"e2"}}`]);
    expect(summary).toMatchObject({ failed: 0, requeued: 1, complete: false });
  });
});

describe("dead-letter skip rule", () => {
  test("etag-less bodies never revive a dead row; a new revision does", () => {
    const dead = { attempts: 25, dead: true, etag: "e1", version: null };
    // Same revision: skip. Etag-less: skip (it cannot prove it is newer).
    expect(shouldSkipDeadLetter(dead, { key: "k", etag: "e1" })).toBe(true);
    expect(shouldSkipDeadLetter(dead, { key: "k" })).toBe(true);
    // New revision: revive. Live rows and non-reconcile bodies: keep.
    expect(shouldSkipDeadLetter(dead, { key: "k", etag: "e2" })).toBe(false);
    expect(shouldSkipDeadLetter({ attempts: 1, dead: false, etag: "e1", version: null }, { key: "k", etag: "e1" })).toBe(false);
    expect(shouldSkipDeadLetter(dead, null)).toBe(false);
    expect(shouldSkipDeadLetter(undefined, { key: "k" })).toBe(false);
  });
  test("makeDedupe drops dead same-revision and etag-less bodies on real rows", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-01T12:00:00Z");
      const past = new Date(at.getTime() - 1000);
      await storePending(db, "b", [
        { manifestKey: "k-dead-same", manifestEtag: "e1", attempts: 25, nextAt: past, lastErrorCode: "x", dead: true },
        { manifestKey: "k-dead-diff", manifestEtag: "e1", attempts: 25, nextAt: past, lastErrorCode: "x", dead: true },
        { manifestKey: "k-live", manifestEtag: "e1", attempts: 2, nextAt: past, lastErrorCode: "x", dead: false },
      ], at);
      const out = await makeDedupe(db, "b")([
        `{"reconcile":{"key":"k-dead-same","etag":"e1"}}`,
        `{"reconcile":{"key":"k-dead-same"}}`,
        `{"reconcile":{"key":"k-dead-diff","etag":"e2"}}`,
        `{"reconcile":{"key":"k-live","etag":"e1"}}`,
        `{"reconcile":{"key":"k-new"}}`,
      ]);
      expect(out).toEqual([
        `{"reconcile":{"key":"k-dead-diff","etag":"e2"}}`,
        `{"reconcile":{"key":"k-live","etag":"e1"}}`,
        `{"reconcile":{"key":"k-new"}}`,
      ]);
    });
  }, 120_000);
  test("failure storage keeps the recorded revision when the failure body has none", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-01T12:00:00Z");
      await storePending(db, "b", [
        { manifestKey: "k", manifestEtag: "e9", manifestVersion: "v9", attempts: 3, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "x", dead: false },
      ], at);
      // The retry body carries no revision; the recorded one must survive.
      await storePending(db, "b", [
        { manifestKey: "k", attempts: 4, nextAt: new Date(at.getTime() + 60_000), lastErrorCode: "y", dead: false },
      ], at);
      expect(await loadPendingMap(db, "b", ["k"])).toEqual(new Map([["k", { attempts: 4, dead: false, etag: "e9", version: "v9" }]]));
    });
  }, 120_000);
  test("deferred rows are gated from re-sends but still load", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-01T12:00:00Z");
      await storePending(db, "b", [
        { manifestKey: "k-due", attempts: 1, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "x", dead: false },
        { manifestKey: "k-wait", attempts: 1, nextAt: new Date(at.getTime() + 60_000), lastErrorCode: "x", dead: false },
      ], at);
      expect(await loadPendingDeferred(db, "b", at)).toEqual(new Set(["k-wait"]));
      const sent: string[] = [];
      const gated = gateDeferredSends(new Set(["k-wait"]), async (body) => { sent.push(body); });
      await gated.send(`{"reconcile":{"key":"k-wait"}}`);
      await gated.send(`{"reconcile":{"key":"k-due","etag":"e1"}}`);
      expect(sent).toEqual([`{"reconcile":{"key":"k-due","etag":"e1"}}`]);
      expect(gated.skipped()).toBe(1);
    });
  }, 120_000);
  test("reconcile cursor round-trips per backend and clears on completion", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-02T00:00:00Z");
      expect(await loadReconcileCursor(db, "b")).toBeNull();
      await storeReconcileCursor(db, "b", { cursor: "c1", since: new Date("2026-11-01T00:00:00Z"), lookbackMinutes: 30, keyPrefix: "p" }, at);
      expect(await loadReconcileCursor(db, "b")).toEqual({ cursor: "c1", since: new Date("2026-11-01T00:00:00Z"), lookbackMinutes: 30, keyPrefix: "p" });
      await storeReconcileCursor(db, "b", null, at);
      expect(await loadReconcileCursor(db, "b")).toBeNull();
    });
  }, 120_000);
  test("a cursor from another key prefix or window never resumes", async () => {
    const stored = { cursor: "c1", since: new Date("2026-11-01T00:00:00Z"), lookbackMinutes: 30, keyPrefix: "z" };
    // Same window, different prefix: restart, or the new namespace's head is skipped.
    expect(selectResumeCursor(stored, 30, "a")).toBeNull();
    // Same prefix, different window: restart.
    expect(selectResumeCursor(stored, 60, "z")).toBeNull();
    // Exact match resumes.
    expect(selectResumeCursor(stored, 30, "z")).toEqual(stored);
    expect(selectResumeCursor(null, 30, "z")).toBeNull();
  });
});

describe("reconcile listing resume", () => {
  test("an interrupted listing resumes from its stored cursor without resending", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      const key = (session: string) => `p/testorg/v1/ws=w/m=m/h=h/d=2026/11/01/s=${session}/manifest.json`;
      const item = (session: string, etag: string) => ({
        key: key(session),
        size: 1,
        lastModified: new Date("2026-11-02T00:00:00Z"),
        version: { eTag: `"${etag}"` },
        metadata: {},
      });
      const pages: Record<string, { items: unknown[]; nextCursor?: string }> = {
        "": { items: [item("a", "e1"), item("b", "e2")], nextCursor: "c1" },
        c1: { items: [item("c", "e3")] },
      };
      const store = {
        list: async (_prefix: unknown, cursor?: string) => pages[cursor ?? ""] ?? { items: [] },
        head: async (_key: unknown) => undefined,
      } as unknown as Parameters<typeof reconcileRecent>[0]["store"];
      const emptyDb = { query: async <R>() => ({ rows: [] as R[] }) };
      const since = new Date("2026-11-01T00:00:00Z");
      const at = new Date("2026-11-02T01:00:00Z");
      // First link lists one page and stops: cursor persisted, fixed since.
      const sent1: string[] = [];
      const link1 = await reconcileRecent({ db: emptyDb, store, keyPrefix: "p/testorg", send: async (b) => { sent1.push(b); }, since, maxPages: 1 });
      expect(link1.complete).toBe(false);
      expect(link1.nextCursor).toBe("c1");
      expect(sent1).toHaveLength(2);
      await storeReconcileCursor(db, "b", { cursor: link1.nextCursor!, since, lookbackMinutes: 30, keyPrefix: "p/testorg" }, at);
      // Next tick resumes the stored cursor with the same since: only the
      // tail page is listed, and the chain completes.
      const stored = await loadReconcileCursor(db, "b");
      expect(stored).toMatchObject({ cursor: "c1", lookbackMinutes: 30, keyPrefix: "p/testorg" });
      expect(selectResumeCursor(stored, 30, "p/testorg")).not.toBeNull();
      expect(selectResumeCursor(stored, 30, "p/other")).toBeNull();
      const sent2: string[] = [];
      const link2 = await reconcileRecent({
        db: emptyDb, store, keyPrefix: "p/testorg",
        send: async (b) => { sent2.push(b); },
        since: stored!.since, cursor: stored!.cursor,
      });
      expect(link2.complete).toBe(true);
      expect(sent2).toHaveLength(1);
      expect(sent2[0]).toContain(key("c"));
    });
  }, 120_000);
});

describe("unhealthy marker", () => {
  test("a healthy start clears the marker and tolerates a missing file", () => {
    const dir = mkdtempSync(join(tmpdir(), "consumer-unhealthy-"));
    const file = join(dir, "unhealthy");
    writeFileSync(file, "stale\n");
    clearUnhealthyFile(file);
    expect(existsSync(file)).toBe(false);
    clearUnhealthyFile(file); // missing file is not an error
  });
});

describe("pending key", () => {
  test("extracts reconcile keys and revisions", () => {
    expect(pendingKey(`{"reconcile":{"key":"a/b"}}`)).toBe("a/b");
    expect(pendingKey(`{"reconcile":{"key":"a","etag":"e"}}`)).toBe("a");
    expect(pendingKey("not json")).toBeNull();
    expect(pendingKey(`{"Records":[]}`)).toBeNull();
    expect(reconcileMeta(`{"reconcile":{"key":"a","etag":"e","version":"3"}}`)).toEqual({ key: "a", etag: "e", version: "3" });
    expect(reconcileMeta(`{"reconcile":{"key":"a"}}`)).toEqual({ key: "a" });
  });
});

describe("failure counter", () => {
  test("counts consecutive failures and resets on success", () => {
    expect(nextFailureCount(0, false)).toBe(0);
    expect(nextFailureCount(2, false)).toBe(0);
    expect(nextFailureCount(0, true)).toBe(1);
    expect(nextFailureCount(4, true)).toBe(5);
    expect(5 >= MAX_CONSECUTIVE_TICK_FAILURES).toBe(true);
  });
});

describe("bootstrap ordering", () => {
  test("runBootstrap runs owner steps in order", async () => {
    const calls: string[] = [];
    const owner = { query: async () => ({ rows: [] as unknown[] }) };
    const out = await runBootstrap({
      owner,
      lock: async () => {
        calls.push("lock");
      },
      checkOwner: async () => {
        calls.push("owner");
      },
      ensureIndexSchema: async () => {
        calls.push("index");
      },
      ensureProjectionState: async (o) => {
        expect(o).toBe(owner);
        calls.push("state");
      },
      ensureServiceTables: async (o) => {
        expect(o).toBe(owner);
        calls.push("tables");
      },
      ensureRole: async (o) => {
        expect(o).toBe(owner);
        calls.push("role");
      },
      verifyRole: async (o) => {
        expect(o).toBe(owner);
        calls.push("verify");
      },
      generation: "abc",
    });
    expect(calls).toEqual(["owner", "lock", "index", "state", "tables", "role", "verify"]);
    expect(out).toEqual({ generation: "abc" });
  });
  test("runBootstrap refuses a non-owner role", async () => {
    const owner = {
      query: async () => ({ rows: [] as unknown[] }),
    };
    await expect(
      runBootstrap({
        owner,
        lock: async () => {},
        checkOwner: async () => {
          throw Object.assign(new Error("pipeline consumer bootstrap: current role may not create roles or grant"), { code: "42501" });
        },
        ensureIndexSchema: async () => {
          throw new Error("must not run");
        },
        ensureProjectionState: async () => {},
        ensureServiceTables: async () => {},
        ensureRole: async () => {},
        verifyRole: async () => {},
        generation: "abc",
      }),
    ).rejects.toThrow("may not create roles");
  });
});

/**
 * Every Supabase and HoopDb migration, like pointer-race.test.ts: the role
 * files grant against the real catalog (policies, triggers, SECURITY DEFINER
 * functions), so a hand-made minimal schema hides exactly the drift verify
 * must catch.
 */
const repoRoot = join(import.meta.dir, "../../..");
async function migratedPg(): Promise<PGlite> {
  const pg = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const read = (p: string) => readFileSync(join(repoRoot, p), "utf8");
  await pg.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of [
    "supabase/bootstrap/session_directory_search_worker.sql",
    "supabase/bootstrap/session_token_verifier.sql",
    "supabase/bootstrap/session_ingest_roles.sql",
    "supabase/bootstrap/session_read_api_roles.sql",
    "supabase/bootstrap/session_consumer_roles.sql",
  ])
    await pg.exec(read(f));
  for (const f of readdirSync(join(repoRoot, "supabase/migrations")).sort())
    await pg.exec(read(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(repoRoot, "src/hoopdb/migrations")).filter((x) => x.endsWith(".sql")).sort())
    await pg.exec(read(`src/hoopdb/migrations/${f}`));
  return pg;
}

async function withMigrated(work: (pg: PGlite, db: SqlExecutor) => Promise<void>): Promise<void> {
  const pg = await migratedPg();
  try {
    await work(pg, asDb(pg));
  } finally {
    await pg.close();
  }
}

/** The real search-index tables the bootstrap's ensureIndexSchema creates. */
async function ensureIndexSchema(db: SqlExecutor): Promise<void> {
  const binding = composeSearchService(
    writerComposition("test-backend", 0),
    { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: writerTransactionSql(db).sql, secrets: {} },
    "writer",
  );
  await binding.index.ensureSchema();
}

describe("service tables and pending durability (PGlite, real migrations)", () => {
  test("ensureServiceTables installs the writer tables idempotently", async () => {
    await withMigrated(async (pg, db) => {
      await ensureServiceTables(db);
      await ensureServiceTables(db);
      const pk = await pg.query(
        `SELECT kcu.column_name FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
         WHERE tc.table_schema='sessions' AND tc.table_name='search_consumer_pending' AND tc.constraint_type='PRIMARY KEY'
         ORDER BY kcu.ordinal_position`,
      );
      expect(pk.rows.map((r: { column_name: string }) => r.column_name)).toEqual(["backend_id", "manifest_key"]);
    });
  }, 120_000);

  test("pending rows round-trip due ordering and dead-lettering on the real table", async () => {
    await withMigrated(async (pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-01T12:00:00Z");
      await storePending(db, "b", [
        { manifestKey: "k-future", attempts: 1, nextAt: new Date(at.getTime() + 60_000), lastErrorCode: "x", dead: false },
        { manifestKey: "k-due", attempts: 2, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "boom", dead: false },
        { manifestKey: "k-dead", attempts: MAX_ATTEMPTS + 1, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "boom", dead: true },
      ], at);
      let due = await loadPendingDue(db, "b", 10, at);
      expect(due.map((r) => r.manifestKey)).toEqual(["k-due"]);
      expect(due[0]).toMatchObject({ attempts: 2, lastErrorCode: "boom" });
      await deletePending(db, "b", ["k-due"]);
      due = await loadPendingDue(db, "b", 10, at);
      expect(due).toEqual([]);
      const dead = await pg.query(`SELECT dead FROM sessions.search_consumer_pending WHERE manifest_key='k-dead'`);
      expect((dead.rows[0] as { dead: boolean }).dead).toBe(true);
    });
  }, 30_000);

  test("success clears a dead row and a new etag revives it", async () => {
    await withMigrated(async (pg, db) => {
      await ensureServiceTables(db);
      const at = new Date("2026-11-01T12:00:00Z");
      await storePending(db, "b", [
        { manifestKey: "k-old", attempts: 25, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "boom", dead: true },
      ], at);
      // Same etag: still dead, not due.
      let known = await loadPendingMap(db, "b", ["k-old"]);
      expect(known.get("k-old")).toMatchObject({ dead: true, attempts: 25 });
      expect(await loadPendingDue(db, "b", 10, at)).toEqual([]);
      // New etag: revive as a fresh cycle and store it.
      await storePending(db, "b", [
        { manifestKey: "k-old", manifestEtag: "etag2", attempts: 1, nextAt: new Date(at.getTime() + 30_000), lastErrorCode: "boom", dead: false },
      ], at);
      known = await loadPendingMap(db, "b", ["k-old"]);
      expect(known.get("k-old")).toMatchObject({ dead: false, attempts: 1 });
      // Success deletes the row outright, dead or not.
      await deletePending(db, "b", ["k-old"]);
      known = await loadPendingMap(db, "b", ["k-old"]);
      expect(known.has("k-old")).toBe(false);
    });
  }, 30_000);

  test("high-water mark round-trips", async () => {
    await withMigrated(async (_pg, db) => {
      await ensureServiceTables(db);
      expect(await loadHighWaterMark(db, "b")).toBeNull();
      const at = new Date("2026-11-01T12:00:00Z");
      await storeHighWaterMark(db, "b", at);
      expect(await loadHighWaterMark(db, "b")).toEqual(at);
    });
  }, 30_000);

  test("service-table gate fails closed when the bootstrap never ran", async () => {
    await withMigrated(async (_pg, db) => {
      await expect(assertServiceSchema(db)).rejects.toThrow("session-pipeline-consumer-bootstrap");
      await ensureServiceTables(db);
      await expect(assertServiceSchema(db)).resolves.toBeUndefined();
      // A dropped table fails the gate again by name: the gate checks the
      // real catalog, never a hand-written shadow.
      await db.query(`DROP TABLE sessions.search_consumer_progress`);
      await expect(assertServiceSchema(db)).rejects.toThrow("search_consumer_progress");
    });
  }, 120_000);

  test("attempt scoring never revives on etag-less bodies and dead-letters poison", async () => {
    // Genuine new revision: fresh cycle.
    expect(scoreFailedAttempt({ attempts: 24, etag: "e1", version: null }, { etag: "e2" })).toEqual({ attempts: 1, dead: false });
    // Same revision: keep counting.
    expect(scoreFailedAttempt({ attempts: 3, etag: "e1", version: null }, { etag: "e1" })).toEqual({ attempts: 4, dead: false });
    // Etag-less failure on a dead row: no resurrection, still dead.
    expect(scoreFailedAttempt({ attempts: 25, etag: "e1", version: null }, {})).toEqual({ attempts: 26, dead: true });
    // First failure with no previous row.
    expect(scoreFailedAttempt(undefined, {})).toEqual({ attempts: 1, dead: false });
    // Busy requeue counts: a perpetually busy key dead-letters.
    expect(scoreRequeuedAttempt(undefined, undefined)).toEqual({ attempts: 1, dead: false });
    expect(scoreRequeuedAttempt({ attempts: 23, dead: false, etag: null, version: null }, undefined)).toEqual({ attempts: 24, dead: false });
    expect(scoreRequeuedAttempt({ attempts: 24, dead: false, etag: null, version: null }, undefined)).toEqual({ attempts: 25, dead: true });
    // Carried revision on a dead row revives; etag-less never does.
    expect(scoreRequeuedAttempt({ attempts: 25, dead: true, etag: "e1", version: null }, { etag: "e2" })).toEqual({ attempts: 0, dead: false });
    expect(scoreRequeuedAttempt({ attempts: 25, dead: true, etag: "e1", version: null }, undefined)).toEqual({ attempts: 26, dead: true });
  });
});

describe("writer role SQL (PGlite, real files, real migrations)", () => {
  const ROLE_SQL = fileURLToPath(new URL("../../../supabase/bootstrap/session_consumer_role.sql", import.meta.url));
  const VERIFY_SQL = fileURLToPath(new URL("../../../supabase/bootstrap/session_consumer_role_verify.sql", import.meta.url));

  async function installed(): Promise<{ pg: PGlite; db: SqlExecutor }> {
    const pg = await migratedPg();
    const db = asDb(pg);
    // The ledger and marker are created by the migrator at runtime, not by
    // migrations; the bootstrap contract runs after migrate, so the test
    // creates the same shape the migrator does.
    await pg.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text, updated_at timestamptz DEFAULT now());`);
    // Mirror the migrator hardening (migrate.ts hardenAndCheck): PUBLIC
    // EXECUTE on sessions functions is revoked before this bootstrap ever
    // runs, so verify sees the production shape, not raw DDL defaults.
    await pg.exec(`REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC`);
    const state = postgresSearchState({ backendId: "test-backend", fromMs: 0 });
    await state.ensureSchema(db);
    await ensureServiceTables(db);
    await ensureIndexSchema(db);
    return { pg, db };
  }

  test("role install + verify pass end to end", async () => {
    const { pg } = await installed();
    try {
      const roleSql = readFileSync(ROLE_SQL, "utf8");
      const verifySql = readFileSync(VERIFY_SQL, "utf8");
      expect(stripPsqlMeta(roleSql)).not.toMatch(/^\\s*\\\\/m);
      // The bootstrap files hold no transaction of their own: any of these
      // would end serve()'s outer transaction early and split the atomic run.
      expect(roleSql).not.toMatch(/^\s*(BEGIN|START\s+TRANSACTION|SAVEPOINT|COMMIT|END|ROLLBACK)\s*;/mi);
      expect(stripPsqlMeta(readFileSync(VERIFY_SQL, "utf8"))).not.toMatch(/^\s*(BEGIN|START\s+TRANSACTION|SAVEPOINT|COMMIT|END|ROLLBACK)\s*;/mi);
      // PGlite runs scripts via exec; the bootstrap sends the same stripped text in one query.
      await pg.exec(stripPsqlMeta(roleSql));
      await pg.exec(stripPsqlMeta(verifySql)); // throws on any drift; NOTICE on pass
      const role = await pg.query(`SELECT rolcanlogin FROM pg_roles WHERE rolname='search_consumer_runtime'`);
      expect(role.rows[0]).toMatchObject({ rolcanlogin: true });
      // The writer sees through its own RLS policy on the migrated schema.
      await pg.query(`SET ROLE search_consumer_runtime`);
      const seen = await pg.query(`SELECT count(*)::int AS n FROM sessions.session_index`);
      await pg.query(`RESET ROLE`);
      expect(seen.rows[0]).toMatchObject({ n: 0 });
      // No password assertion: the password is set separately by the operator,
      // and PGlite stores an 8-char placeholder for every role regardless.
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("search writes without tenant GUCs are refused; with GUCs they land", async () => {
    const { pg } = await installed();
    try {
      const roleSql = readFileSync(ROLE_SQL, "utf8");
      const verifySql = readFileSync(VERIFY_SQL, "utf8");
      await pg.exec(stripPsqlMeta(roleSql));
      await pg.exec(stripPsqlMeta(verifySql));
      // No writer policy exists on the search tables: the writer sees and
      // writes projection_state only through the tenant_only policy, so an
      // unbound write fails instead of landing cross-tenant.
      await pg.exec(`SET ROLE search_consumer_runtime`);
      const write = `INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
        VALUES('test-backend','o2','w','h','s','s',3,0,1,0,false,now())`;
      let code: string | undefined;
      try {
        await pg.exec(write);
      } catch (error) {
        code = (error as { code?: string })?.code;
      }
      expect(code).toBe("42501");
      // The same write with the tenant's GUCs bound lands (one
      // transaction: the settings are transaction-local).
      await pg.exec(`BEGIN; SELECT set_config('bulls96_read.org_id','o2',true),set_config('bulls96_read.workspace_id','w',true); ${write}; COMMIT`);
      // Count back as the owner (RLS-blind): the GUC-less writer connection
      // cannot see the row it just wrote, which is the tenant gate working.
      await pg.exec(`RESET ROLE`);
      const seen = await pg.query(`SELECT count(*)::int AS n FROM search_index.projection_state WHERE org_id='o2'`);
      expect(seen.rows[0]).toMatchObject({ n: 1 });
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("systemUserIdentifies matches only a genuine same-user auth method", () => {
    expect(systemUserIdentifies(null, "w")).toBe(true);
    expect(systemUserIdentifies("", "w")).toBe(true);
    expect(systemUserIdentifies("search_consumer_runtime", "search_consumer_runtime")).toBe(true);
    expect(systemUserIdentifies("SCRAM-SHA-256/search_consumer_runtime", "search_consumer_runtime")).toBe(true);
    expect(systemUserIdentifies("other", "search_consumer_runtime")).toBe(false);
    expect(systemUserIdentifies("SCRAM-SHA-256/other", "search_consumer_runtime")).toBe(false);
  });

  // PGlite's RESET SESSION AUTHORIZATION is a no-op (probed: the session
  // identity stays rewritten), so each identity scenario gets its own
  // database instead of reusing one connection across SET/RESET pairs.
  test("a genuine writer login passes admission", async () => {
    const { pg } = await installed();
    try {
      await pg.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      // SET SESSION AUTHORIZATION rewrites the session identity the way a
      // real login does, including session_user.
      await pg.exec(`SET SESSION AUTHORIZATION search_consumer_runtime`);
      await assertRuntimeIdentity(asDb(pg));
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("startup refuses a smuggled writer identity behind SET ROLE", async () => {
    const { pg } = await installed();
    try {
      await pg.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      // An owner login hiding behind SET ROLE refuses: session_user is the
      // owner even while current_user names the writer.
      await pg.exec(`SET ROLE search_consumer_runtime`);
      await expect(assertRuntimeIdentity(asDb(pg))).rejects.toThrow("session_user");
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("startup refuses a BYPASSRLS or membership-drifted writer login", async () => {
    const { pg: bypass } = await installed();
    try {
      await bypass.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      // A writer with BYPASSRLS is indistinguishable from the runtime login
      // by name alone, so the gate checks the attribute instead.
      await bypass.exec(`ALTER ROLE search_consumer_runtime BYPASSRLS`);
      await bypass.exec(`SET SESSION AUTHORIZATION search_consumer_runtime`);
      await expect(assertRuntimeIdentity(asDb(bypass))).rejects.toThrow("BYPASSRLS");
    } finally {
      await bypass.close();
    }
    const { pg: drifted } = await installed();
    try {
      await drifted.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      // A writer that inherited a second role via GRANT keeps the writer name
      // on a genuine login, so the gate checks pg_auth_members instead.
      await drifted.exec(`CREATE ROLE widen_nobody LOGIN; GRANT widen_nobody TO search_consumer_runtime`);
      await drifted.exec(`SET SESSION AUTHORIZATION search_consumer_runtime`);
      await expect(assertRuntimeIdentity(asDb(drifted))).rejects.toThrow("inherited memberships");
    } finally {
      await drifted.close();
    }
  }, 180_000);

  test("startup refuses a database URL carrying startup options", () => {
    expect(() => refuseStartupOptions("postgres://writer:secret@db:5432/hoop?options=-c%20role%3Dwriter")).toThrow(
      "startup options",
    );
    expect(() => refuseStartupOptions("postgres://writer:secret@db:5432/hoop")).not.toThrow();
  });

  test("startup refuses a set PGOPTIONS environment fallback", () => {
    // node-pg resolves a missing pool options from PGOPTIONS, so a set
    // variable fails loudly instead of being silently shadowed by the pin.
    expect(() => refuseEnvOptions({ PGOPTIONS: "-c role=search_consumer_runtime" })).toThrow(
      "PGOPTIONS",
    );
    expect(() => refuseEnvOptions({ PGOPTIONS: "" })).not.toThrow();
    expect(() => refuseEnvOptions({})).not.toThrow();
  });

  test("the writer pool pins options and refuses PGOPTIONS", async () => {
    // Structural half: a non-empty options shadows the environment in
    // node-pg's resolution, so ambient PGOPTIONS can never reach a pooled
    // connection. save/restore the variable: the factory reads the ambient
    // process environment, not the injectable serve env.
    const saved = process.env.PGOPTIONS;
    try {
      delete process.env.PGOPTIONS;
      const db = supabasePool("postgres://writer:secret@db:5432/hoop", 1);
      try {
        expect((db.pool as unknown as { options?: { options?: unknown } }).options?.options).toContain(
          "idle_in_transaction_session_timeout",
        );
      } finally {
        await db.end();
      }
      process.env.PGOPTIONS = "-c role=search_consumer_runtime";
      expect(() => supabasePool("postgres://writer:secret@db:5432/hoop", 1)).toThrow("PGOPTIONS");
    } finally {
      if (saved === undefined) delete process.env.PGOPTIONS;
      else process.env.PGOPTIONS = saved;
    }
  });

  test("the tenant gate activates on the composed bootstrap and refuses a disabled or widened gate", async () => {
    const { pg } = await installed();
    try {
      await pg.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      const db = asDb(pg);
      // P1-a activation: the composed ensureSchema provisions exactly
      // tenant_only with forced RLS on both search tables, so the gate
      // passes with no staging. (pg_policies is visible to the writer
      // through its table grants: this also proves the gate can read its
      // own catalog.)
      await pg.exec(`SET ROLE search_consumer_runtime`);
      await assertSearchTenantGate(db);
      await pg.exec(`RESET ROLE`);
      // A tenant_only-shaped policy without enforced row security is a gate
      // billboard, not a gate: refuse until RLS is enabled and forced.
      await pg.exec(`ALTER TABLE search_index.transcript_events DISABLE ROW LEVEL SECURITY`);
      await expect(assertSearchTenantGate(db)).rejects.toThrow("row security not enforced");
      await pg.exec(`ALTER TABLE search_index.transcript_events ENABLE ROW LEVEL SECURITY`);
      await pg.exec(`ALTER TABLE search_index.transcript_events FORCE ROW LEVEL SECURITY`);
      // A permissive TO PUBLIC policy ORs with tenant_only and widens the
      // tenant without naming the role: refuse that too.
      await pg.exec(`CREATE POLICY open_all ON search_index.projection_state AS PERMISSIVE FOR ALL TO PUBLIC USING (true) WITH CHECK (true)`);
      await expect(assertSearchTenantGate(db)).rejects.toThrow("search_index.projection_state");
      await pg.exec(`DROP POLICY open_all ON search_index.projection_state`);
      // Exactly the tenant gate on both tables passes.
      await pg.exec(`SET ROLE search_consumer_runtime`);
      await assertSearchTenantGate(db);
      await pg.exec(`RESET ROLE`);
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("verify refuses a permissive TO PUBLIC search policy that never names the role", async () => {
    const { pg } = await installed();
    try {
      await pg.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      await pg.exec(`CREATE POLICY open_all ON search_index.projection_state AS PERMISSIVE FOR ALL TO PUBLIC USING (true) WITH CHECK (true)`);
      await expect(pg.exec(stripPsqlMeta(readFileSync(VERIFY_SQL, "utf8")))).rejects.toThrow("non-tenant policy");
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("verify refuses a tenant_only-shaped policy with a widened USING", async () => {
    const { pg } = await installed();
    try {
      await pg.exec(stripPsqlMeta(readFileSync(ROLE_SQL, "utf8")));
      // Keep the name and shape, widen the predicate: a LIKE/substring
      // verifier would still pass, the exact-expression verifier must not.
      await pg.exec(`DROP POLICY tenant_only ON search_index.projection_state`);
      await pg.exec(`CREATE POLICY tenant_only ON search_index.projection_state AS PERMISSIVE FOR ALL TO PUBLIC
        USING (true OR (org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true)))
        WITH CHECK (org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true))`);
      await expect(pg.exec(stripPsqlMeta(readFileSync(VERIFY_SQL, "utf8")))).rejects.toThrow("non-tenant policy");
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("a pre-existing elevated role fails the bootstrap", async () => {
    const { pg } = await installed();
    try {
      // installed() provisions the bare role (plus the migration's twelve
      // EXECUTEs, which block DROP ROLE); elevate it in place so the
      // bootstrap meets the pre-existing elevated shape it must refuse.
      await pg.exec(`ALTER ROLE search_consumer_runtime SUPERUSER LOGIN`);
      const roleSql = readFileSync(ROLE_SQL, "utf8");
      await expect(pg.exec(stripPsqlMeta(roleSql))).rejects.toThrow("different attributes");
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("reserved role names are refused before any role lookup", async () => {
    const roleSql = readFileSync(ROLE_SQL, "utf8");
    // The guard fires on the name alone, so the real file is executed with
    // only the name constant swapped: owner, reader, and pg_* names fail.
    // Each name gets a fresh instance: the refused script aborts its own
    // transaction, which a shared connection would carry into the next run.
    for (const name of ["postgres", "bulls96_read_api", "pg_read_all_data", "public"]) {
      const swapped = roleSql.replace(
        "v_name text := 'search_consumer_runtime'",
        `v_name text := '${name}'`,
      );
      expect(swapped).not.toBe(roleSql);
      const pg = await migratedPg();
      try {
        // migratedPg provisions the bare role (plus the migration's twelve
        // EXECUTEs, which block DROP ROLE); revoke and drop it so the
        // no-side-effect assertion below is exact.
        await pg.exec(`REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sessions FROM search_consumer_runtime; DROP ROLE search_consumer_runtime`);
        await expect(pg.exec(stripPsqlMeta(swapped))).rejects.toThrow("reserved role name");
        // The refused script aborts its own transaction; roll back so the
        // no-side-effect assertion runs on a clean session.
        await pg.exec(`ROLLBACK`);
        expect(await pg.query(`SELECT count(*)::int AS n FROM pg_roles WHERE rolname='search_consumer_runtime'`))
          .toMatchObject({ rows: [{ n: 0 }] });
      } finally {
        await pg.close();
      }
    }
  }, 300_000);
});

describe("tombstone backstop and freshness (PGlite, real partials, real migrations)", () => {
  async function fixture(): Promise<{ pg: PGlite; db: SqlExecutor; runner: TenantRunner }> {
    const pg = await migratedPg();
    const db = asDb(pg);
    const state = postgresSearchState({ backendId: "test-backend", fromMs: 0 });
    await state.ensureSchema(db);
    await ensureServiceTables(db);
    await ensureIndexSchema(db);
    const runner: TenantRunner = async (orgId, workspaceId, work) =>
      pg.transaction(async (tx) => {
        await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [orgId, workspaceId]);
        return work({
          query: async (text: string, params?: unknown[]) => {
            const r = await tx.query(text, params as unknown[]);
            return { rows: r.rows };
          },
        });
      });
    return { pg, db, runner };
  }

  /** One transcript row with every NOT NULL column the adapter DDL requires. */
  const transcriptRow = (sessionId: string) =>
    `('o','w',1,'${sessionId}','h','m','message','e1','user','u','user','r','','','t','ref','ref1','{}','{}','','','', 'x',1,'x','t','fp',1)`;

  test("a crash after the pointer leaves searchable rows resendable", async () => {
    const { pg, db, runner } = await fixture();
    try {
      // Pointer succeeded but the search write crashed mid-pass: the projection
      // row is incomplete, so freshness must re-send it until it completes.
      // session_index has more columns on the migrated schema; name them.
      await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
        VALUES('o','w','h','s','m','u',now(),now(),'s3','b','96bulls/o/manifest.json','00000000-0000-0000-0000-000000000000');
        INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,event_projection_epoch,event_projection_ready)
        VALUES('o','w','h','s',now(),now(),3,false);`);
      const state = postgresSearchState({ backendId: "test-backend", fromMs: 0, reader: runner });
      await state.record(asDb(pg), { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s" },
        { revision: 3, rows: 1, complete: false, at: new Date(), canonicalSessionId: "s" });
      const sent: string[] = [];
      const out = await reconcileStaleProjections(db, (t, after) => state.partials(t, { limit: 100, ...(after === undefined ? {} : { after }) }), "96bulls/o", async (b) => {
        sent.push(b);
      });
      expect(out).toMatchObject({ stale: 1, overflow: false });
      expect(sent).toHaveLength(1);
      expect(sent[0]).toContain("96bulls/o/manifest.json");
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("stale sessions page by cursor instead of skipping overflow tenants", async () => {
    const { pg, db, runner } = await fixture();
    try {
      const values = Array.from({ length: 5 }, (_, i) =>
        `('o','w','h','s${i}','m','u',now(),now(),'s3','b','96bulls/o/manifest-${i}.json','00000000-0000-0000-0000-000000000000')`).join(",");
      await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
        VALUES ${values};`);
      const state = postgresSearchState({ backendId: "test-backend", fromMs: 0, reader: runner });
      const sent: string[] = [];
      // Page size 2 forces three pages; all five sessions are re-sent and no
      // tenant is skipped with overflow.
      const out = await reconcileStaleProjections(
        db,
        (t, after) => state.partials(t, { limit: 2, ...(after === undefined ? {} : { after }) }),
        "96bulls/o",
        async (b) => {
          sent.push(b);
        },
      );
      expect(out).toMatchObject({ stale: 5, overflow: false });
      expect(sent).toHaveLength(5);
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("a tombstoned session with leftover rows is re-sent until erased", async () => {
    const { pg, db, runner } = await fixture();
    try {
      await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,deleted_at)
        VALUES('o','w','h','s','m','u',now(),now(),'s3','b','96bulls/o/manifest.json','00000000-0000-0000-0000-000000000000',now());
        INSERT INTO search_index.transcript_events(org_id,workspace_id,ts_ms,session_id,source,machine_id,kind,event_id,type,user_id,actor_name,actor_role,subagent_id,project_id,task_title,task_ref,task_ref_id,tags,files_touched,tool_name,tool_status,delivery_record_id,content,content_bytes,content_prefix_folded,task_title_folded,fingerprint,version)
        VALUES ${transcriptRow("s")};`);
      const writer = postgresSearchState({ backendId: "test-backend", fromMs: 0 });
      await writer.record(asDb(pg), { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s" },
        { revision: 1, rows: 4, complete: true, at: new Date(), canonicalSessionId: "s" });
      const sent: string[] = [];
      const out = await reconcileTombstonedScopes(db, runner, "test-backend", "96bulls/o", 1000, async (b) => {
        sent.push(b);
      });
      expect(out).toMatchObject({ tombstoned: 1, overflow: false });
      expect(sent).toEqual([JSON.stringify({ reconcile: { key: "96bulls/o/manifest.json" } })]);
      // Erasure commits the way indexSearchScope does: transcript rows gone,
      // complete row_count=0 state recorded. The backstop retires the
      // residue itself: nothing re-sent, the state row deleted, no manual
      // DELETE of projection_state anywhere in this test.
      await pg.exec(`DELETE FROM search_index.transcript_events;`);
      await writer.record(asDb(pg), { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s" },
        { revision: 1, rows: 0, complete: true, at: new Date(), canonicalSessionId: "s" });
      const retired = await reconcileTombstonedScopes(db, runner, "test-backend", "96bulls/o", 1000, async (b) => {
        sent.push(b);
      });
      expect(retired).toMatchObject({ tombstoned: 0, overflow: false });
      expect(sent).toHaveLength(1);
      const remaining = await pg.query(`SELECT count(*)::int AS n FROM search_index.projection_state`);
      expect(remaining.rows[0]).toMatchObject({ n: 0 });
      // Steady state: quiet.
      const clean = await reconcileTombstonedScopes(db, runner, "test-backend", "96bulls/o", 1000, async () => {});
      expect(clean).toMatchObject({ tombstoned: 0, overflow: false });
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("ten thousand erasure residues retire with no resend and no manual delete", async () => {
    const { pg, db, runner } = await fixture();
    try {
      const N = 10_001;
      const chunk = 1000;
      for (let start = 0; start < N; start += chunk) {
        const end = Math.min(start + chunk, N);
        const pointers: string[] = [];
        const states: string[] = [];
        for (let i = start; i < end; i += 1) {
          pointers.push(`('o','w','h','s${i}','m','u',now(),now(),'s3','b','96bulls/o/manifest-${i}.json','00000000-0000-0000-0000-000000000000',now())`);
          states.push(`('test-backend','o','w','h','s${i}','s${i}',3,0,1,0,true,now())`);
        }
        await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id,deleted_at)
          VALUES ${pointers.join(",")};
          INSERT INTO search_index.projection_state(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
          VALUES ${states.join(",")};`);
      }
      const sent: string[] = [];
      const out = await reconcileTombstonedScopes(db, runner, "test-backend", "96bulls/o", 1000, async (b) => {
        sent.push(b);
      });
      // The residue retires instead of resending: the same backlog selected
      // forever on the old query, starving later scopes and pinning the
      // high-water mark.
      expect(out).toMatchObject({ tombstoned: 0, overflow: false });
      expect(sent).toHaveLength(0);
      const remaining = await pg.query(`SELECT count(*)::int AS n FROM search_index.projection_state`);
      expect(remaining.rows[0]).toMatchObject({ n: 0 });
    } finally {
      await pg.close();
    }
  }, 300_000);

  test("dead keys no longer block completion: the mark advances past them", async () => {
    const { pg, db, runner } = await fixture();
    try {
      await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
        VALUES('o','w','h','s-dead','m','u',now(),now(),'s3','b','96bulls/o/manifest-dead.json','00000000-0000-0000-0000-000000000000');`);
      const at = new Date("2026-11-01T12:00:00Z");
      // The dead key exhausted its attempts; nothing live is stale.
      await storePending(db, "test-backend", [
        { manifestKey: "96bulls/o/manifest-dead.json", manifestEtag: "e1", attempts: 25, nextAt: new Date(at.getTime() - 1000), lastErrorCode: "x", dead: true },
      ], at);
      const sent: string[] = [];
      const collected = await collectFreshnessSend(
        db,
        "test-backend",
        "96bulls/o",
        async () => ({ sessions: ["s-dead"], overflow: false }),
        runner,
        1000,
        new Set(),
        async (b) => { sent.push(b); },
      );
      // The dead key is filtered before counting: nothing to consume, and it
      // does not appear in the completion counts.
      expect(sent).toEqual([]);
      expect(collected).toMatchObject({ stale: 0, tombstoned: 0, overflow: false });
      // A tick built on this collection completes, so serve advances the
      // high-water mark instead of stalling the listing window forever.
      const io = tickIo(ok);
      io.checkFreshness = async () => ({ stale: collected.stale, tombstoned: collected.tombstoned, overflow: collected.overflow });
      const summary = await runConsumerTick(io, new Date());
      expect(summary).toMatchObject({ collected: 0, failed: 0, complete: true });
      expect(io.calls).toEqual([[]]); // consume ran once, on zero bodies
    } finally {
      await pg.close();
    }
  }, 180_000);

  test("a recorded floor mismatch keeps listing the session", async () => {
    const { pg, db, runner } = await fixture();
    try {
      await pg.exec(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id)
        VALUES('o','w','h','s','m','u',now(),now(),'s3','b','96bulls/o/manifest.json','00000000-0000-0000-0000-000000000000');
        INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,event_projection_epoch,event_projection_ready,event_projection_version,event_projected_at,event_projection_sources)
        VALUES('o','w','h','s',now(),now(),3,true,1,now(),'{"96bulls/o/manifest.json": "3"}');`);
      const at = new Date();
      const writer = postgresSearchState({ backendId: "test-backend", fromMs: 0, reader: runner });
      await writer.record(asDb(pg), { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s" },
        { revision: 3, rows: 1, complete: true, at, canonicalSessionId: "s" },
      );
      const sameFloor = postgresSearchState({ backendId: "test-backend", fromMs: 0, reader: runner });
      expect((await sameFloor.partials({ orgId: "o", workspaceId: "w" }, { limit: 100 })).sessions).toEqual([]);
      const otherFloor = postgresSearchState({ backendId: "test-backend", fromMs: 100, reader: runner });
      expect((await otherFloor.partials({ orgId: "o", workspaceId: "w" }, { limit: 100 })).sessions).toEqual(["s"]);
    } finally {
      await pg.close();
    }
  }, 180_000);
});

describe("backoff and high-water math", () => {
  test("retryDelayMs backs off to a one-hour ceiling", () => {
    expect(retryDelayMs(0)).toBe(30_000);
    expect(retryDelayMs(1)).toBe(60_000);
    expect(retryDelayMs(100)).toBe(3_600_000);
    expect(retryDelayMs(-3)).toBe(30_000);
  });
  test("computeSince honors the high-water mark with margin", () => {
    const now = Date.parse("2026-11-01T12:00:00Z");
    expect(computeSince(now, 30 * 60_000, null)).toEqual(new Date(now - 30 * 60_000));
    const recent = new Date(now - 60_000);
    expect(computeSince(now, 30 * 60_000, recent)).toEqual(new Date(now - 30 * 60_000));
    const old = new Date(now - 3 * 60 * 60_000);
    expect(computeSince(now, 30 * 60_000, old)).toEqual(new Date(old.getTime() - HIGH_WATER_MARGIN_MS));
  });
});
