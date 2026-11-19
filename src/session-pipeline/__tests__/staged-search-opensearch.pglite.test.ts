/**
 * The staged reconcile on the OpenSearch adapter binding (Phase 3 slice 4, the default flip), with
 * SEARCH_INDEX_STAGED_RECONCILE_ENABLED UNSET (default on), as serve() builds it for
 * SEARCH_INDEX_BACKEND=opensearch.
 *
 * In process (always): the OpenSearch binding composed over a scripted fetch keeps the default-on
 * staged reconcile (it has keyset pages), and its scan pages are keyset requests (search_after).
 *
 * Real server (SEARCH_INDEX_OPENSEARCH_TEST_URL, a disposable unauthenticated OpenSearch or
 * Elasticsearch, SEARCH_INDEX_OPENSEARCH_TEST_DIALECT selecting the dialect, as
 * packages/pipeline/test/search-index-opensearch-real.test.ts): a scope past the whole-read bound runs as
 * a staged run through paged scans and ends complete and equal to the whole read; a second pass sends
 * nothing; with output facts on, the staged run stages and reconciles them too. Each run uses its own
 * index prefix and deletes its indexes. Without the URL it is skipped, unless OPENSEARCH_PROOF_REQUIRED=1
 * (the opensearch-proof workflow), where a missing URL fails.
 *
 * Synthetic, secret-free data only. Staging and HoopDb in PGlite.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { prefixAllowed, prefixOrgResolver } from "../../shared/vendor.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { createOpenSearchSearchComposition } from "../../shared/opensearch-composition.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { outputFactsDeps } from "../consumer-service.ts";
import { droppedFanout } from "../pointer.ts";
import type { SearchProjectionState, SearchServiceBinding } from "../search-adapter.ts";
import { indexSearchScope, searchIndexStagedForIndex, searchIndexStagedStartup, type SearchIndexDeps, type SearchIndexResult } from "../search-index.ts";
import { SCOPE, exportStreamScope, generate, prefixFor, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";

const NOW = new Date("2026-11-01T12:00:00.000Z");
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const STAGED_LIMITS = { ...TINY, insertBatch: 4 };
const bindings = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const RUNTIME = "search_consumer_runtime";
const URL_VAR = "SEARCH_INDEX_OPENSEARCH_TEST_URL";
const url = process.env[URL_VAR];
const dialect = process.env.SEARCH_INDEX_OPENSEARCH_TEST_DIALECT || "opensearch";
const required = process.env.OPENSEARCH_PROOF_REQUIRED === "1";

/** The default-on staged deps serve() builds (the flag unset), with the test batch sizes. */
function stagedByDefault(outputFacts = false) {
  const started = searchIndexStagedStartup({}, UNLIMITED, outputFacts);
  expect(started.state).toBe("default_on");
  return { started, staged: { ...started.staged!, run: { stageBatch: 7, writeKeys: 5, factsChunk: 7 } } };
}
/** A projection state that records nothing (the index under test is the point). */
const quietState: SearchProjectionState = { fromMs: async () => 0, record: async () => {}, partials: async () => ({ sessions: [], overflow: false }) };

describe("OpenSearch binding, in process: the default-on staged reconcile is kept and its scans page by keyset", () => {
  test("composed for SEARCH_INDEX_BACKEND=opensearch over a scripted fetch", async () => {
    const calls: Array<{ method: string; url: string; body: Record<string, unknown> }> = [];
    const hit = (n: number) => ({
      _source: { workspace_id: "ws", ts_ms: 1000 + n, session_id: "s", source: "claude", machine_id: "m", kind: "history", event_id: `e${n}`, fingerprint: `f${n}` },
    });
    const fetch = async (u: string, init: { method?: string; body?: unknown }) => {
      const body = typeof init.body === "string" ? (JSON.parse(init.body) as Record<string, unknown>) : {};
      calls.push({ method: init.method ?? "GET", url: u, body });
      // limit 2 asks for 3 hits: three come back, so a next page exists.
      const size = typeof body.size === "number" ? body.size : 0;
      return new Response(JSON.stringify({ timed_out: false, _shards: { failed: 0 }, hits: { hits: Array.from({ length: size }, (_, i) => hit(i)) } }), {
        status: 200, headers: { "content-type": "application/json" },
      });
    };
    const base = createOpenSearchSearchComposition({ env: {}, backendId: "os-test", fromMs: 0 });
    const binding = composeSearchService(
      { ...base, extras: { ...(base.extras ?? {}), fetch }, state: () => quietState },
      { env: { SEARCH_INDEX_BACKEND: "opensearch", SEARCH_INDEX_OPENSEARCH_URL: "http://search.test:9200" } },
      "writer",
    );
    const { started } = stagedByDefault();
    expect(typeof binding.index.scanFingerprintsPage).toBe("function");
    expect(searchIndexStagedForIndex(started, binding.index)).toEqual({ staged: started.staged });
    const scope = { orgId: "org", workspaceId: "ws", source: "claude", sessionIds: ["s"] };
    const first = await binding.index.scanFingerprintsPage!(scope, { fromMs: 0, limit: 2 });
    expect(first.rows).toHaveLength(2);
    expect(first.next).toBeDefined();
    await binding.index.scanFingerprintsPage!(scope, { fromMs: 0, limit: 2, after: first.next });
    const searches = calls.filter((c) => c.url.endsWith("/_search"));
    expect(searches).toHaveLength(2);
    expect(searches[0]!.body).toMatchObject({ size: 3, sort: expect.any(Array) });
    expect(searches[0]!.body.search_after).toBeUndefined();
    // The second page continues strictly after the first page's last row: a keyset request.
    expect(searches[1]!.body.search_after).toEqual([1001, "s", "claude", "m", "history", "e1"]);
  });
});

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});

if (!url) {
  if (required)
    test("the real-server staged proof is required (OPENSEARCH_PROOF_REQUIRED=1) but SEARCH_INDEX_OPENSEARCH_TEST_URL is unset", () => {
      throw new Error(`${URL_VAR} is required when OPENSEARCH_PROOF_REQUIRED=1`);
    });
  else describe.skip("staged reconcile on a real OpenSearch / Elasticsearch", () => {
    test(`needs ${URL_VAR}`, () => {});
  });
} else {
  describe(`staged reconcile on a real ${dialect} server (flag unset: default on)`, () => {
    let x: ExportedScope;
    const prefixes: string[] = [];
    beforeAll(async () => {
      await warmReplayTemplate();
      x = await exportStreamScope(generate(31), 1200);
    }, 180_000);
    afterAll(async () => {
      for (const p of prefixes) await fetch(`${url!.replace(/\/+$/, "")}/${p}-transcript-events`, { method: "DELETE" }).catch(() => undefined);
      x?.dispose();
      await closeReplayTemplate();
    });

    /** One consumer: a HoopDb clone (staging, facts) and the OpenSearch binding under its own index prefix. */
    async function rig() {
      const prefix = `staged-${process.pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
      prefixes.push(prefix);
      const hoop = await clonedReplayDatabase();
      cleanup.push(() => hoop.pg.close());
      await hoop.pg.exec(`GRANT USAGE ON SCHEMA sessions TO ${RUNTIME}`);
      const base = createOpenSearchSearchComposition({ env: {}, backendId: "os-proof", fromMs: 0 });
      const composed = composeSearchService(
        { ...base, state: () => quietState },
        {
          env: {
            SEARCH_INDEX_BACKEND: "opensearch", SEARCH_INDEX_OPENSEARCH_URL: url!, SEARCH_INDEX_OPENSEARCH_DIALECT: dialect,
            SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX: prefix, SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS: "60000",
          },
        },
        "writer",
      );
      await composed.index.ensureSchema();
      const adapter: SearchServiceBinding = { index: composed.index, state: composed.state };
      const ROLE_CALLS = ["sessions.search_reconcile_", "sessions.staged_facts_", "sessions.output_facts_"];
      const transact = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
        hoop.pg.transaction((tx) =>
          work({
            query: async (sql: string, params?: unknown[]) => {
              if (!ROLE_CALLS.some((p) => sql.includes(p))) return tx.query(sql, params) as never;
              await tx.exec(`SET LOCAL ROLE ${RUNTIME}`);
              const result = await tx.query(sql, params);
              await tx.exec("RESET ROLE");
              return result as never;
            },
          }),
        ) as Promise<T>;
      const consumer = (search: Partial<SearchIndexDeps>): ConsumerDeps => ({
        pointer: {
          db: { ...hoop.db, pool: undefined, end: async () => {} } as never,
          lock: (async (_s: unknown, work: () => Promise<unknown>) => work()) as never,
          reader: x.reader,
          exists: async (k: string) => (await x.store.head(objectKey(k as never))) !== undefined,
          orgs: prefixOrgResolver,
          allowPrefix: (p: string, o: string) => prefixAllowed(bindings, p, o),
          fanout: droppedFanout,
        } as never,
        search: { transact, reader: x.reader, store: x.store, adapter, prefixFor, now: () => NOW, ...search },
        requeue: async () => {},
      });
      const messages = x.keys.map((key, n) => ({ messageId: `m-${n}`, body: JSON.stringify({ reconcile: { key } }) }));
      const tick = (search: Partial<SearchIndexDeps>) => processBatch(messages, consumer(search));
      const pass = (search: Partial<SearchIndexDeps>): Promise<SearchIndexResult> => indexSearchScope(SCOPE, { ...consumer(search).search } as SearchIndexDeps);
      const scan = async () =>
        (await adapter.index.scanFingerprints({ orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, source: SCOPE.harness, sessionIds: [SCOPE.sessionId, x.canonicalId] }, { fromMs: 0, limit: 100_000 })).rows
          .map((r) => JSON.stringify([r.sessionId, r.machineId, r.kind, r.eventId, r.tsMs, r.fingerprint]))
          .sort();
      const count = async (table: string) => (await hoop.pg.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${table}`)).rows[0]!.n;
      return { hoop, adapter, tick, pass, scan, count };
    }

    test("a scope past the whole-read bound runs staged through paged scans, ends equal to the whole read, and a second pass sends nothing", async () => {
      // The whole read with unlimited limits, under its own prefix: the reference.
      const ref = await rig();
      await ref.tick({});
      expect(await ref.pass({})).toMatchObject({ complete: true });
      const want = await ref.scan();
      expect(want.length).toBeGreaterThan(10);
      // The default-on staged run.
      const r = await rig();
      await r.tick({});
      const { staged } = stagedByDefault();
      expect(searchIndexStagedForIndex({ state: "default_on", staged }, r.adapter.index)).toEqual({ staged });
      const out = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged });
      expect(out).toMatchObject({ complete: true, reasons: [] });
      expect(out.stream?.passes).toBeGreaterThanOrEqual(3);
      expect(await r.scan()).toEqual(want);
      expect(await r.count("search_reconcile_runs_v1")).toBe(0);
      const again = await r.pass({ limits: STAGED_LIMITS, stream: UNLIMITED, staged });
      expect(again).toMatchObject({ complete: true, inserted: 0, deleted: 0 });
      expect(await r.scan()).toEqual(want);
    }, 600_000);

    test("with output facts on: the staged run stages and reconciles the facts too", async () => {
      const r = await rig();
      await r.tick({});
      const { staged } = stagedByDefault(true);
      const out = await r.pass({ ...outputFactsDeps({ outputFacts: true }), limits: STAGED_LIMITS, stream: UNLIMITED, staged });
      expect(out).toMatchObject({ complete: true });
      expect(out.outputFacts).toMatchObject({ complete: true });
      expect(await r.count("session_event_facts")).toBeGreaterThan(10);
      expect([await r.count("search_reconcile_runs_v1"), await r.count("staged_facts_runs_v1")]).toEqual([0, 0]);
    }, 600_000);
  });
}
