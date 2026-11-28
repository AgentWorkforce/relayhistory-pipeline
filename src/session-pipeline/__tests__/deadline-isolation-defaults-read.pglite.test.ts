/**
 * T124 (Skip 1baaadb2a585, layered-relayhistory 74c9350c372e): recovery under DEFAULT flags (no managed search wake) for
 * a deadline-isolated scope whose current incarnation B has an exact SUPERSEDED large-read job, whose own search
 * delivery is lost and with no later append; only a delayed hold of an older incarnation A survives. A's hold chain is
 * the only thing left: it must not be acked as covered (a superseded row is never claimed again), and past
 * LARGE_READ_UNCLAIMED_MS its bounded inline pass must make B visible to an AUTHORIZED caller.
 *
 * Real: the pointer stage and every migration on PGlite (pointer-lock-harness), the consumer (processBatch), the REAL
 * search pass (indexSearchScope, no searchPass stub) on the registered Postgres search projection, the large-read job
 * SQL including migration 20261105062410, and the read API (createReadApi GET /v1/events, a fixture token, pointer
 * liveness from the real session_index). Mocked: S3 is the fs store; the deadline mark table is in memory; the clock.
 * "B's delivery lost": B's object event runs the pointer stage only (no search stage), so B's pointer is committed and
 * nothing searched it. The managed wake is off: no search-index job exists at any point. Synthetic data only.
 */
import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { createReadApi, AuthError } from "../../session-read-api/app.ts";
import type { SearchProjectionState } from "../search-adapter.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import type { ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { requeueBody, type LargeReadHoldBody } from "../s3-events.ts";
import { STREAM_DEADLINE } from "../../session-read-api/canonical-stream-projection.ts";
import {
  captureLargeReadSource, deadlineMarkName, enqueueLargeRead, enqueueLargeReadExpected, holdId, largeReadProgress,
} from "../search-deadline-isolation.ts";
import { NOW, loc, pointerRig } from "./pointer-lock-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
const scope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };

test("defaults (no wake): B superseded, B's delivery lost, no append; A's surviving hold is not acked and its bounded inline pass makes B's marker visible to an authorized caller", async () => {
  const r = await pointerRig();
  cleanup.push(r.close);
  const proj = new PGlite({ extensions: { pg_trgm } });
  await proj.exec("CREATE EXTENSION pg_trgm");
  cleanup.push(() => proj.close());
  const tenants: TenantSqlRunner = (orgId, workspaceId, work) =>
    proj.transaction(async (tx) => { await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]); return work(tx); });
  let complete = false;
  const state: SearchProjectionState = {
    async fromMs() { return 0; },
    async record(_tx, _s, value) { complete = value.complete; },
    async partials() { return { sessions: complete ? [] : [scope.sessionId], overflow: false }; },
  };
  const projection = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: proj }, "writer", tenants);
  await projection.index.ensureSchema();
  const exec: SqlExecutor = { query: async (t: string, p?: unknown[]) => ({ rows: (await r.pg.query(t, p as unknown[])).rows }) } as SqlExecutor;
  const search: SearchIndexDeps = {
    transact: (work) => r.pg.transaction(async (tx) => work({ query: async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows }) } as SqlExecutor)),
    reader: r.pointer().reader, store: r.store, adapter: projection, prefixFor: (org) => `96bulls/${org}`, now: () => NOW,
    stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 },
  };
  // The deadline mark lives in memory (search_ingest_state shape); B's scope is marked by an earlier deadline.
  const marks: { name: string; value: string; updated_at: string }[] = [];
  const ch = {
    database: "t", async command() { throw new Error("no command"); },
    async select<T>(q: string, o?: { params?: Record<string, unknown> }): Promise<T[]> {
      if (!q.includes(`FROM ${INGEST_STATE_TABLE}`)) throw new Error("unexpected select");
      const hit = marks.filter((m) => m.name === o!.params!.name).sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1))[0];
      return hit ? ([{ value: hit.value, at_ms: Date.parse(`${hit.updated_at.replace(" ", "T")}Z`) }] as T[]) : [];
    },
    async insert(_t: string, rows: readonly object[]) { marks.push(...(rows as typeof marks)); },
  } as unknown as ClickHouseClient;
  const clock = { t: Date.now() };
  marks.push({ name: deadlineMarkName(scope), value: JSON.stringify({ v: 1, reason: STREAM_DEADLINE }), updated_at: new Date(clock.t).toISOString().replace("T", " ").replace("Z", "") });
  const holds: (LargeReadHoldBody | undefined)[] = [];
  let n = 0;
  const deps = (withSearch: boolean): ConsumerDeps => ({
    pointer: r.pointer(),
    ...(withSearch ? { search } : {}),
    requeue: async (_k, _o, hold) => { holds.push(hold); },
    deadlineIsolation: {
      ch, now: () => clock.t, enqueue: (s) => enqueueLargeRead(exec, s), progress: (s) => largeReadProgress(exec, s),
      current: (s) => captureLargeReadSource(exec, s), enqueueExpected: (s, e) => enqueueLargeReadExpected(exec, s, e),
    },
  });
  const app = createReadApi({ adapter: projection, completeSinceMs: 0,
    verify: async (token) => { if (token !== "fixture") throw new AuthError(401, "invalid_token", "invalid token"); return { orgId: scope.orgId, workspaceId: scope.workspaceId, userId: "user_1", scopes: ["rth:read"] }; },
    liveness: {
      async erased() { return new Set(); },
      async sessionStart() { return { pointers: 1, live: 1, createdAtMs: Date.parse("2026-11-01T10:00:00.000Z") }; },
      async livePointerSessions(org, ws, ids) {
        const live = (await r.pg.query<{ session_id: string }>(
          "SELECT DISTINCT session_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3) AND deleted_at IS NULL", [org, ws, [...ids]])).rows;
        return new Set(live.map((x) => x.session_id));
      },
    },
  });
  const read = async (q: string) => {
    const res = await app.request(`/v1/events?workspace=${scope.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
    const body = (await res.json()) as { events?: { text?: string; content?: string }[]; error?: { code: string; reason?: string } };
    return res.status === 200 ? { status: 200, texts: (body.events ?? []).map((e) => String(e.text ?? e.content ?? "")) } : { status: res.status, reason: body.error?.reason };
  };

  // B is uploaded; its delivery reaches the pointer stage only (the search delivery is lost): pointer committed, unsearched.
  const key = await r.writeManifest(1, loc, "markerb", "markerb");
  expect((await processBatch([{ messageId: `m${++n}`, body: r.body(key, "ObjectCreated:Put", "0001", "eb") }], deps(false))).batchItemFailures).toEqual([]);
  expect(await r.row()).toEqual([expect.objectContaining({ deleted_at: null })]);
  // B's exact large-read job exists but is superseded (never claimed again).
  await enqueueLargeRead(exec, scope);
  await r.pg.query("UPDATE sessions.session_large_read_jobs SET state='superseded'");
  expect((await r.pg.query("SELECT sessions.claim_session_large_read(gen_random_uuid(),120) j")).rows[0]).toEqual({ j: null });
  // Defaults: the managed wake never wrote a search-index job.
  expect((await r.pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index'")).rows).toEqual([]);
  expect(await read("markerb")).toEqual({ status: 501, reason: "session_partial" });

  // A's delayed hold (an older incarnation) is the only surviving obligation.
  const A = { epoch: "1", fingerprint: "a".repeat(64) };
  const upload = new Date(clock.t).toISOString();
  const canonical = (await captureLargeReadSource(exec, scope))!.identity;
  const holdA = (seq: number) => ({ ...A, seq, id: holdId(canonical, A, seq) });
  const first = await processBatch([{ messageId: `m${++n}`, body: requeueBody(key, upload, holdA(3)) }], deps(true));
  expect(first.batchItemFailures).toEqual([]);
  expect(holds).toEqual([holdA(4)]); // held for A only; not acked as covered
  expect(await read("markerb")).toEqual({ status: 501, reason: "session_partial" });

  // Past LARGE_READ_UNCLAIMED_MS: A's next hold runs the bounded inline pass, the REAL search pass, over B's source.
  clock.t += 6 * 60_000;
  const second = await processBatch([{ messageId: `m${++n}`, body: requeueBody(key, upload, holds[0]!) }], deps(true));
  expect(second.batchItemFailures).toEqual([]);
  expect(holds).toHaveLength(1); // no further hold
  expect((await r.pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index'")).rows).toEqual([]);
  // The parsed authorized read now returns B's marker; a wrong token is refused.
  expect(await read("markerb")).toEqual({ status: 200, texts: ["markerb"] });
  expect((await app.request(`/v1/events?workspace=${scope.workspaceId}&q=markerb&limit=50`, { headers: { authorization: "Bearer wrong" } })).status).toBe(401);
}, 120_000);
