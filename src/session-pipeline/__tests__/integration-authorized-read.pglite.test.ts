/**
 * T124 + T126 integration: PARSED AUTHORIZED READS for a same-key, same-revision replacement WITHOUT a delete (not T139-blocked).
 * A is indexed; B replaces A at the same key and revision; delayed and duplicate A deliveries follow. The marker an
 * AUTHORIZED caller gets back from the real read API (createReadApi, GET /v1/events) must be B's, before and after
 * A's deliveries finish, on the standard queue and on the FIFO path, with SEARCH_INDEX_SKIP_CURRENT_ENABLED on.
 *
 * Real: the consumer (processBatch / dispatchSqsBatch), the pointer stage on PGlite with every migration, the search stage
 * (indexSearchScope) on that database, the registered Postgres search projection, and the read API with pointer liveness
 * from the real session_index. Mocked: S3 is the fs store (its ETag stands in for the versionId); the scope lock is the
 * harness mutex; the projection is its own PGlite; the token verifier is a fixture; SQS records are built as the ESM
 * delivers them. Out of scope (T139, 8aba8f82): a delete then re-create reusing revision numbers. Synthetic data only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey } from "../../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { createReadApi, AuthError } from "../../session-read-api/app.ts";
import type { SearchProjectionState } from "../search-adapter.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { dispatchSqsBatch, type RevisionWiring, type SqsRecord } from "../revision-dispatch.ts";
import { revisionMessage } from "../revision-events.ts";
import { pointerRig, loc } from "./pointer-lock-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });

const NOW = new Date("2026-11-02T00:00:00.000Z");
const scope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const tenant = { orgId: scope.orgId, workspaceId: scope.workspaceId };
const ARN = "arn:aws:sqs:us-east-1:000000000000:synthetic-revisions.fifo";

async function rig() {
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
    async record(_tx, _scope, value) { complete = value.complete; },
    async partials() { return { sessions: complete ? [] : [scope.sessionId], overflow: false }; },
  };
  const adapter = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: proj }, "writer", tenants);
  await adapter.index.ensureSchema();
  const transact: SearchIndexDeps["transact"] = (work) =>
    r.pg.transaction(async (tx) => work({ query: async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows }) } as SqlExecutor));
  const reader = r.pointer().reader;
  const search = (skipCurrent: boolean): SearchIndexDeps => ({ transact, reader, store: r.store, adapter, prefixFor: (org) => `96bulls/${org}`, now: () => NOW, ...(skipCurrent ? { skipCurrent: true } : {}) });
  const deps = (skipCurrent = true): ConsumerDeps => ({ pointer: r.pointer(), search: search(skipCurrent), requeue: async (k) => { r.requeued.push(k); } });
  let n = 0;
  const etag = async (key: string) => (await r.store.head(objectKey(key)))!.version.eTag!.replace(/^"|"$/g, "");
  /** Standard queue: an S3 ObjectCreated notification for `eTag`. */
  const standard = async (key: string, eTag: string, skipCurrent = true) =>
    (await processBatch([{ messageId: `m${++n}`, body: r.body(key, "ObjectCreated:Put", String(n).padStart(4, "0"), eTag) }], deps(skipCurrent))).batchItemFailures;
  const wiring: RevisionWiring = {
    config: { queueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/synthetic-revisions.fifo", mode: "direct", busyRetrySeconds: 5 },
    sender: async () => ({ failed: [] }), changeVisibility: async () => {},
  };
  /** FIFO: a writer revision event for `eTag` at `revision`. */
  const fifo = async (key: string, eTag: string, revision: number, skipCurrent = true) => {
    const m = revisionMessage({ key, etag: eTag, revision, committedAt: NOW.toISOString(), source: "writer" })!;
    const id = `f${++n}`;
    const rec: SqsRecord = { messageId: id, receiptHandle: `rh-${id}`, body: m.MessageBody, eventSourceARN: ARN,
      attributes: { MessageGroupId: m.MessageGroupId, MessageDeduplicationId: m.MessageDeduplicationId, SentTimestamp: String(NOW.getTime()), ApproximateReceiveCount: "1" } };
    return (await dispatchSqsBatch([rec], deps(skipCurrent), wiring)).batchItemFailures;
  };
  // The AUTHORIZED read: the real read API, a fixture token, pointer liveness from the real session_index.
  const app = createReadApi({ adapter, completeSinceMs: 0,
    verify: async (token) => { if (token !== "fixture") throw new AuthError(401, "invalid_token", "invalid token"); return { ...tenant, userId: "user_1", scopes: ["rth:read"] }; },
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
  /** Parsed authorized response: the marker texts an authorized caller gets back for a text search. */
  const hits = async (q: string) => {
    const res = await app.request(`/v1/events?workspace=${scope.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { events: { text?: string; content?: string }[] };
    return body.events.map((e) => String(e.text ?? e.content ?? ""));
  };
  /** An unauthorized caller is refused (the read is really authorized, not open). */
  const unauthorized = async () => (await app.request(`/v1/events?workspace=${scope.workspaceId}&q=markerb&limit=50`, { headers: { authorization: "Bearer wrong" } })).status;
  return { r, standard, fifo, hits, etag, unauthorized };
}

/** A indexed, then B written over it at the same key and revision. Returns A's stale ETag. */
async function aThenB(t: Awaited<ReturnType<typeof rig>>, notifyB: boolean, path: "standard" | "fifo") {
  const key = await t.r.writeManifest(1, loc, "markera", "markera");
  const a = await t.etag(key);
  expect(path === "standard" ? await t.standard(key, a) : await t.fifo(key, a, 1)).toEqual([]);
  expect(await t.hits("markera")).toEqual(["markera"]);
  await t.r.writeManifest(1, loc, "markerb", "markerb");
  const b = await t.etag(key);
  expect(b).not.toBe(a);
  if (notifyB) expect(path === "standard" ? await t.standard(key, b) : await t.fifo(key, b, 1)).toEqual([]);
  return { key, a };
}

for (const path of ["standard", "fifo"] as const) {
  describe(`${path} path, skip-current on: parsed authorized reads`, () => {
    test("B notified and indexed; delayed A then duplicate A: an authorized caller gets B before and after both A deliveries, never A", async () => {
      const t = await rig();
      const { key, a } = await aThenB(t, true, path);
      expect(await t.hits("markerb")).toEqual(["markerb"]);
      expect(await t.hits("markera")).toEqual([]);
      for (let i = 0; i < 2; i++) {
        expect(path === "standard" ? await t.standard(key, a) : await t.fifo(key, a, 1)).toEqual([]);
        expect(await t.hits("markerb")).toEqual(["markerb"]);
        expect(await t.hits("markera")).toEqual([]);
      }
      expect(await t.unauthorized()).toBe(401);
    }, 60_000);

    test("B's notification LOST; delayed A alone: after A finishes an authorized caller gets B (not A)", async () => {
      const t = await rig();
      const { key, a } = await aThenB(t, false, path);
      // Before A's delayed delivery: the index still serves A (B was never notified).
      expect(await t.hits("markera")).toEqual(["markera"]);
      expect(path === "standard" ? await t.standard(key, a) : await t.fifo(key, a, 1)).toEqual([]);
      expect(await t.hits("markerb")).toEqual(["markerb"]);
      expect(await t.hits("markera")).toEqual([]);
    }, 60_000);
  });
}
