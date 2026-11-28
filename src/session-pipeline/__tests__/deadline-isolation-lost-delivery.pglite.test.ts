/**
 * T124 (Pipeline 220fe2274d20 / ec758b1fab8e): a below-threshold scope B whose S3-event delivery is lost, with no later
 * append, recovers only through a durable obligation written with its pointer. The candidate is the managed search
 * wake: hoopDbSessionIndex's publication transaction writes the `search-index` job (pipeline_jobs) in the SAME
 * authority transaction as the pointer when publication.enqueueSearchWake is exactly true (lambda.ts:
 * SEARCH_WAKE_ENQUEUE=1; consumer-service.ts / indexer.ts: their own flags), and the managed search-jobs drain
 * (SEARCH_JOBS_ENABLED) claims and runs it.
 *
 * Enabled test flags: publication.enqueueSearchWake = true (publishedHead's default). Everything else default.
 * Real: migrations on PGlite, the ingest delivery, the portable publisher against a MinIO-shaped fake, object-landed and
 * the managed session-indexed authority path (search-jobs-publication.ts publishedHead), claimSearchJobs,
 * runSearchJob with the real indexSearchScope and a recording ClickHouse, settleSearchJob.
 * Forced: one more deadline. Drain 1's managed pass stops mid-stream (stream_deadline) and is settled as the Lambda's
 * loop settles it (searchSettlementFor: retry 30 s, one attempt spent); the wake survives (not dead-lettered). After
 * the backoff, drain 2 completes it. A scope whose every managed pass hits the deadline is retried until
 * SEARCH_JOB_MAX_ATTEMPTS, then dead-lettered (a durable letter, not a completed pass).
 * Suppressed: B's S3-event delivery to the session pipeline consumer (processBatch is never called) and every later
 * append. Test 2 ends at a PARSED AUTHORIZED READ: the real createReadApi (GET /v1/events, fixture token verifier,
 * pointer liveness from the publication database's session_index) over the registered Postgres search projection
 * (latency-session-coalesce's integration-authorized-read rig, e4ab1141, adapted): 501 session_partial while B is
 * partial, B's marker after the wake's second drain, 401 for a wrong token. Not covered: an actual SQS loss, the
 * hosted schedule that drains held scopes, a real ClickHouse backend, and a delete then re-create (T139).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { claimSearchJobs, runSearchJob, searchSettlementFor, settleSearchJob } from "../search-jobs.ts";
import { objectStoreManifestReader } from "../../shared/vendor.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { publishedHead, type PublishedHead } from "./search-jobs-publication.ts";
import { recordingReplayClickHouse } from "./replay-clickhouse-fixture.ts";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { createReadApi, AuthError } from "../../session-read-api/app.ts";
import type { SearchProjectionState } from "../search-adapter.ts";

describe("below-threshold B, delivery lost, no later append: the managed wake committed with the pointer recovers it", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead({ enqueueSearchWake: true }); }, 180_000);
  afterAll(() => head?.close());

  test("the wake commits with the pointer; with B's delivery suppressed, the drain claims it, runs B's managed pass and settles done", async () => {
    // The pointer and its wake committed together in the authority transaction (no consumer ran for B).
    expect(head.authority).toMatchObject({ acked: 1 });
    const pointer = (await head.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_index WHERE manifest_key=$1 AND deleted_at IS NULL", [head.key])).rows[0]!.n;
    expect(pointer).toBe(1);
    const wakes = head.jobsAfter.filter((j) => j.stage === "search-index");
    expect(wakes).toHaveLength(1);
    // Below the large-read discovery thresholds: discovery would never pick B up.
    const size = (await head.db.query<{ b: string; n: string }>(
      "SELECT coalesce(sum(raw_bytes),0)::text b, coalesce(sum(event_count),0)::text n FROM sessions.session_index WHERE manifest_key=$1", [head.key])).rows[0]!;
    expect(BigInt(size.b)).toBeLessThanOrEqual(67108864n);
    expect(BigInt(size.n)).toBeLessThanOrEqual(100000n);
    const recorded = recordingReplayClickHouse();
    const prefix = head.key.slice(0, head.key.indexOf("/", head.key.indexOf("/") + 1));
    const search = {
      transact: (work) => head.db.transaction((tx) => work(tx)),
      reader: objectStoreManifestReader(head.store, { provider: "s3", bucket: "selfhost-sessions" }),
      store: head.store, ch: recorded.ch, prefixFor: () => prefix,
    } as SearchIndexDeps;
    // Drain 1, forced into another deadline: B's managed pass cannot finish (whole-read bound of 1 byte, streaming on,
    // its stream deadline crossed after the start check). Settled as the Lambda's loop settles it (searchSettlementFor).
    let reads = 0; // the deadline is open for the start check, then crossed: the stream stops mid-pass
    const [first] = await claimSearchJobs(head.db, 1);
    expect(first).toBeDefined();
    const stalled = await runSearchJob(first!.claimedMessage, {
      search: { ...search, limits: { maxScopeBytes: 1, maxScopeEvents: 1 }, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 }, streamDeadline: { at: 1e14, now: () => (++reads <= 1 ? 0 : 2e14) } },
    });
    expect(stalled).toMatchObject({ outcome: "held", reasons: ["scope_too_large", "stream_deadline"] });
    const settlement = searchSettlementFor(stalled, first!.attempts);
    expect(settlement).toEqual({ retry: 30 });
    expect(await settleSearchJob(head.db, first!, settlement)).toMatchObject({ applied: true });
    // The wake survives the stalled pass: still queued, not dead-lettered, backed off.
    const kept = (await head.db.query<{ dlq: boolean; attempts: number }>("SELECT dlq, attempts FROM sessions.pipeline_jobs WHERE stage='search-index'")).rows;
    expect(kept).toEqual([{ dlq: false, attempts: 1 }]);
    // The backoff elapses (test clock: make the job visible now), and drain 2 runs B's managed pass to completion.
    await head.db.query("UPDATE sessions.pipeline_jobs SET visible_at=now() WHERE stage='search-index'");
    const [claimed] = await claimSearchJobs(head.db, 1);
    expect(claimed).toBeDefined();
    const out = await runSearchJob(claimed!.claimedMessage, { search });
    expect(out.outcome).toBe("indexed");
    expect(await settleSearchJob(head.db, claimed!, "done")).toMatchObject({ applied: true });
    expect((await head.db.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index' AND dlq=false")).rows).toEqual([]);
    // Furthest endpoint verified here: B's search state written by its managed pass (recording ClickHouse).
    const states = Object.values(recorded.stats().states) as { session_id: string; needs_full: number }[];
    expect(states).toEqual([expect.objectContaining({ session_id: head.scope.sessionId, needs_full: 0 })]);
  }, 180_000);
});

// The parsed AUTHORIZED read (latency-session-coalesce's integration-authorized-read rig, e4ab1141, adapted): the search
// backend is the registered Postgres projection on its own PGlite (indexSearchScope writes it through deps.adapter), and
// the read is the real createReadApi (GET /v1/events, a fixture token verifier) with pointer liveness from the real
// session_index of the managed publication's database.
describe("below-threshold B, delivery lost, another deadline: an authorized caller gets B's marker after the wake's second drain", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead({ enqueueSearchWake: true }); }, 180_000);
  afterAll(() => head?.close());

  test("no authorized hit before the drain; drain 1 stops on the deadline (wake kept); drain 2 indexes; the parsed authorized read returns B's marker", async () => {
    const proj = new PGlite({ extensions: { pg_trgm } });
    await proj.exec("CREATE EXTENSION pg_trgm");
    try {
      const tenants: TenantSqlRunner = (orgId, workspaceId, work) =>
        proj.transaction(async (tx) => { await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]); return work(tx); });
      let complete = false;
      const state: SearchProjectionState = {
        async fromMs() { return 0; },
        async record(_tx, _scope, value) { complete = value.complete; },
        async partials() { return { sessions: complete ? [] : [head.scope.sessionId], overflow: false }; },
      };
      const adapter = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: proj }, "writer", tenants);
      await adapter.index.ensureSchema();
      const prefix = head.key.slice(0, head.key.indexOf("/", head.key.indexOf("/") + 1));
      // The fixture's event is at 2025-11-01; the pass's clock sits one day later so it is inside the search window.
      const NOW = 1_761_955_200_000 + 86_400_000;
      const search = {
        transact: (work) => head.db.transaction(async (tx) => work({ query: async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows }) } as SqlExecutor)),
        reader: objectStoreManifestReader(head.store, { provider: "s3", bucket: "selfhost-sessions" }),
        store: head.store, adapter, prefixFor: () => prefix, now: () => new Date(NOW),
      } as SearchIndexDeps;
      const tenant = { orgId: head.scope.orgId, workspaceId: head.scope.workspaceId };
      const app = createReadApi({ adapter, completeSinceMs: 0,
        verify: async (token) => { if (token !== "fixture") throw new AuthError(401, "invalid_token", "invalid token"); return { ...tenant, userId: "user_1", scopes: ["rth:read"] }; },
        liveness: {
          async erased() { return new Set(); },
          async sessionStart() { return { pointers: 1, live: 1, createdAtMs: Date.parse("2025-11-01T00:00:00.000Z") }; },
          async livePointerSessions(org, ws, ids) {
            const live = (await head.db.query<{ session_id: string }>(
              "SELECT DISTINCT session_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3) AND deleted_at IS NULL", [org, ws, [...ids]])).rows;
            return new Set(live.map((x) => x.session_id));
          },
        },
      });
      /** The parsed authorized response: 200 with the event texts, or the read API's refusal code. */
      const read = async (q: string) => {
        const res = await app.request(`/v1/events?workspace=${tenant.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
        const body = (await res.json()) as { events?: { text?: string; content?: string }[]; error?: { code: string; reason?: string } };
        return res.status === 200
          ? { status: 200, texts: (body.events ?? []).map((e) => String(e.text ?? e.content ?? "")) }
          : { status: res.status, code: body.error?.code, reason: body.error?.reason };
      };
      // While B is partial the read API refuses to answer for it (never a silently empty 200).
      const partial = { status: 501, code: "unsupported_by_backend", reason: "session_partial" };
      // B's delivery is suppressed (no consumer ran): B is not served yet.
      expect(await read("needle")).toEqual(partial);
      // Drain 1: another deadline (stops mid-stream), settled as the Lambda's loop settles it; the wake is kept.
      let reads = 0;
      const [first] = await claimSearchJobs(head.db, 1);
      const stalled = await runSearchJob(first!.claimedMessage, {
        search: { ...search, limits: { maxScopeBytes: 1, maxScopeEvents: 1 }, stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 }, streamDeadline: { at: 1e14, now: () => (++reads <= 1 ? 0 : 2e14) } },
      });
      expect(stalled.outcome).toBe("held");
      expect(stalled.reasons).toContain("stream_deadline");
      expect(await settleSearchJob(head.db, first!, searchSettlementFor(stalled, first!.attempts))).toMatchObject({ applied: true });
      expect((await head.db.query<{ dlq: boolean }>("SELECT dlq FROM sessions.pipeline_jobs WHERE stage='search-index'")).rows).toEqual([{ dlq: false }]);
      expect(await read("needle")).toEqual(partial);
      // Drain 2 after the backoff (test clock).
      await head.db.query("UPDATE sessions.pipeline_jobs SET visible_at=now() WHERE stage='search-index'");
      const [second] = await claimSearchJobs(head.db, 1);
      const out = await runSearchJob(second!.claimedMessage, { search });
      expect(out.outcome).toBe("indexed");
      expect(await settleSearchJob(head.db, second!, "done")).toMatchObject({ applied: true });
      // The parsed authorized read: B's marker; an unauthorized caller is refused.
      expect(await read("needle")).toEqual({ status: 200, texts: ["search wake fixture needle"] });
      expect((await app.request(`/v1/events?workspace=${tenant.workspaceId}&q=needle&limit=50`, { headers: { authorization: "Bearer wrong" } })).status).toBe(401);
    } finally {
      await proj.close();
    }
  }, 180_000);
});
