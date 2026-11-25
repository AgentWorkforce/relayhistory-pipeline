/**
 * Review fixes on the search-index consumer (docs/handoffs/incremental-search-indexing.md, M2), beside the accepted
 * acceptance file search-jobs.red.pglite.test.ts:
 *   - writer opt-in: the authority path writes the managed wake only for publication.enqueueSearchWake === true;
 *   - guard scope: the committed-state guard belongs to the managed search-jobs path only; the legacy S3-event
 *     indexSearchScope still erases a scope with no pointer rows, and the managed path refuses to under a restricted
 *     role;
 *   - sibling clock origin: a sibling whose attempt origin is not its own head's contributes nothing (the producer
 *     refuses durably; the catch-up takes the head's own clock), never another origin's clock;
 *   - visibility and trace: an equal or older epoch never pulls an unleased row's visible_at forward (catch-up and
 *     replay alike), the row's trace_id changes only with its message, and the claim never takes a dlq row.
 * Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import {
  catchUpTick, claimSearchJobs, enqueueSearchWake, replaySearchDeadLetter, runSearchJob, searchLetterId, searchWorkKey, settleSearchJob,
  type SearchWakeMessage,
} from "../search-jobs.ts";
import { SCOPE, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { addReplayPointer, clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";
import { publishedHead, siblingHead, type PublishedHead } from "./search-jobs-publication.ts";

const STAGE = "search-index";
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

type Scope = { orgId: string; workspaceId: string; harness: string; sessionId: string };
const wake = (scope: Scope, epoch: number, traceId: string = crypto.randomUUID()): SearchWakeMessage => ({
  v: 1, stage: STAGE, ctx: { traceId, ...scope }, workKey: searchWorkKey(scope), revision: 1,
  payload: { epoch, clock: { source: "pointer", at: new Date().toISOString() } },
});

// ---------------------------------------------------------------------------------------------------------
describe("writer opt-in (review finding 3): no explicit true, no wake", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead({ enqueueSearchWake: undefined }); }, 180_000);
  afterAll(() => head?.close());

  test("the authority transaction commits and fans out, but writes no search-index job and no refusal row", async () => {
    // Fixture soundness: the managed authority path ran and committed (the fan-out exists).
    expect(head.authority).toMatchObject({ acked: 1 });
    expect(head.jobsAfter.map((j) => j.stage)).toEqual(expect.arrayContaining(["summary-short", "summary-long", "status-classify"]));
    expect(head.jobsAfter.filter((j) => j.stage === STAGE)).toEqual([]);
    expect((await head.db.query("SELECT 1 FROM sessions.pipeline_events WHERE stage = 'search-index'")).rows).toEqual([]);
    // The retired control writes nothing either.
    expect(head.retired.jobsWritten).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("sibling clock origin (review finding 6): each sibling's attempt must match its OWN head's origin", () => {
  // A live sibling of the canonical scope whose newer attempt carries the TRIGGER's origin (origin-1), not the
  // sibling head's own (origin-r6-forged): it must contribute nothing.
  let head: PublishedHead;
  let forged = "";
  beforeAll(async () => {
    head = await publishedHead({
      beforeAuthority: async ({ db, key, publishedAt }) => {
        forged = new Date(Date.parse(publishedAt) + 5_000).toISOString();
        await siblingHead(db, key, "r6-forged", { publishedAt: forged, attemptOrigin: "origin-1" });
      },
    });
  }, 180_000);
  afterAll(() => head?.close());

  test("fixture: the sibling is live in the canonical scope, its head origin differs from its attempt's", async () => {
    const { rows } = await head.db.query<{ head_origin: string; attempt_origin: string; state: string }>(
      `SELECT h.origin_id AS head_origin, a.origin_id AS attempt_origin, h.state FROM sessions.ingest_publication_heads h
         JOIN sessions.ingest_publication_images i ON i.manifest_key = h.current_key
         JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
        WHERE h.current_key = $1`, [`${head.key}.r6-forged`]);
    expect(rows).toEqual([{ head_origin: "origin-r6-forged", attempt_origin: "origin-1", state: "live" }]);
  });

  test("producer: no job carries another origin's clock; the scope is refused durably (clock_unprovable), still readable", async () => {
    expect(head.authority).toMatchObject({ acked: 1 });
    expect(head.jobsAfter.filter((j) => j.stage === STAGE)).toEqual([]);
    const refusals = (await head.db.query<{ code: string }>(
      "SELECT error->>'code' AS code FROM sessions.pipeline_events WHERE stage = 'search-index' AND status = 'skipped'")).rows;
    expect(refusals).toEqual([{ code: "clock_unprovable" }]);
    expect((await head.db.query("SELECT 1 FROM sessions.session_index WHERE manifest_key = $1 AND deleted_at IS NULL", [head.key])).rows).toHaveLength(1);
  });

  test("consumer catch-up: the clock is the trigger head's own published_at, never the forged sibling's newer one", async () => {
    await catchUpTick({ sql: head.db, pageSize: 500 });
    const jobs = (await head.db.query<{ message: SearchWakeMessage }>("SELECT message FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows;
    expect(jobs).toHaveLength(1);
    expect(jobs[0]!.message.payload.clock.source).toBe("publish");
    expect(Date.parse(jobs[0]!.message.payload.clock.at)).toBe(Date.parse(head.publishedAt));
    expect(Date.parse(jobs[0]!.message.payload.clock.at)).not.toBe(Date.parse(forged));
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("visibility and trace (review finding 7)", () => {
  const scope = { orgId: "org_r7", workspaceId: "ws_r7", harness: "claude", sessionId: "s_r7" };
  async function clone() {
    const c = await clonedReplayDatabase();
    cleanup.push(() => c.pg.close());
    return c;
  }
  const db = async () => (await clone()).pg;
  const row = async (pg: PGlite) => (await pg.query<{ epoch: string; trace_id: string; message_trace: string; visible: boolean; leased: boolean; dlq: boolean; attempts: number; visible_at: string }>(
    `SELECT message->'payload'->>'epoch' AS epoch, trace_id::text, message->'ctx'->>'traceId' AS message_trace, visible_at <= now() AS visible,
            lease_id IS NOT NULL AS leased, dlq, attempts, visible_at::text FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = $1`,
    [searchWorkKey(scope)])).rows;
  /** Enqueue N, claim it and retry with a long backoff: an unleased row that must stay invisible. */
  async function backedOff(pg: PGlite, epoch: number) {
    const w = wake(scope, epoch);
    await enqueueSearchWake(pg, w);
    const [c] = await claimSearchJobs(pg, 1);
    expect(await settleSearchJob(pg, c!, { retry: 600 })).toMatchObject({ applied: true, action: "retried" });
    const [r] = await row(pg);
    expect(r).toMatchObject({ epoch: String(epoch), visible: false, leased: false, trace_id: w.ctx.traceId, message_trace: w.ctx.traceId });
    return { w, r: r! };
  }

  test("an EQUAL or OLDER epoch (catch-up re-visit) keeps the backoff, the message and its trace; a strictly newer one reopens now with its own trace", async () => {
    const pg = await db();
    const { w, r } = await backedOff(pg, 7);
    await enqueueSearchWake(pg, wake(scope, 7));
    await enqueueSearchWake(pg, wake(scope, 6));
    expect(await row(pg)).toEqual([r]);
    expect(await claimSearchJobs(pg, 5)).toEqual([]);
    const w8 = wake(scope, 8);
    await enqueueSearchWake(pg, w8);
    expect(await row(pg)).toEqual([expect.objectContaining({ epoch: "8", visible: true, attempts: 0, trace_id: w8.ctx.traceId, message_trace: w8.ctx.traceId })]);
    expect(w.ctx.traceId).not.toBe(w8.ctx.traceId);
  }, 120_000);

  test("catchUpTick re-visiting a backed-off scope at the same epoch keeps its backoff", async () => {
    const { pg, db: executor } = await clone();
    // A committed scope the catch-up visits (pointer clock); its job is backed off at the directory epoch.
    await addReplayPointer(executor, { identity: { orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.harness, sessionId: scope.sessionId },
      key: `96bulls/${scope.orgId}/catchup/${scope.sessionId}/manifest.json`, digest: "ab".repeat(32), physicalSession: scope.sessionId, physicalMachine: "m1", canonicalSession: scope.sessionId });
    await projectSessionDirectory(executor, scope);
    await catchUpTick({ sql: pg, pageSize: 10 });
    const [c] = await claimSearchJobs(pg, 1);
    await settleSearchJob(pg, c!, { retry: 600 });
    const before = await row(pg);
    expect(before).toEqual([expect.objectContaining({ visible: false, leased: false })]);
    await catchUpTick({ sql: pg, pageSize: 10 });
    expect(await row(pg)).toEqual(before);
  }, 120_000);

  test("a replay of a letter at an EQUAL epoch keeps the row's backoff, message and trace (no pull-forward)", async () => {
    const pg = await db();
    // A letter at 7 (exhausted), then a fresh wake 7 that is backed off.
    await enqueueSearchWake(pg, wake(scope, 7));
    const [c] = await claimSearchJobs(pg, 1);
    // Exhaustion is the server's (settle_search_job_v1, max 12 attempts): the leased row is on its last attempt.
    await pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [c!.id, c!.lease]);
    expect(await settleSearchJob(pg, c!, "exhausted")).toMatchObject({ applied: true, action: "parked" });
    const { r } = await backedOff(pg, 7);
    expect(await replaySearchDeadLetter(pg, searchLetterId(c!.claimedMessage))).toEqual({ replayed: true });
    expect(await row(pg)).toEqual([r]);
    expect(await claimSearchJobs(pg, 5)).toEqual([]);
  }, 120_000);

  test("the claim never takes a dlq row, visible or not; a newer epoch clears dlq and the row is claimable again", async () => {
    const pg = await db();
    await enqueueSearchWake(pg, wake(scope, 7));
    await pg.query("UPDATE sessions.pipeline_jobs SET dlq = true WHERE stage = 'search-index'");
    expect(await row(pg)).toEqual([expect.objectContaining({ visible: true, leased: false, dlq: true })]);
    expect(await claimSearchJobs(pg, 5)).toEqual([]);
    // An equal epoch keeps dlq (and so stays unclaimable).
    await enqueueSearchWake(pg, wake(scope, 7));
    expect(await claimSearchJobs(pg, 5)).toEqual([]);
    await enqueueSearchWake(pg, wake(scope, 8));
    const claimed = await claimSearchJobs(pg, 5);
    expect(claimed.map((j) => j.claimedEpoch)).toEqual([8]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("guard scope (review finding 4): the committed-state guard is the managed path's only", () => {
  let x: ExportedScope;
  beforeAll(async () => { x = await exportStreamScope(generate(1), 400); }, 120_000);
  afterAll(() => x?.dispose());
  const never = { ...SCOPE, sessionId: "never_committed" };
  const neverRows = (r: Awaited<ReturnType<typeof hostedRig>>) => r.fake.rows.filter((row) => row.session_id === never.sessionId);

  /** A rig whose real scope is indexed, plus three real search rows re-labelled to a scope with no session_index row. */
  async function seeded() {
    const r = await hostedRig(x, cleanup);
    expect((await r.tick({})).batchItemFailures).toEqual([]);
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    const copies = r.fake.rows.filter((row) => row.session_id === SCOPE.sessionId || row.session_id === x.canonicalId).slice(0, 3)
      .map((row) => ({ ...row, session_id: never.sessionId }));
    expect(copies).toHaveLength(3);
    r.fake.rows.push(...copies);
    expect((await r.db.pg.query("SELECT 1 FROM sessions.session_index WHERE session_id = $1 OR canonical_session_id = $1", [never.sessionId])).rows).toEqual([]);
    return r;
  }

  test("LEGACY (no opt-in, the S3-event path): a scope with no pointer rows is erased as before M2, with no committed-state SQL", async () => {
    const r = await seeded();
    r.sql.length = 0;
    const result = await indexSearchScope(never, r.search());
    expect(result.reasons).toContain("erased");
    expect(neverRows(r)).toEqual([]);
    expect(r.sql.filter((s) => /session_erasure_fences|has_table_privilege/.test(String(s.sql)))).toEqual([]);
  }, 120_000);

  test("MANAGED under a restricted role (no fence SELECT): runSearchJob and the opted-in pass refuse to erase the uncommitted scope", async () => {
    const r = await seeded();
    const role = "review_search_restricted";
    await r.db.pg.exec(`CREATE ROLE ${role} NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
      GRANT USAGE ON SCHEMA sessions TO ${role};
      GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO ${role};
      REVOKE ALL ON sessions.session_erasure_fences FROM ${role};
      CREATE POLICY review_restricted_read ON sessions.session_index FOR SELECT TO ${role} USING (true);
      CREATE POLICY review_restricted_read ON sessions.session_directory FOR SELECT TO ${role} USING (true);`);
    const restricted: SearchIndexDeps = {
      ...r.search(),
      transact: (work) => r.transact(async (tx) => { await tx.query(`SET LOCAL ROLE ${role}`); return work(tx); }),
    };
    // Fixture soundness: the role is restricted (no fence SELECT) and sees the committed rows.
    await r.transact(async (tx) => {
      await tx.query(`SET LOCAL ROLE ${role}`);
      expect((await tx.query<{ ok: boolean }>("SELECT has_table_privilege('sessions.session_erasure_fences', 'SELECT') AS ok")).rows).toEqual([{ ok: false }]);
      expect((await tx.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_index WHERE session_id = $1", [SCOPE.sessionId])).rows[0]!.n).toBeGreaterThan(0);
    });
    const before = JSON.stringify(neverRows(r));
    const out = await runSearchJob(wake(never, 0), { search: restricted });
    expect(out.outcome).toBe("not_committed");
    expect(JSON.stringify(neverRows(r))).toBe(before);
    // The work pass's own recheck under the final locks (what runSearchJob passes on): no erase either.
    const pass = await indexSearchScope(never, { ...restricted, committedGuard: true });
    expect(pass.reasons).toEqual(["not_committed"]);
    expect(JSON.stringify(neverRows(r))).toBe(before);
  }, 120_000);
});
