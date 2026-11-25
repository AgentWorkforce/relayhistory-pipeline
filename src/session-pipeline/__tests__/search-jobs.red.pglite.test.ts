/**
 * RED until M2 (docs/handoffs/incremental-search-indexing.md rev 4, §10): the committed `search-index` wake.
 *
 * Every test first proves its fixture is sound with today's code (today's stages insert and claim; the
 * dead-letter table walks dead → replaying → resolved; the committed scope indexes completely; the real
 * publication commits its head; the catch-up page query returns the seeded stale scopes), then asserts the
 * designed contract. Each fails today only on the intended missing piece:
 *   - the migration (§7: the stage CHECKs, the partial unique index of §3, `search_catchup_cursor` of §6),
 *     exercised with the design's verbatim SQL: the §3.1 upsert, the §3.3 stage-specific done / retry /
 *     exhausted settlements and the §3.3b stage-aware replay, all written out below;
 *   - the closed TS stage lists (§7) and therefore `checked()`;
 *   - the authority-path writer (§2);
 *   - the module M2 adds, `src/session-pipeline/search-jobs.ts` (`runSearchJob`, `settleSearchJob`,
 *     `catchUpTick`), imported only after the full fixture is built;
 *   - today's erase-on-absence behaviour of `indexSearchScope` (R3a).
 *
 * Message shape (rev 4 §2): `payload = { epoch, clock: { source: 'publish' | 'pointer', at } }`; never a null
 * clock, never `payload.published_at`. R7 is deferred to M3 and is not in this file.
 *
 * Synthetic, secret-free data only.
 * Run: ~/.local/bin/aw-heavy -- bun test src/session-pipeline/__tests__/search-jobs.red.pglite.test.ts
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PIPELINE_STAGES } from "@rhc/lib/pipeline/trace.ts";
import { QUEUED_STAGES, STAGE_REGISTRY } from "@rhc/lib/pipeline/registry.ts";
import { InMemoryDeadLetterStore, deadLetterId, isStageMessage, stageForQueue, stageQueueName, type StageMessage, type StageQueueDeps } from "@rhc/lib/pipeline/dlq.ts";
import { MemoryPipelineQueue, postgresPipelineQueue, postgresStageConsumer, type PostgresStageWorkItem } from "@rhc/lib/pipeline/queue.ts";
import { InMemoryPipelineEventSink } from "@rhc/lib/pipeline/events.ts";
import { postgresDeadLetterStore } from "@rhc/lib/pipeline/postgres-dead-letters.ts";
import { summarizeTrace } from "@rhc/lib/pipeline/traces.ts";
import { pipelineQueueDeps, stageBindings } from "@rhc/lib/pipeline/index.ts";
import { stageTimesSql } from "../../proof/aws/probes.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { indexSearchScope } from "../search-index.ts";
import { SCOPE, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { addReplayPointer, clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";
import { publishedHead, type PublishedHead } from "./search-jobs-publication.ts";

const STAGE = "search-index";
const root = join(import.meta.dir, "../../..");
const source = (p: string) => readFileSync(join(root, p), "utf8");
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

type Db = Awaited<ReturnType<typeof clonedReplayDatabase>>;
type Scope = { orgId: string; workspaceId: string; harness: string; sessionId: string };
type Clock = { source: "publish" | "pointer"; at: string };
type WakePayload = { epoch: number; clock: Clock; replay_id?: string };

/** A migrated PGlite clone whose queue accepts and claims today's stages (fixture soundness). */
async function migrated(): Promise<Db> {
  const db = await clonedReplayDatabase();
  cleanup.push(() => db.pg.close());
  await queueIsSound(db);
  return db;
}

/** Today's stage, through the real producer and the real claim/start/settle: proves the queue fixture. */
async function queueIsSound(db: Db) {
  const message = stageMessage("session-indexed", { orgId: "org_f", workspaceId: "ws_f", harness: "claude", sessionId: "s_f" }, "fixture@1", {});
  await postgresPipelineQueue(db.pg).send(message);
  const consumer = postgresStageConsumer({ sql: db.pg, stage: "session-indexed", deps: noDeps(), maxAttempts: 6 });
  const [claimed] = await consumer.source.claim(1);
  const started = await consumer.source.start!(claimed!);
  expect((started!.message as StageMessage).workKey).toBe("fixture@1");
  await consumer.source.settle(started!, { status: "done" });
  expect((await db.pg.query("SELECT 1 FROM sessions.pipeline_jobs")).rows).toHaveLength(0);
}

/**
 * Today's stage through the real dead-letter store (postgres-dead-letters.ts): quarantine → `dead`,
 * claimForReplay → `replaying` with last_replay_id, resolve → `resolved`. Proves the table and the state
 * machine that the in-test §3.3 exhausted and §3.3b replay transactions write. Leaves the table empty.
 */
async function deadLettersAreSound(db: Db) {
  const store = postgresDeadLetterStore(db.pg);
  const letter = await store.quarantine(stageMessage("session-indexed", { orgId: "org_f", workspaceId: "ws_f", harness: "claude", sessionId: "s_f" }, "fixture-dl@1", {}), new Date());
  expect(letter).toMatchObject({ stage: "session-indexed", state: "dead", deadCount: 1, replayCount: 0 });
  const replayId = crypto.randomUUID();
  expect(await store.claimForReplay(letter.id, { state: "dead", replayCount: 0 }, replayId, new Date())).toBe(true);
  expect((await store.list({ states: ["replaying"] })).map((l) => [l.id, l.lastReplayId])).toEqual([[letter.id, replayId]]);
  await store.resolve(letter.id, new Date());
  expect((await store.list({ states: ["resolved"] })).map((l) => l.id)).toEqual([letter.id]);
  await db.pg.query("DELETE FROM sessions.pipeline_dead_letters");
}

const noDeps = (): StageQueueDeps => ({ handlers: {}, sink: new InMemoryPipelineEventSink(), deadLetters: new InMemoryDeadLetterStore() });

function stageMessage(stage: string, scope: Scope, workKey: string, payload: Record<string, unknown>, traceId: string = crypto.randomUUID()): StageMessage {
  return {
    v: 1, stage: stage as StageMessage["stage"],
    ctx: { traceId, orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, sessionId: scope.sessionId },
    workKey, revision: 1, payload,
  };
}

/** Design §2: `search␟<org>␟<ws>␟<harness>␟<canonical session id>`. */
const searchWorkKey = (s: Scope) => ["search", s.orgId, s.workspaceId, s.harness, s.sessionId].join("\u001f");

const publishClock = (at: string = new Date().toISOString()): Clock => ({ source: "publish", at });

/** The design §2 (rev 4) job message: a full StageMessage, `payload = { epoch, clock: { source, at } }`. */
function wake(scope: Scope, epoch: number | string, clock: Clock = publishClock(), traceId?: string): StageMessage<WakePayload> {
  return stageMessage(STAGE, scope, searchWorkKey(scope), { epoch: Number(epoch), clock }, traceId) as StageMessage<WakePayload>;
}

/** Design §3.1 (rev 4), verbatim: the one-row-per-scope coalescing enqueue; a NEW epoch resets attempts and dlq. */
const ENQUEUE = `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message, visible_at)
SELECT m->>'stage', m->>'workKey', (m->'ctx'->>'traceId')::uuid, m->'ctx'->>'orgId', m->'ctx'->>'workspaceId',
       m->'ctx'->>'sessionId', NULL, m, now()
  FROM (SELECT $1::jsonb AS m) x
ON CONFLICT (work_key) WHERE stage = 'search-index'
DO UPDATE SET
  message = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                 THEN EXCLUDED.message ELSE pipeline_jobs.message END,
  attempts = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
                  THEN 0 ELSE pipeline_jobs.attempts END,
  dlq = CASE WHEN (EXCLUDED.message->'payload'->>'epoch')::bigint > (pipeline_jobs.message->'payload'->>'epoch')::bigint
             THEN false ELSE pipeline_jobs.dlq END,
  visible_at = CASE WHEN pipeline_jobs.lease_id IS NULL THEN least(pipeline_jobs.visible_at, now()) ELSE pipeline_jobs.visible_at END`;

/** Design §3.3 `done`, verbatim: delete only when the wanted epoch did not move on; else reopen. $1 id, $2 lease, $3 claimed epoch. */
const DONE_DELETE = `DELETE FROM sessions.pipeline_jobs WHERE id = $1::bigint AND lease_id = $2::uuid
    AND (message->'payload'->>'epoch')::bigint <= $3::bigint RETURNING id, message->'payload'->>'replay_id' AS replay_id`;
const DONE_REOPEN = `UPDATE sessions.pipeline_jobs SET lease_id = NULL, visible_at = now(), attempts = 0
   WHERE id = $1::bigint AND lease_id = $2::uuid RETURNING id`;

/** Design §3.3 `retry` (rev 4), verbatim: claimed-epoch aware, lease-bound. $1 id, $2 lease, $3 claimed epoch, $4 backoff seconds. */
const RETRY = `UPDATE sessions.pipeline_jobs
   SET lease_id = NULL,
       visible_at = CASE WHEN (message->'payload'->>'epoch')::bigint > $3::bigint THEN now() ELSE now() + make_interval(secs => $4::integer) END,
       attempts   = CASE WHEN (message->'payload'->>'epoch')::bigint > $3::bigint THEN 0 ELSE attempts END
 WHERE id = $1::bigint AND lease_id = $2::uuid RETURNING id`;

/** Design §4.5: the registry backoff, 30 s · 2^n capped at 900 s. */
const backoff = (attempt: number) => Math.min(900, 30 * 2 ** Math.max(0, attempt - 1));

const enqueue = (db: Db, m: StageMessage) => db.pg.query(ENQUEUE, [JSON.stringify(m)]);

/** The rows of one scope's search-index job. */
async function jobs(db: Db, workKey: string) {
  return (await db.pg.query<{ id: string; epoch: string; leased: boolean; visible: boolean; attempts: number; dlq: boolean; lease_id: string | null; replay_id: string | null; clock: Clock | null }>(
    `SELECT id::text, message->'payload'->>'epoch' AS epoch, lease_id IS NOT NULL AS leased, visible_at <= now() AS visible, attempts, dlq, lease_id::text,
            message->'payload'->>'replay_id' AS replay_id, message->'payload'->'clock' AS clock
       FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = $1 ORDER BY id`, [workKey])).rows;
}

type Claimed = { started: PostgresStageWorkItem; lease: string; claimedEpoch: number; claimedMessage: StageMessage<WakePayload> };

/** The generic claim of `queue.ts` for the stage (design §3.2), with the claimed epoch and claim-time message recorded. */
async function claim(db: Db): Promise<Claimed | undefined> {
  const consumer = postgresStageConsumer({ sql: db.pg, stage: STAGE as never, deps: noDeps(), maxAttempts: 12, leaseSeconds: 300 });
  const [item] = await consumer.source.claim(1);
  if (!item) return undefined;
  const started = (await consumer.source.start!(item)) as PostgresStageWorkItem;
  const lease = (await db.pg.query<{ lease_id: string }>("SELECT lease_id::text FROM sessions.pipeline_jobs WHERE id = $1::bigint", [started.id])).rows[0]!.lease_id;
  const claimedMessage = started.message as StageMessage<WakePayload>;
  return { started, lease, claimedEpoch: Number(claimedMessage.payload.epoch), claimedMessage };
}

const retry = (db: Db, c: Claimed, seconds = backoff(c.started.attempt)) => db.pg.query(RETRY, [c.started.id, c.lease, c.claimedEpoch, seconds]);

/**
 * Design §3.3 `done` with §3.3b's letter resolution, in one transaction: the guarded DELETE; when it deletes a
 * replayed message, resolve the letter by `last_replay_id = payload.replay_id`; when it deletes nothing, reopen.
 */
async function settleDone(db: Db, c: Claimed): Promise<"deleted" | "reopened"> {
  return db.pg.transaction(async (tx) => {
    const deleted = (await tx.query<{ replay_id: string | null }>(DONE_DELETE, [c.started.id, c.lease, c.claimedEpoch])).rows;
    if (deleted.length) {
      const replayId = deleted[0]!.replay_id;
      if (replayId) await tx.query(
        `UPDATE sessions.pipeline_dead_letters SET state = 'resolved', resolved_at = now()
          WHERE stage = 'search-index' AND state = 'replaying' AND last_replay_id = $1::uuid`, [replayId]);
      return "deleted";
    }
    expect((await tx.query(DONE_REOPEN, [c.started.id, c.lease])).rows).toHaveLength(1);
    return "reopened";
  });
}

/**
 * Design §3.3 `exhausted` (rev 4), in ONE transaction bound to the lease:
 *  a) park the CLAIMED message only (claim-time snapshot), 0 rows when the lease was lost → abort;
 *  b) no newer wake: delete the row; c) a newer wake arrived during the lease: keep it, fresh budget, visible now.
 * The letter id and the re-quarantine upsert are those of postgres-dead-letters.ts `quarantine` (one letter per
 * unit of work; a re-death returns it to `dead`). pipeline_dead_letters has no error_code column; see the report.
 */
async function exhaust(db: Db, c: Claimed): Promise<{ parked: true; deleted: boolean }> {
  const m = c.claimedMessage;
  return db.pg.transaction(async (tx) => {
    const parked = await tx.query(
      `INSERT INTO sessions.pipeline_dead_letters AS d (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, first_dead_at, last_dead_at)
       SELECT $3, 'search-index', ($4::jsonb->'ctx'->>'traceId')::uuid, $4::jsonb->'ctx'->>'orgId', $4::jsonb->'ctx'->>'workspaceId',
              $4::jsonb->'ctx'->>'sessionId', NULL, $4::jsonb, now(), now()
         FROM sessions.pipeline_jobs WHERE id = $1::bigint AND lease_id = $2::uuid
       ON CONFLICT (id) DO UPDATE SET message = EXCLUDED.message, state = 'dead', last_dead_at = EXCLUDED.last_dead_at,
              dead_count = d.dead_count + 1, resolved_at = NULL
       RETURNING id`,
      [c.started.id, c.lease, deadLetterId(STAGE as never, m.ctx, m.workKey), JSON.stringify(m)]);
    if (parked.rows.length !== 1) throw new Error("exhausted: lease lost, nothing parked or deleted");
    const deleted = (await tx.query(DONE_DELETE, [c.started.id, c.lease, c.claimedEpoch])).rows.length === 1;
    if (!deleted) expect((await tx.query(DONE_REOPEN, [c.started.id, c.lease])).rows).toHaveLength(1);
    return { parked: true as const, deleted };
  });
}

/**
 * Design §3.3b stage-aware replay, one transaction: (1) mark the letter `replaying` with a new replay id;
 * (2) the §3.1 upsert with the letter's message plus payload.replay_id; (3) superseded (the upsert kept the
 * row's message: its epoch is at or above the letter's) → resolve the letter in the SAME transaction; merged
 * (the row now carries the letter's replay id) → the §3.3 done settlement resolves it later.
 */
async function replay(db: Db, letterId: string): Promise<{ replayId: string; outcome: "superseded" | "merged" }> {
  const replayId = crypto.randomUUID();
  return db.pg.transaction(async (tx) => {
    const marked = (await tx.query<{ message: StageMessage<WakePayload> }>(
      `UPDATE sessions.pipeline_dead_letters SET state = 'replaying', replay_count = replay_count + 1,
              last_replay_id = $2::uuid, last_replay_at = now()
        WHERE id = $1 AND state = 'dead' RETURNING message`, [letterId, replayId])).rows;
    expect(marked).toHaveLength(1);
    const m = marked[0]!.message;
    await tx.query(ENQUEUE, [JSON.stringify({ ...m, payload: { ...m.payload, replay_id: replayId } })]);
    const row = (await tx.query<{ replay_id: string | null }>(
      "SELECT message->'payload'->>'replay_id' AS replay_id FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = $1", [m.workKey])).rows;
    expect(row).toHaveLength(1);
    if (row[0]!.replay_id === replayId) return { replayId, outcome: "merged" as const };
    await tx.query("UPDATE sessions.pipeline_dead_letters SET state = 'resolved', resolved_at = now() WHERE id = $1 AND last_replay_id = $2::uuid", [letterId, replayId]);
    return { replayId, outcome: "superseded" as const };
  });
}

/** The search-index dead letters. */
async function letters(db: Db) {
  return (await db.pg.query<{ id: string; stage: string; work_key: string; epoch: string; state: string; last_replay_id: string | null; resolved: boolean; message: StageMessage }>(
    `SELECT id, stage, message->>'workKey' AS work_key, message->'payload'->>'epoch' AS epoch, state, last_replay_id::text, resolved_at IS NOT NULL AS resolved, message
       FROM sessions.pipeline_dead_letters ORDER BY id`)).rows;
}

/** Design §6's page query. NOTE: session_directory has no canonical_session_id column; its session_id IS the canonical id. */
const PAGE = `SELECT d.org_id, d.workspace_id, d.harness, d.session_id AS canonical_session_id, d.event_projection_epoch::text AS epoch
  FROM sessions.session_directory d
 WHERE d.event_projection_ready IS NOT TRUE
   AND EXISTS (SELECT 1 FROM sessions.session_index i WHERE i.org_id = d.org_id AND i.workspace_id = d.workspace_id
                 AND i.harness = d.harness AND coalesce(i.canonical_session_id, i.session_id) = d.session_id AND i.deleted_at IS NULL)
   AND (d.org_id, d.workspace_id, d.harness, d.session_id) > ($1, $2, $3, $4)
 ORDER BY d.org_id, d.workspace_id, d.harness, d.session_id
 LIMIT $5`;
type Page = Array<{ org_id: string; workspace_id: string; harness: string; canonical_session_id: string; epoch: string }>;

/** Design §2 pointer clock: max(session_index.indexed_at) of the canonical scope's live rows (ISO, ms). */
const POINTER_CLOCK = `SELECT to_char(max(i.indexed_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS at
  FROM sessions.session_index i
 WHERE i.org_id = $1 AND i.workspace_id = $2 AND i.harness = $3 AND coalesce(i.canonical_session_id, i.session_id) = $4 AND i.deleted_at IS NULL`;

/** The module M2 adds (design §3.3, §4, §6); absent today. Each export is imported only after its fixture is built. */
type SearchJobsModule = {
  runSearchJob: (message: StageMessage, deps: { search: ReturnType<Awaited<ReturnType<typeof hostedRig>>["search"]> }) => Promise<{
    outcome: "indexed" | "unchanged" | "not_committed" | "erased" | "held" | "busy";
    commitToSearchableMs?: number;
    pointerToSearchableMs?: number;
  }>;
  settleSearchJob: (...args: never[]) => Promise<unknown>;
  catchUpTick: (deps: { sql: unknown; pageSize: number }) => Promise<unknown>;
};
const M2_ROLE: Record<keyof SearchJobsModule, string> = {
  runSearchJob: "the search-index pass of design §4",
  settleSearchJob: "the stage-specific settlement of design §3.3",
  catchUpTick: "the catch-up tick of design §6 with the §2 clock lookup",
};
async function searchJobs<K extends keyof SearchJobsModule>(name: K): Promise<SearchJobsModule[K]> {
  let mod: Partial<SearchJobsModule>;
  try {
    mod = await import("../search-jobs.ts" as string);
  } catch {
    throw new Error(`RED: src/session-pipeline/search-jobs.ts is absent (M2 adds ${name}, ${M2_ROLE[name]})`);
  }
  if (typeof mod[name] !== "function") throw new Error(`RED: src/session-pipeline/search-jobs.ts does not export ${name} (${M2_ROLE[name]})`);
  return mod[name]!;
}

// ---------------------------------------------------------------------------------------------------------
describe("R6: closed stage surfaces (design §7)", () => {
  const SURFACE_SOURCES = {
    env: "packages/pipeline/src/env.ts",
    checks: "src/proof/aws/checks.ts",
    smoke: "src/smoke/e2e.ts",
    sqlTest: "supabase/tests/pipeline_queue_test.sql",
  };
  const bindingOf = (stage: string) =>
    (STAGE_REGISTRY as Record<string, { binding: string } | undefined>)[stage]?.binding ?? `PIPELINE_${stage.toUpperCase().replace(/[-.]/g, "_")}`;

  /** Whether each closed surface carries `stage`. */
  async function surfaces(db: Db, stage: string) {
    const scope = { orgId: "org_r6", workspaceId: "ws_r6", harness: "claude", sessionId: "s_r6" };
    const fakeQueue = { send: async () => {}, sendBatch: async () => {} };
    const binding = bindingOf(stage);
    const accepted = await new MemoryPipelineQueue().send(stageMessage(stage, scope, "w", {})).then(() => true, () => false);
    const queueName = stageQueueName(stage as never);
    const { rows } = await db.pg.query<{ conname: string; def: string }>(
      `SELECT c.conname, pg_get_constraintdef(c.oid) AS def
         FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'sessions' AND NOT t.relispartition AND c.contype = 'c'
          AND t.relname IN ('pipeline_jobs','pipeline_events','pipeline_dead_letters')
          AND c.conname IN ('pipeline_jobs_stage_check','pipeline_events_stage_check','pipeline_dead_letters_stage_check')
        ORDER BY c.conname`);
    expect(rows.map((r) => r.conname)).toEqual(["pipeline_dead_letters_stage_check", "pipeline_events_stage_check", "pipeline_jobs_stage_check"]);
    const sql = Object.fromEntries(rows.map((r) => [`sql:${r.conname}`, r.def.includes(`'${stage}'`)]));
    return {
      "trace.ts PIPELINE_STAGES": (PIPELINE_STAGES as readonly string[]).includes(stage),
      "registry.ts QUEUED_STAGES": (QUEUED_STAGES as readonly string[]).includes(stage),
      "registry.ts STAGE_REGISTRY": stage in STAGE_REGISTRY,
      "env.ts binding": source(SURFACE_SOURCES.env).includes(`${binding}?:`),
      "index.ts stageBindings": stage in stageBindings({ [binding]: fakeQueue } as never),
      "index.ts handler map": stage in (pipelineQueueDeps({} as never, { index: {} as never }).handlers ?? {}),
      "dlq.ts stageQueueName/stageForQueue": stageForQueue(queueName)?.stage === stage && stageForQueue(`${queueName}-dlq`)?.dlq === true,
      "queue.ts checked()": accepted,
      "traces.ts summarizeTrace": stage in summarizeTrace(crypto.randomUUID(), []).stages,
      "probes.ts stageTimesSql": stageTimesSql([crypto.randomUUID()]).includes(`'${stage}'`),
      "checks.ts stage times": source(SURFACE_SOURCES.checks).includes(`"${stage}"`),
      "smoke/e2e.ts ok stages": source(SURFACE_SOURCES.smoke).includes(`"${stage}"`),
      "supabase/tests/pipeline_queue_test.sql": source(SURFACE_SOURCES.sqlTest).includes(`'${stage}'`),
      ...sql,
    };
  }

  test("R6: every closed stage list carries search-index (parity with session-indexed)", async () => {
    const db = await migrated();
    // Fixture soundness: every surface reader sees today's queued stage.
    const today = await surfaces(db, "session-indexed");
    expect(Object.values(today).every(Boolean)).toBe(true);
    const all = Object.fromEntries(Object.keys(today).map((k) => [k, true]));
    expect(await surfaces(db, STAGE)).toEqual(all as never);
    // The registry entry of design §7.
    expect((STAGE_REGISTRY as Record<string, unknown>)[STAGE]).toMatchObject({ stage: STAGE, binding: "PIPELINE_SEARCH_INDEX", next: [], maxAttempts: 12, implemented: true });
  }, 120_000);

  test("R6m: the design §2 (rev 4) job message, payload.clock {source, at}, is a valid StageMessage that checked() accepts", async () => {
    const scope = { orgId: "org_r6", workspaceId: "ws_r6", harness: "claude", sessionId: "s_r6" };
    const m = wake(scope, 7, publishClock("2026-11-20T10:00:00.000Z"));
    expect(m.payload).toEqual({ epoch: 7, clock: { source: "publish", at: "2026-11-20T10:00:00.000Z" } });
    expect(m.payload).not.toHaveProperty("published_at");
    // Fixture soundness: the same message as a session-indexed message is accepted today.
    await new MemoryPipelineQueue().send({ ...m, stage: "session-indexed" });
    expect(isStageMessage(m)).toBe(true);
    await new MemoryPipelineQueue().send(m);
  });

  test("R6g: no generic runWorker registers search-index; M2 exports the stage-specific settler instead", async () => {
    // Design §3.3: only the stage-specific settler is authoritative for search-index; the generic
    // postgresStageConsumer settlement (a plain DELETE on done) must never settle it. The guard is that no
    // generic runWorker registration lists the stage (src/hoopdb/indexer.ts and every other worker main).
    //
    // Red/green reasoning: today search-index is absent from EVERY list, so the "not registered" half is GREEN
    // today and stays green in M2 (it is a regression guard, not the red signal). The red half is the positive
    // pair: M2 must export the stage-specific settler, `settleSearchJob` from src/session-pipeline/search-jobs.ts.
    // That module is absent today, so this test is red only on that absence.
    const files: string[] = [];
    for await (const f of new Bun.Glob("src/**/*.ts").scan({ cwd: root }))
      if (!f.includes("__tests__") && !f.endsWith("src/shared/worker.ts") && /\brunWorker\(\{/.test(source(f))) files.push(f);
    const registrations = Object.fromEntries(files.sort().map((f) => {
      const text = source(f);
      const stages = [...text.matchAll(/\bstages:\s*\[([^\]]*)\]/g)].flatMap((m) => [...m[1]!.matchAll(/["']([^"']+)["']/g)].map((s) => s[1]!));
      const handlerKeys = [...text.matchAll(/["']([a-z][a-z.-]*)["']\s*:\s*[a-zA-Z(]/g)].map((s) => s[1]!).filter((k) => (QUEUED_STAGES as readonly string[]).includes(k) || k === STAGE);
      return [f, { stages, handlerKeys }];
    }));
    // Fixture soundness: the scan finds the indexer worker and reads its stage list and its handler keys.
    expect(registrations["src/hoopdb/indexer.ts"]).toEqual({ stages: ["object-landed", "session-indexed", "backfill-activation"], handlerKeys: ["backfill-activation"] });
    expect(Object.keys(registrations).length).toBeGreaterThanOrEqual(2);
    for (const [f, r] of Object.entries(registrations)) {
      expect({ f, stage: r.stages.includes(STAGE) }).toEqual({ f, stage: false });
      expect({ f, handler: r.handlerKeys.includes(STAGE) }).toEqual({ f, handler: false });
    }
    // The red half: the stage-specific settler M2 adds.
    expect(typeof (await searchJobs("settleSearchJob"))).toBe("function");
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R1: one row per scope, claim and settlement (design §3)", () => {
  const scope = { orgId: "org_r1", workspaceId: "ws_r1", harness: "claude", sessionId: "s_r1" };
  const WK = searchWorkKey(scope);

  test("R1a: wakes for epochs N and N+1, then a late N-1, leave ONE visible row carrying N+1", async () => {
    const db = await migrated();
    await enqueue(db, wake(scope, 7));
    await enqueue(db, wake(scope, 8));
    await enqueue(db, wake(scope, 6));
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible }))).toEqual([{ epoch: "8", leased: false, visible: true }]);
  }, 120_000);

  test("R1b: N claimed, wake N+1 merges into the leased row; after a crash the SAME row is reclaimed with N+1", async () => {
    const db = await migrated();
    await enqueue(db, wake(scope, 7));
    const first = (await claim(db))!;
    expect(first.claimedEpoch).toBe(7);
    await enqueue(db, wake(scope, 8));
    // Atomic max-epoch merge into the single row; the lease is kept.
    expect((await jobs(db, WK)).map((j) => ({ id: j.id, epoch: j.epoch, leased: j.leased, visible: j.visible })))
      .toEqual([{ id: first.started.id, epoch: "8", leased: true, visible: false }]);
    // The worker crashes: its lease expires.
    await db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE id = $1::bigint", [first.started.id]);
    const again = (await claim(db))!;
    expect({ id: again.started.id, epoch: again.claimedEpoch }).toEqual({ id: first.started.id, epoch: 8 });
    expect(await jobs(db, WK)).toHaveLength(1);
  }, 120_000);

  test("R1d: done with claimed N after a wake N+1 during the pass reopens the row (not deleted), carrying N+1", async () => {
    const db = await migrated();
    await enqueue(db, wake(scope, 7));
    const c = (await claim(db))!;
    await enqueue(db, wake(scope, 8));
    expect(await settleDone(db, c)).toBe("reopened");
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ epoch: "8", leased: false, visible: true, attempts: 0 }]);
    // N+1 runs next and its done deletes the row.
    const next = (await claim(db))!;
    expect(next.claimedEpoch).toBe(8);
    expect(await settleDone(db, next)).toBe("deleted");
    expect(await jobs(db, WK)).toEqual([]);
  }, 120_000);

  test("R1e: N leased at attempt 11, wake N+1, the attempt fails: the §3.3 retry reopens ONE row now, epoch N+1, attempts 0", async () => {
    const db = await migrated();
    await enqueue(db, wake(scope, 7));
    const c = (await claim(db))!;
    // The leased row is on its 11th attempt (the claim's start counted 1; earlier retries counted the rest).
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 11 WHERE id = $1::bigint AND lease_id = $2::uuid", [c.started.id, c.lease]);
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, attempts: j.attempts }))).toEqual([{ epoch: "7", leased: true, attempts: 11 }]);
    await enqueue(db, wake(scope, 8));
    // The attempt fails: the stage-specific retry of §3.3 (not the generic postgresStageConsumer settle).
    expect((await retry(db, c, 900)).rows).toHaveLength(1);
    expect((await jobs(db, WK)).map((j) => ({ id: j.id, epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: c.started.id, epoch: "8", leased: false, visible: true, attempts: 0 }]);
    // Control: a retry with NO newer wake keeps the budget and backs off.
    const next = (await claim(db))!;
    expect({ epoch: next.claimedEpoch, attempt: next.started.attempt }).toEqual({ epoch: 8, attempt: 1 });
    expect((await retry(db, next, 30)).rows).toHaveLength(1);
    expect((await jobs(db, WK)).map((j) => ({ id: j.id, epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: c.started.id, epoch: "8", leased: false, visible: false, attempts: 1 }]);
  }, 120_000);

  test("R1e': unleased N at attempt 11 (and dlq), wake N+1: the §3.1 upsert gives the row attempts 0, dlq false, visible now", async () => {
    const db = await migrated();
    await enqueue(db, wake(scope, 7));
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 11, dlq = true, visible_at = now() + interval '15 minutes' WHERE stage = 'search-index' AND work_key = $1", [WK]);
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ epoch: "7", leased: false, visible: false, attempts: 11, dlq: true }]);
    await enqueue(db, wake(scope, 8));
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ epoch: "8", leased: false, visible: true, attempts: 0, dlq: false }]);
    // Control: a same-epoch re-wake is not new work and keeps the budget.
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 3 WHERE stage = 'search-index' AND work_key = $1", [WK]);
    await enqueue(db, wake(scope, 8));
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, attempts: j.attempts }))).toEqual([{ epoch: "8", attempts: 3 }]);
  }, 120_000);

  test("R1f: a wake N+1 during N's exhausting (12th) attempt: no dead letter carries N+1, and N+1 is not lost", async () => {
    const db = await migrated();
    await deadLettersAreSound(db);
    await enqueue(db, wake(scope, 7));
    const seen: number[] = [];
    for (let n = 1; n <= 12; n++) {
      await db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
      const c = (await claim(db))!;
      seen.push(c.started.attempt);
      expect(c.claimedEpoch).toBe(7);
      if (c.started.attempt < 12) { expect((await retry(db, c)).rows).toHaveLength(1); continue; }
      // The final attempt is still running when wake N+1 commits; then the attempt is held and exhausts.
      await enqueue(db, wake(scope, 8));
      expect(await exhaust(db, c)).toEqual({ parked: true, deleted: false });
    }
    expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const parked = await letters(db);
    expect(parked.map((l) => ({ stage: l.stage, work_key: l.work_key, epoch: l.epoch, state: l.state }))).toEqual([{ stage: STAGE, work_key: WK, epoch: "7", state: "dead" }]);
    expect(parked.filter((l) => l.epoch === "8")).toEqual([]);
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ epoch: "8", leased: false, visible: true, attempts: 0 }]);
    // N+1 is not lost: it is claimed next, on a fresh budget.
    const next = (await claim(db))!;
    expect({ epoch: next.claimedEpoch, attempt: next.started.attempt }).toEqual({ epoch: 8, attempt: 1 });
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R1c/R5a/R5c: the wake is written inside the publication-authority transaction (design §2, §0)", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead(); }, 180_000);
  afterAll(() => head?.close());
  const searchJobsOf = () => head.jobsAfter.filter((j) => j.stage === STAGE);

  test("R1c-: a retired publication rolls the authority transaction back: zero jobs of any stage", () => {
    expect(head.retired.outcome).toEqual({ status: "skipped", reason: "publication_retired" });
    expect(head.retired.jobsWritten).toBe(0);
  });

  test("R1c+: a current publication leaves exactly one search-index job, a valid StageMessage at the directory epoch", async () => {
    // Fixture soundness (asserted in publishedHead too): the authority transaction committed its fan-out.
    expect(head.jobsAfter.map((j) => j.stage)).toEqual(expect.arrayContaining(["summary-short", "summary-long", "status-classify"]));
    const found = searchJobsOf();
    expect(found).toHaveLength(1);
    const m = found[0]!.message as StageMessage<WakePayload>;
    expect(isStageMessage(m)).toBe(true);
    await new MemoryPipelineQueue().send(m);
    expect(m).toMatchObject({ v: 1, stage: STAGE, ctx: { traceId: head.indexed.ctx.traceId, orgId: head.scope.orgId, workspaceId: head.scope.workspaceId, harness: head.scope.harness, sessionId: head.scope.canonicalSessionId } });
    expect(m.workKey.split(/[\u001f␟]/)).toEqual(["search", head.scope.orgId, head.scope.workspaceId, head.scope.harness, head.scope.canonicalSessionId]);
    expect(String(m.payload.epoch)).toBe(head.epoch);
    expect(m.payload).not.toHaveProperty("published_at");
  });

  test("R5a: the job carries the publish clock: payload.clock = {source: 'publish', at: the attempt's published_at}", async () => {
    // Fixture soundness: the committed attempt carries its clock.
    expect(Number.isFinite(Date.parse(head.publishedAt))).toBe(true);
    const found = searchJobsOf();
    expect(found).toHaveLength(1);
    const clock = (found[0]!.message.payload as WakePayload).clock;
    expect(clock.source).toBe("publish");
    const { rows } = await head.db.query<{ same: boolean }>(
      "SELECT abs(extract(epoch FROM ($1::timestamptz - published_at))) < 0.001 AS same FROM sessions.ingest_publication_attempts WHERE state = 'published'", [clock.at]);
    expect(rows).toEqual([{ same: true }]);
  });

  // Mutates head.db (drops the wake): keep it last in this block.
  test("R5c: a managed scope's catch-up job carries clock.source='publish', at = the head attempt's published_at", async () => {
    // Fixture soundness: the managed head is stale (committed, not search-ready) and the §6 page sees it; its
    // head has exactly one published attempt carrying the clock.
    const page = (await head.db.query(PAGE, ["", "", "", "", 500])).rows as Page;
    expect(page.map((r) => [r.org_id, r.workspace_id, r.harness, r.canonical_session_id, r.epoch]))
      .toEqual([[head.scope.orgId, head.scope.workspaceId, head.scope.harness, head.scope.canonicalSessionId, head.epoch]]);
    const attempts = (await head.db.query<{ published_at: string }>(
      `SELECT to_char(published_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS published_at FROM sessions.ingest_publication_attempts WHERE state = 'published'`)).rows;
    expect(attempts).toEqual([{ published_at: head.publishedAt }]);
    // The wake was lost.
    await head.db.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'");
    const catchUpTick = await searchJobs("catchUpTick");
    await catchUpTick({ sql: head.db, pageSize: 500 });
    const found = (await head.db.query<{ message: StageMessage<WakePayload> }>("SELECT message FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows;
    expect(found).toHaveLength(1);
    expect(String(found[0]!.message.payload.epoch)).toBe(head.epoch);
    expect(found[0]!.message.payload.clock.source).toBe("publish");
    expect(Date.parse(found[0]!.message.payload.clock.at)).toBe(Date.parse(head.publishedAt));
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("R2: the global keyset catch-up cursor (design §6)", () => {
  /**
   * One catch-up tick, in one transaction: read the cursor, page, enqueue (§3.1), move the cursor. The cursor's
   * key column is `session_id` (§6: the directory's own key, the canonical id). The seeded scopes are pointer-only
   * (non-managed), so each job carries the §2 pointer clock: never a null clock.
   */
  async function tick(db: Db, S: number): Promise<Page> {
    return db.pg.transaction(async (tx) => {
      const cur = (await tx.query<{ org_id: string; workspace_id: string; harness: string; session_id: string }>(
        "SELECT org_id, workspace_id, harness, session_id FROM sessions.search_catchup_cursor FOR UPDATE")).rows;
      if (!cur.length) await tx.query("INSERT INTO sessions.search_catchup_cursor (org_id, workspace_id, harness, session_id, lap, updated_at) VALUES ('', '', '', '', 0, now())");
      const c = cur[0] ?? { org_id: "", workspace_id: "", harness: "", session_id: "" };
      const page = (await tx.query(PAGE, [c.org_id, c.workspace_id, c.harness, c.session_id, S])).rows as Page;
      for (const r of page) {
        const s = { orgId: r.org_id, workspaceId: r.workspace_id, harness: r.harness, sessionId: r.canonical_session_id };
        const at = (await tx.query<{ at: string | null }>(POINTER_CLOCK, [s.orgId, s.workspaceId, s.harness, s.sessionId])).rows[0]!.at;
        expect(at).not.toBeNull();
        await tx.query(ENQUEUE, [JSON.stringify(wake(s, r.epoch, { source: "pointer", at: at! }))]);
      }
      const last = page.at(-1);
      if (page.length < S)
        await tx.query("UPDATE sessions.search_catchup_cursor SET org_id = '', workspace_id = '', harness = '', session_id = '', lap = lap + 1, updated_at = now()");
      else
        await tx.query("UPDATE sessions.search_catchup_cursor SET org_id = $1, workspace_id = $2, harness = $3, session_id = $4, updated_at = now()",
          [last!.org_id, last!.workspace_id, last!.harness, last!.canonical_session_id]);
      return page;
    });
  }
  const lap = async (db: Db) => (await db.pg.query<{ lap: number }>("SELECT lap::int FROM sessions.search_catchup_cursor")).rows[0]?.lap;

  /** Committed, never-indexed scopes: a live session_index row and a projected, not-ready directory row each. */
  async function seed(db: Db, scopes: Scope[]) {
    for (const s of scopes) {
      await addReplayPointer(db.db, { identity: { orgId: s.orgId, workspaceId: s.workspaceId, source: s.harness, sessionId: s.sessionId },
        key: `96bulls/${s.orgId}/catchup/${s.sessionId}/manifest.json`, digest: "ab".repeat(32), physicalSession: s.sessionId, physicalMachine: "m1", canonicalSession: s.sessionId });
      await projectSessionDirectory(db.db, { orgId: s.orgId, workspaceId: s.workspaceId, harness: s.harness, sessionId: s.sessionId });
    }
    // Fixture soundness: the page query sees exactly these stale scopes, in keyset order.
    const all = (await db.pg.query(PAGE, ["", "", "", "", 100])).rows as Page;
    expect(all.map((r) => [r.org_id, r.workspace_id, r.harness, r.canonical_session_id])).toEqual(
      scopes.map((s) => [s.orgId, s.workspaceId, s.harness, s.sessionId]).sort((a, b) => (a.join("\u0000") < b.join("\u0000") ? -1 : 1)));
    return all;
  }
  const one = (org: string, n: number): Scope => ({ orgId: org, workspaceId: "ws_cu", harness: "claude", sessionId: `s_${String(n).padStart(2, "0")}` });

  test("R2a: a lost wake (the job deleted) converges through the catch-up within one lap, with a pointer clock", async () => {
    const db = await migrated();
    const s = one("org_cu_a", 1);
    const [row] = await seed(db, [s]);
    // The wake was lost: there is no search-index job for the scope.
    expect(await jobs(db, searchWorkKey(s))).toEqual([]);
    for (let t = 0; t < 2 && (await lap(db) ?? 0) < 1; t++) await tick(db, 2);
    expect(await lap(db)).toBe(1);
    const found = await jobs(db, searchWorkKey(s));
    expect(found.map((j) => j.epoch)).toEqual([row!.epoch]);
    expect(found[0]!.clock?.source).toBe("pointer");
    expect(Number.isFinite(Date.parse(found[0]!.clock?.at ?? ""))).toBe(true);
  }, 120_000);

  test("R2b: S = 2, five stale scopes in ONE tenant, the first two permanently held: all five enqueued within 3 ticks", async () => {
    const db = await migrated();
    const scopes = [1, 2, 3, 4, 5].map((n) => one("org_cu_b", n));
    await seed(db, scopes);
    const pages: string[][] = [];
    pages.push((await tick(db, 2)).map((r) => r.canonical_session_id));
    // The first page is held: its passes never finish (leased, attempts climbing), so they stay stale.
    for (const s of scopes.slice(0, 2))
      await db.pg.query("UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid(), attempts = 5, visible_at = now() + interval '1 hour' WHERE work_key = $1", [searchWorkKey(s)]);
    pages.push((await tick(db, 2)).map((r) => r.canonical_session_id));
    pages.push((await tick(db, 2)).map((r) => r.canonical_session_id));
    expect(pages).toEqual([["s_01", "s_02"], ["s_03", "s_04"], ["s_05"]]);
    for (const s of scopes) expect(await jobs(db, searchWorkKey(s))).toHaveLength(1);
    // A held scope's re-enqueue (same epoch) is a no-op upsert: still leased, attempts kept.
    await tick(db, 2);
    for (const s of scopes.slice(0, 2)) expect((await jobs(db, searchWorkKey(s))).map((j) => ({ leased: j.leased, attempts: j.attempts }))).toEqual([{ leased: true, attempts: 5 }]);
  }, 120_000);

  test("R2c: five tenants, S = 2: the cursor visits every tenant, wraps after a short page and lap increments", async () => {
    const db = await migrated();
    const scopes = ["org_cu_c1", "org_cu_c2", "org_cu_c3", "org_cu_c4", "org_cu_c5"].map((o) => one(o, 1));
    await seed(db, scopes);
    const pages: string[][] = [];
    for (let t = 0; t < 3; t++) pages.push((await tick(db, 2)).map((r) => r.org_id));
    expect(pages).toEqual([["org_cu_c1", "org_cu_c2"], ["org_cu_c3", "org_cu_c4"], ["org_cu_c5"]]);
    expect(await lap(db)).toBe(1);
    for (const s of scopes) expect(await jobs(db, searchWorkKey(s))).toHaveLength(1);
    // The wrap starts the next lap from the minimum.
    expect((await tick(db, 2)).map((r) => r.org_id)).toEqual(["org_cu_c1", "org_cu_c2"]);
    expect(await lap(db)).toBe(1);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R3: committed-state guard and erase only on authority (design §4.2, §4.4)", () => {
  let x: ExportedScope;
  beforeAll(async () => {
    // A session over at least two machines (manifests), so one can be tombstoned while another stays live.
    let seed = 1;
    while (new Set(generate(seed).map((l) => l.machine)).size < 2) seed++;
    x = await exportStreamScope(generate(seed), 400);
  }, 120_000);
  afterAll(() => x?.dispose());

  const canonicalScope = () => ({ ...SCOPE, sessionId: x.canonicalId });
  async function directory(r: Awaited<ReturnType<typeof hostedRig>>) {
    const { rows } = await r.db.pg.query<{ epoch: string; ready: boolean }>(
      "SELECT event_projection_epoch::text AS epoch, event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
      [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, x.canonicalId]);
    expect(rows).toHaveLength(1);
    return rows[0]!;
  }
  const machinesOf = async (r: Awaited<ReturnType<typeof hostedRig>>) => (await r.db.pg.query<{ machine_id: string }>(
    "SELECT machine_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 ORDER BY machine_id",
    [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId])).rows.map((m) => m.machine_id);
  const scopeRows = (r: Awaited<ReturnType<typeof hostedRig>>, sessions: string[]) => r.fake.rows.filter((row) => sessions.includes(row.session_id));

  /** A rig whose committed scope a complete pass indexed (fixture soundness). */
  async function indexed() {
    const r = await hostedRig(x, cleanup);
    expect((await r.tick({})).batchItemFailures).toEqual([]);
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    expect(scopeRows(r, [SCOPE.sessionId, x.canonicalId]).length).toBeGreaterThan(0);
    return r;
  }

  test("R3a: a pass on a scope that was NEVER committed erases nothing (its search rows stay byte-identical)", async () => {
    const r = await indexed();
    const never = { ...SCOPE, sessionId: "never_committed" };
    // Real search rows for a session id that has no session_index row at all (live or tombstoned).
    const seeded = scopeRows(r, [SCOPE.sessionId, x.canonicalId]).slice(0, 3).map((row) => ({ ...row, session_id: never.sessionId }));
    r.fake.rows.push(...seeded);
    expect((await r.db.pg.query("SELECT 1 FROM sessions.session_index WHERE session_id = $1 OR canonical_session_id = $1", [never.sessionId])).rows).toHaveLength(0);
    const before = JSON.stringify(scopeRows(r, [never.sessionId]));
    expect(seeded.length).toBe(3);
    const result = await indexSearchScope(never, r.search());
    expect(JSON.stringify(scopeRows(r, [never.sessionId]))).toBe(before);
    expect(result.reasons).not.toContain("erased");
  }, 120_000);

  test("R3b: an old-version delete beside a newer live row: the pass indexes, never erases", async () => {
    const r = await indexed();
    // The old version's delete, as the pointer path issues it (fenced on the deleted key): no row names it.
    const old = await r.db.pg.query(`UPDATE sessions.session_index SET deleted_at = now(), indexed_at = now()
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND manifest_key=$5 RETURNING 1`,
      [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId, `${x.keys[0]}.previous-version`]);
    expect(old.rows).toHaveLength(0);
    // One machine's older generation is deleted; the other machine stays live.
    const machines = await machinesOf(r);
    expect(machines.length).toBeGreaterThanOrEqual(2);
    const tombstoned = await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 RETURNING 1",
      [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, SCOPE.sessionId, machines[0]]);
    expect(tombstoned.rows).toHaveLength(1);
    const live = (await r.db.pg.query("SELECT 1 FROM sessions.session_index WHERE org_id=$1 AND session_id=$2 AND deleted_at IS NULL", [SCOPE.orgId, SCOPE.sessionId])).rows;
    expect(live.length).toBeGreaterThan(0);
    const { epoch } = await directory(r);
    const runSearchJob = await searchJobs("runSearchJob");
    const out = await runSearchJob(wake(canonicalScope(), epoch), { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(scopeRows(r, [SCOPE.sessionId, x.canonicalId]).length).toBeGreaterThan(0);
  }, 120_000);

  test("R3c: recreate race (every row tombstoned, then a newer live generation before the pass): live wins", async () => {
    const r = await indexed();
    const machines = await machinesOf(r);
    // An erasure fence cannot model this race: session_erasure_fences is append-only (DELETE is refused) and the
    // fence guard refuses any live session_index row for a fenced id, so a fenced scope is never recreated. The
    // reachable race is condition (b)'s other branch: every row tombstoned, the erase wake carries that epoch.
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, SCOPE.sessionId]);
    const erased = await directory(r);
    expect((await r.db.pg.query("SELECT 1 FROM sessions.session_index WHERE org_id=$1 AND session_id=$2 AND deleted_at IS NULL", [SCOPE.orgId, SCOPE.sessionId])).rows).toHaveLength(0);
    // The recreate commits before the pass: a live generation is back and the epoch bumps.
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = NULL WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, SCOPE.sessionId]);
    const recreated = await directory(r);
    expect(BigInt(recreated.epoch)).toBeGreaterThan(BigInt(erased.epoch));
    expect((await r.db.pg.query("SELECT 1 FROM sessions.session_index WHERE org_id=$1 AND session_id=$2 AND deleted_at IS NULL", [SCOPE.orgId, SCOPE.sessionId])).rows.length).toBe(machines.length);
    const runSearchJob = await searchJobs("runSearchJob");
    const out = await runSearchJob(wake(canonicalScope(), erased.epoch), { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(scopeRows(r, [SCOPE.sessionId, x.canonicalId]).length).toBeGreaterThan(0);
  }, 120_000);

  test("R3d: a stale queued epoch after a newer one is indexed answers unchanged, with no ClickHouse write", async () => {
    const r = await indexed();
    const now = await directory(r);
    expect(now.ready).toBe(true);
    expect(BigInt(now.epoch)).toBeGreaterThan(0n);
    r.fake.calls.length = 0;
    const runSearchJob = await searchJobs("runSearchJob");
    const out = await runSearchJob(wake(canonicalScope(), String(BigInt(now.epoch) - 1n)), { search: r.search() });
    expect(out.outcome).toBe("unchanged");
    expect(r.fake.calls.filter((c) => c.ch !== "select")).toEqual([]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R4: acceptance held past max attempts (design §4.5, §3.3 exhausted)", () => {
  let held: ExportedScope;
  beforeAll(async () => {
    const lines: Line[] = generate(5);
    const machine = lines[0]!.machine;
    // Two origins contend for one record id without acceptance seats: relayhistory_acceptance_missing.
    lines.push(
      activity(machine, `capacity:${machine}`, "session_event", "unseated", Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }),
      activity(machine, "capacity:other", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }),
    );
    held = await exportStreamScope(lines, 400);
  }, 120_000);
  afterAll(() => held?.dispose());

  test("R4: twelve held attempts (claimed epoch == current) exhaust through §3.3: quarantined; the scope stays not ready and out of search", async () => {
    const r = await hostedRig(held, cleanup);
    await queueIsSound(r.db);
    await deadLettersAreSound(r.db);
    await r.tick({});
    const scope = { ...SCOPE, sessionId: held.canonicalId };
    const dir = async () => (await r.db.pg.query<{ epoch: string; ready: boolean }>(
      "SELECT event_projection_epoch::text AS epoch, event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId])).rows[0]!;
    const inSearch = () => r.fake.rows.filter((row) => row.session_id === SCOPE.sessionId || row.session_id === held.canonicalId).length;
    // Fixture soundness: the committed scope is held on acceptance, not ready, and has no search rows.
    const first = await indexSearchScope(SCOPE, r.search());
    expect(first.complete).toBe(false);
    expect(first.reasons).toContain("relayhistory_acceptance_missing");
    expect((await dir()).ready).toBe(false);
    expect(inSearch()).toBe(0);

    await enqueue(r.db, wake(scope, (await dir()).epoch));
    const attempts: number[] = [];
    let last: Claimed | undefined;
    for (let n = 1; n <= 12; n++) {
      await r.db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
      const c = (await claim(r.db))!;
      attempts.push(c.started.attempt);
      // No newer wake: the claimed epoch is the current one on every attempt.
      expect(c.claimedEpoch).toBe(Number((await dir()).epoch));
      // The held decision of §4.5: an incomplete pass on missing acceptance is retried, never acked.
      const res = await indexSearchScope(SCOPE, r.search());
      expect(res.reasons).toContain("relayhistory_acceptance_missing");
      if (c.started.attempt < 12) expect((await retry(r.db, c)).rows).toHaveLength(1);
      else { expect(await exhaust(r.db, c)).toEqual({ parked: true, deleted: true }); last = c; }
    }
    expect(attempts).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    expect((await r.db.pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows).toHaveLength(0);
    const parked = await letters(r.db);
    expect(parked.map((l) => ({ stage: l.stage, work_key: l.work_key, epoch: l.epoch, state: l.state })))
      .toEqual([{ stage: STAGE, work_key: searchWorkKey(scope), epoch: (await dir()).epoch, state: "dead" }]);
    expect(parked[0]!.message).toEqual(last!.claimedMessage as StageMessage);
    expect((await dir()).ready).toBe(false);
    expect(inSearch()).toBe(0);
    // A later wake inserts a fresh row: an old quarantine never blocks a newly accepted revision.
    await enqueue(r.db, wake(scope, Number((await dir()).epoch) + 1));
    expect((await r.db.pg.query<{ attempts: number }>("SELECT attempts FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows).toEqual([{ attempts: 0 }]);
  }, 180_000);

  test("R4b: N exhausts after N+1 merged into the leased row: only N's claim-time snapshot is parked; the row survives with N+1, attempts 0", async () => {
    const db = await migrated();
    await deadLettersAreSound(db);
    const scope = { orgId: "org_r4b", workspaceId: "ws_r4b", harness: "claude", sessionId: "s_r4b" };
    const WK = searchWorkKey(scope);
    await enqueue(db, wake(scope, 7));
    const c = (await claim(db))!;
    // The leased row is on its 12th and final attempt.
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [c.started.id, c.lease]);
    await enqueue(db, wake(scope, 8));
    expect((await jobs(db, WK)).map((j) => ({ id: j.id, epoch: j.epoch, leased: j.leased, attempts: j.attempts })))
      .toEqual([{ id: c.started.id, epoch: "8", leased: true, attempts: 0 }]);
    // A lost lease aborts the whole transaction: nothing parked, nothing deleted.
    await expect(exhaust(db, { ...c, lease: crypto.randomUUID() })).rejects.toThrow("lease lost");
    expect(await letters(db)).toEqual([]);
    expect(await jobs(db, WK)).toHaveLength(1);
    // N exhausts under its lease.
    expect(await exhaust(db, c)).toEqual({ parked: true, deleted: false });
    const parked = await letters(db);
    expect(parked.map((l) => ({ id: l.id, epoch: l.epoch, state: l.state }))).toEqual([{ id: deadLetterId(STAGE as never, scope, WK), epoch: "7", state: "dead" }]);
    expect(parked[0]!.message).toEqual(c.claimedMessage as StageMessage);
    expect((await jobs(db, WK)).map((j) => ({ id: j.id, epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ id: c.started.id, epoch: "8", leased: false, visible: true, attempts: 0, dlq: false }]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R8: stage-aware dead-letter replay (design §3.3b)", () => {
  const scope = { orgId: "org_r8", workspaceId: "ws_r8", harness: "claude", sessionId: "s_r8" };
  const WK = searchWorkKey(scope);

  /** A search-index letter at epoch N, through the real claim and the §3.3 exhausted settlement (no newer wake). */
  async function letterAt(db: Db, epoch: number) {
    await deadLettersAreSound(db);
    await enqueue(db, wake(scope, epoch));
    const c = (await claim(db))!;
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [c.started.id, c.lease]);
    expect(await exhaust(db, c)).toEqual({ parked: true, deleted: true });
    expect(await jobs(db, WK)).toEqual([]);
    const [letter] = await letters(db);
    expect({ epoch: letter!.epoch, state: letter!.state }).toEqual({ epoch: String(epoch), state: "dead" });
    return letter!;
  }

  test("R8a: superseded — a letter at N while the row carries N+1 is resolved in the replay transaction, never left replaying", async () => {
    const db = await migrated();
    const letter = await letterAt(db, 7);
    await enqueue(db, wake(scope, 8));
    const out = await replay(db, letter.id);
    expect(out.outcome).toBe("superseded");
    expect((await letters(db)).map((l) => ({ id: l.id, state: l.state, resolved: l.resolved, last_replay_id: l.last_replay_id })))
      .toEqual([{ id: letter.id, state: "resolved", resolved: true, last_replay_id: out.replayId }]);
    expect(await postgresDeadLetterStore(db.pg).list({ stage: STAGE as never, states: ["replaying"] })).toEqual([]);
    // Newer work covers it: the row is untouched, N+1 without the replay id.
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, replay_id: j.replay_id }))).toEqual([{ epoch: "8", replay_id: null }]);
  }, 120_000);

  test("R8b: merged — no row exists, the replay carries N with payload.replay_id; the §3.3 done settlement resolves the letter", async () => {
    const db = await migrated();
    const letter = await letterAt(db, 7);
    const out = await replay(db, letter.id);
    expect(out.outcome).toBe("merged");
    expect((await jobs(db, WK)).map((j) => ({ epoch: j.epoch, replay_id: j.replay_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ epoch: "7", replay_id: out.replayId, visible: true, attempts: 0 }]);
    expect((await letters(db)).map((l) => ({ state: l.state, last_replay_id: l.last_replay_id }))).toEqual([{ state: "replaying", last_replay_id: out.replayId }]);
    const c = (await claim(db))!;
    expect(c.claimedMessage.payload.replay_id).toBe(out.replayId);
    expect(await settleDone(db, c)).toBe("deleted");
    expect(await jobs(db, WK)).toEqual([]);
    expect((await letters(db)).map((l) => ({ state: l.state, resolved: l.resolved }))).toEqual([{ state: "resolved", resolved: true }]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R5b/R5d: the clock and its metrics (design §0, §2)", () => {
  let x: ExportedScope;
  beforeAll(async () => { x = await exportStreamScope(generate(5), 400); }, 120_000);
  afterAll(() => x?.dispose());

  test("R5b: a job that waited 120 s in the queue reports at least 120 s, measured from payload.clock.at (source publish)", async () => {
    const r = await hostedRig(x, cleanup);
    expect((await r.tick({})).batchItemFailures).toEqual([]);
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    // A newer committed change the pass must write for (the trigger's own invalidation).
    await r.db.pg.query("UPDATE sessions.session_directory SET event_projection_ready = false, event_projection_epoch = event_projection_epoch + 1 WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, x.canonicalId]);
    const { rows } = await r.db.pg.query<{ epoch: string; published_at: string }>(
      `SELECT event_projection_epoch::text AS epoch, to_char((clock_timestamp() - interval '120 seconds') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS published_at
         FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2`, [SCOPE.orgId, x.canonicalId]);
    expect(rows).toHaveLength(1);
    const runSearchJob = await searchJobs("runSearchJob");
    const out = await runSearchJob(wake({ ...SCOPE, sessionId: x.canonicalId }, rows[0]!.epoch, publishClock(rows[0]!.published_at)), { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(out.commitToSearchableMs).toBeGreaterThanOrEqual(120_000);
    expect(out.commitToSearchableMs).toBeLessThan(180_000);
  }, 120_000);

  test("R5d: a non-managed (pointer-only) scope's catch-up job carries clock.source='pointer', at = max(indexed_at); its pass emits no commit_to_searchable_ms", async () => {
    const r = await hostedRig(x, cleanup);
    expect((await r.tick({})).batchItemFailures).toEqual([]);
    const canonical = { ...SCOPE, sessionId: x.canonicalId };
    // The rig's own consumer pass indexed the scope; a newer committed change makes it stale again (as in R5b),
    // and its wake was lost.
    await r.db.pg.query("UPDATE sessions.session_directory SET event_projection_ready = false, event_projection_epoch = event_projection_epoch + 1 WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, x.canonicalId]);
    // Fixture soundness: no publication attempt exists (non-managed); the scope is committed, stale and paged;
    // its live rows carry the pointer clock.
    expect((await r.db.pg.query("SELECT 1 FROM sessions.ingest_publication_attempts")).rows).toHaveLength(0);
    const page = (await r.db.pg.query(PAGE, ["", "", "", "", 500])).rows as Page;
    expect(page.map((p) => [p.org_id, p.workspace_id, p.harness, p.canonical_session_id])).toEqual([[canonical.orgId, canonical.workspaceId, canonical.harness, canonical.sessionId]]);
    const at = (await r.db.pg.query<{ at: string | null }>(POINTER_CLOCK, [canonical.orgId, canonical.workspaceId, canonical.harness, canonical.sessionId])).rows[0]!.at;
    expect(Number.isFinite(Date.parse(at ?? ""))).toBe(true);
    expect((await r.db.pg.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows).toHaveLength(0);
    const catchUpTick = await searchJobs("catchUpTick");
    await catchUpTick({ sql: r.db.pg, pageSize: 500 });
    const found = (await r.db.pg.query<{ message: StageMessage<WakePayload> }>("SELECT message FROM sessions.pipeline_jobs WHERE stage = 'search-index'")).rows;
    expect(found).toHaveLength(1);
    expect(String(found[0]!.message.payload.epoch)).toBe(page[0]!.epoch);
    expect(found[0]!.message.payload.clock.source).toBe("pointer");
    expect(Date.parse(found[0]!.message.payload.clock.at)).toBe(Date.parse(at!));
    const runSearchJob = await searchJobs("runSearchJob");
    const out = await runSearchJob(found[0]!.message, { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(out.commitToSearchableMs).toBeUndefined();
    expect(out.pointerToSearchableMs).toBeGreaterThanOrEqual(0);
  }, 120_000);
});
