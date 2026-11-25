/**
 * RED until M2 (docs/handoffs/incremental-search-indexing.md rev 8, §10): the committed `search-index` wake.
 *
 * Production-path coupling (rev 5 §10). Every race, settlement, replay, park and catch-up test drives the REAL
 * M2 entry points of `src/session-pipeline/search-jobs.ts`:
 *   enqueueSearchWake, claimSearchJobs, settleSearchJob (done | {retry} | exhausted), replaySearchDeadLetter,
 *   catchUpTick, runSearchJob, parkSearchJobs, and (§8) searchStageDownPrecondition, archiveSearchStage [rev 7: failure-safe,
 *   into sessions.search_stage_archive with a sha256 manifest; with an unresolved letter it THROWS and changes nothing (R9c)];
 *   rev 8: the manifest digest is recomputed in-test from the pre-archive rows with §8's exact rule (R9b); each down
 *   guard on its own (R9g); a search-index job refuses the archive (R9d); the archive is append-only (R9e).
 * No test runs the design's enqueue / settlement / replay / tick / park SQL itself, so a migration (or a stub export
 * plus a migration) can never turn them green: each asserts concrete table state after the call that a no-op
 * export cannot produce. Fixtures (seeding rows, simulating a crash by expiring a lease, raising `attempts`) use
 * plain SQL; assertions observe the tables with plain SELECTs.
 *
 * What stays red for another reason, each the intended absence:
 *   - R6 / R6m: the closed stage lists (§7) and therefore `checked()`;
 *   - R1c+ / R5a / R5f / R5g: the authority-path writer (§2); R5g observes its refusal row in sessions.pipeline_events
 *     [rev 7] with a plain SELECT (no M2 call), red because no writer exists and the stage CHECK forbids the row;
 *   - R3a: today's erase-on-absence behaviour of `indexSearchScope` (§4.2);
 *   - R6w: the Lambda's search-job loop (lambda.ts imports and calls claimSearchJobs / settleSearchJob).
 * Green guards (green today, must stay green): R1c- (a retired publication writes no job), R5f- (a retired trigger
 * writes no job even beside a valid newer sibling) and R6g (no generic runWorker registers the stage).
 *
 * Message shape (§2): `payload = { epoch, clock: { source: 'publish' | 'pointer', at } }`. One dead letter per
 * scope (§3.3, §3.3b) [rev 6]: id = the repository's canonical `deadLetterId('search-index', ctx, workKey)`
 * (dlq.ts), imported here, never re-derived; the R8 and R9 scopes carry ':' and ' ' so the URI encoding is proven.
 * The `resolution` column (rev 6 §3.3b) is read only after an M2 call, so its absence is never the red reason.
 *
 * Contracts this file fixes where the design leaves a choice:
 *   - settleSearchJob under a lost lease returns `{ applied: false }` (never throws) and touches nothing (R1d, R4b).
 *   - R5g, a managed scope with no provable publish clock: the session stays readable (authority commits), no job, and one
 *     pipeline_events refusal row (search-index / skipped / clock_unprovable) committed with the pointer [rev 7];
 *     (superseded rev-6 helper note follows) the authority-path writer previously THREW, so the session-indexed
 *     delivery does not ack (it retries) and the authority transaction rolls back: no job of any stage, and never a
 *     `pointer` clock. That is the design's "fail closed" with the delivery's error as the surfaced error.
 * R7 is deferred to M3 and is not in this file.
 *
 * Synthetic, secret-free data only.
 * Run: ~/.local/bin/aw-heavy -- bun test src/session-pipeline/__tests__/search-jobs.red.pglite.test.ts
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PIPELINE_STAGES } from "@rhc/lib/pipeline/trace.ts";
import { QUEUED_STAGES, STAGE_REGISTRY } from "@rhc/lib/pipeline/registry.ts";
import { InMemoryDeadLetterStore, deadLetterId, isStageMessage, stageForQueue, stageQueueName, type StageMessage, type StageQueueDeps } from "@rhc/lib/pipeline/dlq.ts";
import { MemoryPipelineQueue, postgresPipelineQueue, postgresStageConsumer } from "@rhc/lib/pipeline/queue.ts";
import { InMemoryPipelineEventSink } from "@rhc/lib/pipeline/events.ts";
import { postgresDeadLetterStore } from "@rhc/lib/pipeline/postgres-dead-letters.ts";
import { summarizeTrace } from "@rhc/lib/pipeline/traces.ts";
import { pipelineQueueDeps, stageBindings } from "@rhc/lib/pipeline/index.ts";
import { stageTimesSql } from "../../proof/aws/probes.ts";
import { projectSessionDirectory } from "../../hoopdb/session-directory-projector.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { SCOPE, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { addReplayPointer, clonedReplayDatabase, closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";
import { publishedHead, siblingHead, type PublishedHead } from "./search-jobs-publication.ts";

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
type WakePayload = { epoch: number; clock: Clock };

/** A migrated PGlite clone whose queue accepts and claims today's stages (fixture soundness). */
async function migrated(): Promise<Db> {
  const db = await clonedReplayDatabase();
  cleanup.push(() => db.pg.close());
  await queueIsSound(db.pg);
  return db;
}

/** Today's stage, through the real producer and the real claim/start/settle: proves the queue fixture. */
async function queueIsSound(pg: PGlite) {
  const message = stageMessage("session-indexed", { orgId: "org_f", workspaceId: "ws_f", harness: "claude", sessionId: "s_f" }, "fixture@1", {});
  await postgresPipelineQueue(pg).send(message);
  const consumer = postgresStageConsumer({ sql: pg, stage: "session-indexed", deps: noDeps(), maxAttempts: 6 });
  const [claimed] = await consumer.source.claim(1);
  const started = await consumer.source.start!(claimed!);
  expect((started!.message as StageMessage).workKey).toBe("fixture@1");
  await consumer.source.settle(started!, { status: "done" });
  expect((await pg.query("SELECT 1 FROM sessions.pipeline_jobs")).rows).toHaveLength(0);
}

/**
 * Today's stage through the real dead-letter store (postgres-dead-letters.ts): quarantine → `dead`,
 * claimForReplay → `replaying`, resolve → `resolved`. Proves the table and its state machine. Leaves it empty.
 */
async function deadLettersAreSound(pg: PGlite) {
  const store = postgresDeadLetterStore(pg);
  const letter = await store.quarantine(stageMessage("session-indexed", { orgId: "org_f", workspaceId: "ws_f", harness: "claude", sessionId: "s_f" }, "fixture-dl@1", {}), new Date());
  expect(letter).toMatchObject({ stage: "session-indexed", state: "dead", deadCount: 1, replayCount: 0 });
  const replayId = crypto.randomUUID();
  expect(await store.claimForReplay(letter.id, { state: "dead", replayCount: 0 }, replayId, new Date())).toBe(true);
  await store.resolve(letter.id, new Date());
  expect((await store.list({ states: ["resolved"] })).map((l) => l.id)).toEqual([letter.id]);
  await pg.query("DELETE FROM sessions.pipeline_dead_letters");
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
/**
 * Design §3.3 / §3.3b [rev 6]: one dead letter per scope, id = the canonical deadLetterId('search-index', ctx,
 * workKey) of dlq.ts ('search-index:' || encodeURIComponent(org) || ':' || encodeURIComponent(ws) || ':' || workKey).
 */
const letterIdOf = (s: Scope) => deadLetterId(STAGE as never, s, searchWorkKey(s));

const publishClock = (at: string = new Date().toISOString()): Clock => ({ source: "publish", at });

/** The design §2 job message: a full StageMessage, `payload = { epoch, clock: { source, at } }`. */
function wake(scope: Scope, epoch: number | string, clock: Clock = publishClock(), traceId?: string): StageMessage<WakePayload> {
  return stageMessage(STAGE, scope, searchWorkKey(scope), { epoch: Number(epoch), clock }, traceId) as StageMessage<WakePayload>;
}

/** Design §4.5: the registry backoff, 30 s · 2^n capped at 900 s. */
const backoff = (attempt: number) => Math.min(900, 30 * 2 ** Math.max(0, attempt - 1));

// ---------------------------------------------------------------------------------------------------------
// The M2 module (design §10 "Production-path coupling [rev 5]"). Absent today.
// ---------------------------------------------------------------------------------------------------------

/** What claimSearchJobs returns per job: the lease it took and the claim-time snapshot (§3.2). */
type ClaimedSearchJob = { id: string | number; lease: string; claimedEpoch: number; claimedMessage: StageMessage<WakePayload> };
type SettleOutcome = "done" | { retry: number } | "exhausted";
type SearchJobsModule = {
  /** §3.1: the one-row-per-scope coalescing upsert. */
  enqueueSearchWake: (sql: PGlite, message: StageMessage) => Promise<unknown>;
  /** §3.2: claims up to n visible search-index jobs (lease + counted attempt), recording the claimed epoch and message. */
  claimSearchJobs: (sql: PGlite, n: number) => Promise<ClaimedSearchJob[]>;
  /**
   * §3.3 (+ §3.3b completion): the stage-specific, lease-bound, claimed-epoch-aware settlement. A lost lease
   * (0 rows for (id, lease)) returns `{ applied: false }` and changes nothing.
   */
  settleSearchJob: (sql: PGlite, claim: { id: string | number; lease: string; claimedEpoch: number; claimedMessage: StageMessage }, outcome: SettleOutcome) => Promise<{ applied: boolean } | unknown>;
  /** §3.3b: the stage-aware replay of one letter. */
  replaySearchDeadLetter: (sql: PGlite, letterId: string) => Promise<unknown>;
  /** §6: one catch-up tick (global keyset cursor, fairness, wrap) with the §2 clock lookup. */
  catchUpTick: (deps: { sql: PGlite; pageSize: number }) => Promise<unknown>;
  /** §4: the pass. */
  runSearchJob: (message: StageMessage, deps: { search: SearchIndexDeps }) => Promise<{
    outcome: "indexed" | "unchanged" | "not_committed" | "erased" | "held" | "busy";
    commitToSearchableMs?: number;
    pointerToSearchableMs?: number;
  }>;
  /** §8 park path: one fenced transaction parks every search-index row into its scope letter. */
  parkSearchJobs: (sql: PGlite, opts: { operator: string }) => Promise<unknown>;
  /** §8 [rev 6]: the down migration's precondition: 0 search-index rows in pipeline_jobs, pipeline_events AND pipeline_dead_letters. */
  searchStageDownPrecondition: (sql: PGlite) => Promise<boolean>;
  /**
   * §8 [rev 7]: the failure-safe operator archive. ONE transaction: refuse (throw, roll back) on any unresolved letter or
   * any job; copy every search-index letter and event into sessions.search_stage_archive plus one 'manifest' row
   * (counts, sha256); delete the archived rows.
   */
  archiveSearchStage: (sql: PGlite) => Promise<{ letters: number; events: number; sha256: string }>;
};
type M2Name = keyof SearchJobsModule;

/** The single loader. Called only after a test's fixture is built and proven, so the red is the module's absence. */
async function m2<K extends M2Name>(...names: K[]): Promise<Pick<SearchJobsModule, K>> {
  let mod: Partial<SearchJobsModule>;
  try {
    mod = await import("../search-jobs.ts" as string);
  } catch {
    throw new Error(`RED: src/session-pipeline/search-jobs.ts is absent (${names.join(", ")})`);
  }
  const missing = names.filter((n) => typeof mod[n] !== "function");
  if (missing.length) throw new Error(`RED: src/session-pipeline/search-jobs.ts does not export ${missing.join(", ")}`);
  return Object.fromEntries(names.map((n) => [n, mod[n]])) as Pick<SearchJobsModule, K>;
}

// ---------------------------------------------------------------------------------------------------------
// Observation (plain SELECTs) and coupled helpers.
// ---------------------------------------------------------------------------------------------------------

/** The rows of one scope's search-index job. */
async function jobs(pg: PGlite, workKey: string) {
  return (await pg.query<{ id: string; epoch: string; leased: boolean; lease_id: string | null; visible: boolean; attempts: number; dlq: boolean; message: StageMessage<WakePayload> }>(
    `SELECT id::text, message->'payload'->>'epoch' AS epoch, lease_id IS NOT NULL AS leased, lease_id::text, visible_at <= now() AS visible, attempts, dlq, message
       FROM sessions.pipeline_jobs WHERE stage = 'search-index' AND work_key = $1 ORDER BY id`, [workKey])).rows;
}
const allSearchJobs = async (pg: PGlite) => (await pg.query<{ session_id: string; epoch: string; message: StageMessage<WakePayload> }>(
  "SELECT message->'ctx'->>'sessionId' AS session_id, message->'payload'->>'epoch' AS epoch, message FROM sessions.pipeline_jobs WHERE stage = 'search-index' ORDER BY 1")).rows;

/** The dead letters, every stage. */
async function letters(pg: PGlite) {
  return (await pg.query<{ id: string; stage: string; epoch: string; state: string; dead_count: number; replay_count: number; resolved: boolean; message: StageMessage }>(
    `SELECT id, stage, message->'payload'->>'epoch' AS epoch, state, dead_count, replay_count, resolved_at IS NOT NULL AS resolved, message
       FROM sessions.pipeline_dead_letters ORDER BY id`)).rows;
}

/** claimSearchJobs(…, 1), checked against the table: the returned lease is the row's lease and the attempt was counted. */
async function claimOne(pg: PGlite, claimSearchJobs: SearchJobsModule["claimSearchJobs"]): Promise<ClaimedSearchJob & { attempts: number }> {
  const claimed = await claimSearchJobs(pg, 1);
  expect(claimed).toHaveLength(1);
  const c = claimed[0]!;
  const row = (await pg.query<{ lease_id: string | null; attempts: number; epoch: string }>(
    "SELECT lease_id::text, attempts, message->'payload'->>'epoch' AS epoch FROM sessions.pipeline_jobs WHERE id = $1::bigint", [String(c.id)])).rows;
  expect(row).toHaveLength(1);
  expect(row[0]!.lease_id).toBe(c.lease);
  expect(Number(c.claimedMessage.payload.epoch)).toBe(c.claimedEpoch);
  return { ...c, attempts: row[0]!.attempts };
}

/** Expire every search-index lease or backoff (the worker crashed, or the backoff elapsed): fixture time travel. */
const elapse = (pg: PGlite) => pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");

/** The directory row of a canonical scope. */
async function directoryOf(pg: PGlite, s: Scope) {
  return (await pg.query<{ epoch: string; ready: boolean | null }>(
    "SELECT event_projection_epoch::text AS epoch, event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
    [s.orgId, s.workspaceId, s.harness, s.sessionId])).rows;
}

// ---------------------------------------------------------------------------------------------------------
describe("R6: closed stage surfaces and the consumer wiring (design §7, §3.3, §10)", () => {
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

  test("R6m: the design §2 job message, payload.clock {source, at}, is a valid StageMessage that checked() accepts", async () => {
    const scope = { orgId: "org_r6", workspaceId: "ws_r6", harness: "claude", sessionId: "s_r6" };
    const m = wake(scope, 7, publishClock("2026-11-20T10:00:00.000Z"));
    expect(m.payload).toEqual({ epoch: 7, clock: { source: "publish", at: "2026-11-20T10:00:00.000Z" } });
    expect(m.payload).not.toHaveProperty("published_at");
    // Fixture soundness: the same message as a session-indexed message is accepted today.
    await new MemoryPipelineQueue().send({ ...m, stage: "session-indexed" });
    expect(isStageMessage(m)).toBe(true);
    await new MemoryPipelineQueue().send(m);
  });

  test("R6g (guard, green today): no generic runWorker registers search-index", async () => {
    // Design §3.3 / §7: only the stage-specific settler is authoritative for search-index; the generic
    // postgresStageConsumer settlement (a plain DELETE on done) must never settle it. The Lambda is the hosted
    // consumer, so src/hoopdb/indexer.ts (and every other worker main) does NOT list the stage.
    // GREEN TODAY ON PURPOSE: the stage is absent from every list, and must stay absent in M2. This is a
    // regression guard, not a red signal; the red wiring assertion is R6w.
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
  }, 120_000);

  test("R6w: the Lambda consumer's search-job loop imports and calls claimSearchJobs and settleSearchJob from ./search-jobs.ts", () => {
    // Design §5.1 / §10: the hosted consumer is the session-pipeline Lambda; its search-job loop must claim and
    // settle through the stage-specific path. Red today: lambda.ts has no search-job loop.
    const text = source("src/session-pipeline/lambda.ts");
    // Fixture soundness: the reader sees lambda.ts's real imports.
    expect(text).toMatch(/from\s+["']\.\/search-index\.ts["']/);
    const imported = [...text.matchAll(/import\s*\{([^}]*)\}\s*from\s*["']\.\/search-jobs(?:\.ts)?["']/g)]
      .flatMap((m) => m[1]!.split(",").map((s) => s.trim().split(/\s+as\s+/)[0]!.replace(/^type\s+/, "")).filter(Boolean));
    const body = text.replace(/import\s*\{[^}]*\}\s*from\s*["'][^"']+["'];?/g, "");
    const wiring = Object.fromEntries(["claimSearchJobs", "settleSearchJob"].map((n) => [n, { imported: imported.includes(n), called: new RegExp(`\\b${n}\\s*\\(`).test(body) }]));
    expect(wiring).toEqual({ claimSearchJobs: { imported: true, called: true }, settleSearchJob: { imported: true, called: true } });
    // Guard (green today): the Lambda never routes the stage through the generic consumer.
    expect(text).not.toMatch(/(postgresStageConsumer|pollPostgresStage)\(\{[^}]*["']search-index["']/);
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("R1: one row per scope, claim and settlement (design §3)", () => {
  const scope = { orgId: "org_r1", workspaceId: "ws_r1", harness: "claude", sessionId: "s_r1" };
  const WK = searchWorkKey(scope);

  test("R1a: wakes for epochs N and N+1, then a late N-1, leave ONE visible row carrying N+1's message", async () => {
    const db = await migrated();
    const { enqueueSearchWake } = await m2("enqueueSearchWake");
    const w8 = wake(scope, 8);
    await enqueueSearchWake(db.pg, wake(scope, 7));
    await enqueueSearchWake(db.pg, w8);
    await enqueueSearchWake(db.pg, wake(scope, 6));
    const rows = await jobs(db.pg, WK);
    expect(rows.map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ epoch: "8", leased: false, visible: true, attempts: 0, dlq: false }]);
    expect(rows[0]!.message).toEqual(w8 as StageMessage<WakePayload>);
  }, 120_000);

  test("R1b: N claimed, wake N+1 merges into the leased row; after a crash the SAME row is reclaimed with N+1", async () => {
    const db = await migrated();
    const { enqueueSearchWake, claimSearchJobs } = await m2("enqueueSearchWake", "claimSearchJobs");
    const w7 = wake(scope, 7);
    await enqueueSearchWake(db.pg, w7);
    const first = await claimOne(db.pg, claimSearchJobs);
    expect({ epoch: first.claimedEpoch, attempts: first.attempts }).toEqual({ epoch: 7, attempts: 1 });
    expect(first.claimedMessage).toEqual(w7);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    // Atomic max-epoch merge into the single row; the lease is kept and nothing is claimable.
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible })))
      .toEqual([{ id: String(first.id), epoch: "8", lease_id: first.lease, visible: false }]);
    expect(await claimSearchJobs(db.pg, 5)).toEqual([]);
    // The worker crashes: its lease expires.
    await elapse(db.pg);
    const again = await claimOne(db.pg, claimSearchJobs);
    expect({ id: String(again.id), epoch: again.claimedEpoch }).toEqual({ id: String(first.id), epoch: 8 });
    expect(again.lease).not.toBe(first.lease);
    expect(await jobs(db.pg, WK)).toHaveLength(1);
  }, 120_000);

  test("R1d: done with claimed N after a wake N+1 during the pass reopens the row (epoch N+1, attempts 0, lease NULL); N+1's done deletes it", async () => {
    const db = await migrated();
    const { enqueueSearchWake, claimSearchJobs, settleSearchJob } = await m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob");
    await enqueueSearchWake(db.pg, wake(scope, 7));
    const c = await claimOne(db.pg, claimSearchJobs);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    // A settlement under a lost lease reports {applied: false} (the contract this file fixes) and touches nothing.
    expect(await settleSearchJob(db.pg, { ...c, lease: crypto.randomUUID() }, "done")).toEqual({ applied: false });
    expect((await jobs(db.pg, WK)).map((j) => ({ epoch: j.epoch, lease_id: j.lease_id }))).toEqual([{ epoch: "8", lease_id: c.lease }]);
    await settleSearchJob(db.pg, c, "done");
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: String(c.id), epoch: "8", lease_id: null, visible: true, attempts: 0 }]);
    // N+1 runs next and its done deletes the row.
    const next = await claimOne(db.pg, claimSearchJobs);
    expect(next.claimedEpoch).toBe(8);
    await settleSearchJob(db.pg, next, "done");
    expect(await jobs(db.pg, WK)).toEqual([]);
  }, 120_000);

  test("R1e: N leased at attempt 11, wake N+1, the attempt fails: the stage retry reopens ONE row now, epoch N+1, attempts 0", async () => {
    const db = await migrated();
    const { enqueueSearchWake, claimSearchJobs, settleSearchJob } = await m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob");
    await enqueueSearchWake(db.pg, wake(scope, 7));
    const c = await claimOne(db.pg, claimSearchJobs);
    // Fixture: the leased row is on its 11th attempt.
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 11 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(c.id), c.lease]);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    await settleSearchJob(db.pg, c, { retry: 900 });
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: String(c.id), epoch: "8", lease_id: null, visible: true, attempts: 0 }]);
    // Control: a retry with NO newer wake keeps the budget and backs off.
    const next = await claimOne(db.pg, claimSearchJobs);
    expect({ epoch: next.claimedEpoch, attempts: next.attempts }).toEqual({ epoch: 8, attempts: 1 });
    await settleSearchJob(db.pg, next, { retry: 30 });
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: String(c.id), epoch: "8", lease_id: null, visible: false, attempts: 1 }]);
    const { rows } = await db.pg.query<{ ok: boolean }>("SELECT visible_at BETWEEN now() + interval '20 seconds' AND now() + interval '40 seconds' AS ok FROM sessions.pipeline_jobs WHERE stage = 'search-index'");
    expect(rows).toEqual([{ ok: true }]);
  }, 120_000);

  test("R1e': unleased N at attempt 11 (and dlq), wake N+1: the row gets attempts 0, dlq false, visible now; a same-epoch re-wake keeps the budget", async () => {
    const db = await migrated();
    const { enqueueSearchWake } = await m2("enqueueSearchWake");
    await enqueueSearchWake(db.pg, wake(scope, 7));
    expect(await jobs(db.pg, WK)).toHaveLength(1);
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 11, dlq = true, visible_at = now() + interval '15 minutes' WHERE stage = 'search-index' AND work_key = $1", [WK]);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    expect((await jobs(db.pg, WK)).map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ epoch: "8", leased: false, visible: true, attempts: 0, dlq: false }]);
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 3 WHERE stage = 'search-index' AND work_key = $1", [WK]);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    expect((await jobs(db.pg, WK)).map((j) => ({ epoch: j.epoch, attempts: j.attempts }))).toEqual([{ epoch: "8", attempts: 3 }]);
  }, 120_000);

  test("R1f: a wake N+1 during N's exhausting (12th) attempt: one letter, the claimed N snapshot; N+1 survives on a fresh budget", async () => {
    const db = await migrated();
    await deadLettersAreSound(db.pg);
    const { enqueueSearchWake, claimSearchJobs, settleSearchJob } = await m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob");
    await enqueueSearchWake(db.pg, wake(scope, 7));
    const seen: number[] = [];
    let final: ClaimedSearchJob | undefined;
    for (let n = 1; n <= 12; n++) {
      await elapse(db.pg);
      const c = await claimOne(db.pg, claimSearchJobs);
      seen.push(c.attempts);
      expect(c.claimedEpoch).toBe(7);
      if (c.attempts < 12) { await settleSearchJob(db.pg, c, { retry: backoff(c.attempts) }); continue; }
      // The final attempt is still running when wake N+1 commits; then the attempt is held and exhausts.
      await enqueueSearchWake(db.pg, wake(scope, 8));
      await settleSearchJob(db.pg, c, "exhausted");
      final = c;
    }
    expect(seen).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const parked = await letters(db.pg);
    expect(parked.map((l) => ({ id: l.id, stage: l.stage, epoch: l.epoch, state: l.state, dead_count: l.dead_count, resolved: l.resolved })))
      .toEqual([{ id: letterIdOf(scope), stage: STAGE, epoch: "7", state: "dead", dead_count: 1, resolved: false }]);
    expect(parked[0]!.message).toEqual(final!.claimedMessage as StageMessage);
    expect((await jobs(db.pg, WK)).map((j) => ({ epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ epoch: "8", lease_id: null, visible: true, attempts: 0 }]);
    // N+1 is not lost: it is claimed next, on a fresh budget.
    const next = await claimOne(db.pg, claimSearchJobs);
    expect({ epoch: next.claimedEpoch, attempts: next.attempts }).toEqual({ epoch: 8, attempts: 1 });
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R1c/R5a/R5c/R5e: the wake is written inside the publication-authority transaction (design §2, §0)", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead(); }, 180_000);
  afterAll(() => head?.close());
  const searchJobsOf = () => head.jobsAfter.filter((j) => j.stage === STAGE);
  const canonical = (): Scope => ({ orgId: head.scope.orgId, workspaceId: head.scope.workspaceId, harness: head.scope.harness, sessionId: head.scope.canonicalSessionId });

  test("R1c- (guard, green today): a retired publication rolls the authority transaction back: zero jobs of any stage", () => {
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

  // Mutates head.db (drops the wake).
  test("R5c: a managed scope's catch-up job carries clock.source='publish', at = the head attempt's published_at", async () => {
    // Fixture soundness: the managed scope is committed and stale; its head has one published attempt.
    expect((await directoryOf(head.db, canonical())).map((d) => [d.epoch, d.ready === true])).toEqual([[head.epoch, false]]);
    const attempts = (await head.db.query<{ published_at: string }>(
      `SELECT to_char(published_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS published_at FROM sessions.ingest_publication_attempts WHERE state = 'published'`)).rows;
    expect(attempts).toEqual([{ published_at: head.publishedAt }]);
    // The wake was lost.
    await head.db.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'");
    const { catchUpTick } = await m2("catchUpTick");
    await catchUpTick({ sql: head.db, pageSize: 500 });
    const found = await allSearchJobs(head.db);
    expect(found.map((f) => [f.session_id, f.epoch])).toEqual([[head.scope.canonicalSessionId, head.epoch]]);
    expect(found[0]!.message.payload.clock.source).toBe("publish");
    expect(Date.parse(found[0]!.message.payload.clock.at)).toBe(Date.parse(head.publishedAt));
  });

  // Mutates head.db (adds sibling heads, drops the wake): keep it last in this block.
  test("R5e: two live heads for one canonical scope: the clock is the later published_at; retired, deleted and other-scope heads are excluded", async () => {
    const at = (ms: number) => new Date(Date.parse(head.publishedAt) + ms).toISOString();
    // Fixture: sibling heads cloned from the real one (siblingHead, triggers off): the state the §2 clock query reads.
    const sibling = (n: number, o: Parameters<typeof siblingHead>[3]) => siblingHead(head.db, head.key, `r5e-${n}`, o);
    await sibling(1, { publishedAt: at(5_000) });                                  // the later live head: the expected clock
    await sibling(2, { publishedAt: at(60_000), headState: "retired" });           // excluded: retired head
    await sibling(3, { publishedAt: at(60_000), deleted: true });                  // excluded: its pointer row is deleted
    await sibling(4, { publishedAt: at(60_000), canonical: "r5e-other-scope" });   // excluded: another canonical scope
    // Fixture soundness: five heads, three live for this canonical scope's live pointer rows; the scope is stale.
    const live = (await head.db.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM sessions.ingest_publication_heads h JOIN sessions.session_index p ON p.manifest_key = h.current_key AND p.deleted_at IS NULL
        WHERE h.state = 'live' AND coalesce(p.canonical_session_id, p.session_id) = $1`, [head.scope.canonicalSessionId])).rows[0]!.n;
    expect(live).toBe(2);
    expect((await head.db.query("SELECT 1 FROM sessions.ingest_publication_heads")).rows).toHaveLength(5);
    expect((await directoryOf(head.db, canonical())).map((d) => d.ready === true)).toEqual([false]);
    await head.db.query("DELETE FROM sessions.pipeline_jobs WHERE stage = 'search-index'");
    const { catchUpTick } = await m2("catchUpTick");
    await catchUpTick({ sql: head.db, pageSize: 500 });
    const found = (await allSearchJobs(head.db)).filter((f) => f.session_id === head.scope.canonicalSessionId);
    expect(found).toHaveLength(1);
    expect(found[0]!.message.payload.clock.source).toBe("publish");
    expect(new Date(found[0]!.message.payload.clock.at).toISOString()).toBe(at(5_000));
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("R5f: an OLDER live head triggers the authority enqueue beside a NEWER sibling of the same canonical scope (design §2 [rev 6])", () => {
  // Built with ONE real publication through the authority path plus seeded sibling heads (triggers off, as R5e):
  // a second real publication of the same canonical session from another machine needs a second frontier, seat
  // clock and ledger origin the portable fixture does not model. The siblings are seeded BEFORE the real authority
  // transaction (publishedHead's beforeAuthority hook), so the real §2 writer reads them when it enqueues.
  let head: PublishedHead;
  let newer = "", retiredLater = "";
  beforeAll(async () => {
    head = await publishedHead({
      beforeAuthority: async ({ db, key, publishedAt }) => {
        const at = (ms: number) => new Date(Date.parse(publishedAt) + ms).toISOString();
        newer = at(5_000);
        retiredLater = at(60_000);
        await siblingHead(db, key, "r5f-1", { publishedAt: newer });                                // the NEWER live sibling
        await siblingHead(db, key, "r5f-2", { publishedAt: retiredLater, headState: "retired" });    // excluded: retired
      },
    });
  }, 180_000);
  afterAll(() => head?.close());

  /** Fixture soundness: two live heads of the canonical scope; the trigger is the OLDER one; the newer is a sibling. */
  async function siblingsAreSound() {
    const { rows } = await head.db.query<{ key: string; machine_id: string; state: string; published_at: string }>(
      `SELECT h.current_key AS key, h.machine_id, h.state, to_char(a.published_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS published_at
         FROM sessions.ingest_publication_heads h
         JOIN sessions.ingest_publication_images i ON i.manifest_key = h.current_key
         JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
        ORDER BY a.published_at`);
    expect(rows.map((r) => [r.key, r.state, r.published_at])).toEqual([
      [head.key, "live", head.publishedAt], [`${head.key}.r5f-1`, "live", newer], [`${head.key}.r5f-2`, "retired", retiredLater]]);
    // The session-indexed delivery that ran the authority path names the real (older) head's key.
    expect((head.indexed.payload as { manifestKey?: string }).manifestKey).toBe(head.key);
    expect(Date.parse(newer)).toBeGreaterThan(Date.parse(head.publishedAt));
  }

  test("R5f+: the job written by the OLDER head's authority transaction carries clock.at = the NEWER live sibling's published_at", async () => {
    await siblingsAreSound();
    const found = head.jobsAfter.filter((j) => j.stage === STAGE);
    expect(found).toHaveLength(1);
    const m = found[0]!.message as StageMessage<WakePayload>;
    expect(m.ctx.sessionId).toBe(head.scope.canonicalSessionId);
    expect(m.payload.clock.source).toBe("publish");
    expect(new Date(m.payload.clock.at).toISOString()).toBe(newer);
    expect(new Date(m.payload.clock.at).toISOString()).not.toBe(head.publishedAt);
  });

  test("R5f- (guard, green today): a RETIRED trigger writes no job even though a valid newer sibling exists", async () => {
    // The trigger is validated by its OWN authority (session_publication_lock), never by a sibling's clock. The
    // retired case ran after the siblings were seeded (beforeAuthority), inside its own rolled-back transaction.
    await siblingsAreSound();
    expect(head.retired.outcome).toEqual({ status: "skipped", reason: "publication_retired" });
    expect(head.retired.jobsWritten).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("R5g: a managed scope whose publish clock is unprovable fails closed (design §2 [rev 7])", () => {
  // The real head's own attempt loses its published_at (triggers off) BEFORE the real authority transaction, so the
  // §2 clock aggregate is NULL for a managed, live, current scope. Contract: the session STILL becomes readable (the
  // authority transaction commits the pointer and directory; a search-side problem never blocks upload or read), but
  // the writer enqueues NO search-index job, never falls back to a pointer clock, and records the refusal [rev 7] as a
  // sessions.pipeline_events row IN the authority transaction (stage 'search-index', status 'skipped', error.code
  // 'clock_unprovable'). The test OBSERVES that production substrate with a plain SELECT after the real authority run;
  // no M2 call is made, so only the real writer can produce the row.
  let head: PublishedHead;
  beforeAll(async () => {
    head = await publishedHead({
      beforeAuthority: async ({ db, key }) => {
        await db.transaction(async (tx) => {
          await tx.query("SET LOCAL session_replication_role = replica");
          const r = await tx.query(`UPDATE sessions.ingest_publication_attempts SET published_at = NULL
            WHERE attempt_token = (SELECT attempt_token FROM sessions.ingest_publication_images WHERE manifest_key = $1) RETURNING 1`, [key]);
          expect(r.rows).toHaveLength(1);
        });
      },
    });
  }, 180_000);
  afterAll(() => head?.close());

  test("R5g: NULL published_at on the managed head: the session is readable, no search-index job, never a pointer clock, and one clock_unprovable refusal row in pipeline_events", async () => {
    // Fixture soundness: the head is live, its generation live and its attempt published; only the clock is missing.
    const { rows } = await head.db.query<{ state: string; gen_state: string; attempt_state: string; unclocked: boolean }>(
      `SELECT h.state, g.state AS gen_state, a.state AS attempt_state, a.published_at IS NULL AS unclocked
         FROM sessions.ingest_publication_heads h
         JOIN sessions.ingest_publication_generations g ON g.generation = h.generation
         JOIN sessions.ingest_publication_images i ON i.manifest_key = h.current_key
         JOIN sessions.ingest_publication_attempts a ON a.attempt_token = i.attempt_token
        WHERE h.current_key = $1`, [head.key]);
    expect(rows).toEqual([{ state: "live", gen_state: "live", attempt_state: "published", unclocked: true }]);
    // Readability is never blocked: the authority delivery acks and the live pointer row exists.
    expect(head.authority).toMatchObject({ acked: 1 });
    expect((await head.db.query("SELECT 1 FROM sessions.session_index WHERE manifest_key = $1 AND deleted_at IS NULL", [head.key])).rows).toHaveLength(1);
    // Never a job with a pointer clock, and no search-index job at all.
    expect(head.jobsAfter.filter((j) => (j.message.payload as Partial<WakePayload> | undefined)?.clock?.source === "pointer")).toEqual([]);
    expect(head.jobsAfter.filter((j) => j.stage === STAGE)).toEqual([]);
    // The refusal is durable in pipeline_events, committed with the pointer (red today: no writer exists, and the
    // pipeline_events stage CHECK does not admit 'search-index'). The scope is the canonical one (§2), and the
    // idempotency key is §2's `trace:search-index:skipped:1:<manifest_key>`, `trace` being the trace id (the column's
    // documented `trace:stage:status:attempt:discriminator`, events.ts pipelineEventKey) of the session-indexed message.
    expect(head.scope.sessionId).toBe(head.scope.canonicalSessionId);
    const refusals = (await head.db.query<Record<string, unknown>>(
      `SELECT trace_id::text, org_id, workspace_id, harness, session_id, attempt, error, idempotency_key
         FROM sessions.pipeline_events
        WHERE stage = 'search-index' AND status = 'skipped' AND error->>'code' = 'clock_unprovable'
          AND org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4`,
      [head.scope.orgId, head.scope.workspaceId, head.scope.harness, head.scope.canonicalSessionId])).rows;
    expect(refusals).toEqual([{
      trace_id: head.indexed.ctx.traceId,
      org_id: head.scope.orgId, workspace_id: head.scope.workspaceId, harness: head.scope.harness, session_id: head.scope.canonicalSessionId,
      attempt: 1,
      error: expect.objectContaining({ code: "clock_unprovable", retryable: false }),
      idempotency_key: `${head.indexed.ctx.traceId}:search-index:skipped:1:${head.key}`,
    }]);
  });
});

// ---------------------------------------------------------------------------------------------------------
describe("R2: the global keyset catch-up cursor, through catchUpTick (design §6)", () => {
  const cursor = async (pg: PGlite) => (await pg.query<{ org_id: string; workspace_id: string; harness: string; session_id: string; lap: number }>(
    "SELECT org_id, workspace_id, harness, session_id, lap::int AS lap FROM sessions.search_catchup_cursor")).rows;
  const enqueued = async (pg: PGlite) => (await allSearchJobs(pg)).map((j) => `${j.message.ctx.orgId}/${j.session_id}`).sort();

  /** Committed, never-indexed scopes: a live session_index row and a projected, not-ready directory row each. */
  async function seed(db: Db, scopes: Scope[]) {
    for (const s of scopes) {
      await addReplayPointer(db.db, { identity: { orgId: s.orgId, workspaceId: s.workspaceId, source: s.harness, sessionId: s.sessionId },
        key: `96bulls/${s.orgId}/catchup/${s.sessionId}/manifest.json`, digest: "ab".repeat(32), physicalSession: s.sessionId, physicalMachine: "m1", canonicalSession: s.sessionId });
      await projectSessionDirectory(db.db, { orgId: s.orgId, workspaceId: s.workspaceId, harness: s.harness, sessionId: s.sessionId });
    }
    // Fixture soundness: each scope has a live pointer row and a not-ready directory row; nothing is enqueued.
    for (const s of scopes) {
      expect((await directoryOf(db.pg, s)).map((d) => d.ready === true)).toEqual([false]);
      expect((await db.pg.query("SELECT 1 FROM sessions.session_index WHERE org_id=$1 AND session_id=$2 AND deleted_at IS NULL", [s.orgId, s.sessionId])).rows).toHaveLength(1);
    }
    expect(await allSearchJobs(db.pg)).toEqual([]);
  }
  const one = (org: string, n: number): Scope => ({ orgId: org, workspaceId: "ws_cu", harness: "claude", sessionId: `s_${String(n).padStart(2, "0")}` });

  test("R2a: a lost wake (no job) converges through one catch-up tick (one lap), with a pointer clock = max(indexed_at)", async () => {
    const db = await migrated();
    const s = one("org_cu_a", 1);
    await seed(db, [s]);
    const [dir] = await directoryOf(db.pg, s);
    // Oracle (observation): the pointer clock of §2.
    const pointerAt = (await db.pg.query<{ at: string }>("SELECT max(indexed_at) AS at FROM sessions.session_index WHERE org_id=$1 AND session_id=$2 AND deleted_at IS NULL", [s.orgId, s.sessionId])).rows[0]!.at;
    const { catchUpTick } = await m2("catchUpTick");
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    const found = await jobs(db.pg, searchWorkKey(s));
    expect(found.map((j) => ({ epoch: j.epoch, leased: j.leased, visible: j.visible }))).toEqual([{ epoch: dir!.epoch, leased: false, visible: true }]);
    expect(found[0]!.message.payload.clock.source).toBe("pointer");
    expect(Date.parse(found[0]!.message.payload.clock.at)).toBe(new Date(pointerAt).getTime());
    // A short page (1 < 2) wrapped the cursor: one lap done.
    expect((await cursor(db.pg)).map((c) => c.lap)).toEqual([1]);
  }, 120_000);

  test("R2b: S = 2, five stale scopes in ONE tenant, the first two permanently held: all five enqueued within 3 ticks; the cursor advances", async () => {
    const db = await migrated();
    const scopes = [1, 2, 3, 4, 5].map((n) => one("org_cu_b", n));
    await seed(db, scopes);
    const { catchUpTick } = await m2("catchUpTick");
    const at = (n: number) => ({ org_id: "org_cu_b", workspace_id: "ws_cu", harness: "claude", session_id: `s_0${n}`, lap: 0 });
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await enqueued(db.pg)).toEqual(["org_cu_b/s_01", "org_cu_b/s_02"]);
    expect(await cursor(db.pg)).toEqual([at(2)]);
    // Fixture: the first page is held: its passes never finish (leased, attempts climbing), so they stay stale.
    for (const s of scopes.slice(0, 2))
      await db.pg.query("UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid(), attempts = 5, visible_at = now() + interval '1 hour' WHERE work_key = $1", [searchWorkKey(s)]);
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await enqueued(db.pg)).toEqual(["org_cu_b/s_01", "org_cu_b/s_02", "org_cu_b/s_03", "org_cu_b/s_04"]);
    expect(await cursor(db.pg)).toEqual([at(4)]);
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await enqueued(db.pg)).toEqual(scopes.map((s) => `org_cu_b/${s.sessionId}`));
    expect((await cursor(db.pg)).map((c) => c.lap)).toEqual([1]);
    // A held scope's re-enqueue (same epoch) is a no-op upsert: still leased, attempts kept.
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    for (const s of scopes.slice(0, 2)) expect((await jobs(db.pg, searchWorkKey(s))).map((j) => ({ leased: j.leased, attempts: j.attempts }))).toEqual([{ leased: true, attempts: 5 }]);
    expect(await cursor(db.pg)).toEqual([{ ...at(2), lap: 1 }]);
  }, 120_000);

  test("R2c: five tenants, S = 2: the cursor visits every tenant, wraps after a short page, lap increments and the next lap restarts at the minimum", async () => {
    const db = await migrated();
    const orgs = ["org_cu_c1", "org_cu_c2", "org_cu_c3", "org_cu_c4", "org_cu_c5"];
    await seed(db, orgs.map((o) => one(o, 1)));
    const { catchUpTick } = await m2("catchUpTick");
    const orgsEnqueued = async () => [...new Set((await allSearchJobs(db.pg)).map((j) => j.message.ctx.orgId))].sort();
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await orgsEnqueued()).toEqual(orgs.slice(0, 2));
    expect((await cursor(db.pg)).map((c) => [c.org_id, c.lap])).toEqual([["org_cu_c2", 0]]);
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await orgsEnqueued()).toEqual(orgs.slice(0, 4));
    expect((await cursor(db.pg)).map((c) => [c.org_id, c.lap])).toEqual([["org_cu_c4", 0]]);
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect(await orgsEnqueued()).toEqual(orgs);
    expect((await cursor(db.pg)).map((c) => c.lap)).toEqual([1]);
    // The wrap starts the next lap from the minimum.
    await catchUpTick({ sql: db.pg, pageSize: 2 });
    expect((await cursor(db.pg)).map((c) => [c.org_id, c.lap])).toEqual([["org_cu_c2", 1]]);
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
    const { runSearchJob } = await m2("runSearchJob");
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
    const { runSearchJob } = await m2("runSearchJob");
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
    const { runSearchJob } = await m2("runSearchJob");
    const out = await runSearchJob(wake(canonicalScope(), String(BigInt(now.epoch) - 1n)), { search: r.search() });
    expect(out.outcome).toBe("unchanged");
    expect(r.fake.calls.filter((c) => c.ch !== "select")).toEqual([]);
  }, 120_000);
});


// ---------------------------------------------------------------------------------------------------------
describe("R4: acceptance held past max attempts, quarantined through settleSearchJob(…, 'exhausted') (design §4.5, §3.3)", () => {
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

  test("R4: twelve held passes exhaust: ONE letter deadLetterId('search-index', ctx, work_key) with the claimed snapshot; the scope stays not ready and out of search; a re-death increments dead_count", async () => {
    const r = await hostedRig(held, cleanup);
    await queueIsSound(r.db.pg);
    await deadLettersAreSound(r.db.pg);
    await r.tick({});
    const scope = { ...SCOPE, sessionId: held.canonicalId };
    const WK = searchWorkKey(scope);
    const dir = async () => (await directoryOf(r.db.pg, scope))[0]!;
    const inSearch = () => r.fake.rows.filter((row) => row.session_id === SCOPE.sessionId || row.session_id === held.canonicalId).length;
    // Fixture soundness: the committed scope is held on acceptance, not ready, and has no search rows.
    const first = await indexSearchScope(SCOPE, r.search());
    expect(first.complete).toBe(false);
    expect(first.reasons).toContain("relayhistory_acceptance_missing");
    expect((await dir()).ready).toBe(false);
    expect(inSearch()).toBe(0);

    const { enqueueSearchWake, claimSearchJobs, settleSearchJob, runSearchJob } = await m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob", "runSearchJob");
    await enqueueSearchWake(r.db.pg, wake(scope, (await dir()).epoch));
    const attempts: number[] = [];
    let last: ClaimedSearchJob | undefined;
    for (let n = 1; n <= 12; n++) {
      await elapse(r.db.pg);
      const c = await claimOne(r.db.pg, claimSearchJobs);
      attempts.push(c.attempts);
      // No newer wake: the claimed epoch is the current one on every attempt.
      expect(c.claimedEpoch).toBe(Number((await dir()).epoch));
      // The held decision of §4.5: the pass answers held, never acks.
      expect((await runSearchJob(c.claimedMessage, { search: r.search() })).outcome).toBe("held");
      if (c.attempts < 12) await settleSearchJob(r.db.pg, c, { retry: backoff(c.attempts) });
      else { await settleSearchJob(r.db.pg, c, "exhausted"); last = c; }
    }
    expect(attempts).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    expect(await jobs(r.db.pg, WK)).toEqual([]);
    const parked = await letters(r.db.pg);
    expect(parked.map((l) => ({ id: l.id, stage: l.stage, epoch: l.epoch, state: l.state, dead_count: l.dead_count, replay_count: l.replay_count, resolved: l.resolved })))
      .toEqual([{ id: letterIdOf(scope), stage: STAGE, epoch: (await dir()).epoch, state: "dead", dead_count: 1, replay_count: 0, resolved: false }]);
    expect(parked[0]!.message).toEqual(last!.claimedMessage as StageMessage);
    expect((await dir()).ready).toBe(false);
    expect(inSearch()).toBe(0);
    // A later wake inserts a fresh row: an old quarantine never blocks a newly accepted revision.
    const later = wake(scope, Number((await dir()).epoch) + 1);
    await enqueueSearchWake(r.db.pg, later);
    expect((await jobs(r.db.pg, WK)).map((j) => ({ epoch: j.epoch, attempts: j.attempts, leased: j.leased }))).toEqual([{ epoch: later.payload.epoch.toString(), attempts: 0, leased: false }]);
    // It is held again and exhausts: the SAME letter is refreshed (dead_count 2, the new claimed snapshot).
    const again = await claimOne(r.db.pg, claimSearchJobs);
    await r.db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(again.id), again.lease]);
    await settleSearchJob(r.db.pg, again, "exhausted");
    const refreshed = await letters(r.db.pg);
    expect(refreshed.map((l) => ({ id: l.id, epoch: l.epoch, state: l.state, dead_count: l.dead_count, resolved: l.resolved })))
      .toEqual([{ id: letterIdOf(scope), epoch: later.payload.epoch.toString(), state: "dead", dead_count: 2, resolved: false }]);
    expect(refreshed[0]!.message).toEqual(again.claimedMessage as StageMessage);
    expect(await jobs(r.db.pg, WK)).toEqual([]);
  }, 180_000);

  test("R4b: N exhausts after N+1 merged into the leased row: only N's claim-time snapshot is parked; the row survives with N+1, attempts 0; a lost lease parks nothing", async () => {
    const db = await migrated();
    await deadLettersAreSound(db.pg);
    const scope = { orgId: "org_r4b", workspaceId: "ws_r4b", harness: "claude", sessionId: "s_r4b" };
    const WK = searchWorkKey(scope);
    const { enqueueSearchWake, claimSearchJobs, settleSearchJob } = await m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob");
    await enqueueSearchWake(db.pg, wake(scope, 7));
    const c = await claimOne(db.pg, claimSearchJobs);
    // Fixture: the leased row is on its 12th and final attempt.
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(c.id), c.lease]);
    await enqueueSearchWake(db.pg, wake(scope, 8));
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, attempts: j.attempts })))
      .toEqual([{ id: String(c.id), epoch: "8", lease_id: c.lease, attempts: 0 }]);
    // A lost lease aborts the whole settlement: {applied: false}, nothing parked, nothing deleted, the lease untouched.
    expect(await settleSearchJob(db.pg, { ...c, lease: crypto.randomUUID() }, "exhausted")).toEqual({ applied: false });
    expect(await letters(db.pg)).toEqual([]);
    expect((await jobs(db.pg, WK)).map((j) => ({ epoch: j.epoch, lease_id: j.lease_id }))).toEqual([{ epoch: "8", lease_id: c.lease }]);
    // N exhausts under its lease.
    await settleSearchJob(db.pg, c, "exhausted");
    const parked = await letters(db.pg);
    expect(parked.map((l) => ({ id: l.id, epoch: l.epoch, state: l.state, dead_count: l.dead_count, resolved: l.resolved })))
      .toEqual([{ id: letterIdOf(scope), epoch: "7", state: "dead", dead_count: 1, resolved: false }]);
    expect(parked[0]!.message).toEqual(c.claimedMessage as StageMessage);
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts, dlq: j.dlq })))
      .toEqual([{ id: String(c.id), epoch: "8", lease_id: null, visible: true, attempts: 0, dlq: false }]);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R8: scope-keyed dead-letter replay, through replaySearchDeadLetter and settleSearchJob (design §3.3b)", () => {
  // An org and a workspace that need URI encoding: the letter id must be dlq.ts's canonical deadLetterId.
  const scope = { orgId: "org:r8 enc", workspaceId: "ws r8:x", harness: "claude", sessionId: "s_r8" };
  const WK = searchWorkKey(scope);
  const LETTER = letterIdOf(scope);
  /** The rev 6 `resolution` column. Read only after an M2 call (the column is absent until M2's migration). */
  const resolutionOf = async (pg: PGlite) => (await pg.query<{ resolution: string | null }>(
    "SELECT resolution FROM sessions.pipeline_dead_letters WHERE id = $1", [LETTER])).rows.map((r) => r.resolution);
  const m2r8 = () => m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob", "replaySearchDeadLetter");
  type R8 = Awaited<ReturnType<typeof m2r8>>;
  const letter = async (pg: PGlite) => {
    const all = await letters(pg);
    expect(all.map((l) => l.id)).toEqual([LETTER]);
    return all[0]!;
  };

  /** A wake at `epoch`, claimed, held to its last attempt and exhausted (no newer wake): returns the claimed snapshot. */
  async function deathAt(pg: PGlite, f: R8, epoch: number) {
    await f.enqueueSearchWake(pg, wake(scope, epoch));
    const c = await claimOne(pg, f.claimSearchJobs);
    expect(c.claimedEpoch).toBe(epoch);
    await pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(c.id), c.lease]);
    await f.settleSearchJob(pg, c, "exhausted");
    expect(await jobs(pg, WK)).toEqual([]);
    return c.claimedMessage;
  }

  async function letterAt(db: Db, f: R8, epoch: number) {
    // Fixture soundness: the canonical id encodes this scope's ':' and ' ' (dlq.ts deadLetterId).
    expect(LETTER).toBe(`search-index:org%3Ar8%20enc:ws%20r8%3Ax:${WK}`);
    await deadLettersAreSound(db.pg);
    const snapshot = await deathAt(db.pg, f, epoch);
    const l = await letter(db.pg);
    expect({ epoch: l.epoch, state: l.state, dead_count: l.dead_count, replay_count: l.replay_count, resolved: l.resolved })
      .toEqual({ epoch: String(epoch), state: "dead", dead_count: 1, replay_count: 0, resolved: false });
    expect(l.message).toEqual(snapshot as StageMessage);
    return l;
  }

  test("R8a: superseded — a letter at N replayed while the row carries N+1: the row keeps N+1; the letter resolves when N+1 completes", async () => {
    const db = await migrated();
    const f = await m2r8();
    await letterAt(db, f, 7);
    const w8 = wake(scope, 8);
    await f.enqueueSearchWake(db.pg, w8);
    await f.replaySearchDeadLetter(db.pg, LETTER);
    expect((await letter(db.pg))).toMatchObject({ state: "replaying", replay_count: 1, resolved: false });
    // The upsert coalesced into the newer row: N+1's message is kept.
    const rows = await jobs(db.pg, WK);
    expect(rows.map((j) => j.epoch)).toEqual(["8"]);
    expect(rows[0]!.message).toEqual(w8);
    const c = await claimOne(db.pg, f.claimSearchJobs);
    expect(c.claimedEpoch).toBe(8);
    await f.settleSearchJob(db.pg, c, "done");
    expect(await jobs(db.pg, WK)).toEqual([]);
    expect(await letter(db.pg)).toMatchObject({ state: "resolved", resolved: true, replay_count: 1 });
    // A newer epoch (8) covered the letter's (7).
    expect(await resolutionOf(db.pg)).toEqual(["superseded"]);
  }, 120_000);

  test("R8b: merged — no row exists: the replay enqueues the letter's message N; its done resolves the letter; a second replay of a resolved letter is a no-op", async () => {
    const db = await migrated();
    const f = await m2r8();
    const l = await letterAt(db, f, 7);
    await f.replaySearchDeadLetter(db.pg, LETTER);
    const rows = await jobs(db.pg, WK);
    expect(rows.map((j) => ({ epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts }))).toEqual([{ epoch: "7", lease_id: null, visible: true, attempts: 0 }]);
    expect(rows[0]!.message.workKey).toBe(WK);
    expect(rows[0]!.message.ctx).toEqual(l.message.ctx);
    expect(await letter(db.pg)).toMatchObject({ state: "replaying", replay_count: 1 });
    const c = await claimOne(db.pg, f.claimSearchJobs);
    expect(c.claimedEpoch).toBe(7);
    await f.settleSearchJob(db.pg, c, "done");
    expect(await jobs(db.pg, WK)).toEqual([]);
    expect(await letter(db.pg)).toMatchObject({ state: "resolved", resolved: true });
    // The completed epoch equals the letter's.
    expect(await resolutionOf(db.pg)).toEqual(["completed"]);
    // Replay claims only a dead letter.
    await Promise.resolve(f.replaySearchDeadLetter(db.pg, LETTER)).catch(() => undefined);
    expect(await jobs(db.pg, WK)).toEqual([]);
    expect(await letter(db.pg)).toMatchObject({ state: "resolved", resolved: true, replay_count: 1 });
    expect(await resolutionOf(db.pg)).toEqual(["completed"]);
  }, 120_000);

  test("R8c: the rev-4 race — replay N, claim N, wake N+1, done N reopens, claim N+1, done N+1: the letter is resolved", async () => {
    const db = await migrated();
    const f = await m2r8();
    await letterAt(db, f, 7);
    await f.replaySearchDeadLetter(db.pg, LETTER);
    const cN = await claimOne(db.pg, f.claimSearchJobs);
    expect(cN.claimedEpoch).toBe(7);
    await f.enqueueSearchWake(db.pg, wake(scope, 8));
    await f.settleSearchJob(db.pg, cN, "done");
    // done N fails the epoch guard and reopens the single row with N+1 (no replay handle in the message).
    expect((await jobs(db.pg, WK)).map((j) => ({ id: j.id, epoch: j.epoch, lease_id: j.lease_id, visible: j.visible, attempts: j.attempts })))
      .toEqual([{ id: String(cN.id), epoch: "8", lease_id: null, visible: true, attempts: 0 }]);
    // The reopened row is not a completion: the letter is still replaying, unresolved.
    expect(await letter(db.pg)).toMatchObject({ state: "replaying", resolved: false, replay_count: 1, dead_count: 1 });
    expect(await resolutionOf(db.pg)).toEqual([null]);
    const cN1 = await claimOne(db.pg, f.claimSearchJobs);
    expect(cN1.claimedEpoch).toBe(8);
    await f.settleSearchJob(db.pg, cN1, "done");
    expect(await jobs(db.pg, WK)).toEqual([]);
    // Only N+1's completion resolves it, as superseded.
    expect(await letter(db.pg)).toMatchObject({ state: "resolved", resolved: true, replay_count: 1, dead_count: 1 });
    expect(await resolutionOf(db.pg)).toEqual(["superseded"]);
    expect(await postgresDeadLetterStore(db.pg).list({ stage: STAGE as never, states: ["replaying"] })).toEqual([]);
  }, 120_000);

  test("R8d: re-death after replay — the letter returns to 'dead', resolved_at NULL, message refreshed to the new claimed snapshot (while replaying, and after resolved)", async () => {
    const db = await migrated();
    const f = await m2r8();
    await letterAt(db, f, 7);
    // (1) Re-death while replaying: replay N, a wake N+1 merges, N+1 is claimed and exhausts.
    await f.replaySearchDeadLetter(db.pg, LETTER);
    await f.enqueueSearchWake(db.pg, wake(scope, 8));
    const c8 = await claimOne(db.pg, f.claimSearchJobs);
    expect(c8.claimedEpoch).toBe(8);
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(c8.id), c8.lease]);
    await f.settleSearchJob(db.pg, c8, "exhausted");
    let l = await letter(db.pg);
    expect({ epoch: l.epoch, state: l.state, dead_count: l.dead_count, replay_count: l.replay_count, resolved: l.resolved })
      .toEqual({ epoch: "8", state: "dead", dead_count: 2, replay_count: 1, resolved: false });
    expect(l.message).toEqual(c8.claimedMessage as StageMessage);
    expect(await jobs(db.pg, WK)).toEqual([]);
    // (2) Re-death after resolution: replay, done resolves; a later wake exhausts and refreshes the same letter.
    await f.replaySearchDeadLetter(db.pg, LETTER);
    const c8b = await claimOne(db.pg, f.claimSearchJobs);
    await f.settleSearchJob(db.pg, c8b, "done");
    expect(await letter(db.pg)).toMatchObject({ state: "resolved", resolved: true, replay_count: 2 });
    const snapshot9 = await deathAt(db.pg, f, 9);
    l = await letter(db.pg);
    expect({ epoch: l.epoch, state: l.state, dead_count: l.dead_count, replay_count: l.replay_count, resolved: l.resolved })
      .toEqual({ epoch: "9", state: "dead", dead_count: 3, replay_count: 2, resolved: false });
    expect(l.message).toEqual(snapshot9 as StageMessage);
  }, 120_000);
});

// ---------------------------------------------------------------------------------------------------------
describe("R9: the park path, the held down migration and the failure-safe archive, through parkSearchJobs and archiveSearchStage (design §8 [rev 8])", () => {
  // An org that needs URI encoding: the parked letter ids are dlq.ts's canonical deadLetterId.
  const a = { orgId: "org:r9 enc", workspaceId: "ws_r9", harness: "claude", sessionId: "s_a" };
  const b = { ...a, sessionId: "s_b" };
  const m2r9 = () => m2("enqueueSearchWake", "claimSearchJobs", "settleSearchJob", "replaySearchDeadLetter", "parkSearchJobs",
    "searchStageDownPrecondition", "archiveSearchStage");
  type R9 = Awaited<ReturnType<typeof m2r9>>;

  /** The stage's rows in the three tables the down migration checks (plain SELECTs). */
  async function stageCounts(pg: PGlite) {
    const n = async (t: string) => (await pg.query<{ n: number }>(`SELECT count(*)::int AS n FROM sessions.${t} WHERE stage = 'search-index'`)).rows[0]!.n;
    return { jobs: await n("pipeline_jobs"), events: await n("pipeline_events"), letters: await n("pipeline_dead_letters") };
  }

  /** B died once and was woken again; A is leased by a straggler; another stage's job exists. Then the park. */
  async function parked(db: Db, f: R9) {
    await deadLettersAreSound(db.pg);
    // Fixture soundness: the canonical ids encode the org's ':' and ' '.
    expect(letterIdOf(a)).toBe(`search-index:org%3Ar9%20enc:ws_r9:${searchWorkKey(a)}`);
    await f.enqueueSearchWake(db.pg, wake(b, 3));
    const cb = await claimOne(db.pg, f.claimSearchJobs);
    await db.pg.query("UPDATE sessions.pipeline_jobs SET attempts = 12 WHERE id = $1::bigint AND lease_id = $2::uuid", [String(cb.id), cb.lease]);
    await f.settleSearchJob(db.pg, cb, "exhausted");
    expect((await letters(db.pg)).map((l) => [l.id, l.dead_count])).toEqual([[letterIdOf(b), 1]]);
    await f.enqueueSearchWake(db.pg, wake(b, 4));
    // A is leased (a straggler's claim) at the time of the park.
    await f.enqueueSearchWake(db.pg, wake(a, 7));
    await elapse(db.pg);
    const claimed = await f.claimSearchJobs(db.pg, 1);
    expect(claimed).toHaveLength(1);
    // Another stage's job is never touched.
    await postgresPipelineQueue(db.pg).send(stageMessage("session-indexed", a, "r9-other@1", {}));
    const before = Object.fromEntries([a, b].map((s) => [s.sessionId, null as StageMessage | null]));
    for (const s of [a, b]) { const rows = await jobs(db.pg, searchWorkKey(s)); expect(rows).toHaveLength(1); before[s.sessionId] = rows[0]!.message; }

    await f.parkSearchJobs(db.pg, { operator: "r9-fixture-operator" });

    expect((await db.pg.query<{ stage: string }>("SELECT stage FROM sessions.pipeline_jobs")).rows).toEqual([{ stage: "session-indexed" }]);
    const parkedLetters = await letters(db.pg);
    expect(parkedLetters.map((l) => ({ id: l.id, stage: l.stage, state: l.state, dead_count: l.dead_count, resolved: l.resolved, epoch: l.epoch }))).toEqual([
      { id: letterIdOf(a), stage: STAGE, state: "dead", dead_count: 1, resolved: false, epoch: "7" },
      { id: letterIdOf(b), stage: STAGE, state: "dead", dead_count: 2, resolved: false, epoch: "4" },
    ].sort((x, y) => (x.id < y.id ? -1 : 1)));
    for (const l of parkedLetters) expect(l.message).toEqual(before[(l.message.ctx as { sessionId: string }).sessionId]!);
  }

  /** Every row of a table as jsonb text, sorted: a byte-level snapshot for "nothing changed" (plain SELECT). */
  const snapshot = async (pg: PGlite, table: string) =>
    (await pg.query<{ r: string }>(`SELECT to_jsonb(t)::text AS r FROM sessions.${table} t ORDER BY 1`)).rows.map((x) => x.r);

  /** The consumer is back: replay the scopes' letters, then claim and complete each through the real settler. */
  async function replayAndResolve(db: Db, f: R9, scopes: Scope[]) {
    for (const s of scopes) await f.replaySearchDeadLetter(db.pg, letterIdOf(s));
    for (let n = 0; n < scopes.length; n++) {
      const c = await claimOne(db.pg, f.claimSearchJobs);
      expect(await f.settleSearchJob(db.pg, c, "done")).not.toEqual({ applied: false });
    }
  }

  test("R9a: after a park (every row in its scope letter, state dead, current message), the down precondition does NOT hold: letters and the park's dead_lettered events exist", async () => {
    const db = await migrated();
    const f = await m2r9();
    await parked(db, f);
    const counts = await stageCounts(db.pg);
    // No search-index job is left, but the parked letters are a promise of replay, and settlement and the park wrote
    // the stage's pipeline_events (the park's 'dead_lettered' rows among them).
    expect(counts.jobs).toBe(0);
    expect(counts.letters).toBeGreaterThan(0);
    expect(counts.letters).toBe(2);
    expect(counts.events).toBeGreaterThan(0);
    const deadLettered = (await db.pg.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM sessions.pipeline_events WHERE stage = 'search-index' AND status = 'dead_lettered'")).rows[0]!.n;
    expect(deadLettered).toBeGreaterThan(0);
    expect(await f.searchStageDownPrecondition(db.pg)).toBe(false);
  }, 120_000);

  test("R9b: replay, resolve, then archiveSearchStage: jobs, events and letters are 0; the archive equals the pre-archive rows plus one manifest whose sha256 (and the returned one) equals §8's digest recomputed in-test; the precondition holds; another stage's letter, job and REAL event survive", async () => {
    const db = await migrated();
    const f = await m2r9();
    await parked(db, f);
    await replayAndResolve(db, f, [a, b]);
    expect((await letters(db.pg)).map((l) => ({ id: l.id, state: l.state, resolved: l.resolved }))).toEqual(
      [a, b].map((s) => ({ id: letterIdOf(s), state: "resolved", resolved: true })).sort((x, y) => (x.id < y.id ? -1 : 1)));
    expect(await f.searchStageDownPrecondition(db.pg)).toBe(false);
    // Controls: another stage's letter, job and a REAL pipeline_events row (a valid plain INSERT, proven landed).
    const other = await postgresDeadLetterStore(db.pg).quarantine(stageMessage("session-indexed", a, "r9-other-dl@1", {}), new Date());
    const otherEvent = (await db.pg.query<{ r: string }>(
      `INSERT INTO sessions.pipeline_events (trace_id, stage, status, attempt, idempotency_key, org_id, workspace_id, harness, session_id)
       VALUES ($1::uuid, 'object-landed', 'ok', 1, 'r9-other-event@1', $2, $3, $4, $5) RETURNING to_jsonb(pipeline_events)::text AS r`,
      [crypto.randomUUID(), a.orgId, a.workspaceId, a.harness, a.sessionId])).rows;
    expect(otherEvent).toHaveLength(1);
    const otherEventRow = () => db.pg.query<{ r: string }>(
      "SELECT to_jsonb(e)::text AS r FROM sessions.pipeline_events e WHERE stage = 'object-landed' AND idempotency_key = 'r9-other-event@1'");
    expect((await otherEventRow()).rows).toEqual(otherEvent);

    // The PRE-archive snapshot (plain SELECTs): every search-index letter and event as (kind, source_id, row), with
    // row = to_jsonb(<source row>) as text. §8 [rev 8]: a letter's source_id is its id; an event's is the sha256 hex
    // of to_jsonb(<event row>)::text, computed HERE in TypeScript, independently of any SQL.
    type Src = { kind: "letter" | "event"; source_id: string; row: string };
    const letterSnap: Src[] = (await db.pg.query<{ id: string; r: string }>(
      "SELECT id, to_jsonb(d)::text AS r FROM sessions.pipeline_dead_letters d WHERE stage = 'search-index'")).rows
      .map((x) => ({ kind: "letter", source_id: x.id, row: x.r }));
    const eventSnap: Src[] = (await db.pg.query<{ r: string }>(
      "SELECT to_jsonb(e)::text AS r FROM sessions.pipeline_events e WHERE stage = 'search-index'")).rows
      .map((x) => ({ kind: "event", source_id: createHash("sha256").update(x.r, "utf8").digest("hex"), row: x.r }));
    expect(letterSnap.map((l) => l.source_id).sort()).toEqual([letterIdOf(a), letterIdOf(b)].sort());
    expect(eventSnap.length).toBeGreaterThan(0);
    expect(new Set(eventSnap.map((e) => e.source_id)).size).toBe(eventSnap.length);
    const snap = [...letterSnap, ...eventSnap];
    // The expected digest, recomputed from the SNAPSHOT (a jsonb parameter, not the live tables) with §8's exact rule:
    //   encode(sha256(convert_to(string_agg(jsonb_build_array(kind, source_id, row)::text, E'\n' ORDER BY kind, source_id), 'UTF8')), 'hex')
    // This is observation in SQL (jsonb's text form and the database collation are the rule's), not the archive.
    const expectedSha = (await db.pg.query<{ sha256: string }>(
      `SELECT encode(sha256(convert_to(string_agg(jsonb_build_array(x->>'kind', x->>'source_id', (x->>'row')::jsonb)::text, E'\\n'
                ORDER BY x->>'kind', x->>'source_id'), 'UTF8')), 'hex') AS sha256
         FROM jsonb_array_elements($1::jsonb) AS x`, [JSON.stringify(snap)])).rows[0]!.sha256;
    expect(expectedSha).toMatch(/^[a-f0-9]{64}$/);
    const otherEventsBefore = (await snapshot(db.pg, "pipeline_events")).filter((r) => (JSON.parse(r) as { stage: string }).stage !== STAGE);
    expect(otherEventsBefore).toContain(otherEvent[0]!.r);

    const archived = await f.archiveSearchStage(db.pg);

    expect(archived).toEqual({ letters: 2, events: eventSnap.length, sha256: expectedSha });
    expect(await stageCounts(db.pg)).toEqual({ jobs: 0, events: 0, letters: 0 });

    // The durable archive: one batch; its letter and event rows EQUAL the snapshot (kind, source_id, row); plus exactly
    // one 'manifest' row carrying {letters, events, sha256} with the recomputed digest.
    const arch = (await db.pg.query<{ batch_id: string; kind: string; source_id: string; row: string; stamped: boolean }>(
      "SELECT batch_id::text, kind, source_id::text, row::text, archived_at IS NOT NULL AS stamped FROM sessions.search_stage_archive")).rows;
    expect(new Set(arch.map((r) => r.batch_id)).size).toBe(1);
    expect(arch.every((r) => r.stamped)).toBe(true);
    const key = (r: { kind: string; source_id: string }) => `${r.kind}\u0000${r.source_id}`;
    const byKey = <T extends { kind: string; source_id: string }>(rows: T[]) => [...rows].sort((x, y) => (key(x) < key(y) ? -1 : 1));
    expect(byKey(arch.filter((r) => r.kind !== "manifest").map(({ kind, source_id, row }) => ({ kind, source_id, row })))).toEqual(byKey(snap));
    const manifests = arch.filter((r) => r.kind === "manifest");
    expect(manifests).toHaveLength(1);
    expect(manifests[0]!.source_id).toBe("manifest");
    expect(JSON.parse(manifests[0]!.row)).toMatchObject({ letters: 2, events: eventSnap.length, sha256: expectedSha });

    expect(await f.searchStageDownPrecondition(db.pg)).toBe(true);
    expect((await letters(db.pg)).map((l) => l.id)).toEqual([other.id]);
    expect((await db.pg.query<{ stage: string }>("SELECT stage FROM sessions.pipeline_jobs")).rows).toEqual([{ stage: "session-indexed" }]);
    expect((await otherEventRow()).rows).toEqual(otherEvent);
    expect((await snapshot(db.pg, "pipeline_events")).filter((r) => (JSON.parse(r) as { stage: string }).stage !== STAGE)).toEqual(otherEventsBefore);
  }, 120_000);

  test("R9c: archiveSearchStage with one search-index letter still 'dead' THROWS and changes nothing (failure-safe)", async () => {
    const db = await migrated();
    const f = await m2r9();
    await parked(db, f);
    // Only A is replayed and resolved; B's letter stays 'dead'. No search-index job remains.
    await replayAndResolve(db, f, [a]);
    expect((await letters(db.pg)).map((l) => ({ id: l.id, state: l.state }))).toEqual(
      [{ id: letterIdOf(a), state: "resolved" }, { id: letterIdOf(b), state: "dead" }].sort((x, y) => (x.id < y.id ? -1 : 1)));
    expect((await stageCounts(db.pg)).jobs).toBe(0);
    const before = {
      jobs: await snapshot(db.pg, "pipeline_jobs"),
      letters: await snapshot(db.pg, "pipeline_dead_letters"),
      events: await snapshot(db.pg, "pipeline_events"),
      archive: await snapshot(db.pg, "search_stage_archive"),
    };
    expect(before.events.length).toBeGreaterThan(0);

    // Contract this file fixes: the refusal is a throw (the transaction raises and rolls back).
    await expect(f.archiveSearchStage(db.pg)).rejects.toThrow();

    expect({
      jobs: await snapshot(db.pg, "pipeline_jobs"),
      letters: await snapshot(db.pg, "pipeline_dead_letters"),
      events: await snapshot(db.pg, "pipeline_events"),
      archive: await snapshot(db.pg, "search_stage_archive"),
    }).toEqual(before);
    expect(await f.searchStageDownPrecondition(db.pg)).toBe(false);
  }, 120_000);

  test("R9d: archiveSearchStage with a search-index job row present (every letter resolved) THROWS and changes nothing", async () => {
    const db = await migrated();
    const f = await m2r9();
    await parked(db, f);
    await replayAndResolve(db, f, [a, b]);
    // A new wake after the replay: the only thing that should block the archive is this job row.
    await f.enqueueSearchWake(db.pg, wake(a, 9));
    expect((await letters(db.pg)).filter((l) => l.stage === STAGE).every((l) => l.state === "resolved")).toBe(true);
    expect((await stageCounts(db.pg)).jobs).toBe(1);
    const before = {
      jobs: await snapshot(db.pg, "pipeline_jobs"),
      letters: await snapshot(db.pg, "pipeline_dead_letters"),
      events: await snapshot(db.pg, "pipeline_events"),
      archive: await snapshot(db.pg, "search_stage_archive"),
    };
    expect(before.events.length).toBeGreaterThan(0);

    await expect(f.archiveSearchStage(db.pg)).rejects.toThrow();

    expect({
      jobs: await snapshot(db.pg, "pipeline_jobs"),
      letters: await snapshot(db.pg, "pipeline_dead_letters"),
      events: await snapshot(db.pg, "pipeline_events"),
      archive: await snapshot(db.pg, "search_stage_archive"),
    }).toEqual(before);
    expect(await f.searchStageDownPrecondition(db.pg)).toBe(false);
  }, 120_000);

  test("R9e: after a successful archive, an UPDATE and a DELETE on sessions.search_stage_archive by a non-owner runtime role are refused with 42501 (append-only trigger)", async () => {
    const db = await migrated();
    const f = await m2r9();
    await parked(db, f);
    await replayAndResolve(db, f, [a, b]);
    await f.archiveSearchStage(db.pg);
    const archive = () => snapshot(db.pg, "search_stage_archive");
    const before = await archive();
    expect(before.length).toBeGreaterThan(2);

    // Catalog: a user (non-internal), enabled trigger on the archive that fires on UPDATE (tgtype bit 16) and on
    // DELETE (bit 8), in one trigger or across several.
    const triggers = (await db.pg.query<{ tgname: string; tgtype: number }>(
      `SELECT tgname, tgtype FROM pg_trigger WHERE tgrelid = 'sessions.search_stage_archive'::regclass AND NOT tgisinternal AND tgenabled <> 'D'`)).rows;
    expect(triggers.some((t) => (t.tgtype & 16) !== 0)).toBe(true);
    expect(triggers.some((t) => (t.tgtype & 8) !== 0)).toBe(true);

    // The runtime role. The migrated template's configured runtime role is `postgres` (replay-fixture.ts:
    // runtimeRole "postgres"), which is the superuser schema owner, the one role §7 exempts. So a non-owner,
    // non-superuser stand-in plays the runtime role. It is deliberately granted UPDATE and DELETE beyond §7's
    // SELECT, INSERT, so a refusal can only come from the append-only trigger, never from a missing privilege.
    const role = "r9_runtime_standin";
    await db.pg.exec(`CREATE ROLE ${role} NOLOGIN NOSUPERUSER NOBYPASSRLS;
      GRANT USAGE ON SCHEMA sessions TO ${role};
      GRANT SELECT, INSERT, UPDATE, DELETE ON sessions.search_stage_archive TO ${role};`);
    const asRuntime = async (sql: string): Promise<string> => {
      await db.pg.exec(`SET ROLE ${role}`);
      try { await db.pg.query(sql); return "ok"; } catch (e) { return String((e as { code?: string }).code ?? e); } finally { await db.pg.exec("RESET ROLE"); }
    };
    // The role sees the archived rows, so a row-level trigger has rows to fire on.
    await db.pg.exec(`SET ROLE ${role}`);
    try {
      expect((await db.pg.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.search_stage_archive")).rows[0]!.n).toBe(before.length);
    } finally { await db.pg.exec("RESET ROLE"); }

    expect(await asRuntime("UPDATE sessions.search_stage_archive SET row = row || '{\"tampered\": true}'::jsonb WHERE kind = 'manifest'")).toBe("42501");
    expect(await asRuntime("DELETE FROM sessions.search_stage_archive WHERE kind = 'letter'")).toBe("42501");
    expect(await archive()).toEqual(before);
  }, 120_000);

  // R9g: each down guard on its own. ORDERING: searchStageDownPrecondition is loaded FIRST, before any seeding. Today
  // the stage CHECKs of all three tables refuse 'search-index', so the seed INSERT itself would fail; loading the
  // module first keeps the red reason "module absent", never "fixture failed". Each case seeds its row with a plain
  // INSERT (fixture), proves it landed, and runs on its own fresh clone. Another stage's job, letter and event are
  // seeded in every case, so a precondition that ignores the stage filter also fails.
  describe("R9g: searchStageDownPrecondition, each guard on its own (design §8 [rev 8])", () => {
    const g = { orgId: "org:r9g enc", workspaceId: "ws_r9g", harness: "claude", sessionId: "s_g" };
    const seed = {
      job: async (pg: PGlite, stage: string) => (await pg.query(
        `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, message)
         VALUES ($1, $2, $3::uuid, $4, $5, $6, $7::jsonb) RETURNING id`,
        [stage, stage === STAGE ? searchWorkKey(g) : "r9g-other@1", crypto.randomUUID(), g.orgId, g.workspaceId, g.sessionId,
          JSON.stringify(stage === STAGE ? wake(g, 1) : stageMessage(stage, g, "r9g-other@1", {}))])).rows.length,
      letter: async (pg: PGlite, stage: string) => (await pg.query(
        `INSERT INTO sessions.pipeline_dead_letters (id, stage, trace_id, org_id, workspace_id, session_id, message)
         VALUES ($1, $2, $3::uuid, $4, $5, $6, $7::jsonb) RETURNING id`,
        [stage === STAGE ? letterIdOf(g) : `${stage}:r9g-other@1`, stage, crypto.randomUUID(), g.orgId, g.workspaceId, g.sessionId,
          JSON.stringify(stage === STAGE ? wake(g, 1) : stageMessage(stage, g, "r9g-other@1", {}))])).rows.length,
      event: async (pg: PGlite, stage: string) => (await pg.query(
        `INSERT INTO sessions.pipeline_events (trace_id, stage, status, attempt, idempotency_key, org_id, workspace_id, harness, session_id)
         VALUES ($1::uuid, $2, 'dead_lettered', 1, $3, $4, $5, $6, $7) RETURNING stage`,
        [crypto.randomUUID(), stage, `r9g:${stage}@1`, g.orgId, g.workspaceId, g.harness, g.sessionId])).rows.length,
    };
    const cases = [
      { name: "only a search-index job row", seeded: "job" as const, expected: false },
      { name: "only a search-index letter", seeded: "letter" as const, expected: false },
      { name: "only a search-index event", seeded: "event" as const, expected: false },
      { name: "all three zero (other stages only)", seeded: null, expected: true },
    ];
    for (const c of cases) {
      test(`R9g: ${c.name} gives ${c.expected}`, async () => {
        const db = await migrated();
        const { searchStageDownPrecondition } = await m2("searchStageDownPrecondition");
        // Another stage's rows, every table (fixture, proven landed).
        expect(await seed.job(db.pg, "session-indexed")).toBe(1);
        expect(await seed.letter(db.pg, "session-indexed")).toBe(1);
        expect(await seed.event(db.pg, "object-landed")).toBe(1);
        if (c.seeded) expect(await seed[c.seeded](db.pg, STAGE)).toBe(1);
        const counts = await stageCounts(db.pg);
        expect(counts).toEqual({
          jobs: c.seeded === "job" ? 1 : 0,
          letters: c.seeded === "letter" ? 1 : 0,
          events: c.seeded === "event" ? 1 : 0,
        });
        expect(await searchStageDownPrecondition(db.pg)).toBe(c.expected);
      }, 120_000);
    }
  });
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
    const { runSearchJob } = await m2("runSearchJob");
    const out = await runSearchJob(wake({ ...SCOPE, sessionId: x.canonicalId }, rows[0]!.epoch, publishClock(rows[0]!.published_at)), { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(out.commitToSearchableMs).toBeGreaterThanOrEqual(120_000);
    expect(out.commitToSearchableMs).toBeLessThan(180_000);
  }, 120_000);

  test("R5d: a non-managed (pointer-only) scope's catch-up job carries clock.source='pointer', at = max(indexed_at); its pass emits no commit_to_searchable_ms", async () => {
    const r = await hostedRig(x, cleanup);
    expect((await r.tick({})).batchItemFailures).toEqual([]);
    const canonical = { ...SCOPE, sessionId: x.canonicalId };
    // The rig's own consumer pass indexed the scope; a newer committed change makes it stale again, and its wake was lost.
    await r.db.pg.query("UPDATE sessions.session_directory SET event_projection_ready = false, event_projection_epoch = event_projection_epoch + 1 WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, x.canonicalId]);
    // Fixture soundness: no publication attempt exists (non-managed); the scope is committed and stale.
    expect((await r.db.pg.query("SELECT 1 FROM sessions.ingest_publication_attempts")).rows).toHaveLength(0);
    const dir = await directoryOf(r.db.pg, canonical);
    expect(dir.map((d) => d.ready === true)).toEqual([false]);
    // Oracle (observation): the §2 pointer clock.
    const at = (await r.db.pg.query<{ at: string | null }>(
      "SELECT max(indexed_at) AS at FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND coalesce(canonical_session_id, session_id)=$4 AND deleted_at IS NULL",
      [canonical.orgId, canonical.workspaceId, canonical.harness, canonical.sessionId])).rows[0]!.at;
    expect(Number.isFinite(new Date(at ?? "").getTime())).toBe(true);
    expect(await allSearchJobs(r.db.pg)).toEqual([]);
    const { catchUpTick, runSearchJob } = await m2("catchUpTick", "runSearchJob");
    await catchUpTick({ sql: r.db.pg, pageSize: 500 });
    const found = await allSearchJobs(r.db.pg);
    expect(found).toHaveLength(1);
    expect(found[0]!.epoch).toBe(dir[0]!.epoch);
    expect(found[0]!.message.payload.clock.source).toBe("pointer");
    expect(Date.parse(found[0]!.message.payload.clock.at)).toBe(new Date(at!).getTime());
    const out = await runSearchJob(found[0]!.message, { search: r.search() });
    expect(out.outcome).toBe("indexed");
    expect(out.commitToSearchableMs).toBeUndefined();
    expect(out.pointerToSearchableMs).toBeGreaterThanOrEqual(0);
  }, 120_000);
});
