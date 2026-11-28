/**
 * SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED=true on the hosted path (search-index-hosted-harness.ts: processBatch,
 * the legacy ClickHouse binding, a migrated PGlite with the session directory and its triggers): a pass that is
 * not skipped runs exactly as before and reports what the skip-current check answers, and why not. Nothing is
 * skipped by the shadow flag. Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { alreadyCurrent, indexSearchScope, searchIndexPhaseMetrics, skipCheck, skipCurrentProbe, skipProbeFactsError, type SearchIndexDeps, type SearchIndexResult } from "../search-index.ts";
import { runSearchJob, searchWorkKey, type SearchWakeMessage } from "../search-jobs.ts";
import { sanitizeLogField } from "../../shared/log.ts";
import { lambdaEnv, runtimeFromSecret, searchIndexSkipCurrentShadowFromEnv } from "../lambda.ts";
import { SCOPE, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
/** A scope too large to read whole: every pass streams it. */
const LARGE: Partial<SearchIndexDeps> = { limits: TINY, stream: UNLIMITED };
const SHADOW: Partial<SearchIndexDeps> = { skipCurrentShadow: true };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

let x: ExportedScope;
/** The same records cut into one-record chunks (several hundred objects): the large-manifest fixture. */
let big: ExportedScope;
beforeAll(async () => {
  x = await exportStreamScope(generate(31), 1200);
  big = await exportStreamScope(generate(31), 300);
}, 120_000);
afterAll(async () => {
  await x?.dispose();
  await big?.dispose();
});

/** A rig whose scope a complete pass (no flag) already indexed. */
async function indexed(scope: ExportedScope = x, extra: Partial<SearchIndexDeps> = {}) {
  const r = await hostedRig(scope, cleanup);
  await r.tick({});
  expect((await indexSearchScope(SCOPE, r.search(extra))).complete).toBe(true);
  return r;
}
type Rig = Awaited<ReturnType<typeof indexed>>;
async function pass(r: Rig, extra: Partial<SearchIndexDeps> = SHADOW) {
  const reads = r.reads.count;
  const sql = r.sql.length;
  r.fake.calls.length = 0;
  const result = await indexSearchScope(SCOPE, r.search(extra));
  return { result, reads: r.reads.count - reads, clickhouse: r.fake.calls.length, sql: r.sql.slice(sql).map((s) => String(s.sql)) };
}
const PROBE_SQL = "AS sources_equal";
/** One consumer tick, with the JSON log lines it wrote. */
async function tickLogged(r: Rig, extra: Partial<SearchIndexDeps>) {
  const lines: string[] = [];
  const stdout = process.stdout.write;
  const stderr = process.stderr.write;
  const capture = (chunk: unknown) => {
    lines.push(String(chunk));
    return true;
  };
  process.stdout.write = capture as typeof process.stdout.write;
  process.stderr.write = capture as typeof process.stderr.write;
  let response;
  try {
    response = await r.tick(extra);
  } finally {
    process.stdout.write = stdout;
    process.stderr.write = stderr;
  }
  const named = (message: string) =>
    lines.flatMap((l) => l.split("\n")).filter((l) => l.includes(message)).map((l) => JSON.parse(l) as Record<string, unknown>);
  return { response, named };
}
/** Both answers for the scope's current state, in one read-only transaction. */
const verdicts = (r: Rig, x: ExportedScope) =>
  r.transact(async (tx) => ({
    current: await alreadyCurrent(tx, SCOPE, { canonicalId: x.canonicalId }, x.keys.length),
    probe: await skipCurrentProbe(tx, SCOPE, { canonicalId: x.canonicalId }, x.keys.length),
  }));

describe("skip-current shadow off", () => {
  test("no probe query and no field, with or without skipCurrent", async () => {
    const r = await indexed();
    const off = await pass(r, {});
    expect("skipProbe" in off.result).toBe(false);
    expect(off.sql.some((s) => s.includes(PROBE_SQL))).toBe(false);
    const skipped = await pass(r, { skipCurrent: true });
    expect(skipped.result.skipped).toBe(true);
    expect(skipped.sql.some((s) => s.includes(PROBE_SQL))).toBe(false);
  }, 120_000);
});

describe("skip-current shadow on (hosted ClickHouse binding)", () => {
  test("an unchanged scope: the pass still runs in full, changes nothing, and is reported as would-skip", async () => {
    const r = await indexed();
    const before = r.fake.dump();
    const off = await pass(r, {});
    const on = await pass(r);
    // Nothing skipped: the same reads and ClickHouse calls as the flag-off pass, plus one read-only statement.
    expect(on.result).toEqual({ ...off.result, skipProbe: { current: true } });
    expect([on.result.inserted, on.result.deleted, on.result.complete]).toEqual([0, 0, true]);
    expect([on.reads, on.clickhouse]).toEqual([off.reads, off.clickhouse]);
    expect(on.reads).toBeGreaterThan(0);
    // The probe's statements, in order, and nothing else new: its savepoint, one SELECT, the release.
    const added = on.sql.filter((s) => s.includes(PROBE_SQL) || s.includes("search_skip_probe"));
    expect(added.map((s) => (s.includes(PROBE_SQL) ? "probe" : s))).toEqual(["SAVEPOINT search_skip_probe", "probe", "RELEASE SAVEPOINT search_skip_probe"]);
    expect(on.sql.length).toBe(off.sql.length + 3);
    expect(added.some((s) => /\b(UPDATE|INSERT|DELETE)\b/i.test(s))).toBe(false);
    expect(r.fake.dump()).toEqual(before);
  }, 120_000);

  test("a large manifest set that streams, unchanged: a full no-op re-read, reported as would-skip", async () => {
    const r = await indexed(big, LARGE);
    expect(big.keys.length).toBeGreaterThan(0);
    const before = r.fake.dump();
    const on = await pass(r, { ...SHADOW, ...LARGE });
    expect(on.result.stream?.passes).toBeGreaterThan(0);
    expect(on.result).toMatchObject({ inserted: 0, deleted: 0, complete: true, skipProbe: { current: true } });
    // Every chunk object is read again to change nothing.
    expect(on.reads).toBeGreaterThan(200);
    expect(r.fake.dump()).toEqual(before);
    // The real flag then skips exactly this pass: no object read, no ClickHouse call.
    const skipped = await pass(r, { skipCurrent: true, ...LARGE });
    expect(skipped.result.skipped).toBe(true);
    expect([skipped.reads, skipped.clickhouse]).toEqual([0, 0]);
  }, 120_000);

  test("a changed manifest (new revision) is never would-skip: sources_changed", async () => {
    for (const [scope, extra] of [[x, {}], [big, LARGE]] as const) {
      const r = await indexed(scope, extra);
      await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [scope.keys[0]]);
      const on = await pass(r, { ...SHADOW, ...extra });
      expect(on.result.skipProbe).toEqual({ current: false, miss: "sources_changed" });
      expect(on.reads).toBeGreaterThan(0);
      // With the real flag on too, the changed scope still runs, and says why it was not skipped.
      const both = await pass(r, { ...SHADOW, ...extra, skipCurrent: true });
      expect(both.result.skipped).toBeUndefined();
      expect(both.result.skipProbe).toEqual({ current: false, miss: "sources_changed" });
    }
  }, 120_000);

  test("both flags on: a current scope is skipped as before, with no probe", async () => {
    const r = await indexed();
    const p = await pass(r, { ...SHADOW, skipCurrent: true });
    expect(p.result).toEqual({ manifests: x.keys.length, rows: 0, inserted: 0, deleted: 0, complete: true, skipped: true, reasons: ["already_current"] });
    expect([p.reads, p.clickhouse]).toEqual([0, 0]);
    expect(p.sql.some((s) => s.includes(PROBE_SQL))).toBe(false);
  }, 120_000);

  test("an incomplete previous pass over the same sources: not_ready, then would-skip after a complete one", async () => {
    const r = await indexed();
    expect(await indexSearchScope(SCOPE, r.search({ limits: TINY }))).toMatchObject({ complete: false, reasons: ["scope_too_large"] });
    expect((await pass(r)).result.skipProbe).toEqual({ current: false, miss: "not_ready" });
    expect((await pass(r)).result.skipProbe).toEqual({ current: true });
    // An incomplete pass that reached the check carries its probe too.
    expect((await pass(r, { ...SHADOW, limits: TINY })).result).toMatchObject({ complete: false, skipProbe: { current: true } });
  }, 120_000);

  test("the probe answers as alreadyCurrent does, in every state, and names the first failed condition", async () => {
    const r = await indexed();
    const agree = async (miss?: string) => {
      const v = await verdicts(r, x);
      expect(v.probe.current).toBe(v.current);
      expect(v.probe).toEqual(miss ? { current: false, miss } : { current: true });
    };
    await agree();
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    await agree("not_ready");
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_version = 0");
    await agree("projection_version");
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    await agree();
    await r.db.pg.exec(`UPDATE sessions.session_directory SET event_projection_sources =
      jsonb_set(event_projection_sources, ARRAY[(SELECT min(k) FROM jsonb_object_keys(event_projection_sources) k)], '"999"')
      WHERE event_projection_ready`);
    await agree("sources_changed");
    expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
    await agree();
    if (x.keys.length > 1) {
      await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE manifest_key = $1", [x.keys[1]]);
      await agree("sources_changed");
    }
    await r.db.pg.exec("DELETE FROM sessions.session_directory");
    await agree("no_directory");
    // No live pointer is never current, whatever the directory says.
    expect(await r.transact((tx) => skipCurrentProbe(tx, SCOPE, { canonicalId: x.canonicalId }, 0))).toEqual({ current: false, miss: "sources_changed" });
  }, 120_000);

  test("output facts on: an incomplete, missing or unreadable facts state is facts_incomplete", async () => {
    const r = await indexed();
    const factsReads = () => r.fake.calls.filter((c) => String(c.query).includes("output_facts_session_state")).length;
    expect((await pass(r, { ...SHADOW, outputFacts: true })).result.skipProbe).toEqual({ current: false, miss: "facts_incomplete", factsRead: true });
    r.fake.facts.complete = 0;
    const incomplete = searchIndexPhaseMetrics();
    expect((await pass(r, { ...SHADOW, outputFacts: true, metrics: incomplete })).result.skipProbe).toEqual({ current: false, miss: "facts_incomplete", factsRead: true });
    // A state that was read and is not complete is no error.
    expect("skipProbeFactsError" in incomplete).toBe(false);
    expect(incomplete.skipProbeFactsMs).toBeGreaterThanOrEqual(0);
    r.fake.facts.complete = 1;
    // Exactly one facts state read per probed pass, timed on its own, and counted in the probe's time.
    const metrics = searchIndexPhaseMetrics();
    const current = await pass(r, { ...SHADOW, outputFacts: true, metrics });
    expect(current.result.skipProbe).toEqual({ current: true, factsRead: true });
    expect(factsReads()).toBe(1);
    expect(metrics.skipProbeFactsMs).toBeGreaterThanOrEqual(0);
    expect(metrics.skipProbeMs!).toBeGreaterThanOrEqual(metrics.skipProbeFactsMs!);
    // Postgres already says not current: the facts state is not read at all.
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    const cold = searchIndexPhaseMetrics();
    const notReady = await pass(r, { ...SHADOW, outputFacts: true, metrics: cold });
    expect(notReady.result.skipProbe).toEqual({ current: false, miss: "not_ready" });
    expect(factsReads()).toBe(0);
    expect("skipProbeFactsMs" in cold).toBe(false);
    // Without output facts there is never a facts read.
    const plain = searchIndexPhaseMetrics();
    expect((await pass(r, { ...SHADOW, metrics: plain })).result.skipProbe).toEqual({ current: true });
    expect("skipProbeFactsMs" in plain).toBe(false);
  }, 120_000);

  test("output facts on: a facts read that fails or times out is facts_unreadable with its class and duration, never facts_incomplete", async () => {
    const r = await indexed();
    r.fake.facts.complete = 1;
    /** The rig's ClickHouse with the facts state read replaced: slow, then rejecting as `error`. */
    const failing = (error: Error, afterMs: number) => ({
      ...r.fake.ch,
      select: (async (query: string, options?: unknown) => {
        if (!query.includes("output_facts_session_state")) return r.fake.ch.select(query, options as never);
        r.fake.calls.push({ ch: "select", query });
        await new Promise((done) => setTimeout(done, afterMs));
        throw error;
      }) as typeof r.fake.ch.select,
    });
    const timeout = Object.assign(new Error("synthetic"), { name: "TimeoutError" });
    const refused = Object.assign(new Error("synthetic"), { name: "ClickHouseError", code: "60" });
    for (const [error, cls] of [[timeout, "timeout"], [refused, "clickhouse_error"], [new Error("synthetic"), "error"]] as const) {
      const metrics = searchIndexPhaseMetrics();
      const off = await pass(r, { outputFacts: true });
      const p = await pass(r, { ...SHADOW, outputFacts: true, metrics, ch: failing(error, 40) });
      // The pass itself is untouched: a ClickHouse failure in the probe is not a Postgres error, no rollback.
      expect(p.result).toEqual({ ...off.result, skipProbe: { current: false, miss: "facts_unreadable", factsRead: true } });
      expect(p.sql.filter((s) => s.includes("search_skip_probe"))).toEqual(["SAVEPOINT search_skip_probe", "RELEASE SAVEPOINT search_skip_probe"]);
      expect(metrics.skipProbeFactsError).toBe(cls);
      expect(metrics.skipProbeFactsMs!).toBeGreaterThanOrEqual(35);
      expect(metrics.skipProbeMs!).toBeGreaterThanOrEqual(metrics.skipProbeFactsMs!);
      // A facts failure is not a probe (Postgres) failure.
      expect("skipProbeError" in metrics).toBe(false);
    }
    // The real skip's contract is unchanged: an unreadable facts state is not proof, the pass runs, nothing throws.
    const real = await pass(r, { skipCurrent: true, outputFacts: true, ch: failing(timeout, 0) });
    expect(real.result.skipped).toBeUndefined();
    expect(real.result.complete).toBe(true);
    // And with a readable, complete state it skips.
    expect((await pass(r, { skipCurrent: true, outputFacts: true })).result.skipped).toBe(true);

    // Through the consumer: the class and the duration are on the pass's line.
    const t = await tickLogged(r, { ...SHADOW, outputFacts: true, ch: failing(timeout, 40) });
    expect(t.response.batchItemFailures).toEqual([]);
    const line = t.named("search index reconciled")[0]!;
    expect(line).toMatchObject({ would_skip: false, skip_miss: "facts_unreadable", skip_probe_facts_error: "timeout" });
    expect(Number(line.skip_probe_facts_ms)).toBeGreaterThanOrEqual(35);
    expect("skip_probe_error" in line).toBe(false);
  }, 120_000);

  test("error classification is bounded: three facts classes, a closed miss set, a short code or unknown", async () => {
    expect([{ name: "TimeoutError" }, { name: "AbortError" }, { name: "ClickHouseError" }, { name: "TypeError" }, null, "x"].map(skipProbeFactsError))
      .toEqual(["timeout", "timeout", "clickhouse_error", "error", "error", "error"]);
    // The logger drops anything outside the closed sets.
    expect(sanitizeLogField("skip_probe_facts_error", "timeout")).toBe("timeout");
    expect(sanitizeLogField("skip_probe_facts_error", "connect ECONNREFUSED 10.0.0.1")).toBeUndefined();
    expect(sanitizeLogField("skip_miss", "facts_unreadable")).toBe("facts_unreadable");
    expect(sanitizeLogField("skip_miss", "anything else")).toBeUndefined();
    // A probe error whose code is not a short identifier is logged as unknown, never as its text.
    const r = await indexed();
    const wordy = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) =>
          sql.includes(PROBE_SQL) ? (Promise.reject(Object.assign(new Error("synthetic"), { code: "relation \"x\" does not exist at 10.0.0.1" })) as never) : tx.query(sql, params),
      }));
    const metrics = searchIndexPhaseMetrics();
    expect((await pass(r, { ...SHADOW, metrics, transact: wordy })).result.skipProbe).toEqual({ current: false, miss: "probe_failed" });
    expect(metrics.skipProbeError).toBe("unknown");
  }, 120_000);

  test("the managed search-jobs pass never probes, whatever the shared search deps say (hosted-entry control)", async () => {
    const r = await indexed();
    // Not current, so the job does the work (a current scope answers `unchanged` before any pass).
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    const wake: SearchWakeMessage = {
      v: 1, stage: "search-index", ctx: { traceId: crypto.randomUUID(), ...SCOPE, sessionId: x.canonicalId }, workKey: searchWorkKey({ ...SCOPE, sessionId: x.canonicalId }), revision: 1,
      payload: { epoch: 0, clock: { source: "pointer", at: new Date().toISOString() } },
    };
    const metrics = searchIndexPhaseMetrics();
    const sql = r.sql.length;
    const job = await runSearchJob(wake, { search: r.search({ ...SHADOW, metrics }) });
    expect(job.search?.complete).toBe(true);
    expect("skipProbe" in job.search!).toBe(false);
    const sent = r.sql.slice(sql).map((s) => String(s.sql));
    expect(sent.some((s) => s.includes(PROBE_SQL) || s.includes("search_skip_probe"))).toBe(false);
    expect(Object.keys(metrics).filter((k) => k.startsWith("skipProbe"))).toEqual([]);
    // The same deps on the S3-event path do probe.
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    expect((await pass(r, { ...SHADOW, metrics })).result.skipProbe).toEqual({ current: false, miss: "not_ready" });
    expect(metrics.skipProbeMs).toBeGreaterThanOrEqual(0);
  }, 120_000);

  test("the probe is timed on its own, outside locked_apply_ms; flag off sets no probe metric", async () => {
    const r = await indexed();
    // A probe statement that takes 60 ms: all of it lands in skipProbeMs, none in lockedApplyMs' share.
    const slow = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: async (sql: string, params?: unknown[]) => {
          if (sql.includes(PROBE_SQL)) await new Promise((done) => setTimeout(done, 60));
          return tx.query(sql, params);
        },
      }));
    const on = searchIndexPhaseMetrics();
    const started = Date.now();
    expect((await pass(r, { ...SHADOW, metrics: on, transact: slow })).result.skipProbe).toEqual({ current: true });
    const wall = Date.now() - started;
    expect(on.skipProbeMs!).toBeGreaterThanOrEqual(55);
    expect("skipProbeError" in on).toBe(false);
    expect(on.skipProbeMs! + on.lockedApplyMs + on.sourceReadMs + on.indexScanMs).toBeLessThanOrEqual(wall);
    const off = searchIndexPhaseMetrics();
    await pass(r, { metrics: off });
    expect(Object.keys(off).sort()).toEqual(["indexScanMs", "lockedApplyMs", "sourceReadMs", "writeBatches", "writeCalls"]);
    // Both flags on and the scope current: skipped before the probe, so no probe metric either.
    const skipped = searchIndexPhaseMetrics();
    expect((await pass(r, { ...SHADOW, skipCurrent: true, metrics: skipped })).result.skipped).toBe(true);
    expect("skipProbeMs" in skipped).toBe(false);
  }, 120_000);

  test("a probe statement that fails in Postgres is rolled back to its savepoint: the pass completes, attributed", async () => {
    const r = await indexed();
    const before = r.fake.dump();
    // A real server-side error inside the pass's transaction: without the savepoint every later statement would fail.
    const broken = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) =>
          tx.query(sql.includes(PROBE_SQL) ? sql.replace("FROM sessions.session_directory d", "FROM sessions.no_such_probe_table d") : sql, params),
      }));
    const metrics = searchIndexPhaseMetrics();
    const off = await pass(r, {});
    const p = await pass(r, { ...SHADOW, metrics, transact: broken });
    expect(p.result).toEqual({ ...off.result, skipProbe: { current: false, miss: "probe_failed" } });
    expect(p.result.complete).toBe(true);
    expect([p.reads, p.clickhouse]).toEqual([off.reads, off.clickhouse]);
    expect(p.sql.filter((s) => s.includes("search_skip_probe"))).toEqual(["SAVEPOINT search_skip_probe", "ROLLBACK TO SAVEPOINT search_skip_probe", "RELEASE SAVEPOINT search_skip_probe"]);
    expect(metrics.skipProbeError).toBe("42P01");
    expect(metrics.skipProbeMs).toBeGreaterThanOrEqual(0);
    expect(r.fake.dump()).toEqual(before);
    // The directory is still ready: the failed probe left nothing behind, and the next probe answers.
    expect((await pass(r)).result.skipProbe).toEqual({ current: true });
  }, 120_000);

  test("through the consumer: a failed probe is logged on the pass's own line and fails no message", async () => {
    const r = await indexed();
    const broken = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) =>
          tx.query(sql.includes(PROBE_SQL) ? sql.replace("FROM sessions.session_directory d", "FROM sessions.no_such_probe_table d") : sql, params),
      }));
    const t = await tickLogged(r, { ...SHADOW, transact: broken });
    expect(t.response.batchItemFailures).toEqual([]);
    expect(r.requeued).toEqual([]);
    const line = t.named("search index reconciled");
    expect(line.length).toBe(1);
    expect(line[0]).toMatchObject({ ok: true, would_skip: false, skip_miss: "probe_failed", skip_probe_error: "42P01" });
    expect(typeof line[0]!.skip_probe_ms).toBe("number");
    expect(t.named("search index failed")).toEqual([]);
  }, 120_000);

  test("through the consumer: when the savepoint cannot be rolled back the pass fails, and its failed line names the probe", async () => {
    const r = await indexed();
    const lost = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) => {
          if (sql.includes(PROBE_SQL)) return Promise.reject(Object.assign(new Error("synthetic"), { code: "08006" })) as never;
          if (sql.startsWith("ROLLBACK TO SAVEPOINT")) return Promise.reject(Object.assign(new Error("synthetic"), { code: "08003" })) as never;
          return tx.query(sql, params);
        },
      }));
    const t = await tickLogged(r, { ...SHADOW, transact: lost });
    expect(t.response.batchItemFailures.length).toBe(r.messages.length);
    const failed = t.named("search index failed");
    expect(failed.length).toBe(1);
    // The probe's own error (not the rollback's), with its time, on the failure line.
    expect(failed[0]).toMatchObject({ stage: "search-index", error_code: "08006", skip_probe_error: "08006" });
    expect(typeof failed[0]!.skip_probe_ms).toBe("number");
    expect(t.named("search index reconciled")).toEqual([]);
    // Flag off, the same broken probe is never sent: the pass succeeds.
    const off = await tickLogged(r, { transact: lost });
    expect(off.response.batchItemFailures).toEqual([]);
    expect(off.named("search index failed")).toEqual([]);
  }, 120_000);

  test("the Postgres output facts store is refused (its facts are never checked)", async () => {
    const r = await indexed();
    await expect(indexSearchScope(SCOPE, r.search({ ...SHADOW, outputFactsStore: (() => ({})) as never }))).rejects.toThrow("skip current shadow: not supported with the Postgres output facts store");
  }, 120_000);

  test("through the consumer: the reconciled line carries would_skip and skip_miss, with no warning on a no-op", async () => {
    const r = await indexed();
    const quiet = await tickLogged(r, SHADOW);
    expect(quiet.response.batchItemFailures).toEqual([]);
    expect(r.requeued).toEqual([]);
    const line = quiet.named("search index reconciled");
    expect(line.length).toBe(1);
    expect(line[0]).toMatchObject({ stage: "search-index", ok: true, inserted: 0, deleted: 0, would_skip: true });
    expect("skip_miss" in line[0]!).toBe(false);
    // The probe's own time is on the line; no facts read and no error, so neither field.
    expect(typeof line[0]!.skip_probe_ms).toBe("number");
    expect("skip_probe_facts_ms" in line[0]! || "skip_probe_error" in line[0]!).toBe(false);
    expect(quiet.named("search index skip would have missed changes")).toEqual([]);
    expect(quiet.named("search index skipped")).toEqual([]);

    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    const missed = (await tickLogged(r, SHADOW)).named("search index reconciled");
    expect(missed[0]).toMatchObject({ would_skip: false, skip_miss: "not_ready" });

    // Flag off: neither field.
    const off = (await tickLogged(r, {})).named("search index reconciled");
    expect(off.length).toBe(1);
    expect(Object.keys(off[0]!).filter((k) => k.startsWith("skip_") || k === "would_skip")).toEqual([]);
  }, 120_000);

  test("through the consumer, one skip_check per probed pass: verified no-op, inconclusive, divergent, miss, probe failed", async () => {
    const r = await indexed();
    const check = async (extra: Partial<SearchIndexDeps>) => {
      const t = await tickLogged(r, { ...SHADOW, ...extra });
      const line = t.named("search index reconciled");
      expect(line.length).toBe(1);
      return { line: line[0]!, divergent: t.named("search index skip would have missed changes"), inconclusive: t.named("search index skip check inconclusive"), failures: t.response.batchItemFailures };
    };
    // 1. Called current, the full pass completed and wrote nothing: the only outcome that counts for the skip.
    const verified = await check({});
    expect(verified.line).toMatchObject({ ok: true, inserted: 0, deleted: 0, would_skip: true, skip_check: "verified_noop" });
    expect([verified.divergent, verified.inconclusive]).toEqual([[], []]);

    // 2. Called current, zero writes, but the pass did not complete (here: too large to read): nothing was
    //    verified, and it must not pass silently as a no-op.
    const unverified = await check({ limits: TINY });
    expect(unverified.line).toMatchObject({ ok: false, inserted: 0, deleted: 0, would_skip: true, skip_check: "inconclusive" });
    expect(unverified.divergent).toEqual([]);
    expect(unverified.inconclusive.length).toBe(1);
    expect(unverified.inconclusive[0]).toMatchObject({ stage: "search-index", reason: "skip_probe_inconclusive", skip_check: "inconclusive", ok: false, org_id: SCOPE.orgId, session_id: SCOPE.sessionId });
    expect(unverified.failures).toEqual([]);

    // 3. The incomplete pass reset readiness: the next probe is an ordinary miss, then current again.
    const miss = await check({});
    expect(miss.line).toMatchObject({ ok: true, would_skip: false, skip_miss: "not_ready", skip_check: "miss" });
    expect([miss.divergent, miss.inconclusive]).toEqual([[], []]);
    expect((await check({})).line.skip_check).toBe("verified_noop");

    // 4. Called current, and the pass wrote: divergent.
    const victim = r.fake.rows[0]!.event_id;
    r.fake.rows.splice(0, r.fake.rows.length, ...r.fake.rows.filter((row) => row.event_id !== victim));
    const divergent = await check({});
    expect(divergent.line).toMatchObject({ ok: true, would_skip: true, skip_check: "divergent" });
    expect(Number(divergent.line.inserted)).toBeGreaterThan(0);
    expect(divergent.divergent.length).toBe(1);
    expect(divergent.divergent[0]).toMatchObject({ reason: "skip_probe_divergence", skip_check: "divergent", inserted: divergent.line.inserted });
    expect(divergent.inconclusive).toEqual([]);

    // 5. No verdict: the probe failed.
    const broken = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) =>
          tx.query(sql.includes(PROBE_SQL) ? sql.replace("FROM sessions.session_directory d", "FROM sessions.no_such_probe_table d") : sql, params),
      }));
    const failed = await check({ transact: broken });
    expect(failed.line).toMatchObject({ would_skip: false, skip_miss: "probe_failed", skip_check: "probe_failed" });
    expect([failed.divergent, failed.inconclusive]).toEqual([[], []]);

    // Flag off: no skip_check.
    expect("skip_check" in (await tickLogged(r, {})).named("search index reconciled")[0]!).toBe(false);
  }, 120_000);

  test("skipCheck: every result maps to exactly one outcome, and only a completed zero-write pass is verified", () => {
    const base: SearchIndexResult = { manifests: 1, rows: 5, inserted: 0, deleted: 0, complete: true, reasons: [] };
    const current = { skipProbe: { current: true } };
    const facts = (over: Partial<NonNullable<SearchIndexResult["outputFacts"]>>) => ({ outputFacts: { inserted: 0, deleted: 0, complete: true, reasons: [], ...over } });
    expect(skipCheck(base)).toBeUndefined();
    expect(skipCheck({ ...base, ...current })).toBe("verified_noop");
    expect(skipCheck({ ...base, ...current, ...facts({}) })).toBe("verified_noop");
    expect(skipCheck({ ...base, ...current, inserted: 1 })).toBe("divergent");
    expect(skipCheck({ ...base, ...current, deleted: 1 })).toBe("divergent");
    // A write outranks an incomplete pass: it is evidence against, not a missing comparison.
    expect(skipCheck({ ...base, ...current, complete: false, inserted: 1 })).toBe("divergent");
    expect(skipCheck({ ...base, ...current, ...facts({ inserted: 1 }) })).toBe("facts_divergent");
    expect(skipCheck({ ...base, ...current, ...facts({ deleted: 1 }) })).toBe("facts_divergent");
    expect(skipCheck({ ...base, ...current, complete: false })).toBe("inconclusive");
    expect(skipCheck({ ...base, ...current, complete: false, partial: true, reasons: ["scope_too_large"] })).toBe("inconclusive");
    expect(skipCheck({ ...base, ...current, ...facts({ complete: false }) })).toBe("inconclusive");
    for (const miss of ["no_directory", "directory_deleted", "projection_version", "sources_changed", "not_ready", "facts_incomplete"] as const)
      expect(skipCheck({ ...base, skipProbe: { current: false, miss } })).toBe("miss");
    // No verdict, whatever the pass then did.
    expect(skipCheck({ ...base, inserted: 3, skipProbe: { current: false, miss: "probe_failed" } })).toBe("probe_failed");
    expect(skipCheck({ ...base, skipProbe: { current: false, miss: "facts_unreadable", factsRead: true } })).toBe("probe_failed");
    for (const value of ["verified_noop", "divergent", "facts_divergent", "inconclusive", "probe_failed", "miss"]) expect(sanitizeLogField("skip_check", value)).toBe(value);
    expect(sanitizeLogField("skip_check", "ok")).toBeUndefined();
  });

  test("index rows lost while the directory says current: the pass repairs them and the consumer warns", async () => {
    const r = await indexed();
    const whole = r.fake.dump();
    // What the documented skip-current gap looks like: the index lost rows, Postgres still says ready.
    const victim = r.fake.rows[0]!.event_id;
    const kept = r.fake.rows.filter((row) => row.event_id !== victim);
    r.fake.rows.splice(0, r.fake.rows.length, ...kept);
    expect(r.fake.dump().length).toBeLessThan(whole.length);
    const t = await tickLogged(r, SHADOW);
    expect(t.response.batchItemFailures).toEqual([]);
    const line = t.named("search index reconciled")[0]!;
    expect(line.would_skip).toBe(true);
    expect(Number(line.inserted)).toBeGreaterThan(0);
    const warned = t.named("search index skip would have missed changes");
    expect(warned.length).toBe(1);
    expect(warned[0]).toMatchObject({ stage: "search-index", reason: "skip_probe_divergence", org_id: SCOPE.orgId, session_id: SCOPE.sessionId, inserted: line.inserted });
    // Shadow never skips, so the index is whole again.
    expect(r.fake.dump()).toEqual(whole);
  }, 120_000);
});

describe("SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

  test("unset: no field, no deps flag; true: the search deps carry skipCurrentShadow only; it needs the search stage", async () => {
    expect("skipCurrentShadowEnabled" in lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" })).toBe(false);
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const env = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED: value }) });
      expect(env.skipCurrentShadowEnabled === true).toBe(expected);
      expect("skipCurrentEnabled" in env).toBe(false);
      const rt = runtimeFromSecret(env, secret, "synthetic-generation");
      try {
        expect("skipCurrentShadow" in rt.deps.search!).toBe(expected);
        expect("skipCurrent" in rt.deps.search!).toBe(false);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    expect(() => lambdaEnv({ ...base, SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED: "true" })).toThrow("SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
    for (const bad of ["1", "TRUE", "on", " true"])
      expect(() => searchIndexSkipCurrentShadowFromEnv({ SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED: bad })).toThrow("SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED must be true or false");
  });
});
