/**
 * SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED (forward port of the backport lineage's over-bound skip, 14e9c00a / 95776f5b)
 * on main's hosted path: a migrated PGlite with the session directory and its triggers, the legacy ClickHouse
 * binding (search-index-hosted-harness.ts) and the real exported Lambda handler. A scope refused for its size,
 * whose stored pointer sizes and latest search state already show that refusal at its current revision, repeats
 * the refusal before any manifest read; a new revision, raised limits, unscrubbed pointers, a stream that could
 * take it, a stale or other state row run the normal pass; the managed search-jobs pass never skips.
 * Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { SKIPPED_BEFORE_READ, SKIP_STATE_MAX_AGE_MS, indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { runSearchJob } from "../search-jobs.ts";
import { lambdaEnv, runtimeFromSecret, searchIndexSkipOverBoundFromEnv } from "../lambda.ts";
import { SESSION_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { ID, SCOPE, T0, USER, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { HOSTED_NOW, hostedRig } from "./search-index-hosted-harness.ts";
import { captureLogs, realHandler, statefulClickHouse } from "./search-index-skip-flags-harness.ts";

const ON: Partial<SearchIndexDeps> = { skipOverBound: true };
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const SMALL_STREAM = { maxReadBytes: 8, maxReadEvents: 8 };
const SKIPPED = {
  manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, partial: true, skippedBeforeRead: true,
  reasons: ["scope_too_large", SKIPPED_BEFORE_READ],
};

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

const lines = (extra = false): Line[] => [
  ...generate(33),
  ...(extra ? [activity("machine:0", "capacity:machine:0", "session_event", "late", T0 + 30_000, { role: "user", text: "late" }, "1", 9_000_001)] : []),
];

/** The writer publishing the same session again with one more record: its manifest moves to the next revision. */
async function republish(x: ExportedScope) {
  const encoder = new TextEncoder();
  await exportUnit({ store: x.store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes: 1200 },
    { orgId: ID.orgId, workspaceId: ID.workspaceId, source: ID.source, sessionId: ID.sessionId, firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {} },
    {
      fenced: async () => false,
      async *lines() {
        for (const { machine, record, userId } of lines(true))
          yield { machine, userId: userId ?? USER, bytes: encoder.encode(JSON.stringify(record) + "\n") };
      },
    });
}

let shared: ExportedScope;
beforeAll(async () => {
  shared = await exportStreamScope(lines(), 1200);
}, 120_000);
afterAll(() => shared?.dispose());

/**
 * A rig whose pointer stage ran (search flag off and refused for size: the first refusal's state row exists).
 * `reads` counts the search stage's manifest reads (a size refusal reads manifests, never a chunk).
 */
async function rig(x: ExportedScope = shared, limits: Partial<SearchIndexDeps["limits"]> = TINY) {
  const r = await hostedRig(x, cleanup);
  const state = statefulClickHouse(r.fake);
  const clock = { ms: HOSTED_NOW.getTime() };
  const reads = { count: 0 };
  const reader = { provider: x.reader.provider, bucket: x.reader.bucket, read: async (key: string) => { reads.count += 1; return x.reader.read(key); } };
  const search = (extra: Partial<SearchIndexDeps> = {}) => r.search({ ch: state.ch, reader, now: () => new Date(clock.ms), limits, ...extra });
  /** Consumer deps for the handler: the counted reader on the search stage. */
  const consumer = (extra: Partial<SearchIndexDeps> = {}) => r.consumer({ ch: state.ch, reader, ...extra });
  await r.tick({ ch: state.ch, reader, limits });
  const pass = async (extra: Partial<SearchIndexDeps> = {}) => {
    const before = reads.count, sql = r.sql.length;
    r.fake.calls.length = 0;
    clock.ms += 1_000;
    const result = await indexSearchScope(SCOPE, search({ ...ON, ...extra }));
    return { result, reads: reads.count - before, calls: [...r.fake.calls], statements: r.sql.slice(sql).map((s) => String(s.sql)) };
  };
  return { r, state, search, consumer, pass, clock, reads };
}
const writes = (calls: Array<Record<string, unknown>>) => calls.filter((c) => c.ch === "insert" || c.ch === "command");

describe("flag off", () => {
  test("fixture: refused for size from scrubbed pointers; each pass reads every manifest; no state read", async () => {
    const { r, pass } = await rig();
    const { rows } = await r.db.pg.query<{ scrubbed: boolean; raw_bytes: string }>("SELECT scrubbed, raw_bytes::text AS raw_bytes FROM sessions.session_index WHERE deleted_at IS NULL");
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.scrubbed && Number(row.raw_bytes) > 1)).toBe(true);
    for (let n = 0; n < 2; n++) {
      const p = await pass({ skipOverBound: undefined });
      expect(p.result).toMatchObject({ complete: false, partial: true, reasons: ["scope_too_large"] });
      expect(p.reads).toBeGreaterThan(0);
      expect(p.calls.some((c) => String(c.query ?? "").includes(SESSION_STATE_TABLE))).toBe(false);
    }
  }, 120_000);
});

describe("flag on", () => {
  test("repeat at the same revision: no manifest read, no ClickHouse write, no SQL write, the skipped result", async () => {
    const { pass } = await rig();
    const p = await pass();
    expect(p.result).toEqual(SKIPPED);
    expect(p.reads).toBe(0);
    expect(writes(p.calls)).toEqual([]);
    expect(p.statements.filter((s) => /^(UPDATE|INSERT|DELETE)\b/i.test(s))).toEqual([]);
    expect(p.calls.map((c) => String(c.query))).toEqual([expect.stringContaining(`FROM ${SESSION_STATE_TABLE}`)]);
  }, 120_000);

  test("a new pointer revision (the writer republished) takes the normal pass, then the next repeat skips", async () => {
    const x = await exportStreamScope(lines(), 1200);
    cleanup.push(x.dispose);
    const { r, consumer, pass, clock, reads } = await rig(x);
    expect((await pass()).result).toEqual(SKIPPED);
    await republish(x);
    // The pointer stage takes revision 2; the search pass (flag on) finds the state at revision 1: it reads in full.
    const before = reads.count;
    clock.ms += 1_000;
    await realHandler(consumer({ limits: TINY, now: () => new Date(clock.ms), ...ON }))(r.messages);
    expect(reads.count - before).toBe(x.keys.length);
    expect(String(r.fake.state.at(-1)!.revision)).toBe("2");
    expect((await pass()).result).toEqual(SKIPPED);
    // A pointer row whose revision moved without a pass (no state at that revision) also runs the normal pass.
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [x.keys[0]]);
    const ran = await pass();
    expect(ran.result.skippedBeforeRead).toBeUndefined();
    expect(ran.reads).toBeGreaterThan(0);
  }, 120_000);

  test("limits raised above the size take the normal pass (and complete)", async () => {
    const { pass } = await rig();
    const p = await pass({ limits: {} });
    expect(p.result).toMatchObject({ complete: true });
    expect(p.reads).toBeGreaterThan(0);
    // The complete pass's state row (needs_full 0) is not a refusal: the next refused pass runs in full, then skips.
    expect((await pass()).result.skippedBeforeRead).toBeUndefined();
    expect((await pass()).result).toEqual(SKIPPED);
  }, 120_000);

  test("unscrubbed pointers are not counted: scrubbed bytes under the bound never skip", async () => {
    const { r, pass } = await rig();
    await r.db.pg.exec("UPDATE sessions.session_index SET scrubbed = false");
    const p = await pass();
    expect(p.result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("a scrubbed sum exactly at maxScopeBytes does not skip (the bound is strict); one byte under it does", async () => {
    const { r, pass } = await rig();
    const { rows } = await r.db.pg.query<{ sum: string }>("SELECT sum(raw_bytes)::text AS sum FROM sessions.session_index WHERE deleted_at IS NULL AND scrubbed");
    const sum = Number(rows[0]!.sum);
    expect((await pass({ limits: { ...TINY, maxScopeBytes: sum } })).result.skippedBeforeRead).toBeUndefined();
    // The normal pass at maxScopeBytes = sum reads and is refused (the read sizes at least the pointer bytes).
    expect((await pass({ limits: { ...TINY, maxScopeBytes: sum - 1 } })).result).toEqual(SKIPPED);
  }, 120_000);

  test("state read throws: the normal pass runs", async () => {
    const { state, pass } = await rig();
    state.switches.failStateRead = true;
    const p = await pass();
    expect(p.result).toMatchObject({ partial: true, reasons: ["scope_too_large"] });
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.reads).toBeGreaterThan(0);
  }, 120_000);

  test("the state age bound: a row just under it skips; at the bound, or far in the future, the normal pass runs", async () => {
    const { r, pass } = await rig();
    const last = r.fake.state.at(-1)!;
    const writtenMs = Date.parse(`${String(last.synced_at).replace(" ", "T")}Z`);
    expect((await pass({ now: () => new Date(writtenMs + SKIP_STATE_MAX_AGE_MS - 1) })).result).toEqual(SKIPPED);
    expect((await pass({ now: () => new Date(writtenMs + SKIP_STATE_MAX_AGE_MS) })).result.skippedBeforeRead).toBeUndefined();
    // That normal pass rewrote the state row at its own (later) time; far before it is outside the bound.
    expect((await pass({ now: () => new Date(writtenMs - SKIP_STATE_MAX_AGE_MS) })).result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("with stream deps: a scope the streamed path could take is never skipped; one beyond the budgets is refused once, then skipped", async () => {
    const { pass } = await rig();
    const streamed = await pass({ stream: UNLIMITED });
    expect(streamed.result.skippedBeforeRead).toBeUndefined();
    expect(streamed.result.complete).toBe(true);
    // The streamed pass recorded a complete state: the first beyond-budget pass reads, refuses, then the repeat skips.
    const refused = await pass({ stream: SMALL_STREAM });
    expect(refused.result).toMatchObject({ partial: true });
    expect(refused.result.skippedBeforeRead).toBeUndefined();
    expect((await pass({ stream: SMALL_STREAM })).result).toEqual(SKIPPED);
  }, 120_000);
});

describe("the real exported Lambda handler", () => {
  test("flag on: the repeat batch is acknowledged as the size refusal with no search read; invalidation runs the normal pass", async () => {
    const x = await exportStreamScope(lines(), 1200);
    cleanup.push(x.dispose);
    const { r, consumer, reads } = await rig(x);
    const send = realHandler(consumer({ limits: TINY, ...ON }));
    const before = reads.count;
    const { result, lines: logs } = await captureLogs(() => send(r.messages));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(r.requeued).toEqual([]);
    // The search stage reads no manifest.
    expect(reads.count - before).toBe(0);
    expect(logs.filter((l) => l.message === "search index partial")).toEqual([expect.objectContaining({ level: "warn", reason: `scope_too_large,${SKIPPED_BEFORE_READ}` })]);
    expect(logs.filter((l) => l.message === "search index reconciled")).toEqual([expect.objectContaining({ ok: false, manifests: 0 })]);
    // Invalidation 1: the writer republishes (next revision) through the real pointer stage: a normal pass reads.
    await republish(x);
    const after = reads.count;
    const republished = await captureLogs(() => send(r.messages));
    expect(republished.result).toEqual({ batchItemFailures: [] });
    expect(republished.lines.filter((l) => l.message === "pointer indexed" && l.revision === 2).length).toBeGreaterThan(0);
    expect(reads.count - after).toBe(x.keys.length);
    expect(republished.lines.filter((l) => l.message === "search index partial")).toEqual([expect.objectContaining({ reason: "scope_too_large" })]);
    // Invalidation 2: the deployment's limits raised above the scope: the normal pass completes.
    const raised = await captureLogs(() => realHandler(consumer({ limits: {}, ...ON }))(r.messages));
    expect(raised.lines.filter((l) => l.message === "search index reconciled")).toEqual([expect.objectContaining({ ok: true })]);
  }, 120_000);

  test("flag off through the handler: every batch reads in full", async () => {
    const { r, consumer, reads } = await rig();
    const send = realHandler(consumer({ limits: TINY }));
    for (let n = 0; n < 2; n++) {
      const before = reads.count;
      expect(await send(r.messages)).toEqual({ batchItemFailures: [] });
      expect(reads.count - before).toBe(shared.keys.length);
    }
  }, 120_000);
});

describe("M2 (search jobs) with the flag on", () => {
  test("the managed pass over a refused scope still reads in full and answers held; no state read", async () => {
    const { r, search, pass, reads: manifestReads } = await rig();
    expect((await pass()).result).toEqual(SKIPPED);
    const { rows } = await r.db.pg.query<{ epoch: string }>(
      "SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
      [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, shared.canonicalId]);
    const scope = { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, harness: SCOPE.harness, sessionId: shared.canonicalId };
    const wake = {
      v: 1, stage: "search-index", ctx: { traceId: "00000000-0000-4000-8000-000000000003", ...scope },
      workKey: ["search", scope.orgId, scope.workspaceId, scope.harness, scope.sessionId].join("\u001f"), revision: 1,
      payload: { epoch: Number(rows[0]!.epoch), clock: { source: "publish", at: "2026-11-01T12:00:00.000Z" } },
    };
    r.fake.calls.length = 0;
    const reads = manifestReads.count;
    const job = await runSearchJob(wake as never, { search: search(ON) });
    expect(job.outcome).toBe("held");
    expect(job.search?.skippedBeforeRead).toBeUndefined();
    expect(manifestReads.count - reads).toBeGreaterThan(0);
    expect(r.fake.calls.some((c) => String(c.query ?? "").includes(`FROM ${SESSION_STATE_TABLE}`))).toBe(false);
  }, 120_000);
});

describe("SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", SEARCH_INDEX_ENABLED: "1",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

  test("unset, empty or false: the env and the search deps are exactly as without it; true arms both, independent of the acceptance flag", async () => {
    const { adapterEnv: plainAdapter, ...plainEnv } = lambdaEnv(base);
    for (const [value, armed] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const { adapterEnv, ...env } = lambdaEnv({ ...base, ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: value }) });
      expect(env).toEqual(armed ? { ...plainEnv, searchSkipOverBound: true } : plainEnv);
      expect(adapterEnv).toEqual({ ...plainAdapter, ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: value }) });
      const rt = runtimeFromSecret({ ...env, adapterEnv }, secret, "synthetic-generation");
      try {
        expect("skipOverBound" in rt.deps.search!).toBe(armed);
        expect("skipAcceptanceMissing" in rt.deps.search!).toBe(false);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    const both = lambdaEnv({ ...base, SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true", SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" });
    expect([both.searchSkipOverBound, both.searchSkipAcceptanceMissing]).toEqual([true, true]);
  });

  test("the backport's grammar: only true and false; true needs SEARCH_INDEX_ENABLED=1", () => {
    for (const bad of ["1", "TRUE", "on", " true"])
      expect(() => searchIndexSkipOverBoundFromEnv({ SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: bad })).toThrow("SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED must be true or false");
    const { SEARCH_INDEX_ENABLED: _s, ...noSearch } = base;
    expect(() => lambdaEnv({ ...noSearch, SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true" })).toThrow("SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  });
});
