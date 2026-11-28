/**
 * SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED (forward port of the backport lineage's acceptance skip, 1c931eff /
 * 0c4fe2be / 247d9994) on main's hosted path: a migrated PGlite with the session directory and its triggers, the
 * legacy ClickHouse binding (search-index-hosted-harness.ts) and the real exported Lambda handler. A scope whose
 * fold refuses with relayhistory_acceptance_missing alone is stamped; a repeat over the same pointer set skips
 * before any manifest read and writes nothing; a changed pointer set, an accepted revision, a moved window or
 * changed limits run the normal pass; the managed search-jobs pass (M2) never skips. Synthetic data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  ACCEPTANCE_MISSING,
  ACCEPTANCE_SKIP_VERSION,
  ACCEPTANCE_STAMP_PREFIX,
  SKIPPED_BEFORE_READ,
  SKIP_STATE_MAX_AGE_MS,
  acceptanceStampName,
  indexSearchScope,
  type SearchIndexDeps,
} from "../search-index.ts";
import { claimSearchJobs, enqueueSearchWake, runSearchJob, settleSearchJob } from "../search-jobs.ts";
import { lambdaEnv, runtimeFromSecret, searchIndexSkipAcceptanceMissingFromEnv } from "../lambda.ts";
import { INGEST_STATE_TABLE } from "../vendor/search-index/schema.ts";
import { ID, SCOPE, T0, USER, activity, exportStreamScope, generate, type ExportedScope, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { HOSTED_NOW, hostedRig } from "./search-index-hosted-harness.ts";
import { captureLogs, realHandler, statefulClickHouse } from "./search-index-skip-flags-harness.ts";

const ON: Partial<SearchIndexDeps> = { skipAcceptanceMissing: true };
const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const HELD = [ACCEPTANCE_MISSING];
const SKIPPED = { manifests: 0, rows: 0, inserted: 0, deleted: 0, complete: false, skippedBeforeRead: true, reasons: [ACCEPTANCE_MISSING, SKIPPED_BEFORE_READ] };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

/** One machine's session (no cross-manifest copies) plus two manifests contending for one record id; `seats` grants acceptance. */
function contended(seats = false): Line[] {
  const lines: Line[] = generate(8);
  lines.push(
    activity("machine:contend-a", "capacity:contend-a", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }, "1", seats ? 1_000_001 : undefined),
    activity("machine:contend-b", "capacity:contend-b", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }, "1", seats ? 1_000_002 : undefined),
  );
  return lines;
}

let held: ExportedScope;
beforeAll(async () => {
  held = await exportStreamScope(contended(), 1200);
}, 120_000);
afterAll(async () => {
  await held?.dispose();
});

/**
 * Acceptance granted: the same session re-exported into `x`'s store with seats on the contenders, as the writer
 * would publish it: every manifest at the next revision (the export raises each machine's revision by one).
 */
async function grantAcceptance(x: ExportedScope) {
  const encoder = new TextEncoder();
  await exportUnit({ store: x.store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes: 1200 },
    { orgId: ID.orgId, workspaceId: ID.workspaceId, source: ID.source, sessionId: ID.sessionId, firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {} },
    {
      fenced: async () => false,
      async *lines() {
        for (const { machine, record, userId } of contended(true))
          yield { machine, userId: userId ?? USER, bytes: encoder.encode(JSON.stringify(record) + "\n") };
      },
    });
}

/** A rig over `x` (default the shared `held`) whose pointer stage ran (flag off: search refused for acceptance, no stamp). */
async function rig(x: ExportedScope = held) {
  const r = await hostedRig(x, cleanup);
  const state = statefulClickHouse(r.fake);
  await r.tick({ ch: state.ch });
  // Each pass one second after the previous (a stamp and the tombstone that clears it never share a time).
  const clock = { ms: HOSTED_NOW.getTime() };
  const search = (extra: Partial<SearchIndexDeps> = {}) => r.search({ ch: state.ch, now: () => new Date(clock.ms), ...extra });
  /** One pass with the flag on (unless overridden), and what it touched. */
  const pass = async (extra: Partial<SearchIndexDeps> = {}) => {
    const reads = r.reads.count, sql = r.sql.length;
    r.fake.calls.length = 0;
    clock.ms += 1_000;
    const result = await indexSearchScope(SCOPE, search({ ...ON, ...extra }));
    const statements = r.sql.slice(sql).map((s) => String(s.sql));
    return { result, reads: r.reads.count - reads, calls: [...r.fake.calls], statements };
  };
  const stamps = () => state.ingest.filter((s) => s.name === acceptanceStampName(SCOPE));
  return { r, state, search, pass, stamps, clock };
}
const inserts = (calls: Array<Record<string, unknown>>) => calls.filter((c) => c.ch === "insert" || c.ch === "command");
const writes = (statements: string[]) => statements.filter((s) => /^(UPDATE|INSERT|DELETE)\b/i.test(s) || /\b(invalidate|UPDATE sessions)\b/i.test(s));

describe("flag off", () => {
  test("fixture: the fold refuses for acceptance alone; every repeat reads every manifest; no stamp read or write", async () => {
    const { pass } = await rig();
    for (let n = 0; n < 2; n++) {
      const p = await pass({ skipAcceptanceMissing: undefined });
      expect(p.result).toMatchObject({ complete: false, reasons: HELD });
      expect(p.result.skippedBeforeRead).toBeUndefined();
      expect(p.reads).toBeGreaterThan(0);
      expect(p.calls.some((c) => JSON.stringify(c).includes("acceptance_skip/"))).toBe(false);
    }
  }, 120_000);
});

describe("flag on: stamp and skip", () => {
  test("first pass: the flag-off result plus one stamp; repeat: skipped, no read, no ClickHouse write, no SQL write", async () => {
    const { pass, stamps } = await rig();
    const first = await pass();
    expect(first.result).toMatchObject({ complete: false, reasons: HELD });
    expect(first.reads).toBeGreaterThan(0);
    expect(stamps()).toHaveLength(1);
    const value = JSON.parse(stamps()[0]!.value) as Record<string, unknown>;
    expect(value).toMatchObject({ v: 1, reason: ACCEPTANCE_MISSING });
    expect(value.digest).toMatch(/^[0-9a-f]{64}$/);
    const repeat = await pass();
    expect(repeat.result).toEqual(SKIPPED);
    expect(repeat.reads).toBe(0);
    expect(inserts(repeat.calls)).toEqual([]);
    expect(writes(repeat.statements)).toEqual([]);
    // Its ClickHouse calls: the window read and the one stamp read; no transcript_events scan.
    expect(repeat.calls.map((c) => String(c.query))).toEqual([expect.stringContaining("argMax(value, updated_at)"), expect.stringContaining("ORDER BY updated_at DESC")]);
    expect(stamps()).toHaveLength(1);
  }, 120_000);

  test("a pointer revision raised in session_index: digest changes, the pass reads in full and does not stamp (read differs from the row)", async () => {
    const { r, pass, stamps } = await rig();
    await pass();
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [held.keys[0]]);
    const p = await pass();
    expect(p.result.skippedBeforeRead).toBeUndefined();
    expect(p.reads).toBeGreaterThan(0);
    // Not read as listed: the stamp is cleared (a tombstone), never re-written for this set.
    expect(stamps().at(-1)!.value).toBe("");
    expect((await pass()).result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("a sibling pointer tombstoned: the pass runs; restored, the next pass runs and re-stamps; then it skips again", async () => {
    const { r, pass } = await rig();
    await pass();
    const contendB = held.keys.find((k) => k.includes("contend-b"))!;
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE manifest_key = $1", [contendB]);
    const without = await pass();
    expect(without.result.skippedBeforeRead).toBeUndefined();
    expect(without.reads).toBeGreaterThan(0);
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = NULL WHERE manifest_key = $1", [contendB]);
    // The trigger moved the directory epoch twice: the old digest cannot match, the pass reads and stamps anew.
    const back = await pass();
    expect(back.result).toMatchObject({ complete: false, reasons: HELD });
    expect(back.reads).toBeGreaterThan(0);
    expect((await pass()).result).toEqual(SKIPPED);
  }, 120_000);

  test("the window moves (stored window_start set), limits change, or the stream budgets change: no skip", async () => {
    const { state, pass } = await rig();
    await pass();
    state.switches.window = "2026-10-01T00:00:00.000Z";
    expect((await pass()).result.skippedBeforeRead).toBeUndefined();
    expect((await pass()).result).toEqual(SKIPPED);
    expect((await pass({ limits: { maxScopeEvents: 200_000 } })).result.skippedBeforeRead).toBeUndefined();
    expect((await pass({ stream: { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 } })).result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("the age bound: a stamp just under SKIP_STATE_MAX_AGE_MS skips, one at the bound or older does not", async () => {
    const { pass, clock } = await rig();
    await pass();
    const stampedAt = clock.ms;
    expect((await pass({ now: () => new Date(stampedAt + SKIP_STATE_MAX_AGE_MS - 1) })).result).toEqual(SKIPPED);
    expect((await pass({ now: () => new Date(stampedAt + SKIP_STATE_MAX_AGE_MS) })).result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("stamp read fails: the pass reads in full (fail open) and stamps its outcome", async () => {
    const { state, pass, stamps } = await rig();
    state.switches.failStampRead = true;
    const p = await pass();
    expect(p.result).toMatchObject({ complete: false, reasons: HELD });
    expect(p.reads).toBeGreaterThan(0);
    expect(stamps()).toHaveLength(1);
    state.switches.failStampRead = false;
    expect((await pass()).result).toEqual(SKIPPED);
  }, 120_000);

  test("stamp write fails: the result is unchanged and one warn line carries codes only; the repeat reads in full", async () => {
    const { state, pass } = await rig();
    state.switches.failIngestInsert = true;
    const { result, lines } = await captureLogs(() => pass());
    expect(result.result).toMatchObject({ complete: false, reasons: HELD });
    const warns = lines.filter((l) => l.message === "search index acceptance stamp not written");
    expect(warns).toEqual([expect.objectContaining({ level: "warn", reason: "acceptance_stamp_write_failed", stage: "search-index" })]);
    expect(JSON.stringify(warns)).not.toContain("synthetic insert refusal");
    expect((await pass()).reads).toBeGreaterThan(0);
  }, 120_000);

  test("a size refusal clears the stamp (limits are in the digest); erase clears it too", async () => {
    const { r, pass, stamps } = await rig();
    await pass();
    const refused = await pass({ limits: TINY });
    expect(refused.result).toMatchObject({ partial: true, reasons: ["scope_too_large"] });
    expect(stamps().at(-1)!.value).toBe("");
    await pass();
    expect(JSON.parse(stamps().at(-1)!.value).reason).toBe(ACCEPTANCE_MISSING);
    await r.db.pg.exec("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await pass()).result).toMatchObject({ complete: true, reasons: ["erased"] });
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);

  test("acceptance granted (the accepted manifests at the next revision): the pass reads them, completes and clears the stamp", async () => {
    const x = await exportStreamScope(contended(), 1200);
    cleanup.push(x.dispose);
    const { r, state, pass, stamps } = await rig(x);
    await pass();
    expect(stamps()).toHaveLength(1);
    await grantAcceptance(x);
    // The pointer stage takes the new revisions (search flag off and refused for size, so only the pointers move).
    await r.tick({ ch: state.ch, limits: TINY });
    const p = await pass();
    expect(p.result).toMatchObject({ complete: true, reasons: [] });
    expect(stamps().at(-1)!.value).toBe("");
    expect((await pass()).result.skippedBeforeRead).toBeUndefined();
  }, 120_000);

  test("the stamp name is per scope and prefixed; the salt is not the backport's v1 (its stamps never match here)", () => {
    expect(acceptanceStampName(SCOPE)).toMatch(new RegExp(`^${ACCEPTANCE_STAMP_PREFIX}[0-9a-f]{64}$`));
    expect(acceptanceStampName({ ...SCOPE, sessionId: "other" })).not.toBe(acceptanceStampName(SCOPE));
    expect(ACCEPTANCE_SKIP_VERSION).not.toBe("acceptance-skip-v1");
    expect(INGEST_STATE_TABLE).toBe("search_ingest_state");
  });
});

describe("the real exported Lambda handler", () => {
  test("flag on: the repeat batch is acknowledged as the acceptance outcome with no search read; invalidation runs the normal pass", async () => {
    const { r, state, stamps } = await rig();
    const send = realHandler(r.consumer({ ch: state.ch, ...ON }));
    const ctx = { getRemainingTimeInMillis: () => 300_000 };
    // First batch: normal pass, stamped.
    await send(r.messages, ctx);
    expect(stamps()).toHaveLength(1);
    // Repeat batch: skipped. The pointer stage reads each manifest once; the search stage reads none.
    const reads = r.reads.count;
    const { result, lines } = await captureLogs(() => send(r.messages, ctx));
    expect(result).toEqual({ batchItemFailures: [] });
    expect(r.requeued).toEqual([]);
    expect(r.reads.count - reads).toBeLessThanOrEqual(held.keys.length);
    const reconciled = lines.filter((l) => l.message === "search index reconciled");
    expect(reconciled).toEqual([expect.objectContaining({ ok: false, manifests: 0, reason: `${ACCEPTANCE_MISSING},${SKIPPED_BEFORE_READ}` })]);
    // Invalidation: the window moves; the next batch's pass reads in full (more reads than the pointer stage's).
    state.switches.window = "2026-10-01T00:00:00.000Z";
    const before = r.reads.count;
    const again = await captureLogs(() => send(r.messages, ctx));
    expect(again.result).toEqual({ batchItemFailures: [] });
    expect(r.reads.count - before).toBeGreaterThan(held.keys.length);
    expect(again.lines.filter((l) => l.message === "search index reconciled")).toEqual([expect.objectContaining({ ok: false, reason: ACCEPTANCE_MISSING })]);
  }, 120_000);

  test("acceptance granted end to end: the real pointer stage takes the accepted revisions, the pass completes, the stamp clears", async () => {
    const x = await exportStreamScope(contended(), 1200);
    cleanup.push(x.dispose);
    const { r, state, stamps } = await rig(x);
    const send = realHandler(r.consumer({ ch: state.ch, ...ON }));
    await send(r.messages);
    expect(stamps()).toHaveLength(1);
    expect((await captureLogs(() => send(r.messages))).lines.filter((l) => l.message === "search index reconciled"))
      .toEqual([expect.objectContaining({ reason: `${ACCEPTANCE_MISSING},${SKIPPED_BEFORE_READ}` })]);
    await grantAcceptance(x);
    const { result, lines } = await captureLogs(() => send(r.messages));
    expect(result).toEqual({ batchItemFailures: [] });
    // The two contending manifests changed and moved to revision 2 (an unchanged manifest keeps its revision).
    expect(lines.filter((l) => l.message === "pointer indexed" && l.revision === 2).length).toBeGreaterThanOrEqual(2);
    expect(lines.filter((l) => l.message === "search index reconciled")).toEqual([expect.objectContaining({ ok: true })]);
    expect(stamps().at(-1)!.value).toBe("");
  }, 120_000);

  test("flag off through the handler: every batch reads in full, no stamp", async () => {
    const { r, state, stamps } = await rig();
    const send = realHandler(r.consumer({ ch: state.ch }));
    for (let n = 0; n < 2; n++) {
      const before = r.reads.count;
      expect(await send(r.messages)).toEqual({ batchItemFailures: [] });
      expect(r.reads.count - before).toBeGreaterThan(held.keys.length);
    }
    expect(stamps()).toEqual([]);
  }, 120_000);
});

describe("M2 (search jobs) with the flag on", () => {
  const canonical = () => ({ orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, harness: SCOPE.harness, sessionId: held.canonicalId });
  const wake = (epoch: number) => ({
    v: 1, stage: "search-index", ctx: { traceId: "00000000-0000-4000-8000-000000000002", ...canonical() },
    workKey: ["search", SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, held.canonicalId].join("\u001f"), revision: 1,
    payload: { epoch, clock: { source: "publish", at: "2026-11-01T12:00:00.000Z" } },
  });
  const epochOf = async (r: Awaited<ReturnType<typeof rig>>["r"]) => Number((await r.db.pg.query<{ epoch: string }>(
    "SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
    [SCOPE.orgId, SCOPE.workspaceId, SCOPE.harness, held.canonicalId])).rows[0]!.epoch);

  test("with a matching inline stamp present, the managed pass still reads in full, answers held, never reads the stamp", async () => {
    const { r, pass, search, stamps } = await rig();
    await pass();
    expect((await pass()).result).toEqual(SKIPPED);
    expect(stamps()).toHaveLength(1);
    r.fake.calls.length = 0;
    const reads = r.reads.count;
    const job = await runSearchJob(wake(await epochOf(r)) as never, { search: search(ON) });
    expect(job.outcome).toBe("held");
    expect(job.search?.skippedBeforeRead).toBeUndefined();
    expect(r.reads.count - reads).toBeGreaterThan(0);
    expect(r.fake.calls.some((c) => JSON.stringify(c).includes("acceptance_skip/"))).toBe(false);
    expect(stamps()).toHaveLength(1);
  }, 120_000);

  test("pickup, lease, held retry and the directory's ready flag are the same with the flag on and off", async () => {
    const outcomes: Record<string, unknown[]> = {};
    for (const flag of [false, true]) {
      const { r, search } = await rig();
      const deps = search(flag ? ON : {});
      // Flag on: an inline pass stamps first, so the skip is armed for the S3-event path.
      if (flag) await indexSearchScope(SCOPE, deps);
      await enqueueSearchWake(r.db.pg, wake(await epochOf(r)) as never);
      const seen: unknown[] = [];
      for (let n = 1; n <= 2; n++) {
        await r.db.pg.query("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second' WHERE stage = 'search-index'");
        const [claimed] = await claimSearchJobs(r.db.pg, 1);
        const result = await runSearchJob(claimed!.claimedMessage, { search: deps });
        const settled = await settleSearchJob(r.db.pg, claimed!, { retry: 30 * 2 ** (claimed!.attempts - 1) });
        const ready = (await r.db.pg.query<{ ready: boolean | null }>(
          "SELECT event_projection_ready AS ready FROM sessions.session_directory WHERE org_id=$1 AND session_id=$2", [SCOPE.orgId, held.canonicalId])).rows[0]?.ready;
        seen.push({ attempts: claimed!.attempts, outcome: result.outcome, reasons: result.reasons, settled: settled.applied, ready });
      }
      outcomes[String(flag)] = seen;
    }
    expect(outcomes.true).toEqual(outcomes.false!);
    expect(outcomes.false).toEqual([
      expect.objectContaining({ attempts: 1, outcome: "held", settled: true, ready: false }),
      expect.objectContaining({ attempts: 2, outcome: "held", settled: true, ready: false }),
    ]);
  }, 120_000);
});

describe("SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q", SEARCH_INDEX_ENABLED: "1",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

  test("unset, empty or false: the env and the search deps are exactly as without it; true arms both", async () => {
    const { adapterEnv: plainAdapter, ...plainEnv } = lambdaEnv(base);
    for (const [value, armed] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const { adapterEnv, ...env } = lambdaEnv({ ...base, ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: value }) });
      expect(env).toEqual(armed ? { ...plainEnv, searchSkipAcceptanceMissing: true } : plainEnv);
      // main copies every SEARCH_INDEX_* variable into adapterEnv (read only by a portable backend), as before.
      expect(adapterEnv).toEqual({ ...plainAdapter, ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: value }) });
      const rt = runtimeFromSecret({ ...env, adapterEnv }, secret, "synthetic-generation");
      try {
        expect("skipAcceptanceMissing" in rt.deps.search!).toBe(armed);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
  });

  test("the backport's grammar: only true and false; true needs SEARCH_INDEX_ENABLED=1", () => {
    for (const bad of ["1", "TRUE", "on", " true"])
      expect(() => searchIndexSkipAcceptanceMissingFromEnv({ SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: bad })).toThrow("SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED must be true or false");
    const { SEARCH_INDEX_ENABLED: _s, ...noSearch } = base;
    expect(() => lambdaEnv({ ...noSearch, SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED: "true" })).toThrow("SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
  });

  test("the managed pass is excluded by construction (committedGuard), and search-jobs.ts is untouched by the flag", () => {
    const jobs = readFileSync(join(import.meta.dir, "..", "search-jobs.ts"), "utf8");
    expect(jobs).not.toContain("skipAcceptanceMissing");
    expect(jobs).toContain("committedGuard: true");
  });
});
