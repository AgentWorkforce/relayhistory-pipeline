/**
 * SEARCH_INDEX_SKIP_CURRENT_SHADOW_ENABLED=true on the hosted path (search-index-hosted-harness.ts: processBatch,
 * the legacy ClickHouse binding, a migrated PGlite with the session directory and its triggers): a pass that is
 * not skipped runs exactly as before and reports what the skip-current check answers, and why not. Nothing is
 * skipped by the shadow flag. Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { alreadyCurrent, indexSearchScope, skipCurrentProbe, type SearchIndexDeps } from "../search-index.ts";
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
    expect(on.sql.length).toBe(off.sql.length + 1);
    const probe = on.sql.filter((s) => s.includes(PROBE_SQL));
    expect(probe.length).toBe(1);
    expect(/\b(UPDATE|INSERT|DELETE)\b/i.test(probe[0]!)).toBe(false);
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
    expect((await pass(r, { ...SHADOW, outputFacts: true })).result.skipProbe).toEqual({ current: false, miss: "facts_incomplete" });
    r.fake.facts.complete = 1;
    r.fake.facts.fail = true;
    const unreadable = await pass(r, { ...SHADOW, outputFacts: true });
    expect(unreadable.result.skipProbe).toEqual({ current: false, miss: "facts_incomplete" });
    expect(unreadable.reads).toBeGreaterThan(0);
    r.fake.facts.fail = false;
    expect((await pass(r, { ...SHADOW, outputFacts: true })).result.skipProbe?.current).toBe(true);
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
    expect(quiet.named("search index skip would have missed changes")).toEqual([]);
    expect(quiet.named("search index skipped")).toEqual([]);

    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    const missed = (await tickLogged(r, SHADOW)).named("search index reconciled");
    expect(missed[0]).toMatchObject({ would_skip: false, skip_miss: "not_ready" });

    // Flag off: neither field.
    const off = (await tickLogged(r, {})).named("search index reconciled");
    expect(off.length).toBe(1);
    expect("would_skip" in off[0]! || "skip_miss" in off[0]!).toBe(false);
  }, 120_000);

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
