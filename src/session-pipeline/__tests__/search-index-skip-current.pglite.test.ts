/**
 * SEARCH_INDEX_SKIP_CURRENT_ENABLED=true on the hosted path (search-index-hosted-harness.ts: processBatch, the
 * legacy ClickHouse binding, a migrated PGlite with the session directory and its triggers): a pass that a
 * complete pass already covers returns before any object read or ClickHouse call, and every case that must run
 * still runs. Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { lambdaEnv, runtimeFromSecret, searchIndexSkipCurrentFromEnv } from "../lambda.ts";
import { SCOPE, exportStreamScope, generate, type ExportedScope } from "../../session-read-api/__tests__/stream-fixture.ts";
import { closeReplayTemplate, warmReplayTemplate } from "./replay-fixture.ts";
import { hostedRig } from "./search-index-hosted-harness.ts";

const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const UNLIMITED = { maxReadBytes: 1 << 30, maxReadEvents: 1 << 24 };
const ON: Partial<SearchIndexDeps> = { skipCurrent: true };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});
beforeAll(() => warmReplayTemplate(), 120_000);
afterAll(() => closeReplayTemplate());

let x: ExportedScope;
beforeAll(async () => {
  x = await exportStreamScope(generate(31), 1200);
}, 120_000);
afterAll(() => x?.dispose());

/** A rig whose scope a complete pass (flag off) already indexed. */
async function indexed() {
  const r = await hostedRig(x, cleanup);
  await r.tick({});
  expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
  return r;
}
type Rig = Awaited<ReturnType<typeof indexed>>;
/** One search pass with the flag on, and what it touched. */
async function pass(r: Rig, extra: Partial<SearchIndexDeps> = {}) {
  const reads = r.reads.count;
  r.fake.calls.length = 0;
  const result = await indexSearchScope(SCOPE, r.search({ ...ON, ...extra }));
  return { result, reads: r.reads.count - reads, clickhouse: r.fake.calls.length };
}

describe("skip-current on (hosted ClickHouse binding)", () => {
  test("a pass a complete pass already covers is skipped: no object read, no ClickHouse call, nothing written", async () => {
    const r = await indexed();
    const state = r.fake.state.length;
    const sqlBefore = r.sql.length;
    const p = await pass(r);
    expect(p.result).toEqual({ manifests: x.keys.length, rows: 0, inserted: 0, deleted: 0, complete: true, skipped: true, reasons: ["already_current"] });
    expect([p.reads, p.clickhouse, r.fake.state.length]).toEqual([0, 0, state]);
    // The skip's own statements only: locks, the pointer snapshot, siblings and the one current check; no write.
    expect(r.sql.slice(sqlBefore).some((s) => /\b(UPDATE|INSERT|DELETE)\b/i.test(String(s.sql)))).toBe(false);
  }, 120_000);

  test("through the consumer: logged as skipped, not requeued, not failed", async () => {
    const r = await indexed();
    const lines: string[] = [];
    const stdout = process.stdout.write;
    process.stdout.write = ((chunk: unknown, ...rest: unknown[]) => {
      lines.push(String(chunk));
      return (stdout as (...a: unknown[]) => boolean).call(process.stdout, chunk, ...rest);
    }) as typeof process.stdout.write;
    const reads = r.reads.count;
    let response;
    try {
      response = await r.tick(ON);
    } finally {
      process.stdout.write = stdout;
    }
    expect(response.batchItemFailures).toEqual([]);
    expect(r.requeued).toEqual([]);
    const skipped = lines.flatMap((l) => l.split("\n")).filter((l) => l.includes("search index skipped")).map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(skipped.length).toBe(1);
    expect(skipped[0]).toMatchObject({ stage: "search-index", org_id: SCOPE.orgId, session_id: SCOPE.sessionId, reason: "already_current" });
    expect(lines.some((l) => l.includes("search index reconciled"))).toBe(false);
    // The pointer stage still read the manifest; the search stage read no chunk.
    expect(r.reads.count - reads).toBeLessThanOrEqual(x.keys.length);
  }, 120_000);

  test("a pointer change (new revision) is not skipped: the pass runs, then the next one skips again", async () => {
    const r = await indexed();
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [x.keys[0]]);
    const ran = await pass(r);
    expect(ran.result.skipped).toBeUndefined();
    expect(ran.reads).toBeGreaterThan(0);
    // The directory sources still name the manifests' own revisions, which the bumped pointer no longer
    // matches: the pass runs and stays not current, so the following one runs too.
    const again = await pass(r);
    expect(again.result.skipped).toBeUndefined();
    await r.db.pg.query("UPDATE sessions.session_index SET revision = revision - 1 WHERE manifest_key = $1", [x.keys[0]]);
    expect((await pass(r)).result.complete).toBe(true);
    expect((await pass(r)).result.skipped).toBe(true);
  }, 120_000);

  test("an incomplete previous pass, an invalidated directory or sources that differ from the live set are not skipped", async () => {
    const r = await indexed();
    // A partial pass (too large, no streaming; flag off, or it would itself be skipped).
    expect(await indexSearchScope(SCOPE, r.search({ limits: TINY }))).toMatchObject({ complete: false, reasons: ["scope_too_large"] });
    const after = await pass(r);
    expect(after.result.skipped).toBeUndefined();
    expect(after.reads).toBeGreaterThan(0);
    expect((await pass(r)).result.skipped).toBe(true);
    // Readiness reset (as invalidateDirectoryEvents does).
    await r.db.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    expect((await pass(r)).result.skipped).toBeUndefined();
    expect((await pass(r)).result.skipped).toBe(true);
    // Sources that no longer equal the live pointer set (as left by a write the trigger did not see).
    await r.db.pg.exec(`UPDATE sessions.session_directory SET event_projection_sources =
      jsonb_set(event_projection_sources, ARRAY[(SELECT min(k) FROM jsonb_object_keys(event_projection_sources) k)], '"999"')
      WHERE event_projection_ready`);
    expect((await pass(r)).result.skipped).toBeUndefined();
  }, 120_000);

  test("a sibling manifest removed, or every pointer tombstoned (erase), is not skipped", async () => {
    const r = await indexed();
    expect(x.keys.length).toBeGreaterThan(1);
    await r.db.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE manifest_key = $1", [x.keys[1]]);
    expect((await pass(r)).result.skipped).toBeUndefined();
    await r.db.pg.exec("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await pass(r)).result).toMatchObject({ complete: true, reasons: ["erased"] });
  }, 120_000);

  test("a streamed scope that completed is skipped like any other", async () => {
    const r = await hostedRig(x, cleanup);
    await r.tick({});
    expect((await indexSearchScope(SCOPE, r.search({ limits: TINY, stream: UNLIMITED }))).complete).toBe(true);
    const p = await pass(r, { limits: TINY, stream: UNLIMITED });
    expect(p.result.skipped).toBe(true);
    expect([p.reads, p.clickhouse]).toEqual([0, 0]);
  }, 120_000);

  test("output facts on: skipped only when the facts state of the canonical id is complete", async () => {
    const r = await indexed();
    // No facts state row, or an incomplete one: the pass runs.
    expect((await pass(r, { outputFacts: true })).result.skipped).toBeUndefined();
    r.fake.facts.complete = 0;
    expect((await pass(r, { outputFacts: true })).result.skipped).toBeUndefined();
    r.fake.facts.complete = 1;
    const p = await pass(r, { outputFacts: true });
    expect(p.result.skipped).toBe(true);
    expect(p.reads).toBe(0);
    // One ClickHouse call: the facts state point read; no transcript_events scan.
    expect(r.fake.calls.map((c) => String(c.query))).toEqual([expect.stringContaining("output_facts_session_state")]);
  }, 120_000);

  test("busy still requeues before the check: a held scope lock answers busy, never skipped", async () => {
    const r = await indexed();
    const held = <T,>(work: (tx: SqlExecutor) => Promise<T>) =>
      r.transact((tx) => work({
        query: (sql: string, params?: unknown[]) =>
          sql.includes("pg_try_advisory_xact_lock") ? Promise.resolve({ rows: [{ ok: false }] }) as never : tx.query(sql, params),
      }));
    const p = await pass(r, { transact: held });
    expect(p.result).toMatchObject({ busy: true, reasons: ["scope_busy"] });
    expect(p.result.skipped).toBeUndefined();
  }, 120_000);
});

describe("SEARCH_INDEX_SKIP_CURRENT_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });

  test("unset: no field, no deps flag; true: the search deps carry skipCurrent; it needs the search stage", async () => {
    expect("skipCurrentEnabled" in lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" })).toBe(false);
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const env = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_CURRENT_ENABLED: value }) });
      expect(env.skipCurrentEnabled === true).toBe(expected);
      const rt = runtimeFromSecret(env, secret, "synthetic-generation");
      try {
        expect("skipCurrent" in rt.deps.search!).toBe(expected);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    expect(() => lambdaEnv({ ...base, SEARCH_INDEX_SKIP_CURRENT_ENABLED: "true" })).toThrow("SEARCH_INDEX_SKIP_CURRENT_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
    for (const bad of ["1", "TRUE", "on"]) expect(() => searchIndexSkipCurrentFromEnv({ SEARCH_INDEX_SKIP_CURRENT_ENABLED: bad })).toThrow("must be true or false");
  });
});
