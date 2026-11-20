/**
 * SEARCH_INDEX_SKIP_CURRENT_ENABLED=true on the hosted path (search-index-hosted-harness.ts: processBatch, the
 * ClickHouse binding, a migrated PGlite with the session directory and its triggers): a pass that a complete
 * pass already covers returns before any object read or ClickHouse call, and every case that must run still
 * runs. Synthetic, secret-free data only.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { indexSearchScope, type SearchIndexDeps } from "../search-index.ts";
import { lambdaEnv, searchIndexSkipCurrentFromEnv, searchSkipCurrentDeps } from "../lambda.ts";
import { SCOPE, exportScope, hostedRig, type ExportedScope } from "./search-index-hosted-harness.ts";

const TINY = { maxScopeBytes: 1, maxScopeEvents: 1 };
const ON: Partial<SearchIndexDeps> = { skipCurrent: true };

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const c of cleanup.splice(0).reverse()) await c();
});

let x: ExportedScope;
beforeAll(async () => {
  x = await exportScope(41, 2, 3);
}, 120_000);
afterAll(() => x?.dispose());

/** A rig whose scope a complete pass (flag off) already indexed. */
async function indexed() {
  const r = await hostedRig(x, cleanup);
  await r.tick({});
  expect((await indexSearchScope(SCOPE, r.search())).complete).toBe(true);
  const { rows } = await r.pg.query<{ ready: boolean; version: number }>(
    "SELECT event_projection_ready AS ready, event_projection_version AS version FROM sessions.session_directory WHERE session_id=$1",
    [SCOPE.sessionId],
  );
  // The precondition every skip relies on: a complete pass leaves the directory row ready at version 1.
  expect(rows).toEqual([{ ready: true, version: 1 }]);
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

  test("flag off: the same repeated pass is not skipped and reads the source again", async () => {
    const r = await indexed();
    const p = await pass(r, { skipCurrent: false });
    expect(p.result.skipped).toBeUndefined();
    expect(p.result.complete).toBe(true);
    expect(p.reads).toBeGreaterThan(0);
    expect(p.clickhouse).toBeGreaterThan(0);
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
    r.fake.calls.length = 0;
    let response;
    try {
      response = await r.tick(ON);
    } finally {
      process.stdout.write = stdout;
    }
    expect(response.batchItemFailures).toEqual([]);
    expect(r.requeued).toEqual([]);
    const skipped = lines.flatMap((l) => l.split("\n")).filter((l) => l.includes("search index skipped")).map((l) => JSON.parse(l) as Record<string, unknown>);
    // One search pass per scope per batch (consumer.ts groups the batch's manifests by scope).
    expect(skipped.length).toBe(1);
    expect(skipped[0]).toMatchObject({ stage: "search-index", org_id: SCOPE.orgId, session_id: SCOPE.sessionId, reason: "already_current" });
    expect(lines.some((l) => l.includes("search index reconciled"))).toBe(false);
    expect(r.fake.calls.length).toBe(0);
    // The pointer stage still read the manifests; the search stage read nothing: the skipped tick reads
    // exactly what a flag-off tick reads minus what a flag-off search pass alone reads.
    const onTick = r.reads.count - reads;
    const offPass = (await pass(r, { skipCurrent: false })).reads;
    const before = r.reads.count;
    await r.tick({});
    const offTick = r.reads.count - before;
    expect(offPass).toBeGreaterThan(0);
    expect(onTick).toBe(offTick - offPass);
  }, 120_000);

  test("a pointer change (new revision) is not skipped; once a complete pass covers it again, it skips again", async () => {
    const r = await indexed();
    await r.pg.query("UPDATE sessions.session_index SET revision = revision + 1 WHERE manifest_key = $1", [x.keys[0]]);
    const ran = await pass(r);
    expect(ran.result.skipped).toBeUndefined();
    expect(ran.reads).toBeGreaterThan(0);
    // The manifests still carry their own revision, which the bumped pointer no longer matches: the pass
    // runs and stays not current, so the following one runs too.
    const again = await pass(r);
    expect(again.result.skipped).toBeUndefined();
    await r.pg.query("UPDATE sessions.session_index SET revision = revision - 1 WHERE manifest_key = $1", [x.keys[0]]);
    expect((await pass(r)).result.complete).toBe(true);
    expect((await pass(r)).result.skipped).toBe(true);
  }, 120_000);

  test("an incomplete previous pass, a readiness reset or sources that differ from the live set are not skipped", async () => {
    const r = await indexed();
    // A partial pass (too large; flag off, or it would itself be skipped).
    expect(await indexSearchScope(SCOPE, r.search({ limits: TINY }))).toMatchObject({ complete: false, reasons: ["scope_too_large"] });
    const after = await pass(r);
    expect(after.result.skipped).toBeUndefined();
    expect(after.reads).toBeGreaterThan(0);
    expect((await pass(r)).result.skipped).toBe(true);
    // Readiness reset (as invalidateDirectoryEvents does).
    await r.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false");
    expect((await pass(r)).result.skipped).toBeUndefined();
    expect((await pass(r)).result.skipped).toBe(true);
    // Sources that no longer equal the live pointer set (as left by a write the trigger did not see).
    await r.pg.exec(`UPDATE sessions.session_directory SET event_projection_sources =
      jsonb_set(event_projection_sources, ARRAY[(SELECT min(k) FROM jsonb_object_keys(event_projection_sources) k)], '"999"')
      WHERE event_projection_ready`);
    const differ = await pass(r);
    expect(differ.result.skipped).toBeUndefined();
    expect(differ.reads).toBeGreaterThan(0);
  }, 120_000);

  test("a projection version other than 1 is not skipped", async () => {
    const r = await indexed();
    // As the schema allows it: version 0 is never ready (session_directory_events_ready).
    await r.pg.exec("UPDATE sessions.session_directory SET event_projection_ready = false, event_projection_version = 0");
    expect((await pass(r)).result.skipped).toBeUndefined();
    expect((await pass(r)).result.skipped).toBe(true);
    // The predicate itself, in this throwaway PGlite only: ready with version 0 is still not current.
    await r.pg.exec("ALTER TABLE sessions.session_directory DROP CONSTRAINT session_directory_events_ready");
    await r.pg.exec("UPDATE sessions.session_directory SET event_projection_version = 0 WHERE event_projection_ready");
    const p = await pass(r);
    expect(p.result.skipped).toBeUndefined();
    expect(p.reads).toBeGreaterThan(0);
  }, 120_000);

  test("a deleted directory row is not skipped", async () => {
    const r = await indexed();
    // The predicate itself, in this throwaway PGlite only: the tombstone trigger off, so the row keeps
    // ready, version 1 and its sources while deleted.
    await r.pg.exec("ALTER TABLE sessions.session_directory DISABLE TRIGGER session_directory_events_tombstone");
    await r.pg.exec("UPDATE sessions.session_directory SET deleted_at = now()");
    const { rows } = await r.pg.query<{ ready: boolean }>("SELECT event_projection_ready AS ready FROM sessions.session_directory");
    expect(rows).toEqual([{ ready: true }]);
    const p = await pass(r);
    expect(p.result.skipped).toBeUndefined();
    expect(p.reads).toBeGreaterThan(0);
  }, 120_000);

  test("a deleted directory row through its tombstone trigger is not skipped", async () => {
    const r = await indexed();
    await r.pg.exec("UPDATE sessions.session_directory SET deleted_at = now()");
    expect((await pass(r)).result.skipped).toBeUndefined();
  }, 120_000);

  test("a sibling manifest removed, or every pointer tombstoned (erase), is not skipped", async () => {
    const r = await indexed();
    expect(x.keys.length).toBeGreaterThan(1);
    await r.pg.query("UPDATE sessions.session_index SET deleted_at = now() WHERE manifest_key = $1", [x.keys[1]]);
    expect((await pass(r)).result.skipped).toBeUndefined();
    await r.pg.exec("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await pass(r)).result).toMatchObject({ complete: true, reasons: ["erased"] });
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

  test("unset: no field, no deps flag; true: the search deps carry skipCurrent; it needs the search stage", () => {
    expect("skipCurrentEnabled" in lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1" })).toBe(false);
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const env = lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", ...(value === undefined ? {} : { SEARCH_INDEX_SKIP_CURRENT_ENABLED: value }) });
      expect(env.skipCurrentEnabled === true).toBe(expected);
      expect("skipCurrent" in searchSkipCurrentDeps(env)).toBe(expected);
    }
    expect(() => lambdaEnv({ ...base, SEARCH_INDEX_SKIP_CURRENT_ENABLED: "true" })).toThrow("SEARCH_INDEX_SKIP_CURRENT_ENABLED=true needs SEARCH_INDEX_ENABLED=1");
    for (const bad of ["1", "TRUE", " true", "on", "yes"]) {
      expect(() => searchIndexSkipCurrentFromEnv({ SEARCH_INDEX_SKIP_CURRENT_ENABLED: bad })).toThrow("must be true or false");
      expect(() => lambdaEnv({ ...base, SEARCH_INDEX_ENABLED: "1", SEARCH_INDEX_SKIP_CURRENT_ENABLED: bad })).toThrow("SEARCH_INDEX_SKIP_CURRENT_ENABLED must be true or false");
    }
  });
});
