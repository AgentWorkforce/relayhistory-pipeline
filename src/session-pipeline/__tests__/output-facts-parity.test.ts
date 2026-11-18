/**
 * Staged equals whole (PGlite, always runs): the staged output facts of a 062110 run (migration
 * 20261105062140, output-facts-store.ts stageOutputFactsBatch / finishStagedOutputFacts) leave the
 * facts and state tables exactly as the whole-set path (output_facts_reconcile) does for the same
 * read, for every batch split, across two runs whose facts change, and across a lost lease.
 *
 * Differential parity: relayhistory's own `getSessionOutput` SQL (lib/session-output.ts
 * @ c7de2163, transcribed with positional parameters) over Neon-shaped tables on a
 * real Postgres, against `readSessionOutput` over the facts the producer writes for
 * the same rows on a real ClickHouse. Runs only with BOTH
 * F4_TEST_POSTGRES_URL and READ_API_TEST_CLICKHOUSE_URL (throwaway containers).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { createClickHouseClient, type ClickHouseClient } from "../vendor/search-index/clickhouse.ts";
import { outputFactsSchemaStatements } from "../output-facts-schema.ts";
import { outputFactsFromScope, reconcileOutputFacts } from "../output-facts.ts";
import { readSessionOutput, type SessionOutputFilter } from "../../session-read-api/output-query.ts";
import { outputFixture, OUTPUT_FIXTURE_SCOPE } from "./output-facts-fixture.ts";
import type { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import {
  advanceToFinal, as, chunksOf, claim, clearFacts, code, factsChunk, factsPass, factsSnapshot, finalStep, keyOf, stagedFactsDatabase, stagedPass,
  stagedRows, wantPhase, wholePass, type Read,
} from "./staged-facts-harness.ts";
import { StagedRunRestart } from "../staged-reconcile.ts";

const PG_URL = process.env.F4_TEST_POSTGRES_URL;
const CH_URL = process.env.READ_API_TEST_CLICKHOUSE_URL;
const suite = PG_URL && CH_URL ? describe : describe.skip;

const DDL = `
CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, source text, session_id text, machine_id text,
  event_id text, kind text, type text, ts timestamptz, actor_role text, content text, request_key text,
  delivery_record_id text, record jsonb, tool_name text);
CREATE TABLE sessions.session_commit_links (org_id text, workspace_id text, source text, session_id text, commit_sha text,
  linked_at timestamptz, received_at timestamptz, numstat jsonb);
CREATE TABLE sessions.session_outcomes (org_id text, workspace_id text, source text, session_id text, commit_sha text,
  shipped_at timestamptz, created_at timestamptz, numstat_json jsonb);
CREATE TABLE sessions.conversation_turns (id text, org_id text, workspace_id text, source text, session_id text,
  ts timestamptz, role text, content text);`;

// $1 org, $2 workspace[], $3 since|null, $4 until|null, $5 source|null, $6 session|null
const scope = `org_id = $1 AND workspace_id = ANY($2::text[]) AND ($5::text IS NULL OR source = $5) AND ($6::text IS NULL OR session_id = $6)`;
const time = (c: string) => `AND ($3::timestamptz IS NULL OR ${c} >= $3) AND ($4::timestamptz IS NULL OR ${c} <= $4)`;
const commitOutput = String.raw`(?:^|\n)\[[^\]\n]+ ([0-9a-f]{7,40})\][^\n]*\n[ \t]*[0-9]+ files? changed(?:, ([0-9]+) insertions?\(\+\))?(?:, ([0-9]+) deletions?\(-\))?`;
const commitCommand = String.raw`\mgit\M[^\n]*\mcommit\M`;
const binaryStat = `jsonb_typeof(value->'path') = 'string' AND value->'additions' = 'null'::jsonb AND value->'deletions' = 'null'::jsonb`;
const A = `COALESCE(value->>'additions', value->>'added')`;
const R = `COALESCE(value->>'deletions', value->>'removed', value->>'deleted')`;
const RHC_SQL = `
WITH events AS MATERIALIZED (SELECT * FROM sessions.convergence_events WHERE ${scope} ${time("ts")}
), messages AS (
  SELECT DISTINCT source, session_id,
    COALESCE(NULLIF(request_key, ''), NULLIF(record->'payload'->>'message_id', ''), delivery_record_id, machine_id || ':' || event_id) AS message_id
  FROM events
  WHERE actor_role = 'assistant' AND btrim(COALESCE(content, '')) <> '' AND kind IN ('session_event', 'assistant', 'text')
    AND (record->'payload'->>'kind' = 'text' OR (record->'payload'->>'kind' IS NULL AND type IN ('assistant', 'text')))
), legacy_messages AS (
  SELECT t.id FROM sessions.conversation_turns t
  WHERE ${scope} ${time("ts")} AND role = 'assistant' AND btrim(content) <> ''
    AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.source = t.source AND m.session_id = t.session_id)
), commit_candidates AS (
  SELECT lower(commit_sha) AS sha, COALESCE(linked_at, received_at) AS ts, numstat AS stats FROM sessions.session_commit_links WHERE ${scope}
  UNION ALL
  SELECT lower(commit_sha) AS sha, COALESCE(shipped_at, created_at) AS ts, numstat_json AS stats FROM sessions.session_outcomes WHERE ${scope}
  UNION ALL
  SELECT matched.parts[1], e.ts, jsonb_build_object('added', COALESCE(matched.parts[2], '0'), 'deleted', COALESCE(matched.parts[3], '0'))
  FROM events e CROSS JOIN LATERAL regexp_matches(e.content, '${commitOutput}', 'g') AS matched(parts)
  WHERE e.actor_role = 'tool_result' AND EXISTS (
    SELECT 1 FROM sessions.convergence_events call
    WHERE call.org_id = e.org_id AND call.workspace_id = e.workspace_id AND call.source = e.source AND call.session_id = e.session_id
      AND call.kind = 'tool_call' AND call.ts <= e.ts
      AND NULLIF(call.record->'payload'->>'tool_use_id', '') = NULLIF(e.record->'payload'->>'tool_use_id', '')
      AND (NULLIF(e.record->'payload'->>'parent_id', '') IS NULL
        OR e.record->'payload'->>'parent_id' = call.record->'payload'->>'message_id'
        OR NULLIF(e.record->'payload'->>'message_id', '') = NULLIF(call.record->'payload'->>'message_id', ''))
      AND (NULLIF(e.record->'payload'->>'turn_id', '') IS NULL OR EXISTS (
        SELECT 1 FROM sessions.convergence_events parent
        WHERE parent.org_id = call.org_id AND parent.workspace_id = call.workspace_id AND parent.source = call.source
          AND parent.session_id = call.session_id AND parent.kind = 'session_event'
          AND NULLIF(parent.record->'payload'->>'message_id', '') = NULLIF(call.record->'payload'->>'message_id', '')
          AND parent.record->'payload'->>'turn_id' = e.record->'payload'->>'turn_id'))
    HAVING count(DISTINCT jsonb_build_array(call.record->'payload'->>'message_id', call.tool_name, call.record->'payload'->>'args_json',
      CASE WHEN NULLIF(call.record->'payload'->>'message_id', '') IS NULL THEN call.ts END)) = 1
      AND bool_and(COALESCE(call.tool_name IN ('exec_command', 'exec', 'Bash', 'bash', 'shell', 'run_shell_command')
        AND call.record->'payload'->>'args_json' ~ '${commitCommand}', false)))
), commit_hashes AS MATERIALIZED (SELECT DISTINCT sha FROM commit_candidates
), short_lengths AS (SELECT DISTINCT length(sha) AS size FROM commit_hashes WHERE length(sha) BETWEEN 1 AND 39
), unique_prefixes AS MATERIALIZED (
  SELECT left(full_id.sha, short.size) AS prefix, min(full_id.sha) AS sha FROM commit_hashes full_id CROSS JOIN short_lengths short
  WHERE length(full_id.sha) = 40 GROUP BY left(full_id.sha, short.size) HAVING count(*) = 1
), canonical_commits AS (
  SELECT COALESCE(prefix.sha, c.sha) AS sha, c.ts, c.stats FROM commit_candidates c LEFT JOIN unique_prefixes prefix ON prefix.prefix = c.sha
), stat_entries AS (
  SELECT sha, ts, CASE
    WHEN jsonb_typeof(stats) = 'array' THEN stats
    WHEN jsonb_typeof(stats) = 'object' AND EXISTS (SELECT 1 FROM jsonb_each(stats) field
      WHERE field.key IN ('added', 'deleted', 'removed', 'additions', 'deletions') AND jsonb_typeof(field.value) NOT IN ('object', 'array'))
      THEN jsonb_build_array(stats)
    WHEN jsonb_typeof(stats) = 'object' THEN (SELECT jsonb_agg(value) FROM jsonb_each(stats))
  END AS entries FROM canonical_commits
), normalized_stats AS (
  SELECT sha, ts,
    CASE WHEN entries IS NOT NULL THEN (SELECT CASE WHEN COALESCE(bool_and(COALESCE(${A} ~ '^[0-9]{1,12}$' OR ${A} = '-' OR (${binaryStat}), false)), true)
      THEN COALESCE(sum(CASE WHEN ${A} ~ '^[0-9]{1,12}$' THEN ${A}::bigint ELSE 0 END), 0) END FROM jsonb_array_elements(entries)) END AS added,
    CASE WHEN entries IS NOT NULL THEN (SELECT CASE WHEN COALESCE(bool_and(COALESCE(${R} ~ '^[0-9]{1,12}$' OR ${R} = '-' OR (${binaryStat}), false)), true)
      THEN COALESCE(sum(CASE WHEN ${R} ~ '^[0-9]{1,12}$' THEN ${R}::bigint ELSE 0 END), 0) END FROM jsonb_array_elements(entries)) END AS removed
  FROM stat_entries
), commits AS (SELECT sha, min(ts) AS ts, max(added) AS added, max(removed) AS removed FROM normalized_stats GROUP BY sha)
SELECT ((SELECT count(*) FROM messages) + (SELECT count(*) FROM legacy_messages)) AS messages,
  count(*) AS commits, COALESCE(sum(added), 0) AS added, COALESCE(sum(removed), 0) AS removed,
  count(*) FILTER (WHERE added IS NULL OR removed IS NULL) AS missing
FROM commits WHERE true ${time("ts")}`;

suite("Output parity: relayhistory SQL on Postgres == facts query on ClickHouse", () => {
  let db: pg.Client, admin: ClickHouseClient, ch: ClickHouseClient;
  const schema = `f4_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const database = `output_parity_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
  const s = OUTPUT_FIXTURE_SCOPE;

  // Neon holds the session under the id SP resolves it to (the canonical one); the SP fixture's
  // events carry an owner/alias id ("s1"), so this also proves every fact lands on the canonical key.
  async function load(fx: ReturnType<typeof outputFixture>) {
    await db.query("TRUNCATE sessions.convergence_events, sessions.session_commit_links, sessions.session_outcomes, sessions.conversation_turns".replaceAll("sessions.", `${schema}.`));
    for (const { table, row: r } of fx.projectionRows) {
      if (table === "convergence_events")
        await db.query(`INSERT INTO ${schema}.convergence_events VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, [
          s.orgId, s.workspaceId, r.source, s.canonicalId, r.machine_id, r.event_id, r.kind, r.type, r.ts, r.actor_role, r.content, r.request_key,
          r.delivery_record_id, JSON.stringify(r.record), r.tool_name,
        ]);
      else if (table === "session_commit_links")
        await db.query(`INSERT INTO ${schema}.session_commit_links VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [
          s.orgId, s.workspaceId, r.source, s.canonicalId, r.commit_sha, r.linked_at, r.received_at, JSON.stringify(r.numstat),
        ]);
    }
    for (const rec of fx.records) {
      const p = rec.payload!;
      if (rec.kind === "legacy_session_outcome")
        await db.query(`INSERT INTO ${schema}.session_outcomes VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [
          s.orgId, s.workspaceId, p.source, s.canonicalId, p.commit_sha, p.shipped_at, p.created_at, JSON.stringify(p.numstat_json ?? null),
        ]);
      else if (rec.kind === "legacy_conversation_turn")
        await db.query(`INSERT INTO ${schema}.conversation_turns VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [
          p.id, s.orgId, s.workspaceId, p.source, s.canonicalId, p.ts, p.role, p.content,
        ]);
    }
    await ch.command(`TRUNCATE TABLE IF EXISTS transcript_event_facts`);
    await ch.command(`TRUNCATE TABLE IF EXISTS session_output_facts`);
    await reconcileOutputFacts(ch, s, outputFactsFromScope(s, fx, 1), { maxRows: 100_000, insertBatch: 1000 });
  }

  async function both(filter: SessionOutputFilter) {
    const { rows: [r] } = await db.query(RHC_SQL.replaceAll("sessions.", `${schema}.`), [s.orgId, [s.workspaceId], filter.since?.toISOString() ?? null, filter.until?.toISOString() ?? null, filter.source ?? null, filter.sessionId ?? null]);
    const neon = { agentMessages: Number(r.messages), commits: Number(r.commits), linesAdded: Number(r.added), linesRemoved: Number(r.removed), commitsWithoutLineCounts: Number(r.missing) };
    const facts = await readSessionOutput(ch, { orgId: s.orgId, workspaceIds: [s.workspaceId] }, filter);
    return { neon, facts };
  }

  beforeAll(async () => {
    db = new pg.Client({ connectionString: PG_URL });
    await db.connect();
    await db.query(`CREATE SCHEMA ${schema}`);
    await db.query(`SET search_path = ${schema}`);
    await db.query(DDL.replaceAll("sessions.", `${schema}.`));
    admin = createClickHouseClient({ url: CH_URL!, user: process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default", password: process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "", database: "default" });
    await admin.command(`CREATE DATABASE ${database}`);
    ch = createClickHouseClient({ url: CH_URL!, user: process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default", password: process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "", database });
    for (const st of outputFactsSchemaStatements()) await ch.command(st);
  }, 60_000);
  afterAll(async () => {
    await db?.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await db?.end();
    await admin?.command(`DROP DATABASE IF EXISTS ${database}`);
  }, 60_000);

  // The transcribed SQL names `sessions.`; the test schema stands in for it.
  const run = async (filter: SessionOutputFilter) => {
    const out = await both(filter);
    expect(out.facts).toEqual(out.neon);
    return out;
  };
  const windows: SessionOutputFilter[] = [
    {},
    { since: new Date("2026-11-05T00:00:00Z"), until: new Date("2026-11-05T23:59:59.999Z") },
    { since: new Date("2026-11-05T10:00:08Z"), until: new Date("2026-11-05T10:00:09Z") },
    { since: new Date("2026-11-05T09:30:00Z") },
    { until: new Date("2026-11-05T09:30:00Z") },
    { since: new Date("2026-11-07T00:00:00Z"), until: new Date("2026-11-08T00:00:00Z") },
    { source: "codex", sessionId: OUTPUT_FIXTURE_SCOPE.canonicalId },
    { source: "codex", sessionId: "s1" },
    { source: "claude" },
  ];

  test("fixture: every window and filter agrees", async () => {
    await load(outputFixture());
    for (const w of windows) await run(w);
    // Sanity: the day is not trivially empty on either side.
    const day = await both(windows[1]!);
    expect(day.neon.commits).toBeGreaterThan(0);
    expect(day.neon.agentMessages).toBeGreaterThan(0);
  }, 60_000);

  test("no window messages: the legacy turn counts on both sides", async () => {
    const fx = outputFixture();
    fx.projectionRows = fx.projectionRows.filter((r) => r.row.actor_role !== "assistant");
    await load(fx);
    for (const w of windows) await run(w);
  }, 60_000);

  test("ambiguous prefix stays unresolved; NULL numstat counts as without line counts", async () => {
    const fx = outputFixture();
    const link = fx.projectionRows.find((r) => r.table === "session_commit_links")!;
    fx.projectionRows.push({ table: "session_commit_links", row: { ...link.row, record_id: "link-2", commit_sha: "abc1234" + "e".repeat(33), numstat: null } });
    await load(fx);
    for (const w of windows) await run(w);
  }, 60_000);
});

// ---- staged equals whole (PGlite) -------------------------------------------------------------

const STAGED_SCOPE = { orgId: OUTPUT_FIXTURE_SCOPE.orgId, workspaceId: OUTPUT_FIXTURE_SCOPE.workspaceId, harness: OUTPUT_FIXTURE_SCOPE.harness, sessionId: "s1", canonicalId: OUTPUT_FIXTURE_SCOPE.canonicalId };

/** The fixture with its facts changed: a message, the commit link and the legacy turn gone, one commit result rewritten. */
function changedFixture(): Read {
  const f = outputFixture();
  const rows = f.projectionRows
    .filter((r) => r.table !== "session_commit_links")
    .filter((r) => !(r.table === "convergence_events" && (r.row.record as { payload?: { message_id?: string } })?.payload?.message_id === "msg-2"))
    .map((r) => (typeof r.row.content === "string" && r.row.content.includes("abc1234") ? { ...r, row: { ...r.row, content: "[main abc1234] fix\n 2 files changed, 9 insertions(+)" } } : r));
  return { ...f, projectionRows: rows, records: f.records.filter((r) => r.kind !== "legacy_conversation_turn") };
}

describe("staged output facts equal the whole-set path (062140)", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = await stagedFactsDatabase();
  }, 300_000);
  afterAll(async () => {
    await db?.close();
  });

  const whole = async (passes: Array<[Read, number]>) => {
    await clearFacts(db);
    const out = [];
    for (const [read, v] of passes) out.push(await wholePass(db, STAGED_SCOPE, read, v));
    return { out, snap: await factsSnapshot(db) };
  };
  const staged = async (passes: Array<[Read, number]>, size: number, recordsFirst = false) => {
    await clearFacts(db);
    const out = [];
    for (const [read, v] of passes) out.push(await stagedPass(db, STAGED_SCOPE, read, v, size, recordsFirst));
    return { out, snap: await factsSnapshot(db) };
  };
  const counts = (r: { inserted: number; deleted: number; complete: boolean; applied: boolean; reasons: string[] }) =>
    ({ inserted: r.inserted, deleted: r.deleted, complete: r.complete, applied: r.applied, reasons: r.reasons });

  test("one run: every batch split (a commit call and its result in different batches included) gives the same rows and counts", async () => {
    const w = await whole([[outputFixture(), 1]]);
    expect(w.snap.events.length).toBeGreaterThan(10);
    expect(w.snap.facts.length).toBeGreaterThan(5);
    expect(w.out[0]!.complete).toBe(true);
    for (const size of [1, 2, 3, 7, 100])
      for (const recordsFirst of [false, true]) {
        const s = await staged([[outputFixture(), 1]], size, recordsFirst);
        expect(s.snap, `size ${size}`).toEqual(w.snap);
        expect(counts(s.out[0]!), `size ${size}`).toEqual(counts(w.out[0]!));
        expect(await stagedRows(db)).toBe(0);
      }
  });

  test("two runs whose facts change: the second deletes and rewrites as the whole path does; an older version is refused alike", async () => {
    const w = await whole([[outputFixture(), 1], [changedFixture(), 2]]);
    expect(w.out[1]!.deleted).toBeGreaterThan(0);
    for (const size of [1, 4]) {
      const s = await staged([[outputFixture(), 1], [changedFixture(), 2]], size);
      expect(s.snap).toEqual(w.snap);
      expect(s.out.map(counts)).toEqual(w.out.map(counts));
    }
    // A pass older than the stored version: nothing written on either path.
    const before = await factsSnapshot(db);
    const stale = await stagedPass(db, STAGED_SCOPE, outputFixture(), 1, 3);
    expect(counts(stale)).toEqual(counts(await wholePass(db, STAGED_SCOPE, outputFixture(), 1)));
    expect(stale.applied).toBe(false);
    expect(await factsSnapshot(db)).toEqual(before);
  });

  test("an incomplete read inserts and never deletes, on both paths", async () => {
    const incomplete = { ...changedFixture(), complete: false };
    const w = await whole([[outputFixture(), 1], [incomplete, 2]]);
    expect(w.out[1]!.deleted).toBe(0);
    const s = await staged([[outputFixture(), 1], [incomplete, 2]], 2);
    expect(s.snap).toEqual(w.snap);
    expect(s.out.map(counts)).toEqual(w.out.map(counts));
  });

  test("a lost lease mid facts pass: the old holder's next chunk is RH062 and stages nothing; the taker's pass from chunk 0 gives the whole path's result", async () => {
    const w = await whole([[outputFixture(), 1]]);
    await clearFacts(db);
    const chunks = chunksOf(outputFixture(), 3);
    const a = (await claim(db, STAGED_SCOPE, "mf-1", 1))!;
    await wantPhase(db, a, STAGED_SCOPE, null, 1);
    await factsChunk(db, a, STAGED_SCOPE, chunks[0]!, { seq: 0, last: false }, 1);
    await factsChunk(db, a, STAGED_SCOPE, chunks[1]!, { seq: 1, last: false }, 1);
    await db.exec("UPDATE sessions.search_reconcile_runs_v1 SET lease_expires_at = clock_timestamp() - interval '1 second'");
    const b = (await claim(db, STAGED_SCOPE, "mf-1", 1))!;
    expect([b.runId, b.phase, b.cursor, b.restarted]).toEqual([a.runId, "want", keyOf(2), false]);
    const staged0 = await stagedRows(db);
    expect(await code(factsChunk(db, a, STAGED_SCOPE, chunks[2]!, { seq: 2, last: false }, 1))).toBe("RH062");
    expect(await stagedRows(db)).toBe(staged0);
    // The taker's attempt delivers the facts pass again from chunk 0, which drops the old holder's partial stage.
    await wantPhase(db, b, STAGED_SCOPE, chunks, 1);
    await advanceToFinal(db, b, true);
    expect(await code(finalStep(db, a, STAGED_SCOPE, { sourceComplete: true, sourceVersion: 1 }))).toBe("RH062");
    const r = await finalStep(db, b, STAGED_SCOPE, { sourceComplete: true, sourceVersion: 1 });
    expect(await factsSnapshot(db)).toEqual(w.snap);
    expect(counts(r)).toEqual(counts(w.out[0]!));
  });

  test("a stage that missed part of the facts pass is never reconciled: StagedRunRestart, nothing written; the next attempt converges", async () => {
    const w = await whole([[outputFixture(), 1], [changedFixture(), 2]]);
    await clearFacts(db);
    await wholePass(db, STAGED_SCOPE, outputFixture(), 1);
    const before = await factsSnapshot(db);
    const chunks = chunksOf(changedFixture(), 3);
    const run = (await claim(db, STAGED_SCOPE, "mf-2", 2))!;
    await wantPhase(db, run, STAGED_SCOPE, null, 2);
    await factsPass(db, run, STAGED_SCOPE, chunks, 2, { from: 1 }); // facts turned on after chunk 0
    await advanceToFinal(db, run, true);
    const err = await as(db, (tx) => import("../output-facts-store.ts").then((m) => m.finishStagedOutputFacts(tx, run, STAGED_SCOPE, { sourceComplete: true, sourceVersion: 2, at: new Date() }))).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(StagedRunRestart);
    expect((err as StagedRunRestart).reason).toBe("output_facts_stage_incomplete");
    expect(await factsSnapshot(db)).toEqual(before);
    // The module abandons with that reason: the stage goes with the run's; the next attempt delivers the whole pass.
    await as(db, (tx) => tx.query("SELECT sessions.search_reconcile_abandon_v1($1::bigint,$2::uuid,'output_facts_stage_incomplete')", [run.runId, run.nonce]));
    expect(await stagedRows(db)).toBe(0);
    const again = (await claim(db, STAGED_SCOPE, "mf-2", 2))!;
    expect([again.runId, again.phase, again.cursor]).toEqual([run.runId, "want", null]);
    await wantPhase(db, again, STAGED_SCOPE, chunks, 2);
    await advanceToFinal(db, again, true);
    await finalStep(db, again, STAGED_SCOPE, { sourceComplete: true, sourceVersion: 2 });
    expect(await factsSnapshot(db)).toEqual(w.snap);
  });

  test("a large scope in large chunks (one stage call per chunk): still the whole path's rows", async () => {
    const big = outputFixture();
    const extra = big.projectionRows.filter((r) => r.table === "convergence_events").flatMap((r) =>
      Array.from({ length: 80 }, (_, k) => ({ ...r, row: { ...r.row, event_id: `${String(r.row.event_id)}-x${k}` } })));
    const read = { ...big, projectionRows: [...big.projectionRows, ...extra] };
    expect(read.projectionRows.length).toBeGreaterThan(1200);
    const w = await whole([[read, 1]]);
    for (const size of [5000, 333]) {
      const s = await staged([[read, 1]], size);
      expect(s.snap, `size ${size}`).toEqual(w.snap);
      expect(counts(s.out[0]!)).toEqual(counts(w.out[0]!));
    }
  });

  test("order does not matter: the read shuffled and chunked differently gives the whole path's rows (the facts pass is set-equal, not order-equal, to the whole read)", async () => {
    const w = await whole([[outputFixture(), 1], [changedFixture(), 2]]);
    // A deterministic shuffle (an LCG), several seeds and chunk sizes; records spread across chunks too.
    const shuffled = <T,>(xs: T[], seed: number) => {
      const out = [...xs];
      let x = seed;
      for (let i = out.length - 1; i > 0; i--) {
        x = (x * 1103515245 + 12345) % 2147483648;
        const j = x % (i + 1);
        [out[i], out[j]] = [out[j]!, out[i]!];
      }
      return out;
    };
    for (const [seed, size] of [[1, 1], [7, 2], [42, 5], [99, 100]] as const) {
      await clearFacts(db);
      const out = [];
      for (const [read, v] of [[outputFixture(), 1], [changedFixture(), 2]] as const) {
        const rows = shuffled(read.projectionRows, seed);
        const records = shuffled(read.records, seed + 1);
        const n = Math.max(1, Math.ceil(rows.length / size));
        // Records go one per chunk from the end, so they interleave with the rows.
        const chunks = Array.from({ length: n }, (_, i) => ({ projectionRows: rows.slice(i * size, (i + 1) * size), records: records.filter((_r, j) => (n - 1 - (j % n)) === i) }));
        const run = (await claim(db, STAGED_SCOPE, `mf-${v}-${seed}`, v))!;
        await wantPhase(db, run, STAGED_SCOPE, chunks, v);
        await advanceToFinal(db, run, true);
        out.push(await finalStep(db, run, STAGED_SCOPE, { sourceComplete: read.complete, sourceVersion: v }));
      }
      expect(await factsSnapshot(db), `seed ${seed} size ${size}`).toEqual(w.snap);
      expect(out.map(counts)).toEqual(w.out.map(counts));
    }
  });

  test("a byte-identical legacy record seen twice is one staged item, and the whole path gives the same facts with or without the duplicate", async () => {
    const dup = outputFixture();
    dup.records = [...dup.records, ...dup.records.filter((r) => r.kind !== "legacy_scope")];
    const w = await whole([[outputFixture(), 1]]);
    const wd = await whole([[dup, 1]]);
    expect(wd.snap).toEqual(w.snap);
    const s = await staged([[dup, 1]], 3);
    expect(s.snap).toEqual(w.snap);
  });

  test("a malformed event: both paths are incomplete and delete nothing; the staged one skips only that event (a superset of the whole path's rows)", async () => {
    const bad = outputFixture();
    bad.projectionRows[2] = { ...bad.projectionRows[2]!, row: { ...bad.projectionRows[2]!.row, ts: "not a time" } };
    await whole([[outputFixture(), 1]]);
    const w = await wholePass(db, STAGED_SCOPE, bad, 2);
    const ws = await factsSnapshot(db);
    await clearFacts(db);
    await wholePass(db, STAGED_SCOPE, outputFixture(), 1);
    const s = await stagedPass(db, STAGED_SCOPE, bad, 2, 3);
    const ss = await factsSnapshot(db);
    for (const r of [w, s]) expect([r.complete, r.deleted, r.reasons]).toEqual([false, 0, ["output_facts_invalid_time"]]);
    const ids = (rows: string[]) => new Set(rows.map((j) => (JSON.parse(j) as { row_id: string }).row_id));
    for (const k of ["events", "facts"] as const) for (const id of ids(ws[k])) expect(ids(ss[k]).has(id)).toBe(true);
  });
});
