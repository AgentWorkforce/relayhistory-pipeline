/**
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
