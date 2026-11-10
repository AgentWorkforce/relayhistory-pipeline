/**
 * Differential parity for the digest's deterministic half: relayhistory's own buildDailyDigest
 * SQL (lib/digest.ts computeStats / computeFileStats / computeOutcomes / project and epic
 * rollups, lib/recall.ts sessionRowsFromEvents; @ c7de2163, transcribed with positional
 * parameters) over Neon-shaped rows on a real Postgres, against buildDailyDigest over the facts
 * the producer writes for the same rows on a real ClickHouse.
 *
 * Compared exactly: every stats field except outcomes.reverted (omitted by decision), projects,
 * epics, and each session's window aggregates. Not compared here: a session's title / workState
 * / catalog / summary fallback, which come from the directory (lane F2 titles; see the F4 map).
 * Fixture paths and ids are lowercase ASCII so collation and byte order agree.
 *
 * Runs only with BOTH F4_TEST_POSTGRES_URL and READ_API_TEST_CLICKHOUSE_URL.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { createClickHouseClient, type ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { outputFactsSchemaStatements } from "../../session-pipeline/output-facts-schema.ts";
import { outputFactsFromScope, reconcileOutputFacts } from "../../session-pipeline/output-facts.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { buildDailyDigest } from "../digest-query.ts";
import type { Directory } from "../directory.ts";

const PG_URL = process.env.F4_TEST_POSTGRES_URL;
const CH_URL = process.env.READ_API_TEST_CLICKHOUSE_URL;
const suite = PG_URL && CH_URL ? describe : describe.skip;
const ORG = "org-d", WS = "ws-d";
const SINCE = new Date("2026-11-05T00:00:00.000Z"), UNTIL = new Date("2026-11-06T00:00:00.000Z");

type Ev = { s: string; src?: string; id: string; ts: string; kind?: string; project?: string | null; ref?: Record<string, unknown>; files?: unknown; cost?: number | null; inp?: number; out?: number; model?: string | null; user?: string | null; title?: string | null; cc?: number; c5?: number | null; c1?: number | null };
const EVENTS: Ev[] = [
  { s: "s1", id: "e1", ts: "2026-11-05T01:00:00.000001Z", project: "acme/app", ref: { id: "acme/app@main" }, files: ["a.ts", "b.ts"], cost: 100, inp: 10, out: 5, model: "gpt-5", user: "u1", title: "fix parser" },
  { s: "s1", id: "e2", ts: "2026-11-05T02:00:00.000000Z", project: "acme/app", ref: { id: "acme/app@main" }, files: ["a.ts"], cost: null, inp: 1, out: 1, model: "gpt-5", user: "u2", cc: 10, c5: 10, c1: null },
  { s: "s1", id: "e3", ts: "2026-11-05T03:00:00.000000Z", kind: "tool_call", project: "acme/app", ref: { id: "acme/app@feat" }, files: ["c.ts"], model: null, title: "later title" },
  { s: "s2", id: "e4", ts: "2026-11-05T04:00:00.000000Z", project: null, ref: {}, files: "not-an-array", cost: 7, model: "claude", user: "u1" },
  { s: "s2", id: "e5", ts: "2026-11-05T05:00:00.000000Z", project: null, ref: { id: "norepo" }, files: [], cc: 5, c5: 2, c1: 2 },
  { s: "s3", src: "claude", id: "e6", ts: "2026-11-05T06:00:00.000000Z", project: "acme/web", ref: { id: "acme/web@" }, files: ["b.ts"], cost: 50, inp: 3, out: 3, model: "claude" },
  { s: "s4", id: "e7", ts: "2026-11-05T23:59:59.999500Z", project: "acme/web", ref: { id: "acme/web@main" }, files: ["d.ts"] },
  { s: "s4", id: "e8", ts: "2026-11-06T00:00:00.000000Z", project: "acme/web", files: ["e.ts"] },
  { s: "s5", id: "e9", ts: "2026-11-04T23:59:59.999999Z", project: "acme/app" },
];
const OUTCOMES = [
  { s: "s1", sha: "1".repeat(40), reverted: true },
  { s: "s1", sha: "2".repeat(40), reverted: false },
  { s: "s2", sha: "3".repeat(40), reverted: false },
];

function projectionRow(e: Ev) {
  return {
    table: "convergence_events",
    row: {
      machine_id: "m1", source: e.src ?? "codex", session_id: e.s, event_id: `m1:${e.id}`, kind: e.kind ?? "session_event", type: "assistant",
      ts: e.ts, actor_role: "assistant", content: "x", request_key: null, delivery_record_id: e.id, record: { payload: { kind: "text" } }, tool_name: null,
      task_ref: e.ref ?? {}, project_id: e.project ?? null, files_touched: e.files ?? [], model: e.model === undefined ? "gpt-5" : e.model, provider: null,
      input_tokens: e.inp ?? 0, output_tokens: e.out ?? 0, reasoning_tokens: 0, cache_read_tokens: 0, cache_create_tokens: e.cc ?? 0,
      cache_create_5m_tokens: e.c5 ?? null, cache_create_1h_tokens: e.c1 ?? null, cost_usd_micros: e.cost ?? null, user_id: e.user ?? null, task_title: e.title ?? null,
    },
  };
}

const BRANCH = `(case when task_ref = '{}'::jsonb then null when position('@' in task_ref->>'id') = 0 then null else substring(task_ref->>'id' from position('@' in task_ref->>'id') + 1) end)`;
const W = (until = "ts < $3") => `org_id = $1 AND ts >= $2 AND ${until} AND workspace_id = $4 AND ($5::text IS NULL OR project_id = $5)`;
const RHC = {
  stats: `SELECT count(distinct (source, session_id)) AS sessions, count(*) AS events, count(distinct coalesce(project_id, chr(1))) AS projects,
    count(distinct (coalesce(project_id, chr(1)) || chr(2) || coalesce(${BRANCH}, chr(1)))) AS epics,
    coalesce(sum(cost_usd_micros), 0) AS cost, coalesce(sum(input_tokens), 0) AS input, coalesce(sum(output_tokens), 0) AS output,
    array_remove(array_agg(distinct model), null) AS models FROM S.convergence_events WHERE ${W()}`,
  files: `WITH touched AS (SELECT source, session_id, path FROM S.convergence_events,
      LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(files_touched) = 'array' THEN files_touched ELSE '[]'::jsonb END) AS path WHERE ${W()}),
    by_path AS (SELECT path, count(DISTINCT (source, session_id)) AS session_count FROM touched GROUP BY path)
    SELECT (SELECT count(*) FROM by_path) AS files_touched_count,
      (SELECT coalesce(jsonb_agg(top ORDER BY top.session_count DESC, top.path ASC), '[]'::jsonb)
         FROM (SELECT path, session_count FROM by_path ORDER BY session_count DESC, path ASC LIMIT 10) AS top) AS top_files`,
  outcomes: `WITH day_sessions AS (SELECT DISTINCT source, session_id FROM S.convergence_events WHERE ${W()})
    SELECT count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM S.session_outcomes so WHERE so.org_id = $1 AND so.session_id = day_sessions.session_id AND so.source = day_sessions.source AND so.workspace_id = $4 AND so.reverted = true)
        AND EXISTS (SELECT 1 FROM S.session_outcomes so WHERE so.org_id = $1 AND so.session_id = day_sessions.session_id AND so.source = day_sessions.source AND so.workspace_id = $4)) AS committed,
      count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM S.session_outcomes so WHERE so.org_id = $1 AND so.session_id = day_sessions.session_id AND so.source = day_sessions.source AND so.workspace_id = $4)) AS unknown
    FROM day_sessions`,
  projects: `SELECT project_id, count(distinct (source, session_id)) AS sessions, count(*) AS events, count(distinct coalesce(${BRANCH}, chr(1))) AS epics,
    coalesce(sum(cost_usd_micros), 0) AS cost, min(ts) AS first_ts, max(ts) AS last_ts
    FROM S.convergence_events WHERE ${W()} GROUP BY project_id ORDER BY max(ts) desc LIMIT 50`,
  epics: `SELECT project_id, max(${BRANCH}) AS branch,
    (array_remove(array_agg(task_ref->>'id' order by ts desc, session_id desc), null))[1] AS epic_key,
    count(distinct (source, session_id)) AS sessions, count(*) AS events, coalesce(sum(cost_usd_micros), 0) AS cost, min(ts) AS first_ts, max(ts) AS last_ts
    FROM S.convergence_events WHERE ${W()} GROUP BY project_id, ${BRANCH} ORDER BY max(ts) desc LIMIT 50`,
  sessions: `WITH page AS (SELECT session_id, source FROM S.convergence_events WHERE ${W("ts <= $3")} GROUP BY session_id, source
      ORDER BY max(ts) desc, session_id desc, source desc LIMIT 51)
    SELECT session_id, source, array_remove(array_agg(distinct user_id), null) AS user_ids, max(project_id) AS project_id,
      array_agg(distinct machine_id) AS machine_ids, min(ts) AS first_ts, max(ts) AS last_ts, count(*) AS events,
      array_agg(distinct kind) AS kinds, array_remove(array_agg(distinct model), null) AS models,
      (array_remove(array_agg(task_title order by ts desc), null))[1] AS task_title,
      coalesce(jsonb_agg(distinct task_ref) filter (where task_ref <> '{}'::jsonb), '[]'::jsonb) AS task_refs,
      sum(cost_usd_micros) AS cost, coalesce(sum(input_tokens), 0) AS input, coalesce(sum(output_tokens), 0) AS output,
      coalesce(sum(cache_create_tokens), 0) AS cache_create,
      coalesce(bool_and(cache_create_tokens = 0 or coalesce(cache_create_5m_tokens, 0) + coalesce(cache_create_1h_tokens, 0) = cache_create_tokens
        and (cache_create_5m_tokens is not null or cache_create_1h_tokens is not null)), true) AS split,
      coalesce(sum(cache_create_5m_tokens), 0) AS c5, coalesce(sum(cache_create_1h_tokens), 0) AS c1
    FROM S.convergence_events WHERE ${W("ts <= $3")} AND (session_id, source) IN (SELECT session_id, source FROM page)
    GROUP BY session_id, source ORDER BY max(ts) desc, session_id desc, source desc`,
};

suite("digest parity: relayhistory SQL on Postgres == buildDailyDigest on ClickHouse facts", () => {
  let db: pg.Client, admin: ClickHouseClient, ch: ClickHouseClient;
  const schema = `f4d_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const database = `digest_parity_${crypto.randomUUID().replaceAll("-", "").slice(0, 16)}`;
  const directory: Directory = { list: async () => [], activity: async () => [], query: async () => [], queryActivity: async () => [] };
  const q = (text: string, params: unknown[]) => db.query(text.replaceAll("S.", `${schema}.`), params);

  beforeAll(async () => {
    db = new pg.Client({ connectionString: PG_URL });
    await db.connect();
    await db.query(`CREATE SCHEMA ${schema}`);
    await db.query(`CREATE TABLE ${schema}.convergence_events (org_id text, workspace_id text, source text, session_id text, machine_id text, event_id text,
      kind text, ts timestamptz, project_id text, task_ref jsonb, files_touched jsonb, cost_usd_micros bigint, input_tokens bigint, output_tokens bigint,
      model text, user_id text, task_title text, cache_create_tokens bigint, cache_create_5m_tokens bigint, cache_create_1h_tokens bigint)`);
    await db.query(`CREATE TABLE ${schema}.session_outcomes (org_id text, workspace_id text, source text, session_id text, commit_sha text, reverted boolean)`);
    for (const e of EVENTS) {
      const r = projectionRow(e).row;
      await db.query(`INSERT INTO ${schema}.convergence_events VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, [
        ORG, WS, r.source, r.session_id, r.machine_id, r.event_id, r.kind, r.ts, r.project_id, JSON.stringify(r.task_ref), JSON.stringify(r.files_touched),
        r.cost_usd_micros, r.input_tokens, r.output_tokens, r.model, r.user_id, r.task_title, r.cache_create_tokens, r.cache_create_5m_tokens, r.cache_create_1h_tokens,
      ]);
    }
    for (const o of OUTCOMES) await db.query(`INSERT INTO ${schema}.session_outcomes VALUES ($1,$2,'codex',$3,$4,$5)`, [ORG, WS, o.s, o.sha, o.reverted]);

    admin = createClickHouseClient({ url: CH_URL!, user: process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default", password: process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "", database: "default" });
    await admin.command(`CREATE DATABASE ${database}`);
    ch = createClickHouseClient({ url: CH_URL!, user: process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default", password: process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "", database });
    for (const st of outputFactsSchemaStatements()) await ch.command(st);
    const scopes = new Map<string, Ev[]>();
    for (const e of EVENTS) scopes.set(`${e.src ?? "codex"}\u0000${e.s}`, [...(scopes.get(`${e.src ?? "codex"}\u0000${e.s}`) ?? []), e]);
    for (const [key, evs] of scopes) {
      const [harness, sessionId] = key.split("\u0000") as [string, string];
      const records: RelayhistoryStreamRecord[] = OUTCOMES.filter((o) => o.s === sessionId && harness === "codex").map((o) => ({
        kind: "legacy_session_outcome", origin_id: "legacy:session_outcomes", record_id: `legacy:session_outcomes:${o.sha}`, revision: "1", operation: "upsert",
        received_at: "2026-11-05T00:00:00.000000Z",
        payload: { org_id: ORG, workspace_id: WS, source: harness, session_id: sessionId, commit_sha: o.sha, reverted: o.reverted, shipped_at: null, created_at: "2026-10-01T00:00:00.000000Z" },
      }));
      const scope = { orgId: ORG, workspaceId: WS, harness, sessionIds: [sessionId], canonicalId: sessionId };
      const facts = outputFactsFromScope(scope, { projectionRows: evs.map(projectionRow), records, complete: true }, 1);
      expect(facts.complete).toBe(true);
      await reconcileOutputFacts(ch, scope, facts, { maxRows: 10_000, insertBatch: 1_000 });
    }
  }, 120_000);
  afterAll(async () => {
    await db?.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await db?.end();
    await admin?.command(`DROP DATABASE IF EXISTS ${database}`);
  }, 60_000);

  const iso = (v: unknown) => new Date(v as string).toISOString();
  for (const projectId of [null, "acme/app", "acme/web", "nobody"]) {
    test(`project ${projectId}: stats, files, outcomes (minus reverted), projects, epics and session window aggregates agree`, async () => {
      const args = [ORG, SINCE.toISOString(), UNTIL.toISOString(), WS, projectId];
      const listArgs = [ORG, SINCE.toISOString(), new Date(UNTIL.getTime() - 1).toISOString(), WS, projectId];
      const [s, f, o, p, e, ss] = await Promise.all([
        q(RHC.stats, args), q(RHC.files, args), q(RHC.outcomes, args), q(RHC.projects, args), q(RHC.epics, args), q(RHC.sessions, listArgs),
      ]);
      const sp = await buildDailyDigest(ch, directory, { orgId: ORG, workspaceId: WS, since: SINCE, until: UNTIL, projectId });
      const s0 = s.rows[0];
      const { reverted: _r, ...outcomes } = sp.stats.outcomes;
      expect({ ...sp.stats, outcomes }).toEqual({
        sessionCount: Number(s0.sessions), eventCount: Number(s0.events), projectCount: Number(s0.projects), epicCount: Number(s0.epics),
        totalCostUsdMicros: Number(s0.cost), totalInputTokens: Number(s0.input), totalOutputTokens: Number(s0.output),
        // relayhistory: ((row.models as string[] | null) ?? []).filter(Boolean)
        models: ((s0.models as string[] | null) ?? []).filter(Boolean),
        filesTouchedCount: Number(f.rows[0].files_touched_count),
        topFiles: f.rows[0].top_files.map((t: { path: string; session_count: number }) => ({ path: t.path, sessionCount: Number(t.session_count) })),
        outcomes: { committed: Number(o.rows[0].committed), unknown: Number(o.rows[0].unknown) },
      });
      expect(sp.projects).toEqual(p.rows.map((r) => ({ projectId: r.project_id, sessionCount: Number(r.sessions), eventCount: Number(r.events), epicCount: Number(r.epics), costUsdMicros: Number(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts) })));
      expect(sp.epics).toEqual(e.rows.map((r) => ({ projectId: r.project_id, epicKey: r.epic_key, branch: r.branch, sessionCount: Number(r.sessions), eventCount: Number(r.events), costUsdMicros: Number(r.cost), firstTs: iso(r.first_ts), lastTs: iso(r.last_ts) })));
      const window = (x: Record<string, unknown>) => ({
        sessionId: x.sessionId, userIds: x.userIds, source: x.source, projectId: x.projectId, machineIds: x.machineIds, firstTs: x.firstTs, lastTs: x.lastTs,
        eventCount: x.eventCount, kinds: x.kinds, models: x.models, taskTitle: x.taskTitle,
        taskRefs: (x.taskRefs as unknown[]).map((t) => JSON.stringify(t)).sort(), totalCostUsdMicros: x.totalCostUsdMicros, totalInputTokens: x.totalInputTokens,
        totalOutputTokens: x.totalOutputTokens, totalCacheCreateTokens: x.totalCacheCreateTokens, totalCacheCreate5mTokens: x.totalCacheCreate5mTokens, totalCacheCreate1hTokens: x.totalCacheCreate1hTokens,
      });
      expect(sp.sessions.map(window)).toEqual(ss.rows.map((r) => window({
        sessionId: r.session_id, userIds: [...r.user_ids].sort(), source: r.source, projectId: r.project_id, machineIds: r.machine_ids, firstTs: iso(r.first_ts), lastTs: iso(r.last_ts),
        eventCount: Number(r.events), kinds: r.kinds, models: r.models, taskTitle: r.task_title, taskRefs: r.task_refs, totalCostUsdMicros: r.cost === null ? null : Number(r.cost),
        totalInputTokens: Number(r.input), totalOutputTokens: Number(r.output), totalCacheCreateTokens: Number(r.cache_create),
        totalCacheCreate5mTokens: r.split ? Number(r.c5) : null, totalCacheCreate1hTokens: r.split ? Number(r.c1) : null,
      })));
      if (projectId === null) {
        // Not vacuous: the day has sessions, a boundary event only the stats see, and both outcome classes.
        expect(sp.stats.sessionCount).toBe(4);
        expect(sp.stats.outcomes).toEqual({ committed: 1, reverted: 0, unknown: 2 });
      }
    }, 60_000);
  }
});
