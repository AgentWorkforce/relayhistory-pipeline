import { createHash } from "node:crypto";
/** Owned disposable PG17/CH26 proof; no environment credentials/production targets. */
import pg from "pg";
import {
  eventFeatureRow,
  publishEventFeatureSnapshots,
} from "../src/search-features/event-publisher.ts";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import { createClickHouseClient } from "../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { eventFeatureSchemaStatements } from "../src/search-features/event-schema.ts";
import { briefFeatureSchemaStatements } from "../src/search-features/brief-schema.ts";
import {
  copyEventFeaturePage,
  captureEventFeatureBaseline,
} from "../src/search-features/event-cycle.ts";
import { publishBriefFeatureSnapshots } from "../src/search-features/brief-publisher.ts";
import {
  readFeatureBaselinePage,
  readFeatureJournalPage,
  readFeatureSnapshots,
} from "../src/search-features/source.ts";
import {
  readLegacyCasefoldProfile,
  foldLegacyQuery,
} from "../src/search-features/casefold-source.ts";
const [rhc, p, c, out] = process.argv.slice(2);
if (
  !rhc?.endsWith(
    "/relayhistory-cloud-worktrees/ch-four-pool-search-20261103",
  ) ||
  p !== "32770" ||
  c !== "32771" ||
  !out?.includes("/relay-briefs/ch-event-feature-proof-20261103")
)
  throw Error("owned fixture only");
const name = "event_feature_fixture_codex_builtin",
  credentials = {
    host: "127.0.0.1",
    port: 32770,
    user: "postgres",
    password: "synthetic-feature-only",
  };
const admin = new pg.Client({ ...credentials, database: "postgres" });
await admin.connect();
try {
  await admin.query(
    "CREATE DATABASE " +
      name +
      " TEMPLATE template0 LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'",
  );
} catch (e: any) {
  if (e.code !== "42P04") throw e;
} finally {
  await admin.end();
}
const pool = new pg.Pool({ ...credentials, database: name });
const rootCh = createClickHouseClient({
  url: "http://127.0.0.1:32771",
  user: "proof",
  password: "synthetic-feature-only",
  database: "default",
});
await rootCh.command("CREATE DATABASE IF NOT EXISTS " + name);
const ch = createClickHouseClient({
  url: "http://127.0.0.1:32771",
  user: "proof",
  password: "synthetic-feature-only",
  database: name,
});
const db = {
  async query<T>(text: string, params?: unknown[]) {
    if (!/^\s*(SELECT|WITH)\b/iu.test(text)) throw Error("copier source write");
    return { rows: (await pool.query(text, params)).rows as T[] };
  },
};
const checks: any[] = [];
const assert = (v: unknown, why: string) => {
  if (!v) throw Error(why);
};
const vector = JSON.stringify(
  Array.from({ length: 1536 }, (_, i) => (i === 0 ? 1 : 0)),
);
try {
  assert(
    (await pool.query("SELECT current_database() d")).rows[0].d === name,
    "db identity",
  );
  await pool.query(
    "CREATE EXTENSION IF NOT EXISTS vector;DROP SCHEMA IF EXISTS sessions CASCADE;CREATE SCHEMA sessions",
  );
  await pool.query(
    "CREATE TABLE sessions.convergence_events(\norg_id text NOT NULL,workspace_id text NOT NULL,machine_id text NOT NULL,user_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,kind text NOT NULL,event_id text NOT NULL,type text DEFAULT 'message',\nactor_name text,actor_role text,project_id text,task_title text,task_ref jsonb DEFAULT '{}',tags jsonb DEFAULT '[]',content text,embedding vector(1536),embedding_model text,embedding_dim integer,content_hash text,generated_at timestamptz,embedding_skip_reason text,\nts timestamptz NOT NULL,ingested_at timestamptz NOT NULL,PRIMARY KEY(org_id,machine_id,source,session_id,kind,event_id));\nCREATE TABLE sessions.conversation_turns(org_id text,session_id text,workspace_id text);\nCREATE TABLE sessions.session_brief_search(org_id text NOT NULL,workspace_id text,source text NOT NULL,session_id text NOT NULL,evidence_workspace_ids jsonb,title text,description text,brief_completed_at timestamptz,embedding vector(1536),embedding_model text,embedding_dim integer,content_hash text,embedding_attempts integer,embedding_available_at timestamptz,embedding_error text,updated_at timestamptz,PRIMARY KEY(org_id,session_id));",
  );
  const { readMigrations } = await import(
    rhc + "/packages/relayhistory/scripts/apply-neon-migration.mjs"
  );
  for (const q of readMigrations().find((m: any) => m.name.startsWith("0076_"))
    .statements)
    await pool.query(q);
  for (const table of ["session_event_features", "session_brief_features"])
    await ch.command("DROP TABLE IF EXISTS " + table);
  for (const q of [
    ...eventFeatureSchemaStatements(),
    ...briefFeatureSchemaStatements(),
  ])
    await ch.command(q);
  const event = async (
    id: string,
    project: string | null,
    ts = "2026-11-01T10:00:00.000000Z",
    embedding: string | null = null,
    ingested = "2026-11-01T10:00:01.000000Z",
    source = "codex",
    ws = "ws",
    org = "org",
  ) =>
    pool.query(
      "INSERT INTO sessions.convergence_events(org_id,workspace_id,machine_id,user_id,source,session_id,kind,event_id,content,project_id,ts,ingested_at,embedding,embedding_dim,embedding_model) VALUES($1,$2,'m','u',$3,$4,'prompt',$4,'Needle content',$5,$6,$7,$8::vector,CASE WHEN $8::text IS NULL THEN NULL ELSE 1536 END,CASE WHEN $8::text IS NULL THEN NULL ELSE 'legacy' END)",
      [org, ws, source, id, project, ts, ingested, embedding],
    );
  await pool.query(
    "INSERT INTO sessions.convergence_events(org_id,workspace_id,machine_id,user_id,source,session_id,kind,event_id,content,project_id,ts,ingested_at) SELECT 'org','ws','m','u','codex','blocked-'||n,'prompt','blocked-'||n,'Needle','other','2026-11-01T10:00:00Z','2026-11-01T10:00:01Z' FROM generate_series(1,1001)n",
  );
  await event("eligible-old-unembedded", "p", "2020-01-01T00:00:00.123456Z");
  await event("old-semantic", "p", "2020-01-01T00:00:00.123457Z", vector);
  await event("null-project", null);
  await event("empty-project", "");
  await event("after-until-microsecond", "p", "2026-11-01T10:00:00.000001Z");
  await event(
    "late-ingest",
    "p",
    "2020-01-01T00:00:00.123456Z",
    null,
    "2026-12-01T00:00:00.000000Z",
  );
  await event(
    "foreign-ws",
    "p",
    undefined,
    undefined,
    undefined,
    "codex",
    "other",
  );
  await event(
    "foreign-org",
    "p",
    undefined,
    undefined,
    undefined,
    "codex",
    "ws",
    "foreign",
  );
  await event("mutable", "p");
  await pool.query(
    "INSERT INTO sessions.session_brief_search(org_id,workspace_id,source,session_id,evidence_workspace_ids,title,description,brief_completed_at,embedding,embedding_model,content_hash) SELECT 'org','ws','codex',session_id,'[\"ws\"]','Needle '||session_id,'Description','2026-11-01T10:00:00Z',$1::vector,'legacy','hash' FROM sessions.convergence_events WHERE org_id='org' AND workspace_id='ws'",
    [vector],
  );
  let state = await captureEventFeatureBaseline(db);
  await mkdir(out, { recursive: true });
  await writeFile(out + "/captured-H.json", JSON.stringify(state));
  let pages = 0,
    published = 0;
  for (;;) {
    const r = await copyEventFeaturePage({ db, ch }, state, 200);
    state = r.next;
    pages++;
    published += r.published;
    if (r.complete) break;
    assert(pages < 100, "page bound");
  }
  assert(published >= 1010, "unembedded baseline omitted");
  const h = (await readFeatureJournalPage(db, "brief", "0", 1))
    .observedRevision;
  let cursor: string | null = null;
  do {
    const page = await readFeatureBaselinePage(db, "brief", cursor, 200);
    await publishBriefFeatureSnapshots({ db, ch }, page.rows, { baselineH: h });
    cursor = page.nextCursor;
  } while (cursor);
  const profile = await readLegacyCasefoldProfile(db),
    fold = {
      sourceHash: profile.sourceHash,
      fold: (text: string) => foldLegacyQuery(profile, text),
    };
  const connection = {
    url: "http://127.0.0.1:32771",
    user: "proof",
    password: "synthetic-feature-only",
    database: name,
    table: "transcript_events",
    textPrefilter: "none" as const,
    maxRowsToRead: 0,
    maxThreads: 2,
  };
  const { buildEventVisibility } = await import(
    rhc + "/packages/relayhistory/src/lib/session-search-event-visibility.ts"
  );
  const { clickHouseEventFeaturePools } = await import(
    rhc + "/packages/relayhistory/src/lib/session-search-event-features.ts"
  );
  const { clickHouseBriefFeaturePools } = await import(
    rhc + "/packages/relayhistory/src/lib/session-search-brief-features.ts"
  );
  const { buildWhere } = await import(
    rhc + "/packages/relayhistory/src/lib/recall.ts"
  );
  const { PgDialect } = await import(
    rhc + "/node_modules/drizzle-orm/pg-core/index.js"
  );
  const dialect = new PgDialect();
  const scope = {
    tenant: { orgId: "org", workspaceId: "ws" },
    filters: {},
    snapshot: new Date("2026-11-02T00:00:00Z"),
  };
  const cases: any[] = [
    {},
    { projectId: "p" },
    { noProject: true },
    { projectContains: "P" },
    { projectContains: "%" },
    { source: "codex" },
    { until: new Date("2026-11-01T10:00:00.000Z") },
    { since: new Date("2020-01-01") },
    { projectId: "p", until: new Date("2026-11-01T10:00:00.000Z") },
  ];
  for (const filters of cases) {
    const b = buildEventVisibility(connection, { ...scope, filters }, fold);
    const actual = await ch.select<any>(b.query, { params: b.params });
    const q = dialect.sqlToQuery(
      buildWhere(
        { orgId: "org" },
        {
          ...filters,
          until: filters.until ?? scope.snapshot,
          ingestedUntil: scope.snapshot,
        },
        { workspaceId: "ws" },
      ),
    );
    const expected = (
      await pool.query(
        "SELECT DISTINCT source,session_id FROM sessions.convergence_events WHERE " +
          q.sql,
        q.params,
      )
    ).rows;
    const keys = (a: any[]) =>
      a.map((r) => JSON.stringify([r.source, r.session_id])).sort();
    assert(
      JSON.stringify(keys(actual)) === JSON.stringify(keys(expected)),
      "legacy Where mismatch " + JSON.stringify(filters),
    );
  }
  checks.push({
    name: "actual_PG_legacy_Where_dense_CH_visibility",
    cases: cases.length,
    published,
    pages,
  });
  const briefs = clickHouseBriefFeaturePools(connection, {
    queryFold: fold,
    authority: async () => ({
      version: "synthetic-proof",
      complete: true,
      casefoldHash: profile.sourceHash,
      freshUntil: new Date(Date.now() + 30000).toISOString(),
    }),
    visibility: (i: any) => buildEventVisibility(connection, i, fold),
  });
  const target = {
    ...scope,
    filters: { projectId: "p", until: new Date("2020-01-01T00:00:00.124Z") },
  };
  const semanticBrief = await briefs.semantic(
      target,
      JSON.parse(vector),
      AbortSignal.timeout(10000),
    ),
    keywordBrief = await briefs.keyword(
      target,
      "Needle",
      AbortSignal.timeout(10000),
    );
  assert(
    semanticBrief.some((r: any) => r.sessionId === "eligible-old-unembedded") &&
      keywordBrief.some((r: any) => r.sessionId === "eligible-old-unembedded"),
    "old unembedded visibility lost",
  );
  assert(
    semanticBrief.length === 2 && keywordBrief.length === 2,
    "eligibility applied after1000cap",
  );
  checks.push({
    name: "brief_eligibility_before1000_cap",
    irrelevantEarlier: 1001,
    semantic: semanticBrief.map((r: any) => r.sessionId),
    keyword: keywordBrief.map((r: any) => r.sessionId),
  });
  const events = clickHouseEventFeaturePools(connection, { queryFold: fold });
  const semantic = await events.semantic(
      scope,
      JSON.parse(vector),
      AbortSignal.timeout(10000),
    ),
    keyword = await events.keyword(scope, "Needle", AbortSignal.timeout(10000));
  assert(
    semantic.some((r: any) => r.sessionId === "old-semantic"),
    "semantic keyword-window truncation",
  );
  assert(
    !keyword.some((r: any) => r.sessionId === "old-semantic"),
    "keyword30day bound missing",
  );
  checks.push({
    name: "semantic_full_history_keyword30days",
    semantic: semantic.map((r: any) => r.sessionId),
    keywordSessions: keyword.length,
  });
  await pool.query(
    "UPDATE sessions.convergence_events SET project_id='other' WHERE org_id='org' AND session_id='mutable'",
  );
  const r = await copyEventFeaturePage({ db, ch }, state, 200);
  const b = buildEventVisibility(
    connection,
    { ...scope, filters: { projectId: "p" } },
    fold,
  );
  assert(
    !(await ch.select<any>(b.query, { params: b.params })).some(
      (r) => r.session_id === "mutable",
    ),
    "mutable stale version leaked",
  );
  checks.push({
    name: "unembedded_mutation_collapses_before_filter",
    published: r.published,
    complete: r.complete,
  });

  state = r.next;
  const reference = {
    kind: "event" as const,
    key: {
      orgId: "org",
      workspaceId: "ws",
      source: "codex",
      sessionId: "mutable",
      machineId: "m",
      kind: "prompt",
      eventId: "mutable",
      ts: "2026-11-01T10:00:00.000000Z",
    },
    revision: "0",
    sourceHash: "a".repeat(64),
    deleted: false,
  };
  const old = (await readFeatureSnapshots(db, [reference]))[0]!;
  await pool.query(
    "DELETE FROM sessions.convergence_events WHERE org_id='org' AND session_id='mutable'",
  );
  const gone = (await readFeatureSnapshots(db, [reference]))[0]!;
  await publishEventFeatureSnapshots({ db, ch }, [gone]);
  const erased = await ch.select<any>(
    "SELECT deleted,content,length(embedding) dimensions FROM session_event_features WHERE org_id='org' AND workspace_id='ws' AND session_id='mutable'",
  );
  assert(
    erased.every(
      (r) => r.deleted === 1 && r.content === null && r.dimensions === 0,
    ),
    "old content survived erasure",
  );
  await event("mutable", "p");
  const live = (await readFeatureSnapshots(db, [reference]))[0]!;
  await publishEventFeatureSnapshots({ db, ch }, [live]);
  await ch.insert("session_event_features", [
    eventFeatureRow(old, state.baselineH!),
  ]);
  let staleRefused = false;
  try {
    await publishEventFeatureSnapshots({ db, ch }, [gone]);
  } catch (e: any) {
    staleRefused = e.reason === "source_changed";
  }
  assert(staleRefused, "stale eraser CAS accepted");
  const after = await ch.select<any>(
    "SELECT deleted,content FROM session_event_features FINAL WHERE org_id='org' AND workspace_id='ws' AND session_id='mutable'",
  );
  assert(
    after.length === 1 &&
      after[0].deleted === 0 &&
      after[0].content === "Needle content",
    "stale eraser defeated resurrection",
  );
  await pool.query(
    "UPDATE sessions.convergence_events SET workspace_id='other',ts='2026-11-01T10:00:00.000002Z' WHERE org_id='org' AND session_id='mutable'",
  );
  const moved = await copyEventFeaturePage({ db, ch }, state, 200);
  const visible = buildEventVisibility(connection, scope, fold);
  assert(
    !(await ch.select<any>(visible.query, { params: visible.params })).some(
      (r) => r.session_id === "mutable",
    ),
    "old workspace/timestamp routing survived move",
  );
  checks.push({
    name: "actual_delete_blank_resurrection_stale_eraser_and_move",
    erased: erased.length,
    staleRefused,
    moved: moved.published,
  });
  // Legal large source content is intentionally indexed only through the legacy
  // left(content,4096) predicate; large mandatory task metadata is retained losslessly.
  const largeMetadata = "m".repeat(1_200_000),
    largeContent = "C".repeat(2_360_000);
  await event(
    "large-metadata",
    "p",
    undefined,
    undefined,
    undefined,
    "codex",
    "ws",
    "large-org",
  );
  await pool.query(
    "UPDATE sessions.convergence_events SET content=$1,task_ref=$2::jsonb WHERE org_id='large-org' AND session_id='large-metadata'",
    [largeContent, JSON.stringify({ large: largeMetadata })],
  );
  const largeCopy = await copyEventFeaturePage({ db, ch }, moved.next, 200);
  const largeRows = await ch.select<any>(
    "SELECT lengthUTF8(content) content_chars,lengthUTF8(task_ref_text) metadata_chars,lower(hex(SHA256(JSONExtractString(task_ref_text,'large')))) metadata_hash FROM session_event_features FINAL WHERE org_id='large-org' AND session_id='large-metadata'",
  );
  assert(
    largeRows.length === 1 &&
      Number(largeRows[0].content_chars) === 4096 &&
      Number(largeRows[0].metadata_chars) > 1_000_000 &&
      largeRows[0].metadata_hash ===
        createHash("sha256").update(largeMetadata).digest("hex"),
    "large source metadata/content contract changed",
  );
  checks.push({
    name: "large_metadata_lossless_and_legacy_content_prefix",
    published: largeCopy.published,
    ...largeRows[0],
  });
  const result = {
    collation: (
      await pool.query(
        "SELECT datcollate,datctype,datlocprovider,coalesce(to_jsonb(pg_database)->>'datlocale','') datlocale FROM pg_database WHERE datname=current_database()",
      )
    ).rows[0],
    sourceApiSha: createHash("sha256")
      .update(
        await readFile(
          rhc + "/packages/relayhistory/src/lib/search-feature-journal.ts",
        ),
      )
      .digest("hex"),
    postgres: (await pool.query("SELECT version() v")).rows[0].v,
    clickhouse: (await ch.select<any>("SELECT version() v"))[0].v,
    checks,
  };
  await writeFile(out + "/proof.json", JSON.stringify(result, null, 2) + "\n");
  console.info(JSON.stringify({ checks: checks.length, published, pages }));
} finally {
  await pool.end();
}
