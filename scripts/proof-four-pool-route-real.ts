/** Actual RHC router + actual owned CH; readiness records are synthetic fixture
 * metadata over real completed copier checkpoints, not leased-runtime proof. */
import type { ClickHouseClient } from "../src/session-pipeline/vendor/search-index/clickhouse.ts";
import type { FeatureReader } from "../src/search-features/source.ts";
import {
  copyBriefFeaturePage,
  type BriefFeatureCheckpoint,
} from "../src/search-features/brief-cycle.ts";
import type { LegacyCasefoldProfile } from "../src/search-features/casefold-source.ts";
import { FEATURE_SOURCE_AUTHORITY } from "../src/search-features/source-authority.ts";
import { semanticCopySchemaStatements } from "../src/search-features/semantic-copy-schema.ts";
import {
  semanticSha256,
  semanticDataHashInput,
  semanticAuthorityHashInput,
  semanticTargetHashInput,
} from "../src/search-features/semantic-copy-protocol.ts";
import { eventFeatureSchemaStatements } from "../src/search-features/event-schema.ts";
import { briefFeatureSchemaStatements } from "../src/search-features/brief-schema.ts";
export async function proveFourPoolRoute(input: {
  rhc: string;
  ch: ClickHouseClient;
  db: FeatureReader;
  profile: LegacyCasefoldProfile;
  eventState: BriefFeatureCheckpoint;
  briefH: string;
}) {
  const { rhc, ch, db, profile, eventState, briefH } = input;
  if (ch.database !== "event_feature_fixture_codex_builtin")
    throw Error("owned fixture only");
  let briefState: BriefFeatureCheckpoint = {
    phase: "journal",
    baselineH: briefH,
    baselineCursor: null,
    checkpoint: briefH,
    observedRevision: briefH,
    polled: false,
  };
  for (let n = 0; ; n++) {
    if (n > 100) throw Error("brief fixture catchup bound");
    const page = await copyBriefFeaturePage({ db, ch }, briefState, 200);
    briefState = page.next;
    if (page.complete) break;
  }
  if (briefState.checkpoint !== eventState.checkpoint)
    throw Error("fixture kinds are not caught up");
  for (const table of [
    "session_feature_copy_authority",
    "session_feature_copy_profiles",
  ])
    await ch.command("DROP TABLE IF EXISTS " + table);
  for (const ddl of semanticCopySchemaStatements()) await ch.command(ddl);
  const sha = (value: string) => semanticSha256(value);
  const eventHash = await sha(
    JSON.stringify([
      "event-feature-projection-v1",
      ...eventFeatureSchemaStatements(),
    ]),
  );
  const briefHash = await sha(
    JSON.stringify([
      "brief-feature-projection-v1",
      ...briefFeatureSchemaStatements(),
    ]),
  );
  const sourceId = "owned-fixture",
    sourceAuthorityHash = FEATURE_SOURCE_AUTHORITY.identitySha256;
  const targetHash = await sha(
    semanticTargetHashInput(
      "http://127.0.0.1:32771",
      ch.database,
      eventHash,
      briefHash,
    ),
  );
  const profileJson = JSON.stringify({
    source: profile.source,
    entries: profile.entries,
  });
  const profileHash = await sha(profileJson);
  await ch.insert("session_feature_copy_profiles", [
    {
      source_id: sourceId,
      source_authority_hash: sourceAuthorityHash,
      target_hash: targetHash,
      profile_hash: profileHash,
      profile_json: profileJson,
    },
  ]);
  const records = [];
  for (const [kind, state] of [
    ["event", eventState],
    ["brief", briefState],
  ] as const) {
    const stateJson = JSON.stringify(state),
      stateHash = await sha(stateJson);
    const dataHash = await sha(
      semanticDataHashInput({
        sourceId,
        sourceAuthorityHash,
        targetHash,
        profileHash,
        kind,
        stateHash,
      }),
    );
    records.push({
      source_id: sourceId,
      kind,
      source_authority_hash: sourceAuthorityHash,
      target_hash: targetHash,
      profile_hash: profileHash,
      generation: "1",
      status_version: "3",
      dirty: 0,
      complete: 1,
      state_json: stateJson,
      state_hash: stateHash,
      data_hash: dataHash,
      authority_hash: await sha(
        semanticAuthorityHashInput(dataHash, "1", true),
      ),
      fresh_until: new Date(Date.now() + 120000)
        .toISOString()
        .replace("T", " ")
        .replace("Z", "000"),
    });
  }
  await ch.insert("session_feature_copy_authority", records);
  const { Hono } = await import(rhc + "/node_modules/hono/dist/index.js");
  const { recallRoutes } = await import(
    rhc + "/packages/relayhistory/src/routes/recall.ts"
  );
  const { row } = await import(
    rhc + "/packages/relayhistory/test/session-search-v2.fixture.ts"
  );
  const app = new Hono();
  app.use("*", async (c: any, next: any) => {
    c.set("auth", {
      orgId: "org",
      workspaceId: "ws",
      userId: "u",
      tokenSubject: "fixture",
      scopes: ["rth:read"],
      claims: {},
    });
    await next();
  });
  app.route("/", recallRoutes);
  const original = globalThis.fetch;
  let embeddings = 0,
    authorityReads = 0,
    eventReads = 0,
    briefReads = 0,
    briefTextReads = 0,
    metadataReads = 0,
    forbidden = 0;
  globalThis.fetch = async (input, init) => {
    const url = new URL(String(input));
    const sql = String(init?.body);
    if (url.origin === "http://127.0.0.1:32771") {
      if (url.searchParams.get("database") !== ch.database)
        throw Error("foreign fixture database");
      if (sql.includes("session_feature_copy_authority")) authorityReads++;
      else if (sql.includes("session_brief_features")) {
        if (sql.includes("latest.version={ref_version")) briefTextReads++;
        else briefReads++;
      } else if (sql.includes("session_event_features")) eventReads++;
      return original(input, init);
    }
    if (url.hostname === "api.openai.com") {
      embeddings++;
      return Response.json({
        data: [
          {
            embedding: Array.from({ length: 1536 }, (_, n) =>
              n === 0 ? 1 : 0,
            ),
          },
        ],
      });
    }
    if (url.hostname === "directory-fixture.invalid") {
      metadataReads++;
      const b = JSON.parse(String(init?.body));
      if (
        b.p_query !== undefined ||
        b.p_workspace_id !== "ws" ||
        !Array.isArray(b.p_session_ids) ||
        b.p_session_ids.length > 100
      )
        throw Error("metadata fixture scope");
      return Response.json(
        b.p_session_ids.map((id: string, n: number) =>
          row(id, {
            org_id: "org",
            workspace_id: "ws",
            harness: b.p_session_harnesses[n],
            task_title: "Fixture title",
          }),
        ),
      );
    }
    forbidden++;
    throw Error("request attempted nonfixture network");
  };
  let response: any;
  try {
    const r = await app.request(
      "/sessions?search=hybrid&q=Needle&workspace=ws&project=p&limit=10",
      {},
      {
        SESSION_SEARCH_BACKEND: "v2",
        SESSION_SEARCH_FEATURE_SOURCE_ID: sourceId,
        SESSION_SEARCH_FEATURE_SOURCE_AUTHORITY_HASH: sourceAuthorityHash,
        SESSION_SEARCH_FEATURE_TARGET_HASH: targetHash,
        CLICKHOUSE_URL: "http://127.0.0.1:32771",
        CLICKHOUSE_USER: "proof",
        CLICKHOUSE_PASSWORD: "synthetic-feature-only",
        CLICKHOUSE_DATABASE: ch.database,
        SESSION_SEARCH_SUPABASE_URL: "https://directory-fixture.invalid",
        SESSION_SEARCH_SUPABASE_API_KEY: "synthetic",
        SESSION_SEARCH_SUPABASE_JWT_SECRET: "synthetic",
        SESSION_SEARCH_SUPABASE_JWT_ISSUER: "https://issuer.invalid",
        EMBEDDING_API_KEY: "synthetic",
      },
    );
    response = await r.json();
    if (
      r.status !== 200 ||
      !response.sessions?.length ||
      response.degradedPools?.length ||
      embeddings !== 1 ||
      authorityReads !== 4 ||
      eventReads < 2 ||
      briefReads !== 2 ||
      briefTextReads !== response.sessions.length ||
      metadataReads !== 1 ||
      forbidden
    )
      throw Error(
        "actual four-pool route failed " +
          JSON.stringify({
            status: r.status,
            reason: response.reason,
            error: response.error,
            embeddings,
            authorityReads,
            eventReads,
            briefReads,
            briefTextReads,
            metadataReads,
            forbidden,
          }),
      );
    if (!response.sessions.some((s: any) => s.sessionId === "old-semantic"))
      throw Error("actual route lost old semantic session");
  } finally {
    globalThis.fetch = original;
  }
  return {
    name: "actual_RHC_route_real_CH_four_pools",
    readiness: "synthetic_after_actual_source_copy",
    embeddings,
    authorityReads,
    eventReads,
    briefReads,
    briefTextReads,
    metadataReads,
    forbidden,
    sessions: response.sessions.map((s: any) => ({
      source: s.source,
      sessionId: s.sessionId,
      matchedBy: s.search.matchedBy,
    })),
  };
}
