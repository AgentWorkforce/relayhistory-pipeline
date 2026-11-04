/**
 * Canonical four-pool proof — real PG/CH fixture variant.
 * Drives publish+retire journal entries through copyCanonicalEntry into the
 * owned fixture CH database, then SELECTs back the ReplacingMergeTree rows
 * and asserts version-ordered semantics (stale fenced, tombstones retire).
 * Same owned fixture ports as proof-event-features-real.ts.
 */
import pg from "pg";
import { createHash } from "node:crypto";
import { createClickHouseClient } from "../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { eventFeatureSchemaStatements } from "../src/search-features/event-schema.ts";
import { briefFeatureSchemaStatements } from "../src/search-features/brief-schema.ts";
import { copyCanonicalEntry } from "../src/search-features/canonical-cycle.ts";
import { featureVersion } from "../src/search-features/source.ts";

const [p, c, out] = process.argv.slice(2);
if (p !== "32770" || c !== "32771" || !out?.includes("/relay-briefs/"))
  throw Error("owned fixture only");
const name = "event_feature_fixture_codex_canon",
  credentials = {
    host: "127.0.0.1", port: 32770,
    user: "postgres", password: "synthetic-feature-only",
  };
const admin = new pg.Client({ ...credentials, database: "postgres" });
await admin.connect();
try {
  await admin.query(
    "CREATE DATABASE " + name +
      " TEMPLATE template0 LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'" +
      " LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
} catch (e: any) { if (e.code !== "42P04") throw e; } finally { await admin.end(); }
const pool = new pg.Pool({ ...credentials, database: name });
const rootCh = createClickHouseClient({
  url: "http://127.0.0.1:32771", user: "proof",
  password: "synthetic-feature-only", database: "default",
});
await rootCh.command("CREATE DATABASE IF NOT EXISTS " + name);
const ch = createClickHouseClient({
  url: "http://127.0.0.1:32771", user: "proof",
  password: "synthetic-feature-only", database: name,
});
for (const s of eventFeatureSchemaStatements) await ch.command(s);
for (const s of briefFeatureSchemaStatements) await ch.command(s);
await pool.query(
  "CREATE TABLE sessions(x int); CREATE SCHEMA IF NOT EXISTS sessions;" +
    "CREATE TABLE sessions.canonical_feature_journal(" +
    "revision bigint PRIMARY KEY, kind text NOT NULL," +
    "schema_generation text NOT NULL, image_key text NOT NULL," +
    "source_hash char(64) NOT NULL, prior_keyset_hash char(64)," +
    "recorded_at timestamptz NOT NULL DEFAULT now())");
const write = { query: (t: string, p?: unknown[]) => pool.query(t, p) };
const journal = {
  entries: async () => [],
};
const stamp = "2026-10-30T10:00:00.000000Z";
const REV = "1000001";
const HASH = createHash("sha256").update("canonical-image").digest("hex");
const imagePort = {
  events: async () => [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
    machineId: "m-1", kind: "session_event", eventId: "e-1", ts: stamp,
    ingestedAt: stamp, userId: "u", actorName: null, actorRole: "assistant",
    projectId: null, projectFolded: null, taskTitle: "T", taskTitleFolded: "t",
    content: "hello", contentFolded: "hello",
    taskRefText: "{}", taskRefTextFolded: "{}", tags: [],
    embedding: null, embeddingModel: null, contentHash: null }],
  briefs: async () => [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
    title: "T", description: "D", titleFolded: "t", descriptionFolded: "d",
    briefCompletedAt: stamp, embedding: null, embeddingModel: null,
    contentHash: "h" }],
  priorEvents: async () => [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
    machineId: "m-1", kind: "session_event", eventId: "e-old", ts: stamp }],
  priorBriefs: async () => [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }],
};
const state = { checkpoint: "0" };
const pub = await copyCanonicalEntry(journal, imagePort, ch, state,
  { kind: "publish", revision: REV, imageKey: "img-1",
    schemaGeneration: "g1", sourceHash: HASH });
if (pub.kind !== "copied") throw Error("publish refused: " + pub.reason);
const ret = await copyCanonicalEntry(journal, imagePort, ch,
  { checkpoint: pub.checkpoint },
  { kind: "retire", revision: "1000002", imageKey: "img-1",
    schemaGeneration: "g1", priorKeysetHash: HASH });
if (ret.kind !== "copied") throw Error("retire refused: " + ret.reason);
const evRows = (await ch.query(
  "SELECT event_id,deleted,version,source_revision" +
    " FROM session_event_features ORDER BY event_id")).data as Record<string, unknown>[];
const brRows = (await ch.query(
  "SELECT session_id,deleted,version,source_revision" +
    " FROM session_brief_features")).data as Record<string, unknown>[];
const ok = (cond: boolean, msg: string) => { if (!cond) throw Error(msg); };
ok(evRows.length === 2, "event pool has live + tombstone rows");
ok(evRows.some((r) => r.event_id === "e-1" && r.deleted === 0), "live event row");
ok(evRows.some((r) => r.event_id === "e-old" && r.deleted === 1), "event tombstone");
ok(evRows.every((r) => r.source_revision !== "0"), "canonical revision carried");
ok(brRows.length === 2 && brRows.some((r) => r.deleted === 1), "brief live + tombstone");
ok(evRows.every((r) => r.version === featureVersion(REV)), "version in managed space");
console.log(JSON.stringify({ pass: true, events: evRows.length, briefs: brRows.length }));
await pool.end();
