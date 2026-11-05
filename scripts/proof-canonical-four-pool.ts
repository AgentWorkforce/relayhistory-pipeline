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
for (const s of eventFeatureSchemaStatements()) await ch.command(s);
for (const s of briefFeatureSchemaStatements()) await ch.command(s);
await pool.query(
  "CREATE SCHEMA IF NOT EXISTS sessions;" +
    "CREATE TABLE IF NOT EXISTS sessions.canonical_feature_journal(" +
    "revision bigint PRIMARY KEY, kind text NOT NULL," +
    "schema_generation text NOT NULL, image_key text NOT NULL," +
    "source_hash char(64) NOT NULL, prior_keyset_hash char(64)," +
    "recorded_at timestamptz NOT NULL DEFAULT now())");
const write = { query: (t: string, p?: unknown[]) => pool.query(t, p) };
const stamp = "2026-10-30T10:00:00.000000Z";
const REV = "1000001";
const HASH = createHash("sha256").update("canonical-image").digest("hex");
const SCOPE = { orgId: "org", workspaceId: "ws", harness: "h", sessionId: "s-1" };
const AUTH = { epoch: "1", fingerprint: "a".repeat(64) };
const PTR = { hash: "p".repeat(64), count: 1 };
const page = (rows: unknown[], cursor: string | null = null) => ({
  rows, cursor, source: "img", pointerSetHash: "p".repeat(64), pageSha256: "x" });
const imagePort = {
  imagePages: async (_e: any, kind: "event" | "brief", cursor: string | null) =>
    cursor === null
      ? page(kind === "event" ? [{
          orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
          machineId: "m-1", kind: "session_event", eventId: "e-1", ts: stamp,
          ingestedAt: stamp, userId: "u", actorName: null, actorRole: "assistant",
          projectId: null, projectFolded: null, taskTitle: "T", taskTitleFolded: "t",
          content: "hello", contentFolded: "hello",
          taskRefText: "{}", taskRefTextFolded: "{}", tags: [],
          embedding: null, embeddingModel: null, contentHash: null }] : [{
          orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
          title: "T", description: "D", titleFolded: "t", descriptionFolded: "d",
          briefCompletedAt: stamp, embedding: null, embeddingModel: null,
          contentHash: "h" }], "done")
      : page([], null),
  priorKeyPages: async (_e: any, kind: "event" | "brief", cursor: string | null) =>
    cursor === null
      ? page(kind === "event" ? [{
          orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
          machineId: "m-1", kind: "session_event", eventId: "e-old", ts: stamp }] : [{
          orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }], "done")
      : page([], null),
  imageDescriptor: async () => ({ imageKey: "img-1", sourceHash: HASH,
    pointerSetHash: "p".repeat(64), complete: true }),
};
const acks = { ackPage: async () => {}, finish: async () => {} };
const state = { checkpoint: "0" };
const pub = await copyCanonicalEntry(imagePort, acks, ch, state,
  { kind: "publish", revision: REV, imageKey: "img-1",
    schemaGeneration: "g1", sourceHash: HASH, scope: SCOPE,
    sourceAuthority: AUTH, pointerSet: PTR }, "target-a");
if (pub.kind !== "copied") throw Error("publish refused: " + pub.reason);
const ret = await copyCanonicalEntry(imagePort, acks, ch,
  { checkpoint: pub.checkpoint },
  { kind: "retire", revision: "1000002", imageKey: "img-1",
    schemaGeneration: "g1", priorKeysetHash: HASH, scope: SCOPE,
    priorSourceAuthority: AUTH, pointerSet: PTR }, "target-a");
if (ret.kind !== "copied") throw Error("retire refused: " + ret.reason);
const evRows = await ch.select<Record<string,unknown>>(
  "SELECT event_id,deleted,version,source_revision" +
    " FROM session_event_features FINAL ORDER BY event_id");
const brRows = await ch.select<Record<string,unknown>>(
  "SELECT session_id,deleted,version,source_revision" +
    " FROM session_brief_features FINAL");
const ok = (cond: boolean, msg: string) => { if (!cond) throw Error(msg); };
ok(evRows.length === 2, "event pool has live + tombstone rows");
ok(evRows.some((r) => r.event_id === "e-1" && r.deleted === 0), "live event row");
ok(evRows.some((r) => r.event_id === "e-old" && r.deleted === 1), "event tombstone");
ok(evRows.every((r) => r.source_revision !== "0"), "canonical revision carried");
ok(brRows.length === 1 && brRows[0]!.deleted === 1, "brief FINAL collapses to the tombstone");
ok(evRows.every((r) =>
    [featureVersion(REV), featureVersion("1000002")].includes(String(r.version))),
  "versions in managed space (live=1000001 tombstone=1000002)");
console.log(JSON.stringify({ pass: true, events: evRows.length, briefs: brRows.length }));
await pool.end();
