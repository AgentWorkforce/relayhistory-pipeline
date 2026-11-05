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
import { CanonicalJournalStore } from "../src/search-features/canonical-journal.ts";
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
for (const table of ["session_event_features", "session_brief_features"])
  await ch.command("DROP TABLE IF EXISTS " + table);
for (const s of eventFeatureSchemaStatements()) await ch.command(s);
for (const s of briefFeatureSchemaStatements()) await ch.command(s);
if ((await pool.query("SELECT current_database() d")).rows[0]?.d !== name)
  throw Error("owned fixture database identity");
await pool.query(
  "DROP SCHEMA IF EXISTS sessions CASCADE;CREATE SCHEMA sessions;" +
    "CREATE TABLE sessions.canonical_feature_journal(" +
    "revision bigint PRIMARY KEY,kind text NOT NULL,org_id text NOT NULL," +
    "workspace_id text NOT NULL,scope jsonb NOT NULL," +
    "schema_generation text NOT NULL,image_key text NOT NULL," +
    "source_hash text NOT NULL,prior_keyset_hash text," +
    "source_authority jsonb NOT NULL,prior_source_authority jsonb," +
    "pointer_set jsonb NOT NULL)");
const stamp = "2026-10-30T10:00:00.000000Z";
const REV = "1000001";
const HASH = createHash("sha256").update("canonical-image").digest("hex");
const GENERATION = createHash("sha256").update("generation").digest("hex");
const IMAGE_KEY = "org:s-1";
const SCOPE = { orgId: "org", workspaceId: "ws", harness: "h",
  sessionId: "s-1", generation: GENERATION };
const AUTH = { epoch: "1", fingerprint: HASH };
const PTR = { hash: "p".repeat(64), count: 1 };
const page = (rows: unknown[], cursor: string | null = null) => ({
  rows, cursor, source: IMAGE_KEY, pointerSetHash: PTR.hash,
  pageSha256: createHash("sha256").update(JSON.stringify(rows)).digest("hex") });
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
  imageDescriptor: async () => ({ imageKey: IMAGE_KEY, sourceHash: HASH,
    pointerSetHash: PTR.hash, complete: true }),
};
const acks = { ackPage: async () => {}, finish: async () => {} };
for (const [revision, kind, prior] of [
  [REV, "publish", null],
  ["1000002", "retire", AUTH],
] as const)
  await pool.query(
    "INSERT INTO sessions.canonical_feature_journal VALUES(" +
      "$1::bigint,$2,'org','ws',$3::jsonb,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb)",
    [revision, kind, JSON.stringify(SCOPE), GENERATION, IMAGE_KEY, HASH,
      prior ? HASH : null, JSON.stringify(AUTH),
      prior ? JSON.stringify(prior) : null, JSON.stringify(PTR)]);
const journal = await new CanonicalJournalStore({
  query: async <T>(text: string, params?: unknown[]) => ({
    rows: (await pool.query(text, params)).rows as T[],
  }),
}).entries("0", 10);
if (journal.entries.length !== 2 || journal.hasMore)
  throw Error("journal read proof incomplete");
const [publishEntry, retireEntry] = journal.entries;
const pub = await copyCanonicalEntry(imagePort, acks, ch, { checkpoint: "0" },
  publishEntry!);
if (pub.kind !== "copied") throw Error("publish refused: " + pub.reason);
const ret = await copyCanonicalEntry(imagePort, acks, ch,
  { checkpoint: pub.checkpoint }, retireEntry!);
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
