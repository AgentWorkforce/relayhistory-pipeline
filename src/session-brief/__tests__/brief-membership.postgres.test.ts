/**
 * Real-PostgreSQL proof for T130 Reader v2 + consumed-record membership (062500, 062510). Opt in with
 * the owned synthetic SESSION_INGEST_TEST_PG_URL (SESSION_INGEST_TEST_PG_REQUIRED=1 makes a missing URL
 * a failure); this creates and drops an isolated database and never connects to a deployed database.
 * Exercised through the real recordBrief publication guard and the bulls96_read_api role with its tenant
 * settings: the text-provenance constraint (a served brief always has a binding), binding preservation
 * across reads, a stale token answering changed, and unbound/unevidenced answers.
 */
import { afterAll, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import type { HoopDb } from "../../shared/db.ts";
import { briefSourceBinding, livePointers, recordBrief, type BriefText } from "../store.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";

setDefaultTimeout(180_000);
const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (process.env.SESSION_INGEST_TEST_PG_REQUIRED === "1" && !url) throw new Error("SESSION_INGEST_TEST_PG_URL is required");
const root = join(import.meta.dir, "../../..");
const sql = (path: string) => readFileSync(join(root, path), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const KEY = { orgId: "t130-org", workspaceId: "t130-ws", harness: "claude", sessionId: "t130-s" };
const text = (description: string, fingerprint: string): BriefText => ({
  title: "Native title", description, state: "completed", scrubRuleset: "relayhistory-scrub", scrubVersion: 1,
  fingerprint, promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna",
  messageCount: 2, shortenedMessages: 0, inputTokens: 1, outputTokens: 1, durationMs: 1, generatedAt: new Date("2026-11-28T00:00:00Z"),
});
let admin: pg.Client; let pool: pg.Pool; let name = ""; let db: HoopDb;

beforeAll(async () => {
  if (!url) return;
  admin = new pg.Client({ connectionString: url }); await admin.connect();
  name = "brief_membership_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url); target.pathname = "/" + name;
  pool = new pg.Pool({ connectionString: target.href, max: 4 });
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  for (const file of SESSION_BOOTSTRAP_FILES) await pool.query(sql(file));
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await pool.query(sql("supabase/migrations/" + file));
  // The read-API role and its grants, applied after the chain exactly as the PGlite Reader tests do.
  await pool.query(sql("supabase/bootstrap/session_read_api_role.sql"));
  db = { query: ((t: string, p?: unknown[]) => pool.query(t, p as never)) as never, pool, end: async () => undefined } as HoopDb;
  await pool.query(
    `INSERT INTO sessions.session_index (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
       storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title, manifest_etag)
     VALUES ($1,$2,$3,$4,'m-1','u-1',now(),now(),'s3','bucket','manifest/t130-s/m-1/r1',decode(repeat('ab',32),'hex'),'jsonl',
             '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',1,'pointer title','etag-a')`,
    [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId],
  );
});

afterAll(async () => {
  if (!url) return;
  await pool.end(); await admin.query("DROP DATABASE IF EXISTS " + name); await admin.end();
});

/** One read-only transaction as bulls96_read_api with the tenant settings, as the Reader runs. */
async function asReader<T>(work: (c: pg.PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query("BEGIN READ ONLY"); await c.query("SET LOCAL ROLE bulls96_read_api");
    await c.query(TENANT_SETTINGS_SQL, [KEY.orgId, KEY.workspaceId]);
    const r = await work(c); await c.query("COMMIT"); return r;
  } catch (e) { await c.query("ROLLBACK"); throw e; } finally { c.release(); }
}
const summary = () => asReader(async (c) => (await c.query<{ description: string; brief_binding: string | null; brief_revision: string }>(
  "SELECT description, brief_binding, brief_revision FROM sessions.session_brief_summary_v2_for($1,$2,$3)", [KEY.workspaceId, KEY.harness, KEY.sessionId])).rows);
const membership = (record: string, binding: string | null) => asReader(async (c) => (await c.query<{ membership: string; brief_binding: string | null; brief_revision: string | null }>(
  "SELECT membership, brief_binding, brief_revision FROM sessions.session_brief_membership_for($1,$2,$3,$4,$5)", [KEY.workspaceId, KEY.harness, KEY.sessionId, record, binding])).rows[0]!);

test.skipIf(!url)("T130 on real PostgreSQL: provenance constraint, preserved binding, stale token changed, unbound/unevidenced unknown", async () => {
  // No brief yet: unbound is unknown, a supplied token is changed (nothing it could describe is served).
  expect(await membership("u1", null)).toMatchObject({ membership: "unknown", brief_binding: null });
  expect(await membership("u1", "e".repeat(64))).toMatchObject({ membership: "changed", brief_binding: null });

  // A published WITHOUT membership evidence (flag off): served with a binding; unbound membership is unknown.
  const bindingA = briefSourceBinding(await livePointers(db, KEY));
  expect(await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text: text("Brief A.", "a".repeat(64)) }, { sourceBinding: bindingA })).toEqual({ written: true });
  const [a1] = await summary();
  expect(a1).toMatchObject({ description: "Brief A." });
  expect(a1!.brief_binding).toMatch(/^[0-9a-f]{64}$/);
  expect(await membership("u1", null)).toMatchObject({ membership: "unknown", brief_binding: a1!.brief_binding });

  // Provenance constraint: a served brief cannot lose its fingerprint, so a served binding is never NULL.
  await expect(pool.query("UPDATE sessions.session_briefs SET brief_fingerprint = NULL")).rejects.toMatchObject({ code: "23514" });

  // Republished with evidence (same brief identity): the binding is preserved across reads and in membership.
  expect(await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text: text("Brief A.", "a".repeat(64)) },
    { sourceBinding: bindingA, membership: { members: ["u1", "a1"], complete: true } })).toEqual({ written: true });
  const [a2] = await summary();
  expect(a2!.brief_binding).toBe(a1!.brief_binding);
  expect(await membership("u1", a2!.brief_binding)).toMatchObject({ membership: "member", brief_binding: a2!.brief_binding });
  expect(await membership("zz", a2!.brief_binding)).toMatchObject({ membership: "not_member" });
  expect(await membership("u1", null)).toMatchObject({ membership: "member", brief_binding: a2!.brief_binding });

  // B replaces the content at the SAME revision and publishes: A's token is now stale -> changed with B's binding.
  await pool.query("UPDATE sessions.session_index SET content_sha256 = decode(repeat('cd',32),'hex'), manifest_etag = 'etag-b'");
  const bindingB = briefSourceBinding(await livePointers(db, KEY));
  // A late A over the old incarnation is refused by the guard, so A's evidence cannot come back.
  expect(await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text: text("Brief A late.", "a".repeat(64)) },
    { sourceBinding: bindingA, membership: { members: ["u1"], complete: true } })).toEqual({ written: false });
  expect(await recordBrief(db, KEY, { "m-1": 1 }, { status: "ready", text: text("Brief B.", "b".repeat(64)) },
    { sourceBinding: bindingB, membership: { members: ["b1"], complete: true } })).toEqual({ written: true });
  const [b] = await summary();
  expect(b).toMatchObject({ description: "Brief B." });
  expect(b!.brief_binding).not.toBe(a2!.brief_binding);
  expect(await membership("b1", a2!.brief_binding)).toMatchObject({ membership: "changed", brief_binding: b!.brief_binding });
  expect(await membership("b1", b!.brief_binding)).toMatchObject({ membership: "member", brief_binding: b!.brief_binding });
  expect(await membership("u1", b!.brief_binding)).toMatchObject({ membership: "not_member" });

  // Authority: the read role has EXECUTE and no table access; an invalid token is 22023.
  await expect(membership("b1", "nothex")).rejects.toMatchObject({ code: "22023" });
  await expect(asReader((c) => c.query("SELECT 1 FROM sessions.session_brief_members LIMIT 1"))).rejects.toMatchObject({ code: "42501" });
});
