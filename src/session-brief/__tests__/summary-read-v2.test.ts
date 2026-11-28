/**
 * T130 Reader provenance: sessions.session_brief_summary_v2_for (slot 062500, provisional) serves exactly
 * v1's row under v1's authority, plus the served brief's revision and source revisions, and the read
 * client uses it when installed and falls back to v1 (revision null) when it is not.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { database, pointer, KEY, hoopDb, read, code } from "./harness.ts";
import { briefSourceBinding, livePointers, recordBrief, type BriefText } from "../store.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../../session-read-api/liveness.ts";
import { BriefSummaryError, briefSummaryRevision, sqlBriefSummary } from "../../session-read-api/brief-summary.ts";

type Pg = Awaited<ReturnType<typeof database>>;
const ROLE = read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const text = (description: string): BriefText => ({
  title: "Native title", description, state: "completed", scrubRuleset: "relayhistory-scrub", scrubVersion: 1,
  fingerprint: "f".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna",
  messageCount: 2, shortenedMessages: 0, inputTokens: 10, outputTokens: 5, durationMs: 3, generatedAt: new Date("2026-11-09T00:00:00Z"),
});
const ready = (d: string) => ({ status: "ready" as const, text: text(d) });
const NAME = "upstream:20261105062500_session_brief_summary_v2_read.sql";
const UP = "supabase/migrations/20261105062500_session_brief_summary_v2_read.sql";
const DOWN = "supabase/rollbacks/20261105062500_session_brief_summary_v2_read.down.sql";
const SHA = createHash("sha256").update(read(UP)).digest("hex");
const DIGEST_B = "cd".repeat(32);

async function withDb(run: (pg: Pg) => Promise<void>) {
  const pg = await database();
  try { await pg.exec(ROLE); await pointer(pg); await run(pg); } finally { await pg.close(); }
}
async function asReader<T>(pg: Pg, work: (pg: Pg) => Promise<T>, tenant = [KEY.orgId, KEY.workspaceId]): Promise<T> {
  await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
  try { await pg.query(TENANT_SETTINGS_SQL, tenant); const r = await work(pg); await pg.exec("COMMIT"); return r; }
  catch (e) { await pg.exec("ROLLBACK"); throw e; }
  finally { await pg.exec("RESET ROLE"); }
}
const rows = (pg: Pg, fn: "session_brief_summary_for" | "session_brief_summary_v2_for", sid = KEY.sessionId, tenant?: string[]) =>
  asReader(pg, async (q) => (await q.query<Record<string, unknown>>(`SELECT * FROM sessions.${fn}($1,$2,$3)`, [KEY.workspaceId, KEY.harness, sid])).rows, tenant);
const v2 = (pg: Pg, sid?: string, tenant?: string[]) => rows(pg, "session_brief_summary_v2_for", sid, tenant);
const runner = (pg: Pg): TenantRunner => (org, ws, work) => asReader(pg, (q) => work(q as never), [org, ws]);
const served = async (pg: Pg) => {
  const r = await sqlBriefSummary(runner(pg)).read(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId);
  return r === null ? null : { description: r[0]?.description ?? null, revision: briefSummaryRevision(r) };
};

test("v2 is v1's row plus the served revision and source revisions", async () => withDb(async (pg) => {
  await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, ready("First."));
  const [one] = await rows(pg, "session_brief_summary_for");
  const [two] = await v2(pg);
  expect(two).toEqual({ ...one, brief_revision: 1, source_revisions: { "m-1": 1 }, brief_binding: expect.stringMatching(/^[0-9a-f]{64}$/) });
  expect(await served(pg)).toEqual({ description: "First.", revision: 1 });
}), 60000);

// REVISION PROVENANCE ONLY: recordBrief is called with supplied revisions, so E2 is never fed through
// generation here. Proof that the generator consumed E2 is membership (summary-membership.test.ts).
test("revision provenance: a brief at the old revision is not served once E2 commits; the brief recorded at revision 2 reports it", async () => withDb(async (pg) => {
  await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, ready("Before E2."));
  expect(await served(pg)).toEqual({ description: "Before E2.", revision: 1 });
  // E2 commits (revision 2, backdated events included): the old capture does not cover it and is not served.
  await pointer(pg, { revision: 2, manifestKey: "manifest/s-1/m-1/r2" });
  expect(await v2(pg)).toEqual([]);
  expect(await served(pg)).toEqual({ description: null, revision: null });
  // A brief recorded at revision 2 is served and reports revision 2 (provenance only, not consumption).
  await recordBrief(hoopDb(pg), KEY, { "m-1": 2 }, ready("After E2."));
  expect(await served(pg)).toEqual({ description: "After E2.", revision: 2 });
  expect((await v2(pg))[0]).toMatchObject({ source_revisions: { "m-1": 2 } });
}), 60000);

test("acceptance: equal-revision race, B is served, A's late write is refused, B is still served", async () => withDb(async (pg) => {
  const db = hoopDb(pg);
  const bindingA = briefSourceBinding(await livePointers(db, KEY)); // A's input captured
  // Re-created at the SAME revision with other content (B), and B's brief publishes first.
  await pg.query("UPDATE sessions.session_index SET content_sha256 = decode($1,'hex')", [DIGEST_B]);
  const bindingB = briefSourceBinding(await livePointers(db, KEY));
  expect(await recordBrief(db, KEY, { "m-1": 1 }, ready("Brief B."), { sourceBinding: bindingB })).toEqual({ written: true });
  expect(await served(pg)).toEqual({ description: "Brief B.", revision: 1 });
  // A finishes late at the same revision: refused under the guard.
  expect(await recordBrief(db, KEY, { "m-1": 1 }, ready("Brief A."), { sourceBinding: bindingA })).toEqual({ written: false });
  expect(await served(pg)).toEqual({ description: "Brief B.", revision: 1 });
}), 60000);

test("v2 keeps v1's authority: tenant pin, invalid key, alias refusal, read-role-only EXECUTE, no table grant", async () => withDb(async (pg) => {
  await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, ready("First."));
  expect(await v2(pg, KEY.sessionId, ["foreign", KEY.workspaceId])).toEqual([]);
  expect(await code(v2(pg, KEY.sessionId, [KEY.orgId, "foreign"]))).toBe("42501");
  expect(await code(v2(pg, ""))).toBe("22023");
  await pointer(pg, { key: { ...KEY, sessionId: "second" } });
  await pg.query("UPDATE sessions.session_index SET canonical_session_id='logical-1'");
  expect(await code(v2(pg, "logical-1"))).toBe("0A000");
  const acl = await pg.query<{ role: string; allowed: boolean }>(
    "SELECT rolname AS role, has_function_privilege(oid,'sessions.session_brief_summary_v2_for(text,text,text)','EXECUTE') AS allowed FROM pg_roles WHERE rolname IN ('bulls96_read_api','anon','authenticated','service_role','search_consumer_runtime')");
  expect(acl.rows.length).toBeGreaterThan(0);
  for (const r of acl.rows) expect(r.allowed).toBe(r.role === "bulls96_read_api");
  expect((await pg.query<{ allowed: boolean }>("SELECT has_table_privilege('bulls96_read_api','sessions.session_briefs','SELECT') AS allowed")).rows[0]!.allowed).toBe(false);
}), 60000);

test("rollback needs exact applied history and refuses later migrations; after it the client falls back to v1 (revision null); up again", async () => withDb(async (pg) => {
  await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, ready("First."));
  const down = async () => { try { await pg.exec(read(DOWN)); return "accepted"; } catch (e) { await pg.exec("ROLLBACK"); return (e as { code?: string }).code; } };
  expect(await down()).toBe("55000"); // no ledger
  await pg.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())");
  await pg.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,repeat('0',64))", [NAME]);
  expect(await down()).toBe("55000"); // wrong checksum
  await pg.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2", [SHA, NAME]);
  await pg.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES('upstream:99999999999999_later.sql',repeat('a',64))");
  expect(await down()).toBe("55000"); // a later migration
  expect(await served(pg)).toEqual({ description: "First.", revision: 1 });
  await pg.exec("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:99999999999999_later.sql'");
  expect(await down()).toBe("accepted");
  expect((await pg.query("SELECT to_regprocedure('sessions.session_brief_summary_v2_for(text,text,text)') AS fn")).rows).toEqual([{ fn: null }]);
  expect((await pg.query("SELECT name FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).rows).toEqual([]);
  // v1 untouched; the client serves it with no revision, exactly as before T130.
  expect(await served(pg)).toEqual({ description: "First.", revision: null });
  await pg.exec(read(UP));
  expect(await served(pg)).toEqual({ description: "First.", revision: 1 });
}), 60000);

test("with v2 installed the client keeps v1's authorization failures (refused, invalid key, unqualified alias) and reads v2", async () => withDb(async (pg) => {
  await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, ready("First."));
  const client = sqlBriefSummary(runner(pg));
  const reason = async (p: Promise<unknown>) => p.then(() => "resolved", (e: unknown) => (e instanceof BriefSummaryError ? e.reason : `other:${String(e)}`));
  // The tenant pin: a foreign workspace for the bound org is refused (42501), as under v1.
  expect(await reason(sqlBriefSummary((org, _ws, work) => runner(pg)(org, "foreign", work)).read(KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId))).toBe("refused");
  expect(await reason(client.read(KEY.orgId, KEY.workspaceId, KEY.harness, ""))).toBe("invalid_argument");
  await pointer(pg, { key: { ...KEY, sessionId: "second" } });
  await pg.query("UPDATE sessions.session_index SET canonical_session_id='logical-1'");
  expect(await reason(client.read(KEY.orgId, KEY.workspaceId, KEY.harness, "logical-1"))).toBe("unqualified");
  // And the served row is v2's (it carries the revision fields).
  await pg.exec("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='second'");
  const rows = (await client.read(KEY.orgId, KEY.workspaceId, KEY.harness, "logical-1"))!;
  expect(rows[0]).toMatchObject({ description: "First.", brief_revision: 1, source_revisions: { "m-1": 1 } });
}), 60000);
