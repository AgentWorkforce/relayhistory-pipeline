/**
 * 20261105060000: the scrubbed long summary columns on sessions.session_directory, the trigger that
 * keeps them beside a live short summary, and the read-role reader sessions.session_long_summary_for.
 * In-process PGlite with every supabase migration applied; synthetic ids and text only.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { KEY, code, database, hoopDb, pointer, read } from "../../session-brief/__tests__/harness.ts";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";
import { clearShortSummary, writeShortSummary } from "../session-directory-summary.ts";
import { LONG_SUMMARY_MAX_CHARS, LongSummaryRejected, clearLongSummary, writeLongSummary } from "../session-long-summary.ts";

const ROLE = read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const NAME = "upstream:20261105060000_session_long_summary.sql";
const UP = "supabase/migrations/20261105060000_session_long_summary.sql";
const DOWN = "supabase/rollbacks/20261105060000_session_long_summary.down.sql";
const SHA = createHash("sha256").update(read(UP)).digest("hex");
type Db = Awaited<ReturnType<typeof database>>;
const T0 = new Date("2026-11-09T00:00:00Z");
const T1 = new Date("2026-11-09T01:00:00Z");
const LONG = "The agent refactored the long summary writer and added tests for the read route.";

async function withDb(run: (pg: Db) => Promise<void>) {
  const pg = await database();
  try { await pg.exec(ROLE); await pointer(pg); await run(pg); } finally { await pg.close(); }
}
const db = (pg: Db) => hoopDb(pg) as never;
const short = (pg: Db, at = T0) => writeShortSummary(db(pg), { ...KEY, summary: "Short summary.", source: "bob-costas/extractive/v1", generatedAt: at });
const long = (pg: Db, summary = LONG, at = T0, source = "bob-costas/extractive/v1") =>
  writeLongSummary(db(pg), { ...KEY, summary, source, generatedAt: at });
async function reader(pg: Db, sid = KEY.sessionId, tenant = [KEY.orgId, KEY.workspaceId], workspace = KEY.workspaceId) {
  await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
  try {
    await pg.query(TENANT_SETTINGS_SQL, tenant);
    const r = await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_long_summary_for($1,$2,$3)", [workspace, KEY.harness, sid]);
    await pg.exec("COMMIT");
    return r.rows;
  } catch (e) { await pg.exec("ROLLBACK"); throw e; } finally { await pg.exec("RESET ROLE"); }
}
const stored = async (pg: Db) =>
  (await pg.query<{ long_summary: string | null; long_summary_source: string | null; long_summary_scrub_ruleset: string | null }>(
    "SELECT long_summary, long_summary_source, long_summary_scrub_ruleset FROM sessions.session_directory WHERE session_id=$1", [KEY.sessionId])).rows[0]!;
async function down(pg: Db) {
  try { await pg.exec(read(DOWN)); return "accepted"; } catch (error) { await pg.exec("ROLLBACK"); return (error as { code?: string }).code; }
}

test("writer and reader: needs a short summary first, provenance stored, read role sees only its own tenant", async () => withDb(async (pg) => {
  // No short summary yet: nothing is written (the long stage may finish first).
  expect((await long(pg)).written).toBe(false);
  expect(await reader(pg)).toEqual([]);
  expect((await short(pg)).written).toBe(true);
  expect((await long(pg)).written).toBe(true);
  expect(await stored(pg)).toEqual({ long_summary: LONG, long_summary_source: "bob-costas/extractive/v1", long_summary_scrub_ruleset: "relayhistory-scrub" });
  const rows = await reader(pg);
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({ org_id: KEY.orgId, workspace_id: KEY.workspaceId, harness: KEY.harness, session_id: KEY.sessionId, long_summary: LONG, source: "bob-costas/extractive/v1" });
  // Tenancy: a foreign org sees nothing, a workspace outside the tenant binding is refused.
  expect(await reader(pg, KEY.sessionId, ["org-foreign", KEY.workspaceId])).toEqual([]);
  expect(await code(reader(pg, KEY.sessionId, [KEY.orgId, "ws-foreign"]))).toBe("42501");
  expect(await code(reader(pg, ""))).toBe("22023");
  expect(await reader(pg, "s-unknown")).toEqual([]);
}), 120000);

test("monotonic generated_at, refusals clear only older text, and the database bound holds", async () => withDb(async (pg) => {
  await short(pg);
  expect((await long(pg, LONG, T1)).written).toBe(true);
  // Older generation never overwrites a newer one.
  expect((await long(pg, "An older long summary.", T0)).written).toBe(false);
  expect((await stored(pg)).long_summary).toBe(LONG);
  // A refused older summary does not clear the newer one.
  expect(await long(pg, "   ", T0).catch((e) => (e as LongSummaryRejected).reason)).toBe("empty");
  expect((await stored(pg)).long_summary).toBe(LONG);
  // A refused newer summary clears the older one.
  expect(await long(pg, "x".repeat(LONG_SUMMARY_MAX_CHARS + 1), new Date("2026-11-09T02:00:00Z")).catch((e) => (e as LongSummaryRejected).reason)).toBe("over_length");
  expect((await stored(pg)).long_summary).toBeNull();
  // Exactly the bound is accepted (code points, not UTF-16 units).
  const bound = "\u{1F600}".repeat(LONG_SUMMARY_MAX_CHARS);
  expect((await long(pg, bound, new Date("2026-11-09T03:00:00Z"), "bob-costas/model/v1")).written).toBe(true);
  expect((await stored(pg)).long_summary).toBe(bound);
  // The database refuses an over-length or provenance-less long summary even without the writer.
  expect(await code(pg.query("UPDATE sessions.session_directory SET long_summary=repeat('y',8001) WHERE session_id=$1", [KEY.sessionId]))).toBe("23514");
  expect(await code(pg.query("UPDATE sessions.session_directory SET long_summary_source=NULL WHERE session_id=$1", [KEY.sessionId]))).toBe("23514");
  expect(await clearLongSummary(db(pg), KEY)).toBe(true);
  expect(await clearLongSummary(db(pg), KEY)).toBe(false);
}), 120000);

test("the trigger clears the long summary whenever the short summary goes or the row is tombstoned", async () => withDb(async (pg) => {
  await short(pg); await long(pg);
  // The short writer's own clear path (a non-ready short summary): the trigger clears the long one in the same write.
  expect(await clearShortSummary(db(pg), KEY)).toBe(true);
  expect(await stored(pg)).toEqual({ long_summary: null, long_summary_source: null, long_summary_scrub_ruleset: null });
  expect(await reader(pg)).toEqual([]);
  await short(pg, T1); await long(pg, LONG, T1);
  expect(await reader(pg)).toHaveLength(1);
  await pg.query("UPDATE sessions.session_index SET deleted_at=now()");
  expect(await reader(pg)).toEqual([]);
  await pg.query("UPDATE sessions.session_directory SET deleted_at=now() WHERE session_id=$1", [KEY.sessionId]);
  expect((await stored(pg)).long_summary).toBeNull();
}), 120000);

test("ACL: EXECUTE for bulls96_read_api only, columns closed to end-user roles; rollback guards then applies cleanly; up again", async () => withDb(async (pg) => {
  await short(pg); await long(pg);
  const acl = await pg.query<{ role: string; allowed: boolean; trigger: boolean }>(
    "SELECT rolname AS role, has_function_privilege(oid,'sessions.session_long_summary_for(text,text,text)','EXECUTE') AS allowed, has_function_privilege(oid,'sessions.session_directory_long_summary_follows_short()','EXECUTE') AS trigger FROM pg_roles WHERE rolname IN ('bulls96_read_api','anon','authenticated','service_role','search_consumer_runtime')");
  expect(acl.rows.length).toBeGreaterThan(1);
  for (const r of acl.rows) { expect(r.allowed).toBe(r.role === "bulls96_read_api"); expect(r.trigger).toBe(false); }
  const cols = await pg.query<{ role: string; allowed: boolean }>(
    "SELECT rolname AS role, has_column_privilege(rolname,'sessions.session_directory','long_summary','SELECT') AS allowed FROM pg_roles WHERE rolname IN ('anon','authenticated','bulls96_read_api')");
  for (const r of cols.rows) expect(r.allowed).toBe(false);
  // Without a ledger, with a wrong checksum, or under a later migration the rollback refuses and keeps everything.
  expect(await down(pg)).toBe("55000");
  await pg.exec("CREATE TABLE sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())");
  expect(await down(pg)).toBe("55000");
  await pg.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,repeat('0',64))", [NAME]);
  expect(await down(pg)).toBe("55000");
  await pg.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2", [SHA, NAME]);
  await pg.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES('upstream:99999999999999_later.sql',repeat('a',64))");
  expect(await down(pg)).toBe("55000");
  expect((await stored(pg)).long_summary).toBe(LONG);
  await pg.exec("DELETE FROM sessions.__96bulls_migrations WHERE name='upstream:99999999999999_later.sql'");
  expect(await down(pg)).toBe("accepted");
  expect((await pg.query("SELECT name FROM sessions.__96bulls_migrations WHERE name=$1", [NAME])).rows).toEqual([]);
  expect((await pg.query<{ n: number }>("SELECT count(*)::int n FROM pg_attribute WHERE attrelid='sessions.session_directory'::regclass AND attname LIKE 'long_summary%' AND NOT attisdropped")).rows[0]!.n).toBe(0);
  expect((await pg.query("SELECT to_regprocedure('sessions.session_long_summary_for(text,text,text)') AS fn")).rows).toEqual([{ fn: null }]);
  expect((await pg.query("SELECT to_regprocedure('sessions.session_directory_long_summary_follows_short()') AS fn")).rows).toEqual([{ fn: null }]);
  // The short summary is untouched; running the rollback again refuses.
  expect((await pg.query<{ s: string | null }>("SELECT short_summary s FROM sessions.session_directory WHERE session_id=$1", [KEY.sessionId])).rows[0]!.s).toBe("Short summary.");
  expect(await down(pg)).toBe("55000");
  await pg.exec(read(UP));
  expect(await reader(pg)).toEqual([]);
  expect((await long(pg)).written).toBe(true);
  expect(await reader(pg)).toHaveLength(1);
}), 120000);
