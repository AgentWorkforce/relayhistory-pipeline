/**
 * 062150 relayfacts claims: the new tables and functions, grants, the decision tables untouched, dedupe and merge
 * rules, supersede, the claims reader (claims plus decisions), workspace isolation, the read API unchanged, the
 * extractor and supersession through the real functions, both erases reaching claims, and the rollback.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite, type Transaction } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { claimsHook, postgresClaimsPort } from "../../session-analysis/claims.ts";
import { decisionRecordsHook, postgresDecisionRecordsSink } from "../../session-analysis/decision-records.ts";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../../session-analysis/processor.ts";
import { relayfactsMarkdown, relayfactsTopicsResponse, sqlRelayfacts } from "../../session-read-api/relayfacts.ts";
import { inventoryOf, RUNTIME_APPLICATION_FUNCTIONS } from "../migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062150_relayfacts_claims.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062150_relayfacts_claims.down.sql");
// Later upstream steps that migrated() also applies; their downs come off first (newest first).
const LATER_STEPS = ["upstream:20261105062156_lazy_ack_history_exact.sql"];
const LATER_DOWNS = [read("supabase/rollbacks/20261105062156_lazy_ack_history_exact.down.sql")];
const MEMORY = read("supabase/migrations/20261105062080_memory_documents.sql");
const ERASE_SQL = read("supabase/migrations/20261105062090_relayfacts_erase.sql");
const USER_ERASE_SQL = read("supabase/migrations/20261105062100_relayfacts_user_erase.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const INSERT = "sessions.relayfacts_claims_insert_v1(text,text,jsonb)";
const SUPERSEDE = "sessions.relayfacts_claims_supersede_v1(text,text,uuid,uuid)";
const READER = "sessions.relayfacts_claims_v1(text,text,text,text,timestamp with time zone,integer)";
const GUARD = "sessions.relayfacts_claims_fence_guard_v1()";
const ERASE = "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)";
const USER_ERASE = "sessions.relayfacts_user_erase_v1(text,text,text,text)";

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;
async function migrated() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await runMigrations(client(db), { profile: LOCAL });
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  return db;
}
/** The body of `name` between its dollar tags in `src`. */
const bodyOf = (src: string, name: string, tag: string) => {
  const start = src.search(new RegExp(`CREATE(?: OR REPLACE)? FUNCTION sessions\\.${name}\\(`));
  const open = src.indexOf(tag, start) + tag.length;
  return src.slice(open, src.indexOf(tag, open));
};

type Ev = { source: string; session_id: string; turn_start: number; turn_end: number; claim: string; ref?: string | null };
type Rec = { kind?: string; scope?: string; observed_at?: string | null; topic: string; title: string; statement: string; status?: string; evidence: Ev[] };
const ev = (session = "sess-1", claim = "claim", over: Partial<Ev> = {}): Ev => ({ source: "claude", session_id: session, turn_start: 0, turn_end: 1, claim, ...over });
const rec = (over: Partial<Rec> = {}): Rec => ({
  topic: "src-session-read-api", title: "Use keyset paging", statement: "Pages are keyed on created_at and id.", evidence: [ev()], ...over,
});
const claimRec = (over: Partial<Rec> = {}): Rec => rec({ kind: "constraint", ...over });
type Counts = { inserted: number; merged: number; evidence_added: number };
type ReaderRow = { id: string; kind: string; scope: string | null; topic: string; title: string; statement: string; status: string; observed_at: string | null; updated_at: string; evidence: Record<string, unknown>[] };

function harness(get: () => PGlite) {
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    get().transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const decide = (org: string, ws: string, list: unknown) =>
    as("service_role", async (tx) => (await tx.query<Counts>(
      "SELECT * FROM sessions.relayfacts_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)])).rows[0]!);
  const claim = (org: string, ws: string, list: unknown) =>
    as("service_role", async (tx) => (await tx.query<Counts>(
      "SELECT * FROM sessions.relayfacts_claims_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)])).rows[0]!);
  const supersede = (org: string, ws: string, oldId: string, newId: string) =>
    as("service_role", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.relayfacts_claims_supersede_v1($1,$2,$3::uuid,$4::uuid) AS ok", [org, ws, oldId, newId])).rows[0]!.ok);
  const reader = (org: string, ws: string, f: { topic?: string; kind?: string; since?: string; limit?: number } = {}) =>
    as("service_role", async (tx) => (await tx.query<ReaderRow>(
      "SELECT * FROM sessions.relayfacts_claims_v1($1,$2,$3,$4,$5::timestamptz,$6)",
      [org, ws, f.topic ?? null, f.kind ?? null, f.since ?? null, f.limit ?? 500])).rows);
  const claims = async (org = "org-a") => (await get().query<{ ws: string; kind: string; scope: string; title: string; statement: string; status: string; observed_at: string | null; evidence: string[] }>(
    `SELECT c.workspace_id ws, c.kind, c.scope, c.title, c.statement, c.status, c.observed_at::text observed_at,
            coalesce((SELECT array_agg(e.source || '/' || e.session_id ORDER BY e.source, e.session_id) FROM sessions.relayfacts_claim_evidence_v1 e WHERE e.claim_id = c.id), '{}') evidence
       FROM sessions.relayfacts_claims_v1 c WHERE c.org_id = $1 ORDER BY c.workspace_id, c.kind, c.title`, [org])).rows;
  const decisions = async (org = "org-a") => (await get().query<{ ws: string; title: string; statement: string; evidence: string[] }>(
    `SELECT t.workspace_id ws, t.title, t.statement,
            coalesce((SELECT array_agg(e.source || '/' || e.session_id ORDER BY e.source, e.session_id) FROM sessions.relayfacts_evidence_v1 e WHERE e.decision_id = t.id), '{}') evidence
       FROM sessions.relayfacts_v1 t WHERE t.org_id = $1 ORDER BY t.workspace_id, t.title`, [org])).rows;
  return { as, decide, claim, supersede, reader, claims, decisions };
}

describe("20261105062150 relayfacts claims", () => {
  let db: PGlite;
  const { as, decide, claim, supersede, reader, claims } = harness(() => db);
  const reset = () => db.exec("DELETE FROM sessions.relayfacts_claims_v1; DELETE FROM sessions.relayfacts_v1");
  beforeAll(async () => { db = await migrated(); });
  afterAll(async () => { await db?.close(); });

  test("inventory and sentinel: two new tables built empty with their indexes; relayfacts_v1, its evidence and the 062080 functions are untouched", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.relayfacts_claims_fence_guard_v1", "function:sessions.relayfacts_claims_insert_v1",
      "function:sessions.relayfacts_claims_supersede_v1", "function:sessions.relayfacts_claims_v1",
      "function:sessions.relayfacts_evidence_erase_session_v1", "function:sessions.relayfacts_user_erase_v1",
      "index:relayfacts_claim_evidence_v1_dedupe", "index:relayfacts_claims_v1_dedupe", "index:relayfacts_claims_v1_updated",
      "relation:sessions.relayfacts_claim_evidence_v1", "relation:sessions.relayfacts_claims_v1",
      "trigger:relayfacts_claims_fence_guard", "trigger:relayfacts_claims_lock_evidence", "trigger:relayfacts_claims_lock_records",
    ]);
    expect(SQL).not.toMatch(/CONCURRENTLY|ALTER TABLE sessions\.relayfacts_(v1|evidence_v1)\b|INDEX[^\n]*ON sessions\.relayfacts_(v1|evidence_v1) /i);
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    const cols = async (t: string) => (await db.query<{ column_name: string }>(
      "SELECT column_name FROM information_schema.columns WHERE table_schema='sessions' AND table_name=$1 ORDER BY ordinal_position", [t])).rows.map((c) => c.column_name);
    expect(await cols("relayfacts_v1")).toEqual(["id", "org_id", "workspace_id", "topic", "title", "title_key", "statement", "status", "supersedes", "decided_at", "implemented_at", "implemented_ref", "created_at", "updated_at"]);
    expect(await cols("relayfacts_claims_v1")).toEqual(["id", "org_id", "workspace_id", "kind", "scope", "topic", "title", "title_key", "statement", "status", "supersedes", "observed_at", "created_at", "updated_at"]);
    expect(await cols("relayfacts_claim_evidence_v1")).toEqual(["id", "claim_id", "source", "session_id", "turn_start", "turn_end", "claim", "ref", "created_at"]);
    const defs = async (t: string) => (await db.query<{ d: string }>("SELECT indexdef d FROM pg_indexes WHERE schemaname='sessions' AND tablename=$1 ORDER BY indexname", [t])).rows.map((r) => r.d);
    expect(await defs("relayfacts_v1")).toContain("CREATE UNIQUE INDEX relayfacts_v1_dedupe ON sessions.relayfacts_v1 USING btree (org_id, workspace_id, topic, title_key)");
    expect(await defs("relayfacts_claims_v1")).toContain("CREATE UNIQUE INDEX relayfacts_claims_v1_dedupe ON sessions.relayfacts_claims_v1 USING btree (org_id, workspace_id, kind, topic, title_key)");
    // The title key is the same expression as 062080's.
    const key = async (t: string) => (await db.query<{ e: string }>(
      "SELECT pg_get_expr(d.adbin, d.adrelid) e FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid=$1::regclass AND a.attname='title_key'", [t])).rows[0]!.e;
    expect(await key("sessions.relayfacts_claims_v1")).toBe(await key("sessions.relayfacts_v1"));
    for (const [name, tag] of [["relayfacts_insert_v1", "$relayfacts_insert$"], ["relayfacts_supersede_v1", "$relayfacts_supersede$"], ["relayfacts_topics_v1", "$relayfacts_topics$"], ["relayfacts_list_v1", "$relayfacts_list$"]] as const)
      expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname=$1", [name])).rows.map((r) => r.s)).toEqual([bodyOf(MEMORY, name, tag)]);
    for (const t of ["relayfacts_claims_v1", "relayfacts_claim_evidence_v1"])
      expect((await db.query<{ rls: boolean }>("SELECT relrowsecurity rls FROM pg_class WHERE oid=$1::regclass", [`sessions.${t}`])).rows[0]!.rls).toBe(true);
  });

  test("grants: the writer, supersede and reader for service_role (and the runtime role) only; the guard owner-only; the replaced erases keep their ACL; no table privileges", async () => {
    for (const fn of [INSERT, SUPERSEDE, READER, GUARD, ERASE, USER_ERASE]) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect([fn, r.secdef, r.cfg, r.public_exec]).toEqual([fn, fn !== USER_ERASE, ["search_path=pg_catalog, sessions, pg_temp"], false]);
    }
    const can = async (role: string, fn: string) =>
      (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of [INSERT, SUPERSEDE, READER, GUARD]) {
      for (const role of ["anon", "authenticated", "search_consumer_runtime", "bulls96_read_api", "bulls96_session_ingest"]) expect([fn, role, await can(role, fn)]).toEqual([fn, role, false]);
      expect(await can("service_role", fn)).toBe(fn !== GUARD);
    }
    expect([await can("bulls96_session_ingest", ERASE), await can("service_role", ERASE)]).toEqual([true, false]);
    for (const role of ["service_role", "bulls96_session_ingest", "bulls96_read_api"]) expect(await can(role, USER_ERASE)).toBe(false);
    expect(RUNTIME_APPLICATION_FUNCTIONS).toEqual(expect.arrayContaining([INSERT, SUPERSEDE, READER]));
    expect(RUNTIME_APPLICATION_FUNCTIONS).not.toContain(GUARD);
    for (const table of ["sessions.relayfacts_claims_v1", "sessions.relayfacts_claim_evidence_v1"])
      for (const role of ["service_role", "bulls96_read_api", "bulls96_session_ingest", "search_consumer_runtime"])
        for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
          expect((await db.query<{ ok: boolean }>("SELECT has_table_privilege($1, $2, $3) ok", [role, table, priv])).rows[0]!.ok).toBe(false);
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.relayfacts_claims_v1('org-a','ws-a',NULL,NULL,NULL,10)"), ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT 1 FROM sessions.relayfacts_claims_v1")))).toBe("42501");
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("insert: intents and constraints only, validated; kind is in the unique key; a merge widens the scope, keeps the earliest time and moves updated_at", async () => {
    await reset();
    const bad = async (over: Record<string, unknown>) => code(claim("org-a", "ws-a", [{ ...claimRec(), ...over }]));
    expect(await bad({ kind: undefined })).toBe("22023");
    expect(await bad({ kind: "decision" })).toBe("22023");
    expect(await bad({ scope: "global" })).toBe("22023");
    expect(await bad({ observed_at: 12 })).toBe("22023");
    // observed_at: a finite ISO 8601 timestamp with a zone only; keywords and zone-less or date-only values refuse.
    for (const t of ["not a time", "now", "infinity", "-infinity", "epoch", "today", "2026-11-18", "2026-11-18 10:00:00", "2026-11-18T10:00:00"])
      expect([t, await bad({ observed_at: t })]).toEqual([t, "22023"]);
    expect(await bad({ status: "superseded" })).toBe("22023");
    expect(await bad({ evidence: [] })).toBe("22023");
    expect(await bad({ evidence: Array.from({ length: 11 }, (_, i) => ev(`s${i}`)) })).toBe("22023");
    expect(await bad({ topic: "Bad Topic" })).toBe("23514");
    expect(await bad({ title: "t".repeat(201) })).toBe("23514");
    expect(await bad({ statement: "two\nlines" })).toBe("23514");
    expect(await bad({ evidence: [ev("s", "c".repeat(301))] })).toBe("23514");
    expect(await bad({ evidence: [ev("s", "c", { ref: "https://example.com/x" })] })).toBe("23514");
    expect(await code(claim("org-a", "ws-a", Array.from({ length: 101 }, (_, i) => claimRec({ title: `t${i}` }))))).toBe("22023");
    expect(await code(claim("", "ws-a", [claimRec()]))).toBe("22023");
    expect((await claims()).length).toBe(0);
    // The same title as a constraint, an intent and a decision: three records.
    expect(await claim("org-a", "ws-a", [claimRec({ scope: "project", observed_at: "2026-11-18T10:00:00Z" }), claimRec({ kind: "intent" })])).toEqual({ inserted: 2, merged: 0, evidence_added: 2 });
    expect(await decide("org-a", "ws-a", [rec()])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    // Case, whitespace and trailing punctuation merge; a duplicate inside one batch merges.
    expect(await claim("org-a", "ws-a", [claimRec({ title: "  use KEYSET   paging. ", evidence: [ev(), ev("sess-2")] }), claimRec({ title: "Use keyset paging!", evidence: [ev("sess-3")] })]))
      .toEqual({ inserted: 0, merged: 2, evidence_added: 2 });
    const stamp = async () => (await db.query<{ u: string }>("SELECT updated_at::text u FROM sessions.relayfacts_claims_v1 WHERE kind='constraint'")).rows[0]!.u;
    await db.exec("UPDATE sessions.relayfacts_claims_v1 SET updated_at = now() - interval '1 hour'");
    const before = await stamp();
    // Narrower scope, later time, no new evidence: nothing changes.
    expect(await claim("org-a", "ws-a", [claimRec({ scope: "session", observed_at: "2026-11-18T11:00:00Z" })])).toEqual({ inserted: 0, merged: 1, evidence_added: 0 });
    expect(await stamp()).toBe(before);
    // Wider scope and an earlier time without new evidence: nothing moves either (only a merge that adds evidence
    // moves them, so a write whose evidence is all known, or dropped at the cap, cannot widen a claim).
    expect(await claim("org-a", "ws-a", [claimRec({ scope: "repo", observed_at: "2026-11-18T09:00:00Z" })])).toEqual({ inserted: 0, merged: 1, evidence_added: 0 });
    expect(await stamp()).toBe(before);
    expect((await claims()).filter((r) => r.kind === "constraint").map((r) => [r.scope, r.observed_at])).toEqual([["project", "2026-11-18 10:00:00+00"]]);
    // With a new evidence row: both move, and updated_at with them; a null time never clears one.
    expect(await claim("org-a", "ws-a", [claimRec({ scope: "repo", observed_at: "2026-11-18T09:00:00Z", evidence: [ev("sess-8")] })])).toEqual({ inserted: 0, merged: 1, evidence_added: 1 });
    expect(await stamp()).not.toBe(before);
    await claim("org-a", "ws-a", [claimRec({ observed_at: null, evidence: [ev("sess-9")] })]);
    expect((await claims()).map((r) => [r.kind, r.scope, r.observed_at, r.title, r.evidence])).toEqual([
      ["constraint", "repo", "2026-11-18 09:00:00+00", "Use keyset paging", ["claude/sess-1", "claude/sess-2", "claude/sess-3", "claude/sess-8", "claude/sess-9"]],
      ["intent", "session", null, "Use keyset paging", ["claude/sess-1"]],
    ]);
    // The decision is untouched by any of this.
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_evidence_v1")).rows[0]!.n).toBe(1);
    // At most 50 evidence rows per claim.
    for (let i = 0; i < 6; i++) await claim("org-a", "ws-a", [claimRec({ evidence: Array.from({ length: 10 }, (_, j) => ev(`cap-${i}-${j}`)) })]);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_claim_evidence_v1 e JOIN sessions.relayfacts_claims_v1 c ON c.id=e.claim_id WHERE c.kind='constraint'")).rows[0]!.n).toBe(50);
  });

  test("supersede: same workspace and kind only, never twice, never a superseded claim", async () => {
    await reset();
    await claim("org-a", "ws-a", [claimRec({ title: "Old" }), claimRec({ title: "New" }), claimRec({ title: "Other" }), claimRec({ kind: "intent", title: "Intent" })]);
    await claim("org-a", "ws-b", [claimRec({ title: "Elsewhere" })]);
    const id = async (title: string) => (await db.query<{ id: string }>("SELECT id FROM sessions.relayfacts_claims_v1 WHERE title=$1", [title])).rows[0]!.id;
    expect(await supersede("org-a", "ws-a", await id("Old"), await id("Intent"))).toBe(false);
    expect(await supersede("org-a", "ws-a", await id("Old"), await id("Elsewhere"))).toBe(false);
    expect(await supersede("org-a", "ws-b", await id("Old"), await id("New"))).toBe(false);
    expect(await supersede("org-a", "ws-a", await id("Old"), await id("Old"))).toBe(false);
    expect(await supersede("org-a", "ws-a", await id("Old"), await id("New"))).toBe(true);
    expect(await supersede("org-a", "ws-a", await id("Old"), await id("Other"))).toBe(false);
    expect(await supersede("org-a", "ws-a", await id("Other"), await id("New"))).toBe(false);
    expect((await claims()).filter((r) => r.ws === "ws-a").map((r) => [r.title, r.status])).toEqual([
      ["New", "decided"], ["Old", "superseded"], ["Other", "decided"], ["Intent", "decided"],
    ]);
  });

  test("the reader: current claims and decisions, filtered by topic, kind and since, newest first, bounded, statement capped at 200, pointers without claim text", async () => {
    await reset();
    await claim("org-a", "ws-a", [
      claimRec({ title: "C one", statement: "s".repeat(1000), scope: "repo" }), claimRec({ kind: "intent", title: "I one" }),
      claimRec({ kind: "intent", title: "I other topic", topic: "general" }),
    ]);
    await decide("org-a", "ws-a", [rec({ title: "D one" }), rec({ title: "D gone" })]);
    const ids = (await db.query<{ id: string; title: string }>("SELECT id, title FROM sessions.relayfacts_v1")).rows;
    await as("service_role", (tx) => tx.query("SELECT sessions.relayfacts_supersede_v1('org-a','ws-a',$1,$2)", [ids.find((r) => r.title === "D gone")!.id, ids.find((r) => r.title === "D one")!.id]));
    await db.exec("UPDATE sessions.relayfacts_claims_v1 SET updated_at = now() - make_interval(mins => array_position(ARRAY['C one','D one','I one','I other topic','D gone'], title))");
    await db.exec("UPDATE sessions.relayfacts_v1 SET updated_at = now() - make_interval(mins => array_position(ARRAY['C one','D one','I one','I other topic','D gone'], title))");
    const all = await reader("org-a", "ws-a");
    expect(all.map((r) => [r.title, r.kind, r.scope])).toEqual([
      ["C one", "constraint", "repo"], ["D one", "decision", null], ["I one", "intent", "session"], ["I other topic", "intent", "session"],
    ]);
    expect(all[0]!.statement.length).toBe(200);
    expect(all[1]!.observed_at).toBeNull();
    expect((await reader("org-a", "ws-a", { kind: "decision" })).map((r) => r.title)).toEqual(["D one"]);
    expect((await reader("org-a", "ws-a", { kind: "intent" })).map((r) => r.title)).toEqual(["I one", "I other topic"]);
    expect((await reader("org-a", "ws-a", { topic: "general" })).map((r) => r.title)).toEqual(["I other topic"]);
    expect((await reader("org-a", "ws-a", { limit: 2 })).map((r) => r.title)).toEqual(["C one", "D one"]);
    expect((await reader("org-a", "ws-a", { since: all[2]!.updated_at })).map((r) => r.title)).toEqual(["C one", "D one", "I one"]);
    for (const r of all) for (const e of r.evidence) expect(Object.keys(e).sort()).toEqual(["created_at", "id", "ref", "session_id", "source", "turn_end", "turn_start"]);
    for (const f of [{ limit: 0 }, { limit: 501 }, { kind: "goal" }, { topic: "Bad Topic" }]) expect(await code(reader("org-a", "ws-a", f))).toBe("22023");
  });

  test("workspace isolation: claims and the reader never cross a workspace or org", async () => {
    await reset();
    await claim("org-a", "ws-a", [claimRec({ title: "Only in A" })]);
    await claim("org-a", "ws-b", [claimRec({ title: "Only in B" })]);
    await claim("org-b", "ws-a", [claimRec({ title: "Only in org B" })]);
    await decide("org-a", "ws-b", [rec({ title: "Decision in B" })]);
    expect((await reader("org-a", "ws-a")).map((r) => r.title)).toEqual(["Only in A"]);
    expect((await reader("org-a", "ws-b")).map((r) => r.title).sort()).toEqual(["Decision in B", "Only in B"]);
    expect((await reader("org-b", "ws-a")).map((r) => r.title)).toEqual(["Only in org B"]);
    expect(await reader("org-a", "ws-c")).toEqual([]);
    expect(await claim("org-a", "ws-b", [claimRec({ title: "Only in A" })])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
  });

  test("read API v1 is unchanged by intent and constraint claims: topics, counts, page and markdown", async () => {
    await reset();
    const run = <T,>(org: string, ws: string, work: (q: { query: Transaction["query"] }) => Promise<T>) =>
      as("bulls96_read_api", (tx) => work(tx), [org, ws]);
    const api = sqlRelayfacts(run as never);
    const view = async () => {
      const topics = relayfactsTopicsResponse((await api.topics("org-a", "ws-a"))!);
      const pages: Record<string, unknown> = {};
      for (const t of ["src-session-read-api", "general", "src-intents-only"]) {
        const page = (await api.list("org-a", "ws-a", t, null, 200))!;
        pages[t] = { page, markdown: relayfactsMarkdown(t, page, { continued: false }) };
      }
      return JSON.stringify({ topics, pages });
    };
    await decide("org-a", "ws-a", [rec(), rec({ title: "Open question", status: "open" }), rec({ topic: "general", title: "General decision" })]);
    const before = await view();
    expect(before).toContain("Use keyset paging");
    await claim("org-a", "ws-a", [
      claimRec({ scope: "repo", observed_at: "2026-11-18T10:00:00Z" }),
      claimRec({ kind: "intent", title: "Open question", status: "open" }),
      claimRec({ title: "Never apply migrations", evidence: [ev("sess-7", "constraint claim text")] }),
      claimRec({ kind: "intent", topic: "general", title: "Intent in general" }),
      claimRec({ kind: "intent", topic: "src-intents-only", title: "Only an intent here" }),
    ]);
    expect(await view()).toBe(before);
    expect(before).not.toContain("Never apply migrations");
    expect(before).not.toContain("src-intents-only\",\"records");
    expect(await api.list("org-a", "ws-a", "src-intents-only", null, 200)).toEqual([]);
  });

  test("the extractor through the real functions: decisions through v1 as before, claims through the claims writer; a dropped claim is superseded", async () => {
    await reset();
    const exec = { query: <R,>(text: string, params?: unknown[]) => as("service_role", async (tx) => (await tx.query<R>(text, params)) as { rows: R[] }) } as never;
    const port = postgresClaimsPort(exec);
    const sink = postgresDecisionRecordsSink(exec);
    const refs = [
      { id: "h0", source: "claude", kind: "human-message", sessionId: "sess-root", at: "2026-11-18T10:05:00Z" },
      { id: "a0", source: "claude", kind: "assistant-message", sessionId: "sess-root", at: "2026-11-18T10:01:00Z" },
    ];
    const snap = (constraints: string[], analyzedAt: string) => ({
      source: "claude", sessionId: "sess-root", analyzedAt,
      evidence: { references: refs }, metrics: { filesChanged: [{ path: "supabase/migrations/x.sql", editCount: 1 }] },
      understanding: {
        decisions: [{ text: "Use slot 062150.", evidenceIds: ["a0"], confidence: "high" }],
        intents: [{ text: "Add a claims reader.", evidenceIds: ["a0"], confidence: "medium" }],
        constraints: constraints.map((text) => ({ text, evidenceIds: ["h0"], confidence: "high" })),
      },
    }) as unknown as SessionAnalysisSnapshot;
    const job = { orgId: "org-a", workspaceId: "ws-a" } as AnalysisJob;
    // The worker's order: the unchanged decisions hook, then the claims hook.
    const hook = async (s: SessionAnalysisSnapshot, counts: Record<string, number>) => {
      const out = { result: { analysis: s } } as unknown as AnalysisJobOutput;
      await decisionRecordsHook(sink, counts)(job, out);
      await claimsHook(port, counts)(job, out);
    };
    const counts: Record<string, number> = {};
    await hook(snap(["Never apply migrations to a real database."], new Date(Date.now() - 60_000).toISOString()), counts);
    expect(counts).toEqual({ decision_records: 1, claim_records: 2 });
    expect((await claims()).map((r) => [r.kind, r.scope, r.observed_at, r.title])).toEqual([
      ["constraint", "project", "2026-11-18 10:05:00+00", "Never apply migrations to a real database."],
      ["intent", "session", "2026-11-18 10:01:00+00", "Add a claims reader."],
    ]);
    expect((await db.query<{ title: string }>("SELECT title FROM sessions.relayfacts_v1")).rows).toEqual([{ title: "Use slot 062150." }]);
    await hook(snap(["Never apply migrations to a real database."], new Date(Date.now() + 60_000).toISOString()), counts);
    expect(counts).toEqual({ decision_records: 2, claim_records: 4 });
    await hook(snap(["Apply no migration to any real database without approval."], new Date(Date.now() + 60_000).toISOString()), counts);
    expect(counts).toEqual({ decision_records: 3, claim_records: 6, claim_records_superseded: 1 });
    expect((await reader("org-a", "ws-a", { kind: "constraint" })).map((r) => r.title)).toEqual(["Apply no migration to any real database without approval."]);
    expect((await claims()).filter((r) => r.kind === "constraint").map((r) => [r.title, r.status])).toEqual([
      ["Apply no migration to any real database without approval.", "decided"], ["Never apply migrations to a real database.", "superseded"],
    ]);
    // An older analysis whose hook runs late supersedes nothing.
    await hook(snap(["Never apply migrations to a real database."], new Date(Date.now() - 3_600_000).toISOString()), counts);
    expect(counts.claim_records_superseded).toBe(1);
  });
});

describe("20261105062150 erases reach the claims", () => {
  let db: PGlite;
  const { as, decide, claim, claims, decisions } = harness(() => db);
  const KEY = ["org-a", "ws-a", "claude", "s-1"] as const;
  const ingest = async <R,>(fn: string, params: unknown[]) => as("bulls96_session_ingest", async (tx) =>
    (await tx.query<{ r: R }>(`SELECT sessions.${fn}(${params.map((_, i) => `$${i + 1}`).join(",")}) AS r`, params)).rows[0]!.r);
  const pointer = (org: string, ws: string, session: string, user: string) => db.query(
    `INSERT INTO sessions.session_index(
      org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id)
      VALUES($1,$2,'claude',$3,'machine',$4,now(),now(),'s3','bucket',$5,decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18')`,
    [org, ws, session, user, `96bulls/${org}/v1/ws=${ws}/m=machine/h=claude/d=2026/11/11/s=${session}/manifest.json`]);
  beforeAll(async () => {
    db = await migrated();
    await pointer("org-a", "ws-a", "s-1", "user");
    await pointer("org-u", "ws-u", "u-1", "user-u");
  });
  afterAll(async () => { await db?.close(); });

  test("062090 per-session erase: sole-session claims go, shared claims are rewritten without time or scope, counted with the decisions; a late claim write is refused (RH016)", async () => {
    const T = "2026-11-18T10:00:00Z";
    await decide("org-a", "ws-a", [rec({ title: "Erased decision", evidence: [ev("s-1", "Erased decision claim.")] })]);
    await claim("org-a", "ws-a", [
      claimRec({ title: "Only the erased session", scope: "project", observed_at: T, evidence: [ev("s-1", "Erased constraint.")] }),
      claimRec({ kind: "intent", title: "Shared intent", scope: "repo", observed_at: T, evidence: [ev("s-1", "Erased intent.")] }),
    ]);
    await claim("org-a", "ws-a", [claimRec({ kind: "intent", title: "Shared intent", evidence: [ev("s-2", "Kept intent claim.")] })]);
    await claim("org-a", "ws-a", [claimRec({ title: "Other source", scope: "project", observed_at: T, evidence: [ev("s-1", "Codex.", { source: "codex" })] })]);
    await claim("org-a", "ws-b", [claimRec({ title: "Other workspace", evidence: [ev("s-1", "Workspace B.")] })]);
    expect(await ingest("ingest_operator_runtime_begin", [...KEY, crypto.randomUUID(), "user", "operator", "bucket", "96bulls", "a".repeat(64)]))
      .toMatchObject({ status: "in_progress", uploadsFenced: true });
    const lease = crypto.randomUUID();
    expect(await ingest("ingest_operator_runtime_next", [lease])).toMatchObject({ sessionIds: ["s-1"] });
    // One decision row and two claim rows; the decision and one claim deleted; one claim rewritten.
    expect(await ingest("relayfacts_evidence_erase_session_v1", [...KEY, lease])).toEqual({ evidenceRows: 3, recordsDeleted: 2, recordsRewritten: 1 });
    expect(await decisions()).toEqual([]);
    expect((await claims()).map((r) => [r.ws, r.kind, r.title, r.scope, r.observed_at, r.statement, r.evidence])).toEqual([
      ["ws-a", "constraint", "Other source", "project", "2026-11-18 10:00:00+00", "Pages are keyed on created_at and id.", ["codex/s-1"]],
      ["ws-a", "intent", "Shared intent", "session", null, "Kept intent claim.", ["claude/s-2"]],
      ["ws-b", "constraint", "Other workspace", "session", null, "Pages are keyed on created_at and id.", ["claude/s-1"]],
    ]);
    expect(await ingest("relayfacts_evidence_erase_session_v1", [...KEY, lease])).toEqual({ evidenceRows: 0, recordsDeleted: 0, recordsRewritten: 0 });
    const before = await claims();
    for (const kind of ["intent", "constraint"])
      expect(await code(claim("org-a", "ws-a", [claimRec({ kind, title: "Shared intent", evidence: [ev("s-1", "Late.")] })]))).toBe("RH016");
    expect(await code(decide("org-a", "ws-a", [rec({ title: "Late decision", evidence: [ev("s-1", "Late.")] })]))).toBe("RH016");
    expect(await claims()).toEqual(before);
    expect(await claim("org-a", "ws-a", [claimRec({ kind: "intent", title: "Shared intent", evidence: [ev("s-3", "Another.")] })])).toMatchObject({ merged: 1, evidence_added: 1 });
  });

  test("062100 per-user erase: the user's claims go or are rewritten without time or scope, fenced; a late claim write is refused (RH016)", async () => {
    await decide("org-u", "ws-u", [rec({ title: "User decision", evidence: [ev("u-1", "U decision.")] })]);
    await claim("org-u", "ws-u", [
      claimRec({ title: "Only user u", scope: "repo", observed_at: "2026-11-18T10:00:00Z", evidence: [ev("u-1", "U claim.")] }),
      claimRec({ kind: "intent", title: "Shared", scope: "project", observed_at: "2026-11-18T10:00:00Z", evidence: [ev("u-1", "U shared.")] }),
    ]);
    await claim("org-u", "ws-u", [claimRec({ kind: "intent", title: "Shared", evidence: [ev("v-1", "V shared.")] })]);
    const erased = (await db.query<{ r: Record<string, number> }>("SELECT sessions.relayfacts_user_erase_v1('req-u','org-u','ws-u','user-u') AS r")).rows[0]!.r;
    expect(erased).toEqual({ sessions: 1, fencesAdded: 1, evidenceRows: 3, recordsDeleted: 2, recordsRewritten: 1 });
    expect(await decisions("org-u")).toEqual([]);
    expect((await claims("org-u")).map((r) => [r.kind, r.title, r.scope, r.observed_at, r.statement, r.evidence])).toEqual([
      ["intent", "Shared", "session", null, "V shared.", ["claude/v-1"]],
    ]);
    expect(await code(claim("org-u", "ws-u", [claimRec({ title: "Late", evidence: [ev("u-1", "Late.")] })]))).toBe("RH016");
    expect((await db.query<{ r: Record<string, number> }>("SELECT sessions.relayfacts_user_erase_v1('req-u','org-u','ws-u','user-u') AS r")).rows[0]!.r)
      .toEqual({ sessions: 1, fencesAdded: 0, evidenceRows: 0, recordsDeleted: 0, recordsRewritten: 0 });
  });

  test("F1: at the 50-row cap a late write from a fenced session is refused before any write; it cannot widen scope or move observed_at", async () => {
    // 50 evidence rows from other sessions, so a merge would add none and never reach the row guard.
    await claim("org-u", "ws-u", [claimRec({ title: "Full", scope: "session", observed_at: "2026-11-18T10:00:00Z", evidence: [ev("seed-0")] })]);
    for (let i = 0; i < 5; i++) await claim("org-u", "ws-u", [claimRec({ title: "Full", evidence: Array.from({ length: 10 }, (_, j) => ev(`seed-${1 + i * 10 + j}`)) })]);
    const full = async () => (await db.query<{ scope: string; observed_at: string; n: number }>(
      "SELECT c.scope, c.observed_at::text observed_at, (SELECT count(*)::int FROM sessions.relayfacts_claim_evidence_v1 e WHERE e.claim_id = c.id) n FROM sessions.relayfacts_claims_v1 c WHERE c.org_id='org-u' AND c.title='Full'")).rows[0]!;
    expect(await full()).toEqual({ scope: "session", observed_at: "2026-11-18 10:00:00+00", n: 50 });
    // u-1 is fenced by the per-user erase (062100), the earlier test's; a probe of each fence table.
    expect((await db.query("SELECT 1 FROM sessions.relayfacts_erasure_fences WHERE org_id='org-u' AND session_id='u-1'")).rows).toHaveLength(1);
    const late = claimRec({ title: "Full", scope: "repo", observed_at: "2000-01-01T00:00:00Z", evidence: [ev("u-1", "Late.")] });
    expect(await code(claim("org-u", "ws-u", [late]))).toBe("RH016");
    // A batch mixing a clean record with the fenced one is refused as a whole.
    expect(await code(claim("org-u", "ws-u", [claimRec({ title: "Clean", evidence: [ev("clean-1")] }), late]))).toBe("RH016");
    expect(await full()).toEqual({ scope: "session", observed_at: "2026-11-18 10:00:00+00", n: 50 });
    expect((await db.query("SELECT 1 FROM sessions.relayfacts_claims_v1 WHERE title='Clean'")).rows).toHaveLength(0);
    // The per-session fence (062090, session_erasure_fences) is checked the same way.
    await db.query("INSERT INTO sessions.relayfacts_claims_v1 (org_id, workspace_id, kind, topic, title, statement) VALUES ('org-a','ws-a','constraint','src-session-read-api','Cap A','s')");
    const capId = (await db.query<{ id: string }>("SELECT id FROM sessions.relayfacts_claims_v1 WHERE title='Cap A'")).rows[0]!.id;
    await db.query("INSERT INTO sessions.relayfacts_claim_evidence_v1 (claim_id, source, session_id, turn_start, turn_end, claim) SELECT $1, 'claude', 'cap-' || g, 0, 1, 'c' FROM generate_series(1, 50) g", [capId]);
    expect((await db.query("SELECT 1 FROM sessions.session_erasure_fences WHERE org_id='org-a' AND workspace_id='ws-a' AND source='claude' AND session_id='s-1'")).rows.length).toBeGreaterThan(0);
    expect(await code(claim("org-a", "ws-a", [claimRec({ title: "Cap A", scope: "repo", observed_at: "2000-01-01T00:00:00Z", evidence: [ev("s-1", "Late.")] })]))).toBe("RH016");
    expect((await db.query<{ scope: string; o: string | null }>("SELECT scope, observed_at::text o FROM sessions.relayfacts_claims_v1 WHERE id=$1", [capId])).rows[0]).toEqual({ scope: "session", o: null });
    // An unfenced session at the cap merges but moves nothing (no evidence row was added).
    expect(await claim("org-a", "ws-a", [claimRec({ title: "Cap A", scope: "repo", observed_at: "2000-01-01T00:00:00Z", evidence: [ev("other-1")] })])).toEqual({ inserted: 0, merged: 1, evidence_added: 0 });
    expect((await db.query<{ scope: string; o: string | null }>("SELECT scope, observed_at::text o FROM sessions.relayfacts_claims_v1 WHERE id=$1", [capId])).rows[0]).toEqual({ scope: "session", o: null });
  });

  test("N2: supersede takes the 062100 shared lock before its row locks", async () => {
    await claim("org-l", "ws-l", [claimRec({ title: "Old L", evidence: [ev("l-9")] }), claimRec({ title: "New L", evidence: [ev("l-9")] })]);
    const id = async (t: string) => (await db.query<{ id: string }>("SELECT id FROM sessions.relayfacts_claims_v1 WHERE title=$1", [t])).rows[0]!.id;
    const [o, n] = [await id("Old L"), await id("New L")];
    const shared = await db.transaction(async (tx) => {
      await tx.exec("SET LOCAL ROLE service_role");
      expect((await tx.query<{ ok: boolean }>("SELECT sessions.relayfacts_claims_supersede_v1('org-l','ws-l',$1,$2) ok", [o, n])).rows[0]!.ok).toBe(true);
      return (await tx.query<{ n: number }>("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND mode='ShareLock'")).rows[0]!.n;
    });
    expect(shared).toBe(1);
    const src = bodyOf(SQL, "relayfacts_claims_supersede_v1", "$relayfacts_claims_supersede$");
    expect(src.indexOf("pg_advisory_xact_lock_shared")).toBeGreaterThan(-1);
    expect(src.indexOf("pg_advisory_xact_lock_shared")).toBeLessThan(src.indexOf("FOR UPDATE"));
    const ins = bodyOf(SQL, "relayfacts_claims_insert_v1", "$relayfacts_claims_insert$");
    expect(ins.indexOf("pg_advisory_xact_lock_shared")).toBeLessThan(ins.indexOf("relayfacts_erasure_fences"));
    expect(ins.indexOf("relayfacts_erasure_fences")).toBeLessThan(ins.indexOf("INSERT INTO sessions.relayfacts_claims_v1"));
  });

  test("062100 lock: claim writes take the shared lock the per-user erase takes exclusively", async () => {
    const shared = await db.transaction(async (tx) => {
      await tx.exec("SET LOCAL ROLE service_role");
      await tx.query("SELECT * FROM sessions.relayfacts_claims_insert_v1('org-l','ws-l',$1::jsonb)", [JSON.stringify([claimRec({ evidence: [ev("l-1")] })])]);
      return (await tx.query<{ n: number }>("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND mode='ShareLock'")).rows[0]!.n;
    });
    expect(shared).toBe(1);
  });
});

describe("20261105062150 rollback", () => {
  test("checksum-pinned; 062140's down refuses under it; it refuses while claims exist unless overridden; then restores the 062090 and 062100 erases exactly and leaves the decisions alone", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    expect(DOWN).toContain("SET LOCAL lock_timeout='5s'");
    expect(DOWN).toMatch(/Intent and constraint rows/);
    expect(DOWN).not.toMatch(/CREATE (UNIQUE )?INDEX|relayfacts_v1 DROP|ALTER TABLE/i);
    const db = await migrated();
    const { decide, claim } = harness(() => db);
    const message = async (sql: string) => { const m = await db.exec(sql).then(() => null, (e: Error) => e.message); await db.exec("ROLLBACK"); return m; };
    try {
      expect((await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).map((s) => s.name)).toEqual(LATER_STEPS);
      // 062150's down refuses while a later step is applied; those come off first.
      expect(await message(DOWN)).toContain("a later migration is applied");
      for (const down of LATER_DOWNS) await db.exec(down);
      expect(await message(read("supabase/rollbacks/20261105062140_staged_output_facts.down.sql"))).toContain("a later migration is applied");
      await decide("org-a", "ws-a", [rec()]);
      await claim("org-a", "ws-a", [claimRec(), claimRec({ kind: "intent", title: "I" })]);
      expect(await message(DOWN)).toContain("intent or constraint claims exist");
      for (const v of ["on", "TRUE", "1"]) {
        await db.exec(`SET relayfacts.rollback_drop_claims = '${v}'`);
        expect(await message(DOWN)).toContain("intent or constraint claims exist");
      }
      await db.exec("RESET relayfacts.rollback_drop_claims");
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_claims_v1")).rows[0]!.n).toBe(2);
      await db.exec("SET relayfacts.rollback_drop_claims = 'true'");
      await db.exec(DOWN);
      await db.exec("RESET relayfacts.rollback_drop_claims");
      expect((await db.query<{ title: string }>("SELECT title FROM sessions.relayfacts_v1")).rows).toEqual([{ title: "Use keyset paging" }]);
      for (const [src, name, tag] of [[ERASE_SQL, "relayfacts_evidence_erase_session_v1", "$relayfacts_evidence_erase$"], [USER_ERASE_SQL, "relayfacts_user_erase_v1", "$relayfacts_user_erase$"]] as const)
        expect((await db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname=$1", [name])).rows.map((r) => r.s)).toEqual([bodyOf(src, name, tag)]);
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_session_ingest', $1, 'EXECUTE') ok", [ERASE])).rows[0]!.ok).toBe(true);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'relayfacts\\_claims\\_%'")).rows[0]!.n).toBe(0);
      for (const t of ["sessions.relayfacts_claims_v1", "sessions.relayfacts_claim_evidence_v1"])
        expect((await db.query<{ r: string | null }>("SELECT to_regclass($1)::text r", [t])).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await decide("org-a", "ws-a", [rec({ evidence: [ev("sess-2")] })])).toEqual({ inserted: 0, merged: 1, evidence_added: 1 });
      expect(await message(DOWN)).toContain("own applied checksum required");
      await db.exec(SQL);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.relayfacts_claims_v1')::text r")).rows[0]!.r).toBe("sessions.relayfacts_claims_v1");
    } finally { await db.close(); }
  });

  test("an empty claims table rolls back without the override", async () => {
    const db = await migrated();
    try {
      for (const down of LATER_DOWNS) await db.exec(down);
      await db.exec(DOWN);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
    } finally { await db.close(); }
  });
});
