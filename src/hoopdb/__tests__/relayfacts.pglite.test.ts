/** 062080 relayfacts: role grants, workspace isolation, bounds, dedupe, supersede, paging and rollback; 062090 operator erase. */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite, type Transaction } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { inventoryOf, RUNTIME_APPLICATION_FUNCTIONS } from "../migrate-guards.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { plan, runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const FILE = "20261105062080_memory_documents.sql";
const STEP = `upstream:${FILE}`;
const SQL = read(`supabase/migrations/${FILE}`);
const DOWN = read("supabase/rollbacks/20261105062080_memory_documents.down.sql");
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };

const INSERT = "sessions.relayfacts_insert_v1(text,text,jsonb)";
const SUPERSEDE = "sessions.relayfacts_supersede_v1(text,text,uuid,uuid)";
const TOPICS = "sessions.relayfacts_topics_v1(text,text)";
const LIST = "sessions.relayfacts_list_v1(text,text,text,timestamp with time zone,uuid,integer)";
const WORKER_FNS = [INSERT, SUPERSEDE];
const READER_FNS = [TOPICS, LIST];
const ALL_FNS = [...WORKER_FNS, ...READER_FNS];

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
  return db;
}

type Ev = { source: string; session_id: string; turn_start: number; turn_end: number; claim: string; ref?: string | null };
type Rec = { topic: string; title: string; statement: string; status?: string; decided_at?: string; evidence: Ev[] };
const ev = (i: number, over: Partial<Ev> = {}): Ev =>
  ({ source: "claude", session_id: `sess-${i}`, turn_start: i, turn_end: i + 1, claim: `claim ${i}`, ...over });
const rec = (over: Partial<Rec> = {}): Rec => ({
  topic: "src-session-read-api", title: "Use keyset paging for the topic page", statement: "Pages are keyed on created_at and id.",
  evidence: [ev(1)], ...over,
});
type Row = { id: string; topic: string; title: string; status: string; supersedes: string | null; created_at: string; evidence: Ev[] };

describe("20261105062080 relayfacts", () => {
  let db: PGlite;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>, scope?: [string, string]) =>
    db.transaction(async (tx) => {
      await tx.exec(`SET LOCAL ROLE ${role}`);
      if (scope) await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
      return run(tx);
    });
  const insert = (org: string, ws: string, list: unknown) =>
    as("service_role", async (tx) => (await tx.query<{ inserted: number; merged: number; evidence_added: number }>(
      "SELECT * FROM sessions.relayfacts_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)])).rows[0]!);
  const supersede = (org: string, ws: string, oldId: string, newId: string) =>
    as("service_role", async (tx) => (await tx.query<{ ok: boolean }>(
      "SELECT sessions.relayfacts_supersede_v1($1,$2,$3::uuid,$4::uuid) AS ok", [org, ws, oldId, newId])).rows[0]!.ok);
  const topics = (org: string, ws: string, scope: [string, string] = [org, ws]) =>
    as("bulls96_read_api", async (tx) => (await tx.query<Record<string, unknown>>(
      "SELECT * FROM sessions.relayfacts_topics_v1($1,$2)", [org, ws])).rows, scope);
  const list = (org: string, ws: string, topic: string, opts: { after?: [string, string]; limit?: number; scope?: [string, string] } = {}) =>
    as("bulls96_read_api", async (tx) => (await tx.query<Row>(
      "SELECT * FROM sessions.relayfacts_list_v1($1,$2,$3,$4::timestamptz,$5::uuid,$6)",
      [org, ws, topic, opts.after?.[0] ?? null, opts.after?.[1] ?? null, opts.limit ?? 200])).rows, opts.scope ?? [org, ws]);
  const reset = () => db.exec("DELETE FROM sessions.relayfacts_v1");

  beforeAll(async () => {
    db = await migrated();
    await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and runtime allowlist name exactly the two tables, their indexes and the four functions", async () => {
    expect(inventoryOf(SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.relayfacts_insert_v1", "function:sessions.relayfacts_list_v1",
      "function:sessions.relayfacts_supersede_v1", "function:sessions.relayfacts_topics_v1",
      "index:relayfacts_evidence_v1_dedupe", "index:relayfacts_v1_dedupe", "index:relayfacts_v1_topic",
      "index:relayfacts_v1_updated", "relation:sessions.relayfacts_evidence_v1", "relation:sessions.relayfacts_v1",
    ]);
    expect(SQL).not.toMatch(/CONCURRENTLY/i);
    expect(MIGRATION_SENTINELS[STEP]?.evidence).toContain("relayfacts_v1");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.relayfacts_")).sort()).toEqual([INSERT, SUPERSEDE].sort());
    // Columns: no transcript body anywhere.
    const cols = async (t: string) => (await db.query<{ column_name: string }>(
      "SELECT column_name FROM information_schema.columns WHERE table_schema='sessions' AND table_name=$1 ORDER BY ordinal_position", [t])).rows.map((c) => c.column_name);
    expect(await cols("relayfacts_v1")).toEqual(["id", "org_id", "workspace_id", "topic", "title", "title_key", "statement", "status", "supersedes", "decided_at", "implemented_at", "implemented_ref", "created_at", "updated_at"]);
    expect(await cols("relayfacts_evidence_v1")).toEqual(["id", "decision_id", "source", "session_id", "turn_start", "turn_end", "claim", "ref", "created_at"]);
  });

  test("grants: PUBLIC cannot execute; each role runs only its own functions; no runtime role has a table privilege", async () => {
    for (const fn of ALL_FNS) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r.secdef).toBe(true);
      expect(r.cfg).toEqual(["search_path=pg_catalog, sessions, pg_temp"]);
      expect(r.public_exec).toBe(false);
    }
    await db.exec("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='memdocs_probe') THEN CREATE ROLE memdocs_probe NOLOGIN; END IF; END $$");
    const can = async (role: string, fn: string) =>
      (await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok;
    for (const fn of ALL_FNS) {
      expect(await can("memdocs_probe", fn)).toBe(false);
      for (const r of ["anon", "authenticated", "search_consumer_runtime", "bulls96_session_ingest"]) expect(await can(r, fn)).toBe(false);
      expect(await can("service_role", fn)).toBe(WORKER_FNS.includes(fn));
      expect(await can("bulls96_read_api", fn)).toBe(READER_FNS.includes(fn));
    }
    for (const table of ["sessions.relayfacts_v1", "sessions.relayfacts_evidence_v1"])
      for (const role of ["service_role", "bulls96_read_api", "memdocs_probe"])
        for (const priv of ["SELECT", "INSERT", "UPDATE", "DELETE"])
          expect((await db.query<{ ok: boolean }>("SELECT has_table_privilege($1, $2, $3) ok", [role, table, priv])).rows[0]!.ok).toBe(false);
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.relayfacts_insert_v1('org-a','ws-a','[]'::jsonb)"), ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT * FROM sessions.relayfacts_topics_v1('org-a','ws-a')")))).toBe("42501");
    expect(await code(as("service_role", (tx) => tx.query("SELECT 1 FROM sessions.relayfacts_v1")))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT 1 FROM sessions.relayfacts_evidence_v1"), ["org-a", "ws-a"]))).toBe("42501");
    // The read-API role verifier still passes with the two new grants.
    expect(await code(db.exec(sqlFile("supabase/bootstrap/session_read_api_role_verify.sql")))).toBeNull();
  });

  test("workspace isolation: a workspace A record is never returned for workspace B, and no reader steps outside its scope", async () => {
    await reset();
    expect(await insert("org-a", "ws-a", [rec()])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    // The same topic and title in workspace B is a separate record.
    expect(await insert("org-a", "ws-b", [rec({ statement: "workspace b statement", evidence: [ev(9)] })])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    const a = await list("org-a", "ws-a", "src-session-read-api");
    const b = await list("org-a", "ws-b", "src-session-read-api");
    expect(a.map((r) => r.evidence.map((e) => e.session_id))).toEqual([["sess-1"]]);
    expect(b.map((r) => r.evidence.map((e) => e.session_id))).toEqual([["sess-9"]]);
    expect(a[0]!.id).not.toBe(b[0]!.id);
    expect(await list("org-a", "ws-c", "src-session-read-api")).toEqual([]);
    expect((await topics("org-a", "ws-c"))).toEqual([]);
    // A reader scoped to workspace A cannot read B, nor A under another org.
    expect(await code(list("org-a", "ws-b", "src-session-read-api", { scope: ["org-a", "ws-a"] }))).toBe("42501");
    expect(await code(topics("org-a", "ws-b", ["org-a", "ws-a"]))).toBe("42501");
    expect(await code(list("org-b", "ws-a", "src-session-read-api", { scope: ["org-a", "ws-a"] }))).toBe("42501");
    expect(await code(as("bulls96_read_api", (tx) => tx.query("SELECT * FROM sessions.relayfacts_topics_v1('org-a','ws-a')")))).toBe("42501");
    // The worker cannot supersede across workspaces.
    expect(await supersede("org-a", "ws-a", a[0]!.id, b[0]!.id)).toBe(false);
    expect(await supersede("org-a", "ws-b", a[0]!.id, b[0]!.id)).toBe(false);
    expect((await list("org-a", "ws-a", "src-session-read-api"))[0]!.status).toBe("decided");
  });

  test("dedupe: the same topic and normalized title merges, adding only new evidence; at most 50 evidence rows", async () => {
    await reset();
    expect(await insert("org-a", "ws-a", [rec()])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    const before = (await list("org-a", "ws-a", "src-session-read-api"))[0]!;
    // Case, inner whitespace and trailing punctuation do not make a new record; a known evidence row is not repeated.
    expect(await insert("org-a", "ws-a", [rec({ title: "  use KEYSET   paging for the topic page. ", statement: "other", evidence: [ev(1), ev(2)] })]))
      .toEqual({ inserted: 0, merged: 1, evidence_added: 1 });
    const merged = await list("org-a", "ws-a", "src-session-read-api");
    expect(merged.length).toBe(1);
    expect(merged[0]!.title).toBe(before.title);
    expect(merged[0]!.evidence.map((e) => e.session_id)).toEqual(["sess-1", "sess-2"]);
    // Rerunning the same batch adds nothing.
    expect(await insert("org-a", "ws-a", [rec({ evidence: [ev(1), ev(2)] })])).toEqual({ inserted: 0, merged: 1, evidence_added: 0 });
    // Another topic with the same title is a separate record.
    expect(await insert("org-a", "ws-a", [rec({ topic: "general" })])).toEqual({ inserted: 1, merged: 0, evidence_added: 1 });
    // Evidence is capped at 50 per record.
    for (let i = 0; i < 6; i++)
      await insert("org-a", "ws-a", [rec({ evidence: Array.from({ length: 10 }, (_, j) => ev(100 + i * 10 + j)) })]);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_evidence_v1 e JOIN sessions.relayfacts_v1 r ON r.id=e.decision_id WHERE r.topic='src-session-read-api'")).rows[0]!.n).toBe(50);
  });

  test("bounds: topic, title, statement, claim, turn range, ref, status, batch and evidence sizes", async () => {
    await reset();
    const bad = async (over: Partial<Rec>) => code(insert("org-a", "ws-a", [rec(over)]));
    expect(await bad({ topic: "Bad Topic" })).toBe("23514");
    expect(await bad({ topic: "a".repeat(65) })).toBe("23514");
    expect(await bad({ topic: "-leading" })).toBe("23514");
    expect(await bad({ title: "t".repeat(201) })).toBe("23514");
    expect(await bad({ title: "two\nlines" })).toBe("23514");
    expect(await bad({ title: " " })).toBe("23514");
    expect(await bad({ statement: "s".repeat(1001) })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { claim: "c".repeat(301) })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { claim: "line\nbreak" })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { turn_start: 5, turn_end: 4 })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { turn_start: -1 })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { source: "Not A Source" })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { ref: "https://example.com/x" })] })).toBe("23514");
    expect(await bad({ evidence: [ev(1, { ref: "https://github.com/o/r/pull/1\nx" })] })).toBe("23514");
    expect(await bad({ status: "superseded" })).toBe("22023");
    expect(await bad({ evidence: [] })).toBe("22023");
    expect(await bad({ evidence: Array.from({ length: 11 }, (_, i) => ev(i)) })).toBe("22023");
    expect(await code(insert("", "ws-a", [rec()]))).toBe("22023");
    expect(await code(insert("org-a", "ws-a", { not: "an array" }))).toBe("22023");
    expect(await code(insert("org-a", "ws-a", Array.from({ length: 101 }, (_, i) => rec({ title: `t${i}` }))))).toBe("22023");
    expect(await code(insert("org-a", "ws-a", [{ ...rec(), topic: null }]))).toBe("23502");
    // A failed batch writes nothing.
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_v1")).rows[0]!.n).toBe(0);
    // Edge values pass.
    expect(await insert("org-a", "ws-a", [rec({
      topic: "a".repeat(64), title: "t".repeat(200), statement: "s".repeat(1000),
      evidence: [ev(1, { claim: "c".repeat(300), ref: "https://github.com/AgentWorkforce/relayhistory-pipeline/pull/613" }),
        ev(2, { ref: "https://github.com/o/r/commit/0123abc" }), ev(3, { ref: "https://github.com/o/r/issues/7" })],
    })])).toEqual({ inserted: 1, merged: 0, evidence_added: 3 });
    // The list limit and cursor are validated.
    for (const limit of [0, 201]) expect(await code(list("org-a", "ws-a", "general", { limit }))).toBe("22023");
    expect(await code(as("bulls96_read_api", (tx) => tx.query(
      "SELECT * FROM sessions.relayfacts_list_v1('org-a','ws-a','general',now(),NULL,10)"), ["org-a", "ws-a"]))).toBe("22023");
  });

  test("supersede, topic counts and oldest-first keyset paging", async () => {
    await reset();
    await insert("org-a", "ws-a", Array.from({ length: 5 }, (_, i) => rec({ title: `decision ${i}`, evidence: [ev(i)] })));
    await insert("org-a", "ws-a", [rec({ topic: "general", title: "open question", status: "open" })]);
    // Distinct created_at so the order is the insertion order.
    await db.exec("UPDATE sessions.relayfacts_v1 SET created_at = now() - make_interval(mins => 100 - substring(title from '[0-9]+$')::int) WHERE title LIKE 'decision %'");
    const all = await list("org-a", "ws-a", "src-session-read-api");
    expect(all.map((r) => r.title)).toEqual(["decision 0", "decision 1", "decision 2", "decision 3", "decision 4"]);
    const page1 = await list("org-a", "ws-a", "src-session-read-api", { limit: 2 });
    const last = page1.at(-1)!;
    const page2 = await list("org-a", "ws-a", "src-session-read-api", { limit: 2, after: [last.created_at, last.id] });
    const page3 = await list("org-a", "ws-a", "src-session-read-api", { limit: 2, after: [page2.at(-1)!.created_at, page2.at(-1)!.id] });
    expect([...page1, ...page2, ...page3].map((r) => r.title)).toEqual(all.map((r) => r.title));
    expect(page3.length).toBe(1);
    // Supersede: 0 by 4. A second supersede of 0, a superseded new record and self-supersede all refuse.
    expect(await supersede("org-a", "ws-a", all[0]!.id, all[4]!.id)).toBe(true);
    expect(await supersede("org-a", "ws-a", all[0]!.id, all[3]!.id)).toBe(false);
    expect(await supersede("org-a", "ws-a", all[1]!.id, all[0]!.id)).toBe(false);
    expect(await supersede("org-a", "ws-a", all[1]!.id, all[1]!.id)).toBe(false);
    // 4 already supersedes 0: it cannot also take over 1, so the first link is kept.
    expect(await supersede("org-a", "ws-a", all[1]!.id, all[4]!.id)).toBe(false);
    const after = await list("org-a", "ws-a", "src-session-read-api");
    expect(after[0]!.status).toBe("superseded");
    expect(after[4]!.supersedes).toBe(all[0]!.id);
    expect((await topics("org-a", "ws-a")).map((t) => [t.topic, t.records, t.open, t.decided, t.superseded])).toEqual([
      ["general", 1, 1, 0, 0], ["src-session-read-api", 5, 0, 4, 1],
    ]);
  });

  test("the extractor's records, at their bounds, pass the insert function through the worker's sink", async () => {
    await reset();
    const { extractDecisionRecords, postgresDecisionRecordsSink, recordDecisions } = await import("../../session-analysis/decision-records.ts");
    const long = `${"Keep the keyset cursor at microsecond precision ".repeat(10)}per https://github.com/o/r/pull/12 and ${"never round it ".repeat(60)}`;
    const snapshot = {
      evidence: { references: [{ id: "e0", source: "claude", sessionId: "sess-1" }, { id: "e1", source: "claude", sessionId: "sess-1" }, { id: "e2", source: "codex", sessionId: "sess-2" }] },
      metrics: { filesChanged: [{ path: "/home/someone/work/src/session-read-api/app.ts", editCount: 3 }] },
      understanding: { decisions: [
        { text: long, evidenceIds: ["e0", "e1", "e2"], confidence: "high" },
        { text: "Use one registration call.\nIt keeps app.ts small.", evidenceIds: ["e1"], confidence: "medium" },
      ] },
    } as never;
    const records = extractDecisionRecords(snapshot);
    expect(records.map((r) => [r.topic, r.title.length, r.statement.length, r.evidence[0]!.claim.length])).toEqual([
      ["src-session-read-api", 200, 1000, 300], ["src-session-read-api", 26, 49, 49],
    ]);
    const sink = postgresDecisionRecordsSink({
      query: <R,>(text: string, params?: unknown[]) => as("service_role", async (tx) => (await tx.query<R>(text, params)) as { rows: R[] }),
    } as never);
    expect(await sink({ orgId: "org-a", workspaceId: "ws-a" }, records)).toEqual({ inserted: 2, merged: 0, evidence_added: 3 });
    // The same analysis again merges and adds nothing.
    expect(await recordDecisions(sink, { orgId: "org-a", workspaceId: "ws-a" }, snapshot)).toBe(2);
    // Both rows share created_at (one transaction), so the uuid tiebreak orders them randomly: sort first.
    const rows = (await list("org-a", "ws-a", "src-session-read-api")).sort((a, b) => b.title.length - a.title.length);
    expect(rows.map((r) => [r.title.length, r.evidence.map((e) => [e.source, e.session_id, e.turn_start, e.turn_end, e.ref])])).toEqual([
      [200, [["claude", "sess-1", 0, 1, "https://github.com/o/r/pull/12"], ["codex", "sess-2", 0, 0, "https://github.com/o/r/pull/12"]]],
      [26, [["claude", "sess-1", 1, 1, null]]],
    ]);
    expect(await list("org-a", "ws-b", "src-session-read-api")).toEqual([]);
  });
});

describe("20261105062080 rollback", () => {
  test("the checksum-pinned down after the up leaves no table, no function and no ledger row; a rerun refuses", async () => {
    expect(DOWN).toContain(createHash("sha256").update(SQL).digest("hex"));
    const db = await migrated();
    try {
      const later = (await plan(LOCAL)).filter((s) => s.name.startsWith("upstream:") && s.name > STEP).reverse();
      if (later.length) {
        expect(await code(db.exec(DOWN))).toBe("55000");
        await db.exec("ROLLBACK");
      }
      for (const step of later)
        await db.exec(read(`supabase/rollbacks/${step.name.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`));
      await db.exec(DOWN);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.relayfacts_v1')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.relayfacts_evidence_v1')::text r")).rows[0]!.r).toBeNull();
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname LIKE 'relayfacts\\_%'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM pg_class WHERE relnamespace='sessions'::regnamespace AND relname LIKE 'relayfacts\\_%'")).rows[0]!.n).toBe(0);
      expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [STEP])).rows[0]!.n).toBe(0);
      expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[STEP]!.sql)).rows[0]!.applied).toBe(false);
      expect(await code(db.exec(DOWN))).toBe("55000");
      await db.exec("ROLLBACK");
      // The up applies again cleanly after the down.
      await db.exec(SQL);
      expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.relayfacts_v1')::text r")).rows[0]!.r).toBe("sessions.relayfacts_v1");
    } finally { await db.close(); }
  });
});

const ERASE_FILE = "20261105062090_relayfacts_erase.sql";
const ERASE_STEP = `upstream:${ERASE_FILE}`;
const ERASE_SQL = read(`supabase/migrations/${ERASE_FILE}`);
const ERASE_DOWN = read("supabase/rollbacks/20261105062090_relayfacts_erase.down.sql");
const ERASE = "sessions.relayfacts_evidence_erase_session_v1(text,text,text,text,uuid)";
const GUARD = "sessions.relayfacts_evidence_fence_guard_v1()";

describe("20261105062090 relayfacts operator erase", () => {
  let db: PGlite;
  const KEY = ["org-a", "ws-a", "claude", "s-1"] as const;
  const as = async <R,>(role: string, run: (tx: Transaction) => Promise<R>) =>
    db.transaction(async (tx) => { await tx.exec(`SET LOCAL ROLE ${role}`); return run(tx); });
  const ingest = async <R,>(fn: string, params: unknown[]) => as("bulls96_session_ingest", async (tx) =>
    (await tx.query<{ r: R }>(`SELECT sessions.${fn}(${params.map((_, i) => `$${i + 1}`).join(",")}) AS r`, params)).rows[0]!.r);
  const insert = (org: string, ws: string, list: unknown) =>
    as("service_role", async (tx) => (await tx.query<{ inserted: number; merged: number; evidence_added: number }>(
      "SELECT * FROM sessions.relayfacts_insert_v1($1,$2,$3::jsonb)", [org, ws, JSON.stringify(list)])).rows[0]!);
  const records = async () => (await db.query<{ ws: string; title: string; statement: string; evidence: string[] }>(
    `SELECT t.workspace_id ws, t.title, t.statement,
            coalesce((SELECT array_agg(e.source || '/' || e.session_id ORDER BY e.source, e.session_id) FROM sessions.relayfacts_evidence_v1 e WHERE e.decision_id = t.id), '{}') evidence
       FROM sessions.relayfacts_v1 t ORDER BY t.workspace_id, t.title`)).rows;
  const e = (source: string, session_id: string, claim: string): Ev => ({ source, session_id, turn_start: 0, turn_end: 1, claim });

  beforeAll(async () => {
    db = await migrated();
    // The pointer the operator erase begins from (its manifest layout names the bucket, workspace and source).
    await db.query(`INSERT INTO sessions.session_index(
      org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id)
      VALUES($1,$2,$3,$4,'machine','user',now(),now(),'s3','bucket','96bulls/org-a/v1/ws=ws-a/m=machine/h=claude/d=2026/11/11/s=s-1/manifest.json',decode(repeat('ab',32),'hex'),'jsonl',
      '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18')`, [...KEY]);
  });
  afterAll(async () => { await db?.close(); });

  test("inventory, sentinel and grants: two owner-pinned functions and one trigger, erase for the ingest login only, not a runtime application function", async () => {
    expect(inventoryOf(ERASE_SQL).map((i) => `${i.kind}:${i.name}`).sort()).toEqual([
      "function:sessions.relayfacts_evidence_erase_session_v1", "function:sessions.relayfacts_evidence_fence_guard_v1",
      "trigger:relayfacts_evidence_fence_guard",
    ]);
    expect(ERASE_SQL).not.toMatch(/CONCURRENTLY/i);
    expect(ERASE_DOWN).toContain(createHash("sha256").update(ERASE_SQL).digest("hex"));
    expect(ERASE_DOWN).toContain("SET LOCAL lock_timeout='5s'");
    expect((await db.query<{ applied: boolean }>(MIGRATION_SENTINELS[ERASE_STEP]!.sql)).rows[0]!.applied).toBe(true);
    expect(Object.keys(MIGRATION_SENTINELS).filter((k) => k.startsWith("upstream:")).sort().at(-1)).toBe(ERASE_STEP);
    expect(RUNTIME_APPLICATION_FUNCTIONS.filter((f) => f.startsWith("sessions.relayfacts_evidence_"))).toEqual([]);
    for (const fn of [ERASE, GUARD]) {
      const r = (await db.query<{ secdef: boolean; cfg: string[]; public_exec: boolean }>(
        "SELECT p.prosecdef secdef, p.proconfig cfg, EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0) public_exec FROM pg_proc p WHERE p.oid=$1::regprocedure", [fn])).rows[0]!;
      expect(r.secdef).toBe(true);
      expect(r.cfg).toEqual(["search_path=pg_catalog, sessions, pg_temp"]);
      expect(r.public_exec).toBe(false);
      for (const role of ["anon", "authenticated", "service_role", "search_consumer_runtime", "bulls96_read_api"])
        expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') ok", [role, fn])).rows[0]!.ok).toBe(false);
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_session_ingest', $1, 'EXECUTE') ok", [fn])).rows[0]!.ok).toBe(fn === ERASE);
    }
  });

  test("an erase removes only the erased session's evidence in its workspace and source; empty records go, shared records lose its text", async () => {
    // Only the erased session: deleted with its evidence.
    await insert("org-a", "ws-a", [rec({ title: "Only the erased session", statement: "Erased statement one.", evidence: [e("claude", "s-1", "Erased claim one.")] })]);
    // First written from the erased session, then merged from s-2: kept, statement replaced by s-2's claim.
    await insert("org-a", "ws-a", [rec({ title: "Shared decision", statement: "Erased statement two.", evidence: [e("claude", "s-1", "Erased claim two.")] })]);
    await insert("org-a", "ws-a", [rec({ title: "shared decision.", statement: "Kept statement.", evidence: [e("claude", "s-2", "Kept claim from s-2.")] })]);
    // Same session id under another history source, and in another workspace: untouched.
    await insert("org-a", "ws-a", [rec({ title: "Other source", statement: "Codex statement.", evidence: [e("codex", "s-1", "Codex claim.")] })]);
    await insert("org-a", "ws-b", [rec({ title: "Other workspace", statement: "Workspace B statement.", evidence: [e("claude", "s-1", "Workspace B claim.")] })]);
    await insert("org-b", "ws-a", [rec({ title: "Other org", statement: "Org B statement.", evidence: [e("claude", "s-1", "Org B claim.")] })]);

    // The erase function is not callable without the ingest login, nor without an authorized lease.
    expect(await code(as("service_role", (tx) => tx.query(`SELECT ${ERASE.replace(/\(.*$/, "")}('org-a','ws-a','claude','s-1',gen_random_uuid())`)))).toBe("42501");
    expect(await code(ingest("relayfacts_evidence_erase_session_v1", [...KEY, crypto.randomUUID()]))).toBe("RH014");
    expect(await ingest("ingest_operator_runtime_begin", [...KEY, crypto.randomUUID(), "user", "operator", "bucket", "96bulls", "a".repeat(64)]))
      .toMatchObject({ status: "in_progress", uploadsFenced: true });
    const lease = crypto.randomUUID();
    expect(await ingest("ingest_operator_runtime_next", [lease])).toMatchObject({ sessionIds: ["s-1"] });
    expect(await code(ingest("relayfacts_evidence_erase_session_v1", [...KEY, crypto.randomUUID()]))).toBe("RH014");

    expect(await ingest("relayfacts_evidence_erase_session_v1", [...KEY, lease])).toEqual({ evidenceRows: 2, recordsDeleted: 1, recordsRewritten: 1 });
    expect(await records()).toEqual([
      { ws: "ws-a", title: "Other org", statement: "Org B statement.", evidence: ["claude/s-1"] },
      { ws: "ws-a", title: "Other source", statement: "Codex statement.", evidence: ["codex/s-1"] },
      { ws: "ws-a", title: "Shared decision", statement: "Kept claim from s-2.", evidence: ["claude/s-2"] },
      { ws: "ws-b", title: "Other workspace", statement: "Workspace B statement.", evidence: ["claude/s-1"] },
    ]);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.relayfacts_v1 WHERE statement LIKE 'Erased%'")).rows[0]!.n).toBe(0);
    // Idempotent: a retry within the lease finds nothing more.
    expect(await ingest("relayfacts_evidence_erase_session_v1", [...KEY, lease])).toEqual({ evidenceRows: 0, recordsDeleted: 0, recordsRewritten: 0 });

    // A late analysis write for the fenced session is refused as a whole batch: no record without evidence.
    const before = await records();
    expect(await code(insert("org-a", "ws-a", [
      rec({ title: "Late fresh record", statement: "Late statement.", evidence: [e("claude", "s-1", "Late claim.")] }),
    ]))).toBe("RH016");
    expect(await code(insert("org-a", "ws-a", [
      rec({ title: "Shared decision", statement: "Late merge.", evidence: [e("claude", "s-1", "Late merge claim.")] }),
    ]))).toBe("RH016");
    expect(await records()).toEqual(before);
    // The fence is per workspace and source: other sessions, sources and workspaces still write.
    expect(await insert("org-a", "ws-a", [rec({ title: "Shared decision", evidence: [e("claude", "s-3", "Another session.")] })]))
      .toEqual({ inserted: 0, merged: 1, evidence_added: 1 });
    expect(await insert("org-a", "ws-b", [rec({ title: "Other workspace", evidence: [e("claude", "s-1", "Workspace B again.")], })]))
      .toMatchObject({ merged: 1 });

    // The metadata purge still runs in the same lease afterwards.
    await ingest("ingest_operator_runtime_purge", [...KEY, lease]);
    expect((await db.query("SELECT 1 FROM sessions.session_index WHERE org_id='org-a' AND workspace_id='ws-a'")).rows).toEqual([]);
  });
});
