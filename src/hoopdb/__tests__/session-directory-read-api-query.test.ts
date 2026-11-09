/**
 * Migration 20261105046010 (session list/activity `q` for the self-host Dashboard, #473) on
 * PGlite with every migration: sessions.query_session_directory_for and
 * sessions.query_session_directory_activity_for run AS bulls96_read_api with the tenant bound
 * the way the read Worker binds it. Covers the tenant refusal (42501), every term category
 * with AND/OR/exclusion, LIKE metacharacters taken literally, empty search_text and deleted
 * rows, filtering before LIMIT over a keyset walk, the activity counts, the 22023
 * validations, the ACL (bulls96_read_api only) with the role verifier's allow-list,
 * up/down/up, and the trigram index for a free word. Every id is synthetic.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { TENANT_SETTINGS_SQL } from "../../session-read-api/liveness.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105046010_session_directory_read_api_query.sql";
const DOWN = "supabase/rollbacks/20261105046010_session_directory_read_api_query.down.sql";
const VERIFY = sqlFile("supabase/bootstrap/session_read_api_role_verify.sql");
const T = "timestamp with time zone";
const FILTERS = `${Array(12).fill("text[]").join(",")},boolean,text[],text[],text,text[],text,${T},${T},${T},${T}`;
const QUERY_SIG = `sessions.query_session_directory_for(text,${FILTERS},${T},text,text,integer)`;
const ACTIVITY_SIG = `sessions.query_session_directory_activity_for(text,${T},${T},${FILTERS})`;
const HELPERS = [
  `sessions.session_directory_query_check(${Array(12).fill("text[]").join(",")},text[],text[],text,text[],text,${T},${T},${T},${T})`,
  "sessions.session_directory_query_like(text[])",
  `sessions.session_directory_query_where(${Array(12).fill("integer").join(",")},${Array(10).fill("boolean").join(",")})`,
];

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  const files = readdirSync(join(root, "supabase/migrations")).sort();
  expect(files).toContain(MIGRATION.slice("supabase/migrations/".length));
  for (const file of files) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
  return db;
}

type Row = Record<string, unknown>;
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code;

type Session = {
  org?: string; workspace?: string; harness?: string; id: string; created?: string; last: string;
  title?: string; taskTitle?: string; agents?: string[]; project?: string; state?: string;
  summary?: string; searchText?: string; deleted?: boolean; users?: string[];
};
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions) {
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, task_title, agent_names,
          project_id, state, user_ids, search_text, deleted_at, catalog_projected_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::text, $8::text, $9::text[], $10::text, $11, $12::text[],
               coalesce($13::text, sessions.session_directory_search_text($7::text, $8::text, ($9::text[])[1], $10::text, NULL, NULL, NULL, $14::text)),
               CASE WHEN $15::boolean THEN now() END, now())`,
      [s.org ?? "org-a", s.workspace ?? "ws-a", s.harness ?? "codex", s.id, s.created ?? s.last, s.last, s.title ?? null,
       s.taskTitle ?? null, s.agents ?? [], s.project ?? null, s.state ?? "unknown", s.users ?? [], s.searchText ?? null,
       s.summary ?? null, s.deleted ?? false],
    );
    for (const user of s.users ?? [])
      await db.query("INSERT INTO sessions.session_directory_users VALUES ($1, $2, $3, $4, $5, $6)",
        [s.org ?? "org-a", s.workspace ?? "ws-a", s.harness ?? "codex", s.id, user, s.last]);
  }
}

const FIXTURE: Session[] = [
  { id: "q1", last: "2026-10-05T12:00:00Z", created: "2026-10-04T08:00:00Z", title: "Fix login bug", agents: ["Lead"], project: "github.com/acme/api", state: "active", users: ["u1"] },
  { id: "q2", harness: "claude", last: "2026-10-04T12:00:00Z", title: "Write docs", taskTitle: "Login page copy", agents: ["writer"], project: "github.com/acme/web", state: "done" },
  { id: "q3", last: "2026-10-03T12:00:00Z", title: "100% coverage_push a\\b", state: "abandoned", summary: "Raised branch coverage" },
  { id: "q4", harness: "gemini", last: "2026-10-02T12:00:00Z", title: "Refactor login search", searchText: "" },
  { id: "gone", last: "2026-10-06T00:00:00Z", title: "login gone", deleted: true },
  { id: "other-ws", workspace: "ws-b", last: "2026-10-06T00:00:00Z", title: "login elsewhere" },
  { id: "other-org", org: "org-b", last: "2026-10-06T00:00:00Z", title: "login elsewhere" },
];

/** As the read Worker: BEGIN READ ONLY, bind the tenant, run, COMMIT; as `role`. */
async function asReadApi<R>(db: PGlite, tenant: [string, string] | null, work: () => Promise<R>, role = "bulls96_read_api"): Promise<R> {
  await db.exec(`SET ROLE ${role}`);
  try {
    await db.exec("BEGIN READ ONLY");
    try {
      if (tenant) await db.query(TENANT_SETTINGS_SQL, tenant);
      const out = await work();
      await db.exec("COMMIT");
      return out;
    } catch (error) {
      await db.exec("ROLLBACK");
      throw error;
    }
  } finally {
    await db.exec("RESET ROLE");
  }
}

type Q = {
  workspace?: string; free?: string[]; not_free?: string[]; title?: string[]; not_title?: string[];
  project?: string[]; not_project?: string[]; harness?: string[]; not_harness?: string[];
  agent_terms?: string[]; not_agent_terms?: string[]; id?: string[]; not_id?: string[]; active?: boolean;
  sources?: string[]; not_sources?: string[]; project_contains?: string; user_ids?: string[]; agent?: string;
  last_ts_after?: string; last_ts_before?: string; first_ts_after?: string; first_ts_before?: string;
  cursor_last_activity_at?: string; cursor_session_id?: string; cursor_harness?: string; limit?: number;
};
function namedArgs(q: Q): [string, unknown[]] {
  const entries = Object.entries({ ...q, workspace: undefined }).filter(([, v]) => v !== undefined);
  const values: unknown[] = [q.workspace ?? "ws-a"];
  const parts = ["p_workspace_id => $1"];
  for (const [key, value] of entries) {
    values.push(value);
    parts.push(`p_${key} => $${values.length}`);
  }
  return [parts.join(", "), values];
}
const query = async (db: PGlite, q: Q) => {
  const [args, values] = namedArgs(q);
  return (await db.query<Row>(`SELECT * FROM sessions.query_session_directory_for(${args})`, values)).rows;
};
const ids = async (db: PGlite, q: Q) =>
  (await asReadApi(db, ["org-a", "ws-a"], () => query(db, q))).map((r) => r.session_id);
const activity = async (db: PGlite, since: string, until: string, q: Q) => {
  const [args, values] = namedArgs(q);
  return (await db.query<Row>(`SELECT * FROM sessions.query_session_directory_activity_for(${args.replace("p_workspace_id => $1", "p_workspace_id => $1, p_since => $" + (values.length + 1) + ", p_until => $" + (values.length + 2))})`, [...values, since, until])).rows;
};

describe("query_session_directory_for / query_session_directory_activity_for", () => {
  test("refuse 42501: other roles, the table owner, no binding, another workspace", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    for (const role of ["service_role", "authenticated", "anon", "search_consumer_runtime"]) {
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, { free: ["login"] }), role)), role).toBe("42501");
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activity(db, "2026-10-01T00:00:00Z", "2026-10-06T00:00:00Z", {}), role)), role).toBe("42501");
    }
    // The owner holds EXECUTE; the scope helper's role guard refuses it.
    await db.query(TENANT_SETTINGS_SQL, ["org-a", "ws-a"]);
    expect(await code(query(db, {}))).toBe("42501");
    await db.exec("SELECT set_config('bulls96_read.org_id', '', false), set_config('bulls96_read.workspace_id', '', false)");
    expect(await code(asReadApi(db, null, () => query(db, {})))).toBe("42501");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, { workspace: "ws-b" })))).toBe("42501");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activity(db, "2026-10-01T00:00:00Z", "2026-10-06T00:00:00Z", { workspace: "ws-b" })))).toBe("42501");
    // The read role cannot call the owner-only helpers.
    for (const helper of HELPERS)
      expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege('bulls96_read_api', $1, 'EXECUTE') AS ok", [helper])).rows[0]!.ok, helper).toBe(false);
    // The bound tenant alone decides the rows.
    expect((await asReadApi(db, ["org-a", "ws-b"], () => query(db, { workspace: "ws-b", free: ["login"] }))).map((r) => r.session_id)).toEqual(["other-ws"]);
    expect((await asReadApi(db, ["org-b", "ws-a"], () => query(db, { free: ["login"] }))).map((r) => r.session_id)).toEqual(["other-org"]);
  }, 120_000);

  test("every term category, AND / OR / exclusion, NULL as not matching", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    // No terms: the plain list (deleted and other tenants never appear).
    expect(await ids(db, {})).toEqual(["q1", "q2", "q3", "q4"]);
    // Free words: search_text ILIKE; q4 has an empty search_text, 'gone' is deleted.
    expect(await ids(db, { free: ["login"] })).toEqual(["q1", "q2"]);
    expect(await ids(db, { free: ["LOGIN"] })).toEqual(["q1", "q2"]);
    expect(await ids(db, { free: ["login", "docs"] })).toEqual(["q2"]);
    expect(await ids(db, { free: ["coverage"] })).toEqual(["q3"]);
    expect(await ids(db, { free: ["raised branch"] })).toEqual(["q3"]);
    expect(await ids(db, { free: ["acme/api"] })).toEqual(["q1"]);
    expect(await ids(db, { not_free: ["login"] })).toEqual(["q3", "q4"]);
    expect(await ids(db, { free: ["login"], not_free: ["docs"] })).toEqual(["q1"]);
    // title: title or task title.
    expect(await ids(db, { title: ["login"] })).toEqual(["q1", "q2", "q4"]);
    expect(await ids(db, { not_title: ["login"] })).toEqual(["q3"]);
    // project: OR within the field; excluded rows without a project match the exclusion's NOT.
    expect(await ids(db, { project: ["acme/api"] })).toEqual(["q1"]);
    expect(await ids(db, { project: ["acme/api", "acme/web"] })).toEqual(["q1", "q2"]);
    expect(await ids(db, { not_project: ["acme/api"] })).toEqual(["q2", "q3", "q4"]);
    // harness: OR within the field.
    expect(await ids(db, { harness: ["claude", "gemini"] })).toEqual(["q2", "q4"]);
    expect(await ids(db, { not_harness: ["codex"] })).toEqual(["q2", "q4"]);
    expect(await ids(db, { harness: ["claude", "gemini"], not_harness: ["gemini"] })).toEqual(["q2"]);
    // agent: an element of agent_names, case-insensitive substring.
    expect(await ids(db, { agent_terms: ["lead"] })).toEqual(["q1"]);
    expect(await ids(db, { agent_terms: ["rit"] })).toEqual(["q2"]);
    expect(await ids(db, { not_agent_terms: ["lead"] })).toEqual(["q2", "q3", "q4"]);
    // id.
    expect(await ids(db, { id: ["q3"] })).toEqual(["q3"]);
    expect(await ids(db, { not_id: ["q3"] })).toEqual(["q1", "q2", "q4"]);
    // is:active / is:inactive.
    expect(await ids(db, { active: true })).toEqual(["q1"]);
    expect(await ids(db, { active: false })).toEqual(["q2", "q3", "q4"]);
    // Terms AND across fields, and with the list parameters.
    expect(await ids(db, { free: ["login"], harness: ["claude"] })).toEqual(["q2"]);
    expect(await ids(db, { title: ["login"], active: false, not_harness: ["gemini"] })).toEqual(["q2"]);
    expect(await ids(db, { free: ["login"], sources: ["codex"] })).toEqual(["q1"]);
    expect(await ids(db, { free: ["login"], not_sources: ["codex"] })).toEqual(["q2"]);
    expect(await ids(db, { title: ["login"], project_contains: "ACME" })).toEqual(["q1", "q2"]);
    expect(await ids(db, { title: ["login"], user_ids: ["u1"] })).toEqual(["q1"]);
    // Dates arrive as last-activity bounds.
    expect(await ids(db, { title: ["login"], last_ts_after: "2026-10-04T00:00:00Z", last_ts_before: "2026-10-05T00:00:00Z" })).toEqual(["q2"]);
    expect(await ids(db, { first_ts_after: "2026-10-04T00:00:00Z" })).toEqual(["q1", "q2"]);
    // Rows are complete list rows.
    expect((await asReadApi(db, ["org-a", "ws-a"], () => query(db, { id: ["q1"] })))[0]).toMatchObject({
      org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: "q1", title: "Fix login bug", state: "active", agent_names: ["Lead"],
    });
  }, 120_000);

  test("LIKE metacharacters are literal; empty search_text and deleted rows never match a free word", async () => {
    const db = await database();
    await seed(db, [
      ...FIXTURE,
      { id: "pct", last: "2026-10-01T10:00:00Z", title: "100x coverage" },
      { id: "und", last: "2026-10-01T09:00:00Z", title: "coverageXpush" },
      { id: "bsl", last: "2026-10-01T08:00:00Z", title: "a/b path" },
    ]);
    expect(await ids(db, { free: ["100%"] })).toEqual(["q3"]);
    expect(await ids(db, { title: ["100%"] })).toEqual(["q3"]);
    expect(await ids(db, { free: ["coverage_push"] })).toEqual(["q3"]);
    expect(await ids(db, { free: ["a\\b"] })).toEqual(["q3"]);
    expect(await ids(db, { title: ["___"] })).toEqual([]);
    expect(await ids(db, { id: ["%"] })).toEqual([]);
    expect(await ids(db, { agent_terms: ["%"] })).toEqual([]);
    expect(await ids(db, { project: ["%"] })).toEqual([]);
    // q4 (search_text '') matches by title but never by a free word, and never by an excluded
    // word either way (its search_text is '' so NOT ILIKE holds: it is listed).
    expect(await ids(db, { free: ["refactor"] })).toEqual([]);
    expect(await ids(db, { title: ["refactor"] })).toEqual(["q4"]);
    // The deleted row's title would match; it never appears.
    expect(await ids(db, { title: ["gone"] })).toEqual([]);
    expect(await ids(db, { id: ["gone"] })).toEqual([]);
  }, 120_000);

  test("filters before LIMIT: a three-page keyset walk returns every match once, in order", async () => {
    const db = await database();
    const many: Session[] = [];
    for (let i = 0; i < 40; i++)
      many.push({
        id: `w${String(i).padStart(2, "0")}`, harness: i % 2 ? "claude" : "codex",
        last: new Date(Date.UTC(2026, 9, 1, 0, i)).toISOString(), title: i % 5 === 0 ? `needle ${i}` : `hay ${i}`,
      });
    await seed(db, many);
    const expected = many.filter((s) => s.title!.startsWith("needle")).map((s) => s.id).reverse();
    expect(expected.length).toBe(8);
    const seen: string[] = [];
    let cursor: Pick<Q, "cursor_last_activity_at" | "cursor_session_id" | "cursor_harness"> = {};
    for (let page = 0; page < 3; page++) {
      const rows = await asReadApi(db, ["org-a", "ws-a"], () => query(db, { free: ["needle"], limit: 3, ...cursor }));
      // limit + 1 rows when there is more.
      expect(rows.length).toBe(page < 2 ? 4 : 2);
      const pageRows = rows.slice(0, 3);
      seen.push(...pageRows.map((r) => r.session_id as string));
      const last = pageRows[pageRows.length - 1]!;
      cursor = {
        cursor_last_activity_at: (last.last_activity_at as Date).toISOString(),
        cursor_session_id: last.session_id as string,
        cursor_harness: last.harness as string,
      };
    }
    expect(seen).toEqual(expected);
  }, 120_000);

  test("activity counts the filtered sessions with the SPAN semantics", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const day = (d: string) => new Date(`${d}T00:00:00Z`);
    await asReadApi(db, ["org-a", "ws-a"], async () => {
      // q1 spans 10-04..10-05; q2 is 10-04 only.
      expect(await activity(db, "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z", { free: ["login"] })).toEqual([
        { day: day("2026-10-04"), harness: "claude", sessions: 1 },
        { day: day("2026-10-04"), harness: "codex", sessions: 1 },
        { day: day("2026-10-05"), harness: "codex", sessions: 1 },
      ]);
      expect(await activity(db, "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z", { free: ["login"], harness: ["claude"] })).toEqual([
        { day: day("2026-10-04"), harness: "claude", sessions: 1 },
      ]);
      expect(await activity(db, "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z", { title: ["login"], not_harness: ["codex"] })).toEqual([
        { day: day("2026-10-02"), harness: "gemini", sessions: 1 },
        { day: day("2026-10-04"), harness: "claude", sessions: 1 },
      ]);
      expect(await activity(db, "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z", { free: ["nothing here"] })).toEqual([]);
    });
    // The unfiltered call answers exactly what session_directory_activity_for answers.
    const plain = await asReadApi(db, ["org-a", "ws-a"], () =>
      db.query<Row>("SELECT * FROM sessions.session_directory_activity_for('ws-a', '2026-10-01T00:00:00Z', '2026-10-06T23:59:59Z')"));
    expect(await asReadApi(db, ["org-a", "ws-a"], () => activity(db, "2026-10-01T00:00:00Z", "2026-10-06T23:59:59Z", {}))).toEqual(plain.rows);
  }, 120_000);

  test("22023 for invalid values, 0A000 for agent= before the catalog backfill", async () => {
    const db = await database();
    const bad: Q[] = [
      { limit: 0 }, { limit: 201 },
      { free: ["ab"] }, { free: [" login"] }, { free: ["x".repeat(201)] }, { not_free: ["a b"] },
      { title: [""] }, { title: ["x".repeat(201)] }, { id: [""] },
      { harness: ["Bad Name"] }, { not_harness: [""] },
      { free: Array(17).fill("login") },
      { title: Array(9).fill("a"), id: Array(8).fill("b") },
      { sources: ["Bad Name"] }, { agent: "  " }, { project_contains: " " },
      { last_ts_after: "2026-10-05T00:00:00Z", last_ts_before: "2026-10-04T00:00:00Z" },
      { cursor_session_id: "q1" },
    ];
    for (const q of bad)
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, q))), JSON.stringify(q).slice(0, 120)).toBe("22023");
    // A NULL element.
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => db.query("SELECT * FROM sessions.query_session_directory_for(p_workspace_id => 'ws-a', p_title => ARRAY[NULL]::text[])")))).toBe("22023");
    // Exactly 16 terms is accepted.
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, { title: Array(8).fill("a"), id: Array(8).fill("b") })))).toBeUndefined();
    for (const [since, until] of [["2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z"], ["2025-01-01T00:00:00Z", "2026-10-01T00:00:00Z"]] as const)
      expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activity(db, since, until, {})))).toBe("22023");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => activity(db, "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", { free: ["ab"] })))).toBe("22023");
    await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = NULL");
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, { agent: "lead" })))).toBe("0A000");
    // agent terms (agent_names) need no catalog backfill.
    expect(await code(asReadApi(db, ["org-a", "ws-a"], () => query(db, { agent_terms: ["lead"] })))).toBeUndefined();
  }, 120_000);

  test("ACLs are exactly the manifest's (bulls96_read_api only), and the read role verifier accepts them", async () => {
    const db = await database();
    const manifest = JSON.parse(read("src/hoopdb/sessions-function-acl.json")).functions as Record<string, string[]>;
    const rows = (await db.query<{ fn: string; grantees: string[]; definer: boolean }>(
      `SELECT p.oid::regprocedure::text AS fn, p.prosecdef AS definer,
              array(SELECT CASE WHEN a.grantee = p.proowner THEN '<owner>' ELSE coalesce(r.rolname, 'PUBLIC') END || ':' || a.privilege_type
                      FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
                      LEFT JOIN pg_roles AS r ON r.oid = a.grantee ORDER BY 1) AS grantees
         FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' AND (p.proname LIKE 'query\\_session\\_directory\\_%' OR p.proname LIKE 'session\\_directory\\_query\\_%')
        ORDER BY 1`,
    )).rows;
    const expected = [
      { fn: QUERY_SIG, definer: true, grantees: ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"] },
      { fn: ACTIVITY_SIG, definer: true, grantees: ["<owner>:EXECUTE", "bulls96_read_api:EXECUTE"] },
      ...HELPERS.map((fn) => ({ fn, definer: false, grantees: ["<owner>:EXECUTE"] })),
    ].sort((a, b) => (a.fn < b.fn ? -1 : 1));
    expect(rows).toEqual(expected);
    for (const { fn, grantees } of expected) expect(manifest[fn], fn).toEqual(grantees);
    await db.exec(VERIFY);
    // The verifier still refuses any other definer grant to the read role.
    await db.exec("GRANT EXECUTE ON FUNCTION sessions.session_directory_activity(text, timestamptz, timestamptz) TO bulls96_read_api");
    expect(await code(db.exec(VERIFY))).toBe("P0001");
  }, 120_000);

  test("up, down, up (the down refuses while a later upstream migration is ledgered)", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    // Raw migration files write no ledger (the runner does); a one-column stand-in is enough
    // for the down's guard, which only reads names.
    expect((await db.query<{ r: string | null }>("SELECT to_regclass('sessions.__96bulls_migrations')::text AS r")).rows[0]!.r).toBeNull();
    await db.exec("CREATE TABLE sessions.__96bulls_migrations (name text PRIMARY KEY)");
    await db.query("INSERT INTO sessions.__96bulls_migrations VALUES ('upstream:20261105046010_session_directory_read_api_query.sql'), ('upstream:20261105099000_later_probe.sql')");
    expect(await code(db.exec(read(DOWN)))).toBe("55000");
    expect((await db.query<{ ok: unknown }>(`SELECT to_regprocedure('${QUERY_SIG}') AS ok`)).rows[0]!.ok).not.toBeNull();
    await db.query("DELETE FROM sessions.__96bulls_migrations WHERE name = 'upstream:20261105099000_later_probe.sql'");
    await db.exec(read(DOWN));
    const gone = await db.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_proc WHERE pronamespace = 'sessions'::regnamespace AND (proname LIKE 'query\\_session\\_directory\\_%' OR proname LIKE 'session\\_directory\\_query\\_%')");
    expect(gone.rows).toEqual([{ n: 0 }]);
    expect((await db.query("SELECT name FROM sessions.__96bulls_migrations")).rows).toEqual([]);
    // The 034000 functions and the data are untouched.
    expect((await asReadApi(db, ["org-a", "ws-a"], () =>
      db.query<Row>("SELECT session_id FROM sessions.list_session_directory_for(p_workspace_id => 'ws-a')"))).rows.map((r) => r.session_id)).toEqual(["q1", "q2", "q3", "q4"]);
    await db.exec(read(MIGRATION));
    expect(await ids(db, { free: ["login"] })).toEqual(["q1", "q2"]);
    await db.exec(VERIFY);
  }, 120_000);

  test("a free word uses the tenant trigram index session_directory_tenant_search_trgm_idx", async () => {
    const db = await database();
    // 20,000 rows, as session-directory-short-summary.test.ts sizes its plan checks: a large
    // tenant beside six others; two in every 1,000 titles hold the selective needle.
    await db.exec(`
      INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-a' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-a' END, 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n % 1000 IN (1, 2) THEN 'quokka ledger ' || n ELSE 'ordinary session ' || n END
        FROM generate_series(1, 20000) AS n;
      UPDATE sessions.session_directory SET search_text = sessions.session_directory_search_text(title, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
      ANALYZE sessions.session_directory;
    `);
    // The statement the function executes, built by the same builder (owner-only, so as owner).
    const where = (await db.query<{ w: string }>(
      "SELECT sessions.session_directory_query_where(1,0,0,0,0,0,0,0,0,0,0,0,false,false,false,false,false,false,false,false,false,false) AS w")).rows[0]!.w;
    await db.exec(`PREPARE q_shape (text, text, text[]) AS SELECT d.session_id FROM sessions.session_directory AS d
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL${where}
      ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT 51`);
    const plan = JSON.stringify((await db.query<Row>(
      "EXPLAIN (FORMAT JSON) EXECUTE q_shape('org-a', 'ws-a', sessions.session_directory_query_like(ARRAY['quokka ledger']))")).rows);
    expect(plan).toContain("session_directory_tenant_search_trgm_idx");
    expect(plan).not.toContain('"Seq Scan"');
    const hits = (await db.query<Row>("EXECUTE q_shape('org-a', 'ws-a', sessions.session_directory_query_like(ARRAY['quokka ledger']))")).rows;
    expect(hits.length).toBe(40); // n % 1000 in (1, 2) never has n % 4 = 0: all in org-a/ws-a
  }, 120_000);
});
