/**
 * Agent-name prefix matching (20261105050020): the 17-argument
 * sessions.list_session_directory_for overload (p_agent_match 'exact' | 'prefix'), its
 * text_pattern_ops index, and GET /v1/sessions behind READ_API_AGENT_MATCH. Run AS
 * bulls96_read_api with the tenant bound the way the read Worker binds it. PGlite, every
 * migration.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { TENANT_SETTINGS_SQL, poolTenantRunner } from "../../session-read-api/liveness.ts";
import { createReadApi } from "../../session-read-api/app.ts";
import { sqlDirectory } from "../../session-read-api/directory.ts";
import { agentMatchDeps, agentMatchEnabled } from "../../session-read-api/config.ts";
import { memoryLiveness } from "../../session-read-api/__tests__/support.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const MIGRATION = "supabase/migrations/20261105050020_session_directory_agent_match.sql";
const DOWN = "supabase/rollbacks/20261105050020_session_directory_agent_match.down.sql";
const T = "timestamp with time zone";
const LIST_SIG = `sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer)`;
const MATCH_SIG = `sessions.list_session_directory_for(text,text[],text[],text,text[],text,${T},${T},${T},${T},text[],text[],${T},text,text,integer,text)`;
const INDEX = "session_directory_agents_prefix_idx";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

/** Every migration through ours, the read API role bootstrapped, the catalog marked complete. */
async function database(beforeOurs?: (db: PGlite) => Promise<void>): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of SESSION_BOOTSTRAP_FILES) await db.exec(read(file));
  const files = readdirSync(join(root, "supabase/migrations")).sort();
  expect(files.at(-1)).toBe(MIGRATION.split("/").at(-1)!);
  for (const file of files.slice(0, -1)) await db.exec(read(`supabase/migrations/${file}`));
  if (beforeOurs) await beforeOurs(db);
  await db.exec(read(MIGRATION));
  await db.exec(sql("supabase/bootstrap/session_read_api_role.sql"));
  await db.exec("UPDATE sessions.session_directory_catalog_rollout SET completed_at = now()");
  return db;
}

type Row = Record<string, unknown>;
const failure = async (p: Promise<unknown>) =>
  (await p.then(() => null, (e: unknown) => e)) as { code?: string; message?: string } | null;

type Session = { workspace?: string; harness?: string; id: string; last: string; agents?: string[] };
async function seed(db: PGlite, sessions: Session[]) {
  for (const s of sessions) {
    const org = "org-a", ws = s.workspace ?? "ws-a", harness = s.harness ?? "codex";
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, user_ids, agent_names, catalog_projected_at)
       VALUES ($1, $2, $3, $4, $5, $5, '{}', $6, now())`,
      [org, ws, harness, s.id, s.last, s.agents ?? []],
    );
    for (const agent of s.agents ?? [])
      await db.query(
        `INSERT INTO sessions.session_directory_agents (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
         VALUES ($1, $2, $3, $4, $5, lower(btrim($5)), $6)`,
        [org, ws, harness, s.id, agent, s.last],
      );
  }
}

/** `n` sessions in ws-a, each with one agent `bulk-<i>`. */
async function bulk(db: PGlite, n: number, from = 1) {
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at, user_ids, agent_names, catalog_projected_at)
     SELECT 'org-a', 'ws-a', 'codex', 'bulk-' || g, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '{}', ARRAY['bulk-' || g], now()
       FROM generate_series($1::int, $2::int) AS g`,
    [from, from + n - 1],
  );
  await db.query(
    `INSERT INTO sessions.session_directory_agents (org_id, workspace_id, harness, session_id, agent_name, agent_name_normalized, last_activity_at)
     SELECT 'org-a', 'ws-a', 'codex', 'bulk-' || g, 'bulk-' || g, 'bulk-' || g, '2026-01-01T00:00:00Z'
       FROM generate_series($1::int, $2::int) AS g`,
    [from, from + n - 1],
  );
}

const FIXTURE: Session[] = [
  { id: "s1", last: "2026-10-05T12:00:00Z", agents: ["Worker-One"] },
  { id: "s2", last: "2026-10-04T12:00:00Z", agents: ["lead", "worker-two"] },
  { id: "s3", last: "2026-10-03T12:00:00Z", agents: ["work_x"] },
  { id: "s4", last: "2026-10-02T12:00:00Z", agents: ["workAx"] },
  { id: "s5", last: "2026-10-01T12:00:00Z", agents: ["50%off"] },
  { id: "s6", last: "2026-09-30T12:00:00Z", agents: ["500off"] },
  { id: "s7", last: "2026-09-29T12:00:00Z" },
  { id: "other-ws", workspace: "ws-b", last: "2026-10-06T00:00:00Z", agents: ["worker-one"] },
];

async function asReadApi<R>(db: PGlite, work: () => Promise<R>): Promise<R> {
  await db.exec("SET ROLE bulls96_read_api");
  try {
    await db.exec("BEGIN READ ONLY");
    try {
      await db.query(TENANT_SETTINGS_SQL, ["org-a", "ws-a"]);
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

const ARGS = `p_workspace_id => 'ws-a', p_sources => NULL, p_not_sources => NULL, p_project_contains => NULL,
  p_user_ids => NULL, p_agent => $1, p_last_ts_after => NULL, p_last_ts_before => NULL, p_first_ts_after => NULL,
  p_first_ts_before => NULL, p_session_harnesses => NULL, p_session_ids => NULL,
  p_cursor_last_activity_at => NULL, p_cursor_session_id => NULL, p_cursor_harness => NULL, p_limit => 50`;
const ids = (rows: Row[]) => rows.map((r) => r.session_id);
const list = async (db: PGlite, agent: string | null, match?: string | null) =>
  (await db.query<Row>(
    `SELECT * FROM sessions.list_session_directory_for(${ARGS}${match === undefined ? "" : ", p_agent_match => $2"})`,
    match === undefined ? [agent] : [agent, match],
  )).rows;

describe("list_session_directory_for, p_agent_match (20261105050020)", () => {
  test("exact answers what the 16-argument function answers; prefix is case-insensitive, escaped, tenant-bound", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    for (const agent of ["worker-one", " LEAD ", "work_x", "50%off", "nobody", null]) {
      const old = await asReadApi(db, () => list(db, agent));
      expect(await asReadApi(db, () => list(db, agent, "exact"))).toEqual(old);
    }
    expect(ids(await asReadApi(db, () => list(db, "worker-one")))).toEqual(["s1"]);
    // Newest first; a session with several agents appears once; ws-b never.
    expect(ids(await asReadApi(db, () => list(db, "WORK", "prefix")))).toEqual(["s1", "s2", "s3", "s4"]);
    expect(ids(await asReadApi(db, () => list(db, "worker", "prefix")))).toEqual(["s1", "s2"]);
    // _ and % in the prefix match themselves only.
    expect(ids(await asReadApi(db, () => list(db, "work_", "prefix")))).toEqual(["s3"]);
    expect(ids(await asReadApi(db, () => list(db, "50%", "prefix")))).toEqual(["s5"]);
    expect(ids(await asReadApi(db, () => list(db, "zzz", "prefix")))).toEqual([]);
  }, 300_000);

  test("refuses bad agent_match, a prefix without an agent, a short prefix, and a too-broad prefix", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    for (const [agent, match] of [["worker", "fuzzy"], ["worker", null], [null, "prefix"], ["wo", "prefix"], ["w  o", "prefix"]] as const)
      expect((await failure(asReadApi(db, () => list(db, agent, match))))?.code).toBe("22023");
    // 5000 rows under the prefix are served; 5001 are refused.
    await bulk(db, 5000);
    expect((await asReadApi(db, () => list(db, "bulk", "prefix"))).length).toBe(51); // p_limit + 1
    await bulk(db, 1, 5001);
    const broad = await failure(asReadApi(db, () => list(db, "bulk", "prefix")));
    expect(broad?.code).toBe("54000");
    expect(broad?.message).toContain("agent_prefix_too_broad");
    // Other tenants' rows never count against the budget.
    expect(ids(await asReadApi(db, () => list(db, "worker", "prefix")))).toEqual(["s1", "s2"]);
  }, 300_000);

  test("the candidate budget walks the prefix index", async () => {
    const db = await database();
    await seed(db, FIXTURE);
    const def = (await db.query<{ def: string }>(`SELECT pg_get_indexdef('sessions.${INDEX}'::regclass) AS def`)).rows[0]!.def;
    expect(read(MIGRATION)).toContain(`c_def CONSTANT text:='${def}'`);
    await db.exec("BEGIN; SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_sort = off");
    const plan = (await db.query<{ "QUERY PLAN": string }>(
      `EXPLAIN SELECT count(*) FROM (SELECT 1 FROM sessions.session_directory_agents AS a
        WHERE a.org_id = 'org-a' AND a.workspace_id = 'ws-a' AND a.agent_name_normalized LIKE 'work%' ESCAPE '\\'
        ORDER BY a.agent_name_normalized USING ~<~ LIMIT 5001) AS c`,
    )).rows.map((r) => r["QUERY PLAN"]);
    await db.exec("ROLLBACK");
    const line = plan.find((l) => l.includes(INDEX));
    console.log(`EXPLAIN: ${line?.trim()}`);
    expect(line).toMatch(new RegExp(`Index (Only )?Scan using ${INDEX}`));
    expect(plan.join("\n")).toContain("~>=~");
  }, 300_000);

  test("grants: the read API role only; the 16-argument function is unchanged", async () => {
    const db = await database();
    const grantees = (await db.query<{ r: string }>(
      `SELECT r.rolname AS r FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a JOIN pg_roles r ON r.oid = a.grantee
        WHERE p.oid = '${MATCH_SIG}'::regprocedure AND a.grantee <> p.proowner ORDER BY 1`,
    )).rows.map((r) => r.r);
    expect(grantees).toEqual(["bulls96_read_api"]);
    expect((await db.query(`SELECT to_regprocedure('${LIST_SIG}') IS NOT NULL AS kept, pronargdefaults AS d FROM pg_proc WHERE oid = '${MATCH_SIG}'::regprocedure`)).rows)
      .toEqual([{ kept: true, d: 0 }]);
    // The rollback (generated, ledger-guarded) is exercised by migration-profile.test.ts.
  }, 300_000);

  test("a populated table without the index is refused, not built blocking; a matching prebuilt index is adopted", async () => {
    let refused: { code?: string; message?: string } | null = null;
    const db = await database(async (db) => {
      await seed(db, FIXTURE);
      refused = await failure(db.exec(read(MIGRATION)));
      expect((await db.query(`SELECT to_regprocedure('${MATCH_SIG}') IS NULL AS gone`)).rows).toEqual([{ gone: true }]);
      // Prebuilt out of band with a different definition: refused, nothing changed.
      await db.exec(`CREATE INDEX ${INDEX} ON sessions.session_directory_agents (org_id, workspace_id, agent_name_normalized)`);
      expect((await failure(db.exec(read(MIGRATION))))?.code).toBe("42P07");
      await db.exec(`DROP INDEX sessions.${INDEX}`);
      // Prebuilt with the expected definition: adopted.
      await db.exec(`CREATE INDEX ${INDEX} ON sessions.session_directory_agents USING btree (org_id, workspace_id, agent_name_normalized text_pattern_ops, last_activity_at DESC, session_id DESC, harness DESC)`);
    });
    expect(refused!?.code).toBe("55000");
    expect(ids(await asReadApi(db, () => list(db, "worker", "prefix")))).toEqual(["s1", "s2"]);
  }, 300_000);
});

describe("READ_API_AGENT_MATCH", () => {
  test("on only for the exact string true", () => {
    expect(agentMatchEnabled(undefined)).toBe(false);
    expect(agentMatchEnabled("")).toBe(false);
    expect(agentMatchEnabled("false")).toBe(false);
    expect(agentMatchEnabled("true")).toBe(true);
    for (const bad of ["TRUE", "1", "yes", " true"]) expect(() => agentMatchEnabled(bad)).toThrow();
    expect(agentMatchDeps({})).toEqual({});
    expect(agentMatchDeps({ READ_API_AGENT_MATCH: "true" })).toEqual({ agentMatch: true });
  });
});

describe("GET /v1/sessions agent matching over the real functions", () => {
  async function api(flag: boolean) {
    const db = await database();
    await seed(db, FIXTURE);
    const statements: string[] = [];
    const pool = {
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, p?: unknown[]) => {
            statements.push(text);
            return db.query(text, p as unknown[]) as Promise<{ rows: unknown[] }>;
          },
          release: () => void db.exec("RESET ROLE"),
        };
      },
    };
    const app = createReadApi({
      verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
      ch: {} as never,
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      directory: sqlDirectory(poolTenantRunner(pool)),
      directoryWorkspaces: new Set(["ws-a"]),
      ...(flag ? { agentMatch: true } : {}),
    });
    const get = async (query: string) => {
      const r = await app.request(`/v1/sessions?workspace=ws-a&${query}`, { headers: { authorization: "Bearer t" } });
      return { status: r.status, header: r.headers.get("x-sessions-read-agent-match"), body: (await r.json()) as Record<string, any> };
    };
    return { db, get, statements };
  }
  const sessionIds = (body: Record<string, any>) => body.sessions.map((s: { sessionId: string }) => s.sessionId);

  test("flag off: exact unchanged, agentMatch and a trailing * are 501 param_agentMatch", async () => {
    const { get, statements } = await api(false);
    const exact = await get("agent=Worker-One");
    expect([exact.status, exact.header, sessionIds(exact.body)]).toEqual([200, null, ["s1"]]);
    for (const q of ["agent=work&agentMatch=prefix", "agent=worker-one&agentMatch=exact", "agent=work*"]) {
      const r = await get(q);
      expect([r.status, r.body.error?.code ?? r.body.code, JSON.stringify(r.body)]).toMatchObject([501, expect.anything(), expect.stringContaining("param_agentMatch")]);
    }
    expect(statements.some((s) => s.includes("p_agent_match"))).toBe(false);
  }, 300_000);

  test("flag on: exact, prefix by agentMatch and by *, header, and the 400s", async () => {
    const { get } = await api(true);
    const exact = await get("agent=worker-one");
    expect([exact.status, exact.header, sessionIds(exact.body)]).toEqual([200, "exact", ["s1"]]);
    const explicit = await get("agent=worker-one&agentMatch=exact");
    expect([explicit.status, explicit.header, sessionIds(explicit.body)]).toEqual([200, "exact", ["s1"]]);
    const prefix = await get("agent=Work&agentMatch=prefix");
    expect([prefix.status, prefix.header, sessionIds(prefix.body)]).toEqual([200, "prefix", ["s1", "s2", "s3", "s4"]]);
    const star = await get("agent=worker*");
    expect([star.status, star.header, sessionIds(star.body)]).toEqual([200, "prefix", ["s1", "s2"]]);
    const none = await get("limit=50");
    expect([none.status, none.header]).toEqual([200, null]);
    for (const q of [
      "agent=wo&agentMatch=prefix", "agent=wo*", "agent=*", "agent=work&agentMatch=fuzzy", "agentMatch=prefix",
      "agent=work&agentMatch=prefix&agentMatch=exact", "agent=work*&agentMatch=exact", "agent=lead&agent=worker-one",
    ]) expect([q, (await get(q)).status]).toEqual([q, 400]);
    // Literal * never reaches SQL as a pattern: a name with an inner * is exact.
    expect((await get("agent=wo*rk")).status).toBe(200);
  }, 300_000);

  test("flag on: a too-broad prefix is 400 agent_prefix_too_broad", async () => {
    const { db, get } = await api(true);
    await bulk(db, 5001);
    const r = await get("agent=bulk*");
    expect(r.status).toBe(400);
    expect(JSON.stringify(r.body)).toContain("agent_prefix_too_broad");
  }, 300_000);
});
