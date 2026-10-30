/**
 * G6: short_summary on sessions.session_directory and its one search path,
 * sessions.search_session_directory. In-process PGlite only, with every
 * supabase/migrations file applied in name order after the Supabase role shim; no
 * hosted database is involved. Synthetic ids and text only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { auto_explain } from "@electric-sql/pglite/contrib/auto_explain";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SCRUB_RULESET, SCRUB_RULESET_VERSION } from "../../shared/vendor.ts";
import type { HoopDb } from "../../shared/db.ts";
import { hoopDbSessionIndex } from "../index.ts";
import {
  SESSION_DIRECTORY_SEARCH_BOUNDARY,
  SESSION_DIRECTORY_SEARCH_RPC,
  SESSION_DIRECTORY_SEARCH_SQL,
} from "../search-index-contract.ts";
import {
  SHORT_SUMMARY_MAX_CHARS,
  ShortSummaryRejected,
  clearShortSummary,
  eraseSessionDirectory,
  scrubShortSummary,
  writeShortSummary,
} from "../session-directory-summary.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const MIGRATION = "20261030020000_session_directory_short_summary.sql";
const DOWN = read(`supabase/rollbacks/${MIGRATION.replace(/\.sql$/, ".down.sql")}`);
const UP = read(`supabase/migrations/${MIGRATION}`);
const WORKER = SESSION_DIRECTORY_SEARCH_RPC.workerRole;

// Synthetic secret-shaped values, assembled at runtime so no literal sits on disk.
const FAKE_RELAY_TOKEN = ["relay", "ag", "x".repeat(24)].join("_");
const FAKE_HOME = ["", "Users", "alice", "proj"].join("/");

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { auto_explain, btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  return db;
}

const KEY = { orgId: "org-a", workspaceId: "ws-a", harness: "codex", sessionId: "s-1" };
const AT = new Date("2026-10-30T12:00:00Z");

async function insertRow(db: PGlite, org = "org-a", workspace = "ws-a", session = "s-1") {
  await db.query(
    `INSERT INTO sessions.session_directory
       (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
        title, agent_name, agent_name_normalized, project_id, search_text)
     VALUES ($1, $2, 'codex', $3, $4, $4, 'Fix flaky queue test', 'Helm', 'helm', 'proj-1',
             sessions.session_directory_search_text('Fix flaky queue test', NULL, 'Helm', 'proj-1',
               NULL, NULL, NULL, NULL))`,
    [org, workspace, session, AT.toISOString()],
  );
}

async function summarize(db: PGlite, key: typeof KEY, summary: string, generatedAt = AT) {
  return writeShortSummary(db, { ...key, summary, source: "bob-costas/v1", generatedAt });
}

type Row = Record<string, unknown>;
async function row(db: PGlite, session = "s-1"): Promise<Row | undefined> {
  return (await db.query<Row>(
    "SELECT * FROM sessions.session_directory WHERE session_id = $1", [session],
  )).rows[0];
}

const claims = (org: string, workspaces: string[], role: string = WORKER) =>
  JSON.stringify({ role, app_metadata: { org_id: org, workspace_ids: workspaces } });

/** Calls the RPC exactly as PostgREST would: SET ROLE to the Worker, claims GUC set. */
async function search(
  db: PGlite,
  claimsJson: string | null,
  workspace: string,
  query: string,
  onNotice?: (message: string) => void,
): Promise<Row[]> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [claimsJson ?? ""]);
  await db.exec(`SET ROLE ${WORKER}`);
  try {
    const result = await db.query<Row>(
      "SELECT * FROM sessions.search_session_directory($1, $2)",
      [workspace, query],
      onNotice ? { onNotice: (notice: { message: string }) => onNotice(notice.message) } as never : undefined,
    );
    return result.rows;
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

describe("short_summary writer", () => {
  test("stores only scrubbed text and records the scrub ruleset and version", async () => {
    const db = await database();
    await insertRow(db);
    const summary = `Rotated ${FAKE_RELAY_TOKEN} and edited ${FAKE_HOME}/queue.ts to fix retries.`;
    const result = await summarize(db, KEY, summary);
    expect(result).toEqual({ written: true, scrubVersion: SCRUB_RULESET_VERSION });
    const stored = await row(db);
    expect(stored?.short_summary).toBe("Rotated [REDACTED] and edited ~/proj/queue.ts to fix retries.");
    expect(String(stored?.short_summary)).not.toContain(FAKE_RELAY_TOKEN);
    expect(String(stored?.search_text)).not.toContain(FAKE_RELAY_TOKEN);
    expect(stored?.short_summary_scrub_ruleset).toBe(SCRUB_RULESET);
    expect(stored?.short_summary_scrub_version).toBe(SCRUB_RULESET_VERSION);
    expect(stored?.short_summary_source).toBe("bob-costas/v1");
    expect((stored?.short_summary_generated_at as Date).toISOString()).toBe(AT.toISOString());
  });

  test("refuses over-length text in the writer and in the database", async () => {
    const db = await database();
    await insertRow(db);
    const exact = "a".repeat(SHORT_SUMMARY_MAX_CHARS);
    expect(scrubShortSummary(exact).text).toBe(exact);
    // Counted in code points, as PostgreSQL char_length does: 1000 emoji fit.
    expect(scrubShortSummary("\u{1F402}".repeat(SHORT_SUMMARY_MAX_CHARS)).text).toHaveLength(2000);

    const over = "a".repeat(SHORT_SUMMARY_MAX_CHARS + 1);
    expect(() => scrubShortSummary(over)).toThrow(new ShortSummaryRejected("over_length"));
    await expect(summarize(db, KEY, over)).rejects.toThrow("short_summary rejected: over_length");
    expect((await row(db))?.short_summary).toBeNull();

    // A writer that bypasses the TypeScript path still hits the CHECK.
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = $1, short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
      [over],
    )).rejects.toThrow(/session_directory_short_summary_length/);
  });

  test("refuses when the scrub fails or is not stable, and writes nothing", async () => {
    const db = await database();
    await insertRow(db);
    const input = { ...KEY, summary: "a normal summary", source: "bob-costas/v1", generatedAt: AT };
    await expect(writeShortSummary(db, input, () => { throw new Error("boom"); }))
      .rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(writeShortSummary(db, input, (value) => `${value}!`))
      .rejects.toThrow("short_summary rejected: scrub_unstable");
    await expect(writeShortSummary(db, input, () => undefined as unknown as string))
      .rejects.toThrow("short_summary rejected: scrub_failed");
    await expect(writeShortSummary(db, { ...input, summary: "   " }))
      .rejects.toThrow("short_summary rejected: empty");
    await expect(writeShortSummary(db, { ...input, source: "Not A Source" }))
      .rejects.toThrow("short_summary rejected: invalid_source");
    expect((await row(db))?.short_summary).toBeNull();
  });

  test("the database rejects summary text that lacks scrub provenance labels", async () => {
    const db = await database();
    await insertRow(db);
    await expect(db.query(
      "UPDATE sessions.session_directory SET short_summary = 'unscrubbed text'",
    )).rejects.toThrow(/session_directory_short_summary_provenance/);
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'something-else',
         short_summary_scrub_version = 1, short_summary_source = 'x', short_summary_generated_at = now()`,
    )).rejects.toThrow(/session_directory_short_summary_scrub_ruleset/);
    await expect(db.query(
      `UPDATE sessions.session_directory SET short_summary = 'x', short_summary_scrub_ruleset = 'relayhistory-scrub',
         short_summary_scrub_version = 0, short_summary_source = 'x', short_summary_generated_at = now()`,
    )).rejects.toThrow(/session_directory_short_summary_scrub_version/);
  });

  test("an older summary never replaces a newer one", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "newer");
    const older = await summarize(db, KEY, "older", new Date(AT.getTime() - 1000));
    expect(older.written).toBe(false);
    expect((await row(db))?.short_summary).toBe("newer");
  });
});

describe("short_summary in search_text", () => {
  test("is part of the declared boundary and of search_text, and is found through the RPC", async () => {
    expect(SESSION_DIRECTORY_SEARCH_BOUNDARY.fields).toContain("short_summary");
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Migrated the zebra-crossing scheduler");
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1 Migrated the zebra-crossing scheduler");
    const hits = await search(db, claims("org-a", ["ws-a"]), "ws-a", "ZEBRA-cross");
    expect(hits.map((hit) => hit.session_id)).toEqual(["s-1"]);
    expect(Object.keys(hits[0]!)).toEqual([...SESSION_DIRECTORY_SEARCH_RPC.returns]);
    expect(hits[0]!.short_summary).toBe("Migrated the zebra-crossing scheduler");

    expect(await clearShortSummary(db, KEY)).toBe(true);
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1");
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "zebra")).toEqual([]);
  });
});

describe("search RPC authorization", () => {
  test("claims for tenant A never return tenant B; missing or forged claims are refused", async () => {
    const db = await database();
    await insertRow(db, "org-a", "ws-a", "s-a");
    await insertRow(db, "org-b", "ws-b", "s-b");
    await insertRow(db, "org-a", "ws-hidden", "s-hidden");
    for (const [org, workspace, session] of [["org-a", "ws-a", "s-a"], ["org-b", "ws-b", "s-b"], ["org-a", "ws-hidden", "s-hidden"]]) {
      await summarize(db, { orgId: org!, workspaceId: workspace!, harness: "codex", sessionId: session! },
        `walrus summary for ${session}`);
    }
    const a = claims("org-a", ["ws-a"]);
    expect((await search(db, a, "ws-a", "walrus")).map((hit) => hit.session_id)).toEqual(["s-a"]);

    const refused = async (claimsJson: string | null, workspace: string) =>
      expect(search(db, claimsJson, workspace, "walrus")).rejects.toThrow("tenant claims missing or invalid");
    // Tenant A naming B's workspace, or its own org's unlisted workspace.
    await refused(a, "ws-b");
    await refused(a, "ws-hidden");
    // B's workspace id under A's org claim still cannot reach B's rows.
    await refused(claims("org-a", ["ws-b"]), "ws-x");
    expect(await search(db, claims("org-a", ["ws-b"]), "ws-b", "walrus")).toEqual([]);
    // Missing, malformed, wrong-role, and structurally forged claims.
    await refused(null, "ws-a");
    await refused("{not json", "ws-a");
    await refused("[]", "ws-a");
    await refused(claims("org-a", ["ws-a"], "authenticated"), "ws-a");
    await refused(claims("org-a", ["ws-a"], "service_role"), "ws-a");
    await refused(claims("", ["ws-a"]), "ws-a");
    await refused(JSON.stringify({ role: WORKER, app_metadata: { org_id: "org-a", workspace_ids: "ws-a" } }), "ws-a");
    await refused(JSON.stringify({ role: WORKER, app_metadata: { org_id: ["org-a"], workspace_ids: ["ws-a"] } }), "ws-a");
    await refused(JSON.stringify({ role: WORKER, org_id: "org-a", workspace_ids: ["ws-a"] }), "ws-a");
  });

  test("the Worker role has no table access, no login, no BYPASSRLS, and a statement timeout", async () => {
    const db = await database();
    const role = (await db.query<Row>(
      "SELECT rolcanlogin, rolinherit, rolbypassrls, rolsuper, rolconfig FROM pg_roles WHERE rolname = $1", [WORKER],
    )).rows[0];
    expect(role).toEqual({
      rolcanlogin: false, rolinherit: false, rolbypassrls: false, rolsuper: false,
      rolconfig: ["statement_timeout=2s"],
    });
    const privileged = await db.query<{ table_name: string }>(
      `SELECT c.relname AS table_name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p', 'v', 'm')
          AND (has_table_privilege($1, c.oid, 'SELECT') OR has_table_privilege($1, c.oid, 'INSERT')
            OR has_table_privilege($1, c.oid, 'UPDATE') OR has_table_privilege($1, c.oid, 'DELETE')
            OR has_any_column_privilege($1, c.oid, 'SELECT'))`,
      [WORKER],
    );
    expect(privileged.rows).toEqual([]);
    await db.exec(`SET ROLE ${WORKER}`);
    try {
      await expect(db.query("SELECT search_text FROM sessions.session_directory")).rejects.toThrow(/permission denied/);
      await expect(db.query("SELECT * FROM sessions.session_index")).rejects.toThrow(/permission denied/);
    } finally {
      await db.exec("RESET ROLE");
    }
  });

  test("only the Worker role may execute the RPC, and end users cannot filter search_text directly", async () => {
    const db = await database();
    const fn = "sessions.search_session_directory(text, text)";
    for (const grantee of ["anon", "authenticated", "service_role"]) {
      const allowed = await db.query<{ ok: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') AS ok", [grantee, fn]);
      expect(allowed.rows[0]?.ok, grantee).toBe(false);
    }
    const publicExec = await db.query(
      `SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
        WHERE p.oid = $1::regprocedure AND a.grantee = 0 AND a.privilege_type = 'EXECUTE'`,
      [fn],
    );
    expect(publicExec.rows).toEqual([]);
    await insertRow(db);
    await db.exec(`SET ROLE authenticated;
      SELECT set_config('request.jwt.claims', '{"app_metadata":{"org_id":"org-a","workspace_ids":["ws-a"]}}', false);`);
    try {
      await expect(db.query("SELECT * FROM sessions.search_session_directory('ws-a', 'flaky')")).rejects.toThrow(/permission denied/);
      await expect(db.query("SELECT session_id FROM sessions.session_directory WHERE search_text ILIKE '%flaky%'"))
        .rejects.toThrow(/permission denied/);
      await expect(db.query("SELECT short_summary FROM sessions.session_directory")).rejects.toThrow(/permission denied/);
      // RLS-scoped listing columns stay readable.
      expect((await db.query("SELECT session_id FROM sessions.session_directory")).rows).toEqual([{ session_id: "s-1" }]);
    } finally {
      await db.exec("RESET ROLE");
    }
  });
});

describe("search RPC query bounds", () => {
  test("1-2 character and over-long queries are refused; input is trimmed, NFC-normalized and literal", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "Café menu cut 50%_off for the launch");
    const a = claims("org-a", ["ws-a"]);
    for (const query of ["", "a", "ab", "  ab  ", "x".repeat(201)]) {
      await expect(search(db, a, "ws-a", query)).rejects.toThrow("query must be 3 to 200 characters");
    }
    expect(await search(db, a, "ws-a", "x".repeat(200))).toEqual([]);
    // Decomposed e + combining acute matches the composed stored text after NFC.
    expect(await search(db, a, "ws-a", "  café ")).toHaveLength(1);
    // % and _ are literal, not wildcards.
    expect(await search(db, a, "ws-a", "50%_off")).toHaveLength(1);
    expect(await search(db, a, "ws-a", "5_%")).toEqual([]);
    expect(await search(db, a, "ws-a", "menu%launch")).toEqual([]);
  });
});

type Plan = { [key: string]: unknown; Plans?: Plan[] };
const walk = (plan: Plan): Plan[] => [plan, ...(plan.Plans ?? []).flatMap(walk)];

type CapturedPlan = { queryText: string; plan: Plan; nodes: Plan[]; durationMs: number };

/** auto_explain output (ANALYZE, BUFFERS, FORMAT JSON) of the RPC's inner statement. */
async function explainRpc(db: PGlite, claimsJson: string, workspace: string, query: string) {
  await db.exec(`LOAD 'auto_explain';
    SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on;
    SET auto_explain.log_buffers = on; SET auto_explain.log_timing = on;
    SET auto_explain.log_format = 'json'; SET auto_explain.log_nested_statements = on;
    SET client_min_messages = log;`);
  const messages: string[] = [];
  try {
    const rows = await search(db, claimsJson, workspace, query, (message) => messages.push(message));
    const captured: CapturedPlan[] = messages
      .filter((message) => message.includes("\"Query Text\""))
      .map((message) => {
        const duration = Number(/duration: ([\d.]+) ms/.exec(message)?.[1]);
        const json = JSON.parse(message.slice(message.indexOf("{"))) as { "Query Text": string; Plan: Plan };
        return { queryText: json["Query Text"], plan: json.Plan, nodes: walk(json.Plan), durationMs: duration };
      });
    const squash = (sql: string) => sql.replace(/\s+/g, " ").trim();
    const inner = captured.find((entry) => squash(entry.queryText) === squash(SESSION_DIRECTORY_SEARCH_SQL));
    if (!inner) throw new Error("auto_explain did not capture the RPC's search statement");
    return { rows, inner };
  } finally {
    await db.exec("SET auto_explain.log_min_duration = -1; SET client_min_messages = notice;");
  }
}

const sum = (nodes: Plan[], key: string) => nodes.reduce((total, node) => total + Number(node[key] ?? 0), 0);

describe("search RPC query plan as the Worker identity", () => {
  // 30,000 rows: a large tenant (org-0/ws-0, 22,500 rows) beside 7,500 rows of six
  // other tenants. Two in every 1,000 summaries hold the selective needle; every
  // summary holds the common term.
  async function populated(): Promise<PGlite> {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title,
         short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
         short_summary_source, short_summary_generated_at)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-0' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-0' END, 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             'Session ' || n,
             CASE WHEN n % 1000 IN (0, 1) THEN 'Rebuilt the quokka ledger reconciler, session ' || n
                  ELSE 'Refactored module ' || (n % 997) || ' and updated tests for case ' || n END,
             'relayhistory-scrub', 1, 'bob-costas/v1', now()
        FROM generate_series(1, 30000) AS n;
      UPDATE sessions.session_directory
         SET search_text = sessions.session_directory_search_text(
               title, task_title, agent_name, project_id, git_repo, git_branch, model, short_summary);
      ANALYZE sessions.session_directory;
    `);
    return db;
  }

  test("a selective term uses the tenant-scoped trigram index within the gates", async () => {
    const db = await populated();
    const { rows, inner } = await explainRpc(db, claims("org-0", ["ws-0"]), "ws-0", "quokka ledger");
    console.info(`[G6 plan: selective] ${JSON.stringify(inner.plan)}`);
    expect(rows).toHaveLength(30);
    expect(rows.every((hit) => hit.org_id === "org-0" && hit.workspace_id === "ws-0")).toBe(true);
    expect(inner.nodes.some((node) => node["Index Name"] === "session_directory_tenant_search_trgm_idx")).toBe(true);
    expect(inner.nodes.some((node) => node["Node Type"] === "Seq Scan")).toBe(false);
    // Amplification: heap rows examined (kept + removed by recheck/filter) per row returned.
    const heap = inner.nodes.find((node) => node["Node Type"] === "Bitmap Heap Scan")!;
    const examined = Number(heap["Actual Rows"]) + Number(heap["Rows Removed by Index Recheck"] ?? 0) +
      Number(heap["Rows Removed by Filter"] ?? 0);
    expect(examined / rows.length).toBeLessThanOrEqual(2);
    // Indicative PGlite bounds, not production SLOs.
    expect(sum(inner.nodes.slice(0, 1), "Shared Hit Blocks") + sum(inner.nodes.slice(0, 1), "Shared Read Blocks"))
      .toBeLessThanOrEqual(500);
    expect(inner.durationMs).toBeLessThan(1000);
  });

  test("a common term is capped at 51 rows without a seq scan and within the gates", async () => {
    const db = await populated();
    const { rows, inner } = await explainRpc(db, claims("org-0", ["ws-0"]), "ws-0", "Refactored module");
    console.info(`[G6 plan: common] ${JSON.stringify(inner.plan)}`);
    expect(rows).toHaveLength(SESSION_DIRECTORY_SEARCH_RPC.limit);
    expect(inner.nodes.some((node) => node["Node Type"] === "Seq Scan")).toBe(false);
    const scan = inner.nodes.find((node) => typeof node["Index Name"] === "string")!;
    expect([
      "session_directory_tenant_search_trgm_idx",
      "session_directory_workspace_recent_idx",
    ]).toContain(scan["Index Name"] as string);
    const examined = sum(inner.nodes.filter((node) => /Scan$/.test(String(node["Node Type"])) && node["Node Type"] !== "Bitmap Index Scan"), "Actual Rows") +
      sum(inner.nodes, "Rows Removed by Filter") + sum(inner.nodes, "Rows Removed by Index Recheck");
    expect(examined / rows.length).toBeLessThanOrEqual(100);
    expect(sum(inner.nodes.slice(0, 1), "Shared Hit Blocks") + sum(inner.nodes.slice(0, 1), "Shared Read Blocks"))
      .toBeLessThanOrEqual(2000);
    expect(inner.durationMs).toBeLessThan(2000);
  });
});

describe("short_summary deletion propagation", () => {
  const pointer = (db: PGlite, machine: string) => db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        storage_provider, bucket, manifest_key, trace_id)
     VALUES ('org-a', 'ws-a', 'codex', 's-1', $1, 'u1', now(), now(), 's3', 'bucket', 'key/' || $1,
             '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18')`,
    [machine],
  );
  const hoopDb = (db: PGlite) => ({
    query: async (text: string, params?: unknown[]) => ({ rows: (await db.query(text, params)).rows }),
  }) as unknown as HoopDb;

  test("the indexer's pointer-tombstone path clears the summary, then tombstones the row", async () => {
    const db = await database();
    await insertRow(db);
    await pointer(db, "m1");
    await pointer(db, "m2");
    await summarize(db, KEY, "heron migration notes");
    const index = hoopDbSessionIndex({
      db: hoopDb(db), reader: {} as never, queue: {} as never, current: () => undefined,
    });
    const deleted = (machine: string) => index.upsertRawPointer(
      { deleted: true, workspaceId: "ws-a", harness: "codex", sessionId: "s-1", machineId: machine } as never,
      { orgId: "org-a" } as never,
    );

    // One machine's manifest deleted: the summary goes at once, the row stays live.
    await deleted("m1");
    let current = await row(db);
    expect(current?.deleted_at).toBeNull();
    expect(current?.short_summary).toBeNull();
    expect(current?.short_summary_scrub_version).toBeNull();
    expect(current?.search_text).toBe("Fix flaky queue test Helm proj-1");
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "heron")).toEqual([]);

    // The last machine deleted: the logical row is tombstoned and holds no text.
    await deleted("m2");
    current = await row(db);
    expect(current?.deleted_at).not.toBeNull();
    expect(current?.search_text).toBe("");
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "flaky")).toEqual([]);
    // The writer never puts a summary back on a tombstone.
    expect((await summarize(db, KEY, "heron again", new Date(AT.getTime() + 1000))).written).toBe(false);
  });

  test("the trigger backstop clears a summary when any writer sets deleted_at directly", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "heron migration notes");
    await db.query("UPDATE sessions.session_directory SET deleted_at = now() WHERE session_id = 's-1'");
    const tombstone = await row(db);
    for (const column of [
      "short_summary", "short_summary_scrub_ruleset", "short_summary_scrub_version",
      "short_summary_source", "short_summary_generated_at",
    ]) expect(tombstone?.[column]).toBeNull();
    expect(tombstone?.search_text).toBe("");
    // A direct insert of a tombstone that carries a summary is also cleared.
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, deleted_at,
          short_summary, short_summary_scrub_ruleset, short_summary_scrub_version,
          short_summary_source, short_summary_generated_at, search_text)
       VALUES ('org-a', 'ws-a', 'codex', 's-2', now(), now(), now(),
               'heron', 'relayhistory-scrub', 1, 'bob-costas/v1', now(), 'heron')`,
    );
    const leaks = await db.query(
      "SELECT 1 FROM sessions.session_directory WHERE short_summary ILIKE '%heron%' OR search_text ILIKE '%heron%'",
    );
    expect(leaks.rows).toEqual([]);
  });

  test("erasure deletes the row, the summary and its memberships", async () => {
    const db = await database();
    for (const [workspace, session] of [["ws-a", "s-1"], ["ws-a", "s-2"], ["ws-keep", "s-3"]]) {
      await insertRow(db, "org-a", workspace!, session!);
      await summarize(db, { orgId: "org-a", workspaceId: workspace!, harness: "codex", sessionId: session! },
        `ibis summary ${session}`);
    }
    await db.exec(`INSERT INTO sessions.session_directory_users VALUES ('org-a', 'ws-a', 'codex', 's-1', 'u1', now())`);
    expect(await eraseSessionDirectory(db, KEY)).toBe(1);
    expect(await row(db, "s-1")).toBeUndefined();
    expect((await db.query("SELECT 1 FROM sessions.session_directory_users")).rows).toEqual([]);
    expect(await eraseSessionDirectory(db, { orgId: "org-a", workspaceId: "ws-a" })).toBe(1);
    const left = await db.query<{ session_id: string }>(
      "SELECT session_id FROM sessions.session_directory WHERE short_summary ILIKE '%ibis%' OR search_text ILIKE '%ibis%'",
    );
    expect(left.rows).toEqual([{ session_id: "s-3" }]);
  });
});

describe("short_summary rollback", () => {
  test("down removes every summary column, copy, the RPC and the role, and up applies again", async () => {
    const db = await database();
    await insertRow(db);
    await summarize(db, KEY, "pelican rollback check");
    await db.exec(DOWN);
    const columns = await db.query(
      `SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'sessions' AND table_name = 'session_directory' AND column_name LIKE 'short_summary%'`,
    );
    expect(columns.rows).toEqual([]);
    const objects = await db.query(
      `SELECT 1 FROM pg_class WHERE relname = 'session_directory_tenant_search_trgm_idx'
       UNION ALL SELECT 1 FROM pg_proc WHERE proname IN
         ('session_directory_search_text', 'session_directory_tombstone_clears_summary', 'search_session_directory')
       UNION ALL SELECT 1 FROM pg_trigger WHERE tgname = 'session_directory_tombstone_clears_summary'
       UNION ALL SELECT 1 FROM pg_roles WHERE rolname = $1`,
      [WORKER],
    );
    expect(objects.rows).toEqual([]);
    expect((await db.query("SELECT 1 FROM pg_class WHERE relname = 'session_directory_metadata_trgm_idx'")).rows)
      .toHaveLength(1);
    expect((await row(db))?.search_text).toBe("Fix flaky queue test Helm proj-1");

    await db.exec(UP);
    await summarize(db, KEY, "pelican again");
    expect((await row(db))?.short_summary).toBe("pelican again");
    expect(await search(db, claims("org-a", ["ws-a"]), "ws-a", "pelican")).toHaveLength(1);
  });
});
