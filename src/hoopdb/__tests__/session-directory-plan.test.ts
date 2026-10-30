import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  AGENT_PREFIX_BENCHMARK_GATE,
  SEARCH_INDEX_CATALOG_SQL,
  SESSION_DIRECTORY_QUERIES,
  agentPrefixPattern,
  auditSearchIndexes,
  type SearchIndexCatalogRow,
} from "../search-index-contract.ts";

const migration = readFileSync(
  join(import.meta.dir, "../../../supabase/migrations/20261030010000_session_directory.sql"),
  "utf8",
);
const shim = readFileSync(join(import.meta.dir, "../../../supabase/tests/supabase_shim.sql"), "utf8");
const open: PGlite[] = [];

afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { pg_trgm } });
  open.push(db);
  await db.exec(shim);
  await db.exec(`
    CREATE SCHEMA sessions;
    CREATE FUNCTION sessions.jwt_org_id() RETURNS text LANGUAGE sql STABLE AS $$ SELECT NULL::text $$;
    CREATE FUNCTION sessions.jwt_workspace_ids() RETURNS text[] LANGUAGE sql STABLE AS $$ SELECT '{}'::text[] $$;
    CREATE FUNCTION sessions.member_of(text, text[]) RETURNS boolean
      LANGUAGE sql IMMUTABLE AS $$ SELECT $1 = ANY ($2) $$;
  `);
  await db.exec(migration);
  return db;
}

type Plan = {
  [key: string]: unknown;
  Plans?: Plan[];
  "Node Type"?: string;
  "Index Name"?: string;
  "Actual Rows"?: number;
  "Rows Removed by Filter"?: number;
};

function walk(plan: Plan): Plan[] {
  return [plan, ...(plan.Plans ?? []).flatMap(walk)];
}

async function explain(db: PGlite, sql: string, params: unknown[]): Promise<Plan> {
  const result = await db.query<Record<string, unknown>>(
    `EXPLAIN (ANALYZE, FORMAT JSON) ${sql}`,
    params,
  );
  const envelope = result.rows[0]?.["QUERY PLAN"] as Array<{ Plan: Plan }> | undefined;
  if (!envelope?.[0]?.Plan) throw new Error("PostgreSQL returned no JSON plan");
  return envelope[0].Plan;
}

describe("session_directory migration", () => {
  test("matches the executable catalog audit and starts rollout incomplete", async () => {
    const db = await database();
    const rows = await db.query<SearchIndexCatalogRow>(SEARCH_INDEX_CATALOG_SQL);
    const report = auditSearchIndexes(rows.rows, { directoryProjectionComplete: true });
    expect(report.indexes.filter((index) => !index.ready)).toEqual([]);
    const rollout = await db.query<{ completed_at: Date | null }>(
      "SELECT completed_at FROM sessions.session_directory_rollout",
    );
    expect(rollout.rows).toEqual([{ completed_at: null }]);
  });

  test("enforces one logical row with normalized user and machine membership", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized, machine_ids, user_ids, search_text)
      VALUES
        ('o', 'w', 'codex', 's', '2026-10-30T00:00:00Z', '2026-10-30T01:00:00Z',
         'Helm-Chief', 'helm-chief', ARRAY['m1', 'm2'], ARRAY['u1'], 'helm-chief repo model');
      INSERT INTO sessions.session_directory_users VALUES ('o', 'w', 'codex', 's', 'u1', '2026-10-30T01:00:00Z');
      INSERT INTO sessions.session_directory_machines VALUES
        ('o', 'w', 'codex', 's', 'm1', '2026-10-30T01:00:00Z'),
        ('o', 'w', 'codex', 's', 'm2', '2026-10-30T01:00:00Z');
    `);
    expect((await db.query("SELECT * FROM sessions.session_directory")).rows).toHaveLength(1);
    expect((await db.query("SELECT * FROM sessions.session_directory_machines")).rows).toHaveLength(2);
    await expect(db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      VALUES ('o', 'w', 'codex', 'bad', now(), now(), 'Mixed', 'MIXED')
    `)).rejects.toThrow();
  });
});

describe("session_directory query plans", () => {
  test("uses the declared ordered indexes for list and membership filters", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         project_id, state, search_text)
      SELECT 'o', CASE WHEN n % 2 = 0 THEN 'w' ELSE 'other-w' END,
             CASE WHEN n % 4 < 2 THEN 'codex' ELSE 'claude' END, 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n % 10 = 0 THEN NULL ELSE 'project-' || (n % 100) END,
             CASE WHEN n % 3 = 0 THEN 'done' ELSE 'active' END,
             CASE WHEN n % 100 = 0 THEN 'needle project model' ELSE 'ordinary metadata' END
        FROM generate_series(1, 10000) AS n;
      INSERT INTO sessions.session_directory_users
      SELECT org_id, workspace_id, harness, session_id, 'u-' || (substring(session_id from 3)::integer % 100), last_activity_at
        FROM sessions.session_directory;
      INSERT INTO sessions.session_directory_machines
      SELECT org_id, workspace_id, harness, session_id, 'm-' || (substring(session_id from 3)::integer % 100), last_activity_at
        FROM sessions.session_directory;
      ANALYZE sessions.session_directory;
      ANALYZE sessions.session_directory_users;
      ANALYZE sessions.session_directory_machines;
    `);

    const cases: Array<[string, string, unknown[], string]> = [
      ["workspace", SESSION_DIRECTORY_QUERIES.workspaceRecent, ["o", "w", 50], "session_directory_workspace_recent_idx"],
      ["harness", SESSION_DIRECTORY_QUERIES.harnessRecent, ["o", "w", "codex", 50], "session_directory_harness_recent_idx"],
      ["project", SESSION_DIRECTORY_QUERIES.projectRecent, ["o", "w", "project-12", 50], "session_directory_project_recent_idx"],
      ["no project", SESSION_DIRECTORY_QUERIES.withoutProjectRecent, ["o", "w", 50], "session_directory_no_project_recent_idx"],
      ["created", SESSION_DIRECTORY_QUERIES.createdRange, ["o", "w", "2026-10-01", "2026-11-01", 50], "session_directory_created_recent_idx"],
      ["state", SESSION_DIRECTORY_QUERIES.stateRecent, ["o", "w", "done", 50], "session_directory_state_recent_idx"],
      ["user", SESSION_DIRECTORY_QUERIES.userRecent, ["o", "w", "u-12", 50], "session_directory_users_recent_idx"],
      ["machine", SESSION_DIRECTORY_QUERIES.machineRecent, ["o", "w", "m-12", 50], "session_directory_machines_recent_idx"],
    ];
    for (const [name, sql, params, expectedIndex] of cases) {
      const nodes = walk(await explain(db, sql, params));
      expect(nodes.some((node) => node["Index Name"] === expectedIndex), name).toBe(true);
    }
  });

  test("uses the ordered agent index for exact and useful-prefix lookups", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n < 5000 THEN 'age-' || n ELSE 'helm-' || n END,
             CASE WHEN n < 5000 THEN 'age-' || n ELSE 'helm-' || n END
        FROM generate_series(1, 10000) AS n;
      ANALYZE sessions.session_directory;
    `);

    const exact = walk(await explain(db, SESSION_DIRECTORY_QUERIES.agentExact, ["o", "w", "helm-9999", 50]));
    expect(exact.some((node) => node["Index Name"] === "session_directory_agent_recent_idx")).toBe(true);

    const prefix = walk(await explain(db, SESSION_DIRECTORY_QUERIES.agentPrefix, [
      "o", "w", agentPrefixPattern("helm-99"), 50,
    ]));
    expect(prefix.some((node) => node["Index Name"] === "session_directory_agent_recent_idx")).toBe(true);
  });

  test("keeps a deliberately broad three-character prefix within the row-amplification gate", async () => {
    const db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
         agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'other-' || n END,
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'other-' || n END
        FROM generate_series(1, 10000) AS n;
      ANALYZE sessions.session_directory;
    `);
    const nodes = walk(await explain(db, SESSION_DIRECTORY_QUERIES.agentPrefix, [
      "o", "w", agentPrefixPattern("age"), AGENT_PREFIX_BENCHMARK_GATE.pageSize,
    ]));
    const indexNode = nodes.find((node) =>
      node["Index Name"] === "session_directory_agent_recent_idx" ||
      node["Index Name"] === "session_directory_workspace_recent_idx" ||
      node["Index Name"] === "session_directory_org_recent_idx"
    );
    expect(indexNode).toBeDefined();
    const returned = Number(nodes[0]?.["Actual Rows"] ?? 0);
    const examined = Number(indexNode?.["Actual Rows"] ?? Number.POSITIVE_INFINITY) +
      Number(indexNode?.["Rows Removed by Filter"] ?? 0);
    expect(returned).toBe(AGENT_PREFIX_BENCHMARK_GATE.pageSize);
    expect(examined / returned).toBeLessThanOrEqual(
      AGENT_PREFIX_BENCHMARK_GATE.maxIndexRowsExaminedPerResult,
    );
  });
});
