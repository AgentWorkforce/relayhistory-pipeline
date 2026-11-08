#!/usr/bin/env bun
/**
 * Real-PostgreSQL RLS and query-plan evidence for the session_directory surface, for
 * activation review. LOCAL THROWAWAY CLUSTERS ONLY: the target must be a loopback
 * host, and a database named `g6_plan_evidence` is dropped and recreated there.
 *
 *   bun scripts/session-directory-plan-evidence.ts postgres://postgres@127.0.0.1:55415/postgres \
 *     postgres://postgres@127.0.0.1:55432/postgres > session-directory-plans.md   # local only; not committed
 *
 * Applies supabase/tests/supabase_shim.sql and every supabase/migrations file, seeds
 * synthetic multi-tenant data, then records, for each server:
 * - the version and the proleakproof rows the design depends on;
 * - FORCE ROW LEVEL SECURITY on session_directory;
 * - plans as `authenticated` under the real RLS policy (direct list queries,
 *   including a direct project_id ILIKE);
 * - plans of the RPCs' inner statements as the Worker (auto_explain).
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { Client } from "pg";
import {
  SESSION_DIRECTORY_QUERIES,
  AgentPrefixRequiresRpcError,
  directAgentPrefix,
} from "../src/hoopdb/search-index-contract.ts";
import { SESSION_BOOTSTRAP_FILES } from "../src/hoopdb/session-bootstrap-chain.ts";

const root = join(import.meta.dir, "..");
const LOOPBACK = new Set(["127.0.0.1", "localhost", "::1", "[::1]"]);
const urls = process.argv.slice(2);
if (urls.length === 0)
  throw new Error(
    "usage: session-directory-plan-evidence.ts <local postgres url>...",
  );
for (const url of urls) {
  const host = new URL(url).hostname;
  if (!LOOPBACK.has(host)) throw new Error(`refusing non-local target ${host}`);
}
for (const v of ["PGHOST", "PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"]) {
  if (process.env[v]) throw new Error(`refusing: unset ${v}`);
}

const WORKER = "session_directory_search_worker";
const out: string[] = [];
const log = (line = "") => out.push(line);

async function evidence(adminUrl: string) {
  const admin = new Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query("DROP DATABASE IF EXISTS g6_plan_evidence");
  await admin.query("CREATE DATABASE g6_plan_evidence");
  await admin.end();
  const u = new URL(adminUrl);
  u.pathname = "/g6_plan_evidence";
  const c = new Client({ connectionString: u.toString() });
  await c.connect();
  const notices: string[] = [];
  c.on("notice", (n: { message?: string }) => {
    if (n.message) notices.push(n.message);
  });
  try {
    // Roles are cluster-wide: clear the ones a previous run created.
    await c
      .query(
        `DO $d$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${WORKER}') THEN
        EXECUTE 'DROP OWNED BY ${WORKER}'; EXECUTE 'DROP ROLE ${WORKER}';
      END IF; END $d$`,
      )
      .catch(() => undefined);
    await c.query(
      readFileSync(join(root, "supabase/tests/supabase_shim.sql"), "utf8"),
    );
    // Single-sourced bootstrap chain: the grant migration FAILs closed
    // without the bare consumer grantee.
    for (const f of SESSION_BOOTSTRAP_FILES)
      await c.query(readFileSync(join(root, f), "utf8"));
    for (const f of readdirSync(join(root, "supabase/migrations")).sort()) {
      await c.query(readFileSync(join(root, "supabase/migrations", f), "utf8"));
    }
    await c.query(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title, agent_name,
         agent_name_normalized, project_id, state, machine_ids, user_ids, search_text)
      SELECT CASE WHEN n % 4 = 0 THEN 'org-' || (n % 7 + 1) ELSE 'org-0' END,
             CASE WHEN n % 4 = 0 THEN 'ws-' || (n % 7 + 1) ELSE 'ws-0' END,
             (ARRAY['codex','claude','opencode'])[n % 3 + 1], 's-' || n,
             '2026-09-01T00:00:00Z'::timestamptz + n * interval '1 minute',
             '2026-09-01T00:00:00Z'::timestamptz + n * interval '1 minute' + interval '30 minutes',
             'Session ' || n,
             'agent-' || (ARRAY['queue','ledger','pelican','harbor','falcon'])[n % 5 + 1] || '-' || (n % 50),
             'agent-' || (ARRAY['queue','ledger','pelican','harbor','falcon'])[n % 5 + 1] || '-' || (n % 50),
             CASE WHEN n % 1000 = 1 THEN 'zebra-crossing-project' ELSE 'project-' || (n % 40) END,
             (ARRAY['active','done','abandoned','unknown'])[n % 4 + 1],
             ARRAY['m-' || (n % 30)], ARRAY['u-' || (n % 60)], ''
        FROM generate_series(1, 40000) AS n;
      UPDATE sessions.session_directory SET search_text = sessions.session_directory_search_text(
        title, task_title, agent_name, project_id, git_repo, git_branch, model, short_summary);`);
    await c.query(`INSERT INTO sessions.session_index
        (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
         storage_provider, bucket, manifest_key, trace_id)
      SELECT 'org-0', 'ws-0', 'codex', 's-' || (n % 5000), 'm-' || (n % 3), 'u-1', now(), now(),
             's3', 'bucket', 'key/' || n, '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18'
        FROM generate_series(1, 15000) AS n
      ON CONFLICT DO NOTHING`);
    await c.query("VACUUM ANALYZE sessions.session_directory");
    await c.query("VACUUM ANALYZE sessions.session_index");

    const version = (await c.query("SHOW server_version")).rows[0]
      .server_version;
    log(`## PostgreSQL ${version}`);
    log();
    log("### proleakproof");
    log();
    log("| function | operator | proleakproof |");
    log("| --- | --- | --- |");
    for (const r of (
      await c.query(`SELECT p.proname, o.oprname, p.proleakproof FROM pg_operator o
        JOIN pg_proc p ON p.oid = o.oprcode
       WHERE o.oprleft = 'text'::regtype AND o.oprright = 'text'::regtype
         AND p.proname IN ('textlike','texticlike','texteq','text_pattern_ge','text_pattern_lt')
       ORDER BY 1`)
    ).rows)
      log(`| \`${r.proname}\` | \`${r.oprname}\` | ${r.proleakproof} |`);
    const force = (
      await c.query(`SELECT relrowsecurity, relforcerowsecurity FROM pg_class
       WHERE oid = 'sessions.session_directory'::regclass`)
    ).rows[0];
    log();
    log(
      `RLS on \`sessions.session_directory\`: enabled=${force.relrowsecurity}, FORCE=${force.relforcerowsecurity}. Without FORCE the owner (the RPCs' definer) is not subject to RLS.`,
    );

    const plan = async (sql: string, args: unknown[]) =>
      (
        await c.query(
          `EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) ${sql}`,
          args,
        )
      ).rows.map((r) => r["QUERY PLAN"] as string);
    const asAuthenticated = async <T>(run: () => Promise<T>) => {
      await c.query("SELECT set_config('request.jwt.claims', $1, false)", [
        JSON.stringify({
          role: "authenticated",
          app_metadata: { org_id: "org-0", workspace_ids: ["ws-0"] },
        }),
      ]);
      await c.query("SET ROLE authenticated");
      try {
        return await run();
      } finally {
        await c.query("RESET ROLE");
      }
    };
    const block = (title: string, lines: string[]) => {
      log();
      log(`### ${title}`);
      log();
      log("```");
      for (const l of lines) log(l);
      log("```");
    };

    for (const [title, sql, args] of [
      [
        "authenticated: direct agent exact",
        SESSION_DIRECTORY_QUERIES.agentExact,
        ["org-0", "ws-0", "agent-ledger-21", 50],
      ],
      [
        "authenticated: direct project_id ILIKE (the surface the contract phase closes)",
        `SELECT d.session_id FROM sessions.session_directory AS d
          WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL AND d.project_id ILIKE $3
          ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT 50`,
        ["org-0", "ws-0", "%zebra-cross%"],
      ],
    ] as const) {
      block(title, await asAuthenticated(() => plan(sql, [...args])));
    }
    const denied = await asAuthenticated(() =>
      c
        .query("SELECT short_summary FROM sessions.session_directory LIMIT 1")
        .then(
          () => "allowed",
          (e: { code?: string }) => e.code ?? "error",
        ),
    );
    log();
    log(`authenticated \`SELECT short_summary\`: ${denied} (42501 expected).`);
    let refused = "not refused";
    try {
      directAgentPrefix("age");
    } catch (e) {
      if (e instanceof AgentPrefixRequiresRpcError) refused = e.code;
    }
    log();
    log(
      `direct agent prefix: ${refused} (typed refusal before any SQL; there is no direct prefix query).`,
    );

    // The RPCs' inner statements, as executed by the Worker, via auto_explain.
    await c.query(`LOAD 'auto_explain'; SET auto_explain.log_min_duration = 0; SET auto_explain.log_analyze = on;
      SET auto_explain.log_buffers = on; SET auto_explain.log_timing = off; SET auto_explain.log_format = 'text';
      SET auto_explain.log_nested_statements = on; SET client_min_messages = log`);
    const asWorker = async (sql: string, args: unknown[], pick: RegExp) => {
      notices.length = 0;
      await c.query("SELECT set_config('request.jwt.claims', $1, false)", [
        JSON.stringify({
          role: WORKER,
          app_metadata: { org_id: "org-0", workspace_ids: ["ws-0"] },
        }),
      ]);
      await c.query(`SET ROLE ${WORKER}`);
      let outcome = "ok";
      try {
        const r = await c.query(sql, args);
        outcome = `ok, ${r.rows.length} rows`;
      } catch (e) {
        const err = e as { code?: string; message?: string };
        outcome = `${err.code}: ${err.message}`;
      } finally {
        await c.query("RESET ROLE");
      }
      // The inner statements' plans, chosen by their Query Text.
      const inner = notices.filter((m) =>
        pick.test(/Query Text: ([^\n]*)/.exec(m)?.[1] ?? ""),
      );
      return [
        `-- result: ${outcome}`,
        ...(inner.length ? inner : ["no inner plan captured"]).flatMap((m) =>
          m.split("\n"),
        ),
      ];
    };
    const PAGE = /^SELECT d\.org_id/;
    for (const [title, sql, args, pick] of [
      [
        "Worker RPC: p_project_contains (tenant project trigram index inside the RPC)",
        "SELECT * FROM sessions.search_session_directory(p_workspace_id => $1, p_project_contains => $2)",
        ["ws-0", "zebra-cross"],
        PAGE,
      ],
      [
        "Worker RPC: worst-case agent prefix 'age' (budget probe, then agent_prefix_too_broad)",
        "SELECT * FROM sessions.search_session_directory(p_workspace_id => $1, p_agent => $2, p_agent_match => 'prefix')",
        ["ws-0", "age"],
        /^SELECT (count|d\.org_id)/,
      ],
      [
        "Worker RPC: selective agent prefix (budget probe, then the page)",
        "SELECT * FROM sessions.search_session_directory(p_workspace_id => $1, p_agent => $2, p_agent_match => 'prefix')",
        ["ws-0", "agent-ledger-11"],
        /^SELECT (count|d\.org_id)/,
      ],
      [
        "Worker RPC: p_query selective",
        "SELECT * FROM sessions.search_session_directory($1, $2)",
        ["ws-0", "zebra-crossing"],
        PAGE,
      ],
      [
        "Worker RPC: pointers",
        "SELECT * FROM sessions.session_directory_pointers($1, 'codex', 's-1')",
        ["ws-0"],
        /^SELECT p\.org_id/,
      ],
    ] as const) {
      block(title, await asWorker(sql, [...args], pick));
    }
    await c.query(
      "SET auto_explain.log_min_duration = -1; SET client_min_messages = notice",
    );
  } finally {
    await c.end();
    const a2 = new Client({ connectionString: adminUrl });
    await a2.connect();
    await a2.query("DROP DATABASE IF EXISTS g6_plan_evidence");
    await a2.end();
  }
}

log("# session_directory RLS and plan evidence");
log();
log(
  "Generated by `scripts/session-directory-plan-evidence.ts` on local throwaway clusters,",
);
log(
  "40,000 synthetic sessions (a 30,000-row tenant `org-0/ws-0`). Not production numbers;",
);
log("plans, operator flags and grants are what matter.");
for (const url of urls) {
  log();
  await evidence(url);
}
process.stdout.write(`${out.join("\n")}\n`);
