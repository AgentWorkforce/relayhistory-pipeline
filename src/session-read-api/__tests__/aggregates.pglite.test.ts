/**
 * The aggregate lane: sessions.session_output and sessions.session_activity over
 * PGlite — claim-bound SECURITY DEFINER RPCs called exactly as the route modules
 * do, through the tenant runner's read-only transaction.
 */
import { describe, expect, test, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { join } from "node:path";
import { readFileSync, readdirSync } from "node:fs";
import { poolTenantRunner } from "../liveness.ts";
import { sessionOutput } from "../session-output.ts";
import { sessionActivity, activityDayCount, activityDates } from "../session-activity.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const WORKER = "session_directory_search_worker";
const read = (p: string) => readFileSync(join(root, p), "utf8");

const open: PGlite[] = [];
afterEach(async () => Promise.all(open.splice(0).map((d) => d.close())));

async function db(): Promise<PGlite> {
  const d = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(d);
  await d.exec(read("supabase/tests/supabase_shim.sql"));
  await d.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await d.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await d.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  // The role lands on an existing schema (as production): migrations up to the
  // aggregate step, the role bootstrap, then the step itself and everything after.
  const migrations = readdirSync(join(root, "supabase/migrations")).sort();
  for (const file of migrations.filter((f) => f < "20261103070000"))
    await d.exec(read(`supabase/migrations/${file}`));
  await d.exec(
    read("supabase/bootstrap/session_read_api_role.sql")
      .split("\n")
      .filter((l) => !l.startsWith("\\"))
      .join("\n"),
  );
  for (const file of migrations.filter((f) => f >= "20261103070000"))
    await d.exec(read(`supabase/migrations/${file}`));
  for (const file of readdirSync(join(root, "src/hoopdb/migrations")).sort())
    await d.exec(read(`src/hoopdb/migrations/${file}`));
  // The relayhistory-applied tables the aggregate RPCs read, at their vendored
  // definitions (convergence_events minus the vector column PGlite lacks;
  // conversation_turns carries the workspace_id the later lanes added).
  await d.exec(`
    CREATE TABLE sessions.convergence_events (
      org_id text NOT NULL, workspace_id text NOT NULL, machine_id text NOT NULL,
      user_id text NOT NULL, source text NOT NULL, session_id text NOT NULL,
      event_id text NOT NULL, kind text NOT NULL, type text NOT NULL,
      ts timestamptz NOT NULL, actor_role text, content text, request_key text,
      delivery_record_id text, tool_name text, record jsonb NOT NULL,
      ingested_at timestamptz NOT NULL DEFAULT now(), task_title text,
      PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id)
    );
    CREATE TABLE sessions.conversation_turns (
      id uuid PRIMARY KEY, org_id text NOT NULL, workspace_id text NOT NULL,
      session_id text NOT NULL, source text NOT NULL, session_owner text NOT NULL, turn_index integer NOT NULL,
      role text NOT NULL, content text NOT NULL, actor_name text NOT NULL,
      actor_role text NOT NULL, metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
      ts timestamptz NOT NULL, UNIQUE (org_id, session_id, turn_index)
    );
    CREATE TABLE sessions.session_commit_links (
      org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL,
      session_id text NOT NULL, commit_sha text NOT NULL, match_method text NOT NULL,
      record_id text NOT NULL, origin_id text NOT NULL, user_id text NOT NULL,
      numstat jsonb, linked_at timestamptz, received_at timestamptz NOT NULL,
      PRIMARY KEY (org_id, workspace_id, source, session_id, commit_sha, match_method)
    );
    CREATE TABLE sessions.session_outcomes (
      org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL,
      session_id text NOT NULL, commit_sha text NOT NULL,
      numstat_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      shipped_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (org_id, source, session_id, commit_sha)
    );
    CREATE TABLE sessions.session_rollups (
      org_id text NOT NULL, session_id text NOT NULL, workspace_id text NOT NULL,
      source text NOT NULL, first_ts timestamptz NOT NULL, last_ts timestamptz NOT NULL
    );
    CREATE TABLE sessions.session_rollup_rollout (
      singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
      completed_at timestamptz
    );`);
  return d;
}

const ev = (day: string, sessionId: string, extra: Record<string, string> = {}) =>
  `INSERT INTO sessions.convergence_events
    (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, actor_role, content, record)
    VALUES ('org-a','ws-a','m-1','u-1','src-x','${sessionId}','e-${Math.random().toString(36).slice(2, 10)}','${extra.kind ?? "session_event"}','${extra.type ?? "assistant"}','${day}T12:00:00Z','${extra.actor_role ?? "assistant"}','${(extra.content ?? "some text").replaceAll("'", "''")}','{}'::jsonb)`;

const run = (d: PGlite) =>
  poolTenantRunner({ connect: async () => ({ query: async (t: string, p?: unknown[]) => (await d.query(t, p)) as { rows: unknown[] }, release: () => {} }) });

describe("session aggregates", () => {
  test("activity counts distinct sessions per UTC day; every window day is present", async () => {
    const d = await db();
    await d.exec(ev("2026-10-30", "s-1"));
    await d.exec(ev("2026-10-30", "s-1", { event_id: "e-dup" }));
    await d.exec(ev("2026-11-01", "s-2"));
    const days = await sessionActivity(run(d), "org-a", "ws-a", new Date("2026-10-29T00:00:00Z"), new Date("2026-11-02T00:00:00Z"));
    expect(days.map((x) => x.date)).toEqual(["2026-10-29", "2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02"]);
    expect(days[1]).toEqual({ date: "2026-10-30", total: 1, bySource: { "src-x": 1 } });
    expect(days[3]).toEqual({ date: "2026-11-01", total: 1, bySource: { "src-x": 1 } });
    expect(days[0]!.total).toBe(0);
  });

  test("output aggregates assistant messages and linked commits", async () => {
    const d = await db();
    await d.exec(ev("2026-10-30", "s-1", { kind: "session_event", actor_role: "assistant", content: "work done" }));
    const out = await sessionOutput(run(d), "org-a", "ws-a", new Date("2026-10-29T00:00:00Z"), new Date("2026-11-02T00:00:00Z"));
    expect(out).toEqual({ agentMessages: 1, commits: 0, linesAdded: 0, linesRemoved: 0, commitsWithoutLineCounts: 0 });
  });

  test("settings bind: a workspace outside the bound scope refuses, 42501", async () => {
    const d = await db();
    await d.query("SELECT set_config('bulls96_read.org_id', 'org-a', false)");
    await d.query("SELECT set_config('bulls96_read.workspace_id', 'ws-a', false)");
    await expect(
      d.query("SELECT * FROM sessions.session_output('ws-other','2026-10-29','2026-11-02')"),
    ).rejects.toThrow();
  });

  test("window helpers match the contract caps", () => {
    expect(activityDayCount(new Date("2026-01-01Z"), new Date("2026-01-01Z"))).toBe(1);
    expect(activityDayCount(new Date("2026-01-02Z"), new Date("2026-01-01Z"))).toBe(0);
    expect(activityDates(new Date("2026-01-30Z"), new Date("2026-02-02Z")).length).toBe(4);
  });
});
