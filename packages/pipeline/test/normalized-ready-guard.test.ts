/**
 * A-prime: the guard on normalized-ready rows, attacked AS the shared login's shape.
 * `shared_login` is a non-owner with service_role's grants plus BYPASSRLS, which is
 * what a workload's HOOPDB_URL is expected to be. Every forge and delete path must be
 * refused, while lease fields and the legitimate flow keep working.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  InMemoryPipelineEventSink,
  noopErrorReporter,
  normalizedReadyHandler,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresNormalizedCountStore,
  replayDeadLetters,
  type ConnectSql,
  type SqlExecutor,
  type StageQueueDeps,
} from "../src/lib/pipeline/index.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const TRACE = "0192f000-0000-7000-8000-000000000001";
const LEASE = "0192f000-0000-7000-8000-00000000000a";
const MANIFEST_KEY = "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/manifest.json";
const WORK_KEY = `${MANIFEST_KEY}@etag-1`;
const CONTENT = "a".repeat(64);
const CTX = { traceId: TRACE, orgId: "org_a", workspaceId: "ws_a", machineId: "m-1", sessionId: "s-1", harness: "claude" };
const payload = {
  v: 1, org_id: "org_a", workspace_id: "ws_a", harness: "claude", session_id: "s-1", machine_id: "m-1",
  manifest_key: MANIFEST_KEY, manifest_revision: 1, work_key: WORK_KEY, trace_id: TRACE,
  source_content_sha256: CONTENT, normalized_version: 2, output_key: "k", output_sha256: "b".repeat(64),
  output_bytes: 1, convergence_event_count: 42, unprojected_records: 0, output_reused: false, output_divergent: false,
};
const message = { v: 1, stage: "normalized-ready", ctx: CTX, workKey: WORK_KEY, revision: 1, payload };

let db: PGlite;
const sql = () => db as unknown as SqlExecutor;

async function as<T>(role: string, run: () => Promise<T>) {
  await db.exec(`SET ROLE ${role}`);
  try {
    return await run();
  } finally {
    await db.exec("RESET ROLE");
  }
}
const attacker = (text: string, params: unknown[] = []) => as("shared_login", () => db.query(text, params));
const refused = (text: string, params: unknown[] = [], code = "CN005") =>
  expect(attacker(text, params)).rejects.toMatchObject({ code });

/** As the superuser test setup, standing in for the narrow enqueue function. */
async function job(columns: { lease?: string | null; visible?: string } = {}) {
  const { rows } = await db.query<{ id: number }>(
    `INSERT INTO sessions.pipeline_jobs
       (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message, lease_id, visible_at)
     VALUES ('normalized-ready', $1, $2, 'org_a', 'ws_a', 's-1', 'm-1', $3::jsonb, $4, now() + $5::interval)
     RETURNING id`,
    [WORK_KEY, TRACE, JSON.stringify(message), columns.lease ?? null, columns.visible ?? "0 seconds"],
  );
  return rows[0]!.id;
}
const index = (revision: number, content = CONTENT) =>
  db.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)", [
    JSON.stringify([{
      org_id: "org_a", workspace_id: "ws_a", harness: "claude", session_id: "s-1", machine_id: "m-1",
      user_id: "u", created_at: "2026-10-30T00:00:00Z", last_activity_at: "2026-10-30T00:00:00Z",
      event_count: 7, storage_provider: "s3", bucket: "b", manifest_key: MANIFEST_KEY,
      content_sha256: `\\x${content}`, raw_bytes: 1, chunk_count: 1, raw_format: "jsonl", format_version: 1,
      revision, trace_id: TRACE, scrubbed: true, deleted_at: null,
    }]),
  ]);
const letter = (state = "dead") =>
  db.query(
    `INSERT INTO sessions.pipeline_dead_letters (id, stage, trace_id, org_id, workspace_id, session_id, machine_id, message, state)
     VALUES ('l1', 'normalized-ready', $1, 'org_a', 'ws_a', 's-1', 'm-1', $2::jsonb, $3)`,
    [TRACE, JSON.stringify(message), state],
  );
const row = async () =>
  (await db.query<{ event_count: number; event_count_revision: number | null; revision: number }>(
    "SELECT event_count, event_count_revision, revision FROM sessions.session_index")).rows[0];
const jobs = async () =>
  (await db.query<{ stage: string; message: Record<string, any> }>("SELECT stage, message FROM sessions.pipeline_jobs ORDER BY id")).rows;
const letters = async () =>
  (await db.query<{ stage: string; state: string; dead_count: number }>("SELECT stage, state, dead_count FROM sessions.pipeline_dead_letters")).rows;

beforeAll(async () => {
  db = new PGlite();
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  await db.exec("CREATE ROLE shared_login LOGIN INHERIT BYPASSRLS IN ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  vi.spyOn(console, "info").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
  await db.exec(`RESET ROLE; TRUNCATE sessions.session_index CASCADE;
    TRUNCATE sessions.pipeline_jobs, sessions.pipeline_dead_letters`);
});

describe("forging a job, as the shared login", () => {
  const insert = `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message, lease_id, visible_at)
    VALUES ($1, 'k@e', '${TRACE}', 'org_a', 'ws_a', $2::jsonb, '${LEASE}', now() + interval '300 seconds')`;

  it("can't insert one", async () => {
    await refused(insert, ["normalized-ready", JSON.stringify(message)]);
    expect(await jobs()).toEqual([]);
  });

  it("can turn no other job into one", async () => {
    await attacker(insert, ["session-indexed", JSON.stringify(message)]);
    await refused("UPDATE sessions.pipeline_jobs SET stage = 'normalized-ready'");
    expect((await jobs())[0]?.stage).toBe("session-indexed");
  });

  it.each([["a queued job", {}], ["a leased job", { lease: LEASE, visible: "300 seconds" }]])("can't rewrite %s", async (_, columns) => {
    await job(columns);
    for (const set of [
      `message = message || '{"revision": 9}'::jsonb`,
      `message = jsonb_set(message, '{payload,convergence_event_count}', '999')`,
      "work_key = 'other@etag'",
      "org_id = 'org_b'",
      "workspace_id = 'ws_b'",
      "session_id = 's-2'",
      "machine_id = 'm-2'",
      "trace_id = gen_random_uuid()",
      "stage = 'session-indexed'",
    ])
      await refused(`UPDATE sessions.pipeline_jobs SET ${set}`);
    expect((await jobs())[0]?.message.payload.convergence_event_count).toBe(42);
  });

  it.each([["a queued job", {}], ["a leased job", { lease: LEASE, visible: "300 seconds" }]])("can't delete %s", async (_, columns) => {
    await job(columns);
    await refused("DELETE FROM sessions.pipeline_jobs");
    expect(await jobs()).toHaveLength(1);
  });

  it("can't truncate while one exists", async () => {
    await job();
    await refused("TRUNCATE sessions.pipeline_jobs");
    expect(await jobs()).toHaveLength(1);
  });
});

describe("rewriting the index, as the shared login", () => {
  beforeEach(() => index(1));

  it.each([
    ["revision", "revision = 9"],
    ["content_sha256", "content_sha256 = '\\x00'::bytea"],
    ["a provenance column", "event_count_revision = 1"],
  ])("can't update %s", async (_, set) => {
    await refused(`UPDATE sessions.session_index SET ${set}`, [], "42501");
  });

  it("can't insert a row with them", async () => {
    await refused(
      `INSERT INTO sessions.session_index (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at, revision)
       VALUES ('o', 'w', 'claude', 's', 'm', 'u', now(), now(), 5)`,
      [],
      "42501",
    );
  });

  it("can't delete or truncate", async () => {
    await refused("DELETE FROM sessions.session_index", [], "42501");
    await refused("TRUNCATE sessions.session_index", [], "42501");
    expect((await row())?.revision).toBe(1);
  });

  it("a real upsert to a new revision can't make a count authoritative", async () => {
    await as("shared_login", () => index(2, "c".repeat(64)));
    expect(await row()).toMatchObject({ revision: 2, event_count_revision: null });
  });
});

describe("dead letters, as the shared login", () => {
  const forged = JSON.stringify({ ...message, payload: { ...payload, convergence_event_count: 999 } });

  it("can't forge one", async () => {
    await refused(
      `INSERT INTO sessions.pipeline_dead_letters (id, stage, message) VALUES ('x', 'normalized-ready', $1::jsonb)`,
      [forged],
    );
  });

  it("can't turn another letter into one", async () => {
    await db.exec(`INSERT INTO sessions.pipeline_dead_letters (id, stage, message) VALUES ('o', 'session-indexed', '{}')`);
    await refused("UPDATE sessions.pipeline_dead_letters SET stage = 'normalized-ready'");
  });

  it.each([
    ["message", `message = '{}'::jsonb`],
    ["state", "state = 'resolved'"],
    ["identity", "org_id = 'org_b'"],
  ])("can't rewrite a real letter's %s", async (_, set) => {
    await letter();
    await refused(`UPDATE sessions.pipeline_dead_letters SET ${set}`);
  });

  it("can't delete a dead letter or truncate, while retention's resolved sweep still works", async () => {
    await letter();
    await refused("DELETE FROM sessions.pipeline_dead_letters");
    await refused("TRUNCATE sessions.pipeline_dead_letters");
    await db.exec("TRUNCATE sessions.pipeline_dead_letters");
    await letter("resolved");
    await attacker("DELETE FROM sessions.pipeline_dead_letters");
    expect(await letters()).toEqual([]);
  });

  it("a generic replay can't re-send one", async () => {
    await refused("INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message) VALUES ('normalized-ready', 'k', $1, 'o', 'w', $2::jsonb)", [TRACE, forged]);
  });
});

describe("the guard itself, as the shared login", () => {
  it.each([
    ["drop a trigger", "DROP TRIGGER pipeline_jobs_normalized_ready_guard ON sessions.pipeline_jobs"],
    ["disable a trigger", "ALTER TABLE sessions.pipeline_jobs DISABLE TRIGGER pipeline_jobs_normalized_ready_guard"],
    ["replace the guard", "CREATE OR REPLACE FUNCTION sessions.normalized_ready_guard() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'"],
    ["drop the guard", "DROP FUNCTION sessions.normalized_ready_guard() CASCADE"],
    ["take the guard's owner", "ALTER FUNCTION sessions.normalized_ready_guard() OWNER TO shared_login"],
    ["bypass triggers", "SET session_replication_role = replica"],
    ["call the internal apply", `SELECT sessions.apply_normalized_event_count('org_a','ws_a','claude','s-1','m-1',1,2,'k@e',''::bytea,''::bytea,1)`],
  ])("can't %s", async (_, text) => {
    await expect(attacker(text)).rejects.toMatchObject({ code: expect.stringMatching(/^(42501|42883)$/) });
  });

  // SET ROLE is checked against the session user, a superuser here, so it can't be
  // attempted from this session. Membership is what it would check for a real login.
  it("isn't a member of the writer, directly or through service_role", async () => {
    const { rows } = await db.query(
      `SELECT pg_has_role('shared_login', 'normalized_ready_writer', 'MEMBER') AS shared,
              pg_has_role('service_role', 'normalized_ready_writer', 'MEMBER') AS service`,
    );
    expect(rows).toEqual([{ shared: false, service: false }]);
  });

  it("the guard and the narrow functions belong to the isolated role, which nothing is a member of", async () => {
    const owners = await db.query<{ proname: string; owner: string }>(
      `SELECT p.proname, pg_get_userbyid(p.proowner) AS owner FROM pg_proc p
        WHERE p.proname IN ('normalized_ready_guard', 'record_normalized_event_count', 'apply_normalized_event_count',
                            'normalized_ready_quarantine', 'normalized_ready_replay')`,
    );
    expect(owners.rows).toHaveLength(5);
    for (const { owner } of owners.rows) expect(owner).toBe("normalized_ready_writer");
    const members = await db.query(
      `SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = 'normalized_ready_writer'`,
    );
    expect(members.rows).toEqual([]);
    expect((await db.query("SELECT rolcanlogin FROM pg_roles WHERE rolname = 'normalized_ready_writer'")).rows).toEqual([{ rolcanlogin: false }]);
    const guard = await db.query<{ prosecdef: boolean; proconfig: string[] }>(
      "SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'normalized_ready_guard'",
    );
    expect(guard.rows[0]?.prosecdef).toBe(false);
    expect(guard.rows[0]?.proconfig.join()).toContain("search_path=pg_catalog");
  });
});

describe("the legitimate flow, as the shared login", () => {
  const connect: ConnectSql = async () => {
    await db.exec("SET ROLE shared_login");
    return { query: (text, params) => sql().query(text, params), release: () => void db.exec("RESET ROLE") };
  };
  /** The poller and letter store run as the login too. */
  const login: SqlExecutor = {
    query: (text, params) => as("shared_login", () => sql().query(text, params)),
  };
  const deps = (): StageQueueDeps => ({
    sink: new InMemoryPipelineEventSink(),
    deadLetters: postgresDeadLetterStore(login),
    reporter: noopErrorReporter,
    logger: { environment: "test" },
    handlers: { "normalized-ready": normalizedReadyHandler(postgresNormalizedCountStore(connect)) },
  });
  const poll = (d = deps()) => pollPostgresStage({ sql: login, stage: "normalized-ready", deps: d });
  const dueNow = () => db.exec("UPDATE sessions.pipeline_jobs SET visible_at = now()");

  it("can write lease fields", async () => {
    await job();
    await attacker(`UPDATE sessions.pipeline_jobs SET lease_id = '${LEASE}', visible_at = now() + interval '1 minute', attempts = 1, dlq = true`);
    expect(await jobs()).toHaveLength(1);
  });

  it("can still tombstone, and write ordinary columns", async () => {
    await index(1);
    await attacker("UPDATE sessions.session_index SET deleted_at = now(), indexed_at = now()");
    await attacker("UPDATE sessions.session_index SET title = 't'");
  });

  it("other stages keep working untouched", async () => {
    await attacker(
      `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, message) VALUES ('session-indexed', 'k', $1, 'o', 'w', '{}')`,
      [TRACE],
    );
    await attacker("DELETE FROM sessions.pipeline_jobs");
    expect(await jobs()).toEqual([]);
  });

  it("applies a real count through the normal poller, and acks", async () => {
    await as("shared_login", () => index(1));
    await job();
    expect(await poll()).toMatchObject({ claimed: 1, acked: 1 });
    expect(await row()).toMatchObject({ event_count: 42, event_count_revision: 1 });
    expect(await jobs()).toEqual([]);
  });

  it("quarantines through the narrow function, which removes the job", async () => {
    await index(1, "c".repeat(64));
    await job();
    await poll();
    expect(await letters()).toEqual([{ stage: "normalized-ready", state: "dead", dead_count: 1 }]);
    expect(await jobs()).toEqual([]);
    expect((await row())?.event_count_revision).toBeNull();
  });

  it("replay re-runs file-normalize and never resends the stored count", async () => {
    await index(1, "c".repeat(64));
    await job();
    await poll();
    const d = deps();
    const result = await replayDeadLetters(
      { deadLetters: d.deadLetters, sink: d.sink, send: async () => { throw new Error("must not send"); } },
      { dryRun: false },
    );
    expect(result.sent).toHaveLength(1);
    const queued = await jobs();
    expect(queued.map((q) => q.stage)).toEqual(["file-normalize"]);
    expect(JSON.stringify(queued[0]?.message)).not.toContain("convergence_event_count");
    expect(queued[0]?.message).toMatchObject({ stage: "file-normalize", workKey: WORK_KEY, revision: 1, payload: { manifestKey: MANIFEST_KEY }, ctx: CTX });
    expect(await letters()).toMatchObject([{ state: "resolved" }]);
  });

  it("a second replay of one letter sends nothing", async () => {
    await letter();
    const d = deps();
    const replay = () =>
      replayDeadLetters({ deadLetters: d.deadLetters, sink: d.sink, send: async () => {} }, { dryRun: false, force: true });
    expect((await replay()).sent).toHaveLength(1);
    expect((await replay()).sent).toHaveLength(0);
    expect(await jobs()).toHaveLength(1);
  });

  it("a job that dies again after replay returns to dead, with its count", async () => {
    await index(1, "c".repeat(64));
    await job();
    await poll();
    await db.exec("UPDATE sessions.pipeline_dead_letters SET state = 'resolved'");
    await job();
    await dueNow();
    await poll();
    expect(await letters()).toMatchObject([{ state: "dead", dead_count: 2 }]);
  });
});
