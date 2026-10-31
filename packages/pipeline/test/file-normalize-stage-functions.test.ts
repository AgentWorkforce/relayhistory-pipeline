/**
 * The file-normalize worker's queue access on PGlite, against every migration, as
 * the real `file_normalize_worker`: no table access, and each function fixed to its
 * stage and to the job the caller holds. Semantics match `pollPostgresStage`.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { deadLetterId } from "../src/lib/pipeline/dlq.js";
import { STAGE_REGISTRY } from "../src/lib/pipeline/registry.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const TRACE = "0192f000-0000-7000-8000-000000000001";
const LEASE = "0192f000-0000-7000-8000-00000000000a";
const OTHER_LEASE = "0192f000-0000-7000-8000-00000000000b";
const WORK_KEY = "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/manifest.json@etag-1";

let db: PGlite;

async function asWorker<T>(run: () => Promise<T>) {
  await db.exec("SET ROLE file_normalize_worker");
  try {
    return await run();
  } finally {
    await db.exec("RESET ROLE");
  }
}
const call = <R = Record<string, unknown>>(sql: string, params: unknown[] = []) =>
  asWorker(async () => (await db.query<R>(sql, params)).rows);

const ctx = (org: string) => ({
  traceId: TRACE, orgId: org, workspaceId: "ws_a", harness: "claude", sessionId: "s-1", machineId: "m-1",
});
const message = (org = "org_a", stage = "file-normalize") => ({
  v: 1, stage, ctx: ctx(org), workKey: WORK_KEY, revision: 3, payload: { manifestKey: "k" },
});
async function enqueue(stage = "file-normalize", org = "org_a") {
  await db.query(
    `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message)
     VALUES ($1, $2, $3, $4, 'ws_a', 's-1', 'm-1', $5::jsonb)`,
    [stage, WORK_KEY, TRACE, org, JSON.stringify(message(org, stage))],
  );
}
const claim = async (lease = LEASE, batch = 10) =>
  (await call<{ id: number }>("SELECT id FROM sessions.file_normalize_claim($1, 300, $2)", [batch, lease])).map((r) => r.id);
const start = async (id: number, lease = LEASE) =>
  (await call<{ dlq: boolean; attempts: number; message: { workKey: string } }>(
    "SELECT * FROM sessions.file_normalize_start($1, $2, 300)", [id, lease]))[0];
const retry = async (id: number, lease = LEASE, delay = 0) =>
  (await call<{ r: string }>("SELECT sessions.file_normalize_retry($1, $2, $3) AS r", [id, lease, delay]))[0]!.r;
const jobs = async () =>
  (await db.query<{ stage: string; dlq: boolean; attempts: number; lease_id: string | null; org_id: string; message: unknown }>(
    "SELECT stage, dlq, attempts, lease_id, org_id, message FROM sessions.pipeline_jobs ORDER BY id")).rows;
const ready = (over: Record<string, unknown> = {}) => ({
  ...message("org_a", "normalized-ready"), payload: { convergence_event_count: 3 }, ...over,
});
const enqueueReady = (id: number, record: unknown, lease = LEASE) =>
  call("SELECT sessions.file_normalize_enqueue_ready($1, $2, $3::jsonb)", [id, lease, JSON.stringify(record)]);

beforeAll(async () => {
  db = new PGlite();
  await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
  for (const file of readdirSync(supabaseFile("migrations/")).sort())
    await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  await db.exec(`RESET ROLE; TRUNCATE sessions.pipeline_jobs, sessions.pipeline_dead_letters;
    TRUNCATE sessions.pipeline_events`);
});

describe("claim and lease", () => {
  it("claims only file-normalize, and never a job another lease holds", async () => {
    await enqueue();
    await enqueue("session-indexed");
    const [id, ...rest] = await claim();
    expect(rest).toEqual([]);
    expect(await claim(OTHER_LEASE)).toEqual([]);
    expect((await start(id!))?.message.workKey).toBe(WORK_KEY);
  });

  it("start and ack need the lease, and a lost lease is not an error for the queue", async () => {
    await enqueue();
    const [id] = await claim();
    expect(await start(id!, OTHER_LEASE)).toBeUndefined();
    const [{ ok }] = (await call<{ ok: boolean }>("SELECT sessions.file_normalize_ack($1, $2) AS ok", [id, OTHER_LEASE])) as [{ ok: boolean }];
    expect(ok).toBe(false);
    expect(await retry(id!, OTHER_LEASE)).toBe("lost");
    expect((await call<{ ok: boolean }>("SELECT sessions.file_normalize_ack($1, $2) AS ok", [id, LEASE]))[0]!.ok).toBe(true);
    expect(await jobs()).toEqual([]);
  });

  it("can't touch another stage's job, even with a matching lease", async () => {
    await enqueue("session-indexed");
    await db.query("UPDATE sessions.pipeline_jobs SET lease_id = $1", [LEASE]);
    const { id } = (await db.query<{ id: number }>("SELECT id FROM sessions.pipeline_jobs")).rows[0]!;
    expect(await start(id)).toBeUndefined();
    expect((await call<{ ok: boolean }>("SELECT sessions.file_normalize_ack($1, $2) AS ok", [id, LEASE]))[0]!.ok).toBe(false);
    expect(await retry(id)).toBe("lost");
    await call("SELECT sessions.file_normalize_renew($1::bigint[], $2, 300)", [[id], LEASE]);
    await expect(call("SELECT sessions.file_normalize_quarantine($1, $2)", [id, LEASE])).rejects.toMatchObject({ code: "FN002" });
    expect(await jobs()).toHaveLength(1);
  });

  it("renews only this lease's jobs", async () => {
    await enqueue();
    const [id] = await claim();
    await db.query("UPDATE sessions.pipeline_jobs SET visible_at = now() + interval '1 second'");
    await call("SELECT sessions.file_normalize_renew($1::bigint[], $2, 900)", [[id], OTHER_LEASE]);
    const before = (await db.query<{ s: number }>("SELECT extract(epoch FROM visible_at - now())::float AS s FROM sessions.pipeline_jobs")).rows[0]!.s;
    expect(before).toBeLessThan(5);
    await call("SELECT sessions.file_normalize_renew($1::bigint[], $2, 900)", [[id], LEASE]);
    const after = (await db.query<{ s: number }>("SELECT extract(epoch FROM visible_at - now())::float AS s FROM sessions.pipeline_jobs")).rows[0]!.s;
    expect(after).toBeGreaterThan(800);
  });
});

describe("attempts", () => {
  const budget = STAGE_REGISTRY["file-normalize"].maxAttempts;

  it("retries up to the registry's budget, then moves to the DLQ with its own count", async () => {
    await enqueue();
    for (let attempt = 1; attempt <= budget; attempt++) {
      const [id] = await claim();
      expect((await start(id!))?.attempts).toBe(attempt);
      expect(await retry(id!)).toBe(attempt === budget ? "exhausted" : "retried");
    }
    expect((await jobs())[0]).toMatchObject({ dlq: true, attempts: 0, lease_id: null });
  });

  it("keeps a backoff delay", async () => {
    await enqueue();
    const [id] = await claim();
    await start(id!);
    await retry(id!, LEASE, 60);
    expect(await claim(OTHER_LEASE)).toEqual([]);
  });

  it("a job that killed its consumer every time still reaches the DLQ path", async () => {
    await enqueue();
    for (let attempt = 1; attempt <= budget; attempt++) {
      const [id] = await claim();
      await start(id!);
      // The consumer dies: no ack, no retry, and the lease runs out.
      await db.exec("UPDATE sessions.pipeline_jobs SET visible_at = now() - interval '1 second'");
    }
    const [id] = await claim();
    expect(await start(id!)).toMatchObject({ dlq: true, attempts: 1 });
  });

  it("a DLQ delivery keeps retrying until quarantine succeeds", async () => {
    await enqueue();
    await db.exec("UPDATE sessions.pipeline_jobs SET dlq = true, attempts = 99");
    const [id] = await claim();
    await start(id!);
    expect(await retry(id!)).toBe("retried");
    expect((await jobs())[0]?.dlq).toBe(true);
  });
});

describe("enqueue_ready", () => {
  it("sends one normalized-ready job with the held job's identity", async () => {
    await enqueue();
    const [id] = await claim();
    await enqueueReady(id!, ready());
    const sent = (await jobs())[1]!;
    expect(sent).toMatchObject({ stage: "normalized-ready", org_id: "org_a", dlq: false, lease_id: null });
  });

  it.each([
    ["another stage", { stage: "session-indexed" }],
    ["another tenant", { ctx: ctx("org_b") }],
    ["another work key", { workKey: "other@etag" }],
    ["another revision", { revision: 4 }],
    ["no payload", { payload: null }],
    ["an oversized record", { payload: { x: "y".repeat(17_000) } }],
  ])("refuses %s", async (_, over) => {
    await enqueue();
    const [id] = await claim();
    await expect(enqueueReady(id!, ready(over))).rejects.toMatchObject({ code: "FN003" });
    expect(await jobs()).toHaveLength(1);
  });

  it("refuses without the lease", async () => {
    await enqueue();
    const [id] = await claim();
    await expect(enqueueReady(id!, ready(), OTHER_LEASE)).rejects.toMatchObject({ code: "FN002" });
  });
});

describe("record_event", () => {
  const record = (id: number, error: unknown = null, key = "k1", lease = LEASE) =>
    call("SELECT sessions.file_normalize_record_event($1, $2, 'error', 1, $3::jsonb, '{}'::jsonb, 5, $4)", [
      id, lease, error === null ? null : JSON.stringify(error), key,
    ]);
  const events = async () =>
    (await db.query<{ stage: string; org_id: string; error: Record<string, unknown> | null }>(
      "SELECT stage, org_id, error FROM sessions.pipeline_events")).rows;

  it("takes identity from the job and keeps no error text", async () => {
    await enqueue();
    const [id] = await claim();
    await record(id!, { code: "boom", name: "PipelineError", message: "planted secret text", retryable: false });
    const [event] = await events();
    expect(event).toMatchObject({ stage: "file-normalize", org_id: "org_a" });
    expect(JSON.stringify(event)).not.toContain("planted");
    expect(event!.error).toEqual({ code: "boom", name: "PipelineError", retryable: false });
  });

  it("folds a repeated key, and refuses without the lease or with oversized attrs", async () => {
    await enqueue();
    const [id] = await claim();
    await record(id!);
    await record(id!);
    expect(await events()).toHaveLength(1);
    await expect(record(id!, null, "k2", OTHER_LEASE)).rejects.toMatchObject({ code: "FN002" });
    await expect(
      call("SELECT sessions.file_normalize_record_event($1, $2, 'ok', 1, NULL, $3::jsonb, 1, 'k3')", [
        id, LEASE, JSON.stringify({ x: "y".repeat(5000) }),
      ]),
    ).rejects.toMatchObject({ code: "22023" });
  });
});

describe("quarantine and resolve", () => {
  const letters = async () =>
    (await db.query<{ id: string; stage: string; state: string; dead_count: number; org_id: string }>(
      "SELECT id, stage, state, dead_count, org_id FROM sessions.pipeline_dead_letters")).rows;

  it.each([["org_a"], ["org with space/é"], ["a!~*'()-_.b"]])("uses the same id as deadLetterId for %s", async (org) => {
    await enqueue("file-normalize", org);
    const [id] = await claim();
    const [{ letter }] = (await call<{ letter: string }>("SELECT sessions.file_normalize_quarantine($1, $2) AS letter", [id, LEASE])) as [{ letter: string }];
    expect(letter).toBe(deadLetterId("file-normalize", { orgId: org, workspaceId: "ws_a" }, WORK_KEY));
  });

  it("a second death bumps the count, and resolve closes it", async () => {
    await enqueue();
    const [id] = await claim();
    await call("SELECT sessions.file_normalize_quarantine($1, $2)", [id, LEASE]);
    await call("SELECT sessions.file_normalize_quarantine($1, $2)", [id, LEASE]);
    expect(await letters()).toMatchObject([{ stage: "file-normalize", state: "dead", dead_count: 2, org_id: "org_a" }]);
    await call("SELECT sessions.file_normalize_resolve($1, $2)", [id, LEASE]);
    expect((await letters())[0]?.state).toBe("resolved");
  });

  it("refuses without the lease, and resolve leaves other letters alone", async () => {
    await enqueue();
    const [id] = await claim();
    await expect(call("SELECT sessions.file_normalize_quarantine($1, $2)", [id, OTHER_LEASE])).rejects.toMatchObject({ code: "FN002" });
    await db.exec(`INSERT INTO sessions.pipeline_dead_letters (id, stage, message) VALUES ('other', 'session-indexed', '{}')`);
    await call("SELECT sessions.file_normalize_resolve($1, $2)", [id, LEASE]);
    expect((await letters())[0]?.state).toBe("dead");
  });
});

describe("access", () => {
  it("has no login, no BYPASSRLS, and no privilege on any table in the schema", async () => {
    expect((await db.query("SELECT rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = 'file_normalize_worker'")).rows).toEqual([
      { rolbypassrls: false, rolcanlogin: false },
    ]);
    const { rows } = await db.query(
      `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
          AND has_table_privilege('file_normalize_worker', c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE')`,
    );
    expect(rows).toEqual([]);
    await expect(call("SELECT 1 FROM sessions.pipeline_jobs")).rejects.toMatchObject({ code: "42501" });
  });

  it("can't reach the count writer, the index, or the internal helpers", async () => {
    await expect(
      call(`SELECT sessions.record_normalized_event_count('o','w','h','s','m',1,1,'k@e',''::bytea,''::bytea,1)`),
    ).rejects.toMatchObject({ code: "42501" });
    await expect(call("SELECT 1 FROM sessions.session_index")).rejects.toMatchObject({ code: "42501" });
    await expect(call("SELECT sessions.file_normalize_held_job(1, $1)", [LEASE])).rejects.toMatchObject({ code: "42501" });
  });

  it("tenants and the API roles can't call any of it", async () => {
    for (const role of ["anon", "authenticated"]) {
      await db.exec(`SET ROLE ${role}`);
      try {
        await expect(db.query("SELECT * FROM sessions.file_normalize_claim(1, 1, gen_random_uuid())")).rejects.toMatchObject({ code: "42501" });
      } finally {
        await db.exec("RESET ROLE");
      }
    }
  });

  it("a definer RLS would filter raises FN001, never an empty queue", async () => {
    await enqueue();
    const fn = "sessions.file_normalize_claim(integer, integer, uuid)";
    await db.exec(`CREATE ROLE plain_owner NOLOGIN NOBYPASSRLS;
      GRANT USAGE ON SCHEMA sessions TO plain_owner;
      GRANT SELECT, UPDATE ON sessions.pipeline_jobs TO plain_owner;
      GRANT EXECUTE ON FUNCTION sessions.file_normalize_assert_visible() TO plain_owner;
      ALTER FUNCTION ${fn} OWNER TO plain_owner`);
    try {
      await expect(claim()).rejects.toMatchObject({ code: "FN001" });
    } finally {
      await db.exec(`ALTER FUNCTION ${fn} OWNER TO CURRENT_USER;
        REVOKE ALL ON sessions.pipeline_jobs FROM plain_owner;
        REVOKE EXECUTE ON FUNCTION sessions.file_normalize_assert_visible() FROM plain_owner;
        REVOKE ALL ON SCHEMA sessions FROM plain_owner;
        DROP ROLE plain_owner`);
    }
  });
});
