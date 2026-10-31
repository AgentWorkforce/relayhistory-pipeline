/**
 * `pollFileNormalize` on PGlite against every migration, as the real
 * `file_normalize_worker`, with a real `handleStageQueue`. Holds the same
 * retry and quarantine behaviour as `pollPostgresStage`, with no table access.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  assertQueueLimitedRole,
  deadLetterId,
  FILE_NORMALIZE_MAX_ATTEMPTS,
  noopErrorReporter,
  PipelineError,
  pollFileNormalize,
  STAGE_REGISTRY,
  type NormalizedOutput,
  type SessionTransform,
  type SqlExecutor,
} from "../src/lib/pipeline/index.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const TRACE = "0192f000-0000-7000-8000-000000000001";
const MANIFEST_KEY = "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/manifest.json";
const WORK_KEY = `${MANIFEST_KEY}@etag-1`;
const CTX = { traceId: TRACE, orgId: "org_a", workspaceId: "ws_a", machineId: "m-1", sessionId: "s-1", harness: "claude" };

const output: NormalizedOutput = {
  outputKey: "k",
  revision: 1,
  normalizedVersion: 2,
  contentSha256: "a".repeat(64),
  outputSha256: "b".repeat(64),
  outputBytes: 10,
  convergenceEventCount: 42,
  unprojectedRecords: 0,
  outputReused: false,
  outputDivergent: false,
};

let db: PGlite;
const worker: SqlExecutor = {
  query: async (text, params) => {
    await db.exec("SET ROLE file_normalize_worker");
    try {
      return await (db as unknown as SqlExecutor).query(text, params);
    } finally {
      await db.exec("RESET ROLE");
    }
  },
};
const poll = (transform: SessionTransform, batchSize?: number) =>
  pollFileNormalize({
    sql: worker,
    transform,
    ...(batchSize ? { batchSize } : {}),
    deps: { reporter: noopErrorReporter, logger: { environment: "test" } },
  });
const normalize = (run: SessionTransform["normalize"]): SessionTransform => ({ normalize: run });
const ok = normalize(async () => output);

async function enqueue(workKey = WORK_KEY) {
  const message = { v: 1, stage: "file-normalize", ctx: CTX, workKey, revision: 1, payload: { manifestKey: MANIFEST_KEY } };
  await db.query(
    `INSERT INTO sessions.pipeline_jobs (stage, work_key, trace_id, org_id, workspace_id, session_id, machine_id, message)
     VALUES ('file-normalize', $1, $2, 'org_a', 'ws_a', 's-1', 'm-1', $3::jsonb)`,
    [workKey, TRACE, JSON.stringify(message)],
  );
}
const jobs = async () =>
  (await db.query<{ stage: string; dlq: boolean; message: { payload: Record<string, unknown> } }>(
    "SELECT stage, dlq, message FROM sessions.pipeline_jobs ORDER BY id")).rows;
const dueNow = () => db.exec("UPDATE sessions.pipeline_jobs SET visible_at = now()");
const letters = async () =>
  (await db.query<{ id: string; state: string; dead_count: number }>("SELECT id, state, dead_count FROM sessions.pipeline_dead_letters")).rows;
const events = async () =>
  (await db.query<{ status: string; error: { code?: string; message?: string } | null }>("SELECT status, error FROM sessions.pipeline_events ORDER BY at")).rows;

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
  vi.spyOn(console, "info").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
  await db.exec(`RESET ROLE; TRUNCATE sessions.pipeline_jobs, sessions.pipeline_dead_letters;
    TRUNCATE sessions.pipeline_events`);
});

describe("pollFileNormalize", () => {
  it("normalizes, sends the record, records the event and acks", async () => {
    await enqueue();
    expect(await poll(ok)).toMatchObject({ claimed: 1, acked: 1 });
    const queued = await jobs();
    expect(queued).toHaveLength(1);
    expect(queued[0]).toMatchObject({ stage: "normalized-ready", dlq: false });
    expect(queued[0]!.message.payload).toMatchObject({ org_id: "org_a", convergence_event_count: 42, manifest_revision: 1 });
    expect((await events()).map((e) => e.status)).toContain("ok");
  });

  it("does nothing on an empty queue", async () => {
    expect(await poll(ok)).toEqual({ claimed: 0, acked: 0, retried: 0, exhausted: 0 });
  });

  it("sends nothing for a stale manifest, and acks", async () => {
    await enqueue();
    expect(await poll(normalize(async () => ({ skipped: "stale_manifest" })))).toMatchObject({ acked: 1 });
    expect(await jobs()).toEqual([]);
    expect((await events()).map((e) => e.status)).toContain("skipped");
  });

  it("sends nothing when there's nothing to normalize", async () => {
    await enqueue();
    expect(await poll(normalize(async () => ({})))).toMatchObject({ acked: 1 });
    expect(await jobs()).toEqual([]);
  });

  it("retries a transient failure, then succeeds", async () => {
    await enqueue();
    expect(await poll(normalize(async () => { throw new Error("planted provider text"); }))).toMatchObject({ retried: 1 });
    expect((await events()).map((e) => JSON.stringify(e)).join("")).not.toContain("planted");
    await dueNow();
    expect(await poll(ok)).toMatchObject({ acked: 1 });
    expect((await jobs())[0]?.stage).toBe("normalized-ready");
  });

  it("quarantines a non-retryable refusal at once, with the real letter id", async () => {
    await enqueue();
    await poll(normalize(async () => { throw new PipelineError("relayhistory_export_provenance", "refused", false); }));
    expect(await letters()).toEqual([
      { id: deadLetterId("file-normalize", { orgId: "org_a", workspaceId: "ws_a" }, WORK_KEY), state: "dead", dead_count: 1 },
    ]);
    expect(await jobs()).toEqual([]);
    expect((await events()).map((e) => e.status)).toContain("dead_lettered");
  });

  it("exhausts its budget, then quarantines on the DLQ delivery", async () => {
    await enqueue();
    const failing = normalize(async () => { throw new Error("down"); });
    for (let attempt = 1; attempt <= FILE_NORMALIZE_MAX_ATTEMPTS; attempt++) {
      await poll(failing);
      await dueNow();
    }
    expect((await jobs())[0]?.dlq).toBe(true);
    expect(await poll(failing)).toMatchObject({ acked: 1 });
    expect(await letters()).toHaveLength(1);
    expect(await jobs()).toEqual([]);
  });

  it("a failed send retries the job, so the record isn't lost", async () => {
    await enqueue();
    await db.exec("REVOKE EXECUTE ON FUNCTION sessions.file_normalize_enqueue_ready(bigint, uuid, jsonb) FROM file_normalize_worker");
    try {
      expect(await poll(ok)).toMatchObject({ retried: 1 });
    } finally {
      await db.exec("GRANT EXECUTE ON FUNCTION sessions.file_normalize_enqueue_ready(bigint, uuid, jsonb) TO file_normalize_worker");
    }
    await dueNow();
    expect(await poll(ok)).toMatchObject({ acked: 1 });
    expect((await jobs()).map((j) => j.stage)).toEqual(["normalized-ready"]);
  });

  it("handles a batch one job at a time", async () => {
    await enqueue(`${MANIFEST_KEY}@etag-a`);
    await enqueue(`${MANIFEST_KEY}@etag-b`);
    expect(await poll(ok, 2)).toMatchObject({ claimed: 2, acked: 2 });
    expect((await jobs()).map((j) => j.stage)).toEqual(["normalized-ready", "normalized-ready"]);
  });

  it("a job that lost its lease mid-handling isn't acked by the old holder", async () => {
    await enqueue();
    const stolen = normalize(async () => {
      await db.exec("UPDATE sessions.pipeline_jobs SET lease_id = gen_random_uuid()");
      return {};
    });
    expect(await poll(stolen)).toMatchObject({ claimed: 1, acked: 0 });
    expect(await jobs()).toHaveLength(1);
  });

  it("keeps the budget in SQL equal to the registry", () => {
    expect(FILE_NORMALIZE_MAX_ATTEMPTS).toBe(STAGE_REGISTRY["file-normalize"].maxAttempts);
    expect(FILE_NORMALIZE_MAX_ATTEMPTS).toBe(6);
  });
});

describe("end to end, with the guard on", () => {
  it("the worker's record is the only way in, and it applies", async () => {
    await db.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)", [
      JSON.stringify([{
        org_id: "org_a", workspace_id: "ws_a", harness: "claude", session_id: "s-1", machine_id: "m-1",
        user_id: "u", created_at: "2026-10-30T00:00:00Z", last_activity_at: "2026-10-30T00:00:00Z",
        event_count: 7, storage_provider: "s3", bucket: "b", manifest_key: MANIFEST_KEY,
        content_sha256: `\\x${"a".repeat(64)}`, raw_bytes: 1, chunk_count: 1, raw_format: "jsonl", format_version: 1,
        revision: 1, trace_id: TRACE, scrubbed: true, deleted_at: null,
      }]),
    ]);
    await enqueue();
    await poll(ok);
    const owner = (await db.query<{ owner: string }>(
      `SELECT pg_get_userbyid(proowner) AS owner FROM pg_proc WHERE proname = 'file_normalize_enqueue_ready'`)).rows[0];
    expect(owner?.owner).toBe("normalized_ready_writer");
    const { rows } = await db.query<{ id: number }>("SELECT id FROM sessions.pipeline_jobs WHERE stage = 'normalized-ready'");
    const lease = "0192f000-0000-7000-8000-00000000000a";
    await db.query("UPDATE sessions.pipeline_jobs SET lease_id = $1, visible_at = now() + interval '60 seconds'", [lease]);
    await db.exec("SET ROLE session_count_writer");
    try {
      const result = await db.query<{ outcome: string }>(
        "SELECT sessions.record_normalized_event_count($1, $2) AS outcome", [rows[0]!.id, lease]);
      expect(result.rows).toEqual([{ outcome: "applied" }]);
    } finally {
      await db.exec("RESET ROLE");
    }
    expect((await db.query("SELECT event_count, event_count_revision FROM sessions.session_index WHERE session_id = 's-1'")).rows)
      .toEqual([{ event_count: 42, event_count_revision: 1 }]);
  });
});

describe("assertQueueLimitedRole", () => {
  const as = async (role: string) => {
    await db.exec(`SET ROLE ${role}`);
    try {
      await assertQueueLimitedRole(db as unknown as SqlExecutor);
    } finally {
      await db.exec("RESET ROLE");
    }
  };

  it("accepts the worker role", async () => {
    await expect(as("file_normalize_worker")).resolves.toBeUndefined();
  });

  it("refuses service_role, which could reach the tables", async () => {
    await expect(as("service_role")).rejects.toMatchObject({ code: "worker_role_too_broad" });
  });
});
