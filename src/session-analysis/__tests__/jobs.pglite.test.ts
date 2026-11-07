/**
 * 20261105046000_analysis_jobs on PGlite with the full migration plan (the session-brief
 * harness): relayhistory's admission outcomes and limits, the v1 read projection for
 * native and backfilled rows, the read role guard, erasure, and the worker lease.
 */
import { afterEach, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, hoopDb, pointer, KEY } from "../../session-brief/__tests__/harness.ts";
import { postgresAnalysisJobStore, sweepAnalysisTickets } from "../store.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../../packages/object-store/src/index.ts";
import { analysisResultKey, analysisResultPrefix, encodeAnalysisResult } from "../results.ts";
import type { AnalysisJob, AnalysisJobOutput } from "../processor.ts";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
  SESSION_ANALYSIS_SUMMARY_VERSION,
  SESSION_ANALYSIS_MODEL_VERSION,
  SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
  SESSION_ANALYSIS_VERSION,
} from "../versions.ts";
import { read } from "../../session-brief/__tests__/harness.ts";

// Each test migrates a fresh PGlite with the full plan (the session-brief harness pattern).
setDefaultTimeout(60_000);

type Json = Record<string, any>;
let pg: PGlite;

/** analysis_request_v1 as the admission principal, bound to `bound` (the verified assertion's tenant). */
async function request(session = KEY.sessionId, workspace = KEY.workspaceId, org = KEY.orgId, user = "u-1", bound = { org, ws: workspace }) {
  await pg.exec("BEGIN");
  try {
    await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [bound.org, bound.ws]);
    await pg.exec("SET LOCAL ROLE bulls96_analysis_admit");
    const r = (await pg.query<{ r: Json }>("SELECT sessions.analysis_request_v1($1, $2, $3, $4) AS r", [org, workspace, user, session])).rows[0]!.r;
    await pg.exec("COMMIT");
    return r;
  } catch (error) {
    await pg.exec("ROLLBACK");
    throw error;
  }
}

const results = () => createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-jobs-")) });
/** The unversioned fs store's every-version erase (a versioned bucket uses createS3Eraser). */
const fsErase = (store: ReturnType<typeof results>) => async (key: string) => {
  const had = (await store.head(objectKey(key))) !== undefined;
  await store.delete(objectKey(key));
  return had ? 1 : 0;
};
const ANCHOR = "org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json";
const jobStore = (store = results()) =>
  postgresAnalysisJobStore({ db: hoopDb(pg), store, anchorManifestKey: async () => ANCHOR });
const output = (over: Partial<AnalysisJobOutput> = {}): AnalysisJobOutput => ({
  harness: "claude", sourceFingerprint: "f".repeat(64), sourceGeneration: "e".repeat(64), inputTokens: 5, outputTokens: 2,
  result: { schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: {} as never, durationMs: 1, brief: {} as never }, ...over,
});
/** reserve -> put -> complete, as runAnalysisJob does; returns the stage reached. */
async function publish(store: ReturnType<typeof jobStore>, job: AnalysisJob, between?: () => Promise<void>) {
  const out = output();
  const { bytes, sha256 } = encodeAnalysisResult(out.result);
  const key = await store.reserve(job, out, { sha256, bytes: bytes.length });
  if (!key) return { stage: "refused" as const, key: null };
  await store.put(key, bytes, sha256);
  if (between) await between();
  const ok = await store.complete({ job, output: out, stored: { key, sha256, bytes: bytes.length } });
  return { stage: ok ? ("completed" as const) : ("lost" as const), key };
}
const tx = <T,>(work: (q: { query: (t: string, p?: unknown[]) => Promise<{ rows: any[] }> }) => Promise<T>) => work(hoopDb(pg) as never);

async function readAs(workspace = KEY.workspaceId, session = KEY.sessionId, bound = { org: KEY.orgId, ws: KEY.workspaceId }) {
  await pg.exec("BEGIN");
  try {
    await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [bound.org, bound.ws]);
    await pg.exec("SET LOCAL ROLE bulls96_read_api");
    return (await pg.query<{ r: Json }>("SELECT sessions.analysis_read_v1($1, $2) AS r", [workspace, session])).rows[0]!.r;
  } finally {
    await pg.exec("ROLLBACK");
  }
}

const age = (table: string, interval: string) =>
  pg.query(`UPDATE sessions.${table} SET created_at = created_at - $1::interval`, [interval]);

afterEach(async () => {
  await pg.close();
});

beforeEach(async () => {
  pg = await database();
  // The read role's post-migrate grants (schema USAGE, its column grants and RLS policy).
  await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  // The admission principal's checked-in operator step (LOGIN, schema USAGE).
  await pg.exec(read("supabase/bootstrap/session_analysis_admit_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(pg);
});

describe("analysis_request_v1", () => {
  test("validates, refuses an unknown session, and admits one job per request", async () => {
    expect(await request(" ")).toEqual({ outcome: "invalid_request" });
    expect(await request("nope")).toEqual({ outcome: "not_found" });
    expect(await request(KEY.sessionId, "other-ws")).toEqual({ outcome: "not_found" });
    // The caller's binding pins the tenant: another org/workspace than the bound one is refused.
    await expect(request(KEY.sessionId, KEY.workspaceId, KEY.orgId, "u-1", { org: KEY.orgId, ws: "ws-b" })).rejects.toThrow(/tenant binding/);
    await expect(pg.query("SELECT sessions.analysis_request_v1($1, $2, 'u', $3)", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rejects.toThrow(/admission role/);
    const first = await request();
    expect(first.outcome).toBe("accepted");
    expect(first.request).toMatchObject({ sessionId: KEY.sessionId, status: "queued", error: null });
    const job = (await pg.query<Json>("SELECT * FROM sessions.session_analysis_jobs_v2")).rows;
    expect(job).toHaveLength(1);
    expect(job[0]).toMatchObject({
      id: first.request.analysisId,
      analysis_version: SESSION_ANALYSIS_VERSION,
      schema_version: SESSION_ANALYSIS_RESULT_SCHEMA_VERSION,
      model_version: SESSION_ANALYSIS_MODEL_VERSION,
      summary_version: SESSION_ANALYSIS_SUMMARY_VERSION,
      summary_model_version: SESSION_ANALYSIS_SUMMARY_MODEL_VERSION,
      origin: "native",
    });
    // An in-flight analysis answers a repeat request with the same request.
    expect((await request()).request.id).toBe(first.request.id);
  });

  test("relayhistory limits: 2-minute throttle, 30 per session per day, 20 other sessions per org per day", async () => {
    const first = await request();
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET status = 'failed', last_error = 'model_refusal'");
    // Throttled: the latest request, even failed.
    const again = await request();
    expect(again.request).toMatchObject({ id: first.request.id, status: "failed", error: "model_refusal" });
    await age("session_analysis_requests_v2", "3 minutes");
    const second = await request();
    expect(second.request.id).not.toBe(first.request.id);

    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET status = 'failed', last_error = 'model_refusal'");
    await pg.query(
      `INSERT INTO sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id, created_at)
       SELECT $1, $2, $3, 'u', 'resolved', (SELECT id FROM sessions.session_analysis_jobs_v2 LIMIT 1),
              GREATEST(now() - interval '5 minutes', date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')
         FROM generate_series(1, 28)`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    await pg.query("UPDATE sessions.session_analysis_requests_v2 SET created_at = GREATEST(created_at, date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') WHERE created_at > now() - interval '2 minutes'");
    await pg.query("UPDATE sessions.session_analysis_requests_v2 SET created_at = now() - interval '3 minutes' WHERE created_at > now() - interval '3 minutes'");
    expect(await request()).toEqual({ outcome: "rate_limited" });

    // Org budget: 20 distinct other sessions admitted today.
    const other = { ...KEY, sessionId: "s-2" };
    await pointer(pg, { key: other });
    await pg.query(
      `INSERT INTO sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id)
       SELECT $1, $2, 'x-' || n, 'u', 'resolved', (SELECT id FROM sessions.session_analysis_jobs_v2 LIMIT 1)
         FROM generate_series(1, 20) AS n`,
      [KEY.orgId, KEY.workspaceId],
    );
    expect(await request("s-2")).toEqual({ outcome: "rate_limited" });
  });

  test("an analysis completed in the last 30 minutes is reused", async () => {
    const first = await request();
    const store = jobStore();
    expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
    await age("session_analysis_requests_v2", "3 minutes");
    const second = await request();
    expect(second.request.id).not.toBe(first.request.id);
    expect(second.request).toMatchObject({ analysisId: first.request.analysisId, status: "completed" });
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(1);
  });

  test("P1-2: a completed analysis of an older pointer revision is NOT reused", async () => {
    const first = await request();
    const store = jobStore();
    expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
    await pg.query("UPDATE sessions.session_index SET revision = revision + 1");
    await age("session_analysis_requests_v2", "3 minutes");
    const second = await request();
    expect(second.request.analysisId).not.toBe(first.request.analysisId);
    expect(second.request.status).toBe("queued");
  });

  test("P1-a: a canonical machine id or user id correction is a new revision: no stale reuse", async () => {
    for (const fix of [
      "UPDATE sessions.session_index SET canonical_machine_id = 'm-canonical'",
      "UPDATE sessions.session_index SET user_id = 'u-corrected'",
    ]) {
      await pg.query("DELETE FROM sessions.session_analysis_requests_v2");
      await pg.query("DELETE FROM sessions.session_analysis_jobs_v2");
      const first = await request();
      const store = jobStore();
      expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
      await pg.query(fix);
      await age("session_analysis_requests_v2", "3 minutes");
      const second = await request();
      expect(second.request.analysisId, fix).not.toBe(first.request.analysisId);
    }
  });

  test("P2-a: the revision is injective (no separator collision) and P1-a: it signs manifest_etag", async () => {
    const rev = async () =>
      (await pg.query<{ r: string }>("SELECT sessions.session_analysis_source_revision($1, $2, $3) AS r", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rows[0]!.r;
    // bucket 'x|y' + key 'z' vs bucket 'x' + key 'y|z' joined with '|' would sign alike.
    await pg.query("UPDATE sessions.session_index SET bucket = 'x|y', manifest_key = 'z'");
    const a = await rev();
    await pg.query("UPDATE sessions.session_index SET bucket = 'x', manifest_key = 'y|z'");
    expect(await rev()).not.toBe(a);
    // NULL is a value: an absent etag differs from the empty one.
    await pg.query("UPDATE sessions.session_index SET manifest_etag = NULL");
    const none = await rev();
    await pg.query("UPDATE sessions.session_index SET manifest_etag = ''");
    expect(await rev()).not.toBe(none);
    await pg.query("UPDATE sessions.session_index SET manifest_etag = '\"v2\"'");
    expect(await rev()).not.toBe(none);
  });

  test("the revision signs every canonicalSourceGeneration field", async () => {
    const rev = async () =>
      (await pg.query<{ r: string }>("SELECT sessions.session_analysis_source_revision($1, $2, $3) AS r", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rows[0]!.r;
    const seen = new Set([await rev()]);
    for (const fix of [
      "canonical_machine_id = 'm-c'", "user_id = 'u-2'", "bucket = 'bucket-2'", "storage_provider = 's3'",
      "raw_format = 'ndjson'", "revision = revision + 1", "manifest_key = 'manifest/x'", "content_sha256 = decode(repeat('cd', 32), 'hex')",
    ]) {
      await pg.query(`UPDATE sessions.session_index SET ${fix}`);
      const next = await rev();
      expect(seen.has(next), fix).toBe(false);
      seen.add(next);
    }
  });

  test("P2: a change that flips the never-in-Neon proof (an old tombstoned pointer) moves the revision: no reuse", async () => {
    await pg.query(
      `INSERT INTO sessions.ingest_frozen_source_completeness
         (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
       VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`,
    );
    await pg.query("INSERT INTO sessions.session_neon_census_seal (generation, census_rows, census_sha256, neon_snapshot) VALUES (repeat('e',64), 0, repeat('e',64), '{}'::jsonb)");
    const proof = async () =>
      (await pg.query<{ f: boolean }>("SELECT sessions.session_analysis_neon_free($1, $2, $3) AS f", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rows[0]!.f;
    expect(await proof()).toBe(true);
    const first = await request();
    const store = jobStore();
    expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
    // An already-tombstoned pointer from before the seal arrives (no live->tombstoned UPDATE:
    // the forget trigger does not fire). The proof flips to false...
    await pg.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title, deleted_at)
       VALUES ($1, $2, $3, $4, 'm-old', 'u-1', now() - interval '2 hours', now() - interval '2 hours', 'fs', 'bucket', 'manifest/old',
               decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 't', now() - interval '90 minutes')`,
      [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId],
    );
    expect(await proof()).toBe(false);
    // ...so the completed analysis is not reused.
    await age("session_analysis_requests_v2", "3 minutes");
    const second = await request();
    expect(second.request.analysisId).not.toBe(first.request.analysisId);
    // And a proof-flip during a run refuses its publication.
    const job = (await store.claim({ leaseSeconds: 600 }))!;
    await pg.query("UPDATE sessions.session_index SET created_at = now() WHERE machine_id = 'm-old'");
    expect((await publish(store, job)).stage).toBe("refused");
  });

  test("P1-3: a backfilled in-flight job neither answers nor blocks a new request", async () => {
    await pg.query(
      `INSERT INTO sessions.session_analysis_jobs_v2 (id, org_id, workspace_id, session_id, status, analysis_version, schema_version, model_version, origin)
       VALUES ('11111111-1111-4111-8111-111111111111', $1, $2, $3, 'queued', 'session-analysis-v2.4:relayhistory-session-v2', 2, 'gpt-5.4', 'neon_backfill')`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    await pg.query(
      `INSERT INTO sessions.session_analysis_requests_v2 (org_id, workspace_id, session_id, requested_by_user_id, status, analysis_job_id, origin, created_at)
       VALUES ($1, $2, $3, 'u', 'queued', '11111111-1111-4111-8111-111111111111', 'neon_backfill', now() - interval '1 day')`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    const r = await request();
    expect(r.request.analysisId).not.toBe("11111111-1111-4111-8111-111111111111");
    expect(r.request.status).toBe("queued");
    // And the worker never claims a backfilled row.
    const claimed = await jobStore().claim({ leaseSeconds: 600 });
    expect(claimed?.id).toBe(r.request.analysisId);
  });
});

describe("analysis_read_v1", () => {
  test("is only the read role's, pinned to the bound workspace", async () => {
    await expect(pg.query("SELECT sessions.analysis_read_v1($1, $2)", [KEY.workspaceId, KEY.sessionId])).rejects.toThrow();
    await expect(readAs(KEY.workspaceId, KEY.sessionId, { org: KEY.orgId, ws: "other" })).rejects.toThrow(/tenant binding/);
    await pg.exec("SET ROLE bulls96_read_api");
    try {
      await expect(pg.query("SELECT sessions.analysis_request_v1('o','w','u','s')")).rejects.toThrow(/permission denied/);
    } finally {
      await pg.exec("RESET ROLE");
    }
  });

  test("not_found, empty ok, then relayhistory's v1 envelope with result refs", async () => {
    expect(await readAs(KEY.workspaceId, "nope")).toEqual({ outcome: "not_found" });
    expect(await readAs()).toEqual({ outcome: "ok", sessionId: KEY.sessionId, requests: [], analyses: [] });
    const accepted = await request();
    const job = jobStore();
    const claimed = (await job.claim({ leaseSeconds: 600 }))!;
    const published = await publish(job, claimed);
    expect(published.stage).toBe("completed");
    const env = await readAs();
    expect(env.requests).toEqual([
      expect.objectContaining({ id: accepted.request.id, status: "completed", analysisId: claimed.id, error: null }),
    ]);
    expect(env.analyses).toHaveLength(1);
    expect(env.analyses[0]).toMatchObject({
      id: claimed.id, sourceProvider: "claude", sourceFingerprint: "f".repeat(64), status: "completed",
      attempts: 1, analysisVersion: SESSION_ANALYSIS_VERSION, schemaVersion: 2,
      summaryVersion: "session-summary-v3:workspace-output-v2", modelVersion: "gpt-5.4",
      summaryModelVersion: "gpt-5.6-luna", origin: "native", error: null,
      resultRef: { key: published.key },
    });
    expect(env.analyses[0].completedAt).toEqual(expect.any(String));
  });

  test("backfilled rows serve Neon's public v1 values verbatim", async () => {
    await pg.query(
      `INSERT INTO sessions.session_analysis_jobs_v2 (id, org_id, workspace_id, session_id, harness, status, attempts,
         analysis_version, schema_version, model_version, summary_version, summary_model_version, source_fingerprint,
         result_s3_key, result_sha256, result_bytes, origin, created_at, updated_at, completed_at)
       VALUES ('11111111-1111-4111-8111-111111111111', $1, $2, $3, 'claude', 'completed', 1,
         'session-analysis-v2.4:relayhistory-session-v2', 2, 'gpt-5.4', 'session-summary-v3:workspace-output-v2',
         'gpt-5.6-luna', 'fp', 'k', repeat('c', 64), 9, 'neon_backfill',
         '2026-10-01T00:00:00.123456Z', '2026-10-01T00:00:01.123456Z', '2026-10-01T00:00:02.123456Z')`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    await pg.query(
      `INSERT INTO sessions.session_analysis_requests_v2 (id, org_id, workspace_id, session_id, requested_by_user_id,
         status, analysis_job_id, v1_job_id, origin, created_at, updated_at)
       VALUES ('22222222-2222-4222-8222-222222222222', $1, $2, $3, 'u', 'completed',
         '11111111-1111-4111-8111-111111111111', md5('v1-job:22222222-2222-4222-8222-222222222222')::uuid,
         'neon_backfill', '2026-10-01T00:00:00.123456Z', '2026-10-01T00:00:03.123456Z')`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    const v1 = (await pg.query<{ id: string }>("SELECT md5('v1-job:22222222-2222-4222-8222-222222222222')::uuid::text AS id")).rows[0]!.id;
    const env = await readAs();
    expect(env.requests).toEqual([{
      id: "22222222-2222-4222-8222-222222222222", sessionId: KEY.sessionId, status: "completed", analysisId: v1,
      error: null, createdAt: "2026-10-01T00:00:00.123456+00:00", updatedAt: "2026-10-01T00:00:03.123456+00:00",
    }]);
    expect(env.analyses[0]).toMatchObject({
      id: v1, summaryVersion: "session-summary-v3:workspace-output-v2", summaryModelVersion: "gpt-5.6-luna",
      origin: "neon_backfill", createdAt: "2026-10-01T00:00:00.123456+00:00",
    });
  });

  test("erasure: a tombstoned pointer forgets the session's analysis rows", async () => {
    await request();
    await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows).toHaveLength(0);
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
  });
});

describe("worker lease (relayhistory repository.ts)", () => {
  test("claim, lease fence, bounded retry with backoff, expiry after max attempts", async () => {
    await request();
    const store = jobStore();
    const a = (await store.claim({ leaseSeconds: 600 }))!;
    expect(a.attempts).toBe(1);
    expect(await store.claim({ leaseSeconds: 600 })).toBeNull();
    expect(await store.fail({ jobId: a.id, leaseToken: "00000000-0000-4000-8000-000000000000", error: "x", retryable: true })).toBe(false);
    expect(await store.fail({ jobId: a.id, leaseToken: a.leaseToken, error: "Leaky text!", retryable: true })).toBe(true);
    let row = (await pg.query<Json>("SELECT status, last_error, available_at > now() AS later FROM sessions.session_analysis_jobs_v2")).rows[0]!;
    expect(row).toEqual({ status: "queued", last_error: "analysis_processing_failed", later: true });
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET available_at = now()");
    const b = (await store.claim({ leaseSeconds: 600 }))!;
    expect(b.attempts).toBe(2);
    // A lost lease: it expires; the third attempt reclaims, then expiry at max attempts fails it.
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET lease_expires_at = now() - interval '1 second'");
    expect((await publish(jobStore(), b)).stage).toBe("refused");
    const c = (await store.claim({ leaseSeconds: 600 }))!;
    expect(c.attempts).toBe(3);
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET lease_expires_at = now() - interval '1 second'");
    expect(await store.claim({ leaseSeconds: 600 })).toBeNull();
    row = (await pg.query<Json>("SELECT status, last_error FROM sessions.session_analysis_jobs_v2")).rows[0]!;
    // relayhistory keeps the last recorded error over the expiry code (COALESCE).
    expect(row).toEqual({ status: "failed", last_error: "analysis_processing_failed" });
  });

  test("P2: fail() is fenced by the lease expiry, not only the token", async () => {
    await request();
    const store = jobStore();
    const a = (await store.claim({ leaseSeconds: 600 }))!;
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET lease_expires_at = now() - interval '1 second'");
    expect(await store.fail({ jobId: a.id, leaseToken: a.leaseToken, error: "model_refusal", retryable: false })).toBe(false);
  });

  test("a non-retryable failure is final", async () => {
    await request();
    const store = jobStore();
    const a = (await store.claim({ leaseSeconds: 600 }))!;
    await store.fail({ jobId: a.id, leaseToken: a.leaseToken, error: "session_quarantined", retryable: false });
    expect((await pg.query<Json>("SELECT status, last_error FROM sessions.session_analysis_jobs_v2")).rows[0]).toEqual({
      status: "failed", last_error: "session_quarantined",
    });
  });
});

describe("P0-2 erase-safe publication", () => {
  test("an erase between put and complete leaves no untracked object: the ticket survives and the sweeper deletes it", async () => {
    await request();
    const objects = results();
    const store = jobStore(objects);
    const job = (await store.claim({ leaseSeconds: 600 }))!;
    const out = await publish(store, job, async () => {
      await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    });
    expect(out.stage).toBe("lost");
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
    expect((await objects.head(objectKey(out.key!)))).toBeDefined();
    const tickets = (await pg.query<Json>("SELECT result_key, reason FROM sessions.session_analysis_result_tickets")).rows;
    expect(tickets).toEqual([{ result_key: out.key, reason: "publishing" }]);
    expect(await sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject: fsErase(objects) })).toBe(1);
    expect(await objects.head(objectKey(out.key!))).toBeUndefined();
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  });

  test("an erase before the reservation writes nothing at all", async () => {
    await request();
    const objects = results();
    const store = jobStore(objects);
    const job = (await store.claim({ leaseSeconds: 600 }))!;
    await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await publish(store, job)).stage).toBe("refused");
    const { sha256 } = encodeAnalysisResult(output().result);
    expect(await objects.head(objectKey(analysisResultKey(analysisResultPrefix(ANCHOR), job.id, sha256)))).toBeUndefined();
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  });

  test("a changed pointer revision refuses the reservation", async () => {
    await request();
    const store = jobStore();
    const job = (await store.claim({ leaseSeconds: 600 }))!;
    await pg.query("UPDATE sessions.session_index SET revision = revision + 1");
    expect((await publish(store, job)).stage).toBe("refused");
  });

  test("P1-c: on a versioned bucket the sweeper erases every version and keeps the ticket until none remains", async () => {
    await request();
    const objects = results();
    const store = jobStore(objects);
    const out = await publish(store, (await store.claim({ leaseSeconds: 600 }))!, async () => {
      await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    });
    expect(out.stage).toBe("lost");
    // A versioned bucket: a plain delete leaves a noncurrent version.
    const versions = new Map<string, number>([[out.key!, 2]]);
    let stubborn = true;
    const eraseObject = async (key: string) => {
      const n = versions.get(key) ?? 0;
      if (stubborn) return n; // the first sweep "deletes" nothing: versions remain
      versions.set(key, 0);
      return n;
    };
    await expect(sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject })).rejects.toThrow("analysis_sweep_versions_remain");
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(1);
    stubborn = false;
    expect(await sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject })).toBe(1);
    expect(versions.get(out.key!)).toBe(0);
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  });

  test("P1-c: a ticket under a live writer lease is never swept, however old", async () => {
    await pg.query(
      `INSERT INTO sessions.session_analysis_result_tickets (result_key, job_id, org_id, workspace_id, session_id, reason, created_at, lease_expires_at)
       VALUES ('k/leased', gen_random_uuid(), $1, $2, $3, 'publishing', now() - interval '1 day', now() + interval '5 minutes')`,
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    expect(await sweepAnalysisTickets(results(), { graceSeconds: 0, transact: tx, eraseObject: fsErase(results()) })).toBe(0);
    await pg.query("UPDATE sessions.session_analysis_result_tickets SET lease_expires_at = now() - interval '1 second'");
    expect(await sweepAnalysisTickets(results(), { graceSeconds: 0, transact: tx, eraseObject: fsErase(results()) })).toBe(1);
  });

  test("P1-a: the worker's database half never re-reads a manifest", async () => {
    const { canonicalEvidenceDeps } = await import("../worker.ts");
    const touched: string[] = [];
    const store = new Proxy({}, { get: (_t, k) => () => { touched.push(String(k)); throw new Error("no manifest read expected"); } });
    const deps = canonicalEvidenceDeps(
      { query: async () => ({ rows: [{ free: true }] }) } as never,
      { store: store as never, provider: "s3", bucket: "b", prefix: {}, decompress: (b: Uint8Array) => b },
    );
    expect(await deps.neonFree!({ orgId: "o", workspaceId: "w", sessionId: "s", source: "claude" })).toBe(true);
    expect(touched).toEqual([]);
  });

  test("erasing a completed session tickets its result for deletion; a live result is never swept", async () => {
    await request();
    const objects = results();
    const store = jobStore(objects);
    const out = await publish(store, (await store.claim({ leaseSeconds: 600 }))!);
    expect(out.stage).toBe("completed");
    expect(await sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject: fsErase(objects) })).toBe(0);
    expect(await objects.head(objectKey(out.key!))).toBeDefined();
    await pg.query("UPDATE sessions.session_index SET deleted_at = now()");
    expect((await pg.query<Json>("SELECT reason FROM sessions.session_analysis_result_tickets")).rows).toEqual([{ reason: "forgotten" }]);
    expect(await sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject: fsErase(objects) })).toBe(1);
    expect(await objects.head(objectKey(out.key!))).toBeUndefined();
  });
});

test("the migration's engine identity is src/session-analysis/versions.ts", () => {
  const sql = read("supabase/migrations/20261105046000_analysis_jobs.sql");
  expect(sql).toContain(`c_analysis_version constant text := '${SESSION_ANALYSIS_VERSION}'`);
  expect(sql).toContain(`c_schema_version constant integer := ${SESSION_ANALYSIS_RESULT_SCHEMA_VERSION}`);
  expect(sql).toContain(`c_model_version constant text := '${SESSION_ANALYSIS_MODEL_VERSION}'`);
  expect(sql).not.toMatch(/CONCURRENTLY/i);
});

describe("analysisTick is default-off", () => {
  test("disabled or without a model it claims nothing", async () => {
    const { analysisConfig, analysisTick } = await import("../worker.ts");
    expect(analysisConfig({})).toEqual({ enabled: false, batch: 2, leaseSeconds: 600, assumeNoLegacyTurns: false });
    expect(analysisConfig({ SESSION_ANALYSIS_ENABLED: "1", SESSION_ANALYSIS_BATCH: "99" }).enabled).toBe(false);
    await request();
    const base = { db: hoopDb(pg), store: null as never, canonical: null as never };
    const transport = { complete: async () => { throw new Error("no model call expected"); } };
    await analysisTick({ ...base, config: analysisConfig({}), transport })();
    await analysisTick({ ...base, config: analysisConfig({ SESSION_ANALYSIS_ENABLED: "true" }), transport: null })();
    expect((await pg.query<Json>("SELECT status, attempts FROM sessions.session_analysis_jobs_v2")).rows).toEqual([
      { status: "queued", attempts: 0 },
    ]);
  });
});

describe("never-in-Neon proof (session_analysis_neon_free + live-only manifests)", () => {
  const free = async () =>
    (await pg.query<{ f: boolean }>("SELECT sessions.session_analysis_neon_free($1, $2, $3) AS f", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rows[0]!.f;
  const census = () => pg.query("INSERT INTO sessions.session_neon_census_seal (generation, census_rows, census_sha256, neon_snapshot) VALUES (repeat('e',64), 0, repeat('e',64), '{}'::jsonb)");
  const seal = (ago: string) =>
    pg.query(
      `INSERT INTO sessions.ingest_frozen_source_completeness
         (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
       VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - $1::interval)`,
      [ago],
    );

  test("born after the sealed freeze and absent from the frozen copy: complete without the marker", async () => {
    expect(await free()).toBe(false); // no seal yet: doubt
    await seal("1 hour");
    expect(await free()).toBe(false); // no Neon census sealed: no authoritative legacy coverage
    await census();
    expect(await free()).toBe(true);
  });

  test("#301 (1): an OLD turns-only / not-yet-exported Neon session whose FIRST SP pointer lands after the seal is not Neon-free", async () => {
    // No frozen delivery record (turns-only on the legacy path), every pointer born after the
    // seal (a late first upload or a continuation), no export marker yet: both absence checks
    // hold, and only the Neon census excludes it.
    await seal("1 hour");
    await census();
    expect(await free()).toBe(true);
    await pg.query("WITH g AS (SELECT encode(sha256(convert_to(random()::text, 'UTF8')), 'hex') AS g), ins AS (INSERT INTO sessions.session_neon_census (generation, org_id, workspace_id, session_id) SELECT g.g, $1, 'any-ws', $2 FROM g RETURNING generation) UPDATE sessions.session_neon_census_seal SET generation = (SELECT generation FROM ins LIMIT 1)", [KEY.orgId, KEY.sessionId]);
    expect(await free()).toBe(false);
    // By physical id too (a canonicalized continuation).
    await pg.query("UPDATE sessions.session_neon_census_seal SET generation = encode(sha256(convert_to(random()::text, 'UTF8')), 'hex')");
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = $1, session_id = 'phys-1'", [KEY.sessionId]);
    await pg.query("WITH g AS (SELECT encode(sha256(convert_to(random()::text, 'UTF8')), 'hex') AS g), ins AS (INSERT INTO sessions.session_neon_census (generation, org_id, workspace_id, session_id) SELECT g.g, $1, $2, 'phys-1' FROM g RETURNING generation) UPDATE sessions.session_neon_census_seal SET generation = (SELECT generation FROM ins LIMIT 1)", [KEY.orgId, KEY.workspaceId]);
    expect(await free()).toBe(false);
  });

  test("a Neon-era session is never proven Neon-free", async () => {
    await seal("1 hour");
    await census();
    // In the frozen Neon copy (any workspace): Neon delivery session.
    await pg.query(
      "INSERT INTO sessions.ingest_frozen_source_records (org_id, workspace_id, origin_id, record_id, source, session_id) VALUES ($1, 'other-ws', 'o', 'r', 'claude', $2)",
      [KEY.orgId, KEY.sessionId],
    );
    expect(await free()).toBe(false);
    await pg.query("DELETE FROM sessions.ingest_frozen_source_records");
    // Born before the freeze (a pre-freeze copy or continuation).
    await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '2 hours'");
    expect(await free()).toBe(false);
  });

  test("a Neon export/backfill manifest makes the scope Neon-derived", async () => {
    const { neonDerivedManifest } = await import("../worker.ts");
    const base = { extensions: {}, raw: { epochs: [{ epoch: 1, reason: "live" }] } } as never;
    expect(neonDerivedManifest(base)).toBe(false);
    expect(neonDerivedManifest({ extensions: {}, raw: { epochs: [{ epoch: 2, reason: "backfill" }] } } as never)).toBe(true);
    expect(neonDerivedManifest({ extensions: { "client.relayhistory_export": {} }, raw: { epochs: [] } } as never)).toBe(true);
  });
});

describe("session_neon_free_for (the read role's half of the shared proof)", () => {
  const sealNow = () =>
    pg.query(
      `INSERT INTO sessions.ingest_frozen_source_completeness
         (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
       VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`,
    ).then(() => pg.query("INSERT INTO sessions.session_neon_census_seal (generation, census_rows, census_sha256, neon_snapshot) VALUES (repeat('e',64), 0, repeat('e',64), '{}'::jsonb)"));
  async function asRead(bound: { org: string; ws: string }, ws = KEY.workspaceId) {
    await pg.exec("BEGIN READ ONLY");
    try {
      await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [bound.org, bound.ws]);
      await pg.exec("SET LOCAL ROLE bulls96_read_api");
      return (await pg.query<{ f: boolean }>("SELECT sessions.session_neon_free_for($1, $2) AS f", [ws, KEY.sessionId])).rows[0]!.f;
    } finally {
      await pg.exec("ROLLBACK");
    }
  }

  test("the read role gets the same answer, tenant pinned; never-in-Neon only when proven", async () => {
    expect(await asRead({ org: KEY.orgId, ws: KEY.workspaceId })).toBe(false); // no seal
    await sealNow();
    expect(await asRead({ org: KEY.orgId, ws: KEY.workspaceId })).toBe(true);
    await expect(asRead({ org: KEY.orgId, ws: "ws-b" })).rejects.toThrow(/tenant binding/);
    await pg.query(
      "INSERT INTO sessions.ingest_frozen_source_records (org_id, workspace_id, origin_id, record_id, source, session_id) VALUES ($1, $2, 'o', 'r', 'claude', $3)",
      [KEY.orgId, KEY.workspaceId, KEY.sessionId],
    );
    expect(await asRead({ org: KEY.orgId, ws: KEY.workspaceId })).toBe(false);
    // Not callable by another role.
    await expect(pg.query("SELECT sessions.session_neon_free_for($1, $2)", [KEY.workspaceId, KEY.sessionId])).rejects.toThrow();
    // Caller pin: a role that holds EXECUTE but is not the read role is refused by the read
    // scope guard (session_directory_read_api_scope), tenant settings or not.
    await pg.exec("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'intruder') THEN CREATE ROLE intruder; END IF; END $$");
    await pg.exec("GRANT USAGE ON SCHEMA sessions TO intruder; GRANT EXECUTE ON FUNCTION sessions.session_neon_free_for(text,text) TO intruder");
    await pg.exec("BEGIN");
    try {
      await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [KEY.orgId, KEY.workspaceId]);
      await pg.exec("SET LOCAL ROLE intruder");
      await expect(pg.query("SELECT sessions.session_neon_free_for($1, $2)", [KEY.workspaceId, KEY.sessionId])).rejects.toThrow("caller is not the read api role");
    } finally {
      await pg.exec("ROLLBACK");
    }
  });
});

describe("item 3: every DB proof input is in the source revision (a change to any one invalidates reuse)", () => {
  const rev = async () =>
    (await pg.query<{ r: string }>("SELECT sessions.session_analysis_source_revision($1, $2, $3) AS r", [KEY.orgId, KEY.workspaceId, KEY.sessionId])).rows[0]!.r;
  test("seal, census seal, frozen record, census row, pointer created/deleted, manifest etag and the verdict each move it", async () => {
    const seen = new Set<string>([await rev()]);
    const step = async (label: string, sql: string, params: unknown[] = []) => {
      await pg.query(sql, params);
      const next = await rev();
      expect(next, label).toMatch(/^[0-9a-f]{64}$/);
      expect(seen.has(next), label).toBe(false);
      seen.add(next);
    };
    await step("frozen seal", `INSERT INTO sessions.ingest_frozen_source_completeness
      (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
      VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`);
    await step("census seal (verdict flips true)", "INSERT INTO sessions.session_neon_census_seal (generation, census_rows, census_sha256, neon_snapshot) VALUES (repeat('e',64), 0, repeat('e',64), '{}'::jsonb)");
    await step("frozen seal digest", "UPDATE sessions.ingest_frozen_source_completeness SET manifest_sha256 = repeat('9',64)");
    await step("census digest", "UPDATE sessions.session_neon_census_seal SET census_sha256 = repeat('8',64)");
    await step("frozen record presence", "INSERT INTO sessions.ingest_frozen_source_records (org_id, workspace_id, origin_id, record_id, source, session_id) VALUES ($1, 'other', 'o', 'r', 'claude', $2)", [KEY.orgId, KEY.sessionId]);
    await pg.query("DELETE FROM sessions.ingest_frozen_source_records");
    seen.add(await rev());
    await step("census presence", "WITH g AS (SELECT encode(sha256(convert_to(random()::text, 'UTF8')), 'hex') AS g), ins AS (INSERT INTO sessions.session_neon_census (generation, org_id, workspace_id, session_id) SELECT g.g, $1, 'other', $2 FROM g RETURNING generation) UPDATE sessions.session_neon_census_seal SET generation = (SELECT generation FROM ins LIMIT 1)", [KEY.orgId, KEY.sessionId]);
    await pg.query("UPDATE sessions.session_neon_census_seal SET generation = encode(sha256(convert_to(random()::text, 'UTF8')), 'hex')");
    seen.add(await rev());
    await step("pointer created_at", "UPDATE sessions.session_index SET created_at = created_at + interval '1 second'");
    await step("manifest etag (export/backfill markers live in the manifest)", "UPDATE sessions.session_index SET manifest_etag = 'etag-2'");
    await step("tombstoned pointer deleted_at", `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          storage_provider, bucket, manifest_key, content_sha256, raw_format, trace_id, revision, title, deleted_at)
       VALUES ($1, $2, $3, $4, 'm-t', 'u-1', now(), now(), 'fs', 'bucket', 'manifest/t',
               decode(repeat('ab', 32), 'hex'), 'jsonl', '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, 't', now())`, [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId]);
    await step("deleted_at moves", "UPDATE sessions.session_index SET deleted_at = deleted_at + interval '1 second' WHERE machine_id = 'm-t'");
  });
});

describe("item 2: the worker's result PUT is bounded inside its lease", () => {
  test("the deadline sits inside the 600 s lease and the sweeper's grace", async () => {
    const { ANALYSIS_PUT_TIMEOUT_MS } = await import("../results.ts");
    const { TICKET_GRACE_SECONDS } = await import("../store.ts");
    const { analysisConfig } = await import("../worker.ts");
    const lease = analysisConfig({}).leaseSeconds;
    expect(ANALYSIS_PUT_TIMEOUT_MS).toBeLessThan((lease * 1000) / 2);
    expect(TICKET_GRACE_SECONDS * 1000).toBeGreaterThan(lease * 1000 + ANALYSIS_PUT_TIMEOUT_MS);
  });

  test("a PUT that outlives its deadline is aborted (signal) and rejected; completion then refuses; no object", async () => {
    await request();
    const objects = results();
    let aborted = false;
    const stalling = {
      ...objects,
      put: (_k: unknown, _b: unknown, opts?: { signal?: AbortSignal }) =>
        new Promise<never>((_, reject) => opts?.signal?.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); })),
    } as typeof objects;
    const store = postgresAnalysisJobStore({ db: hoopDb(pg), store: stalling, anchorManifestKey: async () => ANCHOR, putTimeoutMs: 50 });
    const job = (await store.claim({ leaseSeconds: 600 }))!;
    const out = output();
    const { bytes, sha256 } = encodeAnalysisResult(out.result);
    const key = (await store.reserve(job, out, { sha256, bytes: bytes.length }))!;
    await expect(store.put(key, bytes, sha256)).rejects.toThrow("analysis_put_timeout");
    expect(aborted).toBe(true);
    expect(await objects.head(objectKey(key))).toBeUndefined();
    // The ticket stays for the sweeper (no completed owner).
    expect((await pg.query("SELECT reason FROM sessions.session_analysis_result_tickets WHERE result_key = $1", [key])).rows).toEqual([{ reason: "publishing" }]);
  });
});

describe("C3 purge (purge_session_analysis_for_owned): read + admission fences, in-flight refusal, durable every-version erase", () => {
  const ownedSet = async (user = "u-owner", org = KEY.orgId, ws = KEY.workspaceId, session = KEY.sessionId) => {
    await pg.exec(`CREATE TABLE IF NOT EXISTS sessions.ingest_frozen_source_owned_sessions(
      org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,
      source text NOT NULL,session_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id))`);
    await pg.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES ($1, $2, $3, 'origin', 'claude', $4)", [org, ws, user, session]);
  };
  const purge = (user = "u-owner") => pg.query("SELECT sessions.purge_session_analysis_for_owned($1, $2)", [KEY.orgId, user]);

  test("reads hide and admission refuses at once; an in-flight job can neither reserve nor complete; a fresh upload reopens", async () => {
    await ownedSet();
    const objects = results();
    const store = postgresAnalysisJobStore({ db: hoopDb(pg), store: objects, anchorManifestKey: async () => ANCHOR });
    await request();
    expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
    expect((await readAs()).analyses).toHaveLength(1);
    // A second, in-flight job (the completed one is too old to be reused).
    await pg.query("UPDATE sessions.session_analysis_jobs_v2 SET completed_at = now() - interval '1 hour'");
    await age("session_analysis_requests_v2", "3 minutes");
    await request();
    const inflight = (await store.claim({ leaseSeconds: 600 }))!;
    expect(inflight).toBeTruthy();
    const out = output();
    const { bytes, sha256 } = encodeAnalysisResult(out.result);
    const key = (await store.reserve(inflight, out, { sha256, bytes: bytes.length }))!;
    await store.put(key, bytes, sha256);

    await purge();
    // Immediately, before any object is gone:
    expect(await readAs()).toEqual({ outcome: "not_found" });
    await age("session_analysis_requests_v2", "3 minutes");
    expect(await request()).toEqual({ outcome: "not_found" });
    expect(await store.complete({ job: inflight, output: out, stored: { key, sha256, bytes: bytes.length } })).toBe(false);
    expect(await store.reserve(inflight, out, { sha256, bytes: bytes.length })).toBeNull();
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_jobs_v2")).rows).toHaveLength(0);
    // Every result key is ticketed for the every-version eraser.
    const tickets = (await pg.query<{ reason: string }>("SELECT reason FROM sessions.session_analysis_result_tickets")).rows;
    expect(tickets.length).toBeGreaterThanOrEqual(2);
    expect(tickets.every((t) => t.reason === "forgotten")).toBe(true);
    // A fresh upload after the purge (a new pointer) reopens admission.
    await pointer(pg, { machine: "m-new" });
    expect((await request()).outcome).toBe("accepted");
  });

  test("another user's or org's same-id session keeps its analysis and stays readable", async () => {
    await ownedSet("u-other", "org-other");
    const store = jobStore();
    await request();
    expect((await publish(store, (await store.claim({ leaseSeconds: 600 }))!)).stage).toBe("completed");
    await purge("u-owner"); // u-owner owns nothing here
    await pg.query("SELECT sessions.purge_session_analysis_for_owned('org-other', 'u-other')");
    expect((await readAs()).analyses).toHaveLength(1);
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_purge_fences")).rows).toEqual([{ "?column?": 1 }]);
    expect((await pg.query<{ o: string }>("SELECT org_id o FROM sessions.session_analysis_purge_fences")).rows).toEqual([{ o: "org-other" }]);
  });

  test("durable: the result object's ticket survives an eraser failure and a later sweep leaves zero versions", async () => {
    await ownedSet();
    const objects = results();
    const store = jobStore(objects);
    await request();
    const { key } = await publish(store, (await store.claim({ leaseSeconds: 600 }))!);
    // A versioned bucket: the object plus one older version of it.
    const versions = new Map<string, number>([[key!, 2]]);
    let failures = 1;
    const eraseObject = async (k: string) => {
      if (failures-- > 0) throw new Error("S3 503");
      const n = versions.get(k) ?? 0;
      versions.delete(k);
      await objects.delete(objectKey(k));
      return n;
    };
    await purge();
    await expect(sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject })).rejects.toThrow("S3 503");
    expect((await pg.query("SELECT result_key FROM sessions.session_analysis_result_tickets")).rows).toEqual([{ result_key: key }]);
    expect(await sweepAnalysisTickets(objects, { graceSeconds: 0, transact: tx, eraseObject })).toBe(1);
    expect(versions.get(key!) ?? 0).toBe(0);
    expect(await objects.head(objectKey(key!))).toBeUndefined();
    expect((await pg.query("SELECT 1 FROM sessions.session_analysis_result_tickets")).rows).toHaveLength(0);
  });

  test("the sweeper refuses to run without the every-version eraser", async () => {
    await expect(sweepAnalysisTickets(results(), { graceSeconds: 0, transact: tx } as never)).rejects.toThrow("analysis_sweep_eraser_required");
  });
});
