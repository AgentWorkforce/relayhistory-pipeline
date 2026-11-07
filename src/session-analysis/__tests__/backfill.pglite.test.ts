/**
 * Backfill + parity (F1.7/F1.8) on the real migration: a Neon v1 payload (the shape
 * read_session_analysis_v1 serves, brief-era md5 ids and microsecond timestamps
 * included) goes in, SP's GET comes out byte-equal under canonical JSON.
 */
import { expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { database, hoopDb, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import { applyBackfill, BackfillError, parityEqual, planBackfill, type NeonV1Payload } from "../backfill.ts";
import { analysisResultPrefix, readAnalysisResult } from "../results.ts";
import { assembleAnalyses, type ReadRpc } from "../read.ts";

setDefaultTimeout(60_000);
const SCOPE = { orgId: KEY.orgId, workspaceId: KEY.workspaceId, sessionId: KEY.sessionId };
const V1 = "6f0c3a52-8d2a-5b3e-a4e1-9f1e0d2c3b4a";
const NEON: NeonV1Payload = {
  outcome: "ok",
  sessionId: KEY.sessionId,
  requests: [
    { id: "22222222-2222-4222-8222-222222222222", sessionId: KEY.sessionId, status: "completed", analysisId: V1, error: null,
      createdAt: "2026-10-01T00:00:00.123456+00:00", updatedAt: "2026-10-01T00:00:03.5+00:00" },
    { id: "33333333-3333-4333-8333-333333333333", sessionId: KEY.sessionId, status: "failed", analysisId: null,
      error: "session_evidence_incomplete", createdAt: "2026-09-30T00:00:00+00:00", updatedAt: "2026-09-30T00:00:01+00:00" },
  ],
  analyses: [
    { id: V1, sessionId: KEY.sessionId, sourceProvider: "claude", sourceFingerprint: "abc", status: "completed", attempts: 1,
      analysisVersion: "session-analysis-v2.4:relayhistory-session-v2", schemaVersion: 2,
      summaryVersion: "session-summary-v3:workspace-output-v2", modelVersion: "gpt-5.4", summaryModelVersion: "gpt-5.6-luna",
      result: { schemaVersion: 1, analysisVersion: "session-analysis-v2.4:relayhistory-session-v2", sourceFingerprint: "abc",
                analysis: { title: "Fix the queue" }, durationMs: 12, brief: { title: "Fix the queue", state: "completed" } },
      error: null, createdAt: "2026-10-01T00:00:00.2+00:00", updatedAt: "2026-10-01T00:00:03.5+00:00",
      completedAt: "2026-10-01T00:00:03.5+00:00" },
  ],
};

test("backfilled history is served byte-equal to Neon, idempotently", async () => {
  const pg = await database();
  await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(pg);
  const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-bf-")) });
  const prefix = analysisResultPrefix("org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json");
  const plan = planBackfill(SCOPE, NEON);
  expect(await applyBackfill(hoopDb(pg), store, SCOPE, prefix, plan)).toEqual({ jobs: 1, requests: 2, objects: 1 });
  await applyBackfill(hoopDb(pg), store, SCOPE, prefix, plan);
  expect((await pg.query("SELECT 1 FROM sessions.session_analysis_requests_v2")).rows).toHaveLength(2);

  await pg.exec("BEGIN");
  await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [KEY.orgId, KEY.workspaceId]);
  await pg.exec("SET LOCAL ROLE bulls96_read_api");
  const rpc = (await pg.query<{ r: ReadRpc }>("SELECT sessions.analysis_read_v1($1, $2) AS r", [KEY.workspaceId, KEY.sessionId])).rows[0]!.r;
  await pg.exec("ROLLBACK");
  if (rpc.outcome !== "ok") throw new Error("expected ok");
  const sp = await assembleAnalyses(rpc, { result: (ref) => readAnalysisResult(store, ref) });
  expect(parityEqual(NEON, sp)).toEqual({ equal: true, requests: 2, analyses: 1 });
  // A changed byte is caught, and the verdict carries no content.
  const drift = structuredClone(sp);
  (drift.analyses[0] as any).attempts = 2;
  expect(parityEqual(NEON, drift)).toEqual({ equal: false, requests: 2, analyses: 1 });
});

test("the planner refuses what it cannot keep exactly", () => {
  const bad = (mut: (p: any) => void) => {
    const p = structuredClone(NEON) as any;
    mut(p);
    return () => planBackfill(SCOPE, p);
  };
  expect(bad((p) => (p.sessionId = "other"))).toThrow(BackfillError);
  expect(bad((p) => (p.analyses[0].result = null))).toThrow("backfill_completed_without_result");
  expect(bad((p) => (p.requests[0].analysisId = "44444444-4444-4444-8444-444444444444"))).toThrow("backfill_request_job_unknown");
  expect(planBackfill(SCOPE, { outcome: "not_found" })).toEqual({ jobs: [], requests: [] });
  expect(parityEqual({ outcome: "not_found" }, null).equal).toBe(true);
});
