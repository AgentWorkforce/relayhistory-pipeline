/**
 * sqlAnalysisSource over the real migration (PGlite, full plan): the read role reads
 * relayhistory's v1 rows through a tenant-bound READ ONLY transaction, and the route
 * serves them with the verified result object.
 */
import { expect, setDefaultTimeout, test } from "bun:test";
import { database, hoopDb, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TenantRunner } from "../liveness.ts";
import { sqlAnalysisSource } from "../analysis-source.ts";
import { analysisResultPrefix, writeAnalysisResult } from "../../session-analysis/results.ts";
import { postgresAnalysisJobStore } from "../../session-analysis/store.ts";
import { createReadApi } from "../app.ts";

setDefaultTimeout(60_000);

test("the read role serves a completed analysis end to end", async () => {
  const pg = await database();
  await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(pg);
  await pg.query("SELECT sessions.analysis_request_v1($1, $2, 'u-1', $3)", [KEY.orgId, KEY.workspaceId, KEY.sessionId]);
  const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-src-")) });
  const jobs = postgresAnalysisJobStore({ db: hoopDb(pg), store, anchorManifestKey: async () => "" });
  const job = (await jobs.claim({ leaseSeconds: 600 }))!;
  const result = { schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: { title: "T" }, durationMs: 1 } as never;
  const stored = await writeAnalysisResult(store, analysisResultPrefix("org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json"), job.id, result);
  await jobs.complete({ jobId: job.id, leaseToken: job.leaseToken, stored, output: { harness: "claude", sourceFingerprint: "f".repeat(64), sourceGeneration: "e".repeat(64), inputTokens: 1, outputTokens: 1, result } });

  const run: TenantRunner = async (org, ws, work) => {
    await pg.exec("BEGIN READ ONLY");
    try {
      await pg.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [org, ws]);
      await pg.exec("SET LOCAL ROLE bulls96_read_api");
      return await work({ query: async (t, p) => ({ rows: (await pg.query(t, p as never)).rows as never[] }) });
    } finally {
      await pg.exec("ROLLBACK");
    }
  };
  const app = createReadApi({
    verify: async () => ({ userId: "u-1", orgId: KEY.orgId, workspaceId: KEY.workspaceId, scopes: ["rth:read"] }),
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set([KEY.workspaceId]),
    analysis: sqlAnalysisSource(run, store),
  });
  const r = await app.request(`/v1/sessions/${KEY.sessionId}/analysis?workspace=${KEY.workspaceId}`, { headers: { authorization: "Bearer t" } });
  expect(r.status).toBe(200);
  const b = (await r.json()) as Record<string, any>;
  expect(b.requests).toHaveLength(1);
  expect(b.analyses[0]).toMatchObject({ id: job.id, status: "completed", sourceProvider: "claude", result });
  const missing = await app.request(`/v1/sessions/nope/analysis?workspace=${KEY.workspaceId}`, { headers: { authorization: "Bearer t" } });
  expect(missing.status).toBe(404);
});
