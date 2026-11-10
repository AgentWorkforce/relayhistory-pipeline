/**
 * sqlAnalysisSource over the real migration (PGlite, full plan), end to end through the
 * route: POST admits as the separate admission principal (bulls96_analysis_admit) with the
 * tenant bound, the worker publishes through reserve -> put -> complete, and GET reads as
 * the read role (bulls96_read_api) with the verified result object.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, hoopDb, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TenantRunner } from "../liveness.ts";
import { sqlAnalysisSource, type AdmitPool } from "../analysis-source.ts";
import { encodeAnalysisResult } from "../../session-analysis/results.ts";
import { postgresAnalysisJobStore } from "../../session-analysis/store.ts";
import { createReadApi } from "../app.ts";
import { handleUrlEvent } from "../lambda.ts";

setDefaultTimeout(60_000);
let pg: PGlite | undefined;
afterEach(async () => {
  await pg?.close();
  pg = undefined;
});

const roleRunner = (db: PGlite, role: string, readOnly: boolean) => {
  const q = { query: async (t: string, p?: unknown[]) => ({ rows: (await db.query(t, p as never)).rows as never[] }) };
  const run: TenantRunner = async (org, ws, work) => {
    await db.exec(readOnly ? "BEGIN READ ONLY" : "BEGIN");
    try {
      await db.query("SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)", [org, ws]);
      await db.exec(`SET LOCAL ROLE ${role}`);
      return await work(q);
    } finally {
      await db.exec("ROLLBACK");
    }
  };
  // The admission pool: a client whose session runs as the admission role.
  const admit: AdmitPool = {
    async connect() {
      return {
        async query(t: string, p?: unknown[]) {
          if (t === "BEGIN") {
            await db.exec("BEGIN");
            await db.exec(`SET LOCAL ROLE ${role}`);
            return { rows: [] };
          }
          return { rows: (await db.query(t, p as never)).rows as never[] };
        },
        release() {},
      };
    },
  };
  return { run, admit };
};

test("POST admits as the admission principal; GET serves the published analysis as the read role", async () => {
  const db = (pg = await database());
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await db.exec(read("supabase/bootstrap/session_analysis_admit_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(db);
  const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "analysis-src-")) });
  const reader = roleRunner(db, "bulls96_read_api", true);
  const admitter = roleRunner(db, "bulls96_analysis_admit", false);
  const app = createReadApi({
    verify: async () => ({ userId: "u-1", orgId: KEY.orgId, workspaceId: KEY.workspaceId, scopes: ["rth:read", "rth:sync"] }),
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set([KEY.workspaceId]),
    analysis: sqlAnalysisSource(reader.run, store, admitter.admit),
  });
  const path = `/v1/sessions/${KEY.sessionId}/analysis?workspace=${KEY.workspaceId}`;
  const posted = await app.request(path, { method: "POST", headers: { authorization: "Bearer t" } });
  expect(posted.status).toBe(202);
  const request = ((await posted.json()) as Record<string, any>).request;

  const jobs = postgresAnalysisJobStore({ db: hoopDb(db), store, anchorManifestKey: async () => "org-a/ws-a/m-1/claude/2026-10-29/s-1/manifest.json" });
  const job = (await jobs.claim({ leaseSeconds: 600 }))!;
  expect(job.id).toBe(request.analysisId);
  const output = { harness: "claude", sourceFingerprint: "f".repeat(64), sourceGeneration: "e".repeat(64), inputTokens: 1, outputTokens: 1,
    result: { schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: { title: "T" }, durationMs: 1, brief: { title: "B" } } as never };
  const { bytes, sha256 } = encodeAnalysisResult(output.result);
  const key = (await jobs.reserve(job, output, { sha256, bytes: bytes.length }))!;
  await jobs.put(key, bytes, sha256);
  expect(await jobs.complete({ job, output, stored: { key, sha256, bytes: bytes.length } })).toBe(true);

  const r = await app.request(`${path}&source=claude`, { headers: { authorization: "Bearer t" } });
  expect(r.status).toBe(200);
  const b = (await r.json()) as Record<string, any>;
  expect(b.requests).toHaveLength(1);
  expect(b.analyses[0]).toMatchObject({ id: job.id, status: "completed", sourceProvider: "claude", result: output.result });
  expect((await app.request(`/v1/sessions/nope/analysis?workspace=${KEY.workspaceId}`, { headers: { authorization: "Bearer t" } })).status).toBe(404);
});

test("P0-1: the read Lambda forwards POST only for the analysis path, with no body", async () => {
  const seen: string[] = [];
  const app = { fetch: async (req: Request) => (seen.push(`${req.method} ${new URL(req.url).pathname}`), new Response("{}", { status: 202 })) };
  const ev = (method: string, rawPath: string, body?: string) => ({ rawPath, headers: {}, requestContext: { http: { method } }, ...(body ? { body } : {}) });
  expect((await handleUrlEvent(app, ev("POST", "/v1/sessions/s-1/analysis"))).statusCode).toBe(202);
  expect((await handleUrlEvent(app, ev("POST", "/v1/sessions/s-1/events"))).statusCode).toBe(405);
  expect((await handleUrlEvent(app, ev("DELETE", "/v1/sessions/s-1/analysis"))).statusCode).toBe(405);
  expect((await handleUrlEvent(app, ev("POST", "/v1/sessions/s-1/analysis", "{\"model\":\"x\"}"))).statusCode).toBe(400);
  expect(seen).toEqual(["POST /v1/sessions/s-1/analysis"]);
});
