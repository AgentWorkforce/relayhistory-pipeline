/**
 * GET / POST /v1/sessions/:id/analysis (lane F1): the canary allowlist, scopes, the
 * relayhistory outcome map, result verification, the native brief merge, and the exact
 * relayhistory v1 shape Cloud's consumers read (sessionAnalysesResponse fields).
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type AnalysisDeps, type VerifiedIdentity } from "../app.ts";
import type { ReadRpc, ReadRpcAnalysis } from "../../session-analysis/read.ts";
import { AnalysisResultError } from "../../session-analysis/results.ts";

const base: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const KEY = `org-a/ws-a/m/claude/2026-11-05/s-1/normalized/session-analysis-v2/0b6f3a3e-6a59-4d5c-9b1e-2f7f6c1d9a10/${"a".repeat(64)}.json`;
const row = (over: Partial<ReadRpcAnalysis> = {}): ReadRpcAnalysis => ({
  id: "0b6f3a3e-6a59-4d5c-9b1e-2f7f6c1d9a10", sessionId: "s-1", sourceProvider: "claude", sourceFingerprint: "f",
  status: "completed", attempts: 1, analysisVersion: "session-analysis-v2.4:96bulls-s3-v1", schemaVersion: 2,
  summaryVersion: "", modelVersion: "gpt-5.4", summaryModelVersion: "",
  resultRef: { key: KEY, sha256: "a".repeat(64), bytes: 10 }, origin: "native", harness: "claude", error: null,
  createdAt: "2026-11-05T12:00:00.000000+00:00", updatedAt: "2026-11-05T12:00:01.000000+00:00",
  completedAt: "2026-11-05T12:00:01.000000+00:00", ...over,
});
const REQ = { id: "r-1", sessionId: "s-1", status: "completed", analysisId: row().id, error: null, createdAt: "x", updatedAt: "y" };

function api(over: { analysis?: Partial<AnalysisDeps> | null; rpc?: ReadRpc; scopes?: string[]; workspaces?: ReadonlySet<string> | "*" } = {}) {
  const identity = { ...base, scopes: over.scopes ?? base.scopes };
  const seen: unknown[] = [];
  const analysis: AnalysisDeps | undefined =
    over.analysis === null
      ? undefined
      : {
          read: async (_i, s) => (seen.push(["read", s]), over.rpc ?? { outcome: "ok", sessionId: "s-1", requests: [REQ], analyses: [row()] }),
          result: async (ref) => (seen.push(["result", ref.key]), { schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: { title: "T" }, durationMs: 1 }),
          ...over.analysis,
        };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return identity;
    },
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: "workspaces" in over ? over.workspaces : new Set(["ws-a"]),
    ...(analysis ? { analysis } : {}),
  });
  const call = (method: "GET" | "POST", path = "/v1/sessions/s-1/analysis?workspace=ws-a") =>
    app.request(path, { method, headers: { authorization: "Bearer good" } });
  return { call, seen };
}
const json = async (r: Response) => (await r.json()) as Record<string, any>;

describe("GET /v1/sessions/:id/analysis", () => {
  test("relayhistory's v1 envelope: refs replaced by the verified result, no internal keys", async () => {
    const { call, seen } = api();
    const r = await call("GET");
    expect(r.status).toBe(200);
    const b = await json(r);
    expect(Object.keys(b).sort()).toEqual(["analyses", "correlationId", "requests", "sessionId"]);
    expect(b.requests).toEqual([REQ]);
    expect(Object.keys(b.analyses[0]).sort()).toEqual(
      ["analysisVersion", "attempts", "completedAt", "createdAt", "error", "id", "modelVersion", "result", "schemaVersion",
       "sessionId", "sourceFingerprint", "sourceProvider", "status", "summaryModelVersion", "summaryVersion", "updatedAt"].sort(),
    );
    expect(b.analyses[0].result.analysis.title).toBe("T");
    expect(seen).toEqual([["read", "s-1"], ["result", KEY]]);
  });

  test("a native result gets the brief half; a backfilled one is served verbatim", async () => {
    const brief = { title: "B", description: "D", state: "completed" };
    const merged = await json(await api({ analysis: { brief: () => async () => brief } }).call("GET"));
    expect(merged.analyses[0].result.brief).toEqual(brief);
    const backfill = await json(
      await api({
        analysis: { brief: () => async () => brief },
        rpc: { outcome: "ok", sessionId: "s-1", requests: [], analyses: [row({ origin: "neon_backfill" })] },
      }).call("GET"),
    );
    expect(backfill.analyses[0].result.brief).toBeUndefined();
  });

  test("queued and failed analyses carry a null result and read no object", async () => {
    const { call, seen } = api({
      rpc: { outcome: "ok", sessionId: "s-1", requests: [], analyses: [row({ status: "queued", resultRef: null }), row({ id: "j2", status: "failed", resultRef: null, error: "model_refusal" })] },
    });
    const b = await json(await call("GET"));
    expect(b.analyses.map((a: any) => [a.status, a.result, a.error])).toEqual([["queued", null, null], ["failed", null, "model_refusal"]]);
    expect(seen).toEqual([["read", "s-1"]]);
  });

  test("not found, canary, configuration, workspace attestation", async () => {
    expect((await api({ rpc: { outcome: "not_found" } }).call("GET")).status).toBe(404);
    const off = await api({ workspaces: new Set() }).call("GET");
    expect([off.status, (await json(off)).error.reason]).toEqual([501, "analysis_workspace_not_enabled"]);
    const none = await api({ analysis: null }).call("GET");
    expect([none.status, (await json(none)).error.reason]).toEqual([501, "analysis_not_configured"]);
    expect((await api().call("GET", "/v1/sessions/s-1/analysis?workspace=ws-b")).status).toBe(403);
  });

  test("a missing, altered or misplaced result object is a 503, never a partial answer", async () => {
    const missing = api({ analysis: { result: async () => { throw new AnalysisResultError("analysis_result_mismatch"); } } });
    expect((await missing.call("GET")).status).toBe(503);
    const misplaced = api({ rpc: { outcome: "ok", sessionId: "s-1", requests: [], analyses: [row({ resultRef: { key: "../other.json", sha256: "a".repeat(64), bytes: 1 } })] } });
    expect((await misplaced.call("GET")).status).toBe(503);
    expect(misplaced.seen).toEqual([["read", "s-1"]]);
  });
});

describe("POST /v1/sessions/:id/analysis", () => {
  test("needs rth:sync and the admission principal", async () => {
    expect((await api().call("POST")).status).toBe(403);
    const r = await api({ scopes: ["rth:read", "rth:sync"] }).call("POST");
    expect([r.status, (await json(r)).error.reason]).toEqual([501, "analysis_admission_not_configured"]);
  });

  test("relayhistory's outcome map", async () => {
    const at = (outcome: any) =>
      api({ scopes: ["rth:read", "rth:sync"], analysis: { admit: async () => outcome } }).call("POST");
    const ok = await at({ outcome: "accepted", request: REQ });
    expect([ok.status, (await json(ok)).request]).toEqual([202, REQ]);
    expect((await at({ outcome: "not_found" })).status).toBe(404);
    const limited = await at({ outcome: "rate_limited" });
    expect([limited.status, limited.headers.get("retry-after"), (await json(limited)).error.code]).toEqual([429, "86400", "rate_limited"]);
    expect((await at({ outcome: "invalid_request" })).status).toBe(400);
  });
});
