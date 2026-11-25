/**
 * READ_API_ANALYSIS_BUCKET on LOCKER_PROVIDER=azure (lambda.ts readApiAnalysisBucket): the S3 result reader does not
 * apply, so analysis stays unconfigured (the route's 501 analysis_not_configured), no S3 client is built, and one
 * startup warning carries the fixed code read_api_analysis_bucket_ignored_on_azure. The Azure store path is S2b.
 * Every other provider keeps the bucket exactly as before.
 */
import { afterEach, expect, test } from "bun:test";
import { readApiAnalysisBucket, readApiAnalysisSource } from "../lambda.ts";
import { AuthError, createReadApi } from "../app.ts";

const write = process.stderr.write.bind(process.stderr);
afterEach(() => { process.stderr.write = write; });

function capture(fn: () => unknown): { value: unknown; lines: string[] } {
  const lines: string[] = [];
  process.stderr.write = ((chunk: string | Uint8Array) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write;
  try { return { value: fn(), lines }; } finally { process.stderr.write = write; }
}

test("azure: the bucket is ignored with one coded warning (no S3 client, analysis unconfigured)", () => {
  const { value, lines } = capture(() => readApiAnalysisBucket({ LOCKER_PROVIDER: "azure", READ_API_ANALYSIS_BUCKET: "fx-analysis-bucket" }));
  expect(value).toBeUndefined();
  expect(lines).toHaveLength(1);
  const line = JSON.parse(lines[0]!);
  expect(line).toMatchObject({ level: "warn", reason: "read_api_analysis_bucket_ignored_on_azure" });
  expect(lines[0]).not.toContain("fx-analysis-bucket");
});

test("unset LOCKER_PROVIDER, s3, gcs and fs keep the bucket unchanged and log nothing; unset bucket stays unset", () => {
  for (const provider of [undefined, "s3", "gcs", "fs"]) {
    const { value, lines } = capture(() => readApiAnalysisBucket({ ...(provider ? { LOCKER_PROVIDER: provider } : {}), READ_API_ANALYSIS_BUCKET: " fx-analysis-bucket " }));
    expect([provider, value, lines]).toEqual([provider, "fx-analysis-bucket", []]);
  }
  const { value, lines } = capture(() => readApiAnalysisBucket({ LOCKER_PROVIDER: "azure" }));
  expect([value, lines]).toEqual([undefined, []]);
});

test("end to end on azure: no S3 store is built, and GET and POST /v1/sessions/:id/analysis answer 501 analysis_not_configured", async () => {
  let stores = 0;
  const run = (() => { throw new Error("no database work on an unconfigured analysis source"); }) as never;
  const { value: analysis } = capture(() => readApiAnalysisSource({ LOCKER_PROVIDER: "azure", READ_API_ANALYSIS_BUCKET: "fx-analysis-bucket" }, run, undefined,
    (() => { stores++; throw new Error("no S3 client on azure"); }) as never));
  expect(analysis).toBeUndefined();
  expect(stores).toBe(0);
  const app = createReadApi({
    verify: async (t) => { if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid"); return { userId: "u-1", orgId: "fx-org", workspaceId: "fx-ws", scopes: ["rth:read", "rth:sync"] }; },
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set(["fx-ws"]),
    ...(analysis ? { analysis: analysis as never } : {}),
  } as never);
  for (const method of ["GET", "POST"] as const) {
    const r = await app.request("/v1/sessions/fx-session/analysis?workspace=fx-ws", { method, headers: { authorization: "Bearer good" } });
    expect([method, r.status, ((await r.json()) as { error: { reason: string } }).error.reason]).toEqual([method, 501, "analysis_not_configured"]);
  }
});

test("off azure the source is built over the S3 store exactly as before", () => {
  const made: Record<string, unknown>[] = [];
  const source = readApiAnalysisSource({ READ_API_ANALYSIS_BUCKET: "fx-analysis-bucket", AWS_REGION: "us-east-1" }, (() => {}) as never, undefined,
    ((c: Record<string, unknown>) => { made.push(c); return {} as never; }) as never);
  expect(source).toBeDefined();
  expect(made.map((c) => [c.bucket, c.region, Object.keys(c).sort()])).toEqual([["fx-analysis-bucket", "us-east-1", ["bucket", "credentials", "region"]]]);
});
