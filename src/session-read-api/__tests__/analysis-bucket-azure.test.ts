/**
 * The read API's analysis source on LOCKER_PROVIDER=azure (lambda.ts readApiAnalysisSource, slice S2b): the Azure
 * store is built from the runner's own settings parser (src/shared/azure-blob-settings.ts), Shared Key, never
 * following a redirect; READ_API_ANALYSIS_BUCKET must name the same container. Invalid settings or another container
 * leave analysis unconfigured (501) with one coded warning, never a failed start. Results are the current object at
 * the job's result key, verified by digest and size: a different later version is unavailable, never served.
 * Off azure the S3 source is built exactly as before.
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { READER_LOGIN_SQL, readApiAnalysisBucket, readApiAnalysisSource, readApiDepsFromSecret } from "../lambda.ts";
import { selfhostSearchComposition } from "../server.ts";
import { READER_LOGIN_ROW, readerLoginAnswer } from "./reader-login-support.ts";
import { AuthError, createReadApi, type AnalysisDeps } from "../app.ts";
import { createAzureBlobStore, type AzureBlobConfig } from "../../../packages/object-store/src/azure.ts";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import { azureNoRedirectFetch } from "../../shared/azure-blob-settings.ts";
import { FAKE_ENDPOINT, FakeAzureBlob } from "../../migration/__tests__/fake-azure-blob.ts";

const write = process.stderr.write.bind(process.stderr);
let dir = "", KEY = "", keyFile = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "read-api-azure-"));
  KEY = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
  keyFile = join(dir, "account-key");
  writeFileSync(keyFile, KEY);
});
afterEach(() => { process.stderr.write = write; rmSync(dir, { recursive: true, force: true }); });

function capture<T>(fn: () => T): { value: T; lines: string[] } {
  const lines: string[] = [];
  process.stderr.write = ((chunk: string | Uint8Array) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write;
  try { return { value: fn(), lines }; } finally { process.stderr.write = write; }
}

const azureEnv = (extra: Record<string, string | undefined> = {}) => {
  const env: Record<string, string | undefined> = {
    LOCKER_PROVIDER: "azure", READ_API_ANALYSIS_BUCKET: "fx-container",
    SESSION_ANALYSIS_AZURE_ACCOUNT: "fxacct", SESSION_ANALYSIS_AZURE_CONTAINER: "fx-container",
    SESSION_ANALYSIS_AZURE_ENDPOINT: FAKE_ENDPOINT, SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: keyFile, ...extra,
  };
  for (const k of Object.keys(env)) if (env[k] === undefined) delete env[k];
  return env as NodeJS.ProcessEnv;
};
const noRun = (() => { throw new Error("no database work in this test"); }) as never;
const noS3 = (() => { throw new Error("no S3 client on azure"); }) as never;

test("azure: the Azure store is built from the shared settings (Shared Key, no-redirect fetch); no S3 client, no warning", () => {
  const made: AzureBlobConfig[] = [];
  const { value, lines } = capture(() => readApiAnalysisSource(azureEnv(), noRun, undefined, noS3, (c) => { made.push(c); return {} as ObjectStore; }));
  expect(value).toBeDefined();
  expect(lines).toEqual([]);
  expect(made).toHaveLength(1);
  expect(made[0]).toMatchObject({ account: "fxacct", container: "fx-container", endpoint: FAKE_ENDPOINT, auth: { kind: "sharedKey", accountKey: KEY } });
  expect(typeof made[0]!.fetch).toBe("function");
  expect("versionIds" in made[0]!).toBe(false);
});

test("azure: invalid settings or another container leave analysis unconfigured with one coded warning (no store, no key in the log)", () => {
  const cases: [Record<string, string | undefined>, string, string | undefined][] = [
    [{ SESSION_ANALYSIS_AZURE_ACCOUNT: "Fx-Acct" }, "read_api_analysis_azure_settings_invalid", "azure_account_invalid"],
    [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: undefined }, "read_api_analysis_azure_settings_invalid", "azure_account_key_missing"],
    [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: "AAAA" }, "read_api_analysis_azure_settings_invalid", "azure_account_key_plaintext"],
    [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://fxacct.blob.example.test" }, "read_api_analysis_azure_settings_invalid", "azure_endpoint_invalid"],
    [{ READ_API_ANALYSIS_BUCKET: "fx-other-container" }, "read_api_analysis_container_mismatch", undefined],
  ];
  for (const [extra, reason, errorCode] of cases) {
    let stores = 0;
    const { value, lines } = capture(() => readApiAnalysisSource(azureEnv(extra), noRun, undefined, noS3, () => { stores++; return {} as ObjectStore; }));
    expect([reason, value, stores, lines.length]).toEqual([reason, undefined, 0, 1]);
    const line = JSON.parse(lines[0]!);
    expect(line).toMatchObject({ level: "warn", reason, ...(errorCode ? { error_code: errorCode } : {}) });
    expect(lines[0]).not.toContain(KEY.slice(0, 16));
  }
  // No bucket: unconfigured and silent, as before.
  const { value, lines } = capture(() => readApiAnalysisSource(azureEnv({ READ_API_ANALYSIS_BUCKET: undefined }), noRun, undefined, noS3, noS3));
  expect([value, lines]).toEqual([undefined, []]);
});

test("off azure the S3 source is built exactly as before (bucket, region, credentials) and no Azure setting is read", () => {
  for (const provider of [undefined, "s3", "gcs", "fs"]) {
    const made: Record<string, unknown>[] = [];
    const env = { ...(provider ? { LOCKER_PROVIDER: provider } : {}), READ_API_ANALYSIS_BUCKET: " fx-analysis-bucket ", AWS_REGION: "us-east-1",
      SESSION_ANALYSIS_AZURE_ACCOUNT: "Not-Valid" };
    const { value, lines } = capture(() => readApiAnalysisSource(env, noRun, undefined,
      ((c: Record<string, unknown>) => { made.push(c); return {} as never; }) as never, () => { throw new Error("no Azure store off azure"); }));
    expect([provider, value !== undefined, lines]).toEqual([provider, true, []]);
    expect(made.map((c) => [c.bucket, c.region, Object.keys(c).sort()])).toEqual([["fx-analysis-bucket", "us-east-1", ["bucket", "credentials", "region"]]]);
  }
  expect(readApiAnalysisBucket({})).toBeUndefined();
  expect(readApiAnalysisBucket({ READ_API_ANALYSIS_BUCKET: "  " })).toBeUndefined();
});

// ---- end to end over the fake Azure container ------------------------------------------------------------

const JOB = "0b6f3a3e-6a59-4d5c-9b1e-2f7f6c1d9a10";
function api(analysis: AnalysisDeps) {
  const app = createReadApi({
    verify: async (t) => { if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid"); return { userId: "u-1", orgId: "fx-org", workspaceId: "fx-ws", scopes: ["rth:read", "rth:sync"] }; },
    ch: { database: "x", async command() {}, async insert() {}, async select() { throw new Error("no ClickHouse"); } },
    liveness: { async erased() { return new Set(); }, async livePointerSessions() { return new Set(); }, async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; } },
    completeSinceMs: 0,
    directoryWorkspaces: new Set(["fx-ws"]),
    analysis,
  } as never);
  return (method: "GET" | "POST") => app.request("/v1/sessions/fx-session/analysis?workspace=fx-ws", { method, headers: { authorization: "Bearer good" } });
}

test("end to end on azure: a completed result is served from the Azure store; a DIFFERENT later version at the key is unavailable (503), never served; POST admits", async () => {
  const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY });
  const bytes = new TextEncoder().encode(JSON.stringify({ schemaVersion: 1, analysisVersion: "v", sourceFingerprint: "f", analysis: { title: "fx-owned" }, durationMs: 1 }));
  const sha = createHash("sha256").update(bytes).digest("hex");
  const key = `fxroot/fx-org/v1/ws=fx-ws/m=fx/h=claude/d=2026/11/05/s=fx-session/normalized/session-analysis-v2/${JOB}/${sha}.json`;
  fake.put(key, new TextDecoder().decode(bytes));
  const admitted: string[] = [];
  const admitPool = {
    async connect() {
      return {
        async query(text: string) {
          if (text.includes("analysis_request_v1")) { admitted.push("admit"); return { rows: [{ r: { outcome: "accepted", request: { id: "fx-request" } } }] }; }
          return { rows: [] };
        },
        release() {},
      };
    },
  };
  // The same store the source builds, with its no-redirect fetch put in front of the fake container.
  const source = readApiAnalysisSource(azureEnv(), noRun, admitPool as never, noS3,
    (c) => createAzureBlobStore({ ...c, fetch: azureNoRedirectFetch(fake.fetch) }) as unknown as ObjectStore)!;
  expect(source).toBeDefined();
  const row = { id: JOB, sessionId: "fx-session", sourceProvider: "claude", sourceFingerprint: "f", status: "completed", attempts: 1,
    analysisVersion: "v", schemaVersion: 2, summaryVersion: "", modelVersion: "m", summaryModelVersion: "",
    resultRef: { key, sha256: sha, bytes: bytes.length }, origin: "native", harness: "claude", error: null,
    createdAt: "x", updatedAt: "y", completedAt: "z" };
  const call = api({
    ...source,
    read: async () => ({ outcome: "ok", sessionId: "fx-session", requests: [], analyses: [row] }) as never,
    harnesses: async () => ["claude"],
  });
  const ok = await call("GET");
  expect(ok.status).toBe(200);
  expect(((await ok.json()) as { analyses: { result: { analysis: { title: string } } }[] }).analyses[0]!.result.analysis.title).toBe("fx-owned");

  // Something outside the runner writes different bytes at the same key: the read is refused, never served.
  fake.put(key, JSON.stringify({ analysis: { title: "fx-foreign" } }));
  const refused = await call("GET");
  const text = await refused.text();
  expect(refused.status).toBe(503);
  expect(text).toContain("backend_unavailable");
  expect(text).not.toContain("fx-foreign");

  const post = await call("POST");
  expect(post.status).toBe(202);
  expect(admitted).toEqual(["admit"]);
  expect(fake.requests.every((r) => r.redirect === "manual")).toBe(true);
});

test("the DEFAULT Azure store wiring never follows a redirect (real fetch, two local origins): nothing reaches the second origin", async () => {
  let reachedB = 0, reachedA = 0;
  const b = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => { reachedB++; return new Response("fx-foreign"); } });
  const a = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: (req) => {
    reachedA++;
    return new Response(null, { status: 302, headers: { location: `http://127.0.0.1:${b.port}${new URL(req.url).pathname}` } });
  } });
  try {
    const source = readApiAnalysisSource(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: `http://127.0.0.1:${a.port}/fxacct` }), noRun, undefined, noS3)!;
    expect(source).toBeDefined();
    const ref = { key: `fxroot/fx-org/v1/normalized/session-analysis-v2/${"0".repeat(8)}-0000-4000-8000-${"0".repeat(12)}/${"a".repeat(64)}.json`, sha256: "a".repeat(64), bytes: 10 };
    const error = await source.result(ref).then(() => null, (e) => e);
    expect((error?.cause as { code?: string })?.code).toBe("azure_redirect");
    expect([reachedA, reachedB]).toEqual([1, 0]);
  } finally {
    a.stop(true); b.stop(true);
  }
});

test("composition on the self-host service with LOCKER_PROVIDER=azure: the Azure analysis source with its admission pool", async () => {
  const RealPool = pg.Pool;
  const built: string[] = [];
  const answer = async (text: string) => ({ rows: text === READER_LOGIN_SQL ? [{ ...READER_LOGIN_ROW }] : readerLoginAnswer(text) ?? [] });
  class RecordingPool {
    constructor(config: { application_name?: string }) { built.push(config?.application_name ?? "pool"); }
    on() { return this; }
    async query(text: string) { return answer(text); }
    async connect() { return { query: async (text: string) => answer(text), release() {} }; }
    async end() {}
  }
  pg.Pool = RecordingPool as unknown as typeof pg.Pool;
  try {
    const env = {
      RELAYHISTORY_SELFHOST_READ_API: "true", READ_API_COMPLETE_SINCE: "epoch", SESSION_POSTGRES_TLS_PROFILE: "supabase",
      SEARCH_INDEX_POSTGRES_BACKEND_ID: "selfhost-pg",
      PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "fxroot",
      LOCKER_BUCKET: "fx-container", LOCKER_AZURE_ACCOUNT: "fxacct", LOCKER_AZURE_ACCOUNT_KEY: KEY, LOCKER_ENDPOINT: FAKE_ENDPOINT,
      ...azureEnv(),
    } as NodeJS.ProcessEnv;
    const secret = JSON.stringify({ supabase_url: "postgresql://reader:pw@db.selfhost.internal:6543/history",
      analysis_admit_url: "postgresql://admit:pw@db.selfhost.internal:6543/history" });
    const { value: deps, lines } = capture(() => readApiDepsFromSecret(secret, env, selfhostSearchComposition(env), { entry: "service" }));
    const result = await deps;
    try {
      expect(lines.filter((l) => l.includes("read_api_analysis"))).toEqual([]);
      expect(result.deps.analysis).toBeDefined();
      expect(typeof result.deps.analysis!.admit).toBe("function");
      expect(typeof result.deps.analysis!.result).toBe("function");
      expect(built).toContain("96bulls-session-analysis-admit");
    } finally {
      await result.close();
    }
  } finally {
    pg.Pool = RealPool;
  }
});
