/**
 * SERVICE=session-analysis on any S3-compatible store (runner.ts, runtime-deps.ts, bucket-versioning.ts):
 * the endpoint and static-key env, the provider refusal, the startup versioning check, and that the hosted
 * default (AWS S3, Pod Identity, no endpoint) builds exactly the store and eraser config it did before.
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  AnalysisRunnerRefused,
  analysisRunnerConfig,
  analysisRunnerService,
  assertRunnerBucketVersioned,
  runnerAnalysisDeps,
  runnerExitCode,
  runnerFailureFields,
  runnerS3Target,
  runnerSecrets,
  runnerStaticKeys,
  type RunnerPorts,
} from "../runner.ts";
import { bucketVersioningUrl, readBucketVersioning, versioningStatus } from "../bucket-versioning.ts";
import { lambdaAnalysisDeps } from "../../session-pipeline/lambda.ts";
import { createS3Store, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../../migration/neon-s3-erasure/eraser.ts";
import type { HoopDb } from "../../shared/db.ts";
import { logLine } from "../../shared/log.ts";

const BUCKET = "example-pipeline-bucket";
const MODEL_KEY = "sk-test-RUNNERSECRETKEY0123456789";
const ACCESS = "AKIDSTATICEXAMPLE";
const SECRET = "STATICsecretKEY0123456789";
const DB_URL = "postgresql://worker:pw@db.example.test:5432/postgres";
const ENDPOINT = "http://minio.selfhost.svc:9000";
const POD_URI = "http://169.254.170.23/v1/credentials";

let dir = "";
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "analysis-runner-store-")); });
afterEach(() => rmSync(dir, { recursive: true, force: true }));

const code = async (fn: () => unknown): Promise<string> => {
  try { await fn(); } catch (e) { if (e instanceof AnalysisRunnerRefused) return e.code; throw e; }
  return "no_refusal";
};

function baseEnv(extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  return {
    SESSION_ANALYSIS_ENABLED: "true",
    SESSION_ANALYSIS_S3_BUCKET: BUCKET,
    SESSION_ANALYSIS_S3_REGION: "us-east-1",
    PIPELINE_ORG_FROM_PREFIX: "true",
    PIPELINE_KEY_PREFIX: "96bulls",
    ...extra,
  } as NodeJS.ProcessEnv;
}

const selfhost = (extra: Record<string, string | undefined> = {}) => baseEnv({
  SESSION_ANALYSIS_S3_ENDPOINT: ENDPOINT, ALLOW_IN_CLUSTER_HTTP: "true", SESSION_ANALYSIS_S3_CREDENTIALS: "static",
  SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET, ...extra,
});

const fakeDb = () => ({ query: async () => ({ rows: [] }), pool: { connect: async () => ({ query: async () => ({ rows: [] }), release() {} }) }, end: async () => {} }) as unknown as HoopDb;

// ---- env parsing -------------------------------------------------------------------------------

test("the hosted default config gains no key (no endpoint, no versioning check)", () => {
  const cfg = analysisRunnerConfig(baseEnv());
  expect("endpoint" in cfg).toBe(false);
  expect("versioningCheck" in cfg).toBe(false);
  expect("versioningCheck" in analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_CREDENTIALS: "role" }))).toBe(false);
  // LOCKER_PROVIDER s3 (or blank) is the same store.
  expect(analysisRunnerConfig(baseEnv({ LOCKER_PROVIDER: " s3 " }))).toEqual(cfg);
  expect(analysisRunnerConfig(baseEnv({ LOCKER_PROVIDER: "" }))).toEqual(cfg);
  // ALLOW_IN_CLUSTER_HTTP is not read without an endpoint (the hosted default reads nothing new).
  expect(analysisRunnerConfig(baseEnv({ ALLOW_IN_CLUSTER_HTTP: "yes" }))).toEqual(cfg);
});

test("an endpoint is a bare origin under the shared destination policy; static keys turn the versioning check on", () => {
  expect(analysisRunnerConfig(selfhost())).toMatchObject({ endpoint: ENDPOINT, credentials: "static", versioningCheck: true });
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_ENDPOINT: "https://s3.example.test/" }))).toMatchObject({ endpoint: "https://s3.example.test", versioningCheck: true });
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_ENDPOINT: "http://127.0.0.1:9000" })).endpoint).toBe("http://127.0.0.1:9000");
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_ENDPOINT: "http://minio.selfhost.svc.cluster.local:9000", ALLOW_IN_CLUSTER_HTTP: "true" })).endpoint)
    .toBe("http://minio.selfhost.svc.cluster.local:9000");
  // AWS S3 with static keys, no endpoint: checked too.
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_CREDENTIALS: "static" }))).toMatchObject({ credentials: "static", versioningCheck: true });
  // Forced on the hosted default (once the role may GetBucketVersioning).
  expect(analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_VERSIONING_CHECK: "true" })).versioningCheck).toBe(true);
});

test("malformed store settings refuse with a named code", async () => {
  const cases: [Record<string, string | undefined>, string][] = [
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "http://minio.selfhost.svc:9000" }, "endpoint_invalid"], // in-cluster http needs the flag
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "http://minio.example.test:9000", ALLOW_IN_CLUSTER_HTTP: "true" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "https://s3.example.test/bucket" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "https://user:pw@s3.example.test" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "https://s3.example.test/?x=1" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "ftp://s3.example.test" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "not a url" }, "endpoint_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: ENDPOINT, ALLOW_IN_CLUSTER_HTTP: "yes" }, "allow_in_cluster_http_invalid"],
    [{ SESSION_ANALYSIS_S3_ENDPOINT: "https://storage.googleapis.com" }, "provider_unsupported"],
    [{ LOCKER_PROVIDER: "gcs" }, "provider_unsupported"],
    // azure is its own store contract (runner-azure.test.ts): with S3 settings beside it, it refuses.
    [{ LOCKER_PROVIDER: "azure" }, "azure_s3_settings_conflict"],
    [{ LOCKER_PROVIDER: "fs" }, "provider_unsupported"],
    [{ LOCKER_PROVIDER: "minio" }, "provider_invalid"],
    [{ SESSION_ANALYSIS_S3_CREDENTIALS: "Static" }, "credentials_mode_invalid"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS }, "credentials_mode_conflict"],
    [{ SESSION_ANALYSIS_S3_CREDENTIALS: "role", SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: "/x" }, "credentials_mode_conflict"],
    [{ SESSION_ANALYSIS_S3_SESSION_TOKEN: "t" }, "credentials_mode_conflict"],
    [{ SESSION_ANALYSIS_S3_VERSIONING_CHECK: "false" }, "versioning_check_invalid"],
    [{ SESSION_ANALYSIS_S3_VERSIONING_CHECK: "TRUE" }, "versioning_check_invalid"],
  ];
  for (const [extra, want] of cases) expect([extra, await code(() => analysisRunnerConfig(baseEnv(extra)))]).toEqual([extra, want]);
});

test("static keys: from the env or a Secret file (one trailing newline dropped), exactly one source each", async () => {
  const idFile = join(dir, "id"), secretFile = join(dir, "secret"), tokenFile = join(dir, "token");
  writeFileSync(idFile, `${ACCESS}\n`);
  writeFileSync(secretFile, `${SECRET}\n`);
  writeFileSync(tokenFile, "sessiontoken\n");
  expect(runnerStaticKeys({ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET })).toStrictEqual({ accessKeyId: ACCESS, secretAccessKey: SECRET });
  expect(runnerStaticKeys({ SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: idFile, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: secretFile, SESSION_ANALYSIS_S3_SESSION_TOKEN_FILE: tokenFile }))
    .toStrictEqual({ accessKeyId: ACCESS, secretAccessKey: SECRET, sessionToken: "sessiontoken" });
  const cases: [Record<string, string>, string][] = [
    [{ SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET }, "s3_access_key_id_missing"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS }, "s3_secret_access_key_missing"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: idFile, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET }, "s3_access_key_id_ambiguous"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: "relative/id", SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET }, "s3_access_key_id_invalid"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: join(dir, "absent") }, "s3_secret_access_key_unreadable"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: "a/b", SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET }, "s3_access_key_id_invalid"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: "short" }, "s3_secret_access_key_invalid"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: `${SECRET} x` }, "s3_secret_access_key_invalid"],
    [{ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: SECRET, SESSION_ANALYSIS_S3_SESSION_TOKEN: "t", SESSION_ANALYSIS_S3_SESSION_TOKEN_FILE: tokenFile }, "s3_session_token_ambiguous"],
  ];
  for (const [env, want] of cases) expect([env, await code(() => runnerStaticKeys(env))]).toEqual([env, want]);
  let message = "";
  try { runnerStaticKeys({ SESSION_ANALYSIS_S3_ACCESS_KEY_ID: ACCESS, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: `${SECRET} x` }); } catch (e) { message = (e as Error).message; }
  expect(message).not.toContain(SECRET);
  // runnerSecrets reads them only in static mode; otherwise its result is exactly as before.
  const secrets = { SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY };
  expect(runnerSecrets(secrets)).toStrictEqual({ databaseUrl: DB_URL, modelKey: MODEL_KEY });
  expect(runnerSecrets({ ...secrets, ...selfhost() }).s3).toStrictEqual({ accessKeyId: ACCESS, secretAccessKey: SECRET });
  expect(await code(() => runnerSecrets({ ...secrets, SESSION_ANALYSIS_S3_CREDENTIALS: "static" }))).toBe("s3_access_key_id_missing");
});

// ---- store and eraser config ------------------------------------------------------------------

test("hosted default: the store and the eraser get exactly {bucket, region, credentials}, as the Lambda's eraser does", async () => {
  const env = baseEnv({ AWS_CONTAINER_CREDENTIALS_FULL_URI: POD_URI });
  const cfg = analysisRunnerConfig(env);
  const fetcher = (async () => new Response(JSON.stringify({ AccessKeyId: "AKIDPOD", SecretAccessKey: SECRET, Token: "tok", Expiration: new Date(Date.now() + 3_600_000).toISOString() }))) as unknown as typeof fetch;
  const stores: Record<string, unknown>[] = [];
  const erasers: { runner: S3EraserConfig[]; lambda: S3EraserConfig[] } = { runner: [], lambda: [] };
  const fake = (side: "runner" | "lambda") => (c: S3EraserConfig): PagedEraser => {
    erasers[side].push(c);
    return { eraseStep: async () => ({}) as never, verifyErased: async () => ({}) as never };
  };
  const versioning: string[] = [];
  const ports: RunnerPorts = {
    pool: fakeDb, fetcher, eraserFactory: fake("runner"),
    store: (c) => { stores.push(c as unknown as Record<string, unknown>); return {} as ObjectStore; },
    versioning: async () => { versioning.push("called"); return "Enabled"; },
  };
  const built = runnerAnalysisDeps(env, cfg, { databaseUrl: DB_URL, modelKey: MODEL_KEY }, ports);
  expect(stores).toHaveLength(1);
  expect(Object.keys(stores[0]!).sort()).toEqual(["bucket", "credentials", "region"]);
  expect(stores[0]).toMatchObject({ bucket: BUCKET, region: "us-east-1" });
  await built.deps.eraser!.eraseStep("96bulls/org/", {} as never);
  expect(Object.keys(erasers.runner[0]!).sort()).toEqual(["bucket", "credentials", "region"]);
  expect(erasers.runner[0]).toMatchObject({ bucket: BUCKET, region: "us-east-1", credentials: { accessKeyId: "AKIDPOD", sessionToken: "tok" } });
  const lambda = lambdaAnalysisDeps({ bucket: BUCKET, region: "us-east-1", keyPrefix: "96bulls" }, fakeDb(), {} as ObjectStore, MODEL_KEY, { eraserFactory: fake("lambda") });
  const saved = { id: process.env.AWS_ACCESS_KEY_ID, secret: process.env.AWS_SECRET_ACCESS_KEY };
  process.env.AWS_ACCESS_KEY_ID = "AKIDLAMBDA"; process.env.AWS_SECRET_ACCESS_KEY = SECRET;
  try { await lambda.eraser!.eraseStep("96bulls/org/", {} as never); } finally {
    if (saved.id === undefined) delete process.env.AWS_ACCESS_KEY_ID; else process.env.AWS_ACCESS_KEY_ID = saved.id;
    if (saved.secret === undefined) delete process.env.AWS_SECRET_ACCESS_KEY; else process.env.AWS_SECRET_ACCESS_KEY = saved.secret;
  }
  expect(Object.keys(erasers.lambda[0]!).sort()).toEqual(["bucket", "credentials", "region"]);
  // No GetBucketVersioning request on the hosted default.
  await assertRunnerBucketVersioned(cfg, built.target, ports);
  expect(versioning).toEqual([]);
});

test("endpoint + static keys reach the store and the sweeper's eraser: path-style, same origin, same keys", async () => {
  const env = selfhost();
  const cfg = analysisRunnerConfig(env);
  const secrets = runnerSecrets({ ...env, SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY });
  const sent: { url: string; method: string; auth: string }[] = [];
  const fetcher = (async (input: string | URL | Request, init?: RequestInit) => {
    sent.push({ url: String(input instanceof Request ? input.url : input), method: init?.method ?? "GET", auth: new Headers(init?.headers).get("authorization") ?? "" });
    const u = new URL(String(input));
    if (u.searchParams.has("versions"))
      return new Response(`<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>${BUCKET}</Name><Prefix>96bulls/org/</Prefix><IsTruncated>false</IsTruncated></ListVersionsResult>`);
    return new Response("", { status: 404 });
  }) as typeof fetch;
  const storeConfigs: Record<string, unknown>[] = [], eraserConfigs: S3EraserConfig[] = [];
  const { deps } = runnerAnalysisDeps(env, cfg, secrets, {
    pool: fakeDb,
    store: (c) => { storeConfigs.push(c as unknown as Record<string, unknown>); return createS3Store({ ...c, fetch: fetcher }) as unknown as ObjectStore; },
    eraserFactory: (c) => { eraserConfigs.push(c); return createS3Eraser({ ...c, fetch: fetcher }); },
  });
  expect(storeConfigs[0]).toMatchObject({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT });
  await deps.store.getObject("96bulls/org/v1/x" as never).catch(() => {});
  const proof = await deps.eraser!.verifyErased("96bulls/org/", { authorize: async () => {} });
  expect(proof).toMatchObject({ status: "erased" });
  expect(eraserConfigs[0]).toStrictEqual({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: { accessKeyId: ACCESS, secretAccessKey: SECRET } });
  expect(sent.length).toBeGreaterThanOrEqual(2);
  for (const r of sent) {
    expect(r.url.startsWith(`${ENDPOINT}/${BUCKET}/`)).toBe(true);
    expect(r.auth).toContain(`Credential=${ACCESS}/`);
    expect(r.auth).toContain("/us-east-1/s3/aws4_request");
  }
  expect(sent.some((r) => new URL(r.url).searchParams.has("versions"))).toBe(true);
});

// ---- the versioning check -----------------------------------------------------------------------

const respond = (status: number, body: string) => (async () => new Response(body, { status })) as unknown as typeof fetch;
const creds = { accessKeyId: ACCESS, secretAccessKey: SECRET };

test("GetBucketVersioning: Enabled passes; Suspended or never enabled is not versioned; 4xx or junk is unverified; 5xx throws", async () => {
  const ns = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"';
  const read = (status: number, body: string) => readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: creds, fetch: respond(status, body) });
  expect(await read(200, `<?xml version="1.0" encoding="UTF-8"?>\n<VersioningConfiguration ${ns}><Status>Enabled</Status></VersioningConfiguration>`)).toBe("Enabled");
  // MinIO's extensions next to Status are tolerated.
  expect(await read(200, `<VersioningConfiguration ${ns}><Status>Enabled</Status><ExcludeFolders>false</ExcludeFolders></VersioningConfiguration>`)).toBe("Enabled");
  expect(await read(200, `<VersioningConfiguration ${ns}><Status>Suspended</Status></VersioningConfiguration>`)).toBe("not_versioned");
  expect(await read(200, `<VersioningConfiguration ${ns}></VersioningConfiguration>`)).toBe("not_versioned");
  expect(await read(200, `<VersioningConfiguration ${ns}/>`)).toBe("not_versioned");
  expect(await read(200, `<VersioningConfiguration><Status>Enabled</Status><Status>Suspended</Status></VersioningConfiguration>`)).toBe("unverified");
  expect(await read(200, "<Error><Code>NotImplemented</Code></Error>")).toBe("unverified");
  expect(await read(200, "")).toBe("unverified");
  expect(await read(403, "<Error/>")).toBe("unverified");
  // 501 NotImplemented: the store has no GetBucketVersioning, so the property cannot be proven (a refusal).
  expect(await read(501, "")).toBe("unverified");
  await expect(read(503, "")).rejects.toMatchObject({ code: "bucket_versioning_unavailable" });
  const down = (async () => { throw new Error("ECONNREFUSED"); }) as unknown as typeof fetch;
  await expect(readBucketVersioning({ bucket: BUCKET, region: "us-east-1", credentials: creds, fetch: down })).rejects.toMatchObject({ code: "bucket_versioning_unavailable" });
  expect(versioningStatus("<VersioningConfiguration><Status> Enabled </Status></VersioningConfiguration>")).toBe("Enabled");
});

test("GetBucketVersioning is addressed as the store addresses the bucket, signed for its region", async () => {
  expect(bucketVersioningUrl({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT }).toString()).toBe(`${ENDPOINT}/${BUCKET}?versioning=`);
  expect(bucketVersioningUrl({ bucket: BUCKET, region: "eu-west-1" }).toString()).toBe(`https://${BUCKET}.s3.eu-west-1.amazonaws.com/?versioning=`);
  let seen: { method?: string; auth?: string | null } = {};
  const f = (async (_u: string | URL, init?: RequestInit) => {
    seen = { method: init?.method, auth: new Headers(init?.headers).get("authorization") };
    return new Response("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>");
  }) as typeof fetch;
  await readBucketVersioning({ bucket: BUCKET, region: "eu-west-1", credentials: creds, fetch: f });
  expect(seen.method).toBe("GET");
  expect(seen.auth).toContain(`Credential=${ACCESS}/`);
  expect(seen.auth).toContain("/eu-west-1/s3/aws4_request");
});

test("the service refuses an unversioned bucket at startup, before anything listens; a versioned one starts", async () => {
  const env = { ...selfhost(), SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY };
  for (const [verdict, want] of [["not_versioned", "bucket_not_versioned"], ["unverified", "bucket_versioning_unverified"]] as const) {
    let listened = false;
    const checks: string[] = [];
    const result = await analysisRunnerService(env, {
      host: "127.0.0.1", port: 0, scrubInit: async () => {}, onListening: () => { listened = true; },
      ports: { pool: fakeDb, store: () => ({}) as ObjectStore, versioning: async (c) => { checks.push(`${c.endpoint}|${c.bucket}|${c.credentials.accessKeyId}`); return verdict; } },
    }).then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
    expect(result).toBe(want);
    expect(listened).toBe(false);
    expect(checks).toEqual([`${ENDPOINT}|${BUCKET}|${ACCESS}`]);
  }
  const stop = new AbortController();
  const done = analysisRunnerService(env, {
    host: "127.0.0.1", port: 0, signal: stop.signal, scrubInit: async () => {}, onListening: () => stop.abort(),
    ports: { pool: fakeDb, store: () => ({}) as ObjectStore, versioning: async () => "Enabled" },
  });
  expect((await done).enabled).toBe(true);
});

test("LOCKER_PROVIDER gcs and fs refuse the enabled service at startup (exit 2); azure with S3 settings conflicts", async () => {
  const azure = await analysisRunnerService(baseEnv({ LOCKER_PROVIDER: "azure" }), { host: "127.0.0.1", port: 0, scrubInit: async () => {} })
    .then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
  expect(azure).toBe("azure_s3_settings_conflict");
  for (const provider of ["gcs", "fs"]) {
    const result = await analysisRunnerService(baseEnv({ LOCKER_PROVIDER: provider }), { host: "127.0.0.1", port: 0, scrubInit: async () => {} })
      .then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
    expect(result).toBe("provider_unsupported");
  }
});

// ---- one resolved target ----------------------------------------------------------------------

test("the store, the eraser and the versioning check are built from one resolved target and cannot differ", async () => {
  for (const env of [selfhost(), baseEnv({ AWS_CONTAINER_CREDENTIALS_FULL_URI: POD_URI })]) {
    const cfg = analysisRunnerConfig(env);
    const fetcher = (async () => new Response(JSON.stringify({ AccessKeyId: "AKIDPOD", SecretAccessKey: SECRET, Token: "tok", Expiration: new Date(Date.now() + 3_600_000).toISOString() }))) as unknown as typeof fetch;
    const secrets = runnerSecrets({ ...env, SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY });
    const storeConfigs: { bucket: string; region: string; endpoint?: string; credentials: unknown }[] = [];
    const eraserConfigs: S3EraserConfig[] = [];
    const checks: Parameters<NonNullable<RunnerPorts["versioning"]>>[0][] = [];
    const ports: RunnerPorts = {
      pool: fakeDb, fetcher,
      store: (c) => { storeConfigs.push(c as never); return {} as ObjectStore; },
      eraserFactory: (c) => { eraserConfigs.push(c); return { eraseStep: async () => ({}) as never, verifyErased: async () => ({}) as never }; },
      versioning: async (c) => { checks.push(c); return "Enabled"; },
    };
    const target = runnerS3Target(env, cfg, secrets, fetcher);
    expect(Object.isFrozen(target)).toBe(true);
    await assertRunnerBucketVersioned({ versioningCheck: true }, target, ports);
    const built = runnerAnalysisDeps(env, cfg, secrets, ports, target);
    expect(built.target).toBe(target);
    await built.deps.eraser!.eraseStep("96bulls/org/", {} as never);
    const resolved = await target.credentials();
    // The store holds the target's very credentials provider; the eraser and the check sign with what it resolves.
    expect(storeConfigs[0]!.credentials).toBe(target.credentials);
    expect(eraserConfigs[0]!.credentials).toBe(resolved);
    expect(checks[0]!.credentials).toBe(resolved);
    for (const c of [storeConfigs[0]!, eraserConfigs[0]!, checks[0]!]) {
      expect(c.bucket).toBe(target.bucket);
      expect(c.region).toBe(target.region);
      expect(c.endpoint).toBe(target.endpoint);
      expect("endpoint" in c).toBe("endpoint" in target);
    }
  }
});

// ---- no key material in errors or logs ---------------------------------------------------------

test("no static-key refusal carries key material, in its message or in the real startup log line", async () => {
  const FAKE_ID = "AKIDFAKEKEYMATERIAL7";
  const FAKE_SECRET = "FAKESECRETkeymaterial0042";
  const FAKE_TOKEN = "FAKETOKENkeymaterial0099";
  const file = (name: string, text: string) => { const p = join(dir, name); writeFileSync(p, text); return p; };
  const idFile = file("id", `${FAKE_ID}\n`), secretFile = file("secret", `${FAKE_SECRET}\n`), tokenFile = file("token", FAKE_TOKEN);
  const bad = file("bad", `${FAKE_SECRET} ${FAKE_ID}`), big = file("big", FAKE_SECRET.repeat(4000));
  const ok = { SESSION_ANALYSIS_S3_ACCESS_KEY_ID: FAKE_ID, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: FAKE_SECRET };
  const cases: Record<string, string>[] = [
    { ...ok, SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: idFile },
    { ...ok, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: secretFile },
    { ...ok, SESSION_ANALYSIS_S3_SESSION_TOKEN: FAKE_TOKEN, SESSION_ANALYSIS_S3_SESSION_TOKEN_FILE: tokenFile },
    { ...ok, SESSION_ANALYSIS_S3_ACCESS_KEY_ID: `${FAKE_ID}/x` },
    { ...ok, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: `${FAKE_SECRET} ${FAKE_ID}` },
    { ...ok, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: FAKE_SECRET.repeat(20) },
    { SESSION_ANALYSIS_S3_ACCESS_KEY_ID: FAKE_ID, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: bad },
    { SESSION_ANALYSIS_S3_ACCESS_KEY_ID: FAKE_ID, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: big },
    { SESSION_ANALYSIS_S3_ACCESS_KEY_ID: FAKE_ID, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE: `${dir}/${FAKE_SECRET}` },
    { SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: `relative/${FAKE_ID}`, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: FAKE_SECRET },
    { ...ok, SESSION_ANALYSIS_S3_SESSION_TOKEN: `${FAKE_TOKEN}\u0001` },
    { SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: FAKE_SECRET },
    { SESSION_ANALYSIS_S3_ACCESS_KEY_ID: FAKE_ID },
  ];
  for (const extra of cases) {
    const env = { ...selfhost(), SESSION_ANALYSIS_S3_ACCESS_KEY_ID: undefined, SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY: undefined,
      SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY, ...extra } as NodeJS.ProcessEnv;
    let error: unknown;
    try { runnerStaticKeys(env); } catch (e) { error = e; }
    expect(error).toBeInstanceOf(AnalysisRunnerRefused);
    const service = await analysisRunnerService(env, { host: "127.0.0.1", port: 0, scrubInit: async () => {} }).then(() => undefined, (e) => e);
    expect(service).toBeInstanceOf(AnalysisRunnerRefused);
    const line = logLine("error", "session analysis runner refused", runnerFailureFields(service));
    expect(JSON.parse(line)).toMatchObject({ error_code: (service as AnalysisRunnerRefused).code, ok: false });
    const text = [String((error as Error).message), String((error as Error).stack), String((service as Error).message), line].join("\n");
    for (const material of [FAKE_ID, FAKE_SECRET, FAKE_TOKEN]) expect([extra, text.includes(material)]).toEqual([extra, false]);
  }
});

// ---- refusals come before any store or database request ------------------------------------------

test("every refusal fails at startup before any object or database request; the versioning GET is the only one allowed", async () => {
  const idFile = join(dir, "id");
  writeFileSync(idFile, ACCESS);
  const secrets = { SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY };
  const versioned = (status: string) => `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${status}</VersioningConfiguration>`;
  const cases: [string, NodeJS.ProcessEnv, string, string | null][] = [
    ["suspended bucket", selfhost(secrets), "bucket_not_versioned", versioned("<Status>Suspended</Status>")],
    ["never-versioned bucket", selfhost(secrets), "bucket_not_versioned", versioned("")],
    ["unsupported provider", selfhost({ ...secrets, LOCKER_PROVIDER: "gcs" }), "provider_unsupported", null],
    ["azure beside S3 settings", selfhost({ ...secrets, LOCKER_PROVIDER: "azure" }), "azure_s3_settings_conflict", null],
    ["unsupported provider (fs)", selfhost({ ...secrets, LOCKER_PROVIDER: "fs" }), "provider_unsupported", null],
    ["http outside the in-cluster rule", selfhost({ ...secrets, SESSION_ANALYSIS_S3_ENDPOINT: "http://minio.example.test:9000" }), "endpoint_invalid", null],
    ["in-cluster http without the flag", selfhost({ ...secrets, ALLOW_IN_CLUSTER_HTTP: undefined }), "endpoint_invalid", null],
    ["inline + _FILE access key", selfhost({ ...secrets, SESSION_ANALYSIS_S3_ACCESS_KEY_ID_FILE: idFile }), "s3_access_key_id_ambiguous", null],
  ];
  for (const [name, env, want, versioningDoc] of cases) {
    const requests: string[] = [];
    const queries: string[] = [];
    const s3 = (async (input: string | URL | Request, init?: RequestInit) => {
      const u = new URL(String(input instanceof Request ? input.url : input));
      requests.push(`${init?.method ?? "GET"} ${u.pathname}${u.search}`);
      return new Response(versioningDoc ?? "", { status: versioningDoc === null ? 500 : 200 });
    }) as typeof fetch;
    const db = { query: async (q: string) => { queries.push(q); return { rows: [] }; }, pool: { connect: async () => { queries.push("connect"); return { query: async () => ({ rows: [] }), release() {} }; } }, end: async () => {} } as unknown as HoopDb;
    let listened = false;
    const result = await analysisRunnerService(env, {
      host: "127.0.0.1", port: 0, scrubInit: async () => {}, onListening: () => { listened = true; },
      ports: {
        fetcher: s3, pool: () => db,
        store: (c) => createS3Store({ ...c, fetch: s3 }) as unknown as ObjectStore,
        eraserFactory: (c) => createS3Eraser({ ...c, fetch: s3 }),
        versioning: (c) => readBucketVersioning({ ...c, fetch: s3 }),
      },
    }).then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
    expect([name, result]).toEqual([name, want]);
    expect([name, listened]).toEqual([name, false]);
    expect([name, queries]).toEqual([name, []]);
    expect([name, requests]).toEqual([name, versioningDoc === null ? [] : [`GET /${BUCKET}?versioning=`]]);
  }
});

// ---- strict versioning document, redirects, bounds, timeouts --------------------------------------

test("the versioning document is parsed strictly: comments ignored, known elements only, MinIO exclusions unverified", () => {
  const ns = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"';
  const doc = (inner: string) => `<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration ${ns}>${inner}</VersioningConfiguration>`;
  const cases: [string, string | undefined][] = [
    [doc("<Status>Enabled</Status>"), "Enabled"],
    [doc("<Status>Suspended</Status>"), "Suspended"],
    [doc(""), ""],
    [`<VersioningConfiguration ${ns}/>`, ""],
    [doc("<Status>Enabled</Status><MfaDelete>Disabled</MfaDelete>"), "Enabled"],
    [doc("<Status>Enabled</Status><MfaDelete>Enabled</MfaDelete>"), "Enabled"],
    [doc("<Status>Enabled</Status><ExcludeFolders>false</ExcludeFolders>"), "Enabled"],
    [doc("<Status>Enabled</Status><ExcludedPrefixes></ExcludedPrefixes>"), "Enabled"],
    [doc("<Status>Enabled</Status><ExcludedPrefixes/>"), "Enabled"],
    [doc("<Status>Enabled</Status><ExcludedPrefixes><Prefix></Prefix></ExcludedPrefixes>"), "Enabled"],
    [doc("<!-- note --><Status>Enabled</Status>"), "Enabled"],
    // A comment never supplies a Status.
    [doc("<!-- <Status>Enabled</Status> -->"), ""],
    [doc("<Status>Suspended</Status><!--<Status>Enabled</Status>-->"), "Suspended"],
    [`<!-- <VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration> --><VersioningConfiguration/>`, ""],
    // MinIO exclusions in effect: the excluded keys are unversioned.
    [doc("<Status>Enabled</Status><ExcludeFolders>true</ExcludeFolders>"), undefined],
    [doc("<Status>Enabled</Status><ExcludedPrefixes><Prefix>tmp/</Prefix></ExcludedPrefixes>"), undefined],
    [doc("<Status>Enabled</Status><ExcludedPrefixes><Prefix></Prefix></ExcludedPrefixes><ExcludedPrefixes><Prefix>a/</Prefix></ExcludedPrefixes>"), undefined],
    // Unknown elements, values, repeats, malformed shapes.
    [doc("<Status>Enabled</Status><Other>x</Other>"), undefined],
    [doc("<Status>Enabled</Status><ExcludedPrefixes><Other/></ExcludedPrefixes>"), undefined],
    [doc("<Status>Enabled</Status><Status>Enabled</Status>"), undefined],
    [doc("<Status>Enabled</Status><MfaDelete>maybe</MfaDelete>"), undefined],
    [doc("<Status>Enabled</Status><ExcludeFolders>yes</ExcludeFolders>"), undefined],
    [doc("<Status>enabled</Status>"), undefined],
    [doc("<Status a=\"1\">Enabled</Status>"), undefined],
    [doc("<s3:Status>Enabled</s3:Status>"), undefined],
    [doc("<Status><![CDATA[Enabled]]></Status>"), undefined],
    [doc("<Status>Enabled</Status><!-- unterminated"), undefined],
    [doc("text<Status>Enabled</Status>"), undefined],
    [`<VersioningConfiguration ${ns} extra="1"><Status>Enabled</Status></VersioningConfiguration>`, undefined],
    [`<Other><Status>Enabled</Status></Other>`, undefined],
    [`${doc("<Status>Enabled</Status>")}<VersioningConfiguration/>`, undefined],
  ];
  for (const [xml, want] of cases) expect([xml, versioningStatus(xml)]).toEqual([xml, want]);
});

/** A fetch that honours `redirect` as a real one does: follow (default), error, or manual (the 307 itself). */
const redirecting = (followed: string): typeof fetch => (async (_u: string | URL, init?: RequestInit) => {
  if (init?.redirect === "error") throw new TypeError("redirect");
  if (init?.redirect === "manual") return new Response(null, { status: 307, headers: { location: "https://elsewhere.example.test/" } });
  return new Response(followed);
}) as typeof fetch;

test("a 3xx on the versioning GET is never followed: it refuses as bucket_versioning_redirect (exit 2)", async () => {
  const enabled = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
  expect(await readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: creds, fetch: redirecting(enabled) })).toBe("redirect");
  // A real fetch against a loopback server that answers 307 to a location that would say Enabled.
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => new URL(req.url).pathname === "/other"
    ? new Response(enabled) : new Response(null, { status: 307, headers: { location: "/other" } }) });
  try {
    expect(await readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: `http://127.0.0.1:${server.port}`, credentials: creds })).toBe("redirect");
  } finally { server.stop(true); }
  let code = "";
  try { await assertRunnerBucketVersioned({ versioningCheck: true }, runnerS3Target(selfhost(), analysisRunnerConfig(selfhost()), { s3: creds }), { versioning: async () => "redirect" }); }
  catch (e) { code = e instanceof AnalysisRunnerRefused ? e.code : String(e); }
  expect(code).toBe("bucket_versioning_redirect");
});

test("a body over 64 KiB is unverified, even when it is a valid Enabled document; at the bound it is read", async () => {
  const enabled = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
  const read = (body: string) => readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: creds, fetch: respond(200, body) });
  expect(await read(enabled + " ".repeat(64 * 1024 - enabled.length))).toBe("Enabled");
  expect(await read(enabled + " ".repeat(64 * 1024 - enabled.length + 1))).toBe("unverified");
});

test("a body that stalls past the deadline is bucket_versioning_unavailable (exit 1), not a raw TimeoutError", async () => {
  const stalled = (async (_u: string | URL, init?: RequestInit) => {
    const signal = init!.signal!;
    const body = new ReadableStream<Uint8Array>({
      start(c) { c.enqueue(new TextEncoder().encode("<VersioningConfiguration>")); },
      pull(c) { return new Promise<void>((_, reject) => signal.addEventListener("abort", () => { reject(signal.reason); c.error(signal.reason); }, { once: true })); },
    });
    return new Response(body, { status: 200 });
  }) as typeof fetch;
  const err = await readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: creds, fetch: stalled, timeoutMs: 50 }).then(() => undefined, (e) => e);
  expect(err).toMatchObject({ code: "bucket_versioning_unavailable" });
  expect(JSON.parse(logLine("error", "session analysis runner refused", runnerFailureFields(err)))).toMatchObject({ error_code: "bucket_versioning_unavailable" });
});

test("error responses have their body cancelled", async () => {
  for (const status of [403, 307, 503]) {
    let cancelled = false;
    const body = new ReadableStream({ cancel() { cancelled = true; } });
    const f = (async () => new Response(body, { status })) as unknown as typeof fetch;
    await readBucketVersioning({ bucket: BUCKET, region: "us-east-1", endpoint: ENDPOINT, credentials: creds, fetch: f }).catch(() => {});
    expect([status, cancelled]).toEqual([status, true]);
  }
});

test("a credentials failure before the versioning GET is bucket_versioning_credentials_unavailable (exit 1)", async () => {
  const env = baseEnv({ AWS_CONTAINER_CREDENTIALS_FULL_URI: POD_URI, SESSION_ANALYSIS_S3_VERSIONING_CHECK: "true", SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY });
  const checks: string[] = [];
  let pools = 0;
  const err = await analysisRunnerService(env, {
    host: "127.0.0.1", port: 0, scrubInit: async () => {},
    ports: {
      fetcher: (async () => new Response("", { status: 500 })) as unknown as typeof fetch,
      pool: () => { pools += 1; return fakeDb(); },
      versioning: async () => { checks.push("called"); return "Enabled"; },
    },
  }).then(() => undefined, (e) => e);
  expect(err).not.toBeInstanceOf(AnalysisRunnerRefused);
  expect(err).toMatchObject({ code: "bucket_versioning_credentials_unavailable" });
  expect(runnerExitCode(err)).toBe(1);
  expect(checks).toEqual([]);
  expect(pools).toBe(0);
  expect(JSON.parse(logLine("error", "session analysis runner refused", runnerFailureFields(err)))).toMatchObject({ error_code: "bucket_versioning_credentials_unavailable" });
});

test("a refused versioning check builds no pool and no store", async () => {
  for (const verdict of ["not_versioned", "unverified", "redirect"] as const) {
    let pools = 0, stores = 0, erasers = 0;
    const result = await analysisRunnerService({ ...selfhost(), SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY }, {
      host: "127.0.0.1", port: 0, scrubInit: async () => {},
      ports: {
        pool: () => { pools += 1; return fakeDb(); },
        store: () => { stores += 1; return {} as ObjectStore; },
        eraserFactory: () => { erasers += 1; return { eraseStep: async () => ({}) as never, verifyErased: async () => ({}) as never }; },
        versioning: async () => verdict,
      },
    }).then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
    expect(result).toMatch(/^bucket_/);
    expect([verdict, pools, stores, erasers]).toEqual([verdict, 0, 0, 0]);
  }
});

test("the GCS API host is refused in any case and with a trailing dot", async () => {
  for (const host of ["https://STORAGE.googleapis.com.", "https://storage.googleapis.com.", "https://Storage.GoogleApis.Com", "https://bucket.storage.googleapis.com.", "https://storage.google.com."])
    expect([host, await code(() => analysisRunnerConfig(baseEnv({ SESSION_ANALYSIS_S3_ENDPOINT: host })))]).toEqual([host, "provider_unsupported"]);
});
