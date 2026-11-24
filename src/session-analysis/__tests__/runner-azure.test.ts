/**
 * SERVICE=session-analysis on LOCKER_PROVIDER=azure (runner.ts, runtime-deps.ts, blob-versioning.ts): the azure env
 * contract and every refusal, the key kept behind a closure, the store/eraser/probe built from one target, the
 * canonical provider, and that unset LOCKER_PROVIDER (the hosted default) is exactly what it was.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  AnalysisRunnerRefused,
  analysisRunnerConfig,
  analysisRunnerService,
  runnerAnalysisDeps,
  runnerAzureAnalysisDeps,
  runnerAzureTarget,
  runnerExitCode,
  runnerFailureFields,
  runnerSecrets,
  type RunnerPorts,
} from "../runner.ts";
import { lambdaAnalysisDeps } from "../../session-pipeline/lambda.ts";
import type { AzureBlobConfig } from "../../../packages/object-store/src/azure.ts";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import type { AzureBlobEraserConfig } from "../../migration/azure-blob-erasure/eraser.ts";
import type { PagedEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import type { HoopDb } from "../../shared/db.ts";
import { logLine } from "../../shared/log.ts";
import { classifyAnalysisFailure } from "../processor.ts";
import { ObjectStoreError } from "../../../packages/object-store/src/errors.ts";
import { FAKE_ENDPOINT, FakeAzureBlob } from "../../migration/__tests__/fake-azure-blob.ts";

const MODEL_KEY = "sk-test-RUNNERSECRETKEY0123456789";
const DB_URL = "postgresql://worker:pw@db.example.test:5432/postgres";
const randomKey = () => Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");

let dir = "";
let KEY = "";
let keyFile = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "analysis-runner-azure-"));
  KEY = randomKey();
  keyFile = join(dir, "account-key");
  writeFileSync(keyFile, `${KEY}\n`);
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

const code = async (fn: () => unknown): Promise<string> => {
  try { await fn(); } catch (e) { if (e instanceof AnalysisRunnerRefused) return e.code; throw e; }
  return "no_refusal";
};

function azureEnv(extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  const env: Record<string, string | undefined> = {
    SESSION_ANALYSIS_ENABLED: "true",
    LOCKER_PROVIDER: "azure",
    SESSION_ANALYSIS_AZURE_ACCOUNT: "fxacct",
    SESSION_ANALYSIS_AZURE_CONTAINER: "fx-container",
    SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: keyFile,
    PIPELINE_ORG_FROM_PREFIX: "true",
    PIPELINE_KEY_PREFIX: "fxroot",
    SESSION_ANALYSIS_DATABASE_URL: DB_URL,
    SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY,
    ...extra,
  };
  for (const k of Object.keys(env)) if (env[k] === undefined) delete env[k];
  return env as NodeJS.ProcessEnv;
}
const s3Env = (extra: Record<string, string | undefined> = {}) => ({
  SESSION_ANALYSIS_ENABLED: "true", SESSION_ANALYSIS_S3_BUCKET: "example-pipeline-bucket", SESSION_ANALYSIS_S3_REGION: "us-east-1",
  PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "96bulls", ...extra,
}) as NodeJS.ProcessEnv;

const fakeDb = () => ({ query: async () => ({ rows: [] }), pool: { connect: async () => ({ query: async () => ({ rows: [] }), release() {} }) }, end: async () => {} }) as unknown as HoopDb;

describe("azure config", () => {
  test("accepted: the container is the canonical bucket, the probe always runs, no S3 field is read", () => {
    const cfg = analysisRunnerConfig(azureEnv());
    expect(cfg).toMatchObject({ bucket: "fx-container", region: "", credentials: "sharedKey", versioningCheck: true, keyPrefix: "fxroot",
      azure: { account: "fxacct", container: "fx-container" } });
    expect("endpoint" in cfg).toBe(false);
    expect("endpoint" in cfg.azure!).toBe(false);
  });

  test("an endpoint follows the shared destination policy; path only empty or /<account>", () => {
    expect(analysisRunnerConfig(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://127.0.0.1:10000/fxacct" })).azure!.endpoint).toBe("http://127.0.0.1:10000/fxacct");
    expect(analysisRunnerConfig(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://127.0.0.1:10000/fxacct/" })).azure!.endpoint).toBe("http://127.0.0.1:10000/fxacct");
    expect(analysisRunnerConfig(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: "https://fxacct.blob.example.test/" })).azure!.endpoint).toBe("https://fxacct.blob.example.test");
    expect(analysisRunnerConfig(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://azurite.fx-ns.svc:10000/fxacct", ALLOW_IN_CLUSTER_HTTP: "true" })).azure!.endpoint)
      .toBe("http://azurite.fx-ns.svc:10000/fxacct");
  });

  test("every malformed or conflicting azure setting refuses with its named code", async () => {
    const cases: [Record<string, string | undefined>, string][] = [
      ...["SESSION_ANALYSIS_S3_BUCKET", "SESSION_ANALYSIS_S3_REGION", "SESSION_ANALYSIS_S3_ENDPOINT", "SESSION_ANALYSIS_S3_CREDENTIALS",
        "SESSION_ANALYSIS_S3_VERSIONING_CHECK", "SESSION_ANALYSIS_S3_ACCESS_KEY_ID", "SESSION_ANALYSIS_S3_SECRET_ACCESS_KEY_FILE",
        "SESSION_ANALYSIS_S3_SESSION_TOKEN"].map((name): [Record<string, string>, string] => [{ [name]: "x" }, "azure_s3_settings_conflict"]),
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT: undefined }, "azure_account_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT: "FxAcct" }, "azure_account_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT: "fx-acct" }, "azure_account_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT: "a".repeat(25) }, "azure_account_invalid"],
      [{ SESSION_ANALYSIS_AZURE_CONTAINER: undefined }, "azure_container_invalid"],
      [{ SESSION_ANALYSIS_AZURE_CONTAINER: "Fx_Container" }, "azure_container_invalid"],
      [{ SESSION_ANALYSIS_AZURE_CONTAINER: "fx--container" }, "azure_container_invalid"],
      [{ SESSION_ANALYSIS_AZURE_CONTAINER: "fx" }, "azure_container_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: KEY }, "azure_account_key_plaintext"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://fxacct.blob.example.test" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://azurite.fx-ns.svc:10000/fxacct" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "https://fxacct.blob.example.test/other" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "https://fxacct.blob.example.test/fxacct/x" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "https://fxacct.blob.example.test/?sv=1" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "https://u:p@fxacct.blob.example.test" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "not a url" }, "azure_endpoint_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://127.0.0.1:10000/fxacct", ALLOW_IN_CLUSTER_HTTP: "yes" }, "allow_in_cluster_http_invalid"],
      [{ PIPELINE_ORG_FROM_PREFIX: undefined }, "org_from_prefix_required"],
      [{ PIPELINE_KEY_PREFIX: "" }, "key_prefix_invalid"],
      [{ SESSION_ANALYSIS_BATCH: "0" }, "batch_invalid"],
      [{ SUMMARY_DECISION_RECORDS_ENABLED: "true" }, "azure_decision_records_unsupported"],
    ];
    for (const [extra, want] of cases) expect([extra, await code(() => analysisRunnerConfig(azureEnv(extra)))]).toEqual([extra, want]);
  });

  test("provider_unsupported stays for gcs and fs; anything else is provider_invalid", async () => {
    expect(await code(() => analysisRunnerConfig(s3Env({ LOCKER_PROVIDER: "gcs" })))).toBe("provider_unsupported");
    expect(await code(() => analysisRunnerConfig(s3Env({ LOCKER_PROVIDER: "fs" })))).toBe("provider_unsupported");
    expect(await code(() => analysisRunnerConfig(s3Env({ LOCKER_PROVIDER: "Azure" })))).toBe("provider_invalid");
  });

  test("the key: file only, strict base64 of at least 32 bytes; missing, unreadable, relative or malformed refuse", async () => {
    expect(runnerSecrets(azureEnv()).azure!.accountKey()).toBe(KEY);
    const short = join(dir, "short"); writeFileSync(short, Buffer.from(new Uint8Array(16)).toString("base64"));
    const junk = join(dir, "junk"); writeFileSync(junk, "not*base64*at*all");
    const unpadded = join(dir, "unpadded"); writeFileSync(unpadded, KEY.replace(/=+$/, ""));
    const cases: [Record<string, string | undefined>, string][] = [
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: undefined }, "azure_account_key_missing"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: join(dir, "absent") }, "azure_account_key_unreadable"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: "relative/key" }, "azure_account_key_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: short }, "azure_account_key_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: junk }, "azure_account_key_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: unpadded }, "azure_account_key_invalid"],
      [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: KEY }, "azure_account_key_plaintext"],
    ];
    for (const [extra, want] of cases) expect([extra, await code(() => runnerSecrets(azureEnv(extra)))]).toEqual([extra, want]);
  });

  test("no substring of the key appears in the config, the target, the secrets' JSON, any refusal or the startup failure line", async () => {
    const env = azureEnv();
    const cfg = analysisRunnerConfig(env);
    const secrets = runnerSecrets(env);
    const target = runnerAzureTarget(cfg, secrets);
    const texts = [JSON.stringify(cfg), JSON.stringify(target), JSON.stringify(secrets), String(Object.values(target))];
    for (const extra of [{ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: KEY }, { SESSION_ANALYSIS_AZURE_ENDPOINT: `https://x/${KEY}` }, { SESSION_ANALYSIS_AZURE_ACCOUNT: KEY.slice(0, 20) }]) {
      for (const fn of [() => analysisRunnerConfig(azureEnv(extra)), () => runnerSecrets(azureEnv(extra))]) {
        try { fn(); } catch (e) { texts.push(String((e as Error).message), String((e as Error).stack), logLine("error", "session analysis runner refused", runnerFailureFields(e))); }
      }
    }
    const result = await analysisRunnerService(azureEnv({ SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: KEY }), { host: "127.0.0.1", port: 0, scrubInit: async () => {} }).catch((e) => e);
    texts.push(String(result?.message), logLine("error", "session analysis runner refused", runnerFailureFields(result)));
    const joined = texts.join("\n");
    for (let at = 0; at + 12 <= KEY.length; at += 6) expect(joined.includes(KEY.slice(at, at + 12))).toBe(false);
  });
});

describe("the hosted default is unchanged", () => {
  test("unset LOCKER_PROVIDER reads no azure setting; the canonical provider stays exactly s3 (runner and Lambda)", async () => {
    const plain = analysisRunnerConfig(s3Env());
    const withAzureVars = analysisRunnerConfig(s3Env({ SESSION_ANALYSIS_AZURE_ACCOUNT: "fxacct", SESSION_ANALYSIS_AZURE_CONTAINER: "fx-container",
      SESSION_ANALYSIS_AZURE_ACCOUNT_KEY: KEY, SESSION_ANALYSIS_AZURE_ENDPOINT: "junk" }));
    expect(withAzureVars).toEqual(plain);
    expect("azure" in plain).toBe(false);
    expect(plain.credentials).toBe("podIdentity");
    const env = s3Env({ AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials", SESSION_ANALYSIS_DATABASE_URL: DB_URL, SESSION_ANALYSIS_OPENAI_API_KEY: MODEL_KEY });
    expect("azure" in runnerSecrets(env)).toBe(false);
    const { deps } = runnerAnalysisDeps(env, plain, { databaseUrl: DB_URL, modelKey: MODEL_KEY }, { pool: fakeDb, store: () => ({}) as ObjectStore });
    expect(deps.canonical.provider).toBe("s3");
    expect(deps.canonical.bucket).toBe("example-pipeline-bucket");
    const lambda = lambdaAnalysisDeps({ bucket: "example-pipeline-bucket", region: "us-east-1", keyPrefix: "96bulls" }, fakeDb(), {} as ObjectStore, MODEL_KEY);
    expect(lambda.canonical.provider).toBe("s3");
    expect(Object.keys(lambda.canonical).sort()).toEqual(Object.keys(deps.canonical).sort());
  });
});

describe("azure deps", () => {
  test("store, sweeper eraser and canonical read are built from one target: account, container, endpoint, Shared Key", async () => {
    const env = azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: "http://127.0.0.1:10000/fxacct" });
    const cfg = analysisRunnerConfig(env);
    const secrets = runnerSecrets(env);
    const stores: AzureBlobConfig[] = [];
    const erasers: AzureBlobEraserConfig[] = [];
    const ports: RunnerPorts = {
      pool: fakeDb,
      azureStore: (c) => { stores.push(c); return {} as ObjectStore; },
      azureEraserFactory: (c): PagedEraser => { erasers.push(c); return { eraseStep: async () => ({ status: "traversed", deleted: 0, listed: 0 }), verifyErased: async () => ({ status: "erased", listed: 0 }) }; },
      store: () => { throw new Error("no S3 store on azure"); },
      eraserFactory: () => { throw new Error("no S3 eraser on azure"); },
    };
    const { deps, target } = runnerAzureAnalysisDeps(env, cfg, secrets, ports);
    expect(target).toMatchObject({ provider: "azure", account: "fxacct", container: "fx-container", endpoint: "http://127.0.0.1:10000/fxacct" });
    expect(Object.isFrozen(target)).toBe(true);
    expect(stores).toHaveLength(1);
    expect(stores[0]).toMatchObject({ account: "fxacct", container: "fx-container", endpoint: "http://127.0.0.1:10000/fxacct", auth: { kind: "sharedKey", accountKey: KEY }, versionIds: true });
    expect(deps.canonical).toMatchObject({ provider: "azure", bucket: "fx-container", prefix: { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "fxroot" } });
    expect(erasers).toHaveLength(0);
    await deps.eraser!.eraseStep("fxroot/fx-org/", {} as never);
    await deps.eraser!.verifyErased("fxroot/fx-org/", {} as never);
    // Built per call, from the same target, with the key only behind its closure.
    expect(erasers).toHaveLength(2);
    expect(erasers[0]).toMatchObject({ account: "fxacct", container: "fx-container", endpoint: "http://127.0.0.1:10000/fxacct" });
    expect(erasers[0]!.accountKey()).toBe(KEY);
    expect(JSON.stringify(erasers[0])).not.toContain(KEY.slice(0, 16));
  });
});

describe("the Azure store never follows a redirect (Shared Key requests are not replayed)", () => {
  test("a 3xx on a put or a read is refused as azure_redirect; the Location is never requested; every request is redirect manual", async () => {
    const fake = new FakeAzureBlob({ accountKey: KEY });
    const env = azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: FAKE_ENDPOINT });
    const { deps } = runnerAzureAnalysisDeps(env, analysisRunnerConfig(env), runnerSecrets(env), { pool: fakeDb, fetcher: fake.fetch });
    const key = "fxroot/fx-org/v1/normalized/session-analysis-v2/fx-job.json";
    expect((await deps.store.put(key as never, new TextEncoder().encode("{}"), { ifNoneMatch: true })).created).toBe(true);
    for (const method of ["PUT", "GET"]) {
      fake.hook = (req) => (req.method === method ? new Response(null, { status: 307, headers: { location: "https://fx-elsewhere.invalid/x" } }) : undefined);
      const op = method === "PUT"
        ? deps.store.put(`${key}.2` as never, new TextEncoder().encode("{}"), { ifNoneMatch: true })
        : deps.store.getObject(key as never);
      const error = await op.then(() => null, (e) => e);
      expect([method, (error?.cause as { code?: string })?.code]).toEqual([method, "azure_redirect"]);
      // A misconfigured endpoint is not transient: the job fails without burning retries.
      expect([method, classifyAnalysisFailure(error)]).toEqual([method, { code: "analysis_store_redirect", retryable: false }]);
    }
    // Any other transient store failure keeps its retry.
    expect(classifyAnalysisFailure(new ObjectStoreError("transient", "azure", "request failed"))).toEqual({ code: "analysis_processing_failed", retryable: true });
    expect(fake.requests.every((r) => r.url.hostname === "127.0.0.1" && r.redirect === "manual")).toBe(true);
  });
});

describe("azure startup", () => {
  const run = (env: NodeJS.ProcessEnv, ports: RunnerPorts, onListening?: () => void, signal?: AbortSignal) =>
    analysisRunnerService(env, { host: "127.0.0.1", port: 0, scrubInit: async () => {}, ports, ...(onListening ? { onListening } : {}), ...(signal ? { signal } : {}) });

  test("every probe verdict but Enabled refuses before the pool, the store or a listener; transient exits 1", async () => {
    const cases = [
      ["not_versioned", "blob_not_versioned"], ["unverified", "blob_versioning_unverified"], ["redirect", "blob_versioning_redirect"],
      ["soft_delete_retained", "blob_soft_delete_retained"], ["permanent_delete_forbidden", "blob_permanent_delete_forbidden"],
      ["auth_refused", "blob_auth_refused"], ["container_missing", "blob_container_missing"],
    ] as const;
    for (const [verdict, want] of cases) {
      let listened = false, pools = 0, stores = 0;
      const probes: string[] = [];
      const result = await run(azureEnv(), {
        pool: () => { pools++; return fakeDb(); }, azureStore: () => { stores++; return {} as ObjectStore; },
        blobVersioning: async (c) => { probes.push(`${c.account}|${c.container}|${c.prefix}`); return verdict; },
      }, () => { listened = true; }).catch((e) => e);
      expect([verdict, result instanceof AnalysisRunnerRefused ? result.code : String(result), runnerExitCode(result)]).toEqual([verdict, want, 2]);
      expect([verdict, listened, pools, stores]).toEqual([verdict, false, 0, 0]);
      expect(probes).toEqual(["fxacct|fx-container|fxroot/.runner-probe/session-analysis/"]);
    }
    const transient = await run(azureEnv(), { pool: fakeDb, blobVersioning: async () => { throw Object.assign(new Error("x"), { code: "blob_versioning_unavailable" }); } }).catch((e) => e);
    expect([transient.code, runnerExitCode(transient)]).toEqual(["blob_versioning_unavailable", 1]);
    // Any other throw is not something a restart fixes: a refusal, not a retry.
    const other = await run(azureEnv(), { pool: fakeDb, blobVersioning: async () => { throw new TypeError("unexpected"); } }).catch((e) => e);
    expect([other.code, runnerExitCode(other)]).toEqual(["blob_versioning_unverified", 2]);
  });

  test("against the fake Azure container: a wrong key refuses blob_auth_refused and a missing container blob_container_missing (exit 2, no retry)", async () => {
    const wrongKey = new FakeAzureBlob({ accountKey: randomKey() });
    const missing = new FakeAzureBlob({ accountKey: KEY, container: "fx-other-container" });
    for (const [fake, want] of [[wrongKey, "blob_auth_refused"], [missing, "blob_container_missing"]] as const) {
      const result = await run(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: FAKE_ENDPOINT }), { fetcher: fake.fetch, pool: fakeDb }).catch((e) => e);
      expect([want, result instanceof AnalysisRunnerRefused ? result.code : String(result), runnerExitCode(result)]).toEqual([want, want, 2]);
    }
  });

  test("against the fake Azure container: versioning on starts (real probe, real eraser); versioning off refuses blob_not_versioned", async () => {
    for (const [versioning, want] of [[true, "started"], [false, "blob_not_versioned"]] as const) {
      const fake = new FakeAzureBlob({ accountKey: KEY, versioning });
      const stop = new AbortController();
      let pools = 0;
      const result = await run(azureEnv({ SESSION_ANALYSIS_AZURE_ENDPOINT: FAKE_ENDPOINT }),
        { fetcher: fake.fetch, pool: () => { pools++; return fakeDb(); } }, () => stop.abort(), stop.signal)
        .then(() => "started", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
      expect([versioning, result]).toEqual([versioning, want]);
      expect([versioning, pools]).toEqual([versioning, versioning ? 1 : 0]);
      expect(fake.records).toEqual([]);
      expect(fake.requests.length).toBeGreaterThan(0);
    }
  });
});
