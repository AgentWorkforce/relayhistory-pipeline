/**
 * The coordination advisor with LOCKER_PROVIDER=fs|azure: runWorker's startup past the vendored bindings
 * (worker.ts runWorker -> workerBindings -> pipelineQueueDeps) with the exact options advisorMain passes
 * (main.ts ADVISOR_WORKER), while s3, gcs and an unset provider resolve exactly as vendorBindings() does.
 * The advisor never reads the Locker; advisor-tick-selfhosted.pglite.test.ts runs a tick with the Locker
 * environment unusable. The HoopDb waits before workerBindings need a database and are not exercised here.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";
import { vendorBindings } from "../../shared/env.ts";
import { pipelineQueueDeps, rawStoreFromEnv } from "../../shared/vendor.ts";
import { workerAdapterEntry, workerBindings, type WorkerOptions } from "../../shared/worker.ts";
import { ADVISOR_SERVICE, ADVISOR_WORKER, advisorMain, type AdvisorIo } from "../main.ts";

const ACCOUNT_KEY = Buffer.from("synthetic-azure-account-key").toString("base64");
const KEYS = [
  "LOCKER_PROVIDER", "LOCKER_FS_ROOT", "LOCKER_BUCKET", "LOCKER_ENDPOINT", "LOCKER_ACCESS_KEY_ID", "LOCKER_SECRET_ACCESS_KEY",
  "LOCKER_AZURE_ACCOUNT", "LOCKER_AZURE_ACCOUNT_KEY", "RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER", "RELAYHISTORY_SELFHOST_INGEST",
  "RELAYHISTORY_SELFHOST_READ_API", "PIPELINE_ORG_FROM_PREFIX", "PIPELINE_ORG_ID", "PIPELINE_KEY_PREFIX", "TENANT_ORG_ID",
  "AWS_LAMBDA_FUNCTION_NAME", "SEARCH_INDEX_BACKEND",
] as const;
const saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
const roots: string[] = [];

function setEnv(env: Record<string, string>) {
  for (const k of KEYS) delete process.env[k];
  Object.assign(process.env, env);
}

afterEach(() => {
  for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function fsAdvisorEnv(): Record<string, string> {
  const root = mkdtempSync(join(tmpdir(), "advisor-locker-fs-"));
  roots.push(root);
  return {
    LOCKER_PROVIDER: "fs", LOCKER_FS_ROOT: root, LOCKER_BUCKET: "fx-bucket",
    RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true", TENANT_ORG_ID: "fx-org",
  };
}

const azureAdvisorEnv = (): Record<string, string> => ({
  LOCKER_PROVIDER: "azure", LOCKER_BUCKET: "fx-sessions", LOCKER_AZURE_ACCOUNT: "devstoreaccount1", LOCKER_AZURE_ACCOUNT_KEY: ACCOUNT_KEY,
  PIPELINE_ORG_FROM_PREFIX: "true",
});

/** The options advisorMain hands to runWorker, captured from a real advisorMain call (postgres facts, DM off). */
async function advisorRunWorkerOptions(): Promise<WorkerOptions> {
  let captured: WorkerOptions | undefined;
  const io: AdvisorIo = {
    runWorker: async (opts) => { captured = opts; },
    idle: async () => { throw new Error("an enabled advisor must not idle"); },
    backend: () => () => { throw new Error("no tick runs here"); },
    sender: () => undefined,
  };
  expect(await advisorMain({ COORDINATION_ADVISOR_ENABLED: "true", COORDINATION_ADVISOR_FACTS_BACKEND: "postgres" }, io)).toBe("ran");
  return captured!;
}

/** runWorker from the adapter check through pipelineQueueDeps, with in-memory queue, dead letters and sink. */
function advisorStartupPastBindings(opts: WorkerOptions) {
  validateAdapterEnv(process.env, workerAdapterEntry(opts));
  const env = workerBindings(opts);
  const deps = pipelineQueueDeps(env as Parameters<typeof pipelineQueueDeps>[0], {
    queue: { backend: "postgres", send: async () => {}, sendBatch: async () => {} } as never,
    deadLetters: { quarantine: async () => {} } as never,
    sink: { emit: async () => {} } as never,
    handlers: {},
  });
  return { env, deps };
}

describe("coordination advisor with an fs or azure Locker", () => {
  test("advisorMain runs the worker with ADVISOR_WORKER: no stage, its own (unused) Locker", async () => {
    expect(ADVISOR_WORKER).toEqual({ service: ADVISOR_SERVICE, stages: [], ownLocker: true });
    const opts = await advisorRunWorkerOptions();
    expect([opts.service, opts.stages, opts.ownLocker]).toEqual([ADVISOR_SERVICE, [], true]);
    expect(Object.keys(opts.handlers())).toEqual([]);
    expect(typeof opts.tick).toBe("function");
  });

  for (const [name, envOf] of [["fs", fsAdvisorEnv], ["azure", azureAdvisorEnv]] as const) {
    test(`${name}: startup past the vendored bindings does not throw and carries no raw-store mapping`, async () => {
      setEnv(envOf());
      const { env, deps } = advisorStartupPastBindings(await advisorRunWorkerOptions());
      expect(Object.keys(env).filter((k) => k.startsWith("PIPELINE_RAW_"))).toEqual([]);
      // A stray rawStoreFromEnv sees "not configured", never a store under the wrong provider.
      expect(rawStoreFromEnv(env)).toBeUndefined();
      expect(deps.handlers).toBeDefined();
    });

    test(`${name}: without ownLocker the same environment refuses (the advisor before this change)`, async () => {
      setEnv(envOf());
      const { ownLocker: _, ...before } = await advisorRunWorkerOptions();
      expect(() => workerBindings(before)).toThrow(`LOCKER_PROVIDER=${name} is not available to the vendored pipeline routes`);
    });
  }

  test("negative: every Locker and tenant check still applies to the advisor", async () => {
    const opts = await advisorRunWorkerOptions();
    const fsNoFlag = fsAdvisorEnv();
    delete (fsNoFlag as Record<string, string | undefined>).RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER;
    setEnv(fsNoFlag);
    expect(() => workerBindings(opts)).toThrow("LOCKER_PROVIDER=fs is self-host only");
    const fsNoBucket = fsAdvisorEnv();
    delete (fsNoBucket as Record<string, string | undefined>).LOCKER_BUCKET;
    setEnv(fsNoBucket);
    expect(() => workerBindings(opts)).toThrow();
    const { LOCKER_AZURE_ACCOUNT_KEY: _, ...azureNoKey } = azureAdvisorEnv();
    setEnv(azureNoKey);
    expect(() => workerBindings(opts)).toThrow();
    const { PIPELINE_ORG_FROM_PREFIX: __, ...azureNoTenant } = azureAdvisorEnv();
    setEnv(azureNoTenant);
    expect(() => workerBindings(opts)).toThrow("TENANT_ORG_ID");
    setEnv({ ...azureAdvisorEnv(), LOCKER_PROVIDER: "azurex" });
    expect(() => workerBindings(opts)).toThrow();
  });

  test("s3 (the default, also unset) and gcs: the advisor's bindings are exactly vendorBindings() and those without ownLocker", async () => {
    const opts = await advisorRunWorkerOptions();
    const { ownLocker: _, ...before } = opts;
    for (const env of [
      { LOCKER_BUCKET: "fx-locker", LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b", PIPELINE_ORG_FROM_PREFIX: "true" },
      { LOCKER_PROVIDER: "s3", LOCKER_BUCKET: "fx-locker", LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b", TENANT_ORG_ID: "fx-org" },
      { LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: "fx-locker", PIPELINE_ORG_FROM_PREFIX: "true" },
    ]) {
      setEnv(env);
      const after = JSON.stringify(workerBindings(opts));
      expect(after).toBe(JSON.stringify(vendorBindings()));
      expect(after).toBe(JSON.stringify(workerBindings(before)));
      expect(workerBindings(opts).PIPELINE_RAW_PROVIDER).toBe(env.LOCKER_PROVIDER ?? "s3");
      expect(rawStoreFromEnv(workerBindings(opts))?.provider).toBe(env.LOCKER_PROVIDER ?? "s3");
    }
    // The s3/gcs refusals are unchanged too: no bucket, no tenant.
    setEnv({ LOCKER_PROVIDER: "gcs", PIPELINE_ORG_FROM_PREFIX: "true" });
    expect(() => workerBindings(opts)).toThrow();
    expect(() => workerBindings(before)).toThrow();
    setEnv({ LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: "fx-locker" });
    expect(() => workerBindings(opts)).toThrow("TENANT_ORG_ID");
    expect(() => workerBindings(before)).toThrow("TENANT_ORG_ID");
  });
});
