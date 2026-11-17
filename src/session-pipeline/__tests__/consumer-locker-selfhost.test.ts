/**
 * The self-host pipeline consumer with LOCKER_PROVIDER=fs|azure: runWorker's startup past the vendored
 * bindings (worker.ts runWorker -> workerBindings -> pipelineQueueDeps) with the exact options the consumer
 * passes (consumer-service.ts CONSUMER_WORKER), while every other vendorBindings caller still refuses.
 * The HoopDb waits before that point need a database and are not exercised here.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { validateAdapterEnv } from "@rhc/adapters/registry.ts";
import { CONSUMER_WORKER } from "../consumer-service.ts";
import { vendorBindings } from "../../shared/env.ts";
import { buildLockerStore } from "../../shared/locker.ts";
import { pipelineQueueDeps, rawStoreFromEnv } from "../../shared/vendor.ts";
import { workerAdapterEntry, workerBindings } from "../../shared/worker.ts";

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

function fsConsumerEnv(): Record<string, string> {
  const root = mkdtempSync(join(tmpdir(), "consumer-locker-fs-"));
  roots.push(root);
  return {
    LOCKER_PROVIDER: "fs", LOCKER_FS_ROOT: root, LOCKER_BUCKET: "x",
    RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true", PIPELINE_ORG_FROM_PREFIX: "true",
  };
}

const azureConsumerEnv = (): Record<string, string> => ({
  LOCKER_PROVIDER: "azure", LOCKER_BUCKET: "sessions", LOCKER_AZURE_ACCOUNT: "devstoreaccount1", LOCKER_AZURE_ACCOUNT_KEY: ACCOUNT_KEY,
  RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER: "true", PIPELINE_ORG_FROM_PREFIX: "true",
});

/** runWorker from the adapter check through pipelineQueueDeps, with in-memory queue, dead letters and sink. */
function consumerStartupPastBindings() {
  validateAdapterEnv(process.env, workerAdapterEntry(CONSUMER_WORKER));
  const env = workerBindings(CONSUMER_WORKER);
  const deps = pipelineQueueDeps(env as Parameters<typeof pipelineQueueDeps>[0], {
    queue: { backend: "postgres", send: async () => {}, sendBatch: async () => {} } as never,
    deadLetters: { quarantine: async () => {} } as never,
    sink: { emit: async () => {} } as never,
    handlers: {},
  });
  return { env, deps };
}

describe("self-host pipeline consumer with an fs or azure Locker", () => {
  test("the consumer claims no stage and reads its own Locker store", () => {
    expect(CONSUMER_WORKER.stages).toEqual([]);
    expect(CONSUMER_WORKER.ownLocker).toBe(true);
  });

  for (const [name, envOf, backend] of [["fs", fsConsumerEnv, "fs"], ["azure", azureConsumerEnv, "azure"]] as const) {
    test(`${name}: startup past the vendored bindings does not throw and carries no raw-store mapping`, () => {
      setEnv(envOf());
      const { env, deps } = consumerStartupPastBindings();
      expect(Object.keys(env).filter((k) => k.startsWith("PIPELINE_RAW_"))).toEqual([]);
      // A stray rawStoreFromEnv sees "not configured", never a store under the wrong provider.
      expect(rawStoreFromEnv(env)).toBeUndefined();
      expect(env.PIPELINE_ORG_FROM_PREFIX).toBe("true");
      expect(deps.handlers).toBeDefined();
      // The store the consumer does read (consumer-service.ts lockerStore()).
      const raw = buildLockerStore(process.env);
      expect([raw.provider, raw.bucket, raw.store.backend]).toEqual([backend, envOf === fsConsumerEnv ? "x" : "sessions", backend]);
    });

    test(`${name}: every other vendorBindings caller still refuses (indexer, alleyoop, stage workers)`, () => {
      setEnv(envOf());
      const refusal = `LOCKER_PROVIDER=${name} is not available to the vendored pipeline routes`;
      expect(() => vendorBindings()).toThrow(refusal);
      expect(() => vendorBindings({ EXTRA: "1" })).toThrow(refusal);
      expect(() => workerBindings({ stages: ["object-landed"] })).toThrow(refusal);
      expect(() => workerBindings({ stages: [] })).toThrow(refusal);
      expect(() => workerBindings({ ownLocker: true, stages: ["object-landed"] })).toThrow("only for a worker that claims no stage");
    });
  }

  test("fs still needs the self-host flag, also for the consumer", () => {
    const env = fsConsumerEnv();
    delete (env as Record<string, string | undefined>).RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER;
    setEnv(env);
    expect(() => workerBindings(CONSUMER_WORKER)).toThrow("LOCKER_PROVIDER=fs is self-host only");
  });

  test("s3 (the default) and gcs: the consumer's bindings are exactly vendorBindings()", () => {
    setEnv({ LOCKER_BUCKET: "locker", LOCKER_ACCESS_KEY_ID: "a", LOCKER_SECRET_ACCESS_KEY: "b", PIPELINE_ORG_FROM_PREFIX: "true" });
    expect(JSON.stringify(workerBindings(CONSUMER_WORKER))).toBe(JSON.stringify(vendorBindings()));
    expect(workerBindings(CONSUMER_WORKER).PIPELINE_RAW_PROVIDER).toBe("s3");
    expect(rawStoreFromEnv(workerBindings(CONSUMER_WORKER))?.provider).toBe("s3");
    setEnv({ LOCKER_PROVIDER: "gcs", LOCKER_BUCKET: "locker", PIPELINE_ORG_FROM_PREFIX: "true" });
    expect(JSON.stringify(workerBindings(CONSUMER_WORKER))).toBe(JSON.stringify(vendorBindings()));
    expect(workerBindings(CONSUMER_WORKER).PIPELINE_RAW_PROVIDER).toBe("gcs");
  });
});
