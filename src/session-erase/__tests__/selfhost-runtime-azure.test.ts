/**
 * The in-service operator erase runtime on LOCKER_PROVIDER=azure (selfhost-runtime.ts): the store identity is the
 * writer's Azure store, its storeRef binds the endpoint (two endpoints of one container are two identities, on
 * purpose), the S3 identity is byte-identical to before (pinned by its formula), and the bytes worker refuses to start
 * unless the erase target, the key root and the data-plane preflight all hold, before anything else is built.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAzureBlobEraser } from "../../migration/azure-blob-erasure/eraser.ts";
import { FAKE_ENDPOINT, FakeAzureBlob, type FakeAzureOptions } from "../../migration/__tests__/fake-azure-blob.ts";
import { azureEraseTarget, createAzurePreflightGate } from "../azure-target.ts";
import { sessionEraseWorker } from "../runtime.ts";
import { eraseDiscoveredSessionBytes } from "../selfhost-eraser.ts";
import { buildSelfhostEraseRuntime, eraseStoreIdentity, stagedObjectsAbsent } from "../selfhost-runtime.ts";

const sha = (v: unknown) => createHash("sha256").update(JSON.stringify(v)).digest("hex");
const SEARCH = ["postgres", "search_index"];

let dir = "", KEY = "", keyFile = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "erase-runtime-azure-"));
  KEY = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
  keyFile = join(dir, "account-key");
  writeFileSync(keyFile, KEY);
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

test("the S3 store identity is unchanged (pinned by its formula)", () => {
  const base = { LOCKER_BUCKET: "fx-bucket", LOCKER_ENDPOINT: "http://127.0.0.1:9000" };
  expect(eraseStoreIdentity(base, "root")).toEqual({ bucket: "fx-bucket", keyRoot: "root",
    storeRef: sha(["s3", "fx-bucket", "us-east-1", "http://127.0.0.1:9000", "root", SEARCH]) });
  expect(eraseStoreIdentity({ LOCKER_BUCKET: "fx-bucket", LOCKER_REGION: "eu-west-1" }, "root").storeRef)
    .toBe(sha(["s3", "fx-bucket", "eu-west-1", null, "root", SEARCH]));
  expect(() => eraseStoreIdentity({ LOCKER_BUCKET: "fx-bucket", LOCKER_PROVIDER: "gcs" }, "root")).toThrow(/versioned S3/);
});

test("azure: the writer's store; storeRef binds account, container, endpoint, root and search; the key never enters it", () => {
  const base = { LOCKER_PROVIDER: "azure", LOCKER_AZURE_ACCOUNT: "fxacct", LOCKER_BUCKET: "fx-container", LOCKER_AZURE_ACCOUNT_KEY: KEY };
  const host = eraseStoreIdentity(base, "root");
  expect(host).toEqual({ bucket: "fx-container", keyRoot: "root", storeRef: sha(["azure", "fxacct", "fx-container", null, "root", SEARCH]) });
  const azurite = eraseStoreIdentity({ ...base, LOCKER_ENDPOINT: "http://127.0.0.1:10000/fxacct" }, "root");
  expect(azurite.storeRef).toBe(sha(["azure", "fxacct", "fx-container", "http://127.0.0.1:10000/fxacct", "root", SEARCH]));
  // Two endpoints of the same account and container are two identities (a receipt never moves endpoint).
  expect(azurite.storeRef).not.toBe(host.storeRef);
  expect(eraseStoreIdentity({ ...base, LOCKER_ENDPOINT: "http://127.0.0.1:10000/fxacct/" }, "root")).toEqual(azurite);
  expect(eraseStoreIdentity({ ...base, LOCKER_AZURE_ACCOUNT_KEY: "rotated" }, "root")).toEqual(host);
  expect(eraseStoreIdentity(base, "other").storeRef).not.toBe(host.storeRef);
  // Never the S3 identity of a same-named bucket.
  expect(host.storeRef).not.toBe(eraseStoreIdentity({ LOCKER_BUCKET: "fx-container" }, "root").storeRef);
  expect(JSON.stringify([host, azurite])).not.toContain(KEY.slice(0, 16));
  for (const bad of [{ LOCKER_AZURE_ACCOUNT: undefined }, { LOCKER_BUCKET: undefined }, { LOCKER_ENDPOINT: "http://127.0.0.1:10000/fxacct?sv=1" },
    { LOCKER_ENDPOINT: "http://fxacct.blob.example.test" }])
    expect(() => eraseStoreIdentity({ ...base, ...bad }, "root")).toThrow(/operator erase/);
});

/** The fake container behind a real local port, so the runtime's own (timed, no-redirect) fetch reaches it. */
function serve(opts: FakeAzureOptions = {}) {
  const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY, ...opts });
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (req) =>
    fake.fetch(req.url, { method: req.method, headers: req.headers, redirect: "manual",
      ...(req.method === "PUT" ? { body: new Uint8Array(await req.arrayBuffer()) } : {}) }) });
  return { fake, server, endpoint: `http://127.0.0.1:${server.port}/fxacct` };
}

function runtimeEnv(endpoint: string, extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  const e: Record<string, string | undefined> = {
    LOCKER_PROVIDER: "azure", LOCKER_AZURE_ACCOUNT: "fxacct", LOCKER_BUCKET: "fx-container", LOCKER_ENDPOINT: endpoint,
    SESSION_ERASE_AZURE_ACCOUNT: "fxacct", SESSION_ERASE_AZURE_CONTAINER: "fx-container", SESSION_ERASE_AZURE_ENDPOINT: endpoint,
    SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE: keyFile, ...extra,
  };
  for (const k of Object.keys(e)) if (e[k] === undefined) delete e[k];
  return e as NodeJS.ProcessEnv;
}
const build = (env: NodeJS.ProcessEnv) => buildSelfhostEraseRuntime({ env, keyRoot: "96bulls", api: { enabled: false, reason: "flag_off" } as never,
  enforced: true, bytesEnabled: true, run: async () => ({ rows: [{ ready: true }] }) as never, pool: {} as never, metadata: { max: 2 } });

const OPENSEARCH = { SEARCH_INDEX_BACKEND: "opensearch", SEARCH_INDEX_OPENSEARCH_URL: "http://127.0.0.1:9",
  SESSION_ERASE_OPENSEARCH_USERNAME: "fx-erase", SESSION_ERASE_OPENSEARCH_PASSWORD: "fx-erase-password" };

test("ingest startup never runs the data-plane preflight: zero Azure requests, and startup succeeds even when the probe would fail", async () => {
  for (const opts of [{}, { versioning: false }, { softDelete: true }] as FakeAzureOptions[]) {
    const { fake, server, endpoint } = serve(opts);
    try {
      const runtime = await build(runtimeEnv(endpoint, OPENSEARCH));
      expect(runtime).toBeDefined();
      expect([opts, fake.requests.length]).toEqual([opts, 0]);
      await runtime!.close();
    } finally { server.stop(true); }
  }
});

test("pure misconfiguration still fails startup by code, with no request (a config error, as on S3)", async () => {
  const cases: [Record<string, string | undefined>, RegExp][] = [
    [{ SESSION_ERASE_AZURE_CONTAINER: "fx-other-container" }, /operator erase refused: azure_target_not_writer_store/],
    [{ SESSION_ERASE_KEY_ROOT: "fx-other-root" }, /operator erase refused: azure_key_root_not_writer/],
    [{ SESSION_ERASE_DRY_RUN: "1" }, /must target the writer store, without dry run/],
    [{ SESSION_ERASE_S3_BUCKET: "fx-bucket" }, /operator erase refused: azure_s3_settings_conflict/],
  ];
  for (const [extra, want] of cases) {
    const { fake, server, endpoint } = serve();
    try {
      const error = await build(runtimeEnv(endpoint, { ...OPENSEARCH, ...extra })).then(() => null, (e: Error) => e);
      expect([String(want), String(error?.message).match(want) !== null, fake.requests.length]).toEqual([String(want), true, 0]);
      expect(String(error?.message)).not.toContain(KEY.slice(0, 16));
    } finally { server.stop(true); }
  }
});

describe("the lazy preflight gate (before the worker's first byte work)", () => {
  const target = (fake: FakeAzureBlob) => azureEraseTarget(runtimeEnvFor(fake), (c) => { throw new Error(c); });
  const runtimeEnvFor = (_fake: FakeAzureBlob) => runtimeEnv(FAKE_ENDPOINT);
  const sessionDeletes = (fake: FakeAzureBlob) => fake.requests.filter((q) => q.method === "DELETE" && !decodeURIComponent(q.url.pathname).includes(".runner-probe")).length;

  test("a refusing probe: the work never runs, nothing is deleted, one coded line is logged; once the account is fixed the next claim passes", async () => {
    const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY, versioning: false });
    fake.put("96bulls/fx-org/v1/ws=fx-ws/m=fx/h=claude-code/d=2026/11/01/s=fx-s/manifest.json", "m");
    const logs: Record<string, unknown>[] = [];
    const gate = createAzurePreflightGate(target(fake), "96bulls", (l) => logs.push(l), { fetch: fake.fetch });
    let ran = 0;
    const first = await gate.gate(async () => { ran++; return 1; }).catch((e) => e);
    expect([String(first?.message), ran, sessionDeletes(fake), gate.passed]).toEqual(["operator erase refused: azure_blob_not_versioned", 0, 0, false]);
    expect(logs).toEqual([{ event: "operator_session_erase_preflight", outcome: "refused", reason: "azure_blob_not_versioned" }]);
    fake.versioning = true;
    expect(await gate.gate(async () => { ran++; return 2; })).toBe(2);
    expect([ran, gate.passed]).toEqual([1, true]);
  });

  test("an unavailable probe (5xx) refuses the same way and is retried on the next claim; a passed probe is cached", async () => {
    const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY });
    let down = true;
    fake.hook = (req) => (down && req.method === "PUT" ? new Response(null, { status: 503 }) : undefined);
    const logs: Record<string, unknown>[] = [];
    const gate = createAzurePreflightGate(target(fake), "96bulls", (l) => logs.push(l), { fetch: fake.fetch });
    const first = await gate.gate(async () => 1).catch((e) => e);
    expect((first as { code?: string }).code).toBe("erase_blob_versioning_unavailable");
    expect(logs[0]).toMatchObject({ outcome: "refused", reason: "erase_blob_versioning_unavailable" });
    down = false;
    expect(await gate.gate(async () => 2)).toBe(2);
    const after = fake.requests.length;
    expect(await gate.gate(async () => 3)).toBe(3);
    expect(fake.requests.length).toBe(after);
  });

  test("an Azure refusal mid-run resets the gate: the next claim probes again", async () => {
    const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY });
    const gate = createAzurePreflightGate(target(fake), "96bulls", () => {}, { fetch: fake.fetch });
    await gate.gate(async () => 1);
    await expect(gate.gate(async () => { throw Object.assign(new Error("x"), { code: "azure_soft_delete_retained", op: "delete" }); })).rejects.toBeDefined();
    expect(gate.passed).toBe(false);
    const before = fake.requests.filter((q) => q.method === "PUT").length;
    await gate.gate(async () => 2);
    expect(fake.requests.filter((q) => q.method === "PUT").length).toBe(before + 1);
    // A non-Azure failure (a bug, a 5xx) keeps the pass.
    await expect(gate.gate(async () => { throw new Error("boom"); })).rejects.toBeDefined();
    expect(gate.passed).toBe(true);
  });
});

test("the erase worker over the gate: a refusing probe leaves the receipt failed_retryable with zero deletes; after the fix it erases and purges", async () => {
  const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY, versioning: false });
  const tgt = azureEraseTarget(runtimeEnv(FAKE_ENDPOINT), (c) => { throw new Error(c); });
  const eraser = createAzureBlobEraser({ account: "fxacct", container: "fx-container", endpoint: FAKE_ENDPOINT, accountKey: tgt.accountKey, fetch: fake.fetch });
  const gate = createAzurePreflightGate(tgt, "96bulls", () => {}, { fetch: fake.fetch });
  const identity = { bucket: "fx-container", keyRoot: "96bulls", storeRef: "fx-ref" };
  const work = { orgId: "fx-org", workspaceId: "fx-ws", source: "claude-code", sessionIds: ["fx-s"], ...identity };
  const steps: string[] = [];
  let purged = 0, claims = 0;
  const ledger = {
    next: async () => (claims++ < 1 ? work : null), authorize: async () => true,
    step: async (_w: unknown, _l: unknown, artifact: string, state: string, _n: number, code?: string) => { steps.push([artifact, state, code ?? ""].join(":")); },
    purge: async () => { purged++; }, release: async () => {},
  };
  const worker = sessionEraseWorker({ ledger: ledger as never, identity, search: { erase: async () => {} },
    searchLock: async (_w, run) => run(),
    bytes: {
      erase: (key, authorize) => gate.gate(() => eraseDiscoveredSessionBytes(eraser, key, { keyRoot: "96bulls", bucket: "fx-container", provider: "azure", authorize })),
      stagesAbsent: (key, authorize) => gate.gate(() => stagedObjectsAbsent(eraser, "96bulls", key, authorize)),
    } });
  const DIR = "96bulls/fx-org/v1/ws=fx-ws/m=fx-m/h=claude-code/d=2026/11/01/s=fx-s/";
  fake.put(`${DIR}manifest.json`, "m");
  await worker.scan();
  expect(steps).toEqual(["transcriptObjects:failed_retryable:erase_step_failed"]);
  expect(fake.requests.filter((q) => q.method === "DELETE" && !decodeURIComponent(q.url.pathname).includes(".runner-probe")).length).toBe(0);
  expect(purged).toBe(0);
  // The operator fixes the account (versioning on) and re-puts the object so it has a version; the next claim erases.
  fake.versioning = true;
  fake.records = [];
  fake.put(`${DIR}manifest.json`, "m");
  claims = 0; steps.length = 0;
  await worker.scan();
  expect(steps[0]).toBe("transcriptObjects:done:");
  expect(purged).toBe(1);
  expect(fake.records.filter((r) => r.key.startsWith(DIR))).toEqual([]);
});
