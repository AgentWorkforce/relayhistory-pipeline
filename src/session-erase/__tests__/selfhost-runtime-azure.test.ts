/**
 * The in-service operator erase runtime on LOCKER_PROVIDER=azure (selfhost-runtime.ts): the store identity is the
 * writer's Azure store, its storeRef binds the endpoint (two endpoints of one container are two identities, on
 * purpose), the S3 identity is byte-identical to before (pinned by its formula), and the bytes worker refuses to start
 * unless the erase target, the key root and the data-plane preflight all hold, before anything else is built.
 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeAzureBlob, type FakeAzureOptions } from "../../migration/__tests__/fake-azure-blob.ts";
import { buildSelfhostEraseRuntime, eraseStoreIdentity } from "../selfhost-runtime.ts";

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

test("bytes worker on azure: the preflight passes before anything else is built (it then needs its search login, as on S3)", async () => {
  const { fake, server, endpoint } = serve();
  try {
    const error = await build(runtimeEnv(endpoint)).then(() => null, (e: Error) => e);
    expect(error?.message).toMatch(/SESSION_ERASE_SEARCH_POSTGRES_URL required/);
    expect(fake.requests.some((q) => q.method === "PUT" && decodeURIComponent(q.url.pathname).includes("/96bulls/.runner-probe/session-erase/"))).toBe(true);
    expect(fake.records).toEqual([]);
  } finally { server.stop(true); }
});

test("bytes worker on azure refuses at startup, by code, before the search login: no versioning, wrong store, wrong root", async () => {
  const cases: [FakeAzureOptions, Record<string, string | undefined>, RegExp, boolean][] = [
    [{ versioning: false }, {}, /operator erase refused: azure_blob_not_versioned/, true],
    [{ softDelete: true }, {}, /operator erase refused: azure_blob_soft_delete_retained/, true],
    [{}, { SESSION_ERASE_AZURE_CONTAINER: "fx-other-container" }, /operator erase refused: azure_target_not_writer_store/, false],
    [{}, { SESSION_ERASE_KEY_ROOT: "fx-other-root" }, /operator erase refused: azure_key_root_not_writer/, false],
    [{}, { SESSION_ERASE_DRY_RUN: "1" }, /must target the writer store, without dry run/, false],
    [{}, { SESSION_ERASE_S3_BUCKET: "fx-bucket" }, /operator erase refused: azure_s3_settings_conflict/, false],
  ];
  for (const [opts, extra, want, probed] of cases) {
    const { fake, server, endpoint } = serve(opts);
    try {
      const error = await build(runtimeEnv(endpoint, extra)).then(() => null, (e: Error) => e);
      expect([String(want), String(error?.message).match(want) !== null]).toEqual([String(want), true]);
      expect([String(want), fake.requests.length > 0]).toEqual([String(want), probed]);
      expect(String(error?.message)).not.toContain(KEY.slice(0, 16));
    } finally { server.stop(true); }
  }
});
