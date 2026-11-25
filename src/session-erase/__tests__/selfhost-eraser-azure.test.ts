/**
 * The self-host per-session byte eraser on LOCKER_PROVIDER=azure (selfhost-eraser-main.ts, azure-target.ts) against the
 * in-memory Azure container (migration/__tests__/fake-azure-blob.ts): the target is the writer store exactly, execute
 * runs the data-plane preflight first, a dry run is list-only and says it certifies nothing, every version, snapshot and
 * soft-deleted copy of the session goes and a fresh listing proves it, rows of another provider are refused, and Azure
 * errors are classified by operation (a delete 404 is already gone; a list 404 / 403 refuses; a delete 403 fails).
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAzureBlobEraser } from "../../migration/azure-blob-erasure/eraser.ts";
import { FAKE_ENDPOINT, FakeAzureBlob, type FakeAzureOptions } from "../../migration/__tests__/fake-azure-blob.ts";
import { liveMachineId } from "../../session-ingest/writer.ts";
import { stagedKey } from "../../session-ingest/staging.ts";
import { azureBlobSettings, DEFAULT_AZURE_SETTINGS_PREFIX } from "../../shared/azure-blob-settings.ts";
import { analysisRunnerConfig } from "../../session-analysis/runner.ts";
import { eraseProbePrefix } from "../azure-target.ts";
import { planTargets, SessionEraseRefused, type SessionEraseBytesRequest } from "../selfhost-eraser.ts";
import { errorCodeOf, exitCodeForError, exitCodeForResult, runSessionEraseBytes } from "../selfhost-eraser-main.ts";

const ROOT = "96bulls", ORG = "fx-org", WS = "fx-ws", SRC = "claude-code", SID = "fx-session";
const M = liveMachineId(WS, "fx-origin", "fx-user");
const DIR = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SID}/`;
const POINTER = `${ROOT}/${ORG}/live-sessions/v1/ws=${WS}/m=${M}/h=${SRC}/s=${SID}.json`;
const STAGED = stagedKey(ROOT, { org_id: ORG, workspace_id: WS, origin_id: "fx-origin", user_id: "fx-user", batch_digest: "fx-batch", claim_token: "fx-claim" }, "ab".repeat(32));
const NEIGHBOURS = [
  `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SID}x/manifest.json`,
  `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=fx-other/manifest.json`,
  `${ROOT}/fx-org-b/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SID}/manifest.json`,
];
const DAYS = 24 * 3600_000;

let dir = "", KEY = "", keyFile = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "erase-azure-"));
  KEY = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
  keyFile = join(dir, "account-key");
  writeFileSync(keyFile, KEY);
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

function env(extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  const e: Record<string, string | undefined> = {
    SESSION_ERASE_BYTES_ENABLED: "1", LOCKER_PROVIDER: "azure",
    LOCKER_AZURE_ACCOUNT: "fxacct", LOCKER_BUCKET: "fx-container", LOCKER_ENDPOINT: FAKE_ENDPOINT,
    SESSION_ERASE_AZURE_ACCOUNT: "fxacct", SESSION_ERASE_AZURE_CONTAINER: "fx-container", SESSION_ERASE_AZURE_ENDPOINT: FAKE_ENDPOINT,
    SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE: keyFile, SESSION_ERASE_KEY_ROOT: ROOT, ...extra,
  };
  for (const k of Object.keys(e)) if (e[k] === undefined) delete e[k];
  return e as NodeJS.ProcessEnv;
}

const request = (over: Partial<SessionEraseBytesRequest> = {}): SessionEraseBytesRequest => ({
  key: { orgId: ORG, workspaceId: WS, source: SRC, sessionId: SID },
  locations: [{ workspaceId: WS, machineId: M, harness: SRC, sessionId: SID, manifestKey: `${DIR}manifest.json`, storageProvider: "azure", bucket: "fx-container" }],
  stagedKeys: [STAGED],
  ...over,
});

function seed(opts: FakeAzureOptions = {}) {
  const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY, ...opts });
  fake.put(`${DIR}manifest.json`, "m1"); fake.put(`${DIR}manifest.json`, "m2");
  for (let i = 0; i < 3; i++) fake.put(`${DIR}raw/${String(i).padStart(10, "0")}-0-abcd1234.ndjson.zst`, `r${i}`);
  fake.snapshot(`${DIR}manifest.json`);
  fake.put(POINTER, "p1"); fake.put(POINTER, "p2");
  fake.put(STAGED, "s1");
  for (const r of fake.records) if (r.key === STAGED) r.modified = Date.now() - 3 * DAYS;
  for (const k of NEIGHBOURS) fake.put(k, "keep");
  return fake;
}

const ports = (fake: FakeAzureBlob, built?: { s3: number }) => ({
  azureEraser: (c: Parameters<typeof createAzureBlobEraser>[0]) => createAzureBlobEraser({ ...c, fetch: fake.fetch }),
  preflight: { fetch: fake.fetch },
  eraser: () => { if (built) built.s3++; throw new Error("no S3 eraser on azure"); },
});
const sessionKeys = (fake: FakeAzureBlob) => fake.records.filter((r) => !NEIGHBOURS.includes(r.key)).map((r) => r.key);
const refusal = async (p: Promise<unknown>) => p.then(() => "no_refusal", (e) => (e instanceof SessionEraseRefused ? e.code : `error:${errorCodeOf(e)}`));

describe("execute on azure", () => {
  test("preflight, then every version, snapshot, pointer and staged copy of the session is erased and verified; neighbours survive; no S3 eraser", async () => {
    const fake = seed();
    const built = { s3: 0 };
    const r = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake, built));
    expect(r).toMatchObject({ enabled: true, dryRun: false, verified: true, preflight: "passed" });
    expect(exitCodeForResult(r)).toBe(0);
    expect(sessionKeys(fake)).toEqual([]);
    expect(fake.records.map((x) => x.key).sort()).toEqual([...NEIGHBOURS].sort());
    expect(built.s3).toBe(0);
    // The probe wrote only under its own private namespace, and left nothing.
    const puts = fake.requests.filter((q) => q.method === "PUT").map((q) => decodeURIComponent(q.url.pathname));
    expect(puts.length).toBe(1);
    expect(puts[0]).toContain(`/${eraseProbePrefix(ROOT)}`);
    expect(fake.requests.every((q) => q.redirect === "manual")).toBe(true);
  });

  test("soft delete with permanent delete allowed: the first run leaves soft-deleted copies and is NOT verified (exit 3); the rerun removes them and verifies", async () => {
    const fake = seed({ softDelete: true, permanentDelete: true });
    const first = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake));
    expect(first).toMatchObject({ verified: false, preflight: "passed" });
    expect(exitCodeForResult(first)).toBe(3);
    expect(fake.records.filter((r) => !NEIGHBOURS.includes(r.key)).every((r) => r.deleted)).toBe(true);
    const second = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake));
    expect(second).toMatchObject({ verified: true });
    expect(sessionKeys(fake)).toEqual([]);
  });

  test("a delete 404 (already gone, another actor) still ends verified; nothing is assumed gone without a fresh listing", async () => {
    const fake = seed();
    let once = false;
    fake.hook = (req) => {
      if (!once && req.method === "DELETE" && decodeURIComponent(req.url.pathname).includes("/raw/")) {
        once = true;
        const key = decodeURIComponent(req.url.pathname).slice("/fxacct/fx-container/".length);
        fake.records = fake.records.filter((r) => r.key !== key);
        return new Response(null, { status: 404 });
      }
      return undefined;
    };
    const r = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake));
    expect(once).toBe(true);
    expect(r).toMatchObject({ verified: true });
    expect(sessionKeys(fake)).toEqual([]);
  });

  test("a crash mid-page (network failure) exits 4; the rerun resumes from a fresh listing and ends verified", async () => {
    const fake = seed();
    let deletes = 0;
    fake.hook = (req) => (req.method === "DELETE" && ++deletes === 4 ? (() => { throw new TypeError("connection reset"); })() : undefined);
    const first = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake)).catch((e) => e);
    expect(exitCodeForError(first)).toBe(4);
    expect(sessionKeys(fake).length).toBeGreaterThan(0);
    fake.hook = undefined;
    const second = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake));
    expect(second).toMatchObject({ verified: true });
    expect(sessionKeys(fake)).toEqual([]);
  });
});

describe("dry run on azure", () => {
  test("list only: no probe, no PUT, no DELETE; preflight skipped and execute still requires it", async () => {
    const fake = seed();
    const before = fake.records.length;
    const r = await runSessionEraseBytes(env({ SESSION_ERASE_DRY_RUN: "1" }), () => JSON.stringify(request()), ports(fake));
    expect(r).toMatchObject({ enabled: true, dryRun: true, preflight: "skipped", executeRequiresPreflight: true });
    expect(fake.requests.filter((q) => q.method !== "GET")).toEqual([]);
    expect(fake.records.length).toBe(before);
    expect((r as { versionsDeleted: number }).versionsDeleted).toBeGreaterThan(0);
  });
});

describe("refusals before any destructive work", () => {
  test("the preflight refuses an account the erase cannot complete on (exit 2), before any session delete", async () => {
    const cases: [FakeAzureOptions, string][] = [
      [{ versioning: false }, "azure_blob_not_versioned"],
      [{ softDelete: true, permanentDelete: false }, "azure_blob_soft_delete_retained"],
      [{ softDelete: true, permanentDelete: false, permanentDeleteRefusal: 403 }, "azure_blob_permanent_delete_forbidden"],
      [{ container: "fx-other-container" }, "azure_blob_container_missing"],
    ];
    for (const [opts, want] of cases) {
      const fake = seed(opts);
      const result = await runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake)).catch((e) => e);
      expect([want, result instanceof SessionEraseRefused ? result.code : String(result), exitCodeForError(result)]).toEqual([want, want, 2]);
      const sessionDeletes = fake.requests.filter((q) => q.method === "DELETE" && !decodeURIComponent(q.url.pathname).includes("/.runner-probe/"));
      expect([want, sessionDeletes.length]).toEqual([want, 0]);
    }
  });

  test("the target must be the writer store exactly; S3 settings conflict; the key is a file only — no request at all", async () => {
    const cases: [Record<string, string | undefined>, string][] = [
      [{ LOCKER_BUCKET: "fx-other-container" }, "azure_target_not_writer_store"],
      [{ LOCKER_AZURE_ACCOUNT: "fxother" }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: undefined }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: "http://127.0.0.1:2/fxacct" }, "azure_target_not_writer_store"],
      // A malformed writer URL never compares equal: nothing is dropped before the comparison.
      [{ LOCKER_ENDPOINT: `${FAKE_ENDPOINT}?sv=1` }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: `${FAKE_ENDPOINT}#frag` }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: FAKE_ENDPOINT.replace("http://", "http://u:p@") }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: ` ${FAKE_ENDPOINT}` }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: `${FAKE_ENDPOINT}/extra` }, "azure_target_not_writer_store"],
      [{ LOCKER_ENDPOINT: "http://fxacct.blob.example.test", SESSION_ERASE_AZURE_ENDPOINT: "https://fxacct.blob.example.test" }, "azure_target_not_writer_store"],
      [{ SESSION_ERASE_S3_BUCKET: "fx-bucket" }, "azure_s3_settings_conflict"],
      [{ SESSION_ERASE_S3_ACCESS_KEY_ID_FILE: "/x" }, "azure_s3_settings_conflict"],
      [{ SESSION_ERASE_AZURE_ACCOUNT_KEY: KEY }, "azure_account_key_plaintext"],
      [{ SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE: undefined }, "azure_account_key_missing"],
      [{ SESSION_ERASE_AZURE_CONTAINER: "Fx_Bad" }, "azure_container_invalid"],
    ];
    for (const [extra, want] of cases) {
      const fake = seed();
      const got = await refusal(runSessionEraseBytes(env(extra), () => JSON.stringify(request()), ports(fake)));
      expect([extra, got]).toEqual([extra, want]);
      expect([extra, fake.requests.length]).toEqual([extra, 0]);
    }
  });

  test("an invalid request is refused by the pure plan before the preflight: zero fetches, zero probe calls (execute and dry run)", async () => {
    const base = request().locations[0]!;
    const cases: [Partial<SessionEraseBytesRequest>, string][] = [
      [{ locations: [{ ...base, storageProvider: "s3" }] }, "store_not_azure"],
      [{ locations: [{ ...base, bucket: "fx-other-container" }] }, "bucket_mismatch"],
      [{ locations: [{ ...base, sessionId: "fx-other" }] }, "location_not_this_session"],
      [{ key: { ...request().key, orgId: ".runner-probe" } }, "org_invalid"],
      [{ stagedKeys: [`${ROOT}/${ORG}/buffered-batches/v1/x`] }, "staged_buffered_owned_by_gc"],
    ];
    for (const dry of [undefined, "1"]) for (const [over, want] of cases) {
      const fake = seed();
      let probes = 0;
      const got = await refusal(runSessionEraseBytes(env({ SESSION_ERASE_DRY_RUN: dry }), () => JSON.stringify(request(over)), {
        ...ports(fake), preflight: { probe: async () => { probes++; return "Enabled"; } },
      }));
      expect([dry, want, got, probes, fake.requests.length]).toEqual([dry, want, want, 0, 0]);
    }
  });

  test("a row of another provider is never erased through this store (both directions)", () => {
    const azureRowOnS3 = request({ locations: [{ ...request().locations[0]!, storageProvider: "azure", bucket: "b" }] });
    expect(() => planTargets(azureRowOnS3, { keyRoot: ROOT, bucket: "b" })).toThrow("store_not_versioned_s3");
    const s3RowOnAzure = request({ locations: [{ ...request().locations[0]!, storageProvider: "s3" }] });
    expect(() => planTargets(s3RowOnAzure, { keyRoot: ROOT, bucket: "fx-container", provider: "azure" })).toThrow("store_not_azure");
    const otherContainer = request({ locations: [{ ...request().locations[0]!, bucket: "fx-other-container" }] });
    expect(() => planTargets(otherContainer, { keyRoot: ROOT, bucket: "fx-container", provider: "azure" })).toThrow("bucket_mismatch");
    // A row that omits its provider or bucket is refused on Azure (it could name another store); the S3 path keeps its
    // null tolerance.
    const { storageProvider: _p, ...noProvider } = request().locations[0]!;
    const { bucket: _b, ...noBucket } = request().locations[0]!;
    expect(() => planTargets(request({ locations: [noProvider] }), { keyRoot: ROOT, bucket: "fx-container", provider: "azure" })).toThrow("store_not_azure");
    expect(() => planTargets(request({ locations: [noBucket] }), { keyRoot: ROOT, bucket: "fx-container", provider: "azure" })).toThrow("bucket_mismatch");
    expect(() => planTargets(request({ locations: [{ ...noProvider, bucket: null, storageProvider: null }] }), { keyRoot: ROOT, bucket: "fx-container", provider: "azure" })).toThrow("store_not_azure");
    const { storageProvider: _q, bucket: _c, ...bare } = request().locations[0]!;
    expect(planTargets(request({ locations: [bare] }), { keyRoot: ROOT, bucket: "b" }).length).toBeGreaterThan(0);
    expect(planTargets(request(), { keyRoot: ROOT, bucket: "fx-container", provider: "azure" }).length).toBeGreaterThan(0);
  });
});

describe("mid-run Azure errors, classified by operation", () => {
  const midRun = async (status: number, method: string) => {
    const fake = seed();
    let listed = 0;
    fake.hook = (req) => {
      const probe = decodeURIComponent(req.url.pathname + req.url.search).includes(".runner-probe");
      if (probe) return undefined;
      if (method === "GET" && req.method === "GET" && ++listed === 1) return new Response(null, { status });
      if (method === "DELETE" && req.method === "DELETE") return new Response(null, { status });
      return undefined;
    };
    return runSessionEraseBytes(env(), () => JSON.stringify(request()), ports(fake)).catch((e) => e);
  };

  test("a list 404 (container gone) and a list 403 are refusals (exit 2)", async () => {
    for (const [status, want] of [[404, "azure_blob_container_missing"], [403, "azure_forbidden"]] as const) {
      const e = await midRun(status, "GET");
      expect([status, e instanceof SessionEraseRefused ? e.code : String(e), exitCodeForError(e)]).toEqual([status, want, 2]);
    }
  });

  test("a delete 403 stays a failure (exit 4) with its own code; a 5xx stays a failure; nothing is reported erased", async () => {
    const forbidden = await midRun(403, "DELETE");
    expect([errorCodeOf(forbidden), exitCodeForError(forbidden)]).toEqual(["erase_azure_delete_forbidden", 4]);
    const unavailable = await midRun(503, "DELETE");
    expect([errorCodeOf(unavailable), exitCodeForError(unavailable)]).toEqual(["azure_503", 4]);
  });
});

describe("shared parser and the hosted defaults", () => {
  test("the default prefix is the analysis family: explicit and default parse identically; the runner ignores SESSION_ERASE_AZURE_*", () => {
    const fail = (code: string): never => { throw new Error(code); };
    const analysisEnv = { SESSION_ANALYSIS_AZURE_ACCOUNT: "fxacct", SESSION_ANALYSIS_AZURE_CONTAINER: "fx-container", SESSION_ANALYSIS_AZURE_ENDPOINT: FAKE_ENDPOINT };
    expect(DEFAULT_AZURE_SETTINGS_PREFIX).toBe("SESSION_ANALYSIS_AZURE");
    expect(azureBlobSettings(analysisEnv, fail)).toEqual(azureBlobSettings(analysisEnv, fail, "SESSION_ANALYSIS_AZURE"));
    expect(azureBlobSettings({ SESSION_ERASE_AZURE_ACCOUNT: "fxacct", SESSION_ERASE_AZURE_CONTAINER: "fx-container" }, fail, "SESSION_ERASE_AZURE"))
      .toEqual({ account: "fxacct", container: "fx-container" });
    const runnerEnv = { SESSION_ANALYSIS_ENABLED: "true", LOCKER_PROVIDER: "azure", PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "fxroot", ...analysisEnv };
    expect(analysisRunnerConfig({ ...runnerEnv, SESSION_ERASE_AZURE_ACCOUNT: "Not-Valid", SESSION_ERASE_AZURE_ACCOUNT_KEY: KEY } as NodeJS.ProcessEnv))
      .toEqual(analysisRunnerConfig(runnerEnv as NodeJS.ProcessEnv));
  });

  test("unset LOCKER_PROVIDER keeps the S3 erase path: the Azure eraser is never built and no Azure setting is read", async () => {
    let azure = 0;
    const e = env({ LOCKER_PROVIDER: undefined, SESSION_ERASE_S3_BUCKET: "fx-bucket", SESSION_ERASE_S3_REGION: "us-east-1",
      SESSION_ERASE_S3_ACCESS_KEY_ID: "AKIDFXEXAMPLE", SESSION_ERASE_S3_SECRET_ACCESS_KEY: "fxsecretkey0123456789" });
    const result = await runSessionEraseBytes(e, () => JSON.stringify({ ...request(), locations: [{ ...request().locations[0]!, storageProvider: "s3", bucket: "fx-bucket" }] }), {
      azureEraser: () => { azure++; throw new Error("no Azure eraser on S3"); },
      eraser: () => { throw new Error("s3-eraser-built"); },
    }).catch((x) => x as Error);
    expect((result as Error).message).toBe("s3-eraser-built");
    expect(azure).toBe(0);
  });
});
