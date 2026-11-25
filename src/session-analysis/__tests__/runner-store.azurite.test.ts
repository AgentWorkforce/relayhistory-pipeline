/**
 * The session-analysis runner on LOCKER_PROVIDER=azure against a real Azurite (azurite@3.38.0, opt-in per-account
 * blob versioning): on a versioning account the startup probe passes, a result put records its x-ms-version-id, and
 * the sweeper's strict eraser keeps exactly the owned version, then erases every version and snapshot; on an account
 * without versioning startup refuses blob_not_versioned. Uses the disposable, loopback-only fixture (AZURITE_TEST_*:
 * one origin, a versioning and a plain account with throwaway keys); skipped without it.
 * Azurite has no soft delete or permanent delete: those paths are proven only by the fake server
 * (azure-blob-eraser.test.ts, blob-versioning.test.ts). A real-Azure run is still owed.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AnalysisRunnerRefused, analysisRunnerConfig, assertRunnerBlobVersioned, runnerAzureAnalysisDeps, runnerAzureTarget, runnerSecrets } from "../runner.ts";
import { azureSharedKeyAuthorization, AZURE_API_VERSION } from "../../../packages/object-store/src/azure.ts";
import { createAzureBlobEraser } from "../../migration/azure-blob-erasure/eraser.ts";
import type { EraseCheckpoint } from "../../migration/neon-s3-erasure/eraser.ts";
import type { HoopDb } from "../../shared/db.ts";

const origin = process.env.AZURITE_TEST_BLOB_ORIGIN;
const accounts = {
  versioned: { account: process.env.AZURITE_TEST_VERSIONED_ACCOUNT, key: process.env.AZURITE_TEST_VERSIONED_KEY },
  plain: { account: process.env.AZURITE_TEST_PLAIN_ACCOUNT, key: process.env.AZURITE_TEST_PLAIN_KEY },
};
const enabled = !!(origin && accounts.versioned.account && accounts.versioned.key && accounts.plain.account && accounts.plain.key);
if (origin) {
  const u = new URL(origin);
  if (u.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname)
    || u.username || u.password || u.pathname !== "/" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}

const container = `fx-analysis-${crypto.randomUUID().slice(0, 8)}`;
let dir = "";
beforeAll(() => { dir = mkdtempSync(join(tmpdir(), "analysis-runner-azurite-")); });
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });

async function signed(account: string, key: string, method: string, path: string, query = "", extra: Record<string, string> = {}) {
  const url = new URL(`${origin!.replace(/\/+$/, "")}/${account}/${path}${query}`);
  const headers = new Headers(extra);
  headers.set("x-ms-version", AZURE_API_VERSION);
  headers.set("x-ms-date", new Date().toUTCString());
  headers.set("authorization", await azureSharedKeyAuthorization(account, key, method, url, headers, 0));
  const res = await fetch(url, { method, headers, redirect: "manual", signal: AbortSignal.timeout(10_000) });
  return { status: res.status, headers: res.headers, text: await res.text() };
}

function envFor(which: "versioned" | "plain"): NodeJS.ProcessEnv {
  const { account, key } = accounts[which];
  const keyFile = join(dir, `${which}-key`);
  writeFileSync(keyFile, key!);
  return {
    SESSION_ANALYSIS_ENABLED: "true", LOCKER_PROVIDER: "azure",
    SESSION_ANALYSIS_AZURE_ACCOUNT: account!, SESSION_ANALYSIS_AZURE_CONTAINER: container,
    SESSION_ANALYSIS_AZURE_ENDPOINT: `${origin!.replace(/\/+$/, "")}/${account}`, SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE: keyFile,
    PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "fxroot",
    SESSION_ANALYSIS_DATABASE_URL: "postgresql://unused@127.0.0.1:5432/postgres", SESSION_ANALYSIS_OPENAI_API_KEY: "sk-disabled",
  } as NodeJS.ProcessEnv;
}

const listing = async (which: "versioned" | "plain", prefix: string) => {
  const { account, key } = accounts[which];
  const r = await signed(account!, key!, "GET", container, `?restype=container&comp=list&include=deleted,snapshots,versions&prefix=${encodeURIComponent(prefix)}`);
  expect(r.status).toBe(200);
  return [...r.text.matchAll(/<Blob>([\s\S]*?)<\/Blob>/g)].map((m) => m[1]!);
};

describe.skipIf(!enabled)("session-analysis runner on Azurite (azure)", () => {
  test("versioning account: the probe passes; an owned result version survives the sweep; then every copy is erased", async () => {
    const { account, key } = accounts.versioned;
    expect([201, 409]).toContain((await signed(account!, key!, "PUT", container, "?restype=container")).status);
    const env = envFor("versioned");
    const cfg = analysisRunnerConfig(env);
    const secrets = runnerSecrets(env);
    const target = runnerAzureTarget(cfg, secrets);
    await assertRunnerBlobVersioned(cfg, target);
    expect(await listing("versioned", "fxroot/.runner-probe/")).toEqual([]);

    const db = { end: async () => {} } as unknown as HoopDb;
    const { deps } = runnerAzureAnalysisDeps(env, cfg, secrets, { pool: () => db });
    expect(deps.canonical.provider).toBe("azure");
    const dirKey = `fxroot/fx-org-${crypto.randomUUID().slice(0, 8)}/v1/normalized/session-analysis-v2/`;
    const resultKey = `${dirKey}fx-job.json`;
    const stray = await deps.store.put(resultKey as never, new TextEncoder().encode('{"attempt":1}'), { contentType: "application/json" });
    // A snapshot of the stray (on Azurite a snapshot also writes a new current version), then the owned result.
    expect((await signed(account!, key!, "PUT", `${container}/${resultKey}`, "?comp=snapshot")).status).toBe(201);
    const owned = await deps.store.put(resultKey as never, new TextEncoder().encode('{"attempt":2}'), { contentType: "application/json" });
    expect(stray.version.version).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(owned.version.version).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(owned.version.version).not.toBe(stray.version.version);
    const before = await listing("versioned", resultKey);
    expect(before.some((b) => b.includes("<Snapshot>"))).toBe(true);
    expect(before.filter((b) => b.includes("<IsCurrentVersion>true</IsCurrentVersion>")).map((b) => b.includes(`<VersionId>${owned.version.version}</VersionId>`))).toEqual([true]);
    expect(before.length).toBeGreaterThanOrEqual(3);

    const authorize = async () => {};
    const keep = owned.version.version!;
    const select = (v: { versionId: string; key: string }) => v.key === resultKey && v.versionId !== keep;
    for (let i = 0; i < 10; i++) if ((await deps.eraser!.eraseStep(dirKey, { scope: "fx", authorize, exactKey: resultKey, select })).status === "traversed") break;
    expect(await deps.eraser!.verifyErased(dirKey, { authorize, exactKey: resultKey, select })).toMatchObject({ status: "erased" });
    const left = await listing("versioned", resultKey);
    expect(left).toHaveLength(1);
    expect(left[0]).toContain(`<VersionId>${keep}</VersionId>`);
    // The owned bytes are still the current blob.
    const read = await deps.store.getObject(resultKey as never);
    expect(await new Response(read.body).text()).toBe('{"attempt":2}');

    // A full erase of the directory (small pages, resumed by a fresh eraser from each checkpoint) leaves nothing.
    let checkpoint: EraseCheckpoint | null = null;
    for (let i = 0; i < 20; i++) {
      const e = createAzureBlobEraser({ account: account!, container, endpoint: env.SESSION_ANALYSIS_AZURE_ENDPOINT, accountKey: () => key!, pageSize: 1 });
      const r = await e.eraseStep(dirKey, { scope: "fx-all", authorize, checkpoint, budget: { pages: 2 } });
      if (r.status === "traversed") break;
      checkpoint = r.checkpoint;
    }
    expect(await deps.eraser!.verifyErased(dirKey, { authorize })).toMatchObject({ status: "erased" });
    expect(await listing("versioned", dirKey)).toEqual([]);
  });

  test("account without versioning: startup refuses blob_not_versioned and leaves no probe blob", async () => {
    const { account, key } = accounts.plain;
    expect([201, 409]).toContain((await signed(account!, key!, "PUT", container, "?restype=container")).status);
    const env = envFor("plain");
    const cfg = analysisRunnerConfig(env);
    const target = runnerAzureTarget(cfg, runnerSecrets(env));
    const refused = await assertRunnerBlobVersioned(cfg, target).then(() => "no_refusal", (e) => (e instanceof AnalysisRunnerRefused ? e.code : String(e)));
    expect(refused).toBe("blob_not_versioned");
    expect(await listing("plain", "fxroot/.runner-probe/")).toEqual([]);
  });
});
