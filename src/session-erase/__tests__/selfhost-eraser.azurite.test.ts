/**
 * The self-host per-session byte eraser on LOCKER_PROVIDER=azure against a real Azurite (azurite@3.38.0, opt-in
 * per-account blob versioning), through the real entry (runSessionEraseBytes with no test ports: real fetch, the real
 * preflight probe, the strict Azure eraser). On the versioning account every version and snapshot of one session is
 * erased and a fresh listing proves it, neighbours survive, and the probe leaves nothing; on the account without
 * versioning execute refuses azure_blob_not_versioned before any session delete. Uses the disposable, loopback-only
 * fixture of the runner's Azurite test (AZURITE_TEST_*); skipped without it. Azurite has no soft delete: those paths
 * are fake-server tests (selfhost-eraser-azure.test.ts). A real-Azure run is still owed.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AZURE_API_VERSION, azureSharedKeyAuthorization, createAzureBlobStore } from "../../../packages/object-store/src/azure.ts";
import { liveMachineId } from "../../session-ingest/writer.ts";
import { SessionEraseRefused, type SessionEraseBytesRequest } from "../selfhost-eraser.ts";
import { runSessionEraseBytes } from "../selfhost-eraser-main.ts";

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

const ROOT = "fxroot", ORG = `fx-org-${crypto.randomUUID().slice(0, 8)}`, WS = "fx-ws", SRC = "claude-code", SID = "fx-session";
const M = liveMachineId(WS, "fx-origin", "fx-user");
const DIR = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SID}/`;
const POINTER = `${ROOT}/${ORG}/live-sessions/v1/ws=${WS}/m=${M}/h=${SRC}/s=${SID}.json`;
const NEIGHBOUR = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=fx-other/manifest.json`;
const container = `fx-erase-${crypto.randomUUID().slice(0, 8)}`;
let dir = "";
beforeAll(() => { dir = mkdtempSync(join(tmpdir(), "erase-azurite-")); });
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });

const endpointOf = (account: string) => `${origin!.replace(/\/+$/, "")}/${account}`;
async function signed(account: string, key: string, method: string, path: string, query = "") {
  const url = new URL(`${endpointOf(account)}/${path}${query}`);
  const headers = new Headers({ "x-ms-version": AZURE_API_VERSION, "x-ms-date": new Date().toUTCString() });
  headers.set("authorization", await azureSharedKeyAuthorization(account, key, method, url, headers, 0));
  const res = await fetch(url, { method, headers, redirect: "manual", signal: AbortSignal.timeout(10_000) });
  return { status: res.status, text: await res.text() };
}
const names = async (account: string, key: string, prefix: string) => {
  const r = await signed(account, key, "GET", container, `?restype=container&comp=list&include=deleted,snapshots,versions&prefix=${encodeURIComponent(prefix)}`);
  expect(r.status).toBe(200);
  return [...r.text.matchAll(/<Name>([^<]*)<\/Name>/g)].map((m) => m[1]!);
};

function envFor(which: "versioned" | "plain"): NodeJS.ProcessEnv {
  const { account, key } = accounts[which];
  const keyFile = join(dir, `${which}-key`);
  writeFileSync(keyFile, key!);
  return {
    SESSION_ERASE_BYTES_ENABLED: "1", LOCKER_PROVIDER: "azure",
    LOCKER_AZURE_ACCOUNT: account!, LOCKER_BUCKET: container, LOCKER_ENDPOINT: endpointOf(account!),
    SESSION_ERASE_AZURE_ACCOUNT: account!, SESSION_ERASE_AZURE_CONTAINER: container, SESSION_ERASE_AZURE_ENDPOINT: endpointOf(account!),
    SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE: keyFile, INGEST_KEY_ROOT: ROOT,
  } as NodeJS.ProcessEnv;
}
const request = (): SessionEraseBytesRequest => ({
  key: { orgId: ORG, workspaceId: WS, source: SRC, sessionId: SID },
  locations: [{ workspaceId: WS, machineId: M, harness: SRC, sessionId: SID, manifestKey: `${DIR}manifest.json`, storageProvider: "azure", bucket: container }],
});

describe.skipIf(!enabled)("per-session byte erase on Azurite (azure)", () => {
  test("versioning account: every version and snapshot of the session erased and verified; the neighbour and no probe copy remain", async () => {
    const { account, key } = accounts.versioned;
    expect([201, 409]).toContain((await signed(account!, key!, "PUT", container, "?restype=container")).status);
    const store = createAzureBlobStore({ account: account!, container, endpoint: endpointOf(account!), auth: { kind: "sharedKey", accountKey: key! } });
    const put = (k: string, body: string) => store.put(k as never, new TextEncoder().encode(body), { contentType: "application/json" });
    await put(`${DIR}manifest.json`, "m1"); await put(`${DIR}manifest.json`, "m2");
    expect((await signed(account!, key!, "PUT", `${container}/${DIR}manifest.json`, "?comp=snapshot")).status).toBe(201);
    await put(`${DIR}raw/0000000000-0-abcd1234.ndjson.zst`, "r");
    await put(POINTER, "p1"); await put(POINTER, "p2");
    await put(NEIGHBOUR, "keep");
    expect((await names(account!, key!, DIR)).length).toBeGreaterThan(3);

    const r = await runSessionEraseBytes(envFor("versioned"), () => JSON.stringify(request()));
    expect(r).toMatchObject({ enabled: true, dryRun: false, verified: true, preflight: "passed" });
    expect(await names(account!, key!, DIR)).toEqual([]);
    expect(await names(account!, key!, POINTER)).toEqual([]);
    expect(await names(account!, key!, NEIGHBOUR)).toEqual([NEIGHBOUR]);
    expect(await names(account!, key!, `${ROOT}/.runner-probe/`)).toEqual([]);
  });

  test("account without versioning: execute refuses azure_blob_not_versioned before any session delete", async () => {
    const { account, key } = accounts.plain;
    expect([201, 409]).toContain((await signed(account!, key!, "PUT", container, "?restype=container")).status);
    const store = createAzureBlobStore({ account: account!, container, endpoint: endpointOf(account!), auth: { kind: "sharedKey", accountKey: key! } });
    await store.put(`${DIR}manifest.json` as never, new TextEncoder().encode("m"), { contentType: "application/json" });
    const e = await runSessionEraseBytes(envFor("plain"), () => JSON.stringify(request())).catch((x) => x);
    expect(e instanceof SessionEraseRefused ? e.code : String(e)).toBe("azure_blob_not_versioned");
    expect(await names(account!, key!, DIR)).toEqual([`${DIR}manifest.json`]);
    expect(await names(account!, key!, `${ROOT}/.runner-probe/`)).toEqual([]);
  });
});
