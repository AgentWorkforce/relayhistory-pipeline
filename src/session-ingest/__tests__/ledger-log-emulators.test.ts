/**
 * The ledger-log protocol (ledger-log.ts) against real object-store backends: MinIO (S3), Azurite (Azure Blob) and
 * fake-gcs-server (GCS), when their endpoints are set, the same variables packages/object-store's conformance suite
 * uses:
 *   RH_OBJSTORE_S3_ENDPOINT, RH_OBJSTORE_S3_ACCESS_KEY_ID, RH_OBJSTORE_S3_SECRET_ACCESS_KEY
 *   RH_OBJSTORE_AZURITE_ENDPOINT
 *   RH_OBJSTORE_GCS_BASE_URL
 * Each case is skipped when its variable is unset (the fs store covers the same cases in ledger-log.test.ts).
 *
 * fake-gcs-server ignores x-goog-if-generation-match (the conformance suite skips the same cases), so on GCS only
 * the sequential protocol runs; the create-if-absent races are proven on MinIO and Azurite.
 */
import { describe, expect, test } from "bun:test";
import { createAzureBlobStore, createGcsStore, createS3Store } from "../../../packages/object-store/src/index.ts";
import { fromBase64, hmacSha256, toBase64 } from "../../../packages/object-store/src/crypto.ts";
import { signRequest } from "../../../packages/object-store/src/sigv4.ts";
import type { ObjectStore } from "../../shared/vendor.ts";
import { appendBatches, enableLogMode, LogFenced, logKeys, openStream, putPayload, readEntries, takeStream, type LogBatch } from "../ledger-log.ts";

const BUCKET = "rh-ledger-log";
const AZURITE_ACCOUNT = "devstoreaccount1";
// Azurite's published, well-known development key (not a secret).
const AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const env = (n: string) => process.env[n] || undefined;
const TENANT = { orgId: "org-e", workspaceId: "ws-e", userId: "user-e" };

const batch = (id: string, digest = `d-${id}`): LogBatch => ({ batchId: id, digest, receiptId: `rhr_${id}`, sessions: [["claude", `s-${id}`]], payloads: [] });

/** The protocol cases. `races`: the backend honours create-if-absent preconditions. */
async function protocol(store: ObjectStore, races: boolean) {
  const root = `llog-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const keys = logKeys(root, TENANT, "origin-e");
  // The mode marker: idempotent for the same cutover, refused for another.
  const a0 = await enableLogMode(store, keys, null, { writer: "wa" });
  const again = await enableLogMode(store, keys, null, { writer: "wb" });
  expect(again.head).toBe(a0.head);
  if (races) await expect(enableLogMode(store, keys, "other", { writer: "wc" })).rejects.toBeInstanceOf(LogFenced);
  // Payloads are content addressed and idempotent.
  const bytes = new TextEncoder().encode(JSON.stringify({ v: 1, records: [] }));
  const p1 = await putPayload(store, keys, bytes);
  const p2 = await putPayload(store, keys, bytes);
  expect(p2).toEqual(p1);
  // The owner appends; replay and conflict come from the window.
  // appendBatches advances the state it is given.
  const a = a0;
  const r1 = await appendBatches(store, a, [{ ...batch("b1"), payloads: [p1] }, batch("b2")], "c1", { writer: "wa" });
  expect(r1.admitted.map((b) => b.batchId)).toEqual(["b1", "b2"]);
  const r2 = await appendBatches(store, a, [batch("b1"), batch("b2", "other")], "c2", { writer: "wa" });
  expect([r2.admitted.length, r2.replays.length, r2.conflicts.length]).toEqual([0, 1, 1]);
  // A reopen reads the same chain.
  const reopened = await openStream(store, keys);
  expect(reopened?.head).toBe(a.head);
  const kinds: string[] = [];
  for await (const e of readEntries(store, keys)) kinds.push(e.kind);
  expect(kinds[0]).toBe("open");
  expect(kinds.filter((k) => k === "batches").length).toBe(1);
  if (!races) return;
  // A taker fences the old owner: the old owner's next append is refused, nothing it wrote lands.
  const b = await takeStream(store, (await openStream(store, keys))!, { writer: "wb" });
  await expect(appendBatches(store, a, [batch("b3")], "c3", { writer: "wa" })).rejects.toBeInstanceOf(LogFenced);
  // Two appends from the same owner state racing for one seq: exactly one lands.
  const results = await Promise.allSettled([
    appendBatches(store, b, [batch("x1")], "cx", { writer: "wb" }),
    appendBatches(store, b, [batch("x2")], "cy", { writer: "wb" }),
  ]);
  expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
  const ids: string[] = [];
  for await (const e of readEntries(store, keys)) if (e.kind === "batches") ids.push(...e.batches.map((x) => x.batchId));
  expect(ids.filter((id) => id === "x1" || id === "x2").length).toBe(1);
}

describe("ledger-log protocol on real backends", () => {
  const s3Endpoint = env("RH_OBJSTORE_S3_ENDPOINT");
  test.skipIf(!s3Endpoint)("s3 (MinIO)", async () => {
    const credentials = { accessKeyId: env("RH_OBJSTORE_S3_ACCESS_KEY_ID")!, secretAccessKey: env("RH_OBJSTORE_S3_SECRET_ACCESS_KEY")! };
    const url = new URL(`${s3Endpoint}/${BUCKET}/`);
    const headers = new Headers();
    await signRequest({ method: "PUT", url, headers, payloadSha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", region: "us-east-1", credentials });
    const created = await fetch(url, { method: "PUT", headers });
    expect([200, 409]).toContain(created.status);
    await protocol(createS3Store({ bucket: BUCKET, region: "us-east-1", endpoint: s3Endpoint!, credentials }) as unknown as ObjectStore, true);
  }, 60_000);

  const azureEndpoint = env("RH_OBJSTORE_AZURITE_ENDPOINT");
  test.skipIf(!azureEndpoint)("azure (Azurite)", async () => {
    const u = new URL(`${azureEndpoint!.replace(/\/+$/, "")}/${BUCKET}?restype=container`);
    const date = new Date().toUTCString(), version = "2021-12-02";
    const sts = `PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:${date}\nx-ms-version:${version}\n/${AZURITE_ACCOUNT}${u.pathname}\nrestype:container`;
    const sig = toBase64(await hmacSha256(fromBase64(AZURITE_KEY), sts));
    const res = await fetch(u, { method: "PUT", headers: { "x-ms-date": date, "x-ms-version": version, authorization: `SharedKey ${AZURITE_ACCOUNT}:${sig}` } });
    expect([201, 409]).toContain(res.status);
    await protocol(createAzureBlobStore({ account: AZURITE_ACCOUNT, container: BUCKET, endpoint: azureEndpoint!, auth: { kind: "sharedKey", accountKey: AZURITE_KEY } }) as unknown as ObjectStore, true);
  }, 60_000);

  const gcsBase = env("RH_OBJSTORE_GCS_BASE_URL");
  test.skipIf(!gcsBase)("gcs (fake-gcs-server; sequential protocol only)", async () => {
    const created = await fetch(`${gcsBase}/storage/v1/b?project=test`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name: BUCKET }) });
    expect([200, 409]).toContain(created.status);
    await protocol(createGcsStore({ bucket: BUCKET, baseUrl: gcsBase!, token: { accessToken: "fake-gcs-server-ignores-auth" } }) as unknown as ObjectStore, false);
  }, 60_000);
});
