/**
 * Decision 0005 end to end, no database anywhere: a desktop-written upload object -> the deployed Lambda handler
 * shape (createUploadAdmissionHandler over the HOSTED runtime factory) -> promoteUpload's §6 checks -> the inline
 * admission (admission-consumer.ts admitMessage) with the S3 ledger -> the canonical manifest, the admitted
 * outcome and the committed batch record. Every SQL entry point is mocked to record a call; none may happen.
 */
import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

// Any SQL use on this path is a failure: the S3 ledger replaces every one of these. bun's mock.module is
// process-wide, so the real modules are captured first and restored afterwards for the other test files.
const REAL_LEDGER_SQL = { ...(await import("../ledger-sql.ts")) };
const REAL_CONSENT_SQL = { ...(await import("../upload-consent-sql.ts")) };
afterAll(() => {
  mock.module("../ledger-sql.ts", () => REAL_LEDGER_SQL);
  mock.module("../upload-consent-sql.ts", () => REAL_CONSENT_SQL);
});
const sqlCalls: string[] = [];
mock.module("../ledger-sql.ts", () => ({
  sqlLedger: () => { sqlCalls.push("sqlLedger"); throw new Error("SQL ledger used on the S3 path"); },
  ledgerErrorCode: () => "ledger_error",
  ledgerErrorCodeFrom: () => "ledger_error",
  parseClaimResult: () => { sqlCalls.push("parseClaimResult"); throw new Error("SQL"); },
  acceptedGenerationSet: () => { sqlCalls.push("acceptedGenerationSet"); throw new Error("SQL"); },
}));
mock.module("../upload-consent-sql.ts", () => ({
  sqlUploadConsentStore: () => { sqlCalls.push("sqlUploadConsentStore"); throw new Error("SQL consent store used on the S3 path"); },
  parseConsentSnapshot: () => { sqlCalls.push("parseConsentSnapshot"); throw new Error("SQL"); },
}));

const { createUploadAdmissionHandler } = await import("../upload-admission-lambda.ts");
const { hostedUploadAdmissionRuntime } = await import("../upload-admission-hosted.ts");
const { uploadAdmissionStatus, uploadDirectConfig, uploadStatusKey } = await import("../upload-admission.ts");
const { journalKey, outcomeKey, receiptDir } = await import("../async-admission.ts");
const { fenceAuthorityKey, s3LedgerKeys } = await import("../ledger-s3.ts");
const { encodeUploadWitness, parseUploadKey, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } = await import("../upload-inbox-contract.ts");
const { prepareDelivery } = await import("../vendor/delivery/prepare.ts");

const V = JSON.parse(readFileSync(join(import.meta.dir, "../../../docs/contracts/relay-upload-inbox-v1.vectors.json"), "utf8"));
const WITNESS_KEY = V.witness.testOnlyKey as string;
const KEY = parseUploadWitnessKey(WITNESS_KEY);
const ROOT = "96bulls";
const T0 = 1_800_000_000_000;
const ENV = {
  HISTORY_UPLOAD_DIRECT: "on", INGEST_LEDGER: "s3", AWS_REGION: "us-east-1",
  UPLOAD_ADMISSION_BUCKET: "96bulls-aws-synthetic-transcripts-111111111111", UPLOAD_ADMISSION_SECRET_ID: "96bulls/aws-synthetic/upload-admission", INGEST_KEY_ROOT: ROOT,
};
const saved: Record<string, string | undefined> = {};
beforeAll(() => { for (const [k, v] of Object.entries(ENV)) { saved[k] = process.env[k]; process.env[k] = v; } });
afterAll(() => { for (const k of Object.keys(ENV)) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } });
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const s3Body = (key: string) => JSON.stringify({ Records: [{ eventName: "ObjectCreated:Put", s3: { object: { key: encodeURIComponent(key).replace(/%2F/g, "/") } } }] });
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const json = async (store: ObjectStore, key: string) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body)));

async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const page = await store.list(objectKey(prefix), cursor); out.push(...page.items.map((i) => String(i.key))); cursor = page.nextCursor; } while (cursor);
  return out;
}

/** A desktop upload: zstd JSON delivery request at its content-addressed key, the witness in its metadata. */
async function upload(store: ObjectStore, batchId: string, records = [record({ record_id: "r1" }), record({ record_id: "r2", session_id: "sess-2" })]) {
  const b = await batch(records, { batch_id: batchId });
  const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: ROOT, ...TENANT }), { originId: "origin-1", batchId, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
  const witness = encodeUploadWitness(KEY, { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
  await store.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
  return { key, batch: b };
}

/** The operator's fence-authority marker (ledger-s3.ts FenceAuthorityDoc): without it nothing is admitted. */
const writeAuthority = (store: ObjectStore) => store.put(objectKey(fenceAuthorityKey(ROOT)),
  new TextEncoder().encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", writtenAtMs: T0 })), { contentType: "application/json" });

function lambda(authority = true) {
  const s = scratchStore();
  cleanups.push(s.done);
  const store = s.store;
  const authorityWritten = authority ? writeAuthority(store) : Promise.resolve();
  let fetched = 0;
  const lines: Record<string, unknown>[] = [];
  const handler = createUploadAdmissionHandler(hostedUploadAdmissionRuntime({
    secret: async () => JSON.stringify({ upload_witness_key: WITNESS_KEY }),
    store: () => store,
    fetcher: (async () => { fetched++; throw new Error("no network on this path"); }) as never,
    nowMs: () => T0,
  }), (l) => void lines.push(l));
  return { store, handler: async (e: Parameters<typeof handler>[0]) => { await authorityWritten; return handler(e); }, lines, fetched: () => fetched };
}

describe("upload -> promoteUpload -> inline admission with the S3 ledger (decision 0005)", () => {
  test("admitted: canonical manifests written, outcome recorded, batch committed, revisions stored; no SQL, no network", async () => {
    const { store, handler, lines, fetched } = lambda();
    // The user's privacy journal exists at seq 0 (the witness seq), as the issuer would have read it.
    await store.put(objectKey(journalKey(ROOT, TENANT)), new TextEncoder().encode(JSON.stringify({ v: 3, seq: 0, pending: [], ambiguous: [] })), { contentType: "application/json" });
    const { key, batch: b } = await upload(store, "batch-e2e");
    const res = await handler({ Records: [{ messageId: "m1", body: s3Body(key) }] });
    expect(res).toEqual({ batchItemFailures: [] });
    expect(lines.at(-1)).toMatchObject({ outcome: "batch", records: 1, failures: 0, handed_off: 1 });

    // The upload is consumed; its status resolves to admitted through the outcome marker.
    expect(await exists(store, key)).toBe(false);
    const status = await uploadAdmissionStatus({ store, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), TENANT, key);
    expect(status).toMatchObject({ ok: true, body: { state: "admitted" } });
    const handed = await json(store, uploadStatusKey(parseUploadKey(key) as never));
    expect(await json(store, outcomeKey(receiptDir(ROOT, TENANT, "origin-1", handed.receiptId), "admitted"))).toMatchObject({ v: 1, state: "admitted", code: "accepted" });

    // The canonical layout: one manifest per session, under the m=srv_ machine (what the manifest notification indexes).
    const manifests = (await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).filter((k) => k.endsWith("/manifest.json"));
    expect(manifests).toHaveLength(2);
    expect(manifests.every((k) => /\/m=srv_[0-9a-f]{32}\//.test(k))).toBe(true);

    // The S3 ledger's own records.
    const prepared = await prepareDelivery(TENANT, b);
    const keys = s3LedgerKeys(ROOT, TENANT);
    expect(await json(store, keys.batch("origin-1", "batch-e2e"))).toMatchObject({ state: "committed", digest: prepared.batchDigest, receiptId: prepared.receipt.receiptId });
    expect((await json(store, keys.revisions("origin-1", "r1"))).records.r1.slice(0, 2)).toEqual([1, "r1:r1"]);
    expect((await json(store, keys.revisions("origin-1", "r2"))).records.r2.slice(0, 2)).toEqual([1, "r2:r1"]);

    expect(sqlCalls).toEqual([]);
    expect(fetched()).toBe(0);
  });

  test("a redelivered notification after admission acks without writing again; a disabled session is acknowledged, not stored", async () => {
    const { store, handler } = lambda();
    const { key } = await upload(store, "batch-a");
    expect(await handler({ Records: [{ messageId: "m1", body: s3Body(key) }] })).toEqual({ batchItemFailures: [] });
    const before = (await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).length;
    expect(await handler({ Records: [{ messageId: "m2", body: s3Body(key) }] })).toEqual({ batchItemFailures: [] });
    expect((await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).length).toBe(before);

    // The user disables sess-2 (the S3 fence); the next upload's sess-2 records are acknowledged, not stored.
    const { s3Ledger } = await import("../ledger-s3.ts");
    await s3Ledger(store, ROOT).setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-3"]], enabled: false });
    const manifests = async () => (await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).filter((k) => k.endsWith("/manifest.json")).length;
    const beforeDisabled = await manifests();
    const second = await upload(store, "batch-b", [record({ record_id: "r9", session_id: "sess-3" })]);
    expect(await handler({ Records: [{ messageId: "m3", body: s3Body(second.key) }] })).toEqual({ batchItemFailures: [] });
    expect(await manifests()).toBe(beforeDisabled);
    // Control: the same upload for an enabled session does add its manifest.
    const third = await upload(store, "batch-c", [record({ record_id: "r9", session_id: "sess-4" })]);
    expect(await handler({ Records: [{ messageId: "m4", body: s3Body(third.key) }] })).toEqual({ batchItemFailures: [] });
    expect(await manifests()).toBe(beforeDisabled + 1);
    expect(await json(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-b"))).toMatchObject({ state: "committed" });
    expect(sqlCalls).toEqual([]);
  });

  test("no fence-authority marker: every record is a retry (never an admit), and the upload stays for redelivery", async () => {
    const { store, handler, lines } = lambda(false);
    const { key } = await upload(store, "batch-na");
    expect(await handler({ Records: [{ messageId: "m1", body: s3Body(key) }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
    expect(lines.at(-1)).toMatchObject({ failures: 1, "admission_enqueue_failed:admission_ledger_fence_authority_missing": 1 });
    expect(await exists(store, key)).toBe(true);
    expect(await exists(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-na"))).toBe(false);
    expect((await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).filter((k) => k.endsWith("/manifest.json"))).toEqual([]);
    await writeAuthority(store);
    expect(await handler({ Records: [{ messageId: "m2", body: s3Body(key) }] })).toEqual({ batchItemFailures: [] });
    expect(await exists(store, key)).toBe(false);
    expect(sqlCalls).toEqual([]);
  });

  test("a transient admission failure is a retry (the record fails, SQS redelivers) and the redelivery admits", async () => {
    const { store, handler, lines } = lambda();
    const { key } = await upload(store, "batch-r");
    // The fence is unreadable once: the ledger fails closed (retry), never admits against an empty fence.
    const fence = s3LedgerKeys(ROOT, TENANT).fence;
    await store.put(objectKey(fence), new TextEncoder().encode("garbage"), { contentType: "application/json" });
    expect(await handler({ Records: [{ messageId: "m1", body: s3Body(key) }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
    expect(lines.at(-1)).toMatchObject({ failures: 1, "admission_enqueue_failed:admission_ledger_fence_invalid": 1 });
    expect(await exists(store, key)).toBe(true);
    await store.delete(objectKey(fence));
    expect(await handler({ Records: [{ messageId: "m2", body: s3Body(key) }] })).toEqual({ batchItemFailures: [] });
    expect(await exists(store, key)).toBe(false);
    expect(sqlCalls).toEqual([]);
  });
});

describe("self-host (ingest-edge) with INGEST_LEDGER=s3: the consumer admits through ledger-s3.ts", () => {
  const enabled = {
    RELAYHISTORY_SELFHOST_INGEST: "true", RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
    RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test", RELAYCONTROL_INTROSPECT_SECRET: "s",
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase", LOCKER_BUCKET: "transcripts",
  };

  test("config: unset or sql leaves the settings unchanged; s3 is set; anything else, or s3 with a SQL-only feature, refuses", async () => {
    const { selfhostIngestConfig } = await import("../selfhost-config.ts");
    const base = selfhostIngestConfig(enabled);
    expect("ledger" in base).toBe(false);
    expect(selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "sql" })).toEqual(base);
    expect(selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "" })).toEqual(base);
    expect(selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "s3" })).toMatchObject({ enabled: true, ledger: "s3" });
    expect(() => selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "postgres" })).toThrow(/INGEST_LEDGER must be sql or s3/);
    expect(() => selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "s3", RELAYHISTORY_SELFHOST_SESSION_SELECTION: "true" })).toThrow(/INGEST_LEDGER=s3 refuses/);
    expect(() => selfhostIngestConfig({ ...enabled, INGEST_LEDGER: "s3", INGEST_UPLOAD_CONSENT: "enforce" })).toThrow(/INGEST_LEDGER=s3 refuses/);
  });

  test("upload object -> the consumer's scan -> promoteUpload -> admitMessage over the S3 ledger -> admitted; the SQL runner is never called", async () => {
    const { selfhostIngestRuntime } = await import("../selfhost-server.ts");
    const s = scratchStore();
    cleanups.push(s.done);
    const store = s.store;
    await writeAuthority(store);
    const runCalls: string[] = [];
    const run = async (text: string) => { runCalls.push(text); throw new Error("SQL runner called on the S3 ledger path"); };
    let consumer: { scan(): Promise<void>; settle(): Promise<void> } | undefined;
    const runtime = selfhostIngestRuntime({
      keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 16, ledger: "s3",
      admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 20 },
      uploadDirect: { config: uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), witnessKey: WITNESS_KEY, scanBatch: 16 },
    }, { run: run as never, store, verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} },
    { startConsumer: false, nowMs: () => T0, onConsumer: (c) => { consumer = c; }, log: { info: () => {}, warn: () => {}, error: () => {} } as never });
    const { key, batch: b } = await upload(store, "batch-selfhost");
    // Scan 1 promotes the upload (the hand-off notifies the consumer); settle admits it.
    await consumer!.scan();
    await consumer!.settle();
    expect(await exists(store, key)).toBe(false);
    const status = await uploadAdmissionStatus({ store, keyRoot: ROOT }, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), TENANT, key);
    expect(status).toMatchObject({ ok: true, body: { state: "admitted" } });
    const manifests = async () => (await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).filter((k) => k.endsWith("/manifest.json")).length;
    expect(await manifests()).toBe(2);
    const prepared = await prepareDelivery(TENANT, b);
    expect(await json(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-selfhost"))).toMatchObject({ state: "committed", digest: prepared.batchDigest });
    // The selection route's ledger is the same S3 fence (one authority): a disable fences the next upload.
    const { s3Ledger } = await import("../ledger-s3.ts");
    await s3Ledger(store, ROOT).setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-9"]], enabled: false });
    const next = await upload(store, "batch-selfhost-2", [record({ record_id: "r7", session_id: "sess-9" })]);
    await consumer!.scan();
    await consumer!.settle();
    expect(await exists(store, next.key)).toBe(false);
    expect(await manifests()).toBe(2);
    expect(await json(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-selfhost-2"))).toMatchObject({ state: "committed" });
    expect(runCalls).toEqual([]);
    expect(sqlCalls).toEqual([]);
    await runtime.close();
  });

  test("with direct upload admission and no fence-authority marker, nothing is admitted (the upload waits)", async () => {
    const { selfhostIngestRuntime } = await import("../selfhost-server.ts");
    const s = scratchStore();
    cleanups.push(s.done);
    const store = s.store;
    let consumer: { scan(): Promise<void>; settle(): Promise<void> } | undefined;
    const runtime = selfhostIngestRuntime({
      keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 16, ledger: "s3",
      admission: { mode: "async", concurrency: 2, scanIntervalMs: 15_000, maxAttempts: 20 },
      uploadDirect: { config: uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }), witnessKey: WITNESS_KEY, scanBatch: 16 },
    }, { run: (async () => { throw new Error("SQL runner called"); }) as never, store, verify: async () => { throw new Error("unused"); }, ready: async () => true, close: async () => {} },
    { startConsumer: false, nowMs: () => T0, onConsumer: (c) => { consumer = c; }, log: { info: () => {}, warn: () => {}, error: () => {} } as never });
    await upload(store, "batch-selfhost-na");
    await consumer!.scan();
    await consumer!.settle();
    expect(await exists(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", "batch-selfhost-na"))).toBe(false);
    expect((await listAll(store, `${ROOT}/${TENANT.orgId}/v1`)).filter((k) => k.endsWith("/manifest.json"))).toEqual([]);
    expect(sqlCalls).toEqual([]);
    await runtime.close();
  });

  test("INGEST_LEDGER=s3 requires the marker on EVERY admission path, with direct upload off too (the synchronous HTTP path's ledger)", async () => {
    const { selfhostS3Ledger } = await import("../selfhost-server.ts");
    const { prepareDelivery: prep } = await import("../vendor/delivery/prepare.ts");
    const s = scratchStore();
    cleanups.push(s.done);
    const store = s.store;
    const ledger = selfhostS3Ledger(ROOT, { run: (async () => { throw new Error("SQL runner called"); }) as never, store });
    const p = await prep(TENANT, await batch([record({ record_id: "r1" })], { batch_id: "batch-sync" }));
    const input = { tenant: TENANT, originId: "origin-1", batchId: p.batchId, batchDigest: p.batchDigest, receiptId: p.receipt.receiptId, sessions: [["claude", "sess-1"]] as [string, string][],
      records: [{ source: "claude", sessionId: "sess-1", recordId: "r1", revisionId: "r1:r1", revision: 1, digest: "a".repeat(64) }] };
    await expect(ledger.claim(input)).rejects.toMatchObject({ code: "ledger_fence_authority_missing" });
    expect(await exists(store, s3LedgerKeys(ROOT, TENANT).batch("origin-1", p.batchId))).toBe(false);
    await writeAuthority(store);
    expect((await ledger.claim(input)).kind).toBe("claimed");
    expect(sqlCalls).toEqual([]);
  });

  test("the synchronous runtime (no admission mode, no direct upload) is built on that gated ledger", async () => {
    const src = readFileSync(join(import.meta.dir, "../selfhost-server.ts"), "utf8");
    expect(src).toContain('const ledger: Ledger = s3 ? selfhostS3Ledger(settings.keyRoot, parts) : sqlLedger(');
    expect(src).toContain('s3Ledger(parts.store, keyRoot, { acceptanceMode: "canonical", fenceAuthority: "required" })');
  });

  test("a direct caller cannot combine the S3 ledger with a SQL-only feature", async () => {
    const { selfhostIngestRuntime } = await import("../selfhost-server.ts");
    const s = scratchStore();
    cleanups.push(s.done);
    expect(() => selfhostIngestRuntime({ keyRoot: ROOT, maxPerUser: 4, maxPerOrg: 16, ledger: "s3", sessionSelection: true },
      { run: (async () => ({ rows: [] })) as never, store: s.store, verify: async () => { throw new Error("x"); }, ready: async () => true, close: async () => {} })).toThrow(/INGEST_LEDGER=s3 refuses/);
  });
});
