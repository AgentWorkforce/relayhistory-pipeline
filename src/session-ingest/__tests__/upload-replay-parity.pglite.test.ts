/**
 * Step 8: replay idempotency ACROSS the two admission paths of the self-host ingest.
 *
 *   HTTP:   POST /v1/delivery/batches through the Node ingest edge (createIngestEdge) into createIngestApp
 *           with SELFHOST_INGEST_ADMISSION=async: a queued 200 + x-relay-admission, one inbox attempt.
 *   Upload: a desktop-written object under the upload inbox (HISTORY_UPLOAD_DIRECT=on), promoted by the
 *           consumer's scan (promoteUpload), handed off as an inbox attempt for the same receipt.
 *
 * The identity the code dedupes on is the ledger claim's primary key (org, workspace, origin, batch id)
 * with the batch digest (sessions.ingest_batches), and the receipt id derived from exactly those
 * (vendor/delivery/prepare.ts); the inbox key is per attempt and never dedupes (async-admission.ts
 * inboxKey). Both paths therefore land in the same receipt directory and the second attempt is the
 * claim's replay: one ledger batch row, one commit, one revision row, no outbox row, and the outcome
 * marker of the second attempt says `replay`. Two attempts both pending before the consumer runs
 * both claim (the second is the ledger's `resume`) and both commit idempotently: still one row.
 *
 * PGlite with every migration and the ingest role, a scratch fs store, a fixed clock, the contract
 * vectors' test-only witness key, the edge's verify faked in-process. No network, no AWS.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { SYNC_SCOPE, type IngestIdentity } from "../app.ts";
import { ADMISSION_HEADER, ADMISSION_QUEUED, outcomeKey, receiptDir } from "../async-admission.ts";
import { createIngestEdge } from "../ingest-edge.ts";
import type { SqlRun } from "../ledger-sql.ts";
import { selfhostAdmissionSettings, selfhostUploadDirectSettings } from "../selfhost-config.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime } from "../selfhost-server.ts";
import { uploadAdmissionStatus } from "../upload-admission.ts";
import { encodeUploadWitness, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } from "../upload-inbox-contract.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

setDefaultTimeout(120_000);

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
function sql(path: string): string {
  return read(path).split("\n").map((line) => (line.startsWith("\\ir ") ? sql(join(dirname(path), line.slice(4).trim())) : line.startsWith("\\") ? "" : line)).join("\n");
}

const ORIGIN = "https://history.edge.test";
/** The contract vectors' test-only witness key (bytes 0x00..0x1f): never configured anywhere real. */
const WITNESS_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
/** Fixed clock: 2026-11-01T00:00:00Z. */
const T0 = 1_793_491_200_000;
const IDENTITY: IngestIdentity = { ...TENANT, scopes: [SYNC_SCOPE] };

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const f of cleanups.splice(0).reverse()) await f();
});

/** PGlite with every migration and the ingest role; `run` is SET ROLE bulls96_session_ingest, serialized. */
async function database() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  cleanups.push(() => db.close());
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(sql("supabase/bootstrap/session_ingest_role.sql"));
  let chain: Promise<unknown> = Promise.resolve();
  const run: SqlRun = (text, params) => {
    const next = chain.then(async () => {
      await db.exec("SET ROLE bulls96_session_ingest");
      try {
        return (await db.query(text, params as never)) as { rows: Record<string, unknown>[] };
      } finally {
        await db.exec("RESET ROLE");
      }
    });
    chain = next.catch(() => {});
    return next;
  };
  return { db, run };
}

/** The self-host async runtime (upload-direct on) behind the Node ingest edge. */
async function start() {
  const { db, run: base } = await database();
  const fs = scratchStore();
  cleanups.push(fs.done);
  const store: ObjectStore = fs.store;
  const texts: string[] = [];
  const run: SqlRun = (text, params) => { texts.push(text); return base(text, params); };
  const admission = selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "async", SELFHOST_ADMISSION_MAX_ATTEMPTS: "3" })!;
  const uploadDirect = selfhostUploadDirectSettings({ HISTORY_UPLOAD_DIRECT: "on", UPLOAD_WITNESS_KEY: WITNESS_KEY }, "96bulls", admission)!;
  let consumer: SelfhostAdmissionConsumer | undefined;
  const quiet = { info: () => {}, warn: () => {}, error: () => {} };
  const runtime = selfhostIngestRuntime(
    { keyRoot: "96bulls", maxPerUser: 4, maxPerOrg: 16, admission, uploadDirect },
    { run, store, verify: async () => IDENTITY, ready: async () => true, close: async () => {} },
    { nowMs: () => T0, startConsumer: false, onConsumer: (c) => { consumer = c; }, log: quiet },
  );
  cleanups.push(() => runtime.close());
  // The edge in front, exactly as the self-host wrapper hands over: bind the verified identity, then the app.
  const edge = createIngestEdge(async () => IDENTITY, (identify) => ({
    fetch: (request: Request) => { runtime.bind(request, identify(request)); return runtime.app.fetch(request); },
  }));
  const drain = async () => { await consumer!.scan(); await consumer!.settle(); };
  const count = async (table: string) => Number(((await db.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0] as { n: number }).n);
  return { db, store, edge, drain, texts, count, commits: () => texts.filter((t) => t.includes("ingest_commit_batch")).length };
}
type Started = Awaited<ReturnType<typeof start>>;

async function keys(store: ObjectStore, prefix: string): Promise<{ key: string; size: number }[]> {
  const out: { key: string; size: number }[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => ({ key: String(i.key), size: i.size })));
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}
/** Inbox attempts with content (a retired attempt is a tombstone, under INBOX_MIN_BYTES). */
const attempts = async (s: Started) => (await keys(s.store, "96bulls/.admission-inbox")).filter((k) => k.key.endsWith(".ndjson.zst"));
const uploads = async (s: Started) => (await keys(s.store, "96bulls/.upload-inbox")).map((k) => k.key);

/** The same delivery batch for both paths. */
const theBatch = () => batch([record({ record_id: "rp1" })], { batch_id: "b-replay" });

async function viaHttp(s: Started) {
  const body = JSON.stringify({ protocolVersion: 1, batch: await theBatch() });
  const res = await s.edge.fetch(new Request(`${ORIGIN}/v1/delivery/batches`, {
    method: "POST", body, headers: { authorization: "Bearer sh_at_t", "content-type": "application/json", "x-correlation-id": "corr-replay" },
  }));
  return { status: res.status, admission: res.headers.get(ADMISSION_HEADER), receiptId: ((await res.json()) as { receiptId: string }).receiptId };
}

async function viaUpload(s: Started) {
  const b = await theBatch();
  const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: "96bulls", ...TENANT }), { originId: "origin-1", batchId: b.batch_id, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: 0 });
  const witness = encodeUploadWitness(parseUploadWitnessKey(WITNESS_KEY), { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq: 0, exp: Math.floor(T0 / 1000) + 3600 });
  await s.store.put(objectKey(key), bytes, { metadata: { "relay-witness": witness } });
  return key;
}

const status = async (s: Started, key: string) =>
  uploadAdmissionStatus({ store: s.store, keyRoot: "96bulls" }, { mode: "on", orgs: new Set(), workspaces: new Set() } as never, TENANT, key);

async function outcome(s: Started, receiptId: string) {
  const dir = receiptDir("96bulls", TENANT, "origin-1", receiptId);
  const keyOf = (state: "admitted" | "refused") => objectKey(outcomeKey(dir, state));
  const get = async (state: "admitted" | "refused") => {
    if (!(await s.store.head(keyOf(state)))) return null;
    return JSON.parse(await new Response((await s.store.getObject(keyOf(state))).body).text()) as { state: string; code: string };
  };
  return { admitted: await get("admitted"), refused: await get("refused"), attemptsInDir: (await keys(s.store, dir)).filter((k) => k.key.endsWith(".ndjson.zst")).length };
}

/**
 * Ledger evidence: one batch row (committed, this receipt), one revision row, no outbox row, `commits`
 * commit calls; store evidence: one manifest naming one chunk, that chunk's raw object present and
 * readable at its content-addressed key, and between `rawObjects.min` and `rawObjects.max` raw chunk
 * objects in total. A single sealing attempt leaves exactly one. Two attempts that both reach the writer
 * leave one or two, depending on the interleaving: the raw key is
 * `raw/{seq}-{offset}-{sha256(source)[0:8]}` and each attempt's source lines carry its own wall-clock
 * `received_at` (millisecond resolution), so two seals in different milliseconds write two keys (the
 * manifest CAS loser's chunk stays unreferenced), while two seals in the same millisecond write the same
 * key (create-only put: the second keeps the stored object), and a writer that reads the winner's manifest
 * before sealing replays without sealing. Never more than one object per attempt.
 */
async function exactlyOne(s: Started, receiptId: string, commits = 1, rawObjects: { min: number; max: number } = { min: 1, max: 1 }) {
  const rows = (await s.db.query("SELECT batch_id, receipt_id, state FROM sessions.ingest_batches")).rows;
  expect(rows).toEqual([{ batch_id: "b-replay", receipt_id: receiptId, state: "committed" }]);
  expect(await s.count("sessions.ingest_record_revisions")).toBe(1);
  expect(await s.count("sessions.ingest_buffer_outbox")).toBe(0);
  expect(s.commits()).toBe(commits);
  // The tenant's stored objects: one manifest naming one chunk.
  const tenant = (await keys(s.store, "96bulls/org-a")).map((k) => k.key).sort();
  const manifests = tenant.filter((k) => k.endsWith("/manifest.json"));
  expect(manifests).toHaveLength(1);
  const manifest = JSON.parse(await new Response((await s.store.getObject(objectKey(manifests[0]!))).body).text()) as {
    raw: { chunk_count: number; chunks: { seq: number; start_offset: number; sha256_8: string; stored_bytes: number }[] };
  };
  expect(manifest.raw.chunk_count).toBe(1);
  expect(manifest.raw.chunks).toHaveLength(1);
  // The chunk the manifest references is stored (in every interleaving) and readable in full.
  const chunk = manifest.raw.chunks[0]!;
  const referenced = `${dirname(manifests[0]!)}/raw/${String(chunk.seq).padStart(10, "0")}-${chunk.start_offset}-${chunk.sha256_8}.ndjson.zst`;
  const raw = tenant.filter((k) => k.includes("/raw/"));
  expect(raw).toContain(referenced);
  expect((await new Response((await s.store.getObject(objectKey(referenced))).body).arrayBuffer()).byteLength).toBe(chunk.stored_bytes);
  expect(raw.length).toBeGreaterThanOrEqual(rawObjects.min);
  expect(raw.length).toBeLessThanOrEqual(rawObjects.max);
}

describe("replay idempotency across the HTTP (edge, async) and upload (promoteUpload) paths", () => {
  test("HTTP first, then the upload: one admission; the upload's attempt is the claim's replay", async () => {
    const s = await start();
    const http = await viaHttp(s);
    expect(http.status).toBe(200);
    expect(http.admission).toBe(ADMISSION_QUEUED);
    expect(http.receiptId).toMatch(/^rhr_[0-9a-f]{64}$/);
    expect(await attempts(s)).toHaveLength(1);
    expect(s.texts).toHaveLength(0); // queued: no SQL on the request path
    await s.drain();
    expect((await outcome(s, http.receiptId)).admitted).toMatchObject({ state: "admitted", code: "accepted" });
    await exactlyOne(s, http.receiptId);

    const key = await viaUpload(s);
    await s.drain();
    expect(await uploads(s)).toEqual([]);
    const after = await outcome(s, http.receiptId);
    // Same receipt directory, a second attempt, both retired; the marker now records the replay.
    expect(after.attemptsInDir).toBe(2);
    expect(after.admitted).toMatchObject({ state: "admitted", code: "replay" });
    expect(after.refused).toBeNull();
    const st = await status(s, key);
    expect(st).toMatchObject({ ok: true, body: { state: "admitted", receiptId: http.receiptId } });
    await exactlyOne(s, http.receiptId);
    for (const a of await attempts(s)) expect(a.size).toBeLessThan(64); // tombstoned, nothing left to admit
  });

  test("upload first, then HTTP: one admission; the HTTP attempt is queued 200 with the same receipt, then the claim's replay", async () => {
    const s = await start();
    const key = await viaUpload(s);
    await s.drain();
    expect(await uploads(s)).toEqual([]);
    const st = await status(s, key);
    expect(st.ok).toBe(true);
    const receiptId = (st as { body: { receiptId: string; state: string } }).body.receiptId;
    expect((st as { body: { state: string } }).body.state).toBe("admitted");
    expect((await outcome(s, receiptId)).admitted).toMatchObject({ state: "admitted", code: "accepted" });
    await exactlyOne(s, receiptId);

    const http = await viaHttp(s);
    // The queued ACK is answered before any claim: it is the same 200 + x-relay-admission, the same receipt.
    expect(http).toEqual({ status: 200, admission: ADMISSION_QUEUED, receiptId });
    await s.drain();
    const after = await outcome(s, receiptId);
    expect(after.attemptsInDir).toBe(2);
    expect(after.admitted).toMatchObject({ state: "admitted", code: "replay" });
    expect(after.refused).toBeNull();
    await exactlyOne(s, receiptId);
  });

  test("both attempts pending before the consumer runs (documents current behaviour): one row, but both attempts claim and commit", async () => {
    const s = await start();
    const http = await viaHttp(s);
    const key = await viaUpload(s);
    await s.drain();
    await s.drain(); // the upload's hand-off may land after the first pass's listing
    expect(await uploads(s)).toEqual([]);
    const after = await outcome(s, http.receiptId);
    expect(after.attemptsInDir).toBe(2);
    expect(after.refused).toBeNull();
    // Both claims ran before either commit: the second claim of the still-pending batch is the ledger's
    // `resume` (same digest, new claim token), not a replay, so both attempts commit and both record
    // `accepted`. ingest_commit_batch is idempotent, so the rows stay exactly one and the manifest names one
    // chunk. The raw object count is interleaving-dependent (see exactlyOne): two when the attempts seal in
    // different milliseconds (the CAS loser's chunk stays unreferenced, one orphan), one when they seal in
    // the same millisecond or the second reads the winner's manifest first.
    expect(after.admitted).toMatchObject({ state: "admitted", code: "accepted" });
    const claims = s.texts.filter((t) => t.includes("ingest_claim_batch")).length;
    expect(claims).toBe(2);
    expect(await status(s, key)).toMatchObject({ ok: true, body: { state: "admitted", receiptId: http.receiptId } });
    await exactlyOne(s, http.receiptId, 2, { min: 1, max: 2 });
  });

  test("control: two pending HTTP attempts of the same batch behave identically (path-independent)", async () => {
    const s = await start();
    const a = await viaHttp(s);
    const b = await viaHttp(s);
    expect(b).toEqual(a);
    await s.drain();
    const after = await outcome(s, a.receiptId);
    expect(after.attemptsInDir).toBe(2);
    expect(after.admitted).toMatchObject({ state: "admitted", code: "accepted" });
    expect(s.texts.filter((t) => t.includes("ingest_claim_batch")).length).toBe(2);
    await exactlyOne(s, a.receiptId, 2, { min: 1, max: 2 }); // same interleaving-dependent 1 or 2 raw objects
  });
});
