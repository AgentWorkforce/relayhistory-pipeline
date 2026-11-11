/**
 * HISTORY_UPLOAD_DIRECT on the self-host ingest (selfhost-admission.ts upload scan, upload-admission.ts) end to
 * end over real loopback node:http: the stub relaycontrol introspection, the ledger in PGlite with every
 * Supabase migration and the ingest role, and a scratch fs object store. A desktop-written upload object is
 * promoted by the consumer's scan, admitted through the unchanged admission consumer into the real ledger,
 * and its §5 status read back through GET /v1/delivery/admission?uploadKey=.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { ObjectStoreError, objectKey, type ObjectStore } from "../../shared/vendor.ts";
import type { SqlRun } from "../ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "../selfhost-auth.ts";
import type { SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime, startSelfhostIngestService, type RuntimeBuilder, type SelfhostIngestService } from "../selfhost-server.ts";
import { encodeUploadWitness, parseUploadAdmissionStatus, parseUploadWitnessKey, uploadKeyPrefix, uploadObjectKey, uploadObjectSha256Hex } from "../upload-inbox-contract.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

setDefaultTimeout(90_000);

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
function sql(path: string): string {
  return read(path).split("\n").map((line) => (line.startsWith("\\ir ") ? sql(join(dirname(path), line.slice(4).trim())) : line.startsWith("\\") ? "" : line)).join("\n");
}

const ORIGIN = "https://history.selfhost.test";
const SECRET = "introspect-secret-test";
const TOKEN = "sh_at_desktop-token-1";

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

async function relaycontrol() {
  const now = () => Math.floor(Date.now() / 1000);
  const server: Server = createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ active: true, sub: TENANT.userId, email: null, principal_type: "desktop-user", machine_id: null, org: TENANT.orgId, workspace: TENANT.workspaceId,
        scopes: ["rth:sync"], aud: ORIGIN, iat: now() - 10, exp: now() + 600, token_id: "tok-1" }));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  cleanups.push(() => new Promise<void>((r) => { server.closeAllConnections?.(); server.close(() => r()); }));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

/** The scratch store with injectable faults (an injected fault is a transient store error). */
function faultyStore(inner: ObjectStore) {
  const faults: { put?: (key: string) => boolean; get?: (key: string) => boolean; list?: (prefix: string) => boolean } = {};
  const fail = (key: string) => { throw new ObjectStoreError("transient", "fs", `injected ${key.length}`); };
  const store: ObjectStore = {
    backend: inner.backend,
    head: (k) => inner.head(k),
    get: (k, r) => inner.get(k, r),
    getObject: async (k, r) => (faults.get?.(String(k)) ? fail(String(k)) : inner.getObject(k, r)),
    list: async (p, c) => (faults.list?.(String(p)) ? fail(String(p)) : inner.list(p, c)),
    presignGet: (k, t) => inner.presignGet(k, t),
    put: async (k, b, o) => (faults.put?.(String(k)) ? fail(String(k)) : inner.put(k, b, o)),
    delete: (k) => inner.delete(k),
  };
  return { store, faults };
}

async function keys(store: ObjectStore, prefix = "96bulls"): Promise<{ key: string; size: number }[]> {
  const out: { key: string; size: number }[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    out.push(...page.items.map((i) => ({ key: String(i.key), size: i.size })));
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}
const tenantKeys = async (store: ObjectStore) => (await keys(store)).filter((k) => k.key.startsWith("96bulls/org-a/")).map((k) => k.key);
const inboxObjects = async (store: ObjectStore) => (await keys(store, "96bulls/.admission-inbox")).filter((k) => k.key.endsWith(".ndjson.zst"));
const outcomes = async (store: ObjectStore) => (await keys(store, "96bulls/.admission-inbox")).map((k) => /outcome-(admitted|refused)\.json$/.exec(k.key)?.[0]).filter((k): k is string => k !== undefined);

interface Started {
  service: SelfhostIngestService;
  url: (path: string) => string;
  store: ObjectStore;
  faults: ReturnType<typeof faultyStore>["faults"];
  consumer: () => SelfhostAdmissionConsumer;
  sql: { calls: number; texts: string[]; fail?: (text: string) => boolean };
  run: SqlRun;
  tick: (ms: number) => void;
  now: () => number;
  logs: string[];
}

/** A self-host ingest; `shared` reuses another instance's database and store (a restart, or a second replica). */
async function start(over: NodeJS.ProcessEnv = {}, shared?: { run: SqlRun; store: ObjectStore; inner?: ObjectStore }, opts: { startConsumer?: boolean } = {}): Promise<Started> {
  const rc = await relaycontrol();
  const base = shared ?? await (async () => {
    const { run } = await database();
    const fs = scratchStore();
    cleanups.push(fs.done);
    return { run, store: fs.store };
  })();
  const { store, faults } = faultyStore(base.store);
  const counter: Started["sql"] = { calls: 0, texts: [] };
  const run: SqlRun = async (text, params) => {
    counter.calls++;
    counter.texts.push(text);
    if (counter.fail?.(text)) throw Object.assign(new Error("injected"), { code: "08006" });
    return base.run(text, params);
  };
  let clock = Date.now();
  let consumer: SelfhostAdmissionConsumer | undefined;
  const logs: string[] = [];
  const build: RuntimeBuilder = async (settings, e) =>
    selfhostIngestRuntime(settings, { run, store, verify: relaycontrolIngestIdentity(settings, selfhostIntrospector(settings, e)), ready: async () => true, close: async () => {} }, {
      nowMs: () => clock,
      startConsumer: opts.startConsumer ?? false,
      onConsumer: (c) => { consumer = c; },
      log: {
        info: (m, f) => void logs.push(JSON.stringify({ m, ...f })),
        warn: (m, f) => void logs.push(JSON.stringify({ m, ...f })),
        error: (m, f) => void logs.push(JSON.stringify({ m, ...f })),
      },
    });
  const service = await startSelfhostIngestService({
    RELAYHISTORY_SELFHOST_INGEST: "true",
    RELAYHISTORY_INGEST_ORIGIN: ORIGIN,
    RELAYCONTROL_INTROSPECT_URL: rc,
    RELAYCONTROL_INTROSPECT_SECRET: SECRET,
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres?sslmode=disable",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    LOCKER_BUCKET: "transcripts",
    SELFHOST_INGEST_ADMISSION: "async",
    SELFHOST_ADMISSION_MAX_ATTEMPTS: "3",
    ...over,
  }, { host: "127.0.0.1", port: 0, build });
  cleanups.push(() => service.close());
  return {
    service, store, faults, run, sql: counter, logs,
    url: (path) => `http://127.0.0.1:${service.port}${path}`,
    consumer: () => { if (!consumer) throw new Error("no consumer (sync mode)"); return consumer; },
    tick: (ms) => { clock += ms; },
    now: () => clock,
  };
}

const deliveryBody = async (id = "e1", over: Parameters<typeof batch>[1] = {}, rec: Partial<Parameters<typeof record>[0]> = {}) =>
  JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: id, ...rec })], { batch_id: `b-${id}`, ...over }) });
const post = (url: string, body: string, headers: Record<string, string> = {}) =>
  fetch(url, { method: "POST", body, headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}`, ...headers } });
const errorCode = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;
const drain = async (s: Started) => { await s.consumer().scan(); await s.consumer().settle(); };

/** The contract vectors' test-only witness key (bytes 0x00..0x1f): never configured anywhere real. */
const WITNESS_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const UPLOAD_ON = { HISTORY_UPLOAD_DIRECT: "on", UPLOAD_WITNESS_KEY: WITNESS_KEY };

/** A desktop upload for TENANT: zstd JSON delivery request at its content-addressed key, the witness in metadata. */
async function upload(s: Started, o: { id?: string; witness?: string | null; seq?: number } = {}) {
  const id = o.id ?? "e1", seq = o.seq ?? 0;
  const b = await batch([record({ record_id: id })], { batch_id: `b-${id}` });
  const bytes = new Uint8Array(zstdCompressSync(Buffer.from(JSON.stringify({ protocolVersion: 1, batch: b }))));
  const key = uploadObjectKey(uploadKeyPrefix({ keyRoot: "96bulls", ...TENANT }), { originId: "origin-1", batchId: b.batch_id, objectSha256Hex: uploadObjectSha256Hex(bytes), witnessSeq: seq });
  const witness = o.witness === undefined
    ? encodeUploadWitness(parseUploadWitnessKey(WITNESS_KEY), { v: 1, org: TENANT.orgId, ws: TENANT.workspaceId, user: TENANT.userId, seq, exp: Math.floor(s.now() / 1000) + 3600 })
    : o.witness;
  await s.store.put(objectKey(key), bytes, witness === null ? {} : { metadata: { "relay-witness": witness } });
  return { key, batch: b };
}
const status = (s: Started, key: string) => fetch(s.url(`/v1/delivery/admission?uploadKey=${encodeURIComponent(key)}`), { headers: { authorization: `Bearer ${TOKEN}` } });
const uploads = async (s: Started) => (await keys(s.store, "96bulls/.upload-inbox")).map((k) => k.key);
/** Ledger commits the consumer ran (the self-host path is canonical: ingest_commit_batch once per admitted batch). */
const commits = (s: Started) => s.sql.texts.filter((t) => t.includes("ingest_commit_batch")).length;

describe("HISTORY_UPLOAD_DIRECT=on, self-host: upload -> scan -> admitted", () => {
  test("a witnessed upload is promoted by the scan, admitted into the ledger, removed, and reads back admitted", async () => {
    const s = await start(UPLOAD_ON);
    const { key } = await upload(s);
    // Before the scan: pending (the object exists), with Retry-After.
    const before = await status(s, key);
    expect(before.status).toBe(200);
    expect(before.headers.get("retry-after")).toBe("15");
    expect(parseUploadAdmissionStatus(await before.json())).toMatchObject({ state: "pending", uploadKey: key });
    expect(s.sql.calls).toBe(0);

    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(await tenantKeys(s.store)).not.toEqual([]);
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
    expect(commits(s)).toBe(1);
    const after = await status(s, key);
    expect(after.headers.get("retry-after")).toBeNull();
    const body = parseUploadAdmissionStatus(await after.json());
    expect(body).toMatchObject({ contract: "relay-upload-inbox-v1", uploadKey: key, state: "admitted" });
    expect(body!.receiptId).toMatch(/^rhr_[0-9a-f]{64}$/);
    expect(s.logs.some((l) => l.includes('"route":"ingest.upload_admission"') && l.includes('"handed_off":1'))).toBe(true);
    expect(s.logs.join("\n")).not.toContain(key);

    // Redelivery of the same bytes (the desktop's create-only retry after a lost answer): already admitted, one ledger row.
    await upload(s);
    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(commits(s)).toBe(1);
  });

  test("a bad witness is refused, recorded and removed; the status endpoint reports the §6 code; nothing reaches SQL", async () => {
    const s = await start(UPLOAD_ON);
    const { key } = await upload(s, { witness: null });
    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(s.sql.calls).toBe(0);
    expect(parseUploadAdmissionStatus(await (await status(s, key)).json())).toMatchObject({ state: "refused", code: "upload_witness_invalid" });
  });

  test("a store fault on the upload is retried with backoff, never refused; it lands once the store recovers", async () => {
    const s = await start(UPLOAD_ON);
    const { key } = await upload(s);
    s.faults.get = (k) => k === key;
    await drain(s);
    expect(await uploads(s)).toEqual([key]);
    s.faults.get = undefined;
    await drain(s); // still inside the backoff window: not retried yet
    expect(await uploads(s)).toEqual([key]);
    s.tick(16_000);
    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
  });

  test("no UPLOAD_WITNESS_KEY: every upload waits (retry); nothing refused, nothing removed", async () => {
    const s = await start({ HISTORY_UPLOAD_DIRECT: "on" });
    const { key } = await upload(s);
    await drain(s);
    expect(await uploads(s)).toEqual([key]);
    expect(s.logs.some((l) => l.includes('"upload_witness_key_missing":1'))).toBe(true);
    expect(parseUploadAdmissionStatus(await (await status(s, key)).json())).toMatchObject({ state: "pending" });
  });

  test("the scan promotes at most SELFHOST_UPLOAD_SCAN_BATCH objects per pass", async () => {
    const s = await start({ ...UPLOAD_ON, SELFHOST_UPLOAD_SCAN_BATCH: "1" });
    await upload(s, { id: "e1" });
    await upload(s, { id: "e2" });
    await drain(s);
    expect(await uploads(s)).toHaveLength(1);
    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(commits(s)).toBe(2);
  });

  test("the scan is slash-bounded: a sibling prefix sharing the namespace's name is never promoted or touched", async () => {
    const s = await start(UPLOAD_ON);
    const { key } = await upload(s);
    const sibling = key.replace("/.upload-inbox/", "/.upload-inbox-x/");
    await s.store.put(objectKey(sibling), new Uint8Array([1, 2, 3]));
    await drain(s);
    expect(await uploads(s)).toEqual([]);
    expect(await s.store.head(objectKey(sibling))).not.toBeNull();
    expect(commits(s)).toBe(1);
    expect(s.logs.some((l) => l.includes('"promoted":1'))).toBe(true);
  });
});

describe("HISTORY_UPLOAD_DIRECT off (the default) or misconfigured", () => {
  test("off: the upload prefix is never listed or promoted; ?uploadKey= is ignored (the existing query answers 400)", async () => {
    const s = await start();
    const { key } = await upload(s);
    let listed = 0;
    const prior = s.faults.list;
    s.faults.list = (p) => { if (p.includes(".upload-inbox")) listed++; return prior?.(p) ?? false; };
    await drain(s);
    expect(listed).toBe(0);
    expect(await uploads(s)).toEqual([key]);
    expect((await status(s, key)).status).toBe(400);
  });

  test("on needs async admission and a valid flag value: startup refuses, naming the variable only", async () => {
    await expect(start({ ...UPLOAD_ON, SELFHOST_INGEST_ADMISSION: undefined })).rejects.toThrow(/HISTORY_UPLOAD_DIRECT needs SELFHOST_INGEST_ADMISSION/);
    await expect(start({ HISTORY_UPLOAD_DIRECT: "yes-please" })).rejects.toThrow("HISTORY_UPLOAD_DIRECT must be off, allowlist or on");
  });
});
