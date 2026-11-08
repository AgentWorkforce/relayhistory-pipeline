/**
 * Self-host async admission (SELFHOST_INGEST_ADMISSION, selfhost-admission.ts) end to end over real
 * loopback node:http: a stub relaycontrol introspection server, the ledger in PGlite with every
 * Supabase migration and the ingest role (canonical acceptance, as the self-host ingest runs), and a
 * scratch fs object store (create-only and compare-and-swap honoured, as MinIO / S3 / GCS do).
 *
 * The SQL runner is counted: the request path must never call it in async mode.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { rawZstdFrame } from "@rhc/lib/pipeline/raw-chunks.ts";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { ADMISSION_HEADER, ADMISSION_QUEUED, INBOX_MIN_BYTES, beginPrivacyControl, settlePrivacyControl } from "../async-admission.ts";
import { sqlLedger, type SqlRun } from "../ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "../selfhost-auth.ts";
import { DEAD_LETTER_RETENTION_MS, deadLetterKey, type SelfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { selfhostIngestRuntime, startSelfhostIngestService, type RuntimeBuilder, type SelfhostIngestService } from "../selfhost-server.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../turns.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION } from "../vendor/delivery/delivery-contracts.ts";
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

describe("SELFHOST_INGEST_ADMISSION=sync (the default) is today's server", () => {
  test("unset and explicit sync answer the same bytes, write synchronously, build no consumer and no inbox", async () => {
    const answers: { status: number; body: string; headers: string[]; ready: string; inbox: number; tenant: number; consumer: boolean }[] = [];
    for (const mode of [undefined, "sync", ""]) {
      const s = await start(mode === undefined ? { SELFHOST_INGEST_ADMISSION: undefined } : { SELFHOST_INGEST_ADMISSION: mode });
      const res = await post(s.url("/v1/delivery/batches"), await deliveryBody());
      const headers = [...res.headers.keys()].filter((h) => !["date", "x-request-id", "x-correlation-id"].includes(h)).sort();
      const ready = await (await fetch(s.url("/readyz"))).text();
      let consumer = true;
      try { s.consumer(); } catch { consumer = false; }
      answers.push({ status: res.status, body: await res.text(), headers, ready, inbox: (await keys(s.store, "96bulls/.admission-inbox")).length, tenant: (await tenantKeys(s.store)).length, consumer });
      // The synchronous path ran SQL in the request (claim, slot, commit).
      expect(s.sql.calls).toBeGreaterThan(0);
    }
    expect(answers[0]!.status).toBe(200);
    expect(answers[0]!.headers).not.toContain(ADMISSION_HEADER);
    expect(answers[0]!.ready).toBe(JSON.stringify({ ok: true, enabled: true }));
    expect(answers[0]!.inbox).toBe(0);
    expect(answers[0]!.tenant).toBeGreaterThan(0);
    expect(answers[0]!.consumer).toBe(false);
    expect(answers[1]).toEqual(answers[0]!);
    expect(answers[2]).toEqual(answers[0]!);
  });

  test("an unknown mode refuses startup, naming the variable only", async () => {
    await expect(start({ SELFHOST_INGEST_ADMISSION: "ASYNC" })).rejects.toThrow("SELFHOST_INGEST_ADMISSION must be sync, async or drain");
  });
});

describe("async request path: no SQL, one create-only inbox PUT, the hosted queued receipt", () => {
  test("delivery and turns batches are ACKed with zero SQL calls; nothing is admitted until the consumer runs", async () => {
    const s = await start();
    const secret = "export AWS_SECRET_ACCESS_KEY=abcdEFGHijklMNOPqrstUVWXyz0123456789abcd";
    const payload = { payload: { text: secret, ts_ms: 1_761_955_200_000 } };
    const sync = await (async () => {
      // The same batch through the synchronous path of a separate default server: the receipt bodies must match.
      const other = await start({ SELFHOST_INGEST_ADMISSION: undefined });
      return (await post(other.url("/v1/delivery/batches"), await deliveryBody("e1", {}, payload))).text();
    })();
    const res = await post(s.url("/v1/delivery/batches"), await deliveryBody("e1", {}, payload));
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_QUEUED);
    expect(res.headers.get("x-relay-custody-ack")).toBeNull();
    const body = await res.text();
    expect(body).toBe(sync);
    expect(JSON.parse(body).receiptId).toMatch(/^rhr_[0-9a-f]{64}$/);
    expect(s.sql.calls).toBe(0);
    const inbox = await inboxObjects(s.store);
    expect(inbox).toHaveLength(1);
    expect(inbox[0]!.size).toBeGreaterThanOrEqual(INBOX_MIN_BYTES);
    expect(await tenantKeys(s.store)).toEqual([]);
    // Re-scrubbed at rest, exactly as the synchronous path stages: the planted secret is not in the inbox.
    const raw = await readAll((await s.store.getObject(objectKey(inbox[0]!.key))).body);
    expect(new TextDecoder().decode(raw)).not.toContain("abcdEFGHijklMNOPqrstUVWXyz0123456789abcd");

    const status = await s.consumer().status();
    expect(status.backlog).toBe(1); // notified by the request path
    await drain(s);
    expect(s.sql.calls).toBeGreaterThan(0);
    expect((await tenantKeys(s.store)).length).toBeGreaterThan(0);
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
    // Retired: the inbox now holds a tombstone, which no scan picks up again.
    expect((await inboxObjects(s.store))[0]!.size).toBeLessThan(INBOX_MIN_BYTES);
    expect(s.consumer().status().backlog).toBe(0);
    const calls = s.sql.calls;
    await drain(s);
    expect(s.sql.calls).toBe(calls);
  });

  test("an inbox PUT failure is a 503 with nothing acknowledged and no synchronous fallback", async () => {
    const s = await start();
    s.faults.put = (k) => k.includes("/.admission-inbox/") && k.endsWith(".ndjson.zst");
    const res = await post(s.url("/v1/delivery/batches"), await deliveryBody());
    expect(res.status).toBe(503);
    expect(await errorCode(res)).toBe("delivery_unavailable");
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    expect(s.sql.calls).toBe(0);
    expect(await inboxObjects(s.store)).toEqual([]);
    expect(await tenantKeys(s.store)).toEqual([]);
  });

  test("a journal witness failure is a 503, never the synchronous path", async () => {
    const s = await start();
    s.faults.get = (k) => k.endsWith("/privacy-journal.json");
    const res = await post(s.url("/v1/delivery/batches"), await deliveryBody());
    expect([res.status, await errorCode(res)]).toEqual([503, "delivery_unavailable"]);
    expect(s.sql.calls).toBe(0);
    expect(await inboxObjects(s.store)).toEqual([]);
  });

  test("validation answers keep their synchronous status, code and body, and write nothing", async () => {
    const s = await start();
    const sync = await start({ SELFHOST_INGEST_ADMISSION: undefined });
    const bodies = ["{}", "not json", JSON.stringify({ protocolVersion: 1, batch: { records: [] } }), await deliveryBody("x", { account_id: "acct-of-someone-else" })];
    const answer = async (r: Response) => { const j = (await r.json()) as { error: unknown }; return [r.status, j.error]; };
    for (const body of bodies) {
      const [a, b] = await Promise.all([post(s.url("/v1/delivery/batches"), body), post(sync.url("/v1/delivery/batches"), body)]);
      const [ax, bx] = [await answer(a), await answer(b)];
      expect(ax).toEqual(bx);
      expect(ax[0] as number).toBeGreaterThanOrEqual(400);
    }
    expect(s.sql.calls).toBe(0);
    expect(await inboxObjects(s.store)).toEqual([]);
  });

  test("turns batches take the same queued path, with the synchronous receipt body", async () => {
    const SESSION = "7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a5b";
    const turn = (index: number) => {
      const payload = { id: `${String(index).padStart(8, "0")}-aaaa-4bbb-8ccc-dddddddddddd`, org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, source: "claude", session_id: SESSION, turn_index: index,
        session_owner: TENANT.userId, role: "user", content: `turn ${index}`, actor_name: "Ada", actor_role: "owner", metadata_json: JSON.stringify({ nativeCli: "claude", relaySession: { sessionId: SESSION } }), ts: "2026-11-05T12:00:00.123Z", tagged: true };
      const record_id = turnRecordId(TENANT.workspaceId, "claude", SESSION, index);
      const revision = 1_793_000_000_000_000 + index;
      return { schema_version: 1, origin_id: TURN_ORIGIN_ID, record_id, revision_id: `turnrev:${record_id.slice(5)}:${revision}`, revision, kind: TURN_KIND, source: "claude", session_id: SESSION, operation: "upsert", payload };
    };
    const body = JSON.stringify({ protocolVersion: 1, batch: { schema_version: 1, origin_id: TURN_ORIGIN_ID, batch_id: "turns-1", job_id: "relayhistory-turns", generation: 1, destination_id: "relayhistory",
      instance_id: "relayhistory-api", account_id: await deliveryAccount(TENANT), mapping_version: DELIVERY_MAPPING_VERSION, records: [turn(0), turn(1)] } });
    const sync = await start({ SELFHOST_INGEST_ADMISSION: undefined });
    const syncRes = await post(sync.url("/v1/turns/batches"), body);
    expect(syncRes.status).toBe(200);
    const s = await start();
    const res = await post(s.url("/v1/turns/batches"), body);
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBe(ADMISSION_QUEUED);
    expect(await res.text()).toBe(await syncRes.text());
    expect(s.sql.calls).toBe(0);
    await drain(s);
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
  });

  test("the opt-in is config only: a forged identity header with admission changes nothing, and selection / reset stay 501", async () => {
    const s = await start({ SELFHOST_INGEST_ADMISSION: undefined });
    const forged = Buffer.from(JSON.stringify({ ...TENANT, scopes: [SYNC_SCOPE], admission: "queued-v1" })).toString("base64url");
    const res = await post(s.url("/v1/delivery/batches"), await deliveryBody(), { "x-relay-ingest-identity": forged });
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    const a = await start();
    for (const path of ["/v1/delivery/sessions/reset", "/v1/delivery/sessions/selection", "/v1/delivery/admission"]) {
      const r = await post(a.url(path), "{}");
      expect([path, r.status]).toEqual([path, 501]);
    }
  });
});

describe("the consumer", () => {
  test("a duplicate inbox object (a Desktop retry of the same batch) admits exactly once: the second is a claim replay", async () => {
    const s = await start({ SELFHOST_ADMISSION_CONCURRENCY: "1" });
    const body = await deliveryBody("dup");
    expect((await post(s.url("/v1/delivery/batches"), body)).status).toBe(200);
    expect((await post(s.url("/v1/delivery/batches"), body)).status).toBe(200);
    expect(await inboxObjects(s.store)).toHaveLength(2);
    await drain(s);
    const admitted = s.logs.map((l) => JSON.parse(l)).filter((l) => l.route === "ingest.admission" && l.outcome === "admitted").map((l) => l.error_code).sort();
    expect(admitted).toEqual(["accepted", "replay"]);
    // One receipt directory, one admitted outcome.
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
    // The same key re-put with the same bytes is an idempotent success; other bytes are refused (create-only).
    const [one] = await inboxObjects(s.store);
    const bytes = await readAll((await s.store.getObject(objectKey(one!.key))).body);
    expect((await s.store.put(objectKey(one!.key), bytes, { ifNoneMatch: true })).created).toBe(false);
    await expect(s.store.put(objectKey(one!.key), new Uint8Array(300), { ifNoneMatch: true })).rejects.toMatchObject({ code: "already_exists" });
  });

  test("a crash between claim and commit is resumed by the next process's scan; a crash before the outcome marker replays", async () => {
    const a = await start();
    expect((await post(a.url("/v1/delivery/batches"), await deliveryBody("crash"))).status).toBe(200);
    // Process A dies mid-batch: the claim is taken, the commit never lands.
    a.sql.fail = (t) => t.includes("ingest_commit_batch");
    await drain(a);
    expect(await outcomes(a.store)).toEqual([]);
    expect((await inboxObjects(a.store))[0]!.size).toBeGreaterThanOrEqual(INBOX_MIN_BYTES);
    await a.service.close();
    a.sql.fail = undefined;
    // Process B (a restart, or another replica) over the same database and store: the scan finds the object.
    const b = await start({}, { run: a.run, store: a.store });
    // ...and dies once more after the commit but before the outcome marker.
    let once = true;
    b.faults.put = (k) => k.endsWith("outcome-admitted.json") && once && !(once = false);
    await drain(b);
    expect(await outcomes(b.store)).toEqual([]);
    b.tick(60_000);
    await drain(b);
    expect(await outcomes(b.store)).toEqual(["outcome-admitted.json"]);
    const outcomesLogged = b.logs.map((l) => JSON.parse(l)).filter((l) => l.route === "ingest.admission").map((l) => `${l.outcome}:${l.error_code}`);
    expect(outcomesLogged).toEqual(["retry:outcome_unavailable", "admitted:replay"]);
    expect((await tenantKeys(b.store)).length).toBeGreaterThan(0);
  });

  test("bounded concurrency: never more objects in flight than SELFHOST_ADMISSION_CONCURRENCY", async () => {
    const s = await start({ SELFHOST_ADMISSION_CONCURRENCY: "2" });
    for (let i = 0; i < 5; i++) expect((await post(s.url("/v1/delivery/batches"), await deliveryBody(`c${i}`, {}, { session_id: `sess-c${i}` }))).status).toBe(200);
    let peak = 0;
    const timer = setInterval(() => { peak = Math.max(peak, s.consumer().status().inFlight); }, 1);
    await drain(s);
    clearInterval(timer);
    expect(peak).toBeLessThanOrEqual(2);
    expect((await outcomes(s.store)).length).toBe(5);
  });

  test("poison objects walk to the dead-letter prefix after the attempt budget, are skipped for 14 days, then redriven", async () => {
    const s = await start(); // SELFHOST_ADMISSION_MAX_ATTEMPTS=3
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("poison"))).status).toBe(200);
    const [inbox] = await inboxObjects(s.store);
    // Corrupt the acknowledged object (an operator problem): admission_invalid, retried, never acked.
    await s.store.put(objectKey(inbox!.key), rawZstdFrame(new TextEncoder().encode(JSON.stringify({ v: 1, kind: "not-an-inbox", pad: "x".repeat(400) }) + "\n")));
    for (let i = 0; i < 3; i++) { await drain(s); s.tick(301_000); }
    const dead = deadLetterKey("96bulls", inbox!.key);
    const marker = JSON.parse(new TextDecoder().decode(await readAll((await s.store.getObject(objectKey(dead))).body)));
    expect(marker).toMatchObject({ v: 1, kind: "ingest-admission-dead-letter", code: "admission_invalid", attempts: 3 });
    expect(JSON.stringify(marker)).not.toContain("org-a");
    await drain(s);
    expect(s.consumer().status()).toMatchObject({ backlog: 0, deadLetters: 1 });
    // The bytes are never deleted by the consumer.
    expect((await inboxObjects(s.store))[0]!.size).toBeGreaterThanOrEqual(INBOX_MIN_BYTES);
    const ready = await (await fetch(s.url("/readyz"))).json() as { admission: { deadLetters: number } };
    expect(ready.admission.deadLetters).toBe(1);
    const attempts = () => s.logs.map((l) => JSON.parse(l)).filter((l) => l.route === "ingest.admission").length;
    const before = attempts();
    await drain(s);
    expect(attempts()).toBe(before);
    // Past the 14-day retention the marker has expired and the object is attempted again (every check re-runs).
    s.tick(DEAD_LETTER_RETENTION_MS + 1);
    await drain(s);
    expect(attempts()).toBe(before + 1);
    expect(s.consumer().status().deadLetters).toBe(0);
  });

  test("an operator redrive (deleting the marker) re-admits a dead-lettered upload once its cause is fixed", async () => {
    const s = await start();
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("db-down"))).status).toBe(200);
    s.sql.fail = () => true; // the database is down for the whole budget
    for (let i = 0; i < 3; i++) { await drain(s); s.tick(301_000); }
    const [inbox] = await inboxObjects(s.store);
    await s.store.head(objectKey(deadLetterKey("96bulls", inbox!.key))).then((m) => expect(m).toBeDefined());
    s.sql.fail = undefined;
    await s.store.delete(objectKey(deadLetterKey("96bulls", inbox!.key)));
    await drain(s);
    expect(await outcomes(s.store)).toEqual(["outcome-admitted.json"]);
  });
});

describe("privacy fences: a control that lands before the consumer admits", () => {
  const ctl = (n: number) => ({ epoch: "selfhost-test", version: String(n) });

  test("a disable between upload and admission: the claim's deletion filter stores nothing, exactly as a synchronous upload after the disable", async () => {
    const s = await start();
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("del"))).status).toBe(200);
    await sqlLedger(s.run, { acceptanceMode: "canonical" }).setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: false });
    await drain(s);
    expect(await tenantKeys(s.store)).toEqual([]);
    const line = s.logs.map((l) => JSON.parse(l)).find((l) => l.route === "ingest.admission");
    // The claim acknowledges a deleted session's records without storing them (SQL answers replay_fast when nothing is left to write).
    expect(line.outcome).toBe("admitted");
    expect(["replay_fast", "accepted_all_deleted"]).toContain(line.error_code);
    // The synchronous comparison: a default server over the same ledger refuses to store it too.
    const sync = await start({ SELFHOST_INGEST_ADMISSION: undefined }, { run: s.run, store: s.store });
    expect((await post(sync.url("/v1/delivery/batches"), await deliveryBody("del2"))).status).toBe(200);
    expect(await tenantKeys(s.store)).toEqual([]);
  });

  test("a reset between upload and admission refuses it (delivery_privacy_superseded); an upload after the reset is admitted", async () => {
    const s = await start();
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("pre"))).status).toBe(200);
    // Any self-host reset path while async is on brackets the journal (begin -> SQL -> settle).
    const begun = await beginPrivacyControl(s.store, "96bulls", TENANT, ctl(1), s.now());
    await sqlLedger(s.run, { acceptanceMode: "canonical" }).resetUploadedSessions(TENANT, "reset-1");
    await settlePrivacyControl(s.store, "96bulls", TENANT, ctl(1), begun, "applied");
    s.tick(1000);
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("post", {}, { session_id: "sess-2" }))).status).toBe(200);
    await drain(s);
    const lines = s.logs.map((l) => JSON.parse(l)).filter((l) => l.route === "ingest.admission").map((l) => `${l.outcome}:${l.error_code}`).sort();
    expect(lines).toEqual(["admitted:accepted", "refused:delivery_privacy_superseded"]);
    expect((await outcomes(s.store)).sort()).toEqual(["outcome-admitted.json", "outcome-refused.json"]);
    // Only the post-reset session reached the tenant prefix.
    const stored = await tenantKeys(s.store);
    expect(stored.length).toBeGreaterThan(0);
    // The refused upload's content is retired too (tombstone), and never retried.
    expect((await inboxObjects(s.store)).every((o) => o.size < INBOX_MIN_BYTES)).toBe(true);
  });

  test("a re-enable (after a disable) between upload and admission refuses the pre-control upload", async () => {
    const s = await start();
    const ledger = sqlLedger(s.run, { acceptanceMode: "canonical" });
    await ledger.setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: false });
    const calls = s.sql.calls;
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("re"))).status).toBe(200);
    expect(s.sql.calls).toBe(calls);
    const begun = await beginPrivacyControl(s.store, "96bulls", TENANT, ctl(1), s.now());
    await ledger.setSessionUploads({ tenant: TENANT, originId: "origin-1", sessions: [["claude", "sess-1"]], enabled: true });
    await settlePrivacyControl(s.store, "96bulls", TENANT, ctl(1), begun, "applied");
    await drain(s);
    const line = s.logs.map((l) => JSON.parse(l)).find((l) => l.route === "ingest.admission");
    expect([line.outcome, line.error_code]).toEqual(["refused", "delivery_privacy_superseded"]);
    expect(await tenantKeys(s.store)).toEqual([]);
  });

  test("a control still in flight holds the admission (retry), never admits across it", async () => {
    const s = await start();
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("hold"))).status).toBe(200);
    await beginPrivacyControl(s.store, "96bulls", TENANT, ctl(1), s.now());
    await drain(s);
    const line = s.logs.map((l) => JSON.parse(l)).find((l) => l.route === "ingest.admission");
    expect([line.outcome, line.error_code]).toEqual(["retry", "privacy_control_pending"]);
    expect(await tenantKeys(s.store)).toEqual([]);
  });

  test("the app's own control routes stay fail-closed in self-host async mode: a non-ordered reset is refused before SQL", async () => {
    const { run } = await database();
    const fs = scratchStore();
    cleanups.push(fs.done);
    let calls = 0;
    const counted: SqlRun = (t, p) => { calls++; return run(t, p); };
    const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
    const app = createIngestApp({
      identify: () => identity,
      deliver: { ledger: sqlLedger(counted, { acceptanceMode: "canonical" }), acceptanceMode: "canonical", target: { store: fs.store, keyRoot: "96bulls" } },
      log: () => {},
      asyncAdmission: { mode: "on", enqueue: async () => {}, selfhost: { identityOf: () => identity } },
    });
    const res = await app.request("/v1/delivery/sessions/reset", { method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ account_id: await deliveryAccount(TENANT), request_id: "r1", confirmation: "DELETE ALL SESSIONS" }) });
    expect(res.status).toBe(503);
    expect(calls).toBe(0);
  });
});

describe("readiness and telemetry", () => {
  test("/readyz follows the consumer: starting until its first scan, ready after, not ready once scans go stale", async () => {
    const s = await start();
    const first = await fetch(s.url("/readyz"));
    expect(first.status).toBe(503);
    expect(((await first.json()) as { admission: { state: string } }).admission.state).toBe("starting");
    await drain(s);
    const ok = await fetch(s.url("/readyz"));
    expect(ok.status).toBe(200);
    expect(await ok.json()).toEqual({ ok: true, enabled: true, admission: { state: "ok", backlog: 0, oldestAgeMs: null, deadLetters: 0, inFlight: 0, lastScanAgeMs: 0 } });
    // The store stops answering lists: uploads still work (they are not gated on the DB), readiness goes stale.
    s.faults.list = () => true;
    s.tick(61_000);
    await drain(s);
    const stale = await fetch(s.url("/readyz"));
    expect(stale.status).toBe(503);
    expect(((await stale.json()) as { admission: { state: string } }).admission.state).toBe("stale");
    expect(s.logs.some((l) => JSON.parse(l).outcome === "scan_failed")).toBe(true);
  });

  test("a database outage does not make an async server unready, and shows as backlog with its oldest age", async () => {
    const s = await start();
    s.sql.fail = () => true;
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("lag"))).status).toBe(200);
    s.tick(5_000);
    await drain(s);
    const r = await fetch(s.url("/readyz"));
    expect(r.status).toBe(200);
    const body = (await r.json()) as { admission: { backlog: number; oldestAgeMs: number } };
    expect(body.admission.backlog).toBe(1);
    expect(body.admission.oldestAgeMs).toBeGreaterThanOrEqual(5_000);
    const scanLine = s.logs.map((l) => JSON.parse(l)).filter((l) => l.route === "ingest.admission_consumer").at(-1);
    expect(scanLine).toMatchObject({ outcome: "ok", admission_backlog: 1, admission_dead_letters: 0 });
  });

  test("no log line carries a key, a body, the token or a header value", async () => {
    const s = await start();
    expect((await post(s.url("/v1/delivery/batches"), await deliveryBody("quiet"), { "x-correlation-id": "corr-quiet" })).status).toBe(200);
    s.sql.fail = (t) => t.includes("ingest_commit_batch");
    await drain(s);
    s.sql.fail = undefined;
    s.tick(301_000);
    await drain(s);
    const text = s.logs.join("\n");
    for (const needle of [TOKEN, SECRET, ".admission-inbox", "96bulls/", "hello", "sess-1", "quiet"]) expect([needle, text.includes(needle)]).toEqual([needle, false]);
  });

  test("drain: the synchronous request path, while the consumer finishes the inbox", async () => {
    const a = await start();
    expect((await post(a.url("/v1/delivery/batches"), await deliveryBody("inflight"))).status).toBe(200);
    await a.service.close();
    const d = await start({ SELFHOST_INGEST_ADMISSION: "drain" }, { run: a.run, store: a.store });
    const res = await post(d.url("/v1/delivery/batches"), await deliveryBody("now-sync", {}, { session_id: "sess-sync" }));
    expect(res.status).toBe(200);
    expect(res.headers.get(ADMISSION_HEADER)).toBeNull();
    expect(d.sql.calls).toBeGreaterThan(0);
    await drain(d);
    expect(await outcomes(d.store)).toEqual(["outcome-admitted.json"]);
  });
});
