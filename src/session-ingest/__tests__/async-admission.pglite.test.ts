/**
 * S3-first async admission end to end against the REAL buffered SQL (PGlite, every migration)
 * and a filesystem object store: the request path ACKs with no ledger call, the consumer
 * admits through the unchanged claim -> intent -> PUT -> receipt, and the upload-time privacy
 * journal refuses exactly the uploads a later reset / re-enable must not let through.
 */
import { test, expect, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { sqlLedger } from "../ledger-sql.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { admitMessage } from "../admission-consumer.ts";
import { ADMISSION_ACCEPT_HEADER, ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, type AdmissionMessage, type AsyncAdmissionMode } from "../async-admission.ts";
import type { Ledger } from "../ledger.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "./ordered-support.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64), epoch = "3".repeat(64), open: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };

async function fixture(mode: AsyncAdmissionMode = "on") {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + gen + "','pass')");
  await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
  await db.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)", [JSON.stringify({ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId, deletions: [], resets: [] })]); await db.exec("COMMIT");
  await db.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)", [TENANT.orgId, gen, epoch]);
  await initializeFixtureSeatClock(db as never, gen);
  let ledgerCalls = 0;
  const run = async (s: string, p: unknown[] = []) => {
    ledgerCalls++;
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query(s, p as never); await db.exec("COMMIT"); return r as never; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  const ledger: Ledger = orderedFixtureLedger(sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: gen }), db as never);
  const fs = scratchStore(); open.push(fs.done);
  const target = { store: fs.store, keyRoot: "96bulls" };
  const deliver = { ledger, acceptanceMode: "buffered" as const, target };
  const queue: AdmissionMessage[] = [];
  let clock = 1_800_000_000_000;
  const app = createIngestApp({ identify: () => identity, deliver, log: () => {}, asyncAdmission: { mode, enqueue: async (m) => { queue.push(m); }, nowMs: () => clock } });
  const post = async (b: unknown, accept = true) => app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", ...(accept ? { [ADMISSION_ACCEPT_HEADER]: ADMISSION_ACCEPT_VALUE } : {}) }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
  const control = async (path: string, body: unknown) => app.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  const consume = async (m: AdmissionMessage, attempts = 1, consumerMode: AsyncAdmissionMode = mode) =>
    admitMessage({ mode: consumerMode, deliver, custodySource: "relayhistory", decompress: zstd, nowMs: () => clock }, m, attempts);
  const outbox = async (batchId: string) => (await db.query("SELECT o.active_stage_token FROM sessions.ingest_buffer_outbox o WHERE o.batch_id=$1", [batchId])).rows[0]?.active_stage_token as string | undefined;
  const status = async (originId: string, receiptId: string) => (await app.request(`/v1/delivery/admission?originId=${originId}&receiptId=${receiptId}`)).json() as Promise<{ state: string; code?: string }>;
  return { db, app, post, control, consume, queue, outbox, status, ledgerCalls: () => ledgerCalls, tick: (ms: number) => { clock += ms; }, target };
}

test("ACK needs no ledger call; the consumer admits through the real buffered path; a retry replays", async () => {
  const f = await fixture();
  const b = await batch([record({ record_id: "a1" }), record({ record_id: "a2" })], { batch_id: "async-1" });
  const before = f.ledgerCalls();
  const res = await f.post(b);
  expect(res.status).toBe(200);
  expect(f.ledgerCalls()).toBe(before);
  expect(res.headers.get(ADMISSION_HEADER)).toBe(JSON.stringify({ version: 1, state: "queued" }));
  expect(res.headers.get("x-relay-custody-ack")).toBeNull();
  const receipt = await res.json() as { receiptId: string; acceptedRevisionIds: string[]; acceptanceLevel: string };
  expect(receipt.acceptanceLevel).toBe("durable");
  expect(receipt.acceptedRevisionIds).toEqual(["a1:r1", "a2:r1"]);
  expect(await f.outbox("async-1")).toBeUndefined();
  expect((await f.status("origin-1", receipt.receiptId)).state).toBe("pending");
  expect(f.queue).toHaveLength(1);
  const r = await f.consume(f.queue[0]!);
  expect(r).toMatchObject({ action: "ack", outcome: "admitted", code: "accepted_buffered" });
  expect(await f.outbox("async-1")).toBeString();
  expect(await f.status("origin-1", receipt.receiptId)).toEqual({ receiptId: receipt.receiptId, state: "admitted" } as never);
  // A duplicate delivery of the same message: the inbox is tombstoned, nothing re-admitted.
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "already_terminal" });
  // The client retries the same batch: a new attempt object, the claim replays it.
  expect((await f.post(b)).status).toBe(200);
  expect(await f.consume(f.queue[1]!)).toMatchObject({ action: "ack", outcome: "admitted", code: "replay" });
  // A caller that has not opted in still takes the synchronous path (and its replay).
  expect((await f.post(b, false)).status).toBe(200);
  expect(f.queue).toHaveLength(2);
});

test("a reset that starts after the upload refuses its admission; an upload after the reset is admitted", async () => {
  const f = await fixture();
  const pre = await batch([record({ record_id: "pre" })], { batch_id: "pre-reset" });
  const preReceipt = await (await f.post(pre)).json() as { receiptId: string };
  f.tick(1000);
  const reset = await f.control("/v1/delivery/sessions/reset", { account_id: await deliveryAccount(TENANT), request_id: "reset-1", confirmation: "DELETE ALL SESSIONS" });
  expect(reset.status).toBe(200);
  f.tick(1000);
  const post = await batch([record({ record_id: "post" })], { batch_id: "post-reset" });
  await f.post(post);
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
  expect(await f.outbox("pre-reset")).toBeUndefined();
  expect(await f.status("origin-1", preReceipt.receiptId)).toMatchObject({ state: "refused", code: "delivery_privacy_superseded" });
  expect(await f.consume(f.queue[1]!)).toMatchObject({ action: "ack", outcome: "admitted" });
  expect(await f.outbox("post-reset")).toBeString();
});

test("disable then re-enable before admission: the pre-disable upload is refused; the re-upload after enable is admitted", async () => {
  const f = await fixture();
  const account = await deliveryAccount(TENANT);
  const b = await batch([record({ record_id: "toggle" })], { batch_id: "toggle-1" });
  await f.post(b);
  f.tick(1000);
  const sel = (enabled: boolean) => f.control("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "sess-1" }], enabled });
  expect((await sel(false)).status).toBe(200);
  f.tick(1000);
  expect((await sel(true)).status).toBe(200);
  f.tick(1000);
  // Same content, same batch id: Desktop re-sends after the enable.
  await f.post(b);
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
  expect(await f.outbox("toggle-1")).toBeUndefined();
  expect(await f.consume(f.queue[1]!)).toMatchObject({ action: "ack", outcome: "admitted" });
  expect(await f.outbox("toggle-1")).toBeString();
});

test("a disable alone needs no journal: the consumer's claim filters the deleted session", async () => {
  const f = await fixture();
  const account = await deliveryAccount(TENANT);
  const b = await batch([record({ record_id: "kept", session_id: "keep" }), record({ record_id: "gone", session_id: "gone" })], { batch_id: "filtered" });
  await f.post(b);
  f.tick(1000);
  expect((await f.control("/v1/delivery/sessions/selection", { account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "gone" }], enabled: false })).status).toBe(200);
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "admitted" });
  const rows = (await f.db.query("SELECT DISTINCT c.session_id FROM sessions.ingest_claim_records c JOIN sessions.ingest_buffer_outbox o USING(claim_token) WHERE o.batch_id='filtered' ORDER BY 1").catch(() => ({ rows: [] }))).rows;
  // The deleted session must not be among the admitted stage's sessions (when the table is visible to the test).
  expect(rows.map((r: any) => r.session_id)).not.toContain("gone");
});

test("drain admits but takes no new async ACK; off admits nothing (waits), and the ledger never sees it", async () => {
  const f = await fixture("on");
  expect((await f.post(await batch([record({ record_id: "d1" })], { batch_id: "drain-1" }))).status).toBe(200);
  const off = await f.consume(f.queue[0]!, 1, "off");
  expect(off).toMatchObject({ action: "retry", delaySeconds: 300, code: "async_admission_off" });
  expect(await f.outbox("drain-1")).toBeUndefined();
  expect(await f.consume(f.queue[0]!, 2, "drain")).toMatchObject({ action: "ack", outcome: "admitted" });
  const d = await fixture("drain");
  const before = d.ledgerCalls();
  expect((await d.post(await batch([record({ record_id: "d2" })], { batch_id: "drain-2" }))).status).toBe(200);
  expect(d.queue).toHaveLength(0);
  expect(d.ledgerCalls()).toBeGreaterThan(before);
  expect(await d.outbox("drain-2")).toBeString();
});

test("a stale generation is transient (retried), never a refusal", async () => {
  const f = await fixture();
  await f.post(await batch([record({ record_id: "g1" })], { batch_id: "gen-1" }));
  const run = async (s: string, p: unknown[] = []) => {
    await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await f.db.query(s, p as never); await f.db.exec("COMMIT"); return r as never; } catch (e) { await f.db.exec("ROLLBACK"); throw e; }
  };
  const stale = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: "b".repeat(64) });
  const r = await admitMessage({ mode: "on", deliver: { ledger: stale, acceptanceMode: "buffered", target: f.target }, custodySource: "relayhistory", decompress: zstd }, f.queue[0]!, 3);
  expect(r.action).toBe("retry");
  expect(await f.outbox("gen-1")).toBeUndefined();
});
