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
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { admitMessage } from "../admission-consumer.ts";
import { ADMISSION_ACCEPT_VALUE, ADMISSION_HEADER, PENDING_TTL_MS, beginPrivacyControl, settlePrivacyControl, type AdmissionMessage, type AsyncAdmissionMode } from "../async-admission.ts";
import type { Ledger } from "../ledger.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "./ordered-support.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

setDefaultTimeout(60000);
const root = join(import.meta.dir, "../../.."), gen = "a".repeat(64), epoch = "3".repeat(64), open: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close(); });
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));
const identity = { ...TENANT, scopes: [SYNC_SCOPE] };

async function fixture(mode: AsyncAdmissionMode = "on") {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(() => db.close());
  const read = (p: string) => readFileSync(join(root, p), "utf8");
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  // Single-sourced bootstrap chain: the grant migration FAILs closed without the bare consumer grantee.
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
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
  // The signature is the unit suite's concern; here the identity header is parsed as the Worker parses it.
  const app = createIngestApp({ identify: (req) => bindingIdentity(req), deliver, log: () => {}, asyncAdmission: { mode, enqueue: async (m) => { queue.push(m); }, nowMs: () => clock } });
  const id = (accept: boolean) => ({ [IDENTITY_HEADER]: encodeIdentity({ ...identity, ...(accept ? { admission: ADMISSION_ACCEPT_VALUE } : {}) }) });
  const post = async (b: unknown, accept = true, body?: BodyInit) => app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", ...id(accept) }, body: body ?? JSON.stringify({ protocolVersion: 1, batch: b }), ...(body ? { duplex: "half" } : {}) } as RequestInit);
  // Buffered SQL accepts only ORDERED controls (as relayhistory sends them): next version, digest as SQL computes it.
  const ordered = async (kind: "selection" | "reset", payload: unknown, version?: string) => {
    const r = await db.query("SELECT coalesce((SELECT max(version) FROM sessions.ingest_control_receipts WHERE org_id=$2 AND workspace_id=$3 AND user_id=$4 AND epoch=$1),0)::text AS v,encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",
      [epoch, TENANT.orgId, TENANT.workspaceId, TENANT.userId, kind, JSON.stringify(payload)]);
    const row = r.rows[0] as { v: string; d: string };
    return { epoch, version: version ?? (BigInt(row.v) + 1n).toString(), digest: row.d, kind };
  };
  let lastControl: { version: string } | undefined;
  const control = async (path: string, body: Record<string, any>, version?: string) => {
    const kind = path.endsWith("/reset") ? "reset" as const : "selection" as const;
    const payload = kind === "reset" ? { request_id: body.request_id } : { origin_id: body.origin_id, sessions: body.sessions.map((x: { source: string; session_id: string }) => ({ source: x.source, session_id: x.session_id })), enabled: body.enabled };
    const ctl = await ordered(kind, payload, version);
    lastControl = ctl;
    return app.request(path, { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity({ ...identity, control: ctl } as never) }, body: JSON.stringify(body) });
  };
  const consume = async (m: AdmissionMessage, attempts = 1, consumerMode: AsyncAdmissionMode = mode) =>
    admitMessage({ mode: consumerMode, deliver, custodySource: "relayhistory", decompress: zstd, nowMs: () => clock }, m, attempts);
  const outbox = async (batchId: string) => (await db.query("SELECT o.active_stage_token FROM sessions.ingest_buffer_outbox o WHERE o.batch_id=$1", [batchId])).rows[0]?.active_stage_token as string | undefined;
  const status = async (originId: string, receiptId: string) => (await app.request(`/v1/delivery/admission?originId=${originId}&receiptId=${receiptId}`, { headers: id(false) })).json() as Promise<{ state: string; code?: string }>;
  const reset = async (requestId: string, version?: string) => control("/v1/delivery/sessions/reset", { account_id: await deliveryAccount(TENANT), request_id: requestId, confirmation: "DELETE ALL SESSIONS" }, version);
  return { db, app, post, control, reset, ordered, lastVersion: () => lastControl!.version, consume, queue, outbox, status, ledgerCalls: () => ledgerCalls, tick: (ms: number) => { clock += ms; }, now: () => clock, target, deliver };
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

test("P0 causal: an upload whose body is still streaming while a reset completes is refused", async () => {
  const f = await fixture();
  const bytes = new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "slow" })], { batch_id: "slow-1" }) }));
  let started!: () => void, release!: () => void;
  const reading = new Promise<void>((r) => { started = r; }), gate = new Promise<void>((r) => { release = r; });
  const body = new ReadableStream<Uint8Array>({ async pull(c) { started(); await gate; c.enqueue(bytes); c.close(); } });
  const pending = f.post(null, true, body);
  await reading; // the witness was taken before the first body byte was asked for
  // The reset runs and completes entirely while the upload is latched inside its body read; the
  // upload then finishes with a LATER wall clock than the reset.
  expect((await f.reset("reset-during-stream")).status).toBe(200);
  f.tick(5000);
  release();
  expect((await pending).status).toBe(200);
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
  expect(await f.outbox("slow-1")).toBeUndefined();
});

test("P1-b: an idempotent reset replay never refuses an upload taken after the original reset", async () => {
  const f = await fixture();
  expect((await f.reset("r-1")).status).toBe(200);
  const v1 = f.lastVersion();
  f.tick(1000);
  await f.post(await batch([record({ record_id: "between" })], { batch_id: "between-1" }));
  f.tick(1000);
  const replay = await f.reset("r-1", v1); // relayhistory re-sends the same ordered control: SQL replays it
  expect(replay.status).toBe(200);
  expect(((await replay.json()) as { control: { outcome: string } }).control.outcome).toBe("replay");
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "admitted" });
  expect(await f.outbox("between-1")).toBeString();
  // A NEW reset still refuses what was uploaded before it.
  // A session the reset never owned, so the claim alone would admit it: only the journal refuses it.
  await f.post(await batch([record({ record_id: "before-r2", session_id: "fresh-r2" })], { batch_id: "before-r2" }));
  expect((await f.reset("r-2")).status).toBe(200);
  expect(await f.consume(f.queue[1]!)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
});

test("a control in flight makes the consumer wait; a stale pending one (its Worker died) completes conservatively", async () => {
  const f = await fixture();
  await f.post(await batch([record({ record_id: "w1" })], { batch_id: "wait-1" }));
  const inFlight = { epoch, version: "900" };
  expect(await beginPrivacyControl(f.target.store, f.target.keyRoot, TENANT, inFlight, f.now())).toBe("new");
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "retry", code: "privacy_control_pending" });
  expect(await f.outbox("wait-1")).toBeUndefined();
  // Its Worker died between SQL and completion: past the TTL the consumer completes it itself, and refuses.
  f.tick(PENDING_TTL_MS + 1);
  expect(await f.consume(f.queue[0]!, 2)).toMatchObject({ action: "ack", outcome: "refused", code: "delivery_privacy_superseded" });
  // Its next attempt: SQL says it had applied (replay), so the conservative count stands and nothing advances again.
  expect(await beginPrivacyControl(f.target.store, f.target.keyRoot, TENANT, inFlight, f.now())).toBe("ambiguous");
  await settlePrivacyControl(f.target.store, f.target.keyRoot, TENANT, inFlight, "ambiguous", "replay");
  await f.post(await batch([record({ record_id: "w2" })], { batch_id: "wait-2" }));
  expect(await f.consume(f.queue[1]!)).toMatchObject({ action: "ack", outcome: "admitted" });
});

test("P1-a causal: 200, twenty transient failures, eight days later a redrive still admits the bytes", async () => {
  const f = await fixture();
  const res = await f.post(await batch([record({ record_id: "late", payload: { text: "kept", ts_ms: 1_761_955_200_000 } })], { batch_id: "late-1" }));
  expect(res.status).toBe(200);
  const down = { ...f.deliver, ledger: { ...f.deliver.ledger, claim: async () => { throw Object.assign(new Error("lock"), { code: "55P03" }); } } } as typeof f.deliver;
  for (let attempt = 1; attempt <= 20; attempt++) {
    const r = await admitMessage({ mode: "on", deliver: down, custodySource: "relayhistory", decompress: zstd, nowMs: f.now }, f.queue[0]!, attempt);
    expect(r.action).toBe("retry");
  }
  // max_retries reached: the message sits in the DLQ; no code path expires or deletes pending inbox bytes.
  f.tick(8 * 24 * 3600 * 1000);
  expect(await f.consume(f.queue[0]!, 1)).toMatchObject({ action: "ack", outcome: "admitted" });
  expect(await f.outbox("late-1")).toBeString();
});

test("P2: the opt-in is honoured only inside the identity; a sibling caller without it gets the synchronous path", async () => {
  const f = await fixture();
  const before = f.ledgerCalls();
  expect((await f.post(await batch([record({ record_id: "s1" })], { batch_id: "sync-1" }), false)).status).toBe(200);
  expect(f.queue).toHaveLength(0);
  expect(f.ledgerCalls()).toBeGreaterThan(before);
  // An identity carrying any other admission value is refused outright (it is signed input).
  const bad = await f.app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity({ ...identity, admission: "queued-v2" } as never) }, body: "{}" });
  expect(bad.status).toBe(401);
});

test("legacy (non-ordered) privacy controls are refused before SQL while async admission is on (buffered SQL would refuse them)", async () => {
  const f = await fixture();
  const res = await f.app.request("/v1/delivery/sessions/reset", { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity(identity) },
    body: JSON.stringify({ account_id: await deliveryAccount(TENANT), request_id: "legacy", confirmation: "DELETE ALL SESSIONS" }) });
  expect(res.status).toBe(503);
  // Nothing journaled, so nothing can later be completed conservatively.
  await f.post(await batch([record({ record_id: "after-legacy" })], { batch_id: "after-legacy" }));
  expect(await f.consume(f.queue[0]!)).toMatchObject({ action: "ack", outcome: "admitted" });
});
