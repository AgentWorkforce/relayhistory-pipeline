/**
 * 20261105058000 (foreign-key indexes) and 20261105058010 (operator per-user erase) on PGlite, every migration
 * applied. Real buffered deliveries for two users of one workspace build the production shape (batches, claims,
 * stage intents, outbox, lazy slots, receipt seats, ack decisions, buffered objects in the store); erasing one
 * user removes all of it and leaves the other user unchanged. Nothing here touches a real database or bucket.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../../session-ingest/deliver.ts";
import { sqlLedger } from "../../session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../session-ingest/vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "../../session-ingest/__tests__/support.ts";
import { authorityFixture, epoch, generation, identity } from "../../session-ingest/__tests__/privacy-authority-support.ts";
import { storeEraser } from "../../migration/neon-s3-erasure/eraser.ts";
import { liveMachineId } from "../../migration/neon-s3-erasure/live.ts";
import { objectKey } from "../../shared/vendor.ts";
import { runUserErase, type UserEraseDb } from "../user-erase.ts";

setDefaultTimeout(300_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Tenant = typeof TENANT;

const ALICE: Tenant = TENANT;
const BOB: Tenant = { ...TENANT, userId: "user-2" };
const ORIGIN: Record<string, string> = { [ALICE.userId]: "origin-alice", [BOB.userId]: "origin-bob" };

async function fixture(): Promise<Fixture> {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  return f;
}

/** One real buffered delivery (claim -> stage -> commit -> lazy ACK) for `tenant`. */
async function deliver(f: Fixture, tenant: Tenant, batchId: string, n: number) {
  const origin = ORIGIN[tenant.userId]!;
  const raw = await batch(
    Array.from({ length: n }, (_, i) => record({ record_id: `${batchId}-r${i}`, revision: 2, session_id: `${batchId}-s${i % 2}`, origin_id: origin })),
    { batch_id: batchId, origin_id: origin, account_id: await deliveryAccount(tenant) },
  );
  const p = await prepareDelivery(tenant, raw);
  const w = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const sourceDigest = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(w)])).rows[0]!.d;
  const ledger = sqlLedger(f.run, { acceptanceMode: "buffered", expectedGeneration: generation });
  const out = await deliverBatch({ ...f.deps, ledger, custody: { witness: w, sourceDigest } } as never, tenant, raw);
  expect(out.outcome).toBe("accepted_buffered");
}

async function authSession(f: Fixture, tenant: Tenant, id: string) {
  await f.db.query(
    `INSERT INTO sessions.auth_sessions(id,token_family_id,subject_type,user_id,org_id,workspace_id,scopes,access_token_hash,access_token_expires_at,
       refresh_token_hash,refresh_token_expires_at,created_at,updated_at)
     VALUES($1,$1,'cli',$2,$3,$4,'["rth:sync"]'::jsonb,encode(sha256(convert_to('a'||$1,'UTF8')),'hex'),now()+interval '1 day',
       encode(sha256(convert_to('r'||$1,'UTF8')),'hex'),now()+interval '90 days',now(),now())`,
    [id, tenant.userId, tenant.orgId, tenant.workspaceId],
  );
}

/** Every row of the user in every sessions table with a user, owner or origin column. */
async function footprint(f: Fixture, tenant: Tenant): Promise<Record<string, number>> {
  const cols = (await f.db.query<{ t: string; c: string }>(
    `SELECT c.table_name t, c.column_name c FROM information_schema.columns c JOIN information_schema.tables tb USING (table_schema,table_name)
     WHERE tb.table_type='BASE TABLE' AND c.table_schema='sessions' AND c.column_name IN ('user_id','current_owner','origin_id')
       AND c.table_name<>'operator_user_erasures' ORDER BY 1,2`)).rows;
  const out: Record<string, number> = {};
  for (const { t, c } of cols) {
    const v = c === "origin_id" ? ORIGIN[tenant.userId] : tenant.userId;
    const n = (await f.db.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.${t} WHERE ${c}=$1`, [v])).rows[0]!.n;
    if (n > 0) out[`${t}.${c}`] = n;
  }
  return out;
}
/** Seats and acks carry no user column: count them through the user's slots. */
async function receiptRows(f: Fixture, tenant: Tenant) {
  return (await f.db.query<{ slots: number; seats: number; acks: number }>(
    `SELECT count(DISTINCT l.key_sha256)::int slots, count(DISTINCT s.key_sha256)::int seats, count(DISTINCT (a.claim_token,a.ord))::int acks
     FROM sessions.ingest_lazy_native_slots l
     LEFT JOIN sessions.ingest_receipt_seats s ON s.key_sha256=l.current_seat_key
     LEFT JOIN sessions.ingest_lazy_ack_decisions a ON a.native_key_sha256=l.key_sha256 OR a.seat_key_sha256=l.current_seat_key
     WHERE l.current_owner=$1`, [tenant.userId])).rows[0]!;
}

/** A row with only the given columns set and every other NOT NULL, default-less column filled by type (test-only). */
async function insertMinimal(f: Fixture, table: string, values: Record<string, string>) {
  const cols = (await f.db.query<{ c: string; t: string }>(
    `SELECT column_name c, data_type t FROM information_schema.columns WHERE table_schema='sessions' AND table_name=$1
       AND is_nullable='NO' AND column_default IS NULL`, [table])).rows;
  const fill: Record<string, string> = { "timestamp with time zone": "now()", integer: "0", bigint: "0", smallint: "0", boolean: "false",
    jsonb: "'{}'::jsonb", uuid: "gen_random_uuid()", text: "'x'", numeric: "0", ARRAY: "'{}'" };
  const names = Object.keys(values), exprs = names.map((_, i) => `$${i + 1}`);
  for (const { c, t } of cols) if (!(c in values)) { names.push(c); exprs.push(fill[t] ?? "'x'"); }
  await f.db.query(`INSERT INTO sessions.${table}(${names.join(",")}) VALUES(${exprs.join(",")})`, Object.values(values));
}

const call = async (f: Fixture, sql: string, params: unknown[]) => (await f.db.query<{ r: any }>(sql, params as never)).rows[0]!.r;
const begin = (f: Fixture, req: string, t: Tenant) => call(f, "SELECT sessions.operator_user_erase_begin($1,$2,$3,$4) r", [req, t.orgId, t.workspaceId, t.userId]);
const step = (f: Fixture, req: string, limit = 2) => call(f, "SELECT sessions.operator_user_erase_step($1,$2) r", [req, limit]);
const complete = (f: Fixture, req: string) => call(f, "SELECT sessions.operator_user_erase_complete($1) r", [req]);
async function purge(f: Fixture, req: string, limit = 2) {
  const phases: string[] = [];
  for (let i = 0; i < 100; i++) {
    const r = await step(f, req, limit);
    phases.push(r.phase ?? "done");
    if (r.done) return { r, phases };
  }
  throw new Error("purge did not finish");
}
const sqlstate = async (p: Promise<unknown>) => { try { await p; return null; } catch (e) { return (e as { code?: string }).code ?? "?"; } };
const pgDb = (f: Fixture): UserEraseDb => ({ query: async (text, params) => (await f.db.query(text, (params ?? []) as never)) as never });
async function storeKeys(f: Fixture): Promise<string[]> {
  const store = f.deps.target.store, out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey("96bulls"), cursor);
    out.push(...page.items.map((i) => String(i.key)));
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}
const machine = (t: Tenant) => liveMachineId(t.workspaceId, ORIGIN[t.userId]!, t.userId);

describe("operator per-user erase (058010)", () => {
  test("erases one user's buffered uploads in bounded steps and leaves the other user unchanged", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 3);
    await deliver(f, ALICE, "a2", 2);
    await deliver(f, BOB, "b1", 2);
    await authSession(f, ALICE, "auth-alice-1");
    await authSession(f, BOB, "auth-bob-1");
    const bobBefore = await footprint(f, BOB);
    const bobReceipts = await receiptRows(f, BOB);
    const alice = await receiptRows(f, ALICE);
    expect(alice.slots).toBe(5);
    expect(alice.seats).toBe(5);
    expect(alice.acks).toBeGreaterThan(0);
    expect(Object.keys(await footprint(f, ALICE))).toEqual(expect.arrayContaining(["ingest_batches.user_id", "ingest_stage_intents.user_id", "ingest_buffer_outbox.user_id", "auth_sessions.user_id"]));

    const b = await begin(f, "req-1", ALICE);
    expect(b).toMatchObject({ state: "purging", origins: ["origin-alice"], tokens_revoked: 1, done: false });
    expect((await f.db.query<{ r: string }>("SELECT revoked_reason r FROM sessions.auth_sessions WHERE id='auth-alice-1'")).rows[0]!.r).toBe("operator_user_erase");
    expect((await f.db.query<{ r: string | null }>("SELECT revoked_at::text r FROM sessions.auth_sessions WHERE id='auth-bob-1'")).rows[0]!.r).toBeNull();

    const { r, phases } = await purge(f, "req-1", 2);
    expect(phases).toEqual(["slots", "slots", "slots", "purged"]); // 5 slots at 2 per step, then the purge
    expect(r).toMatchObject({ state: "metadata_purged", done: true });
    expect(r.deleted).toMatchObject({ ingest_lazy_native_slots: 5, ingest_receipt_seats: 5, ingest_batches: 2, auth_sessions_revoked_late: 0 });
    expect(r.deleted).not.toHaveProperty("auth_sessions");

    // Only the revoked auth session stays: deleting it would let a replayed Neon mirror row re-insert it live.
    expect(await footprint(f, ALICE)).toEqual({ "auth_sessions.user_id": 1 });
    expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.auth_sessions WHERE user_id=$1 AND revoked_at IS NULL", [ALICE.userId])).rows[0]!.n).toBe(0);
    expect(await receiptRows(f, ALICE)).toEqual({ slots: 0, seats: 0, acks: 0 });
    expect(await footprint(f, BOB)).toEqual(bobBefore);
    expect(await receiptRows(f, BOB)).toEqual(bobReceipts);

    // A purged erase answers from the ledger; complete clears the captured origins.
    expect((await step(f, "req-1")).state).toBe("metadata_purged");
    expect(await complete(f, "req-1")).toMatchObject({ state: "bytes_verified", origins: [] });
    expect((await complete(f, "req-1")).state).toBe("bytes_verified");
  });

  test("begin is idempotent per request and refuses a reused request id or a second active request", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    const first = await begin(f, "req-1", ALICE);
    expect(await begin(f, "req-1", ALICE)).toEqual(first);
    expect(await sqlstate(begin(f, "req-1", BOB))).toBe("22023");
    expect(await sqlstate(begin(f, "req-2", ALICE))).toBe("55000");
    expect(await sqlstate(complete(f, "req-1"))).toBe("55000"); // metadata not purged yet
    expect(await sqlstate(step(f, "nope"))).toBe("22023");
    expect(await sqlstate(step(f, "req-1", 0))).toBe("22023");
  });

  test("begin refuses when a device origin of the user also carries another user's uploads", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    // Bob uploads from Alice's origin (a shared device): un-owned slots under it could be his.
    ORIGIN[BOB.userId] = ORIGIN[ALICE.userId]!;
    try {
      await deliver(f, BOB, "b1", 1);
      expect(await sqlstate(begin(f, "req-1", ALICE))).toBe("55000");
      expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.operator_user_erasures")).rows[0]!.n).toBe(0);
    } finally {
      ORIGIN[BOB.userId] = "origin-bob";
    }
  });

  test("a step waits out a live buffered-GC lease on the user's stage", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    await begin(f, "req-1", ALICE);
    await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_token=gen_random_uuid(),gc_lease_until=clock_timestamp()+interval '2 minutes',gc_lease_version=1 WHERE user_id=$1", [ALICE.userId]);
    const r = await step(f, "req-1");
    expect(r.phase).toBe("gc_lease_active");
    expect(r.retry_after_ms).toBeGreaterThan(60_000);
    expect(r.state).toBe("purging");
    expect((await receiptRows(f, ALICE)).slots).toBe(1); // nothing deleted
    await f.db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE user_id=$1", [ALICE.userId]);
    expect((await purge(f, "req-1", 10)).r.state).toBe("metadata_purged");
  });

  test("a user with published sessions is refused (RH040) before anything changes", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    await authSession(f, ALICE, "auth-alice-1");
    const before = await footprint(f, ALICE);
    await f.db.query("SET session_replication_role=replica");
    await insertMinimal(f, "session_index", { org_id: ALICE.orgId, workspace_id: ALICE.workspaceId, harness: "claude", session_id: "s-pub", machine_id: "srv_x", user_id: ALICE.userId, storage_provider: "s3" });
    await f.db.query("SET session_replication_role=origin");
    expect(await sqlstate(begin(f, "req-1", ALICE))).toBe("RH040");
    expect(await footprint(f, ALICE)).toEqual({ ...before, "session_index.user_id": 1 });
    expect((await f.db.query<{ r: string | null }>("SELECT revoked_at::text r FROM sessions.auth_sessions WHERE id='auth-alice-1'")).rows[0]!.r).toBeNull();
    expect((await f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.operator_user_erasures")).rows[0]!.n).toBe(0);
  });

  test("the orchestrator purges metadata, erases the user's buffered objects, verifies and completes; the other user's objects stay", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 3);
    await deliver(f, BOB, "b1", 2);
    const before = await storeKeys(f);
    expect(before.filter((k) => k.includes("/buffered-batches/") && k.includes(`/m=${machine(ALICE)}/`)).length).toBeGreaterThan(0);
    const bobKeys = before.filter((k) => k.includes(`/m=${machine(BOB)}/`));
    expect(bobKeys.length).toBeGreaterThan(0);

    const sleeps: number[] = [];
    const out = await runUserErase(pgDb(f), { requestId: "req-e2e", ...ALICE }, {
      keyRoot: "96bulls", eraser: storeEraser(f.deps.target.store), stepLimit: 2, settleMs: 5, sleep: async (ms) => { sleeps.push(ms); },
    });
    expect(out).toMatchObject({ state: "bytes_verified", verified: true, prefixes: 4, byteRounds: 1 });
    expect(out.versionsDeleted).toBeGreaterThan(0);
    expect(sleeps).toEqual([5]);
    const after = await storeKeys(f);
    expect(after.filter((k) => k.includes(`/m=${machine(ALICE)}/`))).toEqual([]);
    expect(after.filter((k) => k.includes(`/m=${machine(BOB)}/`))).toEqual(bobKeys);
    expect(await footprint(f, ALICE)).toEqual({});

    // A rerun of the same request is a no-op that reports the completed erase.
    const again = await runUserErase(pgDb(f), { requestId: "req-e2e", ...ALICE }, { keyRoot: "96bulls", eraser: storeEraser(f.deps.target.store), sleep: async () => {} });
    expect(again).toMatchObject({ state: "bytes_verified", verified: true, prefixes: 0, versionsDeleted: 0 });
  });

  test("an object that lands under the user's prefix during the settle window is found and erased in the next round", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    const store = f.deps.target.store;
    const late = `96bulls/${ALICE.orgId}/buffered-batches/v1/ws=${ALICE.workspaceId}/m=${machine(ALICE)}/b=late.staged.ndjson.zst`;
    let rounds = 0;
    const out = await runUserErase(pgDb(f), { requestId: "req-late", ...ALICE }, {
      keyRoot: "96bulls", eraser: storeEraser(store), settleMs: 1,
      sleep: async () => { if (rounds++ === 0) await store.put(objectKey(late), new TextEncoder().encode("late") as never); },
    });
    expect(out).toMatchObject({ state: "bytes_verified", verified: true, byteRounds: 2 });
    expect((await storeKeys(f)).filter((k) => k.includes(`/m=${machine(ALICE)}/`))).toEqual([]);
  });

  test("a publication that commits after begin turns the next step into RH040; the request stays purging", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 3);
    await begin(f, "req-1", ALICE);
    expect((await step(f, "req-1", 1)).phase).toBe("slots");
    await f.db.query("SET session_replication_role=replica");
    await insertMinimal(f, "session_index", { org_id: ALICE.orgId, workspace_id: ALICE.workspaceId, harness: "claude", session_id: "s-pub", machine_id: "srv_x", user_id: ALICE.userId, storage_provider: "s3" });
    await f.db.query("SET session_replication_role=origin");
    const before = await footprint(f, ALICE);
    expect(await sqlstate(step(f, "req-1", 1))).toBe("RH040");
    expect(await footprint(f, ALICE)).toEqual(before); // the step rolled back whole
    expect((await f.db.query<{ s: string }>("SELECT state s FROM sessions.operator_user_erasures WHERE request_id='req-1'")).rows[0]!.s).toBe("purging");
    // The same for every published-state table the probe covers, e.g. the canonical history scope owners.
    await f.db.query("DELETE FROM sessions.session_index WHERE user_id=$1", [ALICE.userId]);
    await insertMinimal(f, "ingest_history_scope_owners", { org_id: ALICE.orgId, workspace_id: ALICE.workspaceId, user_id: ALICE.userId, control_epoch: "a".repeat(64) });
    expect(await sqlstate(step(f, "req-1", 1))).toBe("RH040");
    await f.db.query("DELETE FROM sessions.ingest_history_scope_owners WHERE user_id=$1", [ALICE.userId]);
    expect((await purge(f, "req-1", 10)).r.state).toBe("metadata_purged");
  });

  test("the final purge refuses (RH040) when a publication committed while its deletes waited", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    await begin(f, "req-1", ALICE);
    expect((await step(f, "req-1", 10)).phase).toBe("slots");
    // Stand-in for a publication committing during the purge: a parked session_index row becomes Alice's when the
    // purge deletes her batches (ENABLE ALWAYS fires under replica, which the parked row needs to skip its checks).
    await f.db.query("SET session_replication_role=replica");
    await insertMinimal(f, "session_index", { org_id: ALICE.orgId, workspace_id: ALICE.workspaceId, harness: "claude", session_id: "s-pub", machine_id: "srv_x", user_id: "parked", storage_provider: "s3" });
    await f.db.exec(`CREATE FUNCTION pg_temp.publish_now() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      UPDATE sessions.session_index SET user_id='${ALICE.userId}' WHERE user_id='parked'; RETURN NULL; END $$;
      CREATE TRIGGER publish_now AFTER DELETE ON sessions.ingest_batches FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.publish_now();
      ALTER TABLE sessions.ingest_batches ENABLE ALWAYS TRIGGER publish_now;`);
    const before = await footprint(f, ALICE);
    expect(await sqlstate(step(f, "req-1", 10))).toBe("RH040");
    expect(await footprint(f, ALICE)).toEqual(before);
    expect((await f.db.query<{ s: string }>("SELECT state s FROM sessions.operator_user_erasures WHERE request_id='req-1'")).rows[0]!.s).toBe("purging");
    await f.db.exec("DROP TRIGGER publish_now ON sessions.ingest_batches; SET session_replication_role=origin;");
    await f.db.query("DELETE FROM sessions.session_index WHERE user_id IN ($1,'parked')", [ALICE.userId]);
    expect((await purge(f, "req-1", 10)).r.state).toBe("metadata_purged");
  });

  test("a receipt seat another user's slot references keeps its seat-keyed rows", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 2);
    await deliver(f, BOB, "b1", 2);
    const slot = async (u: string) => (await f.db.query<{ k: string; s: string }>(
      "SELECT key_sha256 k, current_seat_key s FROM sessions.ingest_lazy_native_slots WHERE current_owner=$1 ORDER BY key_sha256 LIMIT 1", [u])).rows[0]!;
    const shared = (await slot(ALICE.userId)).s, bob = (await slot(BOB.userId)).k;
    // Bob's slot and its seat-keyed receipt rows move onto Alice's seat (never expected in production).
    await f.db.query("SET session_replication_role=replica");
    await f.db.query("UPDATE sessions.ingest_lazy_native_slots SET current_seat_key=$1 WHERE key_sha256=$2", [shared, bob]);
    await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET seat_key_sha256=$1 WHERE native_key_sha256=$2", [shared, bob]);
    await f.db.query("UPDATE sessions.ingest_receipt_ord_seats SET key_sha256=$1 WHERE candidate_key_sha256=$2", [shared, bob]);
    await f.db.query("SET session_replication_role=origin");
    const bobRows = async () => (await f.db.query<{ a: number; o: number; seat: number }>(
      `SELECT (SELECT count(*)::int FROM sessions.ingest_lazy_ack_decisions WHERE native_key_sha256=$1 AND seat_key_sha256=$2) a,
              (SELECT count(*)::int FROM sessions.ingest_receipt_ord_seats WHERE candidate_key_sha256=$1 AND key_sha256=$2) o,
              (SELECT count(*)::int FROM sessions.ingest_receipt_seats WHERE key_sha256=$2) seat`, [bob, shared])).rows[0]!;
    const before = await bobRows();
    expect(before.a + before.o).toBeGreaterThan(0);
    expect(before.seat).toBe(1);
    await begin(f, "req-1", ALICE);
    expect((await purge(f, "req-1", 1)).r.state).toBe("metadata_purged");
    expect(await bobRows()).toEqual(before);
    expect((await slot(BOB.userId)).k).toBeDefined();
    expect((await receiptRows(f, ALICE)).slots).toBe(0);
  });

  test("every per-user row goes: publication frontiers, consent, tenant slots, rearm, parity, read API tokens; late auth sessions are revoked", async () => {
    const f = await fixture();
    await deliver(f, ALICE, "a1", 1);
    await deliver(f, BOB, "b1", 1);
    const hex = (c: string) => c.repeat(64);
    const perUser = async (t: Tenant, n: string) => {
      const k = { org_id: t.orgId, workspace_id: t.workspaceId, user_id: t.userId };
      await insertMinimal(f, "upload_consent_policies", { ...k, mode: "all", revision: "1" });
      await insertMinimal(f, "upload_consent_sessions", { ...k, source: "claude", session_id: `s-${n}` });
      await insertMinimal(f, "upload_consent_origins", { ...k, origin_id: ORIGIN[t.userId]! });
      await insertMinimal(f, "ingest_tenant_slots", { ...k, holder: `h-${n}` });
      await insertMinimal(f, "upload_erasure_rearm", { ...k, origin_id: ORIGIN[t.userId]!, machine_id: machine(t), source: "claude", session_id: `s-${n}` });
      await insertMinimal(f, "owner_buffered_parity", { ...k, cutover_epoch: hex("a"), no_head: true as never, privacy_baseline_sha256: hex("b"),
        deletion_set_digest: hex("c"), reset_set_digest: hex("d"), target_lineage_digest: hex("e"), target_generation: hex("f") });
      await insertMinimal(f, "read_api_tokens", { ...k, id: `rt-${n}`, token_family_id: `rt-${n}`, subject_type: "cli", audience: "rth",
        access_token_hash: hex(n === "a" ? "1" : "2"), refresh_token_hash: hex(n === "a" ? "3" : "4") });
      await insertMinimal(f, "ingest_publication_frontiers", { ...k, origin_id: ORIGIN[t.userId]!, source_frontier_sha256: hex("a"),
        record_maxima_sha256: hex("b"), target_region: "us-east-1", key_root: "96bulls", schema_generation: hex("c") });
    };
    await perUser(ALICE, "a");
    await perUser(BOB, "b");
    const bobBefore = await footprint(f, BOB);
    const b = await begin(f, "req-1", ALICE);
    expect(b.deleted).toEqual({ ingest_publication_frontiers: 1, read_api_tokens: 1 });
    // Arrivals after begin: the external writer re-registers a frontier, the Neon mirror adds a live auth session.
    await insertMinimal(f, "ingest_publication_frontiers", { org_id: ALICE.orgId, workspace_id: ALICE.workspaceId, user_id: ALICE.userId,
      origin_id: "origin-late", source_frontier_sha256: hex("a"), record_maxima_sha256: hex("b"), target_region: "us-east-1", key_root: "96bulls", schema_generation: hex("c") });
    await authSession(f, ALICE, "auth-alice-late");
    const { r } = await purge(f, "req-1", 10);
    expect(r.deleted).toMatchObject({ ingest_publication_frontiers: 2, read_api_tokens: 1, upload_consent_policies: 1, upload_consent_sessions: 1,
      upload_consent_origins: 1, ingest_tenant_slots: 1, upload_erasure_rearm: 1, owner_buffered_parity: 1, auth_sessions_revoked_late: 1 });
    expect(await footprint(f, ALICE)).toEqual({ "auth_sessions.user_id": 1 });
    expect((await f.db.query<{ r: string }>("SELECT revoked_reason r FROM sessions.auth_sessions WHERE id='auth-alice-late'")).rows[0]!.r).toBe("operator_user_erase");
    expect(await footprint(f, BOB)).toEqual(bobBefore);
  });

  test("the eleven support indexes exist", async () => {
    const f = await fixture();
    const idx = (await f.db.query<{ d: string }>(
      `SELECT pg_get_indexdef(indexrelid) d FROM pg_index WHERE indexrelid::regclass::text IN (
        'sessions.ingest_lazy_ack_decisions_native_key_sha256_idx','sessions.ingest_lazy_ack_decisions_seat_key_sha256_idx',
        'sessions.ingest_lazy_native_slots_current_seat_key_idx','sessions.ingest_receipt_ord_seats_key_sha256_idx',
        'sessions.ingest_receipt_record_heads_winner_key_sha256_idx','sessions.ingest_stage_intents_parent_stage_token_idx',
        'sessions.ingest_stage_intents_gc_replacement_stage_token_idx','sessions.ingest_stage_intents_batch_idx',
        'sessions.ingest_buffer_outbox_active_stage_token_idx','sessions.ingest_history_plans_stage_token_idx',
        'sessions.ingest_lazy_native_slots_unowned_origin_idx') ORDER BY 1`)).rows.map((r) => r.d);
    expect(idx).toHaveLength(11);
  });
});
