/**
 * C3 privacy-authority races on actual PostgreSQL (real locks, real READ COMMITTED snapshots). Opt in with an
 * owned, synthetic SESSION_INGEST_TEST_PG_URL; never a deployed database. Each test creates its own database.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { prepareDelivery, deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";
import { epoch, generation, identity, baseline, INDEX_SQL, LAZY_SOURCE_SQL, PENDING_SQL } from "./privacy-authority-support.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const root = join(import.meta.dir, "../../..");
const sql = (p: string) => readFileSync(join(root, p), "utf8");
const cleanup: (() => Promise<void> | void)[] = [];
afterEach(async () => {
  for (const f of cleanup.splice(0).reverse()) await f();
}, 60_000);

async function database() {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  const name = "c3_authority_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  const target = new URL(url!);
  target.pathname = "/" + name;
  const pool = new pg.Pool({ connectionString: target.href, max: 12 });
  const held: pg.PoolClient[] = [];
  cleanup.push(async () => {
    for (const c of held) c.release(true);
    await pool.end();
    await admin.query("DROP DATABASE " + name + " WITH (FORCE)");
    await admin.end();
  });
  await pool.query("CREATE SCHEMA sessions");
  await pool.query(sql("supabase/tests/supabase_shim.sql"));
  await pool.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"]) await pool.query(sql("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pool.query(sql("supabase/migrations/" + f));
  await pool.query(sql(LAZY_SOURCE_SQL));
  await pool.query(sql(PENDING_SQL));
  await pool.query(sql(INDEX_SQL));
  await pool.query("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (fn: string, p: unknown, c: pg.ClientBase | pg.Pool = pool) => (await c.query("SELECT sessions." + fn + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0].r;
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
  await pool.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baseline, "e".repeat(64)]);
  /** A dedicated connection (owner, or the ingest role) that the test can hold a transaction open on. */
  const conn = async (role?: "ingest") => {
    const c = await pool.connect();
    held.push(c);
    if (role) await c.query("SET ROLE bulls96_session_ingest");
    return c;
  };
  const ingestLedger = async () => {
    const c = await conn("ingest");
    return sqlLedger(async (t, p) => ({ rows: (await c.query(t, p)).rows }), { acceptanceMode: "buffered", expectedGeneration: generation });
  };
  /** Activation with an empty Neon (no heads, deletions or resets): the fingerprint of three empty arrays. */
  const emptyNeon = async () => (await pool.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0].f as string;
  const activateEmpty = async (c: pg.ClientBase | pg.Pool = pool) => owner("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: await emptyNeon(), heads: [], deletions: [], resets: [] }, c);
  return { pool, conn, owner, ingestLedger, activateEmpty };
}

/** Resolves once `pid` is waiting on a lock (proves the race is real, not sequential). */
async function waiting(pool: pg.Pool, pid: number) {
  for (let i = 0; i < 200; i++) {
    const r = await pool.query("SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1", [pid]);
    if (r.rows[0]?.wait_event_type === "Lock") return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("never blocked");
}
const settle = async (p: Promise<unknown>) => p.then((v) => ({ ok: true as const, v }), (e) => ({ ok: false as const, e: String((e as Error).message) }));
const call = (c: pg.ClientBase, fn: string, p: unknown) => c.query("SELECT sessions." + fn + "($1::jsonb) AS r", [JSON.stringify(p)]).then((r) => r.rows[0].r);

describe.skipIf(!url)("C3 privacy authority on actual PostgreSQL", () => {
  test("a legacy (Neon-first) begin queued behind the fence refuses once the fence commits; nothing is written", async () => {
    const d = await database();
    const o = await d.conn();
    await o.query("BEGIN");
    await call(o, "ingest_privacy_authority_fence", { controlEpoch: epoch });
    const a = await d.conn("ingest");
    const pending = settle(call(a, "ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "r" } }));
    await waiting(d.pool, (a as unknown as { processID: number }).processID);
    await o.query("COMMIT");
    const r = await pending;
    expect(r.ok).toBe(false);
    expect((r as { e: string }).e).toContain("privacy authority moved");
    expect((await d.pool.query("SELECT count(*)::int AS n FROM sessions.ingest_source_control_heads")).rows[0].n).toBe(0);
  });

  test("a legacy begin in flight holds the fence off; activation then refuses its pending head until it completes", async () => {
    const d = await database();
    const a = await d.conn("ingest");
    await a.query("BEGIN");
    const head = await call(a, "ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "r" } });
    const o = await d.conn();
    const fenced = settle(call(o, "ingest_privacy_authority_fence", { controlEpoch: epoch }));
    await waiting(d.pool, (o as unknown as { processID: number }).processID);
    await a.query("COMMIT");
    expect(await fenced).toMatchObject({ ok: true });
    const act = await settle(d.activateEmpty());
    expect((act as { e: string }).e).toContain("mirror control pending");
    // The straggler completes (as relayhistory would after its Neon side); the legacy complete still works while fenced.
    await call(a, "ingest_source_control_complete", { epoch, tenant: TENANT, version: head.version, digest: head.digest, kind: "reset", outcome: "applied" });
    // Its Neon head (version 1) has no target receipt here, so activation with an empty Neon still refuses: no silent adoption.
    expect(((await settle(d.activateEmpty())) as { e: string }).e).toContain("mirror head without a neon head");
  });

  test("an authority begin queued behind an activation that rolls back refuses; behind one that commits it proceeds", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    for (const outcome of ["ROLLBACK", "COMMIT"] as const) {
      const o = await d.conn();
      await o.query("BEGIN");
      await d.activateEmpty(o);
      const a = await d.conn("ingest");
      const begin = settle(call(a, "ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "q" } }));
      await waiting(d.pool, (a as unknown as { processID: number }).processID);
      await o.query(outcome);
      const r = await begin;
      if (outcome === "ROLLBACK") {
        expect((r as { e: string }).e).toContain("not active");
        expect((await d.pool.query("SELECT count(*)::int AS n FROM sessions.ingest_source_control_heads")).rows[0].n).toBe(0);
      } else expect(r).toMatchObject({ ok: true, v: { version: "1", apply: true } });
    }
  });

  test("a release racing an authority control waits for it and then refuses (one-way once anything changed)", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.activateEmpty();
    const a = await d.conn("ingest");
    await a.query("BEGIN");
    await call(a, "ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "q" } });
    const o = await d.conn();
    const rel = settle(call(o, "ingest_privacy_authority_release", { controlEpoch: epoch }));
    await waiting(d.pool, (o as unknown as { processID: number }).processID);
    await a.query("COMMIT");
    expect(((await rel) as { e: string }).e).toContain("diverged from neon");
    expect((await d.pool.query("SELECT state FROM sessions.ingest_privacy_authority")).rows).toEqual([{ state: "active" }]);
  });

  test("an upload racing a deletion: a witness read before the disable cannot land the deleted session after it", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.activateEmpty();
    const fs = scratchStore();
    cleanup.push(fs.done);
    const ledger = await d.ingestLedger();
    const deps = { ledger, acceptanceMode: "buffered" as const, target: { store: fs.store, keyRoot: "96bulls" } };
    const raw = await batch([record({ record_id: "x1", session_id: "S" })], { batch_id: "race" });
    const p = await prepareDelivery(TENANT, raw);
    const witnessInput = { kind: "batch" as const, tenant: TENANT, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: 1,
      candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id })) };
    // The disable begins (uncommitted) while the upload's witness is read: READ COMMITTED sees no pending head yet.
    const a = await d.conn("ingest");
    await a.query("BEGIN");
    const head = await call(a, "ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "selection", payload: { origin_id: p.originId, sessions: [{ source: "claude", session_id: "S" }], enabled: false } });
    const stale = await ledger.frozenWitness!(witnessInput);
    expect(stale.witness).toMatchObject({ pending: false, sourceControlVersion: "0" });
    await a.query("COMMIT");
    // After the begin commits, a fresh witness refuses (pending), exactly as Neon's pending gate.
    expect(await ledger.frozenWitness!(witnessInput).then(() => "ok", (e) => String(e.code ?? e.message))).not.toBe("ok");
    // The fence applies the disable at the target, then the head completes.
    const control = { epoch, version: head.version, digest: head.digest, kind: "selection" as const };
    const cw = await ledger.frozenWitness!({ kind: "selection", tenant: TENANT, originId: p.originId, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
    const applied = await ledger.applyOrderedControl!({ tenant: TENANT, originId: p.originId, sessions: [["claude", "S"]], enabled: false, control, custody: cw });
    expect(applied.control).toMatchObject({ outcome: "applied", version: "1" });
    await ledger.privacyAuthorityComplete!({ ...control, outcome: "applied", tenant: TENANT });
    // The upload, still carrying the stale (version 0) witness, is refused by the target: nothing is accepted.
    const late = await deliverBatch({ ...deps, custody: stale }, TENANT, raw).then(() => "accepted", (e) => String(e.code ?? e.message));
    expect(late).not.toBe("accepted");
    expect((await d.pool.query("SELECT count(*)::int AS n FROM sessions.upload_deletions WHERE session_id='S'")).rows[0].n).toBe(1);
    // A fresh witness now reports the session fenced; the batch is acknowledged (so the client stops retrying)
    // with the session counted as deleted, exactly like relayhistory's fenced acknowledgement.
    const fresh = await ledger.frozenWitness!(witnessInput);
    expect(fresh.witness.records[0]).toMatchObject({ candidateFenced: true });
    const ok = await deliverBatch({ ...deps, custody: fresh }, TENANT, raw);
    expect(ok.stats.deleted_sessions).toBe(1);
    expect(await deliveryAccount(TENANT)).toBe(raw.account_id);
  });
});
