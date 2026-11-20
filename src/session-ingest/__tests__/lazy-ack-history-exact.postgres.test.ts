/**
 * Opt-in OWNED PostgreSQL (SESSION_INGEST_TEST_PG_URL, loopback fixture only): multi-connection checks of
 * 20261105062156_lazy_ack_history_exact (gate bulls96.lazy_ack_h_exact). Serial parity is the PGlite suite's claim;
 * these are the concurrent invariants and the forced-abort sequence positions:
 *   C1  two concurrent gated ACKs on overlapping managed scopes, opposite first-scope order: both commit, no 40P01,
 *       request_version advances by the sum of both ACKs' invalidations.
 *   C2  a concurrent non-ACK consumer of session_replay_epoch_seq: every drawn value unique; every invalidated scope's
 *       epoch strictly increased (the stale-build CAS compares with IS DISTINCT FROM).
 *   A1  a forced 55P03 at a scope-lock wait (another session holds the scope's directory-events lock): main and the
 *       candidate fail the same way after the SAME number of draws.
 *   A2  a forced 55P03 inside the deferred row work (another session holds the required_scopes row): the known
 *       residue. Reports the nextval delta and the next successful epoch for both bodies, and checks the bound.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";
import { runMigrations, type MigrationProfile } from "../../hoopdb/migrate.ts";
import { derivedSchemaAssets } from "../../session-pipeline/derived-schema.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
}
const epoch = "3".repeat(64), identity = "c".repeat(64);
const profile: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" };
const COMMIT = "SELECT sessions.ingest_origin_commit_buffered_batch($1::jsonb) AS result";
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const SEQ = "SELECT last_value::text v FROM pg_sequences WHERE schemaname='sessions' AND sequencename='session_replay_epoch_seq'";

describe.skipIf(!url)("062156 history-exact staged ACK (owned PostgreSQL)", () => {
  let admin: pg.Client, pool: pg.Pool, name: string, generation: string, target: string;
  let scratch: ReturnType<typeof scratchStore>, serial = 0;
  const ledgerFor = (intercept?: (sql: string, p: unknown[]) => void) => sqlLedger(async (sql, params) => {
    intercept?.(sql, params as unknown[]);
    const c = await pool.connect();
    try { await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest"); const r = await c.query(sql, params as never); await c.query("COMMIT"); return r as never; }
    catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; } finally { c.release(); }
  }, { acceptanceMode: "buffered", expectedGeneration: generation, originClaimFence: true });
  async function custody(t: any, raw: any) {
    const p = await prepareDelivery(t, raw);
    const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: t,
      originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
      originalRecordCount: raw.records.length, candidateCount: p.records.length,
      records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
        revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true, deletions: [], resets: [] } };
    return { witness, sourceDigest: (await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0].d as string };
  }
  /** A fresh user + origin; a warm delivery; then the captured commit payload of a batch whose records sit in `sessions`. */
  async function prepared(records: Array<[string, string]>) {
    const t = { ...TENANT, userId: `hx-u${++serial}` }, origin = `hx-o${serial}`;
    const mk = async (id: string, recs: Array<[string, string]>) => batch(recs.map(([rid, sess]) => record({ record_id: rid, revision: 2, session_id: sess, origin_id: origin })),
      { batch_id: id, account_id: await deliveryAccount(t), origin_id: origin });
    const warm = await mk(`hx-warm-${serial}`, [[`hx-warm-r${serial}`, `hx-warm-s${serial}`]]);
    expect((await deliverBatch({ ledger: ledgerFor(), acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, warm) } as never, t, warm)).outcome).toBe("accepted_buffered");
    const raw = await mk(`hx-b-${serial}`, records);
    let payload: string | undefined;
    const ledger = ledgerFor((sql, params) => { if (sql.includes("commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); } });
    await deliverBatch({ ledger, acceptanceMode: "buffered", target: { store: scratch.store, keyRoot: "96bulls" }, custody: await custody(t, raw) } as never, t, raw).catch(() => {});
    if (typeof payload !== "string") throw Error("payload_not_captured");
    return payload;
  }
  /** Committed fixture: managed scopes (one live derived object each) and scope_keys mapping records to them. */
  async function managed(scopes: string[], keys: Array<[string, string]>, existing: string[] = []) {
    const c = await pool.connect();
    try {
      await c.query("BEGIN;SET LOCAL session_replication_role=replica");
      for (const sess of scopes) {
        const auth = (await c.query("SELECT md5($1)::uuid::text u", ["hx-auth-" + sess])).rows[0].u;
        await c.query(`INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,
          pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
          VALUES($1,$2,$3,'claude',$4,1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64))`, [auth, TENANT.orgId, TENANT.workspaceId, sess]);
        await c.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,'claude',$3,$4,true)", [TENANT.orgId, TENANT.workspaceId, sess, auth]);
        await c.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES($1,$2,repeat('b',64),1)", ["hx-obj-" + sess, auth]);
      }
      for (const [r, sess] of keys) await c.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,$3,'claude',$4) ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, r, sess]);
      for (const sess of existing) {
        await c.query("INSERT INTO sessions.ingest_history_required_scopes(org_id,workspace_id,harness,session_id,request_version) VALUES($1,$2,'claude',$3,1)", [TENANT.orgId, TENANT.workspaceId, sess]);
        await c.query("INSERT INTO sessions.session_replay_publications(org_id,workspace_id,harness,session_id) VALUES($1,$2,'claude',$3)", [TENANT.orgId, TENANT.workspaceId, sess]);
      }
      await c.query("COMMIT");
    } finally { c.release(); }
  }
  const gate = (on: boolean) => pool.query(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_h_exact='${on ? "on" : "off"}'`);
  const seqPos = async () => BigInt((await pool.query(SEQ)).rows[0].v);
  const scopeKey = (sess: string) => `hashtextextended(concat_ws(chr(31),'96bulls:directory-events','${TENANT.orgId}','${TENANT.workspaceId}','claude','${sess}'),0)`;
  /** One commit on its own connection: BEGIN, limits, runtime role, commit, then COMMIT or ROLLBACK. */
  async function run(payload: string, opts: { commit?: boolean; lockTimeout?: string; read?: string } = {}) {
    const c = new pg.Client({ connectionString: target }); await c.connect();
    try {
      await c.query(`BEGIN;SET LOCAL lock_timeout='${opts.lockTimeout ?? "10s"}';SET LOCAL statement_timeout='20s';SET LOCAL ROLE bulls96_session_ingest`);
      try {
        const r = (await c.query(COMMIT, [payload])).rows[0].result;
        await c.query("RESET ROLE");
        const read = opts.read ? (await c.query(opts.read)).rows : undefined;
        await c.query(opts.commit ? "COMMIT" : "ROLLBACK");
        return { ok: true as const, kind: r.kind as string, read };
      } catch (e: any) { await c.query("ROLLBACK"); return { ok: false as const, code: String(e.code), message: String(e.message) }; }
    } finally { await c.end(); }
  }

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url }); await admin.connect();
    name = "hx_" + crypto.randomUUID().replaceAll("-", "");
    await admin.query("CREATE DATABASE " + name);
    const u = new URL(url!); u.pathname = "/" + name; target = u.href;
    pool = new pg.Pool({ connectionString: target, max: 12 });
    scratch = scratchStore();
    await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
    const c = await pool.connect(); try { await runMigrations(c, { profile }); } finally { c.release(); }
    generation = (await derivedSchemaAssets()).expected.generation;
    await pool.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch,
      privacyBaselineSha256: "a".repeat(64), retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" })]);
    await pool.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch,
      capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
      allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) })]);
    await pool.query("UPDATE sessions.ingest_history_switch SET live=true WHERE singleton");
    // Adopt the org with one delivery first: history fixture rows carrying the org id before any adoption would read
    // as unbound prior org authority (55000) to the next first-seen claim.
    await prepared([["hx-boot", "hx-boot-s"]]);
  }, 300000);
  afterAll(async () => {
    await pool?.end(); scratch?.done();
    if (admin) { if (name) await admin.query("DROP DATABASE IF EXISTS " + name + " WITH(FORCE)"); await admin.end(); }
  }, 60000);

  test("C1: two concurrent gated ACKs on overlapping scopes in opposite first-scope order both commit; no 40P01", async () => {
    await managed(["c1-S1", "c1-S2"], [["c1-a", "c1-S1"], ["c1-b", "c1-S2"], ["c1-c", "c1-S2"], ["c1-d", "c1-S1"]]);
    const p1 = await prepared([["c1-a", "c1-own1"], ["c1-b", "c1-own1"]]);
    const p2 = await prepared([["c1-c", "c1-own2"], ["c1-d", "c1-own2"]]);
    await gate(true);
    try {
      // The claims' slot INSERTs already invalidated both scopes (insert trigger, unchanged); each ACK adds one per scope.
      const versions = async () => Object.fromEntries((await pool.query("SELECT session_id,request_version::int v FROM sessions.ingest_history_required_scopes WHERE session_id IN('c1-S1','c1-S2')")).rows.map((x) => [x.session_id, x.v]));
      const before = await versions();
      const [r1, r2] = await Promise.all([run(p1, { commit: true }), run(p2, { commit: true })]);
      expect([r1, r2]).toEqual([{ ok: true, kind: "committed", read: undefined }, { ok: true, kind: "committed", read: undefined }]);
      expect(await versions()).toEqual({ "c1-S1": (before["c1-S1"] ?? 0) + 2, "c1-S2": (before["c1-S2"] ?? 0) + 2 });
      expect((await pool.query("SELECT count(*)::int n FROM sessions.lazy_ack_history_marks")).rows[0].n).toBe(0);
    } finally { await gate(false); }
  }, 120000);

  test("C2: a concurrent non-ACK nextval consumer: all values unique, every invalidated scope's epoch strictly increases", async () => {
    const scopes = ["c2-S1", "c2-S2", "c2-S3"];
    const recs = Array.from({ length: 30 }, (_, i) => [`c2-r${i}`, scopes[i % 3]!] as [string, string]);
    await managed(scopes, recs, ["c2-S1"]);
    const payloads = [];
    for (let k = 0; k < 4; k++) payloads.push(await prepared(recs.slice(k * 7, k * 7 + 7).map(([r]) => [r, `c2-own${k}`] as [string, string])));
    const epochOf = async () => Object.fromEntries((await pool.query("SELECT session_id,epoch::text e FROM sessions.session_replay_publications WHERE session_id = ANY($1)", [scopes])).rows.map((x) => [x.session_id, BigInt(x.e)]));
    await gate(true);
    try {
      const before = await epochOf();
      let stop = false; const drawn: bigint[] = [];
      const consumer = (async () => { const c = new pg.Client({ connectionString: target }); await c.connect();
        try { while (!stop) drawn.push(BigInt((await c.query("SELECT nextval('sessions.session_replay_epoch_seq') v")).rows[0].v)); } finally { await c.end(); } })();
      const results = [];
      for (const p of payloads) results.push(await run(p, { commit: true }));
      stop = true; await consumer;
      expect(results.every((r) => r.ok && r.kind === "committed")).toBe(true);
      const after = await epochOf();
      for (const s of scopes) {
        expect(after[s]).toBeDefined();
        if (before[s] !== undefined) expect(after[s]! > before[s]!).toBe(true);
        expect(drawn).not.toContain(after[s]);
      }
      expect(new Set(drawn.map(String)).size).toBe(drawn.length);
      expect(drawn.length).toBeGreaterThan(0);
    } finally { await gate(false); }
  }, 180000);

  test("A1: a forced 55P03 at a scope-lock wait fails both bodies after the same number of draws", async () => {
    await managed(["a1-T", "a1-L"], [["a1-1", "a1-T"], ["a1-2", "a1-T"], ["a1-3", "a1-L"]], ["a1-T"]);
    const p = await prepared([["a1-1", "a1-own"], ["a1-2", "a1-own"], ["a1-3", "a1-own"]]);
    const holder = new pg.Client({ connectionString: target }); await holder.connect();
    const deltas: Record<string, bigint> = {};
    try {
      await holder.query("BEGIN"); await holder.query(`SELECT pg_advisory_xact_lock(${scopeKey("a1-L")})`);
      for (const on of [false, true]) {
        await gate(on);
        const s0 = await seqPos();
        const r = await run(p, { lockTimeout: "1s" });
        expect(r).toMatchObject({ ok: false, code: "55P03" });
        deltas[on ? "candidate" : "main"] = (await seqPos()) - s0;
      }
    } finally { await holder.query("ROLLBACK").catch(() => {}); await holder.end(); await gate(false); }
    // Records 1 and 2 invalidate a1-T (row exists: 2 draws each) before record 3 waits on a1-L's lock.
    expect(deltas).toEqual({ main: 4n, candidate: 4n });
  }, 120000);

  test("A2: a forced 55P03 inside the deferred row work: the bounded residue, in nextval calls", async () => {
    await managed(["a2-S", "a2-U"], [["a2-1", "a2-S"], ["a2-2", "a2-U"], ["a2-3", "a2-S"], ["a2-4", "a2-U"]], ["a2-S", "a2-U"]);
    const p = await prepared([["a2-1", "a2-own"], ["a2-2", "a2-own"], ["a2-3", "a2-own"], ["a2-4", "a2-own"]]);
    const holder = new pg.Client({ connectionString: target }); await holder.connect();
    const out: Record<string, { delta: bigint; nextEpochOffset: bigint }> = {};
    try {
      for (const on of [false, true]) {
        await gate(on);
        const s0 = await seqPos();
        await holder.query("BEGIN");
        await holder.query("SELECT 1 FROM sessions.ingest_history_required_scopes WHERE session_id='a2-U' FOR UPDATE");
        const r = await run(p, { lockTimeout: "1s" });
        await holder.query("ROLLBACK");
        expect(r).toMatchObject({ ok: false, code: "55P03" });
        const s1 = await seqPos();
        // The next successful ACK (rolled back afterwards, so both bodies start the comparison from the same rows).
        const ok = await run(p, { read: "SELECT epoch::text e FROM sessions.session_replay_publications WHERE session_id='a2-S'" });
        expect(ok).toMatchObject({ ok: true, kind: "committed" });
        out[on ? "candidate" : "main"] = { delta: s1 - s0, nextEpochOffset: BigInt((ok as any).read[0].e) - s0 };
      }
    } finally { await holder.query("ROLLBACK").catch(() => {}); await holder.end(); await gate(false); }
    console.log("A2 " + JSON.stringify(out, (_, v) => (typeof v === "bigint" ? v.toString() : v)));
    // main: record 1 invalidates a2-S (2 draws), record 2 blocks on a2-U's required_scopes row BEFORE its draws: 2.
    // candidate: all 4 intents draw during the loop (8), then the replay blocks on a2-U's row.
    // Bound: extra draws <= 2 x (intents from the first blocked one to the end), here 2 x 3 = 6.
    expect(out.main!.delta).toBe(2n);
    expect(out.candidate!.delta).toBe(8n);
    expect(out.candidate!.delta - out.main!.delta).toBeLessThanOrEqual(6n);
    expect(out.candidate!.nextEpochOffset - out.main!.nextEpochOffset).toBe(out.candidate!.delta - out.main!.delta);
  }, 120000);
});
