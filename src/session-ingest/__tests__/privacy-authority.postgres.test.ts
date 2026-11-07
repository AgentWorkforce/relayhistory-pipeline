/**
 * C3 privacy-authority races on actual PostgreSQL (real locks, real READ COMMITTED snapshots). Opt in with an
 * owned, synthetic SESSION_INGEST_TEST_PG_URL; never a deployed database. Each test creates its own database.
 */
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { sqlLedger } from "../ledger-sql.ts";
import { deliverBatch } from "../deliver.ts";
import { prepareDelivery, deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";
import { epoch, generation, identity, baseline, INDEX_SQL, fakeNeonDdl } from "./privacy-authority-support.ts";
import { activate, census, fence, syncOwned } from "../../migration/privacy-authority-handoff.ts";
import { assertEraserIdentity, supabaseAuthority } from "../../migration/neon-s3-erasure/authority.ts";
import { authorityUrl } from "../../migration/neon-s3-erasure/main.ts";
import { pinnedPgClient } from "../../migration/stdin-credentials.ts";

const errCode = (fn: () => unknown): string | null => {
  try {
    fn();
    return null;
  } catch (e) {
    return (e as { code?: string }).code ?? "thrown";
  }
};

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
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await pool.query(sql("supabase/bootstrap/" + f + ".sql"));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await pool.query(sql("supabase/migrations/" + f));
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
  const emptyOwned = async () => (await pool.query("SELECT sessions.ingest_privacy_authority_owned_digest() AS d")).rows[0].d;
  const activateEmpty = async (c: pg.ClientBase | pg.Pool = pool) => owner("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: await emptyNeon(), heads: [], deletions: [], resets: [], neonOwned: await emptyOwned() }, c);
  /** A second synthetic database standing in for Neon (relayhistory's privacy and owned-identity tables). */
  const neon = async () => {
    const nn = name + "_neon";
    await admin.query("CREATE DATABASE " + nn);
    const u = new URL(url!);
    u.pathname = "/" + nn;
    const npool = new pg.Pool({ connectionString: u.href, max: 4 });
    cleanup.push(async () => {
      await npool.end();
      await admin.query("DROP DATABASE " + nn + " WITH (FORCE)");
    });
    await npool.query(fakeNeonDdl());
    return npool;
  };
  return { pool, conn, owner, ingestLedger, activateEmpty, neon, url: target.href };
}

/** Resolves once `pid` is waiting on a lock (proves the race is real, not sequential). */
async function waiting(pool: pg.Pool, pid: number) {
  for (let i = 0; i < 120; i++) {
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

  test("reset coverage on real PG: sync-owned copies Neon's exact owned set (digest byte-equal), activation binds it, reset fences it", async () => {
    const d = await database();
    const neon = await d.neon();
    const nc = await neon.connect();
    const sc = await d.conn();
    try {
      const q = (c: pg.PoolClient) => ({ query: async (t: string, p?: unknown[]) => ({ rows: (await c.query(t, p as never)).rows }) });
      const m = (await nc.query("SELECT 'delivery:'||md5(jsonb_build_array($1::text,'o1')::text) AS m", [TENANT.workspaceId])).rows[0].m;
      await nc.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1','r1',$3,'claude','ONLY-IDENTITY'),($1,$2,'o1','r9','user-2','claude','OTHER')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
      await nc.query("INSERT INTO sessions.delivery_records VALUES($1,$2,'o1','r2',$3,'claude',NULL,'delete')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
      await nc.query("INSERT INTO sessions.neighborhood_claims VALUES('c1',$1,$2::jsonb)", [TENANT.orgId, JSON.stringify([{ machineId: m, source: "claude", eventId: m + ":r2", sessionId: "FROM-CLAIM" }])]);
      await fence(q(sc), epoch);
      expect(await syncOwned(q(nc), q(sc), { pageRows: 2 })).toEqual({ rows: 3, inserted: 3, removed: 0 });
      const c = await census(q(nc), q(sc), epoch);
      expect(c.ownedEqual).toBe(true);
      expect(c.supabase.owned).toEqual(c.neon.owned);
      expect((await activate(q(nc), q(sc), { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 })).activated.state).toBe("active");
      const a = await d.conn("ingest");
      const head = await call(a, "ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "cover" } });
      expect(head).toMatchObject({ deleted: "2", apply: true });
      expect((await d.pool.query("SELECT session_id FROM sessions.ingest_frozen_source_deletions ORDER BY 1")).rows.map((r) => r.session_id)).toEqual(["FROM-CLAIM", "ONLY-IDENTITY"]);
      // Neon was only read.
      expect((await nc.query("SELECT count(*)::int AS n FROM sessions.deleted_upload_sessions")).rows[0].n).toBe(0);
    } finally {
      nc.release();
    }
  });

  /** A fenced authority whose owned copy holds X, and the activation that binds exactly {X}. */
  async function ownedRace(d: Awaited<ReturnType<typeof database>>) {
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.pool.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,'o1','claude','X')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    const owned = (await d.pool.query("SELECT sessions.ingest_privacy_authority_owned_digest() AS d")).rows[0].d;
    const fp = (await d.pool.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0].f;
    const activation = { controlEpoch: epoch, neonFingerprintSha256: fp, heads: [], deletions: [], resets: [], neonOwned: owned };
    const ownedX = async () => (await d.pool.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions WHERE session_id='X'")).rows[0].n as number;
    const state = async () => (await d.pool.query("SELECT state FROM sessions.ingest_privacy_authority")).rows[0]?.state as string | undefined;
    return { activation, ownedX, state };
  }
  const pid = (c: pg.PoolClient) => (c as unknown as { processID: number }).processID;

  test("P2 race, page first: an uncommitted owned-copy DELETE holds activation off; activation then refuses the stale digest", async () => {
    const d = await database();
    const r = await ownedRace(d);
    const page = await d.conn();
    await page.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    await page.query("DELETE FROM sessions.ingest_frozen_source_owned_sessions WHERE session_id='X'");
    const o = await d.conn();
    const act = settle(call(o, "ingest_privacy_authority_activate", r.activation));
    await waiting(d.pool, pid(o));
    await page.query("COMMIT");
    expect(((await act) as { e?: string }).e).toContain("owned sessions differ from neon");
    expect(await r.state()).toBe("fenced");
  }, 30_000);

  test("P2 race, activation first: a page queued behind activation refuses after it commits; the owned set stays complete", async () => {
    const d = await database();
    const r = await ownedRace(d);
    const o = await d.conn();
    await o.query("BEGIN");
    await call(o, "ingest_privacy_authority_activate", r.activation);
    const page = await d.conn();
    const del = settle(page.query("DELETE FROM sessions.ingest_frozen_source_owned_sessions WHERE session_id='X'"));
    await waiting(d.pool, pid(page));
    await o.query("COMMIT");
    expect(((await del) as { e?: string }).e).toContain("legacy writer refused");
    expect(await r.state()).toBe("active");
    expect(await r.ownedX()).toBe(1);
  }, 30_000);

  test("P2 race: an owned-copy write in REPEATABLE READ (a snapshot that could predate activation) is refused", async () => {
    const d = await database();
    await ownedRace(d);
    const page = await d.conn();
    await page.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
    const r = await settle(page.query("DELETE FROM sessions.ingest_frozen_source_owned_sessions WHERE session_id='X'"));
    await page.query("ROLLBACK");
    expect((r as { e?: string }).e).toContain("read committed");
  }, 30_000);

  test("P2 race, stale concurrent syncs: a second sync refuses while one runs; a stale sync result makes activation refuse", async () => {
    const d = await database();
    const neon = await d.neon();
    const nc = await neon.connect();
    const s1 = await d.conn();
    const s2 = await d.conn();
    const q = (c: pg.PoolClient) => ({ query: async (t: string, p?: unknown[]) => ({ rows: (await c.query(t, p as never)).rows }) });
    const extra: pg.PoolClient[] = [];
    try {
      await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
      // Run 1 opens its Neon cursor on an empty owned set and pauses before its (only) page commits.
      let release!: () => void;
      const paused = new Promise<void>((r) => (release = r));
      let reached!: () => void;
      const atPage = new Promise<void>((r) => (reached = r));
      const nc1 = await neon.connect();
      extra.push(nc1);
      const run1 = syncOwned(q(nc1), q(s1), { hooks: { beforePageCommit: async () => { reached(); await paused; } } } as never);
      await atPage;
      // Neon gains X; a second sync would copy it, but must refuse while run 1 is in progress.
      await nc.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1','r1',$3,'claude','X')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
      const nc2 = await neon.connect();
      extra.push(nc2);
      const run2 = await settle(syncOwned(q(nc2), q(s2)));
      expect((run2 as { e?: string }).e).toContain("sync_owned_in_progress");
      release();
      await run1;
      // Run 1's result is stale (no X): activation against the current Neon refuses.
      const c = await census(q(nc), q(s2), epoch);
      expect(c.ownedEqual).toBe(false);
      expect(((await settle(activate(q(nc), q(s2), { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 }))) as { e?: string }).e).toContain("owned sessions differ from neon");
    } finally {
      for (const c of extra) c.release(true);
      nc.release();
    }
  }, 30_000);

  /**
   * A transaction-pooler stand-in over real backends: outside a transaction each statement goes to the next backend in
   * turn; BEGIN..COMMIT/ROLLBACK stays on one backend (what PgBouncer transaction mode does).
   */
  function pooler(backends: pg.PoolClient[]) {
    let next = 0;
    let pinned: pg.PoolClient | undefined;
    return {
      query: async (t: string, p?: unknown[]) => {
        const c = pinned ?? backends[next++ % backends.length]!;
        if (/^\s*BEGIN/i.test(t)) pinned = c;
        const r = await c.query(t, p as never);
        if (/^\s*(COMMIT|ROLLBACK)/i.test(t)) pinned = undefined;
        return { rows: r.rows };
      },
    };
  }
  const syncLocks = async (d: Awaited<ReturnType<typeof database>>) =>
    (await d.pool.query("SELECT count(*)::int AS n FROM pg_locks WHERE locktype='advisory' AND classid=1919249529 AND objid=97 AND granted")).rows[0].n as number;

  test("P2 session affinity: a sync whose session is reassigned between statements refuses before writing a page", async () => {
    const d = await database();
    const neon = await d.neon();
    const nc = await neon.connect();
    try {
      await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
      await nc.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1','r1',$3,'claude','X')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
      const A = await d.conn(), B = await d.conn();
      const q = { query: async (t: string, p?: unknown[]) => ({ rows: (await nc.query(t, p as never)).rows }) };
      const r = await settle(syncOwned(q, pooler([A, B])));
      expect((r as { e?: string }).e).toContain("sync_session_reassigned");
      // The guarded acquire never took the lock on a backend other than the one it checked: nothing leaks.
      expect(await syncLocks(d)).toBe(0);
      expect((await d.pool.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions")).rows[0].n).toBe(0);
    } finally {
      nc.release();
    }
  }, 30_000);

  test("P2 session affinity: a backend handed to a second operator mid-sync cannot let both syncs enter", async () => {
    const d = await database();
    const neon = await d.neon();
    const n1 = await neon.connect(), n2 = await neon.connect();
    try {
      await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
      for (const sid of ["S1", "S2"])
        await n1.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1',$3,$4,'claude',$3)", [TENANT.orgId, TENANT.workspaceId, sid, TENANT.userId]);
      const A = await d.conn();
      // Run 1 keeps backend A (its lock lives there) and pauses BETWEEN pages, so A is idle;
      // the pooler then hands the same A to run 2, where the session lock is reentrant.
      const shared = pooler([A]);
      let release!: () => void;
      const paused = new Promise<void>((r) => (release = r));
      let reached!: () => void;
      const between = new Promise<void>((r) => (reached = r));
      // Run 1's view of the pooler: after its first page COMMIT (A idle, lock still held on A), it stalls.
      let commits = 0;
      const run1View = {
        query: async (t: string, p?: unknown[]) => {
          const r = await shared.query(t, p);
          if (/^\s*COMMIT/i.test(t) && ++commits === 1) { reached(); await paused; }
          return r;
        },
      };
      const q = (c: pg.PoolClient) => ({ query: async (t: string, p?: unknown[]) => ({ rows: (await c.query(t, p as never)).rows }) });
      const run1 = settle(syncOwned(q(n1), run1View, { pageRows: 1 }));
      await between;
      const run2 = await settle(syncOwned(q(n2), shared));
      expect((run2 as { e?: string }).e).toContain("sync_owned_in_progress");
      release();
      expect(await run1).toMatchObject({ ok: true });
      expect(await syncLocks(d)).toBe(0);
      expect((await d.pool.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions")).rows[0].n).toBe(2);
    } finally {
      n1.release();
      n2.release();
    }
  }, 30_000);

  test("P1 on real PG: the eraser role reads only through its four RPCs, no table; the generated verifier passes", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.activateEmpty();
    const e = await d.conn();
    await e.query("SET ROLE bulls96_privacy_eraser");
    for (const t of ["ingest_frozen_source_deletions", "upload_deletions", "ingest_privacy_authority", "ingest_frozen_source_owned_sessions"])
      expect(((await settle(e.query(`SELECT 1 FROM sessions.${t} LIMIT 1`))) as { e?: string }).e).toContain("permission denied");
    expect((await e.query("SELECT sessions.ingest_privacy_eraser_state() AS r")).rows[0].r).toEqual({ active: true });
    expect((await e.query("SELECT sessions.ingest_privacy_eraser_page($1::jsonb) AS r", [JSON.stringify({ after: null, limit: 10 })])).rows[0].r).toEqual([]);
    expect(((await settle(e.query("SELECT sessions.ingest_privacy_authority_state()"))) as { e?: string }).e).toContain("permission denied");
    await e.query("RESET ROLE");
    await d.pool.query(readFileSync(join(root, "supabase/bootstrap/privacy_eraser_verify.sql"), "utf8"));
  }, 30_000);

  test("P1 on real PG: only an AUTHENTICATED bulls96_privacy_eraser login passes; SET ROLE / SET SESSION AUTHORIZATION / startup options are refused", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.activateEmpty();
    // Operator phase (LOGIN + timeouts), then a TEST-ONLY password on this owned synthetic container (scram-sha-256).
    await d.pool.query(readFileSync(join(root, "supabase/bootstrap/privacy_eraser_role.sql"), "utf8"));
    await d.pool.query("ALTER ROLE bulls96_privacy_eraser PASSWORD 'c3-test-only-synthetic'");
    cleanup.push(async () => {
      await d.pool.query("ALTER ROLE bulls96_privacy_eraser NOLOGIN PASSWORD NULL; ALTER ROLE bulls96_privacy_eraser RESET ALL").catch(() => undefined);
    });
    await d.pool.query(readFileSync(join(root, "supabase/bootstrap/privacy_eraser_verify.sql"), "utf8"));
    const client = async (config: pg.ClientConfig) => {
      const c = new pg.Client(config);
      await c.connect();
      cleanup.push(() => c.end().catch(() => undefined));
      return { c, q: { query: async (t: string, p?: unknown[]) => ({ rows: (await c.query(t, p as never)).rows }) } };
    };
    const as = (user: string, password: string) => {
      const u = new URL(d.url);
      u.username = user;
      u.password = password;
      return u.href;
    };
    const auth = (await d.pool.query("SELECT current_setting('server_version_num')::int AS v")).rows[0].v;
    expect(auth).toBeGreaterThanOrEqual(160000);
    // Genuine authenticated login: accepted, reads through its RPCs, no table.
    const login = await client({ connectionString: as("bulls96_privacy_eraser", "c3-test-only-synthetic") });
    expect((await login.c.query("SELECT system_user::text AS s")).rows[0].s).toBe("scram-sha-256:bulls96_privacy_eraser");
    await assertEraserIdentity(login.q);
    expect(await supabaseAuthority(login.q).fencedLivePage(null)).toEqual([]);
    expect(((await settle(login.c.query("SELECT 1 FROM sessions.upload_deletions LIMIT 1"))) as { e?: string }).e).toContain("permission denied");
    // Causal negatives (accepted on 70c03606): the superuser's own credential with a forged session identity.
    const su = new URL(d.url);
    // Exactly the reviewed vector: the startup `options` parameter inside the connection URL (node-postgres forwards it).
    const forged = new URL(d.url);
    forged.searchParams.set("options", "-c session_authorization=bulls96_privacy_eraser");
    // The effective bypass: SET SESSION AUTHORIZATION rewrites BOTH session_user and current_user (70c03606 accepted it).
    const viaSet = await client({ connectionString: su.href });
    await viaSet.c.query("SET SESSION AUTHORIZATION bulls96_privacy_eraser");
    expect((await viaSet.c.query("SELECT session_user::text AS s, current_user::text AS c")).rows[0]).toEqual({ s: "bulls96_privacy_eraser", c: "bulls96_privacy_eraser" });
    expect(await assertEraserIdentity(viaSet.q).then(() => "ok", (e) => e.code)).toBe("eraser_identity_not_authenticated_as_privacy_eraser");
    await viaSet.c.query("RESET SESSION AUTHORIZATION");
    expect((await viaSet.c.query("SELECT rolsuper FROM pg_roles WHERE rolname=session_user")).rows[0].rolsuper).toBe(true);
    // (PostgreSQL 17 does not honour session_authorization from the startup packet, so session_user may stay the
    // superuser; whether or not a server honours it, the authenticated identity is the superuser's: refused. The URL
    // itself is refused before connecting by eraserDatabaseUrl, covered in neon-s3-erasure-authority.test.ts.)
    const viaOptions = await client({ connectionString: forged.href });
    expect(await assertEraserIdentity(viaOptions.q).then(() => "ok", (e) => e.code)).toBe("eraser_identity_not_authenticated_as_privacy_eraser");
    const viaRole = await client({ connectionString: su.href });
    await viaRole.c.query("SET ROLE bulls96_privacy_eraser");
    expect(await assertEraserIdentity(viaRole.q).then(() => "ok", (e) => e.code)).toBe("eraser_identity_not_authenticated_as_privacy_eraser");
  }, 30_000);

  test("P2 on real PG: a password-less eraser URL reaches pg's pgpass fallback; the stdin guard and pinned client never do", async () => {
    const d = await database();
    await d.owner("ingest_privacy_authority_fence", { controlEpoch: epoch });
    await d.activateEmpty();
    await d.pool.query(readFileSync(join(root, "supabase/bootstrap/privacy_eraser_role.sql"), "utf8"));
    await d.pool.query("ALTER ROLE bulls96_privacy_eraser PASSWORD 'c3-test-only-synthetic'");
    cleanup.push(async () => {
      await d.pool.query("ALTER ROLE bulls96_privacy_eraser NOLOGIN PASSWORD NULL; ALTER ROLE bulls96_privacy_eraser RESET ALL").catch(() => undefined);
    });
    // An in-memory sentinel in place of pg's bundled pgpass module (no credential file anywhere): it records every
    // fallback attempt and supplies nothing.
    const req = createRequire(join(root, "package.json"));
    const fromPg = createRequire(req.resolve("pg"));
    const pgpassPath = fromPg.resolve("pgpass");
    fromPg("pgpass");
    const entry = (req.cache as Record<string, { exports: unknown }>)[pgpassPath]!;
    const real = entry.exports;
    let fallbacks = 0;
    entry.exports = (_params: unknown, cb: (pass?: string) => void) => {
      fallbacks += 1;
      cb(undefined);
    };
    try {
      const u = new URL(d.url);
      u.username = "bulls96_privacy_eraser";
      u.password = "";
      const passwordless = u.href;
      // The vector (ff29da7f's construction): pg reaches its pgpass fallback during SCRAM auth for this URL.
      const raw = new pg.Client({ connectionString: passwordless });
      expect(await raw.connect().then(() => "connected", () => "refused")).toBe("refused");
      await raw.end().catch(() => undefined);
      expect(fallbacks).toBe(1);
      // The E3 guards refuse it by name before any Client exists.
      expect(errCode(() => authorityUrl("supabase", passwordless))).toBe("database_url_password_required");
      expect(errCode(() => pinnedPgClient(pg, passwordless, "databaseUrl", "supabase"))).toBe("database_url_password_required");
      // The genuine narrow-role SCRAM login through the pinned DISCRETE config (percent-encoded password in the URL):
      await d.pool.query("ALTER ROLE bulls96_privacy_eraser PASSWORD 'c3@test:only/synthetic%'");
      const good = new URL(d.url);
      good.username = "bulls96_privacy_eraser";
      good.password = encodeURIComponent("c3@test:only/synthetic%");
      good.searchParams.set("sslmode", "disable"); // loopback test container: the only place disable is accepted
      const ok = pinnedPgClient(pg, authorityUrl("supabase", good.href), "databaseUrl", "supabase");
      await ok.connect();
      try {
        expect((await ok.query("SELECT system_user::text AS s")).rows[0].s).toBe("scram-sha-256:bulls96_privacy_eraser");
        await assertEraserIdentity({ query: async (t: string, p?: unknown[]) => ({ rows: (await ok.query(t, p as never)).rows }) });
      } finally {
        await ok.end();
      }
      expect(fallbacks).toBe(1);
    } finally {
      entry.exports = real;
    }
  }, 30_000);
});
