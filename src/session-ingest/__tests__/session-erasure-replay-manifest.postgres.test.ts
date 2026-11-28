/**
 * 20261105062360 session erasure replay manifest on real PostgreSQL (SESSION_INGEST_TEST_PG_URL; run.ts gives disposable
 * 15/16/17), independent connections, every block proven from pg_locks (hold-harness), never from timing.
 *   1 the capture commits with the erase: while the erase transaction is open another connection reads no manifest
 *     (and is not blocked by it); after COMMIT the complete sealed set; a ROLLBACK leaves neither effects nor witness
 *   2 two first calls for one key: the second waits on the erase gate, then repeats; one manifest, the first request id
 *   3 a caught RH010 keeps the fences and the intent, unsealed; a retry with the same, a larger or a smaller closure
 *     after the locks unwound fences with the existing derivation and never seals
 *   4 org SHARED precedes the global erase gate (961996,50000) on BOTH entry paths, direct owner _session and
 *     runtime_begin as a real ingest login: behind an open install (org X) the waiter waits on org S holding no gate;
 *     behind a held gate it already holds org S; nothing leaks past either commit; the live body has the check first
 *   6 the child and seal guards refuse any isolation but READ COMMITTED (stale REPEATABLE READ insert-after-seal and
 *     seal-from-old-snapshot are refused 40001); a READ COMMITTED seal waits for an in-flight child, then recounts
 *   5 the down keeps a populated witness (the reviewed shape on this major), a reapply reuses it; an empty manifest's
 *     down drops it with the receipts binding
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import pg from "pg";
import { readFileSync } from "node:fs";
import { fixturePgUrl, migrateAsOwner, rollBackLaterSlots, scratchDatabase, type ScratchDatabase } from "./hold-harness/pg.ts";
import { expectBlocks, expectNotBlocked, openPair, type Pair } from "./hold-harness/two-conn.ts";
import { ReplayDigest, type ReplayTarget } from "../../hoopdb/session-erasure-replay-digest.ts";

setDefaultTimeout(300_000);
const url = fixturePgUrl();
const ORG = "org-manifest", WS = "ws", SRC = "claude", SID = "s-1";
const TURNS = "relayhistory:conversation_turns:v1";
const DOWN = readFileSync("supabase/rollbacks/20261105062360_session_erasure_replay_manifest.down.sql", "utf8");
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanups.splice(0).reverse()) await c().catch(() => {}); }, 120_000);

async function database(): Promise<ScratchDatabase> {
  const db = await scratchDatabase(url!, { bootstrap: true, prefix: "rmanif" });
  cleanups.push(() => db.drop());
  await migrateAsOwner(db.ownerUrl);
  return db;
}
async function pair(db: ScratchDatabase): Promise<Pair> {
  const p = await openPair({ url: db.ownerUrl, observerUrl: db.fixtureUrl });
  cleanups.push(() => p.close());
  return p;
}
async function owner(db: ScratchDatabase) {
  const c = new pg.Client({ connectionString: db.ownerUrl }); await c.connect();
  cleanups.push(() => c.end());
  return c;
}
type Q = { query: (text: string, params?: unknown[]) => Promise<pg.QueryResult> };
const pointer = (c: Q, user: string, session: string, machine: string, canonical: string | null = null) => c.query(
  `INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
     storage_provider,bucket,manifest_key,content_sha256,raw_format,trace_id,revision,canonical_session_id)
   VALUES($1,$2,$3,$4,$5,$6,now(),now(),'s3','bucket','k/'||$4||$5,decode(repeat('ab',32),'hex'),'jsonl','019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18',2,$7)`,
  [ORG, WS, SRC, session, machine, user, canonical]);
async function directoryUser(c: Q, session: string, user: string) {
  await c.query(`INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at)
    VALUES($1,$2,$3,$4,now(),now()) ON CONFLICT DO NOTHING`, [ORG, WS, SRC, session]);
  await c.query(`INSERT INTO sessions.session_directory_users(org_id,workspace_id,harness,session_id,user_id,last_activity_at)
    VALUES($1,$2,$3,$4,$5,now())`, [ORG, WS, SRC, session, user]);
}
/** Owners from pointers (two machines, one alias) and the directory: a 3-owner, 2-alias, turns-origin set. */
async function seed(c: Q) {
  await pointer(c, "u-b", SID, "m-1");
  await pointer(c, "U-a", "s-alias", "m-2", SID);
  await directoryUser(c, SID, "u-d");
}
const eraseSql = "SELECT sessions.ingest_operator_erase_session($1,$2,$3,$4,$5,'user','operator-1') AS r";
const erase = (c: Q, request = "req-1", session = SID) => c.query(eraseSql, [ORG, WS, SRC, session, request]);
async function read(c: Q, fence: string, cursor: unknown = null, size = 512) {
  await c.query("BEGIN READ ONLY");
  try {
    return (await c.query("SELECT sessions.session_erasure_replay_read($1,$2,$3,$4,$5::jsonb,$6) p",
      [ORG, WS, SRC, fence, cursor === null ? null : JSON.stringify(cursor), size])).rows[0].p as Record<string, unknown> & { targets: ReplayTarget[] };
  } finally { await c.query("COMMIT"); }
}
/** Drain and validate as the bridge's preparation pass: same metadata on every page, keyset order, count, digest. */
async function drain(c: Q, fence: string, size: number) {
  const first = await read(c, fence, null, size);
  expect(first.complete).toBe(true);
  const scope = first.scope as { org_id: string; workspace_id: string; source: string; requested_session_id: string };
  const d = new ReplayDigest({ capture_version: 1, manifest_id: first.manifest_id as string, request_id: first.request_id as string, ...scope },
    first.alias_ids as string[], Number(first.target_count));
  const all: ReplayTarget[] = [];
  let p = first;
  for (;;) {
    expect({ ...p, targets: 0, next_cursor: 0 }).toEqual({ ...first, targets: 0, next_cursor: 0 });
    for (const t of p.targets) { d.push(t); all.push(t); }
    if (p.next_cursor === null) break;
    p = await read(c, fence, p.next_cursor, size);
  }
  expect(d.hex()).toBe(first.manifest_sha256 as string);
  return { first, all };
}
/** The erase never seals (F1 ruling (c)); the TEST OWNER seals captured manifests to exercise seal, guards and reader. */
const ownerSeal = async (c: Q) => {
  await c.query('ALTER TABLE sessions.session_erasure_replay_completions DISABLE TRIGGER session_erasure_replay_seal');
  try { await c.query(`INSERT INTO sessions.session_erasure_replay_completions(manifest_id,org_id,workspace_id,source,derivation_version,alias_count,owner_count,pair_count,target_count,manifest_sha256)
  SELECT r.manifest_id,r.org_id,r.workspace_id,r.source,1,(SELECT count(*) FROM sessions.session_erasure_replay_aliases a WHERE a.manifest_id=r.manifest_id),
    count(DISTINCT t.user_id),count(DISTINCT (t.user_id,t.origin_id)),count(*),sessions.session_erasure_replay_digest(r.manifest_id)
  FROM sessions.session_erasure_replay_requests r JOIN sessions.session_erasure_replay_targets t ON t.manifest_id=r.manifest_id
  WHERE NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_completions c WHERE c.manifest_id=r.manifest_id)
  GROUP BY r.manifest_id,r.org_id,r.workspace_id,r.source`); } finally { await c.query('ALTER TABLE sessions.session_erasure_replay_completions ENABLE TRIGGER session_erasure_replay_seal'); }
};
const n = async (c: Q, sql: string) => Number((await c.query(sql)).rows[0].n);
/** The retained witness stays owner-only and immutable: no runtime role reads or writes it, no row is mutable. */
async function retainedSafe(c: Q) {
  for (const role of ["bulls96_session_ingest", "authenticated", "anon", "service_role"]) {
    const r = (await c.query(`SELECT bool_or(has_table_privilege($1,'sessions.'||t,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')) t,
        has_function_privilege($1,'sessions.session_erasure_replay_read(text,text,text,text,jsonb,integer)','EXECUTE') f,
        has_function_privilege($1,'sessions.session_erasure_replay_digest(uuid)','EXECUTE') d
      FROM unnest(ARRAY['session_erasure_replay_requests','session_erasure_replay_aliases','session_erasure_replay_targets',
        'session_erasure_replay_completions']) AS t`, [role])).rows[0];
    expect([role, r]).toEqual([role, { t: false, f: false, d: false }]);
  }
  await c.query("BEGIN");
  await expect(c.query("DELETE FROM sessions.session_erasure_replay_targets")).rejects.toMatchObject({ code: "55000" });
  await c.query("ROLLBACK");
}

describe.skipIf(!url)("062360 replay manifest on real PostgreSQL", () => {
  test("1. the capture commits with the erase effects, never before; a rollback leaves neither", async () => {
    const db = await database();
    const o = await owner(db);
    await seed(o);
    const p = await pair(db);
    // Reader in another connection while the erase is open: not blocked, and no manifest yet.
    const r = await expectNotBlocked(p, { holder: p.a, waiter: p.b, hold: (c) => erase(c), wait: (c) => c.query(
      "SELECT sessions.session_erasure_replay_read($1,$2,$3,$4) p", [ORG, WS, SRC, SID]) });
    expect((r.waited as { ok: true; value: pg.QueryResult }).value.rows[0].p.reason).toBe("manifest_missing");
    // The holder committed after the read: captured, never sealed by the erase (F1 ruling (c)).
    expect((await read(o, "s-alias")).reason).toBe("manifest_unsealed");
    await ownerSeal(o);
    const { first, all } = await drain(o, "s-alias", 2);
    const effects = (await o.query(`SELECT user_id,origin_id,source,session_id FROM sessions.upload_deletions WHERE org_id=$1
      ORDER BY user_id COLLATE "C",origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"`, [ORG])).rows;
    expect(all).toEqual(effects);
    expect(all).toEqual(["U-a", "u-b", "u-d"].flatMap((u) => [SID, "s-alias"].map((s) => ({ user_id: u, origin_id: TURNS, source: SRC, session_id: s })))
      .sort((x, y) => Buffer.compare(Buffer.from(x.user_id + "\0" + x.session_id), Buffer.from(y.user_id + "\0" + y.session_id))));
    expect(first.target_count).toBe("6");
    // A rolled-back first erase of another key commits nothing: no receipt, fence, manifest or selection.
    await pointer(o, "u-r", "s-rb", "m-r");
    await o.query("BEGIN");
    await erase(o, "req-rb", "s-rb");
    await o.query("ROLLBACK");
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_receipts WHERE session_id='s-rb'")).toBe(0);
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_aliases WHERE alias_session_id='s-rb'")).toBe(0);
    expect(await n(o, "SELECT count(*)::int n FROM sessions.upload_deletions WHERE session_id='s-rb'")).toBe(0);
    // Rolled back entirely, so a new call is a genuinely fresh first attempt and records its targets (unsealed).
    await erase(o, "req-rb", "s-rb");
    expect((await read(o, "s-rb")).reason).toBe("manifest_unsealed");
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets t JOIN sessions.session_erasure_replay_requests r USING(manifest_id) WHERE r.requested_session_id='s-rb'")).toBeGreaterThan(0);
  });

  test("2. two first calls for one key: the second waits on the erase gate, then repeats; one manifest, the first request", async () => {
    const db = await database();
    const o = await owner(db);
    await seed(o);
    const p = await pair(db);
    const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => erase(c, "req-first"), wait: (c) => erase(c, "req-second") });
    expect(r.receipt.waiting[0]).toMatchObject({ locktype: "advisory" });
    expect(r.waited.ok).toBe(true);
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_requests")).toBe(1);
    expect((await o.query("SELECT request_id FROM sessions.session_erasure_replay_requests")).rows[0].request_id).toBe("req-first");
  });

  test("3. a caught RH010 keeps fences and intent unsealed; same, larger and smaller retries never seal", async () => {
    for (const change of ["same", "larger", "smaller"] as const) {
      const db = await database();
      const o = await owner(db);
      await pointer(o, "u-0", SID, "m-0");
      if (change === "smaller") await pointer(o, "u-0", "s-gone", "m-9", SID);
      for (let i = 1; i <= 65; i++) await directoryUser(o, SID, `u-${String(i).padStart(2, "0")}`);
      expect((await erase(o)).rows[0].r).toMatchObject({ status: "failed", failure: { code: "RH010" } });
      expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_requests")).toBe(1);
      expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets")).toBe(0);
      expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_fences")).toBeGreaterThan(0);
      // Between attempts the owner locks are gone: owners, origins and aliases change.
      await o.query("DELETE FROM sessions.session_directory_users WHERE user_id<>'u-01'");
      if (change === "larger") await pointer(o, "u-0", "s-new", "m-8", SID);
      if (change === "smaller") await o.query("DELETE FROM sessions.session_index WHERE session_id='s-gone'");
      expect((await erase(o)).rows[0].r).toMatchObject({ status: "in_progress", uploadsFenced: true, failure: null });
      expect((await read(o, SID)).reason).toBe("manifest_unsealed");
      expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toBe(0);
      expect(await n(o, "SELECT count(*)::int n FROM sessions.upload_deletions")).toBeGreaterThan(0);
      // The role verifier is cluster-wide: one scratch database at a time.
      await o.end(); await db.drop();
    }
  });

  test("4. org SHARED precedes the global erase gate on both entry paths (direct owner _session, runtime_begin)", async () => {
    const db = await database();
    const o = await owner(db);
    await seed(o);
    const live = (await o.query("SELECT prosrc s FROM pg_proc WHERE oid='sessions.ingest_operator_erase_session(text,text,text,text,text,text,text)'::regprocedure")).rows[0].s as string;
    const lines = live.split("\n").map((l) => l.trim());
    expect(lines[lines.indexOf("BEGIN") + 1]).toBe("PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold");
    const p = await pair(db);
    // A real ingest-role login for runtime_begin (the NOINHERIT owner cannot SET ROLE).
    const su = new pg.Client({ connectionString: db.fixtureUrl }); await su.connect(); cleanups.push(() => su.end());
    const worker = `worker_erase_${db.name.slice(-8)}`;
    await su.query(`CREATE ROLE ${worker} LOGIN IN ROLE bulls96_session_ingest`);
    cleanups.push(() => su.query(`DROP ROLE IF EXISTS ${worker}`));
    const u = new URL(db.fixtureUrl); u.username = worker;
    const w = await openPair({ url: u.href, observerUrl: db.fixtureUrl });
    cleanups.push(() => w.close());
    const orgKey = BigInt.asUintN(64, BigInt((await o.query("SELECT sessions.ingest_org_admission_hold_key($1)::text k", [ORG])).rows[0].k));
    const [kc, ko] = [String(orgKey >> 32n), String(orgKey & 0xffffffffn)];
    const locks = async (pid: number) => (await p.observer.query(
      `SELECT CASE WHEN classid=$2::bigint::oid AND objid=$3::bigint::oid AND objsubid=1 THEN 'org' WHEN classid=961996 AND objid=50000 AND objsubid=2 THEN 'global' END k,
              mode,granted FROM pg_locks WHERE pid=$1 AND locktype='advisory' ORDER BY 1`, [pid, kc, ko])).rows.filter((r) => r.k !== null);
    const install = (c: Q) => c.query("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG]);
    const gate = (c: Q) => c.query("SELECT pg_advisory_xact_lock(961996,50000)");
    const direct = (session: string, request: string) => (c: Q) => erase(c, request, session);
    const runtime = (session: string, request: string) => async (c: Q) => {
      await c.query("SET LOCAL ROLE bulls96_session_ingest");
      return c.query("SELECT sessions.ingest_operator_runtime_begin($1,$2,$3,$4,$5,'user','operator-1','bucket','root',$6) r",
        [ORG, WS, SRC, session, request, "f".repeat(64)]);
    };
    const paths = [{ name: "direct", waiter: p.b, op: direct }, { name: "runtime", waiter: w.a, op: runtime }] as const;
    let i = 0;
    for (const path of paths) {
      // Direction 1: A holds the org key EXCLUSIVE (an open install). B waits on the org key and holds no global gate.
      i++;
      const d1 = await expectBlocks(p, { holder: p.a, waiter: path.waiter, hold: install, wait: path.op(`s-org-${i}`, `req-org-${i}`), holderEnd: "rollback",
        whileBlocked: async () => {
          expect(await locks(path.waiter.pid)).toEqual([{ k: "org", mode: "ShareLock", granted: false }]);
        } });
      expect([path.name, d1.waited.ok]).toEqual([path.name, true]);
      expect(await locks(path.waiter.pid)).toEqual([]); // nothing leaks past the waiter's commit
      expect((await read(o, `s-org-${i}`)).reason).toBe("manifest_unsealed"); // no owners: never sealed, but captured as intent
      // Direction 2: A holds the global gate. B already holds the org key SHARED while it waits for the gate.
      i++;
      const d2 = await expectBlocks(p, { holder: p.a, waiter: path.waiter, hold: gate, wait: path.op(`s-gate-${i}`, `req-gate-${i}`),
        whileBlocked: async () => {
          expect(await locks(path.waiter.pid)).toEqual([{ k: "global", mode: "ExclusiveLock", granted: false }, { k: "org", mode: "ShareLock", granted: true }]);
        } });
      expect([path.name, d2.waited.ok]).toEqual([path.name, true]);
      expect(await locks(path.waiter.pid)).toEqual([]);
      expect(await locks(p.a.pid)).toEqual([]);
    }
    // A capture after the gates are free: a seeded session, the direct path; captured, never sealed by the erase.
    await erase(o, "req-seeded");
    expect((await read(o, SID)).reason).toBe("manifest_unsealed");
  });

  test("6. the guards hold under any isolation: stale REPEATABLE READ inserts and seals are refused; READ COMMITTED races serialize", async () => {
    const db = await database();
    const o = await owner(db);
    // An unsealed manifest built directly by the owner: receipt, fence, intent, one alias, one target.
    const M = "6f1e2d3c-4b5a-4968-8776-655443322110";
    await o.query(`INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id)
      SELECT $1,$2,$3,'s-m','req-m',jsonb_object_agg(k,jsonb_build_object('count',0,'state','pending')),'user','t'
      FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k`, [ORG, WS, SRC]);
    await o.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,'s-m')", [ORG, WS, SRC]);
    await o.query("INSERT INTO sessions.session_erasure_replay_requests(manifest_id,request_id,org_id,workspace_id,source,requested_session_id,capture_version) VALUES($4,'req-m',$1,$2,$3,'s-m',1)", [ORG, WS, SRC, M]);
    await o.query("INSERT INTO sessions.session_erasure_replay_aliases VALUES($4,$1,$2,$3,'s-m')", [ORG, WS, SRC, M]);
    const target = (u: string) => ["INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,$4,'o',$5,'s-m')", [M, ORG, WS, u, SRC]] as const;
    await o.query(...target("u-1"));
    const seal = async (c: Q) => {
      const x = (await c.query(`SELECT count(DISTINCT user_id)::int ow,count(*)::int t,sessions.session_erasure_replay_digest($1) h
        FROM sessions.session_erasure_replay_targets WHERE manifest_id=$1`, [M])).rows[0];
      return c.query("INSERT INTO sessions.session_erasure_replay_completions VALUES($1,$2,$3,$4,1,1,$5::int,$5::bigint,$6::bigint,$7)", [M, ORG, WS, SRC, x.ow, x.t, x.h]);
    };
    const p = await pair(db);
    // (b) A seal computed from an old REPEATABLE READ snapshot, after another transaction added a target: refused 40001.
    await p.a.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
    await p.a.query("SELECT count(*) FROM sessions.session_erasure_replay_targets"); // the snapshot
    await o.query(...target("u-2"));
    await expect(seal(p.a)).rejects.toMatchObject({ code: "40001" });
    await p.a.query("ROLLBACK");
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toBe(0);
    // (a) A target inserted from an old REPEATABLE READ snapshot after another transaction sealed: refused 40001.
    await p.a.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
    await p.a.query("SELECT count(*) FROM sessions.session_erasure_replay_completions"); // the snapshot, before the seal
    await o.query('ALTER TABLE sessions.session_erasure_replay_completions DISABLE TRIGGER session_erasure_replay_seal');
    await seal(o); // a matching seal, made by the test owner (the trigger refuses every seal in this slot)
    await o.query('ALTER TABLE sessions.session_erasure_replay_completions ENABLE TRIGGER session_erasure_replay_seal');
    await expect(p.a.query(...target("u-3"))).rejects.toMatchObject({ code: "40001" });
    await p.a.query("ROLLBACK");
    for (const iso of ["SERIALIZABLE", "REPEATABLE READ"]) {
      await p.a.query(`BEGIN ISOLATION LEVEL ${iso}`);
      await expect(p.a.query(...target("u-4"))).rejects.toMatchObject({ code: "40001" });
      await p.a.query("ROLLBACK");
    }
    // The sealed manifest is still exactly consistent: two targets, counts and digest match.
    const page = await read(o, "s-m");
    expect(page).toMatchObject({ complete: true, target_count: "2" });
    // The guards bind only mutations: the reader keeps working in REPEATABLE READ and SERIALIZABLE READ ONLY transactions
    // (a bridge drain can pin one snapshot across pages), with identical pages.
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      await p.a.query(`BEGIN ISOLATION LEVEL ${iso} READ ONLY`);
      const q = (await p.a.query("SELECT sessions.session_erasure_replay_read($1,$2,$3,'s-m') p", [ORG, WS, SRC])).rows[0].p;
      await p.a.query("COMMIT");
      expect([iso, q]).toEqual([iso, page]);
    }
    // READ COMMITTED race, the seal first: a child insert waits on the parent lock, then sees the seal (55000).
    await o.query("INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id) VALUES($1,$2,$3,'s-n')", [ORG, WS, SRC]);
    await o.query(`INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id)
      SELECT $1,$2,$3,'s-n','req-n',artifacts,actor_kind,actor_id FROM sessions.session_erasure_receipts WHERE session_id='s-m'`, [ORG, WS, SRC]);
    const M2 = "7f2e3d4c-5b6a-4a79-9887-766554433221";
    await o.query("INSERT INTO sessions.session_erasure_replay_requests(manifest_id,request_id,org_id,workspace_id,source,requested_session_id,capture_version) VALUES($4,'req-n',$1,$2,$3,'s-n',1)", [ORG, WS, SRC, M2]);
    await o.query("INSERT INTO sessions.session_erasure_replay_aliases VALUES($4,$1,$2,$3,'s-n')", [ORG, WS, SRC, M2]);
    await o.query("INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,'u-1','o',$4,'s-n')", [M2, ORG, WS, SRC]);
    const seal2 = (c: Q) => c.query(`INSERT INTO sessions.session_erasure_replay_completions
      SELECT $1,$2,$3,$4,1,1,1,1,1,sessions.session_erasure_replay_digest($1)`, [M2, ORG, WS, SRC]);
    const child2 = (u: string) => (c: Q) => c.query("INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,$4,'o',$5,'s-n')", [M2, ORG, WS, u, SRC]);
    // The seal waits for an in-flight child's parent lock, whether the child then rolls back or commits, and is refused
    // either way (fail-closed): no seal ever commits against a moving set.
    // The guard validates BEFORE its fail-closed refusal, so the refusal message shows what the waiting recount saw:
    //   child rolled back: the 1-target seal still matches its rows -> refused only for want of an alias proof;
    //   child COMMITTED:   the waiter recounts the committed second target -> the stale 1-target seal does not match.
    const PROOF = "session erasure replay manifest: sealing requires an authoritative alias-coverage proof";
    const STALE = "session erasure replay manifest: seal does not match its rows";
    for (const [end, u, why] of [["rollback", "u-9", PROOF], ["commit", "u-8", STALE]] as const) {
      const r = await expectBlocks(p, { holder: p.a, waiter: p.b, hold: (c) => child2(u)(c), wait: seal2, holderEnd: end });
      expect([end, r.receipt.waiting[0]?.locktype]).toEqual([end, "transactionid"]);
      expect([end, r.holderEnded.ok]).toEqual([end, true]);
      expect([end, r.waited]).toMatchObject([end, { ok: false, error: { code: "55000", message: why } }]);
    }
    // A fresh seal over the two committed targets validates (counts and digest match), and is then refused only for
    // want of an alias proof.
    await expect(o.query(`INSERT INTO sessions.session_erasure_replay_completions
      SELECT $1,$2,$3,$4,1,1,2,2,2,sessions.session_erasure_replay_digest($1)`, [M2, ORG, WS, SRC])).rejects.toMatchObject({ code: "55000", message: PROOF });
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_targets WHERE manifest_id='" + M2 + "'")).toBe(2); // u-1 and the committed u-8
    await o.query('ALTER TABLE sessions.session_erasure_replay_completions DISABLE TRIGGER session_erasure_replay_seal');
    await o.query(`INSERT INTO sessions.session_erasure_replay_completions
      SELECT $1,$2,$3,$4,1,1,2,2,2,sessions.session_erasure_replay_digest($1)`, [M2, ORG, WS, SRC]);
    await o.query('ALTER TABLE sessions.session_erasure_replay_completions ENABLE TRIGGER session_erasure_replay_seal');
    // Now sealed: a later READ COMMITTED child sees the seal and is refused.
    await expect(o.query("INSERT INTO sessions.session_erasure_replay_targets VALUES($1,$2,$3,'u-7','o',$4,'s-n')", [M2, ORG, WS, SRC])).rejects.toMatchObject({ code: "55000" });
    // After every transaction above: each persisted seal equals its rows (counts and digest recomputed).
    const drift = await n(o, `SELECT count(*)::int n FROM sessions.session_erasure_replay_completions c WHERE
      c.target_count<>(SELECT count(*) FROM sessions.session_erasure_replay_targets t WHERE t.manifest_id=c.manifest_id)
      OR c.owner_count<>(SELECT count(DISTINCT user_id) FROM sessions.session_erasure_replay_targets t WHERE t.manifest_id=c.manifest_id)
      OR c.manifest_sha256<>sessions.session_erasure_replay_digest(c.manifest_id)`);
    expect(drift).toBe(0);
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toBe(2);
  });

  test("5. down: a populated witness is kept in the reviewed shape and reused by a reapply; an empty one is dropped", async () => {
    const db = await database();
    const o = await owner(db);
    await seed(o);
    await erase(o);
    await ownerSeal(o);
    const sealed = await drain(o, SID, 512);
    await rollBackLaterSlots(db.ownerUrl, "20261105062360_session_erasure_replay_manifest");
    await o.query(DOWN);
    expect(await n(o, "SELECT count(*)::int n FROM sessions.session_erasure_replay_completions")).toBe(1);
    expect(await drain(o, SID, 512)).toEqual(sealed);
    await retainedSafe(o);
    // The erase parents cannot be dropped out from under the witness: no CASCADE, no bypass (2BP01).
    for (const t of ["session_erasure_receipts", "session_erasure_fences"]) {
      await o.query("BEGIN");
      await expect(o.query(`DROP TABLE sessions.${t}`)).rejects.toMatchObject({ code: "2BP01" });
      await o.query("ROLLBACK");
    }
    await migrateAsOwner(db.ownerUrl); // the up's shape check passes on this major over the retained tables
    expect(await drain(o, SID, 512)).toEqual(sealed);
    await retainedSafe(o);
    expect((await o.query("SELECT sessions.ingest_org_erasure_history_recording() r")).rows[0].r).toBe(true);
    await o.end(); await db.drop(); // the role verifier is cluster-wide: one scratch database at a time
    const empty = await database();
    const e = await owner(empty);
    await rollBackLaterSlots(empty.ownerUrl, "20261105062360_session_erasure_replay_manifest");
    await e.query(DOWN);
    expect((await e.query("SELECT to_regclass('sessions.session_erasure_replay_requests') r")).rows[0].r).toBeNull();
    expect(await n(e, "SELECT count(*)::int n FROM pg_constraint WHERE conname='session_erasure_receipts_request_binding'")).toBe(0);
    // Nothing of 062360 is left depending on the 050000 tables: an older down's DROP TABLE is not stranded.
    // (Every remaining dependent of the two tables is a 050000/050020 object, dropped before them in the real order.)
    expect(await n(e, `SELECT count(*)::int n FROM pg_constraint k WHERE k.confrelid IN('sessions.session_erasure_receipts'::regclass,
      'sessions.session_erasure_fences'::regclass) AND k.conrelid::regclass::text NOT IN('sessions.session_erasure_events',
      'sessions.session_erasure_scopes','sessions.session_erasure_runs','sessions.session_erasure_writes')`)).toBe(0);
    // 062340's history stays armed through the 062360 down.
    expect((await e.query("SELECT sessions.ingest_org_erasure_history_recording() r")).rows[0].r).toBe(true);
    await migrateAsOwner(empty.ownerUrl);
    expect((await e.query("SELECT to_regclass('sessions.session_erasure_replay_requests')::text r")).rows[0].r).toBe("sessions.session_erasure_replay_requests");
  });
});
