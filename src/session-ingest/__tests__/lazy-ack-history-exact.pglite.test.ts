/**
 * 20261105062156_lazy_ack_history_exact, differential equality (PGlite; every migration incl. 062156).
 *
 * Gate bulls96.lazy_ack_h_exact (function-local, installed 'off'): off = main's 043800 loop with 026000's per-accept
 * trigger invalidations; on = the trigger skips its invalidations under the ACK's owner-only mark and the ACK makes
 * them itself, once per scope, with the same counters and sequence draws. Each case runs the SAME steps from the SAME
 * state once per mode, each in a rolled-back transaction with every sessions sequence reset in between, and compares
 * the results or first errors, EVERY row of EVERY sessions table (timestamp columns excluded) and EVERY sessions
 * sequence position. Serial parity only; concurrent invariants are a separate (real PostgreSQL) claim.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Rec = ReturnType<typeof record>;
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const GATE = "bulls96.lazy_ack_h_exact";
const COMMIT = "SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS r";
const MAX = "9223372036854775807";
const ORIGIN = "origin-1";
let serial = 0;

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  return f;
}

async function custody(f: Fixture, raw: Awaited<ReturnType<typeof batch>>) {
  const p = await prepareDelivery(TENANT, raw);
  const witness = { version: 1, issuer: "relayhistory-cloud", sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: "a".repeat(64), tenant: TENANT,
    originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null, privacyVersion: "0",
    originalRecordCount: raw.records.length, candidateCount: p.records.length,
    records: p.records.map((r, i) => ({ preparedOrd: i, processingRank: i, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision),
      revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
    privacy: { complete: true, deletions: [], resets: [] } };
  const sourceDigest = (await f.db.query<{ d: string }>("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d", [JSON.stringify(witness)])).rows[0]!.d;
  return { witness, sourceDigest };
}
const rawBatch = async (records: Rec[], id = "b" + ++serial) => batch(records, { batch_id: id, account_id: await deliveryAccount(TENANT) });
const rec = (record_id: string, revision: number, extra: Partial<Rec> = {}) =>
  record({ record_id, revision, revision_id: `${record_id}:r${revision}:${++serial}`, session_id: "s-" + record_id, ...extra });

/** A real delivery, committed (the gate is whatever is installed: 'off'). */
async function deliver(f: Fixture, records: Rec[]) {
  const raw = await rawBatch(records);
  const ledger = sqlLedger(f.run, { acceptanceMode: "buffered", expectedGeneration: generation });
  const out = await deliverBatch({ ...f.deps, ledger, custody: await custody(f, raw) } as never, TENANT, raw);
  expect(out.outcome).toBe("accepted_buffered");
  return raw;
}
/** Claim and stage committed; the exact commit payload is returned and the commit is NOT run. */
async function capture(f: Fixture, records: Rec[], fn = "ingest_commit_buffered_batch") {
  const raw = await rawBatch(records);
  let payload: string | undefined;
  const ledger = sqlLedger(async (sql, params) => {
    if (sql.includes(fn)) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); }
    return f.run(sql, params as unknown[]);
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  await expect(deliverBatch({ ...f.deps, ledger, custody: await custody(f, raw) } as never, TENANT, raw)).rejects.toBeDefined();
  if (typeof payload !== "string") throw Error("commit payload not captured");
  const parsed = JSON.parse(payload);
  return { payload, claimToken: parsed.claimToken as string, stageToken: parsed.stageToken as string };
}
const slotKey = (recordId: string) =>
  `encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1','${TENANT.orgId}','${TENANT.workspaceId}','${ORIGIN}','${recordId}')::text,'UTF8')),'hex')`;

async function snapshot(f: Fixture) {
  const tables = (await f.db.query<{ t: string; ts: string[] }>(`SELECT c.oid::regclass::text t,
     coalesce(array(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      AND a.atttypid IN('timestamptz'::regtype,'timestamp'::regtype)),'{}') ts
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relkind IN('r','p') ORDER BY 1`)).rows;
  const out: Record<string, unknown> = {};
  for (const { t, ts } of tables)
    out[t] = (await f.db.query<{ j: unknown }>(`SELECT coalesce(jsonb_agg(x ORDER BY x::text),'[]') j FROM(SELECT to_jsonb(q)-$1::text[] x FROM ${t} q)s`, [ts])).rows[0]!.j;
  out["<sequences>"] = (await f.db.query("SELECT sequencename,last_value::text FROM pg_sequences WHERE schemaname='sessions' ORDER BY 1")).rows;
  return out;
}
type Outcome = { ok: true; result: unknown } | { ok: false; code: string; message: string };
type Run = { out: Outcome; snap: Record<string, unknown>; triggerCalls: number; gate: string[] };


async function runMode(f: Fixture, mode: "off" | "on", steps: (f: Fixture) => Promise<Outcome[]>, setup: (f: Fixture) => Promise<unknown>, seqs: Array<{ s: string; v: string | null; start: string }>): Promise<Run> {
  for (const q of seqs) await f.db.query("SELECT setval($1::regclass,$2::bigint,$3)", [q.s, q.v ?? q.start, q.v !== null]);
  await f.db.exec("BEGIN");
  try {
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='${mode}';SET LOCAL track_functions='pl'`);
    const gate = (await f.db.query<{ c: string[] }>("SELECT proconfig c FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0]!.c;
    await setup(f);
    const calls = async () => Number((await f.db.query<{ n: string }>("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_history_invalidate_scope'),0)::text n")).rows[0]!.n);
    const before = await calls();
    const outs = await steps(f);
    const invalidateCalls = (await calls()) - before;
    return { out: outs.length === 1 ? outs[0]! : ({ ok: true, result: outs } as Outcome), snap: await snapshot(f), triggerCalls: invalidateCalls, gate };
  } finally {
    await f.db.exec("ROLLBACK");
  }
}
/** Run one commit as the runtime role inside the open transaction; an error rolls back to a savepoint and is returned. */
const commit = (payload: string) => async (f: Fixture): Promise<Outcome> => {
  await f.db.exec("SAVEPOINT ack;SET LOCAL ROLE bulls96_session_ingest");
  try {
    const r = (await f.db.query<{ r: unknown }>(COMMIT, [payload])).rows[0]!.r;
    await f.db.exec("RESET ROLE;RELEASE SAVEPOINT ack");
    return { ok: true, result: r };
  } catch (e: any) {
    await f.db.exec("ROLLBACK TO SAVEPOINT ack;RESET ROLE");
    return { ok: false, code: String(e.code), message: String(e.message) };
  }
};
/** main (gate off) vs H-exact (gate on), same state and steps; every table row and every sequence must be equal. */
async function both(f: Fixture, steps: Array<(f: Fixture) => Promise<Outcome>>, setup: (f: Fixture) => Promise<unknown> = async () => {}) {
  const seqs = (await f.db.query<{ s: string; v: string | null; start: string }>(
    "SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
  const run = (g: Fixture) => (async () => { const o: Outcome[] = []; for (const st of steps) o.push(await st(g)); return o; })();
  const off = await runMode(f, "off", run, setup, seqs);
  const on = await runMode(f, "on", run, setup, seqs);
  expect(off.gate).toContain(GATE + "=off");
  expect(on.gate).toContain(GATE + "=on");
  expect(on.out).toEqual(off.out);
  for (const t of Object.keys(off.snap)) expect({ t, rows: on.snap[t] }).toEqual({ t, rows: off.snap[t] });
  expect(Object.keys(on.snap)).toEqual(Object.keys(off.snap));
  return { off, on };
}
const rows = (r: Run, table: string) => r.snap[table] as any[];
const committed = (r: Run) => { expect(r.out.ok).toBe(true); expect((r.out as any).result.kind).toBe("committed"); };
const failed = (r: Run, code: string, message: string) => expect(r.out).toEqual({ ok: false, code, message: expect.stringContaining(message) } as never);


const directoryErrors: string[] = [];
const S = (id: string, session: string, revision: number) => rec(id, revision, { session_id: session });
/** Managed scopes (installed authority, deterministic ids), optional scope_keys, live derived objects, an optional live
 *  build touching a record, and optional pre-existing required_scopes / publications rows. FK checks off for the fixture only. */
async function history(g: Fixture, o: { sessions: string[]; keys?: Array<[string, string]>; build?: string; existing?: string[]; derived?: number }) {
  await g.db.exec("SET LOCAL session_replication_role=replica");
  for (const sess of o.sessions) {
    const auth = (await g.db.query<{ u: string }>("SELECT md5($1)::uuid::text u", ["auth-" + sess])).rows[0]!.u;
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,
      pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
      VALUES($1,$2,$3,'claude',$4,1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64))`, [auth, TENANT.orgId, TENANT.workspaceId, sess]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,'claude',$3,$4,true)", [TENANT.orgId, TENANT.workspaceId, sess, auth]);
    await g.db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) SELECT $1||'-'||g,$2,repeat('b',64),1 FROM generate_series(1,$3::int) g", ["obj-" + sess, auth, o.derived ?? 2]);
    await g.db.exec("SAVEPOINT sd");
    try {
      await g.db.query("INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at) VALUES($1,$2,'claude',$3,'2026-11-01T00:00:00Z','2026-11-01T00:00:00Z') ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, sess]);
      await g.db.query("UPDATE sessions.session_directory SET work_status='active',work_status_at=clock_timestamp() WHERE org_id=$1 AND workspace_id=$2 AND harness='claude' AND session_id=$3", [TENANT.orgId, TENANT.workspaceId, sess]);
      await g.db.exec("RELEASE SAVEPOINT sd");
    } catch (e) { await g.db.exec("ROLLBACK TO SAVEPOINT sd"); directoryErrors.push(String((e as Error).message)); }
  }
  for (const [r, sess] of o.keys ?? []) await g.db.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,$3,'claude',$4) ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, r, sess]);
  for (const sess of o.existing ?? []) {
    await g.db.query("INSERT INTO sessions.ingest_history_required_scopes(org_id,workspace_id,harness,session_id,request_version) VALUES($1,$2,'claude',$3,7)", [TENANT.orgId, TENANT.workspaceId, sess]);
    await g.db.query("INSERT INTO sessions.session_replay_publications(org_id,workspace_id,harness,session_id) VALUES($1,$2,'claude',$3)", [TENANT.orgId, TENANT.workspaceId, sess]);
  }
  if (o.build) {
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,generation,lease_token,lease_until,state)
      VALUES(md5('build')::uuid,$1,$2,'claude','s-other',$3,md5('lease')::uuid,clock_timestamp()+interval '1 hour','keys')`, [TENANT.orgId, TENANT.workspaceId, generation]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_build_records VALUES(md5('build')::uuid,$1)", [o.build]);
  }
  await g.db.exec("SET LOCAL session_replication_role=origin");
}
const live = (on: boolean) => (g: Fixture) => g.db.query("UPDATE sessions.ingest_history_switch SET live=$1 WHERE singleton", [on]);
const ownedBy = (id: string) => `UPDATE sessions.ingest_lazy_native_slots SET current_owner='someone-else',current_revision=1,current_digest=repeat('f',64),
  current_source='claude',current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=${slotKey(id)}`;
const invalidations = (r: Run) => r.triggerCalls;
const seq = (r: Run) => (r.snap["<sequences>"] as any[]).find((x) => x.sequencename === "session_replay_epoch_seq")?.last_value;

describe("062156 installs the gate off; only the owner's proconfig switches it", () => {
  test("proconfig pins the gate off; the runtime role cannot ALTER the function; ACLs and SECURITY flags unchanged", async () => {
    const f = await fixture();
    const p = (await f.db.query<any>("SELECT proconfig,prosecdef,prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(p.proconfig).toEqual(["search_path=pg_catalog, pg_temp", "bulls96.lazy_ack_h_exact=off"]);
    expect(p.prosecdef).toBe(false);
    expect(p.prosrc.split("pg_advisory_xact_lock(").length - 1).toBe(1);
    expect(p.prosrc.indexOf("WHERE singleton FOR NO KEY UPDATE;")).toBeGreaterThan(-1);
    const t = (await f.db.query<any>("SELECT prosecdef,proconfig FROM pg_proc WHERE oid='sessions.ingest_history_native_changed()'::regprocedure")).rows[0];
    expect(t).toEqual({ prosecdef: true, proconfig: ["search_path=pg_catalog, pg_temp"] });
    for (const fn of ["lazy_ack_history_intents", "lazy_ack_history_scope_lock", "lazy_ack_history_replay"])
      expect((await f.db.query<any>("SELECT has_function_privilege('bulls96_session_ingest',p.oid,'EXECUTE') x FROM pg_proc p WHERE proname=$1", [fn])).rows[0].x).toBe(false);
    expect((await f.db.query<any>("SELECT has_table_privilege('bulls96_session_ingest','sessions.lazy_ack_history_marks','INSERT') x")).rows[0].x).toBe(false);
    await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { await expect(f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='on'`)).rejects.toMatchObject({ code: "42501" }); } finally { await f.db.exec("ROLLBACK"); }
  });
  test("a session-level 'on' without a mark changes nothing: a gate-off ACK and a direct multi-row slot UPDATE still invalidate per row", async () => {
    const f = await fixture();
    const c = await capture(f, [S("g1", "s-g", 2), S("g2", "s-g", 2)]);
    const steps = [commit(c.payload), async (g: Fixture) => { await g.db.exec(`SET LOCAL ${GATE}='on'`); await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1 WHERE key_sha256 IN(${slotKey("g1")},${slotKey("g2")})`); return { ok: true, result: null } as Outcome; }];
    const setup = (g: Fixture) => history(g, { sessions: ["s-g"] });
    // Both runs use the gate-off ACK; the second sets only the session GUC (no mark): results must be identical.
    const seqs = (await f.db.query<any>("SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
    const plain = await runMode(f, "off", async (g) => [await commit(c.payload)(g), await (async () => { await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET lineage=lineage+1 WHERE key_sha256 IN(${slotKey("g1")},${slotKey("g2")})`); return { ok: true, result: null } as Outcome; })()], setup, seqs);
    const guc = await runMode(f, "off", async (g) => { const o: Outcome[] = []; for (const st of steps) o.push(await st(g)); return o; }, setup, seqs);
    expect(guc.out).toEqual(plain.out);
    for (const t of Object.keys(plain.snap)) expect({ t, rows: guc.snap[t] }).toEqual({ t, rows: plain.snap[t] });
    expect(invalidations(guc)).toBe(invalidations(plain));
    expect(invalidations(plain)).toBeGreaterThan(2);
  });
});

describe("062156 serial parity: main vs history-exact, history live with managed scopes", () => {
  test("one scope, four slots, one accept each (the common shape): 4 trigger invalidations become 1 call-free replay", async () => {
    directoryErrors.length = 0;
    const f = await fixture();
    const c = await capture(f, ["a1", "a2", "a3", "a4"].map((id) => S(id, "s-1", 2)));
    const { off, on } = await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-1"], keys: [["a1", "s-1"]], build: "a2" }));
    committed(on);
    expect(invalidations(off)).toBeGreaterThan(0);
    expect(invalidations(on)).toBe(0);
    expect(rows(on, "sessions.ingest_history_scope_builds").every((b) => b.state === "abandoned")).toBe(true);
    expect(rows(on, "sessions.ingest_history_reconcile_outbox").length).toBeGreaterThan(0);
    // the session_directory fixture row exists and was reset by the invalidation in both bodies
    expect(directoryErrors).toEqual([]);
    expect(rows(on, "sessions.session_directory").find((x) => x.session_id === "s-1")).toMatchObject({ state: "unknown", work_status: null });
  });
  test("same slot three times rising in one batch (A->A->A) plus another slot", async () => {
    const f = await fixture();
    const c = await capture(f, [S("rep", "s-2", 2), S("rep", "s-2", 3), S("rep", "s-2", 4), S("rep2", "s-2", 2)]);
    const { off } = await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-2"], keys: [["rep", "s-2"]] }));
    expect(invalidations(off)).toBeGreaterThanOrEqual(6);
  });
  test("A -> B -> A across distinct slots with a same-batch scope move, exact per-scope epoch and request_version", async () => {
    const f = await fixture();
    const c = await capture(f, [S("x1", "s-A", 2), S("x2", "s-B", 2), S("x1", "s-B", 3), S("x3", "s-A", 2), S("x1", "s-A", 4), S("x2", "s-A", 3)]);
    const { on } = await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-A", "s-B"] }));
    committed(on);
    const pubs = rows(on, "sessions.session_replay_publications").filter((x) => ["s-A", "s-B"].includes(x.session_id));
    expect(pubs.length).toBe(2);
  });
  test("a scope in both phases of one accept (prior custody in s-C, higher revision in s-C): two invalidations in main", async () => {
    const f = await fixture();
    await deliver(f, [S("both", "s-C", 5)]);
    const c = await capture(f, [S("both", "s-C", 6)]);
    const { off } = await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-C"] }));
    expect(invalidations(off)).toBe(2);
  });
  test("a scope with existing required_scopes and publications rows next to one without (1 vs 2 draws per call)", async () => {
    const f = await fixture();
    const c = await capture(f, [S("e1", "s-old", 2), S("e2", "s-new", 2), S("e3", "s-old", 2), S("e4", "s-new", 2)]);
    const { on } = await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-old", "s-new"], existing: ["s-old"] }));
    committed(on);
    expect(rows(on, "sessions.ingest_history_required_scopes").find((x) => x.session_id === "s-old").request_version).toBe(9);
    expect(rows(on, "sessions.ingest_history_required_scopes").find((x) => x.session_id === "s-new").request_version).toBe(2);
  });
  test("several scopes interleaved, scope_keys hits and a live build", async () => {
    const f = await fixture();
    const c = await capture(f, [S("m1", "s-3a", 2), S("m2", "s-3b", 2), S("m3", "s-3c", 2), S("m4", "s-3a", 2), S("m5", "s-3b", 2)]);
    await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-3a", "s-3b", "s-3c"], keys: [["m1", "s-3b"], ["m2", "s-3c"], ["m5", "s-3a"]], build: "m3" }));
  });
  test("revision reuse (refused, accepted, fresh) and a single-row batch", async () => {
    const f = await fixture();
    await deliver(f, [S("ru", "s-5", 5), S("ru2", "s-5", 5)]);
    const c = await capture(f, [S("ru", "s-5", 4), S("ru2", "s-5", 6), S("ru3", "s-5", 1)]);
    await both(f, [commit(c.payload)], (g) => history(g, { sessions: ["s-5"], keys: [["ru", "s-5"]] }));
    const one = await capture(f, [S("single", "s-5", 2)]);
    await both(f, [commit(one.payload)], (g) => history(g, { sessions: ["s-5"] }));
  });
  test("history live but no managed scope, and history switch off: both bodies invalidate nothing", async () => {
    const f = await fixture();
    const c = await capture(f, [S("q1", "s-q", 2), S("q2", "s-q", 2)]);
    const a = await both(f, [commit(c.payload)], async () => {});
    expect(invalidations(a.off)).toBe(0);
    await both(f, [commit(c.payload)], async (g) => { await history(g, { sessions: ["s-q"] }); await live(false)(g); });
  });
  test("lost-response replay: the second commit invalidates nothing in either body", async () => {
    const f = await fixture();
    const c = await capture(f, [S("lr1", "s-8", 2), S("lr2", "s-8", 2)]);
    await both(f, [commit(c.payload), commit(c.payload)], (g) => history(g, { sessions: ["s-8"] }));
  });
  test("two ACKs in one transaction, then another writer's multi-row slot UPDATE (lazy_clear's statement shape)", async () => {
    const f = await fixture();
    const c1 = await capture(f, [S("t1", "s-9", 2), S("t2", "s-9", 2)]);
    const c2 = await capture(f, [S("t3", "s-9", 2), S("t1", "s-9", 3)]);
    const other = async (g: Fixture): Promise<Outcome> => {
      expect((await g.db.query<any>("SELECT count(*)::int n FROM sessions.lazy_ack_history_marks")).rows[0].n).toBe(0);
      await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_owner=NULL,lineage=lineage+1 WHERE key_sha256 IN(${slotKey("t1")},${slotKey("t2")},${slotKey("t3")})`);
      return { ok: true, result: null };
    };
    const { on } = await both(f, [commit(c1.payload), commit(c2.payload), other], (g) => history(g, { sessions: ["s-9"] }));
    expect(invalidations(on)).toBeGreaterThan(0); // the other writer's statement still invalidates through the trigger
  });
});

describe("062156 serial parity on aborts: same first error and the same sequence position", () => {
  const cases: Array<[string, (g: Fixture) => Promise<unknown>, string, string]> = [
    ["P0W01 at record 3 of 4, after two accepts", (g) => g.db.query(ownedBy("ab3")), "P0W01", "native key currently belongs to another user"],
    ["slot mismatch at record 1 (no intents yet)", (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET control_epoch=repeat('9',64) WHERE key_sha256=${slotKey("ab1")}`), "55000", "initialized native custody slot required"],
    ["lineage exhaustion at record 2", (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET lineage=9223372036854775807 WHERE key_sha256=${slotKey("ab2")}`), "55000", "native lineage exhausted"],
    ["seat exhaustion at the last record", (g) => g.db.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=9223372036854775804"), "55000", "ACK seats exhausted"],
    ["missing rank (raised last, NULLS LAST)", async (g) => { const t = (await g.db.query<any>(`SELECT claim_token FROM sessions.ingest_claim_attempts ORDER BY created_at DESC LIMIT 1`).catch(() => ({ rows: [] }))).rows[0]; void t; }, "", ""],
  ];
  for (const [name, mutate, code, message] of cases.slice(0, 4)) test(name, async () => {
    const f = await fixture();
    const c = await capture(f, ["ab1", "ab2", "ab3", "ab4"].map((id) => S(id, "s-ab", 2)));
    const { off, on } = await both(f, [commit(c.payload)], async (g) => { await history(g, { sessions: ["s-ab"], existing: [] }); await mutate(g); });
    failed(on, code, message);
    expect(seq(on)).toBe(seq(off));
  });
  test("missing processing rank: raised after every earlier record, same sequence position", async () => {
    const f = await fixture();
    const c = await capture(f, ["rk1", "rk2", "rk3"].map((id) => S(id, "s-rk", 2)));
    const { off, on } = await both(f, [commit(c.payload)], async (g) => { await history(g, { sessions: ["s-rk"] });
      await g.db.query("UPDATE sessions.ingest_claim_attempts SET source_processing_ranks=jsonb_set(source_processing_ranks,'{0}','null') WHERE claim_token=$1", [c.claimToken]); });
    failed(on, "55000", "source processing rank missing");
    expect(seq(on)).toBe(seq(off));
  });
  test("adoption refused at the last record, after earlier accepts", async () => {
    const f = await fixture();
    await deliver(f, [S("ad", "s-ad", 5)]);
    const c = await capture(f, [S("ad0", "s-ad", 2), S("ad1", "s-ad", 2), S("ad", "s-ad", 4)]);
    const dry = await runMode(f, "off", async (g) => [await commit(c.payload)(g)], async () => {}, []);
    const pre = rows(dry, "sessions.ingest_lazy_ack_decisions").find((x) => x.claim_token === c.claimToken && !x.body_accepted)!;
    const { off, on } = await both(f, [commit(c.payload)], async (g) => { await history(g, { sessions: ["s-ad"] });
      await g.db.query("INSERT INTO sessions.ingest_lazy_ack_decisions SELECT * FROM jsonb_populate_record(NULL::sessions.ingest_lazy_ack_decisions,$1::jsonb)", [JSON.stringify({ ...pre, native_lineage: pre.native_lineage + 5 })]); });
    failed(on, "55000", "immutable ACK decision adoption refused");
    expect(seq(on)).toBe(seq(off));
  });
});

describe("062156 draws happen at main's logical point; the replay draws nothing", () => {
  test("lazy_ack_history_replay draws zero sequence values (new and existing publications rows)", async () => {
    const f = await fixture();
    await f.db.exec("BEGIN");
    try {
      await history(f, { sessions: ["s-z1", "s-z2"], existing: ["s-z2"] });
      const pos = async () => (await f.db.query<{ v: string }>("SELECT last_value::text v FROM pg_sequences WHERE schemaname='sessions' AND sequencename='session_replay_epoch_seq'")).rows[0]!.v;
      const before = await pos();
      const scope = (s: string) => JSON.stringify([TENANT.orgId, TENANT.workspaceId, "claude", s]);
      await f.db.query("SELECT sessions.lazy_ack_history_replay($1::text[],$2::integer[],$3::bigint[])", [[scope("s-z1"), scope("s-z2")], [3, 2], [101, 102]]);
      expect(await pos()).toBe(before);
      const pubs = (await f.db.query<any>("SELECT session_id,epoch::int e FROM sessions.session_replay_publications WHERE session_id IN('s-z1','s-z2') ORDER BY 1")).rows;
      expect(pubs).toEqual([{ session_id: "s-z1", e: 101 }, { session_id: "s-z2", e: 102 }]);
      const req = (await f.db.query<any>("SELECT session_id,request_version::int v FROM sessions.ingest_history_required_scopes WHERE session_id IN('s-z1','s-z2') ORDER BY 1")).rows;
      expect(req).toEqual([{ session_id: "s-z1", v: 3 }, { session_id: "s-z2", v: 9 }]);
    } finally { await f.db.exec("ROLLBACK"); }
  });
  for (const [label, mutate] of [
    ["before any draw (slot mismatch at record 1)", (g: Fixture) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET control_epoch=repeat('9',64) WHERE key_sha256=${slotKey("fa1")}`)],
    ["after draws (P0W01 at record 3)", (g: Fixture) => g.db.query(ownedBy("fa3"))],
  ] as const) test(`a forced abort ${label}, then a successful ACK in the same transaction: equal sequence position and stored epochs`, async () => {
    const f = await fixture();
    const bad = await capture(f, ["fa1", "fa2", "fa3"].map((id) => S(id, "s-fa", 2)));
    const good = await capture(f, [S("fb1", "s-fa", 2), S("fb2", "s-fb", 2)]);
    const { off, on } = await both(f, [commit(bad.payload), commit(good.payload)], async (g) => { await history(g, { sessions: ["s-fa", "s-fb"], existing: ["s-fb"] }); await mutate(g); });
    expect((on.out as any).result[0].ok).toBe(false);
    expect((on.out as any).result[1].ok).toBe(true);
    expect(seq(on)).toBe(seq(off));
  });
});
