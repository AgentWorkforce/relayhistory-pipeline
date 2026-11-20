/** unit 1b H-equality matrix: loop vs H (set body with round UPDATEs forced while history is live).
 * Reports differences instead of asserting. Throwaway PGlite only.
 * 20261105062155_lazy_ack_set_based, differential equality (PGlite; every migration incl. 062155).
 *
 * sessions.ingest_lazy_ack_batch carries two bodies for its per-record loop behind the function-local gate
 * bulls96.lazy_ack_set_based: 'off' (installed) runs the 043800 loop verbatim, 'on' the set-based twin. Each case
 * below builds one state with a real staged delivery (claim and stage committed, the commit payload captured), then
 * runs the SAME commit from the SAME state twice, once per mode, each in its own transaction that is rolled back
 * (sequences reset between runs). It compares:
 *   - the commit's result, or its first error (SQLSTATE and message);
 *   - every row of every table in schema sessions, timestamp columns excluded (clock_timestamp differs per run);
 *   - every sessions sequence (session_replay_epoch_seq counts history invalidations).
 * pg_stat_xact_user_functions counts the history statement trigger's calls, which proves which body ran: the loop
 * fires it once per accept; the set body once per round when history is dormant, once per accept when it is active.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { deliverBatch } from "../../src/session-ingest/deliver.ts";
import { sqlLedger } from "../../src/session-ingest/ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "../../src/session-ingest/__tests__/support.ts";
import { authorityFixture, epoch, generation, identity } from "../../src/session-ingest/__tests__/privacy-authority-support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Rec = ReturnType<typeof record>;
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
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

async function runMode(f: Fixture, mode: "off" | "on", payload: string, setup: (f: Fixture) => Promise<unknown>, seqs: Array<{ s: string; v: string | null; start: string }>, sql = COMMIT): Promise<Run> {
  for (const q of seqs) await f.db.query("SELECT setval($1::regclass,$2::bigint,$3)", [q.s, q.v ?? q.start, q.v !== null]);
  await f.db.exec("BEGIN");
  try {
    await f.db.exec(`ALTER FUNCTION ${ACK} SET bulls96.lazy_ack_set_based='${mode}';SET LOCAL track_functions='pl'`);
    if (mode === "on") {
      const def = (await f.db.query<{ d: string }>("SELECT pg_get_functiondef($1::regprocedure) d", [ACK])).rows[0]!.d;
      if (!def.includes("IF sessions.ingest_history_live() THEN")) throw Error("H anchor");
      await f.db.exec(def.replace("IF sessions.ingest_history_live() THEN", "IF false THEN"));
    }
    const gate = (await f.db.query<{ c: string[] }>("SELECT proconfig c FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0]!.c;
    await setup(f);
    const calls = async () => Number((await f.db.query<{ n: string }>("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_history_native_changed'),0)::text n")).rows[0]!.n);
    const before = await calls();
    await f.db.exec("SAVEPOINT ack;SET LOCAL ROLE bulls96_session_ingest");
    let out: Outcome;
    try {
      out = { ok: true, result: (await f.db.query<{ r: unknown }>(sql, [payload])).rows[0]!.r };
      await f.db.exec("RESET ROLE");
    } catch (e: any) {
      out = { ok: false, code: String(e.code), message: String(e.message) };
      await f.db.exec("ROLLBACK TO SAVEPOINT ack;RESET ROLE");
    }
    const triggerCalls = (await calls()) - before;
    return { out, snap: await snapshot(f), triggerCalls, gate };
  } finally {
    await f.db.exec("ROLLBACK");
  }
}

function diff(off: Run, on: Run) {
  const out: Record<string, unknown> = {};
  for (const t of Object.keys(off.snap)) {
    const a = off.snap[t] as any[], b = on.snap[t] as any[];
    if (JSON.stringify(a) === JSON.stringify(b)) continue;
    if (!Array.isArray(a)) { out[t] = { loop: a, H: b }; continue; }
    const cols = new Set<string>(); const sa = new Set(a.map((x) => JSON.stringify(x))), sb = new Set(b.map((x) => JSON.stringify(x)));
    const onlyA = a.filter((x) => !sb.has(JSON.stringify(x))), onlyB = b.filter((x) => !sa.has(JSON.stringify(x)));
    for (const x of [...onlyA, ...onlyB]) for (const [k, v] of Object.entries(x)) if (onlyA.concat(onlyB).some((y) => JSON.stringify(y[k]) !== JSON.stringify(v))) cols.add(k);
    out[t] = { rowsLoop: a.length, rowsH: b.length, onlyLoop: onlyA.length, onlyH: onlyB.length, differingColumns: [...cols], sampleLoop: onlyA.slice(0, 2), sampleH: onlyB.slice(0, 2) };
  }
  return out;
}
async function compare(name: string, f: Fixture, payload: string, setup: (f: Fixture) => Promise<unknown> = async () => {}) {
  const seqs = (await f.db.query<{ s: string; v: string | null; start: string }>(
    "SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
  const withLive = async (g: Fixture) => { await g.db.query("UPDATE sessions.ingest_history_switch SET live=true WHERE singleton"); await setup(g); };
  const off = await runMode(f, "off", payload, withLive, seqs), on = await runMode(f, "on", payload, withLive, seqs);
  const report = { name, firstErrorEqual: JSON.stringify(off.out) === JSON.stringify(on.out), loopOut: off.out.ok ? "ok" : off.out, HOut: on.out.ok ? "ok" : on.out,
    triggerCalls: { loop: off.triggerCalls, H: on.triggerCalls }, diffs: diff(off, on) };
  console.log("HDIFF " + JSON.stringify(report));
  return report;
}
const rows = (r: Run, table: string) => r.snap[table] as any[];
const committed = (r: Run) => { expect(r.out.ok).toBe(true); expect((r.out as any).result.kind).toBe("committed"); };
const failed = (r: Run, code: string, message: string) => expect(r.out).toEqual({ ok: false, code, message: expect.stringContaining(message) } as never);


async function history(g: Fixture, sessions: string[], keys: Array<[string, string]>, buildRecord?: string) {
  await g.db.exec("SET LOCAL session_replication_role=replica");
  for (const s of sessions) {
    const auth = crypto.randomUUID();
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,
      pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
      VALUES($1,$2,$3,'claude',$4,1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64))`, [auth, TENANT.orgId, TENANT.workspaceId, s]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,'claude',$3,$4,true)", [TENANT.orgId, TENANT.workspaceId, s, auth]);
    await g.db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES($1,$2,repeat('b',64),1)", ["obj-" + s, auth]);
  }
  for (const [r, s] of keys) await g.db.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,$3,'claude',$4) ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, r, s]);
  if (buildRecord) {
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,generation,lease_token,lease_until,state)
      VALUES('22222222-2222-2222-2222-222222222222',$1,$2,'claude','s-other',$3,'33333333-3333-3333-3333-333333333333',clock_timestamp()+interval '1 hour','keys')`, [TENANT.orgId, TENANT.workspaceId, generation]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_build_records VALUES('22222222-2222-2222-2222-222222222222',$1)", [buildRecord]);
  }
  await g.db.exec("SET LOCAL session_replication_role=origin");
}
const S = (id: string, s: string, revision: number) => rec(id, revision, { session_id: s });
test("H equality matrix (reports)", async () => {
  const f = await fixture();
  const all: any[] = [];
  // 1 one scope, several slots, each accepted once (the common shape)
  { const c = await capture(f, [S("a1", "s-1", 2), S("a2", "s-1", 2), S("a3", "s-1", 2), S("a4", "s-1", 2)]);
    all.push(await compare("1 one managed scope, 4 slots, one accept each", f, c.payload, (g) => history(g, ["s-1"], [["a1", "s-1"]], "a2"))); }
  // 2 same slot repeated (rising) inside one batch, same scope
  { const c = await capture(f, [S("rep", "s-2", 2), S("rep", "s-2", 3), S("rep", "s-2", 4), S("rep2", "s-2", 2)]);
    all.push(await compare("2 same slot x3 rising + another slot, one scope", f, c.payload, (g) => history(g, ["s-2"], [["rep", "s-2"]]))); }
  // 3 several scopes
  { const c = await capture(f, [S("m1", "s-3a", 2), S("m2", "s-3b", 2), S("m3", "s-3c", 2), S("m4", "s-3a", 2), S("m5", "s-3b", 2)]);
    all.push(await compare("3 three managed scopes interleaved", f, c.payload, (g) => history(g, ["s-3a", "s-3b", "s-3c"], [["m1", "s-3a"], ["m2", "s-3b"]], "m3"))); }
  // 4 a slot moving session inside one batch (intermediate state)
  { const c = await capture(f, [S("mv", "s-4a", 2), S("mv", "s-4b", 3)]);
    all.push(await compare("4 one slot moves s-4a -> s-4b inside the batch (both managed)", f, c.payload, (g) => history(g, ["s-4a", "s-4b"], []))); }
  // 5 revision reuse: prior custody; equal (refused) and higher (accepted) in one later batch
  { await deliver(f, [S("ru", "s-5", 5), S("ru2", "s-5", 5)]);
    const c = await capture(f, [S("ru", "s-5", 4), S("ru2", "s-5", 6), S("ru3", "s-5", 1)]);
    all.push(await compare("5 revision reuse: refused + accepted + fresh, one scope", f, c.payload, (g) => history(g, ["s-5"], [["ru", "s-5"]]))); }
  // 6 abort mid-batch: P0W01 on the third record
  { const c = await capture(f, [S("ab1", "s-6", 2), S("ab2", "s-6", 2), S("ab3", "s-6", 2), S("ab4", "s-6", 2)]);
    all.push(await compare("6 abort mid-batch (P0W01 at record 3)", f, c.payload, async (g) => { await history(g, ["s-6"], []);
      await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_owner='someone-else',current_revision=1,current_digest=repeat('f',64),current_source='claude',current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=${slotKey("ab3")}`); })); }
  // 7 suppressed ACK (claim path; ACK body not reached)
  { await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude','s-7')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN]);
    const c = await capture(f, [S("sp", "s-7", 2)], "ingest_claim_buffered_batch");
    const seqs = (await f.db.query<any>("SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
    const off = await runMode(f, "off", c.payload, (g) => history(g, ["s-7"], []), seqs, "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r");
    const on = await runMode(f, "on", c.payload, (g) => history(g, ["s-7"], []), seqs, "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r");
    const r = { name: "7 suppressed ACK", firstErrorEqual: JSON.stringify(off.out) === JSON.stringify(on.out), diffs: diff(off, on) }; console.log("HDIFF " + JSON.stringify(r)); all.push(r); }
  // 8 lost-response replay of a committed commit
  { const c = await capture(f, [S("lr1", "s-8", 2), S("lr2", "s-8", 2)]);
    expect((((await f.run(COMMIT, [c.payload])) as any).rows[0].r).kind).toBe("committed");
    all.push(await compare("8 lost-response replay", f, c.payload, (g) => history(g, ["s-8"], []))); }
  expect(all.length).toBe(8);
});
