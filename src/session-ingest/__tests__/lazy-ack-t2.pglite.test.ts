/**
 * 20261105062210_lazy_ack_t2, differential equality (PGlite; every migration incl. 062210).
 *
 * sessions.ingest_lazy_ack_batch selects its body from the function-local gate bulls96.lazy_ack_t2 (installed 'off':
 * main's 043800 body, no table read) and, when the gate is 'fenced', from the owner-only row sessions.lazy_ack_t2_mode
 * ('main' installed | 't2'). T2 takes the clock FOR KEY SHARE, plans under the slot locks, then upgrades to FOR NO KEY
 * UPDATE for the seat numbers and writes (062155's set body). Each case builds one state with a real staged delivery
 * (claim and stage committed, the commit payload captured), then runs the SAME commit from the SAME state three times:
 * gate off (main), fenced + mode 'main', fenced + mode 't2', each in its own transaction that is rolled back
 * (sequences reset between runs). It compares the commit's result or first error (SQLSTATE and message), every row of
 * every sessions table (timestamp columns excluded; the mode row itself excluded) and every sessions sequence.
 * pg_stat_xact_user_functions counts the history statement trigger's calls, which proves which body ran: the loop
 * fires it once per accept; T2 once per round when history is dormant, once per accept when it is live.
 * Serial parity only: the concurrent claims (fence drain, upgrade under contention, fresh last_seat) are the
 * real-PostgreSQL suite's.
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

type Mode = "off" | "fenced-main" | "t2";
const GATE = "bulls96.lazy_ack_t2";
async function runMode(f: Fixture, mode: Mode, payload: string, setup: (f: Fixture) => Promise<unknown>, seqs: Array<{ s: string; v: string | null; start: string }>, sql = COMMIT): Promise<Run> {
  for (const q of seqs) await f.db.query("SELECT setval($1::regclass,$2::bigint,$3)", [q.s, q.v ?? q.start, q.v !== null]);
  await f.db.exec("BEGIN");
  try {
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='${mode === "off" ? "off" : "fenced"}';SET LOCAL track_functions='pl'`);
    if (mode !== "off") expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode($1) old", [mode === "t2" ? "t2" : "main"])).rows[0].old).toBe("main");
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
    const snap = await snapshot(f);
    delete snap["sessions.lazy_ack_t2_mode"]; // the one row that differs by construction; the gate tests check it
    return { out, snap, triggerCalls, gate };
  } finally {
    await f.db.exec("ROLLBACK");
  }
}

/**
 * The same commit from the same state: gate off (main), fenced with mode 'main', fenced with mode 't2'; history switch
 * off (T2's round UPDATEs) and on (per-accept UPDATEs). Asserts all three equal; returns off = main, on = t2.
 */
async function both(f: Fixture, payload: string, setup: (f: Fixture) => Promise<unknown> = async () => {}, sql = COMMIT, lives: boolean[] = [false, true]) {
  const seqs = (await f.db.query<{ s: string; v: string | null; start: string }>(
    "SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
  const runs: Array<{ live: boolean; off: Run; fmain: Run; on: Run }> = [];
  for (const live of lives) {
    const withSwitch = async (g: Fixture) => { await g.db.query("UPDATE sessions.ingest_history_switch SET live=$1 WHERE singleton", [live]); await setup(g); };
    const off = await runMode(f, "off", payload, withSwitch, seqs, sql);
    const fmain = await runMode(f, "fenced-main", payload, withSwitch, seqs, sql);
    const on = await runMode(f, "t2", payload, withSwitch, seqs, sql);
    expect(off.gate).toContain(GATE + "=off");
    expect(fmain.gate).toContain(GATE + "=fenced");
    expect(on.gate).toContain(GATE + "=fenced");
    for (const [name, r] of [["fenced-main", fmain], ["t2", on]] as const) {
      expect({ live, name, out: r.out }).toEqual({ live, name, out: off.out });
      for (const t of Object.keys(off.snap)) expect({ live, name, t, rows: r.snap[t] }).toEqual({ live, name, t, rows: off.snap[t] });
      expect(Object.keys(r.snap)).toEqual(Object.keys(off.snap));
    }
    expect(fmain.triggerCalls).toBe(off.triggerCalls); // fenced + 'main' runs main's loop
    if (live) expect(on.triggerCalls).toBe(off.triggerCalls); // switch on: T2 also issues one UPDATE per accept
    runs.push({ live, off, fmain, on });
  }
  return { ...runs[0]!, runs };
}
const rows = (r: Run, table: string) => r.snap[table] as any[];
const committed = (r: Run) => { expect(r.out.ok).toBe(true); expect((r.out as any).result.kind).toBe("committed"); };
const failed = (r: Run, code: string, message: string) => expect(r.out).toEqual({ ok: false, code, message: expect.stringContaining(message) } as never);

const ackBody = (sql: string) => {
  const m = sql.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?AS \$lazy_ack_batch\$([\s\S]*?)\$lazy_ack_batch\$;/);
  if (!m) throw Error("ACK body not found");
  return m[1]!;
};
const MIG = "supabase/migrations/20261105062210_lazy_ack_t2.sql";
const MAIN_MIG = "supabase/migrations/20261105043800_lazy_ack_clock_no_key_update.sql";
/** Body-selection discriminator: history switch off, two fresh records; main fires the slot trigger per accept (2), T2 per round (1). */
async function bodyCalls(f: Fixture, tag: string, prelude: string, form?: string): Promise<number> {
  const c = await capture(f, [rec(tag + "-1", 2), rec(tag + "-2", 2)]);
  await f.db.exec(`BEGIN;UPDATE sessions.ingest_history_switch SET live=false WHERE singleton;${prelude};SET LOCAL track_functions='pl';SET LOCAL ROLE bulls96_session_ingest`);
  try {
    if (form) await f.db.exec(form);
    const n = async () => Number((await f.db.query<any>("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE funcname='ingest_history_native_changed'),0) n")).rows[0].n);
    const before = await n();
    expect(((await f.db.query<any>(COMMIT, [c.payload])).rows[0].r).kind).toBe("committed");
    return (await n()) - before;
  } finally { await f.db.exec("ROLLBACK"); if (form) await f.db.exec(`RESET ${GATE}`); }
}

describe("062210 installs the gate off, the mode 'main', and keeps the 043800 contract", () => {
  test("proconfig pins the gate 'off'; mode row 'main'; owner-only table and flip function; SECURITY INVOKER", async () => {
    const f = await fixture();
    const p = (await f.db.query<any>("SELECT proconfig,prosecdef FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(p.proconfig).toEqual(["search_path=pg_catalog, pg_temp", GATE + "=off"]);
    expect(p.prosecdef).toBe(false);
    expect((await f.db.query<any>("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows).toEqual([{ mode: "main" }]);
    const priv = (await f.db.query<any>(`SELECT r.rolname,
        has_table_privilege(r.oid,'sessions.lazy_ack_t2_mode','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') t,
        has_function_privilege(r.oid,'sessions.lazy_ack_t2_set_mode(text)','EXECUTE') OR has_function_privilege(r.oid,'sessions.lazy_ack_t2_set_gate(text)','EXECUTE') x
      FROM pg_roles r WHERE r.rolname IN('bulls96_session_ingest','bulls96_session_ingest_seeder','anon','authenticated','service_role') ORDER BY 1`)).rows;
    expect(priv.length).toBeGreaterThan(0);
    for (const r of priv) expect(r).toEqual({ rolname: r.rolname, t: false, x: false });
    const sm = (await f.db.query<any>("SELECT prosecdef,proconfig FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0];
    expect(sm).toEqual({ prosecdef: false, proconfig: ["search_path=pg_catalog, pg_temp"] });
    expect((await f.db.query<any>("SELECT relrowsecurity FROM pg_class WHERE oid='sessions.lazy_ack_t2_mode'::regclass")).rows[0].relrowsecurity).toBe(true);
  });
  test("with every T2 addition removed, the installed body is 043800's body byte for byte", async () => {
    const f = await fixture();
    const { readFileSync } = await import("node:fs");
    const installed = (await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].prosrc as string;
    expect(installed).toBe(ackBody(readFileSync(MIG, "utf8")));
    let x = installed.replace(/\n v_t2 boolean;[^\n]*/, "");
    x = x.replace(/ -- 062210 gate[\s\S]*?ELSE v_t2:=false;END IF;\n/, "");
    x = x.replace(/ IF NOT v_t2 THEN\n( SELECT \* INTO STRICT c FROM sessions\.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c\.last_seat;\n) ELSE\n[\s\S]*?\n END IF;\n/, "$1");
    x = x.replace(" IF NOT v_t2 THEN\n FOR r IN", " FOR r IN");
    x = x.replace(/( END LOOP;\n) ELSE\n[\s\S]*\n END IF;\n( UPDATE sessions\.ingest_receipt_seat_clock SET last_seat=v_last)/, "$1$2");
    expect(x).toBe(ackBody(readFileSync(MAIN_MIG, "utf8")));
  });
  test("statement order: fences (barrier) -> gate -> RC check -> mode read -> clock; T2: KEY SHARE, then the upgrade reads last_seat", async () => {
    const f = await fixture();
    const src = (await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].prosrc as string;
    const at = (s: string, from = 0) => { const i = src.indexOf(s, from); expect({ s, i: i > -1 }).toEqual({ s, i: true }); return i; };
    const fences = at("PERFORM sessions.ingest_lock_fences("), gate = at("current_setting('bulls96.lazy_ack_t2',true) IS NOT DISTINCT FROM 'fenced'");
    const rc = at("current_setting('transaction_isolation')<>'read committed'"), mode = at("FROM sessions.lazy_ack_t2_mode m WHERE m.singleton;");
    const ks = at("WHERE singleton FOR KEY SHARE;\n END IF;"), up = at("SELECT ck.last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock ck WHERE ck.singleton FOR NO KEY UPDATE;");
    expect(fences < gate && gate < rc && rc < mode && mode < ks && ks < up).toBe(true);
    expect(src.split("current_setting(").length - 1).toBe(2);
    expect(src.split("lazy_ack_t2_mode").length - 1).toBe(1);
    // The T2 branch never reads last_seat before its upgrade (the KEY SHARE row variable is used only for identity columns).
    const t2 = src.slice(src.indexOf(" ELSE\n  -- (A)")).replace(/--[^\n]*/g, "");
    expect(t2.indexOf("last_seat")).toBe(t2.indexOf("ck.last_seat") + 3);
    expect(t2.slice(0, t2.indexOf("ck.last_seat"))).not.toContain("c.last_seat");
  });
  test("the runtime role cannot select T2: ALTER FUNCTION, the flip function and the mode table are refused; session values are overridden", async () => {
    const f = await fixture();
    for (const stmt of [`ALTER FUNCTION ${ACK} SET ${GATE}='fenced'`, "SELECT sessions.lazy_ack_t2_set_mode('t2')", "SELECT sessions.lazy_ack_t2_set_gate('fenced')", "UPDATE sessions.lazy_ack_t2_mode SET mode='t2'",
      "SELECT mode FROM sessions.lazy_ack_t2_mode", "INSERT INTO sessions.lazy_ack_t2_mode VALUES(true,'t2')",
      // No runtime lock on the mode row in any mode: only the flip (after its 50000 try-lock) can lock it.
      "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR UPDATE", "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR NO KEY UPDATE",
      "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR SHARE", "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR KEY SHARE",
      "LOCK TABLE sessions.lazy_ack_t2_mode IN ACCESS SHARE MODE", "DELETE FROM sessions.lazy_ack_t2_mode", "TRUNCATE sessions.lazy_ack_t2_mode"]) {
      await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
      try { await expect(f.db.exec(stmt)).rejects.toMatchObject({ code: "42501" }); } finally { await f.db.exec("ROLLBACK"); }
    }
    // Mode 't2' committed by the owner (under the fenced gate), then the gate set 'off' (the operator never does that while
    // the row says 't2'; here it isolates the override): session/local values cannot select the fenced body.
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='fenced'`);
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='off'`);
    try {
      for (const form of [`SET LOCAL ${GATE}='fenced'`, `SELECT set_config('${GATE}','fenced',true)`, `SELECT set_config('${GATE}','fenced',false)`])
        expect({ form, calls: await bodyCalls(f, "s" + form.length, "SELECT 1", form) }).toEqual({ form, calls: 2 });
    } finally { await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); }
  });
  test("body selection: only gate exactly 'fenced' AND mode 't2' runs T2", async () => {
    const f = await fixture();
    const alter = (v: string) => `ALTER FUNCTION ${ACK} SET ${GATE}='${v}'`;
    expect(await bodyCalls(f, "m0", alter("off"))).toBe(2);
    expect(await bodyCalls(f, "m1", alter("fenced"))).toBe(2); // mode 'main'
    expect(await bodyCalls(f, "m2", `${alter("fenced")};SELECT sessions.lazy_ack_t2_set_mode('t2');${alter("off")}`)).toBe(2); // gate off ignores the row
    expect(await bodyCalls(f, "m3", `${alter("fenced")};SELECT sessions.lazy_ack_t2_set_mode('t2')`)).toBe(1);
    for (const v of ["FENCED", "on", "t2", " fenced", "fenced ", ""])
      expect({ v, calls: await bodyCalls(f, "m4" + v.trim().length + v.length, `${alter("fenced")};SELECT sessions.lazy_ack_t2_set_mode('t2');${alter(v)}`) }).toEqual({ v, calls: 2 });
    // The flips above were rolled back with their transactions.
    expect((await f.db.query<any>("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows).toEqual([{ mode: "main" }]);
  });
  test("fenced refuses REPEATABLE READ and SERIALIZABLE (either mode) before reading the mode; gate off does not", async () => {
    const f = await fixture();
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      // Gate and mode are set by the owner at READ COMMITTED (the operator functions require it), before the RR transaction.
      for (const [setup, refused] of [[["off", "main"], false], [["fenced", "main"], true], [["fenced", "t2"], true]] as const) {
        const c = await capture(f, [rec("iso-" + ++serial, 2)]);
        await f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [setup[0]]);
        await f.db.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [setup[1]]);
        await f.db.exec(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL ROLE bulls96_session_ingest`);
        try {
          const q = f.db.query<any>(COMMIT, [c.payload]);
          if (refused) await expect(q).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
          else expect((await q).rows[0].r.kind).toBe("committed");
        } finally {
          await f.db.exec("ROLLBACK");
          await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
          await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
        }
      }
    }
  });
  test("lazy_ack_t2_set_mode only TRY-locks 50000 (never queues), then the mode row NOWAIT, in that order", async () => {
    const f = await fixture();
    const src = ((await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0].prosrc as string).replace(/--[^\n]*/g, "");
    const tryLock = src.indexOf("IF NOT pg_try_advisory_xact_lock(961996,50000) THEN"), row = src.indexOf("FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE NOWAIT;");
    expect(tryLock).toBeGreaterThan(-1);
    expect(row).toBeGreaterThan(tryLock);
    expect(src.split("lazy_ack_t2_mode").length - 1).toBe(2); // the locking read and the UPDATE, both after the try-lock
    expect(src).not.toMatch(/pg_advisory_xact_lock\(|pg_advisory_lock\(|lock_timeout/);
  });
  test("the down refuses unless the mode is main and the gate is not fenced; then restores 043800 exactly", async () => {
    const f = await fixture();
    const { readFileSync } = await import("node:fs");
    const DOWN = readFileSync("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql", "utf8");
    // The PGlite fixture has no migrator ledger: a minimal one with this file's own checksum, as the real migrator records it.
    const sha = (await import("node:crypto")).createHash("sha256").update(readFileSync(MIG, "utf8")).digest("hex");
    // (the fixture's marker table, privacy-authority-support.ts, lacks the updated_at the real one has)
    await f.db.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL);
      ALTER TABLE sessions.__96bulls_role_validation ADD COLUMN IF NOT EXISTS updated_at timestamptz`);
    await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20261105062210_lazy_ack_t2.sql',$1)", [sha]);
    const attempt = async () => { const e = await f.db.exec(DOWN).then(() => null, (x: any) => x); if (e) await f.db.exec("ROLLBACK"); return e; };
    const alter = (v: string) => f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='${v}'`);
    await alter("fenced");
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    await alter("off"); // (never done by the operator while the row says 't2'; the down still refuses)
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    await alter("fenced");
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the gate is fenced") });
    expect(await f.db.query<any>("SELECT to_regclass('sessions.lazy_ack_t2_mode') r").then((r) => r.rows[0].r)).toBe("sessions.lazy_ack_t2_mode");
    await alter("off");
    expect(await attempt()).toBeNull();
    expect((await f.db.query<any>("SELECT to_regclass('sessions.lazy_ack_t2_mode') r")).rows[0].r).toBeNull();
    const p = (await f.db.query<any>("SELECT proconfig,prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(p.proconfig).toEqual(["search_path=pg_catalog, pg_temp"]);
    expect(p.prosrc).toBe(ackBody(readFileSync(MAIN_MIG, "utf8")));
  });
  test("lazy_ack_t2_set_gate: the supported gate change; requires mode main, returns the previous value, owner-only, read committed", async () => {
    const f = await fixture();
    const gate = async () => (await f.db.query<any>("SELECT proconfig c FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].c as string[];
    const sm = (await f.db.query<any>("SELECT prosecdef,proconfig FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_gate(text)'::regprocedure")).rows[0];
    expect(sm).toEqual({ prosecdef: false, proconfig: ["search_path=pg_catalog, pg_temp"] });
    for (const bad of ["on", "FENCED", "", "t2", "main"])
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [bad])).rejects.toMatchObject({ code: "22023" });
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate(NULL)")).rejects.toMatchObject({ code: "22023" });
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("off");
    expect(await gate()).toContain(GATE + "=fenced");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("fenced");
    // mode 't2' (set under the fenced gate): no gate change either way, gate unchanged.
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
    for (const g of ["off", "fenced"]) {
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [g])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("only while the mode is main") });
      expect(await gate()).toContain(GATE + "=fenced");
    }
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('off') o")).rows[0].o).toBe("fenced");
    expect(await gate()).toEqual(["search_path=pg_catalog, pg_temp", GATE + "=off"]);
    // An out-of-protocol RESET leaves no entry: reported as 'absent', and set_gate restores an explicit value.
    await f.db.exec(`ALTER FUNCTION ${ACK} RESET ${GATE}`);
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('off') o")).rows[0].o).toBe("absent");
    expect(await gate()).toContain(GATE + "=off");
    // Both operator functions refuse anything but READ COMMITTED (their reads must follow the try-lock).
    for (const [iso, stmt] of [["REPEATABLE READ", "SELECT sessions.lazy_ack_t2_set_gate('fenced')"], ["SERIALIZABLE", "SELECT sessions.lazy_ack_t2_set_gate('fenced')"],
      ["REPEATABLE READ", "SELECT sessions.lazy_ack_t2_set_mode('main')"], ["SERIALIZABLE", "SELECT sessions.lazy_ack_t2_set_mode('main')"]] as const) {
      await f.db.exec(`BEGIN ISOLATION LEVEL ${iso}`);
      try { await expect(f.db.exec(stmt)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("requires read committed") }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
    expect(await gate()).toContain(GATE + "=off");
  });
  test("lazy_ack_t2_set_mode: returns the old mode, refuses anything but main/t2 (22023) and leaves the row unchanged", async () => {
    const f = await fixture();
    for (const bad of ["T2", "off", "fenced", "", " t2"])
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [bad])).rejects.toMatchObject({ code: "22023" });
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode(NULL)")).rejects.toMatchObject({ code: "22023" });
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('main') o")).rows[0].o).toBe("main");
    // 't2' only under the fenced gate (review M1): refused (55000) with the gate off or any non-'fenced' value, row unchanged.
    for (const v of ["off", "FENCED", "on", ""]) {
      await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='${v}'`);
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("requires the gate fenced first") });
      expect((await f.db.query<any>("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows).toEqual([{ mode: "main" }]);
    }
    await f.db.exec(`ALTER FUNCTION ${ACK} RESET ${GATE}`); // no gate entry at all: refused too
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')")).rejects.toMatchObject({ code: "55000" });
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='fenced'`);
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
    await f.db.exec(`ALTER FUNCTION ${ACK} SET ${GATE}='off'`);
    // 'main' is always allowed (the way back), whatever the gate says.
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('main') o")).rows[0].o).toBe("t2");
    await expect(f.db.query("UPDATE sessions.lazy_ack_t2_mode SET mode='x'")).rejects.toMatchObject({ code: "23514" });
    await expect(f.db.query("INSERT INTO sessions.lazy_ack_t2_mode VALUES(false,'main')")).rejects.toMatchObject({ code: "23514" });
    expect((await f.db.query<any>("SELECT mode FROM sessions.lazy_ack_t2_mode")).rows).toEqual([{ mode: "main" }]);
  });
});

describe("062210 differential equality: accepted, refused, same slot in one batch", () => {
  test("fresh distinct records: all accepted, consecutive seats; T2 runs one round", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("a1", 2), rec("a2", 3), rec("a3", 1)]);
    const { off, on } = await both(f, c.payload);
    committed(on);
    expect(off.triggerCalls).toBe(3);
    expect(on.triggerCalls).toBe(1);
    expect(rows(on, "sessions.ingest_lazy_ack_decisions").filter((d) => d.claim_token === c.claimToken).map((d) => d.body_accepted)).toEqual([true, true, true]);
  });
  test("one slot three times in one batch, rising revisions: three accepts, lineage +3, three rounds", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("dup", 2), rec("dup", 3), rec("dup", 4)]);
    const { off, on } = await both(f, c.payload);
    committed(on);
    expect([off.triggerCalls, on.triggerCalls]).toEqual([3, 3]);
    const d = rows(on, "sessions.ingest_lazy_ack_decisions").filter((x) => x.claim_token === c.claimToken).sort((x, y) => x.ord - y.ord);
    expect(d.map((x) => [x.body_accepted, x.native_lineage])).toEqual([[true, 1], [true, 2], [true, 3]]);
  });
  test("one slot twice in one batch, equal revision: the second is refused and reuses the seat allocated by the first", async () => {
    // A real delivery refuses an equal revision with different content at the claim (delivery_conflict), so the state is
    // staged: the attempt's second record is rewritten to the first's revision before the commit (both modes alike).
    const f = await fixture();
    const c = await capture(f, [rec("eq", 2), rec("eq", 3)]);
    const { on } = await both(f, c.payload, (g) => g.db.query(`UPDATE sessions.ingest_claim_attempts SET records=(SELECT jsonb_agg(CASE WHEN (e->>'ord')::int=1
      THEN jsonb_set(e,'{revision}','2') ELSE e END ORDER BY i) FROM jsonb_array_elements(records) WITH ORDINALITY x(e,i)) WHERE claim_token=$1`, [c.claimToken]));
    committed(on);
    const d = rows(on, "sessions.ingest_lazy_ack_decisions").filter((x) => x.claim_token === c.claimToken).sort((x, y) => x.ord - y.ord);
    expect(d.map((x) => x.body_accepted)).toEqual([true, false]);
    expect(d[1].seat_key_sha256).toBe(d[0].seat_key_sha256);
    expect(d[1].first_digest).toBe(d[0].candidate_digest);
  });
  test("one slot twice in one batch, falling revision: the second is refused against the first", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("fall", 4), rec("fall", 2)]);
    const { on } = await both(f, c.payload);
    committed(on);
    const d = rows(on, "sessions.ingest_lazy_ack_decisions").filter((x) => x.claim_token === c.claimToken).sort((x, y) => x.ord - y.ord);
    expect(d.map((x) => [x.body_accepted, x.native_lineage])).toEqual([[true, 1], [false, 1]]);
  });
  test("interleaved slots with mixed accepts and refusals across rounds", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("m-a", 2), rec("m-b", 3), rec("m-a", 3), rec("m-c", 1), rec("m-b", 2), rec("m-a", 5), rec("m-a", 4), rec("m-b", 1)]);
    const { off, on } = await both(f, c.payload);
    committed(on);
    expect(off.triggerCalls).toBe(5); // accepts: a2, b3, a3, c1, a5
    expect(on.triggerCalls).toBe(3); // T2 rounds: a has three accepts
  });
  test("a later batch: equal revision keeps the current seat and body, a higher revision gets a new seat, a lower one is refused", async () => {
    const f = await fixture();
    await deliver(f, [rec("later", 7, { payload: { text: "first", ts_ms: 1 } })]);
    for (const [revision, text] of [[7, "later different"], [8, "new revision"], [6, "older"]] as const) {
      const c = await capture(f, [rec("later", revision, { payload: { text, ts_ms: 2 } })]);
      const { on } = await both(f, c.payload);
      committed(on);
    }
  });
  test("a large mixed batch (500 records over 40 slots, seeded revisions) with prior custody on some slots", async () => {
    const f = await fixture();
    await deliver(f, Array.from({ length: 10 }, (_, i) => rec("big-" + i, 5)));
    let x = 12345;
    const rnd = () => (x = (x * 1103515245 + 12345) % 2147483648) / 2147483648;
    // Unique (record, revision) pairs: a real delivery refuses an equal revision with different content.
    const seen = new Set<string>(), recs: Rec[] = [];
    while (recs.length < 500) {
      const id = "big-" + Math.floor(rnd() * 40), revision = 1 + Math.floor(rnd() * 40);
      if (seen.has(id + "/" + revision)) continue;
      seen.add(id + "/" + revision); recs.push(rec(id, revision));
    }
    const c = await capture(f, recs);
    const { off, on } = await both(f, c.payload);
    committed(on);
    expect(on.triggerCalls).toBeLessThan(off.triggerCalls);
    const d = rows(on, "sessions.ingest_lazy_ack_decisions").filter((y) => y.claim_token === c.claimToken);
    expect(d.length).toBe(500);
    expect(d.some((y) => y.body_accepted) && d.some((y) => !y.body_accepted)).toBe(true);
  });
});

describe("062210 differential equality: refusals without a seat, adoption, replay, suppression, expiry", () => {
  test("a refused record whose slot has no current seat allocates one (twice for two such records)", async () => {
    const f = await fixture();
    await deliver(f, [rec("noseat", 5)]);
    const c = await capture(f, [rec("noseat", 4), rec("noseat", 3)]);
    const { on } = await both(f, c.payload, (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_seat_key=NULL WHERE key_sha256=${slotKey("noseat")}`));
    committed(on);
    const d = rows(on, "sessions.ingest_lazy_ack_decisions").filter((x) => x.claim_token === c.claimToken).sort((x, y) => x.ord - y.ord);
    expect(d.map((x) => x.body_accepted)).toEqual([false, false]);
    expect(d[0].seat_key_sha256).not.toBe(d[1].seat_key_sha256);
  });
  test("a matching decision already present is adopted silently", async () => {
    const f = await fixture();
    await deliver(f, [rec("adopt", 5)]);
    const c = await capture(f, [rec("adopt", 4)]);
    const dry = await runMode(f, "off", c.payload, async () => {}, []);
    const pre = rows(dry, "sessions.ingest_lazy_ack_decisions").find((x) => x.claim_token === c.claimToken)!;
    expect(pre.body_accepted).toBe(false); // refused: its seat already exists, so the row can be staged before the ACK
    const { on } = await both(f, c.payload, (g) => g.db.query("INSERT INTO sessions.ingest_lazy_ack_decisions SELECT * FROM jsonb_populate_record(NULL::sessions.ingest_lazy_ack_decisions,$1::jsonb)", [JSON.stringify(pre)]));
    committed(on);
  });
  test("replay of a committed commit (lost response): same result and custody ack, no second seat", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("replay-1", 2), rec("replay-2", 2)]);
    expect((((await f.run(COMMIT, [c.payload])) as any).rows[0].r).kind).toBe("committed");
    const { on } = await both(f, c.payload);
    committed(on);
    expect((on.out as any).result.custodyAck).toBeDefined();
  });
  test("a suppressed ACK (deleted session) takes the unchanged KEY SHARE path in both modes", async () => {
    const f = await fixture();
    await deliver(f, [rec("supp-warm", 1)]); // first-seen adoption before any per-user row (else 55000 unbound prior authority)
    await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude','s-supp')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN]);
    // A suppressed receipt is ACKed by the claim itself (its batch row commits there), so the claim is the call compared.
    const c = await capture(f, [rec("supp", 2, { session_id: "s-supp" })], "ingest_claim_buffered_batch");
    const { on } = await both(f, c.payload, async () => {}, "SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r");
    expect(on.out).toMatchObject({ ok: true });
    expect(rows(on, "sessions.ingest_batches").some((b) => b.buffer_receipt_kind === "suppressed" && b.state === "committed")).toBe(true);
  });
  test("an expired stage is refused identically", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("exp", 2)]);
    const { on } = await both(f, c.payload, (g) => g.db.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1", [c.stageToken]));
    expect(on.out).toBeDefined();
  });
});

describe("062210 differential equality: the first error, in the loop's order", () => {
  test("another user's slot on the third of four records: P0W01", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("e-1", 2), rec("e-2", 2), rec("e-3", 2), rec("e-4", 2)]);
    const { on } = await both(f, c.payload, (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_owner='someone-else',current_revision=1,current_digest=repeat('f',64),
      current_source='claude',current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=${slotKey("e-3")}`));
    failed(on, "P0W01", "native key currently belongs to another user");
  });
  test("a later P0W01 loses to an earlier slot mismatch", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("o-1", 2), rec("o-2", 2), rec("o-3", 2)]);
    const { on } = await both(f, c.payload, async (g) => {
      await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET control_epoch=repeat('9',64) WHERE key_sha256=${slotKey("o-2")}`);
      await g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_owner='someone-else',current_revision=1,current_digest=repeat('f',64),
        current_source='claude',current_reset_epoch=0,current_fence_epoch=0 WHERE key_sha256=${slotKey("o-3")}`);
    });
    failed(on, "55000", "initialized native custody slot required");
  });
  test("a missing processing rank is raised when the loop reaches it (NULLS LAST), after the earlier records", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("r-1", 2), rec("r-2", 2), rec("r-3", 2)]);
    const { on } = await both(f, c.payload, (g) => g.db.query("UPDATE sessions.ingest_claim_attempts SET source_processing_ranks=jsonb_set(source_processing_ranks,'{0}','null') WHERE claim_token=$1", [c.claimToken]));
    failed(on, "55000", "source processing rank missing");
  });
  test("custody seat drift on a refused record", async () => {
    const f = await fixture();
    await deliver(f, [rec("drift", 5)]);
    const c = await capture(f, [rec("drift", 4)]);
    const { on } = await both(f, c.payload, (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET current_digest=repeat('e',64) WHERE key_sha256=${slotKey("drift")}`));
    failed(on, "55000", "current custody seat drift");
  });
  test("seat exhaustion on the second allocation, and on the first", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("x-1", 2), rec("x-2", 2)]);
    for (const last of ["9223372036854775806", MAX]) {
      const { on } = await both(f, c.payload, (g) => g.db.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=$1::bigint", [last]));
      failed(on, "55000", "ACK seats exhausted");
    }
  });
  test("seat numbering up to the last representable seat succeeds", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("y-1", 2), rec("y-2", 2)]);
    const { on } = await both(f, c.payload, (g) => g.db.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=9223372036854775805"));
    committed(on);
    expect((rows(on, "sessions.ingest_receipt_seat_clock")[0] as any).last_seat).toBe(Number(MAX));
  });
  test("native lineage exhaustion on the second record", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("l-1", 2), rec("l-2", 2)]);
    const { on } = await both(f, c.payload, (g) => g.db.query(`UPDATE sessions.ingest_lazy_native_slots SET lineage=${MAX} WHERE key_sha256=${slotKey("l-2")}`));
    failed(on, "55000", "native lineage exhausted");
  });
  test("a conflicting decision already present: adoption refused", async () => {
    const f = await fixture();
    await deliver(f, [rec("conf", 5)]);
    const c = await capture(f, [rec("conf-0", 2), rec("conf", 4)]);
    const dry = await runMode(f, "off", c.payload, async () => {}, []);
    const pre = rows(dry, "sessions.ingest_lazy_ack_decisions").find((x) => x.claim_token === c.claimToken && !x.body_accepted)!;
    const bad = { ...pre, native_lineage: pre.native_lineage + 7 };
    const { on } = await both(f, c.payload, (g) => g.db.query("INSERT INTO sessions.ingest_lazy_ack_decisions SELECT * FROM jsonb_populate_record(NULL::sessions.ingest_lazy_ack_decisions,$1::jsonb)", [JSON.stringify(bad)]));
    failed(on, "55000", "immutable ACK decision adoption refused");
  });
});

describe("062210 differential equality with canonical history active", () => {
  /** A managed (installed) scope for session s-h, scope keys for its records, a live build touching one record, a live derived object. */
  async function history(g: Fixture, recordIds: string[]) {
    const auth = "11111111-1111-1111-1111-111111111111";
    await g.db.exec("SET LOCAL session_replication_role=replica");
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,
      pointer_snapshot,pointer_snapshot_sha256,pointer_count,content_fingerprint)
      VALUES($1,$2,$3,'claude','s-h',1,repeat('a',64),0,'anchor','bucket','region','root','ready','[]',encode(sha256(convert_to('[]'::jsonb::text,'UTF8')),'hex'),0,repeat('c',64))`, [auth, TENANT.orgId, TENANT.workspaceId]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,'claude','s-h',$3,true)", [TENANT.orgId, TENANT.workspaceId, auth]);
    for (const r of recordIds) await g.db.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,$3,'claude','s-h') ON CONFLICT DO NOTHING", [TENANT.orgId, TENANT.workspaceId, r]);
    await g.db.query("INSERT INTO sessions.ingest_history_derived_objects(object_key,authority_id,sha256,bytes) VALUES('obj-1',$1,repeat('b',64),1)", [auth]);
    await g.db.query(`INSERT INTO sessions.ingest_history_scope_builds(build_id,org_id,workspace_id,harness,session_id,generation,lease_token,lease_until,state)
      VALUES('22222222-2222-2222-2222-222222222222',$1,$2,'claude','s-other',$3,'33333333-3333-3333-3333-333333333333',clock_timestamp()+interval '1 hour','keys')`, [TENANT.orgId, TENANT.workspaceId, generation]);
    await g.db.query("INSERT INTO sessions.ingest_history_scope_build_records VALUES('22222222-2222-2222-2222-222222222222',$1)", [recordIds[0]]);
    await g.db.exec("SET LOCAL session_replication_role=origin");
    expect((await g.db.query<any>("SELECT sessions.ingest_history_active() a")).rows[0].a).toBe(true);
  }
  test("same-session records, one slot repeated: per-accept trigger calls, request_version and replay epochs equal", async () => {
    const f = await fixture();
    const ids = ["h-1", "h-2", "h-1", "h-3"];
    const c = await capture(f, ids.map((id, i) => rec(id, 2 + i, { session_id: "s-h" })));
    const { off, on } = await both(f, c.payload, (g) => history(g, ["h-1", "h-2", "h-3"]), COMMIT, [true]);
    committed(on);
    expect([off.triggerCalls, on.triggerCalls]).toEqual([4, 4]);
    const req = rows(on, "sessions.ingest_history_required_scopes");
    expect(req.length).toBeGreaterThan(0);
    expect(Math.max(...req.map((r) => Number(r.request_version)))).toBeGreaterThan(1);
    expect(rows(on, "sessions.ingest_history_scope_builds").every((b) => b.state === "abandoned")).toBe(true);
    expect(rows(on, "sessions.ingest_history_reconcile_outbox").length).toBeGreaterThan(0);
  });
});
