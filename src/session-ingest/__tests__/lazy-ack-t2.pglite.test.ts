/**
 * 20261105062210_lazy_ack_t2, differential equality (PGlite; every migration incl. 062210).
 *
 * sessions.ingest_lazy_ack_batch selects its body from the owner-only singleton row sessions.lazy_ack_t2_mode(gate, mode)
 * (installed off/main: main's 043800 body plus one singleton read); T2 runs only for fenced/t2. T2 takes the clock FOR KEY SHARE, plans under the slot locks, then upgrades to FOR NO KEY
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
/** The exact gate read 062210 inserts after the suppressed return (pinned by the byte-identity test). */
const GATE_READ = ` -- 062210 gate row, read once: after ingest_lock_fences (which took (961996,50000) SHARED, held to transaction end, so
 -- no gate or mode change can commit from here on) and after the suppressed return, before the clock lock is chosen.
 -- READ COMMITTED: a plain read; its snapshot is taken now, after 50000, so it is current. Otherwise the transaction
 -- snapshot may predate a committed change: FOR SHARE conflicts with the setters' UPDATE, so a replaced version gives
 -- 40001 (FOR KEY SHARE would not: non-key update); a current 'fenced' row is refused, a current 'off' row runs main.
 IF current_setting('transaction_isolation')='read committed' THEN
  SELECT m.gate,m.mode INTO v_gate,v_mode FROM sessions.lazy_ack_t2_mode m WHERE m.singleton;
 ELSE
  SELECT m.gate,m.mode INTO v_gate,v_mode FROM sessions.lazy_ack_t2_mode m WHERE m.singleton FOR SHARE;
 END IF;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: the T2 gate row is missing';END IF;
 IF v_gate<>'off' AND current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: the fenced staged ACK requires read committed';END IF;
 v_t2:=v_gate='fenced' AND v_mode='t2';
`;
const gateRow = async (f: Fixture) => (await f.db.query<{ gate: string; mode: string }>("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows;
/** The supported path to a row state (owner, READ COMMITTED, one statement each): mode main first, then the gate, then the mode. */
async function setRow(f: Fixture, gate: "off" | "fenced", mode: "main" | "t2") {
  await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
  await f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [gate]);
  await f.db.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [mode]);
}
async function runMode(f: Fixture, mode: Mode, payload: string, setup: (f: Fixture) => Promise<unknown>, seqs: Array<{ s: string; v: string | null; start: string }>, sql = COMMIT): Promise<Run> {
  for (const q of seqs) await f.db.query("SELECT setval($1::regclass,$2::bigint,$3)", [q.s, q.v ?? q.start, q.v !== null]);
  await f.db.exec("BEGIN");
  try {
    await f.db.exec("SET LOCAL track_functions='pl'");
    if (mode !== "off") {
      expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') old")).rows[0].old).toBe("off");
      expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode($1) old", [mode === "t2" ? "t2" : "main"])).rows[0].old).toBe("main");
    }
    const g = (await gateRow(f))[0]!;
    const gate = [g.gate, g.mode];
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
    expect(off.gate).toEqual(["off", "main"]);
    expect(fmain.gate).toEqual(["fenced", "main"]);
    expect(on.gate).toEqual(["fenced", "t2"]);
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
  } finally { await f.db.exec("ROLLBACK"); }
}

describe("062210 installs the gate row off/main, and keeps the 043800 contract", () => {
  test("no function-level gate: proconfig is search_path only; row off/main; owner-only table, setters and keep trigger; SECURITY INVOKER", async () => {
    const f = await fixture();
    const p = (await f.db.query<any>("SELECT proconfig,prosecdef FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(p.proconfig).toEqual(["search_path=pg_catalog, pg_temp"]);
    expect(p.prosecdef).toBe(false);
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    // Logical key and cardinality: the boolean singleton PK with CHECK(singleton); the two-column CHECK; DELETE/TRUNCATE refused.
    const cons = (await f.db.query<any>("SELECT conname,contype,pg_get_constraintdef(oid) d FROM pg_constraint WHERE conrelid='sessions.lazy_ack_t2_mode'::regclass AND contype IN('p','c','u','f','x') ORDER BY conname")).rows;
    expect(cons.map((c: any) => [c.contype, c.d])).toEqual(expect.arrayContaining([["p", "PRIMARY KEY (singleton)"], ["c", "CHECK (singleton)"],
      ["c", "CHECK ((mode = ANY (ARRAY['main'::text, 't2'::text])))"], ["c", "CHECK ((gate = ANY (ARRAY['off'::text, 'fenced'::text])))"],
      ["c", "CHECK (((mode = 'main'::text) OR (gate = 'fenced'::text)))"]]));
    expect(cons.length).toBe(5);
    expect((await f.db.query<any>("SELECT tgname,tgtype FROM pg_trigger WHERE tgrelid='sessions.lazy_ack_t2_mode'::regclass AND NOT tgisinternal ORDER BY 1")).rows.map((r: any) => r.tgname))
      .toEqual(["lazy_ack_t2_mode_keep_row", "lazy_ack_t2_mode_no_truncate"]);
    const priv = (await f.db.query<any>(`SELECT r.rolname,
        has_table_privilege(r.oid,'sessions.lazy_ack_t2_mode','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') t,
        has_function_privilege(r.oid,'sessions.lazy_ack_t2_set_mode(text)','EXECUTE') OR has_function_privilege(r.oid,'sessions.lazy_ack_t2_set_gate(text)','EXECUTE')
         OR has_function_privilege(r.oid,'sessions.lazy_ack_t2_mode_keep()','EXECUTE') x
      FROM pg_roles r WHERE r.rolname IN('bulls96_session_ingest','bulls96_session_ingest_seeder','anon','authenticated','service_role') ORDER BY 1`)).rows;
    expect(priv.length).toBeGreaterThan(0);
    for (const r of priv) expect(r).toEqual({ rolname: r.rolname, t: false, x: false });
    for (const fn of ["sessions.lazy_ack_t2_set_mode(text)", "sessions.lazy_ack_t2_set_gate(text)", "sessions.lazy_ack_t2_mode_keep()"])
      expect((await f.db.query<any>("SELECT prosecdef,proconfig FROM pg_proc WHERE oid=$1::regprocedure", [fn])).rows[0]).toEqual({ prosecdef: false, proconfig: ["search_path=pg_catalog, pg_temp"] });
    expect((await f.db.query<any>("SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid='sessions.lazy_ack_t2_mode'::regclass")).rows[0]).toEqual({ relrowsecurity: true, relforcerowsecurity: false });
    // No function in the sessions schema carries any bulls96.* setting.
    expect((await f.db.query<any>("SELECT count(*)::int n FROM pg_proc p,unnest(p.proconfig) c WHERE c LIKE 'bulls96.%'")).rows[0].n).toBe(0);
  });
  test("with every T2 addition removed, the installed body is 043800's body byte for byte; the one gate read is pinned exactly", async () => {
    // What this pins (it replaced r3's "gate off = same bytes, no read"): the installed body is 043800's body plus exactly
    // (1) the extended DECLARE line, (2) GATE_READ below, inserted right after the suppressed return and before the clock,
    // and (3) the two IF NOT v_t2 ... ELSE <T2> END IF branches. So with gate 'off' the statements that run are 043800's
    // plus GATE_READ: one singleton read (READ COMMITTED: plain, AccessShareLock on the table only; otherwise FOR SHARE on
    // the row) and the isolation checks. Outputs, first errors, rows and sequences are the differential suites' claim.
    const f = await fixture();
    const { readFileSync } = await import("node:fs");
    const installed = (await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].prosrc as string;
    expect(installed).toBe(ackBody(readFileSync(MIG, "utf8")));
    expect(installed.split(GATE_READ).length - 1).toBe(1);
    let x = installed.replace(/\n v_t2 boolean;[^\n]*/, "");
    x = x.replace(GATE_READ, "");
    x = x.replace(/ IF NOT v_t2 THEN\n( SELECT \* INTO STRICT c FROM sessions\.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c\.last_seat;\n) ELSE\n[\s\S]*?\n END IF;\n/, "$1");
    x = x.replace(" IF NOT v_t2 THEN\n FOR r IN", " FOR r IN");
    x = x.replace(/( END LOOP;\n) ELSE\n[\s\S]*\n END IF;\n( UPDATE sessions\.ingest_receipt_seat_clock SET last_seat=v_last)/, "$1$2");
    expect(x).toBe(ackBody(readFileSync(MAIN_MIG, "utf8")));
  });
  test("the suppressed path is 043800's text: everything from BEGIN through the suppressed RETURN is identical in both bodies (guard, fences, seal, clock KEY SHARE, return), and no gate read precedes it", async () => {
    const { readFileSync } = await import("node:fs");
    const upToSuppressed = (body: string) => {
      const b = body.indexOf("\nBEGIN\n"), r = body.indexOf("FOR KEY SHARE;RETURN;END IF;\n");
      expect(b > -1 && r > b).toBe(true);
      return body.slice(b, r + "FOR KEY SHARE;RETURN;END IF;\n".length);
    };
    const t2 = upToSuppressed(ackBody(readFileSync(MIG, "utf8"))), main = upToSuppressed(ackBody(readFileSync(MAIN_MIG, "utf8")));
    expect(t2).toBe(main);
    expect(t2).toContain("IF p->>'buffer_receipt_kind'='suppressed' THEN\n  PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;RETURN;END IF;");
    expect(t2).not.toContain("lazy_ack_t2_mode");
    expect(t2).not.toContain("transaction_isolation");
  });
  test("statement order: fences (barrier) -> suppressed return -> gate row read -> isolation check -> clock; T2: KEY SHARE, then the upgrade reads last_seat", async () => {
    const f = await fixture();
    const src = (await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].prosrc as string;
    const at = (s: string, from = 0) => { const i = src.indexOf(s, from); expect({ s, i: i > -1 }).toEqual({ s, i: true }); return i; };
    const fences = at("PERFORM sessions.ingest_lock_fences("), supp = at("IF p->>'buffer_receipt_kind'='suppressed' THEN");
    const rcRead = at("FROM sessions.lazy_ack_t2_mode m WHERE m.singleton;"), rrRead = at("FROM sessions.lazy_ack_t2_mode m WHERE m.singleton FOR SHARE;");
    const missing = at("the T2 gate row is missing"), iso = at("the fenced staged ACK requires read committed");
    const ks = at("WHERE singleton FOR KEY SHARE;\n END IF;"), up = at("SELECT ck.last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock ck WHERE ck.singleton FOR NO KEY UPDATE;");
    expect(fences < supp && supp < rcRead && rcRead < rrRead && rrRead < missing && missing < iso && iso < ks && ks < up).toBe(true);
    expect(src.split("lazy_ack_t2_mode").length - 1).toBe(2); // the two read forms, one executed per call
    expect(src).not.toMatch(/FOR KEY SHARE[^;]*lazy_ack_t2_mode|lazy_ack_t2_mode[^;]*FOR KEY SHARE/);
    expect(src).not.toContain("bulls96.");
    const t2 = src.slice(src.indexOf(" ELSE\n  -- (A)")).replace(/--[^\n]*/g, "");
    expect(t2.indexOf("last_seat")).toBe(t2.indexOf("ck.last_seat") + 3);
    expect(t2.slice(0, t2.indexOf("ck.last_seat"))).not.toContain("c.last_seat");
  });
  test("the runtime role takes the normal ACK path; direct table access and the setters are refused (42501); a session value of the old parameter selects nothing", async () => {
    const f = await fixture();
    for (const stmt of ["SELECT sessions.lazy_ack_t2_set_mode('t2')", "SELECT sessions.lazy_ack_t2_set_gate('fenced')", "SELECT sessions.lazy_ack_t2_mode_keep()",
      "UPDATE sessions.lazy_ack_t2_mode SET mode='t2'", "UPDATE sessions.lazy_ack_t2_mode SET gate='fenced'",
      "SELECT mode FROM sessions.lazy_ack_t2_mode", "INSERT INTO sessions.lazy_ack_t2_mode VALUES(true,'t2','fenced')",
      "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR UPDATE", "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR NO KEY UPDATE",
      "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR SHARE", "SELECT 1 FROM sessions.lazy_ack_t2_mode FOR KEY SHARE",
      "LOCK TABLE sessions.lazy_ack_t2_mode IN ACCESS SHARE MODE", "DELETE FROM sessions.lazy_ack_t2_mode", "TRUNCATE sessions.lazy_ack_t2_mode",
      `ALTER FUNCTION ${ACK} SET search_path=public`]) {
      await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
      try { await expect(f.db.exec(stmt)).rejects.toMatchObject({ code: "42501" }); } finally { await f.db.exec("ROLLBACK"); }
    }
    // The normal path (runtime role, SECURITY DEFINER commit entry point) commits with the row off/main and fenced/t2.
    expect(await bodyCalls(f, "rt0", "SELECT 1")).toBe(2);
    expect(await bodyCalls(f, "rt1", "SELECT sessions.lazy_ack_t2_set_gate('fenced');SELECT sessions.lazy_ack_t2_set_mode('t2')")).toBe(1);
    for (const form of [`SET LOCAL bulls96.lazy_ack_t2='fenced'`, `SELECT set_config('bulls96.lazy_ack_t2','fenced',true)`])
      expect({ form, calls: await bodyCalls(f, "s" + form.length, "SELECT 1", form) }).toEqual({ form, calls: 2 });
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
  });
  test("body selection: only the row fenced/t2 runs T2; the row can never say off/t2", async () => {
    const f = await fixture();
    expect(await bodyCalls(f, "m0", "SELECT 1")).toBe(2); // off/main
    expect(await bodyCalls(f, "m1", "SELECT sessions.lazy_ack_t2_set_gate('fenced')")).toBe(2); // fenced/main
    expect(await bodyCalls(f, "m3", "SELECT sessions.lazy_ack_t2_set_gate('fenced');SELECT sessions.lazy_ack_t2_set_mode('t2')")).toBe(1);
    for (const [sql, code] of [["UPDATE sessions.lazy_ack_t2_mode SET mode='t2'", "23514"], ["UPDATE sessions.lazy_ack_t2_mode SET gate='on'", "23514"],
      ["UPDATE sessions.lazy_ack_t2_mode SET gate=NULL", "23502"], ["UPDATE sessions.lazy_ack_t2_mode SET mode='x'", "23514"],
      ["INSERT INTO sessions.lazy_ack_t2_mode VALUES(false,'main','off')", "23514"], ["INSERT INTO sessions.lazy_ack_t2_mode VALUES(true,'main','off')", "23505"]] as const) {
      await f.db.exec("BEGIN;SELECT pg_advisory_xact_lock(961996,50000)"); // past the keep trigger: the constraints themselves
      try { await expect(f.db.exec(sql)).rejects.toMatchObject({ code }); } finally { await f.db.exec("ROLLBACK"); }
    }
    // Without 50000 EXCLUSIVE even a valid direct UPDATE by the owner is refused (the trigger), and with 50000 SHARED too.
    for (const pre of ["SELECT 1", "SELECT pg_advisory_xact_lock_shared(961996,50000)", "SELECT pg_advisory_xact_lock(961996,50001)"]) {
      await f.db.exec(`BEGIN;${pre}`);
      try { await expect(f.db.exec("UPDATE sessions.lazy_ack_t2_mode SET gate='fenced'")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("changed only by") }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
    await f.db.exec("BEGIN");
    try { await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
      await expect(f.db.exec("UPDATE sessions.lazy_ack_t2_mode SET gate='off'")).rejects.toMatchObject({ code: "23514" }); } finally { await f.db.exec("ROLLBACK"); }
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
  });
  test("exactly one row: DELETE and TRUNCATE are refused (55000); a missing row fails the staged ACK and both setters closed (55000), never main", async () => {
    const f = await fixture();
    for (const sql of ["DELETE FROM sessions.lazy_ack_t2_mode", "TRUNCATE sessions.lazy_ack_t2_mode"]) {
      await f.db.exec("BEGIN");
      try { await expect(f.db.exec(sql)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the gate row is a singleton") }); } finally { await f.db.exec("ROLLBACK"); }
    }
    const c = await capture(f, [rec("miss-1", 2)]);
    const gone = "ALTER TABLE sessions.lazy_ack_t2_mode DISABLE TRIGGER lazy_ack_t2_mode_keep_row;DELETE FROM sessions.lazy_ack_t2_mode";
    for (const iso of ["READ COMMITTED", "REPEATABLE READ"]) {
      await f.db.exec(`BEGIN ISOLATION LEVEL ${iso};${gone};SET LOCAL ROLE bulls96_session_ingest`);
      try { await expect(f.db.query(COMMIT, [c.payload])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the T2 gate row is missing") }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
    for (const [sql, msg] of [["SELECT sessions.lazy_ack_t2_set_gate('fenced')", "gate unchanged"], ["SELECT sessions.lazy_ack_t2_set_mode('main')", "mode unchanged"]] as const) {
      await f.db.exec(`BEGIN;${gone}`);
      try { await expect(f.db.exec(sql)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the gate row is missing; " + msg) }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    expect(((await f.run(COMMIT, [c.payload])) as any).rows[0].r.kind).toBe("committed");
  });
  test("REPEATABLE READ / SERIALIZABLE: off/main runs main; fenced refuses a staged ACK (either mode); suppressed receipts are never refused by the gate", async () => {
    // Serial (one connection): the row is current here. The stale-snapshot schedules (FOR SHARE -> 40001) are the
    // real-PostgreSQL suite's. Suppressed receipts: the claim that ACKs them refuses non-READ COMMITTED itself (043500,
    // 40001 'lazy records require read committed') in every state, as on main; the ACK's own suppressed path, called
    // directly at RR/SER on a real suppressed batch row, returns before the gate read in every state: no refusal, no
    // lock on the gate table.
    const f = await fixture();
    await deliver(f, [rec("iso-supp-warm", 1)]);
    const sid = "s-isosupp";
    await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, ORIGIN, sid]);
    const s0 = await capture(f, [rec("isosupp-0", 2, { session_id: sid })], "ingest_claim_buffered_batch");
    await f.db.exec("SET ROLE bulls96_session_ingest");
    try { await f.db.query("SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r", [s0.payload]); } finally { await f.db.exec("RESET ROLE"); }
    const suppRow = "SELECT sessions.ingest_lazy_ack_batch(to_jsonb(b)) FROM sessions.ingest_batches b WHERE b.buffer_receipt_kind='suppressed' AND b.state='committed' ORDER BY b.batch_id LIMIT 1";
    expect((await f.db.query<any>("SELECT count(*)::int n FROM sessions.ingest_batches WHERE buffer_receipt_kind='suppressed' AND state='committed'")).rows[0].n).toBe(1);
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      for (const [state, refused] of [[["off", "main"], false], [["fenced", "main"], true], [["fenced", "t2"], true]] as const) {
        const c = await capture(f, [rec("iso-" + ++serial, 2)]);
        const s = await capture(f, [rec("isosupp-" + serial, 2, { session_id: sid })], "ingest_claim_buffered_batch");
        await setRow(f, state[0], state[1]);
        try {
          await f.db.exec(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL ROLE bulls96_session_ingest`);
          try {
            const q = f.db.query<any>(COMMIT, [c.payload]);
            if (refused) await expect(q).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("the fenced staged ACK requires read committed") });
            else expect((await q).rows[0].r.kind).toBe("committed");
          } finally { await f.db.exec("ROLLBACK"); }
          await f.db.exec(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL ROLE bulls96_session_ingest`);
          try { await expect(f.db.query("SELECT sessions.ingest_claim_buffered_batch($1::jsonb) AS r", [s.payload])).rejects.toMatchObject({ code: "40001", message: expect.stringContaining("lazy records require read committed") }); }
          finally { await f.db.exec("ROLLBACK"); }
          await f.db.exec(`BEGIN ISOLATION LEVEL ${iso}`);
          try {
            expect((await f.db.query(suppRow)).rows.length).toBe(1);
            expect({ iso, state, gateLocks: Number((await f.db.query<any>("SELECT count(*)::int n FROM pg_locks WHERE locktype='relation' AND relation='sessions.lazy_ack_t2_mode'::regclass AND pid=pg_backend_pid()")).rows[0].n) })
              .toEqual({ iso, state, gateLocks: 0 });
          } finally { await f.db.exec("ROLLBACK"); }
        } finally { await setRow(f, "off", "main"); }
      }
    }
  });
  test("lazy_ack_t2_set_mode and lazy_ack_t2_set_gate only TRY-lock 50000 (never queue), then the row NOWAIT, in that order; no ALTER, no SET", async () => {
    const f = await fixture();
    for (const fn of ["sessions.lazy_ack_t2_set_mode(text)", "sessions.lazy_ack_t2_set_gate(text)"]) {
      const src = ((await f.db.query<any>("SELECT prosrc FROM pg_proc WHERE oid=$1::regprocedure", [fn])).rows[0].prosrc as string).replace(/--[^\n]*/g, "");
      const iso = src.indexOf("current_setting('transaction_isolation')<>'read committed'");
      const tryLock = src.indexOf("IF NOT pg_try_advisory_xact_lock(961996,50000) THEN"), row = src.indexOf("FROM sessions.lazy_ack_t2_mode WHERE singleton FOR UPDATE NOWAIT;");
      expect({ fn, ok: iso > -1 && tryLock > iso && row > tryLock }).toEqual({ fn, ok: true });
      expect(src.split("lazy_ack_t2_mode").length - 1).toBe(2); // the locking read and the UPDATE, both after the try-lock
      expect(src).not.toMatch(/pg_advisory_xact_lock\(|pg_advisory_lock\(|lock_timeout|ALTER\s|set_config|\bSET\s+(LOCAL\s+)?[a-z_]+\./i);
    }
  });
  test("the down refuses unless the row says off/main; then restores 043800 exactly", async () => {
    const f = await fixture();
    const { readFileSync } = await import("node:fs");
    const DOWN = readFileSync("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql", "utf8");
    const sha = (await import("node:crypto")).createHash("sha256").update(readFileSync(MIG, "utf8")).digest("hex");
    await f.db.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL);
      ALTER TABLE sessions.__96bulls_role_validation ADD COLUMN IF NOT EXISTS updated_at timestamptz`);
    await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES('upstream:20261105062210_lazy_ack_t2.sql',$1)", [sha]);
    const attempt = async () => { const e = await f.db.exec(DOWN).then(() => null, (x: any) => x); if (e) await f.db.exec("ROLLBACK"); return e; };
    await setRow(f, "fenced", "t2");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "t2" }]);
    await setRow(f, "fenced", "main");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the gate is not off") });
    expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "main" }]);
    await setRow(f, "off", "main");
    expect(await attempt()).toBeNull();
    expect((await f.db.query<any>("SELECT to_regclass('sessions.lazy_ack_t2_mode') r")).rows[0].r).toBeNull();
    expect((await f.db.query<any>("SELECT count(*)::int n FROM pg_proc WHERE proname LIKE 'lazy\\_ack\\_t2\\_%'")).rows[0].n).toBe(0);
    const p = (await f.db.query<any>("SELECT proconfig,prosrc FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0];
    expect(p.proconfig).toEqual(["search_path=pg_catalog, pg_temp"]);
    expect(p.prosrc).toBe(ackBody(readFileSync(MAIN_MIG, "utf8")));
  });
  test("caller pin: the ACK reads its gate row only after its own 50000 S; its only caller is the seat-ACK trigger on ingest_batches", async () => {
    // The ACK takes (961996,50000) shared in its own ingest_lock_fences before it reads the gate row, and every existing
    // path into it (commit and claim) already holds 50000 S before it (the down's quiescence relies on that). A new
    // caller (a function calling it, or another trigger using its trigger function) turns this red: check the rule.
    const f = await fixture();
    const callers = (await f.db.query<{ f: string }>(`SELECT p.oid::regprocedure::text f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql') AND p.oid<>'sessions.ingest_lazy_ack_batch(jsonb)'::regprocedure
       AND regexp_replace(regexp_replace(coalesce(p.prosrc,'')||' '||coalesce(pg_get_function_sqlbody(p.oid),''),'--[^\n]*','','g'),
         'ingest_lazy_ack_batch\\s*[(]\\s*jsonb\\s*[)]','','gi') ~* 'ingest_lazy_ack_batch' ORDER BY 1`)).rows.map((r) => r.f);
    // Any remaining mention counts: a call with whitespace before the paren, a dynamic EXECUTE/format string, or a
    // BEGIN ATOMIC (SQL-standard) body read through pg_get_function_sqlbody. Only exact '(jsonb)' signatures are removed.
    // (A signature reference, 'ingest_lazy_ack_batch(jsonb)' as in set_mode/set_gate, is not a call.)
    expect(callers).toEqual(["sessions.ingest_receipt_seat_ack()"]);
    // The pin is not evaded by a space before the paren in a dynamic EXECUTE, nor by a SQL-standard (BEGIN ATOMIC) body.
    await f.db.exec(`BEGIN;
      CREATE FUNCTION sessions.t2_pin_probe_dyn() RETURNS void LANGUAGE plpgsql AS $p$ BEGIN
        EXECUTE format('SELECT sessions.ingest_lazy_ack_batch (%L::jsonb)','{}'); END $p$;
      CREATE FUNCTION sessions.t2_pin_probe_atomic() RETURNS void LANGUAGE sql BEGIN ATOMIC SELECT sessions.ingest_lazy_ack_batch('{}'::jsonb); END;`);
    try {
      const probe = (await f.db.query<{ f: string }>(`SELECT p.oid::regprocedure::text f FROM pg_proc p
        WHERE p.proname LIKE 't2\\_pin\\_probe\\_%' AND regexp_replace(regexp_replace(coalesce(p.prosrc,'')||' '||coalesce(pg_get_function_sqlbody(p.oid),''),'--[^\n]*','','g'),
          'ingest_lazy_ack_batch\\s*[(]\\s*jsonb\\s*[)]','','gi') ~* 'ingest_lazy_ack_batch' ORDER BY 1`)).rows.map((r) => r.f);
      expect(probe).toEqual(["sessions.t2_pin_probe_atomic()", "sessions.t2_pin_probe_dyn()"]);
    } finally { await f.db.exec("ROLLBACK"); }
    const sqlCallers = (await f.db.query<{ f: string }>(`SELECT p.oid::regprocedure::text f FROM pg_proc p
      WHERE p.prolang=(SELECT oid FROM pg_language WHERE lanname='sql')
       AND (coalesce(p.prosrc,'')||' '||coalesce(pg_get_function_sqlbody(p.oid),'')) ~* 'ingest_lazy_ack_batch'`)).rows;
    expect(sqlCallers).toEqual([]);
    const triggers = (await f.db.query<{ t: string }>(`SELECT tgrelid::regclass::text||'.'||tgname t FROM pg_trigger
      WHERE tgfoid='sessions.ingest_receipt_seat_ack()'::regprocedure AND NOT tgisinternal ORDER BY 1`)).rows.map((r) => r.t);
    expect(triggers).toEqual(["sessions.ingest_batches.ingest_receipt_seat_ack"]);
  });
  test("lazy_ack_t2_set_gate: requires mode main, returns the previous gate, UPDATEs the row only, read committed", async () => {
    const f = await fixture();
    for (const bad of ["on", "FENCED", "", "t2", "main"])
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [bad])).rejects.toMatchObject({ code: "22023" });
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate(NULL)")).rejects.toMatchObject({ code: "22023" });
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("off");
    expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "main" }]);
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("fenced");
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
    for (const g of ["off", "fenced"]) {
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [g])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("only while the mode is main") });
      expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "t2" }]);
    }
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('off') o")).rows[0].o).toBe("fenced");
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    expect((await f.db.query<any>("SELECT proconfig c FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0].c).toEqual(["search_path=pg_catalog, pg_temp"]);
    for (const [iso, stmt] of [["REPEATABLE READ", "SELECT sessions.lazy_ack_t2_set_gate('fenced')"], ["SERIALIZABLE", "SELECT sessions.lazy_ack_t2_set_gate('fenced')"],
      ["REPEATABLE READ", "SELECT sessions.lazy_ack_t2_set_mode('main')"], ["SERIALIZABLE", "SELECT sessions.lazy_ack_t2_set_mode('main')"]] as const) {
      await f.db.exec(`BEGIN ISOLATION LEVEL ${iso}`);
      try { await expect(f.db.exec(stmt)).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("requires read committed") }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
  });
  test("lazy_ack_t2_set_mode: returns the old mode, refuses anything but main/t2 (22023); 't2' only with the row's gate fenced", async () => {
    const f = await fixture();
    for (const bad of ["T2", "off", "fenced", "", " t2"])
      await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [bad])).rejects.toMatchObject({ code: "22023" });
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode(NULL)")).rejects.toMatchObject({ code: "22023" });
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('main') o")).rows[0].o).toBe("main");
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("requires the gate fenced first") });
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('main') o")).rows[0].o).toBe("t2");
    await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
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
