/**
 * 20261105062250 mode 'p' (the pre-planned staged ACK), PGlite, every migration incl. 062250.
 *
 * The broad differential (every 062210 case: accepted, refused, adoption, replay, suppression, expiry, first errors,
 * history live and dormant) runs mode 'p' beside main and t2 in lazy-ack-t2.pglite.test.ts. This file covers what is
 * specific to 'p':
 *   - the install: the widened mode CHECK, the replaced ACK and setter are 062210's plus exactly 062250's additions;
 *   - the setter: 'p' only fenced, no direct t2 <-> p switch, back through main;
 *   - which path ran: the pre-plan is used on valid input (the real sessions.ingest_receipt_seat_records is not called)
 *     and recomputed (called once, as main and t2 do) when any ONE captured input differs from the value read under the
 *     locks (fixture injection: a copy of the installed body that alters one captured variable after the pre-plan) or
 *     when the input is malformed; results, first errors, rows and sequences always equal main's;
 *   - the pre-plan statement's shape: read-only, one statement, between the gate read and the clock, narrow handler.
 * Concurrent behaviour (real races, lock order, apply/down under live ACKs) is the real-PostgreSQL suite's.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash } from "node:crypto";
import { deliverBatch } from "../deliver.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { deliveryAccount, prepareDelivery } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, generation, identity } from "./privacy-authority-support.ts";
import { PREPLAN_DOWN, PREPLAN_MIG, PREPLAN_STEP, T2_MIG, ackBodyOf, preplanBlock, read, setModeBodyOf, stripPreplan, stripPreplanSetMode } from "./lazy-ack-preplan-support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
type Rec = ReturnType<typeof record>;
const ACK = "sessions.ingest_lazy_ack_batch(jsonb)";
const COMMIT = "SELECT sessions.ingest_commit_buffered_batch($1::jsonb) AS r";
let serial = 0;

async function fixture() { const f = await authorityFixture(); open.push(f.close); await f.seal(); return f; }
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
const rec = (record_id: string, revision: number, extra: Partial<Rec> = {}) =>
  record({ record_id, revision, revision_id: `${record_id}:r${revision}:${++serial}`, session_id: "s-" + record_id, ...extra });
/** Claim and stage committed; the exact commit payload is returned and the commit is NOT run. */
async function capture(f: Fixture, records: Rec[]) {
  const raw = await batch(records, { batch_id: "b" + ++serial, account_id: await deliveryAccount(TENANT) });
  let payload: string | undefined;
  const ledger = sqlLedger(async (sql, params) => {
    if (sql.includes("ingest_commit_buffered_batch")) { payload = params[0] as string; throw Object.assign(Error("captured"), { code: "XXCAP" }); }
    return f.run(sql, params as unknown[]);
  }, { acceptanceMode: "buffered", expectedGeneration: generation });
  await expect(deliverBatch({ ...f.deps, ledger, custody: await custody(f, raw) } as never, TENANT, raw)).rejects.toBeDefined();
  if (typeof payload !== "string") throw Error("commit payload not captured");
  return { payload, claimToken: JSON.parse(payload).claimToken as string, stageToken: JSON.parse(payload).stageToken as string };
}
async function snapshot(f: Fixture) {
  const tables = (await f.db.query<{ t: string; ts: string[] }>(`SELECT c.oid::regclass::text t,
     coalesce(array(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      AND a.atttypid IN('timestamptz'::regtype,'timestamp'::regtype)),'{}') ts
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND c.relkind IN('r','p') AND c.relname<>'lazy_ack_t2_mode' ORDER BY 1`)).rows;
  const out: Record<string, unknown> = {};
  for (const { t, ts } of tables)
    out[t] = (await f.db.query<{ j: unknown }>(`SELECT coalesce(jsonb_agg(x ORDER BY x::text),'[]') j FROM(SELECT to_jsonb(q)-$1::text[] x FROM ${t} q)s`, [ts])).rows[0]!.j;
  out["<sequences>"] = (await f.db.query("SELECT sequencename,last_value::text FROM pg_sequences WHERE schemaname='sessions' ORDER BY 1")).rows;
  return out;
}
type Outcome = { ok: true; result: unknown } | { ok: false; code: string; message: string };
type Run = { out: Outcome; snap: Record<string, unknown>; seatRecordCalls: number };
/**
 * One commit in its own transaction, rolled back (sequences reset first). mode 'main' = the installed row off/main;
 * 'p' = fenced/p through the setters. setup runs as the owner inside the transaction before the commit; body, when
 * given, replaces the installed ACK inside the transaction (fixture injection; rolled back with it).
 */
async function runOnce(f: Fixture, mode: "main" | "p", payload: string, seqs: Array<{ s: string; v: string | null; start: string }>, setup = "", body?: string): Promise<Run> {
  for (const q of seqs) await f.db.query("SELECT setval($1::regclass,$2::bigint,$3)", [q.s, q.v ?? q.start, q.v !== null]);
  await f.db.exec("BEGIN");
  try {
    await f.db.exec("SET LOCAL track_functions='pl'");
    if (mode === "p") { await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('p')"); }
    if (setup) await f.db.exec(setup);
    if (body) await f.db.exec(body);
    const calls = async () => Number((await f.db.query<{ n: string }>("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE schemaname='sessions' AND funcname='ingest_receipt_seat_records'),0)::text n")).rows[0]!.n);
    const before = await calls();
    await f.db.exec("SAVEPOINT ack;SET LOCAL ROLE bulls96_session_ingest");
    let out: Outcome;
    try { out = { ok: true, result: (await f.db.query<{ r: unknown }>(COMMIT, [payload])).rows[0]!.r }; await f.db.exec("RESET ROLE"); }
    catch (e: any) { out = { ok: false, code: String(e.code), message: String(e.message) }; await f.db.exec("ROLLBACK TO SAVEPOINT ack;RESET ROLE"); }
    const seatRecordCalls = (await calls()) - before;
    return { out, snap: await snapshot(f), seatRecordCalls };
  } finally { await f.db.exec("ROLLBACK"); }
}
const sequences = async (f: Fixture) => (await f.db.query<{ s: string; v: string | null; start: string }>(
  "SELECT format('%I.%I',schemaname,sequencename) s,last_value::text v,start_value::text start FROM pg_sequences WHERE schemaname='sessions'")).rows;
/** main and p from the same state; asserts equality; returns both. */
async function pair(f: Fixture, payload: string, setup = "", body?: string) {
  const seqs = await sequences(f);
  const main = await runOnce(f, "main", payload, seqs, setup);
  const p = await runOnce(f, "p", payload, seqs, setup, body);
  expect(p.out).toEqual(main.out);
  for (const t of Object.keys(main.snap)) expect({ t, rows: p.snap[t] }).toEqual({ t, rows: main.snap[t] });
  expect(Object.keys(p.snap)).toEqual(Object.keys(main.snap));
  expect(main.seatRecordCalls).toBe(main.out.ok || (main.out as any).code !== "XXCAP" ? main.seatRecordCalls : 0);
  return { main, p };
}
const installedAck = async (f: Fixture) => (await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid=$1::regprocedure", [ACK])).rows[0]!.s;
const gateRow = async (f: Fixture) => (await f.db.query<{ gate: string; mode: string }>("SELECT gate,mode FROM sessions.lazy_ack_t2_mode")).rows;

describe("062250 install: mode 'p' on 062210's gate row; 062210's bodies plus exactly 062250's additions", () => {
  test("the mode CHECK has 'p'; the row stays off/main; the ACK and setter are 062210's plus 062250's additions only; ACL, owner and flags unchanged", async () => {
    const f = await fixture();
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    const cons = (await f.db.query<any>("SELECT conname,pg_get_constraintdef(oid) d FROM pg_constraint WHERE conrelid='sessions.lazy_ack_t2_mode'::regclass AND contype IN('p','c') ORDER BY conname")).rows;
    expect(cons).toEqual([
      { conname: "lazy_ack_t2_mode_gate_check", d: "CHECK ((gate = ANY (ARRAY['off'::text, 'fenced'::text])))" },
      { conname: "lazy_ack_t2_mode_mode_check", d: "CHECK ((mode = ANY (ARRAY['main'::text, 't2'::text, 'p'::text])))" },
      { conname: "lazy_ack_t2_mode_pkey", d: "PRIMARY KEY (singleton)" },
      { conname: "lazy_ack_t2_mode_singleton_check", d: "CHECK (singleton)" },
      { conname: "lazy_ack_t2_mode_t2_needs_fenced", d: "CHECK (((mode = 'main'::text) OR (gate = 'fenced'::text)))" },
    ]);
    const ack = await installedAck(f);
    expect(ack).toBe(ackBodyOf(read(PREPLAN_MIG)));
    expect(stripPreplan(ack)).toBe(ackBodyOf(read(T2_MIG)));
    const set = (await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0]!.s;
    expect(set).toBe(setModeBodyOf(read(PREPLAN_MIG)));
    expect(stripPreplanSetMode(set)).toBe(setModeBodyOf(read(T2_MIG)));
    for (const fn of [ACK, "sessions.lazy_ack_t2_set_mode(text)"])
      expect((await f.db.query<any>("SELECT prosecdef,proconfig FROM pg_proc WHERE oid=$1::regprocedure", [fn])).rows[0]).toEqual({ prosecdef: false, proconfig: ["search_path=pg_catalog, pg_temp"] });
    // No new function or table: the sessions function and relation names are 062210's set.
    expect((await f.db.query<any>("SELECT count(*)::int n FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND (proname LIKE '%preplan%' OR proname LIKE 'lazy\\_ack\\_p\\_%')")).rows[0].n).toBe(0);
    expect((await f.db.query<any>("SELECT count(*)::int n FROM pg_class WHERE relnamespace='sessions'::regnamespace AND relname LIKE '%preplan%'")).rows[0].n).toBe(0);
    for (const role of ["bulls96_session_ingest", "bulls96_session_ingest_seeder", "anon", "authenticated", "service_role"])
      expect((await f.db.query<any>("SELECT has_function_privilege($1,'sessions.lazy_ack_t2_set_mode(text)','EXECUTE') x, has_table_privilege($1,'sessions.lazy_ack_t2_mode','SELECT,UPDATE') t", [role])).rows[0]).toEqual({ x: false, t: false });
  });
  test("the pre-plan block: one read-only statement after the gate read and before the clock; no lock, write or advisory call; the handler names four classes only", async () => {
    const f = await fixture();
    const ack = await installedAck(f);
    const block = preplanBlock(ack);
    const code = block.replace(/--[^\n]*/g, "");
    expect(code).not.toMatch(/\bFOR\s+(UPDATE|NO\s+KEY\s+UPDATE|SHARE|KEY\s+SHARE)\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|pg_advisory|\bLOCK\b|set_config|\bPERFORM\b|ingest_receipt_seat_records/i);
    expect(code.match(/\bSELECT\b[\s\S]*?\bINTO STRICT\b/g)?.length).toBe(1);
    expect(code.split(";").filter((s) => /\bINTO\b/.test(s)).length).toBe(1); // exactly one statement reads tables
    expect(code).toContain("EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows OR cardinality_violation THEN v_pre:=false;");
    expect(code).not.toMatch(/WHEN\s+OTHERS/i);
    const at = (s: string) => { const i = ack.indexOf(s); expect({ s, found: i > -1 }).toEqual({ s, found: true }); return i; };
    const gate = at(" v_t2:=v_gate='fenced' AND v_mode='t2';"), pre = at(block), clock = at("SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;");
    expect(gate < pre && pre < clock).toBe(true);
    // In p the T2 upgrade never runs (the clock is held NO KEY UPDATE from main's statement) and no KEY SHARE is taken.
    expect(ack).toContain("  IF v_t2 THEN\n  SELECT ck.last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock ck WHERE ck.singleton FOR NO KEY UPDATE;\n  END IF;");
    expect(ack).toContain(" IF NOT v_t2 THEN\n SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR NO KEY UPDATE;");
  });
});

describe("062250 setter: 'p' only fenced, through main only, owner-only", () => {
  test("set_mode('p') needs the gate fenced; t2 <-> p is refused (55000, row unchanged); p returns through main; set_gate needs main", async () => {
    const f = await fixture();
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('p')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("mode t2 or p requires the gate fenced first") });
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    for (const bad of ["P", "pre", "", "off"]) await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode($1)", [bad])).rejects.toMatchObject({ code: "22023", message: expect.stringContaining("main, t2 or p") });
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_gate('fenced') o")).rows[0].o).toBe("off");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('p') o")).rows[0].o).toBe("main");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('p') o")).rows[0].o).toBe("p"); // idempotent
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("only through main") });
    for (const g of ["off", "fenced"]) await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_gate($1)", [g])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("only while the mode is main") });
    expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "p" }]);
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('main') o")).rows[0].o).toBe("p");
    expect((await f.db.query<any>("SELECT sessions.lazy_ack_t2_set_mode('t2') o")).rows[0].o).toBe("main");
    await expect(f.db.query("SELECT sessions.lazy_ack_t2_set_mode('p')")).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("only through main") });
    expect(await gateRow(f)).toEqual([{ gate: "fenced", mode: "t2" }]);
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
    // The table itself refuses off/p, even past the keep trigger.
    await f.db.exec("BEGIN;SELECT pg_advisory_xact_lock(961996,50000)");
    try { await expect(f.db.exec("UPDATE sessions.lazy_ack_t2_mode SET mode='p'")).rejects.toMatchObject({ code: "23514" }); } finally { await f.db.exec("ROLLBACK"); }
    // The runtime role cannot set or read it.
    await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try { await expect(f.db.exec("SELECT sessions.lazy_ack_t2_set_mode('p')")).rejects.toMatchObject({ code: "42501" }); } finally { await f.db.exec("ROLLBACK"); }
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
  });
  test("fenced p outside READ COMMITTED: a staged ACK is refused (55000) before any pre-plan, as fenced t2", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("iso-1", 2)]);
    await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('p')");
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      await f.db.exec(`BEGIN ISOLATION LEVEL ${iso};SET LOCAL ROLE bulls96_session_ingest`);
      try { await expect(f.db.query(COMMIT, [c.payload])).rejects.toMatchObject({ code: "55000", message: expect.stringContaining("requires read committed") }); }
      finally { await f.db.exec("ROLLBACK"); }
    }
  });
});

describe("062250 which path ran: pre-plan used on valid input, recomputed on any captured-input change or malformed input; always main's result", () => {
  test("valid input (distinct, repeated and refused records): the pre-plan is used (the real seat-records function is not called); main calls it once", async () => {
    const f = await fixture();
    const a = await capture(f, [rec("v1", 2), rec("v2", 3), rec("v1", 4), rec("v3", 1)]);
    const { main, p } = await pair(f, a.payload);
    expect(main.out.ok).toBe(true);
    expect(main.seatRecordCalls).toBe(1);
    expect(p.seatRecordCalls).toBe(0);
  });
  test("fixture injection: each captured input changed alone after the pre-plan -> recompute under the locks (one real call), main's result", async () => {
    const f = await fixture();
    const a = await capture(f, [rec("inj1", 2), rec("inj2", 3)]);
    const installed = await installedAck(f);
    const anchor = "   v_pre:=coalesce(v_pok,false);\n";
    expect(installed.split(anchor).length - 1).toBe(1);
    const fields: Array<[string, string]> = [
      ["v_pstage", "v_pstage:=gen_random_uuid();"], ["v_pclaim", "v_pclaim:=gen_random_uuid();"],
      ["v_pgroups", "v_pgroups:=v_pgroups||'[[0]]'::jsonb;"], ["v_porg", "v_porg:=v_porg||'x';"], ["v_pws", "v_pws:=v_pws||'x';"],
      ["v_puser", "v_puser:=v_puser||'x';"], ["v_porigin", "v_porigin:=v_porigin||'x';"], ["v_precords", "v_precords:=v_precords||'[{}]'::jsonb;"],
      ["v_pepochs", "v_pepochs:=coalesce(v_pepochs,'[]')||'[{}]'::jsonb;"], ["v_preset", "v_preset:=v_preset+1;"],
      ["v_pranks", "v_pranks:=v_pranks||'[9]'::jsonb;"], ["v_pce", "v_pce:=v_pce||'x';"], ["v_psi", "v_psi:=v_psi||'x';"],
      // Null-safety: a captured value that became NULL is a mismatch too (IS NOT DISTINCT FROM).
      ["v_porg (null)", "v_porg:=NULL;"], ["v_pgroups (null)", "v_pgroups:=NULL;"],
      // The (A) list itself corrupted but every input equal: still used (it is a pure function of the inputs) -> the
      // corrupted list would decide the writes. This case documents that the guard is the inputs, not the output:
      // it must DIFFER from main, proving the injected body really ran.
      ["v_precs (control)", "v_precs:='[]'::jsonb;"],
    ];
    const seqs = await sequences(f);
    const main = await runOnce(f, "main", a.payload, seqs);
    expect(main.out.ok).toBe(true);
    for (const [name, tamper] of fields) {
      const body = `CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_ack_batch$${installed.replace(anchor, anchor + "   " + tamper + "\n")}$lazy_ack_batch$;`;
      const p = await runOnce(f, "p", a.payload, seqs, "", body);
      if (name === "v_precs (control)") { expect({ name, calls: p.seatRecordCalls }).toEqual({ name, calls: 0 }); expect(p.snap).not.toEqual(main.snap); continue; }
      expect({ name, calls: p.seatRecordCalls }).toEqual({ name, calls: 1 });
      expect({ name, out: p.out }).toEqual({ name, out: main.out });
      for (const t of Object.keys(main.snap)) expect({ name, t, rows: p.snap[t] }).toEqual({ name, t, rows: main.snap[t] });
    }
    // The injected bodies were rolled back with their transactions.
    expect(await installedAck(f)).toBe(installed);
  });
  test("malformed or mixed-invalid input: no pre-plan (recompute), and the same first error, rows and sequences as main", async () => {
    const f = await fixture();
    const attempt = (c: { claimToken: string }) => `sessions.ingest_claim_attempts WHERE claim_token='${c.claimToken}'`;
    const stage = (c: { stageToken: string }) => `sessions.ingest_stage_intents WHERE stage_token='${c.stageToken}'`;
    const cases: Array<[string, (c: { claimToken: string; stageToken: string }) => string]> = [
      ["a record digest that is not hex", (c) => `UPDATE ${attempt(c)}`.replace(" WHERE", " SET records=jsonb_set(records,'{1,digest}','\"zz\"') WHERE")],
      ["a record ord that is not an integer", (c) => `UPDATE ${attempt(c)}`.replace(" WHERE", " SET records=jsonb_set(records,'{0,ord}','\"x\"') WHERE")],
      ["records retired (NULL)", (c) => `ALTER TABLE sessions.ingest_claim_attempts DISABLE TRIGGER USER;UPDATE ${attempt(c)}`.replace(" WHERE", " SET records=NULL WHERE")],
      ["a duplicated seat ordinal in the stage groups", (c) => `UPDATE ${stage(c)}`.replace(" WHERE", " SET group_ordinals=group_ordinals||'[[0]]'::jsonb WHERE")],
      ["a seat ordinal that is not a number", (c) => `UPDATE ${stage(c)}`.replace(" WHERE", " SET group_ordinals='[[\"0\"]]'::jsonb WHERE")],
      ["an empty group", (c) => `UPDATE ${stage(c)}`.replace(" WHERE", " SET group_ordinals=group_ordinals||'[[]]'::jsonb WHERE")],
      ["a session epoch listed twice", (c) => `UPDATE ${attempt(c)}`.replace(" WHERE", " SET epochs=epochs||jsonb_build_array(epochs->0) WHERE")],
      ["a missing session epoch", (c) => `UPDATE ${attempt(c)}`.replace(" WHERE", " SET epochs='[]'::jsonb WHERE")],
      // (A negative reset epoch is refused by the attempt table's own CHECK before any ACK.)
    ];
    for (const [name, sql] of cases) {
      const c = await capture(f, [rec(`mx${serial}a`, 2), rec(`mx${serial}b`, 3)]);
      const setup = "ALTER TABLE sessions.ingest_stage_intents DISABLE TRIGGER USER;" + sql(c) + ";";
      const { main, p } = await pair(f, c.payload, setup);
      // Each case must actually reach the ACK and fail there, or be refused before it identically.
      expect({ name, ok: main.out.ok }).toEqual({ name, ok: false });
      expect({ name, calls: p.seatRecordCalls }).toEqual({ name, calls: main.seatRecordCalls });
    }
  });
  test("a missing processing rank (valid seat records, invalid plan input): the pre-plan is used and the plan raises main's first error", async () => {
    const f = await fixture();
    const c = await capture(f, [rec("rk1", 2), rec("rk2", 3), rec("rk3", 4)]);
    const { main, p } = await pair(f, c.payload, `UPDATE sessions.ingest_claim_attempts SET source_processing_ranks=jsonb_set(source_processing_ranks,'{1}','null') WHERE claim_token='${c.claimToken}';`);
    expect(main.out).toMatchObject({ ok: false, code: "55000", message: expect.stringContaining("source processing rank missing") });
    expect(p.seatRecordCalls).toBe(0);
  });
});

describe("062250 rollback (PGlite): refused unless off/main, restores 062210's bodies and CHECK exactly", () => {
  test("down: refused while fenced (p, t2 or main); succeeds at off/main; 062210 bodies and the CHECK restored; ledger row removed", async () => {
    const f = await fixture();
    const sha = createHash("sha256").update(read(PREPLAN_MIG)).digest("hex");
    await f.db.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL);
      ALTER TABLE sessions.__96bulls_role_validation ADD COLUMN IF NOT EXISTS updated_at timestamptz`);
    await f.db.query("INSERT INTO sessions.__96bulls_migrations VALUES($1,$2)", [PREPLAN_STEP, sha]);
    const DOWN = read(PREPLAN_DOWN);
    const attempt = async () => { const e = await f.db.exec(DOWN).then(() => null, (x: any) => x); if (e) await f.db.exec("ROLLBACK"); return e; };
    const before = await installedAck(f);
    await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('p')");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')"); await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('t2')");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the mode is not main") });
    await f.db.query("SELECT sessions.lazy_ack_t2_set_mode('main')");
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("the gate is not off") });
    expect(await installedAck(f)).toBe(before);
    await f.db.query("SELECT sessions.lazy_ack_t2_set_gate('off')");
    expect(await attempt()).toBeNull();
    expect(await installedAck(f)).toBe(ackBodyOf(read(T2_MIG)));
    expect((await f.db.query<{ s: string }>("SELECT prosrc s FROM pg_proc WHERE oid='sessions.lazy_ack_t2_set_mode(text)'::regprocedure")).rows[0]!.s).toBe(setModeBodyOf(read(T2_MIG)));
    expect((await f.db.query<any>("SELECT pg_get_constraintdef(oid) d FROM pg_constraint WHERE conname='lazy_ack_t2_mode_mode_check'")).rows[0].d).toBe("CHECK ((mode = ANY (ARRAY['main'::text, 't2'::text])))");
    expect((await f.db.query<any>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [PREPLAN_STEP])).rows[0].n).toBe(0);
    expect(await gateRow(f)).toEqual([{ gate: "off", mode: "main" }]);
    // A rerun refuses (own checksum required); reapplying the migration restores 062250 exactly.
    expect(await attempt()).toMatchObject({ code: "55000", message: expect.stringContaining("own applied checksum required") });
    await f.db.exec(read(PREPLAN_MIG));
    expect(await installedAck(f)).toBe(before);
  });
});
