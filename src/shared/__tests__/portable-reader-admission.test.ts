import { expect, test } from "bun:test";
import {
  READER_LEDGER_TABLE, READER_MARKER_NAME, READER_MARKER_TABLE, READER_READINESS_SQL, admitPortableReader, gatedReaderRunner,
  portableReaderGate, readCommittedTenantRunner, readerWriterRole, type ReaderAdmission,
} from "../portable-reader-admission.ts";
import { PortableConnectionRefused } from "../portable-connection-identity.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../db.ts";
import { MIGRATION_LEDGER, expectedMigrationNames } from "../../hoopdb/migrate.ts";
import { readFileSync } from "node:fs";

const NAMES = ["upstream:a.sql", "96bulls:b.sql", "verify:x"];
const A: ReaderAdmission = { generation: "gen-a", expectedNames: NAMES, storageVerifySql: "DO $s$ BEGIN END $s$", readerVerifySql: "DO $r$ BEGIN END $r$", writerRole: "bulls96_runtime", dbMode: "rds" };

function fake() {
  const st = {
    who: { cu: "bulls96_read_api", su: "bulls96_read_api", iso: "read committed", ro: "on", dro: "on", st: "2s" },
    rpcError: undefined as undefined | string, forceVerdict: undefined as undefined | string,
    marker: [{ status: "pass", generation: "gen-a" }], ledger: new Set(["upstream:a.sql", "96bulls:b.sql"]),
    readerFails: false, storageFails: false, onStorage: undefined as undefined | (() => void),
    texts: [] as string[], params: [] as unknown[][],
  };
  const tx = {
    async query(text: string, params?: unknown[]) {
      st.texts.push(text); st.params.push(params ?? []);
      if (text.includes("current_setting('transaction_isolation')")) return { rows: [st.who] };
      if (text.includes("rolsuper")) return { rows: [{ present: true, super: false, bypassrls: false, owner_member: false, can_write: false }] };
      if (text === READER_READINESS_SQL) {
        if (st.rpcError) throw Object.assign(new Error("rpc refused"), { code: st.rpcError });
        const [names, gen] = params as [string[], string];
        const v = !names.every(n => st.ledger.has(n)) ? "schema_pending"
          : st.marker.length !== 1 ? "marker_missing"
          : st.marker[0]!.generation !== gen ? "generation_mismatch"
          : st.marker[0]!.status !== "pass" ? "validation_pending" : "pass";
        return { rows: [{ v: st.forceVerdict ?? v }] };
      }
      if (text === A.readerVerifySql && st.readerFails) throw Object.assign(new Error("session read api role is not in the approved shape"), { code: "P0001" });
      if (text === A.storageVerifySql) { st.onStorage?.(); if (st.storageFails) throw Object.assign(new Error("portable search storage is not in the approved shape"), { code: "55000" }); }
      return { rows: [] };
    },
  };
  return { st, tx: tx as never };
}
const code = async (p: Promise<unknown>) => ((await p.then(() => undefined, (e: unknown) => e)) as PortableConnectionRefused | undefined)?.code;

test("constants are exactly the shared marker and ledger", () => {
  expect(READER_MARKER_TABLE).toBe(ROLE_VALIDATION_TABLE);
  expect(READER_MARKER_NAME).toBe(ROLE_VALIDATION_NAME);
  expect(READER_LEDGER_TABLE).toBe(MIGRATION_LEDGER);
});

test("success: identity/defaults, initial readiness RPC, reader verifier, writer-bound storage verifier, FINAL readiness RPC", async () => {
  const f = fake();
  await portableReaderGate(f.tx, A);
  const t = f.st.texts;
  const rpcs = t.map((x, i) => x === READER_READINESS_SQL ? i : -1).filter(i => i >= 0);
  expect(t.findIndex(x => x.includes("current_setting('transaction_isolation')"))).toBe(0);
  expect(rpcs.length).toBe(2);
  expect(rpcs[0]!).toBeLessThan(t.indexOf(A.readerVerifySql));
  const bind = t.findIndex(x => x.includes("hoopdb.runtime_role"));
  expect(f.st.params[bind]).toEqual(["bulls96_runtime", "rds"]); // the configured writer, never the reader
  expect(t[bind + 1]).toBe(A.storageVerifySql);
  expect(rpcs[1]!).toBeGreaterThan(bind + 1);
  // Only schema steps go to the RPC; the reader never SELECTs the ledger or marker.
  expect(f.st.params[rpcs[0]!]).toEqual([["upstream:a.sql", "96bulls:b.sql"], "gen-a"]);
  expect(t.some(x => x.includes(READER_LEDGER_TABLE) || x.includes(READER_MARKER_TABLE))).toBe(false);
});

test("refusals: identity (incl. SET ROLE), isolation, read-only, timeout, ledger, marker, verifiers, flip during verification, missing bodies", async () => {
  const cases: [string, (f: ReturnType<typeof fake>) => void, string][] = [
    ["another login", f => { f.st.who = { ...f.st.who, cu: "bulls96_runtime", su: "bulls96_runtime" }; }, "reader_identity"],
    ["SET ROLE from another login", f => { f.st.who = { ...f.st.who, su: "rdsadmin" }; }, "reader_identity"],
    ["repeatable read", f => { f.st.who = { ...f.st.who, iso: "repeatable read" }; }, "reader_isolation"],
    ["writable transaction", f => { f.st.who = { ...f.st.who, ro: "off" }; }, "reader_not_read_only"],
    ["no statement_timeout", f => { f.st.who = { ...f.st.who, st: "0" }; }, "reader_statement_timeout"],
    ["statement_timeout above 2s", f => { f.st.who = { ...f.st.who, st: "30s" }; }, "reader_statement_timeout"],
    ["startup default not read-only", f => { f.st.who = { ...f.st.who, dro: "off" }; }, "reader_default_not_read_only"],
    ["missing ledger row", f => { f.st.ledger.delete("96bulls:b.sql"); }, "reader_schema_pending"],
    ["marker pending", f => { f.st.marker = [{ status: "pending", generation: "gen-a" }]; }, "reader_validation_pending"],
    ["readiness RPC refuses the caller", f => { f.st.rpcError = "42501"; }, "reader_readiness_sqlstate_42501"],
    ["unexpected RPC payload is not echoed", f => { f.st.forceVerdict = "pass; DROP TABLE x"; }, "reader_readiness_unknown"],
    ["newer generation", f => { f.st.marker = [{ status: "pass", generation: "gen-b" }]; }, "reader_generation_mismatch"],
    ["marker missing", f => { f.st.marker = []; }, "reader_marker_missing"],
    ["reader contract drift", f => { f.st.readerFails = true; }, "reader_contract_unverified_sqlstate_P0001"],
    ["storage drift", f => { f.st.storageFails = true; }, "reader_storage_unverified_sqlstate_55000"],
    ["flip to pending during verification (final read)", f => { f.st.onStorage = () => { f.st.marker = [{ status: "pending", generation: "gen-a" }]; }; }, "reader_validation_pending"],
  ];
  for (const [, arrange, expected] of cases) {
    const f = fake();
    arrange(f);
    expect(await code(portableReaderGate(f.tx, A))).toBe(expected);
  }
  expect(await code(portableReaderGate(fake().tx, { ...A, storageVerifySql: " " }))).toBe("reader_verifier_missing");
  expect(await code(portableReaderGate(fake().tx, { ...A, readerVerifySql: "" }))).toBe("reader_verifier_missing");
  expect(await code(portableReaderGate(fake().tx, { ...A, expectedNames: ["verify:x"] }))).toBe("reader_expected_names_missing");
});

test("the gated runner never runs work on refusal; the writer role is required configuration", async () => {
  const f = fake();
  f.st.marker = [{ status: "pending", generation: "gen-a" }];
  let worked = 0;
  const base = (async (_o: string, _w: string, work: (tx: never) => Promise<unknown>) => work(f.tx)) as never;
  expect(await code(gatedReaderRunner(base, A)("org-a", "ws-a", async () => { worked++; }))).toBe("reader_validation_pending");
  expect(worked).toBe(0);
  f.st.marker = [{ status: "pass", generation: "gen-a" }];
  await gatedReaderRunner(base, A)("org-a", "ws-a", async () => { worked++; });
  expect(worked).toBe(1);
  expect(() => readerWriterRole({})).toThrow("reader_writer_role_unset");
  expect(() => readerWriterRole({ PORTABLE_SEARCH_WRITER_ROLE: "bulls96_read_api" })).toThrow("reader_writer_role_is_reader");
  expect(() => readerWriterRole({ PORTABLE_SEARCH_WRITER_ROLE: "x; DROP" })).toThrow("reader_writer_role_unset");
  expect(readerWriterRole({ PORTABLE_SEARCH_WRITER_ROLE: "bulls96_runtime" })).toBe("bulls96_runtime");
});

test("assembly admission runs on one READ ONLY, READ COMMITTED transaction and releases the client on refusal", async () => {
  const f = fake();
  f.st.marker = [{ status: "pending", generation: "gen-a" }];
  let released = 0;
  const pool = { async connect() { return { query: (t: string, p?: unknown[]) => (f.tx as any).query(t, p), release() { released++; } }; } };
  expect(await code(admitPortableReader(pool as never, A))).toBe("reader_validation_pending");
  expect(f.st.texts[0]).toBe("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
  // Identity gate runs before the privilege checks (attributable substitution refusal).
  expect(f.st.texts.findIndex(x => x.includes("current_setting('transaction_isolation')"))).toBeLessThan(f.st.texts.findIndex(x => x.includes("rolsuper")) === -1 ? Infinity : f.st.texts.findIndex(x => x.includes("rolsuper")));
  expect(f.st.texts).toContain("ROLLBACK");
  expect(released).toBe(1);
});

test("the gated reader's tenant runner fixes READ COMMITTED in BEGIN, before the tenant SELECT", async () => {
  const texts: string[] = [];
  let released = 0;
  const pool = { async connect() { return { async query(t: string) { texts.push(t); return { rows: [] }; }, release() { released++; } }; } };
  await readCommittedTenantRunner(pool as never)("org-a", "ws-a", async (q) => { await q.query("SELECT 1"); });
  expect(texts[0]).toBe("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
  expect(texts[1]).toContain("set_config('bulls96_read.org_id'");
  expect(texts.at(-1)).toBe("COMMIT");
  expect(released).toBe(1);
});

test("the readiness RPC's admitted name syntax is derived from, and admits, every compiled schema-step name (no duplicates)", async () => {
  const sql = readFileSync(new URL("../../../supabase/migrations/20261105048000_portable_search_reader_contract.sql", import.meta.url), "utf8");
  const pattern = /n\.name !~ '([^']+)'/.exec(sql)?.[1];
  expect(pattern).toBeDefined();
  const re = new RegExp(pattern!);
  const names = (await expectedMigrationNames()).filter(n => !/^(shim|bootstrap|verify):/.test(n));
  expect(names.length).toBeGreaterThan(0);
  expect(names.length).toBeLessThanOrEqual(512);
  for (const n of names) expect(re.test(n)).toBe(true);
  expect(new Set(names).size).toBe(names.length);
});
