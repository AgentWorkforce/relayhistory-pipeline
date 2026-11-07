import { expect, test } from "bun:test";
import {
  READER_LEDGER_TABLE, READER_MARKER_NAME, READER_MARKER_TABLE, admitPortableReader, gatedReaderRunner,
  portableReaderGate, readCommittedTenantRunner, readerWriterRole, type ReaderAdmission,
} from "../portable-reader-admission.ts";
import { PortableConnectionRefused } from "../portable-connection-identity.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../db.ts";
import { MIGRATION_LEDGER } from "../../hoopdb/migrate.ts";

const NAMES = ["upstream:a.sql", "96bulls:b.sql", "verify:x"];
const A: ReaderAdmission = { generation: "gen-a", expectedNames: NAMES, storageVerifySql: "DO $s$ BEGIN END $s$", readerVerifySql: "DO $r$ BEGIN END $r$", writerRole: "bulls96_runtime", dbMode: "rds" };

function fake() {
  const st = {
    who: { cu: "bulls96_read_api", su: "bulls96_read_api", iso: "read committed", ro: "on", st: "2s" },
    marker: [{ status: "pass", generation: "gen-a" }], ledger: new Set(["upstream:a.sql", "96bulls:b.sql"]),
    readerFails: false, storageFails: false, onStorage: undefined as undefined | (() => void),
    texts: [] as string[], params: [] as unknown[][],
  };
  const tx = {
    async query(text: string, params?: unknown[]) {
      st.texts.push(text); st.params.push(params ?? []);
      if (text.includes("current_setting('transaction_isolation')")) return { rows: [st.who] };
      if (text.includes("rolsuper")) return { rows: [{ present: true, super: false, bypassrls: false, owner_member: false, can_write: false }] };
      if (text.includes(READER_LEDGER_TABLE)) return { rows: [{ n: (params![0] as string[]).filter(n => st.ledger.has(n)).length }] };
      if (text.includes(READER_MARKER_TABLE)) return { rows: st.marker.map(m => ({ ...m })) };
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

test("success: identity, ledger, marker, reader verifier, writer-bound storage verifier, FINAL marker read", async () => {
  const f = fake();
  await portableReaderGate(f.tx, A);
  const t = f.st.texts;
  const idx = (pred: (s: string) => boolean) => t.findIndex(pred);
  const markers = t.map((s, i) => s.includes(READER_MARKER_TABLE) ? i : -1).filter(i => i >= 0);
  expect(idx(s => s.includes("current_setting('transaction_isolation')"))).toBe(0);
  expect(idx(s => s.includes(READER_LEDGER_TABLE))).toBeLessThan(markers[0]!);
  expect(markers[0]!).toBeLessThan(t.indexOf(A.readerVerifySql));
  const bind = idx(s => s.includes("hoopdb.runtime_role"));
  expect(f.st.params[bind]).toEqual(["bulls96_runtime", "rds"]); // the configured writer, never the reader
  expect(t[bind + 1]).toBe(A.storageVerifySql);
  expect(markers[1]!).toBeGreaterThan(bind + 1);
  // Only the schema steps are required in the ledger.
  expect(f.st.params[idx(s => s.includes(READER_LEDGER_TABLE))]).toEqual([["upstream:a.sql", "96bulls:b.sql"]]);
});

test("refusals: identity (incl. SET ROLE), isolation, read-only, timeout, ledger, marker, verifiers, flip during verification, missing bodies", async () => {
  const cases: [string, (f: ReturnType<typeof fake>) => void, string][] = [
    ["another login", f => { f.st.who = { ...f.st.who, cu: "bulls96_runtime", su: "bulls96_runtime" }; }, "reader_identity"],
    ["SET ROLE from another login", f => { f.st.who = { ...f.st.who, su: "rdsadmin" }; }, "reader_identity"],
    ["repeatable read", f => { f.st.who = { ...f.st.who, iso: "repeatable read" }; }, "reader_isolation"],
    ["writable transaction", f => { f.st.who = { ...f.st.who, ro: "off" }; }, "reader_not_read_only"],
    ["no statement_timeout", f => { f.st.who = { ...f.st.who, st: "0" }; }, "reader_statement_timeout"],
    ["statement_timeout above 2s", f => { f.st.who = { ...f.st.who, st: "30s" }; }, "reader_statement_timeout"],
    ["missing ledger row", f => { f.st.ledger.delete("96bulls:b.sql"); }, "reader_schema_pending"],
    ["marker pending", f => { f.st.marker = [{ status: "pending", generation: "gen-a" }]; }, "reader_marker_pending"],
    ["newer generation", f => { f.st.marker = [{ status: "pass", generation: "gen-b" }]; }, "reader_generation_mismatch"],
    ["marker missing", f => { f.st.marker = []; }, "reader_marker_missing"],
    ["reader contract drift", f => { f.st.readerFails = true; }, "reader_contract_unverified_sqlstate_P0001"],
    ["storage drift", f => { f.st.storageFails = true; }, "reader_storage_unverified_sqlstate_55000"],
    ["flip to pending during verification (final read)", f => { f.st.onStorage = () => { f.st.marker = [{ status: "pending", generation: "gen-a" }]; }; }, "reader_marker_pending"],
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
  expect(await code(gatedReaderRunner(base, A)("org-a", "ws-a", async () => { worked++; }))).toBe("reader_marker_pending");
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
  expect(await code(admitPortableReader(pool as never, A))).toBe("reader_marker_pending");
  expect(f.st.texts[0]).toBe("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
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
