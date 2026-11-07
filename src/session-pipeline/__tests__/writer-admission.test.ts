/** Writer assembly admission (SQL-owner M5 + storage admission): the newly opened pool
 * is closed on any refusal, the primary refusal is rethrown unchanged, success keeps the
 * pool, and the storage verifier runs on the SAME transaction with the identity bound. */
import { expect, test } from "bun:test";
import { admitWriterRuntime } from "../lambda.ts";
import { PortableConnectionRefused, admitPortableConnection } from "../../shared/portable-connection-identity.ts";
import type { SearchComposition } from "../../shared/search-composition.ts";

function fakeRuntime(rows: Record<string, unknown> | ((text: string) => unknown) = {}) {
  const statements: { client: number; text: string; params?: unknown[] }[] = [];
  let clients = 0;
  let ended = 0;
  let released = 0;
  const reply = typeof rows === "function" ? rows : () => rows;
  const pool = {
    async connect() {
      const id = ++clients;
      return {
        async query(text: string, params?: unknown[]) {
          statements.push({ client: id, text, params });
          const r = reply(text);
          if (r instanceof Error) throw r;
          return { rows: r ? [r] : [] };
        },
        release() { released++; },
      };
    },
  };
  const db = { pool, query: async () => ({ rows: [] }), end: async () => { ended++; } };
  return { rt: { deps: { pointer: { db } } } as never, statements, stats: () => ({ ended, released, clients }) };
}

const PORTABLE = { searchEnabled: true, searchBackend: "opensearch" } as never;
const composition = (admit: SearchComposition["admit"]): SearchComposition => ({ registry: {} as never, state: () => ({}) as never, admit });

test("refusal closes the newly opened pool and rethrows the primary refusal unchanged; repeated refusals each close their own pool", async () => {
  const refusal = new PortableConnectionRefused("superuser");
  for (let attempt = 0; attempt < 3; attempt++) {
    const f = fakeRuntime();
    const error = await admitWriterRuntime(f.rt, PORTABLE, composition(async () => { throw refusal; }), "SELECT 1", "rds").catch(e => e);
    expect(error).toBe(refusal);
    expect(f.stats()).toEqual({ ended: 1, released: 1, clients: 1 });
    expect(f.statements.map(s => s.text)).toContain("ROLLBACK");
  }
});

test("success keeps the pool; identity checks and the storage verifier share one transaction with the identity bound", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper")
    ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: true }
    : {});
  const VERIFY = "DO $verify$ BEGIN RAISE NOTICE 'ok'; END $verify$";
  await admitWriterRuntime(f.rt, PORTABLE, composition((sql, role, options) => admitPortableConnection(sql, role, options)), VERIFY, "rds");
  expect(f.stats()).toEqual({ ended: 0, released: 1, clients: 1 });
  const texts = f.statements.map(s => s.text);
  const bind = texts.findIndex(t => t.includes("set_config('hoopdb.runtime_role', current_user, true)"));
  expect(texts[0]).toBe("BEGIN");
  expect(bind).toBeGreaterThan(texts.findIndex(t => t.includes("rolsuper")));
  expect(texts[bind + 1]).toBe(VERIFY);
  expect(texts.at(-1)).toBe("COMMIT");
  expect(new Set(f.statements.map(s => s.client))).toEqual(new Set([1]));
  expect(f.statements[bind]!.params).toEqual(["rds"]);
});

test("a failing storage verifier is a stable refusal and closes the pool; non-portable selections skip admission", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper")
    ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: true }
    : text.startsWith("DO") ? Object.assign(new Error("portable search storage is not in the approved shape: x"), { code: "55000" }) : {});
  const error = await admitWriterRuntime(f.rt, PORTABLE, composition((sql, role, options) => admitPortableConnection(sql, role, options)), "DO $v$ BEGIN END $v$", "rds").catch(e => e);
  expect(error).toBeInstanceOf(PortableConnectionRefused);
  expect((error as PortableConnectionRefused).code).toBe("storage_unverified_sqlstate_55000");
  expect(f.stats().ended).toBe(1);
  for (const env of [{ searchEnabled: false, searchBackend: "opensearch" }, { searchEnabled: true, searchBackend: "clickhouse" }, { searchEnabled: true }]) {
    const g = fakeRuntime();
    await admitWriterRuntime(g.rt, env as never, composition(async () => { throw new Error("must not run"); }), "SELECT 1", "rds");
    expect(g.stats()).toEqual({ ended: 0, released: 0, clients: 0 });
  }
  const h = fakeRuntime();
  await admitWriterRuntime(h.rt, PORTABLE, { registry: {} as never, state: () => ({}) as never }, "SELECT 1", "rds"); // v1 composition: no admit
  expect(h.stats()).toEqual({ ended: 0, released: 0, clients: 0 });
});

test("the reader may not run the storage verifier", async () => {
  const f = fakeRuntime((text) => text.includes("rolsuper") ? { present: true, super: false, bypassrls: false, owner_member: false, can_write: false } : {});
  const client = await (f.rt as any).deps.pointer.db.pool.connect();
  await expect(admitPortableConnection(client, "reader", { storageVerifySql: "SELECT 1" })).rejects.toThrow("storage_verify_writer_only");
});
