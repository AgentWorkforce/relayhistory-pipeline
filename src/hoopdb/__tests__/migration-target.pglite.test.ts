/**
 * The release target on the real migrator (runMigrations) in in-process PGlite:
 * steps after the target are held (never applied, never ledgered), phase 2 does not
 * run and the marker stays 'pending'; a pending set other than the approved one, an
 * applied/unknown target or a plan without a target refuse before any write.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import {
  generation,
  runMigrations,
  type MigrationClient,
  type MigrationProfile,
} from "../migrate.ts";

setDefaultTimeout(120_000);
const LOCAL: MigrationProfile = { mode: "local", shim: false, bootstrap: false, verify: true, runtimeRole: "postgres" };
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});
function client(db: PGlite, writes?: string[]): MigrationClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (writes && /^\s*(CREATE|ALTER|GRANT|REVOKE|INSERT|UPDATE|DELETE|DROP)\b/i.test(text)) writes.push(text);
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}
const dir = mkdtempSync(join(tmpdir(), "migration-target-"));
const step = (name: string, sql: string) => {
  const path = join(dir, name.replace(/[:/]/g, "_"));
  writeFileSync(path, sql);
  const table = /sessions\.(\w+)/.exec(sql)![1]!;
  // A real catalog sentinel: the step's table exists.
  const sentinel = { evidence: table, sql: `SELECT to_regclass('sessions.${table}') IS NOT NULL AS applied` };
  return { name, path, sentinel };
};
const u = (n: string) => `upstream:2099010100000${n}_t${n}.sql`;
const steps = [
  step(u("1"), "CREATE TABLE sessions.t1 (x int);"),
  step(u("2"), "CREATE TABLE sessions.t2 (x int);"),
  step(u("3"), "CREATE TABLE sessions.t3 (x int);"),
  step(u("4"), "CREATE TABLE sessions.t4 (x int);"),
  step("verify:probe", "CREATE TABLE IF NOT EXISTS sessions.verify_ran (x int);"),
];
const fresh = async () => {
  const db = new PGlite();
  open.push(db);
  return db;
};
const ledger = async (db: PGlite) =>
  (await db.query<{ name: string }>("SELECT name FROM sessions.__96bulls_migrations ORDER BY name")).rows.map((r) => r.name);
const marker = async (db: PGlite) =>
  (await db.query<{ generation: string; status: string }>("SELECT generation, status FROM sessions.__96bulls_role_validation")).rows;
const exists = async (db: PGlite, rel: string) =>
  (await db.query<{ r: string | null }>("SELECT to_regclass($1)::text AS r", [rel])).rows[0]!.r !== null;
const failure = (p: Promise<unknown>) => p.then(() => null, (e: { code?: string }) => e.code ?? "no_code");

test("steps after the target are held: never applied or ledgered, no phase 2, marker stays pending", async () => {
  const db = await fresh();
  const gen = await generation();
  const first = await runMigrations(client(db), { profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")] });
  expect(first).toEqual({ applied: 2, skipped: 0, held: [u("3"), u("4")] });
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  expect(await exists(db, "sessions.t3")).toBe(false);
  expect(await exists(db, "sessions.t4")).toBe(false);
  expect(await exists(db, "sessions.verify_ran")).toBe(false);
  expect(await marker(db)).toEqual([{ generation: gen, status: "pending" }]);
  // The next release targets the tail: the held steps apply, phase 2 runs and passes.
  const second = await runMigrations(client(db), { profile: LOCAL, steps, target: u("4"), expectedPending: [u("3"), u("4")] });
  expect(second).toEqual({ applied: 2, skipped: 2, held: [] });
  expect(await ledger(db)).toEqual([u("1"), u("2"), u("3"), u("4")]);
  expect(await exists(db, "sessions.verify_ran")).toBe(true);
  expect(await marker(db)).toEqual([{ generation: gen, status: "pass" }]);
});

test("a pending set other than the approved one refuses under the lock before any write", async () => {
  const db = await fresh();
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("1"), expectedPending: [u("1")] });
  const ledgerBefore = await ledger(db);
  const markerBefore = await marker(db);
  for (const expectedPending of [[u("2")], [u("1"), u("2")], [u("2"), u("3"), u("4")], [u("3"), u("2")], [u("3")], []]) {
    const writes: string[] = [];
    let gate = false;
    expect(
      await failure(
        runMigrations(client(db, writes), {
          profile: LOCAL,
          steps,
          target: u("3"),
          expectedPending,
          beforeWrite: async () => {
            gate = true;
          },
        }),
      ),
    ).toBe("release_target_pending_changed");
    expect(writes).toEqual([]);
    expect(gate).toBe(false);
  }
  expect(await ledger(db)).toEqual(ledgerBefore);
  expect(await marker(db)).toEqual(markerBefore);
});

test("unknown, applied or missing targets refuse before any write; no target keeps the full run", async () => {
  const db = await fresh();
  await runMigrations(client(db), { profile: LOCAL, steps, target: u("2"), expectedPending: [u("1"), u("2")] });
  for (const [opts, code] of [
    [{ target: "upstream:20990101000009_t9.sql" }, "release_target_unknown"],
    [{ target: "verify:probe" }, "release_target_unknown"],
    [{ target: u("2") }, "release_target_not_pending"],
    [{ target: u("1"), expectedPending: [] }, "release_target_not_pending"],
    [{ expectedPending: [u("3")] }, "release_target_required"],
  ] as const) {
    const writes: string[] = [];
    expect(await failure(runMigrations(client(db, writes), { profile: LOCAL, steps, ...opts }))).toBe(code);
    expect(writes).toEqual([]);
  }
  expect(await ledger(db)).toEqual([u("1"), u("2")]);
  // Without a target the migrator is unchanged: every pending step and phase 2.
  expect(await runMigrations(client(db), { profile: LOCAL, steps })).toEqual({ applied: 2, skipped: 2, held: [] });
  expect((await marker(db))[0]!.status).toBe("pass");
});
