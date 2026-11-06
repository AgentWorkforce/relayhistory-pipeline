// Disposable PG17 proof of the frozen-source seal timer and parallel seal (real sessions, real
// concurrency, C.UTF-8 source vs ICU target like Neon vs Supabase). Skipped unless
// FROZEN_SEAL_TEST_PG_URL names a loopback admin URL of a throwaway container.
import { test, expect, setDefaultTimeout, afterAll } from "bun:test";
import pg from "pg";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initSource, initTarget, sealRows, ORGS, ORG_ORDER_C } from "./fixtures/frozen-seal-fixture.ts";
import { runScript, readOnlyViolations, leaksOrgIds, ESCAPES } from "./fixtures/frozen-seal-harness.ts";
setDefaultTimeout(240000);

const admin = process.env.FROZEN_SEAL_TEST_PG_URL;
const t = admin && /^postgres(ql)?:\/\/[^@]*@(127\.0\.0\.1|localhost):\d+\//.test(admin) ? test : test.skip;
const created: string[] = [];
const withDb = async <T>(db: string, fn: (c: pg.Client) => Promise<T>) => { const c = new pg.Client({ connectionString: url(db) }); await c.connect(); try { return await fn(c); } finally { await c.end(); } };
const url = (db: string) => admin!.replace(/\/[^/?]*(\?|$)/, `/${db}$1`);
afterAll(async () => { if (!admin) return; await withDb("postgres", async (c) => { for (const d of created) await c.query(`DROP DATABASE IF EXISTS ${d} WITH (FORCE)`); }); });

let seq = 0;
async function fixture() {
  const id = `fseal_${process.pid}_${++seq}`, src = id + "_src", tgt = id + "_tgt";
  await withDb("postgres", async (c) => {
    await c.query(`CREATE DATABASE ${src} TEMPLATE template0 LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8' ENCODING 'UTF8'`);
    await c.query(`CREATE DATABASE ${tgt} TEMPLATE template0 LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C.UTF-8' ENCODING 'UTF8'`);
  });
  created.push(src, tgt);
  await withDb(src, (c) => initSource(c as any, (s) => c.query(s)));
  await withDb(tgt, (c) => initTarget((s) => c.query(s)));
  const env = { SEAL_TEST_BACKEND: "pg", SEAL_TEST_SRC_URL: url(src), SEAL_TEST_TGT_URL: url(tgt) };
  const run = (args: string[], extra: Record<string, string> = {}) => runScript(args, env, join(tmpdir(), id + ".log.json"), extra);
  const copy = () => { const r = run(["--copy", "--concurrency", "2", "--page", "7"]); expect(r.status).toBe(0); };
  const writes = async () => {
    const q = "SELECT coalesce(sum(n_tup_ins+n_tup_upd+n_tup_del),0)::bigint::text AS w FROM pg_stat_user_tables";
    await new Promise((r) => setTimeout(r, 1200));
    return { src: await withDb(src, async (c) => { await c.query("SELECT pg_stat_clear_snapshot()"); return (await c.query(q)).rows[0].w; }),
             tgt: await withDb(tgt, async (c) => { await c.query("SELECT pg_stat_clear_snapshot()"); return (await c.query(q)).rows[0].w; }) };
  };
  return { run, copy, writes, rows: () => withDb(tgt, (c) => sealRows(c as any)), src, tgt };
}
const stopped = (r: { lines: any[] }) => r.lines.find((l) => l.m === "STOPPED")?.why as string | undefined;
const empty = { seals: [], completeness: [] };

t("PG17: parallel seal with 4 workers writes exactly the unchanged serial seal's attestation", async () => {
  const out: any[] = [];
  for (const args of [["--seal"], ["--seal", "--workers", "4"]]) {
    const f = await fixture(); f.copy();
    const r = f.run([...args, "--page", "7"]);
    expect(stopped(r)).toBeUndefined(); expect(r.status).toBe(0); expect(r.log.live).toEqual([]);
    const rows = await f.rows();
    expect(rows.seals.length).toBe(ORG_ORDER_C.length);
    expect(rows.completeness[0].record_rows).toBe(String(Object.values(ORGS).reduce((a, b) => a + b, 0)));
    out.push(rows);
  }
  expect(out[1]).toEqual(out[0]);
});

t("PG17: --time-leg issues only SELECTs in READ ONLY transactions and writes nothing", async () => {
  const f = await fixture(); f.copy();
  const before = await f.writes();
  const r = f.run(["--time-leg", "--orgs", "0,1,3,4", "--workers", "2", "--page", "7"], { SEAL_TEST_PROBE: "1" });
  expect(stopped(r)).toBeUndefined(); expect(r.status).toBe(0);
  const tl = r.lines.find((l) => l.m === "time-leg");
  expect(tl).toMatchObject({ orgsSelected: 4, orgsFullyScanned: 4, orgsDigestEqual: 4, orgsDigestUnequal: 0, stoppedBy: "complete", workers: 2 });
  expect(readOnlyViolations(r.log.statements)).toEqual([]);
  // Every SELECT was probed on its own session and the server reported transaction_read_only=on.
  const selects = r.log.statements.filter((s: any) => s.sql.startsWith("SELECT "));
  expect(selects.length).toBeGreaterThan(20);
  expect(selects.every((s: any) => s.probe === "on")).toBe(true);
  expect(leaksOrgIds(r.stdout + r.stderr)).toBe(false);
  expect(await f.writes()).toEqual(before);
  expect(await f.rows()).toEqual(empty);
});

t("PG17: --plan counts every org exactly by ordinal, read-only", async () => {
  const f = await fixture();
  const r = f.run(["--plan"], { SEAL_TEST_PROBE: "1" });
  expect(r.status).toBe(0);
  expect(r.lines.filter((l) => l.m === "plan org").map((l) => l.records)).toEqual(ORG_ORDER_C.map((o) => ORGS[o as keyof typeof ORGS]));
  expect(readOnlyViolations(r.log.statements)).toEqual([]);
  expect(leaksOrgIds(r.stdout)).toBe(false);
});

t("PG17: a committed concurrent control on another session during verification makes the parallel seal refuse", async () => {
  for (const write of ["disable", "empty-reset"]) {
    const f = await fixture(); f.copy();
    const r = f.run(["--seal", "--workers", "4", "--page", "7"], { SEAL_TEST_WRITE: write, SEAL_TEST_WRITE_WHEN_ORG: "org-z", SEAL_TEST_WRITE_WHEN_ORG_TARGET: "Org-B", SEAL_TEST_WRITE_MIRROR: "1" });
    expect(r.log.wrote).toBe(true);
    expect(r.status).toBe(1);
    expect(stopped(r)).toMatch(/^(source_changed|privacy_copy_mismatch|reset_copy_mismatch)/);
    expect(await f.rows()).toEqual(empty);
  }
});

t("PG17: a terminated worker backend fails the seal closed", async () => {
  const f = await fixture(); f.copy();
  const r = f.run(["--seal", "--workers", "4", "--page", "7"], { SEAL_TEST_FAIL_PAGE: "5" });
  expect(r.status).toBe(1);
  expect(stopped(r)).toBeTruthy();
  expect(await f.rows()).toEqual(empty);
  expect(r.log.live).toEqual([]);
});

for (const c of ESCAPES) t(`PG17: serial seal misses a ${c.name} committed on another session; the fenced seal refuses it`, async () => {
  const a = await fixture(); a.copy();
  const serial = a.run(["--seal", "--page", "7"], c.hook);
  expect(serial.log.wrote).toBe(true); expect(serial.status).toBe(0);
  expect((await a.rows()).completeness.length).toBe(1);
  for (const workers of ["1", "4"]) {
    const b = await fixture(); b.copy();
    const fenced = b.run(["--seal", "--workers", workers, "--page", "7"], c.hook);
    expect(fenced.log.wrote).toBe(true); expect(fenced.status).toBe(1);
    expect(stopped(fenced)).toMatch(c.fenced);
    expect(await b.rows()).toEqual(empty);
  }
});
