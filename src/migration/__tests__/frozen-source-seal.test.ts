import { test, expect, setDefaultTimeout, afterAll } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtempSync, readFileSync, rmSync, cpSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as seal from "../frozen-source-seal.ts";
import { initSource, initTarget, sealRows, ORGS, ORG_ORDER_C } from "./fixtures/frozen-seal-fixture.ts";
import { runScript, readOnlyViolations, leaksOrgIds, SCRIPT, ESCAPES } from "./fixtures/frozen-seal-harness.ts";
setDefaultTimeout(180000);

const dirs: string[] = [];
afterAll(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
// Two masters, built once: a frozen source with an empty target, and the same after the real --copy.
let masters: Promise<{ fresh: string; copied: string }> | undefined;
async function buildMaster(copied: boolean) {
  const dir = mkdtempSync(join(tmpdir(), "frozen-seal-master-")); dirs.push(dir);
  const src = new PGlite(join(dir, "src")), tgt = new PGlite(join(dir, "tgt"));
  await initSource(src, (s) => src.exec(s)); await initTarget((s) => tgt.exec(s));
  await src.close(); await tgt.close();
  if (copied) expect(harness(dir).run(["--copy", "--concurrency", "2", "--page", "7"]).status).toBe(0);
  return dir;
}
function harness(dir: string) {
  const env = { SEAL_TEST_BACKEND: "pglite", SEAL_TEST_SRC_DIR: join(dir, "src"), SEAL_TEST_TGT_DIR: join(dir, "tgt") };
  const run = (args: string[], extra: Record<string, string> = {}) => runScript(args, env, join(dir, "log.json"), extra);
  const onTarget = async <T>(fn: (db: PGlite) => Promise<T>) => { const db = new PGlite(join(dir, "tgt")); try { return await fn(db); } finally { await db.close(); } };
  return { dir, run, onTarget, rows: () => onTarget((db) => sealRows(db as any)) };
}
/** A private copy of a master fixture; `copied` = after a real `--copy` run. */
async function fixture(copied: boolean) {
  masters ??= (async () => ({ fresh: await buildMaster(false), copied: await buildMaster(true) }))();
  const m = await masters, dir = mkdtempSync(join(tmpdir(), "frozen-seal-")); dirs.push(dir);
  for (const side of ["src", "tgt"]) cpSync(join(copied ? m.copied : m.fresh, side), join(dir, side), { recursive: true });
  return harness(dir);
}
const stopped = (r: { lines: any[] }) => r.lines.find((l) => l.m === "STOPPED")?.why as string | undefined;
const total = Object.values(ORGS).reduce((a, b) => a + b, 0);

test("module queries, row text and digest are byte-identical to the serial script's", () => {
  const script = readFileSync(SCRIPT, "utf8"), mod = readFileSync(new URL("../frozen-source-seal.ts", import.meta.url), "utf8");
  const tpl = (src: string, name: string) => new RegExp(`const ${name} = (\`[^\`]*\`|"[^\\n]*");`).exec(src)?.[1];
  for (const name of ["NEON_PAGE", "NEON_RECEIPTS"]) { expect(tpl(script, name)).toBeTruthy(); expect(tpl(mod, name)).toBe(tpl(script, name)); }
  const line = (src: string, name: string) => new RegExp(`const ${name} = ([^\\n]*)`).exec(src)?.[1];
  for (const name of ["COLS", "rowText", "receiptText", "MOD", "addDigest"]) { expect(line(script, name)).toBeTruthy(); expect(line(mod, name)).toBe(line(script, name)); }
  expect(script).toContain('"SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions"');
  expect(script).toContain('(await s.query("SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.ingest_frozen_source_deletions")).rows');
  expect(script).toContain(`"${seal.NEON_HEADS}"`);
  expect(script).toContain(seal.NEON_ORGS.replace(/"/g, '\\"'));
});

test("parallel seal (1 and 4 workers) writes exactly the serial seal's attestation", async () => {
  const runs: any[] = [];
  for (const args of [["--seal"], ["--seal", "--workers", "1"], ["--seal", "--workers", "4"]]) {
    const f = await fixture(true);
    const r = f.run([...args, "--page", "7"]);
    expect(stopped(r)).toBeUndefined(); expect(r.status).toBe(0);
    const rows = await f.rows();
    expect(rows.seals.length).toBe(ORG_ORDER_C.length);
    expect(rows.completeness.length).toBe(1);
    expect(rows.completeness[0].record_rows).toBe(String(total));
    expect(r.lines.find((l) => l.m === "sealed").manifestSha).toBe(rows.completeness[0].manifest_sha256);
    expect(r.log.live).toEqual([]);
    runs.push(rows);
  }
  expect(runs[1]).toEqual(runs[0]);
  expect(runs[2]).toEqual(runs[0]);
});

test("--plan prints exact per-org counts by ordinal and no identifiers", async () => {
  const f = await fixture(false);
  const r = f.run(["--plan"]);
  expect(r.status).toBe(0);
  const per = r.lines.filter((l) => l.m === "plan org");
  expect(per.map((l) => l.records)).toEqual(ORG_ORDER_C.map((o) => ORGS[o as keyof typeof ORGS]));
  expect(per.map((l) => l.ordinal)).toEqual(ORG_ORDER_C.map((_, i) => i));
  const sum = r.lines.find((l) => l.m === "plan counts");
  expect(sum).toMatchObject({ orgs: ORG_ORDER_C.length, records: total, emptyOrgs: 1, maxOrgRecords: 63 });
  expect(per.reduce((a, l) => a + l.receipts, 0)).toBe(sum.receipts);
  expect(leaksOrgIds(r.stdout)).toBe(false);
  expect(readOnlyViolations(r.log.statements)).toEqual([]);
  expect(f.run(["--plan", "--estimate-only"]).lines.some((l) => l.m === "plan org")).toBe(false);
});

for (const [name, args] of [["ordinal subset", ["--orgs", "1,3"]], ["row cap", ["--row-cap", "30", "--workers", "3"]], ["max seconds", ["--row-cap", "1000000", "--max-seconds", "1"]]] as const) {
  test(`--time-leg (${name}) times the exact seal legs read-only with aggregate-only output`, async () => {
    const f = await fixture(true);
    const before = await f.onTarget(async (db) => (await db.query("SELECT count(*)::int n FROM sessions.ingest_frozen_source_records")).rows);
    const r = f.run(["--time-leg", ...args, "--page", "7"]);
    expect(stopped(r)).toBeUndefined(); expect(r.status).toBe(0);
    const t = r.lines.find((l) => l.m === "time-leg");
    for (const leg of ["neonRecords", "supabaseRecords", "neonReceipts", "supabaseReceipts"]) expect(t.legs[leg]).toHaveProperty("rowsPerBusySecond");
    expect(t.sealFencePremise).toBe("ok");
    if (name === "ordinal subset") {
      expect(t).toMatchObject({ orgsSelected: 2, orgsFullyScanned: 2, orgsDigestEqual: 2, orgsDigestUnequal: 0, stoppedBy: "complete" });
      expect(t.legs.neonRecords.rows).toBe(ORGS[ORG_ORDER_C[1] as keyof typeof ORGS] + ORGS[ORG_ORDER_C[3] as keyof typeof ORGS]);
      expect(t.legs.supabaseRecords.rows).toBe(t.legs.neonRecords.rows);
    }
    if (name === "row cap") { expect(t.stoppedBy).toBe("row-cap"); expect(t.legs.neonRecords.rows).toBeLessThan(30 + 3 * 7); expect(t.legs.neonRecords.rows).toBeGreaterThanOrEqual(30); }
    expect(leaksOrgIds(r.stdout + r.stderr)).toBe(false);
    expect(r.stdout).not.toContain("synthetic-cli-password");
    // Only SELECTs, each inside a READ ONLY transaction (explicit or session default), on every connection.
    expect(r.log.statements.length).toBeGreaterThan(10);
    expect(readOnlyViolations(r.log.statements)).toEqual([]);
    expect(r.log.statements.some((s: any) => s.side === "target" && s.sql.startsWith("SELECT org_id,workspace_id,origin_id,record_id"))).toBe(true);
    expect(r.log.live).toEqual([]);
    expect(await f.rows()).toEqual({ seals: [], completeness: [] });
    expect(await f.onTarget(async (db) => (await db.query("SELECT count(*)::int n FROM sessions.ingest_frozen_source_records")).rows)).toEqual(before);
  });
}

for (const args of [["--time-leg"], ["--time-leg", "--org", "x"], ["--time-leg", "--orgs", "1,x"], ["--time-leg", "--row-cap", "0"], ["--time-leg", "--orgs", "1", "--max-seconds", "4000"],
  ["--seal", "--workers", "0"], ["--seal", "--workers", "9"], ["--seal", "--workers", "4", "--org", "x"], ["--copy", "--workers", "2"], ["--seal", "--time-leg", "--orgs", "1"]]) {
  test("refuses before reading stdin: " + JSON.stringify(args), () => {
    const r = runScript(args, { SEAL_TEST_BACKEND: "none" }, join(tmpdir(), "frozen-seal-refuse.json"));
    expect(r.status).toBe(64);
    expect(r.stdout).toBe("");
  });
}

test("--time-leg refuses an out-of-range ordinal without printing identifiers", async () => {
  const f = await fixture(false);
  const r = f.run(["--time-leg", "--orgs", "0,99"]);
  expect(r.status).toBe(1); expect(stopped(r)).toBe("invalid_ordinals");
  expect(leaksOrgIds(r.stdout)).toBe(false);
});

for (const workers of ["1", "4"]) {
  test(`a permitted concurrent source write during verification makes the parallel seal refuse (${workers} workers)`, async () => {
    const f = await fixture(true);
    const r = f.run(["--seal", "--workers", workers, "--page", "7"], { SEAL_TEST_WRITE: "disable", SEAL_TEST_WRITE_WHEN_ORG: "org-z", SEAL_TEST_WRITE_WHEN_ORG_TARGET: "Org-B", SEAL_TEST_WRITE_MIRROR: "1" });
    expect(r.log.wrote).toBe(true);
    expect(r.status).toBe(1);
    expect(stopped(r)).toMatch(/^(source_changed|privacy_copy_mismatch)/);
    expect(await f.rows()).toEqual({ seals: [], completeness: [] });
    expect(r.log.live).toEqual([]);
  });
}

for (const c of ESCAPES) {
  test(`serial seal misses a ${c.name} during verification; the fenced seal refuses it`, async () => {
    const a = await fixture(true);
    const serial = a.run(["--seal", "--page", "7"], c.hook);
    expect(serial.log.wrote).toBe(true);
    expect(serial.status).toBe(0);
    expect((await a.rows()).completeness.length).toBe(1);
    for (const workers of ["1", "4"]) {
      const b = await fixture(true);
      const fenced = b.run(["--seal", "--workers", workers, "--page", "7"], c.hook);
      expect(fenced.log.wrote).toBe(true);
      expect(fenced.status).toBe(1);
      expect(stopped(fenced)).toMatch(c.fenced);
      expect(await b.rows()).toEqual({ seals: [], completeness: [] });
      expect(fenced.log.live).toEqual([]);
    }
  });
}

test("a records-only control on the org being verified refuses", async () => {
  const f = await fixture(true);
  const r = f.run(["--seal", "--workers", "4", "--page", "7"], { SEAL_TEST_WRITE: "disable", SEAL_TEST_WRITE_WHEN_ORG: "org-a", SEAL_TEST_WRITE_WHEN_ORG_TARGET: "org-a", SEAL_TEST_WRITE_MIRROR: "1" });
  expect(r.log.wrote).toBe(true);
  expect(r.status).toBe(1); expect(stopped(r)).toMatch(/^source_changed/);
  expect(await f.rows()).toEqual({ seals: [], completeness: [] });
});

test("without the 0078 freeze guards the parallel seal refuses up front and the timer reports it", async () => {
  const f = await fixture(true);
  const db = new PGlite(join(f.dir, "src"));
  try { await db.exec("ALTER TABLE sessions.delivery_receipts DISABLE TRIGGER delivery_cutover_statement"); } finally { await db.close(); }
  const r = f.run(["--seal", "--workers", "2", "--page", "7"]);
  expect(r.status).toBe(1); expect(stopped(r)).toMatch(/^source_not_frozen/);
  expect(r.log.statements.some((s: any) => s.sql.startsWith("SELECT org_id,workspace_id,origin_id,record_id"))).toBe(false);
  expect(await f.rows()).toEqual({ seals: [], completeness: [] });
  const t = f.run(["--time-leg", "--orgs", "0", "--page", "7"]);
  expect(t.status).toBe(0);
  expect(t.lines.find((l) => l.m === "time-leg").sealFencePremise).toBe("missing");
});

test("a worker failure fails closed: no seal, every connection closed", async () => {
  const f = await fixture(true);
  const r = f.run(["--seal", "--workers", "4", "--page", "7"], { SEAL_TEST_FAIL_PAGE: "6" });
  expect(r.status).toBe(1);
  expect(stopped(r)).toContain("synthetic_worker_failure");
  expect(await f.rows()).toEqual({ seals: [], completeness: [] });
  expect(r.log.live).toEqual([]);
});

test("a genuine copy mismatch refuses by ordinal without printing the org identifier", async () => {
  const f = await fixture(true);
  await f.onTarget((db) => db.query("DELETE FROM sessions.ingest_frozen_source_records WHERE ctid IN (SELECT ctid FROM sessions.ingest_frozen_source_records WHERE org_id='org-z' LIMIT 1)"));
  const r = f.run(["--seal", "--workers", "3", "--page", "7"]);
  expect(r.status).toBe(1);
  expect(stopped(r)).toBe(`org_record_mismatch: org #${ORG_ORDER_C.indexOf("org-z")} neon rows 25 supabase rows 24`);
  expect(leaksOrgIds(r.stdout)).toBe(false);
  expect(await f.rows()).toEqual({ seals: [], completeness: [] });
});

test("the fixture's 0078 guard refuses an unpermitted legacy write while frozen", async () => {
  const f = await fixture(false);
  const db = new PGlite(join(f.dir, "src"));
  try {
    await expect(db.query("UPDATE sessions.delivery_records SET operation='delete' WHERE org_id='org-a'")).rejects.toThrow("delivery_cutover_held");
    const h = (await db.query<{ h: any }>("SELECT sessions.fixture_disable_session_v1('org-a','ws-1','u1','origin-1','claude','s1') AS h")).rows[0]!.h;
    expect(Number(h.version)).toBe(3);
  } finally { await db.close(); }
});

test("timeLegs rejects unbounded or invalid options before any statement", async () => {
  const calls: string[] = [];
  const q = { query: async (sql: string) => { calls.push(sql); return { rows: [] }; } };
  for (const o of [{ workers: 0 }, { workers: 9 }, { maxSeconds: 0 }, { maxSeconds: 4000 }, { rowCap: 0 }]) {
    await expect(seal.timeLegs({ target: q, openSource: async () => ({ ...q, end: async () => {} }), orgs: ["x"], workers: 1, page: 5, maxPageMs: 1000, maxSeconds: 10, ...o })).rejects.toThrow();
  }
  expect(calls).toEqual([]);
});
