/**
 * The Neon census loader (src/migration/neon-census.ts, scripts/neon-census-load.ts) against
 * the real 046000 migration (PGlite) and a fake Neon (PGlite with the legacy session tables).
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { spawnSync } from "node:child_process";
import { database, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import { CensusDigest, censusGeneration, loadAndSeal, pinSource, readNeonCensus, verifyCensus, type Queryable } from "../neon-census.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => {
  while (open.length) await open.pop()!.close();
});

async function fakeNeon(opts: { without?: string } = {}) {
  const neon = new PGlite();
  open.push(neon);
  const tables: Record<string, string> = {
    delivery_records: "org_id text, workspace_id text, origin_id text, record_id text, user_id text, source text, session_id text",
    conversation_turns: "id uuid, org_id text, workspace_id text, source text, session_id text, session_owner text, ts timestamptz",
    convergence_events: "org_id text, workspace_id text, source text, session_id text",
    session_links: "id bigint, org_id text, workspace_id text, source text, session_id text",
    session_outcomes: "org_id text, workspace_id text, source text, session_id text, user_id text",
    session_rollups: "org_id text, session_id text",
    session_catalog: "org_id text, session_id text",
  };
  await neon.exec("CREATE SCHEMA sessions");
  for (const [t, cols] of Object.entries(tables)) if (t !== opts.without) await neon.exec(`CREATE TABLE sessions.${t} (${cols})`);
  if (opts.without !== "delivery_records")
    await neon.exec(`INSERT INTO sessions.delivery_records VALUES ('org-a','ws-a','o','r1','u-1','claude','s-delivered'),
      ('org-a','ws-a','o','r2','u-1','claude','s-delivered'), ('org-a','ws-a','o','r3','u-1','claude',NULL)`);
  if (opts.without !== "conversation_turns")
    // A turns-only Neon session: no delivery record anywhere.
    await neon.exec(`INSERT INTO sessions.conversation_turns VALUES (gen_random_uuid(),'org-a','ws-old','claude','s-turns-only','owner',now())`);
  if (opts.without !== "session_rollups") await neon.exec(`INSERT INTO sessions.session_rollups VALUES ('org-b','s-rollup')`);
  return neon;
}
const q = (pg: PGlite): Queryable => ({ query: async (t, p) => ({ rows: (await pg.query(t, p as never)).rows as never[] }) });

async function supabase() {
  const db = await database();
  open.push(db);
  await db.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await db.query(
    `INSERT INTO sessions.ingest_frozen_source_completeness
       (source_identity_sha256, control_epoch, privacy_baseline_sha256, org_count, record_rows, manifest_sha256, neon_snapshot, sealed_at)
     VALUES (repeat('a',64), repeat('b',64), repeat('c',64), 1, 0, repeat('d',64), '{}'::jsonb, now() - interval '1 hour')`,
  );
  return db;
}
/** Pin the (fake) Neon as the authoritative source, as the lead does with --pin. */
const pin = (db: PGlite, census: { snapshot: { source_identity: Record<string, unknown> } }) =>
  db.query("SELECT sessions.session_neon_census_pin_source($1::jsonb)", [JSON.stringify(census.snapshot.source_identity)]);
const free = async (db: PGlite, session: string) => {
  await pointer(db, { key: { ...KEY, sessionId: session }, machine: `m-${session}` });
  return (await db.query<{ f: boolean }>("SELECT sessions.session_analysis_neon_free($1, $2, $3) AS f", [KEY.orgId, KEY.workspaceId, session])).rows[0]!.f;
};
const censusRows = async (db: PGlite) => (await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_neon_census")).rows[0]!.n;

test("dry run: reads Neon read-only (turns-only and rollup-only sessions included), writes nothing; the JS digest equals the SQL digest", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  const census = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  expect(census.rows.map((r) => r.join("/")).sort()).toEqual([
    "org-a/ws-a/u-1/claude/s-delivered",
    "org-a/ws-old//claude/s-turns-only",
    "org-b////s-rollup",
  ]);
  expect(census.count).toBe(3);
  expect(census.sha256).toMatch(/^[0-9a-f]{64}$/);
  expect(census.snapshot.snapshot).toBeTruthy();
  expect(await censusRows(db)).toBe(0);
  expect((await db.query("SELECT 1 FROM sessions.session_neon_census_seal")).rows).toHaveLength(0);
  // A read-only transaction: a write inside the reader's snapshot would have failed.
  expect((await neon.query<{ n: number }>("SELECT count(*)::int n FROM sessions.conversation_turns")).rows[0]!.n).toBe(1);
  // Same digest on both sides once loaded (unsealed).
  const generation = censusGeneration(census);
  await db.query("SELECT sessions.session_neon_census_load($1, $2::jsonb)", [generation, JSON.stringify(census.rows)]);
  const sql = (await db.query<{ d: { rows: number; sha256: string } }>("SELECT sessions.session_neon_census_digest($1) AS d", [generation])).rows[0]!.d;
  expect(sql).toEqual({ rows: 3, sha256: census.sha256 });
});

test("partial load never admits; a resume continues the same unsealed generation and still refuses; the seal admits only sessions it does not name", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  const census = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  await pin(db, census);
  const generation = censusGeneration(census);
  // Partial (one row of three), unsealed:
  await db.query("SELECT sessions.session_neon_census_load($1, $2::jsonb)", [generation, JSON.stringify(census.rows.slice(0, 1))]);
  expect(await free(db, "s-fresh")).toBe(false);
  // Resume: the same generation, idempotent, still unsealed.
  await db.query("SELECT sessions.session_neon_census_load($1, $2::jsonb)", [generation, JSON.stringify(census.rows)]);
  expect(await censusRows(db)).toBe(3);
  expect(await free(db, "s-fresh")).toBe(false);
  // Execute (resumes and seals):
  const out = await loadAndSeal(q(db), census, { batch: 2 });
  expect(out).toMatchObject({ generation, inserted: 0, sealed: true, already: false });
  expect(await free(db, "s-fresh")).toBe(true); // sealed census, no matching row: admit
  expect(await free(db, "s-turns-only")).toBe(false); // the old turns-only session (another workspace): refused
  expect(await free(db, "s-delivered")).toBe(false);
  // Idempotent rerun.
  expect(await loadAndSeal(q(db), census)).toMatchObject({ already: true, inserted: 0 });
  expect((await verifyCensus(q(db), census)).ok).toBe(true);
});

test("the seal refuses a digest or row-count mismatch, and a seal without the snapshot identity", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  const census = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  await pin(db, census);
  const generation = censusGeneration(census);
  await db.query("SELECT sessions.session_neon_census_load($1, $2::jsonb)", [generation, JSON.stringify(census.rows)]);
  const seal = (rows: number, sha: string, snapshot: unknown = census.snapshot) =>
    db.query("SELECT sessions.session_neon_census_seal($1, $2::bigint, $3, $4::jsonb)", [generation, rows, sha, JSON.stringify(snapshot)]);
  await expect(seal(3, "f".repeat(64))).rejects.toThrow("recomputed digest does not match");
  await expect(seal(4, census.sha256)).rejects.toThrow("expected 4");
  await expect(seal(3, census.sha256, { rows: 3 })).rejects.toThrow("snapshot and source identity");
  expect((await db.query("SELECT 1 FROM sessions.session_neon_census_seal")).rows).toHaveLength(0);
  expect(await free(db, "s-fresh")).toBe(false);
});

test("a sealed census is immutable: load, insert, update, delete and truncate are refused", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  const census = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  await pin(db, census);
  const { generation } = await loadAndSeal(q(db), census);
  const refused = "a sealed census is immutable";
  await expect(db.query("SELECT sessions.session_neon_census_load($1, $2::jsonb)", [generation, JSON.stringify([["org-a", "w", "", "", "x"]])])).rejects.toThrow(refused);
  await expect(db.query("INSERT INTO sessions.session_neon_census (generation, org_id, session_id) VALUES ($1, 'org-a', 'x')", [generation])).rejects.toThrow(refused);
  await expect(db.query("UPDATE sessions.session_neon_census SET source = 'x'")).rejects.toThrow(refused);
  await expect(db.query("DELETE FROM sessions.session_neon_census")).rejects.toThrow(refused);
  await expect(db.query("TRUNCATE sessions.session_neon_census")).rejects.toThrow(refused);
  await expect(db.query("SELECT sessions.session_neon_census_seal($1, 2, $2, $3::jsonb)", [generation, census.sha256, JSON.stringify(census.snapshot)])).rejects.toThrow(refused);
  expect(await censusRows(db)).toBe(3);
});

test("verify recomputes from Neon: a row missing from the sealed census is a mismatch; a reload is a NEW generation that replaces the old", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  await pin(db, await readNeonCensus(q(neon), { endpoint: "ep-prod.example" }));
  const first = await loadAndSeal(q(db), await readNeonCensus(q(neon), { endpoint: "ep-prod.example" }));
  // Neon holds a session the sealed census lacks (e.g. it was loaded from an incomplete read).
  await neon.exec("INSERT INTO sessions.session_links VALUES (1, 'org-a', 'ws-a', 'codex', 's-link-only')");
  const now = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  const v = await verifyCensus(q(db), now);
  expect(v).toMatchObject({ ok: false, reason: "mismatch", neon: { rows: 4 }, sealed: { rows: 3 } });
  expect(await free(db, "s-link-only")).toBe(true); // the stale seal does not know it: why verify matters
  // Reload: a new generation, loaded and sealed; the old generation's rows are dropped.
  const second = await loadAndSeal(q(db), now);
  expect(second.generation).not.toBe(first.generation);
  expect(await censusRows(db)).toBe(4);
  expect((await verifyCensus(q(db), now)).ok).toBe(true);
  expect(await free(db, "s-link-only")).toBe(false);
});

test("an incomplete Neon source (a missing table) refuses the read; nothing is unsealed-admitted", async () => {
  const neon = await fakeNeon({ without: "session_outcomes" });
  await expect(readNeonCensus(q(neon), { endpoint: "ep-prod.example" })).rejects.toThrow("neon_census_source_missing: sessions.session_outcomes");
});

test("the digest is order-independent and tenant-keyed", () => {
  const a = new CensusDigest(), b = new CensusDigest(), c = new CensusDigest();
  a.add(["o", "w", "u", "s", "x"]); a.add(["o", "w", "u", "s", "y"]);
  b.add(["o", "w", "u", "s", "y"]); b.add(["o", "w", "u", "s", "x"]);
  c.add(["o", "w2", "u", "s", "x"]); c.add(["o", "w", "u", "s", "y"]);
  expect(a.sha256).toBe(b.sha256);
  expect(c.sha256).not.toBe(a.sha256);
});

test("the load/seal/digest functions are owner-only and caller-pinned", async () => {
  const db = await supabase();
  await db.exec("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'intruder') THEN CREATE ROLE intruder; END IF; END $$");
  await db.exec(`GRANT USAGE ON SCHEMA sessions TO intruder;
    GRANT EXECUTE ON FUNCTION sessions.session_neon_census_load(text, jsonb), sessions.session_neon_census_digest(text) TO intruder`);
  for (const sql of ["SELECT sessions.session_neon_census_load(repeat('a',64), '[]'::jsonb)", "SELECT sessions.session_neon_census_digest(repeat('a',64))"]) {
    await db.exec("BEGIN");
    try {
      await db.exec("SET LOCAL ROLE intruder");
      await expect(db.query(sql)).rejects.toThrow("caller is not a role this function serves");
    } finally {
      await db.exec("ROLLBACK");
    }
  }
  const grants = (await db.query<{ g: string }>(`SELECT DISTINCT r.rolname g FROM pg_proc p
    CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a JOIN pg_roles r ON r.oid = a.grantee
    WHERE p.proname = 'session_neon_census_seal' AND a.grantee <> p.proowner`)).rows;
  expect(grants).toEqual([]);
});

test("the CLI takes credentials from stdin only: argv credentials and a missing stdin URL are refused before any connection", () => {
  const bun = process.execPath;
  const run = (args: string[], stdin: string) => spawnSync(bun, ["scripts/neon-census-load.ts", ...args], { input: stdin, encoding: "utf8", timeout: 30_000 });
  const argv = run(["postgresql://user:secret@host/db"], "");
  expect(argv.status).toBe(64);
  expect(argv.stderr).toContain("credentials go on stdin only");
  expect(argv.stderr + argv.stdout).not.toContain("secret");
  expect(run([], "{}").status).toBe(64);
  const exec = run(["--execute"], JSON.stringify({ neonUrl: "postgresql://n:pw@127.0.0.1:1/x" }));
  expect(exec.status).toBe(64);
  expect(exec.stderr).toContain("supabaseUrl (owner) required");
  expect(exec.stderr + exec.stdout).not.toContain("pw@");
});

test("final review P2: only the PINNED production source seals; a staging/wrong-branch identity is refused; the pin binds the frozen copy", async () => {
  const neon = await fakeNeon();
  const db = await supabase();
  const census = await readNeonCensus(q(neon), { endpoint: "ep-prod.example" });
  // Not pinned: refused.
  await expect(loadAndSeal(q(db), census)).rejects.toThrow("not from the pinned production Neon source");
  // Pinned to a DIFFERENT source (e.g. a staging endpoint): refused.
  await db.query("SELECT sessions.session_neon_census_pin_source($1::jsonb)", [JSON.stringify({ ...census.snapshot.source_identity, endpoint: "ep-staging.example" })]);
  await expect(loadAndSeal(q(db), census)).rejects.toThrow("not from the pinned production Neon source");
  expect((await db.query("SELECT 1 FROM sessions.session_neon_census_seal")).rows).toHaveLength(0);
  // --pin compares the live identity with the operator's expected one before pinning.
  expect(await pinSource(q(db), census, { ...census.snapshot.source_identity, system_identifier: "999" })).toMatchObject({ pinned: false, differs: ["system_identifier"] });
  expect(await pinSource(q(db), census, census.snapshot.source_identity)).toMatchObject({ pinned: true });
  // A frozen copy re-sealed under another source identity unbinds the pin: refused.
  await db.query("UPDATE sessions.ingest_frozen_source_completeness SET source_identity_sha256 = repeat('9', 64)");
  await expect(loadAndSeal(q(db), census)).rejects.toThrow("not from the pinned production Neon source");
  await db.query("UPDATE sessions.ingest_frozen_source_completeness SET source_identity_sha256 = repeat('a', 64)");
  expect(await loadAndSeal(q(db), census)).toMatchObject({ sealed: true });
  // Once sealed, the pin cannot move.
  await expect(db.query("SELECT sessions.session_neon_census_pin_source($1::jsonb)", [JSON.stringify(census.snapshot.source_identity)])).rejects.toThrow("pinned before any seal");
});

test("final re-review P1: the CLI's --pin branch (injected Neon + Supabase clients) writes the pin for a matching identity and exits 2 on a mismatch", async () => {
  const { runCensusCli } = await import("../../../scripts/neon-census-load.ts");
  const neon = await fakeNeon();
  const db = await supabase();
  const asClient = (pg: PGlite) => ({ ...q(pg), connect: async () => {}, end: async () => {} });
  const lines: string[] = [];
  const deps = { client: (url: string) => asClient(url.includes("neon") ? neon : db), out: (l: string) => lines.push(l), err: (l: string) => lines.push(l) };
  // The identity the dry run prints (the endpoint is the neonUrl host).
  const live = (await readNeonCensus(q(neon), { endpoint: "ep-prod.neon.example" })).snapshot.source_identity;
  const stdin = (expected: unknown) => JSON.stringify({ neonUrl: "postgresql://ro@ep-prod.neon.example/db", supabaseUrl: "postgresql://owner@supabase.example/postgres", expectedSourceIdentity: expected });
  expect(await runCensusCli(["--pin"], stdin({ ...live, system_identifier: "0" }), deps)).toBe(2);
  expect((await db.query("SELECT 1 FROM sessions.session_neon_census_source")).rows).toHaveLength(0);
  expect(await runCensusCli(["--pin"], stdin(live), deps)).toBe(0);
  expect((await db.query<{ i: unknown }>("SELECT source_identity i FROM sessions.session_neon_census_source")).rows).toEqual([{ i: live }]);
  // Then --execute seals through the same CLI.
  expect(await runCensusCli(["--execute"], stdin(undefined), deps)).toBe(0);
  expect((await db.query("SELECT 1 FROM sessions.session_neon_census_seal")).rows).toHaveLength(1);
  expect(lines.join("\n")).not.toContain("owner@");
});
