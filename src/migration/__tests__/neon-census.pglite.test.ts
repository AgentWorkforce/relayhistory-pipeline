import { describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DIGEST_SQL,
  LOAD_LOCK_SQL,
  NULL_WORKSPACE,
  censusDigest,
  censusLine,
  censusTables,
  loadCensus,
  lsnCompare,
  parseCensusFile,
  produceCensus,
  verifyCensus,
  type CensusRow,
  type Queryable,
  type SourceClaim,
} from "../neon-census.ts";

/** The two census tables, sliced from the 046000 migration fixture (not copied DDL). */
async function censusDdl(): Promise<string> {
  const text = await Bun.file(new URL("../../session-ingest/__tests__/fixtures/analysis-jobs-046000.sql", import.meta.url)).text();
  const take = (name: string) => {
    const start = text.indexOf(`CREATE TABLE sessions.${name} (`);
    if (start < 0) throw new Error(`fixture lacks ${name}`);
    return text.slice(start, text.indexOf(");", start) + 2);
  };
  return `${take("session_neon_census")}\n${take("session_neon_census_seal")}`;
}

async function source(): Promise<PGlite> {
  const pg = new PGlite();
  await pg.exec(`
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, session_id text, event_id text);
    CREATE TABLE sessions.delivery_records (org_id text, workspace_id text, session_id text, body text);
    CREATE TABLE sessions.machines (org_id text, machine_id text);
    CREATE TABLE sessions.session_timing_cache (org_id text NOT NULL, workspace_id text, session_id text NOT NULL);
    CREATE TABLE sessions.no_ws (org_id text, session_id text);
    CREATE VIEW sessions.v_sessions AS SELECT org_id, session_id FROM sessions.convergence_events;
    INSERT INTO sessions.convergence_events VALUES ('o2','w1','s9','e1'),('o1','w1','s2','e2'),('o1','w1','s2','e3'),('O1','w1','s1','e4');
    INSERT INTO sessions.delivery_records VALUES ('o1','w1','s2','b'),('o1','w2','s2','b'),('o3','w1','é','b'),('o5','w1',NULL,'b');
    INSERT INTO sessions.machines VALUES ('o9','m1');
    INSERT INTO sessions.session_timing_cache VALUES ('o4', NULL, 's4');
    INSERT INTO sessions.no_ws VALUES ('o6','s6');`);
  return pg;
}

async function target(): Promise<PGlite> {
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions; ${await censusDdl()}`);
  return pg;
}

/** PGlite is not Supabase and not in recovery; pg_current_wal_lsn() is its "server" LSN. */
const NEON: SourceClaim = { lsn: "0/0", ref: "test-branch", seal: null, urlSupabaseRef: null };

describe("neon session census", () => {
  test("covers base tables with org_id + session_id; views are reported, not read", async () => {
    const pg = await source();
    expect(await censusTables(pg)).toEqual(["convergence_events", "delivery_records", "no_ws", "session_timing_cache"]);
  });

  test("produce: distinct union, C-ordered, NULL/absent workspace kept, unkeyed counted; JS digest == SQL digest after load", async () => {
    const { rows, manifest } = await produceCensus(await source(), NEON);
    expect(rows).toEqual([
      { org_id: "O1", workspace_id: "w1", session_id: "s1" },
      { org_id: "o1", workspace_id: "w1", session_id: "s2" },
      { org_id: "o1", workspace_id: "w2", session_id: "s2" },
      { org_id: "o2", workspace_id: "w1", session_id: "s9" },
      { org_id: "o3", workspace_id: "w1", session_id: "é" },
      { org_id: "o4", workspace_id: NULL_WORKSPACE, session_id: "s4" },
      { org_id: "o6", workspace_id: NULL_WORKSPACE, session_id: "s6" },
    ]);
    const s = manifest.neon_snapshot;
    expect(s.tables).toEqual({ convergence_events: 4, delivery_records: 4, no_ws: 1, session_timing_cache: 1 });
    expect(s.unkeyed_rows).toEqual({ delivery_records: 1 });
    expect(s.ignored_relations).toEqual({ v_sessions: "view" });
    expect(s.lsn_evidence).toBe("neon-server");
    const tgt = await target();
    const readback = await loadCensus(tgt, rows, manifest, 2);
    expect(readback.census_rows).toBe(7);
    expect((await tgt.query<{ sha: string }>(DIGEST_SQL)).rows[0]!.sha).toBe(manifest.census_sha256);
  });

  test("P2-1a: a Supabase source is refused unless named, and then needs a legacy_copy.seal at or after the claimed LSN", async () => {
    const pg = await source();
    await expect(produceCensus(pg, { ...NEON, urlSupabaseRef: "abcdefghijklmnopqrst" })).rejects.toThrow("Supabase URL");
    await expect(produceCensus(pg, { ...NEON, urlSupabaseRef: "abcdefghijklmnopqrst", allowSupabaseRef: "zzzzzzzzzzzzzzzzzzzz" })).rejects.toThrow("Supabase URL");
    await expect(produceCensus(pg, { ...NEON, urlSupabaseRef: "abcdefghijklmnopqrst", allowSupabaseRef: "abcdefghijklmnopqrst" })).rejects.toThrow("no legacy_copy.seal");
    await pg.exec(`CREATE SCHEMA legacy_copy; CREATE TABLE legacy_copy.seal(sealed_at timestamptz DEFAULT now(), manifest jsonb);
      INSERT INTO legacy_copy.seal(manifest) VALUES ('{"source":{"cut_lsn":"36B/9D2C2D8"}}');`);
    const ok = await produceCensus(pg, { ...NEON, lsn: "36B/9D2C2D8", urlSupabaseRef: "abcdefghijklmnopqrst", allowSupabaseRef: "abcdefghijklmnopqrst" });
    expect(ok.manifest.neon_snapshot.lsn_evidence).toBe("legacy-copy-seal");
    expect(ok.manifest.neon_snapshot.verified_lsn).toBe("36B/9D2C2D8");
    await expect(produceCensus(pg, { ...NEON, lsn: "36B/A0000000", urlSupabaseRef: "abcdefghijklmnopqrst", allowSupabaseRef: "abcdefghijklmnopqrst" }))
      .rejects.toThrow("cut BEFORE");
  });

  test("P2-1b: a source whose server LSN is below the claimed cut is refused", async () => {
    await expect(produceCensus(await source(), { ...NEON, lsn: "FFFFFFFF/FFFFFFFF" })).rejects.toThrow("cut BEFORE");
    expect(lsnCompare("36B/9D2C2D8", "36B/9D43108")).toBe(-1);
    expect(lsnCompare("36C/0", "36B/FFFFFFFF")).toBe(1);
  });

  test("P2-1c: a census table still writable by a non-owner role is refused", async () => {
    const pg = await source();
    await pg.exec("CREATE ROLE app_writer; GRANT INSERT ON sessions.convergence_events TO app_writer;");
    await expect(produceCensus(pg, NEON)).rejects.toThrow("still writable by a non-owner role");
  });

  test("P3-3: a sessions foreign table with census keys is refused (checked via the discovery query)", async () => {
    const fake: Queryable = {
      async query<T>(sql: string) {
        if (sql.startsWith("BEGIN") || sql.startsWith("ROLLBACK")) return { rows: [] as T[] };
        return { rows: [{ relname: "remote_events", relkind: "f", relispartition: false, ws: true }] as T[] };
      },
    };
    await expect(produceCensus(fake, NEON)).rejects.toThrow("foreign table");
  });

  test("P2-2: load uses lock_timeout and EXCLUSIVE (not ACCESS EXCLUSIVE), before any other statement", async () => {
    expect(LOAD_LOCK_SQL[0]).toBe("SET LOCAL lock_timeout = '5s'");
    expect(LOAD_LOCK_SQL[1]).toContain("IN EXCLUSIVE MODE");
    expect(LOAD_LOCK_SQL[1]).not.toContain("ACCESS EXCLUSIVE");
    const seen: string[] = [];
    const tgt = await target();
    const spy: Queryable = { query: (sql, params) => { seen.push(sql); return tgt.query(sql, params) as never; } };
    const { rows, manifest } = await produceCensus(await source(), NEON);
    await loadCensus(spy, rows, manifest);
    expect(seen.slice(0, 3)).toEqual(["BEGIN", ...LOAD_LOCK_SQL]);
  });

  test("P3-8: UTF-8 byte order differs from JS UTF-16 order, and backslash/quote keys; JS digest == SQL digest", async () => {
    const pg = new PGlite();
    await pg.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.t (org_id text, workspace_id text, session_id text);");
    // U+FF61 sorts after U+1F600 in UTF-16 (0xFF61 > 0xD83D) but before it in UTF-8 (EF.. < F0..).
    await pg.query("INSERT INTO sessions.t VALUES ($1,'w','s1'),($2,'w','s2'),('o\\\\x','w''q','s\"3')", ["\u{1F600}", "｡"]);
    const { rows, manifest } = await produceCensus(pg, NEON);
    expect(rows.map((r) => r.org_id)).toEqual(["o\\\\x", "｡", "\u{1F600}"]);
    const tgt = await target();
    await loadCensus(tgt, rows, manifest);
    expect((await tgt.query<{ sha: string }>(DIGEST_SQL)).rows[0]!.sha).toBe(manifest.census_sha256);
  });

  test("the file round-trips and an empty census seals with sha256('')", async () => {
    const { rows, manifest } = await produceCensus(await source(), NEON);
    expect(() => verifyCensus(parseCensusFile(rows.map(censusLine).join("")), manifest)).not.toThrow();
    const empty = new PGlite();
    await empty.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.t (org_id text, workspace_id text, session_id text);");
    const e = await produceCensus(empty, NEON);
    expect(e.manifest.census_sha256).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect((await loadCensus(await target(), e.rows, e.manifest)).census_rows).toBe(0);
  });

  test("load refuses an existing seal or rows and writes nothing; tampering is refused before any write", async () => {
    const { rows, manifest } = await produceCensus(await source(), NEON);
    const tgt = await target();
    await loadCensus(tgt, rows, manifest);
    await expect(loadCensus(tgt, rows, manifest)).rejects.toThrow("seal already exists");
    const tgt2 = await target();
    await tgt2.exec("INSERT INTO sessions.session_neon_census VALUES ('x','y','z')");
    await expect(loadCensus(tgt2, rows, manifest)).rejects.toThrow("census rows already exist");
    await expect(loadCensus(tgt2, rows.slice(1), manifest)).rejects.toThrow("row count differs");
    expect(() => verifyCensus([rows[1]!, rows[0]!, ...rows.slice(2)], manifest)).toThrow("not strictly sorted");
    expect(() => verifyCensus(rows, { ...manifest, census_sha256: "0".repeat(64) })).toThrow("digest differs");
    expect((await tgt2.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_neon_census_seal")).rows[0]!.n).toBe(0);
  });

  test("keys with a tab or newline are refused", () => {
    const bad: CensusRow = { org_id: "o\t1", workspace_id: "w", session_id: "s" };
    expect(() => censusLine(bad)).toThrow("tab or newline");
    expect(() => censusLine({ org_id: "o", workspace_id: "w\n", session_id: "s" })).toThrow("tab or newline");
  });

  test("CLI: load without --execute never connects; --execute without --expect-supabase-ref is refused", async () => {
    const { rows, manifest } = await produceCensus(await source(), NEON);
    const dir = mkdtempSync(join(tmpdir(), "neon-census-"));
    const file = join(dir, "c.tsv");
    writeFileSync(file, rows.map(censusLine).join(""));
    writeFileSync(`${file}.manifest.json`, JSON.stringify(manifest));
    const script = new URL("../neon-census.ts", import.meta.url).pathname;
    // Port 1 on localhost: any connection attempt fails, so success proves no connect.
    const env = { ...process.env, BULLS_TARGET_URL: "postgresql://u:p@127.0.0.1:1/db" };
    const dry = Bun.spawnSync(["bun", "run", script, "load", `--in=${file}`], { env });
    expect(dry.exitCode).toBe(0);
    expect(dry.stdout.toString()).toContain('"write":"dry-run"');
    const refused = Bun.spawnSync(["bun", "run", script, "load", `--in=${file}`, "--execute"], { env });
    expect(refused.exitCode).toBe(1);
    expect(refused.stderr.toString()).toContain("--expect-supabase-ref");
  });
});
