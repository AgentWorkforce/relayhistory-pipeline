import { afterEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DIGEST_SQL,
  LOAD_LOCK_SQL,
  SOURCE_STATE_SQL,
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

const opened: PGlite[] = [];
function openPglite(): PGlite {
  const pg = new PGlite();
  opened.push(pg);
  return pg;
}

afterEach(async () => {
  await Promise.all(opened.splice(0).map((pg) => (pg.closed ? undefined : pg.close())));
});

async function source(): Promise<PGlite> {
  const pg = openPglite();
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
  const pg = openPglite();
  await pg.exec(`CREATE SCHEMA sessions; ${await censusDdl()}`);
  return pg;
}

/** Synthetic LSNs only (the repo is public). */
const NEON: SourceClaim = { lsn: "1/0", ref: "test-branch", seal: null, urlSupabaseRef: null };

/** PGlite is never in recovery; stand in a hot standby replayed to `replay` by answering SOURCE_STATE_SQL. */
function standby(db: PGlite, replay = "2/0", target = false): Queryable {
  return {
    query: (sql: string, params?: unknown[]) =>
      sql === SOURCE_STATE_SQL
        ? (Promise.resolve({ rows: [{ rec: true, lsn: replay, target }] }) as never)
        : (db.query(sql, params) as never),
  };
}

describe("neon session census", () => {
  test("covers base tables with org_id + session_id; views are reported, not read", async () => {
    const pg = await source();
    expect(await censusTables(pg)).toEqual(["convergence_events", "delivery_records", "no_ws", "session_timing_cache"]);
  });

  test("produce: distinct union, C-ordered, NULL/absent workspace kept, unkeyed counted; JS digest == SQL digest after load", async () => {
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
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
    expect(s.in_recovery).toBe(true);
    expect(s.replayed_lsn).toBe("2/0");
    const tgt = await target();
    const readback = await loadCensus(tgt, rows, manifest, 2);
    expect(readback.census_rows).toBe(7);
    expect((await tgt.query<{ sha: string }>(DIGEST_SQL)).rows[0]!.sha).toBe(manifest.census_sha256);
  });

  test("P2-1: only a hot standby at or after the claimed cut is accepted; Supabase, a pipeline target and 0/0 are refused", async () => {
    const pg = await source();
    await expect(produceCensus(pg, NEON)).rejects.toThrow("not a hot standby"); // real PGlite: not in recovery
    await expect(produceCensus(standby(pg, "1/0"), { ...NEON, lsn: "2/0" })).rejects.toThrow("cut BEFORE");
    await expect(produceCensus(standby(pg), { ...NEON, urlSupabaseRef: "abcdefghijklmnopqrst" })).rejects.toThrow("Supabase URL");
    await expect(produceCensus(standby(pg, "2/0", true), NEON)).rejects.toThrow("pipeline target");
    await expect(produceCensus(standby(pg), { ...NEON, lsn: "0/0" })).rejects.toThrow("above 0/0");
    const ok = await produceCensus(standby(pg, "3/0"), NEON);
    expect(ok.manifest.neon_snapshot.replayed_lsn).toBe("3/0");
    expect(ok.manifest.neon_snapshot.in_recovery).toBe(true);
    expect(lsnCompare("1/FFFFFFFF", "2/0")).toBe(-1);
    expect(lsnCompare("10/0", "F/FFFFFFFF")).toBe(1);
  });

  test("P2-1: the source state is the snapshot's first statement after BEGIN and row_security = off", async () => {
    const seen: string[] = [];
    const inner = standby(await source());
    const spy: Queryable = { query: (sql, params) => { seen.push(sql); return inner.query(sql, params) as never; } };
    await produceCensus(spy, NEON);
    expect(seen.slice(0, 3)).toEqual(["BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", "SET LOCAL row_security = off", SOURCE_STATE_SQL]);
  });

  test("P2-1: an RLS-filtered read by a non-owner role errors instead of shrinking the census", async () => {
    const pg = await source();
    await pg.exec(`CREATE ROLE census_reader; GRANT USAGE ON SCHEMA sessions TO census_reader;
      GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO census_reader;
      ALTER TABLE sessions.convergence_events ENABLE ROW LEVEL SECURITY;
      CREATE POLICY only_o1 ON sessions.convergence_events FOR SELECT TO census_reader USING (org_id = 'o1');
      SET ROLE census_reader;`);
    await expect(produceCensus(standby(pg), NEON)).rejects.toThrow(/row-level security/i);
  });

  test("P3-7: a sessions partition whose parent is outside sessions is refused", async () => {
    const pg = await source();
    await pg.exec(`CREATE SCHEMA other; CREATE TABLE other.parent (org_id text, workspace_id text, session_id text) PARTITION BY LIST (org_id);
      CREATE TABLE sessions.child PARTITION OF other.parent FOR VALUES IN ('o7');`);
    await expect(produceCensus(standby(pg), NEON)).rejects.toThrow("parent outside sessions");
  });

  test("P3-3: a sessions foreign table with census keys is refused (checked via the discovery query)", async () => {
    const fake: Queryable = {
      async query<T>(sql: string) {
        if (sql.startsWith("BEGIN") || sql.startsWith("ROLLBACK") || sql.startsWith("SET LOCAL")) return { rows: [] as T[] };
        if (sql === SOURCE_STATE_SQL) return { rows: [{ rec: true, lsn: "2/0", target: false }] as T[] };
        if (sql.includes("pg_inherits")) return { rows: [] as T[] };
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
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
    await loadCensus(spy, rows, manifest);
    expect(seen.slice(0, 3)).toEqual(["BEGIN", ...LOAD_LOCK_SQL]);
  });

  test("P3-8: UTF-8 byte order differs from JS UTF-16 order, and backslash/quote keys; JS digest == SQL digest", async () => {
    const pg = openPglite();
    await pg.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.t (org_id text, workspace_id text, session_id text);");
    // U+FF61 sorts after U+1F600 in UTF-16 (0xFF61 > 0xD83D) but before it in UTF-8 (EF.. < F0..).
    await pg.query("INSERT INTO sessions.t VALUES ($1,'w','s1'),($2,'w','s2'),('o\\\\x','w''q','s\"3')", ["\u{1F600}", "｡"]);
    const { rows, manifest } = await produceCensus(standby(pg), NEON);
    expect(rows.map((r) => r.org_id)).toEqual(["o\\\\x", "｡", "\u{1F600}"]);
    const tgt = await target();
    await loadCensus(tgt, rows, manifest);
    expect((await tgt.query<{ sha: string }>(DIGEST_SQL)).rows[0]!.sha).toBe(manifest.census_sha256);
  });

  test("the file round-trips and an empty census seals with sha256('')", async () => {
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
    expect(() => verifyCensus(parseCensusFile(rows.map(censusLine).join("")), manifest)).not.toThrow();
    const empty = openPglite();
    await empty.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.t (org_id text, workspace_id text, session_id text);");
    const e = await produceCensus(standby(empty), NEON);
    expect(e.manifest.census_sha256).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect((await loadCensus(await target(), e.rows, e.manifest)).census_rows).toBe(0);
  });

  test("load refuses an existing seal or rows and writes nothing; tampering is refused before any write", async () => {
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
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

  test("P3-8: a readback mismatch or failure after COMMIT says COMMITTED and never suggests a retry", async () => {
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
    const tgt = await target();
    let committed = false;
    const lying: Queryable = {
      query: (sql: string, params?: unknown[]) => {
        if (sql === "COMMIT") committed = true;
        if (committed && sql.includes("FROM sessions.session_neon_census_seal WHERE singleton"))
          return Promise.resolve({ rows: [{ census_rows: "1", census_sha256: "0".repeat(64), sealed_at: "x" }] }) as never;
        return tgt.query(sql, params) as never;
      },
    };
    await expect(loadCensus(lying, rows, manifest)).rejects.toThrow("COMMITTED; readback mismatch");
    const tgt2 = await target();
    let done = false;
    const failing: Queryable = {
      query: (sql: string, params?: unknown[]) => {
        if (sql === "COMMIT") done = true;
        if (done && sql.includes("WHERE singleton")) return Promise.reject(new Error("connection lost")) as never;
        return tgt2.query(sql, params) as never;
      },
    };
    await expect(loadCensus(failing, rows, manifest)).rejects.toThrow("COMMITTED; readback failed");
  });

  test("keys with a tab or newline are refused", () => {
    const bad: CensusRow = { org_id: "o\t1", workspace_id: "w", session_id: "s" };
    expect(() => censusLine(bad)).toThrow("tab or newline");
    expect(() => censusLine({ org_id: "o", workspace_id: "w\n", session_id: "s" })).toThrow("tab or newline");
  });

  test("CLI: load without --execute never connects; --execute without --expect-supabase-ref is refused", async () => {
    const { rows, manifest } = await produceCensus(standby(await source()), NEON);
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
    const produce = Bun.spawnSync(["bun", "run", script, "produce", `--out=${file}`, "--source-lsn=1/0", "--source-ref=t"],
      { env: { ...process.env, CENSUS_SOURCE_URL: "postgresql://u:p@127.0.0.1:1/db" } });
    expect(produce.exitCode).toBe(1);
    expect(produce.stderr.toString()).toContain("already exists");
  });
});
