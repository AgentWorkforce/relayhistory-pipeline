import { describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import {
  DIGEST_SQL,
  censusDigest,
  censusLine,
  censusTables,
  loadCensus,
  parseCensusFile,
  produceCensus,
  verifyCensus,
  type CensusRow,
} from "../neon-census.ts";

const CENSUS_DDL = `
CREATE TABLE sessions.session_neon_census (
  org_id text NOT NULL CHECK (char_length(org_id) BETWEEN 1 AND 512),
  workspace_id text NOT NULL CHECK (char_length(workspace_id) BETWEEN 1 AND 512),
  session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 512),
  PRIMARY KEY (org_id, session_id, workspace_id)
);
CREATE TABLE sessions.session_neon_census_seal (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  census_rows bigint NOT NULL CHECK (census_rows >= 0),
  census_sha256 text NOT NULL CHECK (census_sha256 ~ '^[0-9a-f]{64}$'),
  neon_snapshot jsonb NOT NULL,
  sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);`;

async function source(): Promise<PGlite> {
  const pg = new PGlite();
  await pg.exec(`
    CREATE SCHEMA sessions;
    CREATE TABLE sessions.convergence_events (org_id text, workspace_id text, session_id text, event_id text);
    CREATE TABLE sessions.delivery_records (org_id text, workspace_id text, session_id text, body text);
    CREATE TABLE sessions.machines (org_id text, machine_id text);
    INSERT INTO sessions.convergence_events VALUES ('o2','w1','s9','e1'),('o1','w1','s2','e2'),('o1','w1','s2','e3'),('O1','w1','s1','e4');
    INSERT INTO sessions.delivery_records VALUES ('o1','w1','s2','b'),('o1','w2','s2','b'),('o3','w1','é','b');
    INSERT INTO sessions.machines VALUES ('o9','m1');`);
  return pg;
}

async function target(): Promise<PGlite> {
  const pg = new PGlite();
  await pg.exec(`CREATE SCHEMA sessions; ${CENSUS_DDL}`);
  return pg;
}

const SRC = { lsn: "36B/9D2C2D8", ref: "test-branch", seal: null };

describe("neon session census", () => {
  test("covers exactly the sessions tables with all three key columns", async () => {
    const pg = await source();
    expect(await censusTables(pg)).toEqual(["convergence_events", "delivery_records"]);
  });

  test("produce: distinct union, C-ordered, per-table counts, and the JS digest equals the SQL digest after load", async () => {
    const src = await source();
    const { rows, manifest } = await produceCensus(src, SRC);
    expect(rows).toEqual([
      { org_id: "O1", workspace_id: "w1", session_id: "s1" },
      { org_id: "o1", workspace_id: "w1", session_id: "s2" },
      { org_id: "o1", workspace_id: "w2", session_id: "s2" },
      { org_id: "o2", workspace_id: "w1", session_id: "s9" },
      { org_id: "o3", workspace_id: "w1", session_id: "é" },
    ]);
    expect(manifest.census_rows).toBe(5);
    expect(manifest.neon_snapshot.tables).toEqual({ convergence_events: 4, delivery_records: 3 });
    expect(manifest.census_sha256).toBe(censusDigest(rows));

    const tgt = await target();
    const readback = await loadCensus(tgt, rows, manifest, 2);
    expect(readback.census_rows).toBe(5);
    expect(readback.census_sha256).toBe(manifest.census_sha256);
    const sql = await tgt.query<{ sha: string }>(DIGEST_SQL);
    expect(sql.rows[0]!.sha).toBe(manifest.census_sha256);
    const seal = await tgt.query<{ neon_snapshot: { source_lsn: string } }>("SELECT neon_snapshot FROM sessions.session_neon_census_seal");
    expect(seal.rows[0]!.neon_snapshot.source_lsn).toBe("36B/9D2C2D8");
  });

  test("the file round-trips and verifies", async () => {
    const { rows, manifest } = await produceCensus(await source(), SRC);
    const text = rows.map(censusLine).join("");
    expect(() => verifyCensus(parseCensusFile(text), manifest)).not.toThrow();
  });

  test("an empty census still seals with the sha256 of the empty string", async () => {
    const src = new PGlite();
    await src.exec("CREATE SCHEMA sessions; CREATE TABLE sessions.t (org_id text, workspace_id text, session_id text);");
    const { rows, manifest } = await produceCensus(src, SRC);
    expect(manifest.census_sha256).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    const readback = await loadCensus(await target(), rows, manifest);
    expect(readback.census_rows).toBe(0);
  });

  test("load refuses an existing seal or existing rows, and writes nothing", async () => {
    const { rows, manifest } = await produceCensus(await source(), SRC);
    const tgt = await target();
    await loadCensus(tgt, rows, manifest);
    await expect(loadCensus(tgt, rows, manifest)).rejects.toThrow("seal already exists");
    const tgt2 = await target();
    await tgt2.exec("INSERT INTO sessions.session_neon_census VALUES ('x','y','z')");
    await expect(loadCensus(tgt2, rows, manifest)).rejects.toThrow("census rows already exist");
    const n = await tgt2.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_neon_census_seal");
    expect(n.rows[0]!.n).toBe(0);
  });

  test("a tampered file or manifest is refused before any write", async () => {
    const { rows, manifest } = await produceCensus(await source(), SRC);
    const tgt = await target();
    const dropped = rows.slice(1);
    await expect(loadCensus(tgt, dropped, manifest)).rejects.toThrow("row count differs");
    const swapped = [rows[1]!, rows[0]!, ...rows.slice(2)];
    expect(() => verifyCensus(swapped, manifest)).toThrow("not strictly sorted");
    expect(() => verifyCensus(rows, { ...manifest, census_sha256: "0".repeat(64) })).toThrow("digest differs");
    const n = await tgt.query<{ n: number }>("SELECT count(*)::int AS n FROM sessions.session_neon_census");
    expect(n.rows[0]!.n).toBe(0);
  });

  test("keys with a tab or newline are refused (ambiguous encoding)", () => {
    const bad: CensusRow = { org_id: "o\t1", workspace_id: "w", session_id: "s" };
    expect(() => censusLine(bad)).toThrow("tab or newline");
    expect(() => censusLine({ org_id: "o", workspace_id: "w\n", session_id: "s" })).toThrow("tab or newline");
  });
});
