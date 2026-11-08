/**
 * Opt-in, owned PostgreSQL proof for PR B.  It never connects unless
 * SESSION_INGEST_TEST_PG_URL names a disposable server whose role can create a
 * database.  The suite creates/drops its own database and applies 043700 through
 * the real migration chain; no deployed database or migration apply is involved.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { bufferedParityProofCurrent, bufferedVisibleAfterFetch } from "../buffered-privacy.ts";
import { causal, gateInput, parity, parityState, root, type CausalCase, type ParityCase } from "./buffered-privacy-fixture-support.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
const sql = (path: string) => readFileSync(join(root, path), "utf8");

describe.skipIf(!url)("043700 + PR B fixtures on actual PostgreSQL", () => {
  let admin: pg.Client;
  let db: pg.Client;
  let name = "";

  beforeAll(async () => {
    const configured = new URL(url!);
    if (!new Set(["127.0.0.1", "localhost", "::1", "postgres"]).has(configured.hostname))
      throw new Error("PR B real-PG proof refuses a non-local database host");
    admin = new pg.Client({ connectionString: url });
    await admin.connect();
    name = `prb_fixtures_${crypto.randomUUID().replaceAll("-", "")}`;
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(url!);
    target.pathname = `/${name}`;
    db = new pg.Client({ connectionString: target.toString() });
    await db.connect();
    await db.query("SET TIME ZONE 'UTC'; CREATE SCHEMA sessions; CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
    await db.query(sql("supabase/tests/supabase_shim.sql"));
    for (const file of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"])
      await db.query(sql(`supabase/bootstrap/${file}.sql`));
    for (const file of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
      await db.query(sql(`supabase/migrations/${file}`));
    await db.query("CREATE TEMP TABLE prb_release_vectors(kind text NOT NULL, id text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY(kind,id))");
    for (const item of causal.cases) await db.query("INSERT INTO prb_release_vectors VALUES('causal',$1,$2::jsonb)", [item.id, JSON.stringify(item)]);
    for (const item of parity.cases) await db.query("INSERT INTO prb_release_vectors VALUES('parity',$1,$2::jsonb)", [item.id, JSON.stringify(item)]);
  }, 120_000);

  afterAll(async () => {
    if (db) await db.end();
    if (admin && name) await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    if (admin) await admin.end();
  }, 30_000);

  test("043700 returns the original admission witness and current G2 proof with every stage", async () => {
    const { rows } = await db.query<{ result: string }>(
      "SELECT pg_get_function_result('sessions.buffered_session_stages_for(text,text,text)'::regprocedure) AS result",
    );
    const result = rows[0]?.result ?? "";
    expect(result).toContain("source_control_epoch text");
    expect(result).toContain("source_control_version bigint");
    expect(result).toContain("parity_proof jsonb");
  });

  test("all 26 causal vectors round-trip through PostgreSQL jsonb and drive the final post-S3 gate", async () => {
    const { rows } = await db.query<{ payload: CausalCase }>("SELECT payload FROM prb_release_vectors WHERE kind='causal' ORDER BY id");
    expect(rows).toHaveLength(26);
    for (const { payload } of rows)
      expect(bufferedVisibleAfterFetch(gateInput(payload)), payload.id).toBe(payload.required.finalBufferedContentEmitted);
  });

  test("all 16 parity vectors round-trip through PostgreSQL jsonb and drive the G2 predicate", async () => {
    const { rows } = await db.query<{ payload: ParityCase }>("SELECT payload FROM prb_release_vectors WHERE kind='parity' ORDER BY id");
    expect(rows).toHaveLength(16);
    for (const { payload } of rows)
      expect(bufferedParityProofCurrent(parityState(payload)), payload.id).toBe(payload.emitBuffered);
  });
});
