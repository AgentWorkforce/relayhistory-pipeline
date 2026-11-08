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
import { causal, consumerVisible, parity, parityVisible, root, type CausalCase, type ParityCase } from "./buffered-privacy-fixture-support.ts";

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

  async function contract(name: string) {
    const { rows } = await db.query<{ args: string; result: string }>(`
      SELECT pg_get_function_arguments(p.oid) AS args, pg_get_function_result(p.oid) AS result
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE n.nspname='sessions' AND p.proname=$1`, [name]);
    expect(rows, name).toHaveLength(1);
    expect(rows[0]!.args, `${name}: p_head_kind`).toContain("p_head_kind text");
    expect(rows[0]!.args, `${name}: p_privacy_baseline_sha256`).toContain("p_privacy_baseline_sha256 text");
    return rows[0]!;
  }

  test("PR A seam 1: owner_buffered_read_scope binds v5.3 authority and mandatory G2", async () => {
    const fn = await contract("owner_buffered_read_scope");
    expect(fn.result).toContain("gate_open boolean");
    expect(fn.result).toContain("admission_epoch text");
    expect(fn.result).toContain("admission_version bigint");
    const proof = await db.query<{ name: string | null }>(
      "SELECT to_regclass('sessions.owner_buffered_parity')::text AS name",
    );
    // v5.3 authorizes the table name and semantics, but leaves columns/PK/CAS to the migration owner.
    expect(proof.rows[0]?.name).toBe("sessions.owner_buffered_parity");
  });

  test("PR A seam 2: buffered_sessions_for returns the immutable admission witness", async () => {
    const fn = await contract("buffered_sessions_for");
    expect(fn.result).toContain("admission_epoch text");
    expect(fn.result).toContain("admission_version text");
  });

  test("PR A seam 3: buffered_session_stages_for returns witness and full group ordinals", async () => {
    const fn = await contract("buffered_session_stages_for");
    expect(fn.result).toContain("group_ordinals jsonb");
    expect(fn.result).toContain("admission_epoch text");
    expect(fn.result).toContain("admission_version text");
  });

  test("all 26 causal vectors round-trip through PostgreSQL jsonb and drive the final post-S3 gate", async () => {
    const { rows } = await db.query<{ payload: CausalCase }>("SELECT payload FROM prb_release_vectors WHERE kind='causal' ORDER BY id");
    expect(rows).toHaveLength(26);
    for (const { payload } of rows)
      expect(consumerVisible(payload), payload.id).toBe(payload.required.finalBufferedContentEmitted);
  });

  test("all 16 parity vectors round-trip through PostgreSQL jsonb and drive the G2 predicate", async () => {
    const { rows } = await db.query<{ payload: ParityCase }>("SELECT payload FROM prb_release_vectors WHERE kind='parity' ORDER BY id");
    expect(rows).toHaveLength(16);
    for (const { payload } of rows)
      expect(parityVisible(payload), payload.id).toBe(payload.emitBuffered);
  });
});
