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

  test("043700 exposes the concrete PR B authority inputs, admission witness, and G2 table", async () => {
    const { rows } = await db.query<{ proname: string; args: string; result: string }>(`
      SELECT p.proname, pg_get_function_arguments(p.oid) AS args, pg_get_function_result(p.oid) AS result
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE n.nspname='sessions' AND p.proname IN
         ('owner_buffered_read_scope','buffered_sessions_for','buffered_session_stages_for')`);
    for (const name of ["owner_buffered_read_scope", "buffered_sessions_for", "buffered_session_stages_for"]) {
      const fn = rows.find((row) => row.proname === name);
      expect(fn, name).toBeDefined();
      expect(fn!.args, `${name}: p_head_kind`).toContain("p_head_kind text");
      expect(fn!.args, `${name}: p_privacy_baseline_sha256`).toContain("p_privacy_baseline_sha256 text");
    }
    const list = rows.find((row) => row.proname === "buffered_sessions_for")!.result;
    const stages = rows.find((row) => row.proname === "buffered_session_stages_for")!.result;
    for (const result of [list, stages]) {
      expect(result).toContain("admission_epoch text");
      expect(result).toContain("admission_version bigint");
    }
    expect(stages).toContain("group_ordinals bigint[]");

    const proofColumns = (await db.query<{ column_name: string }>(`
      SELECT column_name FROM information_schema.columns
       WHERE table_schema='sessions' AND table_name='owner_buffered_parity'`)).rows.map((row) => row.column_name);
    for (const column of ["org_id", "workspace_id", "user_id", "cutover_epoch", "head_epoch", "head_version",
      "head_digest", "head_kind", "no_head", "deletion_set_digest", "reset_set_digest",
      "target_lineage_digest", "target_generation", "privacy_baseline_sha256"])
      expect(proofColumns, column).toContain(column);
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
