/**
 * The proof's HoopDb probe must see the V1 search RPC exactly when it is
 * deployed. In-process PGlite only.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
// The root workspace has no PGlite dependency; use the pipeline package's copy (as manifests-migration.test.ts does).
import { PGlite } from "../../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { indexStateSql } from "../probes.ts";

const root = join(import.meta.dir, "../../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const ids = ["01a18f4a-e978-7d04-b07c-4aa8dc28dc65"];
// Each PGlite instance takes about a second on CI; bun's 5 s default is too tight for several per test.
const PGLITE_MS = 60_000;

async function state(db: PGlite) {
  const r = await db.query<{
    json_build_object: {
      directory_present: boolean;
      search_rpc_present: boolean;
    };
  }>(indexStateSql(ids));
  return r.rows[0]!.json_build_object;
}

async function bare(fn?: string) {
  const db = new PGlite();
  await db.exec(
    "CREATE SCHEMA sessions; CREATE TABLE sessions.session_index (org_id text, workspace_id text, session_id text);",
  );
  if (fn) await db.exec(fn);
  return db;
}

const RET =
  "RETURNS TABLE (session_id text) LANGUAGE sql AS $$ SELECT NULL::text WHERE false $$";

describe("search_rpc_present", () => {
  test("no RPC: false", async () => {
    expect((await state(await bare())).search_rpc_present).toBe(false);
  }, PGLITE_MS);
  test("the contract's shape (many trailing defaulted args) is seen; the old (text,text) lookup missed it", async () => {
    const db = await bare(
      `CREATE FUNCTION sessions.search_session_directory(p_workspace_id text DEFAULT NULL, p_query text DEFAULT NULL, p_harness text DEFAULT NULL, p_no_project boolean DEFAULT false, p_since timestamptz DEFAULT NULL, p_limit integer DEFAULT 50) ${RET};`,
    );
    expect((await state(db)).search_rpc_present).toBe(true);
    const old = await db.query<{ v: boolean }>(
      "SELECT to_regprocedure('sessions.search_session_directory(text,text)') IS NOT NULL AS v",
    );
    expect(old.rows[0]!.v).toBe(false);
  }, PGLITE_MS);
  test("a plain (p_workspace_id, p_query) function is seen", async () => {
    expect(
      (
        await state(
          await bare(
            `CREATE FUNCTION sessions.search_session_directory(p_workspace_id text, p_query text) ${RET};`,
          ),
        )
      ).search_rpc_present,
    ).toBe(true);
  }, PGLITE_MS);
  test("wrong leading arguments or a later argument without a default: false", async () => {
    expect(
      (
        await state(
          await bare(
            `CREATE FUNCTION sessions.search_session_directory(p_query text, p_workspace_id text) ${RET};`,
          ),
        )
      ).search_rpc_present,
    ).toBe(false);
    expect(
      (
        await state(
          await bare(
            `CREATE FUNCTION sessions.search_session_directory(p_workspace_id text, p_query text, p_limit integer) ${RET};`,
          ),
        )
      ).search_rpc_present,
    ).toBe(false);
    expect(
      (
        await state(
          await bare(
            `CREATE FUNCTION sessions.search_session_directory(p_workspace_id integer, p_query text) ${RET};`,
          ),
        )
      ).search_rpc_present,
    ).toBe(false);
    expect(
      (
        await state(
          await bare(
            `CREATE FUNCTION public.search_session_directory(p_workspace_id text, p_query text) ${RET};`,
          ),
        )
      ).search_rpc_present,
    ).toBe(false);
  }, PGLITE_MS);
});

describe("after every supabase migration", () => {
  const files = readdirSync(join(root, "supabase/migrations")).sort();
  const defined = files.some((f) =>
    /CREATE (OR REPLACE )?FUNCTION sessions\.search_session_directory\(/.test(
      read(`supabase/migrations/${f}`),
    ),
  );
  test(`the probe reports the RPC exactly when a migration defines it (defined here: ${defined})`, async () => {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
    await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
    for (const f of files) await db.exec(read(`supabase/migrations/${f}`));
    const s = await state(db);
    expect(s.search_rpc_present).toBe(defined);
    // When the directory migrations are present, so is the RPC (#38): both or neither.
    expect(s.search_rpc_present).toBe(s.directory_present);
  }, PGLITE_MS);
});
