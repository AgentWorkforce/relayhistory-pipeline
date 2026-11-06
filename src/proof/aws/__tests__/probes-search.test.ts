/**
 * The proof's direct-SQL probes against the real migrations: the RPC presence
 * probe names the deployed signature, and the rpc-sql search batch carries claims
 * the RPC's token pins accept. In-process PGlite; synthetic data.
 */
import { afterEach, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../../../hoopdb/session-directory-projector.ts";
import { indexStateSql, searchBatchSql } from "../probes.ts";

const root = join(import.meta.dir, "../../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  await db.exec(read("supabase/bootstrap/session_read_api_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read(`supabase/migrations/${file}`));
  return db;
}

/** psql meta-commands are not SQL; the probes only use `\g /dev/null`. */
const asSql = (psql: string) => psql.replaceAll(" \\g /dev/null", ";");

test("the RPC presence probe finds the deployed search RPC", async () => {
  const db = await database();
  const { rows } = await db.query<{ json_build_object: { search_rpc_present: boolean; directory_present: boolean } }>(
    indexStateSql(["s-1"]).replace(/;$/, ""),
  );
  expect(rows[0]!.json_build_object).toMatchObject({ directory_present: true, search_rpc_present: true });
}, 60_000);

test("the rpc-sql search batch passes the RPC's token pins and stays in its tenant", async () => {
  const db = await database();
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ('https://issuer.test/sd', 'session-directory-search')",
  );
  for (const [org, title] of [["org_a", "zqterm alpha"], ["org_b", "zqterm beta"]] as const) {
    await db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
          title, storage_provider, bucket, manifest_key, trace_id, revision)
       VALUES ($1, 'default', 'claude', 's-' || $1, 'm', 'u', now(), now(), $2, 's3', 'b', 'k/' || $1,
               '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1)`,
      [org, title],
    );
    await projectSessionDirectory(db as never, { orgId: org, workspaceId: "default", harness: "claude", sessionId: `s-${org}` });
  }
  const results = await db.exec(asSql(searchBatchSql({ orgId: "org_a", workspaceId: "default" } as never, [{ tag: "own", q: "zqterm" }])));
  const line = results
    .flatMap((r) => r.rows as Array<{ json_build_object?: { tag: string; rows: Array<{ org_id: string }> } }>)
    .find((r) => r.json_build_object?.tag === "own")!.json_build_object!;
  expect(line.rows.map((r) => r.org_id)).toEqual(["org_a"]);
}, 60_000);

test("claims honour a stricter policy lifetime and take iat/exp from one capture", async () => {
  const db = await database();
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience, max_lifetime_seconds) VALUES ('https://issuer.test/sd', 'session-directory-search', 10)",
  );
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        title, storage_provider, bucket, manifest_key, trace_id, revision)
     VALUES ('org_a', 'default', 'claude', 's-a', 'm', 'u', now(), now(), 'zqterm alpha', 's3', 'b', 'k/a',
             '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1)`,
  );
  await projectSessionDirectory(db as never, { orgId: "org_a", workspaceId: "default", harness: "claude", sessionId: "s-a" });
  const sql = asSql(searchBatchSql({ orgId: "org_a", workspaceId: "default" } as never, [{ tag: "own", q: "zqterm" }]));
  // The claim statement alone, then read back what it set.
  const claimStmt = sql.split("\n").slice(1).join("\n").split("SET LOCAL ROLE")[0]!;
  for (let i = 0; i < 20; i += 1) {
    await db.exec(`BEGIN; ${claimStmt}`);
    const c = (await db.query<{ c: { iat: number; exp: number; iss: string; aud: string } }>(
      "SELECT current_setting('request.jwt.claims')::json AS c",
    )).rows[0]!.c;
    await db.exec("ROLLBACK");
    expect(c.exp - c.iat).toBe(10);
    expect([c.iss, c.aud]).toEqual(["https://issuer.test/sd", "session-directory-search"]);
  }
  const results = await db.exec(sql);
  const line = results
    .flatMap((r) => r.rows as Array<{ json_build_object?: { tag: string; rows: Array<{ org_id: string }> } }>)
    .find((r) => r.json_build_object?.tag === "own")!.json_build_object!;
  expect(line.rows.map((r) => r.org_id)).toEqual(["org_a"]);
}, 60_000);
