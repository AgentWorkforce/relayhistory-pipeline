import { afterEach, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";

const sql = (name: string) => readFileSync(join(import.meta.dir, "../migrations", name), "utf8");
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map(db => db.close())); });
const target = ["o", "w", "claude", "s", "m"];
const keys = [target, ["o", "w", "claude", "s", "m2"], ["o", "w", "codex", "s", "m"],
  ["o", "w2", "claude", "s", "m"], ["o2", "w", "claude", "s", "m"], ["o", "w", "claude", "s2", "m"]];
const where = "org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5";
const trace = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";

async function setup() {
  const db = new PGlite(); open.push(db);
  await db.exec("CREATE SCHEMA sessions; CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;");
  await db.exec(sql("0002_session_ext_and_summaries.sql"));
  await db.exec(sql("0004_summary_pointers.sql"));
  await db.exec(`CREATE TABLE sessions.session_index (
    org_id text, workspace_id text, harness text, session_id text, machine_id text,
    deleted_at timestamptz, revision bigint DEFAULT 1,
    PRIMARY KEY(org_id,workspace_id,harness,session_id,machine_id));`);
  for (const key of keys) {
    await db.query("INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id) VALUES($1,$2,$3,$4,$5)", key);
    for (const kind of ["short", "long"])
      await db.query(`INSERT INTO sessions.session_summaries
        (org_id,workspace_id,harness,session_id,machine_id,kind,trace_id,revision,content_sha256,status,object_key,object_sha256,key_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,1,$8,'ready','fixture/object',$8,'fixture')`, [...key, kind, trace, "a".repeat(64)]);
    await db.query(`INSERT INTO sessions.session_verdicts(org_id,workspace_id,harness,session_id,machine_id,trace_id,revision,state,rule)
      VALUES($1,$2,$3,$4,$5,$6,1,'done','fixture')`, [...key, trace]);
  }
  await db.exec(sql("0005_derived_pointer_erasure.sql"));
  return db;
}
async function counts(db: PGlite, key = target) {
  return [
    (await db.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.session_summaries WHERE ${where}`, key)).rows[0]!.n,
    (await db.query<{ n: number }>(`SELECT count(*)::int n FROM sessions.session_verdicts WHERE ${where}`, key)).rows[0]!.n,
  ];
}

test("installation keeps existing rows; tombstone erases only its complete physical tenant key", async () => {
  const db = await setup();
  for (const key of keys) expect(await counts(db, key)).toEqual([2, 1]);
  await db.query(`UPDATE sessions.session_index SET revision=2 WHERE ${where}`, target);
  expect(await counts(db)).toEqual([2, 1]);
  await db.query(`UPDATE sessions.session_index SET deleted_at=now() WHERE ${where}`, target);
  expect(await counts(db)).toEqual([0, 0]);
  for (const key of keys.slice(1)) expect(await counts(db, key)).toEqual([2, 1]);
  await db.query(`UPDATE sessions.session_index SET deleted_at=now() WHERE ${where}`, target);
  expect(await counts(db)).toEqual([0, 0]);
});

test("physical deletion and scoped bulk reset cleanup are atomic with the index operation", async () => {
  const db = await setup();
  await db.exec("BEGIN");
  await db.query(`DELETE FROM sessions.session_index WHERE ${where}`, target);
  expect(await counts(db)).toEqual([0, 0]);
  await db.exec("ROLLBACK");
  expect(await counts(db)).toEqual([2, 1]);
  await db.query("DELETE FROM sessions.session_index WHERE org_id='o' AND workspace_id='w'");
  for (const key of keys.filter(k => k[0] === "o" && k[1] === "w")) expect(await counts(db, key)).toEqual([0, 0]);
  for (const key of keys.filter(k => k[0] !== "o" || k[1] !== "w")) expect(await counts(db, key)).toEqual([2, 1]);
});

test("trigger-only definer cleans up for a pointer writer without granting direct erasure access", async () => {
  const db = await setup();
  await db.exec(`CREATE ROLE pointer_writer;
    GRANT USAGE ON SCHEMA sessions TO pointer_writer;
    GRANT SELECT, UPDATE ON sessions.session_index TO pointer_writer;`);
  for (const role of ["pointer_writer", "service_role", "authenticated", "anon"])
    expect((await db.query<{ ok: boolean }>("SELECT has_function_privilege($1,'sessions.forget_pointer_derivatives()','EXECUTE') ok", [role])).rows[0]!.ok).toBe(false);
  await db.exec("SET ROLE pointer_writer");
  await db.query(`UPDATE sessions.session_index SET deleted_at=now() WHERE ${where}`, target);
  await db.exec("RESET ROLE");
  expect(await counts(db)).toEqual([0, 0]);
});
