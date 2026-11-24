/**
 * supabase/bootstrap/session_read_api_role.sql on PGlite with the full migration plan:
 * - three transactions, each with lock_timeout 3s right after BEGIN; no transaction names both
 *   sessions.session_index and sessions.session_directory; DROP POLICY only in a policy step's
 *   mismatch branch (after its early RETURN);
 * - a fresh database: the file runs, session_read_api_role_verify.sql passes, both policies have the
 *   wanted definition; a second run keeps both policies (same pg_policy.oid: no drop, no create);
 * - lock scope (pg_locks of this backend before COMMIT): a matching policy takes no
 *   AccessExclusiveLock on either table; a missing or different one takes it on its own table only;
 * - every kind of different policy (USING, roles, RESTRICTIVE, command, WITH CHECK) is replaced:
 *   the wanted definition with a new oid;
 * - a run that stopped after transaction 1 is completed by the next run.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { btree_gin } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/btree_gin.js";
import { pg_trgm } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/contrib/pg_trgm.js";
import { runMigrations, type MigrationClient, type MigrationProfile } from "../migrate.ts";

setDefaultTimeout(300_000);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (text: string) => text.split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const RAW = read("supabase/bootstrap/session_read_api_role.sql");
const ROLE = psql(RAW);
const VERIFY = psql(read("supabase/bootstrap/session_read_api_role_verify.sql"));
const LOCAL: MigrationProfile = { mode: "local", shim: true, bootstrap: true, verify: false, runtimeRole: "postgres" };
const INDEX = "sessions.session_index";
const DIRECTORY = "sessions.session_directory";
const WANTED_QUAL =
  "((org_id = current_setting('bulls96_read.org_id'::text, true)) AND (workspace_id = current_setting('bulls96_read.workspace_id'::text, true)))";
const POLICY = { [INDEX]: "session_index_read_api", [DIRECTORY]: "session_directory_read_api" } as const;
type Table = keyof typeof POLICY;

/** The file's transaction blocks, BEGIN through COMMIT inclusive, in order. */
function transactions(text: string): string[] {
  const blocks = [...text.matchAll(/^BEGIN;\n[\s\S]*?^COMMIT;$/gm)].map((m) => m[0]);
  return blocks;
}
const withoutCommit = (block: string) => block.replace(/\nCOMMIT;$/, "\n");

function client(db: PGlite): MigrationClient {
  return { async query<R>(text: string, params?: unknown[]) {
    if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
    const results = await db.exec(text);
    return { rows: (results.at(-1)?.rows ?? []) as R[] };
  } };
}

type PolicyRow = { oid: number; permissive: boolean; cmd: string; roles: string[]; qual: string | null; withcheck: string | null };
async function policy(db: PGlite, table: Table): Promise<PolicyRow | undefined> {
  const r = await db.query<PolicyRow>(
    `SELECT p.oid::int AS oid, p.polpermissive AS permissive, p.polcmd::text AS cmd,
            ARRAY(SELECT CASE WHEN x = 0 THEN 'public' ELSE x::regrole::text END FROM unnest(p.polroles) x ORDER BY 1) AS roles,
            pg_get_expr(p.polqual, p.polrelid) AS qual, pg_get_expr(p.polwithcheck, p.polrelid) AS withcheck
       FROM pg_policy p WHERE p.polrelid = $1::regclass AND p.polname = $2`, [table, POLICY[table]]);
  return r.rows[0];
}
const wanted = (p: PolicyRow | undefined) =>
  ({ permissive: p?.permissive, cmd: p?.cmd, roles: p?.roles, qual: p?.qual, withcheck: p?.withcheck });
const WANTED = { permissive: true, cmd: "r", roles: ["bulls96_read_api"], qual: WANTED_QUAL, withcheck: null };

async function exclusiveLocks(db: PGlite): Promise<string[]> {
  const r = await db.query<{ rel: string }>(
    `SELECT l.relation::regclass::text AS rel FROM pg_locks l
      WHERE l.pid = pg_backend_pid() AND l.mode = 'AccessExclusiveLock' AND l.locktype = 'relation'
        AND l.relation IN ('sessions.session_index'::regclass, 'sessions.session_directory'::regclass)
      ORDER BY 1`);
  return r.rows.map((x) => x.rel);
}
/** Run one transaction block up to (not including) COMMIT, read this backend's locks, roll back. */
async function locksOf(db: PGlite, block: string): Promise<string[]> {
  await db.exec(withoutCommit(block));
  try { return await exclusiveLocks(db); } finally { await db.exec("ROLLBACK"); }
}

describe("session_read_api_role.sql: static shape", () => {
  const blocks = transactions(ROLE);
  test("three transactions, each sets lock_timeout 3s right after BEGIN, and nothing runs outside them", () => {
    expect(blocks.length).toBe(3);
    for (const b of blocks) expect(b.split("\n")[1]).toBe("SET LOCAL lock_timeout = '3s';");
    let rest = ROLE;
    for (const b of blocks) rest = rest.replace(b, "");
    expect(rest.split("\n").filter((l) => l.trim() !== "" && !l.trim().startsWith("--"))).toEqual([]);
    expect(RAW.split("\n").filter((l) => l.startsWith("\\"))).toEqual(["\\set ON_ERROR_STOP 1"]);
  });
  test("no transaction names both tables; transaction 0 names neither", () => {
    for (const b of blocks) expect(b.includes(INDEX) && b.includes(DIRECTORY)).toBe(false);
    expect(blocks[0]!.includes(INDEX) || blocks[0]!.includes(DIRECTORY)).toBe(false);
    expect(blocks[1]!.includes(INDEX)).toBe(true);
    expect(blocks[2]!.includes(DIRECTORY)).toBe(true);
  });
  test("DROP POLICY only in a policy DO block's mismatch branch, after its early RETURN", () => {
    const code = ROLE.replace(/--[^\n]*/g, "");
    expect([...code.matchAll(/DROP POLICY/g)].length).toBe(2);
    expect([...code.matchAll(/CREATE POLICY/g)].length).toBe(2);
    for (const [i, table] of [[1, INDEX], [2, DIRECTORY]] as const) {
      const b = blocks[i]!;
      const doBlock = /^DO (\$[a-z_]+\$)\n([\s\S]*?)^\1;$/m.exec(b);
      expect(doBlock).not.toBeNull();
      const body = doBlock![2]!;
      expect(b.replace(doBlock![0], "")).not.toContain("POLICY");
      const ret = body.indexOf("RETURN;"), drop = body.indexOf(`DROP POLICY IF EXISTS ${POLICY[table]} ON ${table};`);
      expect(ret).toBeGreaterThan(0);
      expect(drop).toBeGreaterThan(ret);
      expect(body.indexOf("CREATE POLICY")).toBeGreaterThan(drop);
    }
  });
});

describe("session_read_api_role.sql on PGlite", () => {
  let db: PGlite;
  const blocks = transactions(ROLE);
  const oids = async () => ({ index: (await policy(db, INDEX))?.oid, directory: (await policy(db, DIRECTORY))?.oid });
  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await runMigrations(client(db), { profile: LOCAL });
    await db.exec(`CREATE ROLE read_api_policy_probe_a NOLOGIN; CREATE ROLE read_api_policy_probe_b NOLOGIN;`);
  });
  afterAll(async () => { await db?.close(); });
  // Each test starts from the state migrate leaves: no read API policy, no directory column grant.
  beforeEach(async () => {
    await db.exec(`DROP POLICY IF EXISTS session_index_read_api ON ${INDEX};
      DROP POLICY IF EXISTS session_directory_read_api ON ${DIRECTORY};
      DO $r$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bulls96_read_api') THEN
        REVOKE SELECT (org_id, workspace_id, harness, session_id, event_projection_ready, event_projection_epoch)
          ON ${DIRECTORY} FROM bulls96_read_api;
      END IF; END $r$;`);
  });

  test("fresh database: the file runs, verify passes, both policies are the wanted ones", async () => {
    expect(await policy(db, INDEX)).toBeUndefined();
    expect(await policy(db, DIRECTORY)).toBeUndefined();
    await db.exec(ROLE);
    await db.exec(VERIFY);
    expect(wanted(await policy(db, INDEX))).toEqual(WANTED);
    expect(wanted(await policy(db, DIRECTORY))).toEqual(WANTED);
  });

  test("a second run keeps both policies (same oid) and verify still passes", async () => {
    await db.exec(ROLE);
    const before = await oids();
    await db.exec(ROLE);
    expect(await oids()).toEqual(before);
    await db.exec(VERIFY);
  });

  test("lock scope: a matching policy takes no AccessExclusiveLock on either table", async () => {
    await db.exec(ROLE);
    expect(await locksOf(db, blocks[0]!)).toEqual([]);
    expect(await locksOf(db, blocks[1]!)).toEqual([]);
    expect(await locksOf(db, blocks[2]!)).toEqual([]);
  });

  test("lock scope: a missing policy locks its own table only", async () => {
    expect(await locksOf(db, blocks[0]!)).toEqual([]);
    expect(await locksOf(db, blocks[1]!)).toEqual([INDEX]);
    expect(await locksOf(db, blocks[2]!)).toEqual([DIRECTORY]);
  });

  test("lock scope: a different policy locks its own table only", async () => {
    await db.exec(ROLE);
    await db.exec(`DROP POLICY session_index_read_api ON ${INDEX};
      CREATE POLICY session_index_read_api ON ${INDEX} AS PERMISSIVE FOR SELECT TO bulls96_read_api USING (true);`);
    expect(await locksOf(db, blocks[1]!)).toEqual([INDEX]);
    expect(await locksOf(db, blocks[2]!)).toEqual([]);
    await db.exec(`DROP POLICY session_directory_read_api ON ${DIRECTORY};
      CREATE POLICY session_directory_read_api ON ${DIRECTORY} AS PERMISSIVE FOR SELECT TO bulls96_read_api USING (true);`);
    expect(await locksOf(db, blocks[2]!)).toEqual([DIRECTORY]);
  });

  const Q = "org_id = current_setting('bulls96_read.org_id', true) AND workspace_id = current_setting('bulls96_read.workspace_id', true)";
  const MISMATCHES: [string, () => string][] = [
    ["USING binds the org only", () => "AS PERMISSIVE FOR SELECT TO bulls96_read_api USING (org_id = current_setting('bulls96_read.org_id', true))"],
    ["TO another role", () => `AS PERMISSIVE FOR SELECT TO read_api_policy_probe_a USING (${Q})`],
    ["TO two roles", () => `AS PERMISSIVE FOR SELECT TO bulls96_read_api, read_api_policy_probe_b USING (${Q})`],
    ["TO PUBLIC", () => `AS PERMISSIVE FOR SELECT TO PUBLIC USING (${Q})`],
    ["RESTRICTIVE", () => `AS RESTRICTIVE FOR SELECT TO bulls96_read_api USING (${Q})`],
    ["FOR ALL", () => `AS PERMISSIVE FOR ALL TO bulls96_read_api USING (${Q})`],
    ["FOR ALL with a WITH CHECK", () => `AS PERMISSIVE FOR ALL TO bulls96_read_api USING (${Q}) WITH CHECK (${Q})`],
  ];
  for (const [name, def] of MISMATCHES) {
    test(`a different policy is replaced (new oid, wanted definition): ${name}`, async () => {
      await db.exec(ROLE);
      for (const table of [INDEX, DIRECTORY] as const) {
        await db.exec(`DROP POLICY ${POLICY[table]} ON ${table}; CREATE POLICY ${POLICY[table]} ON ${table} ${def()};`);
      }
      const before = await oids();
      expect(wanted(await policy(db, INDEX))).not.toEqual(WANTED);
      expect(wanted(await policy(db, DIRECTORY))).not.toEqual(WANTED);
      await db.exec(ROLE);
      const after = await oids();
      expect(after.index).not.toBe(before.index);
      expect(after.directory).not.toBe(before.directory);
      expect(wanted(await policy(db, INDEX))).toEqual(WANTED);
      expect(wanted(await policy(db, DIRECTORY))).toEqual(WANTED);
      await db.exec(VERIFY);
    });
  }

  test("a run that stopped after transaction 1 is completed by the next run", async () => {
    await db.exec(blocks[0]!);
    await db.exec(blocks[1]!);
    const index = (await policy(db, INDEX))?.oid;
    expect(index).toBeDefined();
    expect(await policy(db, DIRECTORY)).toBeUndefined();
    await db.exec(ROLE);
    expect((await policy(db, INDEX))?.oid).toBe(index);
    expect(wanted(await policy(db, DIRECTORY))).toEqual(WANTED);
    await db.exec(VERIFY);
  });

  test("a failed transaction 2 leaves its grant uncommitted with its policy", async () => {
    await db.exec(blocks[0]!);
    await db.exec(blocks[1]!);
    // Transaction 2 fails at its end (after its grant and policy): neither the grant nor the policy commits.
    await expect(db.exec(withoutCommit(blocks[2]!) + "SELECT 1/0;\nCOMMIT;")).rejects.toThrow();
    await db.exec("ROLLBACK").catch(() => undefined);
    expect(await policy(db, DIRECTORY)).toBeUndefined();
    const granted = await db.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_attribute a WHERE a.attrelid = '${DIRECTORY}'::regclass AND a.attnum > 0
          AND NOT a.attisdropped AND has_column_privilege('bulls96_read_api', a.attrelid, a.attnum, 'SELECT')`);
    expect(granted.rows[0]!.n).toBe(0);
    await db.exec(ROLE);
    await db.exec(VERIFY);
  });
});
