/**
 * supabase/bootstrap/session_read_api_roles.sql: the inbound-membership predicate (anyone granted
 * the role could SET ROLE into it) for the two admission roles, bulls96_analysis_admit and
 * bulls96_digest_narrative, exactly as for bulls96_read_api. PGlite (PostgreSQL 16+), real roles,
 * the file's own SQL text:
 *   - drift guard: the inbound predicate is byte-identical in all three DO blocks;
 *   - PG16+: any inbound member refuses 55000 and changes nothing, unless it is the exact Supabase
 *     creator edge (member postgres, grantor the superuser supabase_admin, ADMIN without INHERIT
 *     or SET) with hoopdb.db_mode=supabase bound;
 *   - PG15 and older (the version read substituted, the only change): any inbound member refuses;
 *   - the positive creator edge, modelled as role-proof.ts models Supabase: the bootstrap
 *     superuser renamed supabase_admin and a non-superuser CREATEROLE `postgres` that runs the
 *     file, so PostgreSQL itself adds the edge; and every single mutation of it refuses.
 * The same refusal on real PostgreSQL 15/16/17 is in scripts/pg-proof/role-proof.ts.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";

const root = join(import.meta.dir, "../../..");
const BOOT = readFileSync(join(root, "supabase/bootstrap/session_read_api_roles.sql"), "utf8");
const ROLES = ["bulls96_analysis_admit", "bulls96_digest_narrative"] as const;
const VERSION_READ = "current_setting('server_version_num')::int";
/** The file with the server version read pinned to a PostgreSQL 15 value (pre-16 branch). */
const BOOT_PG15 = BOOT.replaceAll(VERSION_READ, "150000");

/** The file's DO blocks, in order; [1] and [2] are the admission roles. */
const blocks = BOOT.split(/^(?=DO \$)/m).slice(1);
const blockFor = (role: string) => {
  const found = blocks.filter((b) => b.includes(`v_name text := '${role}';`));
  if (found.length !== 1) throw new Error(`expected one DO block for ${role}`);
  return found[0]!;
};
/** The inbound predicate as the bulls96_read_api block states it (its 16 lines, verbatim). */
const INBOUND = (() => {
  const b = blockFor("bulls96_read_api");
  const start = b.indexOf("  IF v_version >= 160000 THEN\n");
  const tail = "  ELSIF EXISTS (SELECT 1 FROM pg_auth_members WHERE roleid = r.oid) THEN\n";
  const t = b.indexOf(tail, start);
  const end = b.indexOf("  END IF;\n", t) + "  END IF;\n".length;
  if (start < 0 || t < 0) throw new Error("inbound predicate not found in the read_api block");
  return b.slice(start, end);
})();

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

type Err = { code?: string; message?: string };
const outcome = (p: Promise<unknown>) => p.then(() => "ok", (e: Err) => `${e.code} ${e.message}`);
const unexpected = (role: string) => `55000 bootstrap ${role}: the role has an unexpected member`;

/** Run sql in one transaction with hoopdb.db_mode bound (as migrate's bindDbMode does) or unset. */
async function run(db: PGlite, sql: string, mode?: string): Promise<string> {
  await db.exec("BEGIN");
  try {
    if (mode !== undefined) await db.query("SELECT set_config('hoopdb.db_mode', $1, true)", [mode]);
    await db.exec(sql);
    await db.exec("COMMIT");
    return "ok";
  } catch (e) {
    await db.exec("ROLLBACK");
    return `${(e as Err).code} ${(e as Err).message}`;
  }
}

/** Every membership edge into or out of the bootstrap's roles, and their attributes. */
async function catalog(db: PGlite): Promise<unknown[]> {
  return (await db.query(
    `SELECT r.rolname, r.rolcanlogin, r.rolinherit, r.rolsuper, r.rolbypassrls,
            array(SELECT u.rolname || '<' || g.rolname || '>' || x.rolname || ':' || a.admin_option || a.inherit_option || a.set_option
                    FROM pg_auth_members a JOIN pg_roles u ON u.oid = a.member JOIN pg_roles g ON g.oid = a.roleid
                    JOIN pg_roles x ON x.oid = a.grantor
                   WHERE a.roleid = r.oid OR a.member = r.oid ORDER BY 1) AS edges
       FROM pg_roles r WHERE r.rolname IN ('bulls96_read_api', 'bulls96_analysis_admit', 'bulls96_digest_narrative')
      ORDER BY 1`,
  )).rows;
}

/** A plain cluster: the stock bootstrap superuser `postgres` ran the file. */
async function plain(): Promise<PGlite> {
  const db = new PGlite();
  open.push(db);
  expect(await run(db, BOOT)).toBe("ok");
  await db.exec("CREATE ROLE probe_member LOGIN NOINHERIT; CREATE ROLE probe_parent NOLOGIN");
  return db;
}

/**
 * Supabase PG16+: the bootstrap superuser (OID 10) is supabase_admin and `postgres` is a
 * non-superuser CREATEROLE role. `postgres` runs the file with hoopdb.db_mode=supabase, so
 * PostgreSQL adds each role's creator edge itself. The session is left as `postgres`.
 */
async function supabase(): Promise<PGlite> {
  const db = new PGlite();
  open.push(db);
  await db.exec("CREATE ROLE bootstrap_super SUPERUSER NOLOGIN; CREATE ROLE probe_member LOGIN NOINHERIT");
  await db.exec("SET SESSION AUTHORIZATION bootstrap_super");
  await db.exec("ALTER ROLE postgres RENAME TO supabase_admin");
  await db.exec("CREATE ROLE postgres LOGIN CREATEROLE");
  await db.exec("SET SESSION AUTHORIZATION postgres");
  expect(await run(db, BOOT, "supabase")).toBe("ok");
  return db;
}
/** Run privileged statements as the Supabase bootstrap superuser, then return to `postgres`. */
async function asSupabaseAdmin(db: PGlite, sql: string): Promise<void> {
  await db.exec("SET SESSION AUTHORIZATION supabase_admin");
  try {
    await db.exec(sql);
  } finally {
    await db.exec("SET SESSION AUTHORIZATION postgres");
  }
}

describe("the inbound predicate is one text in all three blocks", () => {
  test("three DO blocks, each with the read_api block's inbound predicate verbatim and its loop variable", () => {
    expect(blocks.map((b) => /v_name text := '([a-z0-9_]+)';/.exec(b)?.[1])).toEqual([
      "bulls96_read_api",
      ...ROLES,
    ]);
    expect(INBOUND).toContain("m.rolname = 'postgres' AND m.grantor = 'supabase_admin' AND m.grantor_super");
    expect(INBOUND).toContain("m.admin_option AND NOT m.inherit_option AND NOT m.set_option");
    expect(BOOT.split(INBOUND).length - 1).toBe(3);
    for (const b of blocks) {
      expect(b.split(INBOUND).length - 1).toBe(1);
      expect(b).toContain("\n  m record;\n");
      // Outbound first, then inbound, then the block ends.
      expect(b.indexOf("WHERE member = r.oid")).toBeLessThan(b.indexOf(INBOUND));
      expect(b.slice(b.indexOf(INBOUND) + INBOUND.length)).toMatch(/^END\n\$[a-z_]+\$;\n/);
    }
    // The PG15 substitution touches only the three version reads.
    expect(BOOT.split(VERSION_READ).length - 1).toBe(3);
  });
});

describe.each(ROLES)("%s: inbound members", (role) => {
  test("PG16+: any inbound member refuses 55000 and changes nothing; revoked, the rerun passes", async () => {
    const db = await plain();
    for (const grant of [
      `GRANT ${role} TO probe_member`,
      // Even a non-assumable ADMIN-only edge: not the creator edge (member, grantor, mode).
      `GRANT ${role} TO probe_member WITH ADMIN TRUE, INHERIT FALSE, SET FALSE`,
    ]) {
      await db.exec(grant);
      const before = await catalog(db);
      for (const mode of [undefined, "supabase", "selfhosted"]) {
        expect([grant, mode, await run(db, blockFor(role), mode)]).toEqual([grant, mode, unexpected(role)]);
        expect([grant, mode, await run(db, BOOT, mode)]).toEqual([grant, mode, unexpected(role)]);
      }
      expect(await catalog(db)).toEqual(before);
      await db.exec(`REVOKE ${role} FROM probe_member`);
      expect(await run(db, BOOT)).toBe("ok");
    }
  }, 60_000);

  test("PG15 and older: any inbound member refuses 55000, whatever its options or the mode", async () => {
    const db = await plain();
    expect(await run(db, BOOT_PG15)).toBe("ok");
    for (const grant of [
      `GRANT ${role} TO probe_member`,
      `GRANT ${role} TO probe_member WITH ADMIN TRUE, INHERIT FALSE, SET FALSE`,
    ]) {
      await db.exec(grant);
      for (const mode of [undefined, "supabase"])
        expect([grant, mode, await run(db, BOOT_PG15, mode)]).toEqual([grant, mode, unexpected(role)]);
      await db.exec(`REVOKE ${role} FROM probe_member`);
      expect(await run(db, BOOT_PG15, "supabase")).toBe("ok");
    }
  }, 60_000);

  test("the outbound refusal is unchanged (a member of another role, 55000)", async () => {
    const db = await plain();
    await db.exec(`GRANT probe_parent TO ${role}`);
    expect(await run(db, BOOT)).toBe(`55000 bootstrap ${role}: the role is a member of another role`);
  }, 60_000);
});

describe("Supabase PG16+ creator edge (bootstrap superuser supabase_admin, non-superuser CREATEROLE postgres)", () => {
  test("postgres creates all three roles with exactly the creator edge; reruns pass and change nothing", async () => {
    const db = await supabase();
    const after = await catalog(db);
    expect(after).toEqual(
      ["bulls96_analysis_admit", "bulls96_digest_narrative", "bulls96_read_api"].map((rolname) => ({
        rolname, rolcanlogin: false, rolinherit: false, rolsuper: false, rolbypassrls: false,
        edges: [`postgres<${rolname}>supabase_admin:truefalsefalse`],
      })),
    );
    expect(await run(db, BOOT, "supabase")).toBe("ok");
    expect(await run(db, BOOT, "supabase")).toBe("ok");
    expect(await catalog(db)).toEqual(after);
  }, 60_000);

  // Each case breaks exactly one term of the predicate.
  const mutations: [string, (role: string) => string, string | undefined][] = [
    ["hoopdb.db_mode unset", () => "", undefined],
    ["hoopdb.db_mode not supabase", () => "", "selfhosted"],
    ["inherit_option true", (r) => `GRANT ${r} TO postgres WITH INHERIT TRUE GRANTED BY supabase_admin`, "supabase"],
    ["set_option true", (r) => `GRANT ${r} TO postgres WITH SET TRUE GRANTED BY supabase_admin`, "supabase"],
    ["admin_option false", (r) => `REVOKE ADMIN OPTION FOR ${r} FROM postgres GRANTED BY supabase_admin`, "supabase"],
    // The same edge, but its grantor (the bootstrap superuser) is no longer named supabase_admin.
    // (PostgreSQL 16+ refuses GRANTED BY any other superuser lacking its own ADMIN edge, and the
    // bootstrap superuser cannot drop SUPERUSER, so grantor_super false is not modelled here.)
    ["grantor not named supabase_admin", () => "RENAME", "supabase"],
    ["an extra member with the creator edge's options",
      (r) => `GRANT ${r} TO probe_member WITH ADMIN TRUE, INHERIT FALSE, SET FALSE GRANTED BY supabase_admin`, "supabase"],
  ];
  for (const role of ROLES)
    for (const [name, mutate, mode] of mutations)
      test(`${role}: ${name} refuses 55000 and changes nothing`, async () => {
        const db = await supabase();
        const sql = mutate(role);
        if (sql === "RENAME") {
          await db.exec("SET SESSION AUTHORIZATION bootstrap_super");
          await db.exec("ALTER ROLE supabase_admin RENAME TO platform_admin");
          await db.exec("SET SESSION AUTHORIZATION postgres");
        } else if (sql) await asSupabaseAdmin(db, sql);
        const before = await catalog(db);
        expect(await run(db, blockFor(role), mode)).toBe(unexpected(role));
        expect(await catalog(db)).toEqual(before);
        // The other admission role, still on its exact creator edge, passes under supabase mode
        // (except after the rename, which changes its grantor's name too).
        const other = ROLES.find((r) => r !== role)!;
        expect(await run(db, blockFor(other), "supabase")).toBe(sql === "RENAME" ? unexpected(other) : "ok");
      }, 60_000);
});
