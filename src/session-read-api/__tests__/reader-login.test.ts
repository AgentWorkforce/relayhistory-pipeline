/**
 * Reader admission (lambda.ts readerLoginRefusal / assertReaderRole): the read pool's session must be a
 * real login as exactly bulls96_read_api, shaped as supabase/bootstrap/session_read_api_role.sql
 * provisions it and session_read_api_role_verify.sql requires. Pure decision tests over the live rows
 * (PGlite has no real login), the production row shape, the bootstrap's actual role through PGlite, and
 * a SET ROLE negative.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { assertReaderRole, READER_LOGIN_SQL, READER_PG16_SQL, ReaderRoleError, readerLoginRefusal, type ReaderRoleRefusal } from "../lambda.ts";
import { READER_LOGIN_ROW, READER_PG16_ROW, readerLoginAnswer } from "./reader-login-support.ts";

const SUPABASE = { supabase: true };
const OTHER = { supabase: false };
const CREATOR_EDGE = { member: "postgres", grantor: "supabase_admin", grantor_super: true, admin_option: true, inherit_option: false, set_option: false };
const login = (over: Record<string, unknown> = {}) => ({ ...READER_LOGIN_ROW, ...over });
const pg16 = (over: Record<string, unknown> = {}) => ({ ...READER_PG16_ROW, ...over });

describe("readerLoginRefusal (pure)", () => {
  test("the production login shape is admitted: PG17 SCRAM, PG16 cert/md5, PG15 (no system_user), on either provider", () => {
    for (const provider of [SUPABASE, OTHER]) {
      expect(readerLoginRefusal(login(), pg16(), provider)).toBeNull();
      expect(readerLoginRefusal(login({ server_version: 160003 }), pg16({ system_user: "md5:bulls96_read_api" }), provider)).toBeNull();
      expect(readerLoginRefusal(login({ server_version: 160003 }), pg16({ system_user: "cert:bulls96_read_api" }), provider)).toBeNull();
      expect(readerLoginRefusal(login({ server_version: 150008 }), undefined, provider)).toBeNull();
    }
    // The verifier's `rolconfig @>`: other role settings are allowed, the order is not significant.
    expect(readerLoginRefusal(login({ rolconfig: ["search_path=sessions", "default_transaction_read_only=on", "statement_timeout=2s"] }), pg16(), OTHER)).toBeNull();
  });

  test("identity: another login, SET ROLE, a foreign or unauthenticated system_user", () => {
    const cases: Array<[Record<string, unknown>, Record<string, unknown> | undefined, ReaderRoleRefusal]> = [
      [login({ session_user: "postgres" }), pg16(), "reader_role_session_user"],
      [login({ session_user: "search_reader", current_user: "search_reader" }), pg16(), "reader_role_session_user"],
      [login({ current_user: "postgres" }), pg16(), "reader_role_current_user"],
      [login({ role_setting: "bulls96_read_api" }), pg16(), "reader_role_set_role"],
      [login(), pg16({ system_user: null }), "reader_role_system_user"], // trust auth proves nothing
      [login(), pg16({ system_user: "scram-sha-256:postgres" }), "reader_role_system_user"],
      [login(), pg16({ system_user: "bulls96_read_api" }), "reader_role_system_user"],
      [login(), pg16({ system_user: ":bulls96_read_api" }), "reader_role_system_user"],
      [login(), pg16({ system_user: "scram-sha-256:bulls96_read_api_x" }), "reader_role_system_user"],
      [login(), undefined, "reader_role_unknown"], // PG16+ without its row
    ];
    for (const [l, p, code] of cases) expect([l, p, readerLoginRefusal(l, p, SUPABASE)]).toEqual([l, p, code]);
  });

  test("attributes: every forbidden one, and LOGIN required", () => {
    expect(readerLoginRefusal(login({ rolsuper: true }), pg16(), SUPABASE)).toBe("reader_role_superuser");
    expect(readerLoginRefusal(login({ rolbypassrls: true }), pg16(), SUPABASE)).toBe("reader_role_bypassrls");
    for (const attr of ["rolcreaterole", "rolcreatedb", "rolreplication", "rolinherit"])
      expect([attr, readerLoginRefusal(login({ [attr]: true }), pg16(), SUPABASE)]).toEqual([attr, "reader_role_attributes"]);
    expect(readerLoginRefusal(login({ rolcanlogin: false }), pg16(), SUPABASE)).toBe("reader_role_attributes");
    // Unreadable values never admit.
    for (const attr of ["rolsuper", "rolbypassrls", "rolcreaterole", "rolinherit", "rolcanlogin"])
      expect(readerLoginRefusal(login({ [attr]: null }), pg16(), SUPABASE)).not.toBeNull();
  });

  test("membership both ways: member of nothing; only the Supabase PG16+ creator edge may point at it", () => {
    expect(readerLoginRefusal(login({ memberships: 1 }), pg16(), SUPABASE)).toBe("reader_role_membership");
    // The creator edge: admitted on a Supabase provider, on PG16+.
    expect(readerLoginRefusal(login({ members: 1 }), pg16({ member_edges: [CREATOR_EDGE] }), SUPABASE)).toBeNull();
    // ...refused off Supabase, before PG16, or with any property different.
    expect(readerLoginRefusal(login({ members: 1 }), pg16({ member_edges: [CREATOR_EDGE] }), OTHER)).toBe("reader_role_members");
    expect(readerLoginRefusal(login({ members: 1, server_version: 150008 }), undefined, SUPABASE)).toBe("reader_role_members");
    for (const over of [{ set_option: true }, { inherit_option: true }, { admin_option: false }, { member: "authenticator" }, { grantor: "postgres" }, { grantor_super: false }, { member: null }])
      expect([over, readerLoginRefusal(login({ members: 1 }), pg16({ member_edges: [{ ...CREATOR_EDGE, ...over }] }), SUPABASE)]).toEqual([over, "reader_role_members"]);
    // A second edge, or a count that disagrees with the listed edges.
    expect(readerLoginRefusal(login({ members: 2 }), pg16({ member_edges: [CREATOR_EDGE, { ...CREATOR_EDGE, member: "other" }] }), SUPABASE)).toBe("reader_role_members");
    expect(readerLoginRefusal(login({ members: 2 }), pg16({ member_edges: [CREATOR_EDGE] }), SUPABASE)).toBe("reader_role_members");
    expect(readerLoginRefusal(login({ members: 0 }), pg16({ member_edges: [CREATOR_EDGE] }), SUPABASE)).toBe("reader_role_members");
    expect(readerLoginRefusal(login({ members: null }), pg16(), SUPABASE)).toBe("reader_role_unknown");
  });

  test("settings: both pinned in rolconfig and live", () => {
    for (const over of [
      { rolconfig: null },
      { rolconfig: ["statement_timeout=2s"] },
      { rolconfig: ["default_transaction_read_only=on"] },
      { rolconfig: ["statement_timeout=5s", "default_transaction_read_only=on"] },
      { statement_timeout: "0" },
      { statement_timeout: "2000ms" },
      { default_transaction_read_only: "off" },
    ])
      expect([over, readerLoginRefusal(login(over), pg16(), SUPABASE)]).toEqual([over, "reader_role_settings"]);
  });

  test("an unreadable login row never admits", () => {
    expect(readerLoginRefusal(undefined, undefined, SUPABASE)).toBe("reader_role_unknown");
    for (const v of [undefined, "170004", 99_999, 17.5]) expect(readerLoginRefusal(login({ server_version: v }), pg16(), SUPABASE)).toBe("reader_role_unknown");
  });
});

describe("assertReaderRole", () => {
  test("runs the login query, the PG16 query only on PG16+, and admits the production shape", async () => {
    const seen: string[] = [];
    await assertReaderRole({ async query(text: string) { seen.push(text); return { rows: readerLoginAnswer(text) ?? [] } as never; } }, SUPABASE);
    expect(seen).toEqual([READER_LOGIN_SQL, READER_PG16_SQL]);
    seen.length = 0;
    await assertReaderRole({ async query(text: string) { seen.push(text); return { rows: readerLoginAnswer(text, login({ server_version: 150008 })) ?? [] } as never; } });
    expect(seen).toEqual([READER_LOGIN_SQL]);
  });

  test("refusals throw ReaderRoleError naming only the property", async () => {
    const error = await assertReaderRole({ async query(text: string) { return { rows: readerLoginAnswer(text, login({ session_user: "secret_admin_login" })) ?? [] } as never; } }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ReaderRoleError);
    expect((error as ReaderRoleError).code).toBe("reader_role_session_user");
    expect((error as Error).message).not.toContain("secret_admin_login");
    for (const rows of [[], [READER_LOGIN_ROW, READER_LOGIN_ROW]])
      await expect(assertReaderRole({ async query() { return { rows } as never; } })).rejects.toMatchObject({ code: "reader_role_unknown" });
  });
});

describe("against the bootstrap's actual role (PGlite)", () => {
  const root = join(import.meta.dir, "../../..");
  const read = (path: string) => readFileSync(join(root, path), "utf8");
  const psql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
  let db: PGlite;
  beforeAll(async () => {
    db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
    await db.exec(read("supabase/bootstrap/session_token_verifier.sql"));
    await db.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
    await db.exec(read("supabase/bootstrap/session_read_api_roles.sql"));
    for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.exec(read(`supabase/migrations/${file}`));
    await db.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
    await db.exec(psql("supabase/bootstrap/session_read_api_role_verify.sql"));
  }, 180_000);
  afterAll(async () => db?.close());
  const sql = { query: async (text: string) => (await db.query(text)) as never };

  test("SET ROLE into bulls96_read_api from another login is refused (session_user)", async () => {
    await db.exec("SET ROLE bulls96_read_api");
    try {
      await expect(assertReaderRole(sql, SUPABASE)).rejects.toMatchObject({ name: "ReaderRoleError", code: "reader_role_session_user" });
      const { rows } = await db.query<Record<string, unknown>>(READER_LOGIN_SQL);
      expect(rows[0]).toMatchObject({ session_user: "postgres", current_user: "bulls96_read_api", role_setting: "bulls96_read_api" });
    } finally {
      await db.exec("RESET ROLE");
    }
    // The superuser itself is refused too.
    await expect(assertReaderRole(sql, SUPABASE)).rejects.toMatchObject({ code: "reader_role_session_user" });
  });

  test("the bootstrapped role's shape (attributes, rolconfig, memberships) passes once it is a real login", async () => {
    await db.exec("SET ROLE bulls96_read_api");
    let row: Record<string, unknown>;
    try {
      row = (await db.query<Record<string, unknown>>(READER_LOGIN_SQL)).rows[0]!;
    } finally {
      await db.exec("RESET ROLE");
    }
    // Only what a real login changes is substituted: the session identity and the role's settings
    // applied at login (SET ROLE does not apply them). Everything else is the bootstrap's own row.
    const real = { ...row, session_user: "bulls96_read_api", role_setting: "none", statement_timeout: "2s", default_transaction_read_only: "on" };
    expect(row.rolconfig).toEqual(expect.arrayContaining(["statement_timeout=2s", "default_transaction_read_only=on"]));
    expect(readerLoginRefusal(real, { system_user: "scram-sha-256:bulls96_read_api", member_edges: [] }, OTHER)).toBeNull();
  });
});
