/**
 * Test-only PostgreSQL fixture for the 062320 hold harness (and any two-connection *.postgres.test.ts).
 *
 * The fixture is SESSION_INGEST_TEST_PG_URL: the bootstrap superuser of a disposable, trust-auth, loopback
 * PostgreSQL on its `postgres` database, without a password (owner-role-harness.ts ownedLoopbackUrl). Unset, suites
 * skip, except under SESSION_INGEST_TEST_PG_REQUIRED=1 (pg-proof, run.ts), where an unset URL is a failure. When
 * SESSION_INGEST_TEST_PG_MAJOR is set (run.ts sets it per container), the server must be that major.
 *
 * Each case gets its own scratch database owned by the non-superuser OWNER_ROLE (LOGIN CREATEROLE NOINHERIT
 * NOSUPERUSER NOBYPASSRLS, the owner-role-harness shape). `bootstrap: true` delegates to ownerDatabase (Supabase shim
 * plus the session bootstrap chain, ready for migrateAsOwner); the default is a bare database, which is all a lock or
 * isolation probe needs. Nothing here touches production code or any non-loopback host.
 *
 * Version difference to expect: on PostgreSQL 15 the NOINHERIT owner cannot CREATE in `public` (that privilege comes
 * through pg_database_owner, which NOINHERIT does not use there); 16 and 17 allow it. Create objects in a schema of
 * your own (the migrations use `sessions`).
 */
import pg from "pg";
import { OWNER_ROLE, ownedLoopbackUrl, ownerDatabase } from "../owner-role-harness.ts";

export { OWNER_ROLE, migrateAsOwner, rollBackLaterSlots, roleReceipt } from "../owner-role-harness.ts";

/** The validated fixture URL, or undefined (skip). Throws when required and unset, or when not an owned loopback fixture. */
export function fixturePgUrl(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const raw = env.SESSION_INGEST_TEST_PG_URL || undefined;
  if (!raw && env.SESSION_INGEST_TEST_PG_REQUIRED === "1") throw Error("SESSION_INGEST_TEST_PG_URL required (SESSION_INGEST_TEST_PG_REQUIRED=1)");
  return ownedLoopbackUrl(raw)?.href;
}

/** The server's major version (15, 16, 17), checked against SESSION_INGEST_TEST_PG_MAJOR when that is set. */
export async function serverMajor(url: string, env: NodeJS.ProcessEnv = process.env): Promise<number> {
  const c = new pg.Client({ connectionString: url }); await c.connect();
  try {
    const major = Math.floor(Number((await c.query("SELECT current_setting('server_version_num') AS v")).rows[0].v) / 10000);
    if (env.SESSION_INGEST_TEST_PG_MAJOR && Number(env.SESSION_INGEST_TEST_PG_MAJOR) !== major)
      throw Error(`pg_major_mismatch: server ${major}, SESSION_INGEST_TEST_PG_MAJOR ${env.SESSION_INGEST_TEST_PG_MAJOR}`);
    return major;
  } finally { await c.end(); }
}

export interface ScratchDatabase {
  name: string;
  /** OWNER_ROLE login on the scratch database (what the code under test runs as) */
  ownerUrl: string;
  /** fixture superuser on the scratch database (provisioning, SET ROLE, and the lock observer) */
  fixtureUrl: string;
  drop(): Promise<void>;
}

async function ensureOwnerRole(admin: pg.Client) {
  // A demoted fixture (role-proof's Supabase-identity case leaves postgres NOSUPERUSER) fails here, named.
  if (!(await admin.query("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")).rows[0]?.rolsuper) throw Error("fixture_superuser_required");
  if (!(await admin.query("SELECT 1 FROM pg_roles WHERE rolname=$1", [OWNER_ROLE])).rowCount) {
    // Roles are cluster-global: a concurrent suite may create it first (42710, or 23505 on pg_authid's index).
    await admin.query(`CREATE ROLE ${OWNER_ROLE} LOGIN CREATEROLE NOINHERIT NOSUPERUSER NOBYPASSRLS`).catch((e: { code?: string }) => {
      if (e.code !== "42710" && e.code !== "23505") throw e;
    });
  }
  const shape = (await admin.query("SELECT rolsuper,rolbypassrls,rolinherit,rolcreaterole,rolcanlogin FROM pg_roles WHERE rolname=$1", [OWNER_ROLE])).rows[0];
  if (shape.rolsuper || shape.rolbypassrls || shape.rolinherit || !shape.rolcreaterole || !shape.rolcanlogin) throw Error("owner_role_shape_drift");
}

/** A fresh database owned by OWNER_ROLE; `bootstrap: true` also applies the shim and bootstrap chain (ownerDatabase). */
export async function scratchDatabase(adminUrl: string, opts: { prefix?: string; bootstrap?: boolean } = {}): Promise<ScratchDatabase> {
  const prefix = opts.prefix ?? "hold";
  if (!/^[a-z][a-z0-9_]{0,20}$/.test(prefix)) throw Error("scratch_prefix_invalid");
  if (opts.bootstrap) return ownerDatabase(adminUrl, prefix);
  const name = prefix + "_" + crypto.randomUUID().replaceAll("-", "").slice(0, 20);
  const admin = new pg.Client({ connectionString: adminUrl }); await admin.connect();
  try {
    await ensureOwnerRole(admin);
    await admin.query(`CREATE DATABASE ${name} OWNER ${OWNER_ROLE}`);
  } finally { await admin.end(); }
  const at = (user: string) => { const u = new URL(adminUrl); u.username = user; u.pathname = "/" + name; return u.href; };
  return {
    name, ownerUrl: at(OWNER_ROLE), fixtureUrl: at("postgres"),
    async drop() {
      const a = new pg.Client({ connectionString: adminUrl }); await a.connect();
      try { await a.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`); } finally { await a.end(); }
    },
  };
}
