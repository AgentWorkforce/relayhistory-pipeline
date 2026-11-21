/**
 * Non-superuser owner harness for opt-in loopback PostgreSQL suites.
 *
 * A privileged fixture (the cluster's bootstrap superuser, a trust-auth loopback container) provisions the roles and
 * the database; everything under test then runs as the database OWNER, shaped like the consumer credential probe
 * (src/session-pipeline/__tests__/consumer-credential-probe.postgres.test.ts): LOGIN CREATEROLE NOINHERIT NOSUPERUSER
 * NOBYPASSRLS, owning the database, with no grant, membership, parameter privilege or setting added for it.
 *
 *   - The fixture runs the test-only Supabase shim and the bootstrap chain (SESSION_BOOTSTRAP_FILES) except
 *     session_consumer_roles.sql, which the owner runs itself (as pg-proof's consumer writer job does).
 *   - The owner runs runMigrations with no shim and no bootstrap (it cannot create the BYPASSRLS shim roles).
 *   - The runtime role (bulls96_session_ingest, NOLOGIN) is reached from the fixture by SET LOCAL ROLE: the fixture
 *     is the only login that can become it; current_user is then the runtime role, with its own privileges only.
 *
 * One owner role per cluster (roles are cluster-global and the bootstrap chain is idempotent); one fresh database per
 * suite. The URL must be the fixture superuser on a loopback host and the postgres database, without a password.
 */
import { readdirSync, readFileSync } from "node:fs";
import pg from "pg";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { runMigrations, type MigrationProfile, type RunOptions } from "../../hoopdb/migrate.ts";

export const OWNER_ROLE = "t2_gate_owner";
const read = (p: string) => readFileSync(new URL("../../../" + p, import.meta.url), "utf8");
const psqlFree = (s: string) => s.split("\n").filter((l) => !l.startsWith("\\")).join("\n");

export function ownedLoopbackUrl(url: string | undefined): URL | undefined {
  if (!url) return undefined;
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
  return u;
}

export const ownerProfile: MigrationProfile = { mode: "local", shim: false, bootstrap: false, verify: true, runtimeRole: OWNER_ROLE };

export interface RoleReceipt { current_user: string; rolsuper: boolean; rolbypassrls: boolean; rolcreaterole: boolean; rolinherit: boolean; server_version_num: number }
/** current_user and its role flags, read in the connection under test. */
export async function roleReceipt(c: { query: pg.Client["query"] }): Promise<RoleReceipt> {
  const r = (await c.query(`SELECT current_user::text AS current_user,r.rolsuper,r.rolbypassrls,r.rolcreaterole,r.rolinherit,
    current_setting('server_version_num')::int AS server_version_num FROM pg_roles r WHERE r.rolname=current_user`)).rows[0];
  return r as RoleReceipt;
}

export interface OwnerDatabase {
  name: string;
  /** owner login on the database under test */
  ownerUrl: string;
  /** fixture superuser on the database under test (provisioning, SET ROLE to the runtime role, observation) */
  fixtureUrl: string;
  drop(): Promise<void>;
}

/** Provision the owner role (once per cluster) and a fresh database it owns, with the bootstrap chain applied. */
export async function ownerDatabase(adminUrl: string, prefix = "t2g"): Promise<OwnerDatabase> {
  const admin = new pg.Client({ connectionString: adminUrl }); await admin.connect();
  const name = prefix + "_" + crypto.randomUUID().replaceAll("-", "").slice(0, 20);
  try {
    if (!(await admin.query("SELECT 1 FROM pg_roles WHERE rolname=$1", [OWNER_ROLE])).rowCount)
      await admin.query(`CREATE ROLE ${OWNER_ROLE} LOGIN CREATEROLE NOINHERIT NOSUPERUSER NOBYPASSRLS`);
    const shape = (await admin.query("SELECT rolsuper,rolbypassrls,rolinherit,rolcreaterole,rolcanlogin FROM pg_roles WHERE rolname=$1", [OWNER_ROLE])).rows[0];
    if (shape.rolsuper || shape.rolbypassrls || shape.rolinherit || !shape.rolcreaterole || !shape.rolcanlogin) throw Error("owner_role_shape_drift");
    await admin.query(`CREATE DATABASE ${name} OWNER ${OWNER_ROLE}`);
  } finally { await admin.end(); }
  const at = (user: string) => { const u = new URL(adminUrl); u.username = user; u.pathname = "/" + name; return u.href; };
  const fixtureUrl = at("postgres"), ownerUrl = at(OWNER_ROLE);
  const op = new pg.Client({ connectionString: fixtureUrl }); await op.connect();
  const owner = new pg.Client({ connectionString: ownerUrl }); await owner.connect();
  try {
    await op.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin");
    await op.query(psqlFree(read("supabase/tests/supabase_shim.sql")));
    for (const f of SESSION_BOOTSTRAP_FILES) await (f.endsWith("session_consumer_roles.sql") ? owner : op).query(psqlFree(read(f)));
  } finally { await owner.end(); await op.end(); }
  return {
    name, ownerUrl, fixtureUrl,
    async drop() {
      const a = new pg.Client({ connectionString: adminUrl }); await a.connect();
      try { await a.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`); } finally { await a.end(); }
    },
  };
}

/**
 * Every migration slot later than `slot` (a filename stem such as "20261105062210_lazy_ack_t2"), discovered by listing
 * supabase/migrations, newest first (reverse filename order), each paired with its down in supabase/rollbacks. A later
 * slot without a down file throws: the caller cannot roll back past it, so the test must fail, never skip.
 */
export function laterSlotDowns(slot: string): Array<{ stem: string; name: string; down: string }> {
  const dir = (d: string) => new URL("../../../supabase/" + d + "/", import.meta.url);
  const downs = new Set(readdirSync(dir("rollbacks")).filter((f) => f.endsWith(".down.sql")));
  const later = readdirSync(dir("migrations")).filter((f) => /^\d{14}_.*\.sql$/.test(f)).map((f) => f.slice(0, -4))
    .filter((stem) => stem > slot).sort().reverse();
  const missing = later.filter((stem) => !downs.has(stem + ".down.sql"));
  if (missing.length) throw Error("later_slot_without_down: " + missing.join(", "));
  return later.map((stem) => ({ stem, name: "upstream:" + stem + ".sql", down: readFileSync(new URL(stem + ".down.sql", dir("rollbacks")), "utf8") }));
}

/**
 * Roll back every slot later than `slot` (laterSlotDowns), newest first, each from its real down file on its own OWNER
 * login (asserted non-superuser). Any failure throws with the slot named; returns the ledger names rolled back.
 */
export async function rollBackLaterSlots(ownerUrl: string, slot: string): Promise<string[]> {
  const done: string[] = [];
  for (const { stem, name, down } of laterSlotDowns(slot)) {
    const c = new pg.Client({ connectionString: ownerUrl, application_name: "later-slot-down" }); await c.connect();
    try {
      const who = await roleReceipt(c);
      if (who.current_user !== OWNER_ROLE || who.rolsuper || who.rolbypassrls) throw Error("owner_login_required");
      try { await c.query(down); }
      catch (e: any) { await c.query("ROLLBACK").catch(() => {}); throw Error(`later_slot_down_failed: ${stem}: ${e.code} ${e.message}`); }
      if ((await c.query("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1", [name])).rows[0].n !== 0)
        throw Error(`later_slot_down_left_ledger_row: ${stem}`);
    } finally { await c.end(); }
    done.push(name);
  }
  return done;
}

/** runMigrations as the owner login (no shim, no bootstrap). */
export async function migrateAsOwner(ownerUrl: string, opts: Omit<RunOptions, "profile"> = {}) {
  const c = new pg.Client({ connectionString: ownerUrl }); await c.connect();
  try {
    const who = await roleReceipt(c);
    if (who.current_user !== OWNER_ROLE || who.rolsuper || who.rolbypassrls) throw Error("owner_login_required");
    return await runMigrations(c, { ...opts, profile: ownerProfile });
  } finally { await c.end(); }
}
