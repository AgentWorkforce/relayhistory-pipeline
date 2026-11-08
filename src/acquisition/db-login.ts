/**
 * The installer's database login (ACQUISITION_DATABASE_URL + ACQUISITION_DATABASE_CA_PEM),
 * pinned the same way as src/shared/owner-pg-config.ts and with its helper:
 *
 * A raw `connectionString` would let URL parameters override what is pinned here:
 * `?sslmode=disable` or `?sslmode=no-verify` would send the password and every signed
 * payload over an unverified connection, `?options=-c role=...` would let a different
 * login satisfy the SQL role guard (sessions.ingest_require_role reads the `role`
 * setting before session_user), and a missing user or password would silently fall
 * back to PG* environment variables or ~/.pgpass. So the URL is never passed through:
 *  - it must be postgresql://user:password@host[:port]/database with a non-empty
 *    password and at most the one parameter `sslmode=verify-full`; anything else refuses;
 *  - the user is the installer role (or its Supabase-pooler form `<role>.<tenant>`);
 *  - TLS is always verify-full against the CA in ACQUISITION_DATABASE_CA_PEM;
 *  - application_name and `options` are fixed (non-empty, so PGOPTIONS is never read).
 *
 * Then, before the operations listener starts, assertInstallerLogin checks the real
 * session: session_user and current_user are exactly the installer role, no SET ROLE,
 * PG16+ system_user names the same authenticated identity, the role is not superuser,
 * BYPASSRLS, CREATEROLE, CREATEDB, REPLICATION or INHERIT, is a member of no role, and
 * the timeout profile is the expected one. Any mismatch or query failure refuses
 * startup. Errors name the variable or the failed property, never a value.
 */
import { X509Certificate } from "node:crypto";
import type { ClientConfig } from "pg";
import { ownerClientConfig, OwnerUrlError } from "../shared/owner-pg-config.ts";

/** The control role the four installer SQL functions require (docs/factory-acquisition-owner-design.md §6). */
export const ACQUISITION_INSTALLER_ROLE = "bulls96_acquisition_installer";
export const ACQUISITION_APPLICATION_NAME = "96bulls-acquisition-installer";

/** The LOGIN timeout profile (pattern: supabase/bootstrap/privacy_eraser_role.sql), also sent as startup options. */
export const INSTALLER_TIMEOUTS = Object.freeze({ statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s" });
export const INSTALLER_OPTIONS = Object.entries(INSTALLER_TIMEOUTS).map(([k, v]) => `-c ${k}=${v}`).join(" ");

const URL_VAR = "ACQUISITION_DATABASE_URL";
const CA_VAR = "ACQUISITION_DATABASE_CA_PEM";
const POOLER_USER = new RegExp(`^${ACQUISITION_INSTALLER_ROLE}(\\.[a-z0-9]{1,63})?$`);

/** A PEM CA certificate (parsed); a private key is refused. Never echoes the value. */
export function caPem(raw: string): string {
  if (raw.includes("PRIVATE KEY") || !raw.includes("-----BEGIN CERTIFICATE-----")) throw new Error(`96bulls: ${CA_VAR} must be a PEM CA certificate`);
  try {
    new X509Certificate(raw);
  } catch {
    throw new Error(`96bulls: ${CA_VAR} must be a PEM CA certificate`);
  }
  return raw;
}

/** Discrete, pinned pg config for the installer login. No connectionString, no PG* or .pgpass fallback. */
export function installerClientConfig(url: string, ca: string): ClientConfig {
  let base: ClientConfig;
  try {
    base = ownerClientConfig(url, ACQUISITION_APPLICATION_NAME, caPem(ca));
  } catch (e) {
    if (e instanceof OwnerUrlError) throw new Error(`96bulls: ${URL_VAR} refused (${e.code})`);
    throw e;
  }
  if (typeof base.user !== "string" || !POOLER_USER.test(base.user)) throw new Error(`96bulls: ${URL_VAR} user must be the ${ACQUISITION_INSTALLER_ROLE} login`);
  return Object.freeze({ ...base, options: INSTALLER_OPTIONS });
}

export type InstallerLoginRefusal =
  | "login_unreadable" | "session_user" | "current_user" | "set_role" | "system_user"
  | "superuser" | "bypassrls" | "role_attributes" | "role_membership" | "timeouts";

/** Names the failed property only; never a role name, URL or credential. */
export class InstallerLoginError extends Error {
  constructor(readonly code: InstallerLoginRefusal) {
    super(`96bulls: acquisition installer login refused: ${code}`);
    this.name = "InstallerLoginError";
  }
}

export interface LoginQuery {
  query<T = Record<string, unknown>>(text: string, values?: unknown[]): Promise<{ rows: T[] }>;
}

export const LOGIN_SQL = `SELECT session_user::text AS session_user, current_user::text AS current_user,
  current_setting('role') AS role_setting, current_setting('server_version_num')::int AS server_version,
  current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout,
  current_setting('idle_in_transaction_session_timeout') AS idle_in_transaction_session_timeout,
  r.rolsuper, r.rolbypassrls, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolinherit,
  (SELECT count(*)::int FROM pg_auth_members m WHERE m.member = r.oid) AS memberships
  FROM pg_roles r WHERE r.rolname = current_user`;
/** Only on PG16+ (system_user does not parse earlier). */
export const SYSTEM_USER_SQL = "SELECT system_user::text AS system_user";

/** Refuse to serve unless the live session is exactly the installer login with the expected shape. */
export async function assertInstallerLogin(sql: LoginQuery): Promise<void> {
  let row: Record<string, unknown> | undefined;
  try {
    const { rows } = await sql.query<Record<string, unknown>>(LOGIN_SQL);
    if (rows.length !== 1) throw new Error();
    row = rows[0];
  } catch {
    throw new InstallerLoginError("login_unreadable");
  }
  if (!row) throw new InstallerLoginError("login_unreadable");
  if (row.session_user !== ACQUISITION_INSTALLER_ROLE) throw new InstallerLoginError("session_user");
  if (row.current_user !== ACQUISITION_INSTALLER_ROLE) throw new InstallerLoginError("current_user");
  if (row.role_setting !== "none") throw new InstallerLoginError("set_role");
  if (row.rolsuper !== false) throw new InstallerLoginError("superuser");
  if (row.rolbypassrls !== false) throw new InstallerLoginError("bypassrls");
  if (row.rolcreaterole !== false || row.rolcreatedb !== false || row.rolreplication !== false || row.rolinherit !== false)
    throw new InstallerLoginError("role_attributes");
  if (row.memberships !== 0) throw new InstallerLoginError("role_membership");
  for (const [k, v] of Object.entries(INSTALLER_TIMEOUTS)) if (row[k] !== v) throw new InstallerLoginError("timeouts");
  if (typeof row.server_version !== "number") throw new InstallerLoginError("login_unreadable");
  if (row.server_version >= 160000) {
    let systemUser: unknown;
    try {
      const { rows } = await sql.query<{ system_user: unknown }>(SYSTEM_USER_SQL);
      if (rows.length !== 1) throw new Error();
      systemUser = rows[0]!.system_user;
    } catch {
      throw new InstallerLoginError("login_unreadable");
    }
    // "<auth method>:<authenticated identity>"; NULL means trust auth, which no password proves.
    if (typeof systemUser !== "string" || systemUser.slice(systemUser.indexOf(":") + 1) !== ACQUISITION_INSTALLER_ROLE || !systemUser.includes(":"))
      throw new InstallerLoginError("system_user");
  }
}
