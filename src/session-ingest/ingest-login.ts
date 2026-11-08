/** Live identity assertion for a pool that must be a GENUINE login as bulls96_session_ingest.
 *
 * current_user = session_user = the role is not enough: a superuser backend that runs
 * `SET SESSION AUTHORIZATION bulls96_session_ingest` makes both equal the role, and
 * current_setting('role') stays 'none'. Observed on real PostgreSQL 15.19, 16.15 and 17.11
 * (recorded rows in __tests__/ingest-login.test.ts):
 *
 *   signal                         genuine login           superuser + SET SESSION AUTHORIZATION
 *   session_user / current_user    the role                the role
 *   current_setting('role')        none                    none
 *   pg_stat_activity.usename       the role                the authenticated login (e.g. postgres)
 *   system_user (PG16+)            <method>:the role       <method>:the authenticated login
 *
 * So this requires all of: session_user, current_user, role 'none' (no SET ROLE), this
 * backend's pg_stat_activity usename (fixed at authentication, on every supported version),
 * and on server_version_num >= 160000 a non-null system_user "<method>:<role>". NULL
 * system_user (trust auth) proves no authenticated identity and refuses.
 *
 * Approach mirrors #363's ingestLoginRefusal (src/session-ingest/selfhost-server.ts, head
 * 58fdbb10), copied rather than imported so this lands independently. Role shape, membership
 * and timeout profile are not re-checked here: the publication runtime runs the phase-2
 * verifiers (session_ingest_verify.sql) right after this assertion.
 */
export const INGEST_LOGIN_ROLE = "bulls96_session_ingest";
export const INGEST_LOGIN_IDENTITY_SQL = `SELECT session_user::text AS session_user, current_user::text AS current_user,
 current_setting('role') AS role_setting, current_setting('server_version_num')::int AS server_version,
 (SELECT a.usename::text FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid()) AS backend_user`;
/** PG16+ only: system_user does not parse earlier. */
export const INGEST_LOGIN_PG16_SQL = "SELECT system_user::text AS system_user";

export type IngestLoginIdentityRefusal = "login_unreadable" | "session_user" | "current_user" | "set_role" | "backend_user" | "system_user";

/** Pure decision over the live rows; null admits. `pg16` is required when server_version >= 160000. */
export function ingestLoginIdentityRefusal(login: Record<string, unknown> | undefined, pg16: Record<string, unknown> | undefined, role: string = INGEST_LOGIN_ROLE): IngestLoginIdentityRefusal | null {
  if (!login) return "login_unreadable";
  if (login.session_user !== role) return "session_user";
  if (login.current_user !== role) return "current_user";
  if (login.role_setting !== "none") return "set_role";
  const version = login.server_version;
  if (typeof version !== "number" || !Number.isInteger(version) || version < 100000) return "login_unreadable";
  if (login.backend_user !== role) return "backend_user";
  if (version >= 160000) {
    if (!pg16) return "login_unreadable";
    const su = pg16.system_user, colon = typeof su === "string" ? su.indexOf(":") : -1;
    if (typeof su !== "string" || colon < 1 || su.slice(colon + 1) !== role) return "system_user";
  }
  return null;
}

/** Reads both rows through `query` (a failure or shape drift is login_unreadable; an abort rethrows) and decides. */
export async function ingestLoginIdentity(query: (sql: string) => Promise<{ rows: Record<string, unknown>[] }>, role: string = INGEST_LOGIN_ROLE): Promise<IngestLoginIdentityRefusal | null> {
  let login: Record<string, unknown> | undefined, pg16: Record<string, unknown> | undefined;
  try {
    const a = await query(INGEST_LOGIN_IDENTITY_SQL);
    if (a.rows.length !== 1) return "login_unreadable";
    login = a.rows[0];
    if (typeof login?.server_version === "number" && login.server_version >= 160000) {
      const b = await query(INGEST_LOGIN_PG16_SQL);
      if (b.rows.length !== 1) return "login_unreadable";
      pg16 = b.rows[0];
    }
  } catch (error) {
    // Cancellation stays cancellation; anything else is an unreadable login.
    if ((error as { name?: string })?.name === "AbortError") throw error;
    return "login_unreadable";
  }
  return ingestLoginIdentityRefusal(login, pg16, role);
}
