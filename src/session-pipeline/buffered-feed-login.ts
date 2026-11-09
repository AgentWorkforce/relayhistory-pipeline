/**
 * The buffered search feeder's own database login (secret fields buffered_search_feed_database_url +
 * buffered_search_feed_database_ca_pem), pinned and checked the same way as the acquisition installer's
 * (src/acquisition/db-login.ts, whose identity queries it reuses):
 *
 *  - the URL is never passed through as a connectionString: it must be
 *    postgresql://user:password@host[:port]/database with a non-empty password and at most the one
 *    parameter `sslmode=verify-full` (src/shared/owner-pg-config.ts); anything else refuses;
 *  - the user is BUFFERED_SEARCH_FEED_RUNTIME_ROLE (or its Supabase-pooler form `<role>.<tenant>`);
 *  - TLS is always verify-full against the CA in buffered_search_feed_database_ca_pem;
 *  - application_name and `options` (the LOGIN timeout profile of
 *    supabase/bootstrap/buffered_search_feed_runtime_role.sql) are fixed.
 *
 * Then, before any feed RPC runs, assertBufferedFeedLogin checks the live session: session_user and
 * current_user are exactly the runtime role, no SET ROLE, this backend's pg_stat_activity usename (and
 * on PG16+ system_user) name the same authenticated identity (so a superuser's SET SESSION AUTHORIZATION
 * refuses), the role is not superuser, BYPASSRLS, CREATEROLE, CREATEDB, REPLICATION or INHERIT, is a
 * member of no role, has no member except the Supabase PG16+ creator edge (postgres, granted by the
 * superuser supabase_admin, ADMIN only, no SET, no INHERIT), and the timeout profile is the expected one.
 *
 * Server side, the three feed RPCs (migration 20261105043870) admit only session_user = the runtime role
 * with role setting 'none' (the 20261105043860 guard); they tolerate a NULL usename/system_user, so this
 * client check is the strict one. Errors name the secret field or the failed property, never a value.
 *
 * Provisioning the LOGIN (buffered_search_feed_runtime_role.sql) and the credential needs Khaliq's
 * explicit OK in his own words; until both exist the fields are absent and the feeder stays off.
 */
import { X509Certificate } from "node:crypto";
import type { ClientConfig } from "pg";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { LOGIN_SQL, MEMBERS_SQL, SYSTEM_USER_SQL, type LoginQuery } from "../acquisition/db-login.ts";
import { ownerClientConfig, OwnerUrlError } from "../shared/owner-pg-config.ts";
import { BufferedFeedError } from "./buffered-search-feed.ts";
import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE } from "./buffered-feed-role.ts";

export { BUFFERED_SEARCH_FEED_RUNTIME_ROLE };
export const BUFFERED_FEED_APPLICATION_NAME = "96bulls-buffered-search-feed";

/** The LOGIN timeout profile (supabase/bootstrap/buffered_search_feed_runtime_role.sql), also sent as startup options. */
export const BUFFERED_FEED_TIMEOUTS = Object.freeze({ statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s" });
export const BUFFERED_FEED_OPTIONS = Object.entries(BUFFERED_FEED_TIMEOUTS).map(([k, v]) => `-c ${k}=${v}`).join(" ");

export const BUFFERED_FEED_URL_FIELD = "buffered_search_feed_database_url";
export const BUFFERED_FEED_CA_FIELD = "buffered_search_feed_database_ca_pem";
const POOLER_USER = new RegExp(`^${BUFFERED_SEARCH_FEED_RUNTIME_ROLE}(\\.[a-z0-9]{1,63})?$`);

/** A PEM CA certificate (parsed); a private key is refused. Never echoes the value. */
function feedCaPem(raw: unknown): string {
  const bad = () => new Error(`96bulls: ${BUFFERED_FEED_CA_FIELD} must be a PEM CA certificate`);
  if (typeof raw !== "string" || raw.includes("PRIVATE KEY") || !raw.includes("-----BEGIN CERTIFICATE-----")) throw bad();
  try {
    new X509Certificate(raw);
  } catch {
    throw bad();
  }
  return raw;
}

/** Discrete, pinned pg config for the feeder login. No connectionString, no PG* or .pgpass fallback. */
export function bufferedFeedClientConfig(url: unknown, ca: unknown): ClientConfig {
  if (typeof url !== "string") throw new Error(`96bulls: ${BUFFERED_FEED_URL_FIELD} refused (url)`);
  let base: ClientConfig;
  try {
    base = ownerClientConfig(url, BUFFERED_FEED_APPLICATION_NAME, feedCaPem(ca));
  } catch (e) {
    if (e instanceof OwnerUrlError) throw new Error(`96bulls: ${BUFFERED_FEED_URL_FIELD} refused (${e.code})`);
    throw e;
  }
  if (typeof base.user !== "string" || !POOLER_USER.test(base.user))
    throw new Error(`96bulls: ${BUFFERED_FEED_URL_FIELD} user must be the ${BUFFERED_SEARCH_FEED_RUNTIME_ROLE} login`);
  return Object.freeze({ ...base, options: BUFFERED_FEED_OPTIONS });
}

export type BufferedFeedLoginRefusal =
  | "login_unreadable" | "session_user" | "current_user" | "set_role" | "backend_user" | "system_user"
  | "superuser" | "bypassrls" | "role_attributes" | "role_membership" | "role_members" | "timeouts";

/** Names the failed property only; never a role name, URL or credential. */
export class BufferedFeedLoginError extends Error {
  constructor(readonly code: BufferedFeedLoginRefusal) {
    super(`96bulls: buffered search feed login refused: ${code}`);
    this.name = "BufferedFeedLoginError";
  }
}

/** The Supabase PG16+ creator edge (as buffered_search_feed_runtime_verify.sql): ADMIN only, no SET, no INHERIT. */
function isSupabaseCreatorEdge(m: Record<string, unknown>): boolean {
  return m.member === "postgres" && m.grantor === "supabase_admin" && m.grantor_super === true &&
    m.admin_option === true && m.inherit_option === false && m.set_option === false;
}

/** Refuse to feed unless the live session is exactly the feeder login with the expected shape. */
export async function assertBufferedFeedLogin(sql: LoginQuery): Promise<void> {
  const ROLE = BUFFERED_SEARCH_FEED_RUNTIME_ROLE;
  let row: Record<string, unknown> | undefined;
  try {
    const { rows } = await sql.query<Record<string, unknown>>(LOGIN_SQL);
    if (rows.length !== 1) throw new Error();
    row = rows[0];
  } catch {
    throw new BufferedFeedLoginError("login_unreadable");
  }
  if (!row) throw new BufferedFeedLoginError("login_unreadable");
  if (row.session_user !== ROLE) throw new BufferedFeedLoginError("session_user");
  if (row.current_user !== ROLE) throw new BufferedFeedLoginError("current_user");
  if (row.role_setting !== "none") throw new BufferedFeedLoginError("set_role");
  // The authenticated login of this backend (all versions): refuses a superuser's SET SESSION
  // AUTHORIZATION, which leaves session_user/current_user/role looking genuine. NULL or no row refuses.
  if (row.backend_user !== ROLE) throw new BufferedFeedLoginError("backend_user");
  if (row.rolsuper !== false) throw new BufferedFeedLoginError("superuser");
  if (row.rolbypassrls !== false) throw new BufferedFeedLoginError("bypassrls");
  if (row.rolcreaterole !== false || row.rolcreatedb !== false || row.rolreplication !== false || row.rolinherit !== false)
    throw new BufferedFeedLoginError("role_attributes");
  if (row.memberships !== 0) throw new BufferedFeedLoginError("role_membership");
  if (typeof row.members !== "number") throw new BufferedFeedLoginError("login_unreadable");
  for (const [k, v] of Object.entries(BUFFERED_FEED_TIMEOUTS)) if (row[k] !== v) throw new BufferedFeedLoginError("timeouts");
  if (typeof row.server_version !== "number") throw new BufferedFeedLoginError("login_unreadable");
  if (row.server_version >= 160000) {
    let systemUser: unknown;
    try {
      const { rows } = await sql.query<{ system_user: unknown }>(SYSTEM_USER_SQL);
      if (rows.length !== 1) throw new Error();
      systemUser = rows[0]!.system_user;
    } catch {
      throw new BufferedFeedLoginError("login_unreadable");
    }
    // "<auth method>:<authenticated identity>"; NULL means trust auth, which no password proves.
    if (typeof systemUser !== "string" || !systemUser.includes(":") || systemUser.slice(systemUser.indexOf(":") + 1) !== ROLE)
      throw new BufferedFeedLoginError("system_user");
    let members: Record<string, unknown>[];
    try {
      ({ rows: members } = await sql.query<Record<string, unknown>>(MEMBERS_SQL));
    } catch {
      throw new BufferedFeedLoginError("login_unreadable");
    }
    if (members.length !== row.members || !members.every(isSupabaseCreatorEdge)) throw new BufferedFeedLoginError("role_members");
  } else if (row.members !== 0) {
    throw new BufferedFeedLoginError("role_members");
  }
}

/** The slice of pg.Pool / pg.PoolClient the checked executor needs. */
export interface FeedPoolClient extends LoginQuery { release(destroy?: Error | boolean): void }
export interface FeedPool { connect(): Promise<FeedPoolClient> }

/**
 * The feeder's SqlExecutor: every statement runs on a pooled backend that has passed
 * assertBufferedFeedLogin on that same backend. A success is remembered per backend only, so a
 * replaced connection is checked again; a refusal destroys the backend, is never cached, and
 * surfaces as a non-retryable BufferedFeedError(`login_<code>`) before any feed RPC runs.
 */
export function loginCheckedFeedExecutor(pool: FeedPool, assert: (sql: LoginQuery) => Promise<void> = assertBufferedFeedLogin): SqlExecutor {
  const verified = new WeakSet<object>();
  return {
    async query<R = Record<string, unknown>>(text: string, params?: unknown[]) {
      const client = await pool.connect();
      if (!verified.has(client)) {
        try {
          await assert(client);
        } catch (error) {
          const code = error instanceof BufferedFeedLoginError ? error.code : "login_unreadable";
          client.release(new Error("buffered search feed login refused"));
          throw new BufferedFeedError(`login_${code}`, false);
        }
        verified.add(client);
      }
      try {
        const r = await client.query<R>(text, params);
        client.release();
        return { rows: r.rows };
      } catch (error) {
        client.release();
        throw error;
      }
    },
  };
}
