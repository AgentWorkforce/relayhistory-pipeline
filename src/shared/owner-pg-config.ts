/** Discrete, pinned pg client config for an operator OWNER login (the bookkeeping-cleanup tool).
 *
 * A raw `connectionString` lets URL parameters override what the caller pinned: `?sslmode=disable`
 * turns TLS off, `?sslmode=no-verify` keeps TLS but skips verification, `?application_name=` replaces
 * the pinned name, and a missing user/password silently falls back to PG* environment variables or
 * ~/.pgpass. So this NEVER passes a connectionString. It requires an explicit
 * postgresql://user:password@host[:port]/database, accepts at most the one normalized parameter
 * `sslmode=verify-full`, and builds every field itself:
 *  - TLS is always verified against the pinned Supabase Root 2021 CA;
 *  - application_name is fixed;
 *  - `options` is non-empty, so PGOPTIONS is never consulted;
 *  - a non-empty password string means pg never reads .pgpass.
 */
import type { ClientConfig } from "pg";
import { SUPABASE_PROD_CA_2021 } from "../session-pipeline/supabase-ca.ts";

export class OwnerUrlError extends Error { override name = "OwnerUrlError"; constructor(readonly code: string) { super("owner url refused: " + code); } }
const no = (code: string): never => { throw new OwnerUrlError(code); };

export function ownerClientConfig(ownerUrl: string, applicationName: string, ca: string = SUPABASE_PROD_CA_2021): ClientConfig {
  if (!/^[a-z0-9-]{1,63}$/.test(applicationName)) no("application_name");
  let u: URL;
  try { u = new URL(ownerUrl); } catch { return no("url"); }
  if (u.protocol !== "postgresql:" && u.protocol !== "postgres:") no("protocol");
  if (u.hash) no("fragment");
  const params = [...u.searchParams.entries()];
  if (params.length > 1 || (params.length === 1 && (params[0]![0] !== "sslmode" || params[0]![1] !== "verify-full"))) no("query_params");
  const host = u.hostname, user = decodeURIComponent(u.username), password = decodeURIComponent(u.password);
  if (!host || host.startsWith("[") === false && !/^[A-Za-z0-9.-]{1,253}$/.test(host)) no("host");
  if (!user) no("user"); if (!password) no("password");
  const database = decodeURIComponent(u.pathname.replace(/^\//, ""));
  if (!database || database.includes("/")) no("database");
  const port = u.port ? Number(u.port) : 5432;
  if (!Number.isInteger(port) || port < 1 || port > 65535) no("port");
  return {
    host, port, user, password, database, application_name: applicationName,
    ssl: { rejectUnauthorized: true, ca, servername: host },
    options: "-c idle_in_transaction_session_timeout=60000",
  };
}
