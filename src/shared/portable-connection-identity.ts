/** SQL-owner finding M5: refuse privileged connections before a portable
 * composition does any work. FORCE RLS and tenant settings are the isolation
 * model, and a SUPERUSER or BYPASSRLS role bypasses all RLS silently; the owner
 * (or a member of it) can alter or drop the storage. One read-only catalog query,
 * awaited once at assembly; constructors stay free of I/O. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { LEDGERED_SEARCH_STATE_TABLE } from "../session-pipeline/postgres-search-state.ts";

export type PortableRole = "writer" | "reader";

export class PortableConnectionRefused extends Error {
  constructor(readonly code: string) {
    super(`portable search connection refused: ${code}`);
    this.name = "PortableConnectionRefused";
  }
}

interface IdentityRow {
  present: boolean;
  super: boolean;
  bypassrls: boolean;
  owner_member: boolean | null;
  can_write: boolean | null;
}

export const PORTABLE_IDENTITY_SQL = `SELECT to_regclass($1) IS NOT NULL AS present,
  r.rolsuper AS super, r.rolbypassrls AS bypassrls,
  (SELECT pg_has_role(current_user, c.relowner, 'MEMBER') FROM pg_class c WHERE c.oid = to_regclass($1)) AS owner_member,
  CASE WHEN to_regclass($1) IS NULL THEN NULL ELSE
    has_table_privilege(current_user, $1, 'INSERT') OR has_table_privilege(current_user, $1, 'UPDATE')
    OR has_table_privilege(current_user, $1, 'DELETE') OR has_table_privilege(current_user, $1, 'TRUNCATE') END AS can_write
  FROM pg_roles r WHERE r.rolname = current_user`;

export interface AdmissionOptions {
  /** supabase/bootstrap/portable_search_storage_verify.sql (embedded in the artifact).
   * When given, it runs after the identity checks with hoopdb.runtime_role bound to this
   * connection's own current_user. `sql` must then be ONE transaction (the setting is
   * transaction-local), never a pool. Writer only: the reader has no runtime identity. */
  readonly storageVerifySql?: string;
  /** HOOPDB_DB_MODE, bound as hoopdb.db_mode for the verifier. */
  readonly dbMode?: string;
  readonly table?: string;
}

/** Throws PortableConnectionRefused with a stable code; returns nothing on success.
 * Error text never carries connection strings or row data. */
export async function admitPortableConnection(sql: SqlExecutor, role: PortableRole, options: AdmissionOptions = {}): Promise<void> {
  const table = options.table ?? LEDGERED_SEARCH_STATE_TABLE;
  let row: IdentityRow | undefined;
  try {
    row = (await sql.query<IdentityRow>(PORTABLE_IDENTITY_SQL, [table])).rows[0];
  } catch (error) {
    const code = (error as { code?: unknown })?.code;
    throw new PortableConnectionRefused(typeof code === "string" ? `identity_unreadable_sqlstate_${code}` : "identity_unreadable");
  }
  if (!row) throw new PortableConnectionRefused("identity_unreadable");
  if (row.super) throw new PortableConnectionRefused("superuser");
  if (row.bypassrls) throw new PortableConnectionRefused("bypassrls");
  if (!row.present) throw new PortableConnectionRefused("storage_missing");
  if (row.owner_member) throw new PortableConnectionRefused("storage_owner");
  if (role === "reader" && row.can_write) throw new PortableConnectionRefused("reader_can_write");
  if (options.storageVerifySql !== undefined) {
    if (role !== "writer") throw new PortableConnectionRefused("storage_verify_writer_only");
    try {
      await sql.query("SELECT set_config('hoopdb.runtime_role', current_user, true), set_config('hoopdb.db_mode', $1, true)", [options.dbMode ?? ""]);
      await sql.query(options.storageVerifySql);
    } catch (error) {
      const code = (error as { code?: unknown })?.code;
      throw new PortableConnectionRefused(typeof code === "string" ? `storage_unverified_sqlstate_${code}` : "storage_unverified");
    }
  }
}
