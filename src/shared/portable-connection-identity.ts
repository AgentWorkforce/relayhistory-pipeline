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

/** Throws PortableConnectionRefused with a stable code; returns nothing on success.
 * Error text never carries connection strings or row data. */
export async function admitPortableConnection(sql: SqlExecutor, role: PortableRole, table: string = LEDGERED_SEARCH_STATE_TABLE): Promise<void> {
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
}
