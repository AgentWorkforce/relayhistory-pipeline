/** Full admission for the portable READER (048000 reader contract), at assembly and at
 * every completeness read. Distinct from the writer: the reader is the FIXED role
 * bulls96_read_api on its own startup connection, and the storage verifier is bound to an
 * INDEPENDENTLY configured writer role (PORTABLE_SEARCH_WRITER_ROLE), never to the reader
 * and never inferred from policy contents. Current-tick semantics as schemaVerdict's:
 * READ COMMITTED, ledger membership, marker, both verifiers, then a FINAL marker read. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../session-read-api/liveness.ts";
import { PortableConnectionRefused, admitPortableConnection } from "./portable-connection-identity.ts";

export const PORTABLE_READER_ROLE = "bulls96_read_api";
/** Literal (not imported from db.ts/migrate.ts) so the Lambda bundle stays asset-only;
 * pinned equal to the shared constants by portable-reader-admission.test.ts. */
export const READER_MARKER_TABLE = "sessions.__96bulls_role_validation";
export const READER_MARKER_NAME = "session_directory_search_worker";
export const READER_LEDGER_TABLE = "sessions.__96bulls_migrations";
const READINESS_REFUSALS = new Set(["schema_pending", "marker_missing", "generation_mismatch", "validation_pending"]);
/** The reader-only readiness RPC (048000); returns one bounded status code. */
export const READER_READINESS_SQL = "SELECT sessions.portable_reader_readiness($1::text[], $2::text) AS v";

export interface ReaderAdmission {
  readonly generation: string;
  readonly expectedNames: readonly string[];
  readonly storageVerifySql: string;
  readonly readerVerifySql: string;
  /** The independently approved writer role (configuration, never inferred). */
  readonly writerRole: string;
  readonly dbMode?: string;
}

/** PORTABLE_SEARCH_WRITER_ROLE: required for an admitted portable reader. */
export function readerWriterRole(env: Readonly<Record<string, string | undefined>>): string {
  const role = env.PORTABLE_SEARCH_WRITER_ROLE;
  if (!role || !/^[a-z_][a-z0-9_]{0,62}$/.test(role)) throw new PortableConnectionRefused("reader_writer_role_unset");
  if (role === PORTABLE_READER_ROLE) throw new PortableConnectionRefused("reader_writer_role_is_reader");
  return role;
}

/** The gate, inside ONE transaction already bound to a tenant or to none. */
export async function portableReaderGate(tx: SqlExecutor, a: ReaderAdmission): Promise<void> {
  if (!a.storageVerifySql?.trim() || !a.readerVerifySql?.trim()) throw new PortableConnectionRefused("reader_verifier_missing");
  const names = a.expectedNames.filter(n => !/^(shim|bootstrap|verify):/.test(n));
  if (!names.length) throw new PortableConnectionRefused("reader_expected_names_missing");
  const who = (await tx.query<{ cu: string; su: string; iso: string; ro: string; dro: string; st: string }>(
    `SELECT current_user AS cu, session_user AS su, current_setting('transaction_isolation') AS iso,
            current_setting('transaction_read_only') AS ro, current_setting('default_transaction_read_only') AS dro,
            current_setting('statement_timeout') AS st`)).rows[0];
  // The fixed reader on its own startup identity (not SET ROLE from another login).
  if (!who || who.cu !== PORTABLE_READER_ROLE || who.su !== PORTABLE_READER_ROLE) throw new PortableConnectionRefused("reader_identity");
  if (who.iso !== "read committed") throw new PortableConnectionRefused("reader_isolation");
  // BEGIN READ ONLY forces transaction_read_only; the startup default is checked separately.
  if (who.ro !== "on") throw new PortableConnectionRefused("reader_not_read_only");
  if (who.dro !== "on") throw new PortableConnectionRefused("reader_default_not_read_only");
  if (!boundedTimeout(who.st)) throw new PortableConnectionRefused("reader_statement_timeout");
  // Readiness through the reader-only SECURITY DEFINER RPC (048000): the fixed reader may
  // not SELECT the ledger or the marker. A REAL query each time (no cached result).
  const ready = async () => {
    let verdict: string | undefined;
    try {
      verdict = (await tx.query<{ v: string }>(READER_READINESS_SQL, [names, a.generation])).rows[0]?.v;
    } catch (error) {
      const sqlstate = (error as { code?: unknown })?.code;
      throw new PortableConnectionRefused(typeof sqlstate === "string" ? `reader_readiness_sqlstate_${sqlstate}` : "reader_readiness_failed");
    }
    if (verdict === "pass") return;
    // Only the reviewed status enum is mapped; anything else is a fixed code, never echoed.
    throw new PortableConnectionRefused(READINESS_REFUSALS.has(verdict ?? "") ? `reader_${verdict}` : "reader_readiness_unknown");
  };
  await ready(); // initial
  await run(tx, a.readerVerifySql, "reader_contract_unverified");
  await tx.query("SELECT set_config('hoopdb.runtime_role', $1, true), set_config('hoopdb.db_mode', $2, true)", [a.writerRole, a.dbMode ?? ""]);
  await run(tx, a.storageVerifySql, "reader_storage_unverified");
  await ready(); // FINAL read, after both verifiers, immediately before work
}

async function run(tx: SqlExecutor, sql: string, code: string): Promise<void> {
  try { await tx.query(sql); }
  catch (error) {
    const sqlstate = (error as { code?: unknown })?.code;
    throw new PortableConnectionRefused(typeof sqlstate === "string" ? `${code}_sqlstate_${sqlstate}` : code);
  }
}

/** statement_timeout must be set and at most 2 s (the reader bootstrap's bound). */
function boundedTimeout(value: string): boolean {
  const m = /^(\d+)(ms|s|min)?$/.exec(value.trim());
  if (!m) return false;
  const ms = Number(m[1]) * (m[2] === "s" ? 1000 : m[2] === "min" ? 60_000 : 1);
  return ms > 0 && ms <= 2000;
}

/** Assembly: one dedicated READ ONLY, READ COMMITTED transaction on the reader pool. */
export async function admitPortableReader(pool: { connect(): Promise<{ query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void }> }, a: ReaderAdmission): Promise<void> {
  const client = await pool.connect();
  const tx: SqlExecutor = { async query<R>(text: string, params?: unknown[]) { return { rows: (await client.query(text, params)).rows as R[] }; } };
  try {
    await client.query("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
    // Fixed-identity gate first, so substitution is attributed to reader_identity; the
    // privilege refusals (superuser/BYPASSRLS/owner/write-capable) follow in the same tx.
    await portableReaderGate(tx, a);
    await admitPortableConnection(tx, "reader");
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally { client.release(); }
}

/** Every completeness read runs the gate first, inside its own tenant transaction. */
export function gatedReaderRunner(base: TenantRunner, a: ReaderAdmission): TenantRunner {
  return (orgId, workspaceId, work) => base(orgId, workspaceId, async (tx) => {
    await portableReaderGate(tx, a);
    return work(tx);
  });
}

/**
 * The gated reader's tenant transaction: identical to poolTenantRunner except that READ
 * COMMITTED is fixed in BEGIN itself, i.e. before the tenant SELECT set_config (the first
 * snapshot-taking statement), so the gate's final marker read is fresh even when the
 * database default is REPEATABLE READ. poolTenantRunner (liveness/directory) is unchanged.
 */
export function readCommittedTenantRunner(pool: {
  connect(): Promise<{ query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void }>;
}): TenantRunner {
  return async (orgId, workspaceId, work) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
      await client.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      const result = await work({
        async query<R>(text: string, params?: unknown[]) {
          const r = await client.query(text, params);
          return { rows: r.rows as R[] };
        },
      });
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}
