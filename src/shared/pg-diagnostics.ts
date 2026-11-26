/**
 * Allow-listed Postgres diagnostics for a failure log line: which table,
 * constraint, C routine and first PL/pgSQL frame raised the error, and a
 * fixed-vocabulary class of its message. The raw message, detail, hint and
 * full `where` stack are never logged: they can carry SQL, ids or row content.
 *
 * Every value is either a fixed token or a string that matches a strict
 * identifier or frame shape; anything else is dropped (table, constraint,
 * routine) or mapped to "other" (frame, message class). The log allow-list
 * (log.ts) re-checks each value against the same shapes.
 */

/** The class of a known RAISE (or native) message: matched exactly (`exact`) or by prefix (`prefix`). */
export interface PgMessageClass {
  token: string;
  match: "exact" | "prefix";
  text: string;
}

/**
 * Known messages raised with SQLSTATE 55000, 55P03, 40001 or 40P01 on the pointer and
 * search paths: the guard, fence and history triggers on sessions.session_index /
 * session_directory (supabase/migrations), and native PostgreSQL messages. Order matters
 * only among prefixes; the first match wins.
 */
export const PG_MESSAGE_CLASSES: readonly PgMessageClass[] = [
  // sessions.ingest_publication_guard_pointer (BEFORE trigger on session_index)
  { token: "publication_user_fence_missing", match: "exact", text: "publication: user fence missing" },
  { token: "publication_pointer_mutation_busy", match: "exact", text: "publication: pointer mutation busy" },
  { token: "publication_pointer_fresh_snapshots", match: "exact", text: "publication: pointer mutation requires fresh snapshots" },
  { token: "publication_pointer_cas_not_applied", match: "exact", text: "publication: pointer CAS did not apply" },
  { token: "publication_pointer_scope_busy", match: "exact", text: "publication: pointer scope busy" },
  // sessions.ingest_history_sync_pointer (AFTER trigger on session_index) and the history switch
  { token: "history_carrier_hash_collision", match: "exact", text: "history: carrier hash collision" },
  { token: "history_arm_pointer_tracking", match: "exact", text: "history: arm pointer tracking before the backfill" },
  { token: "history_pointer_refs_untracked", match: "exact", text: "history: pointer references not tracked and backfilled" },
  { token: "history_dangling_pointer_refs", match: "exact", text: "history: dangling pointer references; rerun the pointer backfill" },
  { token: "history_switched_off", match: "exact", text: "history: 620 is switched off" },
  { token: "history_index_writer_in_flight", match: "prefix", text: "history: session_index has an in-flight writer" },
  // sessions.ingest_erase_fence_index_lock (statement trigger on session_index / session_directory)
  { token: "erase_writer_fresh_snapshot", match: "exact", text: "operator erase: writer requires a fresh read-committed snapshot" },
  { token: "erase_read_committed_required", match: "exact", text: "operator erase: read-committed transaction required" },
  // sessions.ingest_acquisition_gate
  { token: "acquisition_session_busy", match: "exact", text: "acquisition: session_busy" },
  // Native PostgreSQL
  { token: "pg_currval_undefined", match: "prefix", text: "currval of sequence" },
  { token: "pg_lastval_undefined", match: "prefix", text: "lastval is not yet defined" },
  { token: "pg_lock_not_obtained", match: "prefix", text: "could not obtain lock" },
  { token: "pg_lock_timeout", match: "prefix", text: "canceling statement due to lock timeout" },
  { token: "pg_serialization_failure", match: "prefix", text: "could not serialize access" },
  { token: "pg_deadlock_detected", match: "prefix", text: "deadlock detected" },
];

export const PG_MESSAGE_CLASS_OTHER = "other";

/** Every value `pg_message_class` may take. */
export const PG_MESSAGE_CLASS_TOKENS: ReadonlySet<string> = new Set([...PG_MESSAGE_CLASSES.map((c) => c.token), PG_MESSAGE_CLASS_OTHER]);

/** The fixed token for an error message; "other" for anything not in the table. */
export function pgMessageClass(message: unknown): string {
  if (typeof message !== "string") return PG_MESSAGE_CLASS_OTHER;
  for (const c of PG_MESSAGE_CLASSES) if (c.match === "exact" ? message === c.text : message.startsWith(c.text)) return c.token;
  return PG_MESSAGE_CLASS_OTHER;
}

/** A PL/pgSQL statement kind as PostgreSQL names it in a context line, upper-cased; others become OTHER. */
const STATEMENT_KINDS: ReadonlySet<string> = new Set([
  "RAISE", "SQL STATEMENT", "PERFORM", "ASSIGNMENT", "IF", "CASE", "EXECUTE", "RETURN", "RETURN QUERY", "RETURN NEXT",
  "GET DIAGNOSTICS", "CALL", "FOR OVER SELECT ROWS", "FOR OVER EXECUTE STATEMENT", "FOR WITH INTEGER LOOP VARIABLE",
  "FOREACH OVER ARRAY", "LOOP", "WHILE", "EXIT", "STATEMENT BLOCK", "OPEN", "FETCH", "CLOSE", "ASSERT", "COMMIT", "ROLLBACK",
]);

const SESSIONS_FUNCTION = /^sessions\.[a-z0-9_]+$/;
const FRAME = /^PL\/pgSQL function ([^\s()]+)\([^)]*\) line ([0-9]{1,6}) at ([A-Za-z][A-Za-z ]{0,39})$/;

/** `pg_where_frame` values: "other", or the normalised first frame. log.ts re-checks this shape. */
export const PG_WHERE_FRAME_SHAPE = /^(?:other|PL\/pgSQL function (?:sessions\.[a-z0-9_]+|other)\(\.\.\.\) line [0-9]{1,6} at [A-Z][A-Z ]{0,39})$/;

/**
 * The first frame of `error.where` as "PL/pgSQL function <name>(...) line N at <KIND>", the
 * name kept only when it is a `sessions.` function, else "other"; "other" when the first
 * frame is not a PL/pgSQL line frame (a SQL statement or SQL function frame can carry SQL).
 */
export function pgWhereFrame(where: unknown): string | undefined {
  if (typeof where !== "string" || where.length === 0) return undefined;
  const first = where.split("\n", 1)[0]!.trim();
  const m = FRAME.exec(first);
  if (!m) return "other";
  const name = SESSIONS_FUNCTION.test(m[1]!) ? m[1]! : "other";
  const kind = m[3]!.trim().replace(/\s+/g, " ").toUpperCase();
  return `PL/pgSQL function ${name}(...) line ${Number(m[2])} at ${STATEMENT_KINDS.has(kind) ? kind : "OTHER"}`;
}

/** `pg_table` / `pg_constraint`: a lower-case unquoted identifier. */
export const PG_IDENTIFIER_SHAPE = /^[a-z_][a-z0-9_]{0,62}$/;
/** `pg_routine`: a C function name. */
export const PG_ROUTINE_SHAPE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const SQLSTATE = /^[0-9A-Z]{5}$/;

/**
 * The allow-listed diagnostics of a Postgres error (an error whose `code` is a SQLSTATE);
 * empty for any other error, so non-database failures log exactly as before.
 */
export function pgErrorFields(err: unknown): Record<string, string> {
  const e = err as { code?: unknown; table?: unknown; constraint?: unknown; routine?: unknown; where?: unknown; message?: unknown } | null | undefined;
  if (!e || typeof e !== "object" || typeof e.code !== "string" || !SQLSTATE.test(e.code)) return {};
  const out: Record<string, string> = {};
  if (typeof e.table === "string" && PG_IDENTIFIER_SHAPE.test(e.table)) out.pg_table = e.table;
  if (typeof e.constraint === "string" && PG_IDENTIFIER_SHAPE.test(e.constraint)) out.pg_constraint = e.constraint;
  if (typeof e.routine === "string" && PG_ROUTINE_SHAPE.test(e.routine)) out.pg_routine = e.routine;
  const frame = pgWhereFrame(e.where);
  if (frame !== undefined) out.pg_where_frame = frame;
  out.pg_message_class = pgMessageClass(e.message);
  return out;
}
