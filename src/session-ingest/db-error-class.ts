/**
 * Closed classification of a session-ingest database-client error, for attribution of
 * the private Worker's intermittent 503s. Diagnostic only: it never changes status,
 * retry, timeout, pool or connection behavior.
 *
 * The error is inspected in RAM, from ONE guarded snapshot of its `code`, `message` and
 * `name` (a throwing getter or proxy trap reads as absent; nothing here can throw). Only
 * closed values ever leave this module:
 *   db_error_class  terminated | reset | timeout | pool | tls | unknown
 *   db_error_code   a syntactically valid SQLSTATE (5 x [0-9A-Z]) or one of the finite
 *                   reviewed errno/TLS codes in SAFE_CODES, else "db_error"
 *   db_error_basis  code | message | none   (what the class was derived from)
 *   error_name      an allowlisted constructor name, else "other"
 * The error's message, stack, connection string/URL, query text, any other code value or
 * any user/session data are never returned, logged or interpolated.
 *
 * The class is a HEURISTIC, not a cause: ECONNREFUSED is not proof of pool exhaustion and
 * 57014 can be a cancellation as well as a timeout. That is why the safe code and the basis
 * travel next to it. Do not diagnose a network hop or change a limit from the class alone.
 *
 * Classes (first match wins, codes before message patterns):
 *   terminated  57P01-57P03, 08003/08006; "connection terminated", "server closed the connection", ...
 *   reset       ECONNRESET/EPIPE/ECONNABORTED; "socket hang up", workerd "Network connection lost", ...
 *   timeout     ETIMEDOUT/ESOCKETTIMEDOUT; 57014 (statement timeout OR cancel); 25P03
 *               (idle_in_transaction_session_timeout: the server ends the session); 57P05
 *               (idle_session_timeout); "timeout", "timed out". The SQLSTATE is kept as
 *               db_error_code, so a statement timeout, an idle-in-transaction kill and a
 *               pooler/transport drop (terminated/reset, usually code-less) stay distinct.
 *   pool        53300/53400, ECONNREFUSED, 08001/08004; "too many clients", "remaining connection slots", ...
 *   tls         the reviewed TLS/certificate codes in SAFE_CODES; "certificate", "tls", "ssl", "handshake"
 *   unknown     anything else (including no usable code or message)
 */
export type DbErrorClass = "terminated" | "reset" | "timeout" | "pool" | "tls" | "unknown";
export type DbErrorBasis = "code" | "message" | "none";

export const DB_ERROR_CLASSES: readonly DbErrorClass[] = Object.freeze(["terminated", "reset", "timeout", "pool", "tls", "unknown"]);
export const DB_ERROR_BASES: readonly DbErrorBasis[] = Object.freeze(["code", "message", "none"]);
/** The fixed fallback for any code that is not a valid SQLSTATE or a reviewed errno/TLS code. */
export const DB_ERROR_CODE_FALLBACK = "db_error";

/** Constructor names that are safe to log verbatim. Anything else is "other". */
export const DB_ERROR_NAMES: readonly string[] = Object.freeze(["Error", "TypeError", "RangeError", "DatabaseError", "AbortError", "TimeoutError", "SystemError"]);

const CLASS_SET: ReadonlySet<string> = new Set(DB_ERROR_CLASSES);
const NAME_SET: ReadonlySet<string> = new Set(DB_ERROR_NAMES);
const SQLSTATE = /^[0-9A-Z]{5}$/;

/** SQLSTATEs with a fixed class. A Map: no inherited keys (`constructor`, `__proto__`, ...). */
const SQLSTATE_CLASS: ReadonlyMap<string, DbErrorClass> = new Map<string, DbErrorClass>([
  ["57P01", "terminated"], ["57P02", "terminated"], ["57P03", "terminated"], ["08003", "terminated"], ["08006", "terminated"],
  ["57014", "timeout"], ["25P03", "timeout"], ["57P05", "timeout"],
  ["53300", "pool"], ["53400", "pool"], ["08001", "pool"], ["08004", "pool"],
]);

/** The FINITE reviewed set of non-SQLSTATE codes that may be logged verbatim, each with
 * its class. Any other errno, TLS-prefixed or opaque code is logged as the fallback. */
const SAFE_CODES: ReadonlyMap<string, DbErrorClass> = new Map<string, DbErrorClass>([
  ["ECONNRESET", "reset"], ["EPIPE", "reset"], ["ECONNABORTED", "reset"],
  ["ETIMEDOUT", "timeout"], ["ESOCKETTIMEDOUT", "timeout"],
  ["ECONNREFUSED", "pool"],
  ["ENOTFOUND", "unknown"], ["EAI_AGAIN", "unknown"], ["EHOSTUNREACH", "unknown"], ["ENETUNREACH", "unknown"],
  ["CERT_HAS_EXPIRED", "tls"], ["CERT_NOT_YET_VALID", "tls"], ["CERT_UNTRUSTED", "tls"],
  ["DEPTH_ZERO_SELF_SIGNED_CERT", "tls"], ["SELF_SIGNED_CERT_IN_CHAIN", "tls"],
  ["UNABLE_TO_VERIFY_LEAF_SIGNATURE", "tls"], ["UNABLE_TO_GET_ISSUER_CERT", "tls"], ["UNABLE_TO_GET_ISSUER_CERT_LOCALLY", "tls"],
  ["ERR_TLS_CERT_ALTNAME_INVALID", "tls"], ["ERR_TLS_HANDSHAKE_TIMEOUT", "tls"],
  ["ERR_SSL_WRONG_VERSION_NUMBER", "tls"],
]);

const MESSAGE_CLASS: readonly (readonly [RegExp, DbErrorClass])[] = [
  [/connection terminated|terminating connection|server closed the connection|connection ended unexpectedly|not queryable/, "terminated"],
  [/connection reset|econnreset|socket hang up|broken pipe|network connection lost|connection lost/, "reset"],
  [/timed? ?out|timeout/, "timeout"],
  [/too many (?:clients|connections)|remaining connection slots|connection limit|max client|pool/, "pool"],
  [/certificate|\btls\b|\bssl\b|handshake/, "tls"],
];

/** The closed diagnosis carried next to a ledger 503 and logged on the route line. */
export interface DbErrorDiagnosis {
  readonly db_error_class: DbErrorClass;
  readonly db_error_code: string;
  readonly db_error_basis: DbErrorBasis;
}

/** One guarded read of an error's code, message (bounded, lower-cased) and name. In RAM
 * only: never logged. Every consumer of a caught DB error (the ledger's P0W01 check, its
 * ledger code and this diagnosis) reads THIS snapshot, so a throwing getter or proxy trap
 * cannot escape a catch path. */
export interface DbErrorSnapshot { readonly code?: string; readonly message?: string; readonly name?: string }
type Snapshot = DbErrorSnapshot;

/** One read of a property; any throw (getter, proxy trap, revoked proxy) reads as absent. */
function readString(target: unknown, key: "code" | "message" | "name"): string | undefined {
  try {
    if (target === null || (typeof target !== "object" && typeof target !== "function")) return undefined;
    const value: unknown = (target as Record<string, unknown>)[key];
    return typeof value === "string" ? value : undefined;
  } catch {
    return undefined;
  }
}

function snapshot(error: unknown): Snapshot {
  const message = readString(error, "message");
  // In RAM only; bounded, lower-cased, matched and discarded.
  return { code: readString(error, "code"), message: message === undefined ? undefined : message.slice(0, 2048).toLowerCase(), name: readString(error, "name") };
}

function closedClass(value: DbErrorClass | undefined): DbErrorClass | undefined {
  return value !== undefined && CLASS_SET.has(value) ? value : undefined;
}

/** A code that may be logged: valid SQLSTATE syntax or a reviewed errno/TLS code; else the fallback. */
function safeCode(code: string | undefined): string {
  if (code === undefined) return DB_ERROR_CODE_FALLBACK;
  if (SQLSTATE.test(code) || SAFE_CODES.has(code)) return code;
  return DB_ERROR_CODE_FALLBACK;
}

function diagnose(s: Snapshot): DbErrorDiagnosis {
  const code = safeCode(s.code);
  const byCode = code === DB_ERROR_CODE_FALLBACK ? undefined : closedClass(SQLSTATE_CLASS.get(code) ?? SAFE_CODES.get(code));
  if (byCode !== undefined && byCode !== "unknown") return Object.freeze({ db_error_class: byCode, db_error_code: code, db_error_basis: "code" as const });
  const message = s.message ?? "";
  for (const [pattern, cls] of MESSAGE_CLASS)
    if (pattern.test(message)) return Object.freeze({ db_error_class: cls, db_error_code: code, db_error_basis: "message" as const });
  return Object.freeze({ db_error_class: "unknown" as const, db_error_code: code, db_error_basis: "none" as const });
}

const FALLBACK_DIAGNOSIS: DbErrorDiagnosis = Object.freeze({ db_error_class: "unknown", db_error_code: DB_ERROR_CODE_FALLBACK, db_error_basis: "none" });

const EMPTY_SNAPSHOT: DbErrorSnapshot = Object.freeze({});

/** The guarded snapshot. Never throws; on any inspection failure it is empty. */
export function dbErrorSnapshot(error: unknown): DbErrorSnapshot {
  try {
    return Object.freeze(snapshot(error));
  } catch {
    return EMPTY_SNAPSHOT;
  }
}

/** The closed diagnosis from an already-taken snapshot. Never throws. */
export function dbErrorDiagnosisFrom(s: DbErrorSnapshot): DbErrorDiagnosis {
  try {
    return diagnose(s);
  } catch {
    return FALLBACK_DIAGNOSIS;
  }
}

/** The closed diagnosis of a database error. Never throws. */
export function dbErrorDiagnosis(error: unknown): DbErrorDiagnosis {
  return dbErrorDiagnosisFrom(dbErrorSnapshot(error));
}

export function classifyDbError(error: unknown): DbErrorClass {
  return dbErrorDiagnosis(error).db_error_class;
}

export function dbErrorName(error: unknown): string {
  const name = readString(error, "name");
  return name !== undefined && NAME_SET.has(name) ? name : "other";
}

/** The log fields for a pg Client 'error' event: the safe code, the class, its basis and an
 * allowlisted name. Never throws (it runs inside the pg 'error' listener). */
export function dbClientErrorFields(error: unknown): { error_code: string; db_error_class: DbErrorClass; db_error_basis: DbErrorBasis; error_name: string } {
  try {
    const s = snapshot(error);
    const d = diagnose(s);
    return { error_code: d.db_error_code, db_error_class: d.db_error_class, db_error_basis: d.db_error_basis, error_name: s.name !== undefined && NAME_SET.has(s.name) ? s.name : "other" };
  } catch {
    return { error_code: DB_ERROR_CODE_FALLBACK, db_error_class: "unknown", db_error_basis: "none", error_name: "other" };
  }
}
