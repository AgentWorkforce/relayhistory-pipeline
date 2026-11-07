/**
 * Closed classification of a session-ingest database-client error, for attribution of
 * the private Worker's intermittent 503s. Diagnostic only: it never changes status,
 * retry, timeout, pool or connection behavior.
 *
 * The error is inspected in RAM. Only two closed values ever leave this module:
 *   db_error_class  terminated | reset | timeout | pool | tls | unknown
 *   error_name      an allowlisted constructor name, else "other"
 * The error's message, stack, connection string/URL, query text or any user/session data
 * are never returned, logged or interpolated.
 *
 * Classes (first match wins, codes before message patterns):
 *   terminated  the server or a proxy ended the session (57P01-57P03, 08003/08006,
 *               "connection terminated", "server closed the connection", ...)
 *   reset       the transport dropped (ECONNRESET/EPIPE/ECONNABORTED, "socket hang up",
 *               workerd "Network connection lost", ...)
 *   timeout     a client/server timeout (ETIMEDOUT, 57014, "timeout", "timed out")
 *   pool        acquisition refused or limited (53300/53400, ECONNREFUSED, 08001/08004,
 *               "too many clients/connections", "remaining connection slots", ...)
 *   tls         TLS/certificate failure (CERT_*, ERR_TLS_*, ERR_SSL_*, "certificate", "tls")
 *   unknown     anything else (including no usable code or message)
 */
export type DbErrorClass = "terminated" | "reset" | "timeout" | "pool" | "tls" | "unknown";

export const DB_ERROR_CLASSES: readonly DbErrorClass[] = ["terminated", "reset", "timeout", "pool", "tls", "unknown"];

/** Constructor names that are safe to log verbatim. Anything else is "other". */
export const DB_ERROR_NAMES: readonly string[] = ["Error", "TypeError", "RangeError", "DatabaseError", "AbortError", "TimeoutError", "SystemError"];

const CODE_CLASS: Readonly<Record<string, DbErrorClass>> = {
  "57P01": "terminated", "57P02": "terminated", "57P03": "terminated", "08003": "terminated", "08006": "terminated",
  ECONNRESET: "reset", EPIPE: "reset", ECONNABORTED: "reset",
  ETIMEDOUT: "timeout", ESOCKETTIMEDOUT: "timeout", "57014": "timeout",
  "53300": "pool", "53400": "pool", ECONNREFUSED: "pool", "08001": "pool", "08004": "pool",
};

const MESSAGE_CLASS: readonly [RegExp, DbErrorClass][] = [
  [/connection terminated|terminating connection|server closed the connection|connection ended unexpectedly|not queryable/, "terminated"],
  [/connection reset|econnreset|socket hang up|broken pipe|network connection lost|connection lost/, "reset"],
  [/timed? ?out|timeout/, "timeout"],
  [/too many (?:clients|connections)|remaining connection slots|connection limit|max client|pool/, "pool"],
  [/certificate|\btls\b|\bssl\b|handshake/, "tls"],
];

export function classifyDbError(error: unknown): DbErrorClass {
  const e = error as { code?: unknown; message?: unknown } | null | undefined;
  const code = typeof e?.code === "string" ? e.code : undefined;
  if (code) {
    const fixed = CODE_CLASS[code];
    if (fixed) return fixed;
    if (/^(?:CERT_|ERR_TLS_|ERR_SSL_|UNABLE_TO_VERIFY_|DEPTH_ZERO_|SELF_SIGNED_)/.test(code)) return "tls";
  }
  // In RAM only; the message is matched, never returned.
  const message = typeof e?.message === "string" ? e.message.slice(0, 2048).toLowerCase() : "";
  for (const [pattern, cls] of MESSAGE_CLASS) if (pattern.test(message)) return cls;
  return "unknown";
}

export function dbErrorName(error: unknown): string {
  const name = (error as { name?: unknown } | null | undefined)?.name;
  return typeof name === "string" && DB_ERROR_NAMES.includes(name) ? name : "other";
}

/** The log fields for a pg Client 'error' event: the existing error_code plus the class. */
export function dbClientErrorFields(error: unknown): { error_code: string; db_error_class: DbErrorClass; error_name: string } {
  const code = (error as { code?: unknown } | null | undefined)?.code;
  return {
    // Unchanged contract: the client's string code, else "db_error".
    error_code: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "db_error",
    db_error_class: classifyDbError(error),
    error_name: dbErrorName(error),
  };
}
