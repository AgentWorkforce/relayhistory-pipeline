/**
 * Error codes shared by every adapter, mirroring the Rust crate's `Error`
 * (`plugins/object-store/rust/src/error.rs` in relayhistory). Callers branch on
 * `code`, never on message text.
 */
export type ObjectStoreErrorCode =
  | "not_found"
  /** A create (`ifNoneMatch`) found an object with *different* content. */
  | "already_exists"
  /** A compare-and-swap (`ifMatch`) lost a race, or the object is missing. */
  | "precondition_failed"
  /** The body did not hash to the caller's `contentSha256`. */
  | "content_mismatch"
  | "invalid_key"
  | "invalid_argument"
  | "unsupported"
  | "unauthorized"
  /** Network failure, 408, 429 or 5xx: safe to retry with backoff. */
  | "transient"
  | "backend";

export class ObjectStoreError extends Error {
  readonly code: ObjectStoreErrorCode;
  readonly backend: string;
  readonly status: number | undefined;
  /**
   * A request that failed below HTTP (no status): the closed class of its transport failure
   * (storeTransportCause), diagnostic only. Never the message, a key or a URL.
   */
  readonly transportCause: StoreTransportCause | undefined;

  constructor(
    code: ObjectStoreErrorCode,
    backend: string,
    message: string,
    options: { status?: number; cause?: unknown; transportCause?: StoreTransportCause } = {},
  ) {
    super(`${backend}: ${message}`, { cause: options.cause });
    this.name = "ObjectStoreError";
    this.code = code;
    this.backend = backend;
    this.status = options.status;
    this.transportCause = options.transportCause;
  }

  get retryable(): boolean {
    return this.code === "transient";
  }
}

export function isObjectStoreError(
  err: unknown,
  code?: ObjectStoreErrorCode,
): err is ObjectStoreError {
  return (
    err instanceof ObjectStoreError && (code === undefined || err.code === code)
  );
}

/** Map an HTTP status that is not otherwise handled. Never includes credentials. */
export function errorForStatus(
  backend: string,
  key: string,
  status: number,
  body: string,
): ObjectStoreError {
  const detail = `${key}: HTTP ${status}${body ? `: ${body.slice(0, 300)}` : ""}`;
  if (status === 404)
    return new ObjectStoreError("not_found", backend, detail, { status });
  if (status === 412)
    return new ObjectStoreError("precondition_failed", backend, detail, {
      status,
    });
  if (status === 401 || status === 403)
    return new ObjectStoreError("unauthorized", backend, detail, { status });
  if (status === 408 || status === 429 || status >= 500)
    return new ObjectStoreError("transient", backend, detail, { status });
  return new ObjectStoreError("backend", backend, detail, { status });
}

/** The closed set of transport-failure classes a status-less store error can carry. */
export const STORE_TRANSPORT_CAUSES = [
  "econnreset", "etimedout", "econnrefused", "epipe", "abort", "timeout", "dns", "tls", "socket", "other",
] as const;
export type StoreTransportCause = (typeof STORE_TRANSPORT_CAUSES)[number];

function causeTag(e: unknown): string {
  if (typeof e !== "object" || e === null) return "";
  const o = e as { code?: unknown; name?: unknown; errno?: unknown };
  const parts = [o.code, o.name, o.errno].filter((v) => typeof v === "string" || typeof v === "number");
  return parts.join(" ").toLowerCase();
}

/**
 * Classify a fetch failure into a closed class from its `code` / `name` / `errno` only (Node and Bun
 * spellings), looking one `cause` level down. Never reads the message, so a key or URL cannot leak.
 */
export function storeTransportCause(error: unknown): StoreTransportCause {
  const nested = typeof error === "object" && error !== null ? (error as { cause?: unknown }).cause : undefined;
  const tag = `${causeTag(error)} ${causeTag(nested)}`;
  if (/econnreset|connectionreset/.test(tag)) return "econnreset";
  if (/etimedout/.test(tag)) return "etimedout";
  if (/econnrefused|connectionrefused/.test(tag)) return "econnrefused";
  if (/epipe|brokenpipe/.test(tag)) return "epipe";
  // A socket the peer aborted (ECONNABORTED) is a socket failure, not a caller's abort signal.
  if (/econnaborted/.test(tag)) return "socket";
  if (/aborterror|abort/.test(tag)) return "abort";
  if (/timeout/.test(tag)) return "timeout";
  if (/enotfound|eai_again|dns/.test(tag)) return "dns";
  if (/cert|tls|ssl/.test(tag)) return "tls";
  if (/socket|connectionclosed|econnaborted|und_err|closed/.test(tag)) return "socket";
  return "other";
}
