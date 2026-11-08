/**
 * The acquisition exchange's own relaycontrol token verifier.
 *
 * Purpose-specific on purpose: the shared #313 client
 * (packages/pipeline/src/auth/relaycontrol.ts, unchanged and still used by
 * the session-ingest edge) admits only principal_type "desktop-user", only
 * sh_at_ tokens and only its rth:read/rth:sync allowlist, so it cannot carry
 * the ops port's per-workspace service token (sh_wk_, PR #314
 * docs/factory-acquisition-owner-design.md §9.1) or rth:acquire. This module
 * keeps the #313 wire contract (endpoint, request body, service secret,
 * response shape, size cap, outage mapping) by reusing that file's unchanged
 * exports, and differs only in policy:
 *
 *   route   token prefix   principal_type   scope
 *   native  sh_at_         desktop-user     rth:acquire
 *   ops     sh_wk_         service          acquisition:operate   (Q3 cloud/batches: same row when built)
 *
 * The real principal_type is returned; an unknown or absent one is refused
 * (fail closed). Scopes are returned raw (no allowlist): the route check in
 * app.ts asks for exactly one scope. The audience is ACQUISITION_ORIGIN,
 * compared by normalized origin. No cache: every request introspects (bounded
 * by the timeout). Nothing here logs; the token and secret never leave the
 * request body / header they are sent in.
 */
import {
  introspectEndpoint,
  normalizedAudience,
  RELAYCONTROL_MAX_RESPONSE_BYTES,
  RelaycontrolRejectedError,
  RelaycontrolUnavailableError,
} from "@rhc/auth/relaycontrol.ts";

export type AcquisitionGroup = "native" | "ops";

export interface AcquisitionTokenPolicy {
  readonly prefix: string;
  readonly principalType: string;
  readonly scope: string;
}

export const ACQUISITION_POLICIES: Readonly<Record<AcquisitionGroup, AcquisitionTokenPolicy>> = Object.freeze({
  native: Object.freeze({ prefix: "sh_at_", principalType: "desktop-user", scope: "rth:acquire" }),
  ops: Object.freeze({ prefix: "sh_wk_", principalType: "service", scope: "acquisition:operate" }),
});

/** Every principal_type this service understands; anything else is refused. */
export const KNOWN_PRINCIPAL_TYPES: ReadonlySet<string> = new Set(Object.values(ACQUISITION_POLICIES).map((p) => p.principalType));

export const MAX_TOKEN_LENGTH = 4096;
const DEFAULT_TIMEOUT_MS = 5_000;

/** The introspected caller. principalType is relaycontrol's principal_type, verbatim. */
export interface AcquisitionPrincipal {
  /** relaycontrol `sub`; required for desktop-user, optional for a service principal. */
  readonly userId: string | undefined;
  readonly orgId: string;
  readonly workspaceId: string | undefined;
  readonly scopes: readonly string[];
  readonly principalType: string;
}

/**
 * Token -> principal for one route group. Throws RelaycontrolRejectedError
 * (token refused) or anything else (introspection unavailable: fail closed).
 */
export interface Authenticator {
  authenticate(token: string, group: AcquisitionGroup): Promise<AcquisitionPrincipal>;
}

export interface AcquisitionVerifierOptions {
  /** RELAYCONTROL_INTROSPECT_URL (base; the #313 path is appended). */
  readonly url: string;
  /** RELAYCONTROL_INTROSPECT_SECRET. */
  readonly secret: string;
  /** ACQUISITION_ORIGIN: sent as aud, and the echoed aud must match it by normalized origin. */
  readonly origin: string;
  readonly validateUrl?: (url: string) => void;
  readonly fetch?: typeof fetch;
  /** Epoch ms. */
  readonly now?: () => number;
  readonly timeoutMs?: number;
}

/** The shape check app.ts runs before introspection; the verifier repeats it. */
export function tokenFitsGroup(token: string, group: AcquisitionGroup): boolean {
  return token.startsWith(ACQUISITION_POLICIES[group].prefix) && token.length <= MAX_TOKEN_LENGTH && !/\s/.test(token);
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

/** At most `limit` bytes of the body, or null when larger (the rest is cancelled, never buffered). */
async function readCapped(response: Response, limit: number): Promise<string | null> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > limit) {
    await response.body?.cancel().catch(() => {});
    return null;
  }
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

export function createAcquisitionVerifier(opts: AcquisitionVerifierOptions): Authenticator {
  if (!opts.secret?.trim()) throw new Error("RELAYCONTROL_INTROSPECT_SECRET is required");
  const endpoint = introspectEndpoint(opts.url);
  opts.validateUrl?.(opts.url.trim());
  const audience = normalizedAudience(opts.origin);
  if (audience === null) throw new Error("ACQUISITION_ORIGIN must be an http(s) origin");
  const secret = opts.secret.trim();
  const doFetch = opts.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const now = opts.now ?? (() => Date.now());
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const failure = (error: unknown) => {
    const name = (error as { name?: string } | null)?.name;
    return new RelaycontrolUnavailableError(name === "TimeoutError" || name === "AbortError" ? "timeout" : "network_error");
  };

  return {
    async authenticate(token, group) {
      if (!tokenFitsGroup(token, group)) throw new RelaycontrolRejectedError("wrong_token_class");
      let response: Response;
      try {
        response = await doFetch(endpoint, {
          method: "POST",
          headers: { authorization: `Bearer ${secret}`, "content-type": "application/json", accept: "application/json" },
          body: JSON.stringify({ token, aud: opts.origin }),
          redirect: "manual",
          signal: AbortSignal.timeout(timeoutMs),
        });
      } catch (error) {
        throw failure(error);
      }
      // 401 (our secret), 400 (contract drift), 404 (introspection off), 3xx, 5xx: an outage, never the caller's fault.
      if (response.status !== 200) {
        await response.body?.cancel().catch(() => {});
        throw new RelaycontrolUnavailableError(`status_${response.status}`);
      }
      let text: string | null;
      try {
        text = await readCapped(response, RELAYCONTROL_MAX_RESPONSE_BYTES);
      } catch (error) {
        throw failure(error);
      }
      if (text === null) throw new RelaycontrolUnavailableError("response_too_large");
      let body: Record<string, unknown>;
      try {
        const parsed: unknown = JSON.parse(text);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not an object");
        body = parsed as Record<string, unknown>;
      } catch {
        throw new RelaycontrolUnavailableError("malformed_json");
      }
      if (typeof body.active !== "boolean") throw new RelaycontrolUnavailableError("malformed_response");
      if (!body.active) throw new RelaycontrolRejectedError("inactive");
      const scopes = body.scopes;
      if (
        !nonEmpty(body.org) || !nonEmpty(body.workspace) || !nonEmpty(body.aud) || !nonEmpty(body.token_id) ||
        !Array.isArray(scopes) || !scopes.every((s) => typeof s === "string") ||
        typeof body.exp !== "number" || !Number.isFinite(body.exp) ||
        typeof body.iat !== "number" || !Number.isFinite(body.iat) ||
        !(body.sub === undefined || body.sub === null || typeof body.sub === "string")
      )
        throw new RelaycontrolUnavailableError("malformed_response");
      // Fail closed: an absent, new or unexpected principal_type is refused, never defaulted.
      const principalType = body.principal_type;
      if (typeof principalType !== "string" || !KNOWN_PRINCIPAL_TYPES.has(principalType)) throw new RelaycontrolRejectedError("unknown_principal");
      if (principalType === "desktop-user" && !nonEmpty(body.sub)) throw new RelaycontrolUnavailableError("malformed_response");
      const echoed = normalizedAudience(body.aud);
      if (echoed === null || echoed !== audience) throw new RelaycontrolRejectedError("aud_mismatch");
      if (body.exp * 1000 <= now()) throw new RelaycontrolRejectedError("expired");
      return {
        userId: nonEmpty(body.sub) ? body.sub : undefined,
        orgId: body.org,
        workspaceId: body.workspace,
        scopes: [...new Set(scopes as string[])],
        principalType,
      };
    },
  };
}
