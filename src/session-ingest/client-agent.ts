/**
 * The uploading client, from the request's User-Agent, as closed log fields for the batch route line.
 * Diagnostic only: it never changes admission, status or retry.
 *
 * Only `agent-relay-desktop/<version> <os>` is recognised:
 *   client_kind     desktop | other
 *   client_version  desktop only, when the version is a release version (CLIENT_VERSION, e.g. 2026.11.12 or
 *                   2026.11.12-beta.1); anything else is omitted, never truncated
 *   client_os       desktop only: macos | linux | windows | other
 * Any other User-Agent (absent, a browser, a script, a forged or oversized one) is `client_kind: other`
 * and nothing else: arbitrary User-Agent text is never logged.
 */
export const CLIENT_KINDS = Object.freeze(["desktop", "other"] as const);
export const CLIENT_OSES = Object.freeze(["macos", "linux", "windows", "other"] as const);
export type ClientKind = (typeof CLIENT_KINDS)[number];
export type ClientOs = (typeof CLIENT_OSES)[number];
/** A release version: 2..4 numeric parts and an optional short pre-release tag. Never a token, host or hex id. */
export const CLIENT_VERSION = /^[0-9]{1,5}(?:\.[0-9]{1,5}){1,3}(?:-[0-9A-Za-z.]{1,16})?$/;

export interface ClientFields { client_kind: ClientKind; client_version?: string; client_os?: ClientOs }

/** Bounded before any matching: a real Desktop User-Agent is far shorter. */
export const MAX_USER_AGENT = 256;
/**
 * The Desktop's history upload User-Agent is exactly `agent-relay-desktop/<version> <os>`, where <version> is the
 * desktop release (e.g. 2026.11.12) and <os> is one of macos, linux, windows (relay-app
 * desktop-core/src/history/cloud/history_origin/transport.rs, shipped in stable 2026.11.12). Anything after the
 * OS token is ignored and never logged.
 */
const DESKTOP = /^agent-relay-desktop\/(\S+) (\S+)/;
const OS: ReadonlyMap<string, ClientOs> = new Map<string, ClientOs>([["macos", "macos"], ["linux", "linux"], ["windows", "windows"]]);

const OTHER: ClientFields = Object.freeze({ client_kind: "other" });

/** The closed client fields for a User-Agent header value. Never throws. */
export function clientFields(userAgent: string | null | undefined): ClientFields {
  if (typeof userAgent !== "string" || userAgent.length > MAX_USER_AGENT) return OTHER;
  const m = DESKTOP.exec(userAgent);
  if (!m) return OTHER;
  const version = m[1]!;
  const os = OS.get(m[2]!.toLowerCase()) ?? "other";
  return Object.freeze({ client_kind: "desktop" as const, ...(CLIENT_VERSION.test(version) ? { client_version: version } : {}), client_os: os });
}
