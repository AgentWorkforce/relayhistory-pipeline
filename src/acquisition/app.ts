/**
 * HTTP surface of the acquisition exchange (SERVICE=acquisition). Verify-only:
 * the pipeline never mints, signs or stores acquisition credentials;
 * relaycontrol is the sole issuer. One route group serves requests:
 *
 *   ops (relaycontrol issuer -> History, per-workspace service principal, scope acquisition:operate)
 *     POST /v1/acquisition/ops/install | revoke | rebind | status
 *     Each mutation carries an assertion signed by relaycontrol; installer.ts verifies it
 *     against the pinned cloud-class keys (#322 verifier) and forwards it to the SQL installer.
 *
 * main.ts mounts ops on its OWN private listener. The other listener
 * (ACQUISITION_PORT) is a dormant app: /healthz, and 503 acquisition_disabled
 * for everything else. There is no native (Desktop-facing) route group.
 *
 * Auth: Bearer only, verified by this service's own introspection client
 * (verifier.ts: #313 wire contract, per-route prefix / principal_type / scope,
 * aud = ACQUISITION_ORIGIN compared by normalized origin), identity only from
 * the introspection answer, fail closed (503) when introspection is
 * unavailable. Every ops refusal is 403 not_authorized (contract P2), including
 * a missing bearer. No client header is forwarded anywhere. The bearer, the
 * signature and key material are never logged: the logger receives only
 * allowlisted fields built here.
 */
import { Hono, type Context } from "hono";
import { RelaycontrolRejectedError } from "@rhc/auth/relaycontrol.ts";
import { log as sharedLog, type Level } from "../shared/log.ts";
import { httpForError, type Installer } from "./installer.ts";
import type { AcquisitionKind } from "./validation.ts";
import { ACQUISITION_POLICIES, tokenFitsGroup, type AcquisitionGroup, type AcquisitionPrincipal, type Authenticator } from "./verifier.ts";

export const OPS_SCOPE = ACQUISITION_POLICIES.ops.scope;
export const OPS_PRINCIPAL = ACQUISITION_POLICIES.ops.principalType;
export const MAX_BODY_BYTES = 64 * 1024;
/**
 * Request-target (path + query) cap in UTF-8 bytes of the percent-encoded
 * target; over it is 414 request_uri_too_long, independent of the 500-id cap.
 * Below Node's default 16 KiB header-block limit (which would answer 431 first).
 */
export const MAX_REQUEST_TARGET_BYTES = 8 * 1024;
const CORRELATION_ID = /^[A-Za-z0-9._:-]{1,128}$/;

export type { AcquisitionGroup, AcquisitionPrincipal, Authenticator };

export type AcquisitionLog = (level: Level, message: string, fields: Record<string, unknown>) => void;

export type AcquisitionAppDeps =
  /** Dormant: /healthz, every other route 503 acquisition_disabled. The only shape for the native listener. */
  | { readonly enabled: false; readonly group: AcquisitionGroup; readonly log?: AcquisitionLog }
  | {
      readonly enabled: true;
      readonly group: "ops";
      /** ACQUISITION_OPS_ENABLED. */
      readonly groupEnabled: boolean;
      readonly authenticator: Authenticator;
      readonly installer: Installer | null;
      readonly log?: AcquisitionLog;
    };

type Env = { Variables: { correlationId: string } };
type Answer = { status: number; body: unknown };

class HttpRefusal extends Error {
  constructor(readonly status: 400 | 403 | 404 | 413 | 414 | 415 | 503, readonly code: string) {
    super(code);
    this.name = "HttpRefusal";
  }
}

const OPS_ROUTES = new Set(["POST install", "POST revoke", "POST rebind", "POST status"]);

function bearer(header: string | undefined): string | null {
  const match = /^Bearer[ \t]+(\S+)[ \t]*$/i.exec(header ?? "");
  return match ? match[1]! : null;
}

/** The body, at most MAX_BODY_BYTES, as parsed JSON. */
async function jsonBody(c: Context<Env>): Promise<unknown> {
  if ((c.req.header("Content-Type") ?? "").split(";")[0]!.trim().toLowerCase() !== "application/json") throw new HttpRefusal(415, "unsupported_media_type");
  const declared = c.req.header("Content-Length");
  if (declared !== undefined && (!/^[0-9]+$/.test(declared) || Number(declared) > MAX_BODY_BYTES)) throw new HttpRefusal(413, "payload_too_large");
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = c.req.raw.body?.getReader();
  if (reader)
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_BODY_BYTES) {
        await reader.cancel().catch(() => {});
        throw new HttpRefusal(413, "payload_too_large");
      }
      chunks.push(value);
    }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpRefusal(400, "invalid_request");
  }
}

export function createAcquisitionApp(deps: AcquisitionAppDeps) {
  const app = new Hono<Env>();
  const emit: AcquisitionLog = deps.log ?? ((level, message, fields) => sharedLog[level](message, fields));
  const group = deps.group;
  const prefix = `/v1/acquisition/${group}/`;

  app.use("*", async (c, next) => {
    const given = c.req.header("X-Correlation-Id")?.trim();
    c.set("correlationId", given && CORRELATION_ID.test(given) ? given : crypto.randomUUID());
    await next();
    c.header("X-Correlation-Id", c.get("correlationId"));
    c.header("Cache-Control", "no-store");
  });

  app.get("/healthz", (c) => c.json({ ok: true, enabled: deps.enabled, group }));

  app.all("*", async (c) => {
    const started = Date.now();
    const route = c.req.path.startsWith(prefix) ? `${c.req.method} ${c.req.path.slice(prefix.length)}` : "";
    const known = group === "ops" && OPS_ROUTES.has(route);
    const fields: Record<string, unknown> = { route: known ? `${group} ${route}` : "unknown", correlation_id: c.get("correlationId"), kind: group };
    let answer: Answer;
    try {
      if (!deps.enabled || !deps.groupEnabled) {
        answer = { status: 503, body: { error: { code: "acquisition_disabled" } } };
      } else if (requestTargetBytes(c.req.url) > MAX_REQUEST_TARGET_BYTES) {
        answer = { status: 414, body: { error: { code: "request_uri_too_long" } } };
      } else if (!known) {
        answer = { status: 404, body: { error: { code: "not_found" } } };
      } else {
        const principal = await authenticate(c, deps.authenticator);
        fields.org_id = principal.orgId;
        fields.workspace_id = principal.workspaceId;
        answer = await opsRoute(c, deps.installer, route, principal);
      }
    } catch (error) {
      answer = refusalAnswer(error, group);
      fields.error_code = answer.body && typeof answer.body === "object" ? errorCode(answer.body) : undefined;
      if (answer.status >= 500) fields.error_name = error instanceof Error ? error.name : "unknown";
    }
    fields.status = answer.status;
    fields.outcome = answer.status < 300 ? "ok" : answer.status >= 500 ? "error" : "refused";
    const retryable = (answer.body as { error?: { retryable?: unknown } })?.error?.retryable;
    if (typeof retryable === "boolean") fields.retryable = retryable;
    fields.duration_ms = Date.now() - started;
    emit(answer.status >= 500 ? "warn" : "info", "acquisition request", fields);
    if (answer.status === 503) c.header("Retry-After", errorCode(answer.body as object) === "acquisition_disabled" ? "60" : "5");
    return c.json(answer.body as object, answer.status as 200);
  });
  return app;
}

function requestTargetBytes(raw: string): number {
  const url = new URL(raw);
  return Buffer.byteLength(url.pathname + url.search, "utf8");
}

function errorCode(body: object): string | undefined {
  const error = (body as { error?: { code?: unknown; reason?: unknown } }).error;
  if (!error) return undefined;
  return typeof error.reason === "string" ? error.reason : typeof error.code === "string" ? error.code : undefined;
}

function refusalAnswer(error: unknown, group: AcquisitionGroup): Answer {
  if (error instanceof HttpRefusal) return { status: error.status, body: { error: { code: error.code } } };
  return httpForError(error, group);
}

/** Steps: bearer, token shape, introspection, principal type, workspace, scope. Ops only. */
async function authenticate(c: Context<Env>, authenticator: Authenticator): Promise<AcquisitionPrincipal> {
  const group = "ops" as const;
  // Every refusal, a missing bearer included, is 403 not_authorized (contract P2).
  const rejected = () => new HttpRefusal(403, "not_authorized");
  const token = bearer(c.req.header("Authorization"));
  if (!token) throw rejected();
  // Token class: ops takes only sh_wk_; anything else is refused before introspection.
  if (!tokenFitsGroup(token, group)) throw rejected();
  let principal: AcquisitionPrincipal;
  try {
    principal = await authenticator.authenticate(token, group);
  } catch (error) {
    if (error instanceof RelaycontrolRejectedError) throw rejected();
    throw new HttpRefusal(503, "introspection_unavailable");
  }
  const policy = ACQUISITION_POLICIES[group];
  if (principal.principalType !== policy.principalType || !principal.workspaceId || !principal.scopes.includes(policy.scope))
    throw new HttpRefusal(403, "not_authorized");
  return principal;
}

async function opsRoute(c: Context<Env>, installer: Installer | null, route: string, principal: AcquisitionPrincipal): Promise<Answer> {
  if (!installer) throw new HttpRefusal(503, "acquisition_disabled");
  // Per-workspace service token: org/workspace come only from introspection.
  const identity = { orgId: principal.orgId, workspaceId: principal.workspaceId! };
  const body = await jsonBody(c);
  if (route === "POST status") return { status: 200, body: { result: await installer.status(body, identity) } };
  const kind = route.slice("POST ".length) as AcquisitionKind;
  return { status: 200, body: { result: await installer.apply(kind, body, identity) } };
}
