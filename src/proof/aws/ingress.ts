/**
 * Public ingress check: the customer-facing ingest stays public on its host,
 * HTTPS :443 only, with an ACM certificate, /healthz = 200, and nothing else
 * reachable without the right tenant's credentials.
 *
 *   (a) negative probes: every non-health ingest route refuses no credentials
 *       and a token for the wrong tenant;
 *   (b) no plaintext listener: TCP :80 does not answer, and the ALB has no
 *       HTTP listener;
 *   (c) TLS: the ALB's SSL policy allows nothing below TLS 1.2, and the served
 *       certificate is valid for the host;
 *   (d) WAF: a WAFv2 web ACL is associated with the ALB and has a rate-based
 *       rule; a bounded burst shows the rule blocking.
 *
 * Every verdict here is pure; the live probes are injectable for tests.
 */
import { connect as netConnect } from "node:net";
import { connect as tlsConnect, type PeerCertificate } from "node:tls";
import { lookup } from "node:dns/promises";

/* ------------------------------------------------------------- (a) routes */

export interface RouteProbe {
  method: "GET" | "POST";
  path: string;
  /**
   * Request body. Write probes carry a well-formed body that names the other
   * tenant's object, so the route reaches its ownership check instead of
   * stopping at input validation (a 400 there would prove nothing). Chunk
   * bodies are empty: if ownership were broken the sealer still refuses an
   * empty chunk, so a probe can never write.
   */
  body?: string;
  /** Route that serves another tenant's object by id: 404 is the scoped "not yours" answer. */
  byId?: boolean;
  /** Ops-only route; 404 (not routed publicly) is acceptable. */
  internal?: boolean;
}

/** Every non-health route AlleyOop mounts (src/alleyoop/ingest.ts, manifests.ts, packages/pipeline routes). */
export function ingestRoutes(o: { otherTraceId: string; otherManifestKey: string; other: { machineId: string; harness: string; sessionId: string; startDate: string } }): RouteProbe[] {
  const status = new URLSearchParams({ key: o.otherManifestKey, machine: o.other.machineId, writer: o.other.machineId });
  const chunk = new URLSearchParams({ format: "jsonl", seq: "0", epoch: "0", offset: "0", date: o.other.startDate, machine: o.other.machineId, harness: o.other.harness, session: o.other.sessionId });
  return [
    { method: "POST", path: `/v1/pipeline/chunks?${chunk}`, body: "" },
    { method: "POST", path: "/v1/pipeline/manifests", body: JSON.stringify({ key: o.otherManifestKey, manifest: {} }) },
    { method: "GET", path: `/v1/pipeline/manifests/status?${status}`, byId: true },
    { method: "GET", path: `/v1/pipeline/traces/${o.otherTraceId}`, byId: true },
    { method: "POST", path: "/v1/pipeline/events", body: "{}" },
    { method: "GET", path: "/v1/internal/pipeline/dead-letters", internal: true },
    { method: "POST", path: "/v1/internal/pipeline/telemetry-ids", internal: true, body: "{}" },
    { method: "GET", path: `/v1/internal/pipeline/traces/${o.otherTraceId}`, internal: true },
    { method: "POST", path: "/v1/internal/pipeline/dead-letters/replay", internal: true, body: "{}" },
  ];
}

export type Credential = "none" | "wrong-tenant";

export interface RouteObservation {
  route: RouteProbe;
  credential: Credential;
  status: number;
}

/** Allowed refusals. 401/403 everywhere; 404 only where it means "not yours" or "not routed". */
export function routeRefused(o: RouteObservation): boolean {
  if (o.status === 401 || o.status === 403) return true;
  if (o.status === 404 && (o.route.internal || (o.route.byId && o.credential === "wrong-tenant"))) return true;
  return false;
}

export function evaluateRoutes(obs: readonly RouteObservation[], healthz: number | undefined): string[] {
  const failures: string[] = [];
  if (healthz !== 200) failures.push(`/healthz returned ${healthz ?? "no response"}, expected 200`);
  for (const o of obs)
    if (!routeRefused(o)) failures.push(`${o.route.method} ${o.route.path.split("?")[0]} with ${o.credential} credentials returned ${o.status}`);
  const covered = new Set(obs.map((o) => `${o.credential}`));
  if (!covered.has("none")) failures.push("no unauthenticated probes ran");
  return failures;
}

/* ------------------------------------------------------------- (b) listeners */

export interface AlbListener {
  Port: number;
  Protocol: string;
  SslPolicy?: string;
  Certificates?: Array<{ CertificateArn: string }>;
  DefaultActions?: Array<{ Type: string; RedirectConfig?: { Protocol?: string } }>;
}

export function evaluateListeners(listeners: readonly AlbListener[], port80: "closed" | "open"): string[] {
  const failures: string[] = [];
  const plaintext = listeners.filter((l) => l.Protocol === "HTTP");
  for (const l of plaintext) failures.push(`ALB has a plaintext HTTP listener on :${l.Port}${l.DefaultActions?.some((a) => a.Type === "redirect") ? " (redirect only)" : ""}`);
  const https = listeners.filter((l) => l.Protocol === "HTTPS");
  if (!https.some((l) => l.Port === 443)) failures.push("ALB has no HTTPS :443 listener");
  for (const l of https) if (!l.Certificates?.some((c) => /^arn:aws:acm:/.test(c.CertificateArn))) failures.push(`HTTPS :${l.Port} does not use an ACM certificate`);
  if (port80 === "open") failures.push("TCP :80 on the public host accepted a connection");
  return failures;
}

/** TCP connect with a timeout; refused, reset and timeout all count as closed. */
export function tcpPort(host: string, port: number, timeoutMs = 5_000): Promise<"open" | "closed"> {
  return new Promise((resolve) => {
    const s = netConnect({ host, port });
    const done = (v: "open" | "closed") => {
      s.destroy();
      resolve(v);
    };
    s.setTimeout(timeoutMs, () => done("closed"));
    s.once("connect", () => done("open"));
    s.once("error", () => done("closed"));
  });
}

/* ------------------------------------------------------------- (c) TLS */

export interface SslPolicy {
  Name: string;
  SslProtocols: string[];
}

export interface TlsObservation {
  /** Handshake restricted to TLS 1.0-1.1 succeeded? */
  legacyAccepted: boolean;
  /** Handshake at TLS 1.2+ with certificate verification against the host. */
  modern: { ok: boolean; protocol?: string; authorizationError?: string; validTo?: string; subjectAltName?: string };
}

const LEGACY = /^(SSLv3|TLSv1(\.0)?|TLSv1\.1)$/;

export function evaluateTls(policy: SslPolicy | undefined, tls: TlsObservation, host: string, nowMs: number): string[] {
  const failures: string[] = [];
  if (!policy) failures.push("ALB SSL policy unknown");
  else {
    const weak = policy.SslProtocols.filter((p) => LEGACY.test(p));
    if (weak.length) failures.push(`SSL policy ${policy.Name} allows ${weak.join(", ")}`);
  }
  if (tls.legacyAccepted) failures.push("a TLS 1.0/1.1 handshake succeeded");
  if (!tls.modern.ok) failures.push(`TLS 1.2+ handshake with certificate verification failed${tls.modern.authorizationError ? `: ${tls.modern.authorizationError}` : ""}`);
  else {
    if (tls.modern.validTo && Date.parse(tls.modern.validTo) <= nowMs) failures.push("certificate expired");
    const sans = (tls.modern.subjectAltName ?? "").split(",").map((s) => s.trim().replace(/^DNS:/, "").toLowerCase());
    const h = host.toLowerCase();
    if (!sans.some((s) => s === h || (s.startsWith("*.") && h.endsWith(s.slice(1)) && h.split(".").length === s.split(".").length)))
      failures.push(`certificate has no SAN for ${host}`);
  }
  return failures;
}

function handshake(host: string, opts: { minVersion: "TLSv1" | "TLSv1.2"; maxVersion: "TLSv1.1" | "TLSv1.3" }, timeoutMs: number): Promise<{ ok: boolean; protocol?: string; cert?: PeerCertificate; authorizationError?: string }> {
  return new Promise((resolve) => {
    const s = tlsConnect({ host, port: 443, servername: host, rejectUnauthorized: true, ...opts });
    const t = setTimeout(() => {
      s.destroy();
      resolve({ ok: false, authorizationError: "timeout" });
    }, timeoutMs);
    s.once("secureConnect", () => {
      clearTimeout(t);
      const out = { ok: s.authorized, protocol: s.getProtocol() ?? undefined, cert: s.getPeerCertificate(), authorizationError: s.authorizationError ? String(s.authorizationError) : undefined };
      s.end();
      resolve(out);
    });
    s.once("error", (e: NodeJS.ErrnoException) => {
      clearTimeout(t);
      resolve({ ok: false, authorizationError: e.code ?? "tls_error" });
    });
  });
}

/**
 * Active TLS probe. Note what it cannot distinguish: a refused TLS 1.0/1.1
 * handshake may be the local OpenSSL's own floor rather than the server's
 * policy. The ALB SSL policy (describe-ssl-policies) is the authoritative
 * evidence; this probe can only add a failure, never prove the floor alone.
 */
export async function probeTls(host: string, timeoutMs = 8_000): Promise<TlsObservation> {
  const legacy = await handshake(host, { minVersion: "TLSv1", maxVersion: "TLSv1.1" }, timeoutMs);
  const modern = await handshake(host, { minVersion: "TLSv1.2", maxVersion: "TLSv1.3" }, timeoutMs);
  return {
    legacyAccepted: legacy.ok,
    modern: { ok: modern.ok, protocol: modern.protocol, authorizationError: modern.authorizationError, validTo: modern.cert?.valid_to, subjectAltName: modern.cert?.subjectaltname },
  };
}

/* ------------------------------------------------------------- (d) WAF */

export interface WebAcl {
  Name: string;
  ARN: string;
  Rules?: Array<{ Name: string; Statement?: { RateBasedStatement?: { Limit: number; EvaluationWindowSec?: number; AggregateKeyType?: string } }; Action?: { Block?: unknown; Count?: unknown } }>;
}

export function rateRules(acl: WebAcl | undefined) {
  return (acl?.Rules ?? []).filter((r) => r.Statement?.RateBasedStatement).map((r) => ({
    name: r.Name,
    limit: r.Statement!.RateBasedStatement!.Limit,
    windowSec: r.Statement!.RateBasedStatement!.EvaluationWindowSec ?? 300,
    blocks: r.Action?.Block !== undefined,
  }));
}

export function evaluateWafConfig(acl: WebAcl | undefined): string[] {
  if (!acl) return ["no WAFv2 web ACL is associated with the ALB"];
  const rules = rateRules(acl);
  if (!rules.length) return [`web ACL ${acl.Name} has no rate-based rule`];
  if (!rules.some((r) => r.blocks)) return [`web ACL ${acl.Name} rate-based rules only count; none blocks`];
  return [];
}

export interface BurstPlan {
  maxRequests: number;
  concurrency: number;
  deadlineMs: number;
  path: string;
}

/**
 * The burst is bounded three ways: a request cap, a deadline, and it stops at
 * the first WAF block. The cap must exceed the lowest blocking rule's limit
 * (WAF counts per evaluation window, so a cap under the limit can never trigger
 * and would be a meaningless pass or fail).
 */
export function burstPlan(acl: WebAcl | undefined, o: Partial<BurstPlan> = {}): BurstPlan | { refused: string } {
  const blocking = rateRules(acl).filter((r) => r.blocks).sort((a, b) => a.limit - b.limit)[0];
  if (!blocking) return { refused: "no blocking rate-based rule to exercise" };
  const maxRequests = o.maxRequests ?? Math.ceil(blocking.limit * 1.5) + 20;
  if (maxRequests <= blocking.limit) return { refused: `maxRequests ${maxRequests} cannot exceed rule limit ${blocking.limit}` };
  if (maxRequests > 5_000) return { refused: `maxRequests ${maxRequests} exceeds the hard cap of 5000` };
  return { maxRequests, concurrency: Math.min(o.concurrency ?? 10, 25), deadlineMs: Math.min(o.deadlineMs ?? 180_000, 300_000), path: o.path ?? "/healthz" };
}

export interface BurstResult {
  sent: number;
  ok: number;
  blocked: number;
  otherStatus: number;
  firstBlockAfter?: number;
  elapsedMs: number;
}

/** WAF block responses: 403 (default) or 429 when the rule uses a custom response. */
export const isWafBlock = (status: number) => status === 403 || status === 429;

export async function runBurst(url: string, plan: BurstPlan, f: (u: string) => Promise<{ status: number }> = (u) => fetch(u, { method: "GET" }), now = () => Date.now()): Promise<BurstResult> {
  const start = now();
  const res: BurstResult = { sent: 0, ok: 0, blocked: 0, otherStatus: 0, elapsedMs: 0 };
  while (res.sent < plan.maxRequests && now() - start < plan.deadlineMs && res.blocked === 0) {
    const n = Math.min(plan.concurrency, plan.maxRequests - res.sent);
    const statuses = await Promise.all(Array.from({ length: n }, () => f(url + plan.path).then((r) => r.status, () => 0)));
    for (const s of statuses) {
      res.sent += 1;
      if (s === 200) res.ok += 1;
      else if (isWafBlock(s)) {
        res.blocked += 1;
        res.firstBlockAfter ??= res.sent;
      } else res.otherStatus += 1;
    }
  }
  res.elapsedMs = now() - start;
  return res;
}

export function evaluateBurst(r: BurstResult, plan: BurstPlan): string[] {
  if (r.blocked > 0) return [];
  return [`rate-based rule did not block within ${r.sent}/${plan.maxRequests} requests in ${r.elapsedMs} ms`];
}

/* ------------------------------------------------------------- target guard */

/**
 * The burst (and any wrong-tenant probe) may only hit the proof ALB. The host
 * must resolve to the same addresses as an ALB that `describe-load-balancers`
 * lists in the proof account. A proxied host (for example a Cloudflare-proxied
 * record) resolves elsewhere, so the harness cannot prove the target and refuses.
 */
export function hostIsProofAlb(hostAddrs: readonly string[], albAddrs: readonly string[]): boolean {
  if (!hostAddrs.length || !albAddrs.length) return false;
  const alb = new Set(albAddrs);
  return hostAddrs.every((a) => alb.has(a));
}

export async function resolveAll(name: string): Promise<string[]> {
  try {
    return (await lookup(name, { all: true })).map((a) => a.address).sort();
  } catch {
    return [];
  }
}
