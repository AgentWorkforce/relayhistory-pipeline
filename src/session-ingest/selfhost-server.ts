/**
 * SERVICE=session-ingest: the session ingest Hono app (app.ts) served over
 * node:http in the self-host services image, with no Cloudflare binding, no
 * RelayAuth and no legacy relayhistory-cloud. Default off
 * (RELAYHISTORY_SELFHOST_INGEST, selfhost-config.ts); the hosted Worker
 * (worker.ts) is unchanged and never imports this file.
 *
 *   GET  /healthz              liveness, always 200 { ok, enabled, admission? , consumer? }
 *   GET  /readyz               enabled and the ledger answers SELECT 1: 200; otherwise 503
 *                              (SELFHOST_INGEST_ADMISSION=async: the admission consumer's health instead; below)
 *                              Both probes pin the effective mode once enabled: `admission` is "sync" | "async" |
 *                              "drain", and async / drain add `consumer` { ok, state, backlog, oldestAgeMs,
 *                              deadLettered, inFlight, lastScanAgeMs }. A dormant edge reports neither.
 *   POST /v1/delivery/batches  rth:sync   (app.ts, unchanged handler)
 *   POST /v1/turns/batches     rth:sync   (app.ts, unchanged handler)
 *   GET  /v1/delivery/limits   rth:sync   (app.ts, unchanged handler) plus sessionDeletion:false and sessionReset:false
 *                              at the top level (SELFHOST_CAPABILITIES): the Desktop shows its delete and reset
 *                              controls only when the limits answer says true, and neither hosted route is served here
 *   POST /v1/auth/token/refresh  401 refresh_not_supported, always (see SELFHOST_REFRESH_PATH)
 *   POST /v1/delivery/sessions/selection  rth:sync, only with RELAYHISTORY_SELFHOST_SESSION_SELECTION=true:
 *                              per-session erase (enabled:false; enabled:true answers 400 unsupported_by_backend). The app.ts handler is
 *                              unchanged; the ledger's setSessionUploads fences and tombstones every origin of the caller's session in one transaction
 *                              (migration 043910). 404 afterwards means unreadable, not bytes gone.
 *                              Under a capture policy with user_retroactive_delete:false (relaycontrol introspection v1.5),
 *                              a member's erase is 403 policy_forbids_user_delete; owners and admins still erase (capture-policy.ts).
 *   POST|GET /internal/v1/session-erase/...  operator per-session erase (session-erase.ts). Forwarded to the app
 *                              WITHOUT relaycontrol introspection: the app checks its own bearer
 *                              (SESSION_ERASE_API_SECRET). Default off: 501 not_implemented. Internal only;
 *                              the edge must never route /internal/* (docs/selfhost-composition.md section 2).
 *   POST /v1/onboarding/heartbeat  rth:sync, only with RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true: 204, no body.
 *                              The Desktop's best-effort upload progress; the body is discarded unread past
 *                              REFRESH_DRAIN_BYTES (connection closed, as the refresh path), never stored or logged.
 *                              Off (default): 501 like any other unserved /v1 path.
 *   any other /v1 path         501 unsupported_by_backend (reset, recovery, custody,
 *                              onboarding are relayhistory-internal and not served self-host)
 *
 * Bindings, Node instead of Cloudflare:
 *   HYPERDRIVE     -> pg.Pool as bulls96_session_ingest (discrete fields, SESSION_POSTGRES_TLS_PROFILE),
 *                     admitted at startup only as a real login of exactly that role (assertIngestRole:
 *                     session_user, current_user, no SET ROLE, the backend usename, PG16+ system_user),
 *                     with the bootstrap's role shape, no membership edge in either direction, and the
 *                     pinned timeouts
 *   S3 secrets     -> the Locker store (src/shared/locker.ts: MinIO static keys, EKS Pod Identity, or GCS)
 *   INGEST_FANOUT  -> none. The queue only exists because a Worker invocation has a subrequest budget
 *                     (staging.ts); a Node process does not, so `inlineMaxSessions` is unset and every
 *                     batch is written session by session BEFORE the receipt. Nothing is ever staged, so
 *                     there is nothing to fan out and no outbox to lose on a crash: an interrupted request
 *                     leaves a pending claim the Desktop's retry resumes (ledger `resume`), as on the Worker.
 *   NEON_REPLICA   -> none (refused by config)
 *   INGEST_SIGNING_KEY (relayhistory HMAC) -> replaced by relaycontrol introspection (selfhost-auth.ts)
 *
 * SELFHOST_INGEST_ADMISSION (selfhost-config.ts; default sync, config only, never a header):
 *   sync   (default) everything above, byte for byte: nothing below is constructed.
 *   async  POST /v1/delivery/batches and /v1/turns/batches answer the hosted queued receipt
 *          (x-relay-admission {"version":1,"state":"queued"}) after ONE create-only inbox PUT. The request
 *          path makes no SQL call at all (and never falls back to the synchronous path: a failure is a 503
 *          with nothing acknowledged). The authorization dependency is unchanged: one relaycontrol
 *          introspection per request, no positive cache by default (RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS,
 *          selfhost-auth.ts, opts into a bounded one). An in-process consumer (selfhost-admission.ts)
 *          admits the inbox through the hosted admitMessage. Startup refuses a Locker backend that does not
 *          honour conditional writes (locker.ts assertConditionalWrites). /readyz follows the consumer.
 *   drain  the synchronous request path again, while the consumer keeps admitting the inbox.
 *
 * INGEST_CLAIM_LOCK_BUSY (selfhost-config.ts; default off): =on makes a claim that loses its lock wait
 * (55P03/40P01) a 429 delivery_busy with Retry-After 5-10 s instead of a 503 on the synchronous request path
 * (sync and drain), exactly as the Worker; 57014 and every other cause stay 503. The async consumer gets the
 * same deliver deps, and the hosted admitMessage keeps its normal backoff for it, so nothing changes there.
 *
 * Auth runs in this wrapper BEFORE the request body is read: an unauthenticated
 * or unscoped caller never gets the edge to buffer MAX_DELIVERY_BYTES. The
 * verified identity reaches the app only through a WeakMap keyed by the exact
 * Request object handed to app.fetch; no header (x-relay-ingest-identity
 * included, which is stripped) can assert identity. Bodies, tokens and header
 * values are never logged.
 */
import { sqlUploadConsentStore } from "./upload-consent-sql.ts";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import pg from "pg";
import { errorFields, log } from "../shared/log.ts";
import { initScrubEngine, routedRawChunkSealerDeps, scrubEngineReady } from "@rhc/lib/scrub-engine/index.ts";
import { assertConditionalWrites, buildLockerStore } from "../shared/locker.ts";
import type { ObjectStore } from "../shared/vendor.ts";
import { AuthError } from "../session-read-api/app.ts";
import { createIngestApp, IDENTITY_HEADER, SIGNATURE_HEADER, SYNC_SCOPE, UPLOAD_CONSENT_PATH, type IngestIdentity } from "./app.ts";
import { ADMISSION_ACCEPT_VALUE, INBOX_NAMESPACE, type AdmissionMessage } from "./async-admission.ts";
import { promoteUpload } from "./upload-admission.ts";
import type { DeliverDeps } from "./deliver.ts";
import { selfhostAdmissionConsumer, type SelfhostAdmissionConsumer, type SelfhostAdmissionOptions, type SelfhostAdmissionStatus } from "./selfhost-admission.ts";
import { sqlLedger } from "./ledger-sql.ts";
import type { SqlRun } from "./ledger-sql.ts";
import { relaycontrolIngestIdentity, selfhostIntrospector } from "./selfhost-auth.ts";
import { SELFHOST_INGEST_ROLE, selfhostIngestConfig, type SelfhostIngestSettings } from "./selfhost-config.ts";
import { isSessionErasePath } from "./session-erase.ts";
import type { SessionEraseService } from "./session-erase.ts";
import { buildSelfhostEraseRuntime } from "../session-erase/selfhost-runtime.ts";
import type { SessionWriteGuard } from "../session-erase/runtime.ts";
import type { AnalyticsPort } from "../shared/analytics.ts";
import { ANALYTICS_OFF, analyticsCounterFields, createAnalyticsSink } from "../shared/analytics-config.ts";

type FetchApp = { fetch(req: Request): Response | Promise<Response> };

/** The only routes served, by exact raw pathname. */
export const SELFHOST_INGEST_ROUTES: Readonly<Record<string, "GET" | "POST">> = Object.freeze({
  "/v1/delivery/batches": "POST",
  "/v1/turns/batches": "POST",
  "/v1/delivery/limits": "GET",
});

/** SELFHOST_INGEST_ADMISSION=async|drain only: the hosted admission status read (S3 only, rth:sync, tenant from the verified identity). */
export const SELFHOST_ADMISSION_STATUS_PATH = "/v1/delivery/admission";

/**
 * The Desktop's refresh call. A self-host session has nothing to refresh (the read API's login passes the
 * sh_at_ token through and repeats it as refreshToken), so the only honest answer is that this credential
 * cannot be refreshed. It has to be a 401: the Desktop treats 400/401/403 as a refused refresh, which marks
 * the upload as needing sign-in and starts its renewal from relaycontrol. A 404 or 501 reads as an outage it
 * keeps retrying, so uploads would stop for good once the first token expired. No bearer is sent here and
 * none is read; the answer is the same for every caller.
 */
export const SELFHOST_REFRESH_PATH = "/v1/auth/token/refresh";
/** A refresh body is one small JSON object. Past this it is not read, and the connection closes after the answer. */
const REFRESH_DRAIN_BYTES = 8 * 1024;

/** Discards a small declared body, so the client finishes sending and can read the answer rather than a reset. */
async function drainSmallBody(req: IncomingMessage): Promise<void> {
  if (!(Number(req.headers["content-length"]) <= REFRESH_DRAIN_BYTES) || req.complete) return;
  await new Promise<void>((resolve) => {
    req.once("end", resolve);
    req.once("close", resolve);
    req.resume();
  });
}

export const SELFHOST_SESSION_SELECTION_PATH = "/v1/delivery/sessions/selection";

/** RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true only: the Desktop's onboarding heartbeat, answered 204 here (never reaches the app). */
export const SELFHOST_HEARTBEAT_PATH = "/v1/onboarding/heartbeat";

export const SELFHOST_LIMITS_PATH = "/v1/delivery/limits";
/**
 * Merged at the top level of the app's GET /v1/delivery/limits answer, self-host only (the hosted app.ts
 * answer is unchanged). The Desktop enables session deletion / reset only on an explicit true.
 */
export const SELFHOST_CAPABILITIES = Object.freeze({ sessionDeletion: false, sessionReset: false });

/** The app, with the self-host capabilities merged into a successful limits answer; every other response untouched. */
function withSelfhostCapabilities(app: FetchApp): FetchApp {
  return {
    async fetch(request) {
      const response = await app.fetch(request);
      if (request.method !== "GET" || new URL(request.url).pathname !== SELFHOST_LIMITS_PATH || response.status !== 200) return response;
      const body = (await response.json()) as Record<string, unknown>;
      const headers = new Headers(response.headers);
      headers.delete("content-length");
      return new Response(JSON.stringify({ ...body, ...SELFHOST_CAPABILITIES }), { status: response.status, headers });
    },
  };
}

/** One assembled ingest: the app, the per-request verifier, readiness and teardown. */
export interface SelfhostIngestRuntime {
  app: FetchApp;
  /** The routes this runtime serves, by exact raw pathname (the base set, plus selection when enabled). */
  routes: Readonly<Record<string, "GET" | "POST">>;
  /** Verifies one Authorization header value; throws AuthError to refuse. */
  verify(authorization: string | undefined): Promise<IngestIdentity>;
  /** Hands the verified identity to the app for exactly this Request object. */
  bind(request: Request, identity: IngestIdentity): void;
  ready(): Promise<boolean>;
  close(): Promise<void>;
  /** The effective SELFHOST_INGEST_ADMISSION mode, pinned on both probes ("sync" when absent). */
  admissionMode?: "sync" | "async" | "drain";
  /** SELFHOST_INGEST_ADMISSION=async|drain only: the consumer's status (for the probes). */
  admission?: () => SelfhostAdmissionStatus;
}

export interface RuntimeParts {
  run: SqlRun;
  store: ObjectStore;
  verify: (authorization: string | undefined) => Promise<IngestIdentity>;
  ready: () => Promise<boolean>;
  close: () => Promise<void>;
  /** ANALYTICS_SINK=collector: the batch-completed events' port (closed by `close`). Absent: none. */
  analytics?: AnalyticsPort;
  withSessionWrites?: SessionWriteGuard;
  sessionEraseService?: SessionEraseService;
}

/** Test seams for the async consumer; production passes none. */
export interface SelfhostRuntimeOptions {
  nowMs?: () => number;
  /** false: build the consumer but do not start its scan loop (a test drives scan() / settle()). */
  startConsumer?: boolean;
  onConsumer?: (consumer: SelfhostAdmissionConsumer) => void;
  log?: SelfhostAdmissionOptions["log"];
}

/** The ingest app over the given parts. Canonical acceptance, inline writes, no fan-out, no replica, no custody. */
export function selfhostIngestRuntime(settings: Pick<SelfhostIngestSettings, "keyRoot" | "maxPerUser" | "maxPerOrg" | "admission"> & Partial<Pick<SelfhostIngestSettings, "sessionSelection" | "sessionErase" | "heartbeatAccept" | "claimLockBusy" | "uploadDirect" | "capturePolicyUploads" | "uploadConsent">>, parts: RuntimeParts, options: SelfhostRuntimeOptions = {}): SelfhostIngestRuntime {
  const sessionSelection = settings.sessionSelection === true;
  const verified = new WeakMap<Request, IngestIdentity>();
  const deliver: DeliverDeps = {
    ledger: sqlLedger(parts.run, { acceptanceMode: "canonical", selfhostSelectionErase: sessionSelection }),
    ...(parts.withSessionWrites ? { withSessionWrites: parts.withSessionWrites } : {}),
    acceptanceMode: "canonical",
    // SCRUB_ENGINE=wasm-bindgen: the sealer bound to the verified instance; otherwise {} (unchanged).
    target: { store: parts.store, keyRoot: settings.keyRoot, ...routedRawChunkSealerDeps() },
    // Slot TTL past any request this process serves, so a lost release frees itself (Worker: 90).
    slots: { maxPerUser: settings.maxPerUser, maxPerOrg: settings.maxPerOrg, ttlSeconds: 90 },
    // inlineMaxSessions unset: always inline, so `fanout` is never needed (see the header).
    // INGEST_CLAIM_LOCK_BUSY=on (default off): the same deps in every admission mode, as the Worker's deliverDepsFor.
    ...(settings.claimLockBusy ? { claimLockBusy: true as const } : {}),
  };
  const identify = (request: Request) => {
    const identity = verified.get(request);
    // Unreachable through the wrapper; a direct app.fetch without bind() is refused, never trusted.
    if (!identity) throw new AuthError(401, "missing_authorization", "missing Authorization header");
    return identity;
  };
  // SESSION_ERASE_API_ENABLED (session-erase.ts): the same operator route in every admission mode.
  const sessionErase = { ...(settings.sessionErase ? { sessionErase: settings.sessionErase } : {}),
    ...(parts.sessionEraseService ? { sessionEraseService: parts.sessionEraseService } : {}) };
  // ANALYTICS_SINK=collector: the same port in every admission mode.
  const analytics = parts.analytics ? { analytics: parts.analytics } : {};
  // RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS (capture-policy.ts): the same check in every admission mode.
  const capturePolicyUploads = { capturePolicyUploads: settings.capturePolicyUploads ?? "report" as const };
  // INGEST_UPLOAD_CONSENT (upload-consent.ts): the same check on the request path and in the consumer.
  const uploadConsentConfig = settings.uploadConsent && settings.uploadConsent !== "off"
    ? { enforcement: settings.uploadConsent, store: sqlUploadConsentStore(parts.run), ...(options.nowMs ? { nowMs: options.nowMs } : {}) }
    : undefined;
  const uploadConsent = uploadConsentConfig ? { uploadConsent: uploadConsentConfig } : {};
  const routes: Readonly<Record<string, "GET" | "POST">> = sessionSelection || settings.heartbeatAccept === true || uploadConsentConfig
    ? Object.freeze({
      ...SELFHOST_INGEST_ROUTES,
      ...(sessionSelection ? { [SELFHOST_SESSION_SELECTION_PATH]: "POST" as const } : {}),
      ...(settings.heartbeatAccept === true ? { [SELFHOST_HEARTBEAT_PATH]: "POST" as const } : {}),
      ...(uploadConsentConfig ? { [UPLOAD_CONSENT_PATH]: "POST" as const } : {}),
    })
    : SELFHOST_INGEST_ROUTES;
  const admission = settings.admission;
  if (!admission) {
    // SELFHOST_INGEST_ADMISSION=sync (the default): exactly the app this file always built.
    const app = withSelfhostCapabilities(createIngestApp({ identify, deliver, log: (fields) => log.info("session ingest", fields), ...sessionErase, ...analytics, ...capturePolicyUploads, ...uploadConsent }));
    return { app, routes, verify: parts.verify, bind: (request, identity) => void verified.set(request, identity), ready: parts.ready, close: parts.close, admissionMode: "sync" };
  }
  const mode = admission.mode === "async" ? "on" as const : "drain" as const;
  const inboxRoot = `${settings.keyRoot}/${INBOX_NAMESPACE}/`;
  // No queue: the inbox PUT is the record, and the consumer's scan is the authority. This only lets the
  // consumer start on a fresh upload without waiting for the next scan; it never throws.
  const notify = async (message: AdmissionMessage) => { if (message.key.startsWith(inboxRoot)) consumer.notify(message.key); };
  // HISTORY_UPLOAD_DIRECT allowlist|on (selfhost-config.ts): the consumer's scan also promotes the upload inbox.
  const upload = settings.uploadDirect;
  const uploadInbox = upload
    ? { batchSize: upload.scanBatch, promote: (key: string) => promoteUpload(key, { config: upload.config, deliver, witnessKey: upload.witnessKey, enqueue: notify, ...(options.nowMs ? { nowMs: options.nowMs } : {}) }) }
    : undefined;
  const consumer: SelfhostAdmissionConsumer = selfhostAdmissionConsumer({
    mode, deliver, concurrency: admission.concurrency, scanIntervalMs: admission.scanIntervalMs, maxAttempts: admission.maxAttempts,
    ...(options.nowMs ? { nowMs: options.nowMs } : {}), ...(options.log ? { log: options.log } : {}), ...(uploadInbox ? { uploadInbox } : {}),
    ...uploadConsent,
  });
  options.onConsumer?.(consumer);
  const app = withSelfhostCapabilities(createIngestApp({
    identify,
    deliver,
    log: (fields) => log.info("session ingest", fields),
    asyncAdmission: {
      mode,
      enqueue: notify,
      ...(options.nowMs ? { nowMs: options.nowMs } : {}),
      // The #392 erase ledger answers a re-enable with its own 400 before any SQL; let it, rather than the
      // journal's 503 (a disable needs no journal: the consumer's claim filters the session).
      selfhost: { identityOf: (request) => verified.get(request), reEnableRefusedByLedger: sessionSelection },
    },
    ...(upload ? { uploadDirect: upload.config } : {}),
    ...sessionErase,
    ...analytics,
    ...capturePolicyUploads,
    ...uploadConsent,
  }));
  if (options.startConsumer !== false) consumer.start();
  return {
    app,
    verify: parts.verify,
    // The opt-in is this deployment's config, stamped on the identity the wrapper verified; no request
    // header or body can set it (the identity header is stripped and never parsed here).
    bind: (request, identity) => void verified.set(request, mode === "on" ? { ...identity, admission: ADMISSION_ACCEPT_VALUE } : identity),
    // async: uploads need only the store, so readiness is the consumer's (its scans prove the store answers);
    // the database's health shows as consumer backlog, never as refused uploads. drain: the synchronous
    // request path needs the database too.
    ready: async () => consumer.status().healthy && (mode === "on" || await parts.ready()),
    close: async () => { await consumer.stop(); await parts.close(); },
    admissionMode: admission.mode,
    admission: () => consumer.status(),
    routes: Object.freeze({ ...routes, [SELFHOST_ADMISSION_STATUS_PATH]: "GET" as const }),
  };
}

/**
 * The bootstrap's LOGIN timeout profile (supabase/bootstrap/session_ingest_role.sql, checked exactly by
 * session_ingest_verify.sql). The pool also sends it as startup options, so the live session matches.
 */
export const INGEST_TIMEOUTS = Object.freeze({ statement_timeout: "5s", lock_timeout: "2s", idle_in_transaction_session_timeout: "10s" });
export const INGEST_OPTIONS = Object.entries(INGEST_TIMEOUTS).map(([k, v]) => `-c ${k}=${v}`).join(" ");
const INGEST_ROLCONFIG = JSON.stringify(Object.entries(INGEST_TIMEOUTS).map(([k, v]) => `${k}=${v}`).sort());

export type IngestLoginRefusal =
  | "login_unreadable" | "session_user" | "current_user" | "set_role" | "backend_user" | "system_user"
  | "superuser" | "bypassrls" | "role_attributes" | "role_membership" | "role_members" | "role_settings" | "timeouts";

/** Names the failed property only; never an observed role name, URL or credential. */
export class IngestLoginError extends Error {
  constructor(readonly code: IngestLoginRefusal) {
    super(`96bulls: the session ingest login (${SELFHOST_INGEST_ROLE}) refused: ${code}`);
    this.name = "IngestLoginError";
  }
}

/** The live session and the role's shape in one row (pattern: assertInstallerLogin, src/acquisition/db-login.ts). */
export const INGEST_LOGIN_SQL = `SELECT session_user::text AS session_user, current_user::text AS current_user,
  current_setting('role') AS role_setting, current_setting('server_version_num')::int AS server_version,
  current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout,
  current_setting('idle_in_transaction_session_timeout') AS idle_in_transaction_session_timeout,
  (SELECT a.usename::text FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid()) AS backend_user,
  r.rolsuper, r.rolbypassrls, r.rolcreaterole, r.rolcreatedb, r.rolreplication, r.rolinherit, r.rolconfig,
  (SELECT count(*)::int FROM pg_auth_members m WHERE m.member = r.oid) AS memberships,
  (SELECT count(*)::int FROM pg_auth_members m WHERE m.roleid = r.oid) AS members,
  (SELECT count(*)::int FROM pg_db_role_setting s WHERE s.setrole = r.oid AND s.setdatabase <> 0) AS database_settings
  FROM pg_roles r WHERE r.rolname = current_user`;
/**
 * PG16+ only: system_user, inherit_option and set_option do not exist earlier. LEFT JOINs, so an edge
 * whose member or grantor cannot be named is still listed, and then refused.
 */
export const INGEST_PG16_SQL = `SELECT system_user::text AS system_user,
  (SELECT coalesce(json_agg(json_build_object('member', u.rolname, 'grantor', g.rolname, 'grantor_super', g.rolsuper,
     'admin_option', a.admin_option, 'inherit_option', a.inherit_option, 'set_option', a.set_option)), '[]'::json)
   FROM pg_auth_members a LEFT JOIN pg_roles u ON u.oid = a.member LEFT JOIN pg_roles g ON g.oid = a.grantor
   WHERE a.roleid = (SELECT oid FROM pg_roles WHERE rolname = current_user)) AS member_edges`;

/**
 * The one reverse membership edge permitted, and only on PG16+. When Supabase's non-superuser `postgres`
 * (CREATEROLE) runs session_ingest_roles.sql, PostgreSQL 16 records the creator as a member of the new
 * role, granted by the bootstrap superuser `supabase_admin`, WITH ADMIN, INHERIT FALSE, SET FALSE. That
 * edge lets `postgres` administer grants of the role but never assume it (SET FALSE) or inherit its
 * rights, and the repo's verifiers (session_ingest_verify.sql, and acquisition_installer_verify.sql for
 * the sibling login) permit exactly this edge, with the same PG-version and provider logic:
 *   - PG16+ only (before 16 a CREATEROLE creator gets no edge, so any incoming edge is refused);
 *   - provider: the verifier requires hoopdb.db_mode='supabase', a GUC only migrate sets. At runtime the
 *     declared provider is SESSION_POSTGRES_TLS_PROFILE=supabase (`provider.supabase`); off it, no edge
 *     is permitted at all. A Supabase deployment on custom-ca must revoke the edge (as supabase_admin)
 *     or the edge refuses startup;
 *   - member `postgres`, grantor `supabase_admin` and that grantor a superuser, WITH ADMIN, INHERIT FALSE,
 *     SET FALSE.
 * Every other edge, in either direction, is refused.
 */
export interface IngestProvider {
  /** SESSION_POSTGRES_TLS_PROFILE=supabase: the deployment declares Supabase (the verifier's db_mode). */
  supabase: boolean;
}
function supabaseCreatorEdge(edge: unknown): boolean {
  if (!edge || typeof edge !== "object") return false;
  const e = edge as Record<string, unknown>;
  return e.member === "postgres" && e.grantor === "supabase_admin" && e.grantor_super === true
    && e.admin_option === true && e.inherit_option === false && e.set_option === false;
}

/**
 * The admission decision over the live rows (pure, so a real-login row can be tested where PGlite has no
 * login). `pg16` is the INGEST_PG16_SQL row, required when server_version >= 160000; `provider` gates
 * the Supabase creator edge. null admits.
 */
export function ingestLoginRefusal(login: Record<string, unknown> | undefined, pg16: Record<string, unknown> | undefined, provider: IngestProvider): IngestLoginRefusal | null {
  if (!login) return "login_unreadable";
  // Identity: a real login as the role, never another login that SET ROLE into it.
  if (login.session_user !== SELFHOST_INGEST_ROLE) return "session_user";
  if (login.current_user !== SELFHOST_INGEST_ROLE) return "current_user";
  if (login.role_setting !== "none") return "set_role";
  const version = login.server_version;
  if (typeof version !== "number" || !Number.isInteger(version) || version < 100000) return "login_unreadable";
  // SET SESSION AUTHORIZATION by a superuser makes session_user and current_user the role and leaves role
  // 'none' (real PG 15.19, 16.15, 17.11). This backend's pg_stat_activity usename is fixed at
  // authentication and still names the real login, on every version; on PG15 it is the only signal.
  if (login.backend_user !== SELFHOST_INGEST_ROLE) return "backend_user";
  const modern = version >= 160000;
  if (modern) {
    if (!pg16) return "login_unreadable";
    // "<auth method>:<authenticated identity>"; NULL means trust auth, which proves nothing.
    const su = pg16.system_user;
    const colon = typeof su === "string" ? su.indexOf(":") : -1;
    if (typeof su !== "string" || colon < 1 || su.slice(colon + 1) !== SELFHOST_INGEST_ROLE) return "system_user";
  }
  // Shape: the bootstrap's NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION NOINHERIT.
  if (login.rolsuper !== false) return "superuser";
  if (login.rolbypassrls !== false) return "bypassrls";
  if (login.rolcreaterole !== false || login.rolcreatedb !== false || login.rolreplication !== false || login.rolinherit !== false) return "role_attributes";
  // Membership in both directions: a member of no role, and no role a member of it (but the creator edge).
  if (login.memberships !== 0) return "role_membership";
  if (typeof login.members !== "number") return "login_unreadable";
  if (modern) {
    const edges = pg16!.member_edges;
    if (!Array.isArray(edges) || edges.length !== login.members || !edges.every((edge) => provider.supabase && supabaseCreatorEdge(edge))) return "role_members";
  } else if (login.members !== 0) return "role_members";
  // Settings: no per-database override (the verifier's rule), exactly the role's pinned timeout profile,
  // and the live session's values.
  if (login.database_settings !== 0) return "role_settings";
  const config = login.rolconfig;
  if (!Array.isArray(config) || JSON.stringify([...config].sort()) !== INGEST_ROLCONFIG) return "timeouts";
  for (const [k, v] of Object.entries(INGEST_TIMEOUTS)) if (login[k] !== v) return "timeouts";
  return null;
}

/**
 * Startup admission: the pool's session is a real login as exactly the ingest role (session_user,
 * current_user, no SET ROLE, the backend's pg_stat_activity usename, and on PG16+ system_user), with the
 * bootstrap's role shape, no membership edge in either direction (but the Supabase creator edge) and the
 * pinned timeout profile. Any mismatch
 * or query failure refuses startup; the error names the failed property only.
 */
export async function assertIngestRole(run: SqlRun, provider: IngestProvider): Promise<void> {
  let login: Record<string, unknown> | undefined;
  let pg16: Record<string, unknown> | undefined;
  try {
    const { rows } = await run(INGEST_LOGIN_SQL, []);
    if (rows.length !== 1) throw new Error("rows");
    login = rows[0];
    if (typeof login?.server_version === "number" && login.server_version >= 160000) {
      const extra = await run(INGEST_PG16_SQL, []);
      if (extra.rows.length !== 1) throw new Error("rows");
      pg16 = extra.rows[0];
    }
  } catch {
    throw new IngestLoginError("login_unreadable");
  }
  const refusal = ingestLoginRefusal(login, pg16, provider);
  if (refusal) throw new IngestLoginError(refusal);
}

/** Disjoint from every tenant prefix (leading dot) and inside the srv-writer `<root>/*\/v1/ws=*\/m=srv_*` shape. */
export function conditionalWriteProbeKey(keyRoot: string, nonce: string = crypto.randomUUID().replace(/-/g, "")): string {
  return `${keyRoot}/.admission-probe/v1/ws=probe/m=srv_${nonce}/probe.json`;
}

/**
 * SELFHOST_INGEST_ADMISSION=async|drain only: the inbox (create-only) and the privacy journal (compare-and-swap)
 * need conditional writes the backend really honours, so prove them once or refuse to start. Sync (the default)
 * never touches the store here.
 */
export async function admissionStartupGate(settings: Pick<SelfhostIngestSettings, "keyRoot" | "admission">, store: ObjectStore): Promise<void> {
  if (settings.admission) await assertConditionalWrites(store, conditionalWriteProbeKey(settings.keyRoot));
}

export type RuntimeBuilder = (settings: SelfhostIngestSettings, env: NodeJS.ProcessEnv) => Promise<SelfhostIngestRuntime>;

/**
 * Production: pg pool, Locker store, relaycontrol introspection with no positive cache unless
 * RELAYHISTORY_SELFHOST_INTROSPECT_POSITIVE_TTL_MS sets one (settings.introspectPositiveTtlMs, selfhost-auth.ts).
 */
export const productionRuntime: RuntimeBuilder = async (settings, env) => {
  const pool = new pg.Pool({
    ...settings.pool,
    application_name: "96bulls-session-ingest",
    idleTimeoutMillis: 60_000,
    connectionTimeoutMillis: 2_000,
    // Nonempty, so pg never applies PGOPTIONS; exactly the profile the role itself pins.
    options: INGEST_OPTIONS,
  });
  pool.on("error", (err) => log.error("session ingest pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  const run: SqlRun = async (text, params) => (await pool.query(text, params as unknown[])) as unknown as { rows: Record<string, unknown>[] };
  let operator: Awaited<ReturnType<typeof buildSelfhostEraseRuntime>>;
  let startupAnalytics: ReturnType<typeof createAnalyticsSink> | undefined;
  try {
    await assertIngestRole(run, { supabase: env.SESSION_POSTGRES_TLS_PROFILE === "supabase" });
    const store = buildLockerStore(env).store as unknown as ObjectStore;
    // Async admission needs create-only and compare-and-swap PUTs (the inbox, the privacy journal): prove the
    // backend honours them, or refuse to start. Never run for sync (the default).
    await admissionStartupGate(settings, store);
    const verify = relaycontrolIngestIdentity(settings, selfhostIntrospector(settings, env));
    const analytics = createAnalyticsSink(settings.analytics ?? ANALYTICS_OFF, "session-ingest", "service", {
      report: (counters) => log.warn("analytics export degraded", analyticsCounterFields(analytics.destination, counters)),
    });
    startupAnalytics = analytics;
    operator = await buildSelfhostEraseRuntime({ env, keyRoot: settings.keyRoot, api: settings.sessionErase,
      enforced: settings.eraseFenceEnforced, bytesEnabled: settings.eraseBytesEnabled, run, pool, metadata: settings.pool,
      log: fields => log.info("session erase", fields) });
    const runtime = selfhostIngestRuntime(settings, {
      run,
      store,
      verify,
      ready: async () => {
        try {
          await run("SELECT 1", []);
          return true;
        } catch {
          return false;
        }
      },
      // Shutdown flush first (bounded by SERVICE_LIMITS.shutdownMs, inside serve()'s 10 s exit), then the pool.
      close: async () => { await operator?.close(); await analytics.shutdown(); await pool.end(); },
      ...(analytics.destination !== "noop" ? { analytics } : {}),
      ...(operator ? { withSessionWrites: operator.withSessionWrites, sessionEraseService: operator.service } : {}),
    });
    operator?.start();
    return runtime;
  } catch (error) {
    await operator?.close().catch(() => {});
    await startupAnalytics?.shutdown().catch(() => {});
    await pool.end().catch(() => {});
    throw error;
  }
};

const correlationOf = (req: IncomingMessage) => {
  const raw = req.headers["x-correlation-id"];
  return (typeof raw === "string" ? raw.trim().slice(0, 128) : "") || crypto.randomUUID();
};

function sendJson(req: IncomingMessage, res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  // A refused request's body is never read: close the connection once the answer is out.
  const unread = !req.complete && req.method !== "GET" && req.method !== "HEAD";
  res.writeHead(status, { "content-type": "application/json", "cache-control": "private, no-store", ...(unread ? { connection: "close" } : {}), ...headers });
  res.end(JSON.stringify(body), () => {
    if (unread) req.socket?.destroy();
  });
}

/**
 * The pinned probe fields (the chart's config contract, docs/selfhost-composition.md section 5): the effective
 * admission mode once enabled, and in async / drain the consumer's counters (numbers and a closed state; no
 * key, no tenant). An image that predates SELFHOST_INGEST_ADMISSION reports no `admission` field at all.
 */
function probeFields(runtime: SelfhostIngestRuntime | null): Record<string, unknown> {
  if (!runtime) return {};
  const c = runtime.admission?.();
  return {
    admission: runtime.admissionMode ?? "sync",
    ...(c ? { consumer: { ok: c.healthy, state: c.state, backlog: c.backlog, oldestAgeMs: c.oldestAgeMs, deadLettered: c.deadLetters, inFlight: c.inFlight, lastScanAgeMs: c.lastScanAgeMs } } : {}),
  };
}

const envelope = (code: string, message: string, correlationId: string, extra: Record<string, unknown> = {}) => ({ error: { code, message, ...extra }, correlationId });

/** node:http in front of the runtime (null: dormant). */
export function selfhostIngestHttpServer(runtime: SelfhostIngestRuntime | null): Server {
  const server = createServer((req, res) => {
    void (async () => {
      const correlationId = correlationOf(req);
      try {
        const url = new URL(`http://localhost${req.url?.startsWith("/") ? req.url : "/"}`);
        const path = url.pathname;
        if (req.method === "GET" && path === "/healthz") return sendJson(req, res, 200, { ok: true, enabled: runtime !== null, ...probeFields(runtime) });
        if (req.method === "GET" && path === "/readyz") {
          // Not ready while the selected scrub engine cannot scrub (SCRUB_ENGINE=wasm not initialised, or trapped).
          const ok = runtime ? scrubEngineReady() && (await runtime.ready()) : false;
          return sendJson(req, res, ok ? 200 : 503, { ok, enabled: runtime !== null, ...probeFields(runtime) });
        }
        if (!runtime)
          return sendJson(req, res, 503, envelope("not_configured", "the session ingest is not enabled", correlationId), { "retry-after": "60", "x-correlation-id": correlationId });
        if (path === SELFHOST_REFRESH_PATH) {
          if (req.method !== "POST")
            return sendJson(req, res, 405, envelope("method_not_allowed", "POST only", correlationId), { allow: "POST", "x-correlation-id": correlationId });
          await drainSmallBody(req);
          return sendJson(req, res, 401, envelope("refresh_not_supported", "this session cannot be refreshed; sign in again", correlationId), { "www-authenticate": "Bearer", "x-correlation-id": correlationId });
        }
        // The operator erase route authenticates its own bearer in the app (session-erase.ts), never through
        // relaycontrol introspection, and has no tenant identity: forward it as is. Only GET and POST are
        // forwarded; any other method is refused here (405, Allow: GET, POST) and never reaches the app.
        const erase = isSessionErasePath(path);
        const method = erase ? (req.method === "POST" ? "POST" : "GET") : runtime.routes[path];
        if (!method) {
          if (path.startsWith("/v1/"))
            return sendJson(req, res, 501, envelope("unsupported_by_backend", "this route is not served by the self-host session ingest", correlationId, { reason: "route_not_served_selfhost" }), { "x-correlation-id": correlationId });
          return sendJson(req, res, 404, envelope("not_found", "not found", correlationId), { "x-correlation-id": correlationId });
        }
        if (req.method !== method) {
          const allow = erase ? "GET, POST" : method;
          return sendJson(req, res, 405, envelope("method_not_allowed", `${allow} only`, correlationId), { allow, "x-correlation-id": correlationId });
        }

        // Identity before any body byte is read.
        let identity: IngestIdentity | undefined;
        if (!erase) try {
          identity = await runtime.verify(req.headers.authorization);
        } catch (error) {
          const e = error instanceof AuthError ? error : new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
          log.info("session ingest", { route: "ingest.auth", outcome: e.code, status: e.status, correlation_id: correlationId });
          return sendJson(req, res, e.status, envelope(e.code, e.message, correlationId), {
            "x-correlation-id": correlationId,
            ...(e.status === 503 ? { "retry-after": "1" } : {}),
            ...(e.status === 401 ? { "www-authenticate": "Bearer" } : {}),
          });
        }

        // A consent-only token (rth:consent, no rth:sync) may reach the upload-consent write and nothing else.
        if (identity && path !== UPLOAD_CONSENT_PATH && !(identity.scopes ?? []).includes(SYNC_SCOPE)) {
          log.info("session ingest", { route: "ingest.auth", outcome: "forbidden", status: 403, correlation_id: correlationId });
          await drainSmallBody(req);
          return sendJson(req, res, 403, envelope("forbidden", `missing required scope: ${SYNC_SCOPE}`, correlationId), { "x-correlation-id": correlationId });
        }

        // RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT=true (only then is the path in the route table): authenticated
        // above, so answer 204 and discard the body. A small declared body is drained; anything else (past the
        // cap, or no length) is left unread and the connection closes after the answer, as on the refresh path.
        if (path === SELFHOST_HEARTBEAT_PATH) {
          await drainSmallBody(req);
          const unread = !req.complete;
          res.writeHead(204, { "cache-control": "private, no-store", "x-correlation-id": correlationId, ...(unread ? { connection: "close" } : {}) });
          res.end(() => {
            if (unread) req.socket?.destroy();
          });
          return;
        }

        const headers = new Headers();
        for (const [k, v] of Object.entries(req.headers)) {
          if (v === undefined || k === IDENTITY_HEADER || k === SIGNATURE_HEADER || (k === "authorization" && !erase)) continue;
          if (Array.isArray(v)) for (const i of v) headers.append(k, i);
          else headers.set(k, v);
        }
        headers.set("x-correlation-id", correlationId);
        const post = method === "POST";
        if (!post) req.resume();
        const request = new Request(url.toString(), {
          method,
          headers,
          ...(post ? { body: Readable.toWeb(req) as unknown as ReadableStream<Uint8Array>, duplex: "half" } : {}),
        } as RequestInit);
        if (identity) runtime.bind(request, identity);
        const response = await runtime.app.fetch(request);
        const out: Record<string, string> = {};
        response.headers.forEach((v, k) => (out[k] = v));
        // The app stopped reading at its cap (413) or never needed the rest: do not keep the upload's socket.
        const unread = post && !req.complete;
        if (unread) out.connection = "close";
        res.writeHead(response.status, out);
        if (response.body) for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) res.write(Buffer.from(chunk));
        res.end(() => {
          if (unread) req.socket?.destroy();
        });
      } catch (err) {
        log.error("session ingest request crashed", errorFields(err));
        if (!res.headersSent) sendJson(req, res, 500, envelope("internal_error", "internal error", correlationId));
        else res.destroy();
      }
    })();
  });
  // A slow or stalled upload cannot hold a connection (and its slot) forever.
  server.requestTimeout = 60_000; // inside the 90 s slot TTL
  server.headersTimeout = 30_000;
  return server;
}

export interface SelfhostIngestService {
  server: Server;
  port: number;
  enabled: boolean;
  close(): Promise<void>;
}

/** Parse the config, assemble (enabled) and listen. `host`/`port` override the bind for tests; `build` replaces the production runtime. */
export async function startSelfhostIngestService(
  env: NodeJS.ProcessEnv = process.env,
  options: { host?: string; port?: number; build?: RuntimeBuilder } = {},
): Promise<SelfhostIngestService> {
  const config = selfhostIngestConfig(env);
  const runtime = config.enabled ? await (options.build ?? productionRuntime)(config, env) : null;
  const server = selfhostIngestHttpServer(runtime);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port ?? config.port, options.host ?? "0.0.0.0", () => {
        server.off("error", reject);
        resolve();
      });
    });
  } catch (error) {
    await runtime?.close();
    throw error;
  }
  const port = (server.address() as AddressInfo).port;
  log.info("session ingest listening", { port, state: config.enabled ? "enabled" : "dormant" });
  let closing: Promise<void> | undefined;
  return {
    server,
    port,
    enabled: config.enabled,
    close: () =>
      (closing ??= new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeIdleConnections?.();
      }).then(() => runtime?.close())),
  };
}

export async function serve(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const service = await startSelfhostIngestService(env);
  const stop = () => {
    setTimeout(() => process.exit(0), 10_000).unref();
    service.close().then(
      () => process.exit(0),
      (err) => {
        log.error("session ingest shutdown failed", errorFields(err));
        process.exit(1);
      },
    );
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

// SCRUB_ENGINE (docs/scrub-engine.md): select, and for wasm verify the pinned engine, before serving.
if (import.meta.main) {
  await initScrubEngine();
  await serve();
}
