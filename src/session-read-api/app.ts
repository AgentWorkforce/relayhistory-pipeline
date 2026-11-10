import { BriefSummaryError, briefSummaryResponse, type BriefSummary } from "./brief-summary.ts";
/**
 * The session read API: an API-compatible subset of relayhistory's /v1 read
 * contract (packages/relayhistory/src/routes/recall.ts), served from ClickHouse
 * `transcript_events` with Supabase for erasure and the history floor.
 *
 * Phase A routes (runbooks/sessions-prod-session-read-api.md):
 *   GET /v1/sessions/:id/events     session replay page
 *   GET /v1/events?session=…        in-session reads (q, since, until, kind)
 *   GET /v1/events?q=…              workspace transcript search
 *
 * Anything this backend cannot answer exactly is `501 unsupported_by_backend`
 * with a `reason`, never a best-effort answer: the caller (Cloud) falls back
 * to relayhistory for it during the transition. A 501 is only ever the answer
 * to a FIRST page (no cursor), where the caller can still fall back cleanly.
 * Once this backend has issued a cursor, the caller cannot restart elsewhere
 * without duplicating pages, so:
 *
 *   - A partial session (`needs_full = 1`) is checked when pagination starts.
 *     A cursor continues the read that check admitted.
 *   - The self-host Postgres service can replay an explicitly named older
 *     session from verified canonical objects. These pages recheck readiness
 *     and bind their cursor to the source generation; workspace search keeps
 *     its retention floor.
 *   - A search page that reaches the history floor is a terminal 200 with a
 *     `continuation` (CONTINUATION below): the rows from the floor up, and
 *     where the existing backend must continue strictly below it.
 *
 * Directory routes (P3b, docs in src/session-read-api/directory.ts):
 *   GET /v1/sessions                session list without `q`, from the Supabase directory
 *   GET /v1/sessions/activity       sessions per UTC day without `q` (span semantics)
 * Both answer 501 `directory_workspace_not_enabled` unless the token's workspace is on
 * READ_API_DIRECTORY_WORKSPACES (the canary allowlist; unset = none), and 501
 * `directory_not_migrated` until migration 20261105034000 is applied. Never Neon.
 *   GET /v1/sessions/:id/timing     recorded timing from the session's canonical S3 objects
 *                                   (session-timing-source.ts, timing.ts), same allowlist.
 *   GET /v1/sessions/:id/turns      the relay session journal from canonical S3 (legacy
 *   GET /v1/sessions/:id/metadata   export + live turns: session-turns-source.ts), same allowlist.
 *   GET /v1/sessions/output         Output from the output facts (output-backend.ts: ClickHouse by
 *                                   default, the self-host Postgres facts with
 *                                   READ_API_OUTPUT_BACKEND=postgres), off unless
 *                                   READ_API_OUTPUT_ENABLED=1 (deps.output), same allowlist; 501
 *                                   until the workspace's backfill watermark exists.
 *   GET /v1/digest/daily            the daily digest from the output facts (digest-backend.ts,
 *                                   READ_API_DIGEST_BACKEND) + the directory, narrative from the
 *                                   Supabase cache / narrative Worker (digest-narrative.ts), off
 *                                   unless READ_API_DIGEST_ENABLED=1 (deps.digest), same allowlist.
 *   GET /v1/coordination/overlaps   deterministic overlap findings from those same facts, off
 *                                   unless READ_API_COORDINATION_OVERLAPS_ENABLED=true, same
 *                                   auth, workspace/project scope, readiness and allowlist.
 *   GET /v1/sessions/titles         cached brief titles of up to 100 `key=<source>:<sessionId>`
 *                                   keys, from Supabase session_briefs (brief-titles.ts). Its
 *                                   own allowlist, READ_API_BRIEF_TITLES_WORKSPACES (unset = none,
 *                                   "*" = all): 501 `brief_titles_workspace_not_enabled`, and 501
 *                                   `brief_titles_not_migrated` until 20261105043000 is applied.
 *
 * Self-host routes (RELAYHISTORY_SELFHOST_READ_API=true only; selfhost.ts; unregistered when off):
 *   POST /v1/cli/login              relaycontrol `sh_at_` token in the body, introspected; answers
 *                                   that same token as the access token (pass-through, no minting)
 *   GET /v1/sessions/:id/catalog    catalog, children and commit links from canonical S3
 *
 *   GET  /v1/sessions/:id/analysis  relayhistory's session analysis (lane F1) from migration
 *   POST /v1/sessions/:id/analysis  20261105046000 and its S3 result objects; same allowlist;
 *                                   POST needs `rth:sync` and the admission principal.
 *
 * Auth: a RelayAuth assertion JWT minted for THIS service's origin with scope
 * `rth:read` (verifyAgentRelayToken: RS256, JWKS, issuer, exact audience).
 * Org and workspace come only from the verified claims.
 */
import { Hono, type Context } from "hono";
import { captureSafely, type AnalyticsPort } from "../shared/analytics.ts";
import { bodyLimit } from "hono/body-limit";
import { ClickHouseError, type ClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import type { SearchServiceBinding } from "../session-pipeline/search-adapter.ts";
import { CursorError, decodeBackendCursor, decodeCursor } from "./cursor.ts";
import { TenantMismatchError, partialSessions, partialSessionsInWorkspace, readBufferedEventPage, readEventPage, type Liveness } from "./page.ts";
import type { EventQuery } from "./queries.ts";
import { CanonicalReadError, canonicalReasonCodeFields, type CanonicalIdentity } from "./canonical.ts";
import { CANONICAL_CURSOR_PREFIX, canonicalEventPage, type CanonicalEventsDeps } from "./canonical-events.ts";
import { AnalysisReadError, admitResponse, assembleAnalyses, type AdmitRpc, type ReadRpc } from "../session-analysis/read.ts";
import type { StoredAnalysisResult } from "../session-analysis/processor.ts";
import { timingFromCanonical, type CanonicalRows } from "./session-timing-source.ts";
import { TimingInputError } from "./timing.ts";
import { EvidenceRequestError, readBabysitterEvidence, type EvidenceDeps } from "./babysitter-evidence.ts";
import { MAX_TURN_HARNESSES, TurnsInputError, metadataResponse, turnRows, turnsResponse, type HarnessState } from "./session-turns-source.ts";
import { clickhouseOutputBackend, type OutputBackend } from "./output-backend.ts";
import { clickhouseDigestBackend, type DigestBackend } from "./digest-backend.ts";
import { clickhouseCoordinationBackend, type CoordinationBackend } from "./coordination-backend.ts";
import { detectCoordination } from "../../packages/coordination-detect/src/index.ts";
import { READ_SETTINGS } from "./queries.ts";
import { DigestInputError, resolveDayWindow } from "./day-window.ts";
import { DIGEST_OMITTED_FIELDS, DigestDirectoryUnavailable, computeStatsHash } from "./digest-query.ts";
import { resolveDigestNarrative, type DigestNarrativeDeps } from "./digest-narrative.ts";
import {
  DIRECTORY_LIST_MAX_LIMIT,
  DirectoryError,
  activityDays,
  decodeCompositeListCursor,
  decodeListCursor,
  decodeQueryCursor,
  directoryPage,
  encodeCompositeListCursor,
  LIST_CURSOR_PREFIX,
  QUERY_CURSOR_PREFIX,
  sessionFromDirectoryRow,
  type BufferedListCursor,
  type DirectoryCursor,
  type Directory,
  type DirectoryListArgs,
  type DirectoryListRow,
  type DirectoryListMetadata,
} from "./directory.ts";
import { droppedTotal, type BufferedSearchGate } from "./buffered-search-gate.ts";
import { parseSessionQuery, queryFingerprint, type SessionQueryTerms } from "./list-query.ts";
import {
  BUFFERED_HEADER,
  BUFFERED_IDS_HEADER,
  BUFFERED_LIST_MAX_LIMIT,
  BufferedFenced,
  fenceSubtype,
  BufferedTooLarge,
  bufferedIdsHeader,
  bufferedSessionSummary,
  cursorTsKey,
  mergeListPage,
  readOwnerBufferedList,
  readOwnerBufferedEvents,
  readOwnerBufferedTurns,
  BufferedProjectionFailed,
  type BufferedScope,
  type BufferedSessionRow,
  type OwnerBufferedDeps,
  type UnverifiedBeforeCandidate,
  OwnerBufferedPhaseTimer,
  type OwnerBufferedReadOptions,
} from "./owner-buffered.ts";
import { BRIEF_TITLES_MAX_KEYS, BriefTitlesError, briefTitleEntries, type BriefTitleKey, type BriefTitles } from "./brief-titles.ts";
import {
  CatalogInputError,
  EMPTY_CATALOG,
  LOGIN_MAX_BODY_BYTES,
  LOGIN_MAX_TTL_MS,
  LoginBodyError,
  catalogFromCanonical,
  parseLoginBody,
  type SelfhostDeps,
} from "./selfhost.ts";
import { registerSelfIssuedRoutes, type SelfIssuedDeps } from "./self-issued.ts";

/**
 * The only scopes POST /v1/cli/login advertises (next to LOGIN_MAX_TTL_MS, selfhost.ts). This backend
 * has no write routes, so a client holding `rth:sync` must not believe sync is available here;
 * relaycontrol refuses mode=sync when the login reply lacks `rth:sync`.
 */
const SERVED_SCOPES = ["rth:read"] as const;
/** Pattern A (SelfhostDeps.syncServed): the self-host ingest edge serves sync for this origin. */
const SERVED_SCOPES_WITH_SYNC = ["rth:read", "rth:sync"] as const;

export interface VerifiedIdentity {
  userId: string;
  orgId: string;
  workspaceId: string;
  scopes: string[];
}

/** What the request claims, for verifiers that bind it (relaycontrol introspection). Never identity. */
export interface VerifyRequest {
  /** The single `workspace=` query value, when given exactly once. */
  workspace?: string;
}

export type Verifier = (bearer: string, request?: VerifyRequest) => Promise<VerifiedIdentity>;

export class AuthError extends Error {
  constructor(readonly status: 401 | 403 | 503, readonly code: string, message: string) {
    super(message);
    this.name = "AuthError";
  }
}

export interface ReadApiDeps {
  verify: Verifier;
  ch?: ClickHouseClient;
  adapter?: SearchServiceBinding;
  liveness: Liveness;
  /** Epoch ms from which ClickHouse holds complete history; 0 = complete. */
  completeSinceMs: number;
  log?: (fields: Record<string, unknown>) => void;
  /**
   * Product analytics (src/shared/analytics.ts): one `session_read.request_completed` per /v1 request
   * whose bearer verified (route pattern, outcome, latency bucket, the verified org/workspace). Absent =
   * none. Captured after the answer is final; a sink can never change or fail the response.
   */
  analytics?: AnalyticsPort;
  timeoutMs?: number;
  /** The Supabase directory (list, activity); absent = those routes answer 501. */
  directory?: Directory;
  /** READ_API_DIRECTORY_WORKSPACES: workspaces the directory routes serve; "*" = all. Absent/empty = none. */
  directoryWorkspaces?: ReadonlySet<string> | "*";
  /** Cached brief titles (brief-titles.ts); absent = GET /v1/sessions/titles answers 501. */
  briefTitles?: BriefTitles;
  /** Node self-host only; absence refuses summary reads. Uses directory workspace admission. */
  briefSummary?: BriefSummary;
  /** READ_API_BRIEF_TITLES_WORKSPACES (parseDirectoryWorkspaces syntax): "*" = all. Absent/empty = none. */
  briefTitleWorkspaces?: ReadonlySet<string> | "*";
  /** Session timing from canonical S3 (session-timing-source.ts); absent = 501 `timing_not_configured`. */
  timing?: TimingDeps;
  /** Relay session turns and metadata from canonical S3 (session-turns-source.ts); absent = 501 `turns_not_configured`. */
  turns?: TurnsDeps;
  /** Session analysis (lane F1, migration 20261105046000); absent = 501 `analysis_not_configured`. */
  analysis?: AnalysisDeps;
  /**
   * READ_API_OUTPUT_ENABLED=1: serve GET /v1/sessions/output from `backend`
   * (output-backend.ts), or, without one, the ClickHouse facts (clickhouse/output-query.ts)
   * with `ch`. Absent, or neither a backend nor `ch` = 501 `output_not_configured`.
   */
  output?: { enabled: true; backend?: OutputBackend };
  /**
   * READ_API_DIGEST_ENABLED=1: serve GET /v1/digest/daily from `backend` (digest-backend.ts),
   * or, without one, the ClickHouse facts with `ch`, plus `directory`; the narrative from
   * `cache` and the service-bound `generator`. Absent, or neither a backend nor `ch` = 501
   * `digest_not_configured`.
   */
  digest?: DigestNarrativeDeps & { backend?: DigestBackend };
  /** READ_API_COORDINATION_OVERLAPS_ENABLED=true: on-request deterministic findings. */
  coordination?: { enabled: boolean; backend?: CoordinationBackend };
  /** Service-only verified canonical object reads for exact sessions outside search retention. */
  canonicalEvents?: CanonicalEventsDeps;
  /** Babysitter evidence reader (babysitter-evidence.ts); absent = 501 `delivery_records_not_configured`. */
  babysitter?: EvidenceDeps;
  /**
   * READ_API_BUFFERED_SEARCH_GATE=on: the per-hit R2 privacy gate (buffered-search-gate.ts) on
   * every ClickHouse event page. Absent (the default, and always on the Lambda) = event pages
   * serve canonical rows only (`origin = 'canonical'`; queries.ts OriginMode), never a buffered row.
   * Requires the ClickHouse binding.
   */
  bufferedSearchGate?: BufferedSearchGate;
  /**
   * Owner-only buffered reads (owner-buffered.ts), RELAYHISTORY_READ_OWNER_BUFFERED. Consulted
   * only when `enabled === true`; otherwise every route behaves exactly as without it. Every
   * buffered response passes the HISTORY_CONTROL_HEAD authority gate; no request header is
   * ever an authority.
   */
  ownerBuffered?: OwnerBufferedDeps;
  /**
   * RELAYHISTORY_SELFHOST_READ_API=true only (selfhost.ts): registers POST /v1/cli/login and
   * GET /v1/sessions/:id/catalog. Absent = neither route exists and nothing else changes.
   */
  selfhost?: SelfhostDeps;
  /** READ_API_AUTH_MODE=self_issued only (self-issued.ts): the token routes. Absent = none exist. */
  selfIssued?: SelfIssuedDeps;
  /**
   * Service entry with the canonical store only: `includeMessageId=true` on session events, filled from the
   * session's canonical objects. Absent: the parameter keeps its 501, so the Lambda and Worker are unchanged.
   */
  eventMessageIds?: Pick<TurnsDeps, "source" | "deadlineMs">;
  /**
   * READ_API_SESSION_QUERY=true: `q` (and `search=metadata` with it) on GET /v1/sessions and
   * GET /v1/sessions/activity, parsed by list-query.ts and answered from the directory's query
   * functions (migration 20261105046010). Absent/false: both keep their 501 `param_q`, unchanged.
   */
  sessionQuery?: boolean;
  /** READ_API_CORS_ALLOWED_ORIGINS, service entry only: exact origins GET read routes answer CORS for. Absent/empty = no CORS. */
  corsAllowedOrigins?: ReadonlySet<string>;
}

export interface AnalysisDeps {
  /** sessions.analysis_read_v1 as the read role, tenant-bound. */
  read(identity: VerifiedIdentity, sessionId: string): Promise<ReadRpc>;
  /** sessions.analysis_request_v1 as the separate admission principal; absent = POST 501. */
  admit?(identity: VerifiedIdentity, sessionId: string): Promise<AdmitRpc>;
  /** Read and verify one result object (session-analysis/results.ts readAnalysisResult). */
  result(ref: StoredAnalysisResult): Promise<unknown>;
  /** Live harnesses of the session in the identity's workspace (relayhistory selectedSource). */
  harnesses(identity: VerifiedIdentity, sessionId: string): Promise<string[]>;
}

export interface TurnsDeps {
  /** Harnesses holding a pointer for the session id, with live/total counts (tenant-bound, read role). */
  harnesses(scope: { orgId: string; workspaceId: string; sessionId: string }): Promise<HarnessState[]>;
  /** One harness scope's canonical rows; `signal` aborts its object reads at the deadline. */
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows>;
  /** Below Cloud's 4 s read timeout. Default 3000. */
  deadlineMs?: number;
  /** READ_API_TURNS_ASSUME_NO_LEGACY: this deployment has no Neon source, so no legacy coverage marker is required. */
  assumeNoLegacy?: boolean;
}

export interface TimingDeps {
  /** Pointer rows of the session, live or tombstoned (tenant-bound, read role). */
  state(identity: CanonicalIdentity): Promise<{ pointers: number; live: number }>;
  /** The session's canonical projection; `signal` aborts its object reads at the deadline. */
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows>;
  /** READ_API_TIMING_ASSUME_NO_TURNS (session-timing-source.ts TimingOptions). */
  assumeNoTurns: boolean;
  /** Below Cloud's 4 s read timeout, so a slow session falls back cleanly as a 501. Default 3000. */
  deadlineMs?: number;
}

/** READ_API_DIRECTORY_WORKSPACES: unset or blank = none; "*" = every workspace; else comma-separated ids. */
export function parseDirectoryWorkspaces(raw: string | undefined): ReadonlySet<string> | "*" {
  const value = raw?.trim() ?? "";
  if (value === "*") return "*";
  return new Set(value.split(",").map((v) => v.trim()).filter(Boolean));
}

/** The titles `key` values: 1 to 100 valid `<source>:<sessionId>` keys, duplicates dropped. */
function titleKeys(raw: readonly string[]): BriefTitleKey[] {
  if (raw.length < 1 || raw.length > BRIEF_TITLES_MAX_KEYS) throw bad(`key must be given 1 to ${BRIEF_TITLES_MAX_KEYS} times`);
  const seen = new Set<string>();
  const keys: BriefTitleKey[] = [];
  for (const value of raw) {
    const at = value.indexOf(":");
    const source = at < 0 ? "" : value.slice(0, at);
    const sessionId = at < 0 ? "" : value.slice(at + 1);
    if (!HARNESS_NAME.test(source)) throw bad("key source is invalid");
    if (sessionId.length < 1 || sessionId.length > MAX_LIST_ID_LENGTH) throw bad("key sessionId is invalid");
    if (seen.has(value)) continue;
    seen.add(value);
    keys.push({ source, sessionId });
  }
  return keys;
}

/** relayhistory routes/recall.ts limits for the session list and activity. */
export const SESSIONS_DEFAULT_LIMIT = 50;
export const SESSIONS_MAX_LIMIT = 500;
export const MAX_SESSION_ACTIVITY_DAYS = 92;
const MAX_LIST_SOURCES = 16;
const MAX_LIST_USERS = 20;
const MAX_LIST_ID_LENGTH = 255;
const MAX_AGENT_LENGTH = 200;
const HARNESS_NAME = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
/** Session-list parameters the directory cannot answer: event or text questions. */
const LIST_UNSUPPORTED_PARAMS = ["q", "search", "project", "noProject", "kind", "taskRef", "noTaskRef", "tag", "since", "until"];

/** relayhistory lib/recall.ts limits. */
export const EVENTS_DEFAULT_LIMIT = 200;
export const EVENTS_MAX_LIMIT = 1000;
/** Search pages above this are not served here (Cloud asks for at most 50). */
export const SEARCH_MAX_LIMIT = 100;
export const MAX_Q_CHARS = 500;
const MAX_ID_CHARS = 512;

/**
 * The terminal-page signal for a search cursor page that reached the history
 * floor. `events` holds every match from the floor up (ClickHouse is complete
 * there); the caller continues the same read at relayhistory with `until` set
 * to `until` (inclusive, one microsecond below the floor, so relayhistory's
 * microsecond timestamps neither repeat a row served here nor skip one) and no
 * cursor. Ordering is newest first, so every later relayhistory page is older
 * still and the pagination never revisits this backend. The response carries
 * `X-Sessions-Read-Continuation: history_floor`; a caller that sees the header
 * must finish the page, and must not pass it on as a short final page.
 */
export interface Continuation {
  backend: "relayhistory";
  reason: "history_floor";
  until: string;
}

/** One microsecond below `floorMs`, as an ISO timestamp relayhistory accepts (six fractional digits). */
export function continuationUntil(floorMs: number): string {
  return new Date(floorMs - 1).toISOString().replace(/Z$/, "999Z");
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly reason?: string,
    /** Log-only reason codes (e.g. CanonicalReadError.details); never sent in the response body. */
    readonly details: readonly string[] = [],
  ) {
    super(message);
  }
}
const unsupported = (reason: string, details: readonly string[] = []) =>
  new HttpError(501, "unsupported_by_backend", `not served by this backend: ${reason}`, reason, details);
const bad = (message: string) => new HttpError(400, "bad_request", message);

/** relayhistory clampLimit: truncate, clamp to [1, max]; junk gives the default. */
export function clampLimit(raw: string | undefined, fallback: number, max: number): number {
  if (raw === undefined) return fallback;
  const n = Math.trunc(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(1, n));
}

function positiveInt(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

function parseTime(raw: string | undefined, name: string): number | undefined {
  if (raw === undefined || raw === "") return undefined;
  const t = Date.parse(raw);
  if (Number.isNaN(t)) throw bad(`${name} must be a date`);
  return t;
}

/** Exactly one value or none; a repeated parameter is a 400 like relayhistory's. */
function single(c: Context, name: string): string | undefined {
  const all = c.req.queries(name);
  if (!all || all.length === 0) return undefined;
  if (all.length > 1) throw bad(`${name} must be given at most once`);
  return all[0];
}

/** Parameters this phase never serves: their presence routes the call back to relayhistory. */
const UNSUPPORTED_PARAMS = ["taskRef", "noTaskRef", "tag", "agent", "noProject", "projectContains", "includeMessageId", "search", "kinds"];

function rejectUnsupported(c: Context, served: readonly string[] = []): void {
  for (const p of UNSUPPORTED_PARAMS) if (!served.includes(p) && c.req.queries(p)?.length) throw unsupported(`param_${p}`);
}

function attestWorkspace(c: Context, identity: VerifiedIdentity): void {
  const all = c.req.queries("workspace");
  // relayhistory serves an org-wide read without `workspace`; this backend is
  // workspace-scoped only, so that read stays there.
  if (!all || all.length === 0) throw unsupported("org_scope");
  if (all.length > 1 || !all[0]) throw bad("workspace must be given exactly once");
  if (all[0] !== identity.workspaceId) throw new HttpError(403, "forbidden", "workspace does not match the token");
}

export function createReadApi(deps: ReadApiDeps) {
  if (Boolean(deps.ch) === Boolean(deps.adapter)) throw new Error("read API requires exactly one search service binding");
  if (deps.bufferedSearchGate && !deps.ch) throw new Error("buffered search gate requires the ClickHouse binding");
  const app = new Hono<{
    Variables: {
      identity: VerifiedIdentity;
      correlationId: string;
      /** Date.now() when the request entered the log middleware. */
      requestStarted: number;
      /** READ_API_OWNER_BUFFERED_PHASE_LOG=1: the events route's owner-buffered phase times, added to the request line. */
      obPhases?: OwnerBufferedPhaseTimer;
    };
  }>();
  const log = deps.log ?? (() => {});

  app.use("*", async (c, next) => {
    const incoming = c.req.header("x-correlation-id");
    const correlationId = incoming && /^[A-Za-z0-9._:-]{1,128}$/.test(incoming) ? incoming : crypto.randomUUID();
    c.set("correlationId", correlationId);
    const started = Date.now();
    c.set("requestStarted", started);
    await next();
    c.header("Cache-Control", "private, no-store");
    c.header("X-Sessions-Read-Backend", "96bulls");
    c.header("X-Correlation-Id", correlationId);
    const phases = c.get("obPhases");
    const durationMs = Date.now() - started;
    log({
      route: c.req.routePath, status: c.res.status, duration_ms: durationMs, correlation_id: correlationId,
      ...(phases && !phases.emitted ? phases.fields() : {}),
    });
    // Only a verified identity is attributed (workspace scope); an unauthenticated request is not an event.
    const identity = c.get("identity") as VerifiedIdentity | undefined;
    if (identity) captureSafely(deps.analytics, {
      kind: "session_read.request_completed", traceId: correlationId, orgId: identity.orgId, workspaceId: identity.workspaceId,
      route: c.req.routePath, status: c.res.status, durationMs,
    });
  });

  app.get("/health", (c) => c.json({ ok: true }));

  /**
   * POST /v1/cli/login (self-host only). Registered BEFORE the /v1/* bearer middleware: the
   * token arrives in the body, and this handler answers without calling next(), so that
   * middleware never runs for it. Body `{ agentRelayToken, label? }`; the token must be a
   * relaycontrol `sh_at_` token and is introspected exactly as a relaycontrol read bearer is
   * (auth.ts relaycontrolLogin). Pass-through: the access token IS the presented token, so
   * nothing is minted, signed or stored, and every later read authenticates it through the
   * same introspection. accessTokenExpiresAt = min(token exp, now + 15 min) is advisory: the
   * server keeps accepting the token until relaycontrol's own exp or revocation. There is no
   * refresh route: refreshToken repeats the access token with the same expiry, and a client
   * logs in again. A workspace off the reads' canary allowlist (READ_API_DIRECTORY_WORKSPACES)
   * is refused with 501 `login_workspace_not_enabled`, after introspection. The reply's `scopes`
   * is the token's scopes filtered to SERVED_SCOPES; a token with none of them is 403 `forbidden`
   * after the canary and expiry checks (relaycontrolLogin already refuses no `rth:read` with the
   * same 403 during introspection; this is the handler's own fail-closed check). The label is
   * validated and otherwise unused. Token values and scopes are never logged.
   */
  const selfhost = deps.selfhost;
  if (selfhost?.login) {
    const exchange = selfhost.login;
    const now = selfhost.now ?? (() => Date.now());
    const loginError = (c: Context, status: 400 | 401 | 403 | 413 | 503, code: string, message: string) => {
      if (status === 503) c.header("Retry-After", "1");
      log({ route: "/v1/cli/login", reason: "cli_login_refused", code, status, correlation_id: c.get("correlationId") });
      return c.json({ error: { code, message }, correlationId: c.get("correlationId") }, status);
    };
    app.post(
      "/v1/cli/login",
      bodyLimit({ maxSize: LOGIN_MAX_BODY_BYTES, onError: (c) => loginError(c, 413, "body_too_large", "body is too large") }),
      async (c) => {
        let token: string;
        try {
          token = parseLoginBody(await c.req.text()).agentRelayToken;
        } catch (error) {
          if (error instanceof LoginBodyError) return loginError(c, 400, error.code, error.message);
          return loginError(c, 400, "invalid_body", "body must be a JSON object");
        }
        let grant;
        try {
          grant = await exchange(token);
        } catch (error) {
          const e = error instanceof AuthError ? error : new AuthError(503, "relaycontrol_unavailable", "token introspection is unavailable");
          return loginError(c, e.status, e.code, e.message);
        }
        // Fail closed: the reads' canary allowlist applies before any success (no workspace header on refusal).
        try {
          requireCanary(grant.identity, "login");
        } catch (error) {
          return fail(c, error);
        }
        const at = now();
        const expiresAtMs = Math.min(grant.expiresAtMs, at + LOGIN_MAX_TTL_MS);
        if (!(expiresAtMs > at)) return loginError(c, 401, "token_expired", "token has expired");
        // Advertise only the scopes served here, after canary and expiry: never rth:sync, unless the
        // self-host ingest edge serves it (syncServed) AND introspection granted it.
        const served: readonly string[] = selfhost.syncServed === true ? SERVED_SCOPES_WITH_SYNC : SERVED_SCOPES;
        const scopes = grant.identity.scopes.filter((s) => served.includes(s));
        if (scopes.length === 0) return loginError(c, 403, "forbidden", "missing required scope: rth:read");
        const expiresAt = new Date(expiresAtMs).toISOString();
        log({ route: "/v1/cli/login", reason: "cli_login", status: 200, workspace_id: grant.identity.workspaceId, correlation_id: c.get("correlationId") });
        c.header("X-Relayhistory-Workspace-Id", grant.identity.workspaceId);
        return c.json({
          accessToken: token,
          accessTokenExpiresAt: expiresAt,
          refreshToken: token,
          refreshTokenExpiresAt: expiresAt,
          tokenType: "Bearer",
          scopes,
        });
      },
    );
  }

  if (deps.selfIssued) registerSelfIssuedRoutes(app as never, deps.selfIssued, log);

  /**
   * Browser reads (relay-webapp): exact allowlisted origins only, GET read routes only, bearer
   * header only, so never credentials and never the token routes. A preflight is answered here,
   * before the bearer middleware; an origin off the list gets no allow headers and the browser
   * refuses the response.
   */
  const corsOrigins = deps.corsAllowedOrigins;
  if (corsOrigins && corsOrigins.size > 0) {
    const readRoute = (path: string) => !path.startsWith("/v1/cli/") && !path.startsWith("/v1/auth/");
    app.use("/v1/*", async (c, next) => {
      const origin = c.req.header("origin");
      const allowed = origin !== undefined && corsOrigins.has(origin) && readRoute(c.req.path);
      c.header("Vary", "Origin");
      if (allowed) {
        c.header("Access-Control-Allow-Origin", origin);
        c.header("Access-Control-Expose-Headers", "X-Correlation-Id, X-Relayhistory-Workspace-Id, X-Sessions-Read-Continuation");
      }
      if (c.req.method === "OPTIONS") {
        if (allowed) {
          c.header("Access-Control-Allow-Methods", "GET");
          c.header("Access-Control-Allow-Headers", "authorization, x-correlation-id");
          c.header("Access-Control-Max-Age", "600");
        }
        return c.body(null, 204);
      }
      await next();
    });
  }

  app.use("/v1/*", async (c, next) => {
    const header = c.req.header("authorization") ?? "";
    const m = /^Bearer\s+(\S+)$/i.exec(header);
    if (!m) return c.json({ error: { code: "missing_authorization", message: "Bearer token required" }, correlationId: c.get("correlationId") }, 401);
    let identity: VerifiedIdentity;
    try {
      const workspaces = c.req.queries("workspace");
      identity = await deps.verify(m[1]!, workspaces?.length === 1 && workspaces[0] ? { workspace: workspaces[0] } : {});
    } catch (error) {
      const e = error instanceof AuthError ? error : new AuthError(401, "invalid_token", "token is not valid");
      if (e.status === 503) c.header("Retry-After", "1");
      return c.json({ error: { code: e.code, message: e.message }, correlationId: c.get("correlationId") }, e.status);
    }
    if (!identity.orgId || !identity.workspaceId)
      return c.json({ error: { code: "invalid_token", message: "token carries no workspace" }, correlationId: c.get("correlationId") }, 401);
    if (!identity.scopes.includes("rth:read"))
      return c.json({ error: { code: "forbidden", message: "missing required scope: rth:read" }, correlationId: c.get("correlationId") }, 403);
    c.set("identity", identity);
    await next();
    c.header("X-Relayhistory-Workspace-Id", identity.workspaceId);
  });

  const fail = (c: Context, error: unknown) => {
    const correlationId = c.get("correlationId") as string;
    const identity = c.get("identity") as VerifiedIdentity | undefined;
    if (error instanceof HttpError) {
      if (error.code === "buffered_fenced") {
        c.header("Retry-After", "1");
        log({ route: c.req.routePath, reason: "buffered_fenced", status: 503, correlation_id: correlationId });
      }
      if (error.status === 501)
        log({
          route: c.req.routePath,
          reason: error.reason,
          status: 501,
          ...canonicalReasonCodeFields(error.details),
          correlation_id: correlationId,
        });
      return c.json({ error: { code: error.code, message: error.message, ...(error.reason ? { reason: error.reason } : {}) }, correlationId }, error.status as never);
    }
    if (error instanceof CursorError) {
      return error.code === "foreign_cursor"
        ? c.json({ error: { code: "unsupported_by_backend", message: error.message, reason: "foreign_cursor" }, correlationId }, 501)
        : c.json({ error: { code: "bad_request", message: error.message }, correlationId }, 400);
    }
    if (error instanceof TenantMismatchError) {
      log({ route: c.req.routePath, reason: "cross_tenant_rows", status: 500, org_id: identity?.orgId, workspace_id: identity?.workspaceId, correlation_id: correlationId });
      return c.json({ error: { code: "internal", message: "read refused" }, correlationId }, 500);
    }
    if (error instanceof ClickHouseError) {
      // 159 TIMEOUT_EXCEEDED: the same answer relayhistory gives a cancelled search.
      const timeout = error.code === "159";
      log({ route: c.req.routePath, reason: timeout ? "clickhouse_timeout" : "clickhouse_error", error_code: error.code ?? String(error.status), status: 503, correlation_id: correlationId });
      return c.json({ error: { code: timeout ? "search_timeout" : "backend_unavailable", message: timeout ? "search took too long" : "read backend unavailable" }, correlationId }, 503);
    }
    log({ route: c.req.routePath, reason: "unavailable", error_code: (error as { name?: string })?.name ?? "error", status: 503, correlation_id: correlationId });
    return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
  };

  const pageDeps = () => ({ ch: deps.ch, adapter: deps.adapter, completeSinceMs: deps.completeSinceMs, liveness: deps.liveness, timeoutMs: deps.timeoutMs, bufferedGate: deps.bufferedSearchGate });

  /** Gate telemetry: two integer counts only (checked, and the per-reason drops summed), never ids or content. */
  async function gatedPage(c: Context, input: EventQuery) {
    const page = await readEventPage(pageDeps(), input);
    if (page.buffered && page.buffered.checked > 0)
      log({ route: c.req.routePath, reason: "buffered_gate", buffered_checked: page.buffered.checked, buffered_dropped: droppedTotal(page.buffered.dropped), correlation_id: c.get("correlationId") });
    return page;
  }

  /**
   * Session reads: erased → empty page; history older than the floor → 501;
   * a partial session (`needs_full = 1`) → 501 `session_partial` when
   * pagination starts (no cursor), so the caller reads it from relayhistory.
   */
  async function sessionGate(
    identity: VerifiedIdentity, sessionId: string, source: string | undefined, continuing: boolean,
    /** The same sessionStart answer when the owner-buffered events branch already asked it (one call per request). */
    known?: Awaited<ReturnType<Liveness["sessionStart"]>>,
  ): Promise<"empty" | "ok"> {
    const start = known ?? (await deps.liveness.sessionStart(identity.orgId, identity.workspaceId, sessionId, source));
    if (start.pointers > 0 && start.live === 0) return "empty";
    if (deps.completeSinceMs > 0) {
      if (start.createdAtMs === null) throw unsupported("history_floor_unknown_session");
      if (start.createdAtMs < deps.completeSinceMs) throw unsupported("history_floor");
    }
    if (!continuing && (await partialSessions(pageDeps(), identity.orgId, sessionId, identity.workspaceId)).length > 0) throw unsupported("session_partial");
    return "ok";
  }

  /**
   * Owner-only buffered reads (RELAYHISTORY_READ_OWNER_BUFFERED): the deps when the flag is on,
   * the token carries a user, and the token's workspace is on both READ_API_DIRECTORY_WORKSPACES
   * and the owner-buffered canary allowlist READ_API_OWNER_BUFFERED_WORKSPACES (each "*" or
   * listing it; unset/empty = none). Otherwise undefined and the caller's route runs exactly as
   * without the flag. With READ_API_OWNER_BUFFERED_INDEX_VERIFY=on the deps carry indexCheck, and
   * every gated read (owner-buffered.ts) proves ingest_buffer_outbox_user_buffered_idx first: not
   * proven is a BufferedFenced before any candidate (detail routes answer as without the flag,
   * logged buffered_unverified; the list is fenced), with error_code index_missing, index_invalid or
   * index_unverified.
   */
  function ownerBufferedFor(identity: VerifiedIdentity): OwnerBufferedDeps | undefined {
    const ob = deps.ownerBuffered;
    if (!ob || ob.enabled !== true || !identity.userId) return undefined;
    const listed = (allowed: ReadonlySet<string> | "*" | undefined) => !!allowed && (allowed === "*" || allowed.has(identity.workspaceId));
    if (!listed(deps.directoryWorkspaces) || !listed(ob.workspaces)) return undefined;
    return ob;
  }
  const bufferedScope = (identity: VerifiedIdentity): BufferedScope => ({ orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId });
  const bufferedFenced = () => new HttpError(503, "buffered_fenced", "buffered read fenced; retry", "buffered_fenced");

  /**
   * The caller's buffered turn rows of one session for a detail route, through the gated path
   * (readOwnerBufferedTurns: authority observed before the RPCs and again after every S3 read,
   * before any byte). Authority unavailable before any buffered candidate was found: logged,
   * null (the route's existing 501, exactly as without the flag). Fenced once a candidate was
   * found: 503 buffered_fenced. Over the byte bounds: logged `buffered_too_large`, null. A
   * tenant mismatch is a 500; any other failure is logged and answers null.
   */
  const bufferedTurns = (c: Context, identity: VerifiedIdentity, ob: OwnerBufferedDeps, sessionId: string, source: string) =>
    bufferedDetail(c, () => readOwnerBufferedTurns(ob, bufferedScope(identity), source, sessionId));
  /** The caller's buffered event rows of one session (readOwnerBufferedEvents), with bufferedTurns' outcomes. */
  const bufferedEvents = (c: Context, identity: VerifiedIdentity, ob: OwnerBufferedDeps, sessionId: string, source: string, opts: OwnerBufferedReadOptions) =>
    bufferedDetail(c, () => readOwnerBufferedEvents(ob, bufferedScope(identity), source, sessionId, opts));

  /**
   * READ_API_OWNER_BUFFERED_PHASE_LOG=1: when the client cancels the request (request.signal
   * aborts; in workerd only with the enable_request_signal compatibility flag), log the phase
   * times once, at once, with `aborted_phase` (the phase in progress, else "none"): the request
   * line may never be written for a canceled invocation. Returns the listener's removal.
   */
  function logPhasesOnAbort(c: Context, phases: OwnerBufferedPhaseTimer): () => void {
    const signal = c.req.raw.signal;
    const onAbort = () => {
      if (phases.emitted) return;
      phases.emitted = true;
      log({
        route: c.req.routePath, reason: "request_aborted", duration_ms: Date.now() - (c.get("requestStarted") as number),
        correlation_id: c.get("correlationId"), ...phases.fields("none"),
      });
    };
    signal.addEventListener("abort", onAbort, { once: true });
    return () => signal.removeEventListener("abort", onAbort);
  }

  async function bufferedDetail<T extends object>(c: Context, read: () => Promise<T | null | UnverifiedBeforeCandidate>): Promise<T | null> {
    try {
      const served = await read();
      if (served && "unverifiedBeforeCandidate" in served) {
        log({ route: c.req.routePath, reason: "buffered_unverified", error_code: fenceSubtype(served.unverifiedBeforeCandidate), ...(served.unverifiedSqlstateClass ? { sqlstate: served.unverifiedSqlstateClass } : {}), correlation_id: c.get("correlationId") });
        return null;
      }
      return served as T | null;
    } catch (error) {
      if (error instanceof BufferedTooLarge) {
        log({ route: c.req.routePath, reason: "buffered_too_large", correlation_id: c.get("correlationId") });
        return null;
      }
      if (error instanceof BufferedFenced) {
        log({ route: c.req.routePath, reason: "buffered_fenced", error_code: fenceSubtype(error.reason), ...(error.sqlstateClass ? { sqlstate: error.sqlstateClass } : {}), correlation_id: c.get("correlationId") });
        throw bufferedFenced();
      }
      if (error instanceof TenantMismatchError || error instanceof HttpError) throw error;
      if (error instanceof BufferedProjectionFailed) {
        log({ route: c.req.routePath, reason: "buffered_projection_failed", error_code: error.reason, correlation_id: c.get("correlationId") });
        return null;
      }
      log({ route: c.req.routePath, reason: "buffered_detail_error", error_code: (error as { name?: string })?.name ?? "error", correlation_id: c.get("correlationId") });
      return null;
    }
  }

  /**
   * A cursor issued by one backend (canonical or owner-buffered) presented while the other now
   * serves this session. Refused rather than restarted, skipped or continued: a buffered position
   * continued on canonical rows would skip the partial-session check and could read a canonical
   * transcript that is not yet complete against the buffered set (docs/session-read-api-design.md).
   */
  const cursorTransition = () =>
    new HttpError(409, "cursor_backend_transition", "the cursor was issued by another backend for this session; restart pagination without a cursor", "cursor_backend_transition");

  /**
   * relayhistory's `messageId` (record.payload.message_id, null when the record has none) for one
   * page of a session's events. The search store does not hold it, so it comes from the session's
   * canonical objects, tenant-bound like every canonical read and under the same deadline. `source`
   * is required because a canonical identity names one harness. A refused read is a 501: a page of
   * missing ids would look like messages that have none.
   */
  async function withMessageIds<E extends { eventId: string; machineId: string; kind: string }>(identity: VerifiedIdentity, sessionId: string, source: string | undefined, events: E[]): Promise<Array<E & { messageId: string | null }>> {
    if (source === undefined) throw unsupported("message_id_source_required");
    if (events.length === 0) return [];
    const canonical = deps.eventMessageIds!;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let snapshot: CanonicalRows;
    try {
      snapshot = await Promise.race([
        canonical.source({ orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source }, controller.signal),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(new CanonicalReadError("deadline"));
            controller.abort();
          }, canonical.deadlineMs ?? 3000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
    const key = (eventId: unknown, machineId: unknown, kind: unknown) => JSON.stringify([eventId, machineId, kind]);
    const ids = new Map<string, string>();
    const known = new Set<string>();
    for (const { table, row } of snapshot.rows) {
      if (table !== "convergence_events") continue;
      known.add(key(row.event_id, row.machine_id, row.kind));
      const payload = (row.record as { payload?: { message_id?: unknown } } | undefined)?.payload;
      if (typeof payload?.message_id === "string") ids.set(key(row.event_id, row.machine_id, row.kind), payload.message_id);
    }
    // An event the canonical objects do not hold (backfilled history) has an unknown id, not a null one.
    if (events.some((event) => !known.has(key(event.eventId, event.machineId, event.kind)))) throw unsupported("message_id_no_canonical_event");
    return events.map((event) => ({ ...event, messageId: ids.get(key(event.eventId, event.machineId, event.kind)) ?? null }));
  }

  app.get("/v1/sessions/:sessionId/events", async (c) => {
    let stopAbortLog: (() => void) | undefined;
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c, deps.eventMessageIds ? ["includeMessageId"] : []);
      for (const p of ["q", "since", "until", "kind", "session", "project"]) if (c.req.queries(p)?.length) throw unsupported(`param_${p}`);
      const includeMessageId = deps.eventMessageIds !== undefined && single(c, "includeMessageId") === "true";
      const sessionId = c.req.param("sessionId");
      if (!sessionId || sessionId.length > MAX_ID_CHARS) throw bad("sessionId is not valid");
      const source = single(c, "source") || undefined;
      const order = single(c, "order") === "desc" ? "desc" : "asc";
      const limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      const maxContent = positiveInt(single(c, "maxContent"));
      const rawCursor = single(c, "cursor");
      // Owner-buffered events (READ_API_OWNER_BUFFERED_EVENTS=on on top of owner-buffered reads and
      // both allowlists, `source` given): decided before sessionGate, on the pointer count alone.
      // Any canonical pointer: canonical serves the whole response and no buffered read runs.
      let start: Awaited<ReturnType<Liveness["sessionStart"]>> | undefined;
      const ob = source !== undefined ? ownerBufferedFor(identity) : undefined;
      if (ob?.events === true && source !== undefined && !rawCursor?.startsWith(CANONICAL_CURSOR_PREFIX)) {
        const presented = decodeBackendCursor(rawCursor);
        // READ_API_OWNER_BUFFERED_PHASE_LOG=1: per-phase times on the request line (durations only).
        const phases = ob.phaseLog === true ? new OwnerBufferedPhaseTimer() : undefined;
        if (phases) {
          c.set("obPhases", phases);
          stopAbortLog = logPhasesOnAbort(c, phases);
        }
        const sessionStart = () => deps.liveness.sessionStart(identity.orgId, identity.workspaceId, sessionId, source);
        start = await (phases ? phases.time("session_start", sessionStart) : sessionStart());
        if (start.pointers === 0) {
          const served = await bufferedEvents(c, identity, ob, sessionId, source, { signal: c.req.raw.signal, phases });
          if (served && (served.erased || served.rows.length > 0)) {
            if (served.erased) {
              c.header(BUFFERED_HEADER, "1");
              return c.json({ sessionId, events: [], nextCursor: null, correlationId: c.get("correlationId") });
            }
            if (presented?.backend === "canonical") throw cursorTransition();
            const page = readBufferedEventPage(served.rows, { order, limit, maxContent, cursor: presented?.position });
            c.header(BUFFERED_HEADER, "1");
            log({ route: c.req.routePath, reason: "buffered_events_served", events: page.events.length, correlation_id: c.get("correlationId") });
            return c.json({ sessionId, events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
          }
        }
        // Hidden, or canonical now serves: a buffered cursor cannot continue here.
        if (presented?.backend === "buffered") throw cursorTransition();
      }
      const objectCursor = rawCursor?.startsWith(CANONICAL_CURSOR_PREFIX) === true;
      const cursor = objectCursor ? undefined : decodeCursor(rawCursor);
      // Search retention is not canonical object retention. Only the Node
      // service with a verified Locker source can answer this exact old scope;
      // workspace search and the hosted/Worker paths retain their floor.
      if (source !== undefined && deps.selfhost && deps.canonicalEvents && deps.adapter?.index.backend === "postgres" &&
          (deps.directoryWorkspaces === "*" || deps.directoryWorkspaces?.has(identity.workspaceId))) {
        start ??= await deps.liveness.sessionStart(identity.orgId, identity.workspaceId, sessionId, source);
        if (start.live > 0 && start.createdAtMs !== null && (start.createdAtMs < deps.completeSinceMs || objectCursor)) {
          if (cursor) throw unsupported("canonical_cursor_required");
          const check = async () => {
            if ((await partialSessions(pageDeps(), identity.orgId, sessionId, identity.workspaceId)).length > 0)
              throw unsupported("session_partial");
          };
          // Recheck completeness on every page and after the generation-fenced
          // object read; a cursor never licenses a stale/partial projection.
          await check();
          let page;
          try {
            page = await canonicalEventPage(deps.canonicalEvents, deps.liveness,
              { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, order, limit, maxContent, cursor }, objectCursor ? rawCursor : undefined);
          } catch (error) {
            if (error instanceof CanonicalReadError) throw error.status === 400 ? bad("canonical cursor is not valid") : unsupported(`events_canonical_${error.reason}`, error.details);
            throw error;
          }
          const events = includeMessageId ? await withMessageIds(identity, sessionId, source, page.events) : page.events;
          await check();
          return c.json({ sessionId, events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
        }
      }
      if (objectCursor) throw unsupported("canonical_cursor_scope");
      if ((await sessionGate(identity, sessionId, source, cursor !== undefined, start)) === "empty")
        return c.json({ sessionId, events: [], nextCursor: null, correlationId: c.get("correlationId") });
      const page = await gatedPage(
        c,
        { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, order, limit, maxContent, cursor },
      );
      const events = includeMessageId ? await withMessageIds(identity, sessionId, source, page.events) : page.events;
      return c.json({ sessionId, events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      if (error instanceof CanonicalReadError) return fail(c, unsupported(`message_id_canonical_${error.reason}`, error.details));
      return fail(c, error);
    } finally {
      stopAbortLog?.();
    }
  });

  app.get("/v1/events", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      rejectUnsupported(c);
      const sessionId = single(c, "session") || undefined;
      const qRaw = single(c, "q");
      const q = qRaw === undefined || qRaw === "" ? undefined : qRaw;
      if (q !== undefined && q.length > MAX_Q_CHARS) throw bad(`q must be at most ${MAX_Q_CHARS} characters`);
      if (sessionId === undefined && q === undefined) throw unsupported("unfiltered_events");
      if (sessionId !== undefined && sessionId.length > MAX_ID_CHARS) throw bad("session is not valid");
      const source = single(c, "source") || undefined;
      const project = single(c, "project") || undefined;
      const kind = single(c, "kind") || undefined;
      const order: "asc" | "desc" = single(c, "order") === "asc" ? "asc" : "desc";
      const sinceMs = parseTime(single(c, "since"), "since");
      const untilMs = parseTime(single(c, "until"), "until");
      const maxContent = positiveInt(single(c, "maxContent"));
      const cursor = decodeCursor(single(c, "cursor"));
      const base: EventQuery = { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId, source, kind, projectId: project, q, sinceMs, untilMs, order, limit: 0, maxContent, cursor };

      if (sessionId !== undefined) {
        base.limit = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
        if ((await sessionGate(identity, sessionId, source, cursor !== undefined)) === "empty")
          return c.json({ events: [], nextCursor: null, correlationId: c.get("correlationId") });
        const page = await gatedPage(c, base);
        return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
      }

      // Workspace search: newest first only.
      if (order === "asc") throw unsupported("search_ascending");
      const requested = clampLimit(single(c, "limit"), EVENTS_DEFAULT_LIMIT, EVENTS_MAX_LIMIT);
      if (requested > SEARCH_MAX_LIMIT) throw unsupported("search_limit");
      base.limit = requested;
      // History floor: search only where ClickHouse is complete; a page that
      // reaches the floor without filling would be missing older rows.
      const belowFloor = deps.completeSinceMs > 0 && (sinceMs === undefined || sinceMs < deps.completeSinceMs);
      if (belowFloor) base.sinceMs = deps.completeSinceMs;
      if (untilMs !== undefined && base.sinceMs !== undefined && untilMs < base.sinceMs) throw unsupported("history_floor");
      // A partial session of this workspace may hold matches this backend
      // lacks, so the search starts at relayhistory. Partial sessions of other
      // workspaces of the org cannot hold this workspace's rows and do not count
      // (page.ts partialSessionsInWorkspace says how one is attributed).
      if (cursor === undefined) {
        const partials = await partialSessionsInWorkspace(pageDeps(), identity.orgId, identity.workspaceId);
        if (partials.overflow) throw unsupported("session_partial_unbounded");
        if (partials.sessions.length > 0) throw unsupported("session_partial");
      }
      const page = await gatedPage(c, base);
      if (belowFloor && page.nextCursor === null) {
        // First page: nothing was issued yet, so the whole read falls back.
        if (cursor === undefined) throw unsupported("history_floor");
        // Cursor page: end this backend's part cleanly and hand the rest over.
        const continuation: Continuation = { backend: "relayhistory", reason: "history_floor", until: continuationUntil(deps.completeSinceMs) };
        log({ route: c.req.routePath, reason: "history_floor_continuation", status: 200, correlation_id: c.get("correlationId") });
        // Flags the page so the caller finishes it without parsing every page body.
        c.header("X-Sessions-Read-Continuation", "history_floor");
        return c.json({ events: page.events, nextCursor: null, continuation, correlationId: c.get("correlationId") });
      }
      return c.json({ events: page.events, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });

  /** The P3 canary allowlist (READ_API_DIRECTORY_WORKSPACES), shared by the directory and timing routes. */
  function requireCanary(identity: VerifiedIdentity, route: "directory" | "timing" | "turns" | "analysis" | "output" | "digest" | "coordination" | "catalog" | "login" | "summary"): void {
    const allowed = deps.directoryWorkspaces;
    if (!allowed || (allowed !== "*" && !allowed.has(identity.workspaceId))) throw unsupported(`${route}_workspace_not_enabled`);
  }

  /** The canary allowlist, then the directory itself. */
  function directoryFor(identity: VerifiedIdentity): Directory {
    requireCanary(identity, "directory");
    if (!deps.directory) throw unsupported("directory_not_configured");
    return deps.directory;
  }

  const directoryFail = (c: Context, error: unknown) => {
    if (error instanceof DirectoryError) {
      if (error.reason === "agent_filter_not_ready") return fail(c, unsupported("agent_filter_not_ready"));
      if (error.reason === "tenant_mismatch") return fail(c, new TenantMismatchError());
      // A row outside the cloud-client contract (e.g. a title over 90 characters): a first
      // page falls back whole, as relayhistory's directory backend does; a cursor page
      // cannot restart elsewhere, so it is a 503.
      if (error.reason === "parse" && !single(c, "cursor")) return fail(c, unsupported("directory_row_out_of_contract"));
      const correlationId = c.get("correlationId") as string;
      log({ route: c.req.routePath, reason: `directory_${error.reason}`, status: 503, correlation_id: correlationId });
      return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
    }
    return fail(c, error);
  };

  /** Repeated values, deduplicated and empty values dropped (relayhistory readSessionListFilters). */
  const repeated = (c: Context, name: string) => [...new Set((c.req.queries(name) ?? []).filter(Boolean))];

  /**
   * The list page as canonical rows UNION the caller's buffered sessions (owner-buffered.ts),
   * newest first, canonical first on equal timestamps; a buffered session whose id has a
   * live canonical pointer (any harness) or is on the canonical page is never shown
   * ("canonical wins"; pointers decide nothing else). The cursor carries both positions.
   * Buffered rows come only through readOwnerBufferedList (authority observed before the RPC
   * and again after the RPC and the live-pointer await, immediately before emit); a row whose admission fails the final head advances the buffered position but is
   * never shown. Fenced (including the READ_API_OWNER_BUFFERED_INDEX_VERIFY index check, error_code
   * index_*): the canonical page alone with X-Relayhistory-Buffered: fenced. Any
   * other buffered failure: the canonical page alone, logged `buffered_list_error`. A row of
   * another user is a 500.
   */
  async function bufferedListPage(
    c: Context, identity: VerifiedIdentity, ob: OwnerBufferedDeps,
    canonicalRows: Array<DirectoryListRow & { cursor_ts?: string }>,
    q: { limit: number; sources: string[]; notSources: string[]; bounds: Record<"lastTsAfter" | "lastTsBefore" | "firstTsAfter" | "firstTsBefore", Date | undefined>; metadata: DirectoryListMetadata },
  ) {
    directoryPage(canonicalRows, identity, q.limit); // tenant and shape checks, as the canonical-only page
    const canonical = canonicalRows.map((r) => {
      if (typeof r.cursor_ts !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/u.test(r.cursor_ts)) throw new DirectoryError("parse");
      return r as DirectoryListRow & { cursor_ts: string };
    });
    const incoming = decodeCompositeListCursor(single(c, "cursor"));
    const inCursor = incoming === undefined || typeof incoming === "string" ? {} : incoming;
    const bLimit = Math.min(q.limit + 1, BUFFERED_LIST_MAX_LIMIT);
    let buffered: BufferedSessionRow[] = [];
    let consulted = false;
    let fenced = false;
    const shown = new Set<string>();
    // No buffered position yet: start at the canonical position, ties included.
    const cursor: BufferedListCursor | undefined = inCursor.buffered ??
      (inCursor.canonical ? { ts: cursorTsKey(inCursor.canonical.lastActivityAt), sessionId: "", source: "" } : undefined);
    try {
      // The live-pointer await runs inside the gated read, before its final authority check;
      // from here to the response nothing awaits.
      const rows = await readOwnerBufferedList(ob, bufferedScope(identity), { cursor, limit: bLimit }, (ids) =>
        deps.liveness.livePointerSessions(identity.orgId, identity.workspaceId, ids));
      const onPage = new Set(canonical.map((r) => `${r.harness}\u0000${r.session_id}`));
      for (const r of rows) if (r.visible && !r.live && !onPage.has(`${r.source}\u0000${r.session_id}`)) shown.add(`${r.source}\u0000${r.session_id}`);
      buffered = rows;
      consulted = true;
    } catch (error) {
      if (error instanceof TenantMismatchError) throw error;
      if (error instanceof BufferedFenced) {
        fenced = true;
        log({ route: c.req.routePath, reason: "buffered_fenced", error_code: fenceSubtype(error.reason), ...(error.sqlstateClass ? { sqlstate: error.sqlstateClass } : {}), correlation_id: c.get("correlationId") });
      } else log({ route: c.req.routePath, reason: "buffered_list_error", error_code: (error as { name?: string })?.name ?? "error", correlation_id: c.get("correlationId") });
    }
    const { sources, notSources, bounds } = q;
    const eligible = (r: BufferedSessionRow) => {
      if (!shown.has(`${r.source}\u0000${r.session_id}`)) return false;
      if (sources.length && !sources.includes(r.source)) return false;
      if (notSources.includes(r.source)) return false;
      const first = Date.parse(r.first_ts);
      const last = Date.parse(r.last_ts);
      if (bounds.lastTsAfter && last < bounds.lastTsAfter.getTime()) return false;
      if (bounds.lastTsBefore && last >= bounds.lastTsBefore.getTime()) return false;
      if (bounds.firstTsAfter && first < bounds.firstTsAfter.getTime()) return false;
      if (bounds.firstTsBefore && first >= bounds.firstTsBefore.getTime()) return false;
      return true;
    };
    const merged = mergeListPage({ canonical, buffered, bufferedFull: consulted && buffered.length >= bLimit, eligible, limit: q.limit });
    const canonicalPos: DirectoryCursor | undefined = merged.lastCanonical ?? inCursor.canonical;
    const bufferedPos: BufferedListCursor | undefined = merged.lastBuffered ?? inCursor.buffered;
    const nextCursor = merged.hasMore && (canonicalPos || bufferedPos) ? encodeCompositeListCursor(canonicalPos, bufferedPos) : null;
    const bufferedItems = merged.items.flatMap((i) => (i.kind === "buffered" ? [i.row] : []));
    if (fenced) c.header(BUFFERED_HEADER, "fenced");
    if (bufferedItems.length) c.header(BUFFERED_IDS_HEADER, bufferedIdsHeader(bufferedItems));
    return {
      sessions: merged.items.map((i) => (i.kind === "canonical" ? sessionFromDirectoryRow(i.row, q.metadata) : bufferedSessionSummary(i.row, identity.userId))),
      nextCursor,
    };
  }

  /**
   * READ_API_SESSION_QUERY=true only: the route's `q` as directory terms, or undefined without
   * one (an empty `q=` is no q, as the activity route has always read it). `search` is accepted
   * only as `search=metadata` alongside q (the directory has no other search mode); any other
   * value with q is 501 `param_search`. Without q, the list keeps its 501 `param_search` and
   * the activity route keeps ignoring `search`, as today. Malformed q = 400; a term this backend
   * cannot answer (path:, upload:, is:on-relay/off-relay, an excluded date) = 501 naming it.
   */
  function sessionQueryTerms(c: Context, route: "list" | "activity"): SessionQueryTerms | undefined {
    const raw = single(c, "q");
    const q = raw === "" ? undefined : raw;
    const search = route === "list" || q !== undefined ? single(c, "search") : undefined;
    if (search !== undefined && (search !== "metadata" || q === undefined)) throw unsupported("param_search");
    if (q === undefined) return undefined;
    if (q.length > MAX_Q_CHARS) throw bad(`q must be ${MAX_Q_CHARS} characters or fewer`);
    const parsed = parseSessionQuery(q);
    if (parsed.kind === "bad") throw bad(parsed.reason);
    if (parsed.kind === "unsupported") throw unsupported(parsed.reason);
    return parsed.terms;
  }

  /** The q terms' date window intersected into the lastTs* bounds; an empty intersection is a 400. */
  function intersectQueryDates(terms: SessionQueryTerms, bounds: { lastTsAfter?: Date; lastTsBefore?: Date }) {
    if (terms.lastTsAfter && (!bounds.lastTsAfter || terms.lastTsAfter > bounds.lastTsAfter)) bounds.lastTsAfter = terms.lastTsAfter;
    if (terms.lastTsBefore && (!bounds.lastTsBefore || terms.lastTsBefore < bounds.lastTsBefore)) bounds.lastTsBefore = terms.lastTsBefore;
    if ((terms.lastTsAfter || terms.lastTsBefore) && bounds.lastTsAfter && bounds.lastTsBefore && bounds.lastTsAfter >= bounds.lastTsBefore)
      throw bad("the q date terms do not overlap the requested time window");
  }

  app.get("/v1/sessions", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      // A workspace off the canary allowlist always falls back, whatever it asked.
      const directory = directoryFor(identity);
      const includeAgentName = single(c, "includeAgentName");
      if (includeAgentName !== undefined && includeAgentName !== "recorded") throw bad("includeAgentName must be recorded");
      if (includeAgentName !== undefined && !deps.selfhost) throw unsupported("param_includeAgentName");
      const metadata: DirectoryListMetadata = includeAgentName === "recorded" ? { includeAgentName } : {};
      const queryOn = deps.sessionQuery === true;
      for (const p of LIST_UNSUPPORTED_PARAMS) if (!(queryOn && (p === "q" || p === "search")) && c.req.queries(p)?.length) throw unsupported(`param_${p}`);
      const terms = queryOn ? sessionQueryTerms(c, "list") : undefined;
      const limit = clampLimit(single(c, "limit"), SESSIONS_DEFAULT_LIMIT, SESSIONS_MAX_LIMIT);
      const args: DirectoryListArgs = { limit };
      const sources = repeated(c, "source");
      const notSources = repeated(c, "notSource");
      for (const [name, values] of [["source", sources], ["notSource", notSources]] as const) {
        if (values.length > MAX_LIST_SOURCES) throw bad(`${name} accepts at most ${MAX_LIST_SOURCES} values`);
        if (values.some((v) => v.length > MAX_LIST_ID_LENGTH)) throw bad(`${name} must be ${MAX_LIST_ID_LENGTH} characters or fewer`);
      }
      const users = repeated(c, "user");
      if (users.length > MAX_LIST_USERS) throw bad(`user accepts at most ${MAX_LIST_USERS} values`);
      if (users.some((u) => u.length > MAX_LIST_ID_LENGTH)) throw bad(`user must be ${MAX_LIST_ID_LENGTH} characters or fewer`);
      const agent = single(c, "agent")?.trim();
      if (agent && agent.length > MAX_AGENT_LENGTH) throw bad(`agent must be ${MAX_AGENT_LENGTH} characters or fewer`);
      const projectContains = single(c, "projectContains") || undefined;
      if (projectContains && projectContains.trim().length > 200) throw bad("projectContains must be 200 characters or fewer");
      const bounds: Record<"lastTsAfter" | "lastTsBefore" | "firstTsAfter" | "firstTsBefore", Date | undefined> = {
        lastTsAfter: undefined, lastTsBefore: undefined, firstTsAfter: undefined, firstTsBefore: undefined,
      };
      for (const field of Object.keys(bounds) as Array<keyof typeof bounds>) {
        const t = parseTime(single(c, field), field);
        if (t !== undefined) bounds[field] = new Date(t);
      }
      if (terms) intersectQueryDates(terms, bounds);
      if ((bounds.lastTsAfter && bounds.lastTsBefore && bounds.lastTsAfter > bounds.lastTsBefore) ||
          (bounds.firstTsAfter && bounds.firstTsBefore && bounds.firstTsAfter > bounds.firstTsBefore))
        throw bad("an *After bound must not be later than its *Before bound");
      // What the directory cannot answer exactly stays with the current backend.
      if (limit > DIRECTORY_LIST_MAX_LIMIT) throw unsupported("limit");
      if ([...sources, ...notSources].some((v) => !HARNESS_NAME.test(v))) throw unsupported("source_format");
      if (projectContains !== undefined && projectContains.trim() === "") throw unsupported("project_contains");
      // Buffered rows: only the caller's own; agent/projectContains are not derivable without S3.
      const ob = ownerBufferedFor(identity);
      // Buffered rows have no search document: a q read never drops them silently.
      if (terms && ob !== undefined) throw unsupported("q_with_buffered");
      const withBuffered = ob !== undefined && !agent && !projectContains && (users.length === 0 || users.includes(identity.userId));
      if (sources.length) args.sources = sources;
      if (notSources.length) args.notSources = notSources;
      if (users.length) args.userIds = users;
      if (agent) args.agent = agent;
      if (projectContains) args.projectContains = projectContains;
      for (const [k, v] of Object.entries(bounds)) if (v) args[k as keyof typeof bounds] = v;
      if (terms) {
        // A q page continues only from a `b96q1:` cursor of the same query and filters.
        const fingerprint = queryFingerprint(terms, args);
        const qCursor = decodeQueryCursor(single(c, "cursor"));
        if (qCursor === "foreign" && single(c, "cursor")?.startsWith(LIST_CURSOR_PREFIX)) throw new CursorError("invalid_cursor");
        if (qCursor === "foreign") throw new CursorError("foreign_cursor");
        if (qCursor === "invalid" || (qCursor && qCursor.fingerprint !== fingerprint)) throw new CursorError("invalid_cursor");
        if (qCursor) args.cursor = { lastActivityAt: qCursor.lastActivityAt, sessionId: qCursor.sessionId, harness: qCursor.harness };
        const rows = await directory.query(identity.orgId, identity.workspaceId, { ...args, terms });
        if (rows === null) throw unsupported("directory_not_migrated");
        const page = directoryPage(rows, identity, limit, fingerprint, metadata);
        return c.json({ sessions: page.sessions, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
      }
      // With q on, a `b96q1:` cursor belongs to a q read only.
      if (queryOn && single(c, "cursor")?.startsWith(QUERY_CURSOR_PREFIX)) throw new CursorError("invalid_cursor");
      // The composite cursor exists only on the buffered path; otherwise main's decoder, exactly.
      const composite = withBuffered ? decodeCompositeListCursor(single(c, "cursor")) : undefined;
      const cursor = withBuffered ? (typeof composite === "object" ? composite.canonical : composite) : decodeListCursor(single(c, "cursor"));
      if (cursor === "foreign") throw new CursorError("foreign_cursor");
      if (cursor === "invalid") throw new CursorError("invalid_cursor");
      if (cursor) args.cursor = cursor;
      const rows = await directory.list(identity.orgId, identity.workspaceId, args);
      if (rows === null) throw unsupported("directory_not_migrated");
      if (withBuffered) {
        const union = await bufferedListPage(c, identity, ob, rows, { limit, sources, notSources, bounds, metadata });
        return c.json({ sessions: union.sessions, nextCursor: union.nextCursor, correlationId: c.get("correlationId") });
      }
      const page = directoryPage(rows, identity, limit, undefined, metadata);
      return c.json({ sessions: page.sessions, nextCursor: page.nextCursor, correlationId: c.get("correlationId") });
    } catch (error) {
      return directoryFail(c, error);
    }
  });

  app.get("/v1/sessions/activity", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      const directory = directoryFor(identity);
      const rawSince = single(c, "since");
      const rawUntil = single(c, "until");
      if (!rawSince || !rawUntil) throw bad("since and until are required");
      const sinceMs = parseTime(rawSince, "since")!;
      const untilMs = parseTime(rawUntil, "until")!;
      if (untilMs < sinceMs) throw bad("until must not be earlier than since");
      const since = new Date(sinceMs);
      const until = new Date(untilMs);
      const days = Math.round((Date.UTC(until.getUTCFullYear(), until.getUTCMonth(), until.getUTCDate()) - Date.UTC(since.getUTCFullYear(), since.getUTCMonth(), since.getUTCDate())) / 86_400_000) + 1;
      if (days > MAX_SESSION_ACTIVITY_DAYS) throw bad(`since and until may span at most ${MAX_SESSION_ACTIVITY_DAYS} UTC days`);
      if (deps.sessionQuery === true) {
        const terms = sessionQueryTerms(c, "activity");
        if (terms) {
          const bounds: { lastTsAfter?: Date; lastTsBefore?: Date } = {};
          intersectQueryDates(terms, bounds);
          const rows = await directory.queryActivity(identity.orgId, identity.workspaceId, since, until, { terms, ...bounds });
          if (rows === null) throw unsupported("directory_not_migrated");
          return c.json({ since: since.toISOString(), until: until.toISOString(), days: activityDays(rows, since, until), correlationId: c.get("correlationId") });
        }
      } else {
        const q = single(c, "q");
        if (q && q.length > MAX_Q_CHARS) throw bad(`q must be ${MAX_Q_CHARS} characters or fewer`);
        if (q) throw unsupported("param_q");
      }
      const rows = await directory.activity(identity.orgId, identity.workspaceId, since, until);
      if (rows === null) throw unsupported("directory_not_migrated");
      return c.json({ since: since.toISOString(), until: until.toISOString(), days: activityDays(rows, since, until), correlationId: c.get("correlationId") });
    } catch (error) {
      return directoryFail(c, error);
    }
  });

  /**
   * GET /v1/sessions/:id/timing (relayhistory routes/recall.ts): recorded timing over the
   * whole stored session, from its canonical S3 objects (session-timing-source.ts), never
   * Neon. Same canary allowlist as the directory routes. Every case this backend cannot
   * time exactly is a 501 naming why, and Cloud reads relayhistory instead:
   *   timing_no_pointers       no Supabase pointer row (Neon may still hold the session)
   *   timing_turns_unknown     no legacy turn evidence (READ_API_TIMING_ASSUME_NO_TURNS)
   *   timing_canonical_<r>     the canonical read refused (bounds, digests, generation, deadline)
   *   timing_<r>               an input the port refuses exactly where the SQL would
   * A session whose every pointer is tombstoned is erased: the empty timing, as relayhistory
   * answers once erasure has purged its rows.
   */
  app.get("/v1/sessions/:sessionId/timing", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      requireCanary(identity, "timing");
      const sources = c.req.queries("source") ?? [];
      const source = sources[0]?.trim();
      if (sources.length !== 1 || !source) throw bad("source must be supplied exactly once and be non-empty");
      const timing = deps.timing;
      if (!timing) throw unsupported("timing_not_configured");
      const sessionId = c.req.param("sessionId");
      if (!sessionId || sessionId.length > MAX_ID_CHARS) throw unsupported("timing_session_id");
      const scope: CanonicalIdentity = { orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionId };
      const state = await timing.state(scope);
      if (state.pointers === 0) throw unsupported("timing_no_pointers");
      const controller = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      let snapshot: CanonicalRows;
      if (state.live === 0) snapshot = { rows: [], legacyCoverage: { version: 1, tables: [] } };
      else {
        try {
          snapshot = await Promise.race([
            timing.source(scope, controller.signal),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => {
                // Settle the race first: aborting can reject the read synchronously.
                reject(new CanonicalReadError("deadline"));
                controller.abort();
              }, timing.deadlineMs ?? 3000);
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      }
      const payload = timingFromCanonical(scope, snapshot, { assumeNoTurns: timing.assumeNoTurns });
      log({
        route: c.req.routePath,
        reason: state.live === 0 ? "timing_erased" : "timing_served",
        turns: snapshot.legacyCoverage ? "legacy" : "assumed_none",
        events: payload.coverage.eventCount,
        correlation_id: c.get("correlationId"),
      });
      return c.json({ ...payload, correlationId: c.get("correlationId") });
    } catch (error) {
      if (error instanceof CanonicalReadError) return fail(c, unsupported(`timing_canonical_${error.reason}`, error.details));
      if (error instanceof TimingInputError) return fail(c, unsupported(`timing_${error.reason}`));
      return fail(c, error);
    }
  });

  /**
   * GET /v1/sessions/output (relayhistory routes/recall.ts + lib/session-output.ts): the
   * workspace's agent messages and unique commits in [since, until] (inclusive), from the
   * output facts (output-backend.ts: ClickHouse, or the self-host Postgres facts), never Neon. The 400s are relayhistory's, in its
   * order. Then the canary allowlist, and a 501 for every case not answerable exactly:
   *   output_not_configured      READ_API_OUTPUT_ENABLED is off
   *   output_backfill_incomplete no backfill watermark for the workspace yet
   *   output_before_watermark    `since` is earlier than the watermark's complete_since
   *   output_session_partial     a session whose facts are inexact and whose span overlaps the window
   *   output_session_unindexed   a session with transcript events in the window and no complete
   *                              facts state (the backend's windowReadiness)
   *   output_schema_missing      the output-facts DDL (or one of its columns) is not applied
   */
  app.get("/v1/sessions/output", async (c) => {
    try {
      const identity = c.get("identity");
      if (!c.req.queries("workspace")?.length) throw bad("workspace is required");
      attestWorkspace(c, identity);
      const rawSince = c.req.query("since");
      const rawUntil = c.req.query("until");
      if (!rawSince || !rawUntil || c.req.queries("since")?.length !== 1 || c.req.queries("until")?.length !== 1)
        throw bad("since and until are required exactly once");
      const since = new Date(rawSince);
      const until = new Date(rawUntil);
      const day = 86_400_000;
      if (!Number.isFinite(since.getTime()) || !Number.isFinite(until.getTime()) || until < since ||
          Math.max(0, Math.floor(until.getTime() / day) - Math.floor(since.getTime() / day) + 1) > 366)
        throw bad("Invalid output date range");
      requireCanary(identity, "output");
      // The facts are ClickHouse tables or the self-host Postgres facts (deps.output.backend):
      // a portable search adapter cannot serve Output.
      const backend = deps.output?.backend ?? (deps.output && deps.ch ? clickhouseOutputBackend(deps.ch) : undefined);
      if (!deps.output || !backend) throw unsupported("output_not_configured");
      const opts = { settings: { ...READ_SETTINGS }, timeoutMs: deps.timeoutMs ?? 3_000 };
      let output;
      try {
        const readiness = await backend.windowReadiness(identity.orgId, identity.workspaceId, since, until, opts);
        if (readiness !== "ready") throw unsupported(`output_${readiness}`);
        output = await backend.readOutput({ orgId: identity.orgId, workspaceIds: [identity.workspaceId] }, { since, until }, opts);
      } catch (error) {
        // The fact tables are owner-applied DDL: a missing table or column is a 501, not a 500.
        if (backend.isSchemaMissing(error)) throw unsupported("output_schema_missing");
        throw error;
      }
      log({ route: c.req.routePath, reason: "output_served", correlation_id: c.get("correlationId") });
      return c.json({ since: since.toISOString(), until: until.toISOString(), output, correlationId: c.get("correlationId") });
    } catch (error) {
      return fail(c, error);
    }
  });
  /**
   * Cached brief titles for the session list (feature F2): `workspace=<the token's>` and 1 to
   * 100 repeated `key=<source>:<sessionId>` (the source is a harness name, which has no
   * colon, so the first colon splits). 100 keys of up to 255-character ids can exceed a
   * front-door URL limit (414 before auth); callers keep each URL bounded (Cloud's client
   * splits batches at 8,000 characters). Answers `{ titles: [{ source, sessionId, title }] }`:
   * a requested session with no entry has no title (none generated or backfilled, or erased).
   * It never enqueues generation. A failure is a non-2xx, never an empty 200, so a caller can
   * tell "unavailable" from "no title".
   */
  app.get("/v1/sessions/titles", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      const allowed = deps.briefTitleWorkspaces;
      if (!allowed || (allowed !== "*" && !allowed.has(identity.workspaceId))) throw unsupported("brief_titles_workspace_not_enabled");
      if (!deps.briefTitles) throw unsupported("brief_titles_not_configured");
      const keys = titleKeys(c.req.queries("key") ?? []);
      const rows = await deps.briefTitles.titles(identity.orgId, identity.workspaceId, keys);
      if (rows === null) throw unsupported("brief_titles_not_migrated");
      return c.json({ titles: briefTitleEntries(rows, identity, keys), correlationId: c.get("correlationId") });
    } catch (error) {
      if (error instanceof BriefTitlesError) {
        if (error.reason === "tenant_mismatch") return fail(c, new TenantMismatchError());
        if (error.reason === "invalid_argument") return fail(c, bad("invalid session keys"));
        const correlationId = c.get("correlationId") as string;
        log({ route: c.req.routePath, reason: `brief_titles_${error.reason}`, status: 503, correlation_id: correlationId });
        return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId }, 503);
      }
      return fail(c, error);
    }
  });

  // 044100 (brief summary): the real /v1/sessions/output route is registered above (this branch);
  // main's notMigrated output placeholder is dropped to avoid a duplicate registration.
  app.get("/v1/sessions/:sessionId/summary", async (c) => {
    try {
      const identity=c.get("identity");
      attestWorkspace(c,identity); requireCanary(identity,"summary"); rejectUnsupported(c);
      if(!deps.briefSummary)throw unsupported("summary_not_configured");
      for(const name of Object.keys(c.req.query()))if(!["workspace","source","kind"].includes(name))throw unsupported(`param_${name}`);
      const kind=single(c,"kind")??"short";
      if(kind!=="short")throw unsupported("summary_kind");
      const source=single(c,"source"),sessionId=c.req.param("sessionId");
      if(!source||!HARNESS_NAME.test(source)||!sessionId||sessionId.length>MAX_ID_CHARS)throw bad("invalid session key");
      const rows=await deps.briefSummary.read(identity.orgId,identity.workspaceId,source,sessionId);
      if(rows===null)throw unsupported("summary_not_migrated");
      return c.json({summary:briefSummaryResponse(rows,{...identity,source,sessionId})});
    } catch(error) {
      if(error instanceof BriefSummaryError) {
        if(error.reason==="tenant_mismatch")return fail(c,new TenantMismatchError());
        if(error.reason==="invalid_argument")return fail(c,bad("invalid session key"));
        if(error.reason==="unqualified")return fail(c,unsupported("summary_unqualified"));
        return c.json({error:{code:"backend_unavailable",message:"read backend unavailable"},correlationId:c.get("correlationId")},503);
      }
      return fail(c,error);
    }
  });

  /**
   * GET /v1/sessions/:id/turns and /metadata (relayhistory routes/turns.ts): the relay
   * session journal from canonical S3 (session-turns-source.ts), never Neon. Same canary
   * allowlist. `source` is optional, as relayhistory's (its first value; empty = none).
   * Every case not answerable exactly is a 501 Cloud falls back on:
   *   turns_no_pointers           no Supabase pointer (Neon may still hold the turns)
   *   turns_harness_bound         more harness scopes than one read may open
   *   turns_unknown               a scope without the legacy export's coverage marker
   *   turns_truncated             an exported string the export had to cut
   *   turns_attribution           a live untagged turn whose source needs Neon's events
   *   turns_canonical_<r>         the canonical read refused (bounds, digests, deadline)
   * Erased (every pointer tombstoned): no turns, and metadata's 404, as relayhistory
   * answers after erasure purged the rows (0074).
   */
  const sessionTurns = async (c: Context, identity: VerifiedIdentity) => {
    attestWorkspace(c, identity);
    requireCanary(identity, "turns");
    const turns = deps.turns;
    if (!turns) throw unsupported("turns_not_configured");
    const sessionId = c.req.param("sessionId");
    if (!sessionId || sessionId.length > MAX_ID_CHARS) throw unsupported("turns_session_id");
    const source = c.req.query("source") || undefined;
    if (source !== undefined && source.length > MAX_ID_CHARS) throw unsupported("turns_source");
    const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, sessionId };
    const states = await turns.harnesses(scope);
    if (states.length > MAX_TURN_HARNESSES) throw unsupported("turns_harness_bound");
    const selected = source === undefined ? states : states.filter((h) => h.harness === source);
    if (!selected.some((h) => h.pointers > 0)) {
      // Owner-only buffered (flag on): the caller's own kept buffered stages, `source` required.
      const ob = ownerBufferedFor(identity);
      if (ob && source !== undefined) {
        const served = await bufferedTurns(c, identity, ob, sessionId, source);
        // Stages that project to no turn: the existing answer (Cloud falls back).
        if (served && (served.erased || served.rows.length > 0)) return { ...served, buffered: true };
      }
      throw unsupported("turns_no_pointers");
    }
    const live = selected.filter((h) => h.live > 0);
    if (live.length === 0) return { rows: [] as Record<string, unknown>[], erased: true, buffered: false };
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const snapshots = await Promise.race([
        Promise.all(live.map(async (h) => ({ harness: h.harness, snapshot: await turns.source({ ...scope, source: h.harness }, controller.signal) }))),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(new CanonicalReadError("deadline"));
            controller.abort();
          }, turns.deadlineMs ?? 3000);
        }),
      ]);
      return { rows: turnRows(scope, snapshots, source, { assumeNoLegacy: turns.assumeNoLegacy === true }), erased: false, buffered: false };
    } finally {
      clearTimeout(timer);
    }
  };
  const turnsFail = (c: Context, error: unknown) => {
    if (error instanceof CanonicalReadError) return fail(c, unsupported(`turns_canonical_${error.reason}`, error.details));
    if (error instanceof TurnsInputError) return fail(c, unsupported(error.reason.startsWith("turns_") ? error.reason : `turns_${error.reason}`));
    // A malformed canonical row (canonical-legacy.ts / canonical-turns.ts) is not answerable exactly.
    if (error instanceof Error && /^canonical (legacy evidence|live turn) invalid$/u.test(error.message)) return fail(c, unsupported("turns_canonical_invalid"));
    return fail(c, error);
  };

  app.get("/v1/sessions/:sessionId/turns", async (c) => {
    try {
      const identity = c.get("identity");
      const { rows, erased, buffered } = await sessionTurns(c, identity);
      if (buffered) c.header(BUFFERED_HEADER, "1");
      const body = { sessionId: c.req.param("sessionId"), turns: turnsResponse(rows), correlationId: c.get("correlationId") };
      log({ route: c.req.routePath, reason: erased ? "turns_erased" : "turns_served", turns: body.turns.length, correlation_id: c.get("correlationId") });
      return c.json(body);
    } catch (error) {
      return turnsFail(c, error);
    }
  });

  app.get("/v1/sessions/:sessionId/metadata", async (c) => {
    try {
      const identity = c.get("identity");
      const sessionId = c.req.param("sessionId");
      const { rows, erased, buffered } = await sessionTurns(c, identity);
      if (buffered) c.header(BUFFERED_HEADER, "1");
      const metadata = metadataResponse(rows);
      log({ route: c.req.routePath, reason: erased ? "metadata_erased" : metadata ? "metadata_served" : "metadata_none", correlation_id: c.get("correlationId") });
      if (!metadata)
        return c.json({ error: { code: "not_found", message: `No conversation turns found for session ${sessionId}` }, correlationId: c.get("correlationId") }, 404);
      return c.json(metadata);
    } catch (error) {
      return turnsFail(c, error);
    }
  });
  /**
   * GET /v1/digest/daily (relayhistory routes/digest.ts): one civil day's rollup from the
   * output facts (digest-backend.ts: ClickHouse, or the self-host Postgres facts) and the directory, plus the cached model narrative, never Neon. The 400s
   * are resolveDayWindow's (ported verbatim). A workspace off the allowlist, or a backend not
   * ready, is a 501 naming why:
   *   digest_not_configured       READ_API_DIGEST_ENABLED is off
   *   digest_backfill_incomplete  no output-facts watermark for the workspace yet
   *   digest_before_watermark     the day starts before the watermark's complete_since
   *   digest_session_partial      an inexact session whose facts span overlaps the day
   *   digest_session_unindexed    a session with transcript events in the day and no complete facts state
   *   digest_schema_missing       the output-facts DDL (or a v4 column) is not applied
   *   digest_directory_unavailable the day has sessions and the directory cannot list them
   * Once served, every narrative outcome is a 200 (`narrative.unavailable`), including a
   * miss the Worker could not fill within the deadline. `refresh=true` regenerates.
   * stats.outcomes.reverted is omitted (0, declared in X-Sessions-Digest-Omitted).
   */
  app.get("/v1/digest/daily", async (c) => {
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      let window;
      try {
        window = resolveDayWindow(c.req.query("date"), c.req.query("tz"));
      } catch (error) {
        if (error instanceof DigestInputError) throw bad(error.message);
        throw error;
      }
      const projectId = c.req.query("project")?.trim() || null;
      const refresh = c.req.query("refresh") === "true";
      requireCanary(identity, "digest");
      // The digest reads the ClickHouse facts or the self-host Postgres facts
      // (deps.digest.backend): a portable search adapter cannot serve it.
      const backend = deps.digest?.backend ?? (deps.digest && deps.ch ? clickhouseDigestBackend(deps.ch) : undefined);
      if (!deps.digest || !backend) throw unsupported("digest_not_configured");
      const opts = { settings: { ...READ_SETTINGS }, timeoutMs: deps.timeoutMs ?? 3_000 };
      let digest;
      try {
        // Exactly the stats window, half-open [since, until), so no event the stats count escapes
        // the readiness check (an event at until - 0.5 ms is in both).
        const readiness = await backend.windowReadiness(identity.orgId, identity.workspaceId, window.since, window.until, { ...opts, untilExclusive: true });
        if (readiness !== "ready") throw unsupported(`digest_${readiness}`);
        digest = await backend.buildDailyDigest(deps.directory, { orgId: identity.orgId, workspaceId: identity.workspaceId, since: window.since, until: window.until, projectId }, opts);
      } catch (error) {
        if (backend.isSchemaMissing(error)) throw unsupported("digest_schema_missing");
        throw error;
      }
      const statsHash = await computeStatsHash(digest, identity.workspaceId);
      const narrative = await resolveDigestNarrative(deps.digest, {
        orgId: identity.orgId, workspaceId: identity.workspaceId, day: window.date, tz: window.tz, projectId, statsHash, refresh,
      }, digest);
      c.header("X-Sessions-Digest-Omitted", DIGEST_OMITTED_FIELDS.join(","));
      log({ route: c.req.routePath, reason: "digest_served", narrative: narrative.unavailable ?? (narrative.cached ? "cached" : "generated"), correlation_id: c.get("correlationId") });
      return c.json({
        date: window.date,
        tz: window.tz,
        window: { since: window.since.toISOString(), until: window.until.toISOString() },
        scope: { projectId },
        stats: digest.stats,
        projects: digest.projects,
        epics: digest.epics,
        sessions: digest.sessions,
        narrative,
        correlationId: c.get("correlationId"),
      });
    } catch (error) {
      if (error instanceof DigestDirectoryUnavailable) return fail(c, unsupported("digest_directory_unavailable"));
      if (error instanceof DirectoryError) return directoryFail(c, error);
      return fail(c, error);
    }
  });

  /**
   * Deterministic coordination findings from the same facts and readiness authority as the
   * digest. Incomplete facts are an honest 200 with partial/unknown coverage, never findings
   * presented as exhaustive. No transcript body is read or returned.
   */
  app.get("/v1/coordination/overlaps", async (c) => {
    if (deps.coordination?.enabled !== true)
      return c.json({ error: "coordination_overlaps_disabled" }, 404);
    try {
      const identity = c.get("identity");
      attestWorkspace(c, identity);
      requireCanary(identity, "coordination");
      const now = Date.now();
      const untilRaw = single(c, "until");
      const sinceRaw = single(c, "since");
      const untilMs = untilRaw === undefined ? now : parseTime(untilRaw, "until")!;
      const sinceMs = sinceRaw === undefined ? untilMs - 7 * 24 * 60 * 60 * 1_000 : parseTime(sinceRaw, "since")!;
      if (sinceMs >= untilMs) throw bad("since must be before until");
      if (untilMs - sinceMs > 31 * 24 * 60 * 60 * 1_000) throw bad("window must not exceed 31 days");
      const limitRaw = single(c, "limit");
      const limit = limitRaw === undefined ? 50 : Number(limitRaw);
      if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw bad("limit must be an integer from 1 to 200");
      const projectId = single(c, "project")?.trim() || null;
      const since = new Date(sinceMs), until = new Date(untilMs);
      const backend = deps.coordination.backend ?? (deps.ch ? clickhouseCoordinationBackend(deps.ch, deps.liveness) : undefined);
      if (!backend) throw new HttpError(503, "backend_unavailable", "read backend unavailable");
      const opts = { settings: { ...READ_SETTINGS }, timeoutMs: deps.timeoutMs ?? 3_000 };
      let readiness: Awaited<ReturnType<CoordinationBackend["windowReadiness"]>> = "ready";
      let schemaMissing = false;
      try {
        readiness = await backend.windowReadiness(identity.orgId, identity.workspaceId, since, until, { ...opts, untilExclusive: true });
      } catch (error) {
        if (backend.isSchemaMissing(error)) schemaMissing = true;
        else throw error;
      }
      let facts = { lanes: [], sessionsTruncated: 0, notes: [] } as Awaited<ReturnType<CoordinationBackend["readFacts"]>>;
      if (!schemaMissing) {
        try {
          facts = await backend.readFacts(deps.directory, {
            orgId: identity.orgId, workspaceId: identity.workspaceId, since, until, projectId,
          }, opts);
        } catch (error) {
          if (backend.isSchemaMissing(error)) schemaMissing = true;
          else throw error;
        }
      }
      const notes = [
        ...(schemaMissing ? ["coordination facts schema is unavailable"] : []),
        ...(readiness === "ready" ? [] : [`facts readiness: ${readiness}`]),
        ...facts.notes,
      ];
      const status = schemaMissing ? "unknown" as const
        : readiness !== "ready" || facts.sessionsTruncated > 0 || facts.notes.some((n) => n.includes("directory unavailable")) ? "partial" as const
        : "complete" as const;
      const detected = detectCoordination({
        workspaceId: identity.workspaceId,
        window: { since: since.toISOString(), until: until.toISOString() },
        lanes: facts.lanes,
      }, { limit });
      log({ route: c.req.routePath, reason: "coordination_overlaps_served", coverage: status, findings: detected.findings.length, correlation_id: c.get("correlationId") });
      return c.json({
        contract: "coordination-overlaps/1",
        window: { since: since.toISOString(), until: until.toISOString() },
        generatedAt: new Date().toISOString(),
        detector: detected.detector,
        coverage: {
          status,
          sessionsConsidered: facts.lanes.length,
          sessionsTruncated: facts.sessionsTruncated,
          notes: [...new Set(notes)],
        },
        summary: detected.summary,
        findings: detected.findings,
      });
    } catch (error) {
      if (error instanceof DirectoryError) return directoryFail(c, error);
      return fail(c, error);
    }
  });

  // Babysitter evidence (Neon GET /v1/delivery/records parity for session_lineage /
  // turn_receipt; babysitter-evidence.ts). Tenant from the verified identity, as Neon's
  // auth context; default off (501 until the worker wires the deps).
  app.get("/v1/delivery/records", async (c) => {
    const correlationId = c.get("correlationId");
    try {
      const evidence = deps.babysitter;
      if (!evidence) throw unsupported("delivery_records_not_configured");
      const identity = c.get("identity");
      const query: Record<string, string> = {};
      for (const key of new Set(new URL(c.req.url).searchParams.keys())) {
        const all = c.req.queries(key) ?? [];
        if (all.length > 1) return c.json({ error: { code: "invalid_delivery", message: "Repeated delivery filters are unsupported" }, correlationId }, 400);
        query[key] = all[0]!;
      }
      // Neon order: repeated filters (400) above, then the account assertion (403) before any read.
      return c.json(await readBabysitterEvidence(evidence, { orgId: identity.orgId, workspaceId: identity.workspaceId }, query, c.req.header("X-RelayHistory-Expected-Account")));
    } catch (error) {
      if (error instanceof EvidenceRequestError)
        return c.json({ error: { code: error.code, message: error.message, ...(error.reason ? { reason: error.reason } : {}) }, correlationId }, error.status);
      return fail(c, error);
    }
  });

  /**
   * GET /v1/sessions/:id/catalog (self-host only): `workspace=` and `source=` (exactly once)
   * as the timing route, same canary allowlist, same canonical S3 read (selfhost.ts
   * catalogFromCanonical). `catalog` is the session's latest delivered catalog record,
   * `children` its relationship edges, `commitLinks` its delivered commit links; `parents` is
   * always empty (a parent edge lives with the parent session's objects). A session with no
   * pointer is 404 `not_found` (reason `catalog_no_pointers`), the answer relaycontrol maps to
   * an empty catalog; erased is the empty catalog. The 404 is in-workspace only: it is
   * reachable strictly after attestWorkspace and requireCanary pass and the canonical source
   * is wired, so a caller outside the workspace still gets its 401/403/501 and never learns
   * whether a session id exists.
   */
  if (selfhost) {
    app.get("/v1/sessions/:sessionId/catalog", async (c) => {
      try {
        const identity = c.get("identity");
        attestWorkspace(c, identity);
        requireCanary(identity, "catalog");
        const sources = c.req.queries("source") ?? [];
        const source = sources[0]?.trim();
        if (sources.length !== 1 || !source) throw bad("source must be supplied exactly once and be non-empty");
        if (source.length > MAX_ID_CHARS) throw bad("source is not valid");
        const catalog = selfhost.catalog;
        if (!catalog) throw unsupported("catalog_not_configured");
        const sessionId = c.req.param("sessionId");
        if (!sessionId || sessionId.length > MAX_ID_CHARS) throw unsupported("catalog_session_id");
        const scope: CanonicalIdentity = { orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionId };
        const state = await catalog.state(scope);
        if (state.pointers === 0) {
          log({ route: c.req.routePath, reason: "catalog_no_pointers", status: 404, correlation_id: c.get("correlationId") });
          throw new HttpError(404, "not_found", `No catalog found for session ${sessionId}`, "catalog_no_pointers");
        }
        let payload = EMPTY_CATALOG;
        if (state.live > 0) {
          const controller = new AbortController();
          let timer: ReturnType<typeof setTimeout> | undefined;
          try {
            const snapshot = await Promise.race([
              catalog.source(scope, controller.signal),
              new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                  reject(new CanonicalReadError("deadline"));
                  controller.abort();
                }, catalog.deadlineMs ?? 3000);
              }),
            ]);
            payload = catalogFromCanonical(scope, snapshot);
          } finally {
            clearTimeout(timer);
          }
        }
        log({
          route: c.req.routePath,
          reason: state.live === 0 ? "catalog_erased" : "catalog_served",
          children: payload.children.length,
          commit_links: payload.commitLinks.length,
          correlation_id: c.get("correlationId"),
        });
        return c.json({ workspaceId: identity.workspaceId, ...payload });
      } catch (error) {
        if (error instanceof CanonicalReadError) return fail(c, unsupported(`catalog_canonical_${error.reason}`, error.details));
        if (error instanceof CatalogInputError) return fail(c, unsupported(`catalog_${error.reason}`));
        return fail(c, error);
      }
    });
  }

  /**
   * GET / POST /v1/sessions/:id/analysis (lane F1): relayhistory's session analysis
   * contract from migration 20261105046000 and the analysis result objects, never Neon.
   * Same canary allowlist. POST additionally needs the `rth:sync` scope (relayhistory's) and the separate
   * admission principal (deps.analysis.admit); without it POST is 501
   * `analysis_admission_not_configured`. A missing or altered result object is a 503.
   */
  const sourceMismatch = () => new HttpError(400, "source_mismatch", "Session source does not match or is ambiguous");
  /**
   * relayhistory routes/session-analysis.ts selectedSource: a session id held by more than
   * one live harness, or a `source` that is not its harness, is 400 source_mismatch.
   */
  const analysisSession = async (c: Context, identity: VerifiedIdentity) => {
    attestWorkspace(c, identity);
    requireCanary(identity, "analysis");
    const analysis = deps.analysis;
    if (!analysis) throw unsupported("analysis_not_configured");
    const sessionId = c.req.param("sessionId");
    if (!sessionId || sessionId.length > MAX_ID_CHARS) throw bad("sessionId is not valid");
    const source = single(c, "source");
    if (source !== undefined && (source.length > 128 || /[\x00-\x1f\x7f]/u.test(source))) throw sourceMismatch();
    const harnesses = [...new Set(await analysis.harnesses(identity, sessionId))];
    if (harnesses.length > 1 || (source && harnesses.length === 1 && harnesses[0] !== source)) throw sourceMismatch();
    return { analysis, sessionId };
  };
  const notFound = (c: Context) =>
    c.json({ error: { code: "not_found", message: "Session not found" }, correlationId: c.get("correlationId") }, 404);

  app.get("/v1/sessions/:sessionId/analysis", async (c) => {
    try {
      const identity = c.get("identity");
      const { analysis, sessionId } = await analysisSession(c, identity);
      const rpc = await analysis.read(identity, sessionId);
      if (rpc.outcome !== "ok" || rpc.sessionId !== sessionId) return notFound(c);
      const body = await assembleAnalyses(rpc, { result: (ref) => analysis.result(ref) });
      return c.json({ ...body, correlationId: c.get("correlationId") });
    } catch (error) {
      if (error instanceof AnalysisReadError || (error as { name?: string })?.name === "AnalysisResultError") {
        log({ route: c.req.routePath, reason: (error as Error).message, status: 503, correlation_id: c.get("correlationId") });
        return c.json({ error: { code: "backend_unavailable", message: "read backend unavailable" }, correlationId: c.get("correlationId") }, 503);
      }
      return fail(c, error);
    }
  });

  app.post("/v1/sessions/:sessionId/analysis", async (c) => {
    try {
      const identity = c.get("identity");
      const { analysis, sessionId } = await analysisSession(c, identity);
      // relayhistory routes/session-analysis.ts:174-177: POST needs rth:read and rth:sync.
      if (!identity.scopes.includes("rth:sync"))
        return c.json({ error: { code: "forbidden", message: "missing required scope: rth:sync" }, correlationId: c.get("correlationId") }, 403);
      if (!analysis.admit) throw unsupported("analysis_admission_not_configured");
      const out = admitResponse(await analysis.admit(identity, sessionId));
      if (out.retryAfter) c.header("Retry-After", out.retryAfter);
      return c.json({ ...out.body, correlationId: c.get("correlationId") }, out.status);
    } catch (error) {
      return fail(c, error);
    }
  });

  app.notFound((c) => c.json({ error: { code: "unsupported_by_backend", message: "route not served by this backend", reason: "route" }, correlationId: c.get("correlationId") }, 501));

  return app;
}
