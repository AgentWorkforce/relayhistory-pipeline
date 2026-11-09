/**
 * Owner-only read of buffered sessions (Option C), behind RELAYHISTORY_READ_OWNER_BUFFERED
 * (default off; only "1" or "true" enables it). With the flag off nothing here runs (app.ts).
 *
 * A buffered session is one the caller uploaded under buffered acceptance that is not yet
 * published canonically: its bytes are zstd stage objects under buffered-batches/ (staging.ts)
 * and its ledger rows sit in sessions.ingest_buffer_outbox. Migration 20261105043700 (PR A)
 * adds SECURITY DEFINER readers, EXECUTE to bulls96_read_api only, that apply the buffered
 * GC's keep predicate and the frozen deletion/reset hides at read time and bind the user from
 * `bulls96_read.user_id`, which this module sets from the verified identity in the same
 * READ ONLY transaction. They are called with named arguments, so parameter order is free:
 *
 *   sessions.buffered_sessions_for(p_workspace, p_cursor_ts, p_cursor_session, p_cursor_source,
 *     p_limit, p_cutover_epoch, p_head_epoch, p_head_version, p_head_digest, p_head_kind,
 *     p_head_pending, p_privacy_baseline_sha256)
 *   sessions.buffered_session_stages_for(p_workspace, p_source, p_session_id,
 *     p_cutover_epoch, p_head_epoch, p_head_version, p_head_digest, p_head_kind,
 *     p_head_pending, p_privacy_baseline_sha256)
 *
 * Both readers pass the observed privacy baseline and head kind on to PR A's
 * sessions.owner_buffered_read_scope, which gates on the caller's per-user G2 parity row
 * (sessions.owner_buffered_parity, rule 4: mandatory). PR A b87ad189 does not have
 * p_privacy_baseline_sha256 / p_head_kind on the three functions, nor group_ordinals in the
 * stage reader's result; PR A must add them before any enablement. The SQL-side hides (frozen
 * deletions, upload_deletions, upload_resets) live in PR A.
 *
 * Every reader error is fenced, in one place (rethrowBuffered): SQLSTATE 55006 'buffered read
 * fenced', 57014 (statement_timeout) and any other RPC or connection error. Never a 500.
 *
 * Authority (R2 control-head contract v5, wire frozen at relayhistory-cloud #328 a06a58f9 by
 * lead 245002086845415424, after wire v5.1 (lead 245001538553413632)):
 * the head is never taken from the client. It is observed through the Cloudflare service
 * binding HISTORY_CONTROL_HEAD (named entrypoint SessionReadPrivacyControls) with
 * GET /v1/session-read/control-head for exactly the verified (org, workspace, user), carried
 * in x-relayhistory-* headers, with a 2 s deadline and no caching. Every buffered response
 * observes it twice: before the RPCs (their head parameters) and again after every S3 read and
 * RPC recheck, before any byte is emitted. Any failure, any difference between the two
 * observations (head, controlEpoch, privacy baseline), a pending head or a non-zero head whose
 * sourceControlEpoch is not controlEpoch is fenced. Each row's or stage's immutable admission
 * (epoch, version) is then compared with the final head (BigInt on decimal strings); NULL or
 * absent admission is hidden. The only exported read paths (readOwnerBufferedList,
 * readOwnerBufferedTurns, readOwnerBufferedEvents) run that gate; the raw stage read is
 * module-private.
 *
 * Buffered turn text is scrubbed at read time (scrubBufferedText). Buffered events
 * (readOwnerBufferedEvents) are served only on GET /v1/sessions/:sessionId/events with `source`,
 * and only with READ_API_OWNER_BUFFERED_EVENTS=on as well (default off; ownerBufferedEventsEnabled):
 * the same gated read, projected by canonical-projection.ts projectEventRows (the one projection
 * both readers run), scrubbed and tenant-checked per row. GET /v1/events stays canonical-only.
 *
 * Out-of-band index (READ_API_OWNER_BUFFERED_INDEX_VERIFY, default off; ownerBufferedIndexVerifyEnabled):
 * the readers depend on the partial index ingest_buffer_outbox_user_buffered_idx on
 * sessions.ingest_buffer_outbox, which migration 20261105043700 leaves to an operator's CREATE
 * INDEX CONCURRENTLY. With the flag on, every exported read path first proves it exists and is
 * valid (BufferedIndexVerifier: a catalog probe over the same read connection and statement
 * timeout, a valid answer cached for the isolate's lifetime, a failure re-probed after 60 s,
 * concurrent requests sharing one probe). Not proven: fenced before any candidate as
 * index_missing, index_invalid or index_unverified, so detail routes answer as without the flag
 * and the list serves the canonical page with X-Relayhistory-Buffered: fenced. Off: no probe.
 *
 * Erasure, reset and fence gating come only from the RPCs, the recheck after the S3 fetch and
 * the authority gate, never from canonical liveness. Canonical liveness (pointers) is used only
 * to decide that a canonical session wins over a buffered one of the same id.
 */
import type { TenantRunner } from "./liveness.ts";
import { TenantMismatchError } from "./page.ts";
import { canonicalLiveTurns } from "./canonical-turns.ts";
import { projectEventRows } from "./canonical-projection.ts";
import type { EventRow } from "./queries.ts";
import { liveMachineId } from "../session-ingest/writer.ts";
import { CONVERGENCE_EVENT_KIND, deliveryMachine } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { turnRows } from "./session-turns-source.ts";
import { BUFFER_STAGE_MAX_BYTES, readBufferedStage } from "../session-ingest/staging.ts";
import { bufferedOrdinalSelection } from "../session-ingest/buffered-protocol.ts";
import { isObjectStoreError, type ObjectStore } from "../shared/vendor.ts";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { isSecretKeyName, scrubJson, scrubText } from "../../packages/pipeline/src/lib/scrub.ts";

export const OWNER_BUFFERED_FLAG = "RELAYHISTORY_READ_OWNER_BUFFERED";
export const BUFFERED_HEADER = "X-Relayhistory-Buffered";
export const BUFFERED_IDS_HEADER = "X-Relayhistory-Buffered-Ids";

/** RELAYHISTORY_READ_OWNER_BUFFERED: only "1" or "true" (trimmed) enables; anything else is off. */
export function ownerBufferedEnabled(raw: string | undefined): boolean {
  const value = raw?.trim();
  return value === "1" || value === "true";
}

export const OWNER_BUFFERED_EVENTS_FLAG = "READ_API_OWNER_BUFFERED_EVENTS";

/** READ_API_OWNER_BUFFERED_EVENTS holds a value other than unset, empty or "on". Names the field, never the value. */
export class OwnerBufferedEventsFlagError extends Error {
  constructor() {
    super(`${OWNER_BUFFERED_EVENTS_FLAG} must be unset or "on"`);
    this.name = "OwnerBufferedEventsFlagError";
  }
}

/**
 * READ_API_OWNER_BUFFERED_EVENTS: unset or empty is off, exactly "on" enables, anything else
 * throws OwnerBufferedEventsFlagError (the Worker refuses the deploy's requests, as it refuses
 * owner-buffered on without its binding). On has no effect unless owner-buffered reads are on
 * and the workspace is on both allowlists (app.ts ownerBufferedFor).
 */
export function ownerBufferedEventsEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw === "") return false;
  if (raw === "on") return true;
  throw new OwnerBufferedEventsFlagError();
}

export const OWNER_BUFFERED_STAGE_CONCURRENCY_FLAG = "READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY";
/** The largest READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY accepted (S3 GetObjects in flight per read). */
export const OWNER_BUFFERED_STAGE_CONCURRENCY_MAX = 32;

/** READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY holds an unaccepted value. Names the field, never the value. */
export class OwnerBufferedStageConcurrencyError extends Error {
  constructor() {
    super(`${OWNER_BUFFERED_STAGE_CONCURRENCY_FLAG} must be unset, "0", "1" or an integer 2..${OWNER_BUFFERED_STAGE_CONCURRENCY_MAX}`);
    this.name = "OwnerBufferedStageConcurrencyError";
  }
}

/**
 * READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY: unset, empty, "0" or "1" is 1 (the serial stage
 * fetch, unchanged); an integer "2".."32" (digits only, no sign, no leading zero, no spaces) is
 * the most S3 stage GetObjects one buffered detail read keeps in flight; anything else throws
 * OwnerBufferedStageConcurrencyError (the Worker refuses the deploy's requests, as it does for
 * READ_API_OWNER_BUFFERED_EVENTS). The authority checks keep their order and placement either way.
 */
export function ownerBufferedStageConcurrency(raw: string | undefined): number {
  if (raw === undefined || raw === "" || raw === "0" || raw === "1") return 1;
  if (/^[1-9][0-9]?$/.test(raw)) {
    const n = Number(raw);
    if (n >= 2 && n <= OWNER_BUFFERED_STAGE_CONCURRENCY_MAX) return n;
  }
  throw new OwnerBufferedStageConcurrencyError();
}

export const OWNER_BUFFERED_PHASE_LOG_FLAG = "READ_API_OWNER_BUFFERED_PHASE_LOG";

/** READ_API_OWNER_BUFFERED_PHASE_LOG: exactly "1" enables the per-phase log fields; anything else is off. */
export function ownerBufferedPhaseLogEnabled(raw: string | undefined): boolean {
  return raw === "1";
}

export const OWNER_BUFFERED_INDEX_VERIFY_FLAG = "READ_API_OWNER_BUFFERED_INDEX_VERIFY";

/** READ_API_OWNER_BUFFERED_INDEX_VERIFY holds a value other than unset, empty or "on". Names the field, never the value. */
export class OwnerBufferedIndexVerifyFlagError extends Error {
  constructor() {
    super(`${OWNER_BUFFERED_INDEX_VERIFY_FLAG} must be unset or "on"`);
    this.name = "OwnerBufferedIndexVerifyFlagError";
  }
}

/**
 * READ_API_OWNER_BUFFERED_INDEX_VERIFY: unset or empty is off, exactly "on" enables, anything else
 * throws OwnerBufferedIndexVerifyFlagError (the Worker refuses the deploy's requests, as it does
 * for READ_API_OWNER_BUFFERED_EVENTS). On, every owner-buffered read first proves the out-of-band
 * index BUFFERED_INDEX_NAME exists and is valid (BufferedIndexVerifier); no effect unless
 * owner-buffered reads are on and the workspace is on both allowlists.
 */
export function ownerBufferedIndexVerifyEnabled(raw: string | undefined): boolean {
  if (raw === undefined || raw === "") return false;
  if (raw === "on") return true;
  throw new OwnerBufferedIndexVerifyFlagError();
}

/** The owner-buffered events read's phases, in order; each logs as `<phase>_ms`. */
export const OWNER_BUFFERED_PHASES = ["session_start", "control_head_1", "stages_sql", "s3_fetch", "recheck", "control_head_2", "fold"] as const;
export type OwnerBufferedPhase = (typeof OWNER_BUFFERED_PHASES)[number];

/**
 * READ_API_OWNER_BUFFERED_PHASE_LOG=1: per-phase wall time of one owner-buffered events read,
 * added to the request's existing log line. Holds durations and a stage count only (never
 * content, keys or ids). A phase that throws keeps its elapsed time and is named by
 * `aborted_phase`; a phase never reached has no field. `fold` accumulates the CPU-only work
 * (stage record parse after the recheck, cross-stage fold, projection and scrub): workerd does
 * not advance timers during pure CPU work, so in the Worker it may read 0.
 */
export class OwnerBufferedPhaseTimer {
  private readonly elapsed = new Map<OwnerBufferedPhase, number>();
  private current: OwnerBufferedPhase | undefined;
  private stageCount: number | undefined;
  /** Set once the fields went out on an abort line; the request line then omits them. */
  emitted = false;
  constructor(private readonly now: () => number = () => performance.now()) {}

  async time<T>(phase: OwnerBufferedPhase, fn: () => Promise<T>): Promise<T> {
    this.current = phase;
    const started = this.now();
    try {
      const out = await fn();
      this.current = undefined;
      return out;
    } finally {
      this.add(phase, started);
    }
  }

  timeSync<T>(phase: OwnerBufferedPhase, fn: () => T): T {
    this.current = phase;
    const started = this.now();
    try {
      const out = fn();
      this.current = undefined;
      return out;
    } finally {
      this.add(phase, started);
    }
  }

  /** The number of stages the S3 phase fetches (the first stages() answer). */
  stages(count: number): void {
    this.stageCount = count;
  }

  /** The phase in progress or the one that threw; undefined when none is. */
  get inProgress(): OwnerBufferedPhase | undefined {
    return this.current;
  }

  /** `<phase>_ms` (rounded) for every phase reached, `s3_stage_count`, and `aborted_phase` when a phase did not complete. */
  fields(aborted?: string): Record<string, number | string> {
    const out: Record<string, number | string> = {};
    for (const phase of OWNER_BUFFERED_PHASES) {
      const ms = this.elapsed.get(phase);
      if (ms !== undefined) out[`${phase}_ms`] = Math.round(ms);
    }
    if (this.stageCount !== undefined) out.s3_stage_count = this.stageCount;
    const phase = this.current ?? aborted;
    if (phase !== undefined) out.aborted_phase = phase;
    return out;
  }

  private add(phase: OwnerBufferedPhase, started: number): void {
    this.elapsed.set(phase, (this.elapsed.get(phase) ?? 0) + Math.max(0, this.now() - started));
  }
}

/** Optional per-request context of one gated buffered read (the events route passes it; turns do not). */
export interface OwnerBufferedReadOptions {
  /** The request's signal: with stage concurrency, an abort stops new GetObjects and fails the read. */
  signal?: AbortSignal;
  /** READ_API_OWNER_BUFFERED_PHASE_LOG=1: records each phase's time. */
  phases?: OwnerBufferedPhaseTimer;
}

/** Module-private: the events read adds its stage concurrency; the turns read never does (serial). */
type GatedReadOptions = OwnerBufferedReadOptions & { stageConcurrency?: number };

function timed<T>(phases: OwnerBufferedPhaseTimer | undefined, phase: OwnerBufferedPhase, fn: () => Promise<T>): Promise<T> {
  return phases ? phases.time(phase, fn) : fn();
}

function timedSync<T>(phases: OwnerBufferedPhaseTimer | undefined, phase: OwnerBufferedPhase, fn: () => T): T {
  return phases ? phases.timeSync(phase, fn) : fn();
}

/** The caller: org and workspace from the verified token, user from its verified subject. */
export interface BufferedScope {
  orgId: string;
  workspaceId: string;
  userId: string;
}

/**
 * The buffered read is fenced: the privacy authority could not be observed, answered
 * anything but a valid active non-pending head for exactly this caller, moved during the
 * request, or the RPC refused or failed (55006, 57014, any reader error). Retryable. `reason`
 * is for logs only.
 */
export class BufferedFenced extends Error {
  /** For logs only: the SQLSTATE class (two chars) of an RPC failure, never the message or full code. */
  readonly sqlstateClass?: string;
  constructor(readonly reason: string = "fenced", sqlstateClass?: unknown) {
    super("buffered read fenced");
    this.name = "BufferedFenced";
    const cls = sqlstateClassOf(sqlstateClass);
    if (cls !== undefined) this.sqlstateClass = cls;
  }
}

/** The class (first two chars) of a code that is exactly SQLSTATE-shaped ([0-9A-Z]{5}); anything else is undefined. */
function sqlstateClassOf(code: unknown): string | undefined {
  return typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code.slice(0, 2) : undefined;
}

/**
 * A fence before any buffered candidate was found (the route's existing answer). Both fields are
 * for logs only: the closed reason and, for an RPC failure, the validated SQLSTATE class.
 */
export interface UnverifiedBeforeCandidate {
  unverifiedBeforeCandidate: string;
  unverifiedSqlstateClass?: string;
}

function unverified(error: BufferedFenced): UnverifiedBeforeCandidate {
  const cls = error.sqlstateClass;
  return typeof cls === "string" && /^[0-9A-Z]{2}$/.test(cls)
    ? { unverifiedBeforeCandidate: error.reason, unverifiedSqlstateClass: cls }
    : { unverifiedBeforeCandidate: error.reason };
}

/**
 * Every reason this module fences with (plus the default). Logs carry a fence reason only
 * through fenceSubtype, so a reason outside this closed set can never reach a log line.
 */
export const FENCE_SUBTYPES: readonly string[] = Object.freeze([
  "fenced",
  "rpc_fenced", "rpc_timeout", "rpc_error",
  "gate_unbound", "gate_identity", "gate_status", "gate_cache", "gate_shape", "gate_timeout", "gate_error",
  "gate_echo", "gate_pending", "gate_epoch", "gate_moved",
  "request_aborted",
]);

/** A fence reason for logs: the reason when it is a known subtype, else "fence_other". */
export function fenceSubtype(reason: unknown): string {
  return typeof reason === "string" && FENCE_SUBTYPES.includes(reason) ? reason : "fence_other";
}

/** The RPC's explicit refusal (SQLSTATE 55006 or its message). */
export function isBufferedFencedError(error: unknown): boolean {
  if (error instanceof BufferedFenced) return true;
  const e = error as { code?: unknown; message?: unknown } | null;
  return !!e && (e.code === "55006" || e.message === "buffered read fenced");
}

/** SQLSTATE 57014 query_canceled (the per-transaction statement_timeout). */
export function isBufferedTimeoutError(error: unknown): boolean {
  const e = error as { code?: unknown } | null;
  return !!e && typeof e === "object" && e.code === "57014";
}

/**
 * The one mapping from a reader error: everything the transaction throws is fenced (not
 * visible, retryable), never a generic error. `reason` distinguishes them in logs only.
 */
function rethrowBuffered(error: unknown): never {
  if (error instanceof BufferedFenced) throw error;
  const code = error !== null && typeof error === "object" ? (error as { code?: unknown }).code : undefined;
  if (isBufferedFencedError(error)) throw new BufferedFenced("rpc_fenced", code);
  if (isBufferedTimeoutError(error)) throw new BufferedFenced("rpc_timeout", code);
  throw new BufferedFenced("rpc_error", code);
}

// ---------------------------------------------------------------------------------------------
// Authority gate (control-head contract v5, relayhistory-cloud #328 transport)
// ---------------------------------------------------------------------------------------------

export const CONTROL_HEAD_PATH = "/v1/session-read/control-head";
/** The binding ignores the host; only the path selects the handler on the pinned entrypoint. */
export const CONTROL_HEAD_URL = `https://history-control-head.internal${CONTROL_HEAD_PATH}`;
/** Identity headers; set only from the verified token, never from caller input. */
export const CONTROL_HEAD_USER_HEADER = "x-relayhistory-user-id";
export const CONTROL_HEAD_ORG_HEADER = "x-relayhistory-org-id";
export const CONTROL_HEAD_WORKSPACE_HEADER = "x-relayhistory-workspace-id";
/** Contract v5: 2 s total consumer deadline; timeout = not visible. */
export const CONTROL_HEAD_DEADLINE_MS = 2000;
/** Largest response body read from the binding; anything longer is malformed. */
const CONTROL_HEAD_MAX_BODY = 16 * 1024;

const HEX64 = /^[0-9a-f]{64}$/u;
/** A non-negative decimal within bigint, no sign, no leading zero (except "0"). */
const DECIMAL = /^(?:0|[1-9][0-9]{0,18})$/u;
const BIGINT_MAX = 9223372036854775807n;
const isDecimal = (v: unknown): v is string => typeof v === "string" && DECIMAL.test(v) && BigInt(v) <= BIGINT_MAX;
const isHex64 = (v: unknown): v is string => typeof v === "string" && HEX64.test(v);

/**
 * One validated control-head observation. `cutoverEpoch` is controlEpoch (== cutover.epoch;
 * a head's epoch is sourceControlEpoch, which must equal it). `head` is null exactly for the
 * explicit no-head tuple (sourceControlVersion "0", null source epoch, digest and kind).
 * `privacyBaselineSha256` is validated (hex64), takes part in the two-lookup consistency check
 * and is passed to both readers as p_privacy_baseline_sha256 (PR A's G2 parity gate).
 * Every other key of the wire is a constant or the caller's own identity, so equality of two
 * AuthorityHeads is deep equality of the two 200 bodies.
 */
export interface AuthorityHead {
  cutoverEpoch: string;
  privacyBaselineSha256: string;
  head: null | { epoch: string; version: string; digest: string; kind: "selection" | "reset"; pending: false };
}

/** Observes the caller's control head. Throws BufferedFenced on every failure; never caches. */
export interface ControlHeadGate {
  observe(scope: BufferedScope): Promise<AuthorityHead>;
}

const exactKeys = (v: unknown, keys: readonly string[]): v is Record<string, unknown> => {
  if (!v || typeof v !== "object" || Array.isArray(v)) return false;
  const present = Object.keys(v);
  return present.length === keys.length && keys.every((k) => present.includes(k));
};

/**
 * THE control-head wire table: the one place that names the exact 200 key set (top level and
 * nested) and its constants. Every key is required; any other key, top level or nested, fences.
 * Wire freeze (lead 245002086845415424): the wire is exactly what relayhistory-cloud #328 head
 * a06a58f9 emits; this table moves with the producer, and nothing else in the parser names keys
 * or constants. Any key the producer adds fences every read until this table moves.
 */
export const CONTROL_HEAD_WIRE = {
  top: [
    "protocol", "version", "issuer", "tenant", "controlEpoch", "cutover", "privacyBaselineSha256",
    "sourceControlEpoch", "sourceControlVersion", "sourceControlDigest", "sourceControlKind", "pending",
  ],
  nested: {
    tenant: ["orgId", "workspaceId", "userId"],
    cutover: ["epoch", "status", "mode"],
  },
  constants: { protocol: "relay-session-read-control-head-v1", version: 1, issuer: "relayhistory-cloud" },
  cutoverConstants: { status: "active", mode: "ingest" },
} as const;
/** The top-level key list of CONTROL_HEAD_WIRE. */
export const CONTROL_HEAD_V5_KEYS = CONTROL_HEAD_WIRE.top;

/** Exact-key check of the whole body against CONTROL_HEAD_WIRE (top level and every nested object). */
function wireShapeOk(body: unknown): body is Record<string, unknown> {
  if (!exactKeys(body, CONTROL_HEAD_WIRE.top)) return false;
  return Object.entries(CONTROL_HEAD_WIRE.nested).every(([key, keys]) => exactKeys(body[key], keys));
}
const matchesConstants = (v: Record<string, unknown>, constants: Readonly<Record<string, unknown>>) =>
  Object.entries(constants).every(([k, want]) => v[k] === want);

/** Identifier bound of the wire (the producer answers 403 otherwise). */
export const CONTROL_HEAD_IDENTIFIER_MAX_BYTES = 512;
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;
/** An identifier the producer accepts: 1..512 UTF-8 bytes, equal to its trim(), no U+0000-001F / U+007F. */
export const isControlHeadIdentifier = (v: unknown): v is string =>
  typeof v === "string" && v.length > 0 && new TextEncoder().encode(v).byteLength <= CONTROL_HEAD_IDENTIFIER_MAX_BYTES && v.trim() === v && !CONTROL_CHARS.test(v);

/**
 * Strict parse of the frozen wire (relayhistory-cloud #328 @ a06a58f9, lead 245002086845415424;
 * R2 control-head contract v5 semantics). The keys and constants are
 * CONTROL_HEAD_WIRE's, exactly. tenant is an exact echo of the caller; cutover is
 * {epoch == controlEpoch, status "active", mode "ingest"}; controlEpoch and
 * privacyBaselineSha256 are hex64; sourceControlVersion is a decimal string 0..2^63-1 (never a
 * Number). Version "0": sourceControlEpoch, digest and kind all null, pending false (no head).
 * Version > 0: sourceControlEpoch == controlEpoch (else gate_epoch, never "no head"), digest
 * hex64, kind selection|reset, pending boolean. pending true is fenced. Anything else is fenced.
 */
export function parseControlHeadResponse(body: unknown, scope: BufferedScope): AuthorityHead {
  const fail = (reason: string): never => {
    throw new BufferedFenced(reason);
  };
  if (!wireShapeOk(body)) return fail("gate_shape");
  if (!matchesConstants(body, CONTROL_HEAD_WIRE.constants)) fail("gate_shape");
  const tenant = body.tenant as Record<string, unknown>;
  const cutover = body.cutover as Record<string, unknown>;
  if (typeof tenant.orgId !== "string" || typeof tenant.workspaceId !== "string" || typeof tenant.userId !== "string") fail("gate_shape");
  if (tenant.orgId !== scope.orgId || tenant.workspaceId !== scope.workspaceId || tenant.userId !== scope.userId) fail("gate_echo");
  if (!isHex64(body.controlEpoch) || !isHex64(body.privacyBaselineSha256) || !isDecimal(body.sourceControlVersion)) fail("gate_shape");
  if (!matchesConstants(cutover, CONTROL_HEAD_WIRE.cutoverConstants) || !isHex64(cutover.epoch)) fail("gate_shape");
  if (typeof body.pending !== "boolean") fail("gate_shape");
  const { sourceControlEpoch: sourceEpoch, sourceControlDigest: digest, sourceControlKind: kind } = body;
  if (sourceEpoch !== null && !isHex64(sourceEpoch)) fail("gate_shape");
  if (digest !== null && !isHex64(digest)) fail("gate_shape");
  if (kind !== null && kind !== "selection" && kind !== "reset") fail("gate_shape");
  if (body.pending) fail("gate_pending");
  const controlEpoch = body.controlEpoch as string;
  if (cutover.epoch !== controlEpoch) fail("gate_epoch");
  const version = body.sourceControlVersion as string;
  const base = { cutoverEpoch: controlEpoch, privacyBaselineSha256: body.privacyBaselineSha256 as string };
  if (version === "0") {
    if (sourceEpoch !== null || digest !== null || kind !== null) fail("gate_shape");
    return { ...base, head: null };
  }
  // A non-zero head at a foreign (or no) epoch is fenced, never read as "no head".
  if (sourceEpoch !== controlEpoch) fail("gate_epoch");
  if (digest === null || kind === null) return fail("gate_shape");
  return { ...base, head: { epoch: controlEpoch, version, digest: digest as string, kind: kind as "selection" | "reset", pending: false } };
}

/** The Cloudflare service binding surface used here (a Fetcher). */
export interface ControlHeadBinding {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

/**
 * The gate over the HISTORY_CONTROL_HEAD service binding (named entrypoint
 * SessionReadPrivacyControls on relayhistory-cloud, pinned in the Worker's wrangler config,
 * never a public route): GET /v1/session-read/control-head, no query string, no body,
 * identity only in the x-relayhistory-{org,workspace,user}-id headers from the verified
 * scope, 2 s total deadline. No caching: the request says `cache-control: no-store`, a 200
 * must carry a `cache-control` with the `no-store` token (the producer sends `private, no-store`)
 * and nothing is memoized (every observe() is one binding call). Unbound, an identifier the
 * producer would refuse (see isControlHeadIdentifier), any non-200 (its error body, flat or the
 * nested 503 {version,error:{code}}, is never parsed), a 200 without no-store, over the deadline,
 * over-long, not JSON or invalid: fenced.
 */
export function serviceBindingControlHead(binding: ControlHeadBinding | undefined, options: { deadlineMs?: number } = {}): ControlHeadGate {
  const deadlineMs = options.deadlineMs ?? CONTROL_HEAD_DEADLINE_MS;
  return {
    async observe(scope) {
      if (!binding || typeof binding.fetch !== "function") throw new BufferedFenced("gate_unbound");
      // The producer answers 403 to an identifier it refuses; fence locally, without the call.
      if (![scope.orgId, scope.workspaceId, scope.userId].every(isControlHeadIdentifier)) throw new BufferedFenced("gate_identity");
      const controller = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const work = (async () => {
        const res = await binding.fetch(CONTROL_HEAD_URL, {
          method: "GET",
          headers: {
            accept: "application/json",
            "cache-control": "no-store",
            [CONTROL_HEAD_USER_HEADER]: scope.userId,
            [CONTROL_HEAD_ORG_HEADER]: scope.orgId,
            [CONTROL_HEAD_WORKSPACE_HEADER]: scope.workspaceId,
          },
          signal: controller.signal,
        });
        if (res.status !== 200) {
          await res.body?.cancel().catch(() => undefined);
          throw new BufferedFenced("gate_status");
        }
        const cacheControl = res.headers.get("cache-control") ?? "";
        if (!cacheControl.split(",").some((t) => t.trim().toLowerCase() === "no-store")) {
          await res.body?.cancel().catch(() => undefined);
          throw new BufferedFenced("gate_cache");
        }
        const raw = await res.text();
        if (raw.length > CONTROL_HEAD_MAX_BODY) throw new BufferedFenced("gate_shape");
        let body: unknown;
        try {
          body = JSON.parse(raw);
        } catch {
          throw new BufferedFenced("gate_shape");
        }
        return parseControlHeadResponse(body, scope);
      })();
      work.catch(() => undefined);
      try {
        return await Promise.race([
          work,
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => {
              controller.abort();
              reject(new BufferedFenced("gate_timeout"));
            }, deadlineMs);
          }),
        ]);
      } catch (error) {
        if (error instanceof BufferedFenced) throw error;
        throw new BufferedFenced("gate_error");
      } finally {
        clearTimeout(timer);
      }
    },
  };
}

/** The two-lookup consistency check: every field of both observations must agree. */
const sameAuthority = (a: AuthorityHead, b: AuthorityHead) =>
  a.cutoverEpoch === b.cutoverEpoch &&
  a.privacyBaselineSha256 === b.privacyBaselineSha256 &&
  (a.head === null ? b.head === null : b.head !== null && a.head.epoch === b.head.epoch && a.head.version === b.head.version && a.head.digest === b.head.digest && a.head.kind === b.head.kind);

/** An immutable admission witness from the RPC; either part may be NULL (unknown). */
export interface Admission {
  admission_epoch: string | null;
  admission_version: string | null;
}

/**
 * The consumer rule's item 3. head null: admission epoch == cutover epoch and version 0.
 * Otherwise: admission epoch == head epoch and version == head version (no control of any kind
 * since admission). NULL, absent or non-decimal: hidden. Versions compare as BigInt, never Number.
 */
export function admissionVisible(authority: AuthorityHead, admission: Admission): boolean {
  const { admission_epoch: epoch, admission_version: version } = admission;
  if (typeof epoch !== "string" || !HEX64.test(epoch) || !isDecimal(version)) return false;
  if (authority.head === null) return epoch === authority.cutoverEpoch && BigInt(version) === 0n;
  return epoch === authority.head.epoch && BigInt(version) === BigInt(authority.head.version);
}

/**
 * The RPC authority parameters of one observation. No head: p_head_epoch, p_head_version,
 * p_head_digest and p_head_kind NULL, p_head_pending FALSE (never NULL, never "0"). Head present:
 * p_head_epoch is controlEpoch (== sourceControlEpoch), p_head_version the exact decimal string,
 * p_head_digest, p_head_kind ('selection' | 'reset') and p_head_pending as observed.
 * p_cutover_epoch is controlEpoch; p_privacy_baseline_sha256 is the observed baseline (hex64).
 */
export function headParams(a: AuthorityHead) {
  return {
    cutoverEpoch: a.cutoverEpoch,
    headEpoch: a.head?.epoch ?? null,
    headVersion: a.head?.version ?? null,
    headDigest: a.head?.digest ?? null,
    headKind: a.head?.kind ?? null,
    headPending: a.head ? a.head.pending : false,
    privacyBaselineSha256: a.privacyBaselineSha256,
  };
}

// ---------------------------------------------------------------------------------------------
// RPC readers
// ---------------------------------------------------------------------------------------------

/** A buffered_sessions_for row, validated. */
export interface BufferedSessionRow extends Admission {
  source: string;
  session_id: string;
  first_ts: string;
  last_ts: string;
  stage_count: number;
  bytes: number;
  user_id: string;
  /** last_ts as UTC ISO with six fractional digits: the list cursor. */
  cursor_ts: string;
}

/** A buffered_session_stages_for row, validated. */
export interface BufferedStageRow extends Admission {
  batch_id: string;
  stage_key: string;
  stage_sha256: string;
  stage_bytes: number;
  group_indexes: number[];
  /** The stage's full group_ordinals (readBufferedStage checks the object against it). */
  group_ordinals: number[][];
  kept_ordinals: number[];
  reset_epoch: string;
  accepted_at: string;
}

export interface BufferedListPosition {
  ts: string;
  sessionId: string;
  source: string;
}

export interface OwnerBufferedReads {
  list(scope: BufferedScope, authority: AuthorityHead, args: { cursor?: BufferedListPosition; limit: number }): Promise<BufferedSessionRow[]>;
  stages(scope: BufferedScope, authority: AuthorityHead, source: string, sessionId: string): Promise<BufferedStageRow[]>;
}

/** The app's dependency: `enabled` is the flag; the worker wires the rest only when it is on. */
export interface OwnerBufferedDeps {
  enabled: boolean;
  /**
   * READ_API_OWNER_BUFFERED_WORKSPACES (parseDirectoryWorkspaces syntax): the owner-buffered
   * canary allowlist, on top of READ_API_DIRECTORY_WORKSPACES (a workspace must be on both).
   * Unset/empty = none, so the flag alone serves no buffered read.
   */
  workspaces: ReadonlySet<string> | "*";
  reads: OwnerBufferedReads;
  /** The privacy authority (HISTORY_CONTROL_HEAD); required, there is no ungated read. */
  gate: ControlHeadGate;
  /** The buffered-batches store (INGEST_S3_BUCKET); GetObject only. */
  store: ObjectStore;
  /**
   * zstd with a bounded output: must stop and throw BufferedTooLarge (or any error) as soon as
   * the output would exceed `maxBytes`, never buffering past it. The worker streams fzstd.
   */
  decompress: (bytes: Uint8Array, maxBytes: number) => Uint8Array;
  /**
   * READ_API_OWNER_BUFFERED_EVENTS=on: the session events route may serve buffered events.
   * Absent or false: it never does (every events response exactly as without the flag).
   */
  events?: boolean;
  /**
   * READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY (ownerBufferedStageConcurrency): the most stage
   * GetObjects in flight in one buffered events read (readOwnerBufferedEvents; turns stay
   * serial). Absent or 1: the serial fetch.
   */
  stageConcurrency?: number;
  /** READ_API_OWNER_BUFFERED_PHASE_LOG=1: the events route logs per-phase times. */
  phaseLog?: boolean;
  /**
   * READ_API_OWNER_BUFFERED_INDEX_VERIFY=on: every owner-buffered read (list, turns, metadata,
   * events) first runs ensure(), which throws BufferedFenced (index_missing, index_invalid or
   * index_unverified) unless BUFFERED_INDEX_NAME is proven valid. Absent: no check, no query.
   */
  indexCheck?: BufferedIndexCheck;
}

/** One stage object: the compressed and the decompressed bound (staging.ts BUFFER_STAGE_MAX_BYTES). */
export const BUFFERED_STAGE_MAX_BYTES = BUFFER_STAGE_MAX_BYTES;
/**
 * The memory bound of one buffered detail read: the total of the stages' compressed sizes and,
 * separately, of their decompressed sizes for one session. Over it the session is hidden (the
 * route's existing 501), never served partially. Peak memory per read is roughly the packed
 * stage, its decoder window, the decoded chunks and their concatenation (each <= 16 MiB) plus
 * the parsed records of up to 32 MiB of decoded text. Not measured against an isolate limit.
 */
export const BUFFERED_SESSION_MAX_BYTES = 32 * 1024 * 1024;

/** A stage or a session over its byte bound: the session is hidden. */
export class BufferedTooLarge extends Error {
  constructor() {
    super("buffered session over its byte bound");
    this.name = "BufferedTooLarge";
  }
}

/** A push-style decompressor (fzstd's Decompress): `ondata` per output chunk, synchronously. */
export type StreamDecompressor = new (ondata: (chunk: Uint8Array, final?: boolean) => unknown) => { push(chunk: Uint8Array, final?: boolean): unknown };

const ZSTD_MAGIC = 0xfd2fb528;
const readLE = (b: Uint8Array, at: number, n: number): number => {
  let v = 0;
  for (let i = n - 1; i >= 0; i--) v = v * 256 + b[at + i]!;
  return v;
};

/**
 * Walks every zstd frame header before any decoding (fzstd allocates each frame's window from
 * its header, before the first output chunk): a frame content size or window size above
 * `maxBytes` is BufferedTooLarge; a truncated or reserved structure is an error. Skippable
 * frames are skipped; blocks are walked only to find the next frame. Allocates nothing.
 */
export function assertZstdFramesWithin(bytes: Uint8Array, maxBytes: number): void {
  const bad = (): never => {
    throw new Error("buffered_zstd_invalid");
  };
  let at = 0;
  while (at < bytes.byteLength) {
    if (at + 4 > bytes.byteLength) bad();
    const magic = readLE(bytes, at, 4);
    if (magic >>> 4 === 0x184d2a5) {
      if (at + 8 > bytes.byteLength) bad();
      at += 8 + readLE(bytes, at + 4, 4);
      if (at > bytes.byteLength) bad();
      continue;
    }
    if (magic !== ZSTD_MAGIC || at + 5 > bytes.byteLength) bad();
    const fhd = bytes[at + 4]!;
    at += 5;
    if (fhd & 0x08) bad(); // reserved bit
    const singleSegment = (fhd & 0x20) !== 0;
    const checksum = (fhd & 0x04) !== 0;
    const dictBytes = [0, 1, 2, 4][fhd & 0x03]!;
    const fcsFlag = fhd >> 6;
    const fcsBytes = fcsFlag === 0 ? (singleSegment ? 1 : 0) : [0, 2, 4, 8][fcsFlag]!;
    let window = -1;
    if (!singleSegment) {
      if (at + 1 > bytes.byteLength) bad();
      const wd = bytes[at]!;
      const base = 2 ** (10 + (wd >> 3));
      window = base + (base / 8) * (wd & 7);
      at += 1;
    }
    at += dictBytes;
    if (at + fcsBytes > bytes.byteLength) bad();
    if (fcsBytes > 0) {
      const fcs = readLE(bytes, at, fcsBytes) + (fcsBytes === 2 ? 256 : 0);
      if (fcs > maxBytes) throw new BufferedTooLarge();
      if (singleSegment) window = fcs;
    }
    if (window > maxBytes) throw new BufferedTooLarge();
    at += fcsBytes;
    for (;;) {
      if (at + 3 > bytes.byteLength) bad();
      const bh = readLE(bytes, at, 3);
      const type = (bh >> 1) & 3;
      if (type === 3) bad();
      at += 3 + (type === 1 ? 1 : bh >>> 3);
      if (at > bytes.byteLength) bad();
      if (bh & 1) break;
    }
    if (checksum) at += 4;
    if (at > bytes.byteLength) bad();
  }
}

/**
 * OwnerBufferedDeps.decompress over a streaming decompressor: every frame header is checked
 * first (assertZstdFramesWithin, so no window above the bound is ever allocated), then output
 * chunks are counted as they arrive and the stream is abandoned (BufferedTooLarge) the moment
 * the total passes `maxBytes`, so at most one chunk (a zstd block, <= 128 KiB) is held past it.
 */
export function boundedStreamDecompress(Stream: StreamDecompressor): OwnerBufferedDeps["decompress"] {
  return (bytes, maxBytes) => {
    assertZstdFramesWithin(bytes, maxBytes);
    const chunks: Uint8Array[] = [];
    let total = 0;
    const stream = new Stream((chunk) => {
      total += chunk.byteLength;
      if (total > maxBytes) throw new BufferedTooLarge();
      chunks.push(chunk);
    });
    stream.push(bytes, true);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  };
}

/** The RPC page bound (PR A clamps p_limit to 1..100). */
export const BUFFERED_LIST_MAX_LIMIT = 100;
export const BUFFERED_STATEMENT_TIMEOUT_MS = 2000;

export const BUFFERED_USER_SQL = "SELECT set_config('bulls96_read.user_id', $1, true)";
const HEAD_ARGS =
  "p_cutover_epoch => $2, p_head_epoch => $3, p_head_version => $4::text, p_head_digest => $5, p_head_kind => $6, " +
  "p_head_pending => $7::boolean, p_privacy_baseline_sha256 => $8";
export const BUFFERED_SESSIONS_SQL =
  `SELECT source, session_id, first_ts, last_ts, stage_count, bytes, user_id, admission_epoch, admission_version::text AS admission_version, ` +
  `to_char(last_ts AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_ts ` +
  `FROM sessions.buffered_sessions_for(p_workspace => $1, ${HEAD_ARGS}, p_cursor_ts => $9::timestamptz, p_cursor_session => $10, p_cursor_source => $11, p_limit => $12::integer)`;
/**
 * group_ordinals is required: kept_ordinals name original claim ordinals and only the stage's
 * group_ordinals map them to object lines (bufferedOrdinalSelection) and bind the object
 * (readBufferedStage). PR A's buffered_session_stages_for must return it.
 */
export const BUFFERED_STAGES_SQL =
  `SELECT batch_id, stage_key, stage_sha256, stage_bytes, group_indexes, group_ordinals, kept_ordinals, reset_epoch, accepted_at, admission_epoch, admission_version::text AS admission_version ` +
  `FROM sessions.buffered_session_stages_for(p_workspace => $1, ${HEAD_ARGS}, p_source => $9, p_session_id => $10)`;

export function statementTimeoutSql(ms: number = BUFFERED_STATEMENT_TIMEOUT_MS): string {
  if (!Number.isSafeInteger(ms) || ms < 1 || ms > 30_000) throw new Error("buffered statement timeout out of range");
  return `SET LOCAL statement_timeout = ${ms}`;
}

const CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/u;

function invalid(): never {
  throw new Error("buffered_row_invalid");
}
const text = (v: unknown, max = 1024): string => (typeof v === "string" && v.length >= 1 && v.length <= max ? v : invalid());
const count = (v: unknown): number => {
  const n = typeof v === "number" ? v : typeof v === "string" && /^\d+$/u.test(v) ? Number(v) : NaN;
  return Number.isSafeInteger(n) && n >= 0 ? n : invalid();
};
const isoTs = (v: unknown): string => {
  const d = v instanceof Date ? v : typeof v === "string" ? new Date(v) : null;
  return d && Number.isFinite(d.getTime()) ? d.toISOString() : invalid();
};
const ints = (v: unknown): number[] => (Array.isArray(v) && v.every((n) => Number.isSafeInteger(n) && n >= 0) ? (v as number[]) : invalid());
/** Admission as returned: NULL stays null (hidden later); anything else must be well formed text. */
const admission = (raw: Record<string, unknown>): Admission => {
  const e = raw.admission_epoch ?? null;
  const v = raw.admission_version ?? null;
  if (e !== null && (typeof e !== "string" || !HEX64.test(e))) invalid();
  if (v !== null && !isDecimal(v)) invalid();
  return { admission_epoch: e as string | null, admission_version: v as string | null };
};

function sessionRow(raw: Record<string, unknown>, scope: BufferedScope): BufferedSessionRow {
  // Cross-tenant guard: a row of any other user is a 500, never a page.
  if (raw.user_id !== scope.userId) throw new TenantMismatchError();
  const cursor = raw.cursor_ts;
  if (typeof cursor !== "string" || !CURSOR_TS.test(cursor)) invalid();
  return {
    source: text(raw.source, 255),
    session_id: text(raw.session_id, 512),
    first_ts: isoTs(raw.first_ts),
    last_ts: isoTs(raw.last_ts),
    stage_count: count(raw.stage_count),
    bytes: count(raw.bytes),
    user_id: scope.userId,
    cursor_ts: cursor,
    ...admission(raw),
  };
}

function stageRow(raw: Record<string, unknown>): BufferedStageRow {
  const ordinals = raw.group_ordinals;
  if (!Array.isArray(ordinals) || ordinals.length === 0) invalid();
  const sha = raw.stage_sha256;
  if (typeof sha !== "string" || !HEX64.test(sha)) invalid();
  const epoch = raw.reset_epoch;
  if (!(typeof epoch === "string" && /^\d+$/u.test(epoch)) && !(typeof epoch === "number" && Number.isSafeInteger(epoch) && epoch >= 0)) invalid();
  return {
    batch_id: text(raw.batch_id),
    stage_key: text(raw.stage_key),
    stage_sha256: sha,
    stage_bytes: count(raw.stage_bytes),
    group_indexes: ints(raw.group_indexes),
    group_ordinals: (ordinals as unknown[]).map(ints),
    kept_ordinals: ints(raw.kept_ordinals),
    reset_epoch: String(epoch),
    accepted_at: isoTs(raw.accepted_at),
    ...admission(raw),
  };
}

/**
 * The readers over the read role. Each call is one READ ONLY transaction of the TenantRunner
 * (which binds bulls96_read.org_id / workspace_id): a per-transaction statement_timeout, the
 * user binding, then the RPC with the observed head. No row lock, no write.
 */
export function sqlOwnerBufferedReads(run: TenantRunner, options: { statementTimeoutMs?: number } = {}): OwnerBufferedReads {
  const timeout = statementTimeoutSql(options.statementTimeoutMs ?? BUFFERED_STATEMENT_TIMEOUT_MS);
  const call = async (scope: BufferedScope, sql: string, params: unknown[]) => {
    try {
      return await run(scope.orgId, scope.workspaceId, async (q) => {
        await q.query(timeout);
        await q.query(BUFFERED_USER_SQL, [scope.userId]);
        return (await q.query<Record<string, unknown>>(sql, params)).rows;
      });
    } catch (error) {
      rethrowBuffered(error);
    }
  };
  const head = (a: AuthorityHead) => {
    const h = headParams(a);
    return [h.cutoverEpoch, h.headEpoch, h.headVersion, h.headDigest, h.headKind, h.headPending, h.privacyBaselineSha256];
  };
  return {
    async list(scope, authority, args) {
      const limit = Math.min(BUFFERED_LIST_MAX_LIMIT, Math.max(1, Math.trunc(args.limit)));
      const c = args.cursor;
      const rows = await call(scope, BUFFERED_SESSIONS_SQL, [scope.workspaceId, ...head(authority), c?.ts ?? null, c ? c.sessionId : null, c ? c.source : null, limit]);
      if (rows.length > limit) invalid();
      return rows.map((r) => sessionRow(r, scope));
    },
    async stages(scope, authority, source, sessionId) {
      const rows = await call(scope, BUFFERED_STAGES_SQL, [scope.workspaceId, ...head(authority), source, sessionId]);
      return rows.map(stageRow);
    },
  };
}

// ---------------------------------------------------------------------------------------------
// Out-of-band index verification (READ_API_OWNER_BUFFERED_INDEX_VERIFY, default off)
// ---------------------------------------------------------------------------------------------

/**
 * The partial index the buffered readers depend on. Migration 20261105043700 leaves it out of
 * band on purpose (the migrator is transactional; it must be built CONCURRENTLY by an operator
 * before owner-buffered reads are enabled):
 *
 *   CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_buffer_outbox_user_buffered_idx
 *     ON sessions.ingest_buffer_outbox (org_id, workspace_id, user_id, created_at)
 *     WHERE state = 'buffered';
 *
 * Without it sessions.buffered_sessions_for / buffered_session_stages_for scan the outbox and
 * hit the statement timeout (rpc_timeout).
 */
export const BUFFERED_INDEX_SCHEMA = "sessions";
export const BUFFERED_INDEX_TABLE = "ingest_buffer_outbox";
export const BUFFERED_INDEX_NAME = "ingest_buffer_outbox_user_buffered_idx";

/**
 * The catalog probe: one row per index of that name on that table, `valid` true only when the
 * index is usable (indisvalid, indisready, indislive). A failed or in-progress CONCURRENTLY build
 * leaves indisvalid false (and IF NOT EXISTS then skips it: the operator must drop it first).
 * Catalog views are readable by every role, so the read role needs no grant. Constants only, no
 * parameter.
 */
export const BUFFERED_INDEX_SQL =
  "SELECT (i.indisvalid AND i.indisready AND i.indislive) AS valid " +
  "FROM pg_catalog.pg_index i " +
  "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid " +
  "JOIN pg_catalog.pg_class t ON t.oid = i.indrelid " +
  "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace " +
  `WHERE n.nspname = '${BUFFERED_INDEX_SCHEMA}' AND t.relname = '${BUFFERED_INDEX_TABLE}' AND c.relname = '${BUFFERED_INDEX_NAME}'`;

/** The probe's answer: present and valid, present but not valid, or absent. */
export type BufferedIndexState = "valid" | "invalid" | "missing";
/** One catalog probe over a request's read connection; may throw (timeout, connection). */
export type BufferedIndexProbe = (scope: BufferedScope) => Promise<BufferedIndexState>;

/** The fence reasons (logs only, by name) a failed index check throws BufferedFenced with. */
export const BUFFERED_INDEX_REASONS = Object.freeze({
  /** No index of that name on the table. */
  missing: "index_missing",
  /** Present but not valid (a failed or unfinished CONCURRENTLY build). */
  invalid: "index_invalid",
  /** The probe itself failed (statement timeout, connection, any error) or did not answer in time. */
  unverified: "index_unverified",
} as const);

/** A failed check is re-probed after this long (an operator's CREATE INDEX takes effect without a redeploy). */
export const BUFFERED_INDEX_RETRY_MS = 60_000;
/**
 * The most one request waits for a probe (its own or a shared in-flight one): the statement
 * timeout plus the pg connect timeout plus slack. Past it the request is fenced
 * index_unverified and the probe is forgotten, so a probe whose request was canceled (or whose
 * promise never settles in this request's context) cannot hang later requests.
 */
export const BUFFERED_INDEX_WAIT_MS = 5_000;

/**
 * The probe over the read role: one READ ONLY transaction of the TenantRunner with the same
 * per-transaction statement_timeout as the buffered readers, then BUFFERED_INDEX_SQL. Errors
 * propagate (the verifier fences them as index_unverified).
 */
export function sqlBufferedIndexProbe(run: TenantRunner, options: { statementTimeoutMs?: number } = {}): BufferedIndexProbe {
  const timeout = statementTimeoutSql(options.statementTimeoutMs ?? BUFFERED_STATEMENT_TIMEOUT_MS);
  return (scope) =>
    run(scope.orgId, scope.workspaceId, async (q) => {
      await q.query(timeout);
      const rows = (await q.query<{ valid?: unknown }>(BUFFERED_INDEX_SQL)).rows;
      if (rows.length === 0) return "missing";
      return rows.some((r) => r.valid === true) ? "valid" : "invalid";
    });
}

/** The per-request check the app's read paths call: resolves when the index is proven valid, else throws BufferedFenced. */
export interface BufferedIndexCheck {
  ensure(scope: BufferedScope): Promise<void>;
}

type IndexOutcome = "ok" | (typeof BUFFERED_INDEX_REASONS)[keyof typeof BUFFERED_INDEX_REASONS];

/**
 * The per-isolate cache of the index check (the Worker holds one at module scope; each request
 * binds its own probe with check()). A valid answer is remembered for the isolate's lifetime and
 * no query runs again. A missing, invalid or failed answer is remembered for `retryMs`
 * (fenced without a query), then probed again. Concurrent requests share one in-flight probe;
 * each waits at most `waitMs` for it. `now` is injectable for tests.
 */
export class BufferedIndexVerifier {
  private verified = false;
  private failure: { reason: Exclude<IndexOutcome, "ok">; at: number } | undefined;
  private inflight: Promise<IndexOutcome> | undefined;
  private readonly now: () => number;
  private readonly retryMs: number;
  private readonly waitMs: number;

  constructor(options: { now?: () => number; retryMs?: number; waitMs?: number } = {}) {
    this.now = options.now ?? (() => Date.now());
    this.retryMs = options.retryMs ?? BUFFERED_INDEX_RETRY_MS;
    this.waitMs = options.waitMs ?? BUFFERED_INDEX_WAIT_MS;
  }

  /** The check one request runs, over that request's probe. */
  check(probe: BufferedIndexProbe): BufferedIndexCheck {
    return { ensure: (scope) => this.ensure(scope, probe) };
  }

  async ensure(scope: BufferedScope, probe: BufferedIndexProbe): Promise<void> {
    if (this.verified) return;
    if (this.failure && this.now() - this.failure.at < this.retryMs) throw new BufferedFenced(this.failure.reason);
    const shared = (this.inflight ??= this.start(scope, probe));
    const outcome = await this.within(shared);
    if (outcome === "ok") return;
    throw new BufferedFenced(outcome);
  }

  private start(scope: BufferedScope, probe: BufferedIndexProbe): Promise<IndexOutcome> {
    const run = (async (): Promise<IndexOutcome> => {
      try {
        const state = await probe(scope);
        return state === "valid" ? "ok" : state === "invalid" ? BUFFERED_INDEX_REASONS.invalid : BUFFERED_INDEX_REASONS.missing;
      } catch {
        return BUFFERED_INDEX_REASONS.unverified;
      }
    })();
    const settled = run.then((outcome) => {
      if (this.inflight === settled) this.inflight = undefined;
      this.record(outcome);
      return outcome;
    });
    return settled;
  }

  /** The shared outcome, or index_unverified (remembered for retryMs, probe forgotten) past waitMs. */
  private async within(shared: Promise<IndexOutcome>): Promise<IndexOutcome> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<"late">((resolve) => {
      timer = setTimeout(() => resolve("late"), this.waitMs);
    });
    try {
      const outcome = await Promise.race([shared, late]);
      if (outcome !== "late") return outcome;
      if (this.inflight === shared) this.inflight = undefined;
      this.record(BUFFERED_INDEX_REASONS.unverified);
      return BUFFERED_INDEX_REASONS.unverified;
    } finally {
      clearTimeout(timer);
    }
  }

  private record(outcome: IndexOutcome): void {
    if (outcome === "ok") {
      this.verified = true;
      this.failure = undefined;
    } else if (!this.verified) this.failure = { reason: outcome, at: this.now() };
  }
}

/**
 * One buffered list row in the cloud-client 0.2.0 `sessionSummary` shape, key for key as
 * sessionFromDirectoryRow (no catalog). firstTs/lastTs are acceptance times of the oldest and
 * newest buffered batch holding the session; nothing else is known without opening S3.
 */
export function bufferedSessionSummary(row: BufferedSessionRow, userId: string) {
  return {
    title: null,
    sessionId: row.session_id,
    userIds: [userId],
    source: row.source,
    projectId: null,
    machineIds: [] as string[],
    firstTs: row.first_ts,
    lastTs: row.last_ts,
    workState: null,
    eventCount: 0,
    kinds: [] as string[],
    models: [] as string[],
    taskTitle: null,
    summary: null,
    summarySource: null,
    taskRefs: [] as unknown[],
    totalCostUsdMicros: null,
    totalInputTokens: 0,
    totalOutputTokens: 0,
    totalReasoningTokens: 0,
    totalCacheReadTokens: 0,
    totalCacheCreateTokens: 0,
    totalCacheCreate5mTokens: null,
    totalCacheCreate1hTokens: null,
  };
}

/** `<source>:<sessionId>` for X-Relayhistory-Buffered-Ids; each part encodeURIComponent'd (a no-op for ordinary ids). */
export function bufferedIdsHeader(rows: Array<{ source: string; session_id: string }>): string {
  return rows.map((r) => `${encodeURIComponent(r.source)}:${encodeURIComponent(r.session_id)}`).join(",");
}

/** A cursor timestamp padded to six fractional digits, so two compare as strings. */
export function cursorTsKey(ts: string): string {
  const m = /^(.*T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/u.exec(ts);
  if (!m) throw new Error("buffered_cursor_ts_invalid");
  return `${m[1]}.${(m[2] ?? "").padEnd(6, "0")}Z`;
}

export interface MergeInput<C> {
  /** Canonical rows after the canonical cursor, as fetched (up to limit + 1). */
  canonical: Array<C & { cursor_ts: string; session_id: string; harness: string }>;
  /** Buffered rows after the buffered position, as fetched. */
  buffered: BufferedSessionRow[];
  /** The buffered fetch returned its full limit (more may follow). */
  bufferedFull: boolean;
  /** False for a row filtered out (filters, canonical wins): consumed, never shown. */
  eligible: (row: BufferedSessionRow) => boolean;
  limit: number;
}

export type MergedItem<C> = { kind: "canonical"; row: C & { cursor_ts: string; session_id: string; harness: string } } | { kind: "buffered"; row: BufferedSessionRow };

/**
 * Newest first by last activity; on equal timestamps canonical rows come first. A buffered row
 * that is not eligible advances the buffered position without taking a slot. When the
 * buffered fetch was full and is exhausted locally, the page stops before a canonical row
 * older than the last buffered row (an unseen buffered row could sit between them).
 */
export function mergeListPage<C>(input: MergeInput<C>) {
  const { canonical, buffered, bufferedFull, eligible, limit } = input;
  const items: MergedItem<C>[] = [];
  let i = 0;
  let j = 0;
  let lastCanonical: (typeof canonical)[number] | undefined;
  let lastBuffered: BufferedSessionRow | undefined;
  while (items.length < limit) {
    const c = canonical[i];
    const b = buffered[j];
    if (b && !eligible(b)) {
      lastBuffered = b;
      j += 1;
      continue;
    }
    if (!c && !b) break;
    if (c && (!b || cursorTsKey(c.cursor_ts) >= b.cursor_ts)) {
      if (!b && bufferedFull && lastBuffered && cursorTsKey(c.cursor_ts) < lastBuffered.cursor_ts) break;
      if (!b && bufferedFull && !lastBuffered) break;
      items.push({ kind: "canonical", row: c });
      lastCanonical = c;
      i += 1;
    } else {
      items.push({ kind: "buffered", row: b! });
      lastBuffered = b;
      j += 1;
    }
  }
  const hasMore = i < canonical.length || j < buffered.length || bufferedFull;
  return {
    items,
    hasMore,
    lastCanonical: lastCanonical ? { lastActivityAt: lastCanonical.cursor_ts, sessionId: lastCanonical.session_id, harness: lastCanonical.harness } : undefined,
    lastBuffered: lastBuffered ? { ts: lastBuffered.cursor_ts, sessionId: lastBuffered.session_id, source: lastBuffered.source } : undefined,
  };
}

// ---------------------------------------------------------------------------------------------
// Gated reads: the only exported paths to buffered content
// ---------------------------------------------------------------------------------------------

/** One stage's spec for readBufferedStage. */
const specOf = (row: BufferedStageRow) => ({ key: row.stage_key, sha256: row.stage_sha256, bytes: row.stage_bytes, groupOrdinals: row.group_ordinals });
const sameStage = (a: BufferedStageRow, b: BufferedStageRow) =>
  a.stage_key === b.stage_key && a.stage_sha256 === b.stage_sha256 && a.stage_bytes === b.stage_bytes &&
  JSON.stringify(a.group_indexes) === JSON.stringify(b.group_indexes) &&
  JSON.stringify(a.group_ordinals) === JSON.stringify(b.group_ordinals) &&
  JSON.stringify(a.kept_ordinals) === JSON.stringify(b.kept_ordinals) && a.reset_epoch === b.reset_epoch &&
  a.admission_epoch === b.admission_epoch && a.admission_version === b.admission_version;

type RawSessionRead =
  | { kind: "erased" }
  /** The kept set moved between the fetch and the recheck: the session is hidden. */
  | { kind: "changed" }
  | { kind: "stages"; stages: Array<{ row: BufferedStageRow; records: RelayhistoryStreamRecord[] }> };

/**
 * MODULE-PRIVATE, UNGATED. The caller's kept stages of one session, given the first stages()
 * answer (non-empty): GetObject through readBufferedStage with a bounded decompress, then
 * stages() again with the same observation. The recheck must list exactly the fetched set
 * (same stages, unchanged); an empty recheck is erased; any other difference is "changed"
 * (hidden, never a partial session). An S3 404 re-asks stages() once and refetches what is
 * still listed. Byte bounds: BUFFERED_STAGE_MAX_BYTES per stage, BUFFERED_SESSION_MAX_BYTES per
 * session, compressed (checked before any GetObject) and decompressed; over either throws
 * BufferedTooLarge. Never export this: readOwnerBufferedTurns is the gated path.
 */
async function readStagesUngated(
  deps: OwnerBufferedDeps, scope: BufferedScope, authority: AuthorityHead, source: string, sessionId: string, first: BufferedStageRow[],
  opts: GatedReadOptions = {},
): Promise<RawSessionRead> {
  const { phases } = opts;
  assertCompressedBounds(first);
  let refreshed: BufferedStageRow[] | undefined;
  let decompressed = 0;
  const fetchOne = async (row: BufferedStageRow) => {
    const stage = await readBufferedStage(deps.store, specOf(row), (bytes) => {
      const room = Math.min(BUFFERED_STAGE_MAX_BYTES, BUFFERED_SESSION_MAX_BYTES - decompressed);
      if (room <= 0) throw new BufferedTooLarge();
      const out = deps.decompress(bytes, room);
      if (out.byteLength > room) throw new BufferedTooLarge();
      decompressed += out.byteLength;
      return out;
    });
    return { row, stage };
  };
  const fetched: Array<{ row: BufferedStageRow; stage: Awaited<ReturnType<typeof readBufferedStage>> }> = [];
  const concurrency = opts.stageConcurrency ?? 1;
  phases?.stages(first.length);
  await timed(phases, "s3_fetch", async () => {
    if (concurrency >= 2) {
      // Bounded concurrency: the same fetch, refetch-on-404 and bounds, at most `concurrency`
      // GetObjects in flight, kept in `first` order. Decompress is synchronous, so the shared
      // `decompressed` total is never raced.
      let refreshing: Promise<BufferedStageRow[]> | undefined;
      const refresh = () => (refreshing ??= deps.reads.stages(scope, authority, source, sessionId).then((rows) => {
        assertCompressedBounds(rows);
        return rows;
      }));
      fetched.push(...(await fetchStagesBounded(first, concurrency, opts.signal, async (row) => {
        try {
          return await fetchOne(row);
        } catch (error) {
          if (!isObjectStoreError(error, "not_found")) throw error;
          const again = (await refresh()).find((r) => r.stage_key === row.stage_key);
          if (!again) return undefined;
          try {
            return await fetchOne(again);
          } catch (retryError) {
            if (!isObjectStoreError(retryError, "not_found")) throw retryError;
            return undefined;
          }
        }
      })));
      return;
    }
    for (const row of first) {
      try {
        fetched.push(await fetchOne(row));
      } catch (error) {
        if (!isObjectStoreError(error, "not_found")) throw error;
        refreshed ??= await deps.reads.stages(scope, authority, source, sessionId);
        assertCompressedBounds(refreshed);
        const again = refreshed.find((r) => r.stage_key === row.stage_key);
        if (!again) continue;
        try {
          fetched.push(await fetchOne(again));
        } catch (retryError) {
          if (!isObjectStoreError(retryError, "not_found")) throw retryError;
        }
      }
    }
  });
  const recheck = await timed(phases, "recheck", () => deps.reads.stages(scope, authority, source, sessionId));
  if (recheck.length === 0) return { kind: "erased" };
  if (recheck.length !== fetched.length || !fetched.every(({ row }) => recheck.some((r) => sameStage(r, row))) ||
      !recheck.every((r) => fetched.some(({ row }) => sameStage(r, row)))) return { kind: "changed" };
  const stages: Array<{ row: BufferedStageRow; records: RelayhistoryStreamRecord[] }> = [];
  timedSync(phases, "fold", () => {
    for (const { row, stage } of fetched) {
      const h = stage.header;
      if (h.org_id !== scope.orgId || h.workspace_id !== scope.workspaceId || h.user_id !== scope.userId) throw new TenantMismatchError();
      const records: RelayhistoryStreamRecord[] = [];
      for (const sel of bufferedOrdinalSelection(row.group_ordinals, row.kept_ordinals, row.group_indexes)) {
        const group = stage.groups[sel.groupIndex];
        if (!group || group.source !== source || group.sessionId !== sessionId) throw new Error("buffered_group_mismatch");
        for (const i of sel.lineIndexes) {
          const record = parseRelayhistoryRecord(group.lines[i]!);
          if (record.origin_id !== h.origin_id) throw new Error("buffered_origin_mismatch");
          records.push(record);
        }
      }
      stages.push({ row, records });
    }
  });
  return { kind: "stages", stages };
}

/**
 * The bounded-concurrency stage fetch (READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY >= 2): `fetch`
 * runs for each row in `first` order with at most `limit` in flight; results come back in `first`
 * order (an undefined result, a stage no longer listed, is dropped). The first failure stops new
 * fetches; once the in-flight ones settle, the failure of the lowest failing row is thrown (the
 * row the serial loop would have stopped at among those attempted). An aborted `signal` stops new
 * fetches; once the in-flight ones settle, a read left incomplete throws
 * BufferedFenced("request_aborted") (fail closed: never a partial session).
 */
async function fetchStagesBounded<T>(
  first: BufferedStageRow[], limit: number, signal: AbortSignal | undefined, fetch: (row: BufferedStageRow) => Promise<T | undefined>,
): Promise<T[]> {
  const results: Array<{ value: T | undefined } | undefined> = new Array(first.length);
  let next = 0;
  let failedAt = -1;
  let failure: unknown;
  const lane = async () => {
    for (;;) {
      if (failedAt !== -1 || signal?.aborted) return;
      const i = next++;
      if (i >= first.length) return;
      try {
        results[i] = { value: await fetch(first[i]!) };
      } catch (error) {
        if (failedAt === -1 || i < failedAt) {
          failedAt = i;
          failure = error;
        }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, first.length) }, lane));
  if (failedAt !== -1) throw failure;
  if (next < first.length || results.some((r) => r === undefined)) throw new BufferedFenced("request_aborted");
  return results.flatMap((r) => (r!.value === undefined ? [] : [r!.value]));
}

/** Before any GetObject: each stage and the session's sum of compressed sizes within bounds. */
function assertCompressedBounds(rows: BufferedStageRow[]): void {
  let total = 0;
  for (const r of rows) {
    if (!Number.isSafeInteger(r.stage_bytes) || r.stage_bytes > BUFFERED_STAGE_MAX_BYTES) throw new BufferedTooLarge();
    total += r.stage_bytes;
  }
  if (total > BUFFERED_SESSION_MAX_BYTES) throw new BufferedTooLarge();
}

/** The final observation: after every S3 read and RPC, before any byte; must equal the first. */
async function finalGate(deps: OwnerBufferedDeps, scope: BufferedScope, first: AuthorityHead, phases?: OwnerBufferedPhaseTimer): Promise<AuthorityHead> {
  return timed(phases, "control_head_2", async () => {
    const last = await deps.gate.observe(scope);
    if (!sameAuthority(first, last)) throw new BufferedFenced("gate_moved");
    return last;
  });
}

export type OwnerBufferedTurns =
  | null /** not a visible buffered session of this caller: the route's existing answer */
  /**
   * The authority (or the RPC's own fence) refused before any buffered candidate was found:
   * nothing is known about this session, so the route answers as without the flag (501).
   */
  | UnverifiedBeforeCandidate
  | { erased: true; rows: [] }
  | { erased: false; rows: Record<string, unknown>[] };

/**
 * GATED. The caller's buffered turn rows for one session, scrubbed: observe, stages, S3 fetch,
 * recheck, observe again (fenced unless identical), then every kept stage's admission against
 * the final head. A fence before the first stages() answer is `unverifiedBeforeCandidate`
 * (the route's existing answer); a fence once a candidate exists throws BufferedFenced (503).
 * No stage, a kept set that moved during the read, or one hidden stage: null. Over the byte
 * bounds: BufferedTooLarge (the route answers as hidden). Revisions and tombstones across
 * stages fold through normalizeRelayhistoryRecords. Throws TenantMismatchError on a foreign stage.
 */
export async function readOwnerBufferedTurns(deps: OwnerBufferedDeps, scope: BufferedScope, source: string, sessionId: string): Promise<OwnerBufferedTurns> {
  const read = await readGatedSession(deps, scope, source, sessionId);
  if (read === null || "unverifiedBeforeCandidate" in read) return read;
  if (read.erased) return { erased: true, rows: [] };
  return { erased: false, rows: bufferedTurnRows(scope, source, sessionId, read.records) };
}

type GatedSession =
  | null
  | UnverifiedBeforeCandidate
  | { erased: true }
  | {
      erased: false;
      /** Every kept stage's records folded together (revisions and tombstones across stages). */
      records: RelayhistoryStreamRecord[];
      /** The stages' origins in stage order (the first is the canonical parts[0] equivalent). */
      origins: string[];
      /** The visible stages' admission version (all equal the final head's). */
      admissionVersion: string;
    };

/**
 * GATED, module-private. The one gated read behind readOwnerBufferedTurns and
 * readOwnerBufferedEvents: observe, stages, S3 fetch, recheck, observe again (fenced unless
 * identical; this is the last I/O), then every kept stage's admission against the final head.
 * Its result is plain records; callers only compute from them.
 */
async function readGatedSession(deps: OwnerBufferedDeps, scope: BufferedScope, source: string, sessionId: string, opts: GatedReadOptions = {}): Promise<GatedSession> {
  const { phases } = opts;
  let first: AuthorityHead;
  let initial: BufferedStageRow[];
  try {
    // READ_API_OWNER_BUFFERED_INDEX_VERIFY=on: the out-of-band index first (cached per isolate).
    if (deps.indexCheck) await deps.indexCheck.ensure(scope);
    first = await timed(phases, "control_head_1", () => deps.gate.observe(scope));
    initial = await timed(phases, "stages_sql", () => deps.reads.stages(scope, first, source, sessionId));
  } catch (error) {
    if (error instanceof BufferedFenced) return unverified(error);
    throw error;
  }
  if (initial.length === 0) return null;
  const raw = await readStagesUngated(deps, scope, first, source, sessionId, initial, opts);
  const authority = await finalGate(deps, scope, first, phases);
  if (raw.kind === "erased") return { erased: true };
  if (raw.kind === "changed") return null;
  return timedSync(phases, "fold", () => {
    if (!raw.stages.every(({ row }) => admissionVisible(authority, row))) return null;
    const origins = [...new Set(raw.stages.flatMap((s) => s.records.map((r) => r.origin_id)))];
    const records = normalizeRelayhistoryRecords(raw.stages.flatMap((s) => s.records));
    return { erased: false as const, records, origins, admissionVersion: raw.stages[0]?.row.admission_version ?? "0" };
  });
}

export type OwnerBufferedEvents =
  | null /** not a visible buffered session of this caller: the route's existing answer */
  /** The authority refused before any buffered candidate was found: the route's existing answer. */
  | UnverifiedBeforeCandidate
  | { erased: true; rows: [] }
  | { erased: false; rows: EventRow[] };

/** The buffered stages project to no event row, or the projection refused them: hidden. */
export class BufferedProjectionFailed extends Error {
  constructor(readonly reason: string) {
    super("buffered projection failed");
    this.name = "BufferedProjectionFailed";
  }
}

/**
 * GATED. The caller's buffered event rows for one session (all of them; the route pages them with
 * page.ts readBufferedEventPage): readGatedSession, then canonical-projection.ts projectEventRows,
 * the projection readCanonicalScope runs, grouped the way the publication would be: direct
 * convergence_event lines per origin (machine liveMachineId(workspace, origin, user), the
 * manifest machine staging/publication derives), activity records together under the first
 * stage's origin (canonical parts[0]). Records were folded across stages first. Version: the
 * stages' admission version (the final head's). Identity comes only from the gated scope: every
 * row's org, workspace, user, session and source must be the caller's and this session's, and its
 * machine one of this session's origins' machines, else TenantMismatchError. Text fields are
 * scrubbed (scrubBufferedText; task_ref through scrubJson) before return. Rows carry no stage
 * key, ordinal or raw line. A version beyond Number's safe range is clamped for the (unemitted)
 * index-row version. A projection failure: BufferedProjectionFailed (the route hides the session).
 */
export async function readOwnerBufferedEvents(
  deps: OwnerBufferedDeps, scope: BufferedScope, source: string, sessionId: string, opts: OwnerBufferedReadOptions = {},
): Promise<OwnerBufferedEvents> {
  // READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY applies to this read only (turns stay serial).
  const read = await readGatedSession(deps, scope, source, sessionId, { ...opts, stageConcurrency: deps.stageConcurrency });
  if (read === null || "unverifiedBeforeCandidate" in read) return read;
  if (read.erased) return { erased: true, rows: [] };
  // The projection's index-row version is the admission version. It never reaches an EventRow
  // (the response has no version field), so a version past Number's safe range, still exact and
  // compared as BigInt by the gate above, is clamped here rather than hiding a visible session.
  const exact = Number(read.admissionVersion);
  const version = Number.isSafeInteger(exact) ? exact : Number.MAX_SAFE_INTEGER;
  return { erased: false, rows: timedSync(opts.phases, "fold", () => bufferedEventRows(scope, source, sessionId, read.records, read.origins, version)) };
}

type ProjectedIndexRow = Extract<ReturnType<typeof projectEventRows>, { events: unknown }>["events"][number]["indexRow"];

/** The read API's event row of a projected index row (the shape ClickHouse and the portable index return). */
export function eventRowFromIndex(row: ProjectedIndexRow): EventRow {
  return {
    org_id: row.org_id, workspace_id: row.workspace_id, ts_ms: Date.parse(`${row.ts.replace(" ", "T")}Z`), session_id: row.session_id,
    source: row.source, machine_id: row.machine_id, kind: row.kind, event_id: row.event_id, type: row.type, user_id: row.user_id,
    actor_name: row.actor_name, actor_role: row.actor_role, subagent_id: row.subagent_id, project_id: row.project_id,
    task_title: row.task_title, task_ref: row.task_ref, tags: [...row.tags], files_touched: [...row.files_touched],
    tool_name: row.tool_name, tool_status: row.tool_status, content: row.content, in_budget: 1,
  };
}

function bufferedEventRows(scope: BufferedScope, source: string, sessionId: string, records: RelayhistoryStreamRecord[], origins: string[], version: number): EventRow[] {
  const ws = scope.workspaceId;
  const machines = new Set(origins.flatMap((o) => [liveMachineId(ws, o, scope.userId), deliveryMachine(ws, o)]));
  const direct = new Map<string, RelayhistoryStreamRecord[]>();
  const activity: RelayhistoryStreamRecord[] = [];
  for (const r of records) {
    if (r.kind === CONVERGENCE_EVENT_KIND) direct.set(r.origin_id, [...(direct.get(r.origin_id) ?? []), r]);
    else activity.push(r);
  }
  const groups = [...direct].map(([origin, recs]) => ({ origin, records: recs }));
  if (activity.length > 0 && origins[0] !== undefined) groups.push({ origin: origins[0], records: activity });
  const projectionScope = { orgId: scope.orgId, workspaceId: ws, harness: source, sessionId };
  // Keyed as readCanonicalScope keys its rows: the last projection of an event wins.
  const byKey = new Map<string, EventRow>();
  for (const group of groups) {
    const projected = projectEventRows(group.records, projectionScope, { sessionId, machineId: liveMachineId(ws, group.origin, scope.userId), userId: scope.userId }, version);
    if ("error" in projected) throw new BufferedProjectionFailed((projected.error as { code?: unknown })?.code === undefined ? "projection_failed" : String((projected.error as { code: unknown }).code));
    for (const { indexRow } of projected.events) {
      if (indexRow.org_id !== scope.orgId || indexRow.workspace_id !== ws || indexRow.user_id !== scope.userId ||
          indexRow.session_id !== sessionId || indexRow.source !== source || !machines.has(indexRow.machine_id)) throw new TenantMismatchError();
      byKey.set(JSON.stringify([indexRow.machine_id, indexRow.source, indexRow.kind, indexRow.event_id]), scrubEventRow(eventRowFromIndex(indexRow)));
    }
  }
  return [...byKey.values()];
}

/**
 * GATED. One page of the caller's buffered sessions: observe, list, the caller's canonical
 * presence await (`presence`, e.g. live pointers), then observe again (fenced unless
 * identical). The final observation runs after every await of the read, so nothing the caller
 * emits from the result waits on I/O after it. Rows whose admission fails the final head are
 * returned with `visible: false`: they advance the buffered position but are never shown.
 * `live` is the presence answer for a visible row (canonical wins). Presence is asked only for
 * rows admitted under the first observation; admission depends only on the observation's
 * (cutoverEpoch, head), which the final gate requires to be identical.
 */
export async function readOwnerBufferedList(
  deps: OwnerBufferedDeps,
  scope: BufferedScope,
  args: { cursor?: BufferedListPosition; limit: number },
  presence?: (sessionIds: string[]) => Promise<ReadonlySet<string>>,
): Promise<Array<BufferedSessionRow & { visible: boolean; live: boolean }>> {
  // READ_API_OWNER_BUFFERED_INDEX_VERIFY=on: the out-of-band index first (cached per isolate).
  if (deps.indexCheck) await deps.indexCheck.ensure(scope);
  const first = await deps.gate.observe(scope);
  const rows = await deps.reads.list(scope, first, args);
  const candidates = [...new Set(rows.filter((r) => admissionVisible(first, r)).map((r) => r.session_id))];
  const live: ReadonlySet<string> = presence && candidates.length ? await presence(candidates) : new Set<string>();
  // Final gate: after the RPC and the presence await, immediately before the caller emits.
  const authority = await finalGate(deps, scope, first);
  return rows.map((r) => ({ ...r, visible: admissionVisible(authority, r), live: live.has(r.session_id) }));
}

/** Turn rows of a buffered session (the canonical live-turn projection, one harness), scrubbed. */
function bufferedTurnRows(scope: BufferedScope, source: string, sessionId: string, records: RelayhistoryStreamRecord[]): Record<string, unknown>[] {
  const rows = canonicalLiveTurns(records, { orgId: scope.orgId, workspaceId: scope.workspaceId, source, sessionId });
  return turnRows({ orgId: scope.orgId, workspaceId: scope.workspaceId, sessionId }, [{ harness: source, snapshot: { rows, legacyCoverage: { version: 1, tables: [] } } }], source).map(scrubTurnRow);
}

// ---------------------------------------------------------------------------------------------
// Read-time scrub
// ---------------------------------------------------------------------------------------------

const REDACTED = "[REDACTED]";
/** scrub.ts's quoted key (any escaping depth), then the separator, then a quoted `[REDACTED]`. */
const REDACTED_QUOTED_VALUE =
  /(?<!\\)(\\{0,16})(["'`])([A-Za-z0-9_. -]{1,64})\1\2[ \t]{0,16}(?:=>|[:=])[ \t]{0,16}(\\{0,16})(["'`])\[REDACTED\]/g;

/**
 * scrubText plus the v4 rule for over-long and unclosed quoted secret values.
 *
 * TODO(relayhistory-scrub v4): scrub.ts still has the v3 quoted-value rule: a value under a
 * secret key with no closing quote within MAX_QUOTED_VALUE (4096) units, or before the end of
 * its line, is redacted only up to the cap or the next whitespace, so its suffix survives.
 * v4 redacts the whole remainder. Until v4 lands in scrub.ts (owned elsewhere; not edited
 * here), every `"<secret key>": "[REDACTED]` that scrubText leaves without its closing quote
 * right after the marker has the rest of the text replaced: `"<key>": "[REDACTED]` ends the
 * string. Drop this wrapper once scrubText implements v4.
 */
export function scrubBufferedText(value: string): string {
  return redactQuotedTail(scrubText(value));
}

/** The post-pass of scrubBufferedText over text scrubText already processed. */
function redactQuotedTail(text: string): string {
  if (!text.includes(REDACTED)) return text;
  REDACTED_QUOTED_VALUE.lastIndex = 0;
  for (let m = REDACTED_QUOTED_VALUE.exec(text); m; m = REDACTED_QUOTED_VALUE.exec(text)) {
    if (!isSecretKeyName((m[3] as string).trim())) continue;
    const after = m.index + m[0].length;
    // Closed right after the marker (same backslash run and quote as the opening): complete.
    if (text.startsWith(`${m[4]}${m[5]}`, after)) continue;
    return text.slice(0, after);
  }
  return text;
}

/** redactQuotedTail over every string of a value scrubJson already processed. */
function scrubDeep(value: unknown): unknown {
  if (typeof value === "string") return redactQuotedTail(value);
  if (Array.isArray(value)) return value.map(scrubDeep);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, scrubDeep(v)]));
  return value;
}

/** A buffered turn row with its free text scrubbed: content, actor name, every metadata string. */
function scrubTurnRow(row: Record<string, unknown>): Record<string, unknown> {
  return {
    ...row,
    content: typeof row.content === "string" ? scrubBufferedText(row.content) : row.content,
    actor_name: typeof row.actor_name === "string" ? scrubBufferedText(row.actor_name) : row.actor_name,
    metadata: row.metadata && typeof row.metadata === "object" ? scrubDeep(scrubJson(row.metadata)) : row.metadata,
  };
}

/** A buffered event row with its free text scrubbed: content, actor name, task title, task_ref, tags, files touched. */
function scrubEventRow(row: EventRow): EventRow {
  let taskRef = row.task_ref;
  try {
    const parsed = JSON.parse(taskRef) as unknown;
    taskRef = JSON.stringify(scrubDeep(scrubJson(parsed)));
  } catch {
    taskRef = scrubBufferedText(taskRef);
  }
  return {
    ...row,
    content: scrubBufferedText(row.content),
    actor_name: scrubBufferedText(row.actor_name),
    task_title: scrubBufferedText(row.task_title),
    task_ref: taskRef,
    tags: row.tags.map(scrubBufferedText),
    files_touched: row.files_touched.map(scrubBufferedText),
  };
}
