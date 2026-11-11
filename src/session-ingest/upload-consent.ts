/**
 * Server-owned upload consent (docs/upload-consent.md).
 *
 * Desktop clients that shipped before the server knew anything about their sharing mode can upload
 * every session even when the person chose "Selected". The batch wire carries no sharing mode, so the
 * absence of a disable is not consent. This module is the receiver's own answer. It reads a policy
 * keyed by the VERIFIED principal (org, workspace, user), never by anything in the body, and returns
 * one decision per batch:
 *
 *   - An active containment hold on the principal or its org: HOLD.
 *   - No policy: HOLD. A missing policy never falls back to "all".
 *   - An origin that has not been confirmed under that principal: HOLD. A rotated or fresh origin_id
 *     cannot pick up a new or empty policy.
 *   - An explicit deny: DENY, in every mode. Changing to "all" keeps every "Don't upload".
 *   - selected: only sessions with an explicit allow.
 *   - new: an explicit allow, OR the server first saw the session at or after the boundary AND the
 *     client's `first_activity_ms` is known and at or after it. Anything else is ambiguous: HOLD.
 *   - all: everything except explicit denies.
 *   - Records with no session ride along only in a batch where every session is allowed.
 *
 * Any HOLD or DENY refuses the WHOLE batch with 503 `upload_consent_required` and a long Retry-After.
 * Nothing is claimed, written or receipted. Every released Desktop treats 503 as transient, so it keeps
 * the batch and retries it. A mixed batch therefore never persists denied content and never advances a
 * receipt.
 *
 * Trust: only `applyUploadConsent` with `trusted: true` can broaden (add an allow, widen the mode,
 * confirm an origin, release a hold). The route grants that only to the `rth:consent` scope on a
 * signed identity. A sync credential, a batch field, a client version header or a replay can only narrow.
 *
 * Logging: counts, the outcome class and the policy revision. Never a session id, record id or payload.
 */
import { DeliveryError, type HistoryExportRecord } from "./vendor/delivery/delivery-contracts.ts";
import type { LedgerTenant } from "./ledger.ts";

export type UploadConsentMode = "selected" | "new" | "all";
/**
 * INGEST_UPLOAD_CONSENT.
 *   off       nothing is read (the default until an owner turns it on);
 *   report    decide and log `upload_consent_refused`, accept anyway;
 *   hold-only refuse only principals under a containment hold, report the rest;
 *   enforce   refuse every HOLD and DENY.
 */
export type UploadConsentEnforcement = "off" | "report" | "hold-only" | "enforce";
export const UPLOAD_CONSENT_VAR = "INGEST_UPLOAD_CONSENT";
export const UPLOAD_CONSENT_REQUIRED = "upload_consent_required";
/** Long on purpose: a held Desktop should not hammer the edge while the person has not chosen yet. */
export const UPLOAD_CONSENT_RETRY_AFTER_SECONDS = 3600;
export const CONSENT_SCOPE = "rth:consent";

export function uploadConsentEnforcement(raw: string | undefined): UploadConsentEnforcement {
  const v = (raw ?? "").trim().toLowerCase();
  if (v === "") return "off";
  if (v === "off" || v === "report" || v === "hold-only" || v === "enforce") return v;
  throw new Error(`${UPLOAD_CONSENT_VAR} must be off, report, hold-only or enforce`);
}

export interface ConsentPolicy {
  mode: UploadConsentMode;
  revision: number;
  /** Server clock at the trusted write that chose "new". */
  newSessionBoundaryMs: number | null;
}

export interface SessionConsent {
  decision?: "allow" | "deny";
  /** Server clock when this principal's upload first named the session. */
  firstSeenMs?: number;
  /** The earliest `first_activity_ms` any of this principal's `session` records claimed. */
  firstActivityMs?: number;
}

export interface ConsentSnapshot {
  held: boolean;
  policy: ConsentPolicy | null;
  originConfirmed: boolean;
  /** Keyed by `sessionKey(source, sessionId)`. Missing means nothing is known about the session. */
  sessions: Map<string, SessionConsent>;
}

export type ConsentOutcome = "allowed" | "held" | "no_policy" | "origin_unconfirmed" | "denied" | "not_selected" | "age_unknown" | "consent_store_required";

/**
 * The organization operator's capture policy (relay-control `capture_policy.sharing_mode`, self-host),
 * as a CAP on the person's own choice: the stricter mode wins, and under "new" the later boundary wins.
 * The operator can only narrow; it never grants an allow.
 */
export interface OperatorSharingCap {
  mode: Exclude<UploadConsentMode, "all">;
  sinceMs?: number;
}

const CAP_RANK: Record<UploadConsentMode, number> = { selected: 0, new: 1, all: 2 };
/** The person's policy under the operator's cap. */
export function cappedPolicy(policy: ConsentPolicy | null, cap: OperatorSharingCap | undefined): ConsentPolicy | null {
  if (!policy || !cap) return policy;
  if (CAP_RANK[policy.mode] < CAP_RANK[cap.mode]) return policy;
  if (cap.mode === "selected") return { ...policy, mode: "selected", newSessionBoundaryMs: null };
  // cap.mode === "new": the later of the two boundaries; an unknown operator boundary holds every age.
  const boundaries = [policy.mode === "new" ? policy.newSessionBoundaryMs : null, cap.sinceMs ?? Number.MAX_SAFE_INTEGER].filter((v): v is number => typeof v === "number");
  return { ...policy, mode: "new", newSessionBoundaryMs: Math.max(...boundaries) };
}

export interface ConsentDecision {
  outcome: ConsentOutcome;
  /** True when the batch must be refused under `enforce`. */
  refuse: boolean;
  revision: number | null;
  mode: UploadConsentMode | null;
  /** Counts only. */
  sessions: number;
  refusedSessions: number;
}

export const sessionKey = (source: string, sessionId: string) => JSON.stringify([source, sessionId]);

/** The distinct (source, session) pairs a batch names, and each one's earliest claimed first activity. */
export function batchSessions(records: readonly Pick<HistoryExportRecord, "kind" | "operation" | "source" | "session_id" | "payload">[]) {
  const out = new Map<string, { source: string; sessionId: string; firstActivityMs?: number }>();
  for (const r of records) {
    if (!r.session_id) continue;
    const key = sessionKey(r.source, r.session_id);
    const entry = out.get(key) ?? { source: r.source, sessionId: r.session_id };
    const started = r.kind === "session" && r.operation === "upsert" ? r.payload?.first_activity_ms : undefined;
    if (typeof started === "number" && Number.isFinite(started) && started >= 0)
      entry.firstActivityMs = entry.firstActivityMs === undefined ? started : Math.min(entry.firstActivityMs, started);
    out.set(key, entry);
  }
  return out;
}

/**
 * Pure: the decision for one batch against one snapshot. Order of checks is the order of precedence.
 *
 * `dropped`: sessions the ledger's deletion fence (`upload_deletions`) will acknowledge without storing
 * (the claim's `deleted`). They store nothing, so they never refuse a batch: that is the long-standing
 * "Don't upload" contract, and refusing them would stall a legacy client's queue forever.
 * `deferDenied`: the pre-claim check, which cannot see the fence yet, leaves explicit denies to the
 * post-claim check instead of refusing on them.
 */
export function decideUploadConsent(snapshot: ConsentSnapshot, records: Parameters<typeof batchSessions>[0], options: { dropped?: ReadonlySet<string>; deferDenied?: boolean; operator?: OperatorSharingCap } = {}): ConsentDecision {
  if (options.operator) snapshot = { ...snapshot, policy: cappedPolicy(snapshot.policy, options.operator) };
  const named = batchSessions(records);
  for (const key of options.dropped ?? []) named.delete(key);
  const base = { revision: snapshot.policy?.revision ?? null, mode: snapshot.policy?.mode ?? null, sessions: named.size };
  const whole = (outcome: ConsentOutcome): ConsentDecision => ({ ...base, outcome, refuse: true, refusedSessions: named.size });
  if (snapshot.held) return whole("held");
  const policy = snapshot.policy;
  if (!policy) return whole("no_policy");
  if (!snapshot.originConfirmed) return whole("origin_unconfirmed");
  // A batch of session-less records only (settings, catalogs): allowed under any confirmed policy.
  let worst: ConsentOutcome = "allowed";
  let refused = 0;
  const rank: Record<ConsentOutcome, number> = { allowed: 0, age_unknown: 1, not_selected: 2, denied: 3, origin_unconfirmed: 4, no_policy: 5, consent_store_required: 6, held: 7 };
  for (const [key, claimed] of named) {
    const known = snapshot.sessions.get(key) ?? {};
    let outcome: ConsentOutcome;
    if (known.decision === "deny") outcome = options.deferDenied ? "allowed" : "denied";
    else if (known.decision === "allow") outcome = "allowed";
    else if (policy.mode === "all") outcome = "allowed";
    else if (policy.mode === "selected") outcome = "not_selected";
    else {
      const boundary = policy.newSessionBoundaryMs;
      const activity = [known.firstActivityMs, claimed.firstActivityMs].filter((v): v is number => v !== undefined);
      const firstActivity = activity.length ? Math.min(...activity) : undefined;
      outcome = boundary !== null && known.firstSeenMs !== undefined && known.firstSeenMs >= boundary && firstActivity !== undefined && firstActivity >= boundary
        ? "allowed"
        : "age_unknown";
    }
    if (outcome !== "allowed") refused++;
    if (rank[outcome] > rank[worst]) worst = outcome;
  }
  return { ...base, outcome: worst, refuse: worst !== "allowed", refusedSessions: refused };
}

/** Whether the configured enforcement refuses this decision. */
export function consentRefuses(enforcement: UploadConsentEnforcement, decision: ConsentDecision): boolean {
  if (!decision.refuse) return false;
  if (enforcement === "enforce") return true;
  return enforcement === "hold-only" && decision.outcome === "held";
}

/** The refusal: 503 so every released Desktop keeps the batch and retries it (no ACK, no receipt). */
export class UploadConsentRequired extends DeliveryError {
  readonly retryAfterSeconds = UPLOAD_CONSENT_RETRY_AFTER_SECONDS;
  constructor(readonly outcome: ConsentOutcome) {
    super(UPLOAD_CONSENT_REQUIRED, 503, "Uploads from this installation are paused until its sharing choice is confirmed");
  }
}

// ---- store ----

export interface ConsentWrite {
  mode?: UploadConsentMode;
  /** Sessions to allow (broadening) or deny (narrowing). A session in both is refused as invalid. */
  allow?: { source: string; sessionId: string }[];
  deny?: { source: string; sessionId: string }[];
  /** Origins to confirm under the principal (broadening). */
  confirmOrigins?: string[];
  /** Release this principal's containment holds (broadening). */
  releaseHold?: boolean;
  /** Optimistic concurrency: the revision the writer last read. Mismatch refuses. */
  expectedRevision?: number;
}

export interface UploadConsentStore {
  /**
   * One snapshot for the batch. Records first-seen times and claimed first activity for the named
   * sessions as a side effect (no content). `nowMs` is the server clock.
   */
  snapshot(tenant: LedgerTenant, originId: string, sessions: { source: string; sessionId: string; firstActivityMs?: number }[], nowMs: number): Promise<ConsentSnapshot>;
  /** Applies a write in one transaction and returns the new revision. */
  apply(tenant: LedgerTenant, write: ConsentWrite, options: { trusted: boolean; nowMs: number }): Promise<{ revision: number }>;
}

const MODE_RANK: Record<UploadConsentMode, number> = { selected: 0, new: 1, all: 2 };

/** Whether a write widens what may be uploaded, given the current policy. Only trusted callers may. */
export function writeBroadens(current: ConsentPolicy | null, write: ConsentWrite): boolean {
  if ((write.allow?.length ?? 0) > 0 || (write.confirmOrigins?.length ?? 0) > 0 || write.releaseHold) return true;
  if (write.mode === undefined) return false;
  // Creating a policy is broadening unless it is the strictest mode.
  if (!current) return write.mode !== "selected";
  return MODE_RANK[write.mode] > MODE_RANK[current.mode];
}

export class ConsentWriteRefused extends DeliveryError {
  constructor(code: "upload_consent_untrusted" | "upload_consent_invalid" | "upload_consent_revision", status: 400 | 403 | 409, message: string) {
    super(code, status, message);
  }
}

export function validateConsentWrite(write: ConsentWrite): void {
  const ids = (list: { source: string; sessionId: string }[] | undefined) => (list ?? []).map((s) => sessionKey(s.source, s.sessionId));
  const allow = new Set(ids(write.allow));
  if (ids(write.deny).some((k) => allow.has(k))) throw new ConsentWriteRefused("upload_consent_invalid", 400, "A session cannot be both allowed and denied");
}

/** In-memory store with the same semantics as the SQL functions; tests and the self-host dry run. */
export class MemoryConsentStore implements UploadConsentStore {
  policies = new Map<string, ConsentPolicy>();
  decisions = new Map<string, Map<string, "allow" | "deny">>();
  seen = new Map<string, Map<string, SessionConsent>>();
  origins = new Map<string, Set<string>>();
  holds = new Set<string>();
  private principal = (t: LedgerTenant) => JSON.stringify([t.orgId, t.workspaceId, t.userId]);
  /** Operator containment: a hold on one principal, or on every principal of an org (`orgOnly`). */
  hold(t: LedgerTenant, orgOnly = false) {
    this.holds.add(orgOnly ? JSON.stringify([t.orgId]) : this.principal(t));
  }
  async snapshot(tenant: LedgerTenant, originId: string, sessions: { source: string; sessionId: string; firstActivityMs?: number }[], nowMs: number): Promise<ConsentSnapshot> {
    const p = this.principal(tenant);
    const seen = this.seen.get(p) ?? new Map<string, SessionConsent>();
    this.seen.set(p, seen);
    const decisions = this.decisions.get(p);
    const out = new Map<string, SessionConsent>();
    for (const s of sessions) {
      const key = sessionKey(s.source, s.sessionId);
      const prior = seen.get(key);
      const firstActivityMs = [prior?.firstActivityMs, s.firstActivityMs].filter((v): v is number => v !== undefined);
      const row: SessionConsent = { firstSeenMs: prior?.firstSeenMs ?? nowMs, ...(firstActivityMs.length ? { firstActivityMs: Math.min(...firstActivityMs) } : {}) };
      seen.set(key, row);
      const decision = decisions?.get(key);
      // The snapshot answers what was known BEFORE this batch: a session first seen now is first seen now.
      out.set(key, { ...row, ...(decision ? { decision } : {}) });
    }
    return {
      held: this.holds.has(p) || this.holds.has(JSON.stringify([tenant.orgId])),
      policy: this.policies.get(p) ?? null,
      originConfirmed: this.origins.get(p)?.has(originId) ?? false,
      sessions: out,
    };
  }
  async apply(tenant: LedgerTenant, write: ConsentWrite, options: { trusted: boolean; nowMs: number }): Promise<{ revision: number }> {
    validateConsentWrite(write);
    const p = this.principal(tenant);
    const current = this.policies.get(p) ?? null;
    if (write.expectedRevision !== undefined && write.expectedRevision !== (current?.revision ?? 0))
      throw new ConsentWriteRefused("upload_consent_revision", 409, "The sharing choice changed; read it again");
    if (!options.trusted && writeBroadens(current, write))
      throw new ConsentWriteRefused("upload_consent_untrusted", 403, "Only a confirmed sharing choice can widen uploads");
    const revision = (current?.revision ?? 0) + 1;
    const mode = write.mode ?? current?.mode ?? "selected";
    const boundary = mode === "new" ? (current?.mode === "new" ? current.newSessionBoundaryMs : options.nowMs) : null;
    this.policies.set(p, { mode, revision, newSessionBoundaryMs: boundary });
    const decisions = this.decisions.get(p) ?? new Map<string, "allow" | "deny">();
    this.decisions.set(p, decisions);
    for (const s of write.allow ?? []) decisions.set(sessionKey(s.source, s.sessionId), "allow");
    for (const s of write.deny ?? []) decisions.set(sessionKey(s.source, s.sessionId), "deny");
    const origins = this.origins.get(p) ?? new Set<string>();
    this.origins.set(p, origins);
    for (const o of write.confirmOrigins ?? []) origins.add(o);
    if (write.releaseHold) this.holds.delete(p);
    return { revision };
  }
}

// ---- the check every admission path runs ----

export interface UploadConsentConfig {
  enforcement: UploadConsentEnforcement;
  store: UploadConsentStore;
  nowMs?: () => number;
}

export const UPLOAD_CONSENT_METRIC = "upload_consent_refused";

/**
 * Reads the snapshot, decides, logs the operator signal, and throws `UploadConsentRequired` when the
 * enforcement refuses. The log line carries the verified org and workspace, counts, the outcome class,
 * the mode and revision: never a user, origin, session or record id, and never content.
 */
export async function checkUploadConsent(
  config: UploadConsentConfig | undefined,
  tenant: LedgerTenant,
  originId: string,
  records: Parameters<typeof batchSessions>[0],
  options: {
    dropped?: ReadonlySet<string>; deferDenied?: boolean; phase: "precheck" | "admit" | "commit"; route?: string;
    /**
     * The operator's enforced capture policy cap (capture-policy.ts `operatorSharingCap`). Present only
     * when RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS=enforce: then this check enforces whatever
     * INGEST_UPLOAD_CONSENT says, and with no consent store a capped batch holds, because nothing can
     * prove which sessions were selected or when they started.
     */
    operator?: OperatorSharingCap;
  },
  log: (fields: Record<string, unknown>) => void = () => {},
): Promise<ConsentDecision | null> {
  const enforcement: UploadConsentEnforcement = options.operator ? "enforce" : config?.enforcement ?? "off";
  if (enforcement === "off") return null;
  let decision: ConsentDecision;
  if (!config || config.enforcement === "off") {
    const named = batchSessions(records);
    decision = { outcome: "consent_store_required", refuse: named.size > 0, revision: null, mode: options.operator!.mode, sessions: named.size, refusedSessions: named.size };
  } else {
    const named = [...batchSessions(records).values()];
    let snapshot: ConsentSnapshot;
    try {
      snapshot = await config.store.snapshot(tenant, originId, named, (config.nowMs ?? Date.now)());
    } catch (error) {
      // Report never refuses: with the store unreadable (e.g. the migration not yet applied) it logs and
      // accepts. hold-only / enforce, or an operator cap, fail closed (the caller answers 503, no ACK).
      if (enforcement !== "report") throw error;
      log({ route: UPLOAD_CONSENT_METRIC, error_code: "upload_consent_store_unavailable", state: "reported", stage: `${options.route ?? "upload"}:${options.phase}`, org_id: tenant.orgId, workspace_id: tenant.workspaceId, outcome: enforcement });
      return null;
    }
    decision = decideUploadConsent(snapshot, records, options);
  }
  if (!decision.refuse) return decision;
  const enforced = consentRefuses(enforcement, decision);
  // Only src/shared/log.ts ALLOWED_LOG_FIELDS survive the logger, so the signal uses those names:
  // route = the signal, reason = the outcome class, state = refused|reported, stage = the check,
  // kind = the mode, count = refused sessions. Never a user, origin, session or record id.
  log({
    route: UPLOAD_CONSENT_METRIC,
    error_code: UPLOAD_CONSENT_REQUIRED,
    reason: decision.outcome,
    state: enforced ? "refused" : "reported",
    stage: `${options.route ?? "upload"}:${options.phase}`,
    org_id: tenant.orgId,
    workspace_id: tenant.workspaceId,
    kind: decision.mode ?? "none",
    revision: decision.revision ?? 0,
    count: decision.refusedSessions,
    outcome: enforcement,
  });
  if (enforced) throw new UploadConsentRequired(decision.outcome);
  return decision;
}
