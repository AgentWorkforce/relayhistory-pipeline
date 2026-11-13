/**
 * The upload consent store over SQL (supabase/migrations/20261105057010_upload_consent.sql), as the
 * `bulls96_session_ingest` login. Failures carry a code only (`ledgerErrorCode`), never a driver or
 * database message. A result that does not have the promised shape is `upload_consent_bad_result`, and
 * the caller answers 503: the batch is held, never admitted.
 */
import type { LedgerTenant } from "./ledger.ts";
import { LedgerUnavailableError } from "./ledger.ts";
import { ledgerErrorCode, type SqlRun } from "./ledger-sql.ts";
import {
  ConsentWriteRefused,
  sessionKey,
  validateConsentWrite,
  type ConsentSnapshot,
  type ConsentWrite,
  type SessionConsent,
  type UploadConsentMode,
  type UploadConsentStore,
} from "./upload-consent.ts";

const MODES = new Set<UploadConsentMode>(["selected", "new", "all"]);
const bad = () => new LedgerUnavailableError("upload_consent_bad_result");
const int = (v: unknown): number | undefined => (typeof v === "number" && Number.isSafeInteger(v) ? v : typeof v === "string" && /^-?[0-9]{1,16}$/.test(v) ? Number(v) : undefined);

async function call(run: SqlRun, text: string, params: unknown[]): Promise<unknown> {
  let rows;
  try {
    ({ rows } = await run(text, params));
  } catch (error) {
    throw new LedgerUnavailableError(ledgerErrorCode(error));
  }
  const value = rows[0]?.result;
  return typeof value === "string" ? JSON.parse(value) : value;
}

/** Strict parse of the snapshot function's answer; anything unexpected refuses (503). */
export function parseConsentSnapshot(value: unknown): ConsentSnapshot {
  if (!value || typeof value !== "object") throw bad();
  const v = value as Record<string, unknown>;
  if (typeof v.held !== "boolean" || typeof v.origin_confirmed !== "boolean" || !Array.isArray(v.sessions)) throw bad();
  let policy: ConsentSnapshot["policy"] = null;
  if (v.policy !== null) {
    const p = v.policy as Record<string, unknown> | undefined;
    const revision = int(p?.revision);
    const boundary = p?.new_session_boundary_ms === null ? null : int(p?.new_session_boundary_ms);
    if (!p || !MODES.has(p.mode as UploadConsentMode) || revision === undefined || boundary === undefined) throw bad();
    policy = { mode: p.mode as UploadConsentMode, revision, newSessionBoundaryMs: boundary };
  }
  const sessions = new Map<string, SessionConsent>();
  for (const raw of v.sessions as unknown[]) {
    const s = raw as Record<string, unknown>;
    if (!s || typeof s.source !== "string" || typeof s.session_id !== "string") throw bad();
    const firstSeenMs = int(s.first_seen_ms);
    if (firstSeenMs === undefined) throw bad();
    const row: SessionConsent = { firstSeenMs };
    if (s.decision === "allow" || s.decision === "deny") row.decision = s.decision;
    else if (s.decision !== null && s.decision !== undefined) throw bad();
    const activity = s.first_activity_ms === null || s.first_activity_ms === undefined ? undefined : int(s.first_activity_ms);
    if (activity !== undefined) row.firstActivityMs = activity;
    sessions.set(sessionKey(s.source, s.session_id), row);
  }
  return { held: v.held, policy, originConfirmed: v.origin_confirmed, sessions };
}

export function sqlUploadConsentStore(run: SqlRun): UploadConsentStore {
  return {
    async snapshot(tenant: LedgerTenant, originId, sessions, nowMs) {
      const named = sessions.map((s) => ({ source: s.source, session_id: s.sessionId, ...(s.firstActivityMs !== undefined ? { first_activity_ms: s.firstActivityMs } : {}) }));
      return parseConsentSnapshot(await call(run, "SELECT sessions.upload_consent_snapshot_v1($1,$2,$3,$4,$5::jsonb,$6) AS result",
        [tenant.orgId, tenant.workspaceId, tenant.userId, originId, JSON.stringify(named), nowMs]));
    },
    async apply(tenant: LedgerTenant, write: ConsentWrite, options) {
      validateConsentWrite(write);
      const wire = {
        ...(write.mode ? { mode: write.mode } : {}),
        ...(write.allow ? { allow: write.allow.map((s) => ({ source: s.source, session_id: s.sessionId })) } : {}),
        ...(write.deny ? { deny: write.deny.map((s) => ({ source: s.source, session_id: s.sessionId })) } : {}),
        ...(write.confirmOrigins ? { confirm_origins: write.confirmOrigins } : {}),
        ...(write.releaseHold ? { release_hold: true } : {}),
        ...(write.expectedRevision !== undefined ? { expected_revision: write.expectedRevision } : {}),
      };
      const r = (await call(run, "SELECT sessions.upload_consent_apply_v1($1,$2,$3,$4::jsonb,$5,$6) AS result",
        [tenant.orgId, tenant.workspaceId, tenant.userId, JSON.stringify(wire), options.trusted, options.nowMs])) as Record<string, unknown> | null;
      if (r?.ok === true) {
        const revision = int(r.revision);
        if (revision === undefined) throw bad();
        return { revision };
      }
      if (r?.code === "untrusted") throw new ConsentWriteRefused("upload_consent_untrusted", 403, "Only a confirmed sharing choice can widen uploads");
      if (r?.code === "revision") throw new ConsentWriteRefused("upload_consent_revision", 409, "The sharing choice changed; read it again");
      if (r?.code === "invalid") throw new ConsentWriteRefused("upload_consent_invalid", 400, "Invalid upload consent");
      throw bad();
    },
  };
}
