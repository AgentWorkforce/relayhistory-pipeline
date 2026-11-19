/**
 * Session Intelligence, the workspace view (docs/session-intel-workspace-api.md): GET /v1/session-intel/workspace,
 * behind READ_API_SESSION_INTEL_ENABLED (exactly "true"). Contract `session-intel-workspace/1`.
 *
 * Reads, all as bulls96_read_api in the read pool's READ ONLY tenant transactions:
 * - alerts: sessions.session_intel_workspace_alerts_v1 (migration 20261105062170), the workspace's open advisories,
 *   one entry per advisory id;
 * - status and handoffs: the directory's existing read-API list (sessions.list_session_directory_for), two bounded
 *   pages by last activity;
 * - intents and decisions: sessions.session_intel_workspace_claims_v1 (062170), titles only.
 * A failed claims read never fails the view: status and handoffs are still served, with empty intents and
 * decisions, `hasClaims` null and the claims coverage null. No transcript text is read or returned: the directory
 * gives titles and the stored (scrubbed) short summary; advisories give ids, kinds, artifacts and timestamps.
 */
import type { AdvisoryEvidence } from "./coordination-advisories.ts";
import type { Directory, DirectoryListRow } from "./directory.ts";
import type { SqlQuery, TenantRunner } from "./liveness.ts";
import { TenantMismatchError } from "./page.ts";

export const SESSION_INTEL_CONTRACT = "session-intel-workspace/1";
export const SESSION_INTEL_ALERTS_MAX = 100;
export const SESSION_INTEL_STATUS_MAX = 200;
export const SESSION_INTEL_HANDOFFS_MAX = 100;
export const SESSION_INTEL_ACTIVE_WINDOW_MS = 2 * 60 * 60 * 1000;
export const SESSION_INTEL_HANDOFF_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
export const SESSION_INTEL_INTENTS_MAX = 3;
export const SESSION_INTEL_DECISIONS_MAX = 5;
export const SESSION_INTEL_TITLE_MAX = 120;
/** The directory's title and short-summary bounds; a longer stored value is cut, never failed. */
const SESSION_TITLE_MAX = 200;
const SHORT_SUMMARY_MAX = 1000;

export const SESSION_INTEL_ALERTS_SIGNATURE = "sessions.session_intel_workspace_alerts_v1(text,text,integer)";
export const SESSION_INTEL_CLAIMS_SIGNATURE = "sessions.session_intel_workspace_claims_v1(text,text,text[],integer)";

/** The fixed coverage notes. No id or value is ever interpolated into one. */
export const SESSION_INTEL_NOTES = {
  claimsUnavailable: "claims unavailable",
  alertsCapped: "alerts capped at 100",
  statusCapped: "status capped at 200",
  handoffsCapped: "handoffs capped at 100",
  alertClassUnmapped: "alert kind without a class",
  searchNotServed: "handoff search is not served here; use GET /v1/sessions/summaries/search",
} as const;

/**
 * The alert taxonomy's classes, as src/session-intel/replay/classes.ts classOfKind maps detector kinds. Mirrored,
 * not imported: classes.ts reads its own file at load (CLASS_MAP_SHA256), which the bundled Lambda and the read
 * Worker cannot do. The route test pins this mirror to classOfKind. An unmapped kind is `null` (and a coverage
 * note), never a silent "none".
 */
export type AlertClass = "constraint_violation" | "intent_conflict" | "collision" | "overlap" | "planned_fleet" | "none";
const CLASS_NAMES: readonly AlertClass[] = ["constraint_violation", "intent_conflict", "collision", "overlap", "planned_fleet", "none"];
export function alertClassOf(kind: string, urgency: "high" | "low"): AlertClass | null {
  switch (kind) {
    case "duplicate_work":
    case "superseded_work":
      return "collision";
    case "concurrent_edit":
      return urgency === "high" ? "collision" : "overlap";
    case "nearby_work":
    case "similar_summary":
      return "overlap";
    default:
      return (CLASS_NAMES as readonly string[]).includes(kind) ? (kind as AlertClass) : null;
  }
}

export interface SessionRef { source: string; sessionId: string }

/** One session_intel_workspace_alerts_v1 row. */
export interface SessionIntelAlertRow {
  advisoryId: string;
  urgency: "high" | "low";
  kind: string;
  artifact: string;
  pair: SessionRef[];
  recipients: Array<SessionRef & { acked: boolean }>;
  createdAt: string;
  deliveredAt: string | null;
  evidence: AdvisoryEvidence[];
}

/** One session_intel_workspace_claims_v1 row. */
export interface SessionIntelClaimRow {
  source: string;
  sessionId: string;
  kind: "intent" | "decision";
  title: string;
  updatedAt: string;
}

/** Null: the 062170 reader is not installed. */
export interface SessionIntelSource {
  alerts(orgId: string, workspaceId: string, limit: number): Promise<SessionIntelAlertRow[] | null>;
  claims(orgId: string, workspaceId: string, keys: SessionRef[], limit: number): Promise<SessionIntelClaimRow[] | null>;
}

const INSTALLED_SQL = "SELECT to_regprocedure($1) IS NOT NULL AS installed";
const ALERTS_SQL = `SELECT advisory_id, urgency, kind, artifact, pair, recipients, created_at, delivered_at, evidence
  FROM sessions.session_intel_workspace_alerts_v1($1, $2, $3)`;
const CLAIMS_SQL = `SELECT source, session_id, kind, title, updated_at
  FROM sessions.session_intel_workspace_claims_v1($1, $2, $3::text[], $4)`;

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString());
const isoOrNull = (v: unknown) => (v === null || v === undefined ? null : iso(v));
const json = (raw: unknown): unknown => (typeof raw === "string" ? JSON.parse(raw) : raw);
const str = (v: unknown) => (typeof v === "string" ? v : String(v ?? ""));
const cut = (s: string, max: number) => (Array.from(s).length > max ? Array.from(s).slice(0, max).join("") : s);
const refs = (raw: unknown): SessionRef[] => {
  const list = json(raw);
  return Array.isArray(list) ? list.map((e) => ({ source: str(e?.source), sessionId: str(e?.sessionId) })) : [];
};

/** The source over the 062170 read-API functions, each in its own READ ONLY tenant transaction. */
export function sqlSessionIntel(read: TenantRunner): SessionIntelSource {
  return {
    alerts: (orgId, workspaceId, limit) =>
      read(orgId, workspaceId, async (q: SqlQuery) => {
        const installed = await q.query<{ installed: boolean }>(INSTALLED_SQL, [SESSION_INTEL_ALERTS_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const { rows } = await q.query<Record<string, unknown>>(ALERTS_SQL, [orgId, workspaceId, Math.min(limit, SESSION_INTEL_ALERTS_MAX)]);
        return rows.slice(0, SESSION_INTEL_ALERTS_MAX).map((row) => {
          const recipients = json(row.recipients);
          const evidence = json(row.evidence);
          return {
            advisoryId: str(row.advisory_id),
            urgency: row.urgency === "high" ? "high" as const : "low" as const,
            kind: str(row.kind),
            artifact: str(row.artifact),
            pair: refs(row.pair).slice(0, 4),
            recipients: Array.isArray(recipients)
              ? recipients.slice(0, 4).map((e) => ({ source: str(e?.source), sessionId: str(e?.sessionId), acked: e?.acked === true }))
              : [],
            createdAt: iso(row.created_at),
            deliveredAt: isoOrNull(row.delivered_at),
            evidence: Array.isArray(evidence)
              ? evidence.slice(0, 20).map((e) => ({ sessionId: str(e?.sessionId), eventId: str(e?.eventId), ts: str(e?.ts), kind: str(e?.kind) }))
              : [],
          };
        });
      }),
    claims: (orgId, workspaceId, keys, limit) =>
      read(orgId, workspaceId, async (q: SqlQuery) => {
        const installed = await q.query<{ installed: boolean }>(INSTALLED_SQL, [SESSION_INTEL_CLAIMS_SIGNATURE]);
        if (!installed.rows[0]?.installed) return null;
        const { rows } = await q.query<Record<string, unknown>>(CLAIMS_SQL, [orgId, workspaceId, keys.map((k) => `${k.source}:${k.sessionId}`), limit]);
        return rows.slice(0, limit).map((row) => ({
          source: str(row.source),
          sessionId: str(row.session_id),
          kind: row.kind === "decision" ? "decision" as const : "intent" as const,
          title: cut(str(row.title), SESSION_INTEL_TITLE_MAX),
          updatedAt: iso(row.updated_at),
        }));
      }),
  };
}

/** The view could not be built; the route answers 501 with this reason. */
export class SessionIntelUnsupported extends Error {
  constructor(readonly reason: "directory_not_configured" | "directory_not_migrated" | "session_intel_not_migrated") {
    super(`session intelligence: ${reason}`);
    this.name = "SessionIntelUnsupported";
  }
}

const ENDED = new Set(["done", "abandoned"]);
/** The claim evidence tables' bounds on source and session_id (062080, 062150). */
const CLAIM_SOURCE = /^[a-z][a-z0-9_-]{0,31}$/u;
const CLAIM_SESSION = /^[^\r\n]{1,512}$/u;
const lastActivityMs = (row: DirectoryListRow) => new Date(row.last_activity_at as string | Date).getTime();
const newest = (values: Array<string | null>) => values.filter((v): v is string => v !== null).sort().at(-1) ?? null;

export interface WorkspaceViewArgs {
  orgId: string;
  workspaceId: string;
  now: number;
  source: SessionIntelSource;
  directory: Directory | undefined;
  /** Counts and reason codes only. */
  log: (fields: Record<string, unknown>) => void;
}

/**
 * The whole view. Reads one after another on the request's runner: the alerts, the active page (last activity
 * within 2 hours, at most 200 + 1 rows), the older page (2 hours to 7 days, at most 100 + 1), then the claims of
 * the listed sessions. Status is the active page's sessions that have not ended; handoffs are the ended ones of
 * that page plus the older page, newest first, at most 100.
 */
export async function workspaceView(a: WorkspaceViewArgs) {
  if (!a.directory) throw new SessionIntelUnsupported("directory_not_configured");
  const notes: string[] = [];
  const alertRows = await a.source.alerts(a.orgId, a.workspaceId, SESSION_INTEL_ALERTS_MAX);
  if (alertRows === null) throw new SessionIntelUnsupported("session_intel_not_migrated");
  const activeSince = new Date(a.now - SESSION_INTEL_ACTIVE_WINDOW_MS);
  const active = await a.directory.list(a.orgId, a.workspaceId, { lastTsAfter: activeSince, limit: SESSION_INTEL_STATUS_MAX });
  if (active === null) throw new SessionIntelUnsupported("directory_not_migrated");
  const older = await a.directory.list(a.orgId, a.workspaceId, {
    lastTsAfter: new Date(a.now - SESSION_INTEL_HANDOFF_WINDOW_MS), lastTsBefore: activeSince, limit: SESSION_INTEL_HANDOFFS_MAX,
  });
  if (older === null) throw new SessionIntelUnsupported("directory_not_migrated");
  for (const row of [...active, ...older])
    if (row.org_id !== a.orgId || row.workspace_id !== a.workspaceId) throw new TenantMismatchError();

  if (alertRows.length >= SESSION_INTEL_ALERTS_MAX) notes.push(SESSION_INTEL_NOTES.alertsCapped);
  if (active.length > SESSION_INTEL_STATUS_MAX) notes.push(SESSION_INTEL_NOTES.statusCapped);
  const activeRows = active.slice(0, SESSION_INTEL_STATUS_MAX);
  const statusRows = activeRows.filter((r) => !ENDED.has(r.state ?? ""));
  const handoffCandidates = [...activeRows.filter((r) => ENDED.has(r.state ?? "")), ...older.slice(0, SESSION_INTEL_HANDOFFS_MAX)]
    .sort((x, y) => lastActivityMs(y) - lastActivityMs(x) || (x.session_id < y.session_id ? 1 : x.session_id > y.session_id ? -1 : 0));
  if (older.length > SESSION_INTEL_HANDOFFS_MAX || handoffCandidates.length > SESSION_INTEL_HANDOFFS_MAX) notes.push(SESSION_INTEL_NOTES.handoffsCapped);
  const handoffRows = handoffCandidates.slice(0, SESSION_INTEL_HANDOFFS_MAX);

  // Claims: at most 300 keys and 8 titles each, so the limit never cuts a session's titles.
  const keyOf = (source: string, sessionId: string) => `${source}\u0000${sessionId}`;
  // Only keys a claim's evidence can hold (its source and session_id CHECKs): a session outside them has no claims,
  // and sending it would make the reader refuse the whole batch (22023).
  const keys = [...new Map([...statusRows, ...handoffRows]
    .filter((r) => CLAIM_SOURCE.test(r.harness) && CLAIM_SESSION.test(r.session_id))
    .map((r) => [keyOf(r.harness, r.session_id), { source: r.harness, sessionId: r.session_id }])).values()];
  let claims: SessionIntelClaimRow[] | null = [];
  if (keys.length > 0) {
    try {
      claims = await a.source.claims(a.orgId, a.workspaceId, keys, keys.length * (SESSION_INTEL_INTENTS_MAX + SESSION_INTEL_DECISIONS_MAX));
      if (claims === null) a.log({ reason: "session_intel_claims_unavailable", error_code: "not_migrated" });
    } catch (error) {
      a.log({ reason: "session_intel_claims_unavailable", error_code: (error as { code?: string })?.code ?? (error as { name?: string })?.name ?? "error" });
      claims = null;
    }
  }
  if (claims === null) notes.push(SESSION_INTEL_NOTES.claimsUnavailable);
  const bySession = new Map<string, { intents: string[]; decisions: string[] }>();
  for (const c of claims ?? []) {
    const k = keyOf(c.source, c.sessionId);
    const entry = bySession.get(k) ?? { intents: [], decisions: [] };
    if (c.kind === "intent" && entry.intents.length < SESSION_INTEL_INTENTS_MAX) entry.intents.push(c.title);
    if (c.kind === "decision" && entry.decisions.length < SESSION_INTEL_DECISIONS_MAX) entry.decisions.push(c.title);
    bySession.set(k, entry);
  }
  const claimsOf = (r: DirectoryListRow) => bySession.get(keyOf(r.harness, r.session_id));
  const hasClaims = (r: DirectoryListRow) => (claims === null ? null : claimsOf(r) !== undefined);
  const summaryOf = (r: DirectoryListRow) => {
    const s = typeof r.short_summary === "string" && r.short_summary !== "" ? cut(r.short_summary, SHORT_SUMMARY_MAX) : null;
    return { shortSummary: s, summaryAt: s === null ? null : isoOrNull(r.short_summary_generated_at) };
  };
  const titleOf = (r: DirectoryListRow) => (typeof r.title === "string" ? cut(r.title, SESSION_TITLE_MAX) : null);

  const status = statusRows.map((r) => ({
    source: r.harness,
    sessionId: r.session_id,
    projectId: r.project_id ?? null,
    title: titleOf(r),
    ...summaryOf(r),
    state: r.state ?? "unknown",
    lastActivityAt: iso(r.last_activity_at),
    openIntents: claimsOf(r)?.intents ?? [],
    hasClaims: hasClaims(r),
  }));
  const handoffs = handoffRows.map((r) => ({
    source: r.harness,
    sessionId: r.session_id,
    title: titleOf(r),
    ...summaryOf(r),
    state: r.state ?? "unknown",
    lastActivityAt: iso(r.last_activity_at),
    decisions: claimsOf(r)?.decisions ?? [],
    openIntents: claimsOf(r)?.intents ?? [],
    hasClaims: hasClaims(r),
  }));
  const alerts = alertRows.map((row) => {
    const cls = alertClassOf(row.kind, row.urgency);
    return {
      advisoryId: row.advisoryId,
      urgency: row.urgency,
      kind: row.kind,
      class: cls,
      artifact: row.artifact,
      sessions: row.pair,
      createdAt: row.createdAt,
      deliveredAt: row.deliveredAt,
      acked: row.recipients,
      evidence: row.evidence,
    };
  });
  if (alerts.some((x) => x.class === null)) notes.push(SESSION_INTEL_NOTES.alertClassUnmapped);
  notes.push(SESSION_INTEL_NOTES.searchNotServed);

  const coverage = {
    sessionsActive: status.length,
    sessionsWithSummary: status.filter((s) => s.shortSummary !== null).length,
    sessionsWithClaims: claims === null ? null : status.filter((s) => s.hasClaims === true).length,
    claimsNewestAt: claims === null ? null : newest(claims.map((c) => c.updatedAt)),
    summariesNewestAt: newest([...status, ...handoffs].map((s) => s.summaryAt)),
    // Whether the claims extractor runs is not visible to the read API: never "on".
    claimsExtraction: "unknown" as "off" | "unknown",
    notes,
  };
  return { contract: SESSION_INTEL_CONTRACT, alerts, status, handoffs, coverage, nextCursor: null };
}
