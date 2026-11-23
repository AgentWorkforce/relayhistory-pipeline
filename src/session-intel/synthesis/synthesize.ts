/**
 * workspace-synthesis/1 (docs/session-intel-workspace-synthesis.md): a deterministic aggregate over the completed,
 * scrubbed F1 per-session analysis results (session-analysis schema v2) of ONE workspace and ONE window.
 *
 * Pure: no clock (`opts.now` is injected), no environment, no network, no model. The same cohort in any order gives
 * byte-identical JSON: entries are sorted by (source, sessionId) before anything is capped, counted or summed, every
 * output array is sorted by a stable key, and object keys are written in a fixed contract order.
 *
 * Nothing is inferred. Every number is a count or a ratio of two counts. The only text that leaves is text already in
 * the F1 results (a claim's text, an evidence summary's path or command), cut to a prefix of at most 280 characters,
 * so every output string is a substring of an input F1 string or is built from ids, enums and counts.
 */
import { createHash } from "node:crypto";
import {
  decodeSessionAnalysisResult,
  type EvidenceKind,
  type OutcomeStatus,
  type SessionAnalysisDecodeResult,
  type SessionAnalysisResult,
  type SessionEvidenceReference,
  type SessionType,
} from "../../session-analysis/engine/session-analysis-result.ts";
import { ANALYSIS_RESULT_MAX_BYTES } from "../../session-analysis/results.ts";
import { RELAYHISTORY_SESSION_ANALYSIS_VERSIONS, SESSION_ANALYSIS_VERSION } from "../../session-analysis/versions.ts";

export const SYNTHESIS_CONTRACT = "workspace-synthesis/1";
export const COHORT_FORMAT = "workspace-synthesis-cohort/1";

export const SESSION_TYPES: readonly SessionType[] = [
  "planning", "implementation", "debugging", "review", "testing", "research", "conversation", "mixed", "unknown",
];
export const OUTCOME_STATUSES: readonly OutcomeStatus[] = [
  "not-started", "in-progress", "implemented", "reported-complete", "verified", "blocked", "superseded", "unknown",
];
/** A session's category status: its one distinct outcome status, `mixed` for several, `none` for no outcome. */
export type CategoryStatus = OutcomeStatus | "mixed" | "none";
export const CATEGORY_STATUSES: readonly CategoryStatus[] = [...OUTCOME_STATUSES, "mixed", "none"];

/**
 * Why a considered session is not aggregated. Closed set, from how the analysis store and read path represent a
 * session without a usable result:
 *  - not_analyzed   no completed job (queued or running, read.ts) or a null result (decode `empty`);
 *  - failed         the job failed (read.ts `failed`, store.ts after its attempts);
 *  - unsupported    an envelope or snapshot schema version this reader does not know (decode `unsupported`);
 *  - invalid        the envelope, snapshot, identity, evidence bundle or a claim's evidence reference fails
 *                   validation (decode `malformed`); never trusted, never aggregated;
 *  - too_large      the result is over ANALYSIS_RESULT_MAX_BYTES (results.ts `analysis_result_bound`);
 *  - stale_version  a valid result whose analysisVersion this reader does not accept (versions.ts);
 *  - out_of_window  analyzedAt or any activity timestamp falls outside the cohort window.
 */
export const SKIP_REASONS = [
  "not_analyzed", "failed", "unsupported", "invalid", "too_large", "stale_version", "out_of_window",
] as const;
export type SkipReason = (typeof SKIP_REASONS)[number];

export const ISSUE_KINDS = ["artifact", "failed-command"] as const;
export type IssueKind = (typeof ISSUE_KINDS)[number];

export const SYNTHESIS_BOUNDS = {
  /** A window longer than this is refused. */
  maxWindowMs: 30 * 24 * 60 * 60 * 1_000,
  /** Sessions considered; beyond it the first ones in (source, sessionId) order are kept and inputs.truncated is set. */
  maxSessions: 500,
  /** A cohort above this many entries is refused outright (an input guard, before any result is decoded). */
  maxCohortEntries: 5_000,
  /** Sessions named per category and per issue. */
  maxDrilldown: 50,
  maxIssues: 10,
  maxEvidencePerIssue: 5,
  maxTextChars: 280,
  /** The serialized artifact. Drilldowns are shortened to fit; an artifact that still does not fit is refused. */
  maxArtifactBytes: 256 * 1024,
} as const;
export type TunableBounds = Omit<typeof SYNTHESIS_BOUNDS, "maxWindowMs" | "maxCohortEntries">;

export const SYNTHESIS_DEFAULTS = { staleAfterMs: 24 * 60 * 60 * 1_000, minRecurrence: 3, minRoots: 2 } as const;

/** Plain ids. source: the snapshot harness rule; sessionId: the read API id rule; workspace: the tenant id rule. */
export const SOURCE_ID = /^[A-Za-z0-9_.-]{1,64}$/u;
export const SESSION_ID = /^[A-Za-z0-9._:-]{1,255}$/u;
export const WORKSPACE_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;
export const ROOT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$/u;
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/u;

export class SynthesisInputError extends Error {
  constructor(readonly code: string, message: string) {
    super(`${code}: ${message}`);
    this.name = "SynthesisInputError";
  }
}
const refuse = (code: string, message: string): never => { throw new SynthesisInputError(code, message); };

export interface CohortRoot { projectId: string | null; repo: string | null }
interface EntryBase extends CohortRoot { workspace: string; source: string; sessionId: string }
export interface AnalyzedEntry extends EntryBase { status: "analyzed"; result: unknown }
export interface SkippedEntry extends EntryBase { status: "skipped"; reason: SkipReason }
export type CohortEntry = AnalyzedEntry | SkippedEntry;
export interface Cohort {
  format: typeof COHORT_FORMAT;
  workspace: string;
  window: { since: string; until: string };
  /** Sessions in the window the producer could consider at all; null when it does not know. Never derived. */
  eligible: number | null;
  sessions: CohortEntry[];
}

export interface SynthesisOptions {
  /** The generation time, injected. Freshness is measured against it. */
  now: string | Date;
  staleAfterMs?: number;
  minRecurrence?: number;
  /** analysisVersion values accepted as current; default: this pipeline's and relayhistory's (versions.ts). */
  acceptedAnalysisVersions?: readonly string[];
  /** Lower bounds, to exercise truncation. Each must be at most its SYNTHESIS_BOUNDS value. */
  bounds?: Partial<TunableBounds>;
}

export interface SessionRef { source: string; sessionId: string }
export interface EvidenceRef extends SessionRef { eventId: string; ts?: string; kind?: EvidenceKind }
export interface Cap { limit: number; truncated: boolean }
export interface SourceUsage {
  considered: number;
  analyzed: number;
  observed: number;
  unobserved: number;
  knownCost: number;
  nullCost: number;
  observedRate: number | null;
  nullCostRate: number | null;
}
export interface Issue {
  id: string;
  label: "candidate";
  kind: IssueKind;
  key: string;
  keyTruncated: boolean;
  text: string;
  textTruncated: boolean;
  roots: number;
  sessionCount: number;
  sessionsTruncated: boolean;
  sessions: SessionRef[];
  evidenceTruncated: boolean;
  evidence: EvidenceRef[];
  basis: "deterministic";
}
export interface Category {
  sessionType: SessionType;
  outcomeStatus: CategoryStatus;
  count: number;
  truncated: boolean;
  sessions: SessionRef[];
}
export interface WorkspaceSynthesis {
  contract: typeof SYNTHESIS_CONTRACT;
  workspace: string;
  window: { since: string; until: string };
  generatedAt: string;
  inputs: {
    eligible: number | null;
    considered: number;
    analyzed: number;
    skipped: Record<SkipReason, number>;
    truncated: boolean;
    caps: {
      sessions: Cap & { offered: number };
      drilldown: Cap;
      issues: Cap;
      evidencePerIssue: Cap;
      textChars: Cap;
      artifactBytes: Cap;
    };
    notes: string[];
  };
  freshness: {
    staleAfterMs: number;
    analyzedAtMin: string | null;
    analyzedAtMax: string | null;
    indexedThroughMs: number | null;
    indexedThroughMissing: number;
    staleCount: number;
    analyzedAfterNow: number;
    bySource: Record<string, { analyzed: number; analyzedAtMin: string; analyzedAtMax: string; staleCount: number }>;
  };
  usageCoverage: Record<string, SourceUsage>;
  categories: Category[];
  issues: Issue[];
}

export const textCompare = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const refCompare = (a: SessionRef, b: SessionRef) => textCompare(a.source, b.source) || textCompare(a.sessionId, b.sessionId);
const sessionKey = (r: SessionRef) => `${r.source}\u0000${r.sessionId}`;
/** A ratio of two counts at 4 decimals; null when the denominator is 0 (never a division by zero, never 0 for unknown). */
export const rate = (n: number, d: number): number | null => (d > 0 ? Math.round((n / d) * 1e4) / 1e4 : null);
/** A prefix of at most `cap` code points: always a substring of the input. */
export function capText(text: string, cap: number): { text: string; truncated: boolean } {
  const points = [...text];
  return points.length > cap ? { text: points.slice(0, cap).join(""), truncated: true } : { text, truncated: false };
}

function isoTime(value: unknown, field: string): string {
  if (typeof value !== "string" || !ISO.test(value) || !Number.isFinite(Date.parse(value)))
    refuse("invalid_timestamp", `${field} must be an ISO-8601 timestamp with a zone`);
  return new Date(Date.parse(value as string)).toISOString();
}
function optionalRoot(value: unknown, field: string): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string" || !ROOT_ID.test(value)) refuse("invalid_entry", `${field} must match ${ROOT_ID.source}`);
  return value as string;
}
const ENTRY_KEYS: Record<"analyzed" | "skipped", readonly string[]> = {
  analyzed: ["status", "workspace", "source", "sessionId", "projectId", "repo", "result"],
  skipped: ["status", "workspace", "source", "sessionId", "projectId", "repo", "reason"],
};

/**
 * The cohort wrapper binds one workspace and one window. Anything malformed refuses the whole cohort: an unknown
 * field, a window over 30 days, an entry of another workspace, an id that is not a plain id, a repeated
 * (source, sessionId), or more than maxCohortEntries entries.
 */
export function parseCohort(raw: unknown): Cohort {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return refuse("invalid_cohort", "the cohort must be an object");
  const c = raw as Record<string, unknown>;
  for (const k of Object.keys(c))
    if (!["format", "workspace", "window", "eligible", "sessions"].includes(k)) refuse("invalid_cohort", `undeclared field ${JSON.stringify(k.slice(0, 64))}`);
  if (c.format !== COHORT_FORMAT) refuse("invalid_cohort", `format must be ${COHORT_FORMAT}`);
  if (typeof c.workspace !== "string" || !WORKSPACE_ID.test(c.workspace)) refuse("invalid_cohort", `workspace must match ${WORKSPACE_ID.source}`);
  const workspace = c.workspace as string;
  const w = c.window as Record<string, unknown> | null;
  if (!w || typeof w !== "object" || Array.isArray(w) || Object.keys(w).some((k) => k !== "since" && k !== "until"))
    refuse("invalid_cohort", "window must be {since, until}");
  const since = isoTime(w!.since, "window.since"), until = isoTime(w!.until, "window.until");
  const span = Date.parse(until) - Date.parse(since);
  if (span <= 0) refuse("invalid_window", "window.since must be before window.until");
  if (span > SYNTHESIS_BOUNDS.maxWindowMs) refuse("window_too_long", "the window is longer than 30 days");
  if (!("eligible" in c)) refuse("invalid_cohort", "eligible is required (a count, or null when the producer does not know it)");
  if (c.eligible !== null && (!Number.isSafeInteger(c.eligible) || (c.eligible as number) < 0))
    refuse("invalid_cohort", "eligible must be a non-negative integer or null");
  if (!Array.isArray(c.sessions)) return refuse("invalid_cohort", "sessions must be an array");
  if (c.sessions.length > SYNTHESIS_BOUNDS.maxCohortEntries)
    refuse("cohort_too_large", `${c.sessions.length} entries is above the input guard of ${SYNTHESIS_BOUNDS.maxCohortEntries}; split the window`);
  if (c.eligible !== null && (c.eligible as number) < c.sessions.length)
    refuse("invalid_cohort", "eligible is below the number of sessions in the cohort");
  const seen = new Set<string>();
  const sessions = c.sessions.map((e, i): CohortEntry => {
    const where = `sessions[${i}]`;
    if (!e || typeof e !== "object" || Array.isArray(e)) return refuse("invalid_entry", `${where} must be an object`);
    const r = e as Record<string, unknown>;
    if (r.status !== "analyzed" && r.status !== "skipped") return refuse("invalid_entry", `${where}.status must be analyzed or skipped`);
    for (const k of Object.keys(r)) if (!ENTRY_KEYS[r.status].includes(k)) refuse("invalid_entry", `${where} has undeclared field ${JSON.stringify(k.slice(0, 64))}`);
    if (r.workspace !== workspace) refuse("cross_workspace", `${where}.workspace is not the cohort's workspace`);
    if (typeof r.source !== "string" || !SOURCE_ID.test(r.source)) refuse("invalid_id", `${where}.source must match ${SOURCE_ID.source}`);
    if (typeof r.sessionId !== "string" || !SESSION_ID.test(r.sessionId)) refuse("invalid_id", `${where}.sessionId must match ${SESSION_ID.source}`);
    const source = r.source as string, sessionId = r.sessionId as string;
    const k = sessionKey({ source, sessionId });
    if (seen.has(k)) refuse("duplicate_session", `${where} repeats a (source, sessionId) already in the cohort`);
    seen.add(k);
    const base = { workspace, source, sessionId, projectId: optionalRoot(r.projectId, `${where}.projectId`), repo: optionalRoot(r.repo, `${where}.repo`) };
    if (r.status === "skipped") {
      if (!SKIP_REASONS.includes(r.reason as SkipReason)) refuse("invalid_entry", `${where}.reason must be one of ${SKIP_REASONS.join(", ")}`);
      return { status: "skipped", ...base, reason: r.reason as SkipReason };
    }
    if (!("result" in r)) refuse("invalid_entry", `${where}.result is required (null when there is none)`);
    return { status: "analyzed", ...base, result: r.result };
  });
  return { format: COHORT_FORMAT, workspace, window: { since, until }, eligible: c.eligible as number | null, sessions };
}

/**
 * The independence root of a session: its repository, else its project, else its source. Sessions in one root are
 * not independent witnesses of a recurring failure (one repository's flaky step recurs in every session there).
 */
export function independenceRoot(e: CohortRoot & { source: string }): string {
  return e.repo !== null ? `repo:${e.repo}` : e.projectId !== null ? `project:${e.projectId}` : `source:${e.source}`;
}

export interface AnalyzedSession extends SessionRef, CohortRoot { result: SessionAnalysisResult; analyzedAtMs: number }
const DECODE_SKIP: Record<Exclude<SessionAnalysisDecodeResult["status"], "ready">, SkipReason> = {
  empty: "not_analyzed", unsupported: "unsupported", malformed: "invalid",
};

/** One analyzed entry: the full F1 check, then the version and the window. */
export function classifyEntry(e: AnalyzedEntry, accepted: ReadonlySet<string>, sinceMs: number, untilMs: number): { ok: AnalyzedSession } | { skip: SkipReason } {
  if (e.result === null) return { skip: "not_analyzed" };
  let bytes: number;
  try {
    bytes = new TextEncoder().encode(JSON.stringify(e.result)).length;
  } catch {
    return { skip: "invalid" };
  }
  if (bytes > ANALYSIS_RESULT_MAX_BYTES) return { skip: "too_large" };
  // Envelope, snapshot, identity (source and sessionId equal the entry's), the evidence bundle
  // (validateEvidenceBundle) and every claim's evidence id (validateSemanticAnalysis).
  const decoded = decodeSessionAnalysisResult(e.result, { source: e.source, sessionId: e.sessionId });
  if (decoded.status !== "ready") return { skip: DECODE_SKIP[decoded.status] };
  const result = decoded.result;
  if (!accepted.has(result.analysisVersion)) return { skip: "stale_version" };
  const analyzedAtMs = Date.parse(result.analysis.analyzedAt);
  // Half-open [since, until): analyzedAt and every activity timestamp the result carries.
  const inWindow = (ms: number) => ms >= sinceMs && ms < untilMs;
  const activity = [
    ...result.analysis.evidence.references.flatMap((r) => (r.at === null ? [] : [Date.parse(r.at)])),
    ...result.analysis.metrics.interactionPeriods.flatMap((p) => [Date.parse(p.startedAt), Date.parse(p.endedAt)]),
  ];
  if (!inWindow(analyzedAtMs) || activity.some((ms) => !inWindow(ms))) return { skip: "out_of_window" };
  return { ok: { source: e.source, sessionId: e.sessionId, projectId: e.projectId, repo: e.repo, result, analyzedAtMs } };
}

/** The target of a `<tool name>: <target>` summary (session-evidence.ts), or null. */
function summaryTarget(summary: string): string | null {
  const i = summary.indexOf(": ");
  const t = i < 0 ? "" : summary.slice(i + 2).trim();
  return t ? t : null;
}
const LONE_PATH = /^\S*[/\\]\S*$|^\S+\.[A-Za-z0-9]{1,8}$/u;
export type FailureClaimKind = "blocker" | "failed-verification" | "blocked-outcome";

/**
 * The exact issue keys one cited failure claim contributes. No normalization and no similarity: two keys are the
 * same only when the strings are equal.
 *  - artifact:        the path of a cited file edit, or of a cited tool call whose whole target is one path;
 *  - failed-command:  for a failed verification only, the summary (`<tool>: <command>`) of a cited tool call that
 *                     failed (its own isError, or a cited result linked to it with isError).
 */
export function issueKeys(kind: FailureClaimKind, evidenceIds: readonly string[], byId: ReadonlyMap<string, SessionEvidenceReference>): Array<{ kind: IssueKind; key: string; evidenceId: string }> {
  const out: Array<{ kind: IssueKind; key: string; evidenceId: string }> = [];
  for (const id of evidenceIds) {
    const ref = byId.get(id);
    if (!ref) continue;
    if (ref.kind === "file-edit" || ref.kind === "tool-call") {
      const target = summaryTarget(ref.summary);
      if (target !== null && (ref.kind === "file-edit" || LONE_PATH.test(target))) out.push({ kind: "artifact", key: target, evidenceId: id });
    }
    if (kind !== "failed-verification") continue;
    const call = ref.kind === "tool-call" ? ref : ref.kind === "tool-result" && ref.toolCallId !== null ? byId.get(ref.toolCallId) : undefined;
    if (!call || call.kind !== "tool-call") continue;
    if (call.isError === true || (ref.kind === "tool-result" && ref.isError === true)) out.push({ kind: "failed-command", key: call.summary, evidenceId: id });
  }
  return out;
}

const issueId = (kind: IssueKind, key: string) => `${kind}:${createHash("sha256").update(`${kind}\u0000${key}`).digest("hex").slice(0, 16)}`;

function bound(name: keyof TunableBounds, opts: SynthesisOptions): number {
  const max = SYNTHESIS_BOUNDS[name];
  const v = opts.bounds?.[name] ?? max;
  if (!Number.isSafeInteger(v) || v < 0 || v > max) refuse("invalid_option", `bounds.${name} must be an integer in [0, ${max}]`);
  return v;
}

export function synthesize(rawCohort: unknown, opts: SynthesisOptions): WorkspaceSynthesis {
  if (!opts || opts.now === undefined) refuse("invalid_option", "opts.now is required (no clock is read)");
  const now = isoTime(opts.now instanceof Date ? opts.now.toISOString() : opts.now, "opts.now");
  const nowMs = Date.parse(now);
  const staleAfterMs = opts.staleAfterMs ?? SYNTHESIS_DEFAULTS.staleAfterMs;
  if (!Number.isSafeInteger(staleAfterMs) || staleAfterMs < 0) refuse("invalid_option", "staleAfterMs must be a non-negative integer");
  const minRecurrence = opts.minRecurrence ?? SYNTHESIS_DEFAULTS.minRecurrence;
  if (!Number.isSafeInteger(minRecurrence) || minRecurrence < 2) refuse("invalid_option", "minRecurrence must be an integer >= 2");
  const maxSessions = bound("maxSessions", opts), maxDrilldown = bound("maxDrilldown", opts), maxIssues = bound("maxIssues", opts);
  const maxEvidence = bound("maxEvidencePerIssue", opts), maxText = bound("maxTextChars", opts), maxBytes = bound("maxArtifactBytes", opts);
  const accepted = new Set(opts.acceptedAnalysisVersions ?? [SESSION_ANALYSIS_VERSION, ...RELAYHISTORY_SESSION_ANALYSIS_VERSIONS]);

  const cohort = parseCohort(rawCohort);
  const sinceMs = Date.parse(cohort.window.since), untilMs = Date.parse(cohort.window.until);
  const offered = [...cohort.sessions].sort(refCompare);
  const entries = offered.slice(0, maxSessions);

  const skipped = Object.fromEntries(SKIP_REASONS.map((r) => [r, 0])) as Record<SkipReason, number>;
  const analyzed: AnalyzedSession[] = [];
  const consideredBySource = new Map<string, number>();
  for (const e of entries) {
    consideredBySource.set(e.source, (consideredBySource.get(e.source) ?? 0) + 1);
    if (e.status === "skipped") { skipped[e.reason]++; continue; }
    const c = classifyEntry(e, accepted, sinceMs, untilMs);
    if ("skip" in c) skipped[c.skip]++;
    else analyzed.push(c.ok);
  }
  const n = analyzed.length;
  const caps: WorkspaceSynthesis["inputs"]["caps"] = {
    sessions: { limit: maxSessions, offered: offered.length, truncated: offered.length > maxSessions },
    drilldown: { limit: maxDrilldown, truncated: false },
    issues: { limit: maxIssues, truncated: false },
    evidencePerIssue: { limit: maxEvidence, truncated: false },
    textChars: { limit: maxText, truncated: false },
    artifactBytes: { limit: maxBytes, truncated: false },
  };

  // Freshness, against the injected now.
  const staleBefore = nowMs - staleAfterMs;
  const perSource = new Map<string, AnalyzedSession[]>();
  for (const a of analyzed) perSource.set(a.source, [...(perSource.get(a.source) ?? []), a]);
  const iso = (ms: number) => new Date(ms).toISOString();
  const times = analyzed.map((a) => a.analyzedAtMs);
  const marks = analyzed.map((a) => a.result.analysis.evidence.indexedThroughMs);
  const missing = marks.filter((m) => m === null).length;
  const freshness: WorkspaceSynthesis["freshness"] = {
    staleAfterMs,
    analyzedAtMin: n ? iso(Math.min(...times)) : null,
    analyzedAtMax: n ? iso(Math.max(...times)) : null,
    // A completeness watermark only when every analyzed session carries one: the cohort is complete through the earliest.
    indexedThroughMs: n && missing === 0 ? Math.min(...(marks as number[])) : null,
    indexedThroughMissing: missing,
    staleCount: times.filter((t) => t < staleBefore).length,
    analyzedAfterNow: times.filter((t) => t > nowMs).length,
    bySource: Object.fromEntries([...perSource.keys()].sort(textCompare).map((source) => {
      const ts = perSource.get(source)!.map((a) => a.analyzedAtMs);
      return [source, { analyzed: ts.length, analyzedAtMin: iso(Math.min(...ts)), analyzedAtMax: iso(Math.max(...ts)), staleCount: ts.filter((t) => t < staleBefore).length }];
    })),
  };

  // Usage coverage per considered source. Usage observed and cost known are separate axes.
  const usageCoverage: Record<string, SourceUsage> = Object.fromEntries([...consideredBySource.keys()].sort(textCompare).map((source) => {
    const mine = perSource.get(source) ?? [];
    const observed = mine.filter((a) => a.result.analysis.metrics.usage.observed).length;
    const knownCost = mine.filter((a) => a.result.analysis.metrics.usage.costUSD !== null).length;
    const analyzedHere = mine.length;
    return [source, {
      considered: consideredBySource.get(source)!, analyzed: analyzedHere,
      observed, unobserved: analyzedHere - observed, knownCost, nullCost: analyzedHere - knownCost,
      observedRate: rate(observed, analyzedHere), nullCostRate: rate(analyzedHere - knownCost, analyzedHere),
    }];
  }));

  // Categories: a partition of the analyzed sessions by (sessionType, category status).
  const cats = new Map<string, SessionRef[]>();
  for (const a of analyzed) {
    const u = a.result.analysis.understanding;
    const statuses = new Set(u.outcomes.map((o) => o.status));
    const status: CategoryStatus = statuses.size === 0 ? "none" : statuses.size > 1 ? "mixed" : [...statuses][0]!;
    const k = `${u.sessionType.value}\u0000${status}`;
    cats.set(k, [...(cats.get(k) ?? []), { source: a.source, sessionId: a.sessionId }]);
  }
  const categoryRows = SESSION_TYPES.flatMap((t) => CATEGORY_STATUSES.flatMap((s) => {
    const refs = cats.get(`${t}\u0000${s}`);
    return refs ? [{ sessionType: t, outcomeStatus: s, all: refs }] : [];
  }));

  // Issues: recurrence candidates by exact key, from cited failure claims, across independent roots.
  type Contribution = { ref: SessionRef; root: string; evidence: Map<string, SessionEvidenceReference>; texts: Set<string> };
  const byKey = new Map<string, { kind: IssueKind; key: string; sessions: Map<string, Contribution> }>();
  for (const a of analyzed) {
    const u = a.result.analysis.understanding;
    const byId = new Map(a.result.analysis.evidence.references.map((r) => [r.id, r]));
    const claims = [
      ...u.blockers.map((c) => ({ kind: "blocker" as const, text: c.text, evidenceIds: c.evidenceIds })),
      ...u.outcomes.filter((o) => o.verification.value === "failed").map((o) => ({ kind: "failed-verification" as const, text: o.verification.explanation, evidenceIds: o.verification.evidenceIds })),
      ...u.outcomes.filter((o) => o.status === "blocked").map((o) => ({ kind: "blocked-outcome" as const, text: o.assessment.text, evidenceIds: o.assessment.evidenceIds })),
    ];
    for (const c of claims) {
      // An uncited claim names no artifact or command, so it contributes no key.
      for (const k of issueKeys(c.kind, c.evidenceIds, byId)) {
        const mk = `${k.kind}\u0000${k.key}`;
        const entry = byKey.get(mk) ?? { kind: k.kind, key: k.key, sessions: new Map<string, Contribution>() };
        const sk = sessionKey(a);
        const s = entry.sessions.get(sk) ?? { ref: { source: a.source, sessionId: a.sessionId }, root: independenceRoot(a), evidence: new Map(), texts: new Set<string>() };
        s.evidence.set(k.evidenceId, byId.get(k.evidenceId)!);
        s.texts.add(c.text);
        entry.sessions.set(sk, s);
        byKey.set(mk, entry);
      }
    }
  }
  const eligibleIssues = [...byKey.values()]
    .map((e) => ({ e, roots: new Set([...e.sessions.values()].map((s) => s.root)).size }))
    .filter(({ e, roots }) => e.sessions.size >= minRecurrence && roots >= SYNTHESIS_DEFAULTS.minRoots)
    .sort((x, y) => y.e.sessions.size - x.e.sessions.size || y.roots - x.roots || textCompare(x.e.kind, y.e.kind) || textCompare(x.e.key, y.e.key));
  if (eligibleIssues.length > maxIssues) caps.issues.truncated = true;
  const issueRows = eligibleIssues.slice(0, maxIssues).map(({ e, roots }) => {
    const sessions = [...e.sessions.values()].sort((x, y) => refCompare(x.ref, y.ref));
    const evidence: EvidenceRef[] = sessions.flatMap((s) => [...s.evidence.keys()].sort(textCompare).map((eventId) => {
      const r = s.evidence.get(eventId)!;
      return { ...s.ref, eventId, ...(r.at !== null ? { ts: new Date(Date.parse(r.at)).toISOString() } : {}), kind: r.kind };
    }));
    // The first contributing session's smallest claim text, verbatim up to the cap.
    const text = capText([...sessions[0]!.texts].sort(textCompare)[0]!, maxText);
    const key = capText(e.key, maxText);
    if (text.truncated || key.truncated) caps.textChars.truncated = true;
    if (evidence.length > maxEvidence) caps.evidencePerIssue.truncated = true;
    return { id: issueId(e.kind, e.key), kind: e.kind, key, text, roots, sessions: sessions.map((s) => s.ref), evidence };
  });

  const build = (drill: number): WorkspaceSynthesis => {
    const capsNow = { ...caps, drilldown: { limit: drill, truncated: false }, artifactBytes: { limit: maxBytes, truncated: drill < maxDrilldown } };
    const cut = <T>(xs: T[]) => { if (xs.length > drill) capsNow.drilldown.truncated = true; return xs.slice(0, drill); };
    const categories: Category[] = categoryRows.map((r) => ({ sessionType: r.sessionType, outcomeStatus: r.outcomeStatus, count: r.all.length, truncated: r.all.length > drill, sessions: cut(r.all) }));
    const issues: Issue[] = issueRows.map((r) => ({
      id: r.id, label: "candidate", kind: r.kind, key: r.key.text, keyTruncated: r.key.truncated, text: r.text.text, textTruncated: r.text.truncated,
      roots: r.roots, sessionCount: r.sessions.length, sessionsTruncated: r.sessions.length > drill, sessions: cut(r.sessions),
      evidenceTruncated: r.evidence.length > maxEvidence, evidence: r.evidence.slice(0, maxEvidence), basis: "deterministic",
    }));
    const notes = [
      "Aggregates cover analyzed sessions only; skipped sessions are counted in inputs.skipped and nowhere else.",
      "Usage is observed only: an unobserved session or a null costUSD is counted as such, never as zero, and nothing is imputed.",
      "Issues are recurrence candidates by exact key, not findings.",
    ];
    if (cohort.eligible === null) notes.push("eligible is unknown: the cohort did not state how many sessions in the window could be considered, and it is not derived.");
    else if (cohort.eligible > offered.length) notes.push(`${cohort.eligible - offered.length} eligible sessions are not in the cohort.`);
    if (capsNow.sessions.truncated) notes.push(`${offered.length - entries.length} sessions beyond the cap of ${maxSessions} were not considered.`);
    if (capsNow.artifactBytes.truncated) notes.push(`Drilldowns were shortened to ${drill} sessions to keep the artifact within ${maxBytes} bytes.`);
    if (missing > 0) notes.push(`${missing} analyzed sessions carry no indexedThroughMs, so freshness.indexedThroughMs is null.`);
    if (n === 0) notes.push("No session was analyzed: every rate is null.");
    if (freshness.analyzedAfterNow > 0) notes.push(`${freshness.analyzedAfterNow} sessions were analyzed after generatedAt.`);
    if (skipped.invalid > 0) notes.push(`${skipped.invalid} results failed validation and were not trusted.`);
    return {
      contract: SYNTHESIS_CONTRACT,
      workspace: cohort.workspace,
      window: cohort.window,
      generatedAt: now,
      inputs: { eligible: cohort.eligible, considered: entries.length, analyzed: n, skipped, truncated: Object.values(capsNow).some((c) => c.truncated), caps: capsNow, notes },
      freshness,
      usageCoverage,
      categories,
      issues,
    };
  };
  // Fit the byte cap by shortening drilldowns; an artifact that still does not fit is refused.
  for (const drill of [...new Set([maxDrilldown, 25, 10, 0])].filter((d) => d <= maxDrilldown)) {
    const out = build(drill);
    if (byteLength(out) <= maxBytes) return out;
  }
  return refuse("artifact_too_large", `the synthesis does not fit in ${maxBytes} bytes even without drilldowns`);
}

/** The serialized form the CLI and the job write and the golden pins: two-space JSON and a trailing newline. */
export const synthesisJson = (s: WorkspaceSynthesis) => `${JSON.stringify(s, null, 2)}\n`;
export const byteLength = (s: WorkspaceSynthesis) => new TextEncoder().encode(synthesisJson(s)).length;
