/**
 * The machine-readable proof report and its evidence sanitizer.
 *
 * Every check emits one CheckResult. Evidence is stored as a SHA-256, a byte
 * count and a sanitized excerpt; raw command output never reaches the report.
 * The sanitizer removes secret-shaped values and every planted canary, so a
 * report can be attached to a PR without leaking what it was looking for.
 */
import { createHash } from "node:crypto";
import type { Mode } from "./guard.ts";

export const REPORT_SCHEMA = "96bulls.aws-e2e-proof/v1";

export const CHECK_IDS = [
  "fresh-install",
  "planted-trace",
  "public-ingress",
  "tenant-isolation",
  "v1-search-scope",
  "transcript-search",
  "stale-summary",
  "restart-persistence",
  "upgrade-rollback",
  "backup-restore",
  "teardown",
] as const;
export type CheckId = (typeof CHECK_IDS)[number];

/**
 * pass/fail are verdicts. `unsupported` states a capability the deployment
 * does not have (full-transcript search on V1). `blocked` means a precondition
 * is missing (an unapplied migration, one CNPG instance), so nothing was
 * concluded. `planned` is a dry run. `skipped` is a mutating step in read-only.
 */
export const STATUSES = ["pass", "fail", "unsupported", "blocked", "planned", "skipped"] as const;
export type Status = (typeof STATUSES)[number];

export type Approval = "khaliq" | "operator";

export interface StepRecord {
  id: string;
  command: string;
  mutating: boolean;
  touches: string[];
  /** khaliq: needs Khaliq's direct OK in his own words before --confirm-live. */
  approval: Approval;
  status: "planned" | "ran" | "skipped" | "refused" | "failed";
  exitCode?: number;
  durationMs?: number;
  /** Refusal or failure code; never foreign error text. */
  code?: string;
}

export interface Evidence {
  label: string;
  sha256: string;
  bytes: number;
  /** Sanitized, truncated. Absent when even the sanitized form is not useful. */
  excerpt?: string;
}

export interface CheckResult {
  check: CheckId;
  status: Status;
  startedAt: string;
  finishedAt: string;
  summary: string;
  metrics: Record<string, unknown>;
  failures: string[];
  evidence: Evidence[];
  steps: StepRecord[];
}

export interface ProofReport {
  schema: typeof REPORT_SCHEMA;
  runId: string;
  generatedAt: string;
  mode: Mode;
  target: { stage: string; clusterName: string; kubeContext: string; accountId?: string; region?: string };
  /** Stated on every report, whatever ran: V1 (HoopDb Postgres) has no transcript search. */
  scope: { v1FullTranscriptSearch: "UNSUPPORTED"; reason: string };
  /**
   * The two search gates and exactly which backend each exercised ("none" when
   * not configured). The proof cannot pass unless both gates pass; an
   * unconfigured transcript backend leaves the verdict incomplete.
   */
  searchGates: {
    metadata: { checks: ["planted-trace", "v1-search-scope"]; backend: string };
    transcript: { checks: ["transcript-search"]; backend: string };
  };
  checks: CheckResult[];
  /**
   * Runs this report aggregates, oldest first. A single run has one phase; the
   * two-phase teardown carries the pre-teardown report forward with
   * --prior-report, and the verdict is computed over the merged checks.
   */
  phases: Phase[];
  verdict: "pass" | "fail" | "incomplete";
}

export interface Phase {
  runId: string;
  generatedAt: string;
  mode: Mode;
  checks: CheckId[];
  /** SHA-256 of the prior report file this phase was merged from (absent for the current run). */
  reportSha256?: string;
}

export const V1_SCOPE_STATEMENT =
  "Full-transcript search is UNSUPPORTED on V1 (HoopDb Postgres): V1 search covers session_directory metadata and the " +
  "scrubbed short_summary only, and full transcripts stay in S3. Transcript search is a separate gate (transcript-search) " +
  "against a V2 backend; without one configured the proof is INCOMPLETE, never PASS.";

/* ------------------------------------------------------------------ sanitize */

const SECRET_SHAPES: Array<[RegExp, string]> = [
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)/g, "[redacted:pem]"],
  [/\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*/g, "[redacted:jwt]"],
  [/\bBearer\s+[A-Za-z0-9._~+/=-]{8,}/gi, "Bearer [redacted]"],
  [/\brak_[0-9a-f]{16,}\b/gi, "[redacted:relayauth-key]"],
  [/\b(AKIA|ASIA)[A-Z0-9]{16}\b/g, "[redacted:aws-key-id]"],
  [/(aws_secret_access_key|secretAccessKey|SecretAccessKey)("?\s*[:=]\s*"?)[A-Za-z0-9/+=]{40}/g, "$1$2[redacted]"],
  [/\b(sk-[A-Za-z0-9_-]{20,}|xox[abpr]-[A-Za-z0-9-]{10,})/g, "[redacted:api-key]"],
  // Any URL userinfo, any scheme: https://user:pass@host, postgres://u:p@h, s3://key@bucket.
  [/\b([a-z][a-z0-9+.-]*):\/\/[^\s/?#@]+@/gi, "$1://[redacted]@"],
  [/((?:password|passwd|secret|token|api[_-]?key)["']?\s*[:=]\s*["']?)[^\s"',}]{6,}/gi, "$1[redacted]"],
  // SUMMARY_MASTER_KEYS entries (`<id>:<64 hex>`). Image digests (`sha256:<64 hex>`) are evidence, not secrets.
  [/\b(?!sha256:|sha512:)[a-z0-9]{2,8}:[0-9a-f]{64}\b/gi, "[redacted:key-material]"],
];

export const sha256 = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");

/** A stable, non-reversible tag for a canary: lets evidence say *which* canary hit without carrying it. */
export const canaryTag = (canary: string) => `[canary:${sha256(canary).slice(0, 12)}]`;

/**
 * Redact secret-shaped values, then every exact secret and canary the run knows
 * about (tokens minted this run, planted transcript text). Longest first, so a
 * canary that contains another is replaced whole.
 */
export function sanitize(text: string, known: { secrets?: readonly string[]; canaries?: readonly string[] } = {}): string {
  let out = text;
  for (const s of [...(known.secrets ?? [])].filter((x) => x.length >= 6).sort((a, b) => b.length - a.length))
    out = out.split(s).join("[redacted:run-secret]");
  for (const c of [...(known.canaries ?? [])].filter((x) => x.length >= 6).sort((a, b) => b.length - a.length))
    out = out.split(c).join(canaryTag(c));
  for (const [re, repl] of SECRET_SHAPES) out = out.replace(re, repl);
  return out;
}

/** Secret shapes still present in already-sanitized text (a report self-check). */
export function secretShapesIn(text: string): string[] {
  const found: string[] = [];
  for (const [re, label] of SECRET_SHAPES) {
    const probe = new RegExp(re.source, re.flags);
    for (const m of text.matchAll(probe)) {
      if (/\[redacted/.test(m[0])) continue;
      found.push(label);
      break;
    }
  }
  return found;
}

const EXCERPT_MAX = 2_000;

export function evidence(label: string, raw: string, known: Parameters<typeof sanitize>[1] = {}, withExcerpt = true): Evidence {
  const clean = sanitize(raw, known);
  return {
    label,
    sha256: sha256(raw),
    bytes: Buffer.byteLength(raw),
    ...(withExcerpt && clean.trim() ? { excerpt: clean.length > EXCERPT_MAX ? `${clean.slice(0, EXCERPT_MAX)}…[truncated]` : clean } : {}),
  };
}

/* ------------------------------------------------------------------ build */

/**
 * fail if any check failed. pass only when every check passed, where
 * v1-search-scope may be `unsupported` (it labels V1, it is not a capability
 * claim) and transcript-search passed against a configured backend. Everything
 * else, an unconfigured transcript backend included, is incomplete.
 */
export function verdictOf(checks: readonly CheckResult[], transcriptBackend: string): ProofReport["verdict"] {
  if (checks.some((c) => c.status === "fail")) return "fail";
  if (transcriptBackend === "none") return "incomplete";
  const complete = CHECK_IDS.every((id) =>
    checks.some((c) => c.check === id && (c.status === "pass" || (id === "v1-search-scope" && c.status === "unsupported"))),
  );
  return complete ? "pass" : "incomplete";
}

export function buildReport(
  input: Omit<ProofReport, "schema" | "scope" | "searchGates" | "verdict" | "generatedAt" | "phases"> & { generatedAt?: string; backends: { metadata: string; transcript: string } },
): ProofReport {
  return {
    schema: REPORT_SCHEMA,
    runId: input.runId,
    generatedAt: input.generatedAt ?? new Date().toISOString(),
    mode: input.mode,
    target: input.target,
    scope: { v1FullTranscriptSearch: "UNSUPPORTED", reason: V1_SCOPE_STATEMENT },
    searchGates: {
      metadata: { checks: ["planted-trace", "v1-search-scope"], backend: input.backends.metadata },
      transcript: { checks: ["transcript-search"], backend: input.backends.transcript },
    },
    checks: input.checks,
    phases: [{ runId: input.runId, generatedAt: input.generatedAt ?? new Date().toISOString(), mode: input.mode, checks: input.checks.map((c) => c.check) }],
    verdict: verdictOf(input.checks, input.backends.transcript),
  };
}

/**
 * Merge a prior run's report into the current one, deterministically: for each
 * check id (in CHECK_IDS order) the current run's result wins, otherwise the
 * prior's is carried. Both runs must target the same stage, cluster, context
 * and observed account; a live merge cannot carry dry-run results; the prior
 * must itself validate and be older. Each search gate's backend comes from the
 * run that produced that gate's checks. The verdict is recomputed over the
 * merged set, so one report can carry the whole proof including both teardown
 * phases.
 */
export function mergeReports(prior: ProofReport, priorSha256: string, current: ProofReport): { report: ProofReport } | { error: string } {
  const issues = validateReport(prior);
  if (issues.length) return { error: `prior report invalid: ${issues[0]}` };
  for (const k of ["stage", "clusterName", "kubeContext", "accountId", "region"] as const)
    if (prior.target[k] !== current.target[k]) return { error: `prior report targets a different ${k}` };
  if (current.mode !== "dry-run" && prior.mode === "dry-run") return { error: "a live or read-only run cannot carry dry-run results" };
  if (prior.generatedAt > current.generatedAt) return { error: "prior report is newer than the current run" };
  if (prior.phases.some((p) => p.runId === current.runId)) return { error: "prior report already contains this run" };
  const ran = new Set(current.checks.map((c) => c.check));
  const checks = CHECK_IDS.flatMap((id) => current.checks.filter((c) => c.check === id).concat(ran.has(id) ? [] : prior.checks.filter((c) => c.check === id)));
  const ranAny = (ids: readonly CheckId[]) => ids.some((id) => ran.has(id));
  const searchGates: ProofReport["searchGates"] = {
    metadata: ranAny(current.searchGates.metadata.checks) ? current.searchGates.metadata : prior.searchGates.metadata,
    transcript: ranAny(current.searchGates.transcript.checks) ? current.searchGates.transcript : prior.searchGates.transcript,
  };
  const phases = [...prior.phases.map((p, i) => (i === prior.phases.length - 1 && !p.reportSha256 ? { ...p, reportSha256: priorSha256 } : p)), ...current.phases];
  return { report: { ...current, checks, searchGates, phases, verdict: verdictOf(checks, searchGates.transcript.backend) } };
}

/* ------------------------------------------------------------------ validate */

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

/** Structural validation plus the no-secrets self-check. Empty array = valid. */
export function validateReport(r: unknown, known: { canaries?: readonly string[]; secrets?: readonly string[] } = {}): string[] {
  const issues: string[] = [];
  if (!r || typeof r !== "object") return ["report is not an object"];
  const rep = r as Partial<ProofReport>;
  if (rep.schema !== REPORT_SCHEMA) issues.push(`schema must be ${REPORT_SCHEMA}`);
  if (typeof rep.runId !== "string" || !rep.runId) issues.push("runId missing");
  if (typeof rep.generatedAt !== "string" || !ISO.test(rep.generatedAt)) issues.push("generatedAt must be ISO-8601 UTC");
  if (!["dry-run", "read-only", "live"].includes(rep.mode as string)) issues.push("mode invalid");
  if (!rep.target || typeof rep.target.clusterName !== "string" || typeof rep.target.kubeContext !== "string") issues.push("target incomplete");
  if (rep.scope?.v1FullTranscriptSearch !== "UNSUPPORTED" || !/UNSUPPORTED/.test(rep.scope?.reason ?? ""))
    issues.push("scope must state V1 full-transcript search is UNSUPPORTED");
  const gates = rep.searchGates;
  if (typeof gates?.metadata?.backend !== "string" || !gates.metadata.backend || typeof gates?.transcript?.backend !== "string" || !gates.transcript.backend)
    issues.push("searchGates must name the backend each search gate exercised (or \"none\")");
  if (gates?.transcript?.backend === "none" && rep.checks?.some((c) => c.check === "transcript-search" && c.status === "pass"))
    issues.push("transcript-search cannot pass with no transcript backend");
  if (!Array.isArray(rep.checks)) issues.push("checks must be an array");
  if (!Array.isArray(rep.phases) || rep.phases.length === 0 || rep.phases.at(-1)?.runId !== rep.runId) issues.push("phases must end with this run");
  const seen = new Set<string>();
  for (const [i, c] of (rep.checks ?? []).entries()) {
    const at = `checks[${i}]`;
    if (!CHECK_IDS.includes(c.check)) issues.push(`${at}.check unknown: ${String(c.check)}`);
    if (seen.has(c.check)) issues.push(`${at}.check duplicated: ${c.check}`);
    seen.add(c.check);
    if (!STATUSES.includes(c.status)) issues.push(`${at}.status invalid`);
    if (!ISO.test(c.startedAt ?? "") || !ISO.test(c.finishedAt ?? "")) issues.push(`${at} timestamps must be ISO-8601 UTC`);
    if (c.startedAt > c.finishedAt) issues.push(`${at} finishedAt precedes startedAt`);
    if (!Array.isArray(c.failures) || !Array.isArray(c.evidence) || !Array.isArray(c.steps)) issues.push(`${at} failures/evidence/steps must be arrays`);
    if (c.status === "fail" && !c.failures?.length) issues.push(`${at} fails without a stated failure`);
    if (c.status === "pass" && c.failures?.length) issues.push(`${at} passes with failures recorded`);
    if (c.status === "unsupported" && c.check !== "v1-search-scope") issues.push(`${at} only v1-search-scope may be unsupported`);
    for (const e of c.evidence ?? []) if (!/^[0-9a-f]{64}$/.test(e.sha256) || !Number.isInteger(e.bytes)) issues.push(`${at} evidence '${e.label}' lacks sha256/bytes`);
  }
  if (rep.checks && gates?.transcript?.backend !== undefined) {
    const want = verdictOf(rep.checks, gates.transcript.backend);
    if (rep.verdict !== want) issues.push(`verdict must be ${want}`);
  }
  const text = JSON.stringify(r);
  for (const shape of secretShapesIn(text)) issues.push(`secret-shaped value in report: ${shape}`);
  for (const s of known.secrets ?? []) if (s.length >= 6 && text.includes(s)) issues.push("a run secret appears in the report");
  for (const c of known.canaries ?? []) if (c.length >= 6 && text.includes(c)) issues.push(`planted canary text appears in the report ${canaryTag(c)}`);
  return issues;
}
