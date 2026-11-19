/**
 * Relayfacts claims (Session Intelligence M1-2, docs/relayfacts.md, Claims): the constraints and intents
 * of one completed session analysis become claims with a scope and the time the claim was made, written
 * through sessions.relayfacts_claims_insert_v1 into their own tables (migration 20261105062150).
 * Decisions are not claims here: they keep going through decision-records.ts and the untouched
 * relayfacts_insert_v1, with or without this flag.
 *
 * Off unless SESSION_INTEL_CLAIMS_ENABLED is exactly "true", inside SUMMARY_DECISION_RECORDS_ENABLED
 * (the outer gate). Off, the analysis tick runs decision-records.ts exactly as before and nothing here.
 *
 * Every rule of decision-records.ts holds for every kind (cutClaim): confidence high or medium, at
 * least one cited reference, scrub-clean text only, the same bounds and the same topic. The references'
 * `summary` and `detail` are never read; scope reads only the claim's own text and the cited references'
 * kind and session.
 *
 * Runs after the job has completed, outside every job and summary transaction (runAnalysisJob's
 * onCompleted). Its failures are counted and contained.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { log } from "../shared/log.ts";
import type { SessionIntelControlsReader } from "../shared/session-intel-controls.ts";
import type { SessionAnalysisSnapshot } from "./engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "./processor.ts";
import {
  type CitedReference,
  cutClaim,
  DECISION_BATCH_MAX,
  type DecisionEvidence,
  type DecisionRecordsWrite,
  decisionTitleKey,
  decisionTopic,
  mergeSpans,
  referencePositions,
} from "./decision-records.ts";

/** SESSION_INTEL_CLAIMS_ENABLED, "true" only. */
export function sessionIntelClaimsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.SESSION_INTEL_CLAIMS_ENABLED === "true";
}

export type ClaimKind = "intent" | "constraint";
export type ClaimScope = "session" | "project" | "repo";

/** Batch order: constraints, then intents, up to 100 claims in all. */
export const CLAIM_KINDS: readonly { kind: ClaimKind; field: "constraints" | "intents" }[] = [
  { kind: "constraint", field: "constraints" },
  { kind: "intent", field: "intents" },
];

const SCOPE_RANK: Record<ClaimScope, number> = { session: 0, project: 1, repo: 2 };

export interface ClaimRecordInput {
  kind: ClaimKind;
  scope: ClaimScope;
  observed_at: string | null;
  topic: string;
  title: string;
  statement: string;
  status: "decided";
  evidence: DecisionEvidence[];
}

export interface ScopeInput {
  /** The claim text, whitespace collapsed (already scrub-clean). */
  text: string;
  /** The references the claim's evidence rows cite. */
  cited: readonly CitedReference[];
  /** The analysed (root) session. */
  root: { source: string; sessionId: string };
}

/** A repository instruction file named in the claim text. */
const INSTRUCTION_FILE =
  /(?<![\w.-])(?:AGENTS\.md|CLAUDE\.md|CONTRIBUTING\.md|GEMINI\.md|\.cursorrules|\.windsurfrules|\.clinerules|\.cursor\/rules|copilot-instructions\.md)(?![\w-])/iu;
/** The text calls itself a repository or project rule. */
const REPO_RULE =
  /\b(?:repo|repository|project)(?:'s|-wide| wide)?\s+(?:rules?|polic(?:y|ies)|conventions?|instructions?|guidelines?|standing orders?)\b/iu;
/** The text says it applies beyond this session. */
const BEYOND_SESSION =
  /\b(?:in this (?:repo|repository|project|codebase)|for everyone|for all (?:agents|sessions|contributors)|always|never|from now on|going forward|across the (?:repo|repository|project|codebase))\b/iu;
/** The text limits itself to this session or task. */
const SESSION_LIMIT =
  /\b(?:this session|this task|this branch|my branch|for now|temporarily|just this once|this pr|this pull request|this ticket)\b/iu;

const fromRoot = (c: ScopeInput) =>
  c.cited.some((r) => r.source === c.root.source && r.sessionId === c.root.sessionId);

/**
 * Scope rules, first match wins, else session. Conservative: any ambiguity is session (no
 * propagation). Repo needs the text itself to name an instruction file or a repository rule: the
 * evidence references carry no file path the extractor may read, so an instruction file the session
 * only loaded is not seen here (M1-4).
 */
export const SCOPE_RULES: readonly { scope: ClaimScope; when: string; test: (c: ScopeInput) => boolean }[] = [
  { scope: "session", when: "no cited reference is from the analysed (root) session", test: (c) => !fromRoot(c) },
  { scope: "session", when: "a cited reference is a delegated (child) instruction", test: (c) => c.cited.some((r) => r.kind === "delegated-instruction") },
  { scope: "session", when: "the text limits itself to this session, task or branch", test: (c) => SESSION_LIMIT.test(c.text) },
  { scope: "repo", when: "the text names a repository instruction file", test: (c) => INSTRUCTION_FILE.test(c.text) },
  { scope: "repo", when: "the text calls itself a repository or project rule", test: (c) => REPO_RULE.test(c.text) },
  {
    scope: "project",
    when: "a cited root human message and the text says it applies beyond this session",
    test: (c) =>
      c.cited.some((r) => r.kind === "human-message" && r.source === c.root.source && r.sessionId === c.root.sessionId) &&
      BEYOND_SESSION.test(c.text),
  },
];

export function claimScope(input: ScopeInput): ClaimScope {
  return SCOPE_RULES.find((rule) => rule.test(input))?.scope ?? "session";
}

/** The earliest an observed_at may be (1970-01-01T00:00:00Z) and how far ahead of now it may run (one day). */
export const OBSERVED_AT_MIN_MS = 0;
export const OBSERVED_AT_MAX_AHEAD_MS = 24 * 3600 * 1000;

/**
 * The earliest parseable `at` among the cited references, as ISO 8601; null if none parses. `undefined` when that
 * earliest time is outside 1970-01-01 .. now + 1 day (a bad timestamp): the caller drops the claim, so one bad
 * reference never fails the batch.
 */
export function claimObservedAt(cited: readonly CitedReference[], nowMs: number = Date.now()): string | null | undefined {
  let earliest: number | null = null;
  for (const r of cited) {
    if (r.at === null) continue;
    const ms = Date.parse(r.at);
    if (Number.isFinite(ms) && (earliest === null || ms < earliest)) earliest = ms;
  }
  if (earliest === null) return null;
  if (earliest < OBSERVED_AT_MIN_MS || earliest > nowMs + OBSERVED_AT_MAX_AHEAD_MS) return undefined;
  return new Date(earliest).toISOString();
}

/**
 * The claim records of one snapshot: constraints and intents, deduplicated per batch on
 * (kind, topic, title key). A repeated claim widens its evidence as decision-records.ts does, its
 * scope only widens and its observed_at only moves earlier.
 */
export function extractClaimRecords(snapshot: SessionAnalysisSnapshot, nowMs: number = Date.now()): ClaimRecordInput[] {
  return extractClaims(snapshot, nowMs).records;
}

/** extractClaimRecords, plus the number of claims dropped for a bad timestamp (claimObservedAt undefined). */
export function extractClaims(snapshot: SessionAnalysisSnapshot, nowMs: number = Date.now()): { records: ClaimRecordInput[]; badTime: number } {
  let badTime = 0;
  const position = referencePositions(snapshot);
  const topic = decisionTopic(snapshot.metrics?.filesChanged ?? []);
  const root = { source: snapshot.source, sessionId: snapshot.sessionId };
  const records = new Map<string, ClaimRecordInput>();
  for (const { kind, field } of CLAIM_KINDS) {
    for (const claim of snapshot.understanding?.[field] ?? []) {
      if (records.size >= DECISION_BATCH_MAX) break;
      const cut = cutClaim(claim, position);
      if (!cut) continue;
      const scope = claimScope({ text: cut.statement, cited: cut.cited, root });
      const observed = claimObservedAt(cut.cited, nowMs);
      if (observed === undefined) { badTime += 1; continue; }
      const dedupe = `${kind}\u0000${topic}\u0000${decisionTitleKey(cut.title)}`;
      const existing = records.get(dedupe);
      if (existing) {
        mergeSpans(existing.evidence, cut.spans);
        if (SCOPE_RANK[scope] > SCOPE_RANK[existing.scope]) existing.scope = scope;
        if (observed !== null && (existing.observed_at === null || observed < existing.observed_at)) existing.observed_at = observed;
        continue;
      }
      records.set(dedupe, {
        kind, scope, observed_at: observed, topic, title: cut.title, statement: cut.statement, status: "decided", evidence: cut.spans,
      });
    }
  }
  return { records: [...records.values()], badTime };
}

/** One current record from relayfacts_claims_v1, as the supersession step reads it. */
export interface CurrentClaim {
  id: string;
  kind: ClaimKind;
  title: string;
  supersedes: string | null;
  evidence: { source: string; session_id: string; created_at: string }[];
}

/** relayfacts_claims_v1's row cap; a full page means the topic is too large to judge, so nothing is superseded. */
export const CLAIMS_READ_MAX = 500;

export interface ClaimsPort {
  /** The M3-1 'claims' control for the workspace; false on any doubt (the shared reader fails closed). */
  enabled(scope: { orgId: string; workspaceId: string }): Promise<boolean>;
  insert(scope: { orgId: string; workspaceId: string }, records: ClaimRecordInput[]): Promise<DecisionRecordsWrite | null>;
  current(scope: { orgId: string; workspaceId: string }, filter: { topic: string; kind: ClaimKind }): Promise<CurrentClaim[]>;
  supersede(scope: { orgId: string; workspaceId: string }, oldId: string, newId: string): Promise<boolean>;
}

/** The port over the worker's HoopDb connection (service_role or the runtime role) and the controls reader. */
export function postgresClaimsPort(db: SqlExecutor, controls: SessionIntelControlsReader): ClaimsPort {
  return {
    async enabled(scope) {
      try { return (await controls.read(scope.orgId, scope.workspaceId)).get("claims") === true; }
      catch { return false; }
    },
    async insert(scope, records) {
      if (records.length === 0) return null;
      const { rows } = await db.query<DecisionRecordsWrite>(
        "SELECT inserted, merged, evidence_added FROM sessions.relayfacts_claims_insert_v1($1, $2, $3::jsonb)",
        [scope.orgId, scope.workspaceId, JSON.stringify(records)],
      );
      return rows[0] ?? null;
    },
    async current(scope, filter) {
      const { rows } = await db.query<CurrentClaim>(
        "SELECT id, kind, title, supersedes, evidence FROM sessions.relayfacts_claims_v1($1, $2, $3, $4, NULL, $5)",
        [scope.orgId, scope.workspaceId, filter.topic, filter.kind, CLAIMS_READ_MAX],
      );
      return rows;
    },
    async supersede(scope, oldId, newId) {
      const { rows } = await db.query<{ ok: boolean }>(
        "SELECT sessions.relayfacts_claims_supersede_v1($1, $2, $3::uuid, $4::uuid) AS ok",
        [scope.orgId, scope.workspaceId, oldId, newId],
      );
      return rows[0]?.ok === true;
    },
  };
}

/**
 * Supersession, deliberately narrow (docs/relayfacts.md, Claims), through relayfacts_claims_supersede_v1;
 * decisions are never superseded here. For each (kind, topic)
 * of the snapshot with exactly one record R, an earlier record O is superseded by R only when:
 *   - O is current (not superseded) and of the same kind and topic, and R supersedes nothing yet;
 *   - every evidence row of O is from this snapshot's own (source, session), so no other session holds it;
 *   - every evidence row of O was written before this snapshot's analyzedAt (O comes from an older
 *     analysis, not a newer one whose hook ran first);
 *   - O's title is not in this snapshot (it is no longer claimed);
 *   - O is the only such record (no guess between several).
 * Anything else, including an unparseable analyzedAt or a full read page, supersedes nothing.
 * Returns the number of records superseded.
 */
export async function supersedeDroppedClaims(
  port: ClaimsPort,
  scope: { orgId: string; workspaceId: string },
  snapshot: SessionAnalysisSnapshot,
  records: readonly ClaimRecordInput[],
): Promise<number> {
  const analyzedMs = Date.parse(snapshot.analyzedAt);
  if (!Number.isFinite(analyzedMs) || !snapshot.source || !snapshot.sessionId) return 0;
  const groups = new Map<string, ClaimRecordInput[]>();
  for (const r of records) {
    const key = `${r.kind}\u0000${r.topic}`;
    groups.set(key, [...(groups.get(key) ?? []), r]);
  }
  let superseded = 0;
  for (const group of groups.values()) {
    if (group.length !== 1) continue;
    const record = group[0]!;
    const current = await port.current(scope, { topic: record.topic, kind: record.kind });
    if (current.length >= CLAIMS_READ_MAX) continue;
    const key = decisionTitleKey(record.title);
    const fresh = current.filter((c) => c.kind === record.kind && decisionTitleKey(c.title) === key);
    if (fresh.length !== 1 || fresh[0]!.supersedes !== null) continue;
    const replaced = current.filter(
      (c) =>
        c.kind === record.kind &&
        c.id !== fresh[0]!.id &&
        decisionTitleKey(c.title) !== key &&
        c.evidence.length > 0 &&
        c.evidence.every((e) => {
          const written = Date.parse(e.created_at);
          return e.source === snapshot.source && e.session_id === snapshot.sessionId && Number.isFinite(written) && written < analyzedMs;
        }),
    );
    if (replaced.length !== 1) continue;
    if (await port.supersede(scope, replaced[0]!.id, fresh[0]!.id)) superseded += 1;
  }
  return superseded;
}

/**
 * The claims part of the analysis tick's completion hook with SESSION_INTEL_CLAIMS_ENABLED (the worker runs
 * it after decisionRecordsHook, which is unchanged): writes the job's claims in the job's own workspace,
 * then supersedes dropped claims. Before any write it reads the M3-1 'claims' control for that workspace,
 * fail-closed: off, or any doubt, writes nothing and counts `claim_records_control_off`. A claim whose earliest
 * cited time is out of range is dropped alone (`claim_records_bad_time`, one warning line with the count). Counts
 * `claim_records` (claims sent), `claim_records_fenced` (RH016, an erased session), `claim_records_failed`,
 * `claim_records_superseded` and `claim_supersede_failed`; never throws.
 */
export function claimsHook(
  port: ClaimsPort,
  counts: Record<string, number>,
): (job: AnalysisJob, output: AnalysisJobOutput) => Promise<void> {
  return async (job, output) => {
    const scope = { orgId: job.orgId, workspaceId: job.workspaceId };
    const snapshot = output.result.analysis;
    let records: ClaimRecordInput[];
    try {
      const extracted = extractClaims(snapshot);
      records = extracted.records;
      if (extracted.badTime > 0) {
        counts.claim_records_bad_time = (counts.claim_records_bad_time ?? 0) + extracted.badTime;
        log.warn("claim records dropped", { stage: "claims", reason: "observed_at_out_of_range", count: extracted.badTime });
      }
      if (records.length === 0) return;
      let enabled = false;
      try { enabled = await port.enabled(scope); } catch { enabled = false; }
      if (!enabled) {
        counts.claim_records_control_off = (counts.claim_records_control_off ?? 0) + 1;
        return;
      }
      await port.insert(scope, records);
      counts.claim_records = (counts.claim_records ?? 0) + records.length;
    } catch (e) {
      const key = (e as { code?: unknown } | null)?.code === "RH016" ? "claim_records_fenced" : "claim_records_failed";
      counts[key] = (counts[key] ?? 0) + 1;
      return;
    }
    try {
      const n = await supersedeDroppedClaims(port, scope, snapshot, records);
      if (n > 0) counts.claim_records_superseded = (counts.claim_records_superseded ?? 0) + n;
    } catch {
      counts.claim_supersede_failed = (counts.claim_supersede_failed ?? 0) + 1;
    }
  };
}
