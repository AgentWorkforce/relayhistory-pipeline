/**
 * Memory documents, the extractor (docs/memory-documents.md): the decisions of one completed
 * session analysis become decision records with evidence, written through
 * sessions.decision_records_insert_v1 (migration 20261105062080).
 *
 * Off unless SUMMARY_DECISION_RECORDS_ENABLED is exactly "true". Off, the analysis tick gets no
 * sink: no extractor call, no database call, no log line.
 *
 * Pure apart from the sink. Reads only the decisions (`understanding.decisions`), the evidence
 * references' id, source, session id and order, and `metrics.filesChanged` paths. The references'
 * `summary` and `detail` (transcript excerpts) are never read. A decision whose text the scrubber
 * would change in any way is not recorded at all, so no stored string ever holds a redaction or a
 * secret-like value. Whitespace (including line breaks) collapses to one space.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { cutAt } from "../bob-costas/summarizer.ts";
import { scrubText } from "../shared/vendor.ts";
import type { SessionAnalysisSnapshot } from "./engine/session-analysis-result.ts";
import type { AnalysisJob, AnalysisJobOutput } from "./processor.ts";

export const DECISION_TITLE_MAX = 200;
export const DECISION_STATEMENT_MAX = 1000;
export const DECISION_CLAIM_MAX = 300;
/** Per call of decision_records_insert_v1. */
export const DECISION_BATCH_MAX = 100;
/** Evidence rows per record and call. */
export const DECISION_EVIDENCE_MAX = 10;
const TOPIC_MAX = 64;
const SESSION_ID_MAX = 512;
const TURN_MAX = 1_000_000;
const SOURCE = /^[a-z][a-z0-9_-]{0,31}$/u;
const GITHUB_REF =
  /https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/(?:commit\/[0-9a-f]{7,40}|pull\/[0-9]{1,10}|issues\/[0-9]{1,10})(?![A-Za-z0-9_/-]|\.[A-Za-z0-9])/u;
/** Directory names a repository's code usually sits under; an absolute path is anchored at the first one. */
const SOURCE_ROOTS = new Set(["src", "packages", "apps", "lib", "libs", "services", "crates", "cmd", "internal", "scripts", "docs", "supabase", "flows", "test", "tests", "e2e", "infra"]);

/** SUMMARY_DECISION_RECORDS_ENABLED, "true" only. */
export function decisionRecordsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.SUMMARY_DECISION_RECORDS_ENABLED === "true";
}

export interface DecisionEvidence {
  source: string;
  session_id: string;
  turn_start: number;
  turn_end: number;
  claim: string;
  ref: string | null;
}

export interface DecisionRecordInput {
  topic: string;
  title: string;
  statement: string;
  status: "decided";
  evidence: DecisionEvidence[];
}

const collapse = (text: string) => text.replace(/\s+/gu, " ").trim();

/** The same normalization as the table's title_key: lowercase, whitespace collapsed, trailing punctuation removed. */
export function decisionTitleKey(title: string): string {
  return collapse(title.toLowerCase()).replace(/[\p{P}\p{S}\s]+$/u, "");
}

/** True when the scrubber leaves `text` exactly as it is (and does not refuse it). */
function scrubClean(text: string): boolean {
  try {
    return scrubText(text) === text;
  } catch {
    return false;
  }
}

/**
 * The topic slug: the first two directory segments of the most-edited file, relative to the
 * repository. A relative path starts at its first segment; an absolute one at its first known
 * source root (src, packages, ...), so no home directory or user name becomes a topic. Else "general".
 */
export function decisionTopic(files: readonly { path: string; editCount: number }[]): string {
  let best: { path: string; editCount: number } | undefined;
  for (const file of files) if (!best || file.editCount > best.editCount) best = file;
  if (!best) return "general";
  const raw = best.path.replace(/\\/gu, "/");
  const absolute = /^(?:\/|~|[A-Za-z]:\/)/u.test(raw);
  let parts = raw.split("/").filter((p) => p !== "" && p !== ".");
  if (parts.some((p) => p === ".." || p === "~")) return "general";
  if (absolute) {
    const root = parts.findIndex((p) => SOURCE_ROOTS.has(p.toLowerCase()));
    if (root < 0) return "general";
    parts = parts.slice(root);
  }
  // Directories only: the file name is never part of the topic.
  const dirs = parts.slice(0, -1).slice(0, 2);
  const slug = dirs
    .join("-")
    .toLowerCase()
    .replace(/[^a-z0-9-]+/gu, "-")
    .replace(/-{2,}/gu, "-")
    .replace(/^-+|-+$/gu, "");
  const cut = slug.slice(0, TOPIC_MAX).replace(/-+$/u, "");
  return /^[a-z0-9][a-z0-9-]{0,63}$/u.test(cut) ? cut : "general";
}

/** The first sentence, at most 200 characters, never empty. */
function firstSentence(text: string): string {
  const match = /^(.+?[.!?])(?=\s|$)/u.exec(text);
  return cutAt(match ? match[1]! : text, DECISION_TITLE_MAX).trim();
}

/**
 * The decision records of one analysis snapshot, for the job's own workspace. Only decisions
 * with high or medium confidence and at least one cited reference; at most 100 records of at
 * most 10 evidence rows, and at most one evidence row per session per record. Two decisions with
 * the same topic and title key become one record. Dedupe is per batch here and on the exact
 * (source, session, range) in decision_records_insert_v1, so a re-analysis whose spans shift can
 * still add rows (up to the 50-row cap), and reworded titles give a new record
 * (docs/memory-documents.md, Deduplication limits).
 */
export function extractDecisionRecords(snapshot: SessionAnalysisSnapshot): DecisionRecordInput[] {
  const references = snapshot.evidence?.references ?? [];
  // 0-based position of each reference among the references of its own session.
  const position = new Map<string, { source: string; sessionId: string; index: number }>();
  const perSession = new Map<string, number>();
  for (const reference of references) {
    const index = perSession.get(reference.sessionId) ?? 0;
    perSession.set(reference.sessionId, index + 1);
    if (!position.has(reference.id)) position.set(reference.id, { source: reference.source, sessionId: reference.sessionId, index });
  }
  const topic = decisionTopic(snapshot.metrics?.filesChanged ?? []);
  const records = new Map<string, DecisionRecordInput>();
  for (const decision of snapshot.understanding?.decisions ?? []) {
    if (records.size >= DECISION_BATCH_MAX) break;
    if (decision.confidence !== "high" && decision.confidence !== "medium") continue;
    if (typeof decision.text !== "string") continue;
    const statementFull = collapse(decision.text);
    if (!statementFull) continue;
    const statement = cutAt(statementFull, DECISION_STATEMENT_MAX).trim();
    const title = firstSentence(statement);
    const claim = cutAt(statement, DECISION_CLAIM_MAX).trim();
    // The whole text and every stored cut must pass the scrubber unchanged.
    if (![statementFull, statement, title, claim].every(scrubClean)) continue;
    const refMatch = GITHUB_REF.exec(statementFull);
    const ref = refMatch && refMatch[0].length <= 500 ? refMatch[0] : null;
    // One evidence row per cited session: its lowest and highest cited position.
    const spans = new Map<string, DecisionEvidence>();
    for (const id of decision.evidenceIds ?? []) {
      const at = position.get(id);
      if (!at) continue;
      if (!SOURCE.test(at.source)) continue;
      if (!at.sessionId || at.sessionId.length > SESSION_ID_MAX || /[\r\n]/u.test(at.sessionId)) continue;
      if (at.index > TURN_MAX) continue;
      const key = `${at.source}\u0000${at.sessionId}`;
      const span = spans.get(key);
      if (span) {
        span.turn_start = Math.min(span.turn_start, at.index);
        span.turn_end = Math.max(span.turn_end, at.index);
      } else if (spans.size < DECISION_EVIDENCE_MAX) {
        spans.set(key, { source: at.source, session_id: at.sessionId, turn_start: at.index, turn_end: at.index, claim, ref });
      }
    }
    if (spans.size === 0) continue;
    const dedupe = `${topic}\u0000${decisionTitleKey(title)}`;
    const existing = records.get(dedupe);
    if (existing) {
      // At most one evidence row per session per record in a batch: a second decision with the
      // same title widens that session's span instead of adding a row for a shifted range.
      for (const span of spans.values()) {
        const same = existing.evidence.find((e) => e.source === span.source && e.session_id === span.session_id);
        if (same) {
          same.turn_start = Math.min(same.turn_start, span.turn_start);
          same.turn_end = Math.max(same.turn_end, span.turn_end);
        } else if (existing.evidence.length < DECISION_EVIDENCE_MAX) {
          existing.evidence.push(span);
        }
      }
      continue;
    }
    records.set(dedupe, { topic, title, statement, status: "decided", evidence: [...spans.values()] });
  }
  return [...records.values()];
}

export interface DecisionRecordsWrite {
  inserted: number;
  merged: number;
  evidence_added: number;
}

/** Writes one workspace's records; nothing is called for an empty list. */
export type DecisionRecordsSink = (
  scope: { orgId: string; workspaceId: string },
  records: DecisionRecordInput[],
) => Promise<DecisionRecordsWrite | null>;

/** The sink over the worker's HoopDb connection (service_role or the runtime role). */
export function postgresDecisionRecordsSink(db: SqlExecutor): DecisionRecordsSink {
  return async (scope, records) => {
    if (records.length === 0) return null;
    const { rows } = await db.query<DecisionRecordsWrite>(
      "SELECT inserted, merged, evidence_added FROM sessions.decision_records_insert_v1($1, $2, $3::jsonb)",
      [scope.orgId, scope.workspaceId, JSON.stringify(records)],
    );
    return rows[0] ?? null;
  };
}

/** Extract and write; returns the number of records sent (0 = nothing written, no call). */
export async function recordDecisions(
  sink: DecisionRecordsSink,
  scope: { orgId: string; workspaceId: string },
  snapshot: SessionAnalysisSnapshot,
): Promise<number> {
  const records = extractDecisionRecords(snapshot);
  if (records.length === 0) return 0;
  await sink(scope, records);
  return records.length;
}

/**
 * The analysis tick's completion hook: records the decisions of a job that has just completed,
 * in that job's own workspace. Counts `decision_records` (records sent) and
 * `decision_records_failed`; never throws, never logs.
 */
export function decisionRecordsHook(
  sink: DecisionRecordsSink,
  counts: Record<string, number>,
): (job: AnalysisJob, output: AnalysisJobOutput) => Promise<void> {
  return async (job, output) => {
    try {
      const sent = await recordDecisions(sink, { orgId: job.orgId, workspaceId: job.workspaceId }, output.result.analysis);
      counts.decision_records = (counts.decision_records ?? 0) + sent;
    } catch {
      counts.decision_records_failed = (counts.decision_records_failed ?? 0) + 1;
    }
  };
}
