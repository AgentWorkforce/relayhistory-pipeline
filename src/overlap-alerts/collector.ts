import { type Alert, type Collection, type Dataset, type Judge, hash, sessionKey } from "./contracts.ts";
import { findCandidates } from "./candidates.ts";
import { evidenceAt } from "./evidence.ts";
import { validateVerdict } from "./judgment.ts";

export interface CollectOptions { asOf: string; mode: "retrospective" | "replay"; lookbackHours: number; maxPairs: number }
/** No delivery dependency. Deterministic IDs let a separate consumer deduplicate candidates. */
export async function collectAlerts(dataset: Dataset, judge: Judge, options: CollectOptions): Promise<Collection> {
  if (!Number.isInteger(options.maxPairs) || options.maxPairs < 1 || options.maxPairs > 200) throw new Error("invalid_pair_budget");
  const { sessions, unknownAvailability } = evidenceAt(dataset, options.asOf, options.mode, options.lookbackHours);
  const candidates = findCandidates(dataset.scope, sessions, Date.parse(options.asOf) - 6 * 3600000);
  const result: Collection = {
    version: 1, scope: dataset.scope, datasetFingerprint: hash(dataset), detectorVersion: judge.version,
    asOf: options.asOf, mode: options.mode, expiresAt: dataset.provenance.expiresAt,
    coverage: { sourceComplete: dataset.provenance.complete && sessions.every(s => s.complete), eligibleSessions: sessions.length,
      candidatePairs: candidates.length, assessedPairs: 0, omittedPairs: Math.max(0, candidates.length - options.maxPairs), unknownAvailability, failedPairs: 0, limitedContextPairs: 0 },
    alerts: [], assessments: [],
  };
  for (const c of candidates.slice(0, options.maxPairs)) {
    c.asOf = options.asOf;
    const keys: [string, string] = [sessionKey(c.left), sessionKey(c.right)];
    if (judge.contextLimits && [c.left, c.right].some(s => s.evidence.length > judge.contextLimits!.eventsPerSession ||
        s.evidence.some(e => e.text.length > judge.contextLimits!.charsPerEvent))) result.coverage.limitedContextPairs++;
    let verdict;
    try { verdict = validateVerdict(await judge.assess(c), c); }
    catch {
      result.coverage.failedPairs++;
      verdict = { decision: "uncertain" as const, kind: null, confidence: "low" as const, reason: "Assessment failed or returned ungrounded evidence.", citations: [] };
    }
    result.coverage.assessedPairs++;
    result.assessments.push({ pairId: c.id, sessionKeys: keys, verdict });
    if (verdict.decision !== "suggest" || verdict.confidence === "low" || !verdict.kind) continue;
    const evidenceFingerprint = hash([c.left.evidence, c.right.evidence]);
    const alert: Alert = { id: hash([c.id, verdict.kind]), pairId: c.id, scope: dataset.scope,
      sessionKeys: keys, userIds: [c.left.userId, c.right.userId], kind: verdict.kind, confidence: verdict.confidence,
      reason: verdict.reason, citations: verdict.citations, evidenceFingerprint, evaluatedAt: options.asOf,
      expiresAt: dataset.provenance.expiresAt, status: "candidate" };
    result.alerts.push(alert);
  }
  return result;
}
