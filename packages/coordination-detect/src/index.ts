/** Pure, deterministic coordination detectors. No I/O, clocks, or dependencies. */

export type FindingKind = "duplicate_work" | "concurrent_edit" | "superseded_work" | "repeated_investigation" | "stale_base";
export type Confidence = "high" | "medium" | "low";

export interface TimestampedWrite { path: string; ts: string; eventId: string; kind: string; branch?: string | null }
export interface TimestampedUsage { ts: string; eventId: string; tokens: number | null; costUsdMicros: number | null }
export interface CommitFact { sha: string; ts: string; eventId: string; reverted: boolean | null }
export interface OutcomeFact { key: string; ts: string; eventId: string; reverted: boolean | null }

export interface LaneFacts {
  sessionId: string;
  source: string;
  userId: string | null;
  projectId: string | null;
  repo: string | null;
  branch: string | null;
  taskRef: string | null;
  title: string | null;
  /** An explicit same-source parent. Null means unknown or a root. */
  parentSessionId: string | null;
  startedAt: string;
  lastActionAt: string;
  writes: TimestampedWrite[];
  commits: CommitFact[];
  outcomes: OutcomeFact[];
  usage: TimestampedUsage[];
}

export interface DetectionInput {
  workspaceId: string;
  window: { since: string; until: string };
  lanes: LaneFacts[];
}

export interface SharedWriteParams {
  proximityMs?: number;
  hotPathLaneLimit?: number;
}

export interface OutcomeFactsParams {
  migrationWindowMs?: number;
  outcomeWindowMs?: number;
  weakWriteWindowMs?: number;
  hotPathLaneLimit?: number;
}

export interface FindingLane {
  sessionId: string; source: string; userId: string | null; projectId: string | null;
  branch: string | null; title: string | null; startedAt: string; lastActionAt: string;
  role: "first" | "second";
}
export interface Finding {
  id: string;
  kind: FindingKind;
  score: number;
  confidence: Confidence;
  lanes: FindingLane[];
  artifacts: Array<{
    type: "file" | "branch" | "task" | "commit" | "pull_request" | "migration";
    value: string;
    firstTouchedAt: Record<string, string>;
  }>;
  alertAt: string;
  harmAt: string | null;
  leadTimeMs: number | null;
  communication: "observed" | "not_observed" | "unknown";
  estimatedDuplicateCost: { usdMicros: number | null; tokens: number | null; basis: string };
  evidence: Array<{ sessionId: string; eventId: string; ts: string; kind: string }>;
  explanation: string;
}

export interface DetectorOutput {
  detector: { name: string; version: string; paramsHash: string };
  summary: {
    findingCount: number;
    lanesInvolved: number;
    byKind: Partial<Record<FindingKind, number>>;
    estimatedDuplicateCostUsdMicros: number | null;
    estimatedDuplicateTokens: number | null;
  };
  findings: Finding[];
}

export interface Detector<P = unknown> {
  readonly name: string;
  readonly version: string;
  detect(input: DetectionInput, params?: P, limit?: number): DetectorOutput;
}

const DEFAULT_PROXIMITY_MS = 60 * 60 * 1_000;
const DEFAULT_HOT_PATH_LANE_LIMIT = 8;
const DEFAULT_MIGRATION_WINDOW_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_OUTCOME_WINDOW_MS = 72 * 60 * 60 * 1_000;
const DEFAULT_WEAK_WRITE_WINDOW_MS = 48 * 60 * 60 * 1_000;
const textCompare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const laneKey = (lane: Pick<LaneFacts, "source" | "sessionId">) => `${lane.source}\u0000${lane.sessionId}`;

/** 64-bit FNV-1a. The contract needs a stable hex hash, not a cryptographic identity. */
export function stableHex(parts: unknown): string {
  const bytes = new TextEncoder().encode(JSON.stringify(parts));
  let value = 0xcbf29ce484222325n;
  for (const byte of bytes) {
    value ^= BigInt(byte);
    value = BigInt.asUintN(64, value * 0x100000001b3n);
  }
  return value.toString(16).padStart(16, "0");
}

function roots(lanes: LaneFacts[]): Map<string, string> {
  const byKey = new Map(lanes.map((lane) => [laneKey(lane), lane]));
  const out = new Map<string, string>();
  for (const lane of lanes) {
    const start = laneKey(lane);
    let current = lane;
    let resolved = start;
    const seen = new Set<string>();
    for (;;) {
      const key = laneKey(current);
      resolved = key;
      if (seen.has(key)) {
        resolved = [...seen].sort(textCompare)[0]!; // a malformed cycle is one root, never independent lanes
        break;
      }
      seen.add(key);
      if (!current.parentSessionId) break;
      const parentKey = `${current.source}\u0000${current.parentSessionId}`;
      const parent = byKey.get(parentKey);
      if (!parent) {
        resolved = parentKey; // known parent outside the requested activity window
        break;
      }
      current = parent;
    }
    out.set(start, resolved);
  }
  return out;
}

const asLane = (lane: LaneFacts, role: "first" | "second"): FindingLane => ({
  sessionId: lane.sessionId, source: lane.source, userId: lane.userId, projectId: lane.projectId,
  branch: lane.branch, title: lane.title, startedAt: lane.startedAt, lastActionAt: lane.lastActionAt, role,
});

function duplicateCost(lane: LaneFacts, alertAt: string) {
  const rows = lane.usage.filter((u) => u.ts >= alertAt && u.ts <= lane.lastActionAt);
  const tokensKnown = rows.length > 0 && rows.every((u) => u.tokens !== null);
  const costKnown = rows.length > 0 && rows.every((u) => u.costUsdMicros !== null);
  const tokens = tokensKnown ? rows.reduce((n, u) => n + u.tokens!, 0) : null;
  const usdMicros = costKnown ? rows.reduce((n, u) => n + u.costUsdMicros!, 0) : null;
  const unknown: string[] = [];
  if (!tokensKnown) unknown.push("tokens");
  if (!costKnown) unknown.push("cost");
  return {
    usdMicros,
    tokens,
    basis: unknown.length
      ? `later lane usage from alertAt through lastActionAt; ${unknown.join(" and ")} unknown`
      : "later lane usage from alertAt through lastActionAt",
  };
}

function summarized(findings: Finding[]): DetectorOutput["summary"] {
  const lanes = new Set<string>();
  const byKind: Partial<Record<FindingKind, number>> = {};
  let cost = 0, tokens = 0, costKnown = true, tokensKnown = true;
  for (const finding of findings) {
    for (const lane of finding.lanes) lanes.add(`${lane.source}\u0000${lane.sessionId}`);
    byKind[finding.kind] = (byKind[finding.kind] ?? 0) + 1;
    if (finding.estimatedDuplicateCost.usdMicros === null) costKnown = false;
    else cost += finding.estimatedDuplicateCost.usdMicros;
    if (finding.estimatedDuplicateCost.tokens === null) tokensKnown = false;
    else tokens += finding.estimatedDuplicateCost.tokens;
  }
  return {
    findingCount: findings.length, lanesInvolved: lanes.size, byKind,
    estimatedDuplicateCostUsdMicros: costKnown ? cost : null,
    estimatedDuplicateTokens: tokensKnown ? tokens : null,
  };
}

type OutcomeTier = "T1" | "T2" | "T3";
type OutcomeCandidate = { finding: Finding; tier: OutcomeTier; pair: string };

const normalizedPath = (value: string) => value.trim().replace(/^['"]|['"]$/g, "").replace(/\\/g, "/").replace(/^\.\//, "").replace(/\/+/g, "/").toLowerCase();
const normalizedRepo = (value: string | null) => (value ?? "").trim()
  .replace(/^https?:\/\/github\.com\//i, "").replace(/^git@github\.com:/i, "").replace(/\.git$/i, "").replace(/^\/+|\/+$/g, "").toLowerCase();
const canonicalBranch = (value: string | null | undefined) => (value ?? "").trim().replace(/^refs\/heads\//, "").replace(/^origin\//, "").toLowerCase();
const canonicalSha = (value: string) => /^[0-9a-f]{7,40}$/i.test(value.trim()) ? value.trim().toLowerCase() : "";
const MIGRATION_PATH = /(?:^|\/)(?:migrations?|db\/migrate|schema\/migrations?)\/([^/]+)$/i;
const KNOWN_HOT_PATH = /(?:^|\/)(?:bun\.lock|package-lock\.json|npm-shrinkwrap\.json|pnpm-lock\.yaml|yarn\.lock|cargo\.lock|poetry\.lock|composer\.lock|gemfile\.lock|_journal\.json|generated-manifest\.json|manifest\.generated\.json)$|(?:^|\/)generated\/[^/]*manifest[^/]*\.json$/i;

function migrationKey(path: string): string {
  const base = normalizedPath(path).match(MIGRATION_PATH)?.[1] ?? "";
  if (!base) return "";
  const number = base.match(/^(\d{3,})[_-]/)?.[1];
  return number ? `number:${number}` : `file:${base}`;
}

const outcomePair = (a: LaneFacts, b: LaneFacts) => [laneKey(a), laneKey(b)].sort(textCompare).join("\u0001");
const findingId = (workspaceId: string, kind: FindingKind, a: LaneFacts, b: LaneFacts, artifact: string) =>
  stableHex([workspaceId, kind, [laneKey(a), laneKey(b)].sort(textCompare), artifact]);
const outcomeScore = (tier: OutcomeTier, suppressed: boolean) => suppressed
  ? ({ T1: 0.29, T2: 0.19, T3: 0.09 })[tier]
  : ({ T1: 0.99, T2: 0.89, T3: 0.59 })[tier];
const suppressionText = (suppressed: boolean) => suppressed
  ? " These lanes share the same orchestrating session; the finding is demoted."
  : "";

function orderedWrites(a: LaneFacts, aw: TimestampedWrite, b: LaneFacts, bw: TimestampedWrite) {
  const order = textCompare(aw.ts, bw.ts) || textCompare(laneKey(a), laneKey(b)) || textCompare(aw.eventId, bw.eventId);
  return order <= 0 ? { first: a, firstWrite: aw, second: b, secondWrite: bw } : { first: b, firstWrite: bw, second: a, secondWrite: aw };
}

function outcomeFinding(
  input: DetectionInput,
  tier: OutcomeTier,
  kind: FindingKind,
  first: LaneFacts,
  second: LaneFacts,
  artifact: Finding["artifacts"][number],
  alertAt: string,
  evidence: Finding["evidence"],
  explanation: string,
  suppressed: boolean,
  harmAt: string | null = null,
): OutcomeCandidate {
  return {
    tier,
    pair: outcomePair(first, second),
    finding: {
      id: findingId(input.workspaceId, kind, first, second, artifact.value),
      kind,
      score: outcomeScore(tier, suppressed),
      confidence: suppressed || tier === "T3" ? "low" : "high",
      lanes: [asLane(first, "first"), asLane(second, "second")],
      artifacts: [artifact],
      alertAt,
      harmAt,
      leadTimeMs: harmAt ? Date.parse(harmAt) - Date.parse(alertAt) : null,
      communication: "unknown",
      estimatedDuplicateCost: duplicateCost(second, alertAt),
      evidence,
      explanation: explanation + suppressionText(suppressed),
    },
  };
}

/** Outcome-strength detector using only the facts already available to the read API. */
export const outcomeFacts: Detector<OutcomeFactsParams> = {
  name: "outcome-facts",
  version: "1",
  detect(input, raw = {}, limit = 50) {
    const migrationWindowMs = raw.migrationWindowMs ?? DEFAULT_MIGRATION_WINDOW_MS;
    const outcomeWindowMs = raw.outcomeWindowMs ?? DEFAULT_OUTCOME_WINDOW_MS;
    const weakWriteWindowMs = raw.weakWriteWindowMs ?? DEFAULT_WEAK_WRITE_WINDOW_MS;
    const hotPathLaneLimit = raw.hotPathLaneLimit ?? DEFAULT_HOT_PATH_LANE_LIMIT;
    if (![migrationWindowMs, outcomeWindowMs, weakWriteWindowMs].every((n) => Number.isFinite(n) && n > 0)
      || !Number.isInteger(hotPathLaneLimit) || hotPathLaneLimit < 1)
      throw new Error("outcome-facts: invalid params");
    const params = { migrationWindowMs, outcomeWindowMs, weakWriteWindowMs, hotPathLaneLimit };
    const rootByLane = roots(input.lanes);
    const suppressed = (a: LaneFacts, b: LaneFacts) => rootByLane.get(laneKey(a)) === rootByLane.get(laneKey(b));
    const alertInWindow = (ts: string) => ts >= input.window.since && ts < input.window.until;
    // The backend supplies the bounded lookback. Facts after `until` must never affect a replay.
    const writes = new Map(input.lanes.map((lane) => [laneKey(lane), lane.writes.filter((write) => write.ts < input.window.until)]));
    const candidates: OutcomeCandidate[] = [];

    // T1: the same normalized migration number or filename, in the same repository.
    const migrationRows = input.lanes.flatMap((lane) => (writes.get(laneKey(lane)) ?? []).flatMap((write) => {
      const migration = migrationKey(write.path);
      const repo = normalizedRepo(lane.repo);
      return migration && repo ? [{ lane, write, migration, repo }] : [];
    })).sort((a, b) => textCompare(a.write.ts, b.write.ts) || textCompare(laneKey(a.lane), laneKey(b.lane)));
    for (let i = 0; i < migrationRows.length; i++) for (let j = i + 1; j < migrationRows.length; j++) {
      const a = migrationRows[i]!, b = migrationRows[j]!;
      if (laneKey(a.lane) === laneKey(b.lane) || a.repo !== b.repo || a.migration !== b.migration) continue;
      if (Date.parse(b.write.ts) - Date.parse(a.write.ts) > migrationWindowMs) continue;
      const ordered = orderedWrites(a.lane, a.write, b.lane, b.write);
      if (!alertInWindow(ordered.secondWrite.ts)) continue;
      const isSuppressed = suppressed(a.lane, b.lane);
      candidates.push(outcomeFinding(input, "T1", "duplicate_work", ordered.first, ordered.second, {
        type: "migration", value: a.migration,
        firstTouchedAt: { [a.lane.sessionId]: a.write.ts, [b.lane.sessionId]: b.write.ts },
      }, ordered.secondWrite.ts, [
        { sessionId: ordered.first.sessionId, eventId: ordered.firstWrite.eventId, ts: ordered.firstWrite.ts, kind: ordered.firstWrite.kind },
        { sessionId: ordered.second.sessionId, eventId: ordered.secondWrite.eventId, ts: ordered.secondWrite.ts, kind: ordered.secondWrite.kind },
      ], `Two lanes wrote migration ${a.migration} in repository ${a.repo} within ${migrationWindowMs / 3_600_000} hours.`, isSuppressed));
    }

    // T2: one lane records a reverted outcome for an earlier commit attributed to another lane.
    for (const second of input.lanes) for (const outcome of second.outcomes) {
      if (!outcome.reverted || !alertInWindow(outcome.ts)) continue;
      const sha = canonicalSha(outcome.key);
      if (!sha) continue;
      for (const first of input.lanes) {
        if (laneKey(first) === laneKey(second)) continue;
        for (const commit of first.commits) {
          if (canonicalSha(commit.sha) !== sha || commit.ts > outcome.ts) continue;
          if (Date.parse(outcome.ts) - Date.parse(commit.ts) > outcomeWindowMs) continue;
          const isSuppressed = suppressed(first, second);
          candidates.push(outcomeFinding(input, "T2", "superseded_work", first, second, {
            type: "commit", value: sha,
            firstTouchedAt: { [first.sessionId]: commit.ts, [second.sessionId]: outcome.ts },
          }, outcome.ts, [
            { sessionId: first.sessionId, eventId: commit.eventId, ts: commit.ts, kind: "commit" },
            { sessionId: second.sessionId, eventId: outcome.eventId, ts: outcome.ts, kind: "outcome_reverted" },
          ], `A reverted outcome in one lane matches commit ${sha} from another lane.`, isSuppressed, outcome.ts));
        }
      }
    }

    // T3: a rare, non-generated path is written on two different branches within 48 hours.
    const weakRows = input.lanes.flatMap((lane) => (writes.get(laneKey(lane)) ?? []).flatMap((write) => {
      const path = normalizedPath(write.path);
      const branch = canonicalBranch(write.branch);
      const scope = normalizedRepo(lane.repo) || (lane.projectId ? `project:${lane.projectId}` : "");
      if (!path || !branch || !scope || KNOWN_HOT_PATH.test(path)) return [];
      const group = JSON.stringify([scope, path]);
      return [{ lane, write, path, branch, group }];
    })).sort((a, b) => textCompare(a.write.ts, b.write.ts) || textCompare(laneKey(a.lane), laneKey(b.lane)));
    const weakByGroup = new Map<string, typeof weakRows>();
    for (const row of weakRows) {
      const rows = weakByGroup.get(row.group) ?? [];
      rows.push(row);
      weakByGroup.set(row.group, rows);
    }
    for (const rows of weakByGroup.values()) {
      // Rarity is evaluated at alert time. A later writer cannot erase an earlier replay finding.
      const rootsAt = new Map<string, number>();
      const seenRoots = new Set<string>();
      for (let i = 0; i < rows.length;) {
        let j = i;
        while (j < rows.length && rows[j]!.write.ts === rows[i]!.write.ts) {
          seenRoots.add(rootByLane.get(laneKey(rows[j]!.lane))!);
          j++;
        }
        rootsAt.set(rows[i]!.write.ts, seenRoots.size);
        i = j;
      }
      for (let i = 0; i < rows.length; i++) for (let j = i + 1; j < rows.length; j++) {
        const a = rows[i]!, b = rows[j]!;
        if (laneKey(a.lane) === laneKey(b.lane) || a.branch === b.branch) continue;
        if (Date.parse(b.write.ts) - Date.parse(a.write.ts) > weakWriteWindowMs) continue;
        const ordered = orderedWrites(a.lane, a.write, b.lane, b.write);
        if (!alertInWindow(ordered.secondWrite.ts) || (rootsAt.get(ordered.secondWrite.ts) ?? 0) > hotPathLaneLimit) continue;
        const isSuppressed = suppressed(a.lane, b.lane);
        candidates.push(outcomeFinding(input, "T3", "concurrent_edit", ordered.first, ordered.second, {
          type: "file", value: a.path,
          firstTouchedAt: { [a.lane.sessionId]: a.write.ts, [b.lane.sessionId]: b.write.ts },
        }, ordered.secondWrite.ts, [
          { sessionId: ordered.first.sessionId, eventId: ordered.firstWrite.eventId, ts: ordered.firstWrite.ts, kind: ordered.firstWrite.kind },
          { sessionId: ordered.second.sessionId, eventId: ordered.secondWrite.eventId, ts: ordered.secondWrite.ts, kind: ordered.secondWrite.kind },
        ], `Two lanes on different branches wrote ${a.path} within ${weakWriteWindowMs / 3_600_000} hours.`, isSuppressed));
      }
    }

    // P6's alert budget is per unordered lane pair: retain the strongest tier, then earliest evidence.
    const byPair = new Map<string, OutcomeCandidate>();
    for (const candidate of candidates) {
      const old = byPair.get(candidate.pair);
      if (!old || candidate.finding.score > old.finding.score
        || (candidate.finding.score === old.finding.score && (textCompare(candidate.finding.alertAt, old.finding.alertAt) < 0
          || (candidate.finding.alertAt === old.finding.alertAt && textCompare(candidate.finding.id, old.finding.id) < 0))))
        byPair.set(candidate.pair, candidate);
    }
    const findings = [...byPair.values()].map((row) => row.finding)
      .sort((a, b) => b.score - a.score || textCompare(a.alertAt, b.alertAt) || textCompare(a.id, b.id))
      .slice(0, Math.max(1, Math.min(200, Math.trunc(limit))));
    return { detector: { name: this.name, version: this.version, paramsHash: stableHex(params) }, summary: summarized(findings), findings };
  },
};

export const sharedWrite60m: Detector<SharedWriteParams> = {
  name: "shared-write-60m",
  version: "1.0.0",
  detect(input, raw = {}, limit = 50) {
    const proximityMs = raw.proximityMs ?? DEFAULT_PROXIMITY_MS;
    const hotPathLaneLimit = raw.hotPathLaneLimit ?? DEFAULT_HOT_PATH_LANE_LIMIT;
    if (!(proximityMs > 0) || !Number.isInteger(hotPathLaneLimit) || hotPathLaneLimit < 1)
      throw new Error("shared-write-60m: invalid params");
    const params = { proximityMs, hotPathLaneLimit };
    const rootByLane = roots(input.lanes);
    const laneByKey = new Map(input.lanes.map((lane) => [laneKey(lane), lane]));
    const writesInWindow = (lane: LaneFacts) => lane.writes.filter((write) =>
      write.ts >= input.window.since && write.ts < input.window.until);
    const pathRoots = new Map<string, Set<string>>();
    for (const lane of input.lanes) {
      const root = rootByLane.get(laneKey(lane))!;
      for (const write of writesInWindow(lane)) {
        const key = JSON.stringify([lane.projectId, write.path]);
        const set = pathRoots.get(key) ?? new Set<string>();
        set.add(root);
        pathRoots.set(key, set);
      }
    }
    const grouped = new Map<string, Array<{ lane: LaneFacts; write: TimestampedWrite; root: string }>>();
    for (const lane of input.lanes) {
      if (lane.projectId === null) continue;
      const root = rootByLane.get(laneKey(lane))!;
      for (const write of writesInWindow(lane)) {
        const key = JSON.stringify([lane.projectId, write.path]);
        if ((pathRoots.get(key)?.size ?? 0) > hotPathLaneLimit) continue;
        const rows = grouped.get(key) ?? [];
        rows.push({ lane, write, root });
        grouped.set(key, rows);
      }
    }
    const findings: Finding[] = [];
    const seen = new Set<string>();
    for (const [groupKey, rows] of grouped) {
      rows.sort((a, b) => textCompare(a.write.ts, b.write.ts) || textCompare(laneKey(a.lane), laneKey(b.lane)) || textCompare(a.write.eventId, b.write.eventId));
      const recent = new Map<string, typeof rows[number]>();
      for (const row of rows) {
        const at = Date.parse(row.write.ts);
        for (const [otherRoot, prior] of recent) {
          if (otherRoot === row.root || at - Date.parse(prior.write.ts) > proximityMs) continue;
          const [projectId, path] = JSON.parse(groupKey) as [string, string];
          const rootsSorted = [otherRoot, row.root].sort(textCompare);
          const identity = stableHex([input.workspaceId, "duplicate_work", rootsSorted, path]);
          if (seen.has(identity)) continue;
          seen.add(identity);
          const firstLane = laneByKey.get(laneKey(prior.lane)) ?? prior.lane;
          const secondLane = laneByKey.get(laneKey(row.lane)) ?? row.lane;
          const firstTouches: Record<string, string> = {};
          for (const candidate of rows.filter((r) => r.root === otherRoot || r.root === row.root)) {
            const old = firstTouches[candidate.lane.sessionId];
            if (!old || candidate.write.ts < old) firstTouches[candidate.lane.sessionId] = candidate.write.ts;
          }
          findings.push({
            id: identity, kind: "duplicate_work", score: 1, confidence: "high",
            lanes: [asLane(firstLane, "first"), asLane(secondLane, "second")],
            artifacts: [{ type: "file", value: path, firstTouchedAt: firstTouches }],
            alertAt: row.write.ts, harmAt: null, leadTimeMs: null, communication: "unknown",
            estimatedDuplicateCost: duplicateCost(secondLane, row.write.ts),
            evidence: [
              { sessionId: prior.lane.sessionId, eventId: prior.write.eventId, ts: prior.write.ts, kind: prior.write.kind },
              { sessionId: row.lane.sessionId, eventId: row.write.eventId, ts: row.write.ts, kind: row.write.kind },
            ],
            explanation: `Two distinct session roots wrote ${path} in project ${projectId} within ${proximityMs / 60_000} minutes.`,
          });
        }
        recent.set(row.root, row);
      }
    }
    findings.sort((a, b) => b.score - a.score || textCompare(a.alertAt, b.alertAt) || textCompare(a.id, b.id));
    const bounded = findings.slice(0, Math.max(1, Math.min(200, Math.trunc(limit))));
    return { detector: { name: this.name, version: this.version, paramsHash: stableHex(params) }, summary: summarized(bounded), findings: bounded };
  },
};

const DETECTORS: ReadonlyMap<string, Detector<any>> = new Map([
  [outcomeFacts.name, outcomeFacts],
  [sharedWrite60m.name, sharedWrite60m],
]);
export function detectorByName(name: string): Detector<any> {
  const detector = DETECTORS.get(name);
  if (!detector) throw new Error(`unknown coordination detector: ${name}`);
  return detector;
}
export function detectCoordination(input: DetectionInput, options: { detector?: string; params?: unknown; limit?: number } = {}): DetectorOutput {
  return detectorByName(options.detector ?? outcomeFacts.name).detect(input, options.params, options.limit);
}
