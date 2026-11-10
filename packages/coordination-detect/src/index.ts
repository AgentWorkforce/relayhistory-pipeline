/** Pure, deterministic coordination detectors. No I/O, clocks, or dependencies. */

export type FindingKind = "duplicate_work" | "concurrent_edit" | "superseded_work" | "repeated_investigation" | "stale_base";
export type Confidence = "high" | "medium" | "low";

export interface TimestampedWrite { path: string; ts: string; eventId: string; kind: string }
export interface TimestampedUsage { ts: string; eventId: string; tokens: number | null; costUsdMicros: number | null }
export interface CommitFact { sha: string; ts: string; eventId: string; reverted: boolean | null }
export interface OutcomeFact { key: string; ts: string; eventId: string; reverted: boolean | null }

export interface LaneFacts {
  sessionId: string;
  source: string;
  userId: string | null;
  projectId: string | null;
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

const DETECTORS: ReadonlyMap<string, Detector<any>> = new Map([[sharedWrite60m.name, sharedWrite60m]]);
export function detectorByName(name: string): Detector<any> {
  const detector = DETECTORS.get(name);
  if (!detector) throw new Error(`unknown coordination detector: ${name}`);
  return detector;
}
export function detectCoordination(input: DetectionInput, options: { detector?: string; params?: unknown; limit?: number } = {}): DetectorOutput {
  return detectorByName(options.detector ?? sharedWrite60m.name).detect(input, options.params, options.limit);
}
