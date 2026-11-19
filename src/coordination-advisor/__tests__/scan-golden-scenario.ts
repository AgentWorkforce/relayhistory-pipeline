/**
 * A recorded scan scenario for the conflicts gate golden (conflicts-gate.test.ts): every store, directory,
 * backend and sender call with its arguments, and the stats, over three ticks with a collision, a LOW nearby
 * pair, a similar-summary pair and a planned fleet. The golden file was generated from scan.ts before
 * SESSION_INTEL_CONFLICTS_ENABLED existed, so "flag off" is compared against the scan as it was.
 */
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import type { Directory } from "../../session-read-api/directory.ts";
import { SESSION_INTEL_FEATURES, type SessionIntelControls } from "../../shared/session-intel-controls.ts";
import type { RelayDm, RelaySender } from "../relay-sender.ts";
import { AdvisorLimits, type CandidateLane, type SessionRef } from "../rubric.ts";
import { scanOnce, type AdvisorStore, type AdvisoryRowInput, type UpsertResult } from "../scan.ts";

export const GOLDEN_NOW = new Date(Date.UTC(2026, 10, 17, 12));
const at = (min: number) => new Date(GOLDEN_NOW.getTime() + min * 60_000).toISOString();
const cand = (sessionId: string, over: Partial<CandidateLane> = {}): CandidateLane => ({
  orgId: "org", workspaceId: "ws", harness: "claude", sessionId, projectId: "proj", repo: "o/r",
  parentSessionIds: [], shortSummary: null, lastActivityAt: at(-5), ...over,
});
const lane = (sessionId: string, writes: Array<[string, number]>): LaneFacts => ({
  sessionId, source: "claude", userId: null, projectId: "proj", repo: "o/r", branch: `b/${sessionId}`, taskRef: null, title: null,
  parentSessionId: null, startedAt: at(-90), lastActionAt: at(-5), commits: [], outcomes: [], usage: [],
  writes: writes.map(([path, min]) => ({ path, ts: at(min), eventId: `${sessionId}-${min}`, kind: "tool", branch: `b/${sessionId}` })),
});

const CANDIDATES = [
  cand("a"), cand("b"), cand("c"), cand("d"),
  cand("e", { shortSummary: "refactor the billing retry worker queue" }), cand("f", { shortSummary: "refactor the billing retry worker loop" }),
  cand("lead"), cand("w1", { parentSessionIds: ["lead"] }), cand("w2", { parentSessionIds: ["lead"] }),
];
const LANES = [
  lane("a", [["src/x/shared.ts", -40]]), lane("b", [["src/x/shared.ts", -20]]),
  lane("c", [["docs/guide/a.md", -30]]), lane("d", [["docs/guide/b.md", -25]]),
  lane("e", []), lane("f", []),
  lane("w1", [["src/fleet/one.ts", -30]]), lane("w2", [["src/fleet/one.ts", -10]]),
];

/** Records every call; `claims` (when present) is a spy the flag-off scan must never call. */
export class RecordingStore implements AdvisorStore {
  log: unknown[] = [];
  rows = new Map<string, AdvisoryRowInput>();
  constructor(private readonly ctl: SessionIntelControls) {}
  async controls(orgId: string, workspaceId: string) { this.log.push(["controls", orgId, workspaceId]); return this.ctl; }
  async candidates(since: Date, limit: number, workspaces: string[] | null) {
    this.log.push(["candidates", since.toISOString(), limit, workspaces]);
    return CANDIDATES.map((c) => ({ ...c, parentSessionIds: [...c.parentSessionIds] }));
  }
  directory(): Directory { this.log.push(["directory"]); return {} as Directory; }
  async upsert(orgId: string, workspaceId: string, row: AdvisoryRowInput): Promise<UpsertResult> {
    this.log.push(["upsert", orgId, workspaceId, row]);
    const k = `${row.advisory_id}|${row.harness}|${row.session_id}`;
    if (this.rows.has(k)) return { inserted: 0, upgraded: 0, skipped: 1 };
    this.rows.set(k, row);
    return { inserted: 1, upgraded: 0, skipped: 0 };
  }
  async markDelivered(orgId: string, workspaceId: string, advisoryId: string, r: SessionRef) {
    this.log.push(["markDelivered", orgId, workspaceId, advisoryId, r]);
    return true;
  }
  async sweep() { this.log.push(["sweep"]); return { expired: 0, deleted: 0 }; }
  async recent(orgId: string, workspaceId: string, since: Date) { this.log.push(["recent", orgId, workspaceId, since.toISOString()]); return []; }
}

export function recordingBackend(log: unknown[]): CoordinationBackend {
  return {
    kind: "clickhouse",
    windowReadiness: async () => { throw new Error("unused"); },
    readFacts: async (_d, scope, opts) => {
      log.push(["readFacts", scope.orgId, scope.workspaceId, scope.since.toISOString(), scope.until.toISOString(), opts]);
      return { lanes: LANES.map((l) => ({ ...l, writes: l.writes.map((w) => ({ ...w })) })), sessionsTruncated: false, notes: [] } as unknown as CoordinationFacts;
    },
    isSchemaMissing: () => false,
  };
}

export function recordingSender(log: unknown[]): RelaySender {
  return { async send(dm: RelayDm) { log.push(["send", dm]); } } as RelaySender;
}

export const ALL_ON: SessionIntelControls = new Map(SESSION_INTEL_FEATURES.map((f) => [f, true]));

/**
 * Three ticks, 2 minutes apart, with one limits object; returns the full call log and the stats of each tick.
 * `extra` is spread into the scan deps (the conflicts gate under test); `store` lets a test add a claims spy.
 */
export async function runGoldenScenario(opts: { extra?: Record<string, unknown>; store?: RecordingStore } = {}) {
  const store = opts.store ?? new RecordingStore(ALL_ON);
  const backend = recordingBackend(store.log);
  const sender = recordingSender(store.log);
  const limits = new AdvisorLimits({ pairCooldownMs: 6 * 3_600_000, sessionHourlyCap: 3 });
  const stats: unknown[] = [];
  for (const tick of [0, 2, 4]) {
    const now = new Date(GOLDEN_NOW.getTime() + tick * 60_000);
    stats.push(await scanOnce({ workspaces: "*", store, backend, limits, sender, ...(opts.extra ?? {}) } as Parameters<typeof scanOnce>[0], now));
  }
  return { log: store.log, stats };
}
