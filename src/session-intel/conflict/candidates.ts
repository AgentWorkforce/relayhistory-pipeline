/**
 * M2-1, the conflict candidate generator (docs/session-intel-conflicts.md, "Candidates"). As of a simulated
 * `now`, a candidate is a pair of sessions that:
 *  - are in the same org, workspace and project, both active within the advisor's 2-hour window;
 *  - have a live claim on at least one side (visible, and not superseded by now);
 *  - share a topic: the same first-two-segment directory among their visible writes, or a live claim that
 *    names (by path, or by a directory word) a topic the other side wrote or claimed.
 * Planned fleets are kept, flagged `fleet`, so the classifier can route a conflict to the orchestrator. Self
 * pairs never form. At most 50 pairs per workspace per tick, in a deterministic order.
 */
import { CANDIDATE_WINDOW_MS } from "../../coordination-advisor/rubric.ts";
import type { SnapshotClaim, SnapshotSession } from "../replay/snapshot.ts";
import type { AsOfSession } from "../replay/views.ts";
import { refString, sortedPair, textCompare } from "../replay/util.ts";
import { claimTopics, parseClaimText, writeTopics } from "./topics.ts";

export const CONFLICT_WINDOW_MS = CANDIDATE_WINDOW_MS;
export const MAX_PAIRS_PER_WORKSPACE = 50;
/** The newest sessions per workspace that pairs are formed from, as the advisor's CANDIDATES_SQL bounds it. */
export const MAX_SESSIONS_PER_WORKSPACE = 200;

export interface CandidateOptions { windowMs?: number; maxPairsPerWorkspace?: number; maxSessionsPerWorkspace?: number }

/** Live at `now`: visible (at < now) and not superseded by now (a supersession is visible once it is < now). */
export const isLiveClaim = (c: SnapshotClaim, nowIso: string) =>
  c.at < nowIso && !(c.supersededAt !== undefined && c.supersededAt < nowIso);

export interface ConflictCandidate {
  orgId: string;
  workspaceId: string;
  /** Sorted refs; `a` is pair[0]'s session and `b` pair[1]'s. */
  pair: [string, string];
  a: AsOfSession;
  b: AsOfSession;
  fleet: boolean;
  /** The orchestrating session ids of a fleet pair (the shared parents, or the parent of the pair). */
  orchestratorSessionIds: string[];
  /** The shared topics, sorted. Never empty. */
  topics: string[];
}

export interface CandidateResult {
  candidates: ConflictCandidate[];
  /** Pairs dropped by the per-workspace cap. */
  truncated: number;
  /** Sessions dropped by the per-workspace session bound. */
  sessionsDropped: number;
}

/**
 * The advisor's planned-fleet rule (src/coordination-advisor/rubric.ts isPlannedFleet): parent ids intersect,
 * or one is the other's parent within a harness. Here the fleet is routed, not dropped.
 */
export function fleetOf(a: SnapshotSession, b: SnapshotSession): { fleet: boolean; orchestratorSessionIds: string[] } {
  const parents = new Set(a.parentSessionIds);
  const shared = [...new Set(b.parentSessionIds.filter((p) => parents.has(p)))];
  if (a.harness === b.harness) {
    if (a.parentSessionIds.includes(b.sessionId)) shared.push(b.sessionId);
    if (b.parentSessionIds.includes(a.sessionId)) shared.push(a.sessionId);
  }
  const ids = [...new Set(shared)].sort(textCompare);
  return { fleet: ids.length > 0, orchestratorSessionIds: ids };
}

/** What one side brings to topic matching. */
interface Side {
  view: AsOfSession;
  ref: string;
  live: SnapshotClaim[];
  writes: Map<string, string[]>;
  /** Write topics plus the path topics of live claims, each with its directory segments. */
  known: Map<string, string[]>;
}

function sideOf(view: AsOfSession, nowIso: string): Side {
  const live = view.claims.filter((c) => isLiveClaim(c, nowIso));
  const writes = writeTopics(view.writes);
  const known = new Map(writes);
  for (const c of live) for (const [t, dirs] of parseClaimText(c.text).pathDirs) if (!known.has(t)) known.set(t, dirs);
  return { view, ref: refString(view.session), live, writes, known };
}

/** The topics two sides share; empty when they share none. */
export function sharedTopics(x: Side, y: Side): string[] {
  const out = new Set<string>();
  for (const t of x.writes.keys()) if (y.writes.has(t)) out.add(t);
  for (const [from, to] of [[x, y], [y, x]] as const)
    for (const c of from.live) for (const t of claimTopics(c.text, to.known)) if (to.known.has(t)) out.add(t);
  return [...out].sort(textCompare);
}

export function conflictCandidates(sessions: readonly AsOfSession[], now: Date, opts: CandidateOptions = {}): CandidateResult {
  const windowMs = opts.windowMs ?? CONFLICT_WINDOW_MS;
  const maxPairs = opts.maxPairsPerWorkspace ?? MAX_PAIRS_PER_WORKSPACE;
  const maxSessions = opts.maxSessionsPerWorkspace ?? MAX_SESSIONS_PER_WORKSPACE;
  const nowIso = now.toISOString();
  const sinceIso = new Date(now.getTime() - windowMs).toISOString();
  const byWorkspace = new Map<string, AsOfSession[]>();
  for (const v of sessions) {
    if (v.session.projectId === null || v.lastActivityAt < sinceIso) continue;
    const k = `${v.session.orgId}\u0000${v.session.workspaceId}`;
    byWorkspace.set(k, [...(byWorkspace.get(k) ?? []), v]);
  }
  const candidates: ConflictCandidate[] = [];
  let truncated = 0, sessionsDropped = 0;
  for (const k of [...byWorkspace.keys()].sort(textCompare)) {
    const newest = byWorkspace.get(k)!.sort((a, b) => textCompare(b.lastActivityAt, a.lastActivityAt)
      || textCompare(a.session.harness, b.session.harness) || textCompare(a.session.sessionId, b.session.sessionId));
    sessionsDropped += Math.max(0, newest.length - maxSessions);
    const sides = newest.slice(0, maxSessions).map((v) => sideOf(v, nowIso));
    const mine: Array<ConflictCandidate & { recency: string }> = [];
    for (let i = 0; i < sides.length; i++) {
      for (let j = i + 1; j < sides.length; j++) {
        const x = sides[i]!, y = sides[j]!;
        if (x.ref === y.ref) continue;
        if (x.view.session.projectId !== y.view.session.projectId) continue;
        if (!x.live.length && !y.live.length) continue;
        const topics = sharedTopics(x, y);
        if (!topics.length) continue;
        const pair = sortedPair(x.ref, y.ref);
        const [a, b] = pair[0] === x.ref ? [x, y] : [y, x];
        const { fleet, orchestratorSessionIds } = fleetOf(a.view.session, b.view.session);
        mine.push({
          orgId: a.view.session.orgId, workspaceId: a.view.session.workspaceId, pair, a: a.view, b: b.view, fleet, orchestratorSessionIds, topics,
          recency: a.view.lastActivityAt < b.view.lastActivityAt ? a.view.lastActivityAt : b.view.lastActivityAt,
        });
      }
    }
    // Most recently active pairs first (by the older side's activity), then by pair: deterministic.
    mine.sort((p, q) => textCompare(q.recency, p.recency) || textCompare(p.pair[0], q.pair[0]) || textCompare(p.pair[1], q.pair[1]));
    truncated += Math.max(0, mine.length - maxPairs);
    for (const { recency: _r, ...c } of mine.slice(0, maxPairs)) candidates.push(c);
  }
  return { candidates, truncated, sessionsDropped };
}
