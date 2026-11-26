/**
 * The shipped detector as the benchmark's baseline (docs/coordination-overlap-detection.md): `outcome-facts` v1,
 * run unchanged over the same harvested evidence mapped onto its `LaneFacts`. Writes come from the recovered
 * edits; a write's branch is the latest branch the session named, as production uses the catalog branch. No
 * outcome carries a revert flag here, so T2 cannot fire: that is a property of the evidence, reported as such.
 */
import { detectCoordination, type LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { type Alert, type Context, rootSide } from "./detectors.ts";

export function outcomeFactsBaseline(ctx: Context, window: { since: string; until: string }, opts: { limit: number }): Alert[] {
  const lanes: LaneFacts[] = [];
  for (const s of ctx.sessions.values()) {
    if (!s.edits.length && !s.shas.length) continue;
    const branchAt = (ts: string) => [...s.branches].filter((b) => b.ts <= ts).pop()?.name ?? null;
    lanes.push({
      sessionId: s.sessionId, source: s.source, userId: s.owner, projectId: s.repo, repo: s.repo, branch: s.branches.at(-1)?.name ?? null,
      taskRef: null, title: null, parentSessionId: s.parents[0]?.slice(s.source.length + 1) ?? null, startedAt: s.startedAt, lastActionAt: s.lastTs,
      writes: s.edits.map((e, i) => ({ path: e.path, ts: e.ts, eventId: `${s.key}#w${i}`, kind: e.via, branch: branchAt(e.ts) })),
      commits: s.shas.map((c, i) => ({ sha: c.sha, ts: c.ts, eventId: `${s.key}#c${i}`, reverted: null })),
      outcomes: [], usage: [],
    });
  }
  const out = detectCoordination({ workspaceId: "bench", window, lanes }, { detector: "outcome-facts", limit: opts.limit });
  const rootOf = (source: string, sessionId: string) => ctx.sessions.get(`${source}:${sessionId}`)?.root ?? `${source}:${sessionId}`;
  return out.findings.map((f) => ({
    detector: `outcome-facts/${f.kind}`,
    sides: [rootSide(rootOf(f.lanes[0]!.source, f.lanes[0]!.sessionId)), rootSide(rootOf(f.lanes[1]!.source, f.lanes[1]!.sessionId))],
    at: f.alertAt, artifact: f.artifacts[0]?.value ?? "", score: f.score, owners: [f.lanes[0]!.userId, f.lanes[1]!.userId],
  }));
}
