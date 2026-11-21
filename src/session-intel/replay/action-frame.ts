/** Causal repository partitions over a verified /2 capture, preserving original session identities. */
import type { LaneFacts } from "../../../packages/coordination-detect/src/index.ts";
import { actionKey, readActionScope, type ActionKind } from "../patterns/action-scope.ts";
import { buildCorpus } from "../patterns/corpus.ts";
import type { ActionRecord } from "../import/provenance.ts";
import type { Snapshot, SnapshotSession } from "./snapshot.ts";
import { refString, textCompare } from "./util.ts";

export const ACTION_LOOKBACK_MS = 7 * 24 * 60 * 60_000;
export const ACTION_FRAME_BOUNDS = { visibleFacts: 4_096, repositoryFacts: 256, ancestors: 256 } as const;
type Fact = SnapshotSession["writes"][number] | SnapshotSession["commits"][number] | SnapshotSession["outcomes"][number];
interface Action { session: SnapshotSession; kind: ActionKind; fact: Fact; record: ActionRecord }
const ref = (s: SnapshotSession) => refString(s);
const validSha = (s: string) => /^[a-f0-9]{7,40}$/iu.test(s);

export class ActionFrame {
  readonly records: ReadonlyMap<string, ActionRecord>;
  readonly sessions: ReadonlyMap<string, SnapshotSession>;
  readonly stats: Record<string, number> = { rejected: 0, missingScope: 0, conflictingScope: 0, missingPath: 0, unkeyedCommits: 0, lateFacts: 0, staleOnArrival: 0, visibleFacts: 0 };
  private readonly actions: Action[] = [];
  private cursor = 0;
  private active = new Map<string, Action[]>();
  private lastNow = Number.NEGATIVE_INFINITY;
  private readonly ancestry = new Map<string, Set<string> | null>();
  exhausted = false;

  constructor(snapshot: Snapshot, now: Date) {
    // Charges the full input row/fact frame, retention, tenant and duplicate checks before indexing.
    const corpus = buildCorpus([snapshot], { now });
    this.records = readActionScope(snapshot, now, { repositoryPaths: true });
    this.sessions = new Map(corpus.sessions.map((s) => [ref(s), s]));
    for (const s of corpus.sessions) for (const [kind, facts] of [["write", s.writes], ["commit", s.commits], ["outcome", s.outcomes]] as const) for (const fact of facts) {
      const record = this.records.get(actionKey(s, kind, fact));
      if (!record?.repository) {
        if (record?.repositoryBasis === "conflicting_claims") this.stats.conflictingScope!++;
        else this.stats.missingScope!++;
        this.stats.rejected!++; continue;
      }
      if (kind === "write" && !record.repositoryPath) { this.stats.missingPath!++; this.stats.rejected!++; continue; }
      if (kind === "commit" && !validSha((fact as SnapshotSession["commits"][number]).sha)) {
        this.stats.unkeyedCommits!++; this.stats.rejected!++; continue;
      }
      if (kind === "outcome" && (fact as SnapshotSession["outcomes"][number]).reverted === true && !validSha((fact as SnapshotSession["outcomes"][number]).key)) {
        this.stats.unkeyedCommits!++; this.stats.rejected!++; continue;
      }
      if (record.availableAt > fact.ts) this.stats.lateFacts!++;
      this.actions.push({ session: s, kind, fact, record });
    }
    this.actions.sort((a, b) => textCompare(a.record.availableAt, b.record.availableAt) || textCompare(actionKey(a.session, a.kind, a.fact), actionKey(b.session, b.kind, b.fact)));
  }

  /** Changed repositories only: unchanged facts never rerun a quadratic detector. */
  advance(now: Date): Map<string, LaneFacts[]> {
    const ms = now.getTime(), iso = now.toISOString(), cutoff = new Date(ms - ACTION_LOOKBACK_MS).toISOString();
    if (ms < this.lastNow) throw new Error("action replay: clock went backwards");
    this.lastNow = ms;
    if (this.exhausted) return new Map();
    const changed = new Set<string>();
    for (const [repo, actions] of this.active) {
      const kept = actions.filter((a) => a.fact.ts >= cutoff);
      if (kept.length !== actions.length) { if (kept.length) this.active.set(repo, kept); else this.active.delete(repo); changed.add(repo); }
    }
    while (this.cursor < this.actions.length && this.actions[this.cursor]!.record.availableAt < iso) {
      const a = this.actions[this.cursor++]!;
      if (a.session.createdAt > iso || a.fact.ts >= iso) throw new Error("action replay: inconsistent causal frame");
      if (a.fact.ts < cutoff) { this.stats.staleOnArrival!++; continue; }
      const repo = a.record.repository!;
      if (!this.active.has(repo)) this.active.set(repo, []);
      this.active.get(repo)!.push(a); changed.add(repo);
    }
    const count = [...this.active.values()].reduce((n, a) => n + a.length, 0);
    this.stats.visibleFacts = count;
    const liveRefs = new Set([...this.active.values()].flatMap((a) => a.map((x) => ref(x.session))));
    for (const key of this.ancestry.keys()) if (!liveRefs.has(key)) this.ancestry.delete(key);
    if (count > ACTION_FRAME_BOUNDS.visibleFacts || [...this.active.values()].some((a) => a.length > ACTION_FRAME_BOUNDS.repositoryFacts)) {
      this.reject(); return new Map();
    }
    return new Map([...changed].sort(textCompare).map((repo) => [repo, this.lanes(repo)]));
  }

  reject(): void { if (!this.exhausted) this.stats.rejected!++; this.exhausted = true; }

  /** Original parent graph, including metadata-only ancestors in other repositories. Null means bounded out. */
  planned(a: string, b: string): boolean | null {
    const ancestors = (start: string) => {
      if (this.ancestry.has(start)) return this.ancestry.get(start)!;
      const seen = new Set<string>(), queue = [start];
      for (let i = 0; i < queue.length; i++) {
        const key = queue[i]!;
        if (seen.has(key)) continue;
        if (seen.size >= ACTION_FRAME_BOUNDS.ancestors) { this.ancestry.set(start, null); this.stats.rejected!++; return null; }
        seen.add(key);
        const s = this.sessions.get(key);
        if (s) for (const parent of s.parentSessionIds) queue.push(refString({ harness: s.harness, sessionId: parent }));
      }
      this.ancestry.set(start, seen); return seen;
    };
    const x = ancestors(a), y = ancestors(b);
    return x === null || y === null ? null : [...x].some((key) => y.has(key));
  }

  private lanes(repo: string): LaneFacts[] {
    const lanes = new Map<string, LaneFacts>();
    for (const a of this.active.get(repo) ?? []) {
      const s = a.session, key = ref(s);
      let lane = lanes.get(key);
      if (!lane) {
        lane = { source: s.harness, sessionId: s.sessionId, userId: null, projectId: repo, repo, branch: null, taskRef: null, title: null,
          parentSessionId: s.parentSessionIds[0] ?? null, startedAt: s.createdAt, lastActionAt: a.fact.ts, writes: [], commits: [], outcomes: [], usage: [] };
        lanes.set(key, lane);
      }
      if (a.fact.ts > lane.lastActionAt) lane.lastActionAt = a.fact.ts;
      if (a.kind === "write") lane.writes.push({ ...(a.fact as SnapshotSession["writes"][number]), path: a.record.repositoryPath!, branch: null });
      if (a.kind === "commit") lane.commits.push({ ...(a.fact as SnapshotSession["commits"][number]) });
      if (a.kind === "outcome") lane.outcomes.push({ ...(a.fact as SnapshotSession["outcomes"][number]) });
    }
    for (const lane of lanes.values()) for (const list of [lane.writes, lane.commits, lane.outcomes]) list.sort((a, b) => textCompare(a.ts, b.ts) || textCompare(a.eventId, b.eventId));
    return [...lanes.values()].sort((a, b) => textCompare(a.source, b.source) || textCompare(a.sessionId, b.sessionId));
  }
}
