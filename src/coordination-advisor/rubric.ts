/**
 * The advisor's pure decisions (docs/coordination-live-advisories.md): planned-fleet exclusion, the
 * urgency rubric, the LOW pre-filter, and the per-pair cooldown and per-recipient hourly cap.
 * No I/O here; scan.ts wires it to the facts, the store and the sender.
 */
import {
  findingId, type DetectorOutput, type Finding, type FindingKind, type LaneFacts,
} from "../../packages/coordination-detect/src/index.ts";

export type Urgency = "high" | "low";
export interface SessionRef { harness: string; sessionId: string }
export interface Evidence { sessionId: string; eventId: string; ts: string; kind: string }

/** A session-directory row active inside the candidate window. */
export interface CandidateLane extends SessionRef {
  orgId: string;
  workspaceId: string;
  projectId: string | null;
  repo: string | null;
  parentSessionIds: string[];
  shortSummary: string | null;
  lastActivityAt: string;
}

export interface Advisory {
  advisoryId: string;
  urgency: Urgency;
  kind: string;
  artifact: string;
  /** The two sessions, in the finding's order. Both receive a row. */
  lanes: [SessionRef, SessionRef];
  evidence: Evidence[];
  alertAt: string;
}

export const CANDIDATE_WINDOW_MS = 2 * 60 * 60 * 1_000;
/** The third HIGH row: the detector's T3 narrowed from 48 hours to 60 minutes. */
export const HIGH_SHARED_FILE_WINDOW_MS = 60 * 60 * 1_000;
export const ADVISOR_DETECTOR_PARAMS = { weakWriteWindowMs: HIGH_SHARED_FILE_WINDOW_MS } as const;
/** HIGH kinds: T1, T2 and the 60-minute T3. */
export const HIGH_KINDS: ReadonlySet<string> = new Set<FindingKind>(["duplicate_work", "superseded_work", "concurrent_edit"]);
/**
 * outcome-facts scores a same-root (planned fleet) finding at 0.29/0.19/0.09 and an undemoted one at
 * 0.99/0.89/0.59. Anything below this floor is a demoted fleet finding: SILENT.
 */
export const DEMOTED_SCORE_CEILING = 0.5;
export const LOW_KIND_NEARBY = "nearby_work";
export const LOW_KIND_SUMMARY = "similar_summary";
export const SUMMARY_ARTIFACT = "similar short summaries";
const MAX_EVIDENCE = 10;

const textCompare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
export const refKey = (r: SessionRef) => `${r.harness}\u0000${r.sessionId}`;
export const pairKey = (a: SessionRef, b: SessionRef) => [refKey(a), refKey(b)].sort(textCompare).join("\u0001");

/**
 * A planned fleet: the two sessions' directory parent_session_ids intersect, or one is the other's
 * parent (same harness: relationships are same-harness). Never a candidate.
 */
export function isPlannedFleet(a: CandidateLane, b: CandidateLane): boolean {
  const parents = new Set(a.parentSessionIds);
  if (b.parentSessionIds.some((p) => parents.has(p))) return true;
  if (a.harness === b.harness && (a.parentSessionIds.includes(b.sessionId) || b.parentSessionIds.includes(a.sessionId))) return true;
  return false;
}

/** Only sessions in the same (non-null) project are paired. */
export const sameProject = (a: CandidateLane, b: CandidateLane) =>
  a.projectId !== null && a.projectId !== "" && a.projectId === b.projectId;

/** The rubric for a detector finding: HIGH for T1, T2 and the 60-minute T3; null (SILENT) otherwise. */
export function findingUrgency(f: Finding): Urgency | null {
  if (!HIGH_KINDS.has(f.kind) || f.score < DEMOTED_SCORE_CEILING) return null;
  return "high";
}

const boundedEvidence = (evidence: Evidence[]) =>
  evidence.slice(0, MAX_EVIDENCE).map((e) => ({ sessionId: e.sessionId, eventId: e.eventId, ts: e.ts, kind: e.kind }));

/** Streams the facts read could not cover; their findings are dropped, never guessed. */
export interface TruncatedStreams { writes: boolean; outputs: boolean }
export function truncatedStreams(notes: string[]): TruncatedStreams {
  return {
    writes: notes.some((n) => n.startsWith("write evidence stream cap")),
    outputs: notes.some((n) => n.startsWith("commit/outcome stream cap")),
  };
}
const usesWrites = (kind: string) => kind !== "superseded_work";

/**
 * HIGH advisories from the detector output: same-project, non-fleet pairs of candidates only.
 * A finding from a truncated stream produces nothing.
 */
export function highAdvisories(
  output: DetectorOutput, candidates: ReadonlyMap<string, CandidateLane>, truncated: TruncatedStreams,
): Advisory[] {
  const out: Advisory[] = [];
  for (const f of output.findings) {
    if (findingUrgency(f) !== "high" || f.lanes.length !== 2 || f.artifacts.length < 1) continue;
    if (usesWrites(f.kind) ? truncated.writes : truncated.outputs) continue;
    const [x, y] = f.lanes as [Finding["lanes"][number], Finding["lanes"][number]];
    const a = candidates.get(refKey({ harness: x.source, sessionId: x.sessionId }));
    const b = candidates.get(refKey({ harness: y.source, sessionId: y.sessionId }));
    if (!a || !b || !sameProject(a, b) || isPlannedFleet(a, b)) continue;
    out.push({
      advisoryId: f.id,
      urgency: "high",
      kind: f.kind,
      artifact: f.artifacts[0]!.value,
      lanes: [{ harness: a.harness, sessionId: a.sessionId }, { harness: b.harness, sessionId: b.sessionId }],
      evidence: boundedEvidence(f.evidence),
      alertAt: f.alertAt,
    });
  }
  return out;
}

const normalizedPath = (value: string) => value.trim().replace(/^['"]|['"]$/g, "").replace(/\\/g, "/")
  .replace(/^\.\//, "").replace(/\/+/g, "/").toLowerCase();
/** The top-level directory of a path: its first two segments. A root-level file has none. */
export function topLevelDir(path: string): string | null {
  const parts = normalizedPath(path).split("/").filter(Boolean);
  return parts.length >= 3 ? `${parts[0]}/${parts[1]}` : parts.length === 2 ? parts[0]! : null;
}

const SUMMARY_MIN_SHARED = 3;
const SUMMARY_MIN_JACCARD = 0.5;
const words = (text: string) => new Set((text.toLowerCase().match(/[a-z0-9][a-z0-9_-]{2,}/g) ?? []));
/** Word-set Jaccard similarity of two short summaries: at least 3 shared words and >= 0.5. */
export function similarSummaries(a: string | null, b: string | null): boolean {
  if (!a || !b) return false;
  const x = words(a), y = words(b);
  let shared = 0;
  for (const w of x) if (y.has(w)) shared++;
  const union = x.size + y.size - shared;
  return shared >= SUMMARY_MIN_SHARED && union > 0 && shared / union >= SUMMARY_MIN_JACCARD;
}

/**
 * The LOW pre-filter. Same project (always: candidates pair only inside a project), no HIGH for the
 * pair, no planned fleet, no shared file yet, and either overlapping top-level directories in the
 * window's writes or similar short summaries. It never promotes a pair to HIGH and never suppresses
 * one: pairs that already have a HIGH are skipped.
 */
export function lowAdvisories(
  workspaceId: string, lanes: LaneFacts[], candidates: ReadonlyMap<string, CandidateLane>,
  highPairs: ReadonlySet<string>, windowSince: string, truncated: TruncatedStreams,
): Advisory[] {
  // "No shared file yet" needs the full write evidence.
  if (truncated.writes) return [];
  const live = lanes.filter((l) => candidates.has(refKey({ harness: l.source, sessionId: l.sessionId })))
    .sort((p, q) => textCompare(refKey({ harness: p.source, sessionId: p.sessionId }), refKey({ harness: q.source, sessionId: q.sessionId })));
  const out: Advisory[] = [];
  for (let i = 0; i < live.length; i++) for (let j = i + 1; j < live.length; j++) {
    const la = live[i]!, lb = live[j]!;
    const ra = { harness: la.source, sessionId: la.sessionId }, rb = { harness: lb.source, sessionId: lb.sessionId };
    const a = candidates.get(refKey(ra))!, b = candidates.get(refKey(rb))!;
    if (!sameProject(a, b) || isPlannedFleet(a, b) || highPairs.has(pairKey(ra, rb))) continue;
    const filesA = new Set(la.writes.map((w) => normalizedPath(w.path)));
    if (lb.writes.some((w) => filesA.has(normalizedPath(w.path)))) continue;
    const recent = (l: LaneFacts) => l.writes.filter((w) => w.ts >= windowSince)
      .sort((p, q) => textCompare(p.ts, q.ts) || textCompare(p.eventId, q.eventId));
    const firstByDir = (l: LaneFacts) => {
      const m = new Map<string, LaneFacts["writes"][number]>();
      for (const w of recent(l)) { const d = topLevelDir(w.path); if (d && !m.has(d)) m.set(d, w); }
      return m;
    };
    const da = firstByDir(la), db = firstByDir(lb);
    const shared = [...da.keys()].filter((d) => db.has(d)).sort(textCompare);
    if (shared.length) {
      const dir = shared[0]!, wa = da.get(dir)!, wb = db.get(dir)!;
      const evidence = [
        { sessionId: la.sessionId, eventId: wa.eventId, ts: wa.ts, kind: wa.kind },
        { sessionId: lb.sessionId, eventId: wb.eventId, ts: wb.ts, kind: wb.kind },
      ].sort((p, q) => textCompare(p.ts, q.ts));
      out.push({
        // LOW kinds are not FindingKinds; the doc reuses the detector's id function unchanged.
        advisoryId: findingId(workspaceId, LOW_KIND_NEARBY as FindingKind, la, lb, dir),
        urgency: "low", kind: LOW_KIND_NEARBY, artifact: dir, lanes: [ra, rb], evidence, alertAt: evidence.at(-1)!.ts,
      });
    } else if (similarSummaries(a.shortSummary, b.shortSummary)) {
      out.push({
        advisoryId: findingId(workspaceId, LOW_KIND_SUMMARY as FindingKind, la, lb, SUMMARY_ARTIFACT),
        urgency: "low", kind: LOW_KIND_SUMMARY, artifact: SUMMARY_ARTIFACT, lanes: [ra, rb], evidence: [],
        alertAt: [a.lastActivityAt, b.lastActivityAt].sort(textCompare).at(-1)!,
      });
    }
  }
  return out;
}

/** HIGH first when the cap would cut a scan's output, then the earliest alert, then the id. */
export function orderAdvisories(advisories: Advisory[]): Advisory[] {
  return [...advisories].sort((a, b) => (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1)
    || textCompare(a.alertAt, b.alertAt) || textCompare(a.advisoryId, b.advisoryId));
}

/** One row of 062070 coordination_advisories_recent_v1, times in epoch ms. */
export type RecentAdvisory =
  | { kind: "pair"; recipient: SessionRef; peer: SessionRef; urgency: Urgency; latestAt: number }
  | { kind: "recipient"; recipient: SessionRef; latestAt: number; sentLastHour: number };

/**
 * The source class of a pair cooldown: the advisor's own rows (collision and overlap), or the conflict detector's
 * (M2-4). Each class cools down only itself. `any` is internal: durable state (062070) does not say which class
 * wrote a row, so a seeded row that this process cannot attribute holds back both classes.
 */
export type CooldownSource = "advisor" | "conflict";
type CooldownBucket = CooldownSource | "any";
const levelKey = (pair: string, urgency: Urgency, bucket: CooldownBucket) => `${bucket}\u0003${urgency}\u0002${pair}`;

/**
 * Per-pair cooldown, per urgency and per source class, and per-recipient hourly cap. A HIGH write starts the HIGH
 * cooldown and a LOW write the LOW one; a HIGH advisory waits only on the HIGH cooldown, so a LOW never holds back a
 * HIGH, while a LOW waits on either. A source class waits only on its own writes (and on durable state it cannot
 * attribute), so a conflict row never holds back a collision row for the pair, nor the reverse. The source defaults
 * to `advisor`: with conflicts off, every write and check is the advisor's and the cooldown is exactly as before.
 * In memory, merged with durable state by `seed` when a reader for it exists; the upsert (never re-inserts an
 * existing row, never touches an acked one) keeps a DM from repeating.
 */
export class AdvisorLimits {
  private readonly pairs = new Map<string, number>();
  private readonly sent = new Map<string, number[]>();
  constructor(private readonly cfg: { pairCooldownMs: number; sessionHourlyCap: number }) {}
  get pairCooldownMs(): number { return this.cfg.pairCooldownMs; }

  inCooldown(pair: string, urgency: Urgency, now: number, source: CooldownSource = "advisor"): boolean {
    const cooling = (u: Urgency) => ([source, "any"] as const).some((b) => {
      const at = this.pairs.get(levelKey(pair, u, b));
      return at !== undefined && now - at < this.cfg.pairCooldownMs;
    });
    return cooling("high") || (urgency === "low" && cooling("low"));
  }
  underCap(recipient: SessionRef, now: number): boolean {
    return this.recent(refKey(recipient), now).length < this.cfg.sessionHourlyCap;
  }
  /** Any write attempt for the pair (inserted, upgraded or skipped) starts or re-seeds its cooldown for its source. */
  touchPair(pair: string, urgency: Urgency, at: number, source: CooldownSource = "advisor"): void {
    this.touch(levelKey(pair, urgency, source), at);
  }
  private touch(k: string, at: number): void {
    this.pairs.set(k, Math.max(at, this.pairs.get(k) ?? at));
  }
  /** One advisory written for the recipient (inserted or upgraded). */
  recordSent(recipient: SessionRef, at: number): void {
    const k = refKey(recipient);
    this.sent.set(k, [...(this.sent.get(k) ?? []), at]);
  }
  /**
   * Merges durable state (062070 coordination_advisories_recent_v1) so a restart keeps the cooldown and the
   * cap. A pair keeps its later time per urgency. A recipient with fewer in-memory sends than the durable last-hour count
   * gets the difference at the durable latest time, which ages out no earlier than the real sends.
   */
  seed(rows: RecentAdvisory[], now: number): void {
    for (const r of rows) {
      if (r.kind === "pair") {
        // A durable pair row carries no source class. When this process holds the pair's cooldown for a class at
        // that urgency, the row is its own write: it keeps that class's later time (the advisor's only, with
        // conflicts off: exactly as before). Otherwise (a restart) it holds back both classes.
        const pair = pairKey(r.recipient, r.peer);
        const own = (["advisor", "conflict"] as const).map((b) => levelKey(pair, r.urgency, b)).filter((k) => this.pairs.has(k));
        for (const k of own.length ? own : [levelKey(pair, r.urgency, "any")]) this.touch(k, r.latestAt);
        continue;
      }
      const k = refKey(r.recipient);
      const have = this.recent(k, now);
      if (have.length < r.sentLastHour) this.sent.set(k, [...have, ...Array<number>(r.sentLastHour - have.length).fill(r.latestAt)]);
    }
  }
  /** Drops state older than both windows. */
  prune(now: number): void {
    for (const [k, at] of this.pairs) if (now - at >= this.cfg.pairCooldownMs) this.pairs.delete(k);
    for (const k of [...this.sent.keys()]) { const r = this.recent(k, now); if (r.length) this.sent.set(k, r); else this.sent.delete(k); }
  }
  private recent(k: string, now: number): number[] {
    return (this.sent.get(k) ?? []).filter((at) => now - at < 3_600_000);
  }
}
