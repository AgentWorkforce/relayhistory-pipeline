/**
 * M2-4: the conflict detectors in the live advisor (docs/coordination-live-advisories.md, "Conflict rows", and
 * docs/session-intel-conflicts.md, "Live wiring"). Gated by SESSION_INTEL_CONFLICTS_ENABLED and the M3-1
 * `conflicts` control: with either off, scan.ts never calls anything here.
 *
 * The live inputs are mapped into the replay harness's as-of structures (AsOfSession), and the decisions come from
 * the same shared code the replay detectors use: candidates.ts, classify.ts and the rule judge (judges.ts). Only
 * the rule judge runs live; the model judge is not wired (no transport, model or budget is approved).
 */
import type { LaneFacts } from "../../packages/coordination-detect/src/index.ts";
import { COORDINATION_EVIDENCE_LOOKBACK_MS } from "../session-read-api/coordination-backend.ts";
import { conflictCandidates } from "../session-intel/conflict/candidates.ts";
import {
  alertArtifact, buildJudgeInput, citable, decide, validateCitations, type Classification, type Decision, type JudgeAction, type JudgeClaim, type JudgeSide, type SideName,
} from "../session-intel/conflict/classify.ts";
import { ruleClassify } from "../session-intel/conflict/judges.ts";
import type { ClaimKind, ClaimScope, SnapshotClaim, SnapshotSession } from "../session-intel/replay/snapshot.ts";
import { sha256Hex, textCompare } from "../session-intel/replay/util.ts";
import type { AsOfSession } from "../session-intel/replay/views.ts";
import type { Advisory, CandidateLane, Evidence, SessionRef } from "./rubric.ts";
import { LOW_KIND_NEARBY, LOW_KIND_SUMMARY, pairKey, refKey } from "./rubric.ts";

/** The 062150 claims reader: current claims and decisions of the workspace, newest first. */
export const CLAIMS_SQL = `SELECT id, kind, scope, topic, statement, observed_at, created_at, evidence
  FROM sessions.relayfacts_claims_v1($1, $2, NULL, NULL, $3, $4)`;
/** The reader's own maximum. A read that returns this many rows may be cut: conflicts are skipped, never guessed. */
export const CLAIMS_LIMIT = 500;
/**
 * How far back the read reaches: the 2-hour candidate window's start minus the facts' 7-day lookback. A claim
 * not updated in that span is not read. A long-lived constraint stays readable as long as its evidence keeps
 * arriving (each new evidence row updates the claim), and the bound keeps the read inside the 500-row limit.
 */
export const CLAIMS_LOOKBACK_MS = COORDINATION_EVIDENCE_LOOKBACK_MS;

export const CONFLICT_KINDS = ["constraint_violation", "intent_conflict", "overlap"] as const;
export type ConflictKind = typeof CONFLICT_KINDS[number];
const CONFLICT_CLASSES: ReadonlySet<string> = new Set(["constraint_violation", "intent_conflict"]);
export const isConflictKind = (kind: string): kind is ConflictKind => (CONFLICT_KINDS as readonly string[]).includes(kind);

export interface ClaimEvidenceRow { id: string; source: string; sessionId: string; createdAt: string }
export interface ClaimRow {
  id: string;
  kind: ClaimKind;
  /** Null for decisions (relayfacts_v1 has no scope). */
  scope: ClaimScope | null;
  topic: string;
  /** The stored scrub-clean statement, at most 200 characters (the reader cuts it). */
  statement: string;
  observedAt: string | null;
  createdAt: string;
  evidence: ClaimEvidenceRow[];
}

const KINDS: readonly string[] = ["decision", "intent", "constraint"];
const SCOPES: readonly string[] = ["session", "project", "repo"];
const CLAIM_ID = /^[A-Za-z0-9_.-]{1,128}$/;
const iso = (v: unknown): string => {
  const d = v instanceof Date ? v : new Date(String(v));
  if (!Number.isFinite(d.getTime())) throw new Error("claims: a timestamp is malformed");
  return d.toISOString();
};

/**
 * Validates the reader's rows. Any malformed row fails the whole read (the workspace then skips conflicts): a
 * row is never half-used. Errors name no value.
 */
export function parseClaimRows(rows: ReadonlyArray<Record<string, unknown>>): ClaimRow[] {
  return rows.map((r): ClaimRow => {
    const id = String(r.id ?? "");
    if (!CLAIM_ID.test(id)) throw new Error("claims: a claim id is malformed");
    if (typeof r.kind !== "string" || !KINDS.includes(r.kind)) throw new Error("claims: a claim kind is malformed");
    if (r.scope !== null && (typeof r.scope !== "string" || !SCOPES.includes(r.scope))) throw new Error("claims: a claim scope is malformed");
    if (typeof r.topic !== "string" || !/^[a-z0-9][a-z0-9-]{0,63}$/.test(r.topic)) throw new Error("claims: a claim topic is malformed");
    if (typeof r.statement !== "string" || !r.statement.trim()) throw new Error("claims: a claim statement is malformed");
    let raw: unknown = r.evidence;
    if (typeof raw === "string") {
      try { raw = JSON.parse(raw); } catch { throw new Error("claims: claim evidence is malformed"); }
    }
    if (!Array.isArray(raw)) throw new Error("claims: claim evidence is malformed");
    const evidence = raw.map((e): ClaimEvidenceRow => {
      const x = e as Record<string, unknown>;
      if (!x || typeof x.source !== "string" || typeof x.session_id !== "string" || !x.session_id) throw new Error("claims: claim evidence is malformed");
      return { id: String(x.id ?? ""), source: x.source, sessionId: x.session_id, createdAt: iso(x.created_at) };
    });
    return {
      id, kind: r.kind as ClaimKind, scope: (r.scope as ClaimScope | null) ?? null, topic: r.topic, statement: r.statement.slice(0, 200),
      observedAt: r.observed_at === null || r.observed_at === undefined ? null : iso(r.observed_at), createdAt: iso(r.created_at), evidence,
    };
  });
}

/**
 * A claim's time for one session that holds it, from that session's own evidence (conservative). relayfacts keeps
 * one row per stored claim, with one `observed_at` and evidence rows per holder, and no observed time per evidence
 * row. A merge moves `observed_at` to the earliest any holder reported, and an erase clears it, so on a record held
 * by more than one session `observed_at` cannot be attributed to any one of them. So:
 *  - a record whose evidence names this session only, with an `observed_at`: that `observed_at` (only this session's
 *    own reports can have set or moved it; an erase of another holder would have cleared it);
 *  - otherwise (several holders, a tie, a NULL `observed_at`, a decision): this session's own earliest evidence
 *    `created_at`, which is when the claim was recorded for it, never earlier than its statement.
 * An evidence time is the ingest time, so a session's claim may be timed later than it was made: that can only turn
 * a HIGH into a LOW, never the reverse.
 */
export function claimTimeFor(row: ClaimRow, source: string, sessionId: string): string {
  const own = row.evidence.filter((e) => e.source === source && e.sessionId === sessionId).map((e) => e.createdAt).sort(textCompare)[0];
  if (own === undefined) return row.observedAt ?? row.createdAt;
  const alone = row.evidence.every((e) => e.source === source && e.sessionId === sessionId);
  return alone && row.observedAt !== null ? row.observedAt : own;
}

/**
 * The live inputs as the replay's as-of sessions: one per candidate lane, with its facts when the facts read
 * returned any (a session with claims and no writes yet is still a candidate). A claim is attached to
 * every session its evidence names, at that session's own time (claimTimeFor); a decision has no scope
 * (`session`, which only matters for constraints). Facts at or after `now` are left out (ts < now, as in replay).
 */
export function liveSessions(candidates: readonly CandidateLane[], facts: readonly LaneFacts[], claims: readonly ClaimRow[], now: Date): AsOfSession[] {
  const nowIso = now.toISOString();
  const byRef = new Map(candidates.map((c) => [refKey(c), c]));
  const claimsOf = new Map<string, SnapshotClaim[]>();
  for (const row of claims) {
    const seen = new Set<string>();
    for (const e of row.evidence) {
      const k = refKey({ harness: e.source, sessionId: e.sessionId });
      if (!byRef.has(k) || seen.has(k)) continue;
      seen.add(k);
      const mine = row.evidence.filter((x) => x.source === e.source && x.sessionId === e.sessionId).map((x) => x.id).filter(Boolean);
      claimsOf.set(k, [...(claimsOf.get(k) ?? []), {
        id: row.id, at: claimTimeFor(row, e.source, e.sessionId), kind: row.kind, scope: row.scope ?? "session", text: row.statement,
        confidence: 1, evidenceIds: mine.slice(0, 64), topic: row.topic, recordId: row.id,
      }]);
    }
  }
  const factsOf = new Map(facts.map((l) => [refKey({ harness: l.source, sessionId: l.sessionId }), l]));
  const out: AsOfSession[] = [];
  for (const c of [...candidates].sort((x, y) => textCompare(refKey(x), refKey(y)))) {
    const l = factsOf.get(refKey(c));
    const visible = <T extends { ts: string }>(xs: readonly T[] | undefined) => (xs ?? []).filter((x) => x.ts < nowIso);
    const session: SnapshotSession = {
      orgId: c.orgId, workspaceId: c.workspaceId, harness: c.harness, sessionId: c.sessionId, projectId: c.projectId, repo: c.repo,
      branch: l?.branch ?? null, title: l?.title ?? null, userId: l?.userId ?? null, taskRef: l?.taskRef ?? null, parentSessionIds: [...c.parentSessionIds],
      createdAt: l?.startedAt ?? c.lastActivityAt, lastActivityAt: c.lastActivityAt, summaryTimeline: [],
      writes: visible(l?.writes), commits: visible(l?.commits), outcomes: visible(l?.outcomes), usage: visible(l?.usage),
      claims: (claimsOf.get(refKey(c)) ?? []).sort((a, b) => textCompare(a.at, b.at) || textCompare(a.id, b.id)),
    };
    out.push({ session, lastActivityAt: c.lastActivityAt, shortSummary: c.shortSummary, writes: session.writes, commits: session.commits,
      outcomes: session.outcomes, usage: session.usage, claims: session.claims });
  }
  return out;
}

/** One advisory row to write: a recipient and the peer it names. */
export interface AdvisoryRow { recipient: SessionRef; peer: SessionRef }

/** A conflict advisory: the rows it writes (never both directions blindly) and the claim text for the DM. */
export interface ConflictAdvisory extends Advisory {
  kind: ConflictKind;
  rows: AdvisoryRow[];
  /** The cited claims' stored statements, for the DM template's scrub-checked excerpts. */
  claimTexts: Array<{ kind: ClaimKind; text: string }>;
  /** The cited claim ids (the relayfacts claim and decision ids). */
  claimIds: string[];
}

/** The raw relayfacts id of a cited claim (`<harness>:<sessionId>#<id>`): the part after the last `#`. */
const rawClaimId = (ref: string) => ref.slice(ref.lastIndexOf("#") + 1);

/**
 * A stable advisory id (16..64 hex, the 062000 bound): the workspace, the kind, the pair and the deciding
 * claims. A constraint violation is keyed by its constraint, so its LOW (an intent) and its HIGH (a write) are
 * one advisory and the 062000 upsert upgrades LOW to HIGH; the other kinds by their two claims.
 */
export function conflictAdvisoryId(workspaceId: string, kind: ConflictKind, a: SessionRef, b: SessionRef, cls: Classification): string {
  const key = kind === "constraint_violation" ? [rawClaimId(cls.constraint!.id)] : cls.claims.map((c) => rawClaimId(c.id)).sort(textCompare);
  return sha256Hex(["conflict/1", workspaceId, kind, pairKey(a, b), ...key].join("\u0000")).slice(0, 32);
}

const ref = (s: SnapshotSession): SessionRef => ({ harness: s.harness, sessionId: s.sessionId });

/** The rows a decision writes (docs/coordination-live-advisories.md, "Conflict rows"). */
export function rowsFor(d: Decision, a: AsOfSession, b: AsOfSession, acting: SideName | null): AdvisoryRow[] {
  const ra = ref(a.session), rb = ref(b.session);
  if (d.delivery.recipient === "orchestrator") {
    // A fleet conflict: pull rows to the orchestrating sessions only (same-harness relationships), naming the
    // acting worker as the peer. Never a DM, never a row to the workers.
    const peer = acting === "b" ? rb : ra;
    return (d.delivery.orchestratorSessionIds ?? []).map((id) => {
      const child = [a.session, b.session].find((s) => s.parentSessionIds.includes(id) || s.sessionId === id) ?? a.session;
      return { recipient: { harness: child.harness, sessionId: id }, peer };
    });
  }
  const byRef = new Map([[refKey(ra), ra], [refKey(rb), rb]]);
  const pick = (r: string) => {
    const i = r.indexOf(":");
    return byRef.get(refKey({ harness: r.slice(0, i), sessionId: r.slice(i + 1) }));
  };
  // Every recipient in `pull` gets a row; its peer is the other session of the pair. A repo-policy owner is not
  // a session and gets nothing.
  return d.delivery.pull.map(pick).filter((r): r is SessionRef => !!r)
    .map((r) => ({ recipient: r, peer: refKey(r) === refKey(ra) ? rb : ra }));
}

function evidenceOf(claims: JudgeClaim[], action: JudgeAction | null, sessionOf: (side: SideName) => string): Evidence[] {
  const out: Evidence[] = claims.map((c) => ({ sessionId: sessionOf(c.side), eventId: rawClaimId(c.id), ts: c.at, kind: `claim_${c.kind}` }));
  if (action) out.push({ sessionId: sessionOf(action.side), eventId: action.eventId, ts: action.ts, kind: action.kind });
  return out.sort((x, y) => textCompare(x.ts, y.ts) || textCompare(x.eventId, y.eventId));
}

export interface LiveConflictStats { pairs: number; truncatedPairs: number; vetoed: number; silenced: number; steps: number }

/**
 * The live path's CPU bounds per workspace per tick. The input cap: at most this many visible facts (writes,
 * commits and outcomes) and claims over the workspace's sessions, checked before any pair is formed. The step
 * budget: each candidate pair costs its two sides' facts and claims (the judge input is built from them) plus its
 * constraints times the other side's actions (the rule judge's matching loop); once the running total would pass
 * the budget, the workspace's conflicts are skipped. Never partial: a workspace over either bound gets no conflict
 * rows this tick (scan.ts counts it in conflictsSkipped), and its collision and overlap rows go on.
 */
export const CONFLICT_MAX_INPUTS = 10_000;
export const CONFLICT_STEP_BUDGET = 200_000;
export interface LiveConflictBounds { maxInputs?: number; stepBudget?: number }
export type LiveConflictResult =
  | { advisories: ConflictAdvisory[]; stats: LiveConflictStats; overBudget?: undefined }
  | { advisories: []; stats: LiveConflictStats; overBudget: "inputs" | "steps" };

const sizeOf = (v: AsOfSession) => v.writes.length + v.commits.length + v.outcomes.length + v.claims.length;

/** The conflict advisories of one workspace as of `now`: the rule judge only, through the shared code. */
export function liveConflictAdvisories(workspaceId: string, sessions: readonly AsOfSession[], now: Date, bounds: LiveConflictBounds = {}): LiveConflictResult {
  const maxInputs = bounds.maxInputs ?? CONFLICT_MAX_INPUTS, stepBudget = bounds.stepBudget ?? CONFLICT_STEP_BUDGET;
  const stats: LiveConflictStats = { pairs: 0, truncatedPairs: 0, vetoed: 0, silenced: 0, steps: 0 };
  let inputs = 0;
  for (const v of sessions) inputs += sizeOf(v);
  if (inputs > maxInputs) return { advisories: [], stats, overBudget: "inputs" };
  const { candidates, truncated } = conflictCandidates(sessions, now);
  stats.pairs = candidates.length;
  stats.truncatedPairs = truncated;
  const out: ConflictAdvisory[] = [];
  for (const cand of candidates) {
    stats.steps += sizeOf(cand.a) + sizeOf(cand.b);
    if (stats.steps > stepBudget) return { advisories: [], stats, overBudget: "steps" };
    // Live claim times are ingest times (claimTimeFor): a shared constraint's author is unknown, so it is not cited.
    const built = buildJudgeInput(cand, now, { claimTimes: "ingest" });
    const constraints = (s: JudgeSide) => s.claims.filter((c) => c.kind === "constraint").length;
    stats.steps += constraints(built.input.a) * built.input.b.actions.length + constraints(built.input.b) * built.input.a.actions.length;
    if (stats.steps > stepBudget) return { advisories: [], stats, overBudget: "steps" };
    if (!citable(built.input)) continue;
    const v = validateCitations(built, ruleClassify(built.input));
    if (!v.ok) { stats.vetoed++; continue; }
    if (v.verdict === "none") continue;
    const d = decide(built.input, v.c, cand);
    if (!d) { stats.silenced++; continue; }
    const rows = rowsFor(d, cand.a, cand.b, d.acting);
    if (!rows.length) { stats.silenced++; continue; }
    const ra = ref(cand.a.session), rb = ref(cand.b.session);
    const evidence = evidenceOf(v.c.claims, d.action, (side) => cand[side].session.sessionId);
    out.push({
      advisoryId: conflictAdvisoryId(workspaceId, d.verdict, ra, rb, v.c), urgency: d.urgency, kind: d.verdict,
      artifact: alertArtifact(cand.topics, v.c, d.action), lanes: [ra, rb], evidence, alertAt: evidence.at(-1)?.ts ?? now.toISOString(),
      rows, claimTexts: v.c.claims.map((c) => ({ kind: c.kind, text: c.text })), claimIds: v.c.claims.map((c) => rawClaimId(c.id)),
    });
  }
  return { advisories: out, stats };
}

/**
 * One tick's advisories for a workspace. A conflict never suppresses a collision: the advisor's HIGH rows (T1, T2,
 * the 60-minute T3) are always kept, and a pair with both gets both rows (the pair cooldown is per source class,
 * so neither holds back the other, in this tick or a later one; after a restart, durable rows of either class hold
 * back both for up to one pair cooldown, 6 hours by default). A pair with a constraint_violation or
 * intent_conflict loses its LOW overlap rows (nearby_work, similar_summary). A conflict-detector `overlap` is
 * dropped when the advisor already has a row for the pair.
 */
export function mergeConflicts(high: Advisory[], low: Advisory[], conflicts: ConflictAdvisory[]): Array<Advisory | ConflictAdvisory> {
  const conflictPairs = new Set(conflicts.filter((c) => CONFLICT_CLASSES.has(c.kind)).map((c) => pairKey(c.lanes[0], c.lanes[1])));
  const keptLow = low.filter((a) => !((a.kind === LOW_KIND_NEARBY || a.kind === LOW_KIND_SUMMARY) && conflictPairs.has(pairKey(a.lanes[0], a.lanes[1]))));
  const advisorPairs = new Set([...high, ...keptLow].map((a) => pairKey(a.lanes[0], a.lanes[1])));
  const keptConflicts = conflicts.filter((c) => c.kind !== "overlap" || !advisorPairs.has(pairKey(c.lanes[0], c.lanes[1])));
  return [...high, ...keptLow, ...keptConflicts];
}

export const claimsSince = (windowSince: Date) => new Date(windowSince.getTime() - CLAIMS_LOOKBACK_MS);
