/**
 * M2-2, the pair classifier over structured claims (docs/session-intel-conflicts.md, "Classifier"). Its output
 * is closed-choice (`constraint_violation`, `intent_conflict`, `overlap`, `none`) and must cite one claim id from
 * each side, or a constraint claim id and an action event id of the other side; anything else is `none`.
 *
 * The deterministic vetoes run here, around whichever judge is plugged in (judges.ts):
 *  - before the judge: superseded claims and `session`-scope constraints never reach it, and nor do claims or
 *    actions off the pair's shared topics;
 *  - after it: every cited id must be in its input, a citation from one side only (a self-conflict) is `none`,
 *    and the class's own shape is checked;
 *  - then the urgency: HIGH needs an action in the topic by the acting side (an intent alone is LOW); a fleet
 *    conflict is LOW to the orchestrator only, and a fleet overlap is SILENT.
 * Inputs are claims and fact ids only, never transcript text.
 */
import type { AlertUrgency } from "../replay/detector.ts";
import type { ClaimKind, ClaimScope } from "../replay/snapshot.ts";
import { refString, textCompare } from "../replay/util.ts";
import { isLiveClaim, type ConflictCandidate } from "./candidates.ts";
import { claimKnownDirs, pathTopic, topicsOfClaim, writeTopics } from "./topics.ts";

export const CONFLICT_VERDICTS = ["constraint_violation", "intent_conflict", "overlap", "none"] as const;
export type ConflictVerdict = typeof CONFLICT_VERDICTS[number];
export const isConflictVerdict = (v: unknown): v is ConflictVerdict => typeof v === "string" && (CONFLICT_VERDICTS as readonly string[]).includes(v);

export type SideName = "a" | "b";
const other = (s: SideName): SideName => s === "a" ? "b" : "a";

export interface JudgeClaim {
  /** `<harness>:<sessionId>#<claimId>`. */
  id: string;
  side: SideName;
  kind: ClaimKind;
  scope: ClaimScope;
  /** Scrub-clean claim text from the snapshot (M1's rule), never a transcript excerpt. */
  text: string;
  at: string;
  evidenceIds: string[];
  /** The pair's shared topics this claim names. Never empty: off-topic claims are not in the input. */
  topics: string[];
  /**
   * Present (true) on a stored claim both sessions hold (one recordId): a shared propagating constraint, the only
   * shared claim kept in the input. Citable only as the constraint of a constraint_violation.
   */
  shared?: true;
}

export interface JudgeAction {
  /** `<harness>:<sessionId>@<eventId>`. */
  id: string;
  side: SideName;
  eventId: string;
  kind: "write" | "commit" | "outcome";
  ts: string;
  /** The written path (writes only). */
  path: string | null;
  topic: string | null;
}

export interface JudgeSide { ref: string; claims: JudgeClaim[]; actions: JudgeAction[] }
export interface JudgeInput { topics: string[]; fleet: boolean; a: JudgeSide; b: JudgeSide }

/** A judge's reply. `invalid` names why a reply could not be parsed (it is then `none`). */
export interface JudgeOutput { verdict: string; claimIds: string[]; actionId?: string | null; invalid?: string }

export interface PairJudge {
  readonly name: string;
  readonly version: string;
  /** Everything that changes the judge's output; hashed into the detector's params. */
  readonly params: Record<string, unknown>;
  /** True when a call costs model budget: the detector then enforces the per-workspace daily caps. */
  readonly metered: boolean;
  /** The token estimate of one call, for the budget (metered judges). */
  estimateTokens?(input: JudgeInput): number;
  judge(input: JudgeInput): Promise<JudgeOutput>;
}

/**
 * Why a live-or-not claim was kept out of the judge's input. `shared`: the same stored claim (one recordId) is
 * held by both sessions: that is agreement, never an intent_conflict or an overlap. A shared propagating
 * constraint is the exception: it stays in the input (JudgeClaim.shared), because a session that
 * restates a constraint and then writes against it still violates it.
 */
export type ExclusionReason = "superseded" | "session_scope" | "off_topic" | "shared";

export interface BuiltInput { input: JudgeInput; excluded: Map<string, ExclusionReason> }

export const claimRef = (sessionRef: string, claimId: string) => `${sessionRef}#${claimId}`;
export const actionRef = (sessionRef: string, eventId: string) => `${sessionRef}@${eventId}`;

/**
 * The judge's input for a candidate as of `now`, with the pre-judge vetoes applied: a superseded claim and a
 * `session`-scope constraint are excluded (recorded in `excluded`), as is a claim that names none of the
 * shared topics. Actions are the visible writes in a shared topic, and the commits and outcomes in such a
 * write's event (they carry no path, so they are in the topic only through it).
 */
export function buildJudgeInput(c: ConflictCandidate, now: Date): BuiltInput {
  const nowIso = now.toISOString();
  const topics = new Set(c.topics);
  const excluded = new Map<string, ExclusionReason>();
  const known = (v: ConflictCandidate["a"]) => {
    const m = new Map(writeTopics(v.writes));
    for (const cl of v.claims) if (isLiveClaim(cl, nowIso)) for (const [t, d] of claimKnownDirs(cl)) if (!m.has(t)) m.set(t, d);
    return m;
  };
  const knownA = known(c.a), knownB = known(c.b);
  // A stored claim whose evidence spans both sessions appears on both sides with one recordId. An intent or a
  // decision is left out of both, so it can never be cited (it would be a self-conflict in disguise, or an
  // "overlap" with itself). A propagating constraint stays on both sides, marked shared: it may be cited only as
  // the constraint of a violation (validateCitations), never in an intent_conflict or an overlap.
  const records = (v: ConflictCandidate["a"]) => new Set(v.claims.filter((cl) => cl.recordId !== undefined && isLiveClaim(cl, nowIso)).map((cl) => cl.recordId!));
  const recA = records(c.a), recB = records(c.b);
  const shared = new Set([...recA].filter((r) => recB.has(r)));
  const side = (name: SideName): JudgeSide => {
    const v = name === "a" ? c.a : c.b;
    const ref = refString(v.session);
    const otherKnown = name === "a" ? knownB : knownA;
    const claims: JudgeClaim[] = [];
    for (const cl of v.claims) {
      const id = claimRef(ref, cl.id);
      if (!isLiveClaim(cl, nowIso)) { if (cl.at < nowIso) excluded.set(id, "superseded"); continue; }
      const isShared = cl.recordId !== undefined && shared.has(cl.recordId);
      if (isShared && (cl.kind !== "constraint" || cl.scope === "session")) { excluded.set(id, "shared"); continue; }
      if (cl.kind === "constraint" && cl.scope === "session") { excluded.set(id, "session_scope"); continue; }
      const mine = topicsOfClaim(cl, otherKnown).filter((t) => topics.has(t)).sort(textCompare);
      if (!mine.length) { excluded.set(id, isShared ? "shared" : "off_topic"); continue; }
      claims.push({ id, side: name, kind: cl.kind, scope: cl.scope, text: cl.text, at: cl.at, evidenceIds: [...cl.evidenceIds], topics: mine,
        ...(isShared ? { shared: true as const } : {}) });
    }
    const actions: JudgeAction[] = [];
    for (const w of v.writes) {
      const t = pathTopic(w.path);
      if (t && topics.has(t)) actions.push({ id: actionRef(ref, w.eventId), side: name, eventId: w.eventId, kind: "write", ts: w.ts, path: w.path, topic: t });
    }
    // A commit or an outcome carries no path, so it is in the topic only when an in-topic write shares its event
    // (the event that wrote the file also committed it). Any other commit or outcome of the side is not an action
    // here, wherever else it touched.
    const topicEvents = new Map(actions.map((x) => [x.eventId, x.topic]));
    for (const f of v.commits) if (topicEvents.has(f.eventId))
      actions.push({ id: actionRef(ref, f.eventId), side: name, eventId: f.eventId, kind: "commit", ts: f.ts, path: null, topic: topicEvents.get(f.eventId)! });
    for (const f of v.outcomes) if (topicEvents.has(f.eventId))
      actions.push({ id: actionRef(ref, f.eventId), side: name, eventId: f.eventId, kind: "outcome", ts: f.ts, path: null, topic: topicEvents.get(f.eventId)! });
    const seen = new Set<string>();
    const unique = actions.filter((x) => !seen.has(x.id) && (seen.add(x.id), true))
      .sort((x, y) => textCompare(x.ts, y.ts) || textCompare(x.id, y.id));
    // Shared claims last, so a judge that cites a side's earliest claim (the rule judge's overlap) cites the same
    // claim it would with the shared one absent.
    const sharedRank = (x: JudgeClaim) => x.shared ? 1 : 0;
    claims.sort((x, y) => sharedRank(x) - sharedRank(y) || textCompare(x.at, y.at) || textCompare(x.id, y.id));
    return { ref, claims, actions: unique };
  };
  return { input: { topics: [...c.topics], fleet: c.fleet, a: side("a"), b: side("b") }, excluded };
}

/**
 * Whether any valid citation is possible: claims on both sides, or a propagating constraint on one side and an
 * action on the other. When not, no judge is asked (and no model budget is spent). A shared claim
 * (JudgeClaim.shared) counts only as a constraint: claims on both sides means unshared claims on both sides, or a
 * constraint on one side and an intent or decision on the other.
 */
export function citable(input: JudgeInput): boolean {
  const own = (s: JudgeSide) => s.claims.some((c) => !c.shared);
  if (own(input.a) && own(input.b)) return true;
  const constraint = (s: JudgeSide) => s.claims.some((c) => c.kind === "constraint");
  const target = (s: JudgeSide) => s.actions.length > 0 || s.claims.some((c) => c.kind !== "constraint" && !c.shared);
  return (constraint(input.a) && target(input.b)) || (constraint(input.b) && target(input.a));
}

export type VetoReason =
  | "invalid_reply" | "invalid_verdict" | "bad_ids" | "duplicate_id" | "unknown_id" | "superseded" | "session_scope" | "off_topic" | "shared"
  | "self_conflict" | "bad_citation" | "action_before_constraint" | "action_before_claim";

export interface Classification {
  verdict: Exclude<ConflictVerdict, "none">;
  claims: JudgeClaim[];
  action: JudgeAction | null;
  /** The constraint of a constraint_violation. */
  constraint: JudgeClaim | null;
}

export type Validated = { ok: true; verdict: "none" } | { ok: true; verdict: Classification["verdict"]; c: Classification } | { ok: false; reason: VetoReason };

/** The acting side's cited claim (the constraint's counterpart for a violation, the intent for a conflict). */
export function actingClaim(cls: Pick<Classification, "verdict" | "claims" | "constraint">): JudgeClaim | null {
  const side = actingSide(cls);
  if (!side) return null;
  if (cls.verdict === "constraint_violation") return cls.constraint;
  return cls.claims.find((c) => c.side === side) ?? null;
}

/** The side whose claim or action a conflict acts through (docs/session-intel-conflicts.md, "Urgency"). */
export function actingSide(cls: Pick<Classification, "verdict" | "claims" | "constraint">): SideName | null {
  if (cls.verdict === "constraint_violation") return cls.constraint ? other(cls.constraint.side) : null;
  if (cls.verdict !== "intent_conflict") return null;
  const [x, y] = cls.claims as [JudgeClaim, JudgeClaim];
  if (x.kind !== y.kind) return (x.kind === "intent" ? x : y).side;
  // Both intents or both decisions: the later claim contradicts the earlier one; a tie falls to b.
  return x.at === y.at ? "b" : (x.at > y.at ? x : y).side;
}

/**
 * The post-judge vetoes: the reply's verdict and citations against the input. Ids outside the input are refused
 * (and named `superseded` or `session_scope` when the builder excluded them); a citation that does not span both
 * sides is a self-conflict; each class's citation shape is checked. A judge's `none` is `none`.
 */
export function validateCitations(built: BuiltInput, out: JudgeOutput): Validated {
  if (out.invalid) return { ok: false, reason: "invalid_reply" };
  if (!isConflictVerdict(out.verdict)) return { ok: false, reason: "invalid_verdict" };
  if (out.verdict === "none") return { ok: true, verdict: "none" };
  if (!Array.isArray(out.claimIds) || !out.claimIds.every((x) => typeof x === "string")) return { ok: false, reason: "bad_ids" };
  if (out.actionId !== undefined && out.actionId !== null && typeof out.actionId !== "string") return { ok: false, reason: "bad_ids" };
  if (new Set(out.claimIds).size !== out.claimIds.length) return { ok: false, reason: "duplicate_id" };
  const { input, excluded } = built;
  // An id present on both sides is ambiguous (a session id or event id containing the separator could make
  // two refs equal): it is refused rather than attributed to either side.
  const index = <T extends { id: string }>(xs: T[]) => {
    const m = new Map<string, T | null>();
    for (const x of xs) m.set(x.id, m.has(x.id) ? null : x);
    return m;
  };
  const claimsById = index([...input.a.claims, ...input.b.claims]);
  const actionsById = index([...input.a.actions, ...input.b.actions]);
  const claims: JudgeClaim[] = [];
  for (const id of out.claimIds) {
    const c = claimsById.get(id);
    if (!c) return { ok: false, reason: excluded.get(id) ?? "unknown_id" };
    claims.push(c);
  }
  let action: JudgeAction | null = null;
  if (typeof out.actionId === "string") {
    action = actionsById.get(out.actionId) ?? null;
    if (!action) return { ok: false, reason: "unknown_id" };
  }
  const sides = new Set([...claims.map((c) => c.side), ...(action ? [action.side] : [])]);
  if (sides.size < 2) return { ok: false, reason: "self_conflict" };

  if (out.verdict === "constraint_violation") {
    const constraints = claims.filter((c) => c.kind === "constraint");
    if (constraints.length !== 1) return { ok: false, reason: "bad_citation" };
    const constraint = constraints[0]!;
    // The builder already keeps session-scope constraints out; this is the same rule, checked again.
    if (constraint.scope === "session") return { ok: false, reason: "session_scope" };
    const rest = claims.filter((c) => c !== constraint);
    if (rest.length > 1 || rest.some((c) => c.side === constraint.side || c.kind === "constraint")) return { ok: false, reason: "bad_citation" };
    if (action && action.side === constraint.side) return { ok: false, reason: "self_conflict" };
    if (!rest.length && !action) return { ok: false, reason: "bad_citation" };
    // An action before the constraint existed did not break it.
    if (action && action.ts < constraint.at) return { ok: false, reason: "action_before_constraint" };
    return { ok: true, verdict: out.verdict, c: { verdict: out.verdict, claims, action, constraint } };
  }
  // intent_conflict and overlap: exactly one claim from each side, neither of them shared (agreement).
  if (claims.some((c) => c.shared)) return { ok: false, reason: "shared" };
  if (claims.length !== 2 || claims[0]!.side === claims[1]!.side) return { ok: false, reason: claims.length === 2 ? "self_conflict" : "bad_citation" };
  if (out.verdict === "intent_conflict") {
    if (claims.some((c) => c.kind === "constraint")) return { ok: false, reason: "bad_citation" };
    const cls: Classification = { verdict: out.verdict, claims, action: null, constraint: null };
    if (action && action.side !== actingSide(cls)) return { ok: false, reason: "bad_citation" };
    // An action before the acting claim was made is not that claim being acted on.
    if (action && action.ts < actingClaim(cls)!.at) return { ok: false, reason: "action_before_claim" };
    return { ok: true, verdict: out.verdict, c: { ...cls, action } };
  }
  if (action) return { ok: false, reason: "bad_citation" };
  return { ok: true, verdict: out.verdict, c: { verdict: out.verdict, claims, action: null, constraint: null } };
}

/**
 * Who an alert goes to (docs/session-intel-conflicts.md, "Delivery"). Recorded on the alert only: nothing is
 * sent in this branch. `dm` lists the sessions a HIGH would DM; `pull` the ones whose turn-end pull carries it.
 */
export interface ConflictDelivery {
  recipient: "sessions" | "orchestrator";
  dm: string[];
  pull: string[];
  /** constraint_violation: the acting session. */
  violator?: string;
  /** constraint_violation: the session holding the constraint, or `repo-policy` for a repository rule with none. */
  owner?: string;
  /** A fleet conflict's orchestrating session ids. */
  orchestratorSessionIds?: string[];
}

export const REPO_POLICY_OWNER = "repo-policy";

/**
 * The owner of a violated constraint: the session holding it. A `repo`-scope constraint with no holding
 * session (a policy pack rule, M1-4) is owned by `repo-policy`, which has no DM recipient.
 * TODO(M2-4): the lander requires "repo owner" to be defined before any DM code; until it is, a repo-policy
 * owner is never DMed and no recipient is invented for it. A `project` or `session` constraint always has a
 * holder; without one there is no owner (null).
 */
export function constraintOwner(scope: ClaimScope, holderRef: string | null): { owner: string; dm: string | undefined } | null {
  if (holderRef) return { owner: holderRef, dm: holderRef };
  if (scope === "repo") return { owner: REPO_POLICY_OWNER, dm: undefined };
  return null;
}

/**
 * The artifact an alert or advisory names: the first shared topic (sorted) that a cited claim or the action
 * names, else the pair's first topic. Shared by the replay detectors and the live advisor.
 */
export function alertArtifact(pairTopics: readonly string[], cls: Pick<Classification, "claims">, action: JudgeAction | null): string {
  const named = new Set([...cls.claims.flatMap((x) => x.topics), ...(action?.topic ? [action.topic] : [])]);
  return [...pairTopics].filter((t) => named.has(t)).sort(textCompare)[0] ?? pairTopics[0]!;
}

export interface Decision {
  verdict: Classification["verdict"];
  urgency: AlertUrgency;
  /** The action that makes it HIGH, or the one a fleet conflict cites. */
  action: JudgeAction | null;
  acting: SideName | null;
  delivery: ConflictDelivery;
}

/**
 * The urgency and delivery for a validated classification, or null for SILENT (an overlap in a fleet).
 * - constraint_violation: HIGH when the violator has an action in the topic at or after the constraint.
 * - intent_conflict: HIGH when the acting side (the intent's, or the later claim's) has an action in the topic.
 * - overlap: LOW, never a DM.
 * - In a fleet: a conflict is LOW to the orchestrator only; an overlap is SILENT.
 * `holderRef` overrides the constraint's holder (null: none, for a repo policy rule); by default it is its side.
 */
export function decide(input: JudgeInput, cls: Classification, fleet: { fleet: boolean; orchestratorSessionIds: string[] },
  holderRef?: string | null): Decision | null {
  if (cls.verdict === "overlap") {
    if (fleet.fleet) return null;
    return { verdict: "overlap", urgency: "low", action: null, acting: null, delivery: { recipient: "sessions", dm: [], pull: [input.a.ref, input.b.ref] } };
  }
  const acting = actingSide(cls)!;
  // Only an action in the topic (an in-topic write, or a commit or outcome in such a write's event) at or after
  // the relevant claim may make it HIGH: the constraint for a violation, the acting side's claim for a conflict.
  // The earliest such action is chosen (actions are sorted by time).
  const since = cls.verdict === "constraint_violation" ? cls.constraint!.at : actingClaim(cls)!.at;
  const action = cls.action ?? input[acting].actions.find((x) => x.ts >= since) ?? null;
  if (fleet.fleet) {
    return { verdict: cls.verdict, urgency: "low", action, acting,
      delivery: { recipient: "orchestrator", dm: [], pull: [], orchestratorSessionIds: [...fleet.orchestratorSessionIds] } };
  }
  const urgency: AlertUrgency = action ? "high" : "low";
  if (cls.verdict === "intent_conflict") {
    const both = [input.a.ref, input.b.ref];
    return { verdict: cls.verdict, urgency, action, acting, delivery: { recipient: "sessions", dm: urgency === "high" ? both : [], pull: both } };
  }
  const violator = input[acting].ref;
  const owner = constraintOwner(cls.constraint!.scope, holderRef === undefined ? input[cls.constraint!.side].ref : holderRef);
  const ownerDm = owner?.dm ? [owner.dm] : [];
  return {
    verdict: cls.verdict, urgency, action, acting,
    delivery: {
      recipient: "sessions", dm: urgency === "high" ? [violator, ...ownerDm] : [], pull: [violator, ...ownerDm], violator,
      ...(owner ? { owner: owner.owner } : {}),
    },
  };
}
