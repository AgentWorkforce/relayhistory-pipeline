/**
 * Stages 3 and 4 (docs/proposals/relayfacts-features.md, "Derivation"): linked sessions become feature
 * records with a derived state, as of a time.
 *
 * As-of: only sessions that started before `asOf`, and only items whose time is before `asOf` (an
 * untimed item counts from its session's end), are used, so a replay at T never sees later evidence.
 * State comes from the forge facts when the feature's commits or PRs appear there ("forge" basis), else
 * from the analyses alone ("analysis" basis), which never claims a merge it cannot see.
 */
import { stableId } from "./extract.ts";
import { DEFAULT_LINK_PARAMS, linkSessions, type LinkParams } from "./link.ts";
import type { EvidencePointer, FeatureRecord, FeatureState, ForgeFacts, SessionFacts } from "./types.ts";
import { EMPTY_FORGE, requireCut, sessionKeyString, textCompare, utcForge } from "./types.ts";

export const ABANDON_AFTER_MS = 21 * 24 * 3_600_000;
export const MAX_FEATURE_QUOTES = 20;
export const BRANCH_LEAD_MS = 7 * 24 * 3_600_000;

export interface BuildOptions {
  asOf: string;
  forge?: ForgeFacts;
  link?: LinkParams;
}

/**
 * One session's facts cut to what was known before `asOf`; null when the session had not started.
 *
 * Strict as-of: a timed item (and every evidence pointer and quote inside it) counts only before `asOf`. Anything
 * without a time of its own (mention-only keys, branch and file keys, outcome statuses, untimed items) counts only
 * once the session has ended before `asOf` (so does the session's title); a pointer without its own time takes its
 * item's. A replay at T is therefore invariant to everything observed after T.
 * The directory's own facts (repository, parent sessions) are known from the session's start.
 */
export function factsAsOf(f: SessionFacts, asOfIn: string): SessionFacts | null {
  const asOf = requireCut(asOfIn, "asOf");
  if (f.startedAt >= asOf) return null;
  const ended = f.endedAt < asOf;
  const seen = (at: string | null) => (at === null ? ended : at < asOf);
  // A pointer without its own time takes the time of the item that cites it (which has already passed the cut).
  const cutEvidence = <T extends { evidence: EvidencePointer[] }>(item: T): T => ({ ...item, evidence: item.evidence.filter((e) => e.at === null || e.at < asOf) });
  const keep = <T extends { at: string | null; evidence: EvidencePointer[] }>(items: T[]) => items.filter((i) => seen(i.at)).map(cutEvidence);
  const built = keep(f.built);
  const asked = keep(f.asked).map((a) => ({ ...a, quotes: a.quotes.filter((q) => seen(q.at)) }));
  const builtRefs = new Set(f.built.filter((b) => b.ref).map((b) => `${b.kind}\u0000${b.ref}`));
  const keptRefs = new Set(built.filter((b) => b.ref).map((b) => `${b.kind}\u0000${b.ref}`));
  // A key backed by a built item follows that item's time; a key with no item behind it is known at session end.
  const known = (kind: "commit" | "pr", ref: string) => (builtRefs.has(`${kind}\u0000${ref}`) ? keptRefs.has(`${kind}\u0000${ref}`) : ended);
  return {
    ...f,
    endedAt: ended ? f.endedAt : asOf,
    // The analysis's title summarises the whole session: known only once it ended.
    title: ended ? f.title : "",
    asked,
    built,
    gaps: keep(f.gaps),
    rationale: keep(f.rationale),
    outcomeStatuses: ended ? f.outcomeStatuses : [],
    keys: {
      ...f.keys,
      branches: ended ? f.keys.branches : [],
      files: ended ? f.keys.files : [],
      taskRefs: ended ? f.keys.taskRefs : [],
      commits: f.keys.commits.filter((sha) => known("commit", sha)),
      prs: f.keys.prs.filter((url) => known("pr", url)),
    },
  };
}

interface StateResult { state: FeatureState; basis: "forge" | "analysis"; reason: string }

type ForgeCommit = ForgeFacts["commits"][number];
/** A commit's dated landings on the default branch: its transitions when known, else its first landing. */
interface Transition { at: string; seq?: number; step?: number; unorderedWith?: readonly number[]; unordered?: true; what: string }
const byTime = (a: Transition, b: Transition) => textCompare(a.at, b.at);
/**
 * Whether git orders two transitions of one instant: both have an integration position and neither lists the
 * other as unordered (a pairwise relation: commits of one step that are not ancestor and descendant).
 */
const ordered = (a: Transition, b: Transition) =>
  a.seq !== undefined && b.seq !== undefined && !(a.unorderedWith ?? []).includes(b.seq) && !(b.unorderedWith ?? []).includes(a.seq) &&
  // The legacy flag keeps its meaning: unordered against every other transition of the same step.
  !((a.unordered || b.unordered) && a.step !== undefined && a.step === b.step);
/**
 * The latest transition: the latest instant, and within it the highest integration position when git orders it
 * against every other candidate. Otherwise the instant's order is unknown and the result says so (`seq` absent).
 */
function latestTransition(list: readonly Transition[]): Transition | undefined {
  if (!list.length) return undefined;
  const at = list.map((t) => t.at).sort(textCompare).pop()!;
  const tied = list.filter((t) => t.at === at);
  if (tied.some((t) => t.seq === undefined)) return { at, what: tied[0]!.what };
  const top = tied.reduce((m, t) => (t.seq! > m.seq! ? t : m));
  return tied.every((t) => t === top || ordered(t, top)) ? top : { at, what: top.what };
}
/** A commit transition's position fields, for the replay. */
const positionOf = (t: { seq?: number; step?: number; unorderedWith?: number[]; unordered?: true }) => ({ ...(t.seq !== undefined ? { seq: t.seq } : {}), ...(t.step !== undefined ? { step: t.step } : {}), ...(t.unorderedWith?.length ? { unorderedWith: t.unorderedWith } : {}), ...(t.unordered ? { unordered: true as const } : {}) });
function landingsOf(c: ForgeCommit): Transition[] {
  if (c.transitions) return c.transitions.filter((t) => t.kind === "landed").map((t) => ({ at: t.at, ...positionOf(t), what: t.by === c.sha ? `commit ${c.sha.slice(0, 12)} on the default branch` : `commit ${c.sha.slice(0, 12)} landed as ${t.by.slice(0, 12)}` }));
  return c.onDefaultBranch ? [{ at: c.defaultAt ?? c.at, what: `commit ${c.sha.slice(0, 12)} on the default branch` }] : [];
}
/** A commit's dated reverts: its transitions when known, else its first revert. */
function revertsOf(c: ForgeCommit): Transition[] {
  const what = (by: string | null | undefined) => `commit ${c.sha.slice(0, 12)} reverted${by ? ` by ${by.slice(0, 12)}` : ""}`;
  if (c.transitions) return c.transitions.filter((t) => t.kind === "reverted").map((t) => ({ at: t.at, ...positionOf(t), what: what(t.by) }));
  return c.revertedAt ? [{ at: c.revertedAt, what: what(c.revertedBy) }] : [];
}

export function deriveState(sessions: readonly SessionFacts[], forgeIn: ForgeFacts, asOfIn: string, unknownFlagDefault: "on" | "off" = "off"): StateResult {
  const forge = utcForge(forgeIn), asOf = requireCut(asOfIn, "asOf");
  const built = sessions.flatMap((s) => s.built);
  const shas = new Set(built.filter((b) => b.kind === "commit").map((b) => b.ref!));
  const prUrls = new Set(built.filter((b) => b.kind === "pr").map((b) => b.ref!));
  // A feature's flags: named anywhere in its own text (asks, claims, gaps, decisions, constraints).
  const FLAG = /\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*_ENABLED\b/gu;
  const flags = new Set(built.filter((b) => b.kind === "flag").map((b) => b.ref!));
  // Settings the docs give a default count too when the feature's text names them (not every gate is `*_ENABLED`).
  const SETTING = /\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b/gu;
  const documented = new Set(forge.flags.map((f) => f.name));
  for (const t of sessions.flatMap((s) => [...s.asked.map((a) => a.text), ...s.built.map((b) => b.what), ...s.gaps.map((g) => g.text), ...s.rationale.map((r) => r.text)])) {
    for (const m of t.matchAll(FLAG)) flags.add(m[0]);
    for (const m of t.matchAll(SETTING)) if (documented.has(m[0])) flags.add(m[0]);
  }
  const lastActivity = sessions.map((s) => s.endedAt).sort(textCompare).pop() ?? asOf;
  const stale = Date.parse(asOf) - Date.parse(lastActivity) > ABANDON_AFTER_MS;
  const latest = [...sessions].sort((a, b) => textCompare(a.endedAt, b.endedAt)).pop();
  // Partial means the request is materially unfinished (an unmet ask), not that follow-ups were listed:
  // almost every analysis lists "remaining" items, so `open` gaps never make a landed feature partial.
  const openGaps = (latest?.gaps ?? []).filter((g) => g.kind === "unmet");

  // Every forge fact counts only with a time strictly before the cut: a commit the mirror holds from later never
  // makes a feature a forge decision, even off the default branch.
  const commits = forge.commits.filter((c) => c.at < asOf && (shas.has(c.sha) || [...shas].some((s) => c.sha.startsWith(s) || s.startsWith(c.sha))));
  const branchNames = new Set(sessions.flatMap((s) => s.keys.branches));
  // A branch counts only for work near or after the feature's first session: a branch cut from main and pushed
  // with no commits of its own is an ancestor of main at an older tip, which is not this feature landing. The
  // lead allows for work committed before the first session the corpus holds (a zero lead cut shipped 69 -> 19
  // on the first corpus, nearly all of them real landings; seven days keeps 52).
  const firstStart = sessions.map((s) => s.startedAt).sort(textCompare)[0] ?? asOf;
  const lead = new Date(Date.parse(firstStart) - BRANCH_LEAD_MS).toISOString();
  const branches = (forge.branches ?? []).filter((b) => branchNames.has(b.name) && b.tipAt < asOf && b.tipAt >= lead);
  // The feature's pull requests, each opened before the cut: one a session created; one whose head branch is the
  // feature's branch, in this repository (`headSameRepo`: a fork's branch of the same name is someone else's work,
  // and an unknown head gets no branch credit), opened within the same lead (a head branch is deleted on merge, so
  // the live branches above rarely hold it, and a reused name from older work is not this feature); or one merged before the cut holding a
  // commit a session made or pushed (prefix of at least 7 characters). A PR's commit list is fixed when it merges, so
  // only a merge known before the cut says it held the commit then. Mentioned PRs alone never count.
  const ownShas = [...shas].filter((s) => s.length >= 7);
  const defaultBranch = forge.defaultBranch ?? "main";
  const prs = forge.prs.filter((p) => p.at < asOf && (prUrls.has(p.ref.toLowerCase())
    || (p.headSameRepo === true && !!p.branch && branchNames.has(p.branch) && p.at >= lead)
    || (p.state === "merged" && !!p.mergedAt && p.mergedAt < asOf && (p.commits ?? []).some((c) => ownShas.some((s) => c.startsWith(s))))));
  // A merged PR landed when its merge commit reached the default branch (`landedAt`; null: not yet), which for a
  // PR into a trunk or release branch is the later merge that brought it in. With no `landedAt`, a PR into another
  // base has no known landing (its trunk merge is not a shipment); one into the default branch, or from a forge
  // that records no base, landed at `mergedAt`.
  const prLanding = (p: (typeof prs)[number]) => (p.state !== "merged" ? null : p.landedAt !== undefined ? p.landedAt
    : p.base && p.base !== defaultBranch ? null : p.mergedAt ?? null);
  const merges: Transition[] = [
    ...prs.flatMap((p) => { const at = prLanding(p); return at && at < asOf ? [{ at, what: `merged ${p.ref.replace("https://github.com/", "")}${p.base && p.base !== defaultBranch ? ` into ${p.base}, on ${defaultBranch}` : ""}` }] : []; }),
    // Every landing of a commit's patch known before the cut (a re-landing after a revert restores it).
    ...commits.flatMap((c) => landingsOf(c)).filter((l) => l.at < asOf),
    ...branches.filter((b) => b.landed && b.landedAt && b.landedAt < asOf).map((b) => ({ at: b.landedAt!, ...(b.landedSeq !== undefined ? { seq: b.landedSeq } : {}), ...(b.landedStep !== undefined ? { step: b.landedStep } : {}), ...(b.landedUnorderedWith?.length ? { unorderedWith: b.landedUnorderedWith } : {}), what: `branch ${b.name} landed` })),
  ].sort(byTime);
  const reverts = commits.flatMap((c) => revertsOf(c)).filter((r) => r.at < asOf).sort(byTime);

  if (commits.length || prs.length || branches.length) {
    const lastMerge = latestTransition(merges);
    const lastRevert = latestTransition(reverts);
    // Regressed when the last revert follows the last landing. At one instant the default branch's integration
    // order decides; the same position (a branch whose tip is the revert) is the revert, never a re-landing; and a
    // tie git does not order reads as regressed (the conservative call).
    const revertLast = lastRevert && (!lastMerge || lastRevert.at > lastMerge.at || (lastRevert.at === lastMerge.at && (!ordered(lastRevert, lastMerge) || lastRevert.seq! >= lastMerge.seq!)));
    if (lastRevert && revertLast) return { state: "regressed", basis: "forge", reason: lastRevert.what };
    if (lastMerge) {
      // Flags the feature's own landed branches introduced count as its flags; a flag the docs do not list takes
      // `unknownFlagDefault` (this repository's rule: new behaviour lands behind a default-off flag).
      // Only a landing known to precede the cut: a branch that lands later (or at an unknown time) adds nothing yet.
      for (const b of branches) if (b.landed && b.landedAt && b.landedAt < asOf) for (const f of b.flagsAdded ?? []) flags.add(f);
      const known = new Map(forge.flags.map((f) => [f.name, f.defaultOn]));
      const off = [...flags].filter((f) => (known.get(f) ?? unknownFlagDefault === "on") === false).sort(textCompare);
      if (off.length) return { state: "shipped_off", basis: "forge", reason: `${lastMerge.what}; default off: ${off.join(", ")}` };
      if (openGaps.length) return { state: "partial", basis: "forge", reason: `${lastMerge.what}; ${openGaps.length} open item(s) in the latest session` };
      return { state: "shipped", basis: "forge", reason: lastMerge.what };
    }
    // Closed as of the cut: a close known to precede it. A PR closed later, or at an unknown time, was still open.
    if (prs.length && prs.every((p) => p.state === "closed" && p.closedAt && p.closedAt < asOf)) return { state: "abandoned", basis: "forge", reason: "every pull request closed without merging" };
    // Merged into its base before the cut, not yet on the default branch (a trunk awaiting its merge train): work
    // handed on, not dropped, however long since the session's last activity.
    const waiting = prs.find((p) => p.state === "merged" && p.base && p.base !== defaultBranch && p.mergedAt && p.mergedAt < asOf);
    if (waiting) return { state: "in_progress", basis: "forge", reason: `merged ${waiting.ref.replace("https://github.com/", "")} into ${waiting.base}, ${waiting.landedAt === undefined ? `no dated landing on ${defaultBranch}` : `not yet on ${defaultBranch}`}` };
    if (stale) return { state: "abandoned", basis: "forge", reason: `no merge and no activity since ${lastActivity}` };
    return { state: "in_progress", basis: "forge", reason: "commits or an open pull request, not merged" };
  }

  const statuses = sessions.flatMap((s) => s.outcomeStatuses);
  if (statuses.length && statuses.every((s) => s === "superseded")) return { state: "superseded", basis: "analysis", reason: "every outcome superseded" };
  const anyBuilt = built.some((b) => b.kind !== "flag");
  if (!anyBuilt) return stale
    ? { state: "abandoned", basis: "analysis", reason: `asked, nothing built, no activity since ${lastActivity}` }
    : { state: "planned", basis: "analysis", reason: "asked, nothing built yet" };
  if (stale) return { state: "abandoned", basis: "analysis", reason: `work without a merge the forge shows, no activity since ${lastActivity}` };
  return { state: "in_progress", basis: "analysis", reason: "work recorded, no merge the forge shows" };
}

/**
 * A commit already on the default branch when the session started is the base it worked from (a brief's
 * "base 9d1adf84"), not its work: it is dropped as a link key, so every review cut from one main tip does not
 * become one feature. The session's own built commits stay.
 */
export function withoutBaseCommits(f: SessionFacts, forgeIn: ForgeFacts): SessionFacts {
  const forge = utcForge(forgeIn);
  const builtShas = new Set(f.built.filter((b) => b.kind === "commit").map((b) => b.ref!));
  // On the default branch by the session's start: the time it became reachable from it (defaultAt), not its commit time.
  const base = (sha: string) => !builtShas.has(sha) && forge.commits.some((c) => c.onDefaultBranch && (c.defaultAt ?? c.at) < f.startedAt && (c.sha.startsWith(sha) || sha.startsWith(c.sha)));
  const commits = f.keys.commits.filter((sha) => !base(sha));
  return commits.length === f.keys.commits.length ? f : { ...f, keys: { ...f.keys, commits } };
}

/** At most this many feature branches may hold a commit for it to name its branch (a commit cut into many is shared). */
export const MAX_COMMIT_BRANCHES = 2;

/**
 * A session that names a commit (a reviewer given "review <sha>") without naming its branch: the forge's branches
 * holding that commit become the session's branch keys, so each review round of one branch links to the others
 * and to the session that pushed it. As of `asOf`, only branches whose whole history predates it count (tip before
 * `asOf`); a branch the mirror shows only later is never used for an earlier replay.
 */
export function withCommitBranches(f: SessionFacts, forgeIn: ForgeFacts, asOfIn: string): SessionFacts {
  const forge = utcForge(forgeIn), asOf = requireCut(asOfIn, "asOf");
  if (!f.keys.commits.length || !forge.branches?.length) return f;
  const known = forge.branches.filter((b) => b.tipAt < asOf);
  const added = new Set<string>();
  for (const sha of f.keys.commits) {
    if (sha.length < 7) continue;
    const holders = known.filter((b) => (b.commits ?? []).some((c) => c.startsWith(sha) || sha.startsWith(c)));
    if (holders.length && holders.length <= MAX_COMMIT_BRANCHES) for (const b of holders) added.add(b.name);
  }
  const branches = [...new Set([...f.keys.branches, ...added])].sort(textCompare);
  return branches.length === f.keys.branches.length ? f : { ...f, keys: { ...f.keys, branches } };
}

/** Stages 2 to 4: every feature record as of `asOf`. */
export function buildFeatures(all: readonly SessionFacts[], options: BuildOptions): FeatureRecord[] {
  const forge = utcForge(options.forge ?? EMPTY_FORGE);
  options = { ...options, asOf: requireCut(options.asOf, "asOf") };
  const facts = all.map((f) => factsAsOf(f, options.asOf)).filter((f): f is SessionFacts => f !== null).map((f) => withoutBaseCommits(f, forge)).map((f) => withCommitBranches(f, forge, options.asOf));
  const { groups: linked, links } = linkSessions(facts, options.link ?? DEFAULT_LINK_PARAMS);
  // A feature is founded by work: a group of approval assessors alone is review with no work it can name, and in
  // a mixed group the earliest working session founds it (the assessors stay as contributing sessions).
  const works = (s: SessionFacts) => s.role !== "approval_assessor";
  // An assessor's own analysis is of the review it ran (the action it judged, its verdict), not of the human's
  // request or the work: it contributes its sessions and link keys only, never asks, claims, gaps or outcomes.
  const asContributor = (s: SessionFacts): SessionFacts => (works(s) ? s : { ...s, asked: [], built: [], gaps: [], rationale: [], outcomeStatuses: [] });
  const groups = linked.filter((g) => g.some(works)).map((g) => { const i = g.findIndex(works); return (i === 0 ? g : [g[i]!, ...g.slice(0, i), ...g.slice(i + 1)]).map(asContributor); });
  return groups.map((sessions) => {
    const founding = sessions[0]!;
    const keys = new Set(sessions.map((s) => sessionKeyString(s)));
    const state = deriveState(sessions, forge, options.asOf);
    // At most MAX_FEATURE_QUOTES quotes per feature (docs, "Policy changes"), earliest asks first.
    let quoteBudget = MAX_FEATURE_QUOTES;
    const asked = sessions.flatMap((s) => s.asked).map((a) => {
      const quotes = a.quotes.slice(0, Math.max(0, quoteBudget));
      quoteBudget -= quotes.length;
      return quotes.length === a.quotes.length ? a : { ...a, quotes };
    });
    const times = sessions.flatMap((s) => [s.startedAt, s.endedAt]).sort(textCompare);
    return {
      id: stableId(["feature", founding.source, founding.sessionId]),
      title: asked[0]?.text ? asked[0].text.slice(0, 120) : founding.title || "(in progress, untitled)",
      repos: [...new Set(sessions.map((s) => s.keys.repo).filter((r): r is string => !!r))].sort(textCompare),
      state: state.state,
      stateBasis: state.basis,
      stateReason: state.reason,
      asked,
      built: sessions.flatMap((s) => s.built),
      gaps: sessions.flatMap((s) => s.gaps),
      rationale: sessions.flatMap((s) => s.rationale),
      sessions: sessions.map((s, i) => ({ source: s.source, sessionId: s.sessionId, role: i === 0 ? "founding" as const : "contributing" as const, startedAt: s.startedAt })),
      links: links.filter((l) => keys.has(sessionKeyString(l.a)) && keys.has(sessionKeyString(l.b))),
      firstAt: times[0]!,
      updatedAt: times[times.length - 1]!,
    };
  });
}
