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
import { EMPTY_FORGE, sessionKeyString, textCompare, utc, utcForge } from "./types.ts";

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
  const asOf = utc(asOfIn)!;
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

export function deriveState(sessions: readonly SessionFacts[], forgeIn: ForgeFacts, asOfIn: string, unknownFlagDefault: "on" | "off" = "off"): StateResult {
  const forge = utcForge(forgeIn), asOf = utc(asOfIn)!;
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

  const commits = forge.commits.filter((c) => shas.has(c.sha) || [...shas].some((s) => c.sha.startsWith(s) || s.startsWith(c.sha)));
  const prs = forge.prs.filter((p) => prUrls.has(p.ref.toLowerCase()) && p.at < asOf);
  const branchNames = new Set(sessions.flatMap((s) => s.keys.branches));
  // A branch counts only for work near or after the feature's first session: a branch cut from main and pushed
  // with no commits of its own is an ancestor of main at an older tip, which is not this feature landing. The
  // lead allows for work committed before the first session the corpus holds (a zero lead cut shipped 69 -> 19
  // on the first corpus, nearly all of them real landings; seven days keeps 52).
  const firstStart = sessions.map((s) => s.startedAt).sort(textCompare)[0] ?? asOf;
  const branches = (forge.branches ?? []).filter((b) => branchNames.has(b.name) && b.tipAt < asOf && b.tipAt >= new Date(Date.parse(firstStart) - BRANCH_LEAD_MS).toISOString());
  const merges = [
    ...prs.filter((p) => p.state === "merged" && p.mergedAt && p.mergedAt < asOf).map((p) => ({ at: p.mergedAt!, what: `merged ${p.ref.replace("https://github.com/", "")}` })),
    ...commits.filter((c) => c.onDefaultBranch && (c.defaultAt ?? c.at) < asOf).map((c) => ({ at: c.defaultAt ?? c.at, what: `commit ${c.sha.slice(0, 12)} on the default branch` })),
    ...branches.filter((b) => b.landed && b.landedAt && b.landedAt < asOf).map((b) => ({ at: b.landedAt!, what: `branch ${b.name} landed` })),
  ].sort((a, b) => textCompare(a.at, b.at));
  const reverts = commits.filter((c) => c.revertedAt && c.revertedAt < asOf).map((c) => ({ at: c.revertedAt!, what: `commit ${c.sha.slice(0, 12)} reverted${c.revertedBy ? ` by ${c.revertedBy.slice(0, 12)}` : ""}` })).sort((a, b) => textCompare(a.at, b.at));

  if (commits.length || prs.length || branches.length) {
    const lastMerge = merges[merges.length - 1];
    const lastRevert = reverts[reverts.length - 1];
    if (lastRevert && (!lastMerge || lastRevert.at >= lastMerge.at)) return { state: "regressed", basis: "forge", reason: lastRevert.what };
    if (lastMerge) {
      // Flags the feature's own landed branches introduced count as its flags; a flag the docs do not list takes
      // `unknownFlagDefault` (this repository's rule: new behaviour lands behind a default-off flag).
      for (const b of branches) if (b.landed) for (const f of b.flagsAdded ?? []) flags.add(f);
      const known = new Map(forge.flags.map((f) => [f.name, f.defaultOn]));
      const off = [...flags].filter((f) => (known.get(f) ?? unknownFlagDefault === "on") === false).sort(textCompare);
      if (off.length) return { state: "shipped_off", basis: "forge", reason: `${lastMerge.what}; default off: ${off.join(", ")}` };
      if (openGaps.length) return { state: "partial", basis: "forge", reason: `${lastMerge.what}; ${openGaps.length} open item(s) in the latest session` };
      return { state: "shipped", basis: "forge", reason: lastMerge.what };
    }
    if (prs.length && prs.every((p) => p.state === "closed")) return { state: "abandoned", basis: "forge", reason: "every pull request closed without merging" };
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
  const forge = utcForge(forgeIn), asOf = utc(asOfIn)!;
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
  options = { ...options, asOf: utc(options.asOf)! };
  const facts = all.map((f) => factsAsOf(f, options.asOf)).filter((f): f is SessionFacts => f !== null).map((f) => withoutBaseCommits(f, forge)).map((f) => withCommitBranches(f, forge, options.asOf));
  const { groups, links } = linkSessions(facts, options.link ?? DEFAULT_LINK_PARAMS);
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
