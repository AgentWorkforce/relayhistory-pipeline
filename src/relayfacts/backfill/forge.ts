/**
 * Forge facts for a benchmark corpus (docs/proposals/relayfacts-features.md, "Corpus"): pull requests
 * (state, merge time, head branch) from `gh`, default-branch commits and reverts from a local git
 * mirror, and flag defaults from the repository's own docs tables. Reads only.
 *
 *   bun run src/relayfacts/backfill/forge.ts --repo <owner/name> --git <mirror dir> --since <iso> --until <iso> --out <forge.json>
 */
import { execFileSync } from "node:child_process";
import { parseArgs } from "node:util";
import { requireUtc, utc, utcForge, type ForgeFacts } from "../features/types.ts";
import { parseForge } from "../bench/corpus.ts";
import { writeOutsideFile } from "../out-path.ts";
import { printable, UsageError } from "../errors.ts";

/** Git writes committer times with their own offsets: canonical UTC as read, so every comparison below is time order. */
const gitTime = (t: string) => requireUtc(t, "a git committer time");
const git = (dir: string, args: string[]) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });

/** A commit with more dated landings and reverts than this is refused: a truncated history would replay stale. */
export const MAX_TRANSITIONS = 200;

/** Time order, then integration order on the default branch for one instant (unordered ties keep input order). */
function transitionOrder(a: { at: string; seq?: number }, b: { at: string; seq?: number }): number {
  if (a.at !== b.at) return a.at < b.at ? -1 : 1;
  return a.seq !== undefined && b.seq !== undefined ? a.seq - b.seq : 0;
}

const VALUE_SETTING = /_(?:MS|S|SECONDS|TTL|MODE|PROVIDER|ENGINE|KIND|BUCKET|URL|ID|BYTES|SIZE|COUNT|MAX|MIN)$/u;

/** `FLAG` rows of markdown tables whose cells say the default is on or off. */
export function flagDefaults(markdown: string): Map<string, boolean> {
  const out = new Map<string, boolean>();
  for (const line of markdown.split("\n")) {
    if (!line.startsWith("|")) continue;
    const cells = line.split("|").map((c) => c.trim());
    const flag = /^`([A-Z][A-Z0-9_]*_ENABLED)`$/u.exec(cells[1] ?? "")?.[1];
    if (!flag) continue;
    // The cell's leading phrase decides ("Default on: ... off with `false`" is on); later words never do.
    const verdict = cells.slice(2).map((c) => /^(?:default\s+)?(on|off)\b/iu.exec(c)?.[1]?.toLowerCase()).find((v) => v !== undefined);
    if (verdict && !out.has(flag)) out.set(flag, verdict === "on");
  }
  // Prose: a backticked setting name in the same sentence as "default off" / "defaults to on" / "off by
  // default" (many gates are not `*_ENABLED`, such as INGEST_LEDGER_LOG_COMMIT). Tables win; the first
  // prose statement wins over later ones.
  const prose = new Map<string, boolean>();
  const text = markdown.split("\n").filter((l) => !l.trimStart().startsWith("|")).join("\n");
  for (const sentence of text.replace(/\n(?!\n)/gu, " ").split(/(?<=[.;])\s+|\n\n/u)) {
    const verdict = /\bdefaults?\s+(?:to\s+|is\s+)?(on|off|true|false)\b|\b(on|off)\s+by\s+default\b/iu.exec(sentence);
    if (!verdict) continue;
    const on = /^(?:on|true)$/iu.test(verdict[1] ?? verdict[2] ?? "");
    const names = [...sentence.matchAll(/`([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?:=[^`]*)?`/gu)].map((m) => m[1]!);
    // One verdict, one setting: a sentence naming several settings does not say which one it means.
    // Value settings (a duration, a mode, a provider) are not on/off gates even when "default off" describes them.
    if (names.length === 1 && !VALUE_SETTING.test(names[0]!) && !prose.has(names[0]!)) prose.set(names[0]!, on);
  }
  for (const [name, on] of prose) if (!out.has(name)) out.set(name, on);
  return out;
}

/** At most this many pull requests are listed; a repository with more is refused rather than read partially. */
export const MAX_LISTED_PRS = 50_000;

/**
 * The repository's pull requests from `gh` (the network step; tests pass their own), every one updated since
 * `since`. Listed in full and filtered here: GitHub's search API (`--search updated:>=...`) returns at most
 * 1,000 results, which silently truncated a busy repository's window.
 */
export function ghPullRequests(repo: string, since: string): Array<Record<string, string | null>> {
  const raw = execFileSync("gh", ["pr", "list", "-R", repo, "--state", "all", "--limit", String(MAX_LISTED_PRS),
    "--json", "url,state,createdAt,updatedAt,mergedAt,closedAt,headRefName"], { encoding: "utf8", maxBuffer: 1024 * 1024 * 1024 });
  return prsUpdatedSince(JSON.parse(raw) as Array<Record<string, string | null>>, since);
}

/** The full listing's pull requests updated since `since`; a listing at the cap is refused as partial. */
export function prsUpdatedSince(all: Array<Record<string, string | null>>, since: string, cap = MAX_LISTED_PRS): Array<Record<string, string | null>> {
  if (all.length >= cap) throw new UsageError(`gh pr list hit its ${cap} limit: the listing would be partial`);
  const from = requireUtc(since, "--since");
  return all.filter((p) => { const u = utc(p.updatedAt ?? null); return u !== null && u >= from; });
}

export function forgeFacts(repo: string, mirror: string, since: string, until: string, defaultBranch = "main", pullRequests: typeof ghPullRequests = ghPullRequests, maxTransitions = MAX_TRANSITIONS): ForgeFacts {
  const listed = pullRequests(repo, since);
  const prs = listed
    .filter((p) => { const c = utc(p.createdAt ?? null); return c !== null && c < requireUtc(until, "--until"); })
    .map((p) => ({
      ref: String(p.url).toLowerCase(),
      state: p.state === "MERGED" ? "merged" as const : p.state === "CLOSED" ? "closed" as const : "open" as const,
      at: String(p.createdAt), mergedAt: p.mergedAt ?? null, closedAt: p.closedAt ?? null, branch: p.headRefName ?? null,
    }));
  const sh = (cmd: string, ...args: string[]) => { try { return execFileSync("sh", ["-c", cmd, ...args], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 }).trim(); } catch { return ""; } };
  // Git commands take full refs (a tag or path named like a branch is never ambiguous); names are stored short.
  const mainRef = `refs/heads/${defaultBranch}`;
  const onDefault = new Set(git(mirror, ["rev-list", `--since=${since}`, `--until=${until}`, mainRef]).split("\n").filter(Boolean));
  // When each commit reached the default branch, and how that time is known (`defaultAtBasis`):
  // - `merge`: a commit a first-parent merge brought in, at that merge's committer time;
  // - `patch`: a branch commit whose patch landed as a default-branch commit, at that copy's time (set below);
  // - `committer`: a first-parent commit itself (a squash, a direct push, a fast-forward), at its committer time.
  //   The mirror holds no push times, so a commit made before T and pushed after T reads as on the branch at its
  //   commit time: an approximation a strict replay can exclude by this basis.
  const defaultAt = new Map<string, string>();
  const basis = new Map<string, "merge" | "committer">();
  // Integration order on the default branch, a total order: walking first-parent commits oldest first, the
  // commits a merge brings in take the next positions in their own topological order (parents before children),
  // then the merge itself. It orders transitions that share an instant (a merge dates all it brings in at once;
  // git times are whole seconds). `stepOf` is the first-parent step that integrated each commit: two commits of
  // one step that are not ancestor and descendant have no order of their own (parallel side branches).
  const seqOf = new Map<string, number>();
  const stepOf = new Map<string, number>();
  const fp = git(mirror, ["log", "--first-parent", "--format=%H %P%x09%cI", `--since=${since}`, mainRef]).split("\n").filter(Boolean);
  let seq = 0, step = 0;
  for (const l of fp.reverse()) {
    const [ids, rawAt] = l.split("\t");
    const [sha, first, ...merged] = ids!.split(" ");
    if (!sha || !rawAt) continue;
    const at = gitTime(rawAt);
    defaultAt.set(sha, at);
    basis.set(sha, "committer");
    if (first && merged.length) for (const m of merged) for (const c of git(mirror, ["rev-list", "--reverse", "--topo-order", `${first}..${m}`]).split("\n").filter(Boolean)) if (!defaultAt.has(c)) { defaultAt.set(c, at); basis.set(c, "merge"); seqOf.set(c, seq++); stepOf.set(c, step); }
    seqOf.set(sha, seq++);
    stepOf.set(sha, step++);
  }
  const commits: ForgeFacts["commits"] = [];
  // Every default-branch revert, by the (possibly abbreviated) sha it names.
  const revertsNamed: Array<{ target: string; by: string; at: string }> = [];
  for (const line of git(mirror, ["log", "--all", `--since=${since}`, `--until=${until}`, "--format=%H%x09%cI%x09%B%x1e"]).split("\x1e")) {
    const [sha, rawAt, ...body] = line.trim().split("\t");
    if (!sha || !rawAt) continue;
    const at = gitTime(rawAt);
    commits.push({ sha, at, onDefaultBranch: onDefault.has(sha), ...(defaultAt.has(sha) ? { defaultAt: defaultAt.get(sha)!, defaultAtBasis: basis.get(sha)! } : {}) });
    const m = /This reverts commit ([0-9a-f]{7,40})/u.exec(body.join("\t"));
    // A revert takes effect when the reverting commit reached the default branch (a revert made on a side
    // branch and merged later is dated by the merge), not when it was written.
    if (m && onDefault.has(sha)) revertsNamed.push({ target: m[1]!, by: sha, at: defaultAt.get(sha) ?? at });
  }
  // A branch commit the lander re-cut onto the default branch has a different sha but the same patch: map it
  // by `git patch-id --stable`, and date its landing by the default-branch copy. Its own commit time (`at`) is
  // kept: it existed from then, off the default branch, and only `defaultAt` says when it reached it.
  const patchOf = new Map<string, string>();
  const patchIds = execFileSync("sh", ["-c", `git -C "$0" log --all --since="$1" --until="$2" -p --format='commit %H' | git patch-id --stable`, mirror, since, until], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });
  for (const line of patchIds.split("\n")) { const [pid, sha] = line.split(" "); if (pid && sha) patchOf.set(sha, pid); }
  // Every default-branch copy of each patch, oldest first (git log lists newest first).
  const copiesOf = new Map<string, Array<{ sha: string; at: string; seq?: number }>>();
  for (const c of commits) {
    const pid = patchOf.get(c.sha);
    if (!c.onDefaultBranch || !pid) continue;
    const list = copiesOf.get(pid) ?? [];
    list.push({ sha: c.sha, at: c.defaultAt ?? c.at, ...(seqOf.has(c.sha) ? { seq: seqOf.get(c.sha)! } : {}) });
    copiesOf.set(pid, list);
  }
  for (const list of copiesOf.values()) list.sort(transitionOrder);
  const landedByPatch = new Map([...copiesOf].map(([pid, list]) => [pid, list[0]!]));
  const position = (sha: string) => (seqOf.has(sha) ? { seq: seqOf.get(sha)!, step: stepOf.get(sha)! } : {});
  const revertsOf = (sha: string) => revertsNamed.filter((r) => sha.startsWith(r.target)).map((r) => ({ ...r, ...(seqOf.has(r.by) ? { seq: seqOf.get(r.by)! } : {}) }));
  // A commit's effective history on the default branch, every transition dated by when it reached it: each
  // landing of its patch (the commit itself, a re-cut copy, a re-apply after a revert, a revert of the revert:
  // all share the patch id) and each revert of any of those copies. The state as of T is the last transition
  // before T, so a landing, revert and re-landing replay as shipped, regressed, shipped.
  for (const c of commits) {
    const pid = patchOf.get(c.sha);
    const copies = (pid ? copiesOf.get(pid) : undefined) ?? (c.onDefaultBranch ? [{ sha: c.sha, at: c.defaultAt ?? c.at, ...(seqOf.has(c.sha) ? { seq: seqOf.get(c.sha)! } : {}) }] : []);
    if (!c.onDefaultBranch && copies.length) {
      c.onDefaultBranch = true;
      c.defaultAt = copies[0]!.at;
      c.defaultAtBasis = "patch";
    }
    const transitions = [
      ...copies.map((x) => ({ at: x.at, kind: "landed" as const, by: x.sha, ...position(x.sha) })),
      ...copies.flatMap((x) => revertsOf(x.sha)).map((r) => ({ at: r.at, kind: "reverted" as const, by: r.by, ...position(r.by) })),
    ].sort(transitionOrder);
    // Within one integration step, an order between commits that are not ancestor and descendant is not known:
    // those transitions are marked unordered (within their step only: their step still orders them against every
    // other step), so a tie between them reads conservatively.
    for (let i = 0; i < transitions.length; i++) for (let j = i + 1; j < transitions.length; j++) {
      const a = transitions[i]!, b = transitions[j]!;
      if (a.seq === undefined || b.seq === undefined || stepOf.get(a.by) !== stepOf.get(b.by) || a.by === b.by) continue;
      const [lo, hi] = a.seq < b.seq ? [a, b] : [b, a];
      if (sh(`git -C "$0" merge-base --is-ancestor "$1" "$2" && echo yes`, mirror, lo.by, hi.by) !== "yes") { (a as { unordered?: true }).unordered = true; (b as { unordered?: true }).unordered = true; }
    }
    if (transitions.length > maxTransitions) throw new UsageError(`a commit has more than ${maxTransitions} landings and reverts: its history cannot be kept whole`);
    const firstRevert = transitions.find((t) => t.kind === "reverted");
    if (firstRevert) { c.revertedBy = firstRevert.by; c.revertedAt = firstRevert.at; }
    // Kept whenever the history is more than "this commit landed as itself": a landing by a copy records its copy
    // from the first seal on, so a later revert never changes how an earlier landing reads.
    if (transitions.length > 1 || firstRevert || transitions.some((t) => t.by !== c.sha)) c.transitions = transitions;
  }
  // Branch landing: a branch's work reached the default branch when its tip is an ancestor of it, when any of
  // its own commits has a default-branch copy with the same patch id, or when its combined diff (a squash) does.
  // A patch reached the default branch when its first default-branch copy did (that copy's defaultAt).
  const mainPatch = landedByPatch;
  let docs = "";
  try {
    for (const path of git(mirror, ["ls-tree", "-r", "--name-only", mainRef, "--", "docs"]).split("\n").filter((p) => p.endsWith(".md")))
      docs += git(mirror, ["show", `${mainRef}:${path}`]) + "\n";
  } catch { /* a repository without docs has no flag table */ }
  const known = flagDefaults(docs);
  const flags = [...known].map(([name, defaultOn]) => ({ name, defaultOn })).sort((a, b) => (a.name < b.name ? -1 : 1));
  const branches: NonNullable<ForgeFacts["branches"]> = [];
  for (const ref of git(mirror, ["for-each-ref", "--format=%(refname)", "refs/heads"]).split("\n").filter((r) => r.startsWith("refs/heads/") && r !== mainRef)) {
    const name = ref.slice("refs/heads/".length);
    const rawTip = git(mirror, ["log", "-1", "--format=%cI", ref]).trim();
    if (!rawTip) continue;
    const tipAt = gitTime(rawTip);
    if (tipAt < requireUtc(since, "--since")) continue;
    const ancestor = sh(`git -C "$0" merge-base --is-ancestor "$1" "$2" && echo yes`, mirror, ref, mainRef) === "yes";
    // An ancestral landing is dated by when the tip first became reachable from the default branch (the merge
    // that brought it in, or its own time for a fast-forward), never by the tip's commit time. A tip older than
    // the walk (reachable before `since`) has no known landing time inside the window: its tip time stands.
    const tip = git(mirror, ["rev-parse", "--verify", `${ref}^{commit}`]).trim();
    let landedAt: string | null = ancestor ? (defaultAt.get(tip) ?? tipAt) : null;
    // The landing's integration position, so a branch landing orders against commit transitions at one instant.
    let landedSeq: number | undefined = ancestor ? seqOf.get(tip) : undefined;
    let landedStep: number | undefined = ancestor ? stepOf.get(tip) : undefined;
    const own = git(mirror, ["rev-list", "--max-count=200", `${mainRef}..${ref}`]).split("\n").filter(Boolean);
    if (!landedAt) for (const sha of own) {
      const copy = mainPatch.get(patchOf.get(sha) ?? "");
      if (copy && (!landedAt || transitionOrder(copy, { at: landedAt, ...(landedSeq !== undefined ? { seq: landedSeq } : {}) }) > 0)) { landedAt = copy.at; landedSeq = copy.seq; landedStep = stepOf.get(copy.sha); }
    }
    if (!landedAt) {
      const pid = sh(`git -C "$0" diff "$(git -C "$0" merge-base "$2" "$1")" "$1" | git patch-id --stable | cut -d' ' -f1`, mirror, ref, mainRef);
      const copy = pid ? mainPatch.get(pid) : undefined;
      if (copy) { landedAt = copy.at; landedSeq = copy.seq; landedStep = stepOf.get(copy.sha); }
    }
    // Flags the branch introduces: `*_ENABLED` names, and settings the docs give a default, on added lines.
    const added = sh(`git -C "$0" diff "$(git -C "$0" merge-base "$2" "$1")" "$1" | grep '^+' | grep -oE '[A-Z][A-Z0-9]*(_[A-Z0-9]+)+' | sort -u`, mirror, ref, mainRef);
    const flagsAdded = added.split("\n").filter((f) => f && (f.endsWith("_ENABLED") || known.has(f))).slice(0, 50);
    branches.push({ name, landed: landedAt !== null, landedAt, ...(landedAt !== null && landedSeq !== undefined ? { landedSeq, ...(landedStep !== undefined ? { landedStep } : {}) } : {}), tipAt, ...(flagsAdded.length ? { flagsAdded } : {}), ...(own.length ? { commits: own } : {}) });
  }
  return utcForge(parseForge({ defaultBranch, prs, commits, flags, branches }));
}

if (import.meta.main) {
  try {
    const { values } = parseArgs({ options: { repo: { type: "string" }, git: { type: "string" }, since: { type: "string" }, until: { type: "string" }, out: { type: "string" }, branch: { type: "string" } } });
    if (!values.repo || !values.git || !values.since || !values.until || !values.out) throw new UsageError("--repo, --git, --since, --until and --out are required");
    const facts = forgeFacts(values.repo, values.git, values.since, values.until, values.branch ?? "main");
    writeOutsideFile(values.out, JSON.stringify(facts));
    console.log(JSON.stringify({ prs: facts.prs.length, commits: facts.commits.length, onDefault: facts.commits.filter((c) => c.onDefaultBranch).length, reverted: facts.commits.filter((c) => c.revertedBy).length, flags: facts.flags.length, branches: facts.branches?.length ?? 0, landedBranches: facts.branches?.filter((b) => b.landed).length ?? 0 }));
  } catch (error) {
    console.error(printable(error));
    process.exit(1);
  }
}
