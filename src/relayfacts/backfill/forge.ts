/**
 * Forge facts for a benchmark corpus (docs/proposals/relayfacts-features.md, "Corpus"): pull requests
 * (state, merge time, head branch) from `gh`, default-branch commits and reverts from a local git
 * mirror, and flag defaults from the repository's own docs tables. Reads only.
 *
 *   bun run src/relayfacts/backfill/forge.ts --repo <owner/name> --git <mirror dir> --since <iso> --until <iso> --out <forge.json>
 */
import { execFileSync } from "node:child_process";
import { parseArgs } from "node:util";
import type { ForgeFacts } from "../features/types.ts";
import { parseForge } from "../bench/corpus.ts";
import { writeOutsideFile } from "../out-path.ts";
import { printable, UsageError } from "../errors.ts";

const git = (dir: string, args: string[]) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });

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

/** The repository's pull requests from `gh` (the network step; tests pass their own). */
export function ghPullRequests(repo: string, since: string): Array<Record<string, string | null>> {
  const raw = execFileSync("gh", ["pr", "list", "-R", repo, "--state", "all", "--limit", "5000", "--search", `updated:>=${since.slice(0, 10)}`,
    "--json", "url,state,createdAt,mergedAt,closedAt,headRefName"], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
  return JSON.parse(raw) as Array<Record<string, string | null>>;
}

export function forgeFacts(repo: string, mirror: string, since: string, until: string, defaultBranch = "main", pullRequests: typeof ghPullRequests = ghPullRequests): ForgeFacts {
  const listed = pullRequests(repo, since);
  if (listed.length >= 5000) throw new UsageError("gh pr list hit its 5000 limit: narrow the window");
  const prs = listed
    .filter((p) => (p.createdAt ?? "") < until)
    .map((p) => ({
      ref: String(p.url).toLowerCase(),
      state: p.state === "MERGED" ? "merged" as const : p.state === "CLOSED" ? "closed" as const : "open" as const,
      at: String(p.createdAt), mergedAt: p.mergedAt ?? null, closedAt: p.closedAt ?? null, branch: p.headRefName ?? null,
    }));
  const onDefault = new Set(git(mirror, ["rev-list", `--since=${since}`, `--until=${until}`, defaultBranch]).split("\n").filter(Boolean));
  // When each commit reached the default branch, and how that time is known (`defaultAtBasis`):
  // - `merge`: a commit a first-parent merge brought in, at that merge's committer time;
  // - `patch`: a branch commit whose patch landed as a default-branch commit, at that copy's time (set below);
  // - `committer`: a first-parent commit itself (a squash, a direct push, a fast-forward), at its committer time.
  //   The mirror holds no push times, so a commit made before T and pushed after T reads as on the branch at its
  //   commit time: an approximation a strict replay can exclude by this basis.
  const defaultAt = new Map<string, string>();
  const basis = new Map<string, "merge" | "committer">();
  const fp = git(mirror, ["log", "--first-parent", "--format=%H %P%x09%cI", `--since=${since}`, defaultBranch]).split("\n").filter(Boolean);
  for (const l of fp.reverse()) {
    const [ids, at] = l.split("\t");
    const [sha, first, ...merged] = ids!.split(" ");
    if (!sha || !at) continue;
    defaultAt.set(sha, at);
    basis.set(sha, "committer");
    if (first && merged.length) for (const m of merged) for (const c of git(mirror, ["rev-list", `${first}..${m}`]).split("\n").filter(Boolean)) if (!defaultAt.has(c)) { defaultAt.set(c, at); basis.set(c, "merge"); }
  }
  const commits: ForgeFacts["commits"] = [];
  const reverted = new Map<string, { by: string; at: string }>();
  for (const line of git(mirror, ["log", "--all", `--since=${since}`, `--until=${until}`, "--format=%H%x09%cI%x09%B%x1e"]).split("\x1e")) {
    const [sha, at, ...body] = line.trim().split("\t");
    if (!sha || !at) continue;
    commits.push({ sha, at, onDefaultBranch: onDefault.has(sha), ...(defaultAt.has(sha) ? { defaultAt: defaultAt.get(sha)!, defaultAtBasis: basis.get(sha)! } : {}) });
    const m = /This reverts commit ([0-9a-f]{7,40})/u.exec(body.join("\t"));
    if (m && onDefault.has(sha)) reverted.set(m[1]!, { by: sha, at });
  }
  // A branch commit the lander re-cut onto the default branch has a different sha but the same patch: map it
  // by `git patch-id --stable`, and date its landing by the default-branch copy.
  const patchOf = new Map<string, string>();
  const patchIds = execFileSync("sh", ["-c", `git -C "$0" log --all --since="$1" --until="$2" -p --format='commit %H' | git patch-id --stable`, mirror, since, until], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });
  for (const line of patchIds.split("\n")) { const [pid, sha] = line.split(" "); if (pid && sha) patchOf.set(sha, pid); }
  const landedByPatch = new Map<string, { sha: string; at: string }>();
  for (const c of commits) if (c.onDefaultBranch && patchOf.has(c.sha) && !landedByPatch.has(patchOf.get(c.sha)!)) landedByPatch.set(patchOf.get(c.sha)!, { sha: c.sha, at: c.defaultAt ?? c.at });
  for (const c of commits) {
    const r = reverted.get(c.sha) ?? [...reverted].find(([k]) => c.sha.startsWith(k))?.[1];
    const landed = !c.onDefaultBranch ? landedByPatch.get(patchOf.get(c.sha) ?? "") : undefined;
    if (landed) {
      c.onDefaultBranch = true;
      c.at = landed.at;
      c.defaultAt = landed.at;
      c.defaultAtBasis = "patch";
      const lr = reverted.get(landed.sha) ?? [...reverted].find(([k]) => landed.sha.startsWith(k))?.[1];
      if (lr) { c.revertedBy = lr.by; c.revertedAt = lr.at; }
    }
    if (r) { c.revertedBy = r.by; c.revertedAt = r.at; }
  }
  // Branch landing: a branch's work reached the default branch when its tip is an ancestor of it, when any of
  // its own commits has a default-branch copy with the same patch id, or when its combined diff (a squash) does.
  const mainPatch = new Map<string, string>();
  for (const c of commits) if (c.onDefaultBranch) { const pid = patchOf.get(c.sha); if (pid && !mainPatch.has(pid)) mainPatch.set(pid, c.at); }
  const sh = (cmd: string, ...args: string[]) => { try { return execFileSync("sh", ["-c", cmd, ...args], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 }).trim(); } catch { return ""; } };
  let docs = "";
  try {
    for (const path of git(mirror, ["ls-tree", "-r", "--name-only", defaultBranch, "--", "docs"]).split("\n").filter((p) => p.endsWith(".md")))
      docs += git(mirror, ["show", `${defaultBranch}:${path}`]) + "\n";
  } catch { /* a repository without docs has no flag table */ }
  const known = flagDefaults(docs);
  const flags = [...known].map(([name, defaultOn]) => ({ name, defaultOn })).sort((a, b) => (a.name < b.name ? -1 : 1));
  const branches: NonNullable<ForgeFacts["branches"]> = [];
  for (const name of git(mirror, ["for-each-ref", "--format=%(refname:short)", "refs/heads"]).split("\n").filter((b) => b && b !== defaultBranch)) {
    const tipAt = git(mirror, ["log", "-1", "--format=%cI", name]).trim();
    if (!tipAt || tipAt < since) continue;
    const ancestor = sh(`git -C "$0" merge-base --is-ancestor "$1" "$2" && echo yes`, mirror, name, defaultBranch) === "yes";
    let landedAt: string | null = ancestor ? tipAt : null;
    const own = git(mirror, ["rev-list", "--max-count=200", `${defaultBranch}..${name}`]).split("\n").filter(Boolean);
    if (!landedAt) for (const sha of own) {
      const at = mainPatch.get(patchOf.get(sha) ?? "");
      if (at && (!landedAt || at > landedAt)) landedAt = at;
    }
    if (!landedAt) {
      const pid = sh(`git -C "$0" diff "$(git -C "$0" merge-base "$2" "$1")" "$1" | git patch-id --stable | cut -d' ' -f1`, mirror, name, defaultBranch);
      if (pid) landedAt = mainPatch.get(pid) ?? null;
    }
    // Flags the branch introduces: `*_ENABLED` names, and settings the docs give a default, on added lines.
    const added = sh(`git -C "$0" diff "$(git -C "$0" merge-base "$2" "$1")" "$1" | grep '^+' | grep -oE '[A-Z][A-Z0-9]*(_[A-Z0-9]+)+' | sort -u`, mirror, name, defaultBranch);
    const flagsAdded = added.split("\n").filter((f) => f && (f.endsWith("_ENABLED") || known.has(f))).slice(0, 50);
    branches.push({ name, landed: landedAt !== null, landedAt, tipAt, ...(flagsAdded.length ? { flagsAdded } : {}), ...(own.length ? { commits: own } : {}) });
  }
  return parseForge({ defaultBranch, prs, commits, flags, branches });
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
