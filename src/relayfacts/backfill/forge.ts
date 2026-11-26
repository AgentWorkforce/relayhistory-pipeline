/**
 * Forge facts for a benchmark corpus (docs/proposals/relayfacts-features.md, "Corpus"): pull requests
 * (state, merge time, head branch) from `gh`, default-branch commits and reverts from a local git
 * mirror, and flag defaults from the repository's own docs tables. Reads only.
 *
 *   bun run src/relayfacts/backfill/forge.ts --repo <owner/name> --git <mirror dir> --since <iso> --until <iso> --out <forge.json>
 */
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import type { ForgeFacts } from "../features/types.ts";
import { parseForge } from "../bench/corpus.ts";

const git = (dir: string, args: string[]) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });

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
  return out;
}

export function forgeFacts(repo: string, mirror: string, since: string, until: string, defaultBranch = "main"): ForgeFacts {
  const prsRaw = execFileSync("gh", ["pr", "list", "-R", repo, "--state", "all", "--limit", "5000", "--search", `updated:>=${since.slice(0, 10)}`,
    "--json", "url,state,createdAt,mergedAt,closedAt,headRefName"], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
  const prs = (JSON.parse(prsRaw) as Array<Record<string, string | null>>)
    .filter((p) => (p.createdAt ?? "") < until)
    .map((p) => ({
      ref: String(p.url).toLowerCase(),
      state: p.state === "MERGED" ? "merged" as const : p.state === "CLOSED" ? "closed" as const : "open" as const,
      at: String(p.createdAt), mergedAt: p.mergedAt ?? null, closedAt: p.closedAt ?? null, branch: p.headRefName ?? null,
    }));
  const onDefault = new Set(git(mirror, ["rev-list", `--since=${since}`, `--until=${until}`, defaultBranch]).split("\n").filter(Boolean));
  const commits: ForgeFacts["commits"] = [];
  const reverted = new Map<string, { by: string; at: string }>();
  for (const line of git(mirror, ["log", "--all", `--since=${since}`, `--until=${until}`, "--format=%H%x09%cI%x09%B%x1e"]).split("\x1e")) {
    const [sha, at, ...body] = line.trim().split("\t");
    if (!sha || !at) continue;
    commits.push({ sha, at, onDefaultBranch: onDefault.has(sha) });
    const m = /This reverts commit ([0-9a-f]{7,40})/u.exec(body.join("\t"));
    if (m && onDefault.has(sha)) reverted.set(m[1]!, { by: sha, at });
  }
  // A branch commit the lander re-cut onto the default branch has a different sha but the same patch: map it
  // by `git patch-id --stable`, and date its landing by the default-branch copy.
  const patchOf = new Map<string, string>();
  const patchIds = execFileSync("sh", ["-c", `git -C "$0" log --all --since="$1" --until="$2" -p --format='commit %H' | git patch-id --stable`, mirror, since, until], { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 });
  for (const line of patchIds.split("\n")) { const [pid, sha] = line.split(" "); if (pid && sha) patchOf.set(sha, pid); }
  const landedByPatch = new Map<string, { sha: string; at: string }>();
  for (const c of commits) if (c.onDefaultBranch && patchOf.has(c.sha) && !landedByPatch.has(patchOf.get(c.sha)!)) landedByPatch.set(patchOf.get(c.sha)!, { sha: c.sha, at: c.at });
  for (const c of commits) {
    const r = reverted.get(c.sha) ?? [...reverted].find(([k]) => c.sha.startsWith(k))?.[1];
    const landed = !c.onDefaultBranch ? landedByPatch.get(patchOf.get(c.sha) ?? "") : undefined;
    if (landed) {
      c.onDefaultBranch = true;
      c.at = landed.at;
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
    // Flags the branch introduces: `*_ENABLED` names on added lines of its combined diff.
    const added = sh(`git -C "$0" diff "$(git -C "$0" merge-base "$2" "$1")" "$1" | grep '^+' | grep -oE '[A-Z][A-Z0-9]*(_[A-Z0-9]+)*_ENABLED' | sort -u`, mirror, name, defaultBranch);
    const flagsAdded = added.split("\n").filter(Boolean).slice(0, 50);
    branches.push({ name, landed: landedAt !== null, landedAt, tipAt, ...(flagsAdded.length ? { flagsAdded } : {}), ...(own.length ? { commits: own } : {}) });
  }
  let docs = "";
  try {
    for (const path of git(mirror, ["ls-tree", "-r", "--name-only", defaultBranch, "--", "docs"]).split("\n").filter((p) => p.endsWith(".md")))
      docs += git(mirror, ["show", `${defaultBranch}:${path}`]) + "\n";
  } catch { /* a repository without docs has no flag table */ }
  const flags = [...flagDefaults(docs)].map(([name, defaultOn]) => ({ name, defaultOn })).sort((a, b) => (a.name < b.name ? -1 : 1));
  return parseForge({ defaultBranch, prs, commits, flags, branches });
}

if (import.meta.main) {
  const { values } = parseArgs({ options: { repo: { type: "string" }, git: { type: "string" }, since: { type: "string" }, until: { type: "string" }, out: { type: "string" }, branch: { type: "string" } } });
  if (!values.repo || !values.git || !values.since || !values.until || !values.out) throw new Error("--repo, --git, --since, --until and --out are required");
  const facts = forgeFacts(values.repo, values.git, values.since, values.until, values.branch ?? "main");
  writeFileSync(values.out, JSON.stringify(facts));
  console.log(JSON.stringify({ prs: facts.prs.length, commits: facts.commits.length, onDefault: facts.commits.filter((c) => c.onDefaultBranch).length, reverted: facts.commits.filter((c) => c.revertedBy).length, flags: facts.flags.length, branches: facts.branches?.length ?? 0, landedBranches: facts.branches?.filter((b) => b.landed).length ?? 0 }));
}
