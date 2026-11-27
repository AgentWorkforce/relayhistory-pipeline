/**
 * Repository and path normalisation for the overlap benchmark (docs/session-intel-overlap-bench.md, "Paths").
 * Agents work in many checkouts of one repository (worktrees, scratch clones, T7 copies), so one file appears
 * under many absolute paths. Every write is reduced to `(repo, repo-relative path)` or dropped.
 */

/** Directories that hold extra checkouts of a repository: `<root>/<marker>/<name>/<repo-relative path>`. */
const WORKTREE_MARKERS = [".claude/worktrees", ".codex/worktrees", ".cursor/worktrees", ".worktrees", "worktrees"];

/**
 * `owner/name` from a project id or cwd such as `AgentWorkforce/cloud`, `~/Projects/AgentWorkforce/cloud`,
 * `/Volumes/Ext/Projects/AgentWorkforce/worktrees/cloud/fix-x` or a GitHub URL. Null when no owner segment is found.
 */
export function repoOf(projectOrPath: string | null | undefined, owner = "AgentWorkforce"): string | null {
  if (!projectOrPath) return null;
  const s = projectOrPath.replace(/\\/g, "/").replace(/\.git$/, "");
  const m = new RegExp(`(?:^|/|github\\.com[:/])${owner}/(?:worktrees/)?([A-Za-z0-9._-]+)`, "i").exec(s);
  return m ? `${owner}/${m[1]!.toLowerCase()}` : null;
}

/**
 * The repo-relative form of a written path, given the session's repository. Handles absolute paths under any
 * checkout of the repository, worktree checkouts, and paths that are already relative. Returns null for paths
 * outside the repository (home dotfiles, /tmp scratch, other repositories).
 */
export function repoRelative(path: string, repo: string | null): string | null {
  let p = path.trim().replace(/\\/g, "/").replace(/\/+/g, "/");
  if (!p) return null;
  if (!p.startsWith("/") && !p.startsWith("~")) return clean(p);
  if (!repo) return null;
  const [owner, name] = repo.split("/") as [string, string];
  // `/…/<Owner>/worktrees/<name>/<branch>/rest` (the T7 worktree helper) and `/…/<Owner>/<name>/rest`.
  for (const re of [new RegExp(`/${owner}/worktrees/${name}/[^/]+/(.+)$`, "i"), new RegExp(`/${owner}/${name}/(.+)$`, "i")]) {
    const m = re.exec(p);
    if (m) { p = m[1]!; return stripWorktree(p); }
  }
  return null;
}

function stripWorktree(p: string): string | null {
  for (const marker of WORKTREE_MARKERS) {
    const i = p.indexOf(`${marker}/`);
    if (i === 0 || (i > 0 && p[i - 1] === "/")) {
      const rest = p.slice(i + marker.length + 1);
      const slash = rest.indexOf("/");
      return slash > 0 ? clean(rest.slice(slash + 1)) : null;
    }
  }
  return clean(p);
}

function clean(p: string): string | null {
  const s = p.replace(/^\.\//, "");
  if (!s || s.startsWith("../") || s.includes("/../") || s.length > 1024) return null;
  return s;
}

/** Files every change touches: lockfiles, generated snapshots and journals. Kept separately, never as "rare". */
export const HOT_PATH = /(?:^|\/)(?:package-lock\.json|bun\.lockb?|pnpm-lock\.yaml|yarn\.lock|Cargo\.lock|_journal\.json|CHANGELOG\.md)$|\/meta\/\d+_snapshot\.json$/i;
