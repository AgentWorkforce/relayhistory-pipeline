/**
 * Repository attribution from a session's own canonical evidence (docs/proposals/relayfacts-features.md,
 * "The onboarding backfill"). The session directory names a repository for few sessions (the uploader
 * often resolves none, a path-shaped project id is dropped for privacy, and some remote-derived keys
 * are scrubbed), so the backfill reads the canonical rows and takes, strongest first:
 *
 *   1. remote     the catalog's repo_url, or a remote-derived project_key that is not redacted;
 *   2. events     an `Owner/repo` project id carried by the session's events (the most frequent one);
 *   3. path       the repository directory implied by the working directory's layout
 *                 (`.../<repo>`, `.../<repo>/.claude/worktrees/<x>`, `.../worktrees/<repo>/<x>`),
 *                 which names the repository but not its owner.
 *
 * Pure apart from the snapshot it is given. No path is returned: only a repository name or slug.
 */
import { githubSlug } from "../features/repo.ts";
const SLUG = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u;
const GENERIC_DIRS = new Set(["src", "projects", "work", "workers", "worktrees", "tmp", "private", "users", "home", "volumes", "t7", "agentworkforce", "repos", "code", "dev", "build", "packages"]);

export interface RepoAttribution {
  repo: string | null;
  basis: "remote" | "events" | "path" | "none";
  /** Owner-less repository directory name from the working directory, when that is all there is. */
  repoName: string | null;
  branch: string | null;
}

export function slugFromRemote(value: unknown): string | null {
  return githubSlug(value);
}

/** The repository directory a working directory sits in, by layout; never the path itself. */
export function repoNameFromCwd(cwd: unknown): string | null {
  if (typeof cwd !== "string" || !cwd) return null;
  const parts = cwd.replace(/\\/gu, "/").split("/").filter(Boolean);
  const dot = parts.findIndex((p) => p === ".claude" || p === ".codex" || p === ".worktrees");
  if (dot > 0) return parts[dot - 1]!.toLowerCase();
  const wt = parts.findIndex((p) => p === "worktrees");
  if (wt >= 0 && parts.length > wt + 2) return parts[wt + 1]!.toLowerCase();
  // A home directory (/Users/<name>, /home/<name>) is a person, not a repository.
  if (parts.length <= 2 && (parts[0] === "Users" || parts[0] === "home")) return null;
  const last = parts[parts.length - 1]?.toLowerCase();
  return last && !GENERIC_DIRS.has(last) && !last.startsWith(".") ? last : null;
}

export function attributeRepo(rows: ReadonlyArray<{ table: string; row: Record<string, unknown> }>): RepoAttribution {
  let remote: string | null = null, cwd: unknown = null, branch: string | null = null;
  const eventSlugs = new Map<string, number>();
  const eventPaths = new Map<string, number>();
  for (const { table, row } of rows) {
    if (table === "session_catalog") {
      remote ??= slugFromRemote(row.repo_url) ?? (row.project_key_method === "remote" ? slugFromRemote(row.project_key) : null);
      cwd ??= row.cwd;
      if (typeof row.git_branch === "string" && row.git_branch) branch ??= row.git_branch;
    } else if (typeof row.project_id === "string" && SLUG.test(row.project_id)) {
      const s = row.project_id.toLowerCase();
      eventSlugs.set(s, (eventSlugs.get(s) ?? 0) + 1);
    } else if (typeof row.project_id === "string" && /^[/~]/u.test(row.project_id)) {
      eventPaths.set(row.project_id, (eventPaths.get(row.project_id) ?? 0) + 1);
    }
  }
  // Without a catalog working directory, the events' most frequent path-shaped project id stands in for it.
  cwd ??= [...eventPaths].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1))[0]?.[0] ?? null;
  const repoName = repoNameFromCwd(cwd);
  if (remote) return { repo: remote, basis: "remote", repoName, branch };
  const top = [...eventSlugs].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1))[0];
  if (top) return { repo: top[0], basis: "events", repoName, branch };
  return { repo: null, basis: repoName ? "path" : "none", repoName, branch };
}
