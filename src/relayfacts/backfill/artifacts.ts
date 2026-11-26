/**
 * Build evidence read from a session's own canonical events (docs/proposals/relayfacts-features.md,
 * "Policy changes": "built" is anchored in evidence). A session analysis samples tool calls, not their
 * results, so the commits a session pushed, the pull requests it opened, the files it touched and the
 * task it was delegated are taken here, at harvest, from the transcript itself.
 *
 * Output is bounded metadata only: repository slugs, branch names, short commit ids, PR URLs,
 * repository-relative paths, task ids, and the event id and time each came from. No transcript text.
 *
 *   push       `To <remote>` followed by `<old>..<new>  <src> -> <dst>` or `* [new branch] <src> -> <dst>`
 *   commit     `[<branch> <sha>] <subject>` (git commit without -q)
 *   pr         a github.com pull URL, kept only for a repository the session pushed to or is attributed to
 *   task       `Delegated task <id>` in a user or history event (never the generic `startup-*` ids)
 *   files      the events' files_touched, anchored at the repository root
 */
import { repoRelative } from "../features/extract.ts";
import { slugFromRemote } from "./locate.ts";

export interface ArtifactEvent { eventId: string; at: string | null }
export interface SessionArtifacts {
  pushes: Array<ArtifactEvent & { repo: string; branch: string; sha: string | null }>;
  commits: Array<ArtifactEvent & { sha: string; branch: string | null }>;
  prs: Array<ArtifactEvent & { url: string }>;
  taskRefs: Array<ArtifactEvent & { ref: string }>;
  files: Array<ArtifactEvent & { path: string }>;
  /** Feature branches the session created or pushed (`command`) or was told to work on or review (`mention`). */
  branches: Array<ArtifactEvent & { branch: string; via: "command" | "mention" }>;
}

export const ARTIFACT_BOUNDS = { pushes: 200, commits: 200, prs: 50, taskRefs: 10, files: 500, branches: 50 } as const;
const BRANCH_COMMANDS = [
  /\bgit\s+push\b(?:\s+-{1,2}[A-Za-z-]+)*\s+(?:-u\s+)?origin\s+(?:HEAD:)?(?:refs\/heads\/)?([A-Za-z0-9._-]+\/[A-Za-z0-9._/-]+)/gu,
  /\bgit\s+(?:checkout|switch)\s+(?:-b|-B|-c|-C|--create)\s+([A-Za-z0-9._-]+\/[A-Za-z0-9._/-]+)/gu,
  /\bgit\s+worktree\s+add\s+(?:-q\s+)?(?:-b|-B)\s+([A-Za-z0-9._-]+\/[A-Za-z0-9._/-]+)/gu,
];
/** A branch named in a brief: `branch <name>`, `Branch under review: <name>`, `on branch <name>`; the name must hold a slash. */
const BRANCH_MENTION = /\b[Bb]ranch(?: under review)?[:\s]+`?([A-Za-z0-9._-]+\/[A-Za-z0-9._/-]*[A-Za-z0-9_-])`?/gu;

const PUSH_BLOCK = /To ((?:https:\/\/|git@)github\.com[:/][A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+?(?:\.git)?)\s*\n((?:[ \t]+[^\n]*\n?)+)/gu;
const PUSH_LINE = /^\s+(?:\+?\s*([0-9a-f]{7,40})\.\.\.?([0-9a-f]{7,40})|\*\s+\[new (?:branch|tag)\])\s+([A-Za-z0-9._/-]+)\s+->\s+([A-Za-z0-9._/-]+)/gmu;
const COMMIT_LINE = /^\[([A-Za-z0-9._/-]+)(?: \(root-commit\))? ([0-9a-f]{7,40})\] /gmu;
const PR_URL = /https:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,10})(?![0-9A-Za-z_-])/gu;
const TASK = /Delegated task ([A-Za-z0-9][A-Za-z0-9._:-]{2,127})/gu;
const DEFAULT_BRANCH = /^(?:main|master|trunk|HEAD)$/u;

const text = (v: unknown) => (typeof v === "string" ? v : v == null ? "" : JSON.stringify(v));
const timeOf = (v: unknown) => (typeof v === "string" && Number.isFinite(Date.parse(v)) ? new Date(v).toISOString() : null);

export function extractArtifacts(rows: ReadonlyArray<{ table: string; row: Record<string, unknown> }>, attributedRepo: string | null): SessionArtifacts {
  const out: SessionArtifacts = { pushes: [], commits: [], prs: [], taskRefs: [], files: [], branches: [] };
  const seen = { pushes: new Set<string>(), commits: new Set<string>(), prs: new Set<string>(), taskRefs: new Set<string>(), files: new Set<string>(), branches: new Set<string>() };
  const addBranch = (ev: ArtifactEvent, raw: string, via: "command" | "mention") => {
    const branch = raw.replace(/[.,:;]+$/u, "");
    if (DEFAULT_BRANCH.test(branch) || branch.startsWith("origin/") || seen.branches.has(branch) || out.branches.length >= ARTIFACT_BOUNDS.branches) return;
    seen.branches.add(branch);
    out.branches.push({ ...ev, branch, via });
  };
  const prCandidates: Array<ArtifactEvent & { url: string; repo: string }> = [];
  for (const { table, row } of rows) {
    if (table !== "convergence_events") continue;
    const ev: ArtifactEvent = { eventId: String(row.event_id ?? ""), at: timeOf(row.ts) };
    if (!ev.eventId) continue;
    const type = String(row.type ?? "");
    const body = text(row.content);
    if (type === "tool_result" || row.kind === "tool_result") {
      for (const block of body.matchAll(PUSH_BLOCK)) {
        const repo = slugFromRemote(block[1]!);
        if (!repo) continue;
        for (const line of block[2]!.matchAll(PUSH_LINE)) {
          const branch = line[4]!;
          const key = `${repo}\u0000${branch}\u0000${line[2] ?? ""}`;
          if (seen.pushes.has(key) || out.pushes.length >= ARTIFACT_BOUNDS.pushes) continue;
          seen.pushes.add(key);
          out.pushes.push({ ...ev, repo, branch, sha: line[2]?.toLowerCase() ?? null });
          if (branch.includes("/")) addBranch(ev, branch, "command");
        }
      }
      for (const m of body.matchAll(COMMIT_LINE)) {
        const sha = m[2]!.toLowerCase();
        if (seen.commits.has(sha) || out.commits.length >= ARTIFACT_BOUNDS.commits) continue;
        seen.commits.add(sha);
        out.commits.push({ ...ev, sha, branch: DEFAULT_BRANCH.test(m[1]!) ? null : m[1]! });
      }
      for (const m of body.matchAll(PR_URL)) prCandidates.push({ ...ev, url: `https://github.com/${m[1]}/${m[2]}/pull/${m[3]}`.toLowerCase(), repo: `${m[1]}/${m[2]}`.toLowerCase() });
    }
    if (type === "tool_call" || row.kind === "tool_call") for (const re of BRANCH_COMMANDS) for (const m of body.matchAll(re)) addBranch(ev, m[1]!, "command");
    if (type === "user" || type === "history") for (const m of body.matchAll(BRANCH_MENTION)) addBranch(ev, m[1]!, "mention");
    if (type === "user" || type === "history") for (const m of body.matchAll(TASK)) {
      const ref = m[1]!.replace(/[.:]+$/u, "");
      if (ref.startsWith("startup-") || seen.taskRefs.has(ref) || out.taskRefs.length >= ARTIFACT_BOUNDS.taskRefs) continue;
      seen.taskRefs.add(ref);
      out.taskRefs.push({ ...ev, ref });
    }
    if (Array.isArray(row.files_touched)) for (const p of row.files_touched) {
      const rel = typeof p === "string" ? repoRelative(p) : null;
      if (!rel || seen.files.has(rel) || out.files.length >= ARTIFACT_BOUNDS.files) continue;
      seen.files.add(rel);
      out.files.push({ ...ev, path: rel });
    }
  }
  const repos = new Set([...out.pushes.map((p) => p.repo), ...(attributedRepo ? [attributedRepo] : [])]);
  for (const pr of prCandidates) {
    if (!repos.has(pr.repo) || seen.prs.has(pr.url) || out.prs.length >= ARTIFACT_BOUNDS.prs) continue;
    seen.prs.add(pr.url);
    out.prs.push({ eventId: pr.eventId, at: pr.at, url: pr.url });
  }
  return out;
}
