/**
 * Derived event facts (docs/session-intel-cloud-harvest.md, "Facts"). This is the privacy boundary: a raw event
 * goes in, and only identifiers, paths, refs and short closed-vocabulary codes come out. No content string, or
 * any substring of one longer than an identifier, is ever returned.
 */
import { createHash } from "node:crypto";

export interface RawEvent {
  eventId: string;
  ts: string;
  kind?: string | null;
  type?: string | null;
  actorRole?: string | null;
  toolName?: string | null;
  toolStatus?: string | null;
  subagentId?: string | null;
  userId?: string | null;
  projectId?: string | null;
  filesTouched?: string[] | null;
  content?: string | null;
}

export type GitOp = "commit" | "push" | "checkout" | "switch" | "branch" | "rebase" | "merge" | "revert" | "reset" | "cherry-pick" | "worktree" | "pull" | "fetch" | "stash";
export type GhOp = "pr-create" | "pr-merge" | "pr-close" | "pr-view" | "pr-checkout" | "pr-comment" | "pr-review" | "pr-list" | "issue-create" | "api";

export interface RelayEnvelope { from: string; to: string; messageId: string | null }

export interface EventFacts {
  eventId: string;
  ts: string;
  kind: string | null;
  type: string | null;
  role: string | null;
  tool: string | null;
  toolStatus: string | null;
  subagentId: string | null;
  userId: string | null;
  files: string[];
  /**
   * Paths recovered from commands and their output, by how they were seen: `patch` (apply_patch headers), `add`
   * (git add), `shell` (sed -i, cat >, tee), `status` (git status --short), `stat` (git diff/show --stat).
   * Code-mode harnesses leave `files` empty, so this is most of the edit evidence.
   */
  touched: Array<{ path: string; via: "patch" | "add" | "shell" | "status" | "stat" }>;
  git: GitOp[];
  gh: GhOp[];
  /** Branch names a git command named (checkout -b, switch -c, push origin X, worktree add -b). */
  branches: string[];
  /** `owner/repo#n` from PR URLs, or `#n` from gh pr commands. */
  prRefs: string[];
  /** Commit shas printed by `git commit` / `git rev-parse` style output (7 to 40 hex, in tool results only). */
  shas: string[];
  relay: RelayEnvelope | null;
}

const MAX_LIST = 50;
const SAFE_NAME = /^[A-Za-z0-9][A-Za-z0-9._\/-]{0,199}$/;
const cap = <T>(xs: Iterable<T>) => [...new Set(xs)].slice(0, MAX_LIST);
/** An event id as given when it is identifier-shaped, else a short hash of it, so ids stay distinct but carry no text. */
const eventIdOf = (v: unknown) => {
  const raw = typeof v === "string" ? v : String(v ?? "");
  return idOf(raw) ?? `h:${createHash("sha256").update(raw).digest("hex").slice(0, 24)}`;
};
/** An event time as canonical ISO, or "" when it is not one (such events are dropped by the window check). */
const isoOf = (v: unknown) => (typeof v === "string" && Number.isFinite(Date.parse(v)) ? new Date(v).toISOString() : "");

const GIT = /\bgit\s+(?:-C\s+\S+\s+)?(commit|push|checkout|switch|branch|rebase|merge|revert|reset|cherry-pick|worktree|pull|fetch|stash)\b/g;
const GH = /\bgh\s+(pr|issue|api)\s*(create|merge|close|view|checkout|comment|review|list)?\b/g;
const NEW_BRANCH = /\bgit\s+(?:-C\s+\S+\s+)?(?:checkout\s+-[bB]|switch\s+-[cC]|worktree\s+add\s+(?:\S+\s+)?-[bB])\s+['"]?([A-Za-z0-9._\/-]+)/g;
const PUSH_BRANCH = /\bgit\s+(?:-C\s+\S+\s+)?push\s+(?:-\S+\s+)*(?:origin|upstream)\s+(?:HEAD:)?['"]?([A-Za-z0-9._\/-]+)/g;
const PR_URL = /github\.com\/([A-Za-z0-9._-]+\/[A-Za-z0-9._-]+)\/pull\/(\d{1,7})\b/g;
const GH_PR_NUM = /\bgh\s+pr\s+(?:view|merge|close|checkout|comment|review|diff|edit|ready)\s+#?(\d{1,7})\b/g;
const COMMIT_OUT = /^\[[^\]\s]+\s+(?:\(root-commit\)\s+)?([0-9a-f]{7,40})\]/gm;
/** `[via Agent Relay — from @a (addr) to @b · message 123 · ref …]` as the relay injects it. */
const RELAY = /\[via Agent Relay\s+[—-]\s+from\s+@([A-Za-z0-9._@:-]{1,200})(?:\s+\([^)]{0,300}\))?\s+to\s+@([A-Za-z0-9._@:-]{1,200})(?:\s+·\s+message\s+([0-9]{1,30}))?/;

const PATH_TOKEN = String.raw`([A-Za-z0-9_.~\/@+-]{1,400}\.[A-Za-z0-9]{1,12}|[A-Za-z0-9_.~\/@+-]{1,400}\/[A-Za-z0-9_.@+-]{1,120})`;
const PATCH_HEADER = /^\*\*\* (?:Update|Add|Delete) File: (.+)$/gm;
const SHELL_WRITE = new RegExp(String.raw`(?:\bsed\s+-i(?:\s*''|\s+'')?(?:\s+-e)?\s+(?:'[^']*'|"[^"]*")\s+|\bperl\s+-pi\S*\s+-e\s+(?:'[^']*'|"[^"]*")\s+|\btee\s+(?:-a\s+)?|\bcat\s+>{1,2}\s*)['"]?` + PATH_TOKEN, "g");
const GIT_ADD = /\bgit\s+(?:-C\s+\S+\s+)?add\s+((?:[^;&|\n]+))/g;
const STATUS_LINE = /^ ?[MADRCU?!]{1,2} +(?:\S+ -> )?(\S+)$/gm;
const STAT_LINE = /^ (\S+?)\s+\|\s+(?:\d+|Bin)\b/gm;
/**
 * A recovered path must look like one: path characters only (no whitespace, quotes or punctuation that prose
 * carries), and either a directory separator or a file extension. Anything else is dropped, so free text that
 * lands where a path is expected (a patch header, a redirect target) never leaves the reducer.
 */
const PATH_CHARS = /^[A-Za-z0-9_.~\/@+-]{1,400}$/;
export const looksLikePath = (p: string) =>
  PATH_CHARS.test(p) && !p.startsWith("-") && /[A-Za-z]/.test(p) && !/^[0-9a-f]{7,40}$/.test(p) && !/^\.+$/.test(p)
  && (p.includes("/") || /\.[A-Za-z0-9]{1,12}$/.test(p));
/** Identifiers and closed-vocabulary codes: no whitespace, so no prose. */
const ID = /^[A-Za-z0-9._:@\/-]{1,128}$/;
const idOf = (v: string | null | undefined, max = 128) => (typeof v === "string" && v.length <= max && ID.test(v) ? v : null);

/** Edited paths recovered from commands (tool calls) and from git's own output (tool results). */
function touchedPaths(text: string, isCall: boolean, isResult: boolean): EventFacts["touched"] {
  const out: EventFacts["touched"] = [];
  if (isCall) {
    for (const m of text.matchAll(PATCH_HEADER)) out.push({ path: m[1]!.trim(), via: "patch" });
    for (const m of text.matchAll(SHELL_WRITE)) out.push({ path: m[1]!, via: "shell" });
    for (const m of text.matchAll(GIT_ADD)) for (const tok of m[1]!.trim().split(/\s+/)) {
      const p = tok.replace(/^['"]|['"]$/g, "");
      if (p && p !== "." && p !== "-A" && p !== "--all" && !p.startsWith("-") && looksLikePath(p)) out.push({ path: p, via: "add" });
    }
  }
  if (isResult) {
    for (const m of text.matchAll(STATUS_LINE)) if (looksLikePath(m[1]!)) out.push({ path: m[1]!, via: "status" });
    for (const m of text.matchAll(STAT_LINE)) if (looksLikePath(m[1]!) && !m[1]!.includes("...")) out.push({ path: m[1]!, via: "stat" });
  }
  const seen = new Set<string>();
  return out.filter((t) => looksLikePath(t.path) && !seen.has(t.path) && (seen.add(t.path), true)).slice(0, MAX_LIST);
}

function ghOps(text: string): GhOp[] {
  const out: GhOp[] = [];
  for (const m of text.matchAll(GH)) {
    if (m[1] === "api") out.push("api");
    else if (m[1] === "issue") { if (m[2] === "create") out.push("issue-create"); }
    else if (m[2]) out.push(`pr-${m[2]}` as GhOp);
  }
  return out;
}

export function eventFacts(e: RawEvent): EventFacts {
  const text = typeof e.content === "string" ? e.content : "";
  const isCall = e.kind === "tool_call" || e.type === "tool_call" || e.type === "tool_use";
  const isResult = e.type === "tool_result" || e.actorRole === "tool_result";
  // Commands are read from tool calls only, so an assistant explaining `git revert` is not a revert.
  const commandText = isCall ? text : "";
  const branches = [...commandText.matchAll(NEW_BRANCH), ...commandText.matchAll(PUSH_BRANCH)].map((m) => m[1]!).filter((b) => SAFE_NAME.test(b) && b !== "HEAD");
  const prRefs = [
    ...[...text.matchAll(PR_URL)].map((m) => `${m[1]!.toLowerCase()}#${m[2]}`),
    ...[...commandText.matchAll(GH_PR_NUM)].map((m) => `#${m[1]}`),
  ];
  const relayMatch = e.actorRole === "user" || e.type === "user" || e.kind === "history" ? RELAY.exec(text) : null;
  return {
    eventId: eventIdOf(e.eventId), ts: isoOf(e.ts), kind: idOf(e.kind, 40), type: idOf(e.type, 40), role: idOf(e.actorRole, 40),
    tool: idOf(e.toolName, 80), toolStatus: idOf(e.toolStatus, 40), subagentId: idOf(e.subagentId), userId: idOf(e.userId),
    files: cap((e.filesTouched ?? []).filter((f) => typeof f === "string" && looksLikePath(f))),
    touched: touchedPaths(text, isCall, isResult),
    git: cap([...commandText.matchAll(GIT)].map((m) => m[1] as GitOp)),
    gh: cap(ghOps(commandText)),
    branches: cap(branches), prRefs: cap(prRefs),
    shas: isResult ? cap([...text.matchAll(COMMIT_OUT)].map((m) => m[1]!)) : [],
    relay: relayMatch ? { from: relayMatch[1]!, to: relayMatch[2]!, messageId: relayMatch[3] ?? null } : null,
  };
}

/** True when the facts carry anything a detector can use; empty events are counted, not stored. */
export const informative = (f: EventFacts) =>
  f.files.length > 0 || f.touched.length > 0 || f.git.length > 0 || f.gh.length > 0 || f.branches.length > 0 || f.prRefs.length > 0 || f.shas.length > 0 || f.relay !== null || f.subagentId !== null;

const LIMIT_IDS = 50;
const ids = (v: unknown) => (Array.isArray(v) ? [...new Set(v.map((x) => idOf(typeof x === "string" ? x : null)).filter((x): x is string => !!x))].slice(0, LIMIT_IDS) : []);
const isoOrNull = (v: unknown) => isoOf(v) || null;
const PR_URL_EXACT = /^https:\/\/github\.com\/[A-Za-z0-9._-]{1,100}\/[A-Za-z0-9._-]{1,100}\/pull\/\d{1,7}$/;

/**
 * The directory row the harvest keeps, as a closed schema: every field is named here and validated, and nested
 * objects are rebuilt from validated leaves, never copied. A row without a valid session id and source is dropped.
 * Fields the source does not serve are null or empty.
 */
export interface DirectoryProjection {
  sessionId: string; source: string; lastTs: string; createdAt: string | null; projectId: string | null;
  project: { name: string | null; detail: string | null } | null; parentSessionIds: string[]; userIds: string[];
  machineIds: string[]; eventCount: number | null; pullRequest: { number: number; url: string | null } | null;
  workState: string | null; model: string | null; branch: string | null; via: "cloud" | "reader-mcp";
}

export function directoryProjection(raw: Record<string, unknown>, via: "cloud" | "reader-mcp"): DirectoryProjection | null {
  const sessionId = idOf(raw.sessionId as string), source = idOf(raw.source as string, 40), lastTs = isoOf(raw.lastTs);
  if (!sessionId || !source || !lastTs) return null;
  const p = raw.project && typeof raw.project === "object" ? raw.project as Record<string, unknown> : null;
  const name = idOf(typeof p?.name === "string" ? p.name : null, 200), detail = typeof p?.detail === "string" && looksLikePath(p.detail) ? p.detail : null;
  const pr = raw.pullRequest && typeof raw.pullRequest === "object" ? raw.pullRequest as Record<string, unknown> : null;
  const prNumber = Number(pr?.number);
  const eventCount = Number(raw.eventCount);
  return {
    sessionId, source, lastTs, createdAt: isoOrNull(raw.createdAt), projectId: idOf(raw.projectId as string, 200),
    project: name || detail ? { name, detail } : null,
    parentSessionIds: ids(raw.parentSessionIds), userIds: ids(raw.userIds), machineIds: ids(raw.machineIds),
    eventCount: Number.isSafeInteger(eventCount) && eventCount >= 0 ? eventCount : null,
    pullRequest: Number.isSafeInteger(prNumber) && prNumber > 0 ? { number: prNumber, url: typeof pr?.url === "string" && PR_URL_EXACT.test(pr.url) ? pr.url : null } : null,
    workState: idOf(raw.workState as string, 40), model: idOf(raw.model as string, 80),
    branch: typeof raw.branch === "string" && SAFE_NAME.test(raw.branch) ? raw.branch : null, via,
  };
}
