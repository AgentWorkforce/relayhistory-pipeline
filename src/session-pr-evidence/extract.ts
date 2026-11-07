/**
 * PR evidence for one canonical session: a TypeScript port of relayhistory's
 * `sessions.session_pull_request_evidence_v1` (vendor/relayhistory-cloud/
 * migrations/0065_pr_evidence_custom_plan.sql), run over the rows
 * `projectRelayhistorySession` derives from the canonical records — the same
 * `convergence_events`, `session_catalog` and `session_commit_links` rows the
 * Neon delivery triggers wrote and the Neon function read.
 *
 * The SQL stays the authority: `packages/pipeline/test/pr-evidence-parity.test.ts`
 * delivers the same records through the vendored migrations, calls the real
 * function and requires the same row set. A change to 0065 fails that test
 * until this file follows it.
 *
 * Output is metadata only (URL, owner, repo, number, branch, tool-call id and
 * name, kind, role, two timestamps). No transcript text leaves this module.
 *
 * PostgreSQL ARE notes kept in the port: `.` matches a newline (written here as
 * `[\s\S]`), `$` is end of input only, `[[:alnum:]]` is Unicode-aware (`\p{L}\p{N}`),
 * `substring(x from re)` is case-sensitive while `~*` is not, `->>` renders a
 * non-string JSON value as its jsonb text, and `NULL::text[] || ARRAY[x]` is `{x}`.
 */

/** Bump with any semantic change; stored with each built state. */
export const PR_EVIDENCE_EXTRACTOR_VERSION = "pr-evidence-v1+rhc-0065";

type Json = Record<string, unknown>;

/** A projected row as `projectRelayhistorySession` / `readCanonicalScope` return it. */
export interface PrEvidenceProjectionRow {
  table: string;
  row: Json;
}

/** Exactly the Neon `session_pull_request_evidence_v1` row contract. */
export interface PullRequestEvidenceRow {
  pull_request_url: string | null;
  owner: string;
  repo: string;
  number: number | null;
  head_ref: string | null;
  tool_use_id: string | null;
  tool_name: string | null;
  evidence_kind: string;
  role: "creator" | "contributor";
  /** Canonical UTC timestamp text (`relayhistory-projection.ts` canonicalTimestamp). */
  detected_at: string;
  last_seen_at: string;
}

const isObject = (value: unknown): value is Json =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** jsonb's text form: `{"a": 1, "b": [1, 2]}`, keys by (length, bytes). */
function jsonbText(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) return `[${value.map(jsonbText).join(", ")}]`;
  const enc = new TextEncoder();
  const keys = Object.keys(value as Json).sort((a, b) => {
    const x = enc.encode(a), y = enc.encode(b);
    if (x.length !== y.length) return x.length - y.length;
    for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return x[i]! - y[i]!;
    return 0;
  });
  return `{${keys.map((k) => `${JSON.stringify(k)}: ${jsonbText((value as Json)[k])}`).join(", ")}}`;
}

/** `value->>'key'` on a jsonb value. */
function field(value: unknown, key: string): string | null {
  if (!isObject(value)) return null;
  const v = value[key];
  if (v === undefined || v === null) return null;
  return typeof v === "string" ? v : jsonbText(v);
}

/** `value->'key'` on a jsonb value (undefined = SQL NULL). */
function member(value: unknown, key: string): unknown {
  if (!isObject(value)) return undefined;
  const v = value[key];
  return v === undefined ? undefined : v;
}

/** jsonb_typeof (undefined = SQL NULL). */
function jsonType(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (typeof value === "object") return "object";
  if (typeof value === "string") return "string";
  if (typeof value === "boolean") return "boolean";
  return "number";
}

/** sessions.delivery_catalog_json (0022): objects/arrays as is, a string parsed, else NULL. */
function catalogJson(value: unknown): unknown {
  const t = jsonType(value);
  if (t === "object" || t === "array") return value;
  if (t !== "string") return undefined;
  try {
    // PostgreSQL's jsonb input refuses \u0000; everything else JSON.parse accepts it does.
    if ((value as string).includes("\\u0000")) return undefined;
    return JSON.parse(value as string);
  } catch {
    return undefined;
  }
}

const nonEmpty = (value: string | null) => (value === null || value === "" ? null : value);

/** Order of two canonical timestamps: a longer year is a later one. */
function compareTimes(left: string, right: string): number {
  return left.length - right.length || (left < right ? -1 : left > right ? 1 : 0);
}
const compareText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
/** ORDER BY x ASC: NULLS LAST. */
const ascNullsLast = (a: string | null, b: string | null) =>
  a === b ? 0 : a === null ? 1 : b === null ? -1 : compareText(a, b);

const SHELL_TOOLS = new Set([
  "bash", "shell", "exec", "exec_command", "shell_command", "local_shell",
  "local_shell_call", "container.exec", "unified_exec", "run_shell_command",
  "run_terminal_cmd",
]);
const BOUNDARY = "(?:^|[^\\p{L}\\p{N}_-])";
const END = "(?:$|[^\\p{L}\\p{N}_-])";
const RE = {
  mcpCreate: /(^|[^a-z0-9])create_pull_request$/,
  mcpTool:
    /(^|[^a-z0-9])(update_pull_request|merge_pull_request|create_pull_request_review|create_and_submit_pull_request_review|submit_pending_pull_request_review|add_pull_request_review_comment[a-z_]*|add_issue_comment|update_pull_request_branch|request_copilot_review)$/,
  addIssueComment: /(^|[^a-z0-9])add_issue_comment$/,
  ghCreate: new RegExp(`${BOUNDARY}gh\\s+pr\\s+create${END}`, "iu"),
  ghCommand: new RegExp(
    `${BOUNDARY}gh\\s+pr\\s+(comment|edit|review|merge|checkout|ready|close|reopen|update-branch)${END}`, "iu"),
  gitPush: new RegExp(`${BOUNDARY}git(\\s+-[cC]\\s+\\S+)*\\s+push${END}`, "iu"),
  jsonStart: /^\s*\{/,
  pushRemote: /^\s*To\s+(?:\S*[/@])?github\.com[:/]([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/?\s*$/i,
  pushRef: /^\s*[*+]?\s*(?:\[new branch\]|[0-9a-f]{4,}\.{2,3}[0-9a-f]{4,})\s+\S+\s+->\s+(\S+)/,
  dotGit: /\.git$/i,
  headFlag: /(?:--head|-H)(?:\s+|=)["']?([^\s"';&|]+)/,
  creatingFor: /Creating pull request for (\S+) into /,
  htmlPrUrl: /^https:\/\/github\.com\/[^/]+\/[^/]+\/pull\/[0-9]/i,
  topPrUrl: /^https:\/\/github\.com\/([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,9})(?:[/?#][\s\S]*)?$/i,
  barePrUrlG: /https:\/\/github\.com\/([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,9})(?![0-9])/gi,
  barePrUrl: /https:\/\/github\.com\/([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,9})(?![0-9])/i,
  ghSeg: new RegExp(
    `${BOUNDARY}gh\\s+pr\\s+(?:comment|edit|review|merge|checkout|ready|close|reopen|update-branch)((?:\\s[^;&|\\n]*)?)(?=$|[;&|\\n])`, "u"),
  ghSub: new RegExp(
    `${BOUNDARY}gh\\s+pr\\s+(comment|edit|review|merge|checkout|ready|close|reopen|update-branch)(?:\\s|$|[;&|\\n])`, "u"),
  catalogRepo: /^(?:(?:https?|ssh|git):\/\/)?(?:[^@/\s]+@)?github\.com[:/]([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/?$/i,
  quoted: /"[^"]*"|'[^']*'/g,
  repoFlag: /(?:^|\s)(?:-R|--repo)(?:\s+|=)(?:(?:https:\/\/)?github\.com\/)?([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)/i,
  word: /((?:"[^"]*"|'[^']*'|[^\s"'])+)/g,
  unquote: /"([^"]*)"|'([^']*)'/g,
  isFlag: /^-[\s\S]/,
  argUrl: /^https:\/\/github\.com\/([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,9})(?:[/?#][^\s]*)?$/i,
  argNumber: /^#?[0-9]{1,9}$/,
  allDigits: /^#?[0-9]+$/,
  http: /^https?:/i,
  branchChars: /^[A-Za-z0-9._/:-]+$/,
  commentUrlG: /https:\/\/github\.com\/([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)\/pull\/([0-9]{1,9})#issuecomment-/gi,
  number9: /^[0-9]{1,9}$/,
  commitPr: /^([A-Za-z0-9-]+)\/([A-Za-z0-9_.-]+)#([0-9]{1,9})$/,
  headOwnerPrefix: /^[A-Za-z0-9-]+:/,
  headOwner: /^([A-Za-z0-9-]+):/,
  refsHeads: /^refs\/heads\//,
  owner: /^[A-Za-z0-9-]+$/,
  repo: /^[A-Za-z0-9_.-]+$/,
};

/** `gh pr <sub> --help` value flags (0065 gh_parsed). */
const VALUE_FLAGS: Record<string, readonly string[]> = {
  comment: ["-b", "--body", "-F", "--body-file", "--attach"],
  edit: ["-b", "--body", "-F", "--body-file", "-t", "--title", "-B", "--base", "-m", "--milestone",
    "--add-assignee", "--remove-assignee", "--add-label", "--remove-label", "--add-project",
    "--remove-project", "--add-reviewer", "--remove-reviewer", "--attach"],
  review: ["-b", "--body", "-F", "--body-file"],
  merge: ["-b", "--body", "-F", "--body-file", "-t", "--subject", "-A", "--author-email", "--match-head-commit"],
  checkout: ["-b", "--branch", "--worktree"],
  close: ["-c", "--comment"],
  reopen: ["-c", "--comment"],
};

type Kind = "mcp_create_pull_request" | "mcp_pull_request_tool" | "gh_pr_create" | "gh_pr_command" | "git_push";
interface Res {
  callId: string; callName: string | null; lname: string | null; args: unknown; cmd: string;
  kind: Kind; resultId: string; content: string; ts: string; out: string;
}
interface Raw {
  owner: string | null; repo: string | null; number: number | null; headRef: string | null;
  callId: string | null; callName: string | null; kind: string; ts: string;
}

const match1 = (re: RegExp, s: string | null) => (s === null ? null : re.exec(s));
const substring = (s: string | null, re: RegExp) => (s === null ? null : (re.exec(s)?.[1] ?? null));
const allMatches = (re: RegExp, s: string) => [...s.matchAll(re)];

/** One call's text command (0065 `shaped.cmd`). */
function commandOf(args: unknown, target: string | null): string {
  const command = member(args, "command");
  const t = jsonType(command);
  if (t === "string") return command as string;
  if (t === "array") {
    const parts = (command as unknown[]).filter((p) => p !== null).map((p) => (typeof p === "string" ? p : jsonbText(p)));
    if (parts.length) return parts.join(" ");
  }
  const cmd = member(args, "cmd");
  if (jsonType(cmd) === "string") return cmd as string;
  return target ?? "";
}

/** The kinds one call is (0065 `kinds`; a call can be several). */
function kindsOf(lname: string | null, isShell: boolean, cmd: string): Kind[] {
  const kinds: Kind[] = [];
  if (lname !== null && RE.mcpCreate.test(lname)) kinds.push("mcp_create_pull_request");
  if (lname !== null && RE.mcpTool.test(lname)) kinds.push("mcp_pull_request_tool");
  if (isShell && RE.ghCreate.test(cmd)) kinds.push("gh_pr_create");
  if (isShell && RE.ghCommand.test(cmd)) kinds.push("gh_pr_command");
  if (isShell && RE.gitPush.test(cmd)) kinds.push("git_push");
  return kinds;
}

/** 0065 gh_parsed: the first positional word of the `gh pr <sub>` segment. */
function positionalOf(seg: string | null, sub: string | null): string | null {
  if (seg === null) return null;
  const valueFlags = new Set(["-R", "--repo", ...(sub !== null ? (VALUE_FLAGS[sub] ?? []) : [])]);
  let previousTakesValue = false;
  for (const m of allMatches(RE.word, seg)) {
    const tok = m[1]!.replace(RE.unquote, (_all, a: string | undefined, b: string | undefined) => (a ?? "") + (b ?? ""));
    const isFlag = RE.isFlag.test(tok);
    const takesValue = isFlag && !tok.includes("=") && valueFlags.has(tok);
    const isFlagValue = previousTakesValue;
    previousTakesValue = takesValue;
    if (!isFlag && !isFlagValue) return tok;
  }
  return null;
}

/**
 * The Neon function's rows for one session, from its projected rows. All rows
 * must belong to that one (org, workspace, source, session) scope.
 */
export function extractPullRequestEvidence(rows: readonly PrEvidenceProjectionRow[]): PullRequestEvidenceRow[] {
  const events = rows.filter((r) => r.table === "convergence_events").map((r) => r.row);
  const payload = (e: Json) => member(e.record, "payload");

  // calls / shaped / kinds
  interface Call { callId: string; callName: string | null; lname: string | null; args: unknown; cmd: string; kinds: Kind[] }
  const calls: Call[] = [];
  for (const c of events) {
    if (c.kind !== "tool_call" || c.delivery_record_id === null || c.delivery_record_id === undefined) continue;
    const p = payload(c);
    const callId = nonEmpty(field(p, "tool_use_id"));
    if (callId === null) continue;
    const isError = field(p, "is_error");
    if (isError === "1" || isError === "true") continue;
    if (c.tool_status === "error") continue;
    const callName = typeof c.tool_name === "string" ? c.tool_name : null;
    const lname = callName === null ? null : callName.toLowerCase();
    const first = Array.isArray(c.tool_calls) ? c.tool_calls[0] : undefined;
    let args = member(first, "args");
    if (args === undefined) args = catalogJson(member(p, "args_json"));
    if (args === undefined) args = {};
    const cmd = commandOf(args, field(p, "target"));
    const kinds = kindsOf(lname, lname !== null && SHELL_TOOLS.has(lname), cmd);
    if (kinds.length) calls.push({ callId, callName, lname, args, cmd, kinds });
  }
  if (!calls.length && !rows.some((r) => r.table === "session_commit_links")) return [];

  // res: each call kind joined with each of its successful tool results.
  const results = new Map<string, Json[]>();
  for (const r of events) {
    if (r.kind !== "session_event" || r.delivery_record_id === null || r.delivery_record_id === undefined) continue;
    const p = payload(r);
    if (!(r.type === "tool_result" || r.actor_role === "tool_result" || field(p, "kind") === "tool_result")) continue;
    const status = field(p, "result_status") ?? "";
    if (status === "errored" || status === "cancelled") continue;
    if ((nonEmpty(field(p, "error_signal")) ?? "none") !== "none") continue;
    if (typeof r.content !== "string") continue;
    const id = field(p, "tool_use_id");
    if (id === null) continue;
    results.set(id, [...(results.get(id) ?? []), r]);
  }
  const res: Res[] = [];
  for (const call of calls)
    for (const kind of call.kinds)
      for (const r of results.get(call.callId) ?? []) {
        const content = r.content as string;
        let out = content;
        if (RE.jsonStart.test(content)) {
          const output = field(catalogJson(content), "output");
          if (output !== null) out = output;
        }
        res.push({ ...call, kind, resultId: String(r.event_id), content, ts: String(r.ts), out });
      }

  // pushes
  const pushKey = new Set<string>();
  const pushes: Raw[] = [];
  for (const r of res) {
    if (r.kind !== "git_push") continue;
    const lines = r.out.split(/\r?\n/);
    const remotes: { n: number; owner: string; repo: string }[] = [];
    lines.forEach((text, i) => {
      const m = RE.pushRemote.exec(text);
      if (m) remotes.push({ n: i + 1, owner: m[1]!, repo: m[2]!.replace(RE.dotGit, "") });
    });
    lines.forEach((text, i) => {
      const m = RE.pushRef.exec(text);
      if (!m) return;
      const n = i + 1;
      const remote = remotes.filter((x) => x.n < n).sort((a, b) => b.n - a.n)[0];
      if (!remote) return;
      const headRef = m[1]!.replace(RE.refsHeads, "");
      const key = JSON.stringify([remote.owner, remote.repo, headRef, r.callId, r.callName, r.ts]);
      if (pushKey.has(key)) return;
      pushKey.add(key);
      pushes.push({ owner: remote.owner, repo: remote.repo, number: null, headRef, callId: r.callId, callName: r.callName, kind: "git_push", ts: r.ts });
    });
  }

  // creators
  const creators: Raw[] = [];
  for (const r of res) {
    if (r.kind !== "gh_pr_create" && r.kind !== "mcp_create_pull_request") continue;
    const j = catalogJson(r.content);
    const found: string[][] = [];
    if (r.kind === "mcp_create_pull_request" && jsonType(j) === "object") {
      const html = field(j, "html_url");
      const url = html !== null && RE.htmlPrUrl.test(html) ? html : field(j, "url");
      const top = match1(RE.topPrUrl, url);
      if (top) found.push([top[1]!, top[2]!, top[3]!]);
    }
    if (r.kind === "gh_pr_create" || jsonType(j) !== "object")
      for (const m of allMatches(RE.barePrUrlG, r.out)) found.push([m[1]!, m[2]!, m[3]!]);
    const head =
      (r.kind === "gh_pr_create" ? substring(r.cmd, RE.headFlag) : null) ??
      (r.kind === "mcp_create_pull_request" && jsonType(member(r.args, "head")) === "string" ? (member(r.args, "head") as string) : null) ??
      substring(r.out, RE.creatingFor);
    for (const [owner, repo, number] of found)
      creators.push({ owner: owner!, repo: repo!, number: Number(number), headRef: head, callId: r.callId, callName: r.callName, kind: r.kind, ts: r.ts });
  }

  // gh_cmds
  const catalog = rows.find((r) => r.table === "session_catalog")?.row;
  const catalogRepo = match1(RE.catalogRepo, typeof catalog?.repo_url === "string" ? catalog.repo_url : null);
  const pushRepos = new Set(pushes.map((p) => `${p.owner!.toLowerCase()}/${p.repo!.toLowerCase()}`));
  const minText = (values: string[]) => values.reduce((a, b) => (compareText(b, a) < 0 ? b : a));
  const pushRepo = pushRepos.size === 1 ? [minText(pushes.map((p) => p.owner!)), minText(pushes.map((p) => p.repo!))] : null;
  const ghCmds: Raw[] = [];
  for (const r of res) {
    if (r.kind !== "gh_pr_command") continue;
    const seg = substring(r.cmd, RE.ghSeg);
    const subRaw = substring(r.cmd, RE.ghSub);
    const sub = subRaw === null ? null : subRaw.toLowerCase();
    const repoFlagM = seg === null ? null : RE.repoFlag.exec(seg.replace(RE.quoted, '""'));
    const repoFlag = repoFlagM ? [repoFlagM[1]!, repoFlagM[2]!] : null;
    const positional = positionalOf(seg, sub);
    const outUrl = RE.barePrUrl.exec(r.out);
    const argUrl = match1(RE.argUrl, positional);
    const argNumber = positional !== null && RE.argNumber.test(positional) ? Number(positional.replace(/^#+/, "")) : null;
    const argBranch =
      positional !== null && !RE.allDigits.test(positional) && !RE.http.test(positional) && RE.branchChars.test(positional)
        ? positional : null;
    const catalogPair = catalogRepo ? [catalogRepo[1]!, catalogRepo[2]!] : null;
    let target: (string | null)[] | null;
    if (argUrl) target = [argUrl[1]!, argUrl[2]!, argUrl[3]!];
    else if (argNumber !== null)
      target = [...(repoFlag ?? catalogPair ?? (outUrl && Number(outUrl[3]) === argNumber ? [outUrl[1]!, outUrl[2]!] : null) ?? pushRepo ?? []), String(argNumber)];
    else if (argBranch !== null) target = [...(repoFlag ?? catalogPair ?? pushRepo ?? []), null];
    else target = outUrl ? [outUrl[1]!, outUrl[2]!, outUrl[3]!] : null;
    if (target === null) continue;
    const repo = target[1] ?? null;
    ghCmds.push({
      owner: target[0] ?? null, repo: repo === null ? null : repo.replace(RE.dotGit, ""),
      number: target[2] === undefined || target[2] === null ? null : Number(target[2]),
      headRef: argBranch, callId: r.callId, callName: r.callName, kind: r.kind, ts: r.ts,
    });
  }

  // mcp_tools
  const mcpTools: Raw[] = [];
  for (const r of res) {
    if (r.kind !== "mcp_pull_request_tool") continue;
    if (r.lname !== null && RE.addIssueComment.test(r.lname)) {
      const j = catalogJson(r.content);
      const haystack = jsonType(j) === "object" ? (field(j, "html_url") ?? "") : r.content;
      const owner = field(r.args, "owner"), repo = field(r.args, "repo");
      const issue = field(r.args, "issue_number") ?? field(r.args, "issueNumber");
      const ok = allMatches(RE.commentUrlG, haystack).some((m) =>
        owner !== null && repo !== null && issue !== null &&
        m[1]!.toLowerCase() === owner.toLowerCase() && m[2]!.toLowerCase() === repo.toLowerCase() && m[3] === issue);
      if (!ok) continue;
    }
    const n = ["pull_number", "pullNumber", "issue_number", "issueNumber"]
      .map((k) => field(r.args, k)).find((v) => v !== null && RE.number9.test(v));
    mcpTools.push({
      owner: field(r.args, "owner"), repo: field(r.args, "repo"), number: n === undefined ? null : Number(n),
      headRef: null, callId: r.callId, callName: r.callName, kind: r.kind, ts: r.ts,
    });
  }

  // links
  const links: Raw[] = [];
  for (const { table, row: l } of rows) {
    if (table !== "session_commit_links") continue;
    const evidence = l.evidence;
    if (jsonType(evidence) !== "object") continue;
    const pr = member(evidence, "github_pr");
    if ((field(pr, "system") ?? "github") !== "github") continue;
    const m = match1(RE.commitPr, field(pr, "id"));
    if (!m) continue;
    const ts = (typeof l.linked_at === "string" ? l.linked_at : null) ?? String(l.received_at);
    links.push({ owner: m[1]!, repo: m[2]!, number: Number(m[3]), headRef: null, callId: null, callName: null, kind: "commit_link", ts });
  }

  // evidence / valid
  interface Valid extends Raw { owner: string; repo: string; headOwner: string; ownerKey: string; repoKey: string }
  const valid: Valid[] = [];
  for (const e of [...creators, ...pushes, ...ghCmds, ...mcpTools, ...links]) {
    const headRef = e.headRef === null ? null : e.headRef.replace(RE.headOwnerPrefix, "").replace(RE.refsHeads, "");
    const headOwner = substring(e.headRef, RE.headOwner) ?? e.owner;
    if (e.owner === null || e.repo === null || !RE.owner.test(e.owner) || !RE.repo.test(e.repo)) continue;
    if (e.number === null && nonEmpty(headRef) === null) continue;
    valid.push({ ...e, headRef, owner: e.owner, repo: e.repo, headOwner: headOwner!, ownerKey: e.owner.toLowerCase(), repoKey: e.repo.toLowerCase() });
  }

  // ranked: branch-only rows join a numbered PR on the same branch by timeline.
  const strength = (kind: string) =>
    kind === "gh_pr_create" || kind === "mcp_create_pull_request" ? 1
      : kind === "commit_link" ? 2 : kind === "mcp_pull_request_tool" ? 3 : kind === "gh_pr_command" ? 4 : 5;
  const sameBranch = (n: Valid, v: Valid) =>
    n.number !== null && n.headRef === v.headRef && n.ownerKey === v.ownerKey && n.repoKey === v.repoKey &&
    n.headOwner.toLowerCase() === v.headOwner.toLowerCase();
  interface Ranked extends Valid { prNumber: number | null; strength: number; targetKey: string }
  const ranked: Ranked[] = valid.map((v) => {
    let prNumber = v.number;
    if (prNumber === null) {
      // ORDER BY ts DESC, call_id DESC (NULLS FIRST) LIMIT 1
      const before = valid.filter((n) => sameBranch(n, v) && compareTimes(n.ts, v.ts) <= 0)
        .sort((a, b) => compareTimes(b.ts, a.ts) || (a.callId === b.callId ? 0 : a.callId === null ? -1 : b.callId === null ? 1 : compareText(b.callId, a.callId)))[0];
      // ORDER BY ts, call_id (NULLS LAST) LIMIT 1
      const after = valid.filter((n) => sameBranch(n, v) && compareTimes(n.ts, v.ts) > 0)
        .sort((a, b) => compareTimes(a.ts, b.ts) || ascNullsLast(a.callId, b.callId))[0];
      prNumber = !before ? (after?.number ?? null)
        : !after || after.number === before.number ? before.number : null;
    }
    return { ...v, prNumber, strength: strength(v.kind), targetKey: prNumber !== null ? String(prNumber) : `ref:${v.headRef}` };
  });

  // grouped / DISTINCT ON with window aggregates
  const groups = new Map<string, Ranked[]>();
  for (const r of ranked) {
    const key = JSON.stringify([r.ownerKey, r.repoKey, r.targetKey]);
    groups.set(key, [...(groups.get(key) ?? []), r]);
  }
  const out: PullRequestEvidenceRow[] = [];
  for (const group of groups.values()) {
    const ordered = [...group].sort((a, b) => a.strength - b.strength || compareTimes(a.ts, b.ts) || ascNullsLast(a.callId, b.callId));
    const first = ordered[0]!;
    const heads = group.map((g) => g.headRef).filter((h): h is string => h !== null);
    const maxHead = heads.length ? heads.reduce((a, b) => (compareText(b, a) > 0 ? b : a)) : null;
    const times = group.map((g) => g.ts).sort(compareTimes);
    out.push({
      pull_request_url: first.prNumber !== null ? `https://github.com/${first.owner}/${first.repo}/pull/${first.prNumber}` : null,
      owner: first.owner, repo: first.repo, number: first.prNumber,
      head_ref: first.headRef ?? maxHead,
      tool_use_id: first.callId, tool_name: first.callName, evidence_kind: first.kind,
      role: group.some((g) => g.strength === 1) ? "creator" : "contributor",
      detected_at: times[0]!, last_seen_at: times[times.length - 1]!,
    });
  }
  return out.sort((a, b) =>
    compareText(a.owner.toLowerCase(), b.owner.toLowerCase()) || compareText(a.repo.toLowerCase(), b.repo.toLowerCase()) ||
    compareText(a.number !== null ? String(a.number) : `ref:${a.head_ref}`, b.number !== null ? String(b.number) : `ref:${b.head_ref}`));
}
