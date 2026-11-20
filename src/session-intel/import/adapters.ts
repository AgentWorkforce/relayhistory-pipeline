/**
 * The trace adapters (docs/session-intel-import.md, "Formats"). Each one reads one JSONL file and feeds the
 * session builders with ids, timestamps, paths, shas and token counts. Message text, tool inputs and tool
 * outputs are read only to find a file path or a commit sha, and are never kept.
 */
import { basename } from "node:path";
import { relayhistoryActivityEvent } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { commitShaFromOutput, gitOps } from "./git.ts";
import { codeModeShells, codexCommitSha } from "./code-mode.ts";
import { readJsonl } from "./io.ts";
import { cleanId, IMPORT_BOUNDS, ImportError, SessionBuilder, strictTs, type ImportHarness, type ImportStats, type LineRef } from "./normalize.ts";
import { isRecord } from "../replay/util.ts";

export type ImportFormat = "claude-code" | "codex" | "generic";
export const FORMATS: readonly ImportFormat[] = ["claude-code", "codex", "generic"];

/** Every session of one import, keyed by harness and the source session id. */
export class SessionSet {
  readonly byKey = new Map<string, SessionBuilder>();
  /** `notAfter`: a record stamped after it (the import's clock plus a day of skew) is an outlier, dropped and counted. */
  constructor(readonly stats: ImportStats, readonly notAfter: string | null = null) {}
  /** The record's timestamp if it is usable: null (counted) when it is missing, not strict ISO, or in the future. */
  accept(ts: string | null): string | null {
    if (!ts) { this.stats.untimedRecords++; return null; }
    if (this.notAfter && ts > this.notAfter) { this.stats.futureTimestamps++; return null; }
    return ts;
  }
  get(harness: ImportHarness, id: string): SessionBuilder {
    const k = `${harness}\u0000${id}`;
    let b = this.byKey.get(k);
    if (!b) {
      if (this.byKey.size >= IMPORT_BOUNDS.sessions) throw new ImportError(`more than ${IMPORT_BOUNDS.sessions} sessions`);
      b = new SessionBuilder(harness, id, this.stats);
      this.byKey.set(k, b);
    }
    return b;
  }
  add(b: SessionBuilder) {
    const k = `${b.harness}\u0000${b.sourceId}`;
    const cur = this.byKey.get(k);
    if (cur) cur.absorb(b);
    else {
      if (this.byKey.size >= IMPORT_BOUNDS.sessions) throw new ImportError(`more than ${IMPORT_BOUNDS.sessions} sessions`);
      this.byKey.set(k, b);
    }
  }
}

const str = (v: unknown): string | null => typeof v === "string" ? v : null;
/** Text of a tool result (a string, or an array of `{type: "text", text}` blocks), only to find a sha. */
function resultText(v: unknown): string {
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map((b) => isRecord(b) && typeof b.text === "string" ? b.text : "").join("\n");
  return "";
}
const stem = (file: string) => basename(file).replace(/\.jsonl$/u, "");

// ---------------------------------------------------------------- Claude Code

/** A Map, not an object: a tool named `constructor` or `__proto__` must not resolve to a prototype member. */
const CLAUDE_PATH_TOOLS = new Map<string, readonly string[]>([
  ["Edit", ["file_path"]], ["Write", ["file_path"]], ["MultiEdit", ["file_path"]], ["NotebookEdit", ["notebook_path", "file_path"]],
]);

/**
 * A Claude Code transcript (`~/.claude/projects/<dir>/<sessionId>.jsonl`, and subagent transcripts under
 * `<sessionId>/subagents/agent-<id>.jsonl`). One record per line: `{type, uuid, parentUuid, sessionId,
 * timestamp, cwd, gitBranch, isSidechain, agentId?, message: {id, role, content, usage}, toolUseResult?}`.
 */
export async function readClaudeCode(file: string, fileIndex: number, set: SessionSet): Promise<void> {
  const fallbackId = stem(file);
  const agentFromName = /^agent-([A-Za-z0-9_-]{1,128})$/u.exec(fallbackId)?.[1] ?? null;
  const seenTools = new Map<SessionBuilder, Set<string>>();
  const seenUsage = new Map<SessionBuilder, Set<string>>();
  const pending = new Map<string, { b: SessionBuilder; commit: { sha: string | null; ts: string } }>();
  const once = (m: Map<SessionBuilder, Set<string>>, b: SessionBuilder, id: string) => {
    let s = m.get(b);
    if (!s) m.set(b, s = new Set());
    if (s.has(id)) return false;
    s.add(id);
    return true;
  };
  await readJsonl(file, set.stats, (r, line) => {
    if (!isRecord(r)) { set.stats.malformedLines++; return; }
    const ts = set.accept(strictTs(r.timestamp));
    if (!ts) return;
    const at: LineRef = { file: fileIndex, line };
    const sid = cleanId(r.sessionId) ?? fallbackId;
    // A subagent (sidechain) transcript carries its parent's sessionId and its own agentId.
    const agent = r.isSidechain === true ? cleanId(r.agentId, 128) ?? agentFromName : null;
    const b = set.get("claude", agent ? `${sid}#agent-${agent}` : sid);
    if (agent) b.parent(sid);
    b.touch(ts, at);
    const cwd = cleanId(r.cwd, IMPORT_BOUNDS.path);
    if (cwd) b.cwd ??= cwd;
    const branch = cleanId(r.gitBranch);
    if (branch) b.branch = branch;
    const msg = isRecord(r.message) ? r.message : null;
    if (!msg) return;
    if (r.type === "assistant" && isRecord(msg.usage)) {
      const id = cleanId(msg.id) ?? cleanId(r.uuid) ?? `line-${line}`;
      // Claude Code writes one record per content block, each repeating the message's usage.
      if (once(seenUsage, b, id)) {
        const u = msg.usage;
        const n = ["input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"]
          .reduce((sum, k) => sum + (typeof u[k] === "number" && Number.isSafeInteger(u[k]) && (u[k] as number) > 0 ? u[k] as number : 0), 0);
        b.tokens(n, ts, at);
      }
    }
    if (!Array.isArray(msg.content)) return;
    for (const block of msg.content) {
      if (!isRecord(block)) continue;
      if (block.type === "tool_use") {
        const toolId = cleanId(block.id) ?? `line-${line}`;
        const name = str(block.name) ?? "";
        const input = isRecord(block.input) ? block.input : {};
        const keys = CLAUDE_PATH_TOOLS.get(name);
        // Only the tools acted on are remembered for dedupe, so the set stays as small as the facts.
        if ((keys || name === "Bash") && !once(seenTools, b, toolId)) continue;
        if (keys) {
          const key = keys.find((k) => typeof input[k] === "string");
          b.write(key ? input[key] : null, ts, at, cwd ?? b.cwd, branch ?? b.branch);
        } else if (name === "Bash" && typeof input.command === "string") {
          const ops = gitOps(input.command);
          if (ops.commit) pending.set(toolId, { b, commit: b.commit(null, ts, at) });
          for (const sha of ops.reverts) b.outcome(sha, true, ts, at);
        }
      } else if (block.type === "tool_result") {
        const p = pending.get(str(block.tool_use_id) ?? "");
        if (!p) continue;
        pending.delete(str(block.tool_use_id)!);
        const tur = isRecord(r.toolUseResult) ? r.toolUseResult : null;
        p.commit.sha = commitShaFromOutput(resultText(block.content)) ?? (tur ? commitShaFromOutput(str(tur.stdout) ?? "") : null);
      }
    }
  });
}

// ---------------------------------------------------------------- Codex

const SHELL_TOOLS = new Set(["shell", "container.exec", "shell_command", "exec_command", "local_shell"]);
const PATCH_HEADER = /^\*\*\* (?:Add File|Update File|Delete File|Move to): (.+)$/gmu;

function patchPaths(patch: string): string[] {
  if (typeof patch !== "string" || !patch.includes("*** Begin Patch")) return [];
  return [...patch.matchAll(PATCH_HEADER)].map((m) => m[1]!.trim());
}

function parseArgs(v: unknown): Record<string, unknown> {
  if (isRecord(v)) return v;
  if (typeof v !== "string" || v.length > IMPORT_BOUNDS.inputLineBytes) return {};
  try { const p = JSON.parse(v) as unknown; return isRecord(p) ? p : {}; } catch { return {}; }
}

/** The script of a shell call: `["bash", "-lc", script]` gives the script, any other array is joined. */
function shellScript(command: unknown): { script: string; patch: string | null } {
  if (typeof command === "string") return { script: command, patch: null };
  if (!Array.isArray(command) || !command.every((c) => typeof c === "string")) return { script: "", patch: null };
  const c = command as string[];
  if (c[0] === "apply_patch" || c[0] === "applypatch") return { script: "", patch: c[1] ?? null };
  if (c.length === 3 && /(?:^|\/)(?:ba|z|da)?sh$/u.test(c[0]!) && /^-l?c$/u.test(c[1]!)) return { script: c[2]!, patch: null };
  return { script: c.join(" "), patch: null };
}

/**
 * A Codex rollout (`~/.codex/sessions/YYYY/MM/DD/rollout-<time>-<uuid>.jsonl`). One record per line:
 * `{timestamp, type: session_meta|turn_context|response_item|event_msg, payload}`. Older rollouts put the
 * payload at the top level without a timestamp; those lines take the last timestamp seen.
 */
export async function readCodex(file: string, fileIndex: number, set: SessionSet): Promise<void> {
  const b = new SessionBuilder("codex", "", set.stats);
  let id: string | null = null;
  let cwd: string | null = null;
  let lastTs: string | null = null;
  let lastTotal: number | null = null;
  let childPath: string | null = null;
  let waitingForChildTask = false;
  let childThreadConfirmed = false;
  let metadataSeen = false;
  const pending = new Map<string, { sha: string | null; ts: string; at: LineRef }>();
  await readJsonl(file, set.stats, (r, line) => {
    if (!isRecord(r)) { set.stats.malformedLines++; return; }
    const at: LineRef = { file: fileIndex, line };
    const enveloped = isRecord(r.payload);
    const type = enveloped ? str(r.type) : r.record_type === "state" ? null : "response_item";
    const p = enveloped ? r.payload as Record<string, unknown> : r;
    const ts = set.accept(strictTs(r.timestamp) ?? (enveloped ? null : lastTs));
    if (!ts) return;
    lastTs = ts;
    // Modern child rollouts start with their own metadata, then copied parent
    // records retimestamped to the child's start. A parent task in that prefix
    // is addressed to a different agent_path; neither timestamps nor the first
    // session_meta can distinguish authorship. Do not read the encrypted body.
    if (waitingForChildTask) {
      if (type === "event_msg" && p.type === "thread_settings_applied" && p.thread_id === id)
        childThreadConfirmed = true;
      const ownTask = childThreadConfirmed && childPath !== null && type === "response_item" && p.type === "agent_message"
        && p.recipient === childPath && Array.isArray(p.content)
        && p.content.some((c) => isRecord(c) && c.type === "input_text" && typeof c.text === "string"
          && /^Message Type: NEW_TASK(?:\r?\n|$)/u.test(c.text));
      if (!ownTask) { set.stats.codexPrefixRecordsDropped++; return; }
      waitingForChildTask = false;
    }
    b.touch(ts, at);
    if (type === "session_meta" || (!enveloped && typeof r.id === "string" && r.type === undefined)) {
      id ??= cleanId(p.id);
      cwd = cleanId(p.cwd, IMPORT_BOUNDS.path) ?? cwd;
      b.cwd ??= cwd;
      const git = isRecord(p.git) ? p.git : {};
      b.remote ??= cleanId(git.repository_url, IMPORT_BOUNDS.path);
      b.branch = cleanId(git.branch) ?? b.branch;
      // A spawned subagent thread names its parent (confirmed in the audited format; see the doc).
      const src = isRecord(p.source) && isRecord(p.source.subagent) && isRecord(p.source.subagent.thread_spawn) ? p.source.subagent.thread_spawn : null;
      if (src) b.parent(src.parent_thread_id);
      if (!metadataSeen && src) {
        const parent = cleanId(src.parent_thread_id);
        childPath = cleanId(p.agent_path);
        const inherited = Number.isSafeInteger(p.subagent_history_start_ordinal) && (p.subagent_history_start_ordinal as number) > 0;
        // Legacy children have neither field; retain their existing behavior.
        // An explicit copied prefix without a usable path stays fail-closed.
        waitingForChildTask = parent !== null && parent !== id && (childPath !== null || inherited);
      }
      metadataSeen = true;
      return;
    }
    if (type === "turn_context") { cwd = cleanId(p.cwd, IMPORT_BOUNDS.path) ?? cwd; b.cwd ??= cwd; return; }
    if (type === "event_msg") {
      if (p.type !== "token_count" || !isRecord(p.info)) return;
      const last = isRecord(p.info.last_token_usage) ? p.info.last_token_usage : null;
      const total = isRecord(p.info.total_token_usage) && typeof p.info.total_token_usage.total_tokens === "number" ? p.info.total_token_usage.total_tokens : null;
      if (last) {
        const n = typeof last.total_tokens === "number" ? last.total_tokens
          : (typeof last.input_tokens === "number" ? last.input_tokens : 0) + (typeof last.output_tokens === "number" ? last.output_tokens : 0);
        b.tokens(n, ts, at);
      } else if (total !== null) b.tokens(total - (lastTotal ?? 0), ts, at);
      if (total !== null) lastTotal = total;
      return;
    }
    if (type !== "response_item") return;
    const kind = str(p.type);
    const callId = str(p.call_id) ?? str(p.id) ?? `line-${line}`;
    const write = (paths: string[], workdir: string | null) => { for (const path of paths) b.write(path, ts, at, workdir ?? cwd); };
    const shell = (script: string, patch: string | null, workdir: string | null) => {
      write(patchPaths(patch ?? ""), workdir);
      // A patch applied through a heredoc in the script (`apply_patch <<'EOF' ...`).
      write(patchPaths(script), workdir);
      const ops = gitOps(script);
      if (ops.commit) { const c = b.commit(null, ts, at); pending.set(callId, c); }
      for (const sha of ops.reverts) b.outcome(sha, true, ts, at);
    };
    if (kind === "function_call" || kind === "custom_tool_call") {
      const name = str(p.name) ?? "";
      if (name === "exec") {
        // Code mode captures the outer exec, not its nested tool calls. Use
        // the production projection's bounded static-patch parser unchanged.
        const input = kind === "custom_tool_call" ? str(p.input) : str(parseArgs(p.arguments).input);
        if (input) {
          const flag = p.is_error;
          // Preserve the projection's primitive spellings (the longest is
          // "false") without asking it to serialize arbitrary input objects.
          const isError = typeof flag === "boolean"
            || (typeof flag === "number" && Number.isFinite(flag))
            || (typeof flag === "string" && flag.length <= 5) ? flag : null;
          const event = relayhistoryActivityEvent({
            kind: "tool_call", origin_id: "import", record_id: callId,
            revision: "1", operation: "upsert", received_at: ts,
            payload: { name: "exec", args_json: JSON.stringify({ input }), is_error: isError },
          }, { workspaceId: "import", source: "codex", sessionId: "import", machineId: "import", userId: "import" });
          if (Array.isArray(event?.files_touched))
            write(event.files_touched.filter((path): path is string => typeof path === "string"), cwd);
          const failed = Array.isArray(event?.tool_calls) && event.tool_calls.some((call) => isRecord(call) && call.isError === true);
          if (!failed) {
            const nested = codeModeShells(input);
            if (nested.incomplete) set.stats.codexCodeModeShellUnparsed++;
            const commits = nested.commands.filter((command) => gitOps(shellScript(command).script).commit);
            for (const _ of commits) {
              const c = b.commit(null, ts, at);
              // One outer output cannot safely name several nested commit attempts.
              if (commits.length === 1 && !nested.incomplete) pending.set(callId, c);
            }
          }
        }
      } else if (name === "apply_patch") {
        const input = kind === "custom_tool_call" ? str(p.input) : (str(parseArgs(p.arguments).input) ?? str(p.arguments));
        write(patchPaths(input ?? ""), cwd);
      } else if (SHELL_TOOLS.has(name)) {
        const args = parseArgs(kind === "custom_tool_call" ? p.input : p.arguments);
        const { script, patch } = shellScript(args.command ?? args.cmd);
        shell(script, patch, cleanId(args.workdir, IMPORT_BOUNDS.path));
      }
    } else if (kind === "local_shell_call" && isRecord(p.action)) {
      const { script, patch } = shellScript(p.action.command);
      shell(script, patch, cleanId(p.action.working_directory, IMPORT_BOUNDS.path));
    } else if (kind === "function_call_output" || kind === "custom_tool_call_output" || kind === "local_shell_call_output") {
      const c = pending.get(callId);
      if (!c) return;
      pending.delete(callId);
      c.sha = codexCommitSha(p.output);
      // The SHA is not available at call time: do not leak later output into an earlier replay view.
      if (c.sha) { if (ts > c.ts) c.ts = ts; c.at = at; }
    }
  });
  if (!b.first) return;
  if (waitingForChildTask) set.stats.codexChildrenWithoutTaskBoundary++;
  b.sourceId = id ?? /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/iu.exec(stem(file))?.[1] ?? stem(file);
  // The id is known only now: a session naming itself as its parent is not a fleet.
  b.parents.delete(b.sourceId);
  set.add(b);
}

// ---------------------------------------------------------------- Generic

const GENERIC_KIND = /^[A-Za-z0-9_.-]{1,64}$/u;

/**
 * The generic format customers can export to: one event per line, `{sessionId, ts, kind, files?, commit?,
 * outcome?, tokens?, parentSessionId?, projectId?, repo?, branch?, cwd?}`. A line that breaks the format is
 * skipped and counted; fields outside it are ignored and never copied.
 */
export async function readGeneric(file: string, fileIndex: number, set: SessionSet): Promise<void> {
  await readJsonl(file, set.stats, (r, line) => {
    const bad = () => { set.stats.malformedLines++; };
    if (!isRecord(r)) return bad();
    const sid = cleanId(r.sessionId);
    const ts = strictTs(r.ts);
    if (!sid || !ts || typeof r.kind !== "string" || !GENERIC_KIND.test(r.kind)) return bad();
    if (set.notAfter && ts > set.notAfter) { set.stats.futureTimestamps++; return; }
    const opt = (v: unknown, max: number = IMPORT_BOUNDS.id) => v === undefined || v === null ? undefined : cleanId(v, max);
    const project = opt(r.projectId), repo = opt(r.repo, IMPORT_BOUNDS.path), branch = opt(r.branch), cwd = opt(r.cwd, IMPORT_BOUNDS.path);
    const parent = opt(r.parentSessionId), commit = opt(r.commit);
    if ([project, repo, branch, cwd, parent, commit].includes(null)) return bad();
    if (r.files !== undefined && (!Array.isArray(r.files) || r.files.length > IMPORT_BOUNDS.eventFiles || !r.files.every((f) => typeof f === "string"))) return bad();
    if (r.tokens !== undefined && (typeof r.tokens !== "number" || !Number.isSafeInteger(r.tokens) || r.tokens < 0)) return bad();
    let outcome: { key: string; reverted: boolean } | undefined;
    if (r.outcome !== undefined) {
      if (!isRecord(r.outcome) || typeof r.outcome.reverted !== "boolean") return bad();
      const key = cleanId(r.outcome.key);
      if (!key) return bad();
      outcome = { key, reverted: r.outcome.reverted };
    }
    const at: LineRef = { file: fileIndex, line };
    const b = set.get("generic", sid);
    b.touch(ts, at);
    if (project) b.project ??= project;
    if (repo) b.remote ??= repo;
    if (cwd) b.cwd ??= cwd;
    if (branch) b.branch = branch;
    if (parent) b.parent(parent);
    // The customer's `kind` label is free text: it is validated but never copied.
    for (const f of (r.files as string[] | undefined) ?? []) b.write(f, ts, at, cwd ?? b.cwd, branch ?? b.branch, "write");
    if (commit) b.commit(commit, ts, at);
    if (outcome) b.outcome(outcome.key, outcome.reverted, ts, at);
    if (r.tokens !== undefined) b.tokens(r.tokens, ts, at);
  });
}

export const ADAPTERS: Record<ImportFormat, (file: string, fileIndex: number, set: SessionSet) => Promise<void>> = {
  "claude-code": readClaudeCode, codex: readCodex, generic: readGeneric,
};
