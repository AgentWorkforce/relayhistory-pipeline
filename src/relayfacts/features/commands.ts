/**
 * What a tool call actually ran, for the few commands that decide build evidence. A call counts only when its
 * tool is a shell, its command is read from the structured arguments, and the command line is nothing but
 * `gh pr create ...`, optionally after `cd <dir> &&` and `git push ... &&` steps and environment assignments: then the call's own
 * output is that PR's URL. Anything that could print another URL or hide what runs abstains: a heredoc, a
 * pipe, `||`, `;`, a newline, a subshell or substitution, quoted text standing in for a command, a comment, a
 * `bash -c` string, an unbalanced quote, a truncated or non-string argument.
 */

/** Tool names whose arguments are a shell command line (Claude Code, Codex and common harness spellings). */
export const SHELL_TOOLS = new Set(["Bash", "bash", "shell", "Shell", "shell_command", "exec_command", "exec", "local_shell", "run_shell_command", "terminal"]);
const SHELLS = new Set(["bash", "sh", "zsh", "/bin/bash", "/bin/sh", "/bin/zsh", "/usr/bin/bash", "/usr/bin/zsh"]);

/** True only for a command line that runs `gh pr create` and nothing that could print another PR URL. */
export function runsPrCreate(command: string): boolean {
  if (command.length > 4_000 || command.includes("<<")) return false;
  // Quoted arguments are data (titles, bodies): blank them. A double-quoted string holding `$` or a backtick (an
  // expansion) is not blanked, so it abstains below, as does an unbalanced quote.
  const bare = command.replace(/'[^']*'|"(?:\\.|[^"\\$`])*"/gsu, " Q ");
  if (/["'#`$(){}<>|;\n\r]/u.test(bare) || /&(?!&)/u.test(bare.replace(/&&/gu, ""))) return false;
  const segments = bare.split("&&").map((s) => s.trim().split(/\s+/u).filter(Boolean));
  if (!segments.length || segments.some((t) => !t.length)) return false;
  const last = segments.pop()!;
  // Only `cd <dir>` and `git push ...` steps may come first (a push prints `/pull/new/<branch>`, never a PR number).
  if (!segments.every((t) => (t[0] === "cd" && t.length === 2) || (t[0] === "git" && t[1] === "push"))) return false;
  while (last.length && /^[A-Za-z_][A-Za-z0-9_]*=/u.test(last[0]!)) last.shift();
  return last[0] === "gh" && last[1] === "pr" && last[2] === "create";
}

/**
 * The command line of a tool call's structured arguments, or null (no shell tool, no such argument). A Codex
 * argv `["bash", "-lc", "<line>"]` yields the line; any other argv is joined as words.
 */
export function shellCommand(toolName: string | null | undefined, args: unknown): string | null {
  if (!toolName || !SHELL_TOOLS.has(toolName) || !args || typeof args !== "object") return null;
  const a = args as Record<string, unknown>;
  const v = a.command ?? a.cmd;
  if (typeof v === "string") return v;
  if (Array.isArray(v) && v.length && v.every((x) => typeof x === "string")) {
    const argv = v as string[];
    if (argv.length === 3 && SHELLS.has(argv[0]!) && (argv[1] === "-c" || argv[1] === "-lc")) return argv[2]!;
    return argv.join(" ");
  }
  return null;
}
