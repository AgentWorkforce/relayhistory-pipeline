/**
 * Which `gh` commands a tool call actually runs (docs/session-intel-cloud-harvest.md, "Facts"). A call's text is
 * split into shell segments at unquoted separators (`;`, `&&`, `||`, `|`, `&`, newlines, `$(`, `(`, backticks), and
 * a segment runs `gh` only when `gh` is its first word, after any `NAME=value` assignments and the wrappers
 * `sudo`, `env`, `command`, `exec`, `time` and `nohup`. The script of `bash|sh|zsh -c '…'` (or `-lc`) is read the
 * same way.
 *
 * The script comes from the call's own envelope only: when the whole call text is a JSON object, its `cmd` or
 * `command` string; otherwise the text itself, after an `exec_command` or `Bash` tool-name prefix. JSON found
 * anywhere else (a `printf` of JSON, a file being written) is data, not an envelope. A script holding an unquoted
 * `<<` (a heredoc or here-string, whose body is data the parser cannot delimit safely) yields no command at all.
 *
 * `echo "gh pr create …"`, a quoted mention, a heredoc body, or prose is not a `gh` command. When a call's shape is
 * not recognized the result is no command, which is the conservative direction: a missed create owns nothing.
 */
const MAX_TEXT = 64 * 1024;
const MAX_DEPTH = 3;
const WRAPPERS = new Set(["sudo", "env", "command", "exec", "time", "nohup"]);
const SHELLS = new Set(["bash", "sh", "zsh"]);

/** Splits at unquoted shell separators; quotes are kept in the pieces. Null when the script holds an unquoted `<<`. */
function segments(text: string): string[] | null {
  const out: string[] = [];
  let cur = "", quote: "'" | '"' | null = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (quote) {
      if (c === "\\" && quote === '"' && i + 1 < text.length) { cur += c + text[++i]; continue; }
      if (c === quote) quote = null;
      cur += c; continue;
    }
    if (c === "\\" && i + 1 < text.length) { cur += c + text[++i]; continue; }
    if (c === "'" || c === '"') { quote = c; cur += c; continue; }
    if (c === "<" && text[i + 1] === "<") return null;
    if (c === ";" || c === "\n" || c === "|" || c === "&" || c === "(" || c === ")" || c === "`" || (c === "$" && text[i + 1] === "(")) {
      out.push(cur); cur = ""; if (c === "$") i++; continue;
    }
    cur += c;
  }
  out.push(cur);
  return out.map((s) => s.trim()).filter(Boolean);
}

/** Shell words of one segment, unquoted (no expansion). Null when a quote is unbalanced. */
function words(segment: string): string[] | null {
  const out: string[] = [];
  let cur = "", quote: "'" | '"' | null = null, any = false;
  for (let i = 0; i < segment.length; i++) {
    const c = segment[i]!;
    if (quote) {
      if (c === quote) { quote = null; continue; }
      if (c === "\\" && quote === '"' && i + 1 < segment.length) { cur += segment[++i]; continue; }
      cur += c; continue;
    }
    if (c === "'" || c === '"') { quote = c; any = true; continue; }
    if (c === "\\" && i + 1 < segment.length) { cur += segment[++i]; any = true; continue; }
    if (/\s/.test(c)) { if (cur || any) out.push(cur); cur = ""; any = false; continue; }
    cur += c;
  }
  if (quote) return null;
  if (cur || any) out.push(cur);
  return out;
}

/** The executed command lines (as word lists) in `script`, following `sh -c` scripts. */
function commands(script: string, depth: number, out: string[][]): void {
  if (depth > MAX_DEPTH) return;
  const segs = segments(script);
  if (segs === null) { out.push(["<unsupported>"]); return; }
  for (const seg of segs) {
    let w = words(seg);
    if (!w) continue;
    while (w.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(w[0]!)) w = w.slice(1);
    while (w.length && WRAPPERS.has(w[0]!)) {
      w = w.slice(1);
      while (w.length && (/^-/.test(w[0]!) || /^[A-Za-z_][A-Za-z0-9_]*=/.test(w[0]!))) w = w.slice(1);
    }
    if (!w.length) continue;
    const prog = w[0]!.split("/").pop()!;
    if (SHELLS.has(prog)) {
      const i = w.findIndex((x, k) => k > 0 && /^-[a-z]*c$/.test(x));
      if (i > 0 && w[i + 1] !== undefined) commands(w[i + 1]!, depth + 1, out);
      continue;
    }
    out.push([prog, ...w.slice(1)]);
  }
}

/** The script a call's envelope carries, or null when the envelope is not one this parser reads. */
function scriptOf(text: string): string | null {
  const t = text.trim();
  if (t.startsWith("{")) {
    let v: unknown;
    try { v = JSON.parse(t); } catch { return null; }
    if (!v || typeof v !== "object" || Array.isArray(v)) return null;
    const o = v as Record<string, unknown>;
    const cmd = typeof o.cmd === "string" ? o.cmd : typeof o.command === "string" ? o.command : null;
    return cmd;
  }
  return t.replace(/^(?:exec_command|Bash)\s+(?=\S)/, "");
}

/** Every command a tool call's text executes, each as its words; none when any part is unsupported. */
export function executedCommands(text: string): string[][] {
  if (!text || text.length > MAX_TEXT) return [];
  const script = scriptOf(text);
  if (script === null) return [];
  const out: string[][] = [];
  commands(script, 0, out);
  return out.some((w) => w[0] === "<unsupported>") ? [] : out;
}

/** The `gh` commands a tool call executes, each as its arguments after `gh`. */
export function ghCommands(text: string): string[][] {
  return executedCommands(text).filter((w) => w[0] === "gh").map((w) => w.slice(1));
}
