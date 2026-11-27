/**
 * Which `gh` commands a tool call actually runs (docs/session-intel-cloud-harvest.md, "Facts"). Only a small shell
 * grammar is read, and anything outside it makes the whole call yield no command: the conservative direction, since
 * a missed create owns nothing.
 *
 * Read: segments joined by unquoted `;`, `&&`, `||`, `|` or newlines; in each, optional `NAME=value` assignments, then
 * a command word and its arguments (words are quoted with `'` or `"`, or escaped with `\`); and exactly
 * `bash|sh|zsh -c|-lc '<script>'`, whose script is read the same way. Redirections after the command are arguments.
 *
 * Refused (the call yields nothing): a comment (`#` starting a word), a heredoc or here-string (`<<`), command
 * substitution (`$(` or a backtick), a subshell or group (`(`, `)`), a background `&`, an unbalanced quote, a
 * wrapper or indirection as the command word (`sudo`, `env`, `command`, `exec`, `eval`, `xargs`, …), and a shell
 * invoked any other way.
 *
 * The script comes from the call's own envelope only: when the whole call text is a JSON object, its `cmd` or
 * `command` string; otherwise the text itself, after an `exec_command` or `Bash` tool-name prefix. JSON found
 * anywhere else (a `printf` of JSON) is data, not an envelope.
 */
const MAX_TEXT = 64 * 1024;
const MAX_DEPTH = 3;
const INDIRECT = new Set(["sudo", "doas", "env", "command", "builtin", "exec", "eval", "time", "nohup", "nice", "timeout", "xargs", "watch", "source", ".", "su", "script", "ssh"]);
const SHELLS = new Set(["bash", "sh", "zsh", "dash", "ksh", "fish"]);

/** Splits at unquoted `;`, `&&`, `||`, `|` and newlines into segments of words. Null on any refused construct. */
function parse(text: string): string[][] | null {
  const segs: string[][] = [];
  let words: string[] = [], cur = "", inWord = false, quote: "'" | '"' | null = null;
  const endWord = () => { if (inWord) words.push(cur); cur = ""; inWord = false; };
  const endSeg = () => { endWord(); if (words.length) segs.push(words); words = []; };
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (quote === "'") { if (c === "'") quote = null; else cur += c; continue; }
    if (quote === '"') {
      if (c === '"') { quote = null; continue; }
      if (c === "`" || (c === "$" && text[i + 1] === "(")) return null;
      if (c === "\\" && i + 1 < text.length && '"\\$`\n'.includes(text[i + 1]!)) { cur += text[++i]; continue; }
      cur += c; continue;
    }
    if (c === "'" || c === '"') { quote = c; inWord = true; continue; }
    if (c === "\\") { if (i + 1 >= text.length) return null; if (text[i + 1] !== "\n") { cur += text[i + 1]; inWord = true; } i++; continue; }
    if (c === "#" && !inWord) return null;
    if (c === "`" || c === "(" || c === ")" || (c === "$" && text[i + 1] === "(") || (c === "<" && text[i + 1] === "<")) return null;
    if (c === "&") { if (text[i + 1] === "&") { i++; endSeg(); continue; } if (text[i - 1] === ">" || text[i + 1] === ">") { cur += c; inWord = true; continue; } return null; }
    if (c === ";" || c === "\n" || c === "|") { if (c === "|" && text[i + 1] === "|") i++; endSeg(); continue; }
    if (c === " " || c === "\t") { endWord(); continue; }
    cur += c; inWord = true;
  }
  if (quote) return null;
  endSeg();
  return segs;
}

/** The executed command lines in `script`, following `sh -c` scripts. Null on anything outside the grammar. */
function commands(script: string, depth: number): string[][] | null {
  if (depth > MAX_DEPTH) return null;
  const segs = parse(script);
  if (!segs) return null;
  const out: string[][] = [];
  for (let w of segs) {
    while (w.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(w[0]!)) w = w.slice(1);
    if (!w.length) continue;
    const prog = w[0]!.split("/").pop()!;
    if (INDIRECT.has(prog)) return null;
    if (SHELLS.has(prog)) {
      if (w.length !== 3 || (w[1] !== "-c" && w[1] !== "-lc")) return null;
      const inner = commands(w[2]!, depth + 1);
      if (!inner) return null;
      out.push(...inner);
      continue;
    }
    out.push([prog, ...w.slice(1)]);
  }
  return out;
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

/** Every command a tool call's text executes, each as its words; none when any part is outside the grammar. */
export function executedCommands(text: string): string[][] {
  if (!text || text.length > MAX_TEXT) return [];
  const script = scriptOf(text);
  if (script === null) return [];
  return commands(script, 0) ?? [];
}

/** The `gh` commands a tool call executes, each as its arguments after `gh`. */
export function ghCommands(text: string): string[][] {
  return executedCommands(text).filter((w) => w[0] === "gh").map((w) => w.slice(1));
}
