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

type Sep = ";" | "&&" | "||" | "|";
/** One segment's words and the connector joining it to the previous segment (null for the first). */
interface Segment { words: string[]; after: Sep | null }
/** Splits at unquoted `;`, `&&`, `||`, `|` and newlines into segments of words. Null on any refused construct. */
function parse(text: string): Segment[] | null {
  const segs: Segment[] = [];
  let words: string[] = [], cur = "", inWord = false, quote: "'" | '"' | null = null, pending: Sep | null = null;
  const endWord = () => { if (inWord) words.push(cur); cur = ""; inWord = false; };
  const endSeg = (sep: Sep) => {
    endWord();
    if (words.length) { segs.push({ words, after: segs.length ? pending : null }); words = []; pending = sep; }
    else if (segs.length && sep !== ";") pending = sep; // `a && \n b`: keep the stronger connector
  };
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
    if (c === "&") { if (text[i + 1] === "&") { i++; endSeg("&&"); continue; } if (text[i - 1] === ">" || text[i + 1] === ">") { cur += c; inWord = true; continue; } return null; }
    if (c === ";" || c === "\n") { endSeg(";"); continue; }
    if (c === "|") { if (text[i + 1] === "|") { i++; endSeg("||"); } else endSeg("|"); continue; }
    if (c === " " || c === "\t") { endWord(); continue; }
    cur += c; inWord = true;
  }
  if (quote) return null;
  endSeg(";");
  return segs;
}

/** The executed command lines in `script`, following `sh -c` scripts. Null on anything outside the grammar. */
function commands(script: string, depth: number): string[][] | null {
  if (depth > MAX_DEPTH) return null;
  const segs = parse(script);
  if (!segs) return null;
  const out: string[][] = [];
  for (const { words } of segs) {
    let w = words;
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

/**
 * Whether the call is exactly one `gh pr create` invocation, so that its result's output can only have come from it:
 * the whole script (or the one `sh -c` script it runs) is `gh pr create …`, optionally after `cd <dir> &&` steps
 * (which print nothing and stop the chain on failure), and nothing follows it. A compound call such as
 * `true || gh pr create; printf <url>` can succeed and print a URL without running gh, so it claims nothing.
 */
export function soleCreate(text: string): boolean {
  if (!text || text.length > MAX_TEXT) return false;
  let script = scriptOf(text);
  for (let depth = 0; script !== null && depth <= MAX_DEPTH; depth++) {
    const segs = parse(script);
    if (!segs || !segs.length) return false;
    const strip = (w: string[]) => { let x = w; while (x.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(x[0]!)) x = x.slice(1); return x; };
    const last = strip(segs.at(-1)!.words), prog = last[0]?.split("/").pop();
    const prefixOk = segs.slice(0, -1).every((g) => { const w = strip(g.words); return w[0] === "cd" && w.length <= 2; })
      && segs.slice(1).every((g) => g.after === "&&");
    if (!prefixOk) return false;
    if (prog === "gh") return last[1] === "pr" && last[2] === "create";
    if (prog && SHELLS.has(prog) && segs.length === 1 && last.length === 3 && (last[1] === "-c" || last[1] === "-lc")) { script = last[2]!; continue; }
    return false;
  }
  return false;
}

/** The `gh` commands a tool call executes, each as its arguments after `gh`. */
export function ghCommands(text: string): string[][] {
  return executedCommands(text).filter((w) => w[0] === "gh").map((w) => w.slice(1));
}
