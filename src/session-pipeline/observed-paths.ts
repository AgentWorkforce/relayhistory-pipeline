/**
 * Observed paths (docs/event-facts-observed-paths.md): the paths a tool result shows as changed in the session's
 * checkout, from `git status --short` output. They are observations, not writes, and are kept apart from
 * files_touched. Two lines of work listing the same dirty paths can suggest a shared checkout, or one picking up the
 * other's uncommitted work; it does not establish either.
 *
 * Only `git status --short` is read, because only it names paths unambiguously: git quotes any path containing
 * whitespace, quotes, backslashes or control characters, so an unquoted path is exactly the file's name. `--stat`
 * is not read: it prints names verbatim and pads them with spaces, so a name cannot be recovered from it (`tail`
 * and `tail ` print the same; `a => b` may be a rename or a file so named).
 *
 * A result is read only when its whole text is such a listing; anything else (prose, mixed output, a log that
 * merely contains a line like ` M x`) yields nothing. Pure and bounded: at most MAX_OBSERVED_PATHS paths of at
 * most MAX_OBSERVED_PATH_CHARS each, from at most MAX_OBSERVED_CONTENT characters.
 */
export const MAX_OBSERVED_PATHS = 64;
export const MAX_OBSERVED_PATH_CHARS = 512;
export const MAX_OBSERVED_CONTENT = 256 * 1024;

/**
 * `XY path` or `XY old -> new` (porcelain v1 short format). XY must be a code git prints (STATUS_CODES): untracked
 * `??`, ignored `!!`, an unmerged pair, or index and worktree states of which at least one is set. Two spaces never
 * occur, so indented text is not a listing. The arrow form is a rename or copy and appears exactly when XY holds `R`
 * or `C`. Git quotes any path containing whitespace, a double quote, a backslash or a control character
 * (`"a b.txt"`); a quoted path is not decoded, so its line is skipped and the rest of the listing still counts. An
 * unquoted path is read only when it is printable ASCII without space, `"` or `\`: exactly what default git prints
 * unquoted. Under `core.quotePath=false` git prints other characters raw (a trailing U+00A0 among them), which no
 * line-based reading can delimit safely, so a listing with such a name is refused. Lines are never trimmed: the bytes
 * validated are the bytes stored.
 */
/** Git's C-style quoting, exactly: printable ASCII (space included) except `"` and `\\`, or one of git's escapes. */
const QUOTED = '"(?:\\\\(?:[abfnrtv"\\\\]|[0-3][0-7]{2})|[ !#-\\[\\]-~])*"';
const UNQUOTED = '[!#-\\[\\]-~]+';
const STATUS_LINE = new RegExp(`^(\\S\\S|\\S | \\S) (?:(${UNQUOTED}|${QUOTED}) -> )?(${UNQUOTED}|${QUOTED})$`);
const UNMERGED = ["DD", "AU", "UD", "UA", "DU", "AA", "UU"];
const STATUS_CODES = new Set([
  "??", "!!", ...UNMERGED,
  // Index updated (X), worktree unchanged or changed again (Y).
  ...["M", "T", "A", "R", "C"].flatMap((x) => [" ", "M", "T", "D"].map((y) => x + y)), "D ",
  // Index unchanged, worktree changed; " A" is an intent-to-add, " R"/" C" a worktree rename or copy.
  ...["M", "T", "D", "A", "R", "C"].map((y) => " " + y),
]);
/** A line that is well formed but names no path this parser stores (a quoted path, an ambiguous rename). */
const SKIP = Symbol("skip");
/**
 * The path a status line names (a rename's or copy's destination), SKIP for a valid line whose path is quoted, or
 * null when it is not a line git can print (which refuses the whole listing).
 */
function statusPath(line: string): string | typeof SKIP | null {
  const m = STATUS_LINE.exec(line);
  if (!m || !STATUS_CODES.has(m[1]!)) return null;
  const renames = /[RC]/.test(m[1]!);
  if (renames !== (m[2] !== undefined)) return null;
  // git status --short prints names relative to the working directory, even for an absolute pathspec, so an
  // absolute operand (either side of a rename or copy, quoted or not) means the text is not git's listing.
  if ([m[2], m[3]].some((x) => x !== undefined && /^"?(?:\/|[A-Za-z]:)/.test(x))) return null;
  // A line with any quoted operand stores nothing: its names are not decoded, and a rename's destination is not
  // taken on the word of a source this parser cannot read.
  return m[3]!.startsWith('"') || m[2]?.startsWith('"') ? SKIP : m[3]!;
}
/** `## branch...upstream` header of `git status -sb`. */
const BRANCH_LINE = /^## \S/;
function valid(path: string): boolean {
  return path.length > 0 && path.length <= MAX_OBSERVED_PATH_CHARS && path.trim() === path && !path.startsWith("-")
    && !/[\u0000-\u001f\u007f]/.test(path) && !path.includes("...");
}

function cap(paths: string[]): string[] {
  const out: string[] = [];
  for (const p of paths) if (valid(p) && !out.includes(p)) { out.push(p); if (out.length === MAX_OBSERVED_PATHS) break; }
  return out;
}

/** The paths a tool result's text observes as changed, or [] when the text is not exactly such a listing. */
export function observedPaths(content: string | null | undefined): string[] {
  if (typeof content !== "string" || content.length === 0 || content.length > MAX_OBSERVED_CONTENT) return [];
  // Only the line terminators are removed (\n, or \r\n); nothing else is trimmed, so no name loses a byte.
  const lines = content.replace(/\r\n/g, "\n").split("\n").filter((l) => l !== "");
  if (!lines.length) return [];
  // git status --short (optionally -b): every line is a status line or the one branch header.
  const status = lines.filter((l, i) => !(i === 0 && BRANCH_LINE.test(l)));
  const named = status.map(statusPath);
  if (status.length && named.every((p) => p !== null)) return cap(named.filter((p): p is string => p !== SKIP));
  return [];
}
