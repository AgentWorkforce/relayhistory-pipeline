/**
 * Observed paths (docs/event-facts-observed-paths.md): the paths a tool result shows as changed in
 * the session's checkout, from `git status --short` or `git diff --stat` output. They are observations, not writes,
 * and are kept apart from files_touched. Two lines of work listing the same dirty paths can suggest a shared
 * checkout, or one picking up the other's uncommitted work; it does not establish either.
 *
 * A result is read only when its whole text has one of the two shapes; anything else (prose, mixed output, a log
 * that merely contains a line like ` M x`) yields nothing. Pure and bounded: at most MAX_OBSERVED_PATHS paths of at
 * most MAX_OBSERVED_PATH_CHARS each, from at most MAX_OBSERVED_CONTENT characters.
 */
export const MAX_OBSERVED_PATHS = 64;
export const MAX_OBSERVED_PATH_CHARS = 512;
export const MAX_OBSERVED_CONTENT = 256 * 1024;

/** `XY path` or `XY old -> new` (porcelain v1 short format). Quoted paths (spaces, escapes) are not read. */
const STATUS_LINE = /^([ MADRCUT?!]{2}) (?:([^\s"][^\n]*?) -> )?([^\s"][^\n]*)$/;
/** `## branch...upstream` header of `git status -sb`. */
const BRANCH_LINE = /^## \S/;
/** ` path | 12 +++--` or ` path | Bin 10 -> 20 bytes`. */
const STAT_LINE = /^ (\S[^|]*?)\s+\|\s+(?:\d+(?: [+-]*)?|Bin [^\n]*)$/;
/** ` 3 files changed, 10 insertions(+), 2 deletions(-)` */
const STAT_SUMMARY = /^ \d+ files? changed(?:, \d+ insertions?\(\+\))?(?:, \d+ deletions?\(-\))?$/;

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
  const lines = content.replace(/\r\n/g, "\n").split("\n").filter((l) => l.trimEnd() !== "").map((l) => l.trimEnd());
  if (!lines.length) return [];
  // git status --short (optionally -b): every line is a status line or the one branch header.
  const status = lines.filter((l, i) => !(i === 0 && BRANCH_LINE.test(l)));
  if (status.length && status.every((l) => STATUS_LINE.test(l))) return cap(status.map((l) => STATUS_LINE.exec(l)![3]!));
  // git diff --stat: stat lines, then exactly one summary line, last.
  const last = lines.at(-1)!, body = lines.slice(0, -1);
  if (STAT_SUMMARY.test(last) && body.length && body.every((l) => STAT_LINE.test(l))) return cap(body.map((l) => STAT_LINE.exec(l)![1]!));
  return [];
}
