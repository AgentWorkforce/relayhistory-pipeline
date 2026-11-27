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

/**
 * `XY path` or `XY old -> new` (porcelain v1 short format). XY must be a code git prints (STATUS_CODES): untracked
 * `??`, ignored `!!`, an unmerged pair, or index and worktree states of which at least one is set. Two spaces never
 * occur, so indented text is not a listing. The arrow form is a rename or copy and appears exactly when XY holds `R`
 * or `C`. Git quotes any path containing whitespace or escapes, and quoted paths are not read, so a path here has no
 * whitespace: a line of prose cannot pass as one.
 */
const STATUS_LINE = /^(\S\S|\S | \S) (?:([^\s"]\S*) -> )?([^\s"]\S*)$/;
const UNMERGED = ["DD", "AU", "UD", "UA", "DU", "AA", "UU"];
const STATUS_CODES = new Set([
  "??", "!!", ...UNMERGED,
  // Index updated (X), worktree unchanged or changed again (Y).
  ...["M", "T", "A", "R", "C"].flatMap((x) => [" ", "M", "T", "D"].map((y) => x + y)), "D ",
  // Index unchanged, worktree changed; " A" is an intent-to-add, " R"/" C" a worktree rename or copy.
  ...["M", "T", "D", "A", "R", "C"].map((y) => " " + y),
]);
/** The path a status line names (a rename's or copy's destination), or null when it is not a line git can print. */
function statusPath(line: string): string | null {
  const m = STATUS_LINE.exec(line);
  if (!m || !STATUS_CODES.has(m[1]!)) return null;
  const renames = /[RC]/.test(m[1]!);
  return renames === (m[2] !== undefined) ? m[3]! : null;
}
/** `## branch...upstream` header of `git status -sb`. */
const BRANCH_LINE = /^## \S/;
/**
 * One `--stat` line: ` path | 12 +++--` (a text change: its count, then the scaled graph) or
 * ` path | Bin 10 -> 20 bytes` (a binary change). Git aligns the `|` of every line in a block.
 */
const STAT_LINE = /^ (\S.*?)( +)\| +(?:(\d+)(?: [+-]+)?|Bin \d+ -> \d+ bytes)$/;
/** ` 3 files changed, 10 insertions(+), 2 deletions(-)` */
const STAT_SUMMARY = /^ (\d+) files? changed(?:, (\d+) insertions?\(\+\))?(?:, (\d+) deletions?\(-\))?$/;
/** A `--stat` rename: `old => new`, or a braced common part, `src/{old => new}/a.ts`. */
const BRACED_RENAME = /^([^{}]*)\{([^{}]*) => ([^{}]*)\}([^{}]*)$/;

/** A stat line's path: a rename's destination, or null when the rename notation is not one git prints. */
function statPath(name: string): string | null {
  const arrows = name.split(" => ").length - 1;
  if (arrows === 0) return /[{}]/.test(name) ? null : name;
  if (arrows !== 1) return null;
  const b = BRACED_RENAME.exec(name);
  if (b) return `${b[1]}${b[3]}${b[4]}`.replace(/\/{2,}/g, "/").replace(/^\//, "");
  return /[{}]/.test(name) ? null : name.split(" => ")[1]!;
}

/** A `--stat` block (stat lines, then its summary): the paths, or null when the block is not one git prints. */
function statBlock(body: string[], summary: string): string[] | null {
  const sum = STAT_SUMMARY.exec(summary);
  if (!sum || !body.length || Number(sum[1]) !== body.length) return null;
  const rows = body.map((l) => STAT_LINE.exec(l));
  if (rows.some((m) => !m)) return null;
  // The pipes line up, and the per-file counts add up to the summary's insertions and deletions.
  const pipe = new Set(body.map((l) => l.indexOf(" | ")));
  const counted = rows.reduce((n, m) => n + Number(m![3] ?? 0), 0);
  if (pipe.size !== 1 || counted !== Number(sum[2] ?? 0) + Number(sum[3] ?? 0)) return null;
  const paths = rows.map((m) => statPath(m![1]!));
  return paths.every((p) => p !== null) ? paths as string[] : null;
}

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
  const named = status.map(statusPath);
  if (status.length && named.every((p) => p !== null)) return cap(named as string[]);
  // git diff --stat: stat lines, then exactly one summary line, last, consistent with them.
  const last = lines.at(-1)!, body = lines.slice(0, -1);
  const stat = statBlock(body, last);
  if (stat) return cap(stat);
  return [];
}
