/**
 * Normalization for the enterprise trace import (docs/session-intel-import.md). The adapters feed a
 * `SessionBuilder` with ids, timestamps, repo-relative paths, shas and token counts only; `toSnapshotSession`
 * hashes every customer id with the import salt and validates the result with the M0 loader's own
 * `parseSession`, so an import the replay harness would refuse is never written.
 */
import { createHmac } from "node:crypto";
import { parseSession, SNAPSHOT_BOUNDS, type SnapshotSession } from "../replay/snapshot.ts";
import { isoTime, textCompare } from "../replay/util.ts";

export type ImportHarness = "claude" | "codex" | "generic";

/** Where a fact came from, for the private mapping file only: an index into the input file list and a line. */
export interface LineRef { file: number; line: number }

export const IMPORT_BOUNDS = {
  inputFiles: 100_000,
  inputFileBytes: 2 * 1024 * 1024 * 1024,
  /** An input line longer than this is skipped and counted (a Claude Code line can carry a large tool result). */
  inputLineBytes: 16 * 1024 * 1024,
  walkDepth: 32,
  /** The M0 loader limits. */
  sessions: SNAPSHOT_BOUNDS.sessions,
  facts: SNAPSHOT_BOUNDS.facts,
  parents: SNAPSHOT_BOUNDS.parents,
  id: SNAPSHOT_BOUNDS.id,
  path: SNAPSHOT_BOUNDS.path,
  lineBytes: SNAPSHOT_BOUNDS.lineBytes,
  fileBytes: SNAPSHOT_BOUNDS.fileBytes,
  windowMs: SNAPSHOT_BOUNDS.windowMs,
  /** Files named by one generic event. */
  eventFiles: 1_000,
  /** Facts held in memory across the whole import; beyond it the snapshot could not fit its 256 MiB file anyway. */
  totalFacts: 5_000_000,
} as const;

export interface ImportStats {
  files: number;
  lines: number;
  malformedLines: number;
  oversizeLines: number;
  untimedRecords: number;
  droppedPaths: number;
  truncatedFacts: number;
  oversizeSessions: number;
  sessions: number;
  writes: number;
  commits: number;
  outcomes: number;
  usage: number;
  /** Facts held in memory, across sessions. */
  heldFacts: number;
  /** Records dropped for a timestamp after the import's clock (plus a day of skew). */
  futureTimestamps: number;
  /** Sessions dropped because they ended outside the 120-day window the import keeps. */
  outOfWindowSessions: number;
  /** Facts dropped because they fell before the start of that window. */
  outOfWindowFacts: number;
  /** Sessions kept but clamped to start at that window (their earlier records or facts dropped). */
  clampedSessions: number;
  /** Modern Codex child records before a NEW_TASK addressed to that child (copied context and setup). */
  codexPrefixRecordsDropped: number;
  /** Children with copied-context metadata whose own task boundary could not be verified. */
  codexChildrenWithoutTaskBoundary: number;
}

export const emptyStats = (): ImportStats => ({
  files: 0, lines: 0, malformedLines: 0, oversizeLines: 0, untimedRecords: 0, droppedPaths: 0, truncatedFacts: 0,
  oversizeSessions: 0, sessions: 0, writes: 0, commits: 0, outcomes: 0, usage: 0, heldFacts: 0,
  futureTimestamps: 0, outOfWindowSessions: 0, outOfWindowFacts: 0, clampedSessions: 0,
  codexPrefixRecordsDropped: 0, codexChildrenWithoutTaskBoundary: 0,
});

/** An error whose message is safe to print: it names flags, bounds and codes, never a path, id, salt or text. */
export class ImportError extends Error {
  constructor(message: string) { super(`import: ${message}`); this.name = "ImportError"; }
}

/** A strict ISO-8601 timestamp with a zone, normalized, or null. */
export function strictTs(v: unknown): string | null {
  try { return isoTime(v, "ts"); } catch { return null; }
}

/** A bounded single-line string id, or null. */
export function cleanId(v: unknown, max: number = IMPORT_BOUNDS.id): string | null {
  if (typeof v !== "string") return null;
  const s = v.trim();
  // eslint-disable-next-line no-control-regex
  return s.length > 0 && s.length <= max && !/[\u0000-\u001f\u007f]/u.test(s) ? s : null;
}

/** The same source roots decisionTopic (src/session-analysis/decision-records.ts) anchors an absolute path at. */
export const SOURCE_ROOTS: ReadonlySet<string> = new Set(["src", "packages", "apps", "lib", "libs", "services", "crates", "cmd", "internal", "scripts", "docs", "supabase", "flows", "test", "tests", "e2e", "infra"]);
/**
 * A home directory and its user: `/Users/<u>`, `/home/<u>`, `/var/home/<u>`, `/export/home/<u>`,
 * `/mnt/<drive>/Users/<u>`, `C:/Users/<u>`, or `/root` (no user name to drop, but still a home root).
 */
const HOME = /^(?:(?:[a-z]:|\/mnt\/[a-z])\/users|(?:\/var|\/export)?\/home|\/users)\/([^/]+)|^\/root(?=\/|$)/iu;
const HOME_DIRS = new Set(["users", "home", "root"]);

const slashes = (p: string) => p.replace(/\\/gu, "/");
const trimSlash = (p: string) => p.replace(/\/+$/u, "");

/** The user names of any home directories in these paths (see HOME). */
function homeUsers(...paths: (string | null)[]): Set<string> {
  const users = new Set<string>();
  for (const p of paths) {
    const m = p ? HOME.exec(slashes(p)) : null;
    if (m?.[1]) users.add(m[1].toLowerCase());
  }
  return users;
}

/** True when `dir` is a home directory itself (`/Users/<u>`, `/root`, ...), not a checkout inside one. */
export function isHomeRoot(dir: string | null): boolean {
  if (!dir) return false;
  const d = trimSlash(slashes(dir.trim()).replace(/\/{2,}/gu, "/"));
  const m = HOME.exec(d);
  return !!m && m[0].length === d.length;
}

const anchorAtSourceRoot = (parts: string[]): string[] | null => {
  const root = parts.findIndex((s) => SOURCE_ROOTS.has(s.toLowerCase()));
  return root < 0 ? null : parts.slice(root);
};

/**
 * A repo-relative path, or null when it cannot be made one safely. An absolute path under `cwd` loses the
 * `cwd` prefix; any other absolute path is anchored at its first source-root segment (decisionTopic's rule)
 * or dropped. A path with `..` or `~`, a path into a home directory, and any path that still names the home
 * directory's user are dropped, so no home directory or user name survives.
 */
export function repoRelativePath(raw: string, cwd: string | null): string | null {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > IMPORT_BOUNDS.path) return null;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/u.test(raw)) return null;
  // A URL is not a file path, and may carry credentials.
  if (raw.includes("://")) return null;
  let p = slashes(raw.trim()).replace(/\/{2,}/gu, "/");
  const absolute = /^(?:\/|~|[A-Za-z]:\/)/u.test(p);
  // As decisionTopic: a path with `..` or `~` anywhere is never anchored.
  if (p.split("/").some((s) => s === ".." || s.startsWith("~"))) return null;
  // A cwd at a home root is not a checkout: stripping it would keep home-relative paths (Documents/, dot-dirs).
  const homeCwd = isHomeRoot(cwd);
  const base = cwd && !homeCwd ? trimSlash(slashes(cwd.trim()).replace(/\/{2,}/gu, "/")) : null;
  if (absolute || homeCwd) {
    if (absolute && base && /^(?:\/|[A-Za-z]:\/)/u.test(base) && p.toLowerCase().startsWith(`${base.toLowerCase()}/`)) p = p.slice(base.length + 1);
    else {
      const anchored = anchorAtSourceRoot(p.split("/").filter((s) => s !== "" && s !== "."));
      if (!anchored) return null;
      p = anchored.join("/");
    }
  }
  const parts = p.split("/").filter((s) => s !== "" && s !== ".");
  if (parts.length === 0) return null;
  if (parts.some((s) => s === ".." || s.startsWith("~"))) return null;
  if (/^[A-Za-z]:$/u.test(parts[0]!) || HOME_DIRS.has(parts[0]!.toLowerCase())) return null;
  const users = homeUsers(raw, cwd);
  if (parts.some((s) => users.has(s.toLowerCase()))) return null;
  const out = parts.join("/");
  return out.length <= IMPORT_BOUNDS.path ? out : null;
}

const REPO_NAME = /^[a-z0-9._-]{1,200}$/u;

/**
 * The repository name from a remote, never its credentials. A `scheme://` URL keeps only its path (the authority,
 * with any `user[:password]@`, is dropped); `[user@]host:path` needs a `/` in its path (`host:SECRET` gives
 * nothing); anything else is a local path. Query, fragment and `;params` are cut. A remote with whitespace, a
 * control character or a percent escape, an `@` left in its path, or a last segment that is not a plain name
 * (`[a-z0-9._-]`) gives null rather than a guess. Shared by the import and the harvester so the two cannot drift.
 */
export function repoNameFromRemote(remote: string): string | null {
  const raw = slashes(remote.trim());
  if (!raw || /[\s\u0000-\u001f\u007f%]/u.test(raw)) return null;
  let path: string;
  const url = /^[a-z][a-z0-9+.-]*:\/\/[^/]*(.*)$/isu.exec(raw);
  const scp = url ? null : /^(?:[^/@:]+@)?[^/@:]+:(.*)$/su.exec(raw);
  if (url) path = url[1]!;
  else if (scp) {
    if (!scp[1]!.includes("/")) return null;
    path = scp[1]!;
  } else path = raw;
  path = path.replace(/[?#;].*$/su, "");
  if (path.includes("@")) return null;
  const name = path.split("/").filter(Boolean).pop()?.replace(/\.git$/iu, "").toLowerCase();
  return name && REPO_NAME.test(name) ? name : null;
}

/** The customer's repository or project name: from a remote URL ({@link repoNameFromRemote}), or the cwd's basename. */
export function projectKey(remote: string | null, cwd: string | null): string | null {
  const fromRemote = remote ? repoNameFromRemote(remote) : null;
  if (fromRemote) return fromRemote;
  if (!cwd) return null;
  const c = trimSlash(slashes(cwd.trim()));
  // A home directory itself is a user name, not a project.
  if (isHomeRoot(c)) return null;
  const name = c.split("/").filter(Boolean).pop();
  return name && !/^[A-Za-z]:$/u.test(name) ? name.toLowerCase() : null;
}

/** Git's own branch spelling, so a hash keeps the detectors' branch equality. */
const canonicalBranch = (b: string) => b.trim().replace(/^refs\/heads\//u, "").replace(/^origin\//u, "").toLowerCase();

/** Salted, domain-separated, truncated HMAC-SHA256: a report never carries a raw customer id. */
export class Hasher {
  constructor(private readonly salt: string) {}
  private h(domain: string, value: string, hex: number) {
    return createHmac("sha256", this.salt).update(`${domain}\u0000${value}`).digest("hex").slice(0, hex);
  }
  /** `harness` is an import format's harness, or a read-API source name (the harvest, src/session-intel/harvest). */
  session(harness: string, id: string) { return `s_${this.h(`session\u0000${harness}`, id, 32)}`; }
  project(key: string) { return `p_${this.h("project", key, 24)}`; }
  repo(key: string) { return `r_${this.h("repo", key, 24)}`; }
  branch(b: string | null) { return b ? `b_${this.h("branch", canonicalBranch(b), 16)}` : null; }
  org(id: string) { return `o_${this.h("org", id, 24)}`; }
  workspace(id: string) { return `w_${this.h("workspace", id, 24)}`; }
  /** One path segment, lowercased first (the detectors compare paths lowercased), 12 hex. */
  segment(seg: string) { return this.h("path", seg.toLowerCase(), 12); }
}

/** Directory names the migration detector keys on (`migrations/`, `db/migrate/`), kept in clear with the source roots. */
const MIGRATION_DIRS: ReadonlySet<string> = new Set(["migrations", "migration", "migrate", "db"]);
/**
 * The file names KNOWN_HOT_PATH in packages/coordination-detect/src/index.ts matches by exact name. The
 * detector skips hot paths; hashed, they would raise false duplicate-work alerts in a replay. A test parses
 * KNOWN_HOT_PATH and fails if the two lists drift.
 */
export const HOT_PATH_FILES: ReadonlySet<string> = new Set([
  "bun.lock", "package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "cargo.lock", "poetry.lock",
  "composer.lock", "gemfile.lock", "_journal.json", "generated-manifest.json", "manifest.generated.json",
]);
/** File names kept in clear by exact (lowercased) name: the hot paths and package.json. */
const KNOWN_FILES: ReadonlySet<string> = new Set([...HOT_PATH_FILES, "package.json"]);
/** KNOWN_HOT_PATH's other form, `generated/*manifest*.json`. */
const GENERATED_MANIFEST = /^[^/]*manifest[^/]*\.json$/u;
/**
 * The extensions kept in clear: a fixed allow-list of common code, config and doc extensions. Any other
 * extension (`.initech`, `.production`, a sentence after the last dot) is hashed with the name.
 */
export const CLEAR_EXTENSIONS: ReadonlySet<string> = new Set([
  "ts", "tsx", "js", "jsx", "mjs", "cjs", "json", "jsonc", "jsonl", "md", "mdx", "rst", "sql", "py", "rb", "go", "rs",
  "java", "kt", "kts", "scala", "swift", "c", "h", "cc", "cpp", "hpp", "cs", "php", "lua", "dart", "ex", "exs", "sh",
  "bash", "zsh", "ps1", "yml", "yaml", "toml", "ini", "cfg", "lock", "txt", "html", "css", "scss", "sass", "less",
  "vue", "svelte", "proto", "graphql", "gql", "tf", "hcl", "dockerfile", "xml", "csv", "ipynb", "svg", "png", "jpg",
  "gif", "gradle", "mod", "sum",
]);
/** At most 14 digits stay in clear (a `YYYYMMDDHHMMSS` migration stamp), as a name or as a prefix. */
const MIGRATION_NUMBER = /^\d{1,14}$/u;
const MIGRATION_PREFIX = /^(\d{3,14})([_-])(.+)$/u;
const isMigrationDir = (parts: string[], i: number) => {
  const d = parts[i - 1]?.toLowerCase();
  return d === "migrations" || d === "migration" || (d === "migrate" && parts[i - 2]?.toLowerCase() === "db");
};

/**
 * The path as the snapshot stores it: every segment hashed with the import salt, except a closed set kept in
 * clear so equal paths still match and the detectors still work. In clear, from fixed lists only: the source
 * roots and the migration directory names, the hot-path file names (and `generated/` above a *manifest*.json),
 * an extension in CLEAR_EXTENSIONS, and in a migration directory an all-digit name or number prefix of at
 * most 14 digits (`0042_`, `20261105062150_`). The depth is kept. Everything else, a sentence, a key-shaped name, a name with `@` or
 * whitespace, is hashed.
 */
export function scrubPath(path: string, hasher: Hasher): string {
  const parts = path.split("/");
  const lastLower = parts[parts.length - 1]!.toLowerCase();
  return parts.map((seg, i) => {
    const lower = seg.toLowerCase();
    const last = i === parts.length - 1;
    if (!last) {
      if (SOURCE_ROOTS.has(lower) || MIGRATION_DIRS.has(lower)) return lower;
      // `generated` stays in clear only as the directory right above a *manifest*.json file.
      if (lower === "generated" && i === parts.length - 2 && GENERATED_MANIFEST.test(lastLower)) return lower;
      return hasher.segment(seg);
    }
    if (KNOWN_FILES.has(lower)) return lower;
    const dot = lower.lastIndexOf(".");
    const ext = dot > 0 && CLEAR_EXTENSIONS.has(lower.slice(dot + 1)) ? lower.slice(dot) : "";
    const stem = ext ? lower.slice(0, dot) : lower;
    if (parts[i - 1]?.toLowerCase() === "generated" && GENERATED_MANIFEST.test(lower)) return `${hasher.segment(stem)}.manifest.json`;
    if (isMigrationDir(parts, i)) {
      if (MIGRATION_NUMBER.test(stem)) return `${stem}${ext}`;
      const m = MIGRATION_PREFIX.exec(stem);
      if (m) return `${m[1]}${m[2]}${hasher.segment(m[3]!)}${ext}`;
    }
    return `${hasher.segment(stem)}${ext}`;
  }).join("/");
}

interface Write { path: string; ts: string; branch: string | null; kind: string; at: LineRef }
interface Commit { sha: string | null; ts: string; at: LineRef }
interface Outcome { key: string | null; reverted: boolean; ts: string; at: LineRef }
interface Usage { tokens: number; ts: string; at: LineRef }

/**
 * One session as an adapter sees it. Only ids, timestamps, repo-relative paths, shas and token counts can
 * be put in: there is no field for message text.
 */
export class SessionBuilder {
  first: string | null = null;
  last: string | null = null;
  cwd: string | null = null;
  remote: string | null = null;
  project: string | null = null;
  branch: string | null = null;
  readonly parents = new Set<string>();
  readonly files = new Set<number>();
  readonly writes: Write[] = [];
  readonly commits: Commit[] = [];
  readonly outcomes: Outcome[] = [];
  readonly usage: Usage[] = [];

  constructor(readonly harness: string, public sourceId: string, private readonly stats: ImportStats) {}

  touch(ts: string, at: LineRef) {
    if (!this.first || ts < this.first) this.first = ts;
    if (!this.last || ts > this.last) this.last = ts;
    this.files.add(at.file);
  }
  private push<T>(list: T[], v: T) {
    if (list.length >= IMPORT_BOUNDS.facts) { this.stats.truncatedFacts++; return; }
    if (++this.stats.heldFacts > IMPORT_BOUNDS.totalFacts) throw new ImportError(`more than ${IMPORT_BOUNDS.totalFacts} facts; import fewer traces at a time`);
    list.push(v);
  }
  /** `raw` is normalized here; a path that cannot be made repo-relative safely is dropped and counted. */
  write(raw: unknown, ts: string, at: LineRef, cwd: string | null = this.cwd, branch: string | null = this.branch, kind = "tool_use") {
    this.touch(ts, at);
    const path = typeof raw === "string" ? repoRelativePath(raw, cwd) : null;
    if (!path) { this.stats.droppedPaths++; return; }
    this.push(this.writes, { path, ts, branch, kind, at });
  }
  commit(sha: string | null, ts: string, at: LineRef): Commit {
    this.touch(ts, at);
    const c = { sha, ts, at };
    this.push(this.commits, c);
    return c;
  }
  outcome(key: string | null, reverted: boolean, ts: string, at: LineRef) {
    this.touch(ts, at);
    this.push(this.outcomes, { key, reverted, ts, at });
  }
  tokens(n: unknown, ts: string, at: LineRef) {
    this.touch(ts, at);
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n <= 0) return;
    this.push(this.usage, { tokens: n, ts, at });
  }
  parent(id: unknown) {
    const p = cleanId(id);
    if (p && p !== this.sourceId && this.parents.size < IMPORT_BOUNDS.parents) this.parents.add(p);
  }
  /**
   * Keeps only what lies at or after `since`: facts before it are dropped (counted), and `first` moves up to
   * it. Used when the import keeps a 120-day window and this session started before it.
   */
  clampFrom(since: string) {
    if (!this.first || this.first >= since) return;
    this.stats.clampedSessions++;
    for (const list of [this.writes, this.commits, this.outcomes, this.usage] as { ts: string }[][]) {
      const keep = list.filter((x) => x.ts >= since);
      this.stats.outOfWindowFacts += list.length - keep.length;
      list.length = 0;
      list.push(...keep);
    }
    this.first = since;
  }
  /** Folds another builder for the same session (a session split across files) into this one. */
  absorb(o: SessionBuilder) {
    if (o.first) this.touch(o.first, { file: [...o.files][0] ?? 0, line: 0 });
    if (o.last) this.touch(o.last, { file: [...o.files][0] ?? 0, line: 0 });
    for (const f of o.files) this.files.add(f);
    this.cwd ??= o.cwd; this.remote ??= o.remote; this.project ??= o.project; this.branch ??= o.branch;
    for (const p of o.parents) this.parent(p);
    for (const w of o.writes) this.push(this.writes, w);
    for (const c of o.commits) this.push(this.commits, c);
    for (const x of o.outcomes) this.push(this.outcomes, x);
    for (const u of o.usage) this.push(this.usage, u);
  }
}

export interface Tenant { orgId: string; workspaceId: string }

export interface MappedSession {
  harness: string;
  sessionId: string;
  sourceSessionId: string;
  sourceParentIds: string[];
  /** The raw project key (repository or directory name) the projectId hashes. */
  sourceProject: string | null;
  /** [eventId, file index, line] for each fact, so the customer can find an alert's evidence. */
  events: [string, number, number][];
  /** The repo-relative source path behind each hashed path, so the customer can read an alert's artifact. */
  paths: Record<string, string>;
}

const SHA = /^[0-9a-f]{7,40}$/iu;
const UNKNOWN = "unknown";
const byTs = <T extends { ts: string }>(xs: T[]) => xs.map((x, i) => [x, i] as const)
  .sort((a, b) => textCompare(a[0].ts, b[0].ts) || a[1] - b[1]).map(([x]) => x);

/**
 * The snapshot line for one session, with every customer id hashed, or null when the session has no
 * timestamp at all. `createdAt` and `lastActivityAt` are the earliest and latest timestamped record, which
 * bound every fact (the M0 loader's createdAt..lastActivityAt rule). The summary and claims stay empty.
 */
export function toSnapshotSession(b: SessionBuilder, hasher: Hasher, tenant: Tenant):
  { line: Record<string, unknown>; session: SnapshotSession; mapped: MappedSession } | null {
  if (!b.first || !b.last) return null;
  const key = b.project ?? projectKey(b.remote, b.cwd);
  const repoKey = projectKey(b.remote, null) ?? key;
  const sessionId = hasher.session(b.harness, b.sourceId);
  // A parent outside the import still names the fleet: its hash is kept, so siblings share a root.
  const parents = [...b.parents].sort(textCompare);
  const events: [string, number, number][] = [];
  const paths: Record<string, string> = {};
  const scrub = (raw: string) => { const h = scrubPath(raw, hasher); paths[h] = raw; return h; };
  const ev = (prefix: string, i: number, at: LineRef) => { const id = `${prefix}${i + 1}`; events.push([id, at.file, at.line]); return id; };
  const line = {
    orgId: hasher.org(tenant.orgId), workspaceId: hasher.workspace(tenant.workspaceId), harness: b.harness, sessionId,
    projectId: key ? hasher.project(key) : null, repo: repoKey ? hasher.repo(repoKey) : null, branch: hasher.branch(b.branch),
    title: null, userId: null, taskRef: null,
    parentSessionIds: parents.map((p) => hasher.session(b.harness, p)),
    createdAt: b.first, lastActivityAt: b.last, shortSummary: null,
    writes: byTs(b.writes).map((w, i) => ({ path: scrub(w.path), ts: w.ts, eventId: ev("w", i, w.at), kind: w.kind, branch: hasher.branch(w.branch) })),
    commits: byTs(b.commits).map((c, i) => ({ sha: c.sha && SHA.test(c.sha) ? c.sha.toLowerCase() : UNKNOWN, ts: c.ts, eventId: ev("c", i, c.at) })),
    outcomes: byTs(b.outcomes).map((o, i) => ({
      key: o.key && SHA.test(o.key) ? o.key.toLowerCase() : UNKNOWN, ts: o.ts, eventId: ev("o", i, o.at), reverted: o.reverted,
    })),
    usage: byTs(b.usage).map((u, i) => ({ ts: u.ts, eventId: ev("u", i, u.at), tokens: u.tokens, costUsdMicros: null })),
    claims: [],
  };
  // The M0 loader's own validation: an import it would refuse is never written.
  const session = parseSession(line, `session ${sessionId}`);
  return { line, session, mapped: { harness: b.harness, sessionId, sourceSessionId: b.sourceId, sourceParentIds: parents, sourceProject: key, events, paths } };
}
