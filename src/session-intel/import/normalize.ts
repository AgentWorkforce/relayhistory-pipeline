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
}

export const emptyStats = (): ImportStats => ({
  files: 0, lines: 0, malformedLines: 0, oversizeLines: 0, untimedRecords: 0, droppedPaths: 0, truncatedFacts: 0,
  oversizeSessions: 0, sessions: 0, writes: 0, commits: 0, outcomes: 0, usage: 0, heldFacts: 0,
  futureTimestamps: 0, outOfWindowSessions: 0, outOfWindowFacts: 0,
});

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

/** The customer's repository or project name: the last segment of a remote URL, or the cwd's basename. */
export function projectKey(remote: string | null, cwd: string | null): string | null {
  const fromRemote = remote ? trimSlash(slashes(remote.trim())).replace(/\.git$/iu, "").split(/[/:]/u).filter(Boolean).pop() : undefined;
  if (fromRemote) return fromRemote.toLowerCase();
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
  session(harness: ImportHarness, id: string) { return `s_${this.h(`session\u0000${harness}`, id, 32)}`; }
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
/** Lockfiles, the migration journal and package.json, which the detectors treat as hot paths, by exact (lowercased) name. */
const KNOWN_FILES: ReadonlySet<string> = new Set([
  "bun.lock", "package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "cargo.lock", "poetry.lock",
  "composer.lock", "gemfile.lock", "_journal.json", "package.json",
]);
const EXTENSION = /^[a-z0-9]{1,10}$/u;
const isMigrationDir = (parts: string[], i: number) => {
  const d = parts[i - 1]?.toLowerCase();
  return d === "migrations" || d === "migration" || (d === "migrate" && parts[i - 2]?.toLowerCase() === "db");
};

/**
 * The path as the snapshot stores it: every segment hashed with the import salt, except a closed set kept in
 * clear so equal paths still match and the detectors still work. In clear: the source roots and the migration
 * directory names (fixed lists), the hot-path file names (a fixed list), a file's extension when it is 1 to 10
 * of [a-z0-9] after lowercasing, and in a migration directory the all-digit number prefix (`0042_`,
 * `20261105062150_`). The depth is kept. Everything else, a sentence, a key-shaped name, a name with `@` or
 * whitespace, is hashed.
 */
export function scrubPath(path: string, hasher: Hasher): string {
  const parts = path.split("/");
  return parts.map((seg, i) => {
    const lower = seg.toLowerCase();
    const last = i === parts.length - 1;
    if (!last) return SOURCE_ROOTS.has(lower) || MIGRATION_DIRS.has(lower) ? lower : hasher.segment(seg);
    if (KNOWN_FILES.has(lower)) return lower;
    const dot = lower.lastIndexOf(".");
    const ext = dot > 0 && EXTENSION.test(lower.slice(dot + 1)) ? lower.slice(dot) : "";
    const stem = ext ? lower.slice(0, dot) : lower;
    if (isMigrationDir(parts, i)) {
      if (/^\d+$/u.test(stem)) return `${stem}${ext}`;
      const m = /^(\d{3,})([_-])(.+)$/u.exec(stem);
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

  constructor(readonly harness: ImportHarness, public sourceId: string, private readonly stats: ImportStats) {}

  touch(ts: string, at: LineRef) {
    if (!this.first || ts < this.first) this.first = ts;
    if (!this.last || ts > this.last) this.last = ts;
    this.files.add(at.file);
  }
  private push<T>(list: T[], v: T) {
    if (list.length >= IMPORT_BOUNDS.facts) { this.stats.truncatedFacts++; return; }
    if (++this.stats.heldFacts > IMPORT_BOUNDS.totalFacts) throw new Error(`import: more than ${IMPORT_BOUNDS.totalFacts} facts; import fewer traces at a time`);
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
  harness: ImportHarness;
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
