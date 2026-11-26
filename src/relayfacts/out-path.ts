/**
 * Output paths for the relayfacts tools. Their output holds quotes and metadata from real sessions, so it must
 * never land inside this (public) repository and must stay private to the user running the tool:
 * - a path is checked before anything is created (its nearest existing ancestor is resolved through symlinks),
 *   so a refused path leaves nothing behind;
 * - a directory the tool creates is 0700; an existing one must already be the user's own and closed to group and
 *   others (it is never re-permissioned); every file is 0600, opened without following a link, and must be a
 *   regular file when it already exists; child names are validated (no separators, no `..`, no raw identifiers:
 *   callers pass opaque names).
 * - Limit: on a volume mounted with ownership ignored (an external APFS or exFAT drive with "Ignore ownership"),
 *   modes are stored but every user sees the files as their own: keep such output on a single-user machine.
 */
import { chmodSync, closeSync, constants, existsSync, fchmodSync, lstatSync, mkdirSync, openSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { UsageError } from "./errors.ts";

export const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../..", import.meta.url))));
const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;

/** The path as it will resolve once created: the nearest existing ancestor's real path plus the rest. */
function prospective(path: string): string {
  let head = resolve(path);
  const tail: string[] = [];
  while (!existsSync(head)) {
    const parent = dirname(head);
    if (parent === head) break;
    tail.unshift(basename(head));
    head = parent;
  }
  return join(realpathSync(head), ...tail);
}

const inside = (real: string, root: string) => real === root || real.startsWith(root + sep);

function checkName(name: string): string {
  if (!NAME.test(name) || name.includes("..")) throw new UsageError(`refused output name: ${JSON.stringify(name.slice(0, 40))}`);
  return name;
}

/**
 * A directory this tool created is set to 0700. An existing one is never changed (it may be a home or shared
 * directory): it must already be the user's own and closed to group and others, or it is refused.
 */
function secureDir(path: string, created: boolean): void {
  const st = lstatSync(path);
  if (st.isSymbolicLink() || !st.isDirectory()) throw new UsageError("output path is not a directory");
  if (typeof process.getuid === "function" && st.uid !== process.getuid()) throw new UsageError("output directory is not owned by this user");
  if (created) chmodSync(path, 0o700);
  else if ((st.mode & 0o077) !== 0) throw new UsageError("existing output directory is open to group or others: use a private (0700) directory");
}

/** A directory outside the repository, created (0700) if needed; throws, creating nothing, for a path inside it. */
export function outsideDir(path: string | undefined, root: string = REPO_ROOT): string {
  if (!path) throw new UsageError("--out is required");
  if (inside(prospective(path), root)) throw new UsageError("--out must be outside this repository");
  const created = !existsSync(path);
  mkdirSync(path, { recursive: true, mode: 0o700 });
  const real = realpathSync(path);
  if (inside(real, root)) throw new UsageError("--out must be outside this repository");
  secureDir(real, created);
  return real;
}

/** A private output directory: every read and write goes through validated child names. */
export class PrivateDir {
  private constructor(readonly path: string) {}

  static open(path: string | undefined, root: string = REPO_ROOT): PrivateDir {
    return new PrivateDir(outsideDir(path, root));
  }

  child(name: string): PrivateDir {
    const path = join(this.path, checkName(name));
    const created = !existsSync(path);
    if (created) mkdirSync(path, { mode: 0o700 });
    secureDir(path, created);
    return new PrivateDir(path);
  }

  /** The child's path; an existing child must be a regular file (never a link or directory). */
  file(name: string): string {
    const path = join(this.path, checkName(name));
    let st;
    try { st = lstatSync(path); } catch { return path; }
    if (!st.isFile()) throw new UsageError("output child is not a regular file");
    return path;
  }

  has(name: string): boolean {
    try { return lstatSync(join(this.path, checkName(name))).isFile(); } catch { return false; }
  }

  write(name: string, data: string): void {
    const fd = openSync(this.file(name), constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC | constants.O_NOFOLLOW, 0o600);
    try { fchmodSync(fd, 0o600); writeFileSync(fd, data); } finally { closeSync(fd); }
  }

  /** Append one line, retried on an interrupted system call (a busy external drive can raise EINTR). */
  append(name: string, line: string): void {
    for (let attempt = 0; ; attempt++) {
      try {
        const fd = openSync(this.file(name), constants.O_WRONLY | constants.O_CREAT | constants.O_APPEND | constants.O_NOFOLLOW, 0o600);
        try { fchmodSync(fd, 0o600); writeFileSync(fd, line); } finally { closeSync(fd); }
        return;
      } catch (error) {
        if ((error as { code?: string }).code !== "EINTR" || attempt >= 20) throw error;
      }
    }
  }

  read(name: string): string {
    const fd = openSync(this.file(name), constants.O_RDONLY | constants.O_NOFOLLOW);
    try { return readFileSync(fd, "utf8"); } finally { closeSync(fd); }
  }

  /** Regular files whose names end with `suffix`, sorted. */
  list(suffix: string): string[] {
    return readdirSync(this.path).filter((n) => NAME.test(n) && n.endsWith(suffix) && this.has(n)).sort();
  }
}

/** A private file outside the repository: its directory is opened as a PrivateDir. */
export function writeOutsideFile(path: string | undefined, data: string, root: string = REPO_ROOT): void {
  if (!path) throw new UsageError("--out is required");
  PrivateDir.open(dirname(resolve(path)), root).write(basename(path), data);
}
