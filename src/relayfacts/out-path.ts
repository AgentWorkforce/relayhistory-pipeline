/**
 * Output paths for the relayfacts tools. Their output holds quotes and metadata from real sessions, so it must
 * never land inside this (public) repository and must stay private to the user running the tool:
 * - a path is checked before anything is created (its nearest existing ancestor is resolved through symlinks),
 *   so a refused path leaves nothing behind;
 * - a directory the tool creates is 0700; an existing one must already be the user's own and closed to group and
 *   others (it is never re-permissioned); child names are validated (no separators, no `..`, no raw identifiers:
 *   callers pass opaque names);
 * - files are written atomically (a fresh 0600 temporary renamed into place: old content survives a failure and
 *   a present file is always complete); reads and appends open without following links and check the opened
 *   descriptor (a regular file, the user's own, 0600-closed, one link).
 * - Limit: on a volume mounted with ownership ignored (an external APFS or exFAT drive with "Ignore ownership"),
 *   modes are stored but every user sees the files as their own: keep such output on a single-user machine.
 */
import { randomBytes } from "node:crypto";
import { chmodSync, closeSync, constants, existsSync, fchmodSync, fstatSync, fsyncSync, ftruncateSync, linkSync, lstatSync, mkdirSync, openSync, readSync, readdirSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from "node:fs";
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
  if (!NAME.test(name) || name.includes("..")) throw new UsageError("refused output name");
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

/** The opened descriptor is a regular file, the user's own, closed to group and others, with one link. */
function checkOpened(fd: number): void {
  const st = fstatSync(fd);
  if (!st.isFile()) throw new UsageError("private file is not a regular file");
  if (typeof process.getuid === "function" && st.uid !== process.getuid()) throw new UsageError("private file is not owned by this user");
  if ((st.mode & 0o077) !== 0) throw new UsageError("private file is open to group or others");
  if (st.nlink !== 1) throw new UsageError("private file has other names (hard links)");
}

/** Read a private file's bytes (no links followed; the opened descriptor is checked; at most `maxBytes`). */
export function readPrivateBytes(path: string, maxBytes = 512 * 1024 * 1024): Buffer {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    checkOpened(fd);
    if (fstatSync(fd).size > maxBytes) throw new UsageError("private file over its size bound");
    return readFileSync(fd);
  } finally { closeSync(fd); }
}

/** Read a private file (an input such as --features, --gold or --forge): no links, checked descriptor. */
export const readPrivateFile = (path: string): string => readPrivateBytes(path).toString("utf8");

/** The offset just past the last newline in the file's first `size` bytes (0 when there is none). */
function lastNewlineEnd(fd: number, size: number): number {
  const chunk = Buffer.alloc(64 * 1024);
  for (let end = size; end > 0;) {
    const start = Math.max(0, end - chunk.length);
    const n = readSync(fd, chunk, 0, end - start, start);
    const i = chunk.subarray(0, n).lastIndexOf(0x0a);
    if (i >= 0) return start + i + 1;
    end = start;
  }
  return 0;
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

  private path_(name: string): string {
    return join(this.path, checkName(name));
  }

  /**
   * A complete, safe file of that name exists: written by write() or create(), both atomic, so a present file is a
   * finished one. A link, a directory, or a file open to others or not the user's own is refused, never skipped.
   */
  has(name: string): boolean {
    let st;
    try { st = lstatSync(this.path_(name)); } catch { return false; }
    if (!st.isFile()) throw new UsageError("output child is not a regular file");
    if (typeof process.getuid === "function" && st.uid !== process.getuid()) throw new UsageError("private file is not owned by this user");
    if ((st.mode & 0o077) !== 0 || st.nlink !== 1) throw new UsageError("private file is open to group or others, or has other names");
    return true;
  }

  /** Names of the regular files here, sorted (in-flight temporaries excluded). */
  list(suffix: string): string[] {
    return readdirSync(this.path).filter((n) => NAME.test(n) && !n.startsWith(".tmp-") && n.endsWith(suffix) && this.has(n)).sort();
  }

  /** A fresh, complete, synced 0600 temporary holding `data`; removed again if anything fails. */
  private staged(data: string): string {
    const tmp = join(this.path, `.tmp-${randomBytes(12).toString("hex")}`);
    const fd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    try {
      fchmodSync(fd, 0o600);
      writeFileSync(fd, data);
      fsyncSync(fd);
    } catch (error) {
      closeSync(fd);
      rmSync(tmp, { force: true });
      throw error;
    }
    closeSync(fd);
    return tmp;
  }

  /**
   * Write atomically: a staged temporary is renamed over the name, so a failed or interrupted write leaves the old
   * file (or none), never a partial one, and an existing file is never opened or re-permissioned. The target, if
   * present, must be a regular file (a link or directory is refused).
   */
  write(name: string, data: string): void {
    const target = this.path_(name);
    try { if (!lstatSync(target).isFile()) throw new UsageError("output child is not a regular file"); } catch (error) { if (error instanceof UsageError) throw error; }
    const tmp = this.staged(data);
    try { renameSync(tmp, target); } catch (error) { rmSync(tmp, { force: true }); throw error; }
  }

  /**
   * Create a file only if the name is free, atomically and exclusively: a staged temporary is hard-linked to the
   * name (which fails if it exists, so a concurrent creator fails) and the temporary removed. A present file is
   * always complete.
   */
  create(name: string, data: string): void {
    const tmp = this.staged(data);
    try { linkSync(tmp, this.path_(name)); } finally { rmSync(tmp, { force: true }); }
  }

  /**
   * Append one line to a private log (retried on EINTR); the opened file is checked before writing. A fragment torn
   * by an interrupted earlier append (bytes after the last newline) is truncated away first, so every complete line
   * in a log is whole: only the torn line is lost on resume, and any malformed complete line is real corruption.
   */
  append(name: string, line: string): void {
    for (let attempt = 0; ; attempt++) {
      try {
        const fd = openSync(this.path_(name), constants.O_RDWR | constants.O_CREAT | constants.O_APPEND | constants.O_NOFOLLOW, 0o600);
        try {
          checkOpened(fd);
          const size = fstatSync(fd).size;
          if (size > 0) {
            const last = Buffer.alloc(1);
            readSync(fd, last, 0, 1, size - 1);
            if (last[0] !== 0x0a) ftruncateSync(fd, lastNewlineEnd(fd, size));
          }
          writeFileSync(fd, line.endsWith("\n") ? line : `${line}\n`);
        } finally { closeSync(fd); }
        return;
      } catch (error) {
        if ((error as { code?: string }).code !== "EINTR" || attempt >= 20) throw error;
      }
    }
  }

  read(name: string): string {
    return readPrivateFile(this.path_(name));
  }
}

/** A private file outside the repository: its directory is opened as a PrivateDir. */
export function writeOutsideFile(path: string | undefined, data: string, root: string = REPO_ROOT): void {
  if (!path) throw new UsageError("--out is required");
  PrivateDir.open(dirname(resolve(path)), root).write(basename(path), data);
}
