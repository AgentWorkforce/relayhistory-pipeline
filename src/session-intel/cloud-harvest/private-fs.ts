/**
 * Private destinations for harvested data (docs/session-intel-cloud-harvest.md, "Running it"). Every directory and
 * file the harvest or the bench reads or writes must be a real path (no symbolic link at any level below the
 * checked root), owned by this user, and closed to group and others. A path that fails is refused, never repaired:
 * a permissive or linked file may already have been read by someone else, so the operator has to decide.
 */
import { closeSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

const uid = () => process.getuid?.() ?? -1;

function check(path: string, kind: "dir" | "file") {
  const st = lstatSync(path);
  if (st.isSymbolicLink()) throw new Error(`${path} is a symbolic link; harvested data needs a real path`);
  if (kind === "dir" ? !st.isDirectory() : !st.isFile()) throw new Error(`${path} is not a ${kind === "dir" ? "directory" : "regular file"}`);
  if (uid() >= 0 && st.uid !== uid()) throw new Error(`${path} is not owned by this user`);
  if (st.mode & 0o077) throw new Error(`${path} is open to group or others (mode ${(st.mode & 0o777).toString(8)}); make it private first`);
}

/** Creates `dir` (mode 700) if missing, then verifies it is private. */
export function privateDir(dir: string): string {
  const abs = resolve(dir);
  if (!existsSync(abs)) mkdirSync(abs, { recursive: true, mode: 0o700 });
  check(abs, "dir");
  return abs;
}

/** Verifies an existing file is private; a missing file passes (it will be created private). */
export function assertPrivateFile(path: string): void {
  if (existsSync(path) || isDanglingLink(path)) check(path, "file");
}

/** Verifies an input that must exist: a private file, or a private directory and every entry the caller names. */
export function assertPrivateInput(path: string, kind: "dir" | "file"): void {
  if (!existsSync(path) && !isDanglingLink(path)) throw new Error(`${path} does not exist`);
  check(path, kind);
}

const isDanglingLink = (p: string) => { try { return lstatSync(p).isSymbolicLink(); } catch { return false; } };

/** Writes a private file atomically: a fresh temporary file (exclusive create, mode 600), then rename. */
export function writePrivate(path: string, text: string): void {
  assertPrivateFile(path);
  const tmp = `${path}.${process.pid}.tmp`;
  if (existsSync(tmp) || isDanglingLink(tmp)) unlinkSync(tmp);
  writeFileSync(tmp, text, { mode: 0o600, flag: "wx" });
  renameSync(tmp, path);
}

/** Reads a private file. */
export function readPrivate(path: string): string {
  assertPrivateInput(path, "file");
  return readFileSync(path, "utf8");
}

/**
 * Exclusive ownership of a harvest directory: one process at a time, so the request ledger cannot be spent twice.
 * The lock is created exclusively and holds the owner's pid. A lock left by a process that died is never taken
 * over automatically (two processes could each decide it is stale): the run is refused with the pid, and the
 * operator removes the lock after checking that process is gone. The ledger already counts its requests.
 */
export function lockDir(dir: string): () => void {
  const lock = join(dir, "harvest.lock");
  let fd: number;
  try { fd = openSync(lock, "wx", 0o600); } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw e;
    const pid = (() => { try { return readFileSync(lock, "utf8").trim(); } catch { return "?"; } })();
    throw new Error(`${dir} is locked by process ${pid}${alive(Number(pid)) ? "" : " (not running: remove harvest.lock once you have checked)"}`);
  }
  writeFileSync(fd, String(process.pid)); closeSync(fd);
  let released = false;
  const release = () => { if (!released) { released = true; try { if (readFileSync(lock, "utf8") === String(process.pid)) unlinkSync(lock); } catch { /* gone */ } } };
  process.once("exit", release);
  return release;
}

function alive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (e) { return (e as NodeJS.ErrnoException).code === "EPERM"; }
}

/** The real path of `p`, refusing one inside `root` (lexically before creation and really after). */
export function outsideRoot(p: string, root: string): string {
  const inRoot = (x: string) => (x + "/").startsWith(root + "/");
  const abs = resolve(p);
  if (inRoot(abs)) throw new Error("--out must be outside this repository");
  const dir = privateDir(abs);
  if (inRoot(realpathSync(dir)) || inRoot(realpathSync(dirname(dir)))) throw new Error("--out must be outside this repository");
  return dir;
}
