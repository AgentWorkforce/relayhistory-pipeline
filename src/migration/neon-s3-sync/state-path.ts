/**
 * Where the state database lives, in BOTH S3X_OBLIGATIONS modes (#309 M-C rev 2: ON uses FRESH state only).
 *
 * OFF: the configured S3X_STATE path is the SQLite file, exactly as before, unless that path holds an obligations
 * POINTER, which OFF refuses (obligations_state_in_use). Unflagged (both modes): a symlink or non-regular S3X_STATE,
 * a path naming a sibling / pointer temp file, and retained leftovers (a sibling, its -wal / -shm, a pointer temp)
 * refuse (obligations_state_conflict).
 *
 * ON:
 *  - an ABSENT configured path is bootstrapped. A new sibling database `<base>.obligations.sqlite` is created
 *    exclusively (0600) in the same directory, its schema and a random state_id are committed, the WAL is checkpointed
 *    COMPLETELY and the sole bootstrap connection CLOSED, then the sibling is fsynced. A complete, canonical, versioned
 *    JSON pointer is written to `<base>.pointer.tmp` (0600, exclusive) and fsynced, published with link() (which never
 *    replaces an existing pathname), and the directory is fsynced. Any failure refuses, with no external effect.
 *  - an existing POINTER is validated strictly (bounded size, exact canonical bytes so duplicate or extra keys refuse,
 *    the fixed relative sibling name, regular non-symlink files owned by this user with no group/other bits, the same
 *    parent directory and filesystem) and the sibling's state_id must equal the pointer's.
 *  - an existing SQLite file (or anything else) at the configured path is REFUSED: converting existing state is a
 *    separately reviewed offline contract, never automatic.
 *  - leftovers are QUARANTINED, never adopted, deleted or reset by guess: a sibling without a pointer, a stray
 *    pointer temp file, or any collision refuses with every byte preserved.
 *
 * The pre-obligation code (main 341dd5a9) opening the configured path finds the non-SQLite pointer and fails at its
 * first pragma ("file is not a database"): its eraser makes no S3 or database call before that open; its exporter
 * may first make only a read-only list/head preflight (no write or delete). These filesystem guarantees are stated for the
 * tested local storage only (not network filesystems).
 */
import { closeSync, fsyncSync, linkSync, lstatSync, openSync, readFileSync, readSync, unlinkSync, writeSync } from "node:fs";
import { basename, dirname, join } from "node:path";

export const POINTER_KIND = "s3x-obligations-pointer";
export const POINTER_MAX_BYTES = 1024;
const SQLITE_MAGIC = "SQLite format 3\u0000";

const refuse = (code: string, msg: string): never => {
  throw Object.assign(new Error(msg), { code });
};

export function siblingName(path: string): string {
  return `${basename(path)}.obligations.sqlite`;
}
export function pointerBytes(path: string, stateId: string): string {
  return `${JSON.stringify({ v: 1, kind: POINTER_KIND, db: siblingName(path), state_id: stateId })}\n`;
}

function lstatOrNull(p: string) {
  try {
    return lstatSync(p);
  } catch (e) {
    if ((e as { code?: string }).code === "ENOENT") return null;
    throw e;
  }
}
function fsyncPath(p: string, dir = false) {
  const fd = openSync(p, dir ? "r" : "r+");
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}
function headBytes(p: string, n: number): Buffer {
  const fd = openSync(p, "r");
  try {
    const b = Buffer.alloc(n);
    const got = readSync(fd, b, 0, n, 0);
    return b.subarray(0, got);
  } finally {
    closeSync(fd);
  }
}
function checkPrivateRegular(p: string, what: string, parentDev: number) {
  const st = lstatSync(p);
  if (st.isSymbolicLink() || !st.isFile()) refuse("obligations_state_conflict", `${what} is not a regular file`);
  if ((st.mode & 0o077) !== 0) refuse("obligations_state_conflict", `${what} is accessible to group/other`);
  if (typeof process.getuid === "function" && st.uid !== process.getuid()) refuse("obligations_state_conflict", `${what} is not owned by this user`);
  if (st.dev !== parentDev) refuse("obligations_state_conflict", `${what} is on another filesystem`);
}

/** The parsed, validated pointer at `path` (throws on anything not exactly canonical). */
export function readPointer(path: string): { stateId: string; sibling: string } {
  const parent = dirname(path);
  const pst = lstatSync(parent);
  if (!pst.isDirectory()) refuse("obligations_state_conflict", "the state directory is not a directory");
  checkPrivateRegular(path, "the obligations pointer", pst.dev);
  const st = lstatSync(path);
  if (st.size > POINTER_MAX_BYTES) refuse("obligations_pointer_invalid", "the obligations pointer is too large");
  const text = readFileSync(path, "utf8");
  let obj: unknown;
  try {
    obj = JSON.parse(text);
  } catch {
    refuse("obligations_pointer_invalid", "the obligations pointer is not JSON");
  }
  const o = obj as { v?: unknown; kind?: unknown; db?: unknown; state_id?: unknown };
  if (!o || typeof o !== "object" || o.v !== 1 || o.kind !== POINTER_KIND || typeof o.state_id !== "string" || !/^[0-9a-f-]{36}$/.test(o.state_id))
    refuse("obligations_pointer_invalid", "the obligations pointer has an unexpected shape or version");
  if (o.db !== siblingName(path)) refuse("obligations_pointer_invalid", "the obligations pointer names another database");
  // Exact canonical bytes: duplicate keys, extra keys, reordering or whitespace all refuse.
  if (text !== pointerBytes(path, o.state_id as string)) refuse("obligations_pointer_invalid", "the obligations pointer is not canonical");
  const sibling = join(parent, siblingName(path));
  if (!lstatOrNull(sibling)) refuse("obligations_state_conflict", "the obligations database named by the pointer is missing");
  checkPrivateRegular(sibling, "the obligations database", pst.dev);
  return { stateId: o.state_id as string, sibling };
}

export type Resolved = { db: string; pointer: { stateId: string } | null; bootstrap: boolean };

/**
 * Resolve the database path for `path` in the given mode. `bootstrap` (ON, absent path) returns the sibling to be
 * initialized by the caller, who then calls publishPointer; nothing is published here.
 */
export function resolveStatePath(path: string, obligations: boolean): Resolved {
  // An in-memory database is never a supported state (two processes would see two databases): SyncState allows it
  // only through an explicit test option, before this resolution.
  if (path === ":memory:") refuse("state_memory_refused", "S3X_STATE=:memory: is not a supported state");
  // The ON sibling is never a configured state path itself (either mode): its pointer is.
  if (basename(path).endsWith(".obligations.sqlite") || basename(path).endsWith(".pointer.tmp"))
    refuse("obligations_state_conflict", "S3X_STATE names an obligations sibling / temp file, not a state path");
  const parent = dirname(path);
  const sibling = join(parent, siblingName(path));
  const tmp = join(parent, `${basename(path)}.pointer.tmp`);
  const st = lstatOrNull(path);
  // Retained obligation state (a sibling or a pointer temp file) is QUARANTINED in BOTH modes: OFF must not create a
  // new database beside it, and an existing legacy SQLite path with a retained sibling refuses too.
  const retained = !!(lstatOrNull(sibling) || lstatOrNull(`${sibling}-wal`) || lstatOrNull(`${sibling}-shm`) || lstatOrNull(tmp));
  if (st === null) {
    if (retained) refuse("obligations_state_conflict", "retained obligation state without a pointer: quarantined (reviewed recovery only)");
    if (!obligations) return { db: path, pointer: null, bootstrap: false };
    return { db: sibling, pointer: null, bootstrap: true };
  }
  if (st.isSymbolicLink()) refuse("obligations_state_conflict", "the state path is a symlink");
  if (!st.isFile()) refuse("obligations_state_conflict", "the state path is not a regular file");
  const head = st.size === 0 ? Buffer.alloc(0) : headBytes(path, 16);
  const isSqlite = st.size === 0 || head.toString("latin1") === SQLITE_MAGIC;
  if (isSqlite) {
    if (retained) refuse("obligations_state_conflict", "a SQLite state with retained obligation state beside it: quarantined (reviewed recovery only)");
    if (obligations) refuse("obligations_requires_fresh_state", "S3X_OBLIGATIONS on needs a fresh (absent) S3X_STATE; existing state is converted only by a reviewed offline contract");
    return { db: path, pointer: null, bootstrap: false };
  }
  // A pointer (or anything else that is not SQLite).
  if (!obligations) refuse("obligations_state_in_use", "this state path holds an obligations pointer: S3X_OBLIGATIONS cannot be off for it");
  if (lstatOrNull(tmp)) refuse("obligations_state_conflict", "a stray pointer temp file is present: quarantined");
  const p = readPointer(path);
  return { db: p.sibling, pointer: { stateId: p.stateId }, bootstrap: false };
}

/**
 * Publish the pointer for a freshly bootstrapped, CLOSED sibling: fsync the sibling, write the canonical pointer to an
 * exclusive temp file (0600) and fsync it, link() it into place (never replacing an existing pathname), fsync the
 * directory, then unlink the temp file.
 */
export function publishPointer(path: string, stateId: string) {
  const parent = dirname(path);
  const sibling = join(parent, siblingName(path));
  const tmp = join(parent, `${basename(path)}.pointer.tmp`);
  fsyncPath(sibling);
  const fd = openSync(tmp, "wx", 0o600);
  try {
    writeSync(fd, pointerBytes(path, stateId));
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  linkSync(tmp, path); // EEXIST if anything appeared at the configured path: refuse, nothing replaced
  fsyncPath(parent, true);
  unlinkSync(tmp);
  fsyncPath(parent, true);
}
