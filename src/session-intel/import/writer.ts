/**
 * Writes an imported snapshot in the M0 format (docs/session-intel-replay.md, "Snapshot format"): the
 * directory is built under a temporary sibling name and renamed into place, so a reader never sees a
 * partial snapshot. The private mapping file is written atomically next to it, never inside it.
 */
import { link, lstat, mkdir, readdir, rename, rm, unlink, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { ImportError } from "./io.ts";
import { IMPORT_BOUNDS, type ImportStats, type MappedSession } from "./normalize.ts";
import { MANIFEST_FILE, parseManifest, SESSIONS_FILE, SNAPSHOT_FORMAT, type SnapshotManifest } from "../replay/snapshot.ts";
import { sha256Hex } from "../replay/util.ts";

export const MAPPING_FORMAT = "session-intel-import-mapping/1";
export const DELETE_BY_MAX_MS = 30 * 24 * 60 * 60 * 1_000;
/** One replay tick past the last fact, so the driver's last tick can see it (a fact is visible when ts < now). */
export const WINDOW_TAIL_MS = 120_000;

/** `deleteBy` must be after `now` and at most 30 days out. */
export function checkDeleteBy(deleteBy: string, now: Date): void {
  const ms = Date.parse(deleteBy);
  if (!(ms > now.getTime())) throw new ImportError("--delete-by must be in the future");
  if (ms - now.getTime() > DELETE_BY_MAX_MS) throw new ImportError("--delete-by must be at most 30 days from now");
}

/** The mapping file's path: `<out>.mapping.json`, beside the snapshot directory. */
export const mappingPath = (out: string) => join(dirname(out), `${basename(out)}.mapping.json`);

/**
 * Creates `path` with `data`, mode 0600, never replacing anything: the data goes to a temporary file in the
 * same directory, which is hard-linked to `path` (link(2) fails if `path` exists, symlink or not) and then
 * unlinked. A reader sees either no file or the whole file.
 */
export async function writeNew(path: string, data: string): Promise<void> {
  const tmp = join(dirname(path), `.${basename(path)}.${process.pid}-${Math.random().toString(16).slice(2)}.tmp`);
  await writeFile(tmp, data, { mode: 0o600, flag: "wx" });
  const exists = () => new ImportError("the mapping file beside --out already exists; it is never replaced (delete it with its snapshot, or choose a new --out)");
  try {
    await link(tmp, path);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "EEXIST") throw exists();
    // A filesystem without hard links (exFAT, some network mounts): still create-only (O_EXCL), not atomic.
    if (code !== "ENOTSUP" && code !== "EPERM" && code !== "EOPNOTSUPP" && code !== "ENOSYS") throw err;
    try { await writeFile(path, data, { mode: 0o600, flag: "wx" }); } catch (e) {
      if ((e as NodeJS.ErrnoException).code === "EEXIST") throw exists();
      throw e;
    }
  } finally {
    await unlink(tmp).catch(() => {});
  }
}

export interface WriteInput {
  out: string;
  lines: string[];
  window: { since: string; until: string };
  deleteBy: string;
  now: Date;
  mapping: { inputFiles: string[]; sessions: MappedSession[]; projects: Record<string, string>; tenant: { orgId: string; workspaceId: string }; stats: ImportStats };
}

export async function writeImport(w: WriteInput): Promise<{ manifest: SnapshotManifest; mapping: string }> {
  const body = w.lines.length ? `${w.lines.join("\n")}\n` : "";
  if (Buffer.byteLength(body) > IMPORT_BOUNDS.fileBytes) throw new ImportError(`${SESSIONS_FILE} would exceed ${IMPORT_BOUNDS.fileBytes} bytes; import fewer traces at once`);
  const manifest = parseManifest({
    format: SNAPSHOT_FORMAT, createdAt: w.now.toISOString(), deleteBy: w.deleteBy, window: w.window,
    files: { [SESSIONS_FILE]: sha256Hex(body) },
  });
  const existing = await lstat(w.out).catch(() => null);
  if (existing && !existing.isDirectory()) throw new ImportError("--out exists and is not a directory");
  if (existing && (await readdir(w.out)).length > 0) throw new ImportError("--out exists and is not empty; choose a new directory");
  await mkdir(dirname(w.out), { recursive: true });
  // The private reverse map first, create-only: an earlier one is never replaced, and a snapshot never exists
  // without its map. If the snapshot then fails, the map just created is removed.
  const mapping = mappingPath(w.out);
  await writeNew(mapping, `${JSON.stringify({
    format: MAPPING_FORMAT,
    warning: "PRIVATE to the customer: maps the hashed ids and paths in the snapshot back to the source traces. Never share it with the snapshot. Delete it with the snapshot by deleteBy.",
    createdAt: w.now.toISOString(), deleteBy: w.deleteBy, manifestSha256: sha256Hex(`${JSON.stringify(manifest, null, 2)}\n`),
    tenant: w.mapping.tenant, inputFiles: w.mapping.inputFiles, projects: w.mapping.projects, stats: w.mapping.stats, sessions: w.mapping.sessions,
  })}\n`);
  const tmp = join(dirname(w.out), `.${basename(w.out)}.import-${process.pid}-${Math.random().toString(16).slice(2)}`);
  try {
    await mkdir(tmp, { mode: 0o700 });
    try {
      await writeFile(join(tmp, SESSIONS_FILE), body, { mode: 0o600 });
      await writeFile(join(tmp, MANIFEST_FILE), `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
      // rename(2) replaces an empty directory, and fails if one appeared with content meanwhile.
      await rename(tmp, w.out);
    } catch (err) {
      await rm(tmp, { recursive: true, force: true });
      throw err;
    }
  } catch (err) {
    await unlink(mapping).catch(() => {});
    throw err;
  }
  return { manifest, mapping };
}
