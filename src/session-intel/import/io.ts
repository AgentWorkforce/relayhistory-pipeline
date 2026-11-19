/**
 * Bounded input reading for the trace import: a directory walk that never follows a symlink, and a
 * streaming JSONL reader that skips (and counts) oversize and malformed lines without buffering them.
 */
import { constants } from "node:fs";
import { lstat, open, readdir } from "node:fs/promises";
import { join } from "node:path";
import { IMPORT_BOUNDS, type ImportStats } from "./normalize.ts";
import { textCompare } from "../replay/util.ts";

export class ImportError extends Error {
  constructor(message: string) { super(`import: ${message}`); this.name = "ImportError"; }
}

/** The input bounds (IMPORT_BOUNDS by default); tests pass smaller ones to reach each guard cheaply. */
export interface InputLimits { walkDepth: number; inputFiles: number; inputFileBytes: number; inputLineBytes: number }

/** Every `*.jsonl` regular file at or under `root` (itself already resolved), sorted. Symlinks, FIFOs and dot entries are skipped. */
export async function listInputFiles(root: string, limits: InputLimits = IMPORT_BOUNDS): Promise<string[]> {
  const st = await lstat(root).catch(() => null);
  if (!st) throw new ImportError("--in does not exist");
  if (st.isFile()) return [root];
  if (!st.isDirectory()) throw new ImportError("--in must be a regular file or a directory");
  const found: string[] = [];
  const walk = async (dir: string, depth: number) => {
    if (depth > limits.walkDepth) throw new ImportError(`--in is nested deeper than ${limits.walkDepth} directories`);
    const entries = await readdir(dir, { withFileTypes: true });
    entries.sort((a, b) => textCompare(a.name, b.name));
    for (const e of entries) {
      if (e.name.startsWith(".")) continue;
      const path = join(dir, e.name);
      if (e.isDirectory()) await walk(path, depth + 1);
      else if (e.isFile() && e.name.endsWith(".jsonl")) {
        if (found.length >= limits.inputFiles) throw new ImportError(`--in holds more than ${limits.inputFiles} .jsonl files`);
        found.push(path);
      }
      // Symlinks, sockets and the like are never followed or read.
    }
  };
  await walk(root, 0);
  return found;
}

/**
 * Calls `onRecord` with each line's parsed JSON value and its 1-based line number. A line over
 * IMPORT_BOUNDS.inputLineBytes is skipped without being held in memory; a line that is not JSON is skipped.
 * Both are counted. Neither the line nor its content ever reaches an error message.
 */
export async function readJsonl(path: string, stats: ImportStats, onRecord: (value: unknown, line: number) => void, limits: InputLimits = IMPORT_BOUNDS): Promise<void> {
  // O_NOFOLLOW and fstat on the open handle: a file swapped for a symlink after the walk is refused.
  // O_NONBLOCK: opening a FIFO returns at once (and fstat refuses it) instead of waiting for a writer.
  let handle;
  try { handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); } catch { throw new ImportError("an input file could not be opened (symlinks are refused)"); }
  const st = await handle.stat();
  if (!st.isFile()) { await handle.close(); throw new ImportError("an input is not a regular file"); }
  if (st.size > limits.inputFileBytes) { await handle.close(); throw new ImportError(`an input file exceeds ${limits.inputFileBytes} bytes`); }
  stats.files++;
  let parts: Buffer[] = [];
  let size = 0;
  let skipping = false;
  let line = 0;
  const finish = () => {
    line++;
    if (skipping) { stats.oversizeLines++; skipping = false; parts = []; size = 0; return; }
    const buf = parts.length === 1 ? parts[0]! : Buffer.concat(parts);
    parts = []; size = 0;
    const text = buf.toString("utf8");
    if (!text.trim()) return;
    stats.lines++;
    let value: unknown;
    try { value = JSON.parse(text); } catch { stats.malformedLines++; return; }
    onRecord(value, line);
  };
  for await (const chunk of handle.createReadStream({ highWaterMark: 1024 * 1024, end: limits.inputFileBytes - 1 }) as AsyncIterable<Buffer>) {
    let start = 0;
    while (start <= chunk.length) {
      const nl = chunk.indexOf(0x0a, start);
      const end = nl < 0 ? chunk.length : nl;
      if (!skipping) {
        size += end - start;
        if (size > limits.inputLineBytes) { skipping = true; parts = []; }
        else if (end > start) parts.push(chunk.subarray(start, end));
      }
      if (nl < 0) break;
      finish();
      start = nl + 1;
    }
  }
  if (size > 0 || skipping || parts.length) finish();
}
