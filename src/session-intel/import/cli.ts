/**
 * The enterprise trace import CLI (docs/session-intel-import.md). Offline only: no network, no database,
 * no credentials and no environment reads. Converts Claude Code, Codex or generic JSONL traces into an M0
 * replay snapshot with hashed ids and no message text, plus a private mapping file beside it.
 *
 *   bun run src/session-intel/import/cli.ts --format claude-code|codex|generic --in <path> --out <dir> \
 *     --salt-stdin --delete-by <iso> [--org <id> --workspace <id>]
 *
 * The salt comes from stdin (`op read 'op://...' | bun run ... --salt-stdin ...`), so it is never on disk,
 * in argv or in the shell history. `--salt-file <path>` is a fallback; exactly one of the two is required.
 */
import { constants, existsSync, realpathSync } from "node:fs";
import { open } from "node:fs/promises";
import { basename, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { ADAPTERS, FORMATS, SessionSet, type ImportFormat } from "./adapters.ts";
import { ImportError, listInputFiles } from "./io.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, toSnapshotSession, type ImportStats, type MappedSession, type SessionBuilder, type Tenant } from "./normalize.ts";
import { checkDeleteBy, WINDOW_TAIL_MS, writeImport } from "./writer.ts";
import { isInside, realPathOf } from "../replay/cli.ts";
import { SnapshotError } from "../replay/snapshot.ts";
import { isoTime, textCompare } from "../replay/util.ts";

/** The salt source for --salt-stdin: process.stdin by default; tests inject a stream. */
export interface SaltInput extends AsyncIterable<Buffer | string> { isTTY?: boolean }
export interface CliIo {
  out(line: string): void;
  err(line: string): void;
  now?: Date;
  stdin?: SaltInput;
  /** The repository the --out and --salt-file guards protect; tests point it at a temporary directory. */
  repoRoot?: string;
}
const defaultIo: CliIo = { out: (l) => console.log(l), err: (l) => console.error(l) };

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
const TENANT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;
export const SALT_MIN = 16;

export const SALT_MAX = 1_024;
const SALT_FILE_MAX_BYTES = 4_096;

/**
 * Reads the import salt from a file, so it never lands in argv, the process list or the shell history.
 * The file must be a regular file (opened O_NOFOLLOW, so a symlink is refused), readable by its owner only
 * (mode & 0o077 == 0), outside this repository, and hold 16 to 1024 characters once trimmed. No message
 * ever carries the salt or any part of it.
 */
export async function readSaltFile(path: string, repoRoot = REPO_ROOT): Promise<string> {
  const abs = resolve(path);
  if (isInside(repoRoot, abs)) throw new ImportError("--salt-file is inside the repository; keep it outside, where it cannot be committed");
  let handle;
  // O_NONBLOCK: a FIFO opens at once (and is refused below) instead of waiting for a writer.
  try { handle = await open(abs, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    throw new ImportError(code === "ELOOP" || code === "EMLINK" ? "--salt-file must not be a symlink" : "--salt-file could not be opened");
  }
  try {
    const st = await handle.stat();
    if (!st.isFile()) throw new ImportError("--salt-file must be a regular file");
    if ((st.mode & 0o077) !== 0) throw new ImportError("--salt-file is readable or writable by group or others; chmod 600 it");
    if (st.size > SALT_FILE_MAX_BYTES) throw new ImportError(`--salt-file exceeds ${SALT_FILE_MAX_BYTES} bytes`);
    // A bounded read: a file that grew after the stat still yields at most the bound (plus one byte to notice).
    const buf = Buffer.alloc(SALT_FILE_MAX_BYTES + 1);
    const { bytesRead } = await handle.read(buf, 0, buf.length, 0);
    if (bytesRead > SALT_FILE_MAX_BYTES) throw new ImportError(`--salt-file exceeds ${SALT_FILE_MAX_BYTES} bytes`);
    const salt = buf.subarray(0, bytesRead).toString("utf8").trim();
    buf.fill(0);
    if (salt.length < SALT_MIN || salt.length > SALT_MAX) throw new ImportError(`--salt-file must hold ${SALT_MIN} to ${SALT_MAX} characters`);
    return salt;
  } finally {
    await handle.close();
  }
}

/**
 * Reads the import salt from stdin: refused when stdin is a terminal (a typed salt would echo and could be
 * logged by the terminal), read up to 4 KiB, trimmed, 16 to 1024 characters. No message carries the salt.
 */
export async function readSaltStdin(input: SaltInput): Promise<string> {
  if (input.isTTY) throw new ImportError("--salt-stdin needs the salt piped in (for example from `op read`), not a terminal");
  const parts: Buffer[] = [];
  let size = 0;
  for await (const chunk of input) {
    const b = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
    size += b.length;
    if (size > SALT_FILE_MAX_BYTES) { for (const x of parts) x.fill(0); throw new ImportError(`--salt-stdin exceeds ${SALT_FILE_MAX_BYTES} bytes`); }
    parts.push(Buffer.from(b));
  }
  const buf = Buffer.concat(parts);
  for (const x of parts) x.fill(0);
  const salt = buf.toString("utf8").trim();
  buf.fill(0);
  if (salt.length < SALT_MIN || salt.length > SALT_MAX) throw new ImportError(`--salt-stdin must carry ${SALT_MIN} to ${SALT_MAX} characters`);
  return salt;
}

/** A future timestamp past the clock by more than this is an outlier, not clock skew. */
export const FUTURE_SKEW_MS = 24 * 60 * 60 * 1_000;

/**
 * Keeps the import inside the M0 loader's 120-day window without aborting on one outlier. When the
 * sessions span more, the window kept is the (latest) 120 days in which the most sessions end. A session
 * ending outside it is dropped; one starting before it loses its earlier facts. Both are counted.
 */
export function keepWindow(builders: SessionBuilder[], stats: ImportStats): SessionBuilder[] {
  const timed = builders.filter((b) => b.first && b.last);
  if (!timed.length) return [];
  // A loop, not Math.min(...spread): up to 100,000 sessions would exceed the engine's argument limit.
  let minFirst = Infinity, maxLast = -Infinity;
  for (const b of timed) { minFirst = Math.min(minFirst, Date.parse(b.first!)); maxLast = Math.max(maxLast, Date.parse(b.last!)); }
  if (maxLast + WINDOW_TAIL_MS - minFirst <= IMPORT_BOUNDS.windowMs) return timed;
  const span = IMPORT_BOUNDS.windowMs - WINDOW_TAIL_MS;
  const lasts = timed.map((b) => Date.parse(b.last!)).sort((a, b) => a - b);
  let best = { count: 0, end: lasts[0]! };
  for (let i = 0, j = 0; j < lasts.length; j++) {
    while (lasts[j]! - lasts[i]! > span) i++;
    if (j - i + 1 >= best.count) best = { count: j - i + 1, end: lasts[j]! };
  }
  const sinceMs = best.end - span;
  const kept: SessionBuilder[] = [];
  for (const b of timed) {
    const last = Date.parse(b.last!);
    if (last < sinceMs || last > best.end) { stats.outOfWindowSessions++; continue; }
    b.clampFrom(new Date(sinceMs).toISOString());
    kept.push(b);
  }
  return kept;
}

export interface Row { line: string; harness: string; sessionId: string; createdAt: string; lastActivityAt: string; mapped: MappedSession }

/** The snapshot lines, sorted; refuses an import in which two source sessions hash to the same id. */
export function buildRows(builders: Iterable<SessionBuilder>, hasher: Hasher, tenant: Tenant, stats: ImportStats): Row[] {
  const rows: Row[] = [];
  const seen = new Set<string>();
  for (const b of builders) {
    const r = toSnapshotSession(b, hasher, tenant);
    if (!r) continue;
    const line = JSON.stringify(r.line);
    if (Buffer.byteLength(line) > IMPORT_BOUNDS.lineBytes) { stats.oversizeSessions++; continue; }
    const k = `${r.session.harness}\u0000${r.session.sessionId}`;
    if (seen.has(k)) throw new ImportError("two source sessions hash to the same id; use another salt");
    seen.add(k);
    rows.push({ line, harness: r.session.harness, sessionId: r.session.sessionId, createdAt: r.session.createdAt, lastActivityAt: r.session.lastActivityAt, mapped: r.mapped });
    stats.writes += r.session.writes.length; stats.commits += r.session.commits.length;
    stats.outcomes += r.session.outcomes.length; stats.usage += r.session.usage.length;
  }
  rows.sort((a, b) => textCompare(a.harness, b.harness) || textCompare(a.sessionId, b.sessionId));
  stats.sessions = rows.length;
  return rows;
}

const OPTIONS = {
  format: { type: "string" }, in: { type: "string" }, out: { type: "string" }, "salt-file": { type: "string" }, "salt-stdin": { type: "boolean" },
  "delete-by": { type: "string" }, org: { type: "string" }, workspace: { type: "string" },
} as const;

export async function main(argv: string[], io: CliIo = defaultIo): Promise<number> {
  let values;
  try {
    ({ values } = parseArgs({ args: argv, strict: true, allowPositionals: false, options: OPTIONS }));
  } catch {
    // parseArgs names the offending argument, which may be a salt pasted by mistake: never echo argv.
    throw new ImportError("invalid arguments (unknown option, missing value or a stray positional argument); see docs/session-intel-import.md. Arguments are never echoed.");
  }
  const repoRoot = io.repoRoot ?? REPO_ROOT;
  const format = values.format as ImportFormat | undefined;
  if (!format || !FORMATS.includes(format)) throw new ImportError(`--format must be one of ${FORMATS.join(", ")}`);
  if (!values.in) throw new ImportError("--in is required");
  if (!values.out) throw new ImportError("--out is required");
  const fromStdin = values["salt-stdin"] === true;
  if (fromStdin === (values["salt-file"] !== undefined))
    throw new ImportError("exactly one of --salt-stdin (preferred: pipe the salt from `op read`) or --salt-file is required");
  if (!values["delete-by"]) throw new ImportError("--delete-by is required (at most 30 days out)");
  const now = io.now ?? new Date();
  let deleteBy: string;
  try { deleteBy = isoTime(values["delete-by"], "--delete-by"); } catch { throw new ImportError("--delete-by must be an ISO-8601 timestamp with a zone"); }
  checkDeleteBy(deleteBy, now);
  const tenant = { orgId: values.org ?? "org_import", workspaceId: values.workspace ?? "ws_import" };
  if (!TENANT_ID.test(tenant.orgId) || !TENANT_ID.test(tenant.workspaceId)) throw new ImportError(`--org and --workspace must match ${TENANT_ID.source}`);

  if (!existsSync(resolve(values.in))) throw new ImportError("--in does not exist");
  const input = realpathSync(resolve(values.in));
  const out = realPathOf(values.out);
  // Outputs carry paths and the mapping carries raw ids: neither belongs in this repository (M0's guard).
  if (isInside(repoRoot, out)) throw new ImportError("--out is inside the repository; imported snapshots belong outside it");
  if (isInside(input, out) || isInside(out, input)) throw new ImportError("--out and --in must not contain each other");
  const salt = fromStdin ? await readSaltStdin(io.stdin ?? process.stdin) : await readSaltFile(values["salt-file"]!, repoRoot);

  const stats = emptyStats();
  const files = await listInputFiles(input);
  if (!files.length) throw new ImportError("--in holds no .jsonl files");
  const set = new SessionSet(stats, new Date(now.getTime() + FUTURE_SKEW_MS).toISOString());
  for (let i = 0; i < files.length; i++) await ADAPTERS[format](files[i]!, i, set);

  const hasher = new Hasher(salt);
  const rows = buildRows(keepWindow([...set.byKey.values()], stats), hasher, tenant, stats);
  if (!rows.length) throw new ImportError("no session with a valid timestamp was found");
  const since = rows.reduce((m, r) => r.createdAt < m ? r.createdAt : m, rows[0]!.createdAt);
  const last = rows.reduce((m, r) => r.lastActivityAt > m ? r.lastActivityAt : m, rows[0]!.lastActivityAt);
  const until = new Date(Date.parse(last) + WINDOW_TAIL_MS).toISOString();

  const projects: Record<string, string> = {};
  for (const r of rows) if (r.mapped.sourceProject) projects[hasher.project(r.mapped.sourceProject)] = r.mapped.sourceProject;
  const { mapping } = await writeImport({
    out, lines: rows.map((r) => r.line), window: { since, until }, deleteBy, now,
    mapping: { inputFiles: files.map((f) => relative(input, f) || basename(f)), sessions: rows.map((r) => r.mapped), projects, tenant, stats },
  });
  // Every dropped or clamped count gets a warning, not only the window's.
  if (stats.untimedRecords || stats.futureTimestamps || stats.outOfWindowSessions || stats.outOfWindowFacts || stats.clampedSessions)
    io.err(`import: warning: dropped ${stats.untimedRecords} records without a strict ISO-8601 timestamp and ${stats.futureTimestamps} stamped in the future; `
      + `kept a 120-day window, dropping ${stats.outOfWindowSessions} sessions and ${stats.outOfWindowFacts} facts outside it and clamping ${stats.clampedSessions} sessions to its start`);
  // Counts, then the resolved real paths of the snapshot and the mapping (no ids, source paths or text).
  io.out(`import: ${JSON.stringify(stats)}`);
  io.out(`import: snapshot -> ${out}; private mapping -> ${mapping}`);
  return 0;
}

/**
 * The one line printed for a failed import. An ImportError's (or the M0 loader's SnapshotError's) message is
 * built to be safe; any other error
 * (a Node filesystem error names the full source path in its message) prints only its code.
 */
export function failureMessage(err: unknown): string {
  // The M0 loader's own validation errors name fields, bounds and hashed ids only.
  if (err instanceof ImportError || err instanceof SnapshotError) return `session-intel import: ${err.message}`;
  const code = (err as NodeJS.ErrnoException)?.code;
  return `session-intel import: failed (${typeof code === "string" && /^[A-Z0-9_]{1,32}$/u.test(code) ? code : "internal error"}); details withheld, as they can name source paths`;
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (err) {
    console.error(failureMessage(err));
    process.exitCode = 1;
  }
}
