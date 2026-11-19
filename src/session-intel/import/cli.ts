/**
 * The enterprise trace import CLI (docs/session-intel-import.md). Offline only: no network, no database,
 * no credentials and no environment reads. Converts Claude Code, Codex or generic JSONL traces into an M0
 * replay snapshot with hashed ids and no message text, plus a private mapping file beside it.
 *
 *   bun run src/session-intel/import/cli.ts --format claude-code|codex|generic --in <path> --out <dir> \
 *     --salt <s> --delete-by <iso> [--org <id> --workspace <id>]
 */
import { existsSync, realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { ADAPTERS, FORMATS, SessionSet, type ImportFormat } from "./adapters.ts";
import { ImportError, listInputFiles } from "./io.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, toSnapshotSession, type MappedSession } from "./normalize.ts";
import { checkDeleteBy, WINDOW_TAIL_MS, writeImport } from "./writer.ts";
import { isoTime, textCompare } from "../replay/util.ts";

export interface CliIo { out(line: string): void; err(line: string): void; now?: Date }
const defaultIo: CliIo = { out: (l) => console.log(l), err: (l) => console.error(l) };

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
const TENANT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;
export const SALT_MIN = 16;

/** The real path of `p`: the nearest existing ancestor is realpath'd and the rest joined, so a symlink cannot hide where it points. */
export function realResolve(p: string): string {
  let cur = resolve(p);
  const rest: string[] = [];
  while (!existsSync(cur)) {
    const parent = dirname(cur);
    if (parent === cur) break;
    rest.unshift(basename(cur));
    cur = parent;
  }
  return join(realpathSync(cur), ...rest);
}

/** True when `child` is `parent` or under it. Outside means rel is "..", starts with ".." + sep, or is absolute. */
export function isInside(child: string, parent: string): boolean {
  const rel = relative(parent, child);
  return !(rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel));
}

export async function main(argv: string[], io: CliIo = defaultIo): Promise<number> {
  const { values } = parseArgs({
    args: argv, strict: true, allowPositionals: false,
    options: {
      format: { type: "string" }, in: { type: "string" }, out: { type: "string" }, salt: { type: "string" },
      "delete-by": { type: "string" }, org: { type: "string" }, workspace: { type: "string" },
    },
  });
  const format = values.format as ImportFormat | undefined;
  if (!format || !FORMATS.includes(format)) throw new ImportError(`--format must be one of ${FORMATS.join(", ")}`);
  if (!values.in) throw new ImportError("--in is required");
  if (!values.out) throw new ImportError("--out is required");
  const salt = values.salt;
  if (!salt || salt.length < SALT_MIN || salt.length > 1_024) throw new ImportError(`--salt is required: ${SALT_MIN} to 1024 characters, kept private`);
  if (!values["delete-by"]) throw new ImportError("--delete-by is required (at most 30 days out)");
  const now = io.now ?? new Date();
  let deleteBy: string;
  try { deleteBy = isoTime(values["delete-by"], "--delete-by"); } catch { throw new ImportError("--delete-by must be an ISO-8601 timestamp with a zone"); }
  checkDeleteBy(deleteBy, now);
  const tenant = { orgId: values.org ?? "org_import", workspaceId: values.workspace ?? "ws_import" };
  if (!TENANT_ID.test(tenant.orgId) || !TENANT_ID.test(tenant.workspaceId)) throw new ImportError(`--org and --workspace must match ${TENANT_ID.source}`);

  if (!existsSync(resolve(values.in))) throw new ImportError("--in does not exist");
  const input = realpathSync(resolve(values.in));
  const out = realResolve(values.out);
  // Outputs carry paths and the mapping carries raw ids: neither belongs in this repository.
  if (isInside(out, REPO_ROOT)) throw new ImportError("--out is inside the repository; imported snapshots belong outside it");
  if (isInside(out, input) || isInside(input, out)) throw new ImportError("--out and --in must not contain each other");

  const stats = emptyStats();
  const files = await listInputFiles(input);
  if (!files.length) throw new ImportError("--in holds no .jsonl files");
  const set = new SessionSet(stats);
  for (let i = 0; i < files.length; i++) await ADAPTERS[format](files[i]!, i, set);

  const hasher = new Hasher(salt);
  const rows: { line: string; harness: string; sessionId: string; createdAt: string; lastActivityAt: string; mapped: MappedSession }[] = [];
  const seen = new Set<string>();
  for (const b of set.byKey.values()) {
    const r = toSnapshotSession(b, hasher, tenant);
    if (!r) continue;
    const line = JSON.stringify(r.line);
    if (Buffer.byteLength(line) > IMPORT_BOUNDS.lineBytes) { stats.oversizeSessions++; continue; }
    const k = `${r.session.harness}\u0000${r.session.sessionId}`;
    if (seen.has(k)) throw new ImportError("two source sessions hash to the same id; use another --salt");
    seen.add(k);
    rows.push({ line, harness: r.session.harness, sessionId: r.session.sessionId, createdAt: r.session.createdAt, lastActivityAt: r.session.lastActivityAt, mapped: r.mapped });
    stats.writes += r.session.writes.length; stats.commits += r.session.commits.length;
    stats.outcomes += r.session.outcomes.length; stats.usage += r.session.usage.length;
  }
  if (!rows.length) throw new ImportError("no session with a valid timestamp was found");
  rows.sort((a, b) => textCompare(a.harness, b.harness) || textCompare(a.sessionId, b.sessionId));
  stats.sessions = rows.length;

  const since = rows.reduce((m, r) => r.createdAt < m ? r.createdAt : m, rows[0]!.createdAt);
  const last = rows.reduce((m, r) => r.lastActivityAt > m ? r.lastActivityAt : m, rows[0]!.lastActivityAt);
  const until = new Date(Date.parse(last) + WINDOW_TAIL_MS).toISOString();
  if (Date.parse(until) - Date.parse(since) > IMPORT_BOUNDS.windowMs) throw new ImportError("the traces span more than 120 days; import a shorter period at a time");

  const projects: Record<string, string> = {};
  for (const r of rows) if (r.mapped.sourceProject) projects[hasher.project(r.mapped.sourceProject)] = r.mapped.sourceProject;
  const { mapping } = await writeImport({
    out, lines: rows.map((r) => r.line), window: { since, until }, deleteBy, now,
    mapping: { inputFiles: files.map((f) => relative(input, f) || basename(f)), sessions: rows.map((r) => r.mapped), projects, stats },
  });
  // Counts only: no ids, paths or text on stdout.
  io.out(`import: ${JSON.stringify(stats)}`);
  io.out(`import: snapshot -> ${out}; private mapping -> ${mapping}`);
  return 0;
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (err) {
    console.error(`session-intel import: ${(err as Error).message}`);
    process.exitCode = 1;
  }
}
