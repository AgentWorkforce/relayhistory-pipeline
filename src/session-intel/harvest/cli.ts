/**
 * The Session Intelligence harvest CLI (docs/session-intel-harvest.md): builds a representative M0 replay
 * snapshot of one workspace from the pipeline's own read API, with a coverage receipt. It reads session data
 * with a credential, so running it against any real install needs Khaliq's own OK for the stated scope, each time.
 *
 *   op read 'op://<vault>/<item>/salt' | op run -- bun run src/session-intel/harvest/cli.ts \
 *     --read-api <https url> --workspace <id> --since <iso> --until <iso> --out <dir> \
 *     --salt-stdin --delete-by <iso> [--require-list-backend union,supabase] [--org <label>] [--concurrency 2] \
 *     [--max-requests 20000] [--partial]
 *
 * --require-list-backend is the default for a History API harvest: every frame page must say which store
 * served it (X-Relayhistory-List-Served-By), and any page outside the set exits 4 (EXIT_UNVERIFIED).
 *
 * The bearer token comes only from SESSION_INTEL_READ_TOKEN (injected by `op run`); it is removed from this
 * process's environment once read and is never written, logged or echoed. The salt comes only from stdin.
 * Ids, repo, branch, project, org, workspace and path segments are hashed with the M4 import's normalizer,
 * and the snapshot is written by the M4 writer: the 0600 mapping beside it, a deleteBy at most 14 days out.
 */
import { lstat, mkdir, readdir, rename, truncate, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { HarvestError, LIST_BACKENDS, parseReadApi, ReadApiClient, readToken, redact, RequestCapReached, TOKEN_ENV } from "./client.ts";
import {
  buildReceipt, frameAttribution, newState, progress, readState, RECEIPT_FILE, rowKey, runFacts, runFrame, runOutput, STATE_FILE, STATE_FORMAT,
  StateLog, type HarvestParams, type HarvestState,
} from "./harvest.ts";
import { buildRows, readSaltStdin, type SaltInput } from "../import/cli.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, ImportError, SessionBuilder } from "../import/normalize.ts";
import { checkDeleteBy, writeImport } from "../import/writer.ts";
import { isInside, realPathOf } from "../replay/cli.ts";
import { SnapshotError } from "../replay/snapshot.ts";
import { isoTime } from "../replay/util.ts";

export const HARVEST_MAPPING_FORMAT = "session-intel-harvest-mapping/1";
export const DELETE_BY_MAX_DAYS = 14;
/** Exit code when the request cap stopped the harvest: the state is kept and a rerun resumes. */
export const EXIT_CAPPED = 3;
/** Exit code when --require-list-backend is set and a frame page was not verified: the receipt and snapshot are still written. */
export const EXIT_UNVERIFIED = 4;
const TENANT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u;
const REPO_ROOT = realpathSync(fileURLToPath(new URL("../../..", import.meta.url)));
const DAY_MS = 86_400_000;

export interface HarvestIo {
  out(line: string): void;
  err(line: string): void;
  /** The wall clock (tests inject it). */
  now?: () => Date;
  stdin?: SaltInput;
  /** The environment the token is read from; process.env by default (and the token is then deleted from it). */
  env?: Record<string, string | undefined>;
  repoRoot?: string;
  fetch?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
}
const defaultIo: HarvestIo = { out: (l) => console.log(l), err: (l) => console.error(l) };

const OPTIONS = {
  "read-api": { type: "string" }, workspace: { type: "string" }, since: { type: "string" }, until: { type: "string" },
  out: { type: "string" }, "salt-stdin": { type: "boolean" }, "delete-by": { type: "string" }, org: { type: "string" },
  concurrency: { type: "string" }, "max-requests": { type: "string" }, partial: { type: "boolean" }, "require-list-backend": { type: "string" },
} as const;

function int(raw: string | undefined, name: string, fallback: number, min: number, max: number): number {
  if (raw === undefined) return fallback;
  if (!/^\d{1,9}$/u.test(raw)) throw new HarvestError(`${name} must be an integer from ${min} to ${max}`);
  const n = Number(raw);
  if (n < min || n > max) throw new HarvestError(`${name} must be an integer from ${min} to ${max}`);
  return n;
}
/** --require-list-backend: a comma list of History API list stores (LIST_BACKENDS), sorted and deduplicated; null when absent. */
function listBackends(raw: string | undefined): string[] | null {
  if (raw === undefined) return null;
  const values = [...new Set(raw.split(",").map((v) => v.trim()).filter(Boolean))].sort();
  if (!values.length || values.some((v) => !(LIST_BACKENDS as readonly string[]).includes(v)))
    throw new HarvestError(`--require-list-backend must be a comma list of ${LIST_BACKENDS.join(", ")} (for example union,supabase)`);
  return values;
}
const strictIso = (raw: string | undefined, name: string): string => {
  if (!raw) throw new HarvestError(`${name} is required (ISO-8601 with a zone)`);
  try { return isoTime(raw, name); } catch { throw new HarvestError(`${name} must be an ISO-8601 timestamp with a zone`); }
};

/** --out: a new or empty directory (a fresh harvest), or one holding only the state log (a resume). */
async function openState(out: string, params: HarvestParams, now: Date): Promise<{ state: HarvestState; log: StateLog }> {
  const statePath = join(out, STATE_FILE);
  const st = await lstat(out).catch(() => null);
  if (st && (st.isSymbolicLink() || !st.isDirectory())) throw new HarvestError("--out exists and is not a directory");
  if (!st) await mkdir(out, { recursive: true, mode: 0o700 });
  const entries = await readdir(out);
  if (entries.length === 0) {
    const log = await StateLog.create(statePath);
    const state = newState(params, now.toISOString());
    await log.write({ t: "start", format: STATE_FORMAT, params, startedAt: state.startedAt, requests: 0 });
    return { state, log };
  }
  if (entries.length !== 1 || entries[0] !== STATE_FILE) throw new HarvestError("--out is not empty and holds no harvest state; choose a new directory");
  const sst = await lstat(statePath);
  if (!sst.isFile()) throw new HarvestError(`${STATE_FILE} must be a regular file`);
  if ((sst.mode & 0o077) !== 0) throw new HarvestError(`${STATE_FILE} is readable or writable by group or others; refused`);
  const { goodBytes, torn, ...state } = await readState(statePath);
  if (JSON.stringify(state.params) !== JSON.stringify(params))
    throw new HarvestError("--out holds a harvest with other --read-api, --workspace, --since, --until or --delete-by; resume with the same ones, or choose a new --out");
  if (now.toISOString() > params.deleteBy) throw new HarvestError("the harvest state is past its --delete-by; delete --out instead of resuming");
  // A torn last line (a crash mid-write) is cut off, so the next line does not land on it.
  if (torn) await truncate(statePath, goodBytes);
  return { state, log: await StateLog.append(statePath) };
}

export async function main(argv: string[], io: HarvestIo = defaultIo): Promise<number> {
  let values;
  try {
    ({ values } = parseArgs({ args: argv, strict: true, allowPositionals: false, options: OPTIONS }));
  } catch {
    // parseArgs names the offending argument, which may be a token or salt pasted by mistake: never echo argv.
    throw new HarvestError("invalid arguments (unknown option, missing value or a stray positional argument); see docs/session-intel-harvest.md. Arguments are never echoed.");
  }
  const clock = io.now ?? (() => new Date());
  const now = clock();
  const repoRoot = io.repoRoot ?? REPO_ROOT;
  const base = parseReadApi(values["read-api"]);
  const workspace = values.workspace;
  if (!workspace || !TENANT_ID.test(workspace)) throw new HarvestError(`--workspace is required and must match ${TENANT_ID.source}`);
  const org = values.org ?? "org_readapi";
  if (!TENANT_ID.test(org)) throw new HarvestError(`--org must match ${TENANT_ID.source}`);
  const since = strictIso(values.since, "--since"), until = strictIso(values.until, "--until");
  const span = Date.parse(until) - Date.parse(since);
  if (!(span > 0)) throw new HarvestError("--until must be after --since");
  if (span > IMPORT_BOUNDS.windowMs) throw new HarvestError("--since to --until must span at most 120 days (the M0 snapshot window)");
  if (Date.parse(until) > now.getTime() + DAY_MS) throw new HarvestError("--until must not be more than a day in the future");
  if (!values["delete-by"]) throw new HarvestError(`--delete-by is required (at most ${DELETE_BY_MAX_DAYS} days out)`);
  const deleteBy = strictIso(values["delete-by"], "--delete-by");
  checkDeleteBy(deleteBy, now, DELETE_BY_MAX_DAYS);
  const concurrency = int(values.concurrency, "--concurrency", 2, 1, 8);
  const maxRequests = int(values["max-requests"], "--max-requests", 20_000, 1, 1_000_000);
  const requireListBackend = listBackends(values["require-list-backend"]);
  if (values["salt-stdin"] !== true) throw new HarvestError("--salt-stdin is required: pipe the salt from `op read`");
  if (!values.out) throw new HarvestError("--out is required");
  const out = realPathOf(values.out);
  // The snapshot, its mapping and the state carry session data: none of it belongs in this repository.
  if (isInside(repoRoot, out)) throw new HarvestError("--out is inside the repository; harvested snapshots belong outside it");

  const env = io.env ?? process.env;
  const token = readToken(env);
  // Out of this process's environment once read, so nothing loaded later can see it.
  if (env === process.env) delete process.env[TOKEN_ENV];
  try {
    return await harvest({ io, clock, base, workspace, org, since, until, deleteBy, concurrency, maxRequests, out, token, partial: values.partial === true, requireListBackend });
  } catch (err) {
    // Every message leaving here is redacted against the token, whatever built it.
    if (err instanceof Error) err.message = redact(err.message, token);
    throw err;
  }
}

interface Run {
  io: HarvestIo; clock: () => Date; base: URL; workspace: string; org: string; since: string; until: string; deleteBy: string;
  concurrency: number; maxRequests: number; out: string; token: string; partial: boolean; requireListBackend: string[] | null;
}

async function harvest(r: Run): Promise<number> {
  const salt = await readSaltStdin(r.io.stdin ?? process.stdin);
  const params: HarvestParams = { readApi: r.base.href, workspace: r.workspace, since: r.since, until: r.until, deleteBy: r.deleteBy, requireListBackend: r.requireListBackend };
  const { state, log } = await openState(r.out, params, r.clock());
  const client = new ReadApiClient({
    base: r.base, token: r.token, workspace: r.workspace, maxRequests: r.maxRequests, spent: state.requests, initial: state.stats,
    concurrency: r.concurrency, fetch: r.io.fetch, sleep: r.io.sleep, clock: () => r.clock().getTime(),
  });
  let capped = false;
  try {
    state.runs++;
    await log.write({ t: "run", startedAt: r.clock().toISOString(), ...progress(client) });
    const opts = { client, state, log, concurrency: r.concurrency };
    await runFrame(opts);
    await runOutput(opts);
    await runFacts(opts);
  } catch (err) {
    if (!(err instanceof RequestCapReached)) throw err;
    capped = true;
  } finally {
    // The run's final counts, on success, the cap or an error, so a resume continues them exactly. A run
    // killed before this line is detected on resume, and its counts are then reported as partial.
    await log.write({ t: "runEnd", endedAt: r.clock().toISOString(), ...progress(client) }).catch(() => {});
    await log.close();
  }
  if (capped && !r.partial) {
    r.io.err(`harvest: the request cap (--max-requests ${r.maxRequests}) was reached after ${client.stats.requests} requests; `
      + "the state is kept in --out: rerun the same command (with a higher --max-requests) to resume, or add --partial to write what was read");
    return EXIT_CAPPED;
  }

  const hasher = new Hasher(salt);
  const tenant = { orgId: r.org, workspaceId: r.workspace };
  const stats = emptyStats();
  const lastMs = Date.parse(r.until) - 1;
  const last = new Date(lastMs).toISOString();
  const createdAt = new Map<string, string>();
  const sourceEventIds: Record<string, string[]> = {};
  const builders: SessionBuilder[] = [];
  for (const row of state.frame.rows) {
    const b = new SessionBuilder(row.source, row.sessionId, stats);
    // Clamped into the window: the replay steps over [since, until), and every fact lies inside it.
    const first = row.firstTs > r.since ? row.firstTs : r.since;
    b.touch(first, { file: 0, line: 0 });
    b.touch(row.lastTs < last ? row.lastTs : last, { file: 0, line: 0 });
    const facts = state.facts.get(rowKey(row));
    b.project = row.projectId ?? facts?.eventProjectId ?? null;
    b.remote = row.repo;
    b.branch = row.gitBranch;
    for (const p of row.parentSessionIds) b.parent(p);
    const ids: string[] = [];
    for (const w of facts?.writes ?? []) {
      ids.push(w.eventId);
      b.write(w.path, w.ts, { file: 0, line: ids.length }, null, row.gitBranch, w.kind);
    }
    if (ids.length) sourceEventIds[hasher.session(row.source, row.sessionId)] = ids;
    createdAt.set(rowKey(row), b.first!);
    builders.push(b);
  }
  const rows = buildRows(builders, hasher, tenant, stats);

  const factsRead = state.facts.size;
  const failed = [...state.facts.values()].filter((f) => f.failure).length;
  const notes: string[] = [
    "commits, outcomes and usage are not exposed per session by the allowed read routes: superseded-work and duplicate-cost evidence cannot fire on this snapshot",
  ];
  if (capped) notes.push(`request cap reached (--max-requests ${r.maxRequests}): the frame is ${state.frame.done ? "exhausted" : "NOT exhausted"}; ${state.frame.rows.length - factsRead} sessions' events were not read`);
  if (failed) notes.push(`${failed} sessions' events were not served (reasons in facts.sessionsWithoutEvents); they are in the snapshot without writes`);
  if (stats.oversizeSessions) notes.push(`${stats.oversizeSessions} sessions exceeded the snapshot line bound and were dropped`);
  const censored = [...state.facts.values()].filter((f) => f.writesCensored);
  if (censored.length) notes.push(`${censored.length} sessions' writes were cut at ${IMPORT_BOUNDS.facts} per session (facts.censoredByReason); `
    + `${censored.filter((f) => f.droppedWrites === null).length} of them stopped with pages unread, so their drop count is unknown`);
  if (state.statsPartial) notes.push(`request counts are partial: ${state.statsPartial}`);
  const sources = frameAttribution(state.frame.pageSources, state.params.requireListBackend);
  if (sources.gated && sources.offendingPages)
    notes.push(`frame_backend_unverified: ${sources.offendingPages} of ${state.frame.pages} frame pages were not served by ${sources.required!.join(" or ")} `
      + "(X-Relayhistory-List-Served-By missing, unknown or outside --require-list-backend; frame.sources has the per-provider pages)");
  if (sources.attribution === "unverified" && !sources.gated)
    notes.push("source attribution is unverified: no --require-list-backend, and not every frame page carried the 96Bulls read API's marker; this receipt is not representative proof");
  const incompleteReasons = [
    ...(capped ? ["request_cap"] : []),
    ...(!state.frame.done ? ["frame_not_exhausted"] : []),
    ...(factsRead < state.frame.rows.length ? ["facts_not_read"] : []),
    ...(failed ? ["events_not_served"] : []),
    ...(stats.oversizeSessions ? ["oversize_sessions"] : []),
    ...(censored.length ? ["writes_censored"] : []),
    // Only a gated harvest is held incomplete by attribution; a record-only one says `unverified` instead.
    ...(sources.gated && sources.attribution !== "verified" ? ["frame_backend_unverified"] : []),
  ];
  const complete = incompleteReasons.length === 0;
  const finishedAt = r.clock().toISOString();
  const receipt = buildReceipt({
    state, stats: client.stats, finishedAt, workspaceHash: hasher.workspace(r.workspace), complete, notes, incompleteReasons, createdAt,
    snapshot: { sessions: rows.length, writes: stats.writes, droppedPaths: stats.droppedPaths, oversizeSessions: stats.oversizeSessions },
  });
  const projects: Record<string, string> = {};
  for (const row of rows) if (row.mapped.sourceProject) projects[hasher.project(row.mapped.sourceProject)] = row.mapped.sourceProject;

  // The writer wants --out empty: the state log steps aside, and comes back if the write fails.
  const statePath = join(r.out, STATE_FILE);
  const aside = join(dirname(r.out), `.${basename(r.out)}.${STATE_FILE}.${process.pid}`);
  await rename(statePath, aside);
  let mapping: string;
  try {
    ({ mapping } = await writeImport({
      out: r.out, lines: rows.map((x) => x.line), window: { since: r.since, until: r.until }, deleteBy: r.deleteBy, now: r.clock(),
      extraFiles: { [RECEIPT_FILE]: `${JSON.stringify(receipt, null, 2)}\n` },
      mapping: {
        format: HARVEST_MAPPING_FORMAT,
        warning: "PRIVATE: maps the hashed ids and paths in the snapshot back to the read API's sessions. Never share it with the snapshot. Delete it with the snapshot by deleteBy.",
        inputFiles: ["GET /v1/sessions/:sessionId/events"], sessions: rows.map((x) => x.mapped), projects, tenant,
        stats: { sessions: rows.length, writes: stats.writes, droppedPaths: stats.droppedPaths, requests: client.stats.requests },
        // Each mapped event [id, 0, n] is the n-th id (1-based) in sourceEventIds of its session.
        extra: { readApi: r.base.href, sourceEventIds },
      },
    }));
  } catch (err) {
    await mkdir(r.out, { recursive: true, mode: 0o700 }).catch(() => {});
    await rename(aside, statePath).catch(() => {});
    throw err;
  }
  await unlink(aside).catch(() => {});
  const frame = receipt.frame as { sessions: number; bySource: Record<string, number> };
  r.io.out(`harvest: ${JSON.stringify({ complete, incompleteReasons, sourceAttribution: sources.attribution, sessions: rows.length, frameSessions: frame.sessions, bySource: frame.bySource, writes: stats.writes, droppedPaths: stats.droppedPaths, requests: client.stats.requests })}`);
  r.io.out(`harvest: snapshot -> ${r.out}; private mapping -> ${mapping}`);
  // Strict mode fails closed: the diagnostic receipt and snapshot are written, and the exit code says the
  // frame's source could not be verified (the replay refuses the snapshot unless told otherwise).
  if (sources.gated && sources.attribution !== "verified") {
    r.io.err(`harvest: frame_backend_unverified: ${sources.offendingPages} of ${state.frame.pages} frame pages were not served by ${sources.required!.join(" or ")}; the snapshot is not representative`);
    return EXIT_UNVERIFIED;
  }
  return 0;
}

/** The one line printed for a failed harvest: a safe message (already redacted), or an error code alone. */
export function failureMessage(err: unknown, token?: string): string {
  if (err instanceof HarvestError || err instanceof ImportError || err instanceof SnapshotError) return redact(`session-intel harvest: ${err.message}`, token);
  const code = (err as NodeJS.ErrnoException)?.code;
  return `session-intel harvest: failed (${typeof code === "string" && /^[A-Z0-9_]{1,32}$/u.test(code) ? code : "internal error"}); details withheld`;
}

if (import.meta.main) {
  const token = process.env[TOKEN_ENV];
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (err) {
    console.error(failureMessage(err, token));
    process.exitCode = 1;
  }
}
