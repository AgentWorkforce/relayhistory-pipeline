/**
 * The relayfacts backfill CLI (docs/proposals/relayfacts-features.md, "The onboarding backfill").
 * Reads production through the read API's read-only role and raw store, analyses sessions locally,
 * and writes a benchmark corpus outside this repository. Never writes to any database or bucket.
 *
 * Environment (inject with `op run` or `op read`; never on disk): PGHOST PGPORT PGDATABASE PGUSER
 * PGPASSWORD (the read API role), RF_ORG_ID RF_WORKSPACE_ID, PIPELINE_RAW_S3_BUCKET
 * PIPELINE_RAW_S3_REGION PIPELINE_RAW_S3_ACCESS_KEY_ID PIPELINE_RAW_S3_SECRET_ACCESS_KEY, and
 * SESSION_ANALYSIS_OPENAI_API_KEY (analyse only).
 *
 *   survey    --repo <owner/name>[,<alias>...] --since <iso> --until <iso>       counts only, writes nothing
 *   locate    --repo ... --since ... --until ... --out <dir> --delete-by <iso> [--max <n>] [--min-events 5]
 *   artifacts --repo ... --since ... --until ... --out <dir>                      sessions from <out>/locate.jsonl
 *   analyse   --repo ... --since ... --until ... --out <dir> [--from-locate] [--concurrency 4] [--max <n>]
 *   parents   --repo ... --since ... --until ... --out <dir>
 *   seal      --repo ... --since ... --until ... --out <dir> --forge <forge.json>
 *
 * Every collecting command binds <out> to one scope (backfill/scope.ts: tenant, repositories, window, delete-by
 * within 30 days) before it reads anything: the first one needs an empty --out and --delete-by, and the rest must
 * name exactly the same scope. Every row a command writes carries the scope's id, and seal admits no other row.
 * Output is private (out-path.ts: atomic 0600 files, 0700 directories, no links followed, opaque file names).
 * Session lists come only from the same collection's own locate.jsonl (the repository attribution this tool
 * derived from canonical evidence), cut to the scope's repositories and to what the directory lists for the
 * tenant inside the window (the directory's own time is used); the rest are counted by reason.
 * `analyse` is resumable: each finished session is one file under <out>/sessions/, and a rerun skips it.
 * Failures are recorded with a fixed error code only, never a message or transcript text.
 *
 * Data transfer: `analyse` runs the pipeline's processSessionAnalysis, which sends the session's bounded
 * evidence to the configured model provider (the OpenAI Responses API), exactly as the hosted analysis does.
 * Every other command makes no model call.
 */
import { createHash, randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import { normalizeRepo } from "../features/extract.ts";
import { requireUtc, utc } from "../features/types.ts";
import { CORPUS_FORMAT, parseForge, sha256Hex } from "../bench/corpus.ts";
import { PrivateDir, readPrivateFile } from "../out-path.ts";
import { errorCode, printable, UsageError } from "../errors.ts";
import { ScopeError } from "./scope.ts";

export { errorCode };
import { extractArtifacts } from "./artifacts.ts";
import { attributeRepo } from "./locate.ts";
import { bindScope, readScope, type CollectionScope } from "./scope.ts";

type BoundScope = CollectionScope & { id: string };
import { backfillReadSource, backfillSource, directoryRows, parentsOf, readPool, tenantRunner, type DirectoryRow } from "./source.ts";

/** Harnesses that are chats or bots rather than coding agents: never analysed for feature records. */
export const NON_CODING_HARNESSES = new Set(["grok-bot", "chatgpt", "muse", "muse-assistant", "mcp", "claude-ai", "slack", "relaycast", "company-brain"]);
/** Sessions with fewer directory events than this hold no feature work worth a record (a one-shot stub). */
export const DEFAULT_MIN_EVENTS = 5;

/**
 * True only when the directory row's own git remote is exactly one of the named repositories (owner and name).
 * A row with no remote (path-only, or a scrubbed project) abstains here: such sessions are selected only through
 * `--from-locate`, whose attribution this tool derived from the session's own canonical evidence.
 */
export function rowInRepo(row: Pick<DirectoryRow, "gitRepo" | "projectId">, repos: readonly string[]): boolean {
  const remote = normalizeRepo(row.gitRepo);
  return remote !== null && repos.includes(remote);
}

/**
 * The JSON lines of a resumable log. Only a final fragment with no newline (an interrupted last append) is
 * recoverable and ignored; a malformed complete line is corruption and refused (PrivateDir.append removes a torn
 * fragment before it writes, so complete lines are always whole).
 */
export function parseLines(text: string): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const lines = text.split("\n");
  const tail = lines.pop();
  for (const l of lines) {
    if (!l.trim()) continue;
    let row: unknown;
    try { row = JSON.parse(l); } catch { throw new UsageError("a log holds a corrupt line: recollect it"); }
    if (!row || typeof row !== "object" || Array.isArray(row)) throw new UsageError("a log holds a corrupt line: recollect it");
    out.push(row as Record<string, unknown>);
  }
  void tail; // no trailing newline: the fragment of an interrupted append
  return out;
}
const readLines = (dir: PrivateDir, name: string) => (dir.has(name) ? parseLines(dir.read(name)) : []);

/** An opaque per-session file name: no harness or session id on disk. */
export const fileName = (row: { harness: string; sessionId: string }) =>
  `${createHash("sha256").update(`${row.harness}\u0000${row.sessionId}`).digest("hex").slice(0, 32)}.json`;

/**
 * The in-scope rows of this collection's own locate.jsonl. A row is kept only when it carries this scope's id and
 * is a coding session
 * the directory lists for this tenant inside the window (`directory`: key -> its lastActivityAt, read from the
 * directory, never from the file) and its attributed repository is one of the scope's. Everything else is
 * excluded and counted by reason; duplicates are dropped.
 */
export function scopedSessions(lines: Record<string, unknown>[], scope: BoundScope, directory: ReadonlyMap<string, string>):
  { rows: Array<{ harness: string; sessionId: string; repo: string; branch: string | null; lastActivityAt: string }>; excluded: Record<string, number> } {
  const rows = [];
  const excluded: Record<string, number> = {};
  const seen = new Set<string>();
  const skip = (why: string) => { excluded[why] = (excluded[why] ?? 0) + 1; };
  for (const r of lines) {
    const harness = String(r.harness ?? ""), sessionId = String(r.sessionId ?? "");
    const key = `${harness}\u0000${sessionId}`;
    if (r.scope !== scope.id) { skip("other_collection"); continue; }
    if (!harness || !sessionId || NON_CODING_HARNESSES.has(harness)) { skip("not_coding"); continue; }
    if (r.status !== undefined && r.status !== "ok") { skip("not_located"); continue; }
    const repo = typeof r.repo === "string" ? normalizeRepo(r.repo) : null;
    if (!repo || !scope.repos.includes(repo)) { skip("other_repo"); continue; }
    const at = directory.get(key);
    if (at === undefined) { skip("not_in_directory_window"); continue; }
    if (seen.has(key)) { skip("duplicate"); continue; }
    seen.add(key);
    rows.push({ harness, sessionId, repo, branch: typeof r.branch === "string" ? r.branch : null, lastActivityAt: at });
  }
  return { rows, excluded };
}

/**
 * Why a collected session is outside the window [since, until), or null when it is inside. The test is the
 * session's own event times: every evidence pointer of its analysis and every build-evidence event (both from
 * the canonical events, not the model). The directory's last_activity_at is not used: a re-upload moves it (on
 * the cloud collection, 387 of 661 sessions listed active Nov 21-22 held only September and October events).
 */
export function outsideWindow(row: { snapshot?: unknown; artifacts?: unknown }, since: string, until: string): "no_event_times" | "events_outside_window" | null {
  const times: string[] = [];
  const refs = (row.snapshot as { evidence?: { references?: Array<{ at?: unknown }> } } | undefined)?.evidence?.references ?? [];
  for (const r of refs) { const t = typeof r.at === "string" ? utc(r.at) : null; if (t) times.push(t); }
  for (const list of Object.values((row.artifacts ?? {}) as Record<string, unknown>))
    if (Array.isArray(list)) for (const e of list) { const at = (e as { at?: unknown })?.at; const t = typeof at === "string" ? utc(at) : null; if (t) times.push(t); }
  if (!times.length) return "no_event_times";
  const from = requireUtc(since, "since"), to = requireUtc(until, "until");
  return times.some((t) => t >= from && t < to) ? null : "events_outside_window";
}

/** Every coding session the directory lists for the tenant inside the scope's window: key -> lastActivityAt. */
async function directoryWindow(source: { run: Parameters<typeof directoryRows>[0]; cfg: Parameters<typeof directoryRows>[1] }, scope: CollectionScope): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  for await (const row of directoryRows(source.run, source.cfg, scope.since, scope.until))
    if (!NON_CODING_HARNESSES.has(row.harness)) out.set(`${row.harness}\u0000${row.sessionId}`, row.lastActivityAt);
  return out;
}

async function pool<T>(items: T[], n: number, work: (item: T) => Promise<void>) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.max(1, n) }, async () => {
    while (next < items.length) await work(items[next++]!);
  }));
}

export async function main(argv: string[]): Promise<number> {
  const [command, ...rest] = argv;
  const { values } = parseArgs({ args: rest, options: {
    repo: { type: "string" }, since: { type: "string" }, until: { type: "string" }, out: { type: "string" },
    concurrency: { type: "string" }, max: { type: "string" }, "min-events": { type: "string" }, "delete-by": { type: "string" }, forge: { type: "string" },
    "from-locate": { type: "boolean" },
  } });
  const minEvents = Number(values["min-events"] ?? DEFAULT_MIN_EVENTS);
  if (!Number.isInteger(minEvents) || minEvents < 0) throw new UsageError("--min-events must be a whole number");
  const repos = (values.repo ?? "").split(",").map((r) => r.trim().toLowerCase()).filter(Boolean);
  if (!repos.length) throw new UsageError("--repo is required");
  if (!values.since || !values.until) throw new UsageError("--since and --until are required");
  const instant = (v: string, role: string) => { const t = utc(v); if (t === null) throw new UsageError(`${role} must be a valid zoned ISO 8601 instant`); return t; };
  const window = { since: instant(values.since, "--since"), until: instant(values.until, "--until") };
  const bind = (out: PrivateDir, cfg: { orgId: string; workspaceId: string }) =>
    bindScope(out, { ...cfg, repos, ...window, ...(values["delete-by"] ? { deleteBy: instant(values["delete-by"], "--delete-by") } : {}) });

  if (command === "survey") {
    const { cfg, pool: p } = readPool(process.env, 2);
    const counts: Record<string, { all: number; repo: number }> = {};
    const users = new Map<string, number>();
    let repoRows = 0;
    try {
      for await (const row of directoryRows(tenantRunner(p), cfg, window.since, window.until)) {
        const c = (counts[row.harness] ??= { all: 0, repo: 0 });
        c.all++;
        if (!NON_CODING_HARNESSES.has(row.harness) && rowInRepo(row, repos)) {
          c.repo++; repoRows++;
          for (const u of row.userIds) users.set(u, (users.get(u) ?? 0) + 1);
        }
      }
    } finally {
      await p.end();
    }
    console.log(JSON.stringify({ repoRows, byHarness: counts, distinctUsers: users.size }));
    return 0;
  }

  if (command === "seal") {
    if (!values.forge) throw new UsageError("--forge is required");
    const out = PrivateDir.open(values.out);
    const scope = readScope(out);
    if (scope.since !== window.since || scope.until !== window.until || JSON.stringify([...new Set(repos)].sort()) !== JSON.stringify(scope.repos)) throw new UsageError("seal: --repo, --since and --until must match the collection's scope exactly");
    if (values["delete-by"] && instant(values["delete-by"], "--delete-by") !== scope.deleteBy) throw new UsageError("seal: --delete-by must match the collection's scope");
    // Directory parent_session_ids, from this collection's own `parents` run.
    const parentsBy = new Map(readLines(out, "parents.jsonl").filter((r) => r.scope === scope.id).map((r) => [`${String(r.source)}\u0000${String(r.sessionId)}`, (r.parents as string[] | undefined) ?? []]));
    const sessionsDir = out.child("sessions"), artifactsDir = out.child("artifacts");
    // A directory collected before opaque names would hold the same session twice: refuse it.
    for (const d of [sessionsDir, artifactsDir]) if (d.list(".json").some((f) => !/^[0-9a-f]{32}\.json$/u.test(f))) throw new UsageError("seal: files without opaque names found: migrate or recollect this directory");
    const excluded: Record<string, number> = {};
    const lines = sessionsDir.list(".json").map((f) => {
      const { scope: rowScope, ...row } = JSON.parse(sessionsDir.read(f));
      // Row provenance: only rows this scope's commands wrote, under the name derived from their own key.
      if (rowScope !== scope.id || fileName({ harness: String(row.source), sessionId: String(row.sessionId) }) !== f) throw new UsageError("seal: a session row was not collected under this scope");
      if (artifactsDir.has(f)) {
        const a = JSON.parse(artifactsDir.read(f));
        // Bound to the same session as the row it joins: scope, harness, session id and the name derived from them.
        if (a.scope !== scope.id || a.harness !== row.source || a.sessionId !== row.sessionId) throw new UsageError("seal: an artifacts row was not collected under this scope for this session");
        row.artifacts = a.artifacts;
      }
      const parents = parentsBy.get(`${row.source}\u0000${row.sessionId}`);
      if (parents?.length) row.context = { ...row.context, parentSessionIds: parents };
      return row;
    }).filter((row) => {
      // The window is the session's own events, never the directory's last-activity time (which a re-upload moves).
      const why = outsideWindow(row, scope.since, scope.until);
      if (why) excluded[why] = (excluded[why] ?? 0) + 1;
      return why === null;
    }).map((row) => JSON.stringify(row));
    const corpus = out.child("corpus");
    const sessions = `${lines.join("\n")}\n`;
    const forge = JSON.stringify(parseForge(JSON.parse(readPrivateFile(values.forge))));
    corpus.write("sessions.jsonl", sessions);
    corpus.write("forge.json", forge);
    corpus.write("manifest.json", JSON.stringify({
      format: CORPUS_FORMAT, repo: repos[0], window: { since: scope.since, until: scope.until },
      createdAt: new Date().toISOString(), deleteBy: scope.deleteBy,
      files: { "sessions.jsonl": sha256Hex(sessions), "forge.json": sha256Hex(forge) },
    }, null, 2));
    console.log(JSON.stringify({ corpus: corpus.path, sessions: lines.length, excluded }));
    return 0;
  }

  if (command === "parents") {
    const out = PrivateDir.open(values.out);
    const source = backfillReadSource();
    try {
      const scope = bind(out, source.cfg);
      const sessionsDir = out.child("sessions");
      const keys = sessionsDir.list(".json").map((f) => {
        const row = JSON.parse(sessionsDir.read(f)) as { scope?: string; source: string; sessionId: string };
        if (row.scope !== scope.id) throw new UsageError("parents: a session row was not collected under this scope");
        return { harness: row.source, sessionId: row.sessionId };
      });
      const found = await parentsOf(source.run, source.cfg, keys);
      out.write("parents.jsonl", found.map((r) => JSON.stringify({ scope: scope.id, source: r.harness, sessionId: r.sessionId, parents: r.parents })).join("\n") + "\n");
      console.log(JSON.stringify({ sessions: keys.length, found: found.length, withParents: found.filter((r) => r.parents.length).length }));
    } finally {
      await source.close();
    }
    return 0;
  }

  if (command === "locate") {
    // Repository attribution from canonical evidence, no model. Every coding session of the tenant in the window
    // is located (attribution decides the repository); resumable through <out>/locate.jsonl.
    const out = PrivateDir.open(values.out);
    const source = backfillReadSource(process.env);
    try {
      const scope = bind(out, source.cfg);
      const seen = new Set(readLines(out, "locate.jsonl").map((r) => `${r.harness}\u0000${r.sessionId}`));
      const rows: DirectoryRow[] = [];
      for await (const row of directoryRows(source.run, source.cfg, window.since, window.until))
        if (!NON_CODING_HARNESSES.has(row.harness) && row.eventCount >= minEvents && !seen.has(`${row.harness}\u0000${row.sessionId}`)) rows.push(row);
      // A seeded sample (--max) is spread across the window rather than the newest rows.
      let x = 0x9e3779b9;
      const rand = () => ((x = (x * 1664525 + 1013904223) >>> 0) / 2 ** 32);
      const todo = values.max ? rows.map((r) => ({ r, k: rand() })).sort((a, b) => a.k - b.k).slice(0, Number(values.max)).map((e) => e.r) : rows;
      console.log(JSON.stringify({ event: "start", candidates: rows.length, todo: todo.length }));
      const tally: Record<string, number> = {};
      let consecutiveFaults = 0;
      await pool(todo, Number(values.concurrency ?? 8), async (row) => {
        let line: Record<string, unknown>;
        try {
          const a = attributeRepo(await source.canonical(row.harness, row.sessionId));
          line = { scope: scope.id, harness: row.harness, sessionId: row.sessionId, lastActivityAt: row.lastActivityAt, status: "ok", ...a, directoryRepo: normalizeRepo(row.gitRepo), directoryProject: row.projectId && !/^[/~]/u.test(row.projectId) ? row.projectId : null };
          tally[`ok:${a.basis}`] = (tally[`ok:${a.basis}`] ?? 0) + 1;
        } catch (error) {
          const code = errorCode(error);
          if ((error as { reason?: unknown }).reason === undefined) {
            // An error with no canonical reason is the process's fault (connection, file handles, credentials),
            // not the session's: it is not recorded as done, and a run of them stops the pass.
            if (++consecutiveFaults >= 50) throw new UsageError(`stopping: 50 consecutive process faults; last: ${code}`);
            tally[`fault:${code}`] = (tally[`fault:${code}`] ?? 0) + 1;
            return;
          }
          line = { scope: scope.id, harness: row.harness, sessionId: row.sessionId, lastActivityAt: row.lastActivityAt, status: "refused", reason: code };
          tally[`refused:${code}`] = (tally[`refused:${code}`] ?? 0) + 1;
        }
        consecutiveFaults = 0;
        out.append("locate.jsonl", `${JSON.stringify(line)}\n`);
      });
      console.log(JSON.stringify({ event: "done", tally }));
      return 0;
    } finally {
      await source.close();
    }
  }

  if (command === "artifacts") {
    // Build evidence from each listed session's canonical events (backfill/artifacts.ts). Resumable.
    const out = PrivateDir.open(values.out);
    const source = backfillReadSource(process.env);
    try {
      const scope = bind(out, source.cfg);
      const dir = out.child("artifacts");
      const listed = scopedSessions(readLines(out, "locate.jsonl"), scope, await directoryWindow(source, scope));
      const todo = listed.rows.filter((r) => !dir.has(fileName(r)));
      let done = 0, refused = 0, consecutiveFaults = 0;
      console.log(JSON.stringify({ event: "start", todo: todo.length, excluded: listed.excluded }));
      await pool(todo, Number(values.concurrency ?? 8), async (r) => {
        try {
          const a = extractArtifacts(await source.canonical(r.harness, r.sessionId), r.repo);
          dir.write(fileName(r), JSON.stringify({ scope: scope.id, harness: r.harness, sessionId: r.sessionId, artifacts: a }));
          done++; consecutiveFaults = 0;
        } catch (error) {
          const code = errorCode(error);
          if ((error as { reason?: unknown }).reason === undefined && ++consecutiveFaults >= 50) throw new UsageError(`stopping: 50 consecutive process faults; last: ${code}`);
          refused++;
          out.append("artifacts-errors.jsonl", `${JSON.stringify({ harness: r.harness, sessionId: r.sessionId, code })}\n`);
        }
      });
      console.log(JSON.stringify({ event: "done", harvested: done, refused }));
      return 0;
    } finally {
      await source.close();
    }
  }

  if (command === "analyse") {
    const out = PrivateDir.open(values.out);
    const source = backfillSource(process.env);
    try {
      const scope = bind(out, source.cfg);
      const dir = out.child("sessions");
      const rows: DirectoryRow[] = [];
      if (values["from-locate"]) {
        // A located list, cut to the scope and the directory: the attributed repository and branch stand in for the directory's.
        const listed = scopedSessions(readLines(out, "locate.jsonl"), scope, await directoryWindow(source, scope));
        console.log(JSON.stringify({ event: "listed", kept: listed.rows.length, excluded: listed.excluded }));
        for (const r of listed.rows) rows.push({
          harness: r.harness, sessionId: r.sessionId, createdAt: r.lastActivityAt, lastActivityAt: r.lastActivityAt,
          projectId: null, gitRepo: `https://github.com/${r.repo}`, gitBranch: r.branch,
          taskTitle: null, parentSessionIds: [], userIds: [], eventCount: 0,
        });
      } else {
        for await (const row of directoryRows(source.run, source.cfg, scope.since, scope.until))
          if (!NON_CODING_HARNESSES.has(row.harness) && rowInRepo(row, scope.repos)) rows.push(row);
      }
      rows.sort((a, b) => (a.createdAt < b.createdAt ? -1 : a.createdAt > b.createdAt ? 1 : 0));
      const todo = rows.filter((r) => !dir.has(fileName(r))).slice(0, values.max ? Number(values.max) : undefined);
      let done = 0, failed = 0, tokensIn = 0, tokensOut = 0;
      console.log(JSON.stringify({ event: "start", matched: rows.length, todo: todo.length }));
      await pool(todo, Number(values.concurrency ?? 4), async (row) => {
        const requestId = randomUUID();
        for (const assumeNoLegacyTurns of [false, true]) {
          try {
            const r = await source.analyze(row.harness, row.sessionId, { assumeNoLegacyTurns, requestId });
            tokensIn += r.inputTokens; tokensOut += r.outputTokens;
            dir.write(fileName(row), JSON.stringify({
              scope: scope.id, source: row.harness, sessionId: row.sessionId,
              context: { repo: row.gitRepo, branch: row.gitBranch, taskRef: null, userRole: null, ...(row.parentSessionIds.length ? { parentSessionIds: row.parentSessionIds } : {}) },
              snapshot: r.snapshot,
            }));
            out.append("analysed.jsonl", `${JSON.stringify({ harness: row.harness, sessionId: row.sessionId, assumeNoLegacyTurns, inputTokens: r.inputTokens, outputTokens: r.outputTokens, at: new Date().toISOString() })}\n`);
            done++;
            return;
          } catch (error) {
            const code = errorCode(error);
            // A missing legacy coverage marker is retried once with the operator switch; anything else fails the row.
            // The message is only classified here, never stored or printed.
            const legacy = code === "SessionSourceIncompleteError" || /legacy evidence coverage/iu.test(String((error as Error).message ?? ""));
            if (!assumeNoLegacyTurns && legacy) continue;
            failed++;
            out.append("errors.jsonl", `${JSON.stringify({ harness: row.harness, sessionId: row.sessionId, code, assumeNoLegacyTurns })}\n`);
            return;
          }
        }
      });
      console.log(JSON.stringify({ event: "done", analysed: done, failed, inputTokens: tokensIn, outputTokens: tokensOut }));
      return failed ? 3 : 0;
    } finally {
      await source.close();
    }
  }
  console.error("usage: cli.ts survey|locate|artifacts|analyse|parents|seal ...");
  return 2;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then((code) => process.exit(code), (error) => {
    console.error(printable(error, [ScopeError]));
    process.exit(1);
  });
}
