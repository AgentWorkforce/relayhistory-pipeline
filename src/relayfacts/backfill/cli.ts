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
 *   survey  --repo <owner/name>[,<alias>...] --since <iso> --until <iso>
 *   analyse --repo ... --since ... --until ... --out <dir> [--concurrency 4] [--max <n>]
 *   seal    --out <dir> --repo <owner/name> --since ... --until ... --delete-by <iso> --forge <forge.json>
 *
 * `analyse` is resumable: each finished session is one file under <out>/sessions/, and a rerun skips
 * it. Failures go to <out>/errors.jsonl with a reason code only, never transcript text.
 */
import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { normalizeRepo } from "../features/extract.ts";
import { CORPUS_FORMAT, parseForge, sha256Hex } from "../bench/corpus.ts";
import { attributeRepo } from "./locate.ts";
import { backfillReadSource, backfillSource, directoryRows, tenantRunner, type DirectoryRow } from "./source.ts";

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
/** Harnesses that are chats or bots rather than coding agents: never analysed for feature records. */
export const NON_CODING_HARNESSES = new Set(["grok-bot", "chatgpt", "muse", "muse-assistant", "mcp", "claude-ai", "slack", "relaycast", "company-brain"]);
/** Sessions with fewer directory events than this hold no feature work worth a record (a one-shot stub). */
export const DEFAULT_MIN_EVENTS = 5;

function outside(path: string): string {
  mkdirSync(path, { recursive: true });
  const real = realpathSync(path);
  if (real === REPO_ROOT || real.startsWith(REPO_ROOT + sep)) throw new Error("--out must be outside this repository");
  return real;
}

/** True when the directory row belongs to one of the named repositories (git remote, else the project path's last segment). */
export function rowInRepo(row: Pick<DirectoryRow, "gitRepo" | "projectId">, repos: readonly string[]): boolean {
  const remote = normalizeRepo(row.gitRepo);
  if (remote) return repos.some((r) => remote === r || remote.endsWith(`/${r.split("/").pop()}`));
  const project = (row.projectId ?? "").replace(/\\/gu, "/").toLowerCase().replace(/\/+$/u, "");
  return repos.some((r) => {
    const name = r.split("/").pop()!;
    return project === r || project.endsWith(`/${name}`) || project === name || project.includes(`/${name}/`) || project.includes(`/worktrees/${name}`);
  });
}

const fileName = (row: { harness: string; sessionId: string }) =>
  `${row.harness}__${createHash("sha256").update(`${row.harness}\u0000${row.sessionId}`).digest("hex").slice(0, 24)}.json`;

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
  } });
  const minEvents = Number(values["min-events"] ?? DEFAULT_MIN_EVENTS);
  const repos = (values.repo ?? "").split(",").map((r) => r.trim().toLowerCase()).filter(Boolean);
  if (!repos.length) throw new Error("--repo is required");

  if (command === "seal") {
    const out = outside(values.out ?? "");
    if (!values.since || !values.until || !values["delete-by"] || !values.forge) throw new Error("--since, --until, --delete-by and --forge are required");
    const lines = readdirSync(join(out, "sessions")).filter((f) => f.endsWith(".json")).sort()
      .map((f) => JSON.stringify(JSON.parse(readFileSync(join(out, "sessions", f), "utf8"))));
    const corpus = join(out, "corpus");
    mkdirSync(corpus, { recursive: true });
    const sessions = `${lines.join("\n")}\n`;
    const forge = JSON.stringify(parseForge(JSON.parse(readFileSync(values.forge, "utf8"))));
    writeFileSync(join(corpus, "sessions.jsonl"), sessions);
    writeFileSync(join(corpus, "forge.json"), forge);
    writeFileSync(join(corpus, "manifest.json"), JSON.stringify({
      format: CORPUS_FORMAT, repo: repos[0], window: { since: values.since, until: values.until },
      createdAt: new Date().toISOString(), deleteBy: values["delete-by"],
      files: { "sessions.jsonl": sha256Hex(sessions), "forge.json": sha256Hex(forge) },
    }, null, 2));
    console.log(JSON.stringify({ corpus, sessions: lines.length }));
    return 0;
  }

  if (!values.since || !values.until) throw new Error("--since and --until are required");
  if (command === "survey") {
    const pg = (await import("pg")).default;
    const cfg = { orgId: process.env.RF_ORG_ID ?? "", workspaceId: process.env.RF_WORKSPACE_ID ?? "" };
    const p = new pg.Pool({ max: 2, ssl: { rejectUnauthorized: false } });
    const counts: Record<string, { all: number; repo: number }> = {};
    const users = new Map<string, number>();
    let repoRows = 0;
    for await (const row of directoryRows(tenantRunner(p), cfg, values.since, values.until)) {
      const c = (counts[row.harness] ??= { all: 0, repo: 0 });
      c.all++;
      if (!NON_CODING_HARNESSES.has(row.harness) && rowInRepo(row, repos)) {
        c.repo++; repoRows++;
        for (const u of row.userIds) users.set(u, (users.get(u) ?? 0) + 1);
      }
    }
    await p.end();
    console.log(JSON.stringify({ repoRows, byHarness: counts, distinctUsers: users.size }));
    return 0;
  }

  if (command === "locate") {
    // Repository attribution from canonical evidence, no model. --repo filters nothing here (all coding
    // sessions are located); it is kept required for symmetry. Resumable through <out>/locate.jsonl.
    const out = outside(values.out ?? "");
    const source = backfillReadSource(process.env);
    try {
      const file = join(out, "locate.jsonl");
      const seen = new Set(existsSync(file) ? readFileSync(file, "utf8").split("\n").filter(Boolean).map((l) => { const r = JSON.parse(l); return `${r.harness}\u0000${r.sessionId}`; }) : []);
      const rows: DirectoryRow[] = [];
      for await (const row of directoryRows(source.run, source.cfg, values.since, values.until))
        if (!NON_CODING_HARNESSES.has(row.harness) && row.eventCount >= minEvents && !seen.has(`${row.harness}\u0000${row.sessionId}`)) rows.push(row);
      // A seeded sample (--max) is spread across the window rather than the newest rows.
      let x = 0x9e3779b9;
      const rand = () => ((x = (x * 1664525 + 1013904223) >>> 0) / 2 ** 32);
      const todo = values.max ? rows.map((r) => ({ r, k: rand() })).sort((a, b) => a.k - b.k).slice(0, Number(values.max)).map((e) => e.r) : rows;
      console.log(JSON.stringify({ event: "start", candidates: rows.length, todo: todo.length }));
      const tally: Record<string, number> = {};
      await pool(todo, Number(values.concurrency ?? 8), async (row) => {
        let line: Record<string, unknown>;
        try {
          const a = attributeRepo(await source.canonical(row.harness, row.sessionId));
          line = { harness: row.harness, sessionId: row.sessionId, lastActivityAt: row.lastActivityAt, status: "ok", ...a, directoryRepo: normalizeRepo(row.gitRepo), directoryProject: row.projectId && !/^[/~]/u.test(row.projectId) ? row.projectId : null };
          tally[`ok:${a.basis}`] = (tally[`ok:${a.basis}`] ?? 0) + 1;
        } catch (error) {
          const reason = String((error as { reason?: string }).reason ?? (error as { code?: string }).code ?? "error").slice(0, 60);
          line = { harness: row.harness, sessionId: row.sessionId, lastActivityAt: row.lastActivityAt, status: "refused", reason };
          tally[`refused:${reason}`] = (tally[`refused:${reason}`] ?? 0) + 1;
        }
        appendFileSync(file, `${JSON.stringify(line)}\n`);
      });
      console.log(JSON.stringify({ event: "done", tally }));
      return 0;
    } finally {
      await source.close();
    }
  }

  if (command === "analyse") {
    const out = outside(values.out ?? "");
    mkdirSync(join(out, "sessions"), { recursive: true });
    const source = backfillSource(process.env);
    try {
      const rows: DirectoryRow[] = [];
      for await (const row of directoryRows(source.run, source.cfg, values.since, values.until))
        if (!NON_CODING_HARNESSES.has(row.harness) && rowInRepo(row, repos)) rows.push(row);
      rows.sort((a, b) => (a.createdAt < b.createdAt ? -1 : a.createdAt > b.createdAt ? 1 : 0));
      const todo = rows.filter((r) => !existsSync(join(out, "sessions", fileName(r)))).slice(0, values.max ? Number(values.max) : undefined);
      let done = 0, failed = 0, tokensIn = 0, tokensOut = 0;
      console.log(JSON.stringify({ event: "start", matched: rows.length, todo: todo.length }));
      await pool(todo, Number(values.concurrency ?? 4), async (row) => {
        const requestId = randomUUID();
        for (const assumeNoLegacyTurns of [false, true]) {
          try {
            const r = await source.analyze(row.harness, row.sessionId, { assumeNoLegacyTurns, requestId });
            tokensIn += r.inputTokens; tokensOut += r.outputTokens;
            writeFileSync(join(out, "sessions", fileName(row)), JSON.stringify({
              source: row.harness, sessionId: row.sessionId,
              context: { repo: row.gitRepo, branch: row.gitBranch, taskRef: null, userRole: null },
              snapshot: r.snapshot,
            }));
            appendFileSync(join(out, "analysed.jsonl"), `${JSON.stringify({ harness: row.harness, sessionId: row.sessionId, assumeNoLegacyTurns, inputTokens: r.inputTokens, outputTokens: r.outputTokens, at: new Date().toISOString() })}\n`);
            done++;
            return;
          } catch (error) {
            const code = (error as { code?: string; message?: string }).code ?? (error as Error).constructor?.name ?? "error";
            const reason = String((error as Error).message ?? "").slice(0, 80).replace(/[^A-Za-z0-9_ :.-]/gu, "");
            // A missing legacy coverage marker is retried once with the operator switch; anything else fails the row.
            if (!assumeNoLegacyTurns && /legacy|coverage|neon/iu.test(`${code} ${reason}`)) continue;
            failed++;
            appendFileSync(join(out, "errors.jsonl"), `${JSON.stringify({ harness: row.harness, sessionId: row.sessionId, code, reason, assumeNoLegacyTurns })}\n`);
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
  console.error("usage: cli.ts survey|analyse|seal ...");
  return 2;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then((code) => process.exit(code), (error) => {
    console.error((error as Error).message);
    process.exit(1);
  });
}
