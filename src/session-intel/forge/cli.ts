/**
 * Forge outcome mining CLI (docs/session-intel-forge-outcomes.md).
 *
 *   bun run src/session-intel/forge/cli.ts fetch --repo <owner/name> --since <iso> --until <iso> --out <file.json>
 *   bun run src/session-intel/forge/cli.ts history --repo <owner/name> --prs <file.json> --since <iso> --until <iso> --out <history.json>
 *   bun run src/session-intel/forge/cli.ts mine  --repo <owner/name> --git <mirror.git> --prs <file.json> \
 *     --since <iso> --until <iso> --out <candidates.jsonl>
 *
 * Both outputs hold real identifiers and text, so `--out` must be outside this repository.
 */
import { existsSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { fetchHistories, fetchRepo } from "./fetch.ts";
import { readCommits } from "./git.ts";
import { collisionNotes, crossIdentityReverts, migrationNumberCollisions, supersededPrs } from "./signals.ts";
import type { Candidate, PullRequest } from "./types.ts";

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
/** Merge-train promotion PRs are closed by design and never carry overlap. */
export const TRAIN_HEAD = /^(?:trunk-train|merge-train)\//;

function outsideRepo(path: string): string {
  const abs = resolve(path);
  const parent = realpathSync(existsSync(abs) ? dirname(abs) : dirname(abs));
  if ((parent + "/").startsWith(REPO_ROOT + "/") || parent === REPO_ROOT) throw new Error("--out must be outside this repository");
  return abs;
}
const iso = (v: string | undefined, name: string) => {
  if (!v || !Number.isFinite(Date.parse(v))) throw new Error(`--${name} must be an ISO time`);
  return new Date(v).toISOString();
};

export function mine(repo: string, commits: Parameters<typeof migrationNumberCollisions>[1], prs: PullRequest[]): Candidate[] {
  return [
    ...migrationNumberCollisions(repo, commits),
    ...crossIdentityReverts(repo, commits),
    ...supersededPrs(repo, prs, { skipHeadRef: TRAIN_HEAD }),
    ...collisionNotes(repo, prs, commits),
  ];
}

async function main(argv: string[]) {
  const [cmd, ...rest] = argv;
  const { values } = parseArgs({ args: rest, options: {
    repo: { type: "string" }, since: { type: "string" }, until: { type: "string" }, out: { type: "string" },
    git: { type: "string" }, prs: { type: "string" },
  } });
  if (!values.repo || !/^[\w.-]+\/[\w.-]+$/.test(values.repo)) throw new Error("--repo must be owner/name");
  const since = iso(values.since, "since"), until = iso(values.until, "until");
  if (!values.out) throw new Error("--out is required");
  const out = outsideRepo(values.out);
  if (cmd === "fetch") {
    const prs = await fetchRepo(values.repo, since, until);
    writeFileSync(out, JSON.stringify(prs), { mode: 0o600 });
    console.log(JSON.stringify({ repo: values.repo, prs: prs.length, comments: prs.reduce((n, p) => n + p.comments.length, 0) }));
  } else if (cmd === "mine") {
    if (!values.git || !values.prs) throw new Error("mine needs --git and --prs");
    const commits = await readCommits(values.git, since, until);
    const prs = JSON.parse(readFileSync(values.prs, "utf8")) as PullRequest[];
    const candidates = mine(values.repo, commits, prs);
    writeFileSync(out, candidates.map((c) => JSON.stringify(c)).join("\n") + (candidates.length ? "\n" : ""), { mode: 0o600 });
    const by: Record<string, { all: number; cross: number }> = {};
    for (const c of candidates) { by[c.signal] ??= { all: 0, cross: 0 }; by[c.signal]!.all++; if (c.crossIdentity) by[c.signal]!.cross++; }
    console.log(JSON.stringify({ repo: values.repo, commits: commits.length, prs: prs.length, candidates: by }));
  } else if (cmd === "history") {
    if (!values.prs) throw new Error("history needs --prs (the fetch output)");
    const prs = JSON.parse(readFileSync(values.prs, "utf8")) as PullRequest[];
    const histories = await fetchHistories(values.repo, prs.map((p) => p.number), (m) => console.error(m));
    writeFileSync(out, JSON.stringify(histories), { mode: 0o600 });
    console.log(JSON.stringify({ repo: values.repo, prs: histories.length, renamed: histories.filter((h) => h.titles.length > 1).length, edited: histories.filter((h) => h.bodies.length > 1).length }));
  } else throw new Error("command must be fetch, mine or history");
}

if (import.meta.main) main(process.argv.slice(2)).catch((e) => { console.error(`forge: ${(e as Error).message}`); process.exit(1); });
