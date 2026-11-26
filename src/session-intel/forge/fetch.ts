/**
 * Fetches one repository's pull requests, comments and closed-PR files through the GitHub CLI into a JSON file
 * outside this repository (docs/session-intel-forge-outcomes.md, "Fetch"). It uses the operator's own `gh`
 * login; no token is read or stored here. Network only to api.github.com, through `gh`.
 */
import { spawn } from "node:child_process";
import type { PullRequest } from "./types.ts";

function gh(args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn("gh", args, { stdio: ["ignore", "pipe", "pipe"] });
    const out: Buffer[] = [];
    let err = "";
    child.stdout.on("data", (c: Buffer) => out.push(c));
    child.stderr.on("data", (c: Buffer) => { err += c.toString(); });
    child.on("error", reject);
    child.on("close", (code) => code === 0 ? resolve(Buffer.concat(out).toString("utf8")) : reject(new Error(`gh ${args[0]} exited ${code}: ${err.slice(0, 300)}`)));
  });
}

const DAY = 86_400_000;
const LIST_FIELDS = "number,title,author,state,headRefName,createdAt,closedAt,mergedAt,body";

interface RawPr { number: number; title: string; author: { login: string } | null; state: PullRequest["state"]; headRefName: string; createdAt: string; closedAt: string | null; mergedAt: string | null; body: string | null }

/** Lists PRs in weekly slices so no slice reaches the search API's 1,000-result cap. */
export async function listPullRequests(repo: string, since: string, until: string): Promise<RawPr[]> {
  const byNumber = new Map<number, RawPr>();
  for (let t = Date.parse(since); t < Date.parse(until); t += 7 * DAY) {
    const from = new Date(t).toISOString().slice(0, 10);
    const to = new Date(Math.min(t + 6 * DAY, Date.parse(until))).toISOString().slice(0, 10);
    const rows = JSON.parse(await gh(["pr", "list", "-R", repo, "--state", "all", "--limit", "1000", "--search", `created:${from}..${to}`, "--json", LIST_FIELDS])) as RawPr[];
    if (rows.length >= 1000) throw new Error(`${repo}: slice ${from}..${to} reached the 1,000-result cap`);
    for (const r of rows) byNumber.set(r.number, r);
  }
  return [...byNumber.values()];
}

/** Every issue comment (PR conversation comments included) since `since`, as one paginated stream. */
export async function listIssueComments(repo: string, since: string): Promise<Array<{ number: number; author: string; at: string; body: string }>> {
  const raw = await gh(["api", "--paginate", `repos/${repo}/issues/comments?per_page=100&since=${since}`,
    "-q", ".[] | {u: (.user.login // \"\"), t: .created_at, b: (.body // \"\"), i: .issue_url}"]);
  return raw.split("\n").filter(Boolean).map((line) => {
    const r = JSON.parse(line) as { u: string; t: string; b: string; i: string };
    return { number: Number(r.i.slice(r.i.lastIndexOf("/") + 1)), author: r.u, at: r.t, body: r.b };
  });
}

export async function listFiles(repo: string, number: number): Promise<string[]> {
  const raw = await gh(["api", "--paginate", `repos/${repo}/pulls/${number}/files?per_page=100`, "-q", ".[].filename"]);
  return raw.split("\n").filter(Boolean);
}

/** PRs with their comments, and changed files for every closed or merged PR (the ones outcomes can name). */
export async function fetchRepo(repo: string, since: string, until: string, opts: { concurrency?: number } = {}): Promise<PullRequest[]> {
  const prs = await listPullRequests(repo, since, until);
  const comments = await listIssueComments(repo, since);
  const byPr = new Map<number, PullRequest["comments"]>();
  for (const c of comments) byPr.set(c.number, [...(byPr.get(c.number) ?? []), { author: c.author, at: c.at, body: c.body }]);
  const out: PullRequest[] = prs.map((p) => ({
    number: p.number, author: p.author?.login ?? "", title: p.title, body: p.body ?? "", state: p.state, headRef: p.headRefName,
    createdAt: p.createdAt, closedAt: p.closedAt, mergedAt: p.mergedAt, files: null,
    comments: (byPr.get(p.number) ?? []).sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : 0)),
  }));
  const queue = out.filter((p) => p.state !== "OPEN");
  const workers = Array.from({ length: Math.max(1, Math.min(8, opts.concurrency ?? 4)) }, async () => {
    for (let p = queue.shift(); p; p = queue.shift()) p.files = await listFiles(repo, p.number);
  });
  await Promise.all(workers);
  return out.sort((a, b) => a.number - b.number);
}
