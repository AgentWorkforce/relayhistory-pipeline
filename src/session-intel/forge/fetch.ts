/**
 * Fetches one repository's pull requests, comments and closed-PR files through the GitHub CLI into a JSON file
 * outside this repository (docs/session-intel-forge-outcomes.md, "Fetch"). It uses the operator's own `gh`
 * login; no token is read or stored here. Network only to api.github.com, through `gh`.
 */
import { spawn } from "node:child_process";
import type { PrHistory, PullRequest } from "./types.ts";

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

const HISTORY_BATCH = 20;
interface RawHistory {
  number: number; createdAt: string; title: string; body: string;
  userContentEdits: { nodes: Array<{ editedAt: string; diff: string | null; deletedAt: string | null }> };
  timelineItems: { nodes: Array<{ __typename: string; createdAt?: string; previousTitle?: string; currentTitle?: string }> };
}

/** Rebuilds dated title and description histories from one PR's GraphQL record. Pure. */
export function historyOf(r: RawHistory): PrHistory {
  const renames = r.timelineItems.nodes.filter((n) => n.__typename === "RenamedTitleEvent" && n.createdAt).sort((a, b) => (a.createdAt! < b.createdAt! ? -1 : 1));
  const titles = [{ at: r.createdAt, title: renames[0]?.previousTitle ?? r.title }, ...renames.map((n) => ({ at: n.createdAt!, title: n.currentTitle ?? "" }))];
  // Edits list every version of the description, the original included; with none, it was never edited. A version
  // whose record was deleted or carries no text is unknown (null), never filled in from another version.
  const edits = [...r.userContentEdits.nodes].sort((a, b) => (a.editedAt < b.editedAt ? -1 : 1));
  const bodies = edits.length
    ? edits.map((n) => ({ at: n.editedAt < r.createdAt ? r.createdAt : n.editedAt, body: n.diff !== null && !n.deletedAt ? n.diff : null }))
    : [{ at: r.createdAt, body: r.body ?? "" }];
  return { number: r.number, titles, bodies };
}

/**
 * Title and description histories for `numbers`, in GraphQL batches. Each PR's last 100 description versions
 * and first 100 rename events are read; a PR with more is marked by throwing, so a truncated history is never
 * taken for a complete one.
 */
export async function fetchHistories(repo: string, numbers: number[], log: (m: string) => void = () => {}): Promise<PrHistory[]> {
  const [owner, name] = repo.split("/") as [string, string];
  const out: PrHistory[] = [];
  for (let i = 0; i < numbers.length; i += HISTORY_BATCH) {
    const batch = numbers.slice(i, i + HISTORY_BATCH);
    if (!batch.every((n) => Number.isSafeInteger(n) && n > 0)) throw new Error("PR numbers must be positive integers");
    const fields = batch.map((n) => `p${n}: pullRequest(number: ${n}) { number createdAt title body
      userContentEdits(last: 100) { totalCount nodes { editedAt diff deletedAt } }
      timelineItems(first: 100, itemTypes: [RENAMED_TITLE_EVENT]) { pageInfo { hasNextPage } nodes { __typename ... on RenamedTitleEvent { createdAt previousTitle currentTitle } } } }`).join("\n");
    const query = `query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { ${fields} } }`;
    const data = JSON.parse(await gh(["api", "graphql", "-f", `query=${query}`, "-F", `owner=${owner}`, "-F", `name=${name}`])) as { data: { repository: Record<string, (RawHistory & { userContentEdits: { totalCount: number }; timelineItems: { pageInfo: { hasNextPage: boolean } } }) | null> } };
    for (const r of Object.values(data.data.repository)) {
      if (!r) continue;
      if (r.userContentEdits.totalCount > 100 || r.timelineItems.pageInfo.hasNextPage) throw new Error(`${repo}#${r.number}: history longer than one page`);
      out.push(historyOf(r));
    }
    if ((i / HISTORY_BATCH) % 25 === 0) log(`${repo}: histories ${out.length}/${numbers.length}`);
  }
  return out;
}
