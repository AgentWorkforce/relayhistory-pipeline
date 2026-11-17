/**
 * Relayfacts GitHub push: writes one topic's relayfacts markdown document into
 * configured GitHub repositories through the contents API.
 *
 * Default off (RELAYFACTS_GITHUB_PUSH_ENABLED must be exactly "1"). Pure module:
 * the caller injects fetch and a token getter, so nothing here reads secrets or
 * the network on its own. HTTP and network failures are returned as outcomes and
 * never thrown, so GitHub can never block the pipeline. Programming errors (an
 * invalid path or target) do throw.
 *
 * The token never appears in the returned config, logged events, results or
 * thrown messages. Logged events carry no response bodies, markdown, topic text
 * or workspace id. See docs/relayfacts-github-push.md.
 */
import { createHash } from "node:crypto";

export interface RelayfactsTarget {
  owner: string;
  repo: string;
  branch: string;
  dir: string;
}

export interface RelayfactsPushConfig {
  targets: RelayfactsTarget[];
  apiBase: string;
}

export type RelayfactsPushResult =
  | { outcome: "created" | "updated" | "unchanged" }
  | { outcome: "skipped"; reason: "no_token" }
  | { outcome: "failed"; status?: number; reason: string };

export interface RelayfactsPushDeps {
  fetch: typeof fetch;
  /** Returns RELAYFACTS_GITHUB_TOKEN at push time; undefined or empty skips the push. */
  token: () => string | undefined;
  now?: () => Date;
  log?: (event: Record<string, unknown>) => void;
  /** GitHub API base; defaults to https://api.github.com. Normally config.apiBase. */
  apiBase?: string;
}

export interface RelayfactsPushInput {
  target: RelayfactsTarget;
  path: string;
  markdown: string;
  topic: string;
  workspace: string;
}

export const RELAYFACTS_DEFAULT_API_BASE = "https://api.github.com";
const DEFAULT_BRANCH = "main";
const DEFAULT_DIR = "relayfacts";
const NAME = /^[A-Za-z0-9._-]{1,100}$/u;
const DIR_SEGMENT = /^[A-Za-z0-9._-]{1,100}$/u;
const BRANCH = /^[A-Za-z0-9._/-]{1,200}$/u;
const SLUG_FILE = /^[a-z0-9-]{1,80}\.md$/u;
const MAX_SLUG = 80;
const HASH_LEN = 12;
const REQUEST_TIMEOUT_MS = 15_000;
const USER_AGENT = "relayhistory-pipeline-relayfacts";

function validName(value: string): boolean {
  return NAME.test(value) && value !== "." && value !== "..";
}

function validDir(dir: string): boolean {
  if (dir.length === 0 || dir.length > 200 || dir.startsWith("/") || dir.endsWith("/")) return false;
  return dir.split("/").every((s) => DIR_SEGMENT.test(s) && s !== "." && s !== "..");
}

function validBranch(branch: string): boolean {
  return BRANCH.test(branch) && !branch.includes("..") && !branch.includes("//")
    && !branch.startsWith("/") && !branch.startsWith("-") && !branch.endsWith("/") && !branch.endsWith(".lock");
}

function validTarget(t: RelayfactsTarget): boolean {
  return validName(t.owner) && validName(t.repo) && validBranch(t.branch) && validDir(t.dir);
}

function parseTarget(entry: string, index: number): RelayfactsTarget {
  const where = `RELAYFACTS_GITHUB_REPOS entry ${index + 1}`;
  const colon = entry.indexOf(":");
  const left = colon === -1 ? entry : entry.slice(0, colon);
  const dir = colon === -1 ? DEFAULT_DIR : entry.slice(colon + 1);
  const at = left.indexOf("@");
  const ownerRepo = at === -1 ? left : left.slice(0, at);
  const branch = at === -1 ? DEFAULT_BRANCH : left.slice(at + 1);
  const parts = ownerRepo.split("/");
  if (parts.length !== 2) throw new Error(`${where}: expected owner/repo[@branch][:dir]`);
  const [owner, repo] = parts;
  if (!validName(owner)) throw new Error(`${where}: invalid owner (allowed: A-Z a-z 0-9 . _ -)`);
  if (!validName(repo)) throw new Error(`${where}: invalid repo (allowed: A-Z a-z 0-9 . _ -)`);
  if (!validBranch(branch)) throw new Error(`${where}: invalid branch`);
  if (!validDir(dir)) throw new Error(`${where}: invalid dir (relative, no '..', segments of A-Z a-z 0-9 . _ -)`);
  return { owner, repo, branch, dir };
}

function parseApiBase(raw: string | undefined): string {
  if (raw === undefined || raw.trim() === "") return RELAYFACTS_DEFAULT_API_BASE;
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new Error("RELAYFACTS_GITHUB_API_BASE: not a valid URL");
  }
  if (url.protocol !== "https:") throw new Error("RELAYFACTS_GITHUB_API_BASE: must use https");
  if (url.username || url.password) throw new Error("RELAYFACTS_GITHUB_API_BASE: must not contain credentials");
  if (url.search || url.hash) throw new Error("RELAYFACTS_GITHUB_API_BASE: must not contain a query or fragment");
  return `${url.origin}${url.pathname.replace(/\/+$/u, "")}`;
}

/** Null unless RELAYFACTS_GITHUB_PUSH_ENABLED is exactly "1". Throws on malformed config, naming the field only. */
export function parseRelayfactsPushConfig(env: Record<string, string | undefined>): RelayfactsPushConfig | null {
  if (env.RELAYFACTS_GITHUB_PUSH_ENABLED !== "1") return null;
  const entries = (env.RELAYFACTS_GITHUB_REPOS ?? "").split(",").map((e) => e.trim());
  if (entries.length === 1 && entries[0] === "") throw new Error("RELAYFACTS_GITHUB_REPOS: required when RELAYFACTS_GITHUB_PUSH_ENABLED=1");
  const targets: RelayfactsTarget[] = [];
  const seen = new Set<string>();
  entries.forEach((entry, i) => {
    if (entry === "") throw new Error(`RELAYFACTS_GITHUB_REPOS entry ${i + 1}: empty`);
    const target = parseTarget(entry, i);
    const key = `${target.owner.toLowerCase()}/${target.repo.toLowerCase()}@${target.branch}:${target.dir}`;
    if (seen.has(key)) throw new Error(`RELAYFACTS_GITHUB_REPOS entry ${i + 1}: duplicate target`);
    seen.add(key);
    targets.push(target);
  });
  return { targets, apiBase: parseApiBase(env.RELAYFACTS_GITHUB_API_BASE) };
}

/** `${dir}/${slug}.md`; the slug gains a short sha256 of the topic whenever slugging lost information. */
export function relayfactsDocumentPath(dir: string, topic: string): string {
  if (!validDir(dir)) throw new Error("relayfactsDocumentPath: invalid dir");
  const hash = createHash("sha256").update(topic, "utf8").digest("hex").slice(0, HASH_LEN);
  const slug = topic.toLowerCase().replace(/[^a-z0-9]+/gu, "-").replace(/^-+|-+$/gu, "");
  let name: string;
  if (slug === "") name = `topic-${hash}`;
  else if (slug === topic && slug.length <= MAX_SLUG) name = slug;
  else {
    const base = slug.slice(0, MAX_SLUG - HASH_LEN - 1).replace(/-+$/u, "");
    name = base === "" ? `topic-${hash}` : `${base}-${hash}`;
  }
  return `${dir}/${name}.md`;
}

function assertDocumentPath(target: RelayfactsTarget, path: string): string {
  if (!validTarget(target)) throw new Error("pushRelayfactsDocument: invalid target");
  const prefix = `${target.dir}/`;
  const file = path.startsWith(prefix) ? path.slice(prefix.length) : "";
  if (!SLUG_FILE.test(file)) throw new Error("pushRelayfactsDocument: invalid path (use relayfactsDocumentPath)");
  return file.slice(0, -3);
}

const encodePath = (path: string) => path.split("/").map(encodeURIComponent).join("/");

function isRateLimited(res: Response): boolean {
  if (res.status === 429) return true;
  return res.status === 403 && (res.headers.get("x-ratelimit-remaining") === "0" || res.headers.has("retry-after"));
}

function statusReason(res: Response, stage: "get" | "put"): string {
  if (isRateLimited(res)) return "rate_limited";
  if (res.status === 401) return "unauthorized";
  if (res.status === 403) return "forbidden";
  if (res.status === 404) return "not_found";
  return `${stage}_failed`;
}

type Remote = { kind: "absent" } | { kind: "file"; sha: string; content: string | null } | { kind: "error"; result: RelayfactsPushResult };

/** Push one document. Never throws for HTTP or network failures; throws on an invalid target or path. */
export async function pushRelayfactsDocument(deps: RelayfactsPushDeps, input: RelayfactsPushInput): Promise<RelayfactsPushResult> {
  const { target, path, markdown } = input;
  const slug = assertDocumentPath(target, path);
  if (typeof markdown !== "string") throw new Error("pushRelayfactsDocument: markdown must be a string");
  const apiBase = deps.apiBase ?? RELAYFACTS_DEFAULT_API_BASE;
  const url = `${apiBase}/repos/${encodeURIComponent(target.owner)}/${encodeURIComponent(target.repo)}/contents/${encodePath(path)}`;

  const emit = (result: RelayfactsPushResult, status: number | null): RelayfactsPushResult => {
    if (deps.log) {
      try {
        deps.log({
          event: "relayfacts_github_push",
          outcome: result.outcome,
          status,
          ...("reason" in result ? { reason: result.reason } : {}),
          repository: `${target.owner}/${target.repo}`,
          branch: target.branch,
          path,
          at: (deps.now ?? (() => new Date()))().toISOString(),
        });
      } catch {
        // Logging must never change the push outcome.
      }
    }
    return result;
  };

  let token: string | undefined;
  try {
    token = deps.token();
  } catch {
    token = undefined;
  }
  if (typeof token !== "string" || token.trim() === "") return emit({ outcome: "skipped", reason: "no_token" }, null);

  const headers = {
    Authorization: `Bearer ${token.trim()}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": USER_AGENT,
  };
  // Status of the most recent request only; null when that request got no response, so a
  // failed PUT never logs the status of the GET before it.
  let lastStatus: number | null = null;

  const request = async (method: "GET" | "PUT", requestUrl: string, body?: string): Promise<Response | null> => {
    lastStatus = null;
    try {
      const res = await deps.fetch(requestUrl, {
        method,
        headers: body === undefined ? headers : { ...headers, "Content-Type": "application/json" },
        body,
        redirect: "error",
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      lastStatus = res.status;
      return res;
    } catch {
      // The error may describe the request; never surface it.
      return null;
    }
  };

  const readRemote = async (): Promise<Remote> => {
    const res = await request("GET", `${url}?ref=${encodeURIComponent(target.branch)}`);
    if (!res) return { kind: "error", result: { outcome: "failed", reason: "network" } };
    if (res.status === 404) {
      await res.body?.cancel().catch(() => {});
      return { kind: "absent" };
    }
    if (res.status !== 200) {
      await res.body?.cancel().catch(() => {});
      return { kind: "error", result: { outcome: "failed", status: res.status, reason: statusReason(res, "get") } };
    }
    let json: unknown;
    try {
      json = await res.json();
    } catch {
      return { kind: "error", result: { outcome: "failed", status: 200, reason: "bad_response" } };
    }
    const file = json as { type?: unknown; sha?: unknown; content?: unknown; encoding?: unknown };
    if (!json || typeof json !== "object" || Array.isArray(json) || file.type !== "file" || typeof file.sha !== "string" || file.sha === "")
      return { kind: "error", result: { outcome: "failed", status: 200, reason: "not_a_file" } };
    let content: string | null = null;
    if (file.encoding === "base64" && typeof file.content === "string") {
      try {
        content = Buffer.from(file.content.replace(/\s+/gu, ""), "base64").toString("utf8");
      } catch {
        content = null;
      }
    }
    return { kind: "file", sha: file.sha, content };
  };

  const encoded = Buffer.from(markdown, "utf8").toString("base64");
  for (let attempt = 0; attempt < 2; attempt++) {
    const remote = await readRemote();
    if (remote.kind === "error") return emit(remote.result, lastStatus);
    if (remote.kind === "file" && remote.content === markdown) return emit({ outcome: "unchanged" }, lastStatus);
    const sha = remote.kind === "file" ? remote.sha : undefined;
    const body = JSON.stringify({ message: `relayfacts: update ${slug}`, content: encoded, ...(sha ? { sha } : {}), branch: target.branch });
    const res = await request("PUT", url, body);
    if (!res) return emit({ outcome: "failed", reason: "network" }, lastStatus);
    await res.body?.cancel().catch(() => {});
    if (res.status === 200 || res.status === 201) return emit({ outcome: sha ? "updated" : "created" }, res.status);
    // 409: sha mismatch. 422 without sha: the file appeared since the GET. Re-read once, then give up.
    const conflict = res.status === 409 || (res.status === 422 && !sha);
    if (conflict && attempt === 0) continue;
    return emit({ outcome: "failed", status: res.status, reason: conflict ? "conflict" : statusReason(res, "put") }, res.status);
  }
  return emit({ outcome: "failed", reason: "conflict" }, lastStatus);
}

/** Push one topic to every configured target; one target's failure never stops the others. */
export async function pushRelayfactsTopic(
  deps: RelayfactsPushDeps,
  config: RelayfactsPushConfig,
  input: { workspace: string; topic: string; markdown: string },
): Promise<Array<{ target: RelayfactsTarget; result: RelayfactsPushResult }>> {
  // Validate every target before any network call, so a bad config never half-pushes.
  for (const target of config.targets) if (!validTarget(target)) throw new Error("pushRelayfactsTopic: invalid target");
  const scoped: RelayfactsPushDeps = { ...deps, apiBase: config.apiBase };
  const results: Array<{ target: RelayfactsTarget; result: RelayfactsPushResult }> = [];
  for (const target of config.targets) {
    const path = relayfactsDocumentPath(target.dir, input.topic);
    results.push({ target, result: await pushRelayfactsDocument(scoped, { target, path, markdown: input.markdown, topic: input.topic, workspace: input.workspace }) });
  }
  return results;
}
