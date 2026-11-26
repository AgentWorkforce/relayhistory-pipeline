/**
 * A collection's scope, bound before anything is collected: the tenant, repositories, window and delete-by date
 * (docs/proposals/relayfacts-features.md, "Corpus"). `<out>/scope.json` is written by the first collecting
 * command; every later command on the same directory (resumes, other collectors, seal) must name the same scope,
 * and nothing runs past the delete-by date.
 */
import type { PrivateDir } from "../out-path.ts";

export const SCOPE_FORMAT = "relayfacts-scope/1";
export const MAX_RETENTION_DAYS = 30;

export interface CollectionScope {
  format: typeof SCOPE_FORMAT;
  orgId: string;
  workspaceId: string;
  repos: string[];
  since: string;
  until: string;
  deleteBy: string;
}

const iso = (v: unknown): v is string => typeof v === "string" && Number.isFinite(Date.parse(v)) && /Z$/u.test(v);

export class ScopeError extends Error {
  constructor(message: string) { super(message); this.name = "ScopeError"; }
}

function check(s: CollectionScope, now: Date): void {
  if (!iso(s.since) || !iso(s.until) || s.since >= s.until) throw new ScopeError("scope: --since and --until must be UTC times, since before until");
  if (!iso(s.deleteBy)) throw new ScopeError("scope: --delete-by must be a UTC time");
  const left = Date.parse(s.deleteBy) - now.getTime();
  if (left <= 0) throw new ScopeError("scope: past its delete-by date: delete the collection");
  if (left > MAX_RETENTION_DAYS * 86_400_000) throw new ScopeError(`scope: --delete-by must be within ${MAX_RETENTION_DAYS} days`);
  if (!s.orgId || !s.workspaceId || !s.repos.length) throw new ScopeError("scope: tenant and repositories are required");
}

/**
 * Bind `dir` to `want`: create scope.json when absent (deleteBy required), else require the same tenant,
 * repositories and window (and the same deleteBy when one is given). Returns the bound scope.
 */
export function bindScope(dir: PrivateDir, want: Omit<CollectionScope, "format" | "deleteBy"> & { deleteBy?: string }, now: Date = new Date()): CollectionScope {
  const repos = [...new Set(want.repos)].sort();
  if (dir.has("scope.json")) {
    const have = JSON.parse(dir.read("scope.json")) as CollectionScope;
    if (have.format !== SCOPE_FORMAT) throw new ScopeError("scope.json: unknown format");
    const same = have.orgId === want.orgId && have.workspaceId === want.workspaceId && have.since === want.since && have.until === want.until
      && JSON.stringify([...have.repos].sort()) === JSON.stringify(repos) && (want.deleteBy === undefined || want.deleteBy === have.deleteBy);
    if (!same) throw new ScopeError("this directory was collected for another scope (tenant, repositories, window or delete-by)");
    check(have, now);
    return have;
  }
  if (!want.deleteBy) throw new ScopeError("--delete-by is required before collecting");
  const scope: CollectionScope = { format: SCOPE_FORMAT, orgId: want.orgId, workspaceId: want.workspaceId, repos, since: want.since, until: want.until, deleteBy: want.deleteBy };
  check(scope, now);
  dir.write("scope.json", JSON.stringify(scope, null, 2));
  return scope;
}

/** The bound scope of an existing collection (seal), checked for retention; tenant-free callers use this. */
export function readScope(dir: PrivateDir, now: Date = new Date()): CollectionScope {
  if (!dir.has("scope.json")) throw new ScopeError("no scope.json: collect with a bound scope first");
  const have = JSON.parse(dir.read("scope.json")) as CollectionScope;
  if (have.format !== SCOPE_FORMAT) throw new ScopeError("scope.json: unknown format");
  check(have, now);
  return have;
}
