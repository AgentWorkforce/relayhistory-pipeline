/**
 * The relayfacts benchmark corpus (docs/proposals/relayfacts-features.md, "The benchmark"): one
 * repository over a window, frozen. A directory kept outside this repository:
 *
 *   manifest.json   {"format": "relayfacts-corpus/1", "repo", "window": {since, until}, "createdAt",
 *                    "deleteBy", "files": {"sessions.jsonl": sha256, "forge.json": sha256}}
 *   sessions.jsonl  one {"source", "sessionId", "context"?, "snapshot"} per line: a session analysis
 *                   snapshot and the directory facts it lacks
 *   forge.json      ForgeFacts: the repository's PRs, commits (default branch, reverts) and flag defaults
 *
 * The loader refuses a corpus past its delete-by date, a file whose sha256 differs, a symlink, an
 * unknown field, and anything over its bounds. It never writes.
 */
import { createHash } from "node:crypto";
import { readPrivateBytes } from "../out-path.ts";
import { UsageError } from "../errors.ts";
import { join } from "node:path";
import type { SessionAnalysisSnapshot } from "../../session-analysis/engine/session-analysis-result.ts";
import { utc, utcForge, type ForgeFacts, type SessionContext } from "../features/types.ts";
import type { SessionArtifacts } from "../backfill/artifacts.ts";

export const CORPUS_FORMAT = "relayfacts-corpus/1";
export const CORPUS_BOUNDS = { fileBytes: 512 * 1024 * 1024, lineBytes: 16 * 1024 * 1024, sessions: 50_000, deleteByDays: 30, parents: 16 } as const;

export class CorpusError extends Error {
  constructor(message: string) { super(message); this.name = "CorpusError"; }
}

export interface CorpusSession {
  source: string;
  sessionId: string;
  context: SessionContext;
  snapshot: SessionAnalysisSnapshot;
  /** Build evidence from the session's canonical events (backfill/artifacts.ts); absent when not harvested. */
  artifacts?: SessionArtifacts;
}

export interface CorpusManifest {
  format: typeof CORPUS_FORMAT;
  repo: string;
  window: { since: string; until: string };
  createdAt: string;
  deleteBy: string;
  files: Record<string, string>;
}

export interface Corpus {
  manifest: CorpusManifest;
  /** sha256 of manifest.json: what gold labels name. */
  manifestSha256: string;
  sessions: CorpusSession[];
  forge: ForgeFacts;
}

export const sha256Hex = (bytes: string | Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const iso = (v: unknown) => typeof v === "string" && utc(v) !== null;

function readFile(dir: string, name: string): Buffer {
  if (!/^[a-z][a-z0-9_.-]{0,63}$/u.test(name) || name.includes("..")) throw new CorpusError(`bad file name: ${name}`);
  // Opened without following a link; the descriptor must be a private regular file (out-path.ts).
  try { return readPrivateBytes(join(dir, name), CORPUS_BOUNDS.fileBytes); } catch (error) { throw new CorpusError(`${name}: ${error instanceof UsageError ? error.message : "unreadable"}`); }
}

function only(obj: Record<string, unknown>, allowed: string[], where: string) {
  for (const k of Object.keys(obj)) if (!allowed.includes(k)) throw new CorpusError(`${where}: unknown field`);
}

export function parseForge(raw: unknown): ForgeFacts {
  if (!raw || typeof raw !== "object") throw new CorpusError("forge.json: not an object");
  const f = raw as Record<string, unknown>;
  const positions = (v: unknown) => v === undefined || (Array.isArray(v) && v.length <= 200 && v.every((n) => Number.isInteger(n) && (n as number) >= 0));
  only(f, ["defaultBranch", "prs", "commits", "flags", "branches"], "forge.json");
  if (!Array.isArray(f.prs) || !Array.isArray(f.commits) || !Array.isArray(f.flags)) throw new CorpusError("forge.json: prs, commits and flags must be arrays");
  for (const p of f.prs as Record<string, unknown>[]) {
    only(p, ["ref", "state", "at", "mergedAt", "closedAt", "branch", "base", "mergeCommit", "landedAt", "commits", "headSameRepo"], "forge.json pr");
    if (typeof p.ref !== "string" || !["open", "merged", "closed"].includes(p.state as string) || !iso(p.at)) throw new CorpusError("forge.json: bad pr");
    // A landing is only a merged PR's, and only with its merge commit.
    if (p.base !== undefined && typeof p.base !== "string") throw new CorpusError("forge.json: bad pr base");
    if (p.headSameRepo !== undefined && typeof p.headSameRepo !== "boolean") throw new CorpusError("forge.json: bad pr headSameRepo");
    if (p.mergeCommit !== undefined && (p.state !== "merged" || typeof p.mergeCommit !== "string" || !/^[0-9a-f]{40}$/u.test(p.mergeCommit))) throw new CorpusError("forge.json: bad pr merge commit");
    if (p.landedAt !== undefined && (p.mergeCommit === undefined || (p.landedAt !== null && !iso(p.landedAt)))) throw new CorpusError("forge.json: a pr landing needs its merge commit and a valid time");
    if (p.commits !== undefined && (!Array.isArray(p.commits) || p.commits.length === 0 || p.commits.length > 200 || p.commits.some((c) => typeof c !== "string" || !/^[0-9a-f]{40}$/u.test(c))))
      throw new CorpusError("forge.json: a pr's commits must be 1 to 200 full commit ids");
    // A PR's commit list is dated only by its merge; an unmerged PR's final list says nothing about a cut.
    if (p.commits !== undefined && (p.state !== "merged" || !iso(p.mergedAt))) throw new CorpusError("forge.json: only a pr merged at a valid time records commits");
  }
  for (const c of f.commits as Record<string, unknown>[]) {
    only(c, ["sha", "at", "onDefaultBranch", "defaultAt", "defaultAtBasis", "revertedBy", "revertedAt", "transitions"], "forge.json commit");
    if (c.revertedAt !== undefined && c.revertedAt !== null && !iso(c.revertedAt)) throw new CorpusError("forge.json: bad commit");
    if (c.transitions !== undefined && (!Array.isArray(c.transitions) || c.transitions.length > 200 || c.transitions.some((t: unknown) => {
      const x = t as Record<string, unknown>;
      return !x || typeof x !== "object" || Object.keys(x).some((k) => !["at", "kind", "by", "seq", "step", "unorderedWith", "unordered"].includes(k)) || (x.unordered !== undefined && x.unordered !== true) || [x.seq, x.step].some((n) => n !== undefined && !(Number.isInteger(n) && (n as number) >= 0)) || !positions(x.unorderedWith) || !iso(x.at) || !["landed", "reverted"].includes(x.kind as string) || typeof x.by !== "string" || !/^[0-9a-f]{7,40}$/u.test(x.by);
    }))) throw new CorpusError("forge.json: bad commit");
    if (c.defaultAtBasis !== undefined && !["merge", "patch", "committer"].includes(c.defaultAtBasis as string)) throw new CorpusError("forge.json: bad commit");
    if (c.defaultAt !== undefined && c.defaultAt !== null && !iso(c.defaultAt)) throw new CorpusError("forge.json: bad commit");
    if (typeof c.sha !== "string" || !/^[0-9a-f]{7,40}$/u.test(c.sha) || !iso(c.at) || typeof c.onDefaultBranch !== "boolean") throw new CorpusError("forge.json: bad commit");
  }
  for (const g of f.flags as Record<string, unknown>[]) {
    only(g, ["name", "defaultOn"], "forge.json flag");
    if (typeof g.name !== "string" || typeof g.defaultOn !== "boolean") throw new CorpusError("forge.json: bad flag");
  }
  for (const b of (f.branches ?? []) as Record<string, unknown>[]) {
    only(b, ["name", "landed", "landedAt", "landedSeq", "landedStep", "landedUnorderedWith", "tipAt", "flagsAdded", "commits"], "forge.json branch");
    if ([b.landedSeq, b.landedStep].some((n) => n !== undefined && !(Number.isInteger(n) && (n as number) >= 0)) || !positions(b.landedUnorderedWith)) throw new CorpusError("forge.json: bad branch");
    if (typeof b.name !== "string" || typeof b.landed !== "boolean" || !iso(b.tipAt) || (b.landedAt !== null && !iso(b.landedAt))) throw new CorpusError("forge.json: bad branch");
    if (b.commits !== undefined && (!Array.isArray(b.commits) || b.commits.length > 200 || b.commits.some((c) => typeof c !== "string" || !/^[0-9a-f]{40}$/u.test(c))))
      throw new CorpusError("forge.json: a branch's commits must be at most 200 full commit ids");
  }
  try { return utcForge(f as unknown as ForgeFacts); } catch { throw new CorpusError("forge.json: a time is not a valid instant"); }
}

export function loadCorpus(dir: string, now: Date = new Date()): Corpus {
  const manifestBytes = readFile(dir, "manifest.json");
  const manifest = JSON.parse(manifestBytes.toString("utf8")) as CorpusManifest;
  only(manifest as unknown as Record<string, unknown>, ["format", "repo", "window", "createdAt", "deleteBy", "files"], "manifest.json");
  if (manifest.format !== CORPUS_FORMAT) throw new CorpusError(`manifest.json: format must be ${CORPUS_FORMAT}`);
  if (!iso(manifest.deleteBy) || !iso(manifest.createdAt) || !iso(manifest.window?.since) || !iso(manifest.window?.until)) throw new CorpusError("manifest.json: bad times");
  if (Date.parse(manifest.deleteBy) <= now.getTime()) throw new CorpusError("corpus is past its delete-by date: delete it");
  if (Date.parse(manifest.deleteBy) - Date.parse(manifest.createdAt) > CORPUS_BOUNDS.deleteByDays * 86_400_000) throw new CorpusError(`deleteBy is more than ${CORPUS_BOUNDS.deleteByDays} days after createdAt`);
  for (const name of ["sessions.jsonl", "forge.json"]) if (typeof manifest.files?.[name] !== "string") throw new CorpusError(`manifest.json: files must list ${name}`);
  const verified = (name: string) => {
    const bytes = readFile(dir, name);
    if (sha256Hex(bytes) !== manifest.files[name]) throw new CorpusError(`${name}: sha256 differs from the manifest`);
    return bytes.toString("utf8");
  };
  const sessions: CorpusSession[] = [];
  const seen = new Set<string>();
  for (const [i, line] of verified("sessions.jsonl").split("\n").entries()) {
    if (!line.trim()) continue;
    if (Buffer.byteLength(line) > CORPUS_BOUNDS.lineBytes) throw new CorpusError(`sessions.jsonl:${i + 1}: line over bounds`);
    const row = JSON.parse(line) as Record<string, unknown>;
    only(row, ["source", "sessionId", "context", "snapshot", "artifacts"], `sessions.jsonl:${i + 1}`);
    const snapshot = row.snapshot as SessionAnalysisSnapshot;
    if (typeof row.source !== "string" || typeof row.sessionId !== "string" || !snapshot || snapshot.source !== row.source || snapshot.sessionId !== row.sessionId)
      throw new CorpusError(`sessions.jsonl:${i + 1}: source and sessionId must match the snapshot`);
    const key = `${row.source}\u0000${row.sessionId}`;
    if (seen.has(key)) throw new CorpusError(`sessions.jsonl:${i + 1}: duplicate session`);
    seen.add(key);
    if (seen.size > CORPUS_BOUNDS.sessions) throw new CorpusError("too many sessions");
    const context = (row.context ?? {}) as Record<string, unknown>;
    only(context, ["repo", "branch", "taskRef", "userRole", "parentSessionIds"], `sessions.jsonl:${i + 1} context`);
    const parents = context.parentSessionIds;
    if (parents !== undefined && (!Array.isArray(parents) || parents.length > CORPUS_BOUNDS.parents || parents.some((p) => typeof p !== "string" || !p || p.length > 200)))
      throw new CorpusError(`sessions.jsonl:${i + 1}: context.parentSessionIds must be at most ${CORPUS_BOUNDS.parents} session ids`);
    sessions.push({ source: row.source, sessionId: row.sessionId, context: context as SessionContext, snapshot, ...(row.artifacts ? { artifacts: row.artifacts as SessionArtifacts } : {}) });
  }
  const forge = parseForge(JSON.parse(verified("forge.json")));
  return { manifest, manifestSha256: sha256Hex(manifestBytes), sessions, forge };
}
