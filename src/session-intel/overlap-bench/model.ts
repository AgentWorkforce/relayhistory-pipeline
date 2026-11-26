/**
 * The benchmark's world model (docs/session-intel-overlap-bench.md, "Model"): sessions grouped into work roots,
 * with time-ordered edits, PR references and relay messages, built from a Cloud harvest. Pure: callers load files.
 */
import type { EventFacts } from "../cloud-harvest/features.ts";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import { HOT_PATH, repoOf, repoRelative } from "./paths.ts";

/** `repo` is the edit's own repository: taken from an absolute path when it names one, else the session's. */
export interface Edit { ts: string; repo: string; path: string; via: string; hot: boolean }
export interface PrRef { ts: string; repo: string | null; number: number; action: "create" | "mention" }
export interface Message { ts: string; from: string; to: string }

export interface Session {
  key: string;
  source: string;
  sessionId: string;
  owner: string | null;
  repo: string | null;
  root: string;
  parents: string[];
  startedAt: string;
  lastTs: string;
  edits: Edit[];
  prs: PrRef[];
  branches: Array<{ ts: string; name: string }>;
  shas: Array<{ ts: string; sha: string }>;
  received: Message[];
  /** Relay names this session answers to (the `to` of messages it received). */
  names: Set<string>;
}

export const sessionKey = (source: string, sessionId: string) => `${source}:${sessionId}`;

function projectName(row: DirectoryRow): string | null {
  const p = row.project as { name?: unknown; detail?: unknown } | null | undefined;
  return repoOf(typeof p?.name === "string" ? p.name : null) ?? repoOf(typeof p?.detail === "string" ? p.detail : null) ?? repoOf(row.projectId as string | null);
}

const PR_KEY = /^([a-z0-9._-]+\/[a-z0-9._-]+)?#(\d+)$/i;

/** A `gh pr create` call's PR URL arrives in the tool result after it; a result this soon after claims the number. */
const CREATE_RESULT_MS = 10 * 60_000;

/** Events stamped before this (epoch zero, clock faults) carry no usable time and are dropped. */
export const EARLIEST_TS = "2025-01-01T00:00:00.000Z";

/**
 * How paths a session only observed are treated. `git status` and `git diff --stat` list what is dirty or changed,
 * not what this session wrote: a checkout left dirty by earlier work repeats the same paths in every session that
 * looks at it, and `--stat` often shows someone else's commit. `all` counts them as edits; `fresh` keeps only
 * `git status` paths that the session's first status did not already list (the dirt it started with) and drops
 * `--stat` paths (a path the session wrote is already counted from the write); `none` drops observed paths.
 */
export type ObservedPolicy = "all" | "fresh" | "none";
const OBSERVED = new Set(["status", "stat"]);

function fromFacts(s: Session, events: EventFacts[], observed: ObservedPolicy) {
  let pendingCreate: { ts: string; index: number } | null = null;
  let baseline: Set<string> | null = null;
  for (const e of events) {
    if (!(e.ts >= EARLIEST_TS) || !Number.isFinite(Date.parse(e.ts))) continue;
    const seen = e.touched.filter((t) => t.via === "status").map((t) => t.path);
    const first = baseline === null && seen.length > 0;
    for (const path of [...new Set([...e.files, ...e.touched.map((t) => t.path)])]) {
      const via = e.files.includes(path) ? "files" : e.touched.find((t) => t.path === path)!.via;
      if (OBSERVED.has(via)) {
        if (observed === "none") continue;
        if (observed === "fresh" && (via === "stat" || first || baseline?.has(path))) continue;
      }
      const repo = (path.startsWith("/") || path.startsWith("~") ? repoOf(path) : null) ?? s.repo;
      const rel = repo ? repoRelative(path, repo) : null;
      if (repo && rel && !rel.endsWith("/")) s.edits.push({ ts: e.ts, repo, path: rel, via, hot: HOT_PATH.test(rel) });
    }
    if (first) baseline = new Set(seen);
    const creates = e.gh.includes("pr-create");
    if (pendingCreate && Date.parse(e.ts) - Date.parse(pendingCreate.ts) > CREATE_RESULT_MS) pendingCreate = null;
    for (const ref of e.prRefs) {
      const m = PR_KEY.exec(ref);
      if (!m) continue;
      const repo = m[1] ? m[1].toLowerCase() : s.repo?.toLowerCase() ?? null;
      // The first full PR URL after a create call is the PR it created; it replaces the placeholder.
      if (!creates && pendingCreate && m[1]) {
        s.prs[pendingCreate.index] = { ts: s.prs[pendingCreate.index]!.ts, repo, number: Number(m[2]), action: "create" };
        pendingCreate = null;
        continue;
      }
      s.prs.push({ ts: e.ts, repo, number: Number(m[2]), action: creates ? "create" : "mention" });
    }
    if (creates && !e.prRefs.length) {
      s.prs.push({ ts: e.ts, repo: s.repo?.toLowerCase() ?? null, number: -1, action: "create" });
      pendingCreate = { ts: e.ts, index: s.prs.length - 1 };
    }
    for (const b of e.branches) s.branches.push({ ts: e.ts, name: b });
    for (const sha of e.shas) s.shas.push({ ts: e.ts, sha: sha.toLowerCase() });
    if (e.relay) { s.received.push({ ts: e.ts, from: e.relay.from, to: e.relay.to }); s.names.add(e.relay.to); }
  }
}

/** Builds sessions and resolves each one's work root by following same-source parents. */
export function buildModel(rows: DirectoryRow[], facts: SessionFacts[], observed: ObservedPolicy = "all"): Map<string, Session> {
  const sessions = new Map<string, Session>();
  for (const r of rows) {
    const key = sessionKey(r.source, r.sessionId);
    const users = (r.userIds as string[] | null) ?? [];
    sessions.set(key, {
      key, source: r.source, sessionId: r.sessionId, owner: users[0] ?? null, repo: projectName(r),
      root: key, parents: ((r.parentSessionIds as string[] | null) ?? []).map((p) => sessionKey(r.source, p)),
      startedAt: String(r.createdAt ?? r.lastTs), lastTs: r.lastTs, edits: [], prs: [], branches: [], shas: [], received: [], names: new Set(),
    });
    const pr = r.pullRequest as { url?: string; number?: string | number } | null;
    if (pr?.number) sessions.get(key)!.prs.push({ ts: r.lastTs, repo: repoOf(pr.url ?? null)?.toLowerCase() ?? null, number: Number(pr.number), action: "mention" });
  }
  for (const f of facts) {
    const s = sessions.get(sessionKey(f.source, f.sessionId));
    if (s) fromFacts(s, f.events, observed);
  }
  for (const s of sessions.values()) {
    const seen = new Set<string>();
    let cur = s;
    while (cur.parents.length && !seen.has(cur.key)) {
      seen.add(cur.key);
      const parent = sessions.get(cur.parents[0]!);
      if (!parent) { s.root = cur.parents[0]!; break; }
      cur = parent; s.root = cur.key;
    }
    s.edits.sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0));
  }
  return sessions;
}

/**
 * Pairs that communicated over Agent Relay before `at`: one received a message from a name the other answers to.
 * Keyed by work root, so a lead messaging a worker covers both roots.
 */
export function communicated(sessions: Map<string, Session>): (rootA: string, rootB: string, at: string) => boolean {
  const byName = new Map<string, Set<string>>();
  for (const s of sessions.values()) for (const n of s.names) byName.set(n, (byName.get(n) ?? new Set()).add(s.root));
  const first = new Map<string, string>();
  for (const s of sessions.values()) for (const m of s.received) for (const other of byName.get(m.from) ?? []) {
    if (other === s.root) continue;
    const k = [s.root, other].sort().join("\u0001");
    if (!first.has(k) || m.ts < first.get(k)!) first.set(k, m.ts);
  }
  return (a, b, at) => { const t = first.get([a, b].sort().join("\u0001")); return t !== undefined && t < at; };
}
