/**
 * The fixed query set and its oracle. The oracle reads the generator's output, never
 * an engine, so an engine cannot grade itself.
 *
 * Lane A: text every candidate can answer (title, summary, metadata).
 * Lane B: transcript content. The V1 directory is UNSUPPORTED by contract; it is
 *         reported as such, never as a miss or as equivalent.
 * Lane C: filter-only listing, no text term.
 */
import { agentPrefixPattern, normalizeAgentName, SESSION_DIRECTORY_QUERIES } from "../../src/hoopdb/search-index-contract.ts";
import { TERMS, searchDocument, tenantCanary, type CorpusSession } from "./corpus.ts";

export interface Tenant { orgId: string; workspaceId: string }
export const WHALE: Tenant = { orgId: "org-0", workspaceId: "ws-0-0" };
export const SMALL: Tenant = { orgId: "org-1", workspaceId: "ws-1-1" };
export const PAGE = 50;

export interface TextQuery { id: string; lane: "A" | "B"; term: string }

export const textQueries = (t: Tenant): TextQuery[] => [
  { id: "common", lane: "A", term: TERMS.common },
  { id: "selective", lane: "A", term: TERMS.selective },
  { id: "phrase", lane: "A", term: TERMS.phrase },
  { id: "zero-hit", lane: "A", term: "nosuchtermanywhere" },
  { id: "literal-wildcards", lane: "A", term: TERMS.literalPercent },
  { id: "non-ascii", lane: "A", term: TERMS.nonAscii },
  { id: "cjk", lane: "A", term: TERMS.cjk },
  { id: "tenant-canary", lane: "A", term: tenantCanary(t.orgId, t.workspaceId) },
  { id: "shared-term", lane: "A", term: TERMS.sharedAcrossTenants },
  { id: "tombstoned-only", lane: "A", term: TERMS.tombstonedOnly },
  { id: "transcript-only", lane: "B", term: TERMS.transcriptOnly },
  { id: "transcript-old", lane: "B", term: TERMS.oldTranscriptOnly },
];

/** Exact case-insensitive substring over visible, live sessions, newest first. */
export function oracleLaneA(corpus: readonly CorpusSession[], t: Tenant, term: string, withSummary = true) {
  const needle = term.toLowerCase();
  const hits = corpus
    .filter((s) => s.orgId === t.orgId && s.workspaceId === t.workspaceId && !s.deleted)
    .filter((s) => searchDocument(s, withSummary).toLowerCase().includes(needle))
    .sort(compareNewest);
  return { total: hits.length, page: hits.slice(0, PAGE).map(identity) };
}

/** Lane B: sessions with the term in the first 4096 chars of any event of the tenant. */
export function oracleLaneB(corpus: readonly CorpusSession[], t: Tenant, term: string): Set<string> {
  return new Set(corpus
    .filter((s) => s.orgId === t.orgId && s.workspaceId === t.workspaceId && !s.deleted)
    .filter((s) => s.events.some((e) => e.content.slice(0, 4096).includes(term)))
    .map(identity));
}

export const identity = (s: { harness: string; sessionId: string }) => `${s.harness}/${s.sessionId}`;

function compareNewest(a: CorpusSession, b: CorpusSession): number {
  return b.lastActivityAt.getTime() - a.lastActivityAt.getTime()
    || (a.sessionId < b.sessionId ? 1 : a.sessionId > b.sessionId ? -1 : 0)
    || (a.harness < b.harness ? 1 : a.harness > b.harness ? -1 : 0);
}

export interface ListQuery {
  id: string; sql: string; args: unknown[];
  /** The contract's index for this shape; at small scale the planner may pick another. */
  index: string;
  agent?: "exact" | "selective-prefix" | "broad-prefix";
}

/** The contract's texts. Only the projection changes: `authenticated` may not read `search_text`. */
const contract = (name: keyof typeof SESSION_DIRECTORY_QUERIES) => SESSION_DIRECTORY_QUERIES[name]
  .replace("SELECT d.*", "SELECT d.session_id, d.harness").replace("SELECT *", "SELECT session_id, harness");

const newest = <T>(sessions: CorpusSession[], value: (s: CorpusSession) => T | null | undefined): T => {
  for (const s of sessions) {
    const v = value(s);
    if (v !== null && v !== undefined) return v;
  }
  throw new Error("corpus has no value for this filter");
};

/**
 * Every shape in "Indexed remote query shapes". Filter values come from the tenant's own
 * rows, so each query has something to return. The contract has no text for org-recent
 * or session-ID lookup; those two are written from REQUIRED_SEARCH_INDEXES.
 */
export function listQueries(corpus: readonly CorpusSession[], t: Tenant): ListQuery[] {
  const live = corpus.filter((s) => s.orgId === t.orgId && s.workspaceId === t.workspaceId && !s.deleted)
    .sort((a, b) => b.lastActivityAt.getTime() - a.lastActivityAt.getTime());
  const tenant = [t.orgId, t.workspaceId];
  const agent = newest(live, (s) => s.agentName)!;
  const project = newest(live, (s) => s.projectId)!;
  const middle = live[Math.floor(live.length / 2)]!.createdAt.getTime();
  return [
    { id: "workspace-recent", sql: contract("workspaceRecent"), args: [...tenant, PAGE], index: "session_directory_workspace_recent_idx" },
    { id: "org-recent", index: "session_directory_org_recent_idx", args: [t.orgId, PAGE], sql: `
SELECT session_id, harness FROM sessions.session_directory
 WHERE org_id = $1 AND deleted_at IS NULL
 ORDER BY last_activity_at DESC, session_id DESC, harness DESC, workspace_id DESC
 LIMIT $2` },
    { id: "harness", sql: contract("harnessRecent"), args: [...tenant, newest(live, (s) => s.harness), PAGE], index: "session_directory_harness_recent_idx" },
    { id: "project", sql: contract("projectRecent"), args: [...tenant, project, PAGE], index: "session_directory_project_recent_idx" },
    { id: "no-project", sql: contract("withoutProjectRecent"), args: [...tenant, PAGE], index: "session_directory_no_project_recent_idx" },
    { id: "agent-exact", agent: "exact", sql: contract("agentExact"), args: [...tenant, normalizeAgentName(agent), PAGE], index: "session_directory_agent_recent_idx" },
    { id: "agent-prefix-selective", agent: "selective-prefix", sql: contract("agentPrefix"), args: [...tenant, agentPrefixPattern(agent.slice(0, -1)), PAGE], index: "session_directory_agent_recent_idx" },
    { id: "agent-prefix-broad", agent: "broad-prefix", sql: contract("agentPrefix"), args: [...tenant, agentPrefixPattern("age"), PAGE], index: "session_directory_agent_recent_idx" },
    { id: "created-range", sql: contract("createdRange"), args: [...tenant, new Date(middle - 7 * 86_400_000), new Date(middle + 7 * 86_400_000), PAGE], index: "session_directory_created_recent_idx" },
    { id: "state", sql: contract("stateRecent"), args: [...tenant, newest(live, (s) => s.state), PAGE], index: "session_directory_state_recent_idx" },
    { id: "session-id", index: "session_directory_session_lookup_idx", args: [t.orgId, live[0]!.sessionId], sql: `
SELECT session_id, harness FROM sessions.session_directory
 WHERE org_id = $1 AND session_id = $2` },
    { id: "user", sql: contract("userRecent"), args: [...tenant, newest(live, (s) => s.userIds[0]), PAGE], index: "session_directory_users_recent_idx" },
    { id: "machine", sql: contract("machineRecent"), args: [...tenant, newest(live, (s) => s.machineIds[0]), PAGE], index: "session_directory_machines_recent_idx" },
    { id: "project-substring", sql: contract("projectContains"), args: [...tenant, project.slice(-9), PAGE], index: "session_directory_project_trgm_idx" },
  ];
}
