/**
 * The fixed query set and its oracle. The oracle reads the generator's output, never
 * an engine, so an engine cannot grade itself.
 *
 * Lane A: text every candidate can answer (title, summary, metadata).
 * Lane B: transcript content. The V1 directory is UNSUPPORTED by contract; it is
 *         reported as such, never as a miss or as equivalent.
 * Lane C: filter-only listing, no text term.
 */
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

/** Lane C shapes from docs/session-search-index-contract.md "Indexed remote query shapes". */
export const listQueries = (t: Tenant) => {
  const base = "d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL";
  const order = "ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC LIMIT 50";
  const cols = "SELECT d.session_id FROM sessions.session_directory AS d";
  const args = [t.orgId, t.workspaceId];
  return [
    { id: "workspace-recent", sql: `${cols} WHERE ${base} ${order}`, args },
    { id: "harness", sql: `${cols} WHERE ${base} AND d.harness = 'codex' ORDER BY d.last_activity_at DESC, d.session_id DESC LIMIT 50`, args },
    { id: "project", sql: `${cols} WHERE ${base} AND d.project_id = $3 ${order}`, args: [...args, `${t.orgId}-project-1`] },
    { id: "no-project", sql: `${cols} WHERE ${base} AND d.project_id IS NULL ${order}`, args },
    { id: "state", sql: `${cols} WHERE ${base} AND d.state = 'active' ${order}`, args },
    { id: "agent-exact", sql: `${cols} WHERE ${base} AND d.agent_name_normalized = 'agent-queue-7' ${order}`, args, agent: true },
    { id: "agent-prefix-selective", sql: `${cols} WHERE ${base} AND d.agent_name_normalized LIKE 'agent-queue-1%' ${order}`, args, agent: true },
    { id: "agent-prefix-broad", sql: `${cols} WHERE ${base} AND d.agent_name_normalized LIKE 'age%' ${order}`, args, agent: true },
    { id: "user", sql: `SELECT u.session_id FROM sessions.session_directory_users AS u WHERE u.org_id = $1 AND u.workspace_id = $2 AND u.user_id = $3 ORDER BY u.last_activity_at DESC, u.session_id DESC LIMIT 50`, args: [...args, `${t.orgId}-u1`] },
  ];
};
