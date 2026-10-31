/**
 * Seeded synthetic corpus. No field is derived from production data. The same
 * parameters always produce the same sessions, so a corpus is named by its hash.
 *
 * Shape parameters are assumptions until replaced: pass aggregate production counts
 * (no raw data) through BENCH_SHAPE_FILE; see README.md.
 */
import { createHash } from "node:crypto";

export interface CorpusParams {
  seed: number;
  orgs: number;
  workspacesPerOrg: number;
  sessions: number;
  /** Share of all sessions in org-0, workspace-0 (the whale tenant). */
  whaleShare: number;
  meanEventsPerSession: number;
  multiMachineShare: number;
  tombstoneShare: number;
  summaryReadyShare: number;
  windowDays: number;
  /** Fixed clock, so "last 30 days" is reproducible. */
  now: string;
}

const BASE = {
  seed: 96, orgs: 20, workspacesPerOrg: 3, whaleShare: 0.5, meanEventsPerSession: 12,
  multiMachineShare: 0.05, tombstoneShare: 0.01, summaryReadyShare: 0.8, windowDays: 180,
  now: "2026-10-30T00:00:00.000Z",
};
export const SCALES: Record<string, CorpusParams> = {
  test: { ...BASE, orgs: 3, workspacesPerOrg: 2, sessions: 2_000, meanEventsPerSession: 4 },
  S: { ...BASE, sessions: 20_000 },
  M: { ...BASE, sessions: 200_000 },
  L: { ...BASE, sessions: 1_000_000 },
};

export interface CorpusEvent { eventId: string; ts: Date; content: string; taskTitle: string | null }
export interface CorpusSession {
  orgId: string; workspaceId: string; harness: string; sessionId: string;
  machineIds: string[]; userIds: string[];
  title: string; taskTitle: string | null; agentName: string | null; projectId: string | null;
  gitRepo: string; gitBranch: string; model: string; state: string;
  createdAt: Date; lastActivityAt: Date; deleted: boolean; shortSummary: string | null;
  eventCount: number; events: CorpusEvent[];
}

/** Terms with a known placement. Lane A terms sit in title or summary, lane B in events only. */
export const TERMS = {
  common: "deploy",
  selective: "pelican",
  phrase: "flaky retry loop",
  transcriptOnly: "quokkaledger",
  oldTranscriptOnly: "fossilmarker",
  tombstonedOnly: "erasedterm",
  literalPercent: "100%_done",
  nonAscii: "café-übung",
  cjk: "検索テスト",
  sharedAcrossTenants: "sharedcanary",
} as const;
/** Must never reach any directory column. */
export const PLANTED_SECRET = "rk_live_SYNTHETICbenchmarkSECRET0000";

/** The rare term planted in one tenant only; a hit from any other tenant is a leak. */
export const tenantCanary = (orgId: string, workspaceId: string) => `canary${orgId}${workspaceId}`.replaceAll("-", "");

const WORDS = ["refactor", "migrate", "schema", "worker", "queue", "cache", "index", "parser", "billing",
  "webhook", "router", "session", "token", "metrics", "tracing", "harness", "fixture", "review", "merge", "branch"];
const HARNESSES = ["claude", "codex", "opencode", "gemini", "cursor"];
const MODELS = ["model-a", "model-b", "model-c"];
const STATES = ["done", "done", "done", "active", "abandoned", "unknown"];

/** mulberry32: small, stable across runtimes. */
function prng(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function* generateCorpus(p: CorpusParams): Generator<CorpusSession> {
  const r = prng(p.seed);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(r() * xs.length)]!;
  const phrase = (n: number) => Array.from({ length: n }, () => pick(WORDS)).join(" ");
  const now = Date.parse(p.now);
  for (let i = 0; i < p.sessions; i++) {
    const whale = r() < p.whaleShare;
    const org = whale ? 0 : Math.floor(r() * p.orgs);
    const ws = whale ? 0 : Math.floor(r() * p.workspacesPerOrg);
    const orgId = `org-${org}`;
    const workspaceId = `ws-${org}-${ws}`;
    const harness = pick(HARNESSES);
    const sessionId = `s-${i.toString(36)}`;
    const ageDays = r() * p.windowDays;
    const lastActivityAt = new Date(now - ageDays * 86_400_000 - (i % 1000));
    const createdAt = new Date(lastActivityAt.getTime() - Math.floor(r() * 4 * 3_600_000));
    const deleted = r() < p.tombstoneShare;
    const machines = r() < p.multiMachineShare ? 2 : 1;

    // Placement draws happen for every session, so adding a term never shifts another.
    const draw = { common: r(), selective: r(), phrase: r(), literal: r(), nonAscii: r(), cjk: r(), shared: r(), canary: r(), tx: r() };
    const ready = !deleted && r() < p.summaryReadyShare;
    const titleTerms: string[] = [];
    const summaryTerms: string[] = [];
    (draw.common < 0.3 ? titleTerms : []).push(TERMS.common);
    (draw.selective < 0.005 ? summaryTerms : []).push(TERMS.selective);
    (draw.phrase < 0.01 ? summaryTerms : []).push(TERMS.phrase);
    (draw.literal < 0.002 ? titleTerms : []).push(TERMS.literalPercent);
    (draw.nonAscii < 0.002 ? titleTerms : []).push(TERMS.nonAscii);
    (draw.cjk < 0.002 ? titleTerms : []).push(TERMS.cjk);
    (draw.shared < 0.01 ? titleTerms : []).push(TERMS.sharedAcrossTenants);
    (draw.canary < 0.02 ? titleTerms : []).push(tenantCanary(orgId, workspaceId));
    if (deleted) titleTerms.push(TERMS.tombstonedOnly);

    const count = Math.max(1, Math.round(r() * 2 * p.meanEventsPerSession));
    const events: CorpusEvent[] = Array.from({ length: count }, (_, n) => {
      const parts = [phrase(30)];
      if (n === 0 && draw.tx < 0.01) parts.push(ageDays > 45 ? TERMS.oldTranscriptOnly : TERMS.transcriptOnly);
      if (n === 0 && draw.tx > 0.99) parts.push(PLANTED_SECRET);
      return { eventId: `${sessionId}-e${n}`, ts: new Date(createdAt.getTime() + n * 1000), content: parts.join(" "), taskTitle: null };
    });
    yield {
      orgId, workspaceId, harness, sessionId,
      machineIds: Array.from({ length: machines }, (_, m) => `${orgId}-m${(i + m) % 40}`),
      userIds: [`${orgId}-u${i % 25}`],
      title: `${phrase(4)} ${titleTerms.join(" ")}`.trim().slice(0, 200),
      taskTitle: r() < 0.4 ? phrase(3) : null,
      agentName: r() < 0.7 ? `agent-${pick(WORDS)}-${i % 50}` : null,
      projectId: r() < 0.7 ? `${orgId}-project-${i % 30}` : null,
      gitRepo: `example/repo${i % 40}`,
      gitBranch: `feature/${i % 500}`,
      model: pick(MODELS),
      state: pick(STATES),
      createdAt, lastActivityAt, deleted,
      shortSummary: ready ? `${phrase(60)} ${summaryTerms.join(" ")}`.trim().slice(-1000) : null,
      eventCount: count, events,
    };
  }
}

/** The text sessions.session_directory_search_text builds. */
export function searchDocument(s: CorpusSession, withSummary = true): string {
  if (s.deleted) return "";
  return [s.title, s.taskTitle, s.agentName, s.projectId, s.gitRepo, s.gitBranch, s.model, withSummary ? s.shortSummary : null]
    .filter((v) => v && v.trim()).join(" ").slice(0, 8192);
}

export function corpusHash(p: CorpusParams): string {
  const h = createHash("sha256").update(JSON.stringify(p));
  for (const s of generateCorpus(p)) h.update(JSON.stringify(s));
  return h.digest("hex");
}
