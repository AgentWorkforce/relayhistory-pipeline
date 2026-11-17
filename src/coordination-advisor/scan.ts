/**
 * One advisor scan (docs/coordination-live-advisories.md, "Detection"): candidate lanes from the
 * session directory, facts through a CoordinationBackend, the detector, the rubric, the limits, the
 * writes and the HIGH DMs. Writes go only through sessions.coordination_advisories_upsert_v1,
 * _mark_delivered_v1 and _sweep_v1 (supabase/migrations/20261105062000_coordination_advisories.sql).
 * Before writing, it seeds the limits from _recent_v1 (20261105062070) so the cooldown and cap survive a
 * restart. It reads no transcript body.
 */
import { detectCoordination } from "../../packages/coordination-detect/src/index.ts";
import type { CoordinationBackend } from "../session-read-api/coordination-backend.ts";
import type { Directory, DirectoryListRow } from "../session-read-api/directory.ts";
import type { SqlQuery } from "../session-read-api/liveness.ts";
import { errorFields, log } from "../shared/log.ts";
import type { RelaySender } from "./relay-sender.ts";
import {
  ADVISOR_DETECTOR_PARAMS, AdvisorLimits, CANDIDATE_WINDOW_MS, highAdvisories, lowAdvisories, orderAdvisories,
  pairKey, refKey, truncatedStreams, type Advisory, type CandidateLane, type RecentAdvisory, type SessionRef,
} from "./rubric.ts";
import { advisoryMessage } from "./template.ts";

/** Directory rows read per scan; more is logged and the newest are kept. */
export const CANDIDATE_LIMIT = 2_000;
export const FACTS_TIMEOUT_MS = 30_000;
export const DETECTOR_LIMIT = 500;
export const SWEEP_EVERY_MS = 60 * 60 * 1_000;

const PARENTS_SQL = `ARRAY(SELECT DISTINCT r.parent_session_id
           FROM sessions.session_directory_relationships AS r
          WHERE r.org_id = d.org_id AND r.workspace_id = d.workspace_id AND r.harness = d.harness
            AND r.child_session_id = d.session_id
          ORDER BY 1 LIMIT 64)`;

/** Live (not deleted) directory sessions active since $1, with a project. Newest first; $2 = limit. */
export const CANDIDATES_SQL = `SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.project_id, d.git_repo,
       d.short_summary, d.last_activity_at, ${PARENTS_SQL} AS parent_session_ids
  FROM sessions.session_directory AS d
 WHERE d.deleted_at IS NULL AND d.project_id IS NOT NULL AND d.last_activity_at >= $1
 ORDER BY d.last_activity_at DESC, d.org_id, d.workspace_id, d.harness, d.session_id
 LIMIT $2`;

/** The directory rows normalizeCoordinationFacts reads (git_repo, branch, title, parents, times). */
export const DIRECTORY_KEYS_SQL = `SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,
       d.title, d.project_id, d.git_repo, d.git_branch, d.short_summary, ${PARENTS_SQL} AS parent_session_ids
  FROM sessions.session_directory AS d
  JOIN unnest($3::text[], $4::text[]) AS k(h, s) ON d.harness = k.h AND d.session_id = k.s
 WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL`;

export const UPSERT_SQL = "SELECT inserted, upgraded, skipped FROM sessions.coordination_advisories_upsert_v1($1, $2, $3::jsonb)";
export const MARK_DELIVERED_SQL = "SELECT sessions.coordination_advisories_mark_delivered_v1($1, $2, $3, $4, $5) AS matched";
export const SWEEP_SQL = "SELECT expired, deleted FROM sessions.coordination_advisories_sweep_v1()";
export const RECENT_SQL = `SELECT row_kind, harness, session_id, peer_harness, peer_session_id, latest_at, sent_last_hour
  FROM sessions.coordination_advisories_recent_v1($1, $2, $3)`;

const iso = (v: Date | string) => v instanceof Date ? v.toISOString() : new Date(v).toISOString();

/** The worker's Directory: only `list` by session keys, the one call readFacts makes. */
export function workerDirectory(db: SqlQuery): Directory {
  const unsupported = () => { throw new Error("coordination advisor directory: only keyed list is supported"); };
  return {
    async list(orgId, workspaceId, args) {
      const keys = args.sessionKeys ?? [];
      if (!keys.length) return [];
      const { rows } = await db.query<Record<string, unknown>>(DIRECTORY_KEYS_SQL,
        [orgId, workspaceId, keys.map((k) => k.harness), keys.map((k) => k.sessionId)]);
      return rows.map((r): DirectoryListRow => ({
        org_id: r.org_id as string, workspace_id: r.workspace_id as string, harness: r.harness as string,
        session_id: r.session_id as string, created_at: r.created_at as Date, last_activity_at: r.last_activity_at as Date,
        title: (r.title as string | null) ?? null, task_title: null, agent_names: null,
        project_id: (r.project_id as string | null) ?? null, project_key: null, project_key_method: null,
        git_repo: (r.git_repo as string | null) ?? null, git_branch: (r.git_branch as string | null) ?? null,
        initial_commit: null, originator: null, agent_version: null, models: null, state: null,
        event_count: 0, input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: null, user_ids: null,
        parent_session_ids: (r.parent_session_ids as string[] | null) ?? [], child_session_count: 0,
        short_summary: (r.short_summary as string | null) ?? null, short_summary_generated_at: null,
        catalog_projected_at: null, projected_at: null,
      }));
    },
    activity: unsupported, query: unsupported, queryActivity: unsupported,
  };
}

export interface UpsertResult { inserted: number; upgraded: number; skipped: number }
export interface AdvisoryRowInput {
  advisory_id: string; harness: string; session_id: string; peer_harness: string; peer_session_id: string;
  urgency: "high" | "low"; kind: string; artifact: string; evidence: Advisory["evidence"];
}

/** Everything the scan reads from and writes to Postgres. */
export interface AdvisorStore {
  candidates(since: Date, limit: number): Promise<CandidateLane[]>;
  directory(): Directory;
  upsert(orgId: string, workspaceId: string, row: AdvisoryRowInput): Promise<UpsertResult>;
  markDelivered(orgId: string, workspaceId: string, advisoryId: string, recipient: SessionRef): Promise<boolean>;
  sweep(): Promise<{ expired: number; deleted: number }>;
  /** Durable cooldown and cap state for one tenant (062070). */
  recent(orgId: string, workspaceId: string, pairSince: Date): Promise<RecentAdvisory[]>;
}

export function sqlAdvisorStore(db: SqlQuery): AdvisorStore {
  return {
    async candidates(since, limit) {
      const { rows } = await db.query<Record<string, unknown>>(CANDIDATES_SQL, [since, limit]);
      return rows.map((r) => ({
        orgId: r.org_id as string, workspaceId: r.workspace_id as string, harness: r.harness as string,
        sessionId: r.session_id as string, projectId: (r.project_id as string | null) ?? null,
        repo: (r.git_repo as string | null) ?? null, parentSessionIds: ((r.parent_session_ids as string[] | null) ?? []).filter(Boolean),
        shortSummary: (r.short_summary as string | null) ?? null, lastActivityAt: iso(r.last_activity_at as Date | string),
      }));
    },
    directory: () => workerDirectory(db),
    async upsert(orgId, workspaceId, row) {
      const { rows } = await db.query<{ inserted: number; upgraded: number; skipped: number }>(UPSERT_SQL,
        [orgId, workspaceId, JSON.stringify([row])]);
      const r = rows[0];
      return { inserted: Number(r?.inserted ?? 0), upgraded: Number(r?.upgraded ?? 0), skipped: Number(r?.skipped ?? 0) };
    },
    async markDelivered(orgId, workspaceId, advisoryId, recipient) {
      const { rows } = await db.query<{ matched: boolean }>(MARK_DELIVERED_SQL,
        [orgId, workspaceId, advisoryId, recipient.harness, recipient.sessionId]);
      return rows[0]?.matched === true;
    },
    async sweep() {
      const { rows } = await db.query<{ expired: number; deleted: number }>(SWEEP_SQL);
      return { expired: Number(rows[0]?.expired ?? 0), deleted: Number(rows[0]?.deleted ?? 0) };
    },
    async recent(orgId, workspaceId, pairSince) {
      const { rows } = await db.query<Record<string, unknown>>(RECENT_SQL, [orgId, workspaceId, pairSince]);
      const ms = (v: unknown) => new Date(v as Date | string).getTime();
      return rows.map((r): RecentAdvisory => {
        const recipient = { harness: String(r.harness), sessionId: String(r.session_id) };
        return r.row_kind === "pair"
          ? { kind: "pair", recipient, peer: { harness: String(r.peer_harness), sessionId: String(r.peer_session_id) }, latestAt: ms(r.latest_at) }
          : { kind: "recipient", recipient, latestAt: ms(r.latest_at), sentLastHour: Number(r.sent_last_hour ?? 0) };
      });
    },
  };
}

export interface ScanDeps {
  store: AdvisorStore;
  backend: CoordinationBackend;
  limits: AdvisorLimits;
  /** Absent: DMs are off; HIGH rows stay pending for the pull path. */
  sender?: RelaySender;
}

export interface ScanStats {
  candidates: number; workspaces: number; findings: number; written: number; existing: number;
  cooldown: number; capped: number; dmSent: number; dmFailed: number; truncated: number; failed: number;
}

const emptyStats = (): ScanStats => ({
  candidates: 0, workspaces: 0, findings: 0, written: 0, existing: 0,
  cooldown: 0, capped: 0, dmSent: 0, dmFailed: 0, truncated: 0, failed: 0,
});

function rowFor(a: Advisory, recipient: SessionRef, peer: SessionRef): AdvisoryRowInput {
  return {
    advisory_id: a.advisoryId, harness: recipient.harness, session_id: recipient.sessionId,
    peer_harness: peer.harness, peer_session_id: peer.sessionId, urgency: a.urgency, kind: a.kind,
    artifact: a.artifact, evidence: a.evidence,
  };
}

/** Applies the limits and writes one workspace's advisories; DMs HIGH rows that were newly written. */
async function deliver(deps: ScanDeps, orgId: string, workspaceId: string, advisories: Advisory[], now: number, stats: ScanStats) {
  for (const a of orderAdvisories(advisories)) {
    const pair = pairKey(a.lanes[0], a.lanes[1]);
    if (deps.limits.inCooldown(pair, now)) { stats.cooldown++; continue; }
    let attempted = false;
    for (const [recipient, peer] of [[a.lanes[0], a.lanes[1]], [a.lanes[1], a.lanes[0]]] as const) {
      if (!deps.limits.underCap(recipient, now)) { stats.capped++; continue; }
      attempted = true;
      const result = await deps.store.upsert(orgId, workspaceId, rowFor(a, recipient, peer));
      if (result.inserted + result.upgraded === 0) { stats.existing++; continue; }
      stats.written++;
      deps.limits.recordSent(recipient, now);
      if (a.urgency !== "high" || !deps.sender) continue;
      const text = advisoryMessage({
        advisoryId: a.advisoryId, urgency: a.urgency, kind: a.kind, artifact: a.artifact,
        harness: recipient.harness, sessionId: recipient.sessionId, peerHarness: peer.harness, peerSessionId: peer.sessionId,
        firstSeen: a.evidence.map((e) => e.ts).sort()[0] ?? new Date(now).toISOString(),
      });
      try {
        await deps.sender.send({ to: recipient, text, advisoryId: a.advisoryId });
      } catch (err) {
        // Not delivered: the row stays pending and the pull path still serves it.
        stats.dmFailed++;
        log.warn("coordination advisor dm failed", { org_id: orgId, workspace_id: workspaceId, kind: a.kind, ...errorFields(err) });
        continue;
      }
      stats.dmSent++;
      await deps.store.markDelivered(orgId, workspaceId, a.advisoryId, recipient);
    }
    if (attempted) deps.limits.touchPair(pair, now);
  }
}

/**
 * Loads the tenant's durable cooldown and cap state before writing. A HoopDb without 062070 (42883) keeps the
 * in-memory limits, with a warning; any other failure fails the workspace, so nothing is written unchecked.
 */
async function seedLimits(deps: ScanDeps, orgId: string, workspaceId: string, now: number) {
  try {
    deps.limits.seed(await deps.store.recent(orgId, workspaceId, new Date(now - deps.limits.pairCooldownMs)), now);
  } catch (err) {
    if ((err as { code?: unknown } | null)?.code !== "42883") throw err;
    log.warn("coordination advisor durable limits unavailable (20261105062070 not applied); using in-memory limits",
      { org_id: orgId, workspace_id: workspaceId });
  }
}

/** One full scan at `now`. A failing workspace is logged and does not stop the others. */
export async function scanOnce(deps: ScanDeps, now: Date): Promise<ScanStats> {
  const stats = emptyStats();
  const nowMs = now.getTime();
  deps.limits.prune(nowMs);
  const since = new Date(nowMs - CANDIDATE_WINDOW_MS);
  const rows = await deps.store.candidates(since, CANDIDATE_LIMIT + 1);
  if (rows.length > CANDIDATE_LIMIT) log.warn("coordination advisor candidates truncated", { count: CANDIDATE_LIMIT });
  const byWorkspace = new Map<string, CandidateLane[]>();
  for (const row of rows.slice(0, CANDIDATE_LIMIT)) {
    const k = `${row.orgId}\u0000${row.workspaceId}`;
    byWorkspace.set(k, [...(byWorkspace.get(k) ?? []), row]);
  }
  stats.candidates = Math.min(rows.length, CANDIDATE_LIMIT);
  for (const lanes of byWorkspace.values()) {
    const { orgId, workspaceId } = lanes[0]!;
    // A pair needs two sessions in one project.
    const perProject = new Map<string, number>();
    for (const l of lanes) perProject.set(l.projectId!, (perProject.get(l.projectId!) ?? 0) + 1);
    if (![...perProject.values()].some((n) => n >= 2)) continue;
    stats.workspaces++;
    try {
      const candidates = new Map(lanes.map((l) => [refKey(l), l]));
      const facts = await deps.backend.readFacts(deps.store.directory(),
        { orgId, workspaceId, since, until: now }, { timeoutMs: FACTS_TIMEOUT_MS });
      const truncated = truncatedStreams(facts.notes);
      if (facts.sessionsTruncated) {
        stats.truncated++;
        // The notes are fixed text with numeric caps: no ids or content.
        log.warn(`coordination advisor facts truncated: ${facts.notes.filter((n) => n.includes("stream cap")).join("; ")}`,
          { org_id: orgId, workspace_id: workspaceId });
      }
      const live = facts.lanes.filter((l) => candidates.has(refKey({ harness: l.source, sessionId: l.sessionId })));
      const output = detectCoordination({ workspaceId, window: { since: since.toISOString(), until: now.toISOString() }, lanes: live },
        { params: ADVISOR_DETECTOR_PARAMS, limit: DETECTOR_LIMIT });
      const high = highAdvisories(output, candidates, truncated);
      const low = lowAdvisories(workspaceId, live, candidates, new Set(high.map((a) => pairKey(a.lanes[0], a.lanes[1]))),
        since.toISOString(), truncated);
      stats.findings += high.length + low.length;
      if (high.length + low.length) await seedLimits(deps, orgId, workspaceId, nowMs);
      await deliver(deps, orgId, workspaceId, [...high, ...low], nowMs, stats);
    } catch (err) {
      stats.failed++;
      log.warn("coordination advisor workspace scan failed", { org_id: orgId, workspace_id: workspaceId, ...errorFields(err) });
    }
  }
  return stats;
}

/** The runWorker side tick: a scan every `intervalMs`, a retention sweep every hour. */
export function advisorTick(make: (db: SqlQuery) => ScanDeps, opts: { intervalMs: number; sweepEveryMs?: number }) {
  let lastScan = Number.NEGATIVE_INFINITY;
  let lastSweep = Number.NEGATIVE_INFINITY;
  let deps: ScanDeps | undefined;
  return async (db: SqlQuery, now: Date) => {
    const t = now.getTime();
    if (t - lastScan < opts.intervalMs) return;
    lastScan = t;
    deps ??= make(db);
    const stats = await scanOnce(deps, now);
    // Counts only, in the message: the log allowlist has no fields for them.
    log.info(`coordination advisor scan: ${Object.entries(stats).map(([k, v]) => `${k}=${v}`).join(" ")}`, { count: stats.written });
    if (t - lastSweep >= (opts.sweepEveryMs ?? SWEEP_EVERY_MS)) {
      lastSweep = t;
      const swept = await deps.store.sweep();
      log.info("coordination advisor sweep", { count: swept.expired, deleted: swept.deleted });
    }
  };
}
