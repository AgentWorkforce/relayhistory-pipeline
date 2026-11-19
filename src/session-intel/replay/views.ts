/**
 * As-of views over a frozen snapshot (docs/session-intel-replay.md, "As-of views"). Everything here answers
 * as of a simulated `now` and never later:
 *  - a session exists only once createdAt <= now; its directory activity is what the facts before now show
 *    (its final lastActivityAt only once now has reached it); its short summary is the latest timeline entry
 *    at or before now;
 *  - a fact is visible only when ts < now, the bound the production readers use (ts < until).
 * The in-memory AdvisorStore mirrors the 062000/062070 functions closely enough for the advisor's decisions;
 * the CoordinationBackend builds the production row shapes and runs the real normalizeCoordinationFacts.
 */
import {
  COORDINATION_EVIDENCE_LOOKBACK_MS, COORDINATION_FACTS_ROW_CAP, COORDINATION_OUTPUT_ROW_CAP, normalizeCoordinationFacts,
  type CoordinationBackend, type CoordinationEventRow, type CoordinationOutputRow,
} from "../../session-read-api/coordination-backend.ts";
import type { Directory, DirectoryListRow } from "../../session-read-api/directory.ts";
import type { RelayDm, RelaySender } from "../../coordination-advisor/relay-sender.ts";
import type { CandidateLane, RecentAdvisory, SessionRef } from "../../coordination-advisor/rubric.ts";
import { ARTIFACT_MAX_CHARS, CANDIDATES_PER_WORKSPACE, type AdvisorStore, type AdvisoryRowInput, type UpsertResult } from "../../coordination-advisor/scan.ts";
import { SESSION_INTEL_FEATURES, type SessionIntelControls, type SessionIntelFeature } from "../../shared/session-intel-controls.ts";
import type { Snapshot, SnapshotClaim, SnapshotSession } from "./snapshot.ts";
import type { ClaimRow } from "../../coordination-advisor/conflicts.ts";
import { sha256Hex, textCompare } from "./util.ts";

/** The simulated clock shared by a replay's store, directory, backend and sender. */
export interface SimClock { now: Date }

export interface AsOfSession {
  session: SnapshotSession;
  /** Directory last_activity_at as known at now. */
  lastActivityAt: string;
  shortSummary: string | null;
  writes: SnapshotSession["writes"];
  commits: SnapshotSession["commits"];
  outcomes: SnapshotSession["outcomes"];
  usage: SnapshotSession["usage"];
  claims: SnapshotClaim[];
}

const visible = (ts: string, nowIso: string) => ts < nowIso;

/** The directory's last_activity_at at `now`: the final value once reached, else the latest visible fact. */
export function activityAsOf(s: SnapshotSession, nowIso: string): string {
  if (nowIso >= s.lastActivityAt) return s.lastActivityAt;
  let at = s.createdAt;
  for (const list of [s.writes, s.commits, s.outcomes, s.usage] as Array<Array<{ ts: string }>>)
    for (const f of list) if (visible(f.ts, nowIso) && f.ts > at) at = f.ts;
  return at;
}

export function summaryAsOf(s: SnapshotSession, nowIso: string): string | null {
  let text: string | null = null;
  for (const e of s.summaryTimeline) if (e.at <= nowIso) text = e.text;
  return text;
}

/** The session as known at `now`, or null before it was created. */
export function asOfSession(s: SnapshotSession, now: Date): AsOfSession | null {
  const nowIso = now.toISOString();
  if (s.createdAt > nowIso) return null;
  return {
    session: s,
    lastActivityAt: activityAsOf(s, nowIso),
    shortSummary: summaryAsOf(s, nowIso),
    writes: s.writes.filter((f) => visible(f.ts, nowIso)),
    commits: s.commits.filter((f) => visible(f.ts, nowIso)),
    outcomes: s.outcomes.filter((f) => visible(f.ts, nowIso)),
    usage: s.usage.filter((f) => visible(f.ts, nowIso)),
    // A supersession is a fact like any other: until it is visible the claim is live, and the view does not
    // show that one is coming (no future leak through supersededAt).
    claims: s.claims.filter((c) => visible(c.at, nowIso)).map((c) => {
      if (c.supersededAt === undefined || visible(c.supersededAt, nowIso)) return c;
      const { supersededAt: _hidden, ...live } = c;
      return live;
    }),
  };
}

/** What a detector sees: the snapshot's metadata and the sessions as of a simulated time. */
export class ReplayView {
  constructor(readonly snapshot: Snapshot) {}
  get window() { return this.snapshot.manifest.window; }
  get sessionCount() { return this.snapshot.sessions.length; }
  sessionsAsOf(now: Date): AsOfSession[] {
    return this.snapshot.sessions.flatMap((s) => { const v = asOfSession(s, now); return v ? [v] : []; });
  }
}

const refKey = (harness: string, sessionId: string) => `${harness}\u0000${sessionId}`;

function directoryRow(v: AsOfSession): DirectoryListRow {
  const s = v.session;
  return {
    org_id: s.orgId, workspace_id: s.workspaceId, harness: s.harness, session_id: s.sessionId,
    created_at: new Date(s.createdAt), last_activity_at: new Date(v.lastActivityAt),
    title: s.title, task_title: null, agent_names: null, project_id: s.projectId, project_key: null, project_key_method: null,
    git_repo: s.repo, git_branch: s.branch, initial_commit: null, originator: null, agent_version: null, models: null, state: null,
    event_count: 0, input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: null, user_ids: null,
    parent_session_ids: s.parentSessionIds, child_session_count: 0, short_summary: v.shortSummary, short_summary_generated_at: null,
    catalog_projected_at: null, projected_at: null,
  };
}

/** The worker's keyed directory list, as of the clock. Like workerDirectory, only `list` by session keys. */
export function snapshotDirectory(view: ReplayView, clock: SimClock): Directory {
  const unsupported = () => { throw new Error("replay directory: only keyed list is supported"); };
  return {
    async list(orgId, workspaceId, args) {
      const keys = new Set((args.sessionKeys ?? []).map((k) => refKey(k.harness, k.sessionId)));
      if (!keys.size) return [];
      return view.sessionsAsOf(clock.now)
        .filter((v) => v.session.orgId === orgId && v.session.workspaceId === workspaceId && keys.has(refKey(v.session.harness, v.session.sessionId)))
        .map(directoryRow);
    },
    activity: unsupported, query: unsupported, queryActivity: unsupported,
  };
}

interface StoredRow { orgId: string; workspaceId: string; row: AdvisoryRowInput; status: "pending" | "delivered" | "acked" | "expired"; createdAt: number; deliveredAt: number | null }
export interface RecordedWrite { at: number; orgId: string; workspaceId: string; row: AdvisoryRowInput; result: "inserted" | "upgraded" | "skipped" }

const RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;
const HOUR_MS = 60 * 60 * 1_000;
const dbError = (code: string, message: string) => Object.assign(new Error(message), { code });

/**
 * An in-memory AdvisorStore as of the clock. Mirrors 062000 upsert_v1 (insert new; on the
 * (advisory_id, harness, session_id) key, upgrade LOW to HIGH on a pending or delivered row of the same
 * tenant; otherwise skipped), mark_delivered_v1, sweep_v1, and 062070 recent_v1 (derived from the rows by
 * created_at). Every upsert is recorded with its simulated time.
 */
/** The M3-1 controls a replay assumes, per feature. Required: a replay store never assumes a default. */
export type ReplayControls = Readonly<Record<SessionIntelFeature, boolean>>;

export class ReplayAdvisorStore implements AdvisorStore {
  readonly rows = new Map<string, StoredRow>();
  readonly writes: RecordedWrite[] = [];
  /** `controls`: what sessions.session_intel_controls_v1 would return, for every workspace of the replay. */
  constructor(private readonly view: ReplayView, private readonly clock: SimClock, private readonly replayControls: ReplayControls) {}

  async controls(_orgId: string, _workspaceId: string): Promise<SessionIntelControls> {
    // Every feature is listed, as the production reader returns it; a missing one is off (fail closed).
    return new Map(SESSION_INTEL_FEATURES.map((f) => [f, this.replayControls[f] === true]));
  }
  private key = (advisoryId: string, r: SessionRef) => `${advisoryId}\u0000${r.harness}\u0000${r.sessionId}`;

  async candidates(since: Date, limit: number, workspaces: string[] | null): Promise<CandidateLane[]> {
    const sinceIso = since.toISOString();
    const allowed = workspaces === null ? null : new Set(workspaces);
    const live = this.view.sessionsAsOf(this.clock.now)
      .filter((v) => v.session.projectId !== null && v.lastActivityAt >= sinceIso && (!allowed || allowed.has(v.session.workspaceId)));
    // CANDIDATES_SQL: newest first per (org, workspace), at most CANDIDATES_PER_WORKSPACE each, then the overall limit.
    const order = (a: AsOfSession, b: AsOfSession) => textCompare(b.lastActivityAt, a.lastActivityAt)
      || textCompare(a.session.harness, b.session.harness) || textCompare(a.session.sessionId, b.session.sessionId);
    const byTenant = new Map<string, AsOfSession[]>();
    for (const v of live) {
      const k = `${v.session.orgId}\u0000${v.session.workspaceId}`;
      byTenant.set(k, [...(byTenant.get(k) ?? []), v]);
    }
    const kept = [...byTenant.values()].flatMap((rows) => rows.sort(order).slice(0, CANDIDATES_PER_WORKSPACE));
    kept.sort((a, b) => textCompare(b.lastActivityAt, a.lastActivityAt) || textCompare(a.session.orgId, b.session.orgId)
      || textCompare(a.session.workspaceId, b.session.workspaceId) || textCompare(a.session.harness, b.session.harness)
      || textCompare(a.session.sessionId, b.session.sessionId));
    return kept.slice(0, limit).map((v) => ({
      orgId: v.session.orgId, workspaceId: v.session.workspaceId, harness: v.session.harness, sessionId: v.session.sessionId,
      projectId: v.session.projectId, repo: v.session.repo, parentSessionIds: v.session.parentSessionIds.filter(Boolean),
      shortSummary: v.shortSummary, lastActivityAt: v.lastActivityAt,
    }));
  }

  directory(): Directory { return snapshotDirectory(this.view, this.clock); }

  async upsert(orgId: string, workspaceId: string, row: AdvisoryRowInput): Promise<UpsertResult> {
    if (!orgId || !workspaceId) throw dbError("22023", "coordination advisories: invalid tenant");
    if (!Array.isArray(row.evidence) || row.evidence.length > 10) throw dbError("22023", "coordination advisories: evidence of at most 10 entries");
    if (row.artifact.length < 1 || Array.from(row.artifact).length > ARTIFACT_MAX_CHARS) throw dbError("23514", "artifact length check");
    const at = this.clock.now.getTime();
    const k = this.key(row.advisory_id, { harness: row.harness, sessionId: row.session_id });
    const cur = this.rows.get(k);
    let result: RecordedWrite["result"];
    if (!cur) {
      this.rows.set(k, { orgId, workspaceId, row: { ...row }, status: "pending", createdAt: at, deliveredAt: null });
      result = "inserted";
    } else if (cur.orgId === orgId && cur.workspaceId === workspaceId && (cur.status === "pending" || cur.status === "delivered")
      && cur.row.urgency === "low" && row.urgency === "high") {
      cur.row = { ...cur.row, urgency: "high" };
      result = "upgraded";
    } else result = "skipped";
    this.writes.push({ at, orgId, workspaceId, row: { ...row }, result });
    return { inserted: result === "inserted" ? 1 : 0, upgraded: result === "upgraded" ? 1 : 0, skipped: result === "skipped" ? 1 : 0 };
  }

  async markDelivered(orgId: string, workspaceId: string, advisoryId: string, recipient: SessionRef): Promise<boolean> {
    const cur = this.rows.get(this.key(advisoryId, recipient));
    if (!cur || cur.orgId !== orgId || cur.workspaceId !== workspaceId || cur.status === "expired") return false;
    cur.deliveredAt ??= this.clock.now.getTime();
    if (cur.status === "pending") cur.status = "delivered";
    return true;
  }

  async sweep(): Promise<{ expired: number; deleted: number }> {
    const cutoff = this.clock.now.getTime() - RETENTION_MS;
    let expired = 0, deleted = 0;
    for (const [k, r] of this.rows) {
      if ((r.status === "pending" || r.status === "delivered") && r.createdAt < cutoff) { r.status = "expired"; expired++; }
      if (r.status === "expired" || r.createdAt < cutoff) { this.rows.delete(k); deleted++; }
    }
    return { expired, deleted };
  }

  /**
   * 062150 relayfacts_claims_v1 as of the clock: the tenant's current claims (visible, not superseded), one row per
   * session claim, newest first, updated at or after `since`, at most `limit`. A row's id is a hash of the
   * session and claim id (unique in the workspace, as a relayfacts id is); its evidence names the session.
   */
  async claims(orgId: string, workspaceId: string, since: Date, limit: number): Promise<ClaimRow[]> {
    const nowIso = this.clock.now.toISOString();
    const sinceIso = since.toISOString();
    // One row per stored claim: a claim with a recordId held by several sessions is one row whose evidence names
    // each of them (as relayfacts stores it); without a recordId, one row per session claim.
    const byId = new Map<string, ClaimRow>();
    for (const v of this.view.sessionsAsOf(this.clock.now)) {
      const s = v.session;
      if (s.orgId !== orgId || s.workspaceId !== workspaceId) continue;
      for (const c of v.claims) {
        if (!(c.at < nowIso) || (c.supersededAt !== undefined && c.supersededAt < nowIso) || c.at < sinceIso) continue;
        const ids = c.evidenceIds.length ? c.evidenceIds : [""];
        const id = sha256Hex(c.recordId !== undefined ? `record\u0000${c.recordId}` : `${s.harness}\u0000${s.sessionId}\u0000${c.id}`).slice(0, 32);
        const evidence = ids.map((e) => ({ id: e, source: s.harness, sessionId: s.sessionId, createdAt: c.at }));
        const have = byId.get(id);
        if (have) { have.evidence.push(...evidence); continue; }
        byId.set(id, {
          id, kind: c.kind, scope: c.kind === "decision" ? null : c.scope,
          topic: c.topic ?? "general", statement: c.text.slice(0, 200), observedAt: c.kind === "decision" ? null : c.at, createdAt: c.at, evidence,
        });
      }
    }
    const rows = [...byId.values()];
    return rows.sort((a, b) => textCompare(b.createdAt, a.createdAt) || textCompare(a.id, b.id)).slice(0, limit);
  }

  async recent(orgId: string, workspaceId: string, pairSince: Date): Promise<RecentAdvisory[]> {
    const now = this.clock.now.getTime();
    const since = Math.max(pairSince.getTime(), now - RETENTION_MS);
    const tenant = [...this.rows.values()].filter((r) => r.orgId === orgId && r.workspaceId === workspaceId);
    const pairs = new Map<string, Extract<RecentAdvisory, { kind: "pair" }>>();
    const recipients = new Map<string, Extract<RecentAdvisory, { kind: "recipient" }>>();
    for (const r of tenant) {
      const recipient = { harness: r.row.harness, sessionId: r.row.session_id };
      if (r.createdAt >= since) {
        const k = [r.row.harness, r.row.session_id, r.row.peer_harness, r.row.peer_session_id, r.row.urgency].join("\u0000");
        const cur = pairs.get(k);
        if (!cur || r.createdAt > cur.latestAt) pairs.set(k, {
          kind: "pair", recipient, peer: { harness: r.row.peer_harness, sessionId: r.row.peer_session_id }, urgency: r.row.urgency, latestAt: r.createdAt,
        });
      }
      if (r.createdAt >= now - HOUR_MS) {
        const k = refKey(r.row.harness, r.row.session_id);
        const cur = recipients.get(k);
        recipients.set(k, { kind: "recipient", recipient, latestAt: Math.max(cur?.latestAt ?? r.createdAt, r.createdAt), sentLastHour: (cur?.sentLastHour ?? 0) + 1 });
      }
    }
    return [...pairs.values(), ...recipients.values()];
  }
}

const rowCompare = (a: { row_id: string }, b: { row_id: string }) => textCompare(a.row_id, b.row_id);

/**
 * The facts backend as of the clock: the production row shapes (evidence and output rows over the seven-day
 * lookback, usage over the window), the production caps (ORDER BY row_id LIMIT cap + 1), and the real
 * normalizeCoordinationFacts. The read is bounded by min(until, clock): a caller cannot ask for the future.
 */
export function snapshotCoordinationBackend(view: ReplayView, clock: SimClock): CoordinationBackend {
  return {
    kind: "clickhouse",
    windowReadiness: () => Promise.reject(new Error("replay backend: window readiness is not used by the advisor")),
    isSchemaMissing: () => false,
    async readFacts(directory, p) {
      const bound = new Date(Math.min(p.until.getTime(), clock.now.getTime())).toISOString();
      const sinceIso = p.since.toISOString();
      const evidenceSince = new Date(p.since.getTime() - COORDINATION_EVIDENCE_LOOKBACK_MS).toISOString();
      const cap = Math.max(1, Math.min(COORDINATION_FACTS_ROW_CAP, Math.trunc(p.rowCap ?? COORDINATION_FACTS_ROW_CAP)));
      const outputCap = Math.max(1, Math.min(COORDINATION_OUTPUT_ROW_CAP, Math.trunc(p.rowCap ?? COORDINATION_OUTPUT_ROW_CAP)));
      const evidence: CoordinationEventRow[] = [], outputs: CoordinationOutputRow[] = [], usage: CoordinationEventRow[] = [];
      const inEvidence = (ts: string) => ts >= evidenceSince && ts < bound;
      for (const s of view.snapshot.sessions) {
        if (s.orgId !== p.orgId || s.workspaceId !== p.workspaceId || s.createdAt > bound) continue;
        const base = (ts: string, eventId: string, kind: string, branch: string | null) => ({
          ts, session_id: s.sessionId, source: s.harness, event_id: eventId, kind, project_id: s.projectId,
          task_ref_id: s.taskRef, task_title: null, user_id: s.userId, branch,
          input_tokens: 0, output_tokens: 0, reasoning_tokens: 0, cost_usd_micros: null,
        });
        // One event row per (eventId, ts, kind, branch): an event that touched several files carries them all.
        const events = new Map<string, CoordinationEventRow>();
        for (const w of s.writes) {
          if (!inEvidence(w.ts)) continue;
          const branch = w.branch ?? s.branch;
          const k = [w.eventId, w.ts, w.kind, branch ?? ""].join("\u0000");
          const cur = events.get(k);
          if (cur) cur.files_touched!.push(w.path);
          else events.set(k, { ...base(w.ts, w.eventId, w.kind, branch), files_touched: [w.path], row_id: `e\u0000${s.harness}\u0000${s.sessionId}\u0000${k}` });
        }
        evidence.push(...events.values());
        const output = (fact_kind: string, ts: string, key: string, eventId: string, reverted: boolean) => ({
          fact_kind, ts, scope_source: s.harness, source: s.harness, session_id: s.sessionId, fact_key: key,
          origin: "snapshot", reverted: reverted ? 1 : 0, row_id: `o\u0000${s.harness}\u0000${s.sessionId}\u0000${fact_kind}\u0000${eventId}\u0000${ts}`,
        });
        for (const c of s.commits) if (inEvidence(c.ts)) outputs.push(output("commit", c.ts, c.sha, c.eventId, false));
        for (const o of s.outcomes) if (inEvidence(o.ts)) outputs.push(output("outcome", o.ts, o.key, o.eventId, o.reverted === true));
        for (const u of s.usage) {
          if (!(u.ts >= sinceIso && u.ts < bound)) continue;
          usage.push({
            ...base(u.ts, u.eventId, "usage", s.branch), input_tokens: u.tokens ?? 0, cost_usd_micros: u.costUsdMicros,
            files_touched: null, row_id: `u\u0000${s.harness}\u0000${s.sessionId}\u0000${u.eventId}\u0000${u.ts}`,
          });
        }
      }
      evidence.sort(rowCompare); outputs.sort(rowCompare); usage.sort(rowCompare);
      return normalizeCoordinationFacts(directory, { ...p, until: new Date(bound) },
        evidence.slice(0, cap), outputs.slice(0, outputCap), usage.slice(0, cap), {
          evidence: { cap, truncated: evidence.length > cap },
          outputs: { cap: outputCap, truncated: outputs.length > outputCap },
          usage: { cap, truncated: usage.length > cap },
        });
    },
  };
}

/** Records every DM with the simulated send time; never sends anything. */
export class RecordingRelaySender implements RelaySender {
  readonly sent: Array<RelayDm & { sentAt: string }> = [];
  constructor(private readonly clock: SimClock) {}
  async send(dm: RelayDm): Promise<void> {
    this.sent.push({ ...dm, sentAt: this.clock.now.toISOString() });
  }
}
