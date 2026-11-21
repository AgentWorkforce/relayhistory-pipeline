/**
 * The pull side of live coordination advisories (docs/coordination-live-advisories.md, "Delivery
 * contract"): GET /v1/coordination/advisories and POST /v1/coordination/advisories/:advisoryId/ack,
 * both behind READ_API_COORDINATION_ADVISORIES_ENABLED (exactly "true").
 *
 * Both call the 20261105062000 SECURITY DEFINER functions granted to bulls96_read_api, with the
 * tenant settings bound. The GET runs in the read pool's READ ONLY transaction. The ack is the
 * read API's only write here: one READ WRITE transaction on the same read-role pool, because the
 * role defaults to read-only transactions. No transcript text is read or returned.
 */
import { advisoryMessage } from "../coordination-advisor/template.ts";
import { TENANT_SETTINGS_SQL, type SqlQuery, type TenantRunner } from "./liveness.ts";
import { advisoryNotifyEligible, advisoryPriority } from "./advisory-priority.ts";

export const ADVISORIES_CONTRACT = "coordination-advisories/1";
/** pending_for_v1 returns at most 10 rows; the route never serves more. */
export const ADVISORIES_PAGE_MAX = 10;

export interface AdvisoryEvidence {
  sessionId: string;
  eventId: string;
  ts: string;
  kind: string;
}

/** One row of sessions.coordination_advisories_pending_for_v1. */
export interface AdvisoryRow {
  advisoryId: string;
  harness: string;
  sessionId: string;
  peerHarness: string;
  peerSessionId: string;
  urgency: "high" | "low";
  kind: string;
  artifact: string;
  evidence: AdvisoryEvidence[];
  createdAt: string;
  /** Set once the HIGH DM was sent (mark_delivered_v1); a listed row is never acked, so only the DM sets it. */
  deliveredAt: string | null;
}

export interface AdvisoryRecipient {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
}

export interface AdvisoriesSource {
  /** Pending or delivered rows for the recipient, oldest first, at most 10. */
  pending(recipient: AdvisoryRecipient): Promise<AdvisoryRow[]>;
  /** True when a row matched the recipient (also when it was already acked). */
  ack(recipient: AdvisoryRecipient, advisoryId: string): Promise<boolean>;
}

type PoolClient = { query(text: string, params?: unknown[]): Promise<{ rows: unknown[] }>; release(): void };

/** A TenantRunner like poolTenantRunner, but READ WRITE: the ack's one write. */
export function poolTenantWriter(pool: { connect(): Promise<PoolClient> }): TenantRunner {
  return async (orgId, workspaceId, work) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN READ WRITE");
      await client.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]);
      const result = await work({
        async query<R>(text: string, params?: unknown[]) {
          const r = await client.query(text, params);
          return { rows: r.rows as R[] };
        },
      });
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}

const PENDING_COLUMNS = `SELECT advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact,
       evidence, created_at, delivered_at`;
/** 20261105062190: priority-first (P1, other HIGH, LOW, then oldest), so a new P1 is never hidden behind 10 LOW rows. */
export const PENDING_V2_SQL = `${PENDING_COLUMNS}
  FROM sessions.coordination_advisories_pending_for_v2($1, $2, $3, $4, $5)`;
/** 062000's oldest-first reader with delivered_at: the switch-on fallback while 062190 is not applied. */
export const PENDING_V1_DELIVERY_SQL = `${PENDING_COLUMNS}
  FROM sessions.coordination_advisories_pending_for_v1($1, $2, $3, $4)`;
/** 062000's oldest-first reader, exactly as before M3-7: the only statement with the priority switch off. */
export const PENDING_SQL = `SELECT advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact,
       evidence, created_at
  FROM sessions.coordination_advisories_pending_for_v1($1, $2, $3, $4)`;
export const PENDING_V2_SIGNATURE = "sessions.coordination_advisories_pending_for_v2(text,text,text,text,integer)";
const V2_INSTALLED_SQL = "SELECT to_regprocedure($1) IS NOT NULL AS installed";
/** How long a "pending_for_v2 is not installed" answer is trusted, and the fallback warning's minimum spacing. */
export const PENDING_V2_ABSENT_TTL_MS = 60_000;
const ACK_SQL = "SELECT sessions.coordination_advisories_ack_v1($1, $2, $3, $4, $5) AS ok";

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString());

function evidenceOf(raw: unknown): AdvisoryEvidence[] {
  const list = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (!Array.isArray(list)) return [];
  return list.map((e) => ({ sessionId: String(e?.sessionId ?? ""), eventId: String(e?.eventId ?? ""), ts: String(e?.ts ?? ""), kind: String(e?.kind ?? "") }));
}

/**
 * Shared across sources (the Worker builds one per request): until `absentUntil` the v2 reader is known not to be
 * installed, so the GET goes straight to v1; `warnedAt` spaces the fallback warning; `fallbacks` counts them.
 */
export interface PendingV2Cache { absentUntil: number; warnedAt: number; fallbacks: number }
export const pendingV2Cache = (): PendingV2Cache => ({ absentUntil: -Infinity, warnedAt: -Infinity, fallbacks: 0 });

/**
 * The source over the read-API functions: `read` READ ONLY, `write` READ WRITE.
 *
 * Priority switch off (READ_API_ADVISORY_PRIORITY_ENABLED, the default): the GET issues 062000's pending_for_v1
 * exactly as before M3-7 (oldest first), whether or not 062190 is applied; pending_for_v2 is never called.
 *
 * Switch on: the GET reads 062190's pending_for_v2 (priority-first, limit ADVISORIES_PAGE_MAX). On 42883 it falls
 * back to pending_for_v1 only when pending_for_v2 (that exact signature) is not installed (to_regprocedure); any
 * other 42883 (signature drift, a missing dependency) is rethrown. "Not installed" is cached for
 * PENDING_V2_ABSENT_TTL_MS in `cache`, so a deployment without 062190 does not pay a failed statement and a second
 * transaction per GET, and the warning (counts only, never an id) is logged at most once per that interval.
 */
export function sqlCoordinationAdvisories(
  read: TenantRunner, write: TenantRunner,
  opts: { priority?: boolean; warn?: (fields: Record<string, unknown>) => void; cache?: PendingV2Cache; now?: () => number } = {},
): AdvisoriesSource {
  const cache = opts.cache ?? pendingV2Cache();
  const now = opts.now ?? Date.now;
  const pendingWith = (r: AdvisoryRecipient, sql: string) =>
    read(r.orgId, r.workspaceId, async (q: SqlQuery) => {
      const { rows } = sql === PENDING_V2_SQL
        ? await q.query<Record<string, unknown>>(sql, [r.orgId, r.workspaceId, r.source, r.sessionId, ADVISORIES_PAGE_MAX])
        : await q.query<Record<string, unknown>>(sql, [r.orgId, r.workspaceId, r.source, r.sessionId]);
      return rows;
    });
  const v2Installed = (r: AdvisoryRecipient) =>
    read(r.orgId, r.workspaceId, async (q: SqlQuery) =>
      (await q.query<{ installed: boolean }>(V2_INSTALLED_SQL, [PENDING_V2_SIGNATURE])).rows[0]?.installed === true);
  const fallback = async (r: AdvisoryRecipient) => {
    cache.fallbacks += 1;
    const at = now();
    if (at - cache.warnedAt >= PENDING_V2_ABSENT_TTL_MS) {
      cache.warnedAt = at;
      opts.warn?.({ reason: "coordination_advisories_v1_fallback", error_code: "42883", fallbacks: cache.fallbacks });
    }
    return pendingWith(r, PENDING_V1_DELIVERY_SQL);
  };
  return {
    pending: async (r) => {
      let rows: Record<string, unknown>[];
      if (opts.priority !== true) {
        rows = await pendingWith(r, PENDING_SQL);
      } else if (now() < cache.absentUntil) {
        rows = await fallback(r);
      } else {
        try {
          rows = await pendingWith(r, PENDING_V2_SQL);
        } catch (error) {
          if ((error as { code?: string })?.code !== "42883") throw error;
          if (await v2Installed(r)) throw error;
          cache.absentUntil = now() + PENDING_V2_ABSENT_TTL_MS;
          rows = await fallback(r);
        }
      }
      return rows.slice(0, ADVISORIES_PAGE_MAX).map((row) => ({
        advisoryId: String(row.advisory_id),
        harness: String(row.harness),
        sessionId: String(row.session_id),
        peerHarness: String(row.peer_harness),
        peerSessionId: String(row.peer_session_id),
        urgency: row.urgency === "high" ? "high" as const : "low" as const,
        kind: String(row.kind),
        artifact: String(row.artifact),
        evidence: evidenceOf(row.evidence),
        createdAt: iso(row.created_at),
        deliveredAt: row.delivered_at === null || row.delivered_at === undefined ? null : iso(row.delivered_at),
      }));
    },
    ack: (r, advisoryId) =>
      write(r.orgId, r.workspaceId, async (q: SqlQuery) => {
        const { rows } = await q.query<{ ok: boolean }>(ACK_SQL, [r.orgId, r.workspaceId, advisoryId, r.source, r.sessionId]);
        return rows[0]?.ok === true;
      }),
  };
}

/**
 * The wire item: the stored row plus the same templated message the HIGH DM carries. With the priority switch on
 * (READ_API_ADVISORY_PRIORITY_ENABLED), M3-7 adds three fields, additively under the same contract: `priority`
 * (advisory-priority.ts), `notifyEligible` (the row's (kind, urgency) is on READ_API_ADVISORY_NOTIFY_ROWS, default
 * empty) and `delivery`. `delivery.dm` is true when delivered_at is set: on a listed row (pending or delivered,
 * never acked) only the DM path (coordination_advisories_mark_delivered_v1) sets it, the ack sets it only on rows
 * this GET no longer lists. Switch off: exactly the pre-M3-7 item, none of the three.
 */
export function advisoryItem(row: AdvisoryRow, notifyRows: ReadonlySet<string> = new Set(), priority = false) {
  const firstSeen = row.evidence.map((e) => e.ts).filter((ts) => ts !== "").sort()[0] ?? row.createdAt;
  const base = {
    advisoryId: row.advisoryId,
    urgency: row.urgency,
    kind: row.kind,
    artifact: row.artifact,
    peer: { source: row.peerHarness, sessionId: row.peerSessionId },
    createdAt: row.createdAt,
    message: advisoryMessage({
      advisoryId: row.advisoryId, urgency: row.urgency, kind: row.kind, artifact: row.artifact,
      harness: row.harness, sessionId: row.sessionId, peerHarness: row.peerHarness, peerSessionId: row.peerSessionId, firstSeen,
    }),
    evidence: row.evidence,
  };
  if (priority !== true) return base;
  return {
    ...base,
    priority: advisoryPriority(row),
    notifyEligible: advisoryNotifyEligible(row, notifyRows),
    delivery: { dm: row.deliveredAt !== null, deliveredAt: row.deliveredAt },
  };
}
