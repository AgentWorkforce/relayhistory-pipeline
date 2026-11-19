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

const PENDING_SQL = `SELECT advisory_id, harness, session_id, peer_harness, peer_session_id, urgency, kind, artifact,
       evidence, created_at, delivered_at
  FROM sessions.coordination_advisories_pending_for_v1($1, $2, $3, $4)`;
const ACK_SQL = "SELECT sessions.coordination_advisories_ack_v1($1, $2, $3, $4, $5) AS ok";

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString());

function evidenceOf(raw: unknown): AdvisoryEvidence[] {
  const list = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (!Array.isArray(list)) return [];
  return list.map((e) => ({ sessionId: String(e?.sessionId ?? ""), eventId: String(e?.eventId ?? ""), ts: String(e?.ts ?? ""), kind: String(e?.kind ?? "") }));
}

/** The source over the 062000 read-API functions: `read` READ ONLY, `write` READ WRITE. */
export function sqlCoordinationAdvisories(read: TenantRunner, write: TenantRunner): AdvisoriesSource {
  return {
    pending: (r) =>
      read(r.orgId, r.workspaceId, async (q: SqlQuery) => {
        const { rows } = await q.query<Record<string, unknown>>(PENDING_SQL, [r.orgId, r.workspaceId, r.source, r.sessionId]);
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
      }),
    ack: (r, advisoryId) =>
      write(r.orgId, r.workspaceId, async (q: SqlQuery) => {
        const { rows } = await q.query<{ ok: boolean }>(ACK_SQL, [r.orgId, r.workspaceId, advisoryId, r.source, r.sessionId]);
        return rows[0]?.ok === true;
      }),
  };
}

/**
 * The wire item: the stored row plus the same templated message the HIGH DM carries. M3-7 adds two fields,
 * additively under the same contract: `priority` (advisory-priority.ts) and `delivery`. `delivery.dm` is true
 * when delivered_at is set: on a listed row (pending or delivered, never acked) only the DM path
 * (coordination_advisories_mark_delivered_v1) sets it, the ack sets it only on rows this GET no longer lists.
 * `notifyEligible` (also additive): the row's (kind, urgency) is on READ_API_ADVISORY_NOTIFY_ROWS (default empty).
 */
export function advisoryItem(row: AdvisoryRow, notifyRows: ReadonlySet<string> = new Set()) {
  const firstSeen = row.evidence.map((e) => e.ts).filter((ts) => ts !== "").sort()[0] ?? row.createdAt;
  return {
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
    priority: advisoryPriority(row),
    notifyEligible: advisoryNotifyEligible(row, notifyRows),
    delivery: { dm: row.deliveredAt !== null, deliveredAt: row.deliveredAt },
  };
}
