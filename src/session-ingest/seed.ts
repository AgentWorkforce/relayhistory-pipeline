/**
 * Seed the ingest's committed-revision index from Neon, per org, before that
 * org is routed to the ingest (docs/session-ingest-design.md 2a, runbook
 * runbooks/session-ingest-rollout.md). Then old Desktops that re-send whole
 * histories hit the replay fast path from their first batch.
 *
 * Neon: `sessions.delivery_records` read in READ ONLY transactions, keyset
 * paginated on its primary key; only ids, revisions and digests leave it.
 * Supabase: `sessions.ingest_seed_records` as `bulls96_session_ingest_seeder`
 * (max revision wins, never lowers, equal keeps the stored row), so a re-run,
 * or a run after the org is routed, is safe.
 *
 * Only records whose session the Neon->S3 exporter has fully exported (its
 * state: exported revision at or past the session's Neon revision, no error)
 * are seeded (review Medium). A record of a session the exporter refused,
 * failed or has not reached is left unstored here, so when an old Desktop
 * re-sends it the ingest writes it to S3 instead of acknowledging it unwritten.
 * Records with no session are never seeded (a re-send writes them to the
 * origin's `_none` session; the transform folds the copies).
 *
 * Neon's fences come along (`seedFences`): `deleted_upload_sessions` and
 * `session_resets` for the org, so the ingest's deletion filter and its
 * revision-forgetting start complete.
 *
 * Batch receipts are not copied: with #250 a client retrying a batch Neon
 * already accepted finds every record stale, and the claim commits it as a
 * replay (`replay_fast`) with the same receipt, recomputed from the request.
 */

export interface SeedSql {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

export interface SeedResult {
  pages: number;
  read: number;
  upserted: number;
  /** Read but not seeded: no session, or a session the exporter has not fully exported. */
  skipped: number;
}

/** Whether the exporter has fully exported this Neon session (org-scoped). */
export type Exported = (sessionId: string) => boolean;

/** The seed RPC's limit is 5000 rows per call. */
export const SEED_PAGE = 2000;

interface Row {
  user_id: string;
  source: string;
  session_id: string | null;
  workspace_id: string;
  origin_id: string;
  record_id: string;
  revision: string;
  revision_id: string;
  digest: string;
}

export async function seedOrg(
  neon: { readOnly<T>(work: (sql: SeedSql) => Promise<T>): Promise<T> },
  supabase: SeedSql,
  orgId: string,
  exported: Exported,
  opts: { page?: number; log?: (fields: Record<string, unknown>) => void } = {},
): Promise<SeedResult> {
  const page = Math.min(opts.page ?? SEED_PAGE, 5000);
  let after: [string, string, string] | null = null;
  const result: SeedResult = { pages: 0, read: 0, upserted: 0, skipped: 0 };
  for (;;) {
    const cursor: [string, string, string] | null = after;
    const rows: { rows: Row[] } = await neon.readOnly((sql): Promise<{ rows: Row[] }> =>
      sql.query<Row>(
        `SELECT workspace_id, origin_id, record_id, revision::text AS revision, revision_id, digest,
                user_id, source, session_id
           FROM sessions.delivery_records
          WHERE org_id = $1
            AND ($2::text IS NULL OR (workspace_id, origin_id, record_id) > ($2, $3, $4))
          ORDER BY workspace_id, origin_id, record_id
          LIMIT $5`,
        [orgId, cursor?.[0] ?? null, cursor?.[1] ?? null, cursor?.[2] ?? null, page],
      ),
    );
    if (rows.rows.length === 0) break;
    const eligible = rows.rows.filter((r) => r.session_id && exported(r.session_id));
    result.skipped += rows.rows.length - eligible.length;
    const payload = eligible.map((r) => ({
      org_id: orgId,
      workspace_id: r.workspace_id,
      origin_id: r.origin_id,
      record_id: r.record_id,
      revision: Number(r.revision),
      revision_id: r.revision_id,
      digest: r.digest,
      user_id: r.user_id,
      source: r.source,
      session_id: r.session_id,
    }));
    if (payload.length > 0) {
      const out = await supabase.query<{ result: { upserted: number } }>("SELECT sessions.ingest_seed_records($1::jsonb) AS result", [JSON.stringify(payload)]);
      result.upserted += Number(out.rows[0]?.result.upserted ?? 0);
    }
    result.pages += 1;
    result.read += rows.rows.length;
    const last = rows.rows[rows.rows.length - 1]!;
    after = [last.workspace_id, last.origin_id, last.record_id];
    opts.log?.({ outcome: "page", count: rows.rows.length, inserted: result.upserted });
    if (rows.rows.length < page) break;
  }
  return result;
}

/** Copy the org's Neon fences (tombstones and resets) after its revisions. Idempotent. */
export async function seedFences(
  neon: { readOnly<T>(work: (sql: SeedSql) => Promise<T>): Promise<T> },
  supabase: SeedSql,
  orgId: string,
): Promise<{ deletions: number; resets: number }> {
  const [deletions, resets] = await neon.readOnly(async (sql) => [
    (await sql.query(`SELECT org_id, workspace_id, user_id, origin_id, source, session_id FROM sessions.deleted_upload_sessions WHERE org_id = $1`, [orgId])).rows,
    (await sql.query(`SELECT org_id, workspace_id, user_id, request_id, deleted::text AS deleted FROM sessions.session_resets WHERE org_id = $1`, [orgId])).rows,
  ]);
  const out = { deletions: 0, resets: 0 };
  for (let i = 0; i < Math.max(deletions.length, resets.length); i += 2000) {
    const d = deletions.slice(i, i + 2000);
    const r = resets.slice(i, i + 2000).map((x) => ({ ...x, deleted: Number((x as { deleted: string }).deleted) }));
    const res = await supabase.query<{ result: { deletions: number; resets: number } }>("SELECT sessions.ingest_seed_fences($1::jsonb) AS result", [JSON.stringify({ deletions: d, resets: r })]);
    out.deletions += Number(res.rows[0]?.result.deletions ?? 0);
    out.resets += Number(res.rows[0]?.result.resets ?? 0);
  }
  return out;
}

/**
 * The exporter's verdict per session, from its SQLite state (neon-s3-sync
 * state.ts): exported at or past the session's Neon revision, with no error.
 */
export function exportedFromState(rows: { session_id: string; group_revision: number; exported_revision: number | null; error: string | null; failed_revision: number | null }[]): Exported {
  const ok = new Set(rows.filter((r) => r.exported_revision !== null && r.exported_revision >= r.group_revision && r.error === null && r.failed_revision === null).map((r) => r.session_id));
  return (sessionId) => ok.has(sessionId);
}
