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
}

/** The seed RPC's limit is 5000 rows per call. */
export const SEED_PAGE = 2000;

interface Row {
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
  opts: { page?: number; log?: (fields: Record<string, unknown>) => void } = {},
): Promise<SeedResult> {
  const page = Math.min(opts.page ?? SEED_PAGE, 5000);
  let after: [string, string, string] | null = null;
  const result: SeedResult = { pages: 0, read: 0, upserted: 0 };
  for (;;) {
    const cursor: [string, string, string] | null = after;
    const rows: { rows: Row[] } = await neon.readOnly((sql): Promise<{ rows: Row[] }> =>
      sql.query<Row>(
        `SELECT workspace_id, origin_id, record_id, revision::text AS revision, revision_id, digest
           FROM sessions.delivery_records
          WHERE org_id = $1
            AND ($2::text IS NULL OR (workspace_id, origin_id, record_id) > ($2, $3, $4))
          ORDER BY workspace_id, origin_id, record_id
          LIMIT $5`,
        [orgId, cursor?.[0] ?? null, cursor?.[1] ?? null, cursor?.[2] ?? null, page],
      ),
    );
    if (rows.rows.length === 0) break;
    const payload = rows.rows.map((r) => ({
      org_id: orgId,
      workspace_id: r.workspace_id,
      origin_id: r.origin_id,
      record_id: r.record_id,
      revision: Number(r.revision),
      revision_id: r.revision_id,
      digest: r.digest,
    }));
    const out = await supabase.query<{ result: { upserted: number } }>("SELECT sessions.ingest_seed_records($1::jsonb) AS result", [JSON.stringify(payload)]);
    result.pages += 1;
    result.read += rows.rows.length;
    result.upserted += Number(out.rows[0]?.result.upserted ?? 0);
    const last = rows.rows[rows.rows.length - 1]!;
    after = [last.workspace_id, last.origin_id, last.record_id];
    opts.log?.({ outcome: "page", count: rows.rows.length, inserted: result.upserted });
    if (rows.rows.length < page) break;
  }
  return result;
}
