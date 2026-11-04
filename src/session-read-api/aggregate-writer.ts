/**
 * Publication writer: canonical fold -> compact candidates -> one ready
 * publication per scope, fenced by the fold's source CAS. A later publication
 * supersedes the prior ones for the same scope, so reads see exactly one ready
 * set — staged rows from a non-ready publication stay invisible.
 */
import type { SqlQuery, TenantRunner } from "./liveness.ts";
import type { CandidateRow } from "./aggregate-fold.ts";

export interface PublicationInput {
  orgId: string; workspaceId: string;
  sourceEpoch: Date; unionCas: string;
  candidates: CandidateRow[];
}

export async function publishAggregates(
  run: TenantRunner, input: PublicationInput,
): Promise<{ publicationId: number; rows: number }> {
  return run(input.orgId, input.workspaceId, async (q: SqlQuery) => {
    const pub = await q.query<{ id: string }>(
      `INSERT INTO sessions.session_aggregate_publications
         (org_id, workspace_id, source_epoch, union_cas, ready)
       VALUES ($1,$2,$3,$4,false) RETURNING id`,
      [input.orgId, input.workspaceId, input.sourceEpoch.toISOString(), input.unionCas]);
    const id = Number(pub.rows[0]!.id);
    for (const c of input.candidates)
      await q.query(
        `INSERT INTO sessions.session_aggregate_candidates
           (org_id, workspace_id, source, session_id, candidate_type, dedup_key, ts, stats, provenance, publication_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (publication_id, candidate_type, dedup_key) DO NOTHING`,
        [input.orgId, input.workspaceId, c.source, c.sessionId, c.type,
         c.dedupKey, c.ts, JSON.stringify(c.stats ?? {}), JSON.stringify(c.provenance ?? {}), id]);
    // Fence: a publication goes ready only while the fold's CAS is still the
    // current source — a superseded CAS means a newer fold landed mid-write.
    const check = await q.query<{ cas: string }>(
      `SELECT union_cas AS cas FROM sessions.session_aggregate_publications
        WHERE org_id=$1 AND workspace_id=$2 AND ready AND superseded_by IS NULL
        ORDER BY published_at DESC LIMIT 1`,
      [input.orgId, input.workspaceId]);
    if (check.rows.length && check.rows[0]!.cas === input.unionCas) return { publicationId: id, rows: 0 };
    await q.query(
      `UPDATE sessions.session_aggregate_publications SET superseded_by=$3
        WHERE org_id=$1 AND workspace_id=$2 AND ready AND superseded_by IS NULL`,
      [input.orgId, input.workspaceId, id]);
    await q.query(
      `UPDATE sessions.session_aggregate_publications SET ready=true WHERE id=$1`, [id]);
    return { publicationId: id, rows: input.candidates.length };
  });
}
