/**
 * Backfill for sessions.ingest_claim_sessions (slot 044080): walks sessions.ingest_claim_attempts in
 * claim_token order from the policy row's backfill_cursor, one bounded transaction per batch, and writes the
 * distinct (source, session_id) side rows of every claim's records (the same rule as the writer trigger
 * sessions.ingest_claim_sessions_sync). It also deletes side rows whose pair is no longer in the claim's
 * records (a retired claim has records NULL) and refreshes accepted_at for batches committed since.
 *
 * Resumable: each batch commits its side rows and the advanced cursor atomically, so a stop (timeout, crash,
 * --max-batches) resumes where it left off. It marks backfill_complete_at only when the walk reaches the end
 * while write_enabled is true (the policy CHECK refuses it otherwise; and any write_enabled change clears
 * the cursor and completion, so a walk always starts after the writer was turned on).
 *
 * The report holds counts and booleans only: never a claim token, the cursor value, a tenant id or a record.
 */
export interface SqlClient { query(sql: string, params?: unknown[]): Promise<{ rows: any[] }> }

export interface ClaimSessionsBackfillOptions {
  readonly sql: SqlClient;
  readonly batchSize: number;
  readonly statementTimeoutMs: number;
  /** Stop after this many batches (completion is then not marked unless the end was reached). */
  readonly maxBatches?: number;
  /** Count only, in READ ONLY transactions: no side row, cursor or completion is written. */
  readonly dryRun: boolean;
}

export interface ClaimSessionsBackfillReport {
  mode: "dry-run" | "execute";
  writeEnabled: boolean;
  alreadyComplete: boolean;
  resumed: boolean;
  batches: number;
  claimsScanned: number;
  /** dry-run: pairs found in the scanned records that have no side row yet. */
  rowsMissing: number;
  rowsInserted: number;
  rowsRefreshed: number;
  rowsDeleted: number;
  reachedEnd: boolean;
  completionMarked: boolean;
  /** Set when completion was not marked: "write_disabled", "max_batches" or "dry_run". */
  completionNotMarkedReason: "write_disabled" | "max_batches" | "dry_run" | null;
}

export class ClaimSessionsBackfillError extends Error {
  override name = "ClaimSessionsBackfillError";
  constructor(readonly code: string) { super(code); }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// The batch range is (cursor, last]: claim_token > $1 (NULL = from the start) and <= $2.
const RANGE = "($1::uuid IS NULL OR a.claim_token>$1::uuid) AND a.claim_token<=$2::uuid";
const ELEMENTS = "jsonb_array_elements(CASE WHEN jsonb_typeof(a.records)='array' THEN a.records ELSE '[]'::jsonb END) AS e(v)";
const VALID = "jsonb_typeof(e.v)='object' AND jsonb_typeof(e.v->'source')='string' AND jsonb_typeof(e.v->'session_id')='string'";
const ACCEPTED = "(SELECT b.committed_at FROM sessions.ingest_batches AS b WHERE b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id AND b.state='committed')";

/** The next batch: locked FOR SHARE when writing, so a concurrent retirement either commits first (and the
 * insert below reads its NULL records) or waits for this batch (and its trigger then deletes the rows). */
const nextBatch = (lock: boolean) =>
  "SELECT count(*)::int AS n,(array_agg(t.claim_token ORDER BY t.claim_token DESC))[1]::text AS last FROM (" +
  "SELECT a.claim_token FROM sessions.ingest_claim_attempts AS a WHERE ($1::uuid IS NULL OR a.claim_token>$1::uuid) " +
  "ORDER BY a.claim_token LIMIT $2" + (lock ? " FOR SHARE" : "") + ") AS t";

const INSERT =
  "WITH w AS (INSERT INTO sessions.ingest_claim_sessions(claim_token,source,session_id,org_id,workspace_id,user_id,origin_id,batch_id,digest,reset_epoch,accepted_at) " +
  "SELECT DISTINCT a.claim_token,e.v->>'source',e.v->>'session_id',a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch," + ACCEPTED +
  " FROM sessions.ingest_claim_attempts AS a CROSS JOIN LATERAL " + ELEMENTS + " WHERE " + RANGE + " AND " + VALID +
  " ON CONFLICT(claim_token,source,session_id) DO UPDATE SET accepted_at=excluded.accepted_at" +
  " WHERE sessions.ingest_claim_sessions.accepted_at IS DISTINCT FROM excluded.accepted_at" +
  " RETURNING (xmax=0) AS inserted) SELECT count(*) FILTER (WHERE inserted)::int AS inserted,count(*) FILTER (WHERE NOT inserted)::int AS refreshed FROM w";

const DELETE =
  "WITH d AS (DELETE FROM sessions.ingest_claim_sessions AS s USING sessions.ingest_claim_attempts AS a" +
  " WHERE s.claim_token=a.claim_token AND ($1::uuid IS NULL OR s.claim_token>$1::uuid) AND s.claim_token<=$2::uuid AND " + RANGE +
  " AND NOT EXISTS(SELECT 1 FROM " + ELEMENTS + " WHERE jsonb_typeof(e.v)='object' AND e.v->'source'=to_jsonb(s.source) AND e.v->'session_id'=to_jsonb(s.session_id))" +
  " RETURNING 1) SELECT count(*)::int AS deleted FROM d";

const MISSING =
  "SELECT count(*)::int AS missing FROM (SELECT DISTINCT a.claim_token,e.v->>'source' AS source,e.v->>'session_id' AS session_id" +
  " FROM sessions.ingest_claim_attempts AS a CROSS JOIN LATERAL " + ELEMENTS + " WHERE " + RANGE + " AND " + VALID + ") AS p" +
  " WHERE NOT EXISTS(SELECT 1 FROM sessions.ingest_claim_sessions AS s WHERE s.claim_token=p.claim_token AND s.source=p.source AND s.session_id=p.session_id)";

export async function backfillClaimSessions(o: ClaimSessionsBackfillOptions): Promise<ClaimSessionsBackfillReport> {
  if (!Number.isInteger(o.batchSize) || o.batchSize < 1 || o.batchSize > 10000) throw new ClaimSessionsBackfillError("batch_size_invalid");
  if (!Number.isInteger(o.statementTimeoutMs) || o.statementTimeoutMs < 100 || o.statementTimeoutMs > 600000) throw new ClaimSessionsBackfillError("statement_timeout_invalid");
  if (o.maxBatches !== undefined && (!Number.isInteger(o.maxBatches) || o.maxBatches < 1)) throw new ClaimSessionsBackfillError("max_batches_invalid");
  const { sql } = o;
  const report: ClaimSessionsBackfillReport = {
    mode: o.dryRun ? "dry-run" : "execute", writeEnabled: false, alreadyComplete: false, resumed: false, batches: 0,
    claimsScanned: 0, rowsMissing: 0, rowsInserted: 0, rowsRefreshed: 0, rowsDeleted: 0,
    reachedEnd: false, completionMarked: false, completionNotMarkedReason: null,
  };
  // Dry-run walks an in-memory cursor; execute re-reads the committed cursor under the row lock each batch.
  let dryCursor: string | null | undefined;
  for (;;) {
    if (o.maxBatches !== undefined && report.batches >= o.maxBatches) break;
    await sql.query(o.dryRun ? "BEGIN READ ONLY" : "BEGIN");
    try {
      await sql.query("SELECT set_config('statement_timeout',$1,true)", [String(o.statementTimeoutMs)]);
      const pol = (await sql.query("SELECT write_enabled,backfill_cursor,backfill_complete_at IS NOT NULL AS complete FROM sessions.ingest_claim_sessions_policy WHERE singleton" + (o.dryRun ? "" : " FOR UPDATE"))).rows[0];
      if (!pol) throw new ClaimSessionsBackfillError("policy_row_missing");
      report.writeEnabled = pol.write_enabled === true;
      if (pol.complete === true) { report.alreadyComplete = true; await sql.query("COMMIT"); break; }
      if (dryCursor === undefined) { dryCursor = pol.backfill_cursor ?? null; report.resumed = dryCursor !== null; }
      const cursor: string | null = o.dryRun ? dryCursor : (pol.backfill_cursor ?? null);
      if (cursor !== null && !UUID.test(cursor)) throw new ClaimSessionsBackfillError("cursor_invalid");
      const batch = (await sql.query(nextBatch(!o.dryRun), [cursor, o.batchSize])).rows[0] as { n: number; last: string | null };
      const n = Number(batch.n);
      if (n > 0) {
        report.batches++;
        report.claimsScanned += n;
        if (o.dryRun) {
          report.rowsMissing += Number((await sql.query(MISSING, [cursor, batch.last])).rows[0].missing);
          dryCursor = batch.last;
        } else {
          const ins = (await sql.query(INSERT, [cursor, batch.last])).rows[0];
          report.rowsInserted += Number(ins.inserted);
          report.rowsRefreshed += Number(ins.refreshed);
          report.rowsDeleted += Number((await sql.query(DELETE, [cursor, batch.last])).rows[0].deleted);
        }
      }
      const end = n < o.batchSize;
      if (!o.dryRun) {
        // The completion stamp only while the writer is on (the CHECK enforces the same).
        await sql.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_cursor=coalesce($1,backfill_cursor)," +
          "backfill_complete_at=CASE WHEN $2 AND write_enabled THEN clock_timestamp() ELSE backfill_complete_at END WHERE singleton",
          [n > 0 ? batch.last : null, end]);
      }
      await sql.query("COMMIT");
      if (end) { report.reachedEnd = true; break; }
    } catch (e) {
      await sql.query("ROLLBACK").catch(() => {});
      throw e;
    }
  }
  if (!report.alreadyComplete) {
    report.completionMarked = !o.dryRun && report.reachedEnd && report.writeEnabled;
    if (!report.completionMarked)
      report.completionNotMarkedReason = o.dryRun ? "dry_run" : !report.reachedEnd ? "max_batches" : "write_disabled";
  }
  return report;
}
