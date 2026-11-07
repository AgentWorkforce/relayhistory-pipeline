/**
 * The Neon session census (046000 sessions.session_neon_census): every
 * (org, workspace, user, source, session) Neon holds ANY row for, including turns-only, link-only
 * and outcome-only sessions that have no delivery record. The never-in-Neon proof
 * (session_analysis_neon_free) admits only against a SEALED census generation that names no row
 * for the session.
 *
 * Read side: one REPEATABLE READ READ ONLY transaction on Neon (a single consistent snapshot),
 * one server-side cursor over the DISTINCT union of every session-bearing table. Write side: the
 * owner-only sessions.session_neon_census_load (batches, ON CONFLICT DO NOTHING, resumable into
 * the same unsealed generation) and sessions.session_neon_census_seal (recomputes rows + digest,
 * flips only on an exact match). Never writes Neon. Never logs credentials or rows.
 */
import { createHash } from "node:crypto";

export interface Queryable {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

/** [org, workspace, user, source, session]; a column Neon does not carry for that table is ''. */
export type CensusRow = [string, string, string, string, string];

/**
 * Every Neon table that can name a session. A missing table refuses the read (an incomplete
 * census must never be sealed). Column choices are the ones the export/eraser code already uses.
 */
export const CENSUS_SOURCES = [
  { table: "delivery_records", select: "org_id, workspace_id, user_id, source, session_id" },
  { table: "conversation_turns", select: "org_id, workspace_id, NULL::text, source, session_id" },
  { table: "convergence_events", select: "org_id, workspace_id, NULL::text, source, session_id" },
  { table: "session_links", select: "org_id, workspace_id, NULL::text, source, session_id" },
  { table: "session_outcomes", select: "org_id, workspace_id, user_id, source, session_id" },
  { table: "session_rollups", select: "org_id, NULL::text, NULL::text, NULL::text, session_id" },
  { table: "session_catalog", select: "org_id, NULL::text, NULL::text, NULL::text, session_id" },
] as const;

export const CENSUS_SQL = `SELECT DISTINCT coalesce(c0, '') AS org_id, coalesce(c1, '') AS workspace_id, coalesce(c2, '') AS user_id,
       coalesce(c3, '') AS source, c4 AS session_id
  FROM (${CENSUS_SOURCES.map((s) => `SELECT ${s.select} FROM sessions.${s.table}`).join(" UNION ALL ")}) AS u(c0, c1, c2, c3, c4)
 WHERE c4 IS NOT NULL AND c4 <> '' AND c0 IS NOT NULL AND c0 <> ''`;

/** The census line of one row: length-prefixed (UTF-8 octets), so no separator is ambiguous. */
export function censusLine(row: CensusRow): string {
  return row.map((f) => `${Buffer.byteLength(f, "utf8")}:${f}`).join("|");
}

/** Order-independent digest, identical to sessions.session_neon_census_digest. */
export class CensusDigest {
  rows = 0n;
  sum = 0n;
  add(row: CensusRow) {
    this.rows += 1n;
    this.sum += BigInt("0x" + createHash("sha256").update(censusLine(row), "utf8").digest("hex").slice(0, 15));
  }
  get sha256(): string {
    return createHash("sha256").update(`neon-census-v1:${this.rows}:${this.sum}`, "utf8").digest("hex");
  }
}

export interface NeonCensus {
  rows: CensusRow[];
  count: number;
  sha256: string;
  /** Snapshot identity and per-table presence, recorded in the seal. */
  snapshot: { snapshot: string; read_at: string; lsn: string | null; source_identity: Record<string, unknown>; tables: string[] };
}

/**
 * Read the census from Neon, read-only, in one snapshot. `connection` must be a single
 * connection (the cursor lives in its transaction). Rows are kept in memory (one census is
 * ids only) so the same set is both digested and loaded.
 */
export async function readNeonCensus(connection: Queryable, opts: { fetch?: number; statementTimeoutMs?: number } = {}): Promise<NeonCensus> {
  const fetch = opts.fetch ?? 5000;
  await connection.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await connection.query(`SET LOCAL statement_timeout = ${Math.max(1000, Math.floor(opts.statementTimeoutMs ?? 900_000))}`);
    for (const s of CENSUS_SOURCES) {
      const { rows } = await connection.query<{ t: string | null }>("SELECT to_regclass($1)::text AS t", [`sessions.${s.table}`]);
      if (!rows[0]?.t) throw new Error(`neon_census_source_missing: sessions.${s.table}`);
    }
    const ident = (await connection.query<{ snapshot: string; read_at: string; database: string }>(
      "SELECT pg_current_snapshot()::text AS snapshot, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS read_at, current_database() AS database",
    )).rows[0]!;
    const lsn = await optional(connection, "SELECT pg_current_wal_lsn()::text AS v");
    const system = await optional(connection, "SELECT system_identifier::text AS v FROM pg_control_system()");
    await connection.query(`DECLARE neon_census NO SCROLL CURSOR FOR ${CENSUS_SQL}`);
    const digest = new CensusDigest();
    const out: CensusRow[] = [];
    for (;;) {
      const { rows } = await connection.query<Record<string, string>>(`FETCH ${fetch} FROM neon_census`);
      if (!rows.length) break;
      for (const r of rows) {
        const row: CensusRow = [r.org_id!, r.workspace_id!, r.user_id!, r.source!, r.session_id!];
        digest.add(row);
        out.push(row);
      }
    }
    await connection.query("CLOSE neon_census");
    await connection.query("COMMIT");
    return {
      rows: out,
      count: Number(digest.rows),
      sha256: digest.sha256,
      snapshot: {
        snapshot: ident.snapshot,
        read_at: ident.read_at,
        lsn,
        source_identity: { database: ident.database, system_identifier: system },
        tables: CENSUS_SOURCES.map((s) => `sessions.${s.table}`),
      },
    };
  } catch (error) {
    await connection.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function optional(c: Queryable, sql: string): Promise<string | null> {
  try {
    await c.query("SAVEPOINT neon_census_optional");
    const v = (await c.query<{ v: string }>(sql)).rows[0]?.v ?? null;
    await c.query("RELEASE SAVEPOINT neon_census_optional");
    return v;
  } catch {
    await c.query("ROLLBACK TO SAVEPOINT neon_census_optional");
    return null;
  }
}

/** The generation a census loads under: its own digest (a different Neon read = a new generation). */
export const censusGeneration = (census: Pick<NeonCensus, "sha256">) => census.sha256;

export interface SealState {
  generation: string;
  census_rows: string | number;
  census_sha256: string;
}

export async function currentSeal(supabase: Queryable): Promise<SealState | null> {
  const { rows } = await supabase.query<SealState>(
    "SELECT generation, census_rows, census_sha256 FROM sessions.session_neon_census_seal WHERE singleton",
  );
  return rows[0] ?? null;
}

/** --execute: load (resumable) and seal. Returns what happened; never prints rows. */
export async function loadAndSeal(supabase: Queryable, census: NeonCensus, opts: { batch?: number; log?: (m: string, f?: Record<string, unknown>) => void } = {}) {
  const log = opts.log ?? (() => {});
  const generation = censusGeneration(census);
  const seal = await currentSeal(supabase);
  if (seal?.generation === generation) {
    log("census already sealed", { generation, rows: census.count });
    return { generation, inserted: 0, sealed: true, already: true };
  }
  const batch = Math.min(10_000, Math.max(1, opts.batch ?? 5000));
  let inserted = 0;
  for (let i = 0; i < census.rows.length; i += batch) {
    const { rows } = await supabase.query<{ n: number }>("SELECT sessions.session_neon_census_load($1, $2::jsonb) AS n", [
      generation,
      JSON.stringify(census.rows.slice(i, i + batch)),
    ]);
    inserted += Number(rows[0]?.n ?? 0);
    log("census batch", { from: i, size: Math.min(batch, census.rows.length - i), inserted });
  }
  const { rows } = await supabase.query<{ r: { sealed: boolean; already: boolean } }>(
    "SELECT sessions.session_neon_census_seal($1, $2::bigint, $3, $4::jsonb) AS r",
    [generation, census.count, census.sha256, JSON.stringify({ ...census.snapshot, rows: census.count })],
  );
  return { generation, inserted, sealed: rows[0]?.r.sealed === true, already: rows[0]?.r.already === true };
}

/** --verify: Neon's recomputed census vs the seal vs the sealed rows' recomputed digest. */
export async function verifyCensus(supabase: Queryable, census: NeonCensus) {
  const seal = await currentSeal(supabase);
  if (!seal) return { ok: false, reason: "census_unsealed" as const, neon: { rows: census.count, sha256: census.sha256 } };
  const { rows } = await supabase.query<{ d: { rows: number; sha256: string } }>("SELECT sessions.session_neon_census_digest($1) AS d", [seal.generation]);
  const stored = rows[0]!.d;
  const neon = { rows: census.count, sha256: census.sha256 };
  const sealed = { rows: Number(seal.census_rows), sha256: seal.census_sha256 };
  const ok = neon.rows === sealed.rows && neon.sha256 === sealed.sha256 && Number(stored.rows) === sealed.rows && stored.sha256 === sealed.sha256;
  return { ok, reason: ok ? ("match" as const) : ("mismatch" as const), neon, sealed, stored: { rows: Number(stored.rows), sha256: stored.sha256 } };
}
