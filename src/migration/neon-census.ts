/**
 * Neon session census for sessions.session_analysis_neon_free() (migration 20261105046000).
 *
 * The census is every (org_id, workspace_id, session_id) Neon holds ANY row for, in ANY `sessions`
 * base table with org_id and session_id columns (over-inclusion only makes neon_free false more often,
 * which fails closed). Migration 20261105046000 requires it to be taken AFTER Neon refuses legacy turn
 * writes: a census from an earlier cut misses later sessions and makes neon_free fail OPEN for them.
 *
 *   produce  read-only. One REPEATABLE READ READ ONLY snapshot on CENSUS_SOURCE_URL, with
 *            row_security = off (an RLS-filtered read errors instead of silently shrinking the census).
 *            Before reading it proves the source is a frozen cut, and refuses otherwise:
 *            - the source must be a HOT STANDBY (pg_is_in_recovery() = true): in practice the read-only
 *              endpoint of a Neon branch taken at the cut. Nothing can write to a standby, so no privilege,
 *              definer function or PUBLIC grant can change the census while it is read. A Supabase URL is
 *              refused outright;
 *            - the standby's replay LSN (read in the snapshot's FIRST statement) must be >= --source-lsn, and
 *              --source-lsn must be above 0/0;
 *            - a source that has sessions.session_neon_census (a pipeline target, i.e. swapped URLs) is refused;
 *            - a `sessions` foreign table with census keys, or a `sessions` partition whose parent is outside
 *              `sessions`, is refused.
 *            It writes FILE ("org\tworkspace\tsession\n" lines, strictly sorted COLLATE "C"), then
 *            FILE.manifest.json last (census_rows, census_sha256, neon_snapshot with the claimed and replayed
 *            LSN, per-table counts, unkeyed rows, ignored relations). It refuses to start if either file exists.
 *   load     reads only FILE and its manifest (never a database source), verifies them, and without
 *            --execute stops there WITHOUT connecting. With --execute and the target guard
 *            (--expect-supabase-ref) it runs ONE transaction on BULLS_TARGET_URL as the sessions schema
 *            owner: lock_timeout 5 s; LOCK both census tables IN EXCLUSIVE MODE (blocks other loaders and
 *            writers, not readers: MVCC hides the uncommitted rows and seal from session_analysis_neon_free
 *            readers); refuse an existing seal or rows; batched INSERT ... SELECT unnest; recompute the digest
 *            in SQL; insert the singleton seal; COMMIT; read back.
 *
 * NULL keys. neon_free matches the census on org_id and session_id only, so a NULL workspace_id (and a
 * table with no workspace_id column at all) must NOT drop the session: it is kept with workspace_id =
 * NULL_WORKSPACE. A row with a NULL org_id or session_id cannot be keyed to any session; it is excluded and
 * counted per table in neon_snapshot.unkeyed_rows. Partitions are read through their parent only; views
 * and materialized views are derived and are listed in neon_snapshot.ignored_relations, not read.
 *
 * Canonical digest: sha256 over the concatenation, in (org_id, workspace_id, session_id) order COLLATE "C"
 * (UTF-8 byte order), of `org_id \t workspace_id \t session_id \n` encoded UTF-8. Any key holding a tab,
 * CR or newline is refused (it would make the encoding ambiguous). The manifest digest detects accidental
 * damage only: an edited file with a recomputed manifest still verifies, so the files must be produced and
 * loaded by the operator, from a protected location, with the produce receipt kept.
 *
 * Size ceiling: produce and load hold the census in memory, and the SQL digest builds it as one text value
 * (PostgreSQL's 1 GB limit), so about 9-10 million rows at most. That fails closed (an error, no seal). The
 * frozen copy has far fewer sessions.
 *
 * Usage:
 *   CENSUS_SOURCE_URL=<Neon read-only branch endpoint at the cut> bun run migration:neon-census produce --out=FILE
 *       --source-lsn=<cut LSN> --source-ref=<branch id> [--source-seal=<label>]
 *   BULLS_TARGET_URL=... bun run migration:neon-census load --in=FILE [--execute --expect-supabase-ref=<ref>]
 * No URL, row value or credential is printed; output is counts and digests only.
 */
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { guardFromArgv, supabaseProjectRef } from "./target-guard.js";

export interface Queryable {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export interface CensusRow {
  org_id: string;
  workspace_id: string;
  session_id: string;
}

export interface CensusManifest {
  version: 1;
  census_rows: number;
  census_sha256: string;
  neon_snapshot: {
    source_lsn: string;
    replayed_lsn: string;
    in_recovery: true;
    source_ref: string;
    source_seal: string | null;
    snapshot_at: string;
    tables: Record<string, number>;
    /** Per table: rows excluded because org_id or session_id is NULL (they cannot be keyed to a session). */
    unkeyed_rows: Record<string, number>;
    /** sessions relations with org_id + session_id that were not read (views, materialized views), by kind. */
    ignored_relations: Record<string, string>;
    null_workspace: string;
  };
}

const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;

/** Stand-in for a NULL or absent workspace_id. neon_free() ignores workspace_id; only org + session must be exact. */
export const NULL_WORKSPACE = "~neon-census:null-workspace~";

export interface CensusRelation {
  name: string;
  hasWorkspace: boolean;
}

/**
 * Base tables (non-partition tables and partitioned parents) in `sessions` with org_id and session_id.
 * Views/matviews are reported, not read; a foreign table with those columns is refused.
 */
export async function censusRelations(db: Queryable): Promise<{ tables: CensusRelation[]; ignored: Record<string, string> }> {
  const { rows: stray } = await db.query<{ relname: string }>(
    `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_inherits i ON i.inhrelid = c.oid JOIN pg_class p ON p.oid = i.inhparent
      WHERE n.nspname = 'sessions' AND c.relispartition AND p.relnamespace <> n.oid`,
  );
  if (stray.length > 0) throw new Error(`sessions partition ${stray[0]!.relname} has a parent outside sessions: refusing`);
  const { rows } = await db.query<{ relname: string; relkind: string; relispartition: boolean; ws: boolean }>(
    `SELECT c.relname, c.relkind::text AS relkind, c.relispartition,
            EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND NOT a.attisdropped AND a.attname = 'workspace_id') AS ws
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r','p','v','m','f')
        AND c.relname NOT IN ('session_neon_census')
        AND (SELECT count(*) FROM pg_attribute a
              WHERE a.attrelid = c.oid AND NOT a.attisdropped AND a.attnum > 0
                AND a.attname IN ('org_id','session_id')) = 2
      ORDER BY c.relname COLLATE "C"`,
  );
  const tables: CensusRelation[] = [];
  const ignored: Record<string, string> = {};
  for (const row of rows) {
    if (row.relkind === "f") throw new Error(`sessions foreign table ${row.relname} has census keys: refusing (rows live elsewhere)`);
    if (row.relkind === "v" || row.relkind === "m") {
      ignored[row.relname] = row.relkind === "v" ? "view" : "materialized view";
      continue;
    }
    if (row.relispartition) continue; // read through its partitioned parent
    tables.push({ name: row.relname, hasWorkspace: row.ws });
  }
  return { tables, ignored };
}

/** Back-compat helper: the census table names. */
export async function censusTables(db: Queryable): Promise<string[]> {
  return (await censusRelations(db)).tables.map((table) => table.name);
}

export function censusLine(row: CensusRow): string {
  for (const value of [row.org_id, row.workspace_id, row.session_id]) {
    if (typeof value !== "string" || value.length === 0) throw new Error("census key is empty or not text");
    if (/[\t\n\r]/.test(value)) throw new Error("census key contains a tab or newline");
  }
  return `${row.org_id}\t${row.workspace_id}\t${row.session_id}\n`;
}

/** Byte-wise ("C", UTF-8) ordering, matching ORDER BY ... COLLATE "C" (not JS UTF-16 order). */
export function compareC(a: CensusRow, b: CensusRow): number {
  for (const key of ["org_id", "workspace_id", "session_id"] as const) {
    const c = Buffer.compare(Buffer.from(a[key], "utf8"), Buffer.from(b[key], "utf8"));
    if (c !== 0) return c;
  }
  return 0;
}

export function censusDigest(rows: CensusRow[]): string {
  const hash = createHash("sha256");
  for (const row of rows) hash.update(censusLine(row), "utf8");
  return hash.digest("hex");
}

/** SQL that recomputes the canonical digest over sessions.session_neon_census. */
export const DIGEST_SQL = `SELECT count(*)::bigint AS n,
  encode(sha256(convert_to(coalesce(string_agg(org_id || E'\\t' || workspace_id || E'\\t' || session_id || E'\\n', ''
    ORDER BY org_id COLLATE "C", workspace_id COLLATE "C", session_id COLLATE "C"), ''), 'UTF8')), 'hex') AS sha
  FROM sessions.session_neon_census`;

const LSN = /^[0-9A-F]{1,8}\/[0-9A-F]{1,8}$/i;

/** Compare two pg_lsn text values (hi/lo hex). */
export function lsnCompare(a: string, b: string): number {
  if (!LSN.test(a) || !LSN.test(b)) throw new Error("not an LSN");
  const v = (x: string) => {
    const [hi, lo] = x.split("/");
    return (BigInt(`0x${hi}`) << 32n) + BigInt(`0x${lo}`);
  };
  const d = v(a) - v(b);
  return d === 0n ? 0 : d > 0n ? 1 : -1;
}

export interface SourceClaim {
  lsn: string;
  ref: string;
  seal: string | null;
  /** The source URL's Supabase project ref if it is one (from target-guard's parser), else null. */
  urlSupabaseRef: string | null;
}

/** The snapshot's first statement: standby status and replay LSN (exported so tests can stand in a standby). */
export const SOURCE_STATE_SQL =
  "SELECT pg_is_in_recovery() AS rec, pg_last_wal_replay_lsn()::text AS lsn, to_regclass('sessions.session_neon_census') IS NOT NULL AS target";

/** Inside the snapshot, as its first statement: prove the source is a frozen standby at or after the claimed cut. */
export async function verifySource(db: Queryable, claim: SourceClaim): Promise<{ replayed: string }> {
  if (!LSN.test(claim.lsn)) throw new Error("--source-lsn is not an LSN");
  if (lsnCompare(claim.lsn, "0/0") <= 0) throw new Error("--source-lsn must be above 0/0");
  if (claim.urlSupabaseRef !== null) throw new Error("the source is a Supabase URL: refusing (the census is read from a Neon standby at the cut)");
  const { rows } = await db.query<{ rec: boolean; lsn: string | null; target: boolean }>(SOURCE_STATE_SQL);
  const state = rows[0]!;
  if (state.target) throw new Error("the source has sessions.session_neon_census: it is a pipeline target, not Neon (swapped URLs?)");
  if (state.rec !== true || state.lsn === null) {
    throw new Error("the source is not a hot standby: read the census from a read-only endpoint of a Neon branch at the cut");
  }
  if (!LSN.test(state.lsn) || lsnCompare(state.lsn, claim.lsn) < 0) {
    throw new Error("the source is cut BEFORE --source-lsn: refusing (a census from an earlier cut fails open)");
  }
  return { replayed: state.lsn };
}

/** Read the distinct census from a verified frozen source, in one read-only snapshot. */
export async function produceCensus(
  db: Queryable,
  claim: SourceClaim,
): Promise<{ rows: CensusRow[]; manifest: CensusManifest }> {
  await db.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
  try {
    // An RLS-filtered read must error, not shrink the census (cf. legacy-export's row_security guard).
    await db.query("SET LOCAL row_security = off");
    const { replayed } = await verifySource(db, claim); // first statement: the snapshot's LSN
    const { tables, ignored } = await censusRelations(db);
    if (tables.length === 0) throw new Error("no sessions table carries org_id/session_id");
    const counts: Record<string, number> = {};
    const unkeyed: Record<string, number> = {};
    for (const table of tables) {
      const { rows } = await db.query<{ n: string; u: string }>(
        `SELECT count(*)::text AS n, count(*) FILTER (WHERE org_id IS NULL OR session_id IS NULL)::text AS u
           FROM sessions.${quoteIdent(table.name)}`,
      );
      counts[table.name] = Number(rows[0]!.n);
      if (rows[0]!.u !== "0") unkeyed[table.name] = Number(rows[0]!.u);
    }
    const nullWorkspace = NULL_WORKSPACE.replaceAll("'", "''");
    const union = tables
      .map((table) => {
        const ws = table.hasWorkspace ? `coalesce(workspace_id::text, '${nullWorkspace}')` : `'${nullWorkspace}'::text`;
        return `SELECT org_id::text AS org_id, ${ws} AS workspace_id, session_id::text AS session_id
                  FROM sessions.${quoteIdent(table.name)} WHERE org_id IS NOT NULL AND session_id IS NOT NULL`;
      })
      .join(" UNION ");
    const { rows } = await db.query<CensusRow>(
      `SELECT org_id, workspace_id, session_id FROM (${union}) u
        ORDER BY org_id COLLATE "C", workspace_id COLLATE "C", session_id COLLATE "C"`,
    );
    const { rows: at } = await db.query<{ t: string }>("SELECT now()::text AS t");
    await db.query("ROLLBACK");
    const sorted = [...rows].sort(compareC);
    return {
      rows: sorted,
      manifest: {
        version: 1,
        census_rows: sorted.length,
        census_sha256: censusDigest(sorted),
        neon_snapshot: {
          source_lsn: claim.lsn, replayed_lsn: replayed, in_recovery: true, source_ref: claim.ref,
          source_seal: claim.seal, snapshot_at: at[0]!.t, tables: counts, unkeyed_rows: unkeyed,
          ignored_relations: ignored, null_workspace: NULL_WORKSPACE,
        },
      },
    };
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

export function parseCensusFile(text: string): CensusRow[] {
  if (text.length > 0 && !text.endsWith("\n")) throw new Error("census file does not end with a newline");
  return text.split("\n").slice(0, -1).map((line) => {
    const parts = line.split("\t");
    if (parts.length !== 3) throw new Error("census line does not have 3 fields");
    return { org_id: parts[0]!, workspace_id: parts[1]!, session_id: parts[2]! };
  });
}

/** Verify a file against its manifest: sorted, unique, digest and row count equal. Detects accidental damage only. */
export function verifyCensus(rows: CensusRow[], manifest: CensusManifest): void {
  if (manifest.version !== 1) throw new Error("unknown manifest version");
  if (!/^[0-9a-f]{64}$/.test(manifest.census_sha256)) throw new Error("manifest digest is malformed");
  for (let i = 1; i < rows.length; i += 1) {
    if (compareC(rows[i - 1]!, rows[i]!) >= 0) throw new Error("census file is not strictly sorted and unique");
  }
  if (rows.length !== manifest.census_rows) throw new Error("census row count differs from the manifest");
  if (censusDigest(rows) !== manifest.census_sha256) throw new Error("census digest differs from the manifest");
}

/** The lock statements a load issues, exported for tests. */
export const LOAD_LOCK_SQL = [
  "SET LOCAL lock_timeout = '5s'",
  "LOCK TABLE sessions.session_neon_census, sessions.session_neon_census_seal IN EXCLUSIVE MODE",
];

/** Load into the target in one transaction; refuses an existing seal or rows. Returns the readback. */
export async function loadCensus(
  db: Queryable,
  rows: CensusRow[],
  manifest: CensusManifest,
  batch = 5000,
): Promise<{ census_rows: number; census_sha256: string; sealed_at: string }> {
  verifyCensus(rows, manifest);
  await db.query("BEGIN");
  try {
    for (const sql of LOAD_LOCK_SQL) await db.query(sql);
    const { rows: existing } = await db.query<{ seals: string; census: string }>(
      `SELECT (SELECT count(*) FROM sessions.session_neon_census_seal)::text AS seals,
              (SELECT count(*) FROM sessions.session_neon_census)::text AS census`,
    );
    if (existing[0]!.seals !== "0") throw new Error("a census seal already exists: refusing to load");
    if (existing[0]!.census !== "0") throw new Error("census rows already exist: refusing to load");
    for (let i = 0; i < rows.length; i += batch) {
      const chunk = rows.slice(i, i + batch);
      await db.query(
        `INSERT INTO sessions.session_neon_census (org_id, workspace_id, session_id)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[])`,
        [chunk.map((r) => r.org_id), chunk.map((r) => r.workspace_id), chunk.map((r) => r.session_id)],
      );
    }
    const { rows: digest } = await db.query<{ n: string; sha: string }>(DIGEST_SQL);
    if (Number(digest[0]!.n) !== manifest.census_rows || digest[0]!.sha !== manifest.census_sha256) {
      throw new Error("the loaded census does not match the manifest digest: rolled back");
    }
    await db.query(
      `INSERT INTO sessions.session_neon_census_seal (census_rows, census_sha256, neon_snapshot) VALUES ($1, $2, $3::jsonb)`,
      [manifest.census_rows, manifest.census_sha256, JSON.stringify(manifest.neon_snapshot)],
    );
    await db.query("COMMIT");
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
  let seal: Array<{ census_rows: string; census_sha256: string; sealed_at: string }>;
  let digest: Array<{ n: string; sha: string }>;
  try {
    seal = (await db.query<{ census_rows: string; census_sha256: string; sealed_at: string }>(
      "SELECT census_rows::text, census_sha256, sealed_at::text FROM sessions.session_neon_census_seal WHERE singleton",
    )).rows;
    digest = (await db.query<{ n: string; sha: string }>(DIGEST_SQL)).rows;
  } catch (error) {
    throw new Error(`COMMITTED; readback failed (${error instanceof Error ? error.message : "unknown error"}). Do not retry; read the seal back by hand`);
  }
  const s = seal[0];
  if (!s || Number(s.census_rows) !== manifest.census_rows || s.census_sha256 !== manifest.census_sha256
      || Number(digest[0]!.n) !== manifest.census_rows || digest[0]!.sha !== manifest.census_sha256) {
    throw new Error("COMMITTED; readback mismatch: the seal is in place but does not match the manifest. Do not retry; investigate");
  }
  return { census_rows: Number(s.census_rows), census_sha256: s.census_sha256, sealed_at: s.sealed_at };
}

const arg = (name: string) => process.argv.find((v) => v.startsWith(`--${name}=`))?.slice(name.length + 3);

export async function main() {
  const mode = process.argv[2];
  if (mode === "produce") {
    const url = process.env.CENSUS_SOURCE_URL;
    const out = arg("out"), lsn = arg("source-lsn"), ref = arg("source-ref");
    if (!url || !out || !lsn || !ref) throw new Error("produce needs CENSUS_SOURCE_URL, --out, --source-lsn and --source-ref");
    const claim: SourceClaim = { lsn, ref, seal: arg("source-seal") ?? null, urlSupabaseRef: supabaseProjectRef(url) };
    if (existsSync(out) || existsSync(`${out}.manifest.json`)) throw new Error("the output file or its manifest already exists: refusing");
    const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-census-produce" });
    await client.connect();
    try {
      const { rows, manifest } = await produceCensus(client, claim);
      await writeFile(out, rows.map(censusLine).join(""), { encoding: "utf8", mode: 0o600, flag: "wx" }); // manifest last: it marks completion
      await writeFile(`${out}.manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
      const s = manifest.neon_snapshot;
      process.stdout.write(`${JSON.stringify({ mode, census_rows: manifest.census_rows, census_sha256: manifest.census_sha256, replayed_lsn: s.replayed_lsn, tables: Object.keys(s.tables).length, unkeyed_rows: s.unkeyed_rows, ignored_relations: s.ignored_relations })}\n`);
    } finally {
      await client.end();
    }
    return;
  }
  if (mode === "load") {
    const file = arg("in");
    const url = process.env.BULLS_TARGET_URL;
    if (!file || !url) throw new Error("load needs --in and BULLS_TARGET_URL");
    const rows = parseCensusFile(await readFile(file, "utf8"));
    const manifest = JSON.parse(await readFile(`${file}.manifest.json`, "utf8")) as CensusManifest;
    verifyCensus(rows, manifest);
    const write = guardFromArgv(process.argv, url);
    if (write === "dry-run") {
      process.stdout.write(`${JSON.stringify({ mode, write, verified: true, census_rows: manifest.census_rows, census_sha256: manifest.census_sha256 })}\n`);
      return;
    }
    const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-census-load" });
    await client.connect();
    try {
      const readback = await loadCensus(client, rows, manifest);
      process.stdout.write(`${JSON.stringify({ mode, write, ...readback })}\n`);
    } finally {
      await client.end();
    }
    return;
  }
  throw new Error("usage: neon-census produce|load (see the header)");
}

if (import.meta.main) main().catch((error) => {
  console.error(`neon census failed: ${error instanceof Error ? error.message : "unknown error"}`);
  process.exit(1);
});
