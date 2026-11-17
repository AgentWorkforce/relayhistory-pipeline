/**
 * Neon session census for sessions.session_analysis_neon_free() (migration 20261105046000).
 *
 * The census is every (org_id, workspace_id, session_id) that Neon holds ANY row for, in ANY
 * `sessions` table with those three key columns (over-inclusion only makes neon_free false more
 * often, which fails closed). Migration 20261105046000 requires it to be taken AFTER Neon refuses
 * legacy turn writes: a census from an earlier cut misses later sessions and makes neon_free fail
 * OPEN for them. So `produce` must read a frozen source cut at or after that point (a read-only Neon
 * branch at the post-fence LSN, or a copy sealed at that LSN), never a live or earlier copy.
 *
 *   produce  read-only. Reads the source (CENSUS_SOURCE_URL) in one REPEATABLE READ READ ONLY
 *            snapshot and writes FILE (one "org\tworkspace\tsession\n" line per row, sorted
 *            COLLATE "C") and FILE.manifest.json (census_rows, census_sha256, per-table counts,
 *            the source identity from --source-lsn/--source-ref/--source-seal).
 *   load     writes only with --execute and the target guard. Reads FILE and its manifest (never a
 *            database source), verifies the digest, then in ONE transaction on BULLS_TARGET_URL
 *            (the sessions schema owner): refuses if a seal or any census row exists, COPYs the
 *            rows, recomputes the digest in SQL, and inserts the singleton seal. Then it reads it back.
 *            Without --execute it is a dry run: it verifies the file, and nothing else.
 *
 * Canonical digest: sha256 over the concatenation, in (org_id, workspace_id, session_id) order
 * COLLATE "C", of `org_id \t workspace_id \t session_id \n` encoded UTF-8. Any key holding a tab
 * or newline is refused (it would make the encoding ambiguous).
 *
 * Usage:
 *   CENSUS_SOURCE_URL=... bun run migration:neon-census produce --out=FILE --source-lsn=L --source-ref=R [--source-seal=S]
 *   BULLS_TARGET_URL=... bun run migration:neon-census load --in=FILE [--execute --expect-supabase-ref=<ref>]
 * No URL, row value or credential is printed; output is counts and digests only.
 */
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { guardFromArgv } from "./target-guard.js";

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
    source_ref: string;
    source_seal: string | null;
    snapshot_at: string;
    tables: Record<string, number>;
  };
}

const quoteIdent = (value: string) => `"${value.replaceAll('"', '""')}"`;

/** Tables in `sessions` that carry all three key columns, as text. Sorted for a stable plan. */
export async function censusTables(db: Queryable): Promise<string[]> {
  const { rows } = await db.query<{ relname: string }>(
    `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r','p')
        AND c.relname NOT IN ('session_neon_census')
        AND (SELECT count(*) FROM pg_attribute a
              WHERE a.attrelid = c.oid AND NOT a.attisdropped AND a.attnum > 0
                AND a.attname IN ('org_id','workspace_id','session_id')) = 3
      ORDER BY c.relname COLLATE "C"`,
  );
  return rows.map((row) => row.relname);
}

export function censusLine(row: CensusRow): string {
  for (const value of [row.org_id, row.workspace_id, row.session_id]) {
    if (typeof value !== "string" || value.length === 0) throw new Error("census key is empty or not text");
    if (/[\t\n\r]/.test(value)) throw new Error("census key contains a tab or newline");
  }
  return `${row.org_id}\t${row.workspace_id}\t${row.session_id}\n`;
}

/** Byte-wise ("C") ordering, matching ORDER BY ... COLLATE "C". */
export function compareC(a: CensusRow, b: CensusRow): number {
  for (const key of ["org_id", "workspace_id", "session_id"] as const) {
    const x = Buffer.from(a[key], "utf8");
    const y = Buffer.from(b[key], "utf8");
    const c = Buffer.compare(x, y);
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

/** Read the distinct census from a frozen source, in one read-only snapshot. */
export async function produceCensus(
  db: Queryable,
  source: { lsn: string; ref: string; seal: string | null },
): Promise<{ rows: CensusRow[]; manifest: CensusManifest }> {
  await db.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
  try {
    const tables = await censusTables(db);
    if (tables.length === 0) throw new Error("no sessions table carries org_id/workspace_id/session_id");
    const counts: Record<string, number> = {};
    for (const table of tables) {
      const { rows } = await db.query<{ n: string }>(`SELECT count(*)::text AS n FROM sessions.${quoteIdent(table)}`);
      counts[table] = Number(rows[0]!.n);
    }
    const union = tables
      .map((table) => `SELECT org_id::text, workspace_id::text, session_id::text FROM sessions.${quoteIdent(table)}`)
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
        neon_snapshot: { source_lsn: source.lsn, source_ref: source.ref, source_seal: source.seal, snapshot_at: at[0]!.t, tables: counts },
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

/** Verify a file against its manifest: sorted, unique, digest and row count equal. */
export function verifyCensus(rows: CensusRow[], manifest: CensusManifest): void {
  if (manifest.version !== 1) throw new Error("unknown manifest version");
  for (let i = 1; i < rows.length; i += 1) {
    if (compareC(rows[i - 1]!, rows[i]!) >= 0) throw new Error("census file is not strictly sorted and unique");
  }
  if (rows.length !== manifest.census_rows) throw new Error("census row count differs from the manifest");
  if (censusDigest(rows) !== manifest.census_sha256) throw new Error("census digest differs from the manifest");
  if (!/^[0-9a-f]{64}$/.test(manifest.census_sha256)) throw new Error("manifest digest is malformed");
}

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
    await db.query("LOCK TABLE sessions.session_neon_census, sessions.session_neon_census_seal IN ACCESS EXCLUSIVE MODE");
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
  const { rows: seal } = await db.query<{ census_rows: string; census_sha256: string; sealed_at: string }>(
    "SELECT census_rows::text, census_sha256, sealed_at::text FROM sessions.session_neon_census_seal WHERE singleton",
  );
  const { rows: digest } = await db.query<{ n: string; sha: string }>(DIGEST_SQL);
  const s = seal[0];
  if (!s || s.census_sha256 !== manifest.census_sha256 || digest[0]!.sha !== manifest.census_sha256) {
    throw new Error("readback after COMMIT does not match the manifest");
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
    if (!/^[0-9A-F]+\/[0-9A-F]+$/i.test(lsn)) throw new Error("--source-lsn is not an LSN");
    const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-census-produce" });
    await client.connect();
    try {
      const { rows, manifest } = await produceCensus(client, { lsn, ref, seal: arg("source-seal") ?? null });
      await writeFile(out, rows.map(censusLine).join(""), { encoding: "utf8", mode: 0o600, flag: "wx" });
      await writeFile(`${out}.manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
      process.stdout.write(`${JSON.stringify({ mode, census_rows: manifest.census_rows, census_sha256: manifest.census_sha256, tables: Object.keys(manifest.neon_snapshot.tables).length })}\n`);
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
