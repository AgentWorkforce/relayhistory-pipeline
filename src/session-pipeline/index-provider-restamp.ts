/**
 * Operator re-stamp of session_index rows a GCS install's consumer stamped `s3`.
 *
 * Before the consumer passed its Locker's provider to parseQueueBody (s3-events.ts), every row the
 * self-host consumer indexed was stamped storage_provider 's3', whatever LOCKER_PROVIDER said. On a
 * GCS install the read API (canonical.ts) refuses such a row (provider differs from its Locker's), so
 * replay and /v1/sessions/:id/events answer 501. New revisions are stamped correctly once the fixed
 * consumer runs, but upsert_session_index is revision-fenced, so an unchanged session keeps its 's3'
 * row until this tool re-stamps it.
 *
 * Why an operator tool and not a migration: the database records nothing that says which object
 * store an install uses (the frontier has no provider; publication.ts relies on LOCKER_PROVIDER), and
 * the migrator never sees LOCKER_PROVIDER. A migration gated on a GUC the migrator would bind from
 * env would be ledgered as applied (and never rerun) wherever that env was missing, would race the
 * old consumer still writing 's3' rows during a rollout, and could not prove the objects are in GCS.
 * This tool can: it is gated on the same Locker selection the read API uses, it is rerunnable, and it
 * re-stamps a row only after the GCS Locker answers for that row's manifest.
 *
 * Gates (in order; any failure exits 2 before a credential is read or a store or database touched):
 *   - LOCKER_PROVIDER must be exactly gcs (after trim, as lockerProvider). Unset or s3 is refused:
 *     an S3 install's rows are correct as they are.
 *   - LOCKER_BUCKET set. Only rows whose bucket equals it are candidates; 's3' rows in any other
 *     bucket are counted (other_bucket_s3) and never touched.
 *   - assertLockerSelection: no HMAC keys on GCS, a trusted token URI and endpoint.
 * Then, per row, the manifest must exist in the GCS Locker (HEAD through Workload Identity). A row
 * whose manifest is absent (a tombstone whose bytes are gone, a NULL manifest_key) is counted
 * `unproven` and left alone. A pod without GCS credentials fails its first HEAD before any write.
 * Tombstones (deleted_at set) are not excluded: one whose manifest still exists is re-stamped too,
 * which is harmless (the read API refuses a deleted row whatever its provider).
 *
 * Writes (--apply only): one autocommit UPDATE per row, re-checking in SQL every value it was
 * selected on (key, provider 's3', bucket, manifest_key, revision), so a row the consumer re-upserted
 * meanwhile is counted `raced` and kept as the consumer wrote it. A database guard can also make the
 * UPDATE a silent no-op (a BEFORE trigger returning NULL: ingest_erase_fence_index_guard for an
 * erase-fenced session, ingest_publication_guard_pointer for an acquisition-owned or retired /
 * non-live publication head). After a 0-row UPDATE the row is re-read: still unchanged ('s3', same
 * key, bucket, manifest_key, revision) means a guard kept it, counted `guard_skipped`, not `raced`.
 * Those rows still answer 501 and a rerun will not change them: they need an operator. statement_timeout 30s, lock_timeout
 * 1s (session-wide, reads included). The session_index triggers run as for any owner write (the history carrier sync re-keys the
 * row's carrier to gcs); a trigger refusal is counted by SQLSTATE in `refused` and the run goes on.
 * Idempotent: a rerun selects only rows still stamped 's3'.
 *
 * Usage (a one-shot Job or exec in the services image, with the read API's LOCKER_* env):
 *   bun run /app/src/session-pipeline/index-provider-restamp.ts            plan (default): writes nothing
 *   bun run /app/src/session-pipeline/index-provider-restamp.ts --apply [--page 200] [--max-pages N]
 * Env: LOCKER_PROVIDER=gcs, LOCKER_BUCKET, optional LOCKER_ENDPOINT / LOCKER_GCS_TOKEN_URI;
 *   INDEX_PROVIDER_RESTAMP_DATABASE_URL or INDEX_PROVIDER_RESTAMP_DATABASE_URL_FILE (exactly one): a
 *   login that can UPDATE sessions.session_index (the schema owner); SESSION_POSTGRES_TLS_PROFILE as
 *   for the other self-host workloads. Run it after the fixed consumer is deployed (a consumer still on
 *   the old code keeps writing 's3' rows; a rerun picks them up).
 * Exit: 0 done; 3 incomplete (--max-pages reached, or a refused row: rerun); 5 done, but rows a
 *   database guard kept (guard_skipped > 0, needs_attention true): a rerun will not change them, an
 *   operator must resolve the fence, acquisition owner or publication head first; 2 refused or
 *   misconfigured; 4 any other error (rerun). 3 wins over 5. Output: one JSON line of counts, no keys or ids.
 */
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { assertLockerSelection, lockerProvider } from "../shared/locker-selection.ts";
import { buildLockerStore } from "../shared/locker.ts";
import { objectKey } from "../shared/vendor.ts";
import { postgresTlsOptions } from "../shared/postgres-tls.ts";
import { supabasePool } from "./lambda.ts";

export class RestampRefused extends Error {
  override name = "RestampRefused";
  constructor(readonly code: string) { super(`index provider restamp refused: ${code}`); }
}
const refuse = (code: string): never => { throw new RestampRefused(code); };

export interface RestampDb {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

export interface RestampOptions {
  /** The GCS Locker's bucket (LOCKER_BUCKET), exactly as the consumer stamps it. */
  bucket: string;
  apply: boolean;
  pageSize: number;
  maxPages?: number;
  /** True only when the GCS Locker holds this manifest key. */
  exists: (manifestKey: string) => Promise<boolean>;
  log?: (event: Record<string, unknown>) => void;
}

export interface RestampResult {
  mode: "plan" | "apply";
  scanned: number;
  proven: number;
  unproven: number;
  restamped: number;
  raced: number;
  /** 0-row UPDATEs whose row is still unchanged: a BEFORE trigger guard returned NULL. */
  guard_skipped: number;
  refused: number;
  refused_codes: Record<string, number>;
  other_bucket_s3: number;
  complete: boolean;
  /** True when guard_skipped rows remain: done, but those rows still serve 501. */
  needs_attention: boolean;
}

interface Candidate {
  org_id: string; workspace_id: string; harness: string; session_id: string; machine_id: string;
  manifest_key: string | null; revision: string | null;
}

export const PAGE_SQL = `SELECT org_id, workspace_id, harness, session_id, machine_id, manifest_key, revision::text AS revision
  FROM sessions.session_index
 WHERE storage_provider = 's3' AND bucket = $1
   AND ($2::text IS NULL OR (org_id, workspace_id, harness, session_id, machine_id) > ($2, $3, $4, $5, $6))
 ORDER BY org_id, workspace_id, harness, session_id, machine_id
 LIMIT $7`;

export const UPDATE_SQL = `UPDATE sessions.session_index SET storage_provider = 'gcs'
 WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5
   AND storage_provider = 's3' AND bucket = $6 AND manifest_key = $7 AND revision IS NOT DISTINCT FROM $8::bigint
 RETURNING 1 AS restamped`;

/** Re-read after a 0-row UPDATE: the row still exactly as selected means a guard kept it. */
export const UNCHANGED_SQL = `SELECT 1 AS unchanged FROM sessions.session_index
 WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5
   AND storage_provider = 's3' AND bucket = $6 AND manifest_key = $7 AND revision IS NOT DISTINCT FROM $8::bigint`;

export const OTHER_BUCKET_SQL = `SELECT count(*)::text AS n FROM sessions.session_index
 WHERE storage_provider = 's3' AND bucket IS DISTINCT FROM $1`;

const SQLSTATE = /^[0-9A-Z]{5}$/;

export async function restampIndexProvider(db: RestampDb, o: RestampOptions): Promise<RestampResult> {
  if (!o.bucket) refuse("locker_bucket_missing");
  if (!Number.isInteger(o.pageSize) || o.pageSize < 1 || o.pageSize > 1000) refuse("page_invalid");
  const r: RestampResult = { mode: o.apply ? "apply" : "plan", scanned: 0, proven: 0, unproven: 0, restamped: 0, raced: 0, guard_skipped: 0, refused: 0, refused_codes: {}, other_bucket_s3: 0, complete: false, needs_attention: false };
  await db.query("SET statement_timeout = '30s'");
  await db.query("SET lock_timeout = '1s'");
  r.other_bucket_s3 = Number((await db.query<{ n: string }>(OTHER_BUCKET_SQL, [o.bucket])).rows[0]?.n ?? 0);
  let after: Candidate | undefined;
  for (let page = 0; o.maxPages === undefined || page < o.maxPages; page++) {
    const rows = (await db.query<Candidate>(PAGE_SQL, [o.bucket, after?.org_id ?? null, after?.workspace_id ?? null, after?.harness ?? null,
      after?.session_id ?? null, after?.machine_id ?? null, o.pageSize])).rows;
    if (rows.length === 0) { r.complete = true; break; }
    after = rows[rows.length - 1];
    // Prove the whole page before writing any of it: a Locker that cannot answer fails here, unwritten.
    const proven: Candidate[] = [];
    for (const row of rows) {
      r.scanned++;
      if (row.manifest_key && (await o.exists(row.manifest_key))) proven.push(row);
      else r.unproven++;
    }
    r.proven += proven.length;
    if (o.apply) {
      for (const row of proven) {
        try {
          const params = [row.org_id, row.workspace_id, row.harness, row.session_id, row.machine_id, o.bucket, row.manifest_key, row.revision];
          const done = await db.query(UPDATE_SQL, params);
          if (done.rows.length === 1) r.restamped++;
          else if ((await db.query(UNCHANGED_SQL, params)).rows.length === 1) r.guard_skipped++;
          else r.raced++;
        } catch (e) {
          const code = (e as { code?: unknown })?.code;
          if (typeof code !== "string" || !SQLSTATE.test(code)) throw e;
          r.refused++;
          r.refused_codes[code] = (r.refused_codes[code] ?? 0) + 1;
        }
      }
    }
    o.log?.({ event: "index_provider_restamp_page", page, scanned: r.scanned, proven: r.proven, restamped: r.restamped });
    if (rows.length < o.pageSize) { r.complete = true; break; }
  }
  r.needs_attention = r.guard_skipped > 0;
  return r;
}

/** Every gate that needs only env; throws RestampRefused. Reads no credential and touches nothing. */
export function restampGate(env: NodeJS.ProcessEnv): void {
  let provider: string;
  try { provider = lockerProvider(env); } catch { return refuse("locker_provider_invalid"); }
  if (provider !== "gcs") refuse("locker_provider_not_gcs");
  if (!env.LOCKER_BUCKET) refuse("locker_bucket_missing");
  try { assertLockerSelection(env); } catch { refuse("locker_selection_invalid"); }
}

export function restampArgs(argv: readonly string[]): { apply: boolean; pageSize: number; maxPages?: number } {
  const known = new Set(["--apply", "--plan", "--page", "--max-pages"]);
  const out: { apply: boolean; pageSize: number; maxPages?: number } = { apply: false, pageSize: 200 };
  let plan = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!known.has(a)) refuse("args_invalid");
    if (a === "--apply") out.apply = true;
    else if (a === "--plan") plan = true;
    else {
      const v = argv[++i];
      if (v === undefined || !/^\d{1,4}$/.test(v)) refuse("args_invalid");
      const n = Number(v);
      if (a === "--page") { if (n < 1 || n > 1000) refuse("args_invalid"); out.pageSize = n; }
      else { if (n < 1) refuse("args_invalid"); out.maxPages = n; }
    }
  }
  if (plan && out.apply) refuse("args_invalid");
  return out;
}

const MAX_FILE = 1 << 16;
export function restampDatabaseUrl(env: NodeJS.ProcessEnv): string {
  const value = env.INDEX_PROVIDER_RESTAMP_DATABASE_URL, file = env.INDEX_PROVIDER_RESTAMP_DATABASE_URL_FILE;
  if (value !== undefined && file !== undefined) refuse("secret_source_ambiguous");
  let text = value ?? "";
  if (file !== undefined) {
    if (!isAbsolute(file)) refuse("file_invalid");
    try {
      const st = statSync(file);
      if (!st.isFile() || st.size < 1 || st.size > MAX_FILE) refuse("file_invalid");
      text = readFileSync(file, "utf8").replace(/\r?\n$/, "");
    } catch (e) {
      if (e instanceof RestampRefused) throw e;
      refuse("file_invalid");
    }
  }
  if (!text) refuse("database_url_missing");
  return text;
}

export interface RestampMainOverrides {
  /** Test-only ports; the process entry never takes them from input. */
  db?: RestampDb;
  exists?: (manifestKey: string) => Promise<boolean>;
  log?: (event: Record<string, unknown>) => void;
}

export async function runRestampMain(env: NodeJS.ProcessEnv, argv: readonly string[], overrides: RestampMainOverrides = {}): Promise<RestampResult> {
  restampGate(env);
  const args = restampArgs(argv);
  const raw = buildLockerStore(env);
  if (raw.provider !== "gcs") refuse("locker_provider_not_gcs");
  const exists = overrides.exists ?? (async (key: string) => (await raw.store.head(objectKey(key))) !== undefined);
  let end: (() => Promise<void>) | undefined;
  let db = overrides.db;
  if (!db) {
    const pool = supabasePool(restampDatabaseUrl(env), 1, postgresTlsOptions(env));
    const client = await pool.pool.connect();
    end = async () => { client.release(); await pool.end(); };
    db = { query: async <R>(text: string, params?: unknown[]) => ({ rows: (await client.query(text, params as unknown[])).rows as R[] }) };
  }
  try {
    return await restampIndexProvider(db, { bucket: raw.bucket, ...args, exists, log: overrides.log ?? ((e) => console.error(JSON.stringify(e))) });
  } finally {
    await end?.().catch(() => {});
  }
}

export function exitCodeForResult(r: RestampResult): 0 | 3 | 5 {
  if (!r.complete || r.refused > 0) return 3;
  return r.guard_skipped > 0 ? 5 : 0;
}

export function exitCodeForError(e: unknown): 2 | 4 {
  return e instanceof RestampRefused ? 2 : 4;
}

if (import.meta.main) {
  runRestampMain(process.env, process.argv.slice(2))
    .then((r) => {
      console.log(JSON.stringify({ event: "index_provider_restamp", ...r }));
      process.exit(exitCodeForResult(r));
    })
    .catch((e) => {
      const code = e instanceof RestampRefused ? e.code : typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "restamp_failed";
      console.error(JSON.stringify({ event: "index_provider_restamp", error: code }));
      process.exit(exitCodeForError(e));
    });
}
