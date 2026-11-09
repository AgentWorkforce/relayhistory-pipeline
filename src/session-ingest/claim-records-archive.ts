/** Operator-only retirement of terminal ingest_claim_attempts.records.
 *
 * Order per attempt (intent-after-IO is safe here: SQL keeps the authoritative
 * copy until the very last step):
 *  1. SQL lists a bounded page of TERMINAL attempts with the exact records text,
 *     its SHA-256 and the SQL-derived org/date-scoped archive key.
 *  2. The exact text is written create-only (IfNoneMatch) as one zstd frame, then
 *     read back and hash-verified. An existing object must decode to the same bytes.
 *  3. Only then SQL re-proves terminality/hash/key under USER/BATCH/ATTEMPT locks
 *     and clears the column, keeping hash, count and pointer.
 * Any refusal stops the run. Dry-run (the default) performs no write at all.
 * No transcript content exists in this column; it is record-identity metadata,
 * but it is still never logged.
 */
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { rawZstdFrame } from "@rhc/lib/pipeline/raw-chunks.ts";
import { objectKey, isObjectStoreError, readAll, type ObjectStore } from "../shared/vendor.ts";
export interface ClaimRecordsSql { query(sql: string, params: unknown[]): Promise<{ rows: Record<string, unknown>[] }> }
export interface ClaimRecordsCandidate { claimToken: string; orgId: string; recordsText: string; recordsSha256: string; recordsCount: number; archiveKey: string }
export interface ClaimRecordsArchiveOptions { sql: ClaimRecordsSql; store: ObjectStore; keyRoot: string; limit: number; maxPages: number; execute: boolean }
export interface ClaimRecordsArchiveReport { examined: number; archived: number; retired: number; already: number; recordsBytes: number; next: string | null }
export class ClaimRecordsArchiveError extends Error { constructor(readonly code: string) { super("claim records archive refused: " + code); } }
const no = (code: string): never => { throw new ClaimRecordsArchiveError(code); };
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const enc = new TextEncoder();
const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
export function assertCandidate(v: unknown, keyRoot: string): asserts v is ClaimRecordsCandidate {
  const c = v as ClaimRecordsCandidate;
  if (!c || typeof c !== "object" || typeof c.claimToken !== "string" || !UUID.test(c.claimToken) || typeof c.orgId !== "string"
    || !/^[A-Za-z0-9._-]{1,128}$/.test(c.orgId) || c.orgId === "." || c.orgId === ".." || typeof c.recordsText !== "string"
    || typeof c.recordsSha256 !== "string" || !/^[0-9a-f]{64}$/.test(c.recordsSha256) || !Number.isSafeInteger(c.recordsCount)
    || c.recordsCount < 0 || c.recordsCount > 500 || typeof c.archiveKey !== "string") no("candidate_shape");
  const prefix = `${keyRoot}/${c.orgId}/claim-attempt-records/v1/ws=`;
  if (!c.archiveKey.startsWith(prefix) || c.archiveKey.split("/").some(s => !s || s === "." || s === "..")
    || !/\/d=\d{4}\/\d{2}\/\d{2}\/c=[0-9a-f]{32}-[0-9a-f]{16}\.json\.zst$/.test(c.archiveKey)
    || !c.archiveKey.endsWith(`/c=${sha(enc.encode(c.claimToken)).slice(0, 32)}-${c.recordsSha256.slice(0, 16)}.json.zst`)) no("candidate_key");
  if (sha(enc.encode(c.recordsText)) !== c.recordsSha256) no("candidate_hash");
  let parsed: unknown; try { parsed = JSON.parse(c.recordsText); } catch { no("candidate_records"); }
  if (!Array.isArray(parsed) || parsed.length !== c.recordsCount) no("candidate_records");
}
const decode = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: 4 * 1024 * 1024 }));
async function readBack(store: ObjectStore, key: string): Promise<string> {
  const o = await store.getObject(objectKey(key));
  return sha(decode(await readAll(o.body)));
}
/** Writes (create-only) and proves the archive object; idempotent across crashes. */
export async function archiveCandidate(store: ObjectStore, c: ClaimRecordsCandidate): Promise<void> {
  const bytes = enc.encode(c.recordsText);
  try { await store.put(objectKey(c.archiveKey), rawZstdFrame(bytes), { ifNoneMatch: true, contentType: "application/zstd" }); }
  catch (e) { if (!isObjectStoreError(e, "already_exists") && !isObjectStoreError(e, "precondition_failed")) throw e; }
  if (await readBack(store, c.archiveKey) !== c.recordsSha256) no("archive_readback");
}
export async function archiveClaimAttemptRecords(o: ClaimRecordsArchiveOptions, after: string | null = null): Promise<ClaimRecordsArchiveReport> {
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(o.keyRoot) || !Number.isInteger(o.limit) || o.limit < 1 || o.limit > 100
    || !Number.isInteger(o.maxPages) || o.maxPages < 1 || o.maxPages > 10000) no("options");
  const report: ClaimRecordsArchiveReport = { examined: 0, archived: 0, retired: 0, already: 0, recordsBytes: 0, next: after };
  for (let page = 0; page < o.maxPages; page++) {
    const r = (await o.sql.query("SELECT sessions.ingest_claim_attempt_records_candidates($1::jsonb) AS r",
      [JSON.stringify({ keyRoot: o.keyRoot, limit: o.limit, afterClaimToken: report.next })])).rows[0]?.r as { candidates?: unknown } | undefined;
    const list: unknown = r?.candidates;
    if (!Array.isArray(list) || list.length > o.limit) return no("candidates_result");
    if (!list.length) { report.next = null; break; }
    for (const c of list as unknown[]) {
      assertCandidate(c, o.keyRoot);
      if (report.next !== null && c.claimToken <= report.next) no("candidates_order");
      report.examined++; report.recordsBytes += enc.encode(c.recordsText).byteLength;
      if (o.execute) {
        await archiveCandidate(o.store, c); report.archived++;
        const done = (await o.sql.query("SELECT sessions.ingest_claim_attempt_records_retire($1::jsonb) AS r",
          [JSON.stringify({ keyRoot: o.keyRoot, claimToken: c.claimToken, recordsSha256: c.recordsSha256, archiveKey: c.archiveKey })])).rows[0]?.r as { retired?: unknown; already?: unknown };
        if (done?.retired !== true || typeof done.already !== "boolean") no("retire_result");
        if (done.already) report.already++; else report.retired++;
      }
      report.next = c.claimToken;
    }
  }
  return report;
}
