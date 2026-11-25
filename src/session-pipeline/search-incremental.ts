/**
 * M3's content-bound watermark planner for incremental search indexing.
 *
 * This module is deliberately storage-agnostic. It proves whether a manifest
 * extends the exact chunk prefix recorded by a watermark. The proposed RPC
 * adapter owns the versioned CAS; the caller still owns the actual
 * full/incremental reconcile. Nothing in this file enables incremental
 * indexing by itself.
 */
import { createHash, timingSafeEqual } from "node:crypto";
import type { ChunkEntry } from "@rhc-object-store";

export type SearchChainChunk = Pick<ChunkEntry, "seq" | "epoch" | "start_offset" | "sha256">;

export interface SearchBatchWatermark {
  manifestKey: string;
  lastSeq: number;
  chainDigest: string;
}

/**
 * The version closes the ABA race across a complete canonical scope. The
 * manifest rows alone are not a sufficient CAS token: A -> B -> A would make
 * a stale reader of the first A appear current again.
 */
export interface SearchScopeWatermarkSnapshot {
  version: number;
  watermarks: SearchBatchWatermark[];
}

export interface SearchIncrementalScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}

export interface SearchWatermarkSql {
  query<T>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export interface SearchManifestChain {
  manifestKey: string;
  chunks: readonly SearchChainChunk[];
}

export type SearchIncrementalPlan =
  | { mode: "full"; reason: "missing_watermark" | "manifest_changed" | "invalid_chunks" | "watermark_out_of_range" | "prefix_mismatch" }
  | { mode: "noop"; lastSeq: number; chainDigest: string }
  | { mode: "incremental"; fromSeq: number; lastSeq: number; chainDigest: string };
type SearchIncrementalFullReason = Extract<SearchIncrementalPlan, { mode: "full" }>["reason"];

export type SearchScopeIncrementalPlan =
  | {
      mode: "full";
      reason: "invalid_manifest_set" | "watermark_set_changed" | SearchIncrementalFullReason;
      manifestKey?: string;
    }
  | { mode: "noop"; watermarks: SearchBatchWatermark[] }
  | {
      mode: "incremental";
      reads: Array<{ manifestKey: string; fromSeq: number }>;
      watermarks: SearchBatchWatermark[];
    };

const HEX_64 = /^[0-9a-f]{64}$/u;
const ZERO_DIGEST = Buffer.alloc(32);

function safeUint(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function chunkShapeIsValid(chunks: readonly SearchChainChunk[]): boolean {
  return chunks.every((chunk, index) =>
    chunk.seq === index &&
    safeUint(chunk.seq) &&
    safeUint(chunk.epoch) &&
    safeUint(chunk.start_offset) &&
    HEX_64.test(chunk.sha256));
}

/**
 * M3 chain link:
 *   sha256(previous[32] || seq:u64be || epoch:u64be || start_offset:u64be || content_sha256[32])
 *
 * Fixed-width fields make the encoding injective. `seq` is included even
 * though v1 manifests are dense, so compaction/re-numbering cannot alias an
 * earlier prefix.
 */
export function searchChunkChain(chunks: readonly SearchChainChunk[]): string[] {
  if (!chunkShapeIsValid(chunks)) throw new TypeError("search incremental chunks must be dense, safe unsigned integers with lowercase sha256");
  const out: string[] = [];
  let previous = ZERO_DIGEST;
  for (const chunk of chunks) {
    const link = Buffer.alloc(32 + 8 + 8 + 8 + 32);
    previous.copy(link, 0);
    link.writeBigUInt64BE(BigInt(chunk.seq), 32);
    link.writeBigUInt64BE(BigInt(chunk.epoch), 40);
    link.writeBigUInt64BE(BigInt(chunk.start_offset), 48);
    Buffer.from(chunk.sha256, "hex").copy(link, 56);
    previous = createHash("sha256").update(link).digest();
    out.push(previous.toString("hex"));
  }
  return out;
}

function sameDigest(left: string, right: string): boolean {
  if (!HEX_64.test(left) || !HEX_64.test(right)) return false;
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}

/**
 * Decide whether a pass may read only appended chunks. Any uncertainty fails
 * closed to a full pass; malformed input never produces an incremental plan.
 */
export function planSearchIncrementalApply(input: {
  manifestKey: string;
  chunks: readonly SearchChainChunk[];
  watermark: SearchBatchWatermark | null;
}): SearchIncrementalPlan {
  if (!chunkShapeIsValid(input.chunks)) return { mode: "full", reason: "invalid_chunks" };
  const watermark = input.watermark;
  if (!watermark) return { mode: "full", reason: "missing_watermark" };
  if (watermark.manifestKey !== input.manifestKey) return { mode: "full", reason: "manifest_changed" };
  if (!safeUint(watermark.lastSeq) || watermark.lastSeq >= input.chunks.length)
    return { mode: "full", reason: "watermark_out_of_range" };

  const chain = searchChunkChain(input.chunks);
  if (!sameDigest(chain[watermark.lastSeq]!, watermark.chainDigest))
    return { mode: "full", reason: "prefix_mismatch" };

  const lastSeq = input.chunks.length - 1;
  const chainDigest = chain[lastSeq]!;
  if (watermark.lastSeq === lastSeq) return { mode: "noop", lastSeq, chainDigest };
  return { mode: "incremental", fromSeq: watermark.lastSeq + 1, lastSeq, chainDigest };
}

function uniqueManifestMap<T extends { manifestKey: string }>(rows: readonly T[]): Map<string, T> | null {
  const out = new Map<string, T>();
  for (const row of rows) {
    if (typeof row.manifestKey !== "string" || !row.manifestKey || out.has(row.manifestKey)) return null;
    out.set(row.manifestKey, row);
  }
  return out;
}

/** Canonical database/wire form: validated, copied and sorted by manifest key. */
export function canonicalSearchWatermarks(rows: readonly SearchBatchWatermark[]): SearchBatchWatermark[] | null {
  const unique = uniqueManifestMap(rows);
  if (!unique) return null;
  const out: SearchBatchWatermark[] = [];
  for (const manifestKey of [...unique.keys()].sort()) {
    const row = unique.get(manifestKey)!;
    if (!safeUint(row.lastSeq) || typeof row.chainDigest !== "string" || !HEX_64.test(row.chainDigest)) return null;
    out.push({ manifestKey, lastSeq: row.lastSeq, chainDigest: row.chainDigest });
  }
  return out;
}

/**
 * Plan one canonical scope, which can contain several live manifests. A scope
 * is incremental only when the watermark set is exactly the live manifest set
 * and every manifest proves the prefix it previously recorded. A new, removed
 * or duplicate manifest therefore selects the full path.
 *
 * `reads` contains only manifests with appended chunks. The caller may skip
 * unchanged siblings, but must commit the complete returned watermark set in
 * one CAS with the scope's search update.
 */
export function planSearchScopeIncrementalApply(input: {
  manifests: readonly SearchManifestChain[];
  watermarks: readonly SearchBatchWatermark[] | null;
}): SearchScopeIncrementalPlan {
  const manifests = uniqueManifestMap(input.manifests);
  if (!manifests || manifests.size === 0) return { mode: "full", reason: "invalid_manifest_set" };
  const watermarks = input.watermarks && uniqueManifestMap(input.watermarks);
  if (!watermarks || watermarks.size !== manifests.size || [...manifests.keys()].some((key) => !watermarks.has(key)))
    return { mode: "full", reason: "watermark_set_changed" };

  const reads: Array<{ manifestKey: string; fromSeq: number }> = [];
  const next: SearchBatchWatermark[] = [];
  for (const manifestKey of [...manifests.keys()].sort()) {
    const manifest = manifests.get(manifestKey)!;
    const plan = planSearchIncrementalApply({ manifestKey, chunks: manifest.chunks, watermark: watermarks.get(manifestKey)! });
    if (plan.mode === "full") return { mode: "full", reason: plan.reason, manifestKey };
    next.push({ manifestKey, lastSeq: plan.lastSeq, chainDigest: plan.chainDigest });
    if (plan.mode === "incremental") reads.push({ manifestKey, fromSeq: plan.fromSeq });
  }
  return reads.length === 0 ? { mode: "noop", watermarks: next } : { mode: "incremental", reads, watermarks: next };
}

/** Scope-level CAS: the database adapter updates every manifest watermark atomically. */
export async function commitSearchScopeWatermarksCas(input: {
  expected: SearchScopeWatermarkSnapshot;
  next: readonly SearchBatchWatermark[];
  compareAndSet: (
    expected: SearchScopeWatermarkSnapshot,
    next: readonly SearchBatchWatermark[],
  ) => Promise<SearchScopeWatermarkSnapshot | null>;
}): Promise<{ status: "committed"; snapshot: SearchScopeWatermarkSnapshot } | { status: "retry" }> {
  if (!safeUint(input.expected.version)) throw new TypeError("search incremental watermark version must be a safe unsigned integer");
  const expected = canonicalSearchWatermarks(input.expected.watermarks);
  const next = canonicalSearchWatermarks(input.next);
  if (!expected || !next) throw new TypeError("search incremental watermark sets must be unique and valid");
  const snapshot = await input.compareAndSet({ version: input.expected.version, watermarks: expected }, next);
  if (!snapshot) return { status: "retry" };
  const committed = canonicalSearchWatermarks(snapshot.watermarks);
  if (!safeUint(snapshot.version) || !committed)
    throw new TypeError("search incremental watermark store returned an invalid snapshot");
  return { status: "committed", snapshot: { version: snapshot.version, watermarks: committed } };
}

function parseSnapshot(value: unknown): SearchScopeWatermarkSnapshot {
  const decoded = typeof value === "string" ? JSON.parse(value) : value;
  if (!decoded || typeof decoded !== "object") throw new TypeError("search incremental watermark RPC returned no snapshot");
  const record = decoded as { version?: unknown; watermarks?: unknown };
  const watermarks = Array.isArray(record.watermarks)
    ? canonicalSearchWatermarks(record.watermarks as SearchBatchWatermark[])
    : null;
  if (typeof record.version !== "number" || !safeUint(record.version) || !watermarks)
    throw new TypeError("search incremental watermark RPC returned an invalid snapshot");
  return { version: record.version, watermarks };
}

/**
 * Proposed M3 RPC adapter. Its two function names exist only in
 * `supabase/proposals/incremental-search` until the storage boundary receives
 * a migration slot and a separate production go.
 */
export function createSearchWatermarkStore(sql: SearchWatermarkSql): {
  read(scope: SearchIncrementalScope): Promise<SearchScopeWatermarkSnapshot>;
  compareAndSet(
    scope: SearchIncrementalScope,
    expected: SearchScopeWatermarkSnapshot,
    next: readonly SearchBatchWatermark[],
  ): Promise<SearchScopeWatermarkSnapshot | null>;
} {
  const params = (scope: SearchIncrementalScope): unknown[] =>
    [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId];
  return {
    async read(scope) {
      const { rows } = await sql.query<{ snapshot: unknown }>(
        "SELECT sessions.read_search_batch_watermarks_v1($1,$2,$3,$4) AS snapshot",
        params(scope),
      );
      return parseSnapshot(rows[0]?.snapshot);
    },
    async compareAndSet(scope, expected, next) {
      const expectedRows = canonicalSearchWatermarks(expected.watermarks);
      const nextRows = canonicalSearchWatermarks(next);
      if (!safeUint(expected.version) || !expectedRows || !nextRows)
        throw new TypeError("search incremental watermark CAS received invalid evidence");
      const { rows } = await sql.query<{ result: unknown }>(
        "SELECT sessions.cas_search_batch_watermarks_v1($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb) AS result",
        [...params(scope), expected.version, JSON.stringify(expectedRows), JSON.stringify(nextRows)],
      );
      const raw = typeof rows[0]?.result === "string" ? JSON.parse(rows[0]!.result as string) : rows[0]?.result;
      if (!raw || typeof raw !== "object" || typeof (raw as { committed?: unknown }).committed !== "boolean")
        throw new TypeError("search incremental watermark CAS returned an invalid result");
      return (raw as { committed: boolean }).committed ? parseSnapshot(raw) : null;
    },
  };
}
