/**
 * M3's content-bound watermark planner for incremental search indexing.
 *
 * This module is deliberately storage-agnostic. It proves whether a manifest
 * extends the exact chunk prefix recorded by a watermark. The caller still
 * owns the database row, the CAS and the actual full/incremental reconcile.
 * Nothing in this file enables incremental indexing by itself.
 */
import { createHash, timingSafeEqual } from "node:crypto";
import type { ChunkEntry } from "@rhc-object-store";

export type SearchChainChunk = Pick<ChunkEntry, "seq" | "epoch" | "start_offset" | "sha256">;

export interface SearchBatchWatermark {
  manifestKey: string;
  lastSeq: number;
  chainDigest: string;
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
    if (!row.manifestKey || out.has(row.manifestKey)) return null;
    out.set(row.manifestKey, row);
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

/** The database adapter maps `retry` to a fresh read and plan; it never overwrites a winner. */
export async function commitSearchWatermarkCas(input: {
  expected: SearchBatchWatermark;
  next: SearchBatchWatermark;
  compareAndSet: (expected: SearchBatchWatermark, next: SearchBatchWatermark) => Promise<boolean>;
}): Promise<"committed" | "retry"> {
  return (await input.compareAndSet(input.expected, input.next)) ? "committed" : "retry";
}

/** Scope-level CAS: the database adapter updates every manifest watermark atomically. */
export async function commitSearchScopeWatermarksCas(input: {
  expected: readonly SearchBatchWatermark[];
  next: readonly SearchBatchWatermark[];
  compareAndSet: (
    expected: readonly SearchBatchWatermark[],
    next: readonly SearchBatchWatermark[],
  ) => Promise<boolean>;
}): Promise<"committed" | "retry"> {
  return (await input.compareAndSet(input.expected, input.next)) ? "committed" : "retry";
}
