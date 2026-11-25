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

export type SearchIncrementalPlan =
  | { mode: "full"; reason: "missing_watermark" | "manifest_changed" | "invalid_chunks" | "watermark_out_of_range" | "prefix_mismatch" }
  | { mode: "noop"; lastSeq: number; chainDigest: string }
  | { mode: "incremental"; fromSeq: number; lastSeq: number; chainDigest: string };

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

/** The database adapter maps `retry` to a fresh read and plan; it never overwrites a winner. */
export async function commitSearchWatermarkCas(input: {
  expected: SearchBatchWatermark;
  next: SearchBatchWatermark;
  compareAndSet: (expected: SearchBatchWatermark, next: SearchBatchWatermark) => Promise<boolean>;
}): Promise<"committed" | "retry"> {
  return (await input.compareAndSet(input.expected, input.next)) ? "committed" : "retry";
}
