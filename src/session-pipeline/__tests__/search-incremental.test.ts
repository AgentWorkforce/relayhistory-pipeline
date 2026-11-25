import { describe, expect, test } from "bun:test";
import {
  commitSearchWatermarkCas,
  planSearchIncrementalApply,
  searchChunkChain,
  type SearchBatchWatermark,
  type SearchChainChunk,
} from "../search-incremental.ts";

const hash = (digit: string) => digit.repeat(64);
const chunks: SearchChainChunk[] = [
  { seq: 0, epoch: 1, start_offset: 0, sha256: hash("1") },
  { seq: 1, epoch: 1, start_offset: 100, sha256: hash("2") },
  { seq: 2, epoch: 2, start_offset: 200, sha256: hash("3") },
];
const watermark = (through: number, over: readonly SearchChainChunk[] = chunks): SearchBatchWatermark => ({
  manifestKey: "96bulls/o/w/h/u/m/s/v1/manifest.json",
  lastSeq: through,
  chainDigest: searchChunkChain(over.slice(0, through + 1))[through]!,
});

describe("M3 search chunk watermark", () => {
  test("an exact prefix permits only the appended chunks", () => {
    expect(planSearchIncrementalApply({ manifestKey: watermark(1).manifestKey, chunks, watermark: watermark(1) }))
      .toEqual({ mode: "incremental", fromSeq: 2, lastSeq: 2, chainDigest: searchChunkChain(chunks)[2] });
  });

  test("an exact complete watermark is a no-op", () => {
    expect(planSearchIncrementalApply({ manifestKey: watermark(2).manifestKey, chunks, watermark: watermark(2) }))
      .toEqual({ mode: "noop", lastSeq: 2, chainDigest: searchChunkChain(chunks)[2] });
  });

  test("R7: the same seq with changed content sha256 forces a full pass", () => {
    const changed = chunks.map((chunk) => ({ ...chunk }));
    changed[1] = { ...changed[1]!, sha256: hash("4") };
    expect(planSearchIncrementalApply({ manifestKey: watermark(1).manifestKey, chunks: changed, watermark: watermark(1) }))
      .toEqual({ mode: "full", reason: "prefix_mismatch" });
  });

  test("re-epoching or moving an existing chunk forces a full pass", () => {
    for (const changed of [
      chunks.map((chunk, i) => i === 1 ? { ...chunk, epoch: 9 } : chunk),
      chunks.map((chunk, i) => i === 1 ? { ...chunk, start_offset: 99 } : chunk),
    ]) expect(planSearchIncrementalApply({ manifestKey: watermark(1).manifestKey, chunks: changed, watermark: watermark(1) }))
      .toEqual({ mode: "full", reason: "prefix_mismatch" });
  });

  test("compaction, truncation and a different manifest fail closed to full", () => {
    const compacted = chunks.slice(1).map((chunk, seq) => ({ ...chunk, seq }));
    expect(planSearchIncrementalApply({ manifestKey: watermark(1).manifestKey, chunks: compacted, watermark: watermark(1) }))
      .toEqual({ mode: "full", reason: "prefix_mismatch" });
    expect(planSearchIncrementalApply({ manifestKey: watermark(2).manifestKey, chunks: chunks.slice(0, 2), watermark: watermark(2) }))
      .toEqual({ mode: "full", reason: "watermark_out_of_range" });
    expect(planSearchIncrementalApply({ manifestKey: "different", chunks, watermark: watermark(1) }))
      .toEqual({ mode: "full", reason: "manifest_changed" });
  });

  test("missing or malformed evidence never produces an incremental plan", () => {
    expect(planSearchIncrementalApply({ manifestKey: "m", chunks, watermark: null }))
      .toEqual({ mode: "full", reason: "missing_watermark" });
    expect(planSearchIncrementalApply({ manifestKey: watermark(0).manifestKey, chunks: [{ ...chunks[0]!, seq: 2 }], watermark: watermark(0) }))
      .toEqual({ mode: "full", reason: "invalid_chunks" });
  });

  test("a lost CAS asks the caller to re-read instead of overwriting the winner", async () => {
    const expected = watermark(1);
    const next = watermark(2);
    expect(await commitSearchWatermarkCas({ expected, next, compareAndSet: async () => false })).toBe("retry");
    expect(await commitSearchWatermarkCas({ expected, next, compareAndSet: async (old, value) => old === expected && value === next })).toBe("committed");
  });
});
