import { describe, expect, test } from "bun:test";
import {
  commitSearchScopeWatermarksCas,
  canonicalSearchWatermarks,
  planSearchIncrementalApply,
  planSearchScopeIncrementalApply,
  searchChunkChain,
  type SearchBatchWatermark,
  type SearchChainChunk,
  type SearchScopeWatermarkSnapshot,
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

  test("a multi-manifest scope reads only siblings with appended chunks", () => {
    const sibling = chunks.map((chunk) => ({ ...chunk, sha256: hash(String(chunk.seq + 4)) }));
    const manifests = [
      { manifestKey: "a/manifest.json", chunks },
      { manifestKey: "b/manifest.json", chunks: sibling },
    ];
    const watermarks = [
      { ...watermark(1), manifestKey: manifests[0]!.manifestKey },
      { ...watermark(2, sibling), manifestKey: manifests[1]!.manifestKey },
    ];
    const plan = planSearchScopeIncrementalApply({ manifests, watermarks });
    expect(plan).toEqual({
      mode: "incremental",
      reads: [{ manifestKey: "a/manifest.json", fromSeq: 2 }],
      watermarks: [
        { manifestKey: "a/manifest.json", lastSeq: 2, chainDigest: searchChunkChain(chunks)[2] },
        { manifestKey: "b/manifest.json", lastSeq: 2, chainDigest: searchChunkChain(sibling)[2] },
      ],
    });
  });

  test("a changed manifest set or one changed sibling forces the whole scope full", () => {
    const key = "a/manifest.json";
    expect(planSearchScopeIncrementalApply({ manifests: [{ manifestKey: key, chunks }], watermarks: [] }))
      .toEqual({ mode: "full", reason: "watermark_set_changed" });
    expect(planSearchScopeIncrementalApply({ manifests: [{ manifestKey: key, chunks }, { manifestKey: key, chunks }], watermarks: [] }))
      .toEqual({ mode: "full", reason: "invalid_manifest_set" });
    const changed = chunks.map((chunk, i) => i === 0 ? { ...chunk, sha256: hash("9") } : chunk);
    expect(planSearchScopeIncrementalApply({
      manifests: [{ manifestKey: key, chunks: changed }],
      watermarks: [{ ...watermark(1), manifestKey: key }],
    })).toEqual({ mode: "full", reason: "prefix_mismatch", manifestKey: key });
  });

  test("incremental chunk selection equals the full ordered fixture", () => {
    let state = 0x96b115;
    const random = () => (state = (state * 1664525 + 1013904223) >>> 0);
    for (let fixture = 0; fixture < 100; fixture++) {
      const count = 2 + (random() % 30);
      const split = random() % (count - 1);
      const generated = Array.from({ length: count }, (_, seq): SearchChainChunk => ({
        seq,
        epoch: 1 + (random() % 4),
        start_offset: seq * 100,
        sha256: random().toString(16).padStart(8, "0").repeat(8),
      }));
      const key = `fixture-${fixture}/manifest.json`;
      const prior = generated.slice(0, split + 1).map((chunk) => `${chunk.seq}:${chunk.sha256}`);
      const expected = generated.map((chunk) => `${chunk.seq}:${chunk.sha256}`);
      const wm: SearchBatchWatermark = {
        manifestKey: key,
        lastSeq: split,
        chainDigest: searchChunkChain(generated.slice(0, split + 1))[split]!,
      };
      const plan = planSearchScopeIncrementalApply({ manifests: [{ manifestKey: key, chunks: generated }], watermarks: [wm] });
      expect(plan.mode).toBe("incremental");
      if (plan.mode !== "incremental") throw new Error("fixture did not plan incrementally");
      const appended = generated.slice(plan.reads[0]!.fromSeq).map((chunk) => `${chunk.seq}:${chunk.sha256}`);
      expect([...prior, ...appended]).toEqual(expected);
    }
  });

  test("scope CAS canonicalizes the complete set and never overwrites a winner", async () => {
    const expected: SearchScopeWatermarkSnapshot = { version: 7, watermarks: [{ ...watermark(1), manifestKey: "b" }, { ...watermark(1), manifestKey: "a" }] };
    const next = [{ ...watermark(2), manifestKey: "a" }];
    expect(await commitSearchScopeWatermarksCas({ expected, next, compareAndSet: async () => null })).toEqual({ status: "retry" });
    expect(await commitSearchScopeWatermarksCas({
      expected,
      next,
      compareAndSet: async (old, value) => {
        expect(old).toEqual({ version: 7, watermarks: [
          { ...watermark(1), manifestKey: "a" },
          { ...watermark(1), manifestKey: "b" },
        ] });
        expect(value).toEqual(next);
        return { version: 8, watermarks: value.slice().reverse() };
      },
    })).toEqual({ status: "committed", snapshot: { version: 8, watermarks: next } });
  });

  test("scope CAS rejects malformed evidence before reaching storage", async () => {
    let called = false;
    const compareAndSet = async () => { called = true; return null; };
    await expect(commitSearchScopeWatermarksCas({
      expected: { version: 0, watermarks: [] },
      next: [{ ...watermark(1), manifestKey: "same" }, { ...watermark(2), manifestKey: "same" }],
      compareAndSet,
    })).rejects.toThrow("unique and valid");
    expect(called).toBe(false);
    expect(canonicalSearchWatermarks([{ ...watermark(1), chainDigest: "BAD" }])).toBeNull();
  });
});
