/**
 * Offline synthetic benchmark for M3's content-bound scope planner.
 *
 * It measures manifest-metadata verification and reports the chunk-object reads
 * selected by a full pass versus an incremental pass. It performs no network,
 * database or object-store IO.
 *
 *   bun scripts/bench/search-incremental-plan.ts [--manifests 4] [--chunks 4096] [--append 4] [--runs 25]
 */
import {
  planSearchScopeIncrementalApply,
  searchChunkChain,
  type SearchBatchWatermark,
  type SearchManifestChain,
} from "../../src/session-pipeline/search-incremental.ts";

const args = process.argv.slice(2);
const integer = (name: string, fallback: number, minimum: number): number => {
  const at = args.indexOf(name);
  const value = at < 0 ? fallback : Number(args[at + 1]);
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return value;
};

const manifestCount = integer("--manifests", 4, 1);
const chunkCount = integer("--chunks", 4096, 2);
const appendCount = integer("--append", 4, 1);
const runs = integer("--runs", 25, 1);
if (appendCount >= chunkCount) throw new Error("--append must be smaller than --chunks");

const manifests: SearchManifestChain[] = Array.from({ length: manifestCount }, (_, manifest) => ({
  manifestKey: `synthetic/${manifest}/manifest.json`,
  chunks: Array.from({ length: chunkCount }, (_, seq) => ({
    seq,
    epoch: 1 + Math.floor(seq / 1024),
    start_offset: seq * 1024,
    sha256: (manifest * chunkCount + seq).toString(16).padStart(64, "0"),
  })),
}));
const watermarks: SearchBatchWatermark[] = manifests.map(({ manifestKey, chunks }) => {
  const lastSeq = chunks.length - appendCount - 1;
  return { manifestKey, lastSeq, chainDigest: searchChunkChain(chunks.slice(0, lastSeq + 1))[lastSeq]! };
});

const samples: number[] = [];
let selectedReads = 0;
for (let run = 0; run < runs; run++) {
  const started = performance.now();
  const plan = planSearchScopeIncrementalApply({ manifests, watermarks });
  samples.push(performance.now() - started);
  if (plan.mode !== "incremental") throw new Error(`synthetic append planned ${plan.mode}`);
  selectedReads = plan.reads.reduce((sum, read) => {
    const manifest = manifests.find((candidate) => candidate.manifestKey === read.manifestKey)!;
    return sum + manifest.chunks.length - read.fromSeq;
  }, 0);
}
samples.sort((a, b) => a - b);
const percentile = (p: number) => samples[Math.min(samples.length - 1, Math.floor(samples.length * p))]!;
const fullReads = manifestCount * chunkCount;
console.log(JSON.stringify({
  manifests: manifestCount,
  chunksPerManifest: chunkCount,
  appendedPerManifest: appendCount,
  runs,
  planMsP50: Number(percentile(0.5).toFixed(3)),
  planMsP90: Number(percentile(0.9).toFixed(3)),
  fullChunkObjectReads: fullReads,
  incrementalChunkObjectReads: selectedReads,
  objectReadReduction: Number((1 - selectedReads / fullReads).toFixed(6)),
}));
