/**
 * Peak RSS of the streaming reader's passes 1-3 (canonical-stream-projection.ts) on a synthetic scope of N
 * records, for the staged reconcile's memory budget (Phase 3 slice 2b). Offline and synthetic only: the scope is
 * written by the replay fixture into a local filesystem object store; nothing is read from or sent to a service.
 *
 *   bun scripts/bench/staged-stream-rss.ts --export --records 1000000 --dir <new-empty-directory>
 *   bun scripts/bench/staged-stream-rss.ts --measure --dir <that-directory>        (a fresh process)
 *
 * --measure prints one JSON line: records, passes, peak RSS (sampled every 50 ms and the process maxRSS), the RSS
 * before the read, and seconds. The keyed-records cap is lifted for the measurement (per-pass budgets as the staged
 * reconcile meters them), so the number is the reader's own footprint at N.
 */
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { objectKey, parseManifest, readAll } from "../../packages/object-store/src/index.ts";
import { streamCanonicalScope } from "../../src/session-read-api/canonical-stream-projection.ts";
import { createFsStore, exportReplayFixture, replayDatabase, REPLAY_FIXTURE_IDENTITY as identity } from "../../src/session-pipeline/__tests__/replay-fixture.ts";
import * as zlib from "node:zlib";

const args = process.argv.slice(2);
const arg = (name: string) => {
  const i = args.indexOf(name);
  return i < 0 ? undefined : args[i + 1];
};
const dir = arg("--dir");
if (!dir) throw Error("requires --dir <directory>");
const root = resolve(dir);
if (args.includes("--export")) {
  await mkdir(root, { recursive: true });
  if ((await readdir(root)).length) throw Error("--dir must be empty for --export");
}
const store = createFsStore({ root: join(root, "objects") });

if (args.includes("--export")) {
  const records = Number(arg("--records"));
  if (!Number.isSafeInteger(records) || records < 1) throw Error("requires --records <positive integer>");
  const x = await replayDatabase();
  try {
    const result = await exportReplayFixture(store, x.db, records, 2);
    await writeFile(join(root, "manifests.json"), JSON.stringify({ records, keys: result.manifests.map((m) => m.manifestKey) }) + "\n");
    console.log(JSON.stringify({ phase: "exported", records, manifests: result.manifests.length }));
  } finally {
    await x.pg.close();
  }
} else if (args.includes("--measure")) {
  const { records, keys } = JSON.parse(await readFile(join(root, "manifests.json"), "utf8")) as { records: number; keys: string[] };
  const admitted = [];
  for (const key of keys)
    admitted.push({ key, manifest: await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(key)))), key) });
  // --timeline: peak RSS per pass, the pass told from the chunk reads (every pass reads every chunk once).
  const chunksPerPass = admitted.reduce((n, a) => n + a.manifest.raw.chunks.length, 0);
  let gets = 0;
  const counted = args.includes("--timeline")
    ? ({ ...store, getObject: (...a: Parameters<typeof store.getObject>) => { gets += 1; return store.getObject(...a); } } as typeof store)
    : store;
  const perPass: number[] = [];
  Bun.gc(true);
  const baseline = process.memoryUsage().rss;
  let peak = baseline;
  const sampler = setInterval(() => {
    const rss = process.memoryUsage().rss;
    peak = Math.max(peak, rss);
    const pass = Math.min(Math.floor(gets / Math.max(1, chunksPerPass)), 3);
    perPass[pass] = Math.max(perPass[pass] ?? 0, rss);
  }, 50);
  const started = performance.now();
  try {
    const first = admitted[0]!.manifest;
    const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, harness: identity.source, sessionId: first.session.session_id };
    const read = await streamCanonicalScope(
      scope,
      admitted,
      {
        store: counted,
        prefixFor: (org) => `96bulls/${org}`,
        decompress: (stored, max) => new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength: max } as never)),
        perPass: { keyedRecordsCap: Number.MAX_SAFE_INTEGER },
      },
      { maxReadBytes: Number.MAX_SAFE_INTEGER, maxReadEvents: Number.MAX_SAFE_INTEGER },
      Date.now(),
      0,
    );
    const seconds = (performance.now() - started) / 1000;
    peak = Math.max(peak, process.memoryUsage().rss, process.resourceUsage().maxRSS * 1024);
    console.log(JSON.stringify({
      phase: "measured", records, complete: read.complete, reasons: read.reasons, wanted: read.wanted.size, passes: read.meter.passes,
      baselineRssBytes: baseline, peakRssBytes: peak, growthBytes: peak - baseline, seconds,
      ...(args.includes("--timeline") ? { chunksPerPass, peakGrowthByPass: perPass.map((r) => (r ?? baseline) - baseline) } : {}),
    }));
  } finally {
    clearInterval(sampler);
  }
} else throw Error("requires --export or --measure");
