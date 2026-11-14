/**
 * Child process for scrub-password-family.test.ts (docs/scrub-engine.md, "Default flip" and
 * "Cutover: pinned relayscrubber 0.5.0-alpha.9"). Same shape as scrub-default-child.ts: spawned
 * from a directory without bunfig.toml (no test preload, so no TS grant) with an env -i style
 * environment, so SCRUB_ENGINE, SCRUB_ENGINE_REQUIRED and READER_MAX_RULESET are unset and
 * initScrubEngine() takes the default selection with the real vendored artifact.
 *
 * It initialises the engine ONCE, then drives every input it is given through each seam
 * function (scrubText, scrubLong, scrubJson nested + top level, scrubRecords, and the routed
 * raw-chunk sealer against a temporary fs store) and prints one JSON line. It asserts nothing;
 * the test owns the inputs and the expectations. It never imports a preload or bun:test.
 *
 *   bun scrub-password-family-child.ts '<json: { inputs: string[], benignLines: string[] }>'
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createFsStore } from "../../../../packages/object-store/src/fs.ts";
import { objectKey, readAll, type ObjectStore } from "../../../../packages/pipeline/src/lib/pipeline/object-layout.ts";
import type { SealRawChunk } from "../../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";
import {
  activeScrubEngine,
  initScrubEngine,
  routedRawChunkSealer,
  scrubJson,
  scrubLong,
  scrubRecords,
  scrubText,
} from "../../../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { scrubTsCapabilities } from "../../../../packages/pipeline/src/lib/scrub-engine/ts-capability.ts";

const request = JSON.parse(process.argv[2] ?? "{}") as { inputs: string[]; benignLines: string[] };
const enc = new TextEncoder();
const dec = new TextDecoder();

const out: Record<string, unknown> = {
  grantsAtStart: scrubTsCapabilities(),
  // Evidence that the selection really is the default one: none of these may be set.
  selectionEnv: Object.fromEntries(
    ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"].map((k) => [k, process.env[k] ?? null]),
  ),
};

const engine = await initScrubEngine();
out.kind = engine.kind;
out.sameInstance = engine === activeScrubEngine();
out.marker = engine.marker;
out.crateVersion = (engine as unknown as { manifest?: { crateVersion?: string } }).manifest?.crateVersion ?? null;

/** NDJSON body: the benign records first, then one user record whose message content is `input`. */
const userRecord = (input: string) => JSON.stringify({ type: "user", message: { role: "user", content: input } });
const benignRecords = request.benignLines.map((b) => JSON.stringify({ type: "assistant", message: { role: "assistant", content: b } }));
const body = (input: string) => `${[...benignRecords, userRecord(input)].join("\n")}\n`;
const nested = (input: string) => ({ type: "user", message: { role: "user", content: [{ type: "text", text: input }] }, notes: [input] });

function sealInput(source: Uint8Array, seq: number): SealRawChunk {
  return { location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "claude", sessionId: `pw-${seq}`, startDate: "2026-11-14" }, format: "ndjson", seq, epoch: 0, startOffset: 0, source } as SealRawChunk;
}

const root = await mkdtemp(join(tmpdir(), "scrub-password-family-seal-"));
try {
  const store = createFsStore({ root }) as unknown as ObjectStore;
  const sealer = routedRawChunkSealer();
  out.routed = sealer !== undefined;
  const results: Record<string, unknown>[] = [];
  let seq = 0;
  for (const input of request.inputs) {
    const text = scrubText(input);
    const long = scrubLong(input);
    const jsonNested = scrubJson(nested(input));
    const jsonTop = scrubJson(input);
    const source = body(input);
    const rec = scrubRecords(enc.encode(source));
    const recOut = dec.decode(rec.scrubbed);
    const recTwice = dec.decode(scrubRecords(rec.scrubbed).scrubbed);
    let seal: Record<string, unknown> = { routed: false };
    if (sealer) {
      const sealed = await sealer(store, sealInput(enc.encode(source), seq++));
      const object = await store.getObject(objectKey(sealed.key));
      seal = {
        routed: true,
        records: sealed.entry.records,
        scrub: sealed.scrub,
        metadata: object.meta.metadata,
        stored: dec.decode(zstdDecompressSync(await readAll(object.body))),
      };
    }
    results.push({
      input,
      source,
      text, textTwice: scrubText(text),
      long, longTwice: scrubLong(long),
      jsonNested, jsonNestedTwice: scrubJson(jsonNested),
      jsonTop, jsonTopTwice: scrubJson(jsonTop),
      records: { out: recOut, twice: recTwice, records: rec.records, scrub: rec.scrub },
      seal,
    });
  }
  out.results = results;
} finally {
  await rm(root, { recursive: true, force: true });
}
out.grantsAtEnd = scrubTsCapabilities();
console.log(JSON.stringify(out));
