/**
 * Child process for scrub-default-flip.test.ts (docs/scrub-engine.md, "Default flip").
 * Runs ONE scenario in a fresh process with no test preload (spawned from a directory
 * without bunfig.toml), then prints one JSON line. It never imports a preload or bun:test.
 *
 *   bun scrub-default-child.ts <scenario>
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
  scrubEngineReady,
  scrubText,
  scrubTextForLog,
  sealRawChunk,
} from "../../../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { declareScrubTsCapability, scrubTsCapabilities } from "../../../../packages/pipeline/src/lib/scrub-engine/ts-capability.ts";
import { courierTranscript, COURIER_SESSION } from "../../../../e2e/pipeline/fixtures.ts";

const scenario = process.argv[2] ?? "";
const out: Record<string, unknown> = { scenario, grantsAtStart: scrubTsCapabilities() };
// A secret-shaped token built at run time (nothing secret-shaped is stored in the repo).
const SECRET = `ghp_${"A1b2C3d4E5".repeat(4).slice(0, 36)}`;

async function settle<T>(p: Promise<T> | (() => T)): Promise<{ ok: true; value: T } | { ok: false; name: string; code?: string; reason?: string }> {
  try { return { ok: true, value: await (typeof p === "function" ? p() : p) }; }
  catch (e) { const x = e as { name?: string; code?: string; reason?: string }; return { ok: false, name: String(x?.name), code: x?.code, reason: x?.reason }; }
}
const kindOf = async (p: Promise<{ kind: string }>) => { const r = await settle(p); return r.ok ? { ok: true, kind: r.value.kind } : r; };

function sealInput(source: Uint8Array, seq: number): SealRawChunk {
  return { location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "claude", sessionId: COURIER_SESSION.sessionId, startDate: "2026-11-01" }, format: "ndjson", seq, epoch: 0, startOffset: 0, source } as SealRawChunk;
}

/** Seal the fixture session body through the routed sealer and read it back from the store. */
async function sealAndReadBack() {
  const root = await mkdtemp(join(tmpdir(), "scrub-default-seal-"));
  try {
    const store = createFsStore({ root }) as unknown as ObjectStore;
    const body = courierTranscript() + JSON.stringify({ type: "user", sessionId: COURIER_SESSION.sessionId, message: { role: "user", content: `deploy with token=${SECRET} please` } }) + "\n";
    const sealer = routedRawChunkSealer();
    if (!sealer) return { routed: false };
    const sealed = await sealer(store, sealInput(new TextEncoder().encode(body), 0));
    const object = await store.getObject(objectKey(sealed.key));
    const text = new TextDecoder().decode(zstdDecompressSync(await readAll(object.body)));
    return {
      routed: true, records: sealed.entry.records, scrub: sealed.scrub, metadata: object.meta.metadata,
      secretGone: !text.includes(SECRET) && !text.includes(SECRET.slice(4)),
      markerKept: text.includes(COURIER_SESSION.marker), lines: text.trimEnd().split("\n").length,
      unwired: await settle(sealRawChunk(store, sealInput(new TextEncoder().encode(body), 1))),
    };
  } finally { await rm(root, { recursive: true, force: true }); }
}

switch (scenario) {
  case "default": {
    // Unset env, no grant: a log line before init refuses whole, without latching a failure.
    out.preInitLog = scrubTextForLog(`token=${SECRET}`);
    out.preInitReady = scrubEngineReady();
    out.init = await kindOf(initScrubEngine());
    out.second = await kindOf(initScrubEngine()); // a second initialiser shares the instance
    out.sameInstance = (await initScrubEngine()) === activeScrubEngine();
    out.ready = scrubEngineReady();
    out.marker = activeScrubEngine().marker;
    out.scrubbed = !scrubText(`token=${SECRET}`).includes(SECRET);
    out.postInitLogScrubbed = !scrubTextForLog(`token=${SECRET}`).includes(SECRET);
    out.seal = await sealAndReadBack();
    break;
  }
  case "explicit-bindgen":
  case "explicit-wasm": {
    out.init = await kindOf(initScrubEngine());
    out.ready = scrubEngineReady();
    if (out.ready) out.marker = activeScrubEngine().marker;
    out.routed = routedRawChunkSealer() !== undefined;
    break;
  }
  case "ts-refused": {
    out.init = await settle(initScrubEngine());
    out.second = await settle(initScrubEngine());
    out.ready = scrubEngineReady();
    out.scrub = await settle(() => scrubText("x"));
    out.log = scrubTextForLog(`token=${SECRET}`);
    out.routed = await settle(() => routedRawChunkSealer());
    // A later grant does not lift the latch (only hosted handlers grant, at entry, before any init).
    declareScrubTsCapability("hosted-lambda");
    out.afterGrant = await settle(initScrubEngine());
    break;
  }
  case "required-ok":
  case "required-refused": {
    out.init = await kindOf(initScrubEngine());
    out.second = await settle(initScrubEngine());
    out.ready = scrubEngineReady();
    out.scrub = await settle(() => scrubText("x"));
    out.routed = await settle(() => { routedRawChunkSealer(); return "no-throw"; });
    break;
  }
  case "init-failure": {
    out.init = await settle(initScrubEngine());
    out.second = await settle(initScrubEngine());
    out.ready = scrubEngineReady();
    out.scrub = await settle(() => scrubText("x"));
    out.log = scrubTextForLog(`token=${SECRET}`);
    out.routed = await settle(() => routedRawChunkSealer());
    break;
  }
  case "lazy-then-other-kind": {
    out.preInitLog = scrubTextForLog("hello");
    process.env.SCRUB_ENGINE = "wasm";
    out.init = await settle(initScrubEngine());
    delete process.env.SCRUB_ENGINE;
    out.sameKind = await kindOf(initScrubEngine());
    break;
  }
  case "node-entry": {
    // A Node self-host service entry: initialise before work, as large-read-main does.
    const { largeReadMain } = await import("../../../session-pipeline/large-read-main.ts");
    let factoryCalls = 0;
    out.run = await settle(largeReadMain({ ...process.env, SESSION_LARGE_READ_WORKER_ENABLED: "1" }, () => { factoryCalls++; throw new Error("factory reached"); }));
    out.factoryCalls = factoryCalls;
    out.grantsAfter = scrubTsCapabilities();
    break;
  }
  case "hosted": {
    // The two handler modules that self-host services import for helpers grant nothing at import;
    // their deployed entries are the lambda-entry.ts wrappers (scrub-entry-grant-child covers those).
    await import("../../../session-pipeline/lambda.ts");
    await import("../../../session-read-api/lambda.ts");
    out.grantsAfterHelperImport = scrubTsCapabilities();
    // Hosted-only Lambda modules grant at module load through their first import.
    const gc = await import("../../buffered-gc-lambda.ts");
    const rearm = await import("../../rearm-sweeper-lambda.ts");
    await import("../../upload-admission-lambda.ts");
    out.grantsAfterImport = scrubTsCapabilities();
    // The handler-entry grant repeats it idempotently (both flags off: the handlers return at once).
    out.gc = await gc.handler({});
    out.rearm = await rearm.handler({});
    out.grantsAfterHandler = scrubTsCapabilities();
    out.init = await kindOf(initScrubEngine());
    out.ready = scrubEngineReady();
    out.marker = activeScrubEngine().marker;
    out.routed = routedRawChunkSealer() !== undefined;
    break;
  }
  default:
    throw new Error(`unknown scenario ${scenario}`);
}
out.grantsAtEnd = scrubTsCapabilities();
console.log(JSON.stringify(out));
