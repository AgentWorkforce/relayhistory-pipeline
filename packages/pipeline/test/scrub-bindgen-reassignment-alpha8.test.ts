/**
 * Pipeline-side regression for the relayscrubber alpha.8 bare-reassignment repair
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.8").
 *
 * The relayscrubber receipt (docs/results/alpha8-reassignment-repair.json there) proves the
 * crate. This proves OUR binding uses it: the real vendored wasm, loaded and verified by
 * initScrubEngine under the AA self-host env triple, through the seam functions production
 * imports (scrubText / scrubLong / scrubJson) and the routed raw-chunk sealer
 * (routedRawChunkSealer -> scrubRecords -> zstd object), redacts a bare sensitive
 * reassignment. alpha.6 (the previous pin) returned each of these inputs unchanged.
 *
 * Controls: genuine declarations and non-sensitive assignments stay byte-identical, so a
 * blanket "redact every assignment" engine would fail here too.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { readAll, type ObjectStore } from "../src/lib/pipeline/object-layout.js";
import type { SealRawChunk } from "../src/lib/pipeline/raw-chunks.js";
import {
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  scrubJson,
  scrubLong,
  scrubText,
} from "../src/lib/scrub-engine/index.js";
import { PIN, TRIPLE } from "./support/real-bindgen.js";

/** Bare sensitive reassignments: the right-hand side must not survive. */
const REDACTED = [
  ["password = API_TOKEN;", "password = [REDACTED];", "API_TOKEN"],
  ["clientSecret = getSecret(input);", "clientSecret = [REDACTED];", "getSecret(input)"],
  ["export API_KEY = cfg.apiKey;", "export API_KEY = [REDACTED];", "cfg.apiKey"],
] as const;

/** Genuine declarations (may preserve a reference) and non-sensitive assignments. */
const PRESERVED = ["const password = API_TOKEN;", "const total = a + b;", "user = admin;"] as const;

const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"] as const;
let saved: Record<string, string | undefined>;
const dirs: string[] = [];
beforeEach(() => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

async function realEngine() {
  const engine = await initScrubEngine({ env: TRIPLE });
  // Guard against a silent fallback: this must be the verified alpha.8 instance.
  expect(engine.kind).toBe("wasm-bindgen");
  expect(PIN.package_version).toBe("0.5.0-alpha.8");
  expect((engine as unknown as { manifest: { crateVersion: string } }).manifest.crateVersion).toBe("0.5.0-alpha.8");
  return engine;
}

async function fsStore(): Promise<ObjectStore> {
  const root = await mkdtemp(join(tmpdir(), "reassign-seal-"));
  dirs.push(root);
  const s = createFsStore({ root });
  return {
    backend: s.backend, head: s.head.bind(s), get: s.get.bind(s), getObject: s.getObject.bind(s),
    list: s.list.bind(s), presignGet: s.presignGet.bind(s), put: s.put.bind(s), delete: s.delete.bind(s),
  } satisfies ObjectStore;
}

function chunk(text: string, seq: number): SealRawChunk {
  return {
    location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: `s${seq}`, startDate: "2026-11-11" },
    format: "ndjson", seq, epoch: 0, startOffset: 0, source: new TextEncoder().encode(text),
  };
}

describe("alpha.8 bare-reassignment repair through the pipeline's bound wasm", () => {
  it("seam scrubText / scrubLong redact each bare sensitive reassignment exactly", async () => {
    await realEngine();
    for (const [input, expected, secret] of REDACTED) {
      expect(scrubText(input), input).toBe(expected);
      expect(scrubLong(input), input).toBe(expected);
      expect(scrubText(`  ${input}\n`), input).toBe(`  ${expected}\n`);
      expect(scrubText(input)).not.toContain(secret);
    }
  });

  it("seam scrubJson redacts the reassignment inside a transcript field", async () => {
    await realEngine();
    for (const [input, expected, secret] of REDACTED) {
      const out = scrubJson({ type: "assistant", text: input });
      expect(out, input).toStrictEqual({ type: "assistant", text: expected });
      expect(JSON.stringify(out)).not.toContain(secret);
    }
  });

  it("the routed raw-chunk sealer never persists the right-hand side", async () => {
    await realEngine();
    const sealer = routedRawChunkSealer();
    expect(sealer, "SCRUB_ENGINE=wasm-bindgen must route raw sealing to the bound engine").toBeDefined();
    const store = await fsStore();
    let seq = 0;
    for (const [input, expected, secret] of REDACTED) {
      const out = await sealer!(store, chunk(`${JSON.stringify({ type: "user", text: input })}\n`, seq++));
      const object = await store.getObject(out.key);
      const stored = new TextDecoder().decode(zstdDecompressSync(await readAll(object.body)));
      expect(JSON.parse(stored.trim()).text, input).toBe(expected);
      expect(stored).not.toContain(secret);
      expect(out.entry.records).toBe(1);
      expect(object.meta.metadata["scrub-version"]).toBe("10");
      expect(object.meta.metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
    }
  });

  it("controls: declarations and non-sensitive assignments are byte-identical", async () => {
    await realEngine();
    for (const input of PRESERVED) {
      expect(scrubText(input), input).toBe(input);
      expect(scrubLong(input), input).toBe(input);
      expect(scrubJson({ text: input }), input).toStrictEqual({ text: input });
    }
  });
});
