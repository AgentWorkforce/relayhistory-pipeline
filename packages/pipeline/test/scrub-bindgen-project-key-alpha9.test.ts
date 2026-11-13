/**
 * Pipeline-side check for relayscrubber alpha.9's project_key change
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.9").
 *
 * alpha.8 redacted capture's `project_key` as a generic `*_key`, so the session directory never
 * learned which repository a session worked in. alpha.9 keeps an exact `project_key` whose value
 * is a normalized repository identity (`host.tld/owner/repo`). This proves OUR binding gets that:
 * the real vendored wasm, verified by initScrubEngine under the self-host env triple, through
 * the seam production imports (scrubJson / scrubText) and the routed raw-chunk sealer.
 *
 * Controls: a local-path value, a vendor-style key, a prefixed name and a neighbouring
 * credential stay redacted, so a blanket "keep project_key" engine would fail here too.
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
  scrubText,
} from "../src/lib/scrub-engine/index.js";
import { PIN, TRIPLE } from "./support/real-bindgen.js";

const IDENTITY = "github.com/example/overlap-sandbox";
// Synthetic, assembled so no scanner reads a credential in the source.
const VENDOR_KEY = ["c0ab12cd", "_SYNTHaZ91bY82cX73dW64eV55"].join("");
const STRIPE = ["sk", "_live_", "SYNTHETICaZ91bY82cX73dW64eV55"].join("");

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
  expect(engine.kind).toBe("wasm-bindgen");
  expect(PIN.package_version).toBe("0.5.0-alpha.9");
  expect((engine as unknown as { manifest: { crateVersion: string } }).manifest.crateVersion).toBe("0.5.0-alpha.9");
  return engine;
}

async function fsStore(): Promise<ObjectStore> {
  const root = await mkdtemp(join(tmpdir(), "project-key-seal-"));
  dirs.push(root);
  const s = createFsStore({ root });
  return {
    backend: s.backend, head: s.head.bind(s), get: s.get.bind(s), getObject: s.getObject.bind(s),
    list: s.list.bind(s), presignGet: s.presignGet.bind(s), put: s.put.bind(s), delete: s.delete.bind(s),
  } satisfies ObjectStore;
}

function chunk(text: string, seq: number): SealRawChunk {
  return {
    location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: `s${seq}`, startDate: "2026-11-13" },
    format: "ndjson", seq, epoch: 0, startOffset: 0, source: new TextEncoder().encode(text),
  };
}

describe("alpha.9 keeps capture's project tag through the pipeline's bound wasm", () => {
  it("seam scrubJson keeps a repository project_key and still redacts the rest", async () => {
    await realEngine();
    const record = { type: "session", project_key: IDENTITY, project_key_method: "remote", api_key: STRIPE };
    expect(scrubJson(record)).toStrictEqual({ ...record, api_key: "[REDACTED]" });
    for (const value of ["/Users/someone/Projects/app", VENDOR_KEY]) {
      expect(scrubJson({ project_key: value }), value).toStrictEqual({ project_key: "[REDACTED]" });
    }
    expect(scrubJson({ deta_project_key: IDENTITY })).toStrictEqual({ deta_project_key: "[REDACTED]" });
  });

  it("seam scrubText keeps the identity in an assignment and redacts a vendor value", async () => {
    await realEngine();
    expect(scrubText(`project_key: ${IDENTITY}`)).toBe(`project_key: ${IDENTITY}`);
    expect(scrubText(`project_key=${VENDOR_KEY}`)).not.toContain(VENDOR_KEY);
  });

  it("the routed raw-chunk sealer persists the project tag and never the credential", async () => {
    await realEngine();
    const sealer = routedRawChunkSealer();
    expect(sealer, "SCRUB_ENGINE=wasm-bindgen must route raw sealing to the bound engine").toBeDefined();
    const store = await fsStore();
    const line = JSON.stringify({ type: "session", project_key: IDENTITY, api_key: STRIPE });
    const out = await sealer!(store, chunk(`${line}\n`, 0));
    const object = await store.getObject(out.key as Parameters<ObjectStore["getObject"]>[0]);
    const stored = new TextDecoder().decode(zstdDecompressSync(await readAll(object.body)));
    expect(JSON.parse(stored.trim())).toStrictEqual({ type: "session", project_key: IDENTITY, api_key: "[REDACTED]" });
    expect(stored).not.toContain(STRIPE);
    expect(object.meta.metadata["scrub-version"]).toBe("10");
    expect(object.meta.metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
  });
});
