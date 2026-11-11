/**
 * Readiness and admission under SCRUB_ENGINE_REQUIRED=wasm-bindgen with the pinned
 * relayscrubber 0.5.0-alpha.4 (docs/scrub-engine.md, "Cutover: pinned relayscrubber
 * 0.5.0-alpha.4"): ready ONLY for the verified instance loaded from the exact
 * checked-in pin. Missing/invalid pin, digest mismatch, missing artifacts, reader
 * ceiling refusal, a non-exact pin, poison, or no init at all: not ready, the routed
 * sealer refuses (so a production constructor fails at startup), the unrouted seam
 * refuses, and the HTTP ingress answers 503 scrub_unavailable with nothing stored.
 * With the knob unset nothing changes: TS engine, no routed sealer.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { AppEnv } from "../src/env.js";
import { createPipelineRoutes } from "../src/routes/pipeline.js";
import { tsScrubEngine } from "../src/lib/scrub-engine/engine.js";
import { bindgenFileReader } from "../src/lib/scrub-engine/bindgen-engine.js";
import {
  SCRUB_BINDGEN_PIN,
  activeScrubEngine,
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  routedRawChunkSealerDeps,
  scrubEngineReady,
  sealRawChunk,
} from "../src/lib/scrub-engine/index.js";
import { PIN, TRIPLE, faultGlue, sealInput, tempStores } from "./support/real-bindgen.js";

const enc = new TextEncoder();
const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"] as const;
const stores = tempStores();
const dirs: string[] = [];
let saved: Record<string, string | undefined>;
beforeEach(() => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await stores.cleanup();
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

const LINE = '{"a":"AKIAIOSFODNN7EXAMPLE"}\n';
async function ingress(deps: Parameters<typeof createPipelineRoutes>[0] extends infer D ? Partial<NonNullable<D>> : never = {}) {
  const store = await stores.store();
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => { c.set("auth" as never, { orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:sync"] } as never); c.set("correlationId", "corr"); await next(); });
  app.route("/v1", createPipelineRoutes({ ...deps, rawStore: () => ({ store, provider: "s3", bucket: "fixture" }), sink: () => ({ record: async () => {} }) as never, deadLetters: () => ({}) as never, send: async () => {} } as never));
  const response = await app.request("/v1/pipeline/chunks?machine=m&harness=claude&session=s&date=2026-11-04&seq=0&epoch=0&offset=0&format=ndjson", { method: "POST", body: LINE }, { PIPELINE_ORG_ID: "org-a" });
  return { response, store };
}

/** Not ready: every surface refuses and nothing is stored. */
async function expectRefusing(reason: RegExp | string) {
  expect(scrubEngineReady()).toBe(false);
  expect(() => activeScrubEngine()).toThrow(expect.objectContaining({ reason: expect.stringMatching(reason) }));
  expect(() => routedRawChunkSealer()).toThrow(expect.objectContaining({ reason: expect.stringMatching(reason) }));
  expect(() => routedRawChunkSealerDeps()).toThrow(); // the production constructor fails at startup
  const store = await stores.store();
  await expect(sealRawChunk(store, sealInput(enc.encode(LINE)))).rejects.toMatchObject({ reason: expect.stringMatching(reason) });
  expect(store.puts).toEqual([]);
  const { response, store: s2 } = await ingress();
  expect(response.status).toBe(503);
  expect(((await response.json()) as { error: { code: string } }).error.code).toBe("scrub_unavailable");
  expect(s2.puts).toEqual([]);
}

const reorderedPin = () => Object.fromEntries(Object.entries(PIN).reverse()); // same values, not the checked-in object

describe("ready: only the exact checked-in pin, verified", () => {
  it("explicit env triple: ready, the routed sealer is bound to the active instance and seals version 10 through HTTP", async () => {
    Object.assign(process.env, TRIPLE);
    const engine = await initScrubEngine({ env: TRIPLE });
    expect(engine.kind).toBe("wasm-bindgen");
    expect(engine.marker).toStrictEqual({ ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" });
    expect(scrubEngineReady()).toBe(true);
    expect(activeScrubEngine()).toBe(engine);
    const sealer = routedRawChunkSealer()!;
    expect(routedRawChunkSealer()).toBe(sealer);
    expect(routedRawChunkSealerDeps()).toStrictEqual({ rawChunkSealer: sealer });
    const { response, store } = await ingress(routedRawChunkSealerDeps());
    expect(response.status).toBe(201);
    expect(((await response.json()) as { scrub: unknown }).scrub).toMatchObject({ version: 10, profile: "relayhistory-secrets-v10" });
    expect(store.puts).toHaveLength(1);
  });

  it("from process.env (the production entrypoint form): ready", async () => {
    Object.assign(process.env, TRIPLE);
    await initScrubEngine();
    expect(scrubEngineReady()).toBe(true);
    expect(routedRawChunkSealer()).toBeTypeOf("function");
  });

  it("SCRUB_BINDGEN_DIR pointing at the vendored package (absolute): ready", async () => {
    const env = { ...TRIPLE, SCRUB_BINDGEN_DIR: new URL("../scrub-bindgen/", import.meta.url).pathname };
    Object.assign(process.env, env);
    await initScrubEngine({ env });
    expect(scrubEngineReady()).toBe(true);
  });
});

describe("not ready under SCRUB_ENGINE_REQUIRED=wasm-bindgen: ingress refuses", () => {
  beforeEach(() => { Object.assign(process.env, TRIPLE); });

  it("never initialised", async () => {
    await expectRefusing("not_initialised");
  });

  for (const [name, pin] of [["missing (undefined)", undefined], ["null", null], ["not an object", "pin"], ["empty object", {}]] as const)
    it(`pin ${name}: bindgen_pin_invalid`, async () => {
      await expect(initScrubEngine({ env: TRIPLE, bindgen: { pin } })).rejects.toMatchObject({ reason: "bindgen_pin_invalid" });
      await expectRefusing("bindgen_pin_invalid");
    });

  it("a wasm digest mismatch: bindgen_sha256_mismatch_wasm", async () => {
    const real = bindgenFileReader(undefined);
    const readArtifact = async (name: "wasm" | "glue" | "loader", rel: string) => {
      const bytes = await real(name, rel);
      if (name !== "wasm") return bytes;
      const copy = bytes.slice(); copy[copy.length >> 1] ^= 1; return copy;
    };
    await expect(initScrubEngine({ env: TRIPLE, bindgen: { readArtifact } })).rejects.toMatchObject({ reason: "bindgen_sha256_mismatch_wasm" });
    await expectRefusing("bindgen_sha256_mismatch_wasm");
  });

  it("SCRUB_BINDGEN_DIR is an empty directory: bindgen_artifact_missing_*", async () => {
    const dir = await mkdtemp(join(tmpdir(), "scrub-bindgen-empty-")); dirs.push(dir);
    const env = { ...TRIPLE, SCRUB_BINDGEN_DIR: dir };
    await expect(initScrubEngine({ env })).rejects.toMatchObject({ reason: expect.stringMatching(/^bindgen_artifact_missing_/) });
    await expectRefusing(/^bindgen_artifact_missing_/);
  });

  it("a relative SCRUB_BINDGEN_DIR: refused", async () => {
    await expect(initScrubEngine({ env: { ...TRIPLE, SCRUB_BINDGEN_DIR: "scrub-bindgen" } })).rejects.toMatchObject({ reason: "bindgen_dir_invalid" });
    expect(scrubEngineReady()).toBe(false);
  });

  for (const ceiling of [undefined, "8", "9"])
    it(`READER_MAX_RULESET ${ceiling ?? "unset"}: bindgen_ruleset_above_reader_ceiling`, async () => {
      const env: Record<string, string> = { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen" };
      if (ceiling) env.READER_MAX_RULESET = ceiling;
      delete process.env.READER_MAX_RULESET;
      if (ceiling) process.env.READER_MAX_RULESET = ceiling;
      await expect(initScrubEngine({ env })).rejects.toMatchObject({ reason: "bindgen_ruleset_above_reader_ceiling" });
      await expectRefusing("bindgen_ruleset_above_reader_ceiling");
    });

  it("a pin that verifies the same bytes but is not the checked-in pin: bindgen_pin_not_exact", async () => {
    const pin = reorderedPin();
    expect(pin).toStrictEqual(SCRUB_BINDGEN_PIN); // same content...
    expect(JSON.stringify(pin)).not.toBe(JSON.stringify(SCRUB_BINDGEN_PIN)); // ...not the exact checked-in pin
    await initScrubEngine({ env: TRIPLE, bindgen: { pin } });
    await expectRefusing("bindgen_pin_not_exact");
  });

  it("poisoned after a ready start", async () => {
    const glue = faultGlue();
    await initScrubEngine({ env: TRIPLE, bindgen: { importGlue: glue.importGlue } });
    expect(scrubEngineReady()).toBe(true);
    glue.overrides.scrubTextChecked = () => { throw new WebAssembly.RuntimeError("unreachable"); };
    expect(() => activeScrubEngine().scrubText("x")).toThrow(expect.objectContaining({ disposition: "instance_poison" }));
    expect(scrubEngineReady()).toBe(false);
    expect(() => routedRawChunkSealer()).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned" }));
    const { response, store } = await ingress();
    expect(response.status).toBe(503);
    expect(store.puts).toEqual([]);
  });

  it("SCRUB_ENGINE=ts with SCRUB_ENGINE_REQUIRED=wasm-bindgen: refused at selection, never silently TS", async () => {
    const env = { ...TRIPLE, SCRUB_ENGINE: "ts" };
    Object.assign(process.env, env);
    await expect(initScrubEngine({ env })).rejects.toThrow();
    expect(scrubEngineReady()).toBe(false);
    expect(() => routedRawChunkSealer()).toThrow();
  });
});

describe("default (knob unset) is unchanged", () => {
  for (const value of [undefined, "", "ts"])
    it(`SCRUB_ENGINE=${JSON.stringify(value)}: TS engine, no routed sealer, deps {} and ready`, async () => {
      if (value !== undefined) process.env.SCRUB_ENGINE = value;
      expect(routedRawChunkSealer()).toBeUndefined();
      expect(routedRawChunkSealerDeps()).toStrictEqual({});
      expect(await initScrubEngine()).toBe(tsScrubEngine);
      expect(activeScrubEngine()).toBe(tsScrubEngine);
      expect(routedRawChunkSealerDeps()).toStrictEqual({});
      expect(scrubEngineReady()).toBe(true);
      const { response } = await ingress(routedRawChunkSealerDeps());
      expect(response.status).toBe(201);
      expect(((await response.json()) as { scrub: Record<string, unknown> }).scrub).toMatchObject({ version: 8 });
    });

  it("an invalid SCRUB_ENGINE value: routedRawChunkSealer adds nothing and does not throw (selection fails where it does today)", () => {
    process.env.SCRUB_ENGINE = "WASM-BINDGEN";
    expect(routedRawChunkSealerDeps()).toStrictEqual({});
    expect(() => activeScrubEngine()).toThrow();
  });
});
