/**
 * Cloudflare Workers are excluded from the relayscrubber 0.5.0-alpha.4 cutover
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.4"): the hosted
 * ingest Worker keeps the TS engine (scrub version 8), bundles no relayscrubber package,
 * never wires `createCompiledBindgenEngine`, and refuses SCRUB_ENGINE=wasm-bindgen
 * outright (503 not_configured), so no Worker can claim scrub version 10.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { builtinModules } from "node:module";
import { join } from "node:path";
import worker from "../worker.ts";
import { activeScrubEngine, resetScrubEngineForTests, scrubEngineReady } from "../../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { SCRUB_RULESET_VERSION } from "../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts";

const ROOT = join(import.meta.dir, "../../..");
const NODE = new Set(builtinModules.flatMap((m) => [m, `node:${m}`]));
const TRIPLE = { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen", READER_MAX_RULESET: "10" };
const ctx = { waitUntil() {}, passThroughOnException() {} };
afterEach(() => resetScrubEngineForTests());

describe("hosted ingest Worker refuses the cutover engine", () => {
  for (const [name, env] of [
    ["the full env triple", TRIPLE],
    ["SCRUB_ENGINE=wasm-bindgen alone (reader ceiling 8)", { SCRUB_ENGINE: "wasm-bindgen" }],
  ] as const)
    test(`${name}: 503 not_configured, engine unavailable, nothing read from disk`, async () => {
      resetScrubEngineForTests();
      const response = await worker.fetch(new Request("https://ingest.example/v1/delivery/batches", { method: "POST", body: "{}" }), env as never, ctx as never);
      expect(response.status).toBe(503);
      expect(((await response.json()) as { error: { code: string } }).error.code).toBe("not_configured");
      expect(scrubEngineReady()).toBe(false);
      expect(() => activeScrubEngine()).toThrow(
        expect.objectContaining({ reason: env === TRIPLE ? "bindgen_artifact_missing_wasm" : "bindgen_ruleset_above_reader_ceiling" }),
      );
    });

  test("knob unset: the Worker's engine is the TS engine at scrub version 8", async () => {
    resetScrubEngineForTests();
    await worker.fetch(new Request("https://ingest.example/v1/delivery/batches", { method: "POST", body: "{}" }), {} as never, ctx as never);
    expect(activeScrubEngine().kind).toBe("ts");
    expect(activeScrubEngine().marker.version).toBe(8);
    expect(SCRUB_RULESET_VERSION).toBe(8);
  });
});

describe("Worker bundles and configs never carry the cutover", () => {
  test("the ingest Worker bundle contains no relayscrubber glue or wasm", async () => {
    const result = await Bun.build({
      entrypoints: [join(ROOT, "src/session-ingest/worker.ts")],
      target: "browser",
      format: "esm",
      conditions: ["workerd", "worker", "browser"],
      tsconfig: join(ROOT, "tsconfig.json"),
      throw: false,
      plugins: [{ name: "externals", setup(b) { b.onResolve({ filter: /.*/ }, (a) => (NODE.has(a.path) || a.path.startsWith("cloudflare:") ? { path: a.path, external: true } : undefined)); } }],
    });
    expect(result.success).toBe(true);
    expect(result.outputs.map((o) => o.path).filter((p) => p.endsWith(".wasm"))).toEqual([]);
    const code = await result.outputs[0]!.text();
    expect(code).not.toMatch(/__wbg_init|__wbg_get_imports|relayscrubber_bg\.wasm"\s*,\s*import\.meta\.url/);
    // The writer ceiling the Worker can claim stays the TS ruleset version.
    expect(code).not.toMatch(/SCRUB_RULESET_VERSION\s*=\s*10\b/);
  }, 60_000);

  test("no Worker entrypoint wires createCompiledBindgenEngine or imports the vendored package", () => {
    const files = [
      "src/session-ingest/worker.ts",
      ...readdirSync(join(ROOT, "packages/session-read-worker/src")).filter((f) => f.endsWith(".ts")).map((f) => `packages/session-read-worker/src/${f}`),
      ...readdirSync(join(ROOT, "packages/amp-transport-worker/src")).filter((f) => f.endsWith(".ts")).map((f) => `packages/amp-transport-worker/src/${f}`),
    ];
    for (const file of files) {
      // Code only: comments may name what is excluded.
      const text = readFileSync(join(ROOT, file), "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
      expect(`${file}: ${/createCompiledBindgenEngine|scrub-bindgen\/|initBindgenScrubEngine/.test(text)}`).toBe(`${file}: false`);
    }
  });

  test("no Worker deployment config selects wasm-bindgen or raises the ceiling to 10", () => {
    const configs = [
      "stacks/cloudflare/session-ingest.ts",
      "stacks/cloudflare/alchemy.cloudflare.run.ts",
      "packages/session-read-worker/wrangler.jsonc",
      "packages/session-read-worker/wrangler.digest-narrative.jsonc",
      "packages/amp-transport-worker/wrangler.template.json",
    ];
    for (const file of configs) {
      const text = readFileSync(join(ROOT, file), "utf8");
      expect(`${file}: ${/SCRUB_ENGINE|wasm-bindgen|READER_MAX_RULESET/.test(text)}`).toBe(`${file}: false`);
    }
  });
});
