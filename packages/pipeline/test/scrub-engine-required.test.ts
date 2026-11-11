/**
 * SCRUB_ENGINE_REQUIRED (default off): refuses a selection whose SCRUB_ENGINE kind
 * differs from the required one, permanently for the process, and never falls back to
 * TypeScript. It never selects or initialises anything itself, so a wasm-bindgen
 * requirement with the checked-in alpha.5 pin at the default reader ceiling still refuses
 * through the bindgen refusals, and under it only the exact checked-in pin is ready.
 * Module state (`selected`, the sticky refusal) is reset per case by resetScrubEngineForTests,
 * as in scrub-engine-bindgen.test.ts; process.env is saved and restored around each case.
 */
import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { isSafeIdentifier } from "../src/lib/pipeline/trace.js";
import {
  SCRUB_UNAVAILABLE_LOG_TEXT,
  ScrubEngineConfigError,
  ScrubEngineUnavailable,
  activeScrubEngine,
  activeScrubMarker,
  bindgenProfile,
  initScrubEngine,
  parseScrubEngineRequired,
  resetScrubEngineForTests,
  scrubEngineReady,
  scrubRecords,
  scrubText,
  scrubTextForLog,
  sealRawChunk,
  selectScrubEngine,
  tsScrubEngine,
} from "../src/lib/scrub-engine/index.js";

const enc = new TextEncoder();
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET"] as const;
let saved: Record<string, string | undefined>;
beforeEach(() => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
});
afterEach(() => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
});

const isRequiredRefusal = (e: unknown) =>
  e instanceof ScrubEngineConfigError && e.reason === "engine_selection_required" && e.code === "scrub_engine_invalid" && e.retryable === false;

/** Bun does not support `rejects.toSatisfy`; capture the rejection reason explicitly. */
async function captureRejection(p: Promise<unknown>): Promise<unknown> {
  try { await p; } catch (error) { return error; }
  throw new Error("expected promise to reject");
}

/** Every seam entry point refuses; logging and predicates never see the input; nothing is TypeScript. */
async function expectRequiredRefusalEverywhere() {
  for (const [name, call] of [
    ["activeScrubEngine", () => activeScrubEngine()],
    ["scrubText", () => scrubText("token=abc")],
    ["scrubRecords", () => scrubRecords(enc.encode('{"a":"b"}\n'))],
    ["activeScrubMarker", () => activeScrubMarker()],
  ] as const) {
    let thrown: unknown;
    try { call(); } catch (error) { thrown = error; }
    expect(isRequiredRefusal(thrown), name).toBe(true);
  }
  expect(isRequiredRefusal(await captureRejection(sealRawChunk({} as never, { source: enc.encode("x\n") } as never)))).toBe(true);
  expect(scrubEngineReady()).toBe(false);
  expect(scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
  expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
}

describe("parseScrubEngineRequired", () => {
  it("unset and empty impose nothing; exact kinds parse; anything else is a config error", () => {
    expect(parseScrubEngineRequired(undefined)).toBeUndefined();
    expect(parseScrubEngineRequired("")).toBeUndefined();
    for (const k of ["ts", "wasm", "wasm-bindgen"] as const) expect(parseScrubEngineRequired(k)).toBe(k);
    for (const bad of ["TS", "Wasm-Bindgen", " wasm", "wasm ", "typescript", "1", "true", "wasm_bindgen", "none"])
      expect(() => parseScrubEngineRequired(bad), bad).toThrow(ScrubEngineConfigError);
  });

  it("an invalid value fails selection even when SCRUB_ENGINE is fine, and does not latch a requirement refusal", async () => {
    expect(await captureRejection(initScrubEngine({ env: { SCRUB_ENGINE_REQUIRED: "WASM-BINDGEN" } }))).toBeInstanceOf(ScrubEngineConfigError);
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "ts", SCRUB_ENGINE_REQUIRED: "yes" })).toThrow(
      expect.objectContaining({ code: "scrub_engine_invalid", reason: undefined }),
    );
    expect(selectScrubEngine({})).toBe("ts");
    expect(activeScrubEngine()).toBe(tsScrubEngine);
  });
});

describe("SCRUB_ENGINE_REQUIRED unset: today's behaviour", () => {
  for (const required of [undefined, ""])
    it(`required=${JSON.stringify(required)}: SCRUB_ENGINE unset selects TypeScript and is ready`, async () => {
      expect(await initScrubEngine({ env: { SCRUB_ENGINE_REQUIRED: required } })).toBe(tsScrubEngine);
      expect(activeScrubEngine()).toBe(tsScrubEngine);
      expect(scrubEngineReady()).toBe(true);
    });

  it("process.env without the knob: activeScrubEngine still selects TypeScript lazily", () => {
    expect(activeScrubEngine()).toBe(tsScrubEngine);
    expect(scrubEngineReady()).toBe(true);
  });
});

describe("SCRUB_ENGINE_REQUIRED matching SCRUB_ENGINE", () => {
  it("required=ts with SCRUB_ENGINE unset, empty or ts selects TypeScript", async () => {
    for (const engine of [undefined, "", "ts"]) {
      resetScrubEngineForTests();
      expect(await initScrubEngine({ env: { SCRUB_ENGINE: engine, SCRUB_ENGINE_REQUIRED: "ts" } })).toBe(tsScrubEngine);
      expect(scrubEngineReady()).toBe(true);
    }
  });

  it("required=wasm-bindgen with a verified test-double pin (not the checked-in pin) is NOT ready: bindgen_pin_not_exact", async () => {
    const bytes = { wasm: enc.encode("synthetic wasm"), glue: enc.encode("synthetic glue"), loader: enc.encode("synthetic loader") };
    const manifest = { schema: 3, ruleset: "relayhistory-scrub", rulesetVersion: 10, rulesetSourceSha256: "a".repeat(64), crateVersion: "0.0.0-test", abiVersion: 2, policy: "secrets", redactionToken: "[REDACTED]", scrubProfile: "fixture-v10" };
    const pin = { schema: 2, package: "@agentworkforce/relayscrubber", package_version: manifest.crateVersion, release: { source: "github-release", repository: "AgentWorkforce/relayscrubber", tag: "v0.0.0-test", tarball: "test.tgz", tarball_sha256: "e".repeat(64), sha256sums: { "artifact.json": "f".repeat(64) } }, wasm_sha256: sha(bytes.wasm), glue_sha256: sha(bytes.glue), loader_sha256: sha(bytes.loader), paths: { wasm: "t.wasm", glue: "t.js", loader: "index.js" }, scrub_profile: bindgenProfile(manifest), manifest };
    const engine = await initScrubEngine({
      env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen", READER_MAX_RULESET: "10" },
      bindgen: { pin, readArtifact: async (name) => bytes[name], importGlue: async () => ({ default: async () => ({}), manifestJson: () => JSON.stringify(manifest), scrubTextChecked: (s: string) => s, scrubJson: (s: string) => JSON.stringify({ jsonText: s }), scrubRecords: () => JSON.stringify({ text: "x\n", recordCount: 1 }) }) },
    });
    expect(engine.kind).toBe("wasm-bindgen");
    expect(() => activeScrubEngine()).toThrow(expect.objectContaining({ reason: "bindgen_pin_not_exact" }));
    expect(scrubEngineReady()).toBe(false);
    expect(scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
    // The positive case (exact checked-in pin, real vendored bytes) is in scrub-bindgen-readiness.test.ts.
  });
});

describe("SCRUB_ENGINE_REQUIRED refuses another selection", () => {
  const mismatches: [string | undefined, string][] = [
    [undefined, "wasm-bindgen"], ["", "wasm-bindgen"], ["ts", "wasm-bindgen"], ["wasm", "wasm-bindgen"],
    [undefined, "wasm"], ["wasm-bindgen", "wasm"],
    ["wasm", "ts"], ["wasm-bindgen", "ts"],
  ];
  for (const [engine, required] of mismatches)
    it(`SCRUB_ENGINE=${JSON.stringify(engine)} SCRUB_ENGINE_REQUIRED=${required}: initScrubEngine refuses and nothing runs`, async () => {
      expect(isRequiredRefusal(await captureRejection(initScrubEngine({ env: { SCRUB_ENGINE: engine, SCRUB_ENGINE_REQUIRED: required } })))).toBe(true);
      await expectRequiredRefusalEverywhere();
    });

  it("selectScrubEngine refuses directly with the same reason", () => {
    expect(() => selectScrubEngine({ SCRUB_ENGINE_REQUIRED: "wasm-bindgen" })).toThrow(expect.objectContaining({ reason: "engine_selection_required" }));
    expect(scrubEngineReady()).toBe(false);
  });

  it("from process.env: activeScrubEngine refuses instead of lazily selecting TypeScript", async () => {
    process.env.SCRUB_ENGINE_REQUIRED = "wasm-bindgen";
    expect(() => activeScrubEngine()).toThrow(ScrubEngineConfigError);
    await expectRequiredRefusalEverywhere();
  });

  it("the refusal is sticky: a later explicit TS selection, or an env without the knob, never returns TypeScript", async () => {
    expect(isRequiredRefusal(await captureRejection(initScrubEngine({ env: { SCRUB_ENGINE_REQUIRED: "wasm-bindgen" } })))).toBe(true);
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "ts" })).toThrow(ScrubEngineConfigError);
    expect(() => selectScrubEngine({})).toThrow(ScrubEngineConfigError);
    expect(isRequiredRefusal(await captureRejection(initScrubEngine({ env: {} })))).toBe(true);
    await expectRequiredRefusalEverywhere();
  });

  it("an engine already selected as TypeScript is refused once a requirement for another kind arrives", async () => {
    expect(activeScrubEngine()).toBe(tsScrubEngine);
    expect(isRequiredRefusal(await captureRejection(initScrubEngine({ env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen" } })))).toBe(true);
    await expectRequiredRefusalEverywhere();
  });
});

describe("SCRUB_ENGINE_REQUIRED=wasm-bindgen does not bypass the bindgen refusals", () => {
  it("checked-in alpha.5 pin without READER_MAX_RULESET=10: the reader-ceiling refusal stands, never TypeScript", async () => {
    const env = { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen" };
    const error = await initScrubEngine({ env }).then(() => null, (e: unknown) => e);
    expect(error).toBeInstanceOf(ScrubEngineUnavailable);
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_ruleset_above_reader_ceiling");
    expect(() => activeScrubEngine()).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    expect(scrubEngineReady()).toBe(false);
    expect(scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
  });

  it("a pin whose artifact digest does not match the bytes: sha256 refusal, never TypeScript", async () => {
    const bytes = { wasm: enc.encode("w"), glue: enc.encode("g"), loader: enc.encode("l") };
    const manifest = { schema: 3, ruleset: "relayhistory-scrub", rulesetVersion: 8, rulesetSourceSha256: "a".repeat(64), crateVersion: "0.0.0-test", abiVersion: 2, policy: "secrets", redactionToken: "[REDACTED]", scrubProfile: "fixture-v8" };
    const pin = { schema: 2, package: "@agentworkforce/relayscrubber", package_version: manifest.crateVersion, release: { source: "github-release", repository: "AgentWorkforce/relayscrubber", tag: "v0.0.0-test", tarball: "test.tgz", tarball_sha256: "e".repeat(64), sha256sums: { "artifact.json": "f".repeat(64) } }, wasm_sha256: "b".repeat(64), glue_sha256: sha(bytes.glue), loader_sha256: sha(bytes.loader), paths: { wasm: "t.wasm", glue: "t.js", loader: "index.js" }, scrub_profile: bindgenProfile(manifest), manifest };
    let imported = false;
    const error = await initScrubEngine({
      env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen" },
      bindgen: { pin, readArtifact: async (name) => bytes[name], importGlue: async () => { imported = true; return {}; } },
    }).then(() => null, (e: unknown) => e);
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_sha256_mismatch_wasm");
    expect(imported).toBe(false);
    expect(() => activeScrubEngine()).toThrow(ScrubEngineUnavailable);
    expect(scrubEngineReady()).toBe(false);
  });
});
