/** Candidate guard contract against inert glue, never an artifact qualification. */
import { createHash } from "node:crypto";
import { beforeEach, afterEach, describe, expect, it } from "vitest";
import {
  createCompiledBindgenEngine, initScrubEngine, resetScrubEngineForTests,
  ScrubberError, SCRUBBER_CALL_REFUSAL_CODES, type ScrubEngine,
} from "../src/lib/scrub-engine/index.js";

const enc = new TextEncoder();
const manifest = {
  schema: 3, ruleset: "relayhistory-scrub", rulesetVersion: 10,
  rulesetSourceSha256: "1".repeat(64), crateVersion: "0.0.0-test-double",
  abiVersion: 2, policy: "secrets", redactionToken: "[REDACTED]",
  scrubProfile: "relayhistory-secrets-v10",
};
const bytes = enc.encode("inert test artifact, not relayscrubber");
const digest = createHash("sha256").update(bytes).digest("hex");
const pin = {
  schema: 1, package: "@agentworkforce/relayscrubber", package_version: "0.0.0-test-double",
  wasm_sha256: digest, glue_sha256: digest, loader_sha256: digest,
  paths: { wasm: "test.wasm", glue: "test.js", loader: "index.js" },
  scrub_profile: manifest.scrubProfile, manifest,
};
type Path = "node" | "compiled";
type Op = "scrubText" | "scrubLong" | "scrubJson" | "scrubRecords";
const ops: Op[] = ["scrubText", "scrubLong", "scrubJson", "scrubRecords"];
function call(engine: ScrubEngine, op: Op): unknown {
  if (op === "scrubRecords") return engine.scrubRecords(enc.encode('{}\n'));
  if (op === "scrubJson") return engine.scrubJson({ note: "SYNTH_PRIVATE" });
  return engine[op]("SYNTH_PRIVATE");
}
async function setup(path: Path, fault: unknown, selected: Op) {
  let throwing = true;
  let calls = 0;
  let inits = 0;
  const enter = (op: Op) => { calls++; if (throwing && op === selected) throw fault; };
  const glue = {
    default: async () => { inits++; }, initSync: () => { inits++; },
    manifestJson: () => JSON.stringify(manifest),
    scrubTextChecked: () => { enter(selected === "scrubLong" ? "scrubLong" : "scrubText"); return "safe"; },
    scrubLong: () => { enter("scrubLong"); return "safe"; },
    scrubJson: () => { enter("scrubJson"); return '{"jsonText":"null","redactionCount":0}'; },
    scrubRecords: () => { enter("scrubRecords"); return '{"text":"{}\\n","recordCount":1,"redactionCount":0}'; },
  };
  const ModuleCtor = WebAssembly.Module as unknown as new (b: Uint8Array) => WebAssembly.Module;
  const compiled = { pin, readerMaxRuleset: 10, glue, module: new ModuleCtor(new Uint8Array([0,97,115,109,1,0,0,0])) };
  const node = { env: { SCRUB_ENGINE: "wasm-bindgen", READER_MAX_RULESET: "10" },
    bindgen: { pin, readArtifact: async () => bytes, importGlue: async () => glue } };
  const init = () => path === "node" ? initScrubEngine(node) : createCompiledBindgenEngine(compiled);
  return { engine: await init(), init, calls: () => calls, inits: () => inits, clear: () => { throwing = false; } };
}
function caught(fn: () => unknown): unknown {
  let result: unknown;
  try { result = fn(); } catch (e) { return e; }
  throw new Error(`expected refusal, received ${typeof result}`);
}
beforeEach(resetScrubEngineForTests);
afterEach(resetScrubEngineForTests);

for (const path of ["node", "compiled"] as const) describe(`${path} crate error contract`, () => {
  for (const op of ops) for (const code of SCRUBBER_CALL_REFUSAL_CODES)
    it(`${op}/${code}: explicit tag refuses just one call`, async () => {
      const s = await setup(path, JSON.stringify({ code, disposition: "call_refusal", record: 2 }), op);
      const error = caught(() => call(s.engine, op));
      expect(error).toBeInstanceOf(ScrubberError);
      expect(error).toMatchObject({ code, disposition: "call_refusal", record: 2, retryable: false });
      expect(String(error)).not.toContain("SYNTH_PRIVATE");
      expect((s.engine as { failed?: boolean }).failed).toBe(false);
      s.clear();
      expect(() => call(s.engine, op)).not.toThrow();
      expect(await s.init()).toBe(s.engine);
      expect(s.inits()).toBe(1);
    });

  it("recognized refusal may omit the record number", async () => {
    const s = await setup(path, '{"code":"invalid_json","disposition":"call_refusal"}', "scrubJson");
    const error = caught(() => s.engine.scrubJson(null));
    expect(error).toMatchObject({ code: "invalid_json", disposition: "call_refusal" });
    expect((error as ScrubberError).record).toBeUndefined();
    s.clear(); expect(s.engine.scrubJson(null)).toBe(null);
  });

  const unknowns: [string, () => unknown][] = [
    ["Error", () => new Error("SYNTH_PRIVATE")], ["RangeError", () => new RangeError("SYNTH_PRIVATE")],
    ["trap", () => new WebAssembly.RuntimeError("SYNTH_PRIVATE")], ["raw string", () => "SYNTH_PRIVATE"],
    ["null", () => null], ["undefined", () => undefined], ["object", () => ({ code: "invalid_json", disposition: "call_refusal" })],
    ["malformed JSON", () => '{"code":'], ["JSON array", () => '[]'],
    ["unknown code", () => '{"code":"SYNTH_PRIVATE","disposition":"call_refusal"}'],
    ["missing disposition", () => '{"code":"invalid_json"}'],
    ["other disposition", () => '{"code":"invalid_json","disposition":"instance_poison"}'],
    ["extra field", () => '{"code":"invalid_json","disposition":"call_refusal","message":"SYNTH_PRIVATE"}'],
    ["record zero", () => '{"code":"invalid_json","disposition":"call_refusal","record":0}'],
    ["record negative", () => '{"code":"invalid_json","disposition":"call_refusal","record":-1}'],
    ["record string", () => '{"code":"invalid_json","disposition":"call_refusal","record":"1"}'],
    ["record fraction", () => '{"code":"invalid_json","disposition":"call_refusal","record":1.5}'],
    ["record null", () => '{"code":"invalid_json","disposition":"call_refusal","record":null}'],
    ["record unsafe", () => '{"code":"invalid_json","disposition":"call_refusal","record":1e100}'],
    ["wrapped tag", () => new Error('{"code":"invalid_json","disposition":"call_refusal"}')],
    ["oversized tag", () => ' '.repeat(1025) + '{"code":"invalid_json","disposition":"call_refusal"}'],
  ];
  for (const [name, fault] of unknowns) for (const op of ops)
    it(`${op}/${name}: unknown throw poisons and never re-enters glue`, async () => {
      const s = await setup(path, fault(), op);
      const first = caught(() => call(s.engine, op));
      expect(first).toBeInstanceOf(ScrubberError);
      expect(first).toMatchObject({ code: "scrubber_engine_failure", disposition: "instance_poison", retryable: true });
      expect(String(first)).not.toContain("SYNTH_PRIVATE");
      const before = s.calls(); s.clear();
      for (const next of ops) expect(() => call(s.engine, next)).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned", disposition: "instance_poison" }));
      expect(() => s.engine.scrubJson(undefined)).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned" }));
      await expect(Promise.resolve().then(s.init)).rejects.toMatchObject({ code: "scrubber_instance_poisoned" });
      expect(s.calls()).toBe(before); expect(s.inits()).toBe(1);
    });

  it("the profile is the crate-owned manifest string, not a host-derived label", async () => {
    const s = await setup(path, JSON.stringify({ code: "invalid_json", disposition: "call_refusal" }), "scrubJson");
    s.clear();
    expect(s.engine.marker.profile).toBe("relayhistory-secrets-v10");
    expect(s.engine.scrubRecords(enc.encode('{}\n')).scrub.profile).toBe(manifest.scrubProfile);
  });
});
