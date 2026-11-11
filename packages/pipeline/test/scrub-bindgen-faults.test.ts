/**
 * Fault matrix for the pinned relayscrubber 0.5.0-alpha.4 engine, with faults injected
 * through a wrapper around the REAL verified glue (never by editing vendored bytes):
 *
 * - The crate's explicit JSON-string call-refusal tag refuses ONE call; the instance
 *   stays healthy and the next call reaches the real crate.
 * - WebAssembly.RuntimeError, RangeError (stack/OOM), and ANY unrecognised throw or
 *   malformed return poison the instance: sticky, engine unavailable, readiness false,
 *   routed and already-bound sealers refuse. No Error-vs-trap heuristics: an Error whose
 *   message is a valid tag still poisons.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { SCRUBBER_CALL_REFUSAL_CODES, ScrubberError } from "../src/lib/scrub-engine/scrubber-error.js";
import {
  SCRUB_UNAVAILABLE_LOG_TEXT,
  activeScrubEngine,
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  scrubEngineReady,
  scrubText,
  scrubTextForLog,
  type ScrubEngine,
} from "../src/lib/scrub-engine/index.js";
import { TRIPLE, faultGlue, sealInput, tempStores, type GlueOverrides } from "./support/real-bindgen.js";

const enc = new TextEncoder();
const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET"] as const;
const stores = tempStores();
let saved: Record<string, string | undefined>;
let glue: ReturnType<typeof faultGlue>;
let engine: ScrubEngine & { readonly failed: boolean };

beforeEach(async () => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  Object.assign(process.env, TRIPLE);
  resetScrubEngineForTests();
  glue = faultGlue();
  engine = (await initScrubEngine({ env: TRIPLE, bindgen: { importGlue: glue.importGlue } })) as typeof engine;
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await stores.cleanup();
});

type Op = "scrubTextChecked" | "scrubJson" | "scrubRecords";
const OPS: Op[] = ["scrubTextChecked", "scrubJson", "scrubRecords"];
const CALL: Record<Op, () => unknown> = {
  scrubTextChecked: () => engine.scrubText("token=ghp_abcdefghijklmnopqrstuvwxyz0123456789"),
  scrubJson: () => engine.scrubJson({ a: "AKIAIOSFODNN7EXAMPLE" }),
  scrubRecords: () => engine.scrubRecords(enc.encode('{"a":"AKIAIOSFODNN7EXAMPLE"}\n')),
};
function thrown(call: () => unknown): unknown {
  try { call(); } catch (error) { return error; }
  throw new Error("expected a throw");
}
/** Arm a one-shot fault on `op`: the next call throws `value` (or returns it with `returns`). */
function arm(op: Op, value: () => unknown, returns = false) {
  const o = glue.overrides as GlueOverrides & Record<string, unknown>;
  o[op] = () => {
    delete o[op];
    if (returns) return value();
    throw value();
  };
}

async function expectPoisoned() {
  expect(engine.failed).toBe(true);
  expect(scrubEngineReady()).toBe(false);
  expect(() => activeScrubEngine()).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned", disposition: "instance_poison" }));
  expect(() => routedRawChunkSealer()).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned" }));
  expect(() => scrubText("x")).toThrow(ScrubberError);
  expect(scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
  const before = { ...glue.calls };
  for (const op of OPS) expect(thrown(CALL[op])).toMatchObject({ code: "scrubber_instance_poisoned" });
  expect(glue.calls).toStrictEqual(before); // nothing reaches the crate any more
  // A re-init in the same process does not revive it (sticky).
  await expect(initScrubEngine({ env: TRIPLE, bindgen: { importGlue: glue.importGlue } })).rejects.toMatchObject({ code: "scrubber_instance_poisoned" });
  expect(scrubEngineReady()).toBe(false);
}

describe("tagged call refusals refuse one call only", () => {
  for (const op of OPS)
    for (const code of SCRUBBER_CALL_REFUSAL_CODES)
      it(`${op} throwing the crate tag ${code}: call_refusal, then the next call reaches the real crate`, () => {
        arm(op, () => JSON.stringify({ code, disposition: "call_refusal", ...(op === "scrubRecords" ? { record: 3 } : {}) }));
        const error = thrown(CALL[op]) as ScrubberError;
        expect(error).toBeInstanceOf(ScrubberError);
        expect(error).toMatchObject({ code, disposition: "call_refusal", retryable: false });
        if (op === "scrubRecords") expect(error.record).toBe(3);
        expect(engine.failed).toBe(false);
        expect(scrubEngineReady()).toBe(true);
        const before = glue.calls[op];
        CALL[op]();
        expect(glue.calls[op]).toBe(before + 1);
      });

  it("a real crate refusal (non-JSON record) through the routed sealer: one call refused, nothing stored, still healthy", async () => {
    const store = await stores.store();
    await expect(routedRawChunkSealer()!(store, sealInput(enc.encode("not json\n")))).rejects.toMatchObject({ code: "invalid_json", disposition: "call_refusal" });
    expect(store.puts).toEqual([]);
    expect(engine.failed).toBe(false);
    expect((await routedRawChunkSealer()!(store, sealInput(enc.encode('{"a":1}\n')))).created).toBe(true);
  });
});

const POISONERS: [string, () => unknown, boolean?][] = [
  ["WebAssembly.RuntimeError (trap)", () => new WebAssembly.RuntimeError("unreachable executed")],
  ["RangeError (stack overflow / OOM)", () => new RangeError("Maximum call stack size exceeded")],
  ["RangeError (allocation)", () => new RangeError("WebAssembly.Memory.grow(): Maximum memory size exceeded")],
  ["plain Error", () => new Error("boom")],
  ["TypeError", () => new TypeError("x is not a function")],
  ["Error whose message is a valid tag (no Error-vs-tag heuristics)", () => new Error(JSON.stringify({ code: "invalid_json", disposition: "call_refusal" }))],
  ["tag as an object, not a string", () => ({ code: "invalid_json", disposition: "call_refusal" })],
  ["untagged string", () => "something went wrong"],
  ["tag with an unknown code", () => JSON.stringify({ code: "made_up", disposition: "call_refusal" })],
  ["tag with disposition instance_poison", () => JSON.stringify({ code: "invalid_json", disposition: "instance_poison" })],
  ["tag with record 0", () => JSON.stringify({ code: "stream_too_large", disposition: "call_refusal", record: 0 })],
  ["tag with an extra key", () => JSON.stringify({ code: "invalid_json", disposition: "call_refusal", detail: "secret" })],
  ["oversized tag string", () => JSON.stringify({ code: "invalid_json", disposition: "call_refusal" }) + " ".repeat(2000)],
  ["undefined", () => undefined],
  ["null", () => null],
  ["a number", () => 42],
  ["a Symbol", () => Symbol("x")],
  ["returned a non-string", () => 7, true],
  ["returned a non-JSON string", () => "not json", true],
  ["returned a JSON array", () => "[]", true],
];
const RECORD_RETURN_POISONERS: [string, () => unknown][] = [
  ["recordCount negative", () => JSON.stringify({ text: "x\n", recordCount: -1 })],
  ["recordCount not an integer", () => JSON.stringify({ text: "x\n", recordCount: 1.5 })],
  ["text missing", () => JSON.stringify({ recordCount: 1 })],
];

describe("everything else poisons the instance (sticky)", () => {
  for (const op of OPS)
    for (const [name, value, returns] of POISONERS.filter(
      // scrubTextChecked returns plain scrubbed text, so any string is a valid return there.
      ([n]) => !(op === "scrubTextChecked" && (n === "returned a non-JSON string" || n === "returned a JSON array")),
    ))
      it(`${op}: ${name}`, async () => {
        arm(op, value, returns);
        expect(thrown(CALL[op])).toMatchObject({ code: "scrubber_engine_failure", disposition: "instance_poison", retryable: true });
        await expectPoisoned();
      });

  for (const [name, value] of RECORD_RETURN_POISONERS)
    it(`scrubRecords returned ${name}`, async () => {
      arm("scrubRecords", value, true);
      expect(thrown(CALL.scrubRecords)).toMatchObject({ code: "scrubber_engine_failure" });
      await expectPoisoned();
    });

  it("scrubJson returned an envelope without jsonText", async () => {
    arm("scrubJson", () => JSON.stringify({ value: {} }), true);
    expect(thrown(CALL.scrubJson)).toMatchObject({ code: "scrubber_engine_failure" });
    await expectPoisoned();
  });
});

describe("faults through the routed sealer", () => {
  for (const [name, value] of [
    ["WebAssembly.RuntimeError", () => new WebAssembly.RuntimeError("unreachable")],
    ["RangeError", () => new RangeError("out of memory")],
    ["unknown throw", () => ({ weird: true })],
  ] as const)
    it(`${name} during seal: scrubber_instance_poisoned, nothing stored, every sealer refuses afterwards`, async () => {
      const bound = routedRawChunkSealer()!;
      const store = await stores.store();
      arm("scrubRecords", value);
      await expect(bound(store, sealInput(enc.encode('{"a":1}\n')))).rejects.toMatchObject({ disposition: "instance_poison" });
      expect(store.puts).toEqual([]);
      // The sealer obtained before the fault refuses too, without reaching the crate.
      const before = glue.calls.scrubRecords;
      await expect(bound(store, sealInput(enc.encode('{"b":2}\n')))).rejects.toMatchObject({ code: "scrubber_instance_poisoned" });
      expect(glue.calls.scrubRecords).toBe(before);
      expect(store.puts).toEqual([]);
      await expectPoisoned();
    });

  it("a sealer result with zero records for content is corrupt output: poisons", async () => {
    const bound = routedRawChunkSealer()!;
    arm("scrubRecords", () => JSON.stringify({ text: "", recordCount: 0, redactionCount: 0 }), true);
    await expect(bound(await stores.store(), sealInput(enc.encode('{"a":1}\n')))).rejects.toMatchObject({ code: "scrubber_instance_poisoned" });
    await expectPoisoned();
  });
});
