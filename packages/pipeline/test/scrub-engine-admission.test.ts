/** Adapter admission against inert glue doubles, not a Rust/WASM qualification. */
import { Buffer } from "node:buffer";
import { runInNewContext } from "node:vm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  bindgenProfile,
  createCompiledBindgenEngine,
  resetBindgenScrubEngineForTests,
} from "../src/lib/scrub-engine/bindgen-engine.js";
import { ScrubberError } from "../src/lib/scrub-engine/scrubber-error.js";
import { MAX_RECORD_BYTES, MAX_STREAM_BYTES, MAX_TEXT_BYTES } from "../src/lib/scrub-engine/bindgen-admission.js";

const encoder = new TextEncoder();
const ModuleCtor = WebAssembly.Module as unknown as new (bytes: Uint8Array) => WebAssembly.Module;
const manifest = {
  schema: 3,
  ruleset: "relayhistory-scrub",
  rulesetVersion: 10,
  rulesetSourceSha256: "1".repeat(64),
  crateVersion: "0.0.0-test-double",
  abiVersion: 2,
  policy: "secrets",
  redactionToken: "[REDACTED]",
  scrubProfile: "relayhistory-secrets-v10",
};
const pin = {
  schema: 1,
  package: "@agentworkforce/relayscrubber",
  package_version: manifest.crateVersion,
  wasm_sha256: "2".repeat(64),
  glue_sha256: "3".repeat(64),
  loader_sha256: "4".repeat(64),
  paths: { wasm: "generated/test.wasm", glue: "generated/test.js", loader: "index.js" },
  scrub_profile: bindgenProfile(manifest),
  manifest,
};

function setup() {
  const calls: string[] = [];
  let received: Uint8Array | undefined;
  const engine = createCompiledBindgenEngine({
    readerMaxRuleset: 10,
    pin,
    module: new ModuleCtor(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0])),
    glue: {
      initSync() {},
      manifestJson: () => JSON.stringify(manifest),
      scrubText() { calls.push("text"); return "accepted"; },
      scrubLong() { calls.push("long"); return "accepted"; },
      scrubJson() { calls.push("json"); return JSON.stringify({ jsonText: "null", value: null, redactionCount: 0 }); },
      scrubRecords(bytes: Uint8Array) {
        calls.push("records"); received = bytes;
        return JSON.stringify({ text: "{}\n", recordCount: 1, redactionCount: 0 });
      },
    },
  });
  return { engine, calls, received: () => received };
}

function textOfBytes(bytes: number, unit: string): string {
  const width = encoder.encode(unit).byteLength;
  return unit.repeat(Math.floor(bytes / width)) + "x".repeat(bytes % width);
}

function jsonLineOfBytes(bytes: number, unit: string): string {
  return '{"x":"' + textOfBytes(bytes - 8, unit) + '"}';
}

function expectAdmissionRefusal(call: () => unknown, calls: string[], code = "scrub_input_too_large") {
  let returned = false;
  let error: unknown;
  try { call(); returned = true; } catch (e) { error = e; }
  expect(returned).toBe(false);
  expect(error).toBeInstanceOf(ScrubberError);
  expect(error).toMatchObject({ code, disposition: "call_refusal", retryable: false });
  expect(String(error)).not.toContain("SYNTH_PRIVATE");
  expect(calls).toEqual([]);
}

// Bun also runs this suite; restore the exact descriptors without Vitest-only
// stubGlobal helpers so allocation-failure cases run in both runtimes.
const originalGlobals = new Map<string, PropertyDescriptor | undefined>();
function stubGlobal(key: string, value: unknown) {
  originalGlobals.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
  Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
}
function restoreGlobals() {
  for (const [key, descriptor] of originalGlobals) {
    if (descriptor) Object.defineProperty(globalThis, key, descriptor);
    else Reflect.deleteProperty(globalThis, key);
  }
  originalGlobals.clear();
}
beforeEach(() => resetBindgenScrubEngineForTests());
afterEach(() => { restoreGlobals(); resetBindgenScrubEngineForTests(); });

describe("bindgen text admission before the glue", () => {
  for (const unit of ["a", "¢", "€", "😀"]) {
    for (const delta of [-1, 0, 1]) {
      it(`text and long: ${encoder.encode(unit).byteLength}-byte scalar at limit ${delta >= 0 ? "+" : ""}${delta}`, () => {
        const { engine, calls } = setup();
        const text = textOfBytes(MAX_TEXT_BYTES + delta, unit);
        if (delta > 0) {
          expectAdmissionRefusal(() => engine.scrubText(text), calls);
          expectAdmissionRefusal(() => engine.scrubLong(text), calls);
        } else {
          expect(engine.scrubText(text)).toBe("accepted");
          expect(engine.scrubLong(text)).toBe("accepted");
          expect(calls).toEqual(["text", "long"]);
        }
      });
    }
  }

  for (const unit of ["¢", "€", "😀"]) {
    for (const delta of [-1, 0, 1]) {
      it(`JSON: ${encoder.encode(unit).byteLength}-byte scalar at serialized limit ${delta >= 0 ? "+" : ""}${delta}`, () => {
        const { engine, calls } = setup();
        const value = textOfBytes(MAX_TEXT_BYTES + delta - 2, unit);
        if (delta > 0) expectAdmissionRefusal(() => engine.scrubJson(value), calls);
        else { expect(engine.scrubJson(value)).toBe(null); expect(calls).toEqual(["json"]); }
      });
    }
  }

  it("counts JSON escapes, not the source string's UTF-16 length", () => {
    const { engine, calls } = setup();
    expectAdmissionRefusal(() => engine.scrubJson({ note: "\0".repeat(Math.floor(MAX_TEXT_BYTES / 6)) }), calls);
  });

  it("refuses malformed UTF-16 without replacing it or calling glue", () => {
    const { engine, calls } = setup();
    for (const text of ["SYNTH_PRIVATE\uD800", "\uDC00SYNTH_PRIVATE", "\uD800x\uDC00"])
      expectAdmissionRefusal(() => engine.scrubText(text), calls, "scrub_input_not_well_formed");
    expect(engine.failed).toBe(false);
    expect(engine.scrubText("normal")).toBe("accepted");
  });

  it("does not bypass the glue for empty text or JSON scalars", () => {
    const { engine, calls } = setup();
    engine.scrubText(""); engine.scrubLong(""); engine.scrubJson(null); engine.scrubJson(true);
    expect(calls).toEqual(["text", "long", "json", "json"]);
  });
});

describe("bindgen record admission before the glue", () => {
  for (const delta of [-1, 0, 1]) {
    it(`a multibyte physical record at limit ${delta >= 0 ? "+" : ""}${delta}`, () => {
      const { engine, calls } = setup();
      const input = encoder.encode(jsonLineOfBytes(MAX_RECORD_BYTES + delta, "😀") + "\n");
      if (delta > 0) expectAdmissionRefusal(() => engine.scrubRecords(input), calls);
      else { engine.scrubRecords(input); expect(calls).toEqual(["records"]); }
    });
  }

  it("counts CR in CRLF records, matching the Rust physical-byte limit", () => {
    const { engine, calls } = setup();
    const input = encoder.encode(jsonLineOfBytes(MAX_RECORD_BYTES, "a") + "\r\n");
    expectAdmissionRefusal(() => engine.scrubRecords(input), calls);
    engine.scrubRecords(encoder.encode(jsonLineOfBytes(MAX_RECORD_BYTES - 1, "a") + "\r\n"));
    expect(calls).toEqual(["records"]);
  });

  it("bounds even blank physical records instead of trimming them first", () => {
    const { engine, calls } = setup();
    expectAdmissionRefusal(() => engine.scrubRecords(encoder.encode(" ".repeat(MAX_RECORD_BYTES + 1))), calls);
  });

  it("accepts stream limit-1 and limit, refuses limit+1 before any glue call", () => {
    const { engine, calls } = setup();
    const line = jsonLineOfBytes(MAX_RECORD_BYTES - 1, "¢") + "\n";
    const input = encoder.encode(line.repeat(4));
    expect(input.byteLength).toBe(MAX_STREAM_BYTES);
    engine.scrubRecords(input.subarray(0, input.byteLength - 1));
    engine.scrubRecords(input);
    expect(calls).toEqual(["records", "records"]);
    calls.length = 0;
    const large = new Uint8Array(MAX_STREAM_BYTES + 1); large.set(input); large[MAX_STREAM_BYTES] = 10;
    expectAdmissionRefusal(() => engine.scrubRecords(large), calls);
  }, 20_000);

  it("owns a real copy of a Buffer view, not Buffer.slice's shared view", () => {
    const { engine, calls, received } = setup();
    const source = Buffer.from('{"x":"normal"}\n');
    engine.scrubRecords(source);
    const copy = received()!;
    expect(copy).not.toBe(source);
    expect(copy.buffer).not.toBe(source.buffer);
    source.fill(0x58);
    expect(new TextDecoder().decode(copy)).toBe('{"x":"normal"}\n');
    expect(calls).toEqual(["records"]);
  });

  it("refuses shared buffers before copying or invoking", () => {
    const { engine, calls } = setup();
    expectAdmissionRefusal(() => engine.scrubRecords(new Uint8Array(new SharedArrayBuffer(16))), calls, "scrub_input_shared_buffer");
  });

  it("refuses a shared buffer from another realm and ignores a spoofed .buffer property", () => {
    const { engine, calls } = setup();
    const foreign = runInNewContext("new SharedArrayBuffer(16)") as SharedArrayBuffer;
    const source = new Uint8Array(foreign);
    Object.defineProperty(source, "buffer", { value: new ArrayBuffer(16) });
    expectAdmissionRefusal(() => engine.scrubRecords(source), calls, "scrub_input_shared_buffer");
  });

  it("refuses detached input with a content-free admission error", () => {
    const { engine, calls } = setup();
    const source = new Uint8Array(8); structuredClone(source.buffer, { transfer: [source.buffer] });
    expectAdmissionRefusal(() => engine.scrubRecords(source), calls, "scrub_input_not_well_formed");
  });

  it("does not consult a spoofed byteLength getter while sizing or copying", () => {
    const { engine, received } = setup();
    const source = encoder.encode("{}\n");
    let reads = 0;
    Object.defineProperty(source, "byteLength", { get() { reads++; return MAX_STREAM_BYTES + 1; } });
    engine.scrubRecords(source);
    expect(reads).toBe(0);
    expect(received()!.byteLength).toBe(3);
  });

  it("cannot hide an oversized view behind a spoofed byteLength property", () => {
    const { engine, calls } = setup();
    const source = new Uint8Array(MAX_STREAM_BYTES + 1);
    Object.defineProperty(source, "byteLength", { value: 0 });
    expectAdmissionRefusal(() => engine.scrubRecords(source), calls);
  });

  it("a copy allocation failure is retryable and never calls or poisons the engine", () => {
    const { engine, calls } = setup();
    const source = encoder.encode("{}\n");
    const Original = Uint8Array;
    stubGlobal("Uint8Array", new Proxy(Original, { construct() { throw new RangeError("SYNTH_PRIVATE"); } }));
    let error: unknown;
    try { engine.scrubRecords(source); } catch (e) { error = e; }
    restoreGlobals();
    expect(error).toMatchObject({ code: "scrub_admission_unavailable", retryable: true });
    expect(String(error)).not.toContain("SYNTH_PRIVATE");
    expect(calls).toEqual([]);
    expect(engine.failed).toBe(false);
    engine.scrubRecords(source);
    expect(calls).toEqual(["records"]);
  });

  for (const phase of ["construct", "decode"]) it(`decoder ${phase} allocation failures are not misclassified as invalid UTF-8`, () => {
    const { engine, calls } = setup();
    stubGlobal("TextDecoder", class {
      constructor() { if (phase === "construct") throw new RangeError("SYNTH_PRIVATE"); }
      decode() { throw new RangeError("SYNTH_PRIVATE"); }
    });
    let error: unknown;
    try { engine.scrubRecords(encoder.encode("{}\n")); } catch (e) { error = e; }
    restoreGlobals();
    expect(error).toMatchObject({ code: "scrub_admission_unavailable", retryable: true });
    expect(String(error)).not.toContain("SYNTH_PRIVATE");
    expect(calls).toEqual([]);
    expect(engine.failed).toBe(false);
  });

  for (const bad of [[0xc0, 0xaf], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80], [0xe2, 0x82]]) {
    it(`refuses invalid UTF-8 ${bad.map(b => b.toString(16)).join("-")} atomically before invoking`, () => {
      const { engine, calls } = setup();
      const good = encoder.encode('{"x":"SYNTH_PRIVATE"}\n');
      const input = new Uint8Array(good.length + bad.length); input.set(good); input.set(bad, good.length);
      expectAdmissionRefusal(() => engine.scrubRecords(input), calls, "chunk_not_utf8");
      expect(engine.failed).toBe(false);
    });
  }

  it("preserves valid multibyte sequences spanning the decoder chunk boundary", () => {
    const { engine, received } = setup();
    const input = encoder.encode('{"x":"' + "a".repeat(65_535 - 6) + '😀"}\n');
    engine.scrubRecords(input);
    expect(received()).toEqual(input);
    expect(received()).not.toBe(input);
  });
});
