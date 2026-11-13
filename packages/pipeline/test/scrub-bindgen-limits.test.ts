/**
 * Byte limits for the pinned relayscrubber 0.5.0-alpha.8 engine (bindgen-admission.ts):
 * 16 MiB UTF-8 per text and per LF-delimited record, 64 MiB per record stream. The JS
 * preflight refuses before the engine is called (zero crate calls, never poisons); the
 * Rust crate independently rechecks the same limits (driven directly, without the adapter).
 * Every limit is tested at limit-1 / limit / limit+1, with multibyte characters
 * straddling the boundary. Shared buffers are refused; caller views are copied.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MAX_RECORD_BYTES, MAX_STREAM_BYTES, MAX_TEXT_BYTES } from "../src/lib/scrub-engine/bindgen-admission.js";
import { taggedScrubberRefusal } from "../src/lib/scrub-engine/scrubber-error.js";
import {
  activeScrubEngine,
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  scrubEngineReady,
} from "../src/lib/scrub-engine/index.js";
import { TRIPLE, faultGlue, rawVendoredGlue, sealInput, tempStores, type RealGlue } from "./support/real-bindgen.js";

const MiB = 1024 * 1024;
const LONG = 180_000;
const enc = new TextEncoder();
const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET"] as const;
const stores = tempStores();
let saved: Record<string, string | undefined>;
let glue: ReturnType<typeof faultGlue>;

beforeEach(async () => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  Object.assign(process.env, TRIPLE);
  resetScrubEngineForTests();
  glue = faultGlue();
  await initScrubEngine({ env: TRIPLE, bindgen: { importGlue: glue.importGlue } });
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await stores.cleanup();
});

const engine = () => activeScrubEngine();
function thrown(call: () => unknown): Record<string, unknown> {
  try { call(); } catch (error) { return error as Record<string, unknown>; }
  throw new Error("expected a throw");
}
const failed = () => (engine() as unknown as { failed: boolean }).failed;
const tooLarge = { code: "scrub_input_too_large", disposition: "call_refusal" };
/** `n` UTF-8 bytes: ASCII filler, then `ch` as the last code point. */
const textOf = (n: number, ch = "") => "a".repeat(n - enc.encode(ch).byteLength) + ch;
/** One JSON string record of exactly `n` bytes (excluding LF), ending in `ch` inside the string. */
const recordOf = (n: number, ch = "") => `"${"x".repeat(n - 2 - enc.encode(ch).byteLength)}${ch}"`;

describe("constants", () => {
  it("are 16 MiB per text/record and 64 MiB per stream", () => {
    expect(MAX_TEXT_BYTES).toBe(16 * MiB);
    expect(MAX_RECORD_BYTES).toBe(16 * MiB);
    expect(MAX_STREAM_BYTES).toBe(64 * MiB);
  });
});

describe("text (scrubText / scrubLong) through the real engine", () => {
  // [description, text, accepted]
  const cases: [string, () => string, boolean][] = [
    ["limit-1 ASCII", () => textOf(MAX_TEXT_BYTES - 1), true],
    ["limit ASCII", () => textOf(MAX_TEXT_BYTES), true],
    ["limit+1 ASCII", () => textOf(MAX_TEXT_BYTES + 1), false],
    ["limit-1 ending in 3-byte U+20AC", () => textOf(MAX_TEXT_BYTES - 1, "€"), true],
    ["limit exactly ending in 3-byte U+20AC", () => textOf(MAX_TEXT_BYTES, "€"), true],
    ["U+20AC straddling: limit+1", () => "a".repeat(MAX_TEXT_BYTES - 2) + "€", false],
    ["U+20AC straddling: limit+2", () => "a".repeat(MAX_TEXT_BYTES - 1) + "€", false],
    ["4-byte U+1F600 straddling: limit+1", () => "a".repeat(MAX_TEXT_BYTES - 3) + "\u{1F600}", false],
    ["limit exactly ending in 4-byte U+1F600", () => textOf(MAX_TEXT_BYTES, "\u{1F600}"), true],
    ["2-byte U+00E9 straddling: limit+1", () => "a".repeat(MAX_TEXT_BYTES - 1) + "é", false],
  ];
  for (const [name, make, accepted] of cases)
    it(`${name}: ${accepted ? "scrubbed by the crate" : "scrub_input_too_large before the crate, not poisoned"}`, () => {
      const text = make();
      for (const fn of ["scrubText", "scrubLong"] as const) {
        const before = glue.calls.scrubTextChecked;
        if (accepted) {
          expect(engine()[fn](text).length).toBe(text.length);
          expect(glue.calls.scrubTextChecked).toBe(before + 1);
        } else {
          expect(thrown(() => engine()[fn](text))).toMatchObject(tooLarge);
          expect(glue.calls.scrubTextChecked).toBe(before);
        }
      }
      expect(failed()).toBe(false);
      expect(scrubEngineReady()).toBe(true);
    }, LONG);

  it("scrubJson: JSON text above the limit is refused before the crate", () => {
    expect(thrown(() => engine().scrubJson("a".repeat(MAX_TEXT_BYTES - 1)))).toMatchObject(tooLarge);
    expect(glue.calls.scrubJson).toBe(0);
    expect(engine().scrubJson("a".repeat(MAX_TEXT_BYTES - 2))).toHaveLength(MAX_TEXT_BYTES - 2);
    expect(glue.calls.scrubJson).toBe(1);
  }, LONG);
});

describe("records (scrubRecords and the routed sealer) through the real engine", () => {
  const cases: [string, () => string, boolean][] = [
    ["record limit-1", () => recordOf(MAX_RECORD_BYTES - 1), true],
    ["record limit", () => recordOf(MAX_RECORD_BYTES), true],
    ["record limit+1", () => recordOf(MAX_RECORD_BYTES + 1), false],
    ["record limit ending in U+20AC", () => recordOf(MAX_RECORD_BYTES, "€"), true],
    ["U+20AC straddling the record limit (limit+1)", () => `"${"x".repeat(MAX_RECORD_BYTES - 4)}€"`, false],
    ["U+1F600 straddling the record limit (limit+2)", () => `"${"x".repeat(MAX_RECORD_BYTES - 4)}\u{1F600}"`, false],
    ["record limit plus a CR (CR counts, LF does not)", () => `${recordOf(MAX_RECORD_BYTES)}\r`, false],
  ];
  for (const [name, make, accepted] of cases)
    it(`${name}: ${accepted ? "sealed" : "scrub_input_too_large before the crate, not poisoned"}`, async () => {
      const source = enc.encode(`${make()}\n`);
      if (accepted) {
        expect(engine().scrubRecords(source).records).toBe(1);
        expect(glue.calls.scrubRecords).toBe(1);
        const sealed = await routedRawChunkSealer()!(await stores.store(), sealInput(source));
        expect(sealed.entry.records).toBe(1);
      } else {
        expect(thrown(() => engine().scrubRecords(source))).toMatchObject(tooLarge);
        await expect(routedRawChunkSealer()!(await stores.store(), sealInput(source))).rejects.toMatchObject(tooLarge);
        expect(glue.calls.scrubRecords).toBe(0);
      }
      expect(failed()).toBe(false);
      expect(scrubEngineReady()).toBe(true);
    }, LONG);

  it("two records each at the limit in one stream: both admitted (the limit is per record)", () => {
    const source = enc.encode(`${recordOf(MAX_RECORD_BYTES)}\n${recordOf(MAX_RECORD_BYTES)}\n`);
    expect(engine().scrubRecords(source).records).toBe(2);
  }, LONG);
});

describe("aggregate stream (64 MiB)", () => {
  /** A stream of exactly `n` bytes: LF-separated JSON string records below the record limit. */
  function streamOf(n: number, last = ""): Uint8Array {
    const out = new Uint8Array(n).fill(0x78);
    const lastBytes = enc.encode(last);
    const record = 8 * MiB;
    for (let at = 0; at < n; at += record) {
      out[at] = 0x22; // "
      const end = Math.min(at + record, n) - 1;
      out[end - 1] = 0x22; out[end] = 0x0a;
    }
    // Put `last` just before the final `"\n`.
    out.set(lastBytes, n - 2 - lastBytes.byteLength);
    return out;
  }
  const cases: [string, () => Uint8Array, boolean][] = [
    ["stream limit-1", () => streamOf(MAX_STREAM_BYTES - 1), true],
    ["stream limit", () => streamOf(MAX_STREAM_BYTES), true],
    ["stream limit+1", () => streamOf(MAX_STREAM_BYTES + 1), false],
    ["stream limit ending in U+20AC", () => streamOf(MAX_STREAM_BYTES, "€"), true],
    ["U+20AC straddling the stream limit (limit+2)", () => {
      const s = streamOf(MAX_STREAM_BYTES + 2, "€");
      return s;
    }, false],
  ];
  for (const [name, make, accepted] of cases)
    it(`${name}: ${accepted ? "scrubbed" : "scrub_input_too_large before any copy or crate call"}`, () => {
      const source = make();
      if (accepted) {
        expect(engine().scrubRecords(source).records).toBe(8);
        expect(glue.calls.scrubRecords).toBe(1);
      } else {
        expect(thrown(() => engine().scrubRecords(source))).toMatchObject(tooLarge);
        expect(glue.calls.scrubRecords).toBe(0);
      }
      expect(failed()).toBe(false);
    }, LONG);
});

describe("the Rust crate rechecks the same limits (vendored glue driven directly, no JS preflight)", () => {
  let raw: RealGlue;
  beforeEach(async () => { raw = await rawVendoredGlue(); });
  const rustRefusal = (call: () => unknown) => {
    try { call(); } catch (error) { return JSON.parse(String(error)) as Record<string, unknown>; }
    throw new Error("expected the crate to refuse");
  };

  it("text: limit and limit-1 accepted; limit+1 (ASCII and U+00E9 straddling) text_too_large", () => {
    expect(raw.scrubTextChecked(textOf(MAX_TEXT_BYTES - 1))).toHaveLength(MAX_TEXT_BYTES - 1);
    expect(raw.scrubTextChecked(textOf(MAX_TEXT_BYTES, "€")).endsWith("€")).toBe(true);
    expect(rustRefusal(() => raw.scrubTextChecked(textOf(MAX_TEXT_BYTES + 1)))).toMatchObject({ code: "text_too_large", disposition: "call_refusal" });
    expect(rustRefusal(() => raw.scrubTextChecked("a".repeat(MAX_TEXT_BYTES - 1) + "é"))).toMatchObject({ code: "text_too_large" });
  }, LONG);

  it("record: limit accepted; limit+1 (ASCII and U+20AC straddling) record_too_large with its record number", () => {
    expect(JSON.parse(raw.scrubRecords(enc.encode(`${recordOf(MAX_RECORD_BYTES)}\n`))).recordCount).toBe(1);
    expect(rustRefusal(() => raw.scrubRecords(enc.encode(`${recordOf(MAX_RECORD_BYTES + 1)}\n`)))).toStrictEqual({ code: "record_too_large", disposition: "call_refusal", record: 1 });
    expect(rustRefusal(() => raw.scrubRecords(enc.encode(`{}\n"${"x".repeat(MAX_RECORD_BYTES - 4)}€"\n`)))).toStrictEqual({ code: "record_too_large", disposition: "call_refusal", record: 2 });
  }, LONG);

  it("stream: limit accepted; limit+1 stream_too_large (alpha.8 tags it record 0)", () => {
    expect(JSON.parse(raw.scrubRecords(new Uint8Array(MAX_STREAM_BYTES).fill(0x0a))).recordCount).toBe(0);
    const tag = (() => { try { raw.scrubRecords(new Uint8Array(MAX_STREAM_BYTES + 1).fill(0x0a)); } catch (e) { return String(e); } return ""; })();
    expect(JSON.parse(tag)).toStrictEqual({ code: "stream_too_large", disposition: "call_refusal", record: 0 });
    // The adapter accepts only record >= 1, so this tag is NOT recognised: if the crate ever
    // emitted it through the adapter, the instance would poison (fail closed). The JS
    // preflight refuses above 64 MiB first, so the adapter never forwards such a stream.
    expect(taggedScrubberRefusal(tag)).toBeUndefined();
  }, LONG);
});

describe("shared and caller-owned buffers", () => {
  it("a SharedArrayBuffer-backed view is refused (scrub_input_shared_buffer), zero crate calls, not poisoned", async () => {
    const shared = new Uint8Array(new SharedArrayBuffer(16));
    shared.set(enc.encode('{"a":1}\n'));
    expect(thrown(() => engine().scrubRecords(shared))).toMatchObject({ code: "scrub_input_shared_buffer", disposition: "call_refusal" });
    await expect(routedRawChunkSealer()!(await stores.store(), sealInput(shared))).rejects.toMatchObject({ code: "scrub_input_shared_buffer" });
    expect(glue.calls.scrubRecords).toBe(0);
    expect(failed()).toBe(false);
  });

  it("a subarray view of a larger buffer: only the view's bytes are scrubbed", () => {
    const backing = enc.encode('XXXX{"a":"AKIAIOSFODNN7EXAMPLE"}\nYYYY');
    const view = backing.subarray(4, backing.length - 4);
    expect(new TextDecoder().decode(engine().scrubRecords(view).scrubbed)).toBe('{"a":"[REDACTED]"}\n');
  });

  it("the sealer snapshots before its first await: mutating the caller's buffer afterwards changes nothing", async () => {
    const source = enc.encode('{"a":"AKIAIOSFODNN7EXAMPLE"}\n');
    const original = source.slice();
    const store = await stores.store();
    const pending = routedRawChunkSealer()!(store, sealInput(source));
    source.fill(0x20); // would be blank (chunk_empty) if the sealer read it late
    const sealed = await pending;
    expect(sealed.entry.records).toBe(1);
    expect(sealed.entry.bytes).toBe(original.byteLength);
    expect(glue.calls.scrubRecords).toBe(1);
  });
});
