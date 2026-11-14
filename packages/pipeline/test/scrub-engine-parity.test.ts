/**
 * Scrub engine seam (docs/scrub-engine.md): parity and fail-closed behaviour.
 *
 * - Parity: one harness, parameterised over a `ScrubEngine`, runs the shared secret
 *   corpus and the frozen ruleset fixtures (as long text and as records) and compares
 *   every output byte for byte with the TypeScript engine called directly (scrub.ts /
 *   raw-chunks.ts, the frozen oracle). It runs against the TypeScript engine through
 *   the seam and against a WasmScrubEngine bound to a fake "mirror" module whose ABI
 *   exports wrap the TypeScript functions, so the ABI binding (memory, alloc, result
 *   buffer, status codes) is exercised end to end.
 * - Fault injection: every way wasm init can fail makes init throw and every later
 *   scrub call throw ScrubEngineUnavailable; nothing falls back to TypeScript and
 *   nothing passes through unscrubbed.
 * - Selection: unset/"" is TypeScript (identical to direct calls), an unknown
 *   SCRUB_ENGINE throws, and a second, different selection throws.
 *
 * No secret-shaped value is stored here: corpus placeholders are filled at run time.
 */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  SCRUB_RULESET,
  SCRUB_RULESET_VERSION,
  scrubLong as tsScrubLong,
  scrubRecords as tsScrubRecords,
} from "../src/lib/pipeline/raw-chunks.js";
import { PipelineError } from "../src/lib/pipeline/errors.js";
import { isSafeIdentifier } from "../src/lib/pipeline/trace.js";
import { scrubJson as tsScrubJson, scrubNumber as tsScrubNumber, scrubText as tsScrubText } from "../src/lib/scrub.js";
import * as seam from "../src/lib/scrub-engine/index.js";
import {
  SCRUB_UNAVAILABLE_LOG_TEXT,
  ScrubEngineConfigError,
  ScrubEngineUnavailable,
  createWasmScrubEngine,
  initScrubEngine,
  parseScrubWasmPin,
  resetScrubEngineForTests,
  scrubEngineReady,
  selectScrubEngine,
  tsScrubEngine,
  type ScrubEngine,
  type ScrubEngineFailure,
  type ScrubWasmArtifact,
} from "../src/lib/scrub-engine/index.js";
import { declareScrubTsCapability } from "../src/lib/scrub-engine/ts-capability.js";
import { ABI_STATUS, REQUIRED_EXPORTS, WASM_ABI_VERSION } from "../src/lib/scrub-engine/wasm-abi.js";
import pinFile from "../scrub-wasm-pin.json";

// ---------------------------------------------------------------- inputs

interface Generator {
  prefix?: string;
  suffix?: string;
  parts: [string, number | string][];
}
interface Case {
  id: string;
  input: string;
  record?: boolean;
}
const corpus = JSON.parse(
  readFileSync(fileURLToPath(new URL("./fixtures/scrub-secret-corpus.json", import.meta.url).href), "utf8").replace(/\r\n/g, "\n"),
) as { generators: Record<string, Generator>; cases: Case[] };

const CHARSETS: Record<string, string> = {
  alnum: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
  upper_alnum: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
  letters: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
  digits: "0123456789",
  hex: "0123456789abcdef",
  urlsafe: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-",
  b64: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
};
function randomFrom(charset: string, n: number): string {
  let out = "";
  while (out.length < n)
    for (const b of randomBytes(n * 2)) {
      if (b < 256 - (256 % charset.length)) out += charset[b % charset.length];
      if (out.length === n) break;
    }
  return out;
}
function generate(g: Generator): string {
  let body = "";
  for (const [kind, n] of g.parts) {
    if (kind === "lit") body += String(n);
    else if (kind === "uuid") body += randomUUID();
    else body += randomFrom(CHARSETS[kind] as string, Number(n));
  }
  return `${g.prefix ?? ""}${body}${g.suffix ?? ""}`;
}
/** One instantiation per case, shared by every engine so they see the same bytes. */
const cases = corpus.cases.map((c) => {
  const values = new Map<string, string>();
  const input = c.input.replace(/\{\{([a-z0-9_]+)\}\}/g, (_m, name: string) => {
    if (!values.has(name)) values.set(name, generate(corpus.generators[name] as Generator));
    const value = values.get(name) as string;
    return c.record ? JSON.stringify(value).slice(1, -1) : value;
  });
  return { id: c.id, input, record: c.record === true };
});

const FIXTURES = [
  "scrub-ruleset-3.ts",
  "scrub-ruleset-4.ts",
  "scrub-ruleset-5.ts",
  "scrub-ruleset-6.ts",
  "scrub-ruleset-6-prefix-f6a97053.ts",
  "scrub-ruleset-7.ts",
].map((name) => ({
  name,
  text: readFileSync(fileURLToPath(new URL(`./fixtures/${name}`, import.meta.url).href), "utf8"),
}));

const enc = new TextEncoder();
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");

// ---------------------------------------------------------------- fake mirror module

const MIRROR_BYTES = new TextEncoder().encode("\0asm fake relayscrubber mirror module (test only)");
const MIRROR_SHA = createHash("sha256").update(MIRROR_BYTES).digest("hex");
const MIRROR_PROFILE = "relayscrubber-test-mirror/v1";

interface MirrorOptions {
  abiVersion?: number;
  rulesetVersion?: number;
  profile?: string;
  omit?: string;
  /** Throw (a trap) from this operation. */
  trapOn?: string;
}

/**
 * A fake module with the declared ABI. Its operations call the TypeScript engine, so its
 * outputs are the oracle's; its memory is a plain ArrayBuffer that is replaced (grown) on
 * allocation, so a binding that keeps a stale view of memory fails here.
 */
function mirrorExports(o: MirrorOptions = {}): Record<string, unknown> {
  const memory = { buffer: new ArrayBuffer(1024) };
  let top = 8;
  let result: { ptr: number; len: number; records: number } | null = null;
  const live = new Map<number, number>();
  const alloc = (len: number) => {
    const ptr = top;
    top += Math.max(len, 1);
    if (top > memory.buffer.byteLength) {
      const grown = new ArrayBuffer(Math.max(top * 2, memory.buffer.byteLength * 2));
      new Uint8Array(grown).set(new Uint8Array(memory.buffer));
      memory.buffer = grown;
    }
    live.set(ptr, len);
    return ptr;
  };
  const free = (ptr: number) => {
    live.delete(ptr);
    if (live.size === 0) top = 8;
  };
  const read = (ptr: number, len: number) => new Uint8Array(memory.buffer, ptr, len).slice();
  const setResult = (bytes: Uint8Array, records = 0) => {
    const ptr = alloc(bytes.byteLength);
    new Uint8Array(memory.buffer, ptr, bytes.byteLength).set(bytes);
    result = { ptr, len: bytes.byteLength, records };
    return ABI_STATUS.OK;
  };
  const strict = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const textOp = (name: string, fn: (s: string) => string) => (ptr: number, len: number) => {
    if (o.trapOn === name) throw new Error("unreachable");
    let text: string;
    try {
      text = strict.decode(read(ptr, len));
    } catch {
      return ABI_STATUS.INVALID_UTF8;
    }
    return setResult(enc.encode(fn(text)));
  };
  const exports: Record<string, unknown> = {
    memory,
    scrub_abi_version: () => o.abiVersion ?? WASM_ABI_VERSION,
    scrub_ruleset_version: () => o.rulesetVersion ?? SCRUB_RULESET_VERSION,
    scrub_profile: () => setResult(enc.encode(o.profile ?? MIRROR_PROFILE)),
    scrub_alloc: alloc,
    scrub_free: (ptr: number) => free(ptr),
    scrub_text: textOp("scrub_text", tsScrubText),
    scrub_long: textOp("scrub_long", tsScrubLong),
    scrub_json: (ptr: number, len: number) => {
      if (o.trapOn === "scrub_json") throw new Error("unreachable");
      let value: unknown;
      try {
        value = JSON.parse(strict.decode(read(ptr, len)));
      } catch {
        return ABI_STATUS.INVALID_JSON;
      }
      return setResult(enc.encode(JSON.stringify(tsScrubJson(value))));
    },
    scrub_records: (ptr: number, len: number) => {
      if (o.trapOn === "scrub_records") throw new Error("unreachable");
      try {
        const { scrubbed, records } = tsScrubRecords(read(ptr, len));
        return setResult(scrubbed, records);
      } catch (error) {
        if (error instanceof PipelineError && error.code === "chunk_not_utf8") return ABI_STATUS.INVALID_UTF8;
        throw error;
      }
    },
    scrub_result_ptr: () => result?.ptr ?? 0,
    scrub_result_len: () => result?.len ?? 0,
    scrub_result_records: () => result?.records ?? 0,
    scrub_result_free: () => {
      if (result) free(result.ptr);
      result = null;
    },
  };
  if (o.omit) delete exports[o.omit];
  return exports;
}

const mirrorPin = (over: Record<string, unknown> = {}) => ({
  schema: 1,
  crate: "relayscrubber",
  crate_version: "0.0.0-test-mirror",
  ruleset_version: SCRUB_RULESET_VERSION,
  scrub_profile: MIRROR_PROFILE,
  wasm_sha256: MIRROR_SHA,
  abi_version: WASM_ABI_VERSION,
  ...over,
});

const mirrorInit = (o: MirrorOptions = {}, extra: Partial<Parameters<typeof initScrubEngine>[0]> = {}) => ({
  env: { SCRUB_ENGINE: "wasm" },
  pin: mirrorPin(),
  loadArtifact: async (): Promise<ScrubWasmArtifact> => ({ bytes: MIRROR_BYTES }),
  instantiate: async () => mirrorExports(o),
  ...extra,
});

async function mirrorEngine(o: MirrorOptions = {}): Promise<ScrubEngine> {
  return createWasmScrubEngine({ pin: mirrorPin(), loadArtifact: async () => ({ bytes: MIRROR_BYTES }), instantiate: async () => mirrorExports(o) });
}

// ---------------------------------------------------------------- parity harness

// This file compares the TypeScript engine as a hosted entry runs it: each case starts in a
// granted process (the reset clears grants; "test" stands in for a hosted handler's grant).
// The ungranted default (wasm-bindgen) is asserted in "selection" below and in
// scrub-engine-required.test.ts / src/session-ingest/__tests__/scrub-default-flip.test.ts.
const grantTs = () => declareScrubTsCapability("test");
beforeEach(() => {
  resetScrubEngineForTests();
  grantTs();
});
afterEach(() => resetScrubEngineForTests());

/** A `ScrubEngine` view of the module-level seam functions, so the seam itself is what is compared. */
const seamView = (kind: "ts" | "wasm"): ScrubEngine => ({
  kind,
  get rulesetVersion() {
    return seam.activeScrubEngine().rulesetVersion;
  },
  get scrubProfile() {
    return seam.activeScrubEngine().scrubProfile;
  },
  get marker() {
    return seam.activeScrubMarker();
  },
  scrubText: seam.scrubText,
  scrubLong: seam.scrubLong,
  scrubJson: seam.scrubJson,
  scrubRecords: seam.scrubRecords,
});

const ENGINES: { name: string; setup: () => Promise<ScrubEngine>; profile: string }[] = [
  {
    name: "ts engine through the seam (SCRUB_ENGINE unset)",
    setup: async () => {
      await initScrubEngine({ env: {} });
      return seamView("ts");
    },
    profile: seam.TS_SCRUB_PROFILE,
  },
  { name: "tsScrubEngine object", setup: async () => tsScrubEngine, profile: seam.TS_SCRUB_PROFILE },
  { name: "WasmScrubEngine over the mirror ABI", setup: () => mirrorEngine(), profile: MIRROR_PROFILE },
  {
    name: "mirror WasmScrubEngine through the seam (SCRUB_ENGINE=wasm)",
    setup: async () => {
      await initScrubEngine(mirrorInit());
      return seamView("wasm");
    },
    profile: MIRROR_PROFILE,
  },
];

function recordsOutcome(engine: ScrubEngine, bytes: Uint8Array) {
  try {
    const r = engine.scrubRecords(bytes);
    return { hex: hex(r.scrubbed), records: r.records };
  } catch (error) {
    return { error: error instanceof PipelineError ? `${error.code}/${error.retryable}` : String(error) };
  }
}
function oracleRecords(bytes: Uint8Array) {
  try {
    const r = tsScrubRecords(bytes);
    return { hex: hex(r.scrubbed), records: r.records };
  } catch (error) {
    return { error: error instanceof PipelineError ? `${error.code}/${error.retryable}` : String(error) };
  }
}

for (const { name, setup, profile } of ENGINES) {
  describe(`parity: ${name}`, () => {
    let engine: ScrubEngine;
    beforeEach(async () => {
      engine = await setup();
    });

    it("reports the ruleset version and profile of the engine instance", () => {
      expect(engine.rulesetVersion).toBe(SCRUB_RULESET_VERSION);
      expect(engine.scrubProfile).toBe(profile);
      expect(engine.marker).toEqual({ ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION, profile });
      const r = engine.scrubRecords(enc.encode('{"a":1}\n'));
      expect(r.scrub).toEqual(engine.marker);
    });

    it(`corpus (${cases.length} cases): text, long, json and records are byte-identical to scrub.ts`, () => {
      expect(cases.length).toBeGreaterThan(150);
      for (const c of cases) {
        expect(engine.scrubText(c.input), `${c.id} text`).toBe(tsScrubText(c.input));
        expect(engine.scrubLong(c.input), `${c.id} long`).toBe(tsScrubLong(c.input));
        expect(engine.scrubJson(c.input), `${c.id} json string`).toBe(tsScrubJson(c.input));
        const line = enc.encode(`${c.input}\n`);
        expect(recordsOutcome(engine, line), `${c.id} records`).toEqual(oracleRecords(line));
        if (c.record) {
          const parsed = JSON.parse(c.input) as unknown;
          expect(JSON.stringify(engine.scrubJson(parsed)), `${c.id} json`).toBe(JSON.stringify(tsScrubJson(parsed)));
        }
      }
      // All record cases as one multi-record chunk, with and without a trailing partial line.
      const chunk = cases.filter((c) => c.record).map((c) => c.input).join("\n");
      for (const bytes of [enc.encode(`${chunk}\n`), enc.encode(chunk)])
        expect(recordsOutcome(engine, bytes)).toEqual(oracleRecords(bytes));
    });

    it("frozen ruleset fixtures: long text, text and records are byte-identical to scrub.ts", () => {
      for (const f of FIXTURES) {
        expect(engine.scrubLong(f.text), `${f.name} long`).toBe(tsScrubLong(f.text));
        expect(engine.scrubText(f.text), `${f.name} text`).toBe(tsScrubText(f.text));
        const asRecords = f.text
          .split("\n")
          .map((line, i) => JSON.stringify({ type: "user", n: i, message: { content: line } }))
          .join("\n");
        const bytes = enc.encode(`${asRecords}\n`);
        expect(recordsOutcome(engine, bytes), `${f.name} records`).toEqual(oracleRecords(bytes));
        const raw = enc.encode(f.text);
        expect(recordsOutcome(engine, raw), `${f.name} raw lines`).toEqual(oracleRecords(raw));
      }
    });

    it("edge inputs: empty, BOM, astral text, numbers, nested JSON, invalid UTF-8", () => {
      for (const s of ["", "﻿text", "emoji \u{1F600} and é", "a\r\nb", "x".repeat(300_000)])
        expect(engine.scrubText(s)).toBe(tsScrubText(s));
      for (const n of [0, -1, 4111111111111111, 1.5, 4012888888881881])
        expect(engine.scrubJson(n)).toEqual(tsScrubNumber(n));
      const nested = { a: [1, "b", { c: null, d: true }], "password": { x: ["y"] }, e: "" };
      expect(JSON.stringify(engine.scrubJson(nested))).toBe(JSON.stringify(tsScrubJson(nested)));
      for (const v of [null, true, false]) expect(engine.scrubJson(v)).toBe(tsScrubJson(v));
      for (const bytes of [new Uint8Array([0x7b, 0xff, 0x7d, 0x0a]), new Uint8Array([0xc3]), new Uint8Array(0)])
        expect(recordsOutcome(engine, bytes)).toEqual(oracleRecords(bytes));
    });
  });
}

// ---------------------------------------------------------------- selection

describe("selection", () => {
  it("unset SCRUB_ENGINE is the TypeScript engine, identical to direct scrub.ts calls on the corpus", async () => {
    expect(selectScrubEngine({})).toBe("ts");
    expect(await initScrubEngine({ env: {} })).toBe(tsScrubEngine);
    expect(seam.activeScrubEngine()).toBe(tsScrubEngine);
    expect(scrubEngineReady()).toBe(true);
    for (const c of cases) {
      expect(seam.scrubText(c.input)).toBe(tsScrubText(c.input));
      expect(seam.scrubTextForLog(c.input)).toBe(tsScrubText(c.input));
      expect(seam.scrubLong(c.input)).toBe(tsScrubLong(c.input));
      const line = enc.encode(`${c.input}\n`);
      const via = seam.scrubRecords(line);
      const direct = tsScrubRecords(line);
      expect(hex(via.scrubbed)).toBe(hex(direct.scrubbed));
      expect(via.records).toBe(direct.records);
      if (c.record) {
        const parsed = JSON.parse(c.input) as unknown;
        expect(JSON.stringify(seam.scrubJson(parsed))).toBe(JSON.stringify(tsScrubJson(parsed)));
      }
    }
    // The TypeScript marker is today's constants (sealRawChunk stamps ruleset + version only).
    expect(seam.activeScrubMarker()).toEqual({ ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION, profile: seam.TS_SCRUB_PROFILE });
  });

  it('"" and "ts" are the TypeScript engine', () => {
    expect(selectScrubEngine({ SCRUB_ENGINE: "" })).toBe("ts");
    resetScrubEngineForTests();
    grantTs();
    expect(selectScrubEngine({ SCRUB_ENGINE: "ts" })).toBe("ts");
  });

  it('without a grant: unset and "" are wasm-bindgen (the default); "ts" refuses with scrub_engine_ts_not_allowed, sticky', async () => {
    resetScrubEngineForTests();
    expect(selectScrubEngine({})).toBe("wasm-bindgen");
    resetScrubEngineForTests();
    expect(selectScrubEngine({ SCRUB_ENGINE: "" })).toBe("wasm-bindgen");
    resetScrubEngineForTests();
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "ts" })).toThrow(expect.objectContaining({ reason: "scrub_engine_ts_not_allowed" }));
    grantTs(); // a grant arriving after the refusal does not reopen it
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "ts" })).toThrow(expect.objectContaining({ reason: "scrub_engine_ts_not_allowed" }));
    expect(() => selectScrubEngine({})).toThrow(expect.objectContaining({ reason: "scrub_engine_ts_not_allowed" }));
    await expect(initScrubEngine({ env: {} })).rejects.toMatchObject({ reason: "scrub_engine_ts_not_allowed" });
    expect(() => seam.scrubText("token=abc")).toThrow(ScrubEngineConfigError);
    expect(seam.scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
    expect(scrubEngineReady()).toBe(false);
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
  });

  it("without a grant, the first scrub call selects wasm-bindgen from process.env and refuses (not_initialised), never TypeScript", () => {
    resetScrubEngineForTests();
    const saved = process.env.SCRUB_ENGINE;
    delete process.env.SCRUB_ENGINE;
    try {
      expect(() => seam.scrubText("plain")).toThrow(expect.objectContaining({ reason: "not_initialised" }));
      expect(seam.scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
      expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
    } finally {
      if (saved !== undefined) process.env.SCRUB_ENGINE = saved;
    }
  });

  it("with nothing selected, the first scrub call selects from process.env (unset in a granted process: TypeScript)", () => {
    const saved = process.env.SCRUB_ENGINE;
    delete process.env.SCRUB_ENGINE;
    try {
      expect(seam.scrubText("plain")).toBe("plain");
      expect(seam.activeScrubEngine()).toBe(tsScrubEngine);
    } finally {
      if (saved !== undefined) process.env.SCRUB_ENGINE = saved;
    }
  });

  it("with process.env SCRUB_ENGINE=wasm and no init, the first scrub call refuses", () => {
    const saved = process.env.SCRUB_ENGINE;
    process.env.SCRUB_ENGINE = "wasm";
    try {
      expect(() => seam.scrubText("plain")).toThrow(ScrubEngineUnavailable);
      expect(seam.scrubTextForLog("plain")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
    } finally {
      if (saved === undefined) delete process.env.SCRUB_ENGINE;
      else process.env.SCRUB_ENGINE = saved;
    }
  });

  for (const value of ["WASM", "Ts", "typescript", " ts", "wasm ", "rust", "0", "none", "off"]) {
    it(`unknown SCRUB_ENGINE ${JSON.stringify(value)} throws`, async () => {
      expect(() => selectScrubEngine({ SCRUB_ENGINE: value })).toThrow(ScrubEngineConfigError);
      await expect(initScrubEngine({ env: { SCRUB_ENGINE: value } })).rejects.toBeInstanceOf(ScrubEngineConfigError);
    });
  }

  it("is read once: a different second selection throws instead of switching engines", async () => {
    await initScrubEngine({ env: {} });
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "wasm" })).toThrow(ScrubEngineConfigError);
    await expect(initScrubEngine(mirrorInit())).rejects.toBeInstanceOf(ScrubEngineConfigError);
    expect(seam.activeScrubEngine()).toBe(tsScrubEngine);
    resetScrubEngineForTests();
    grantTs();
    await initScrubEngine(mirrorInit());
    expect(() => selectScrubEngine({})).toThrow(ScrubEngineConfigError);
    expect(seam.activeScrubEngine().kind).toBe("wasm");
  });

  it("the TypeScript seal path is unchanged; with wasm it is refused, never run on TypeScript", async () => {
    await initScrubEngine(mirrorInit());
    await expect(seam.sealRawChunk({} as never, {} as never)).rejects.toMatchObject({ reason: "seal_path_not_routed" });
  });
});

// ---------------------------------------------------------------- fault injection

/** Every scrub entry point of the seam refuses with `reason`. */
async function expectRefused(reason: ScrubEngineFailure) {
  const calls: [string, () => unknown][] = [
    ["scrubText", () => seam.scrubText("token=abc")],
    ["scrubLong", () => seam.scrubLong("token=abc")],
    ["scrubJson", () => seam.scrubJson({ password: "x" })],
    ["scrubNumber", () => seam.scrubNumber(4111111111111111)],
    ["scrubRecords", () => seam.scrubRecords(enc.encode('{"a":"b"}\n'))],
    ["activeScrubMarker", () => seam.activeScrubMarker()],
  ];
  for (const [name, call] of calls) {
    let thrown: unknown;
    try {
      call();
    } catch (error) {
      thrown = error;
    }
    expect(thrown, name).toBeInstanceOf(ScrubEngineUnavailable);
    expect((thrown as ScrubEngineUnavailable).reason, name).toBe(reason);
    expect((thrown as ScrubEngineUnavailable).code, name).toBe("scrub_engine_unavailable");
  }
  await expect(seam.sealRawChunk({} as never, { source: enc.encode("x\n") } as never)).rejects.toBeInstanceOf(ScrubEngineUnavailable);
  expect(scrubEngineReady()).toBe(false);
  // Logging never fails and never carries the unscrubbed text.
  expect(seam.scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
}

describe("fault injection: SCRUB_ENGINE=wasm init failures fail closed", () => {
  const faults: { name: string; init: () => Parameters<typeof initScrubEngine>[0]; reason: ScrubEngineFailure }[] = [
    { name: "the checked-in pin (unpinned placeholder)", init: () => mirrorInit({}, { pin: pinFile }), reason: "unpinned" },
    { name: "the default pin and loader", init: () => ({ env: { SCRUB_ENGINE: "wasm" } }), reason: "unpinned" },
    { name: "an all-zero sha with real-looking versions", init: () => mirrorInit({}, { pin: mirrorPin({ wasm_sha256: "0".repeat(64) }) }), reason: "unpinned" },
    { name: "an UNPINNED profile", init: () => mirrorInit({}, { pin: mirrorPin({ scrub_profile: "UNPINNED" }) }), reason: "unpinned" },
    { name: "a malformed pin (short sha)", init: () => mirrorInit({}, { pin: mirrorPin({ wasm_sha256: "abc" }) }), reason: "pin_invalid" },
    { name: "a malformed pin (other crate)", init: () => mirrorInit({}, { pin: mirrorPin({ crate: "other" }) }), reason: "pin_invalid" },
    { name: "a pin that is not an object", init: () => mirrorInit({}, { pin: null }), reason: "pin_invalid" },
    { name: "a pin older than the TypeScript ruleset", init: () => mirrorInit({ rulesetVersion: 7 }, { pin: mirrorPin({ ruleset_version: 7 }) }), reason: "ruleset_mismatch" },
    { name: "a pin with another ABI version", init: () => mirrorInit({ abiVersion: 2 }, { pin: mirrorPin({ abi_version: 2 }) }), reason: "abi_mismatch" },
    { name: "sha256 mismatch", init: () => mirrorInit({}, { loadArtifact: async () => ({ bytes: enc.encode("\0asm a different module") }) }), reason: "sha256_mismatch" },
    { name: "module reports another ABI version", init: () => mirrorInit({ abiVersion: 2 }), reason: "abi_mismatch" },
    { name: "module reports another ruleset version", init: () => mirrorInit({ rulesetVersion: SCRUB_RULESET_VERSION + 1 }), reason: "ruleset_mismatch" },
    { name: "module reports another profile", init: () => mirrorInit({ profile: "relayscrubber/other" }), reason: "profile_mismatch" },
    { name: "loader throws", init: () => mirrorInit({}, { loadArtifact: async () => { throw new Error("ENOENT"); } }), reason: "artifact_missing" },
    { name: "loader returns no bytes", init: () => mirrorInit({}, { loadArtifact: async () => ({ bytes: new Uint8Array(0) }) }), reason: "artifact_missing" },
    { name: "instantiate throws", init: () => mirrorInit({}, { instantiate: async () => { throw new Error("CompileError"); } }), reason: "instantiate_failed" },
    ...REQUIRED_EXPORTS.map((name) => ({ name: `export ${name} missing`, init: () => mirrorInit({ omit: name }), reason: "export_missing" as const })),
  ];

  for (const f of faults) {
    it(`${f.name}: init throws ${f.reason}, then every scrub call throws ScrubEngineUnavailable`, async () => {
      const rejected = await initScrubEngine(f.init()).then(
        () => null,
        (error: unknown) => error,
      );
      expect(rejected).toBeInstanceOf(ScrubEngineUnavailable);
      expect((rejected as ScrubEngineUnavailable).reason).toBe(f.reason);
      // The message names the reason only; no pin, path or artifact bytes.
      expect((rejected as Error).message).toBe(`the scrub engine is unavailable (${f.reason})`);
      await expectRefused(f.reason);
    });
  }

  it("before init completes, SCRUB_ENGINE=wasm refuses (not_initialised)", async () => {
    selectScrubEngine({ SCRUB_ENGINE: "wasm" });
    await expectRefused("not_initialised");
  });

  it("a later successful init lifts the refusal; the failure did not select TypeScript", async () => {
    await expect(initScrubEngine(mirrorInit({}, { pin: pinFile }))).rejects.toBeInstanceOf(ScrubEngineUnavailable);
    await expectRefused("unpinned");
    const engine = await initScrubEngine(mirrorInit());
    expect(engine.kind).toBe("wasm");
    expect(seam.scrubText("plain")).toBe("plain");
  });

  it("a trap poisons the instance: that call and every later call refuse (engine_failed)", async () => {
    await initScrubEngine(mirrorInit({ trapOn: "scrub_text" }));
    expect(seam.scrubRecords(enc.encode('{"a":"b"}\n')).records).toBe(1);
    expect(() => seam.scrubText("x")).toThrow(ScrubEngineUnavailable);
    await expectRefused("engine_failed");
  });

  it("an unexpected status refuses that call only (engine_refused), never returns the input", async () => {
    const exports = mirrorExports();
    exports.scrub_text = () => 7;
    const engine = await createWasmScrubEngine({ pin: mirrorPin(), loadArtifact: async () => ({ bytes: MIRROR_BYTES }), instantiate: async () => exports });
    expect(() => engine.scrubText("token=abc")).toThrow(expect.objectContaining({ reason: "engine_refused" }));
    expect(engine.scrubLong("plain")).toBe("plain");
  });

  it("invalid UTF-8 output from the module is refused, not decoded with replacement", async () => {
    const exports = mirrorExports();
    const memory = exports.memory as { buffer: ArrayBuffer };
    const engine = await createWasmScrubEngine({ pin: mirrorPin(), loadArtifact: async () => ({ bytes: MIRROR_BYTES }), instantiate: async () => exports });
    // Corrupt the result only after init (init reads the profile through the same result exports).
    exports.scrub_text = () => 0;
    exports.scrub_result_ptr = () => 0;
    exports.scrub_result_len = () => 1;
    new Uint8Array(memory.buffer)[0] = 0xff;
    expect(() => engine.scrubText("x")).toThrow(expect.objectContaining({ reason: "engine_refused" }));
  });

  it("refuses text that cannot cross a UTF-8 ABI unchanged (lone surrogates)", async () => {
    const engine = await mirrorEngine();
    for (const s of ["a\uD800b", "\uDC00", "x\uD83D"])
      expect(() => engine.scrubText(s)).toThrow(expect.objectContaining({ code: "scrub_input_not_well_formed" }));
    expect(engine.scrubText("ok \u{1F600}")).toBe("ok \u{1F600}");
  });

  it("the checked-in pin is the placeholder and parses as unpinned", () => {
    expect(() => parseScrubWasmPin(pinFile)).toThrow(expect.objectContaining({ reason: "unpinned" }));
  });

  it("the marker comes from the instance, not the pin: a pin naming another profile cannot relabel output", async () => {
    // Pin and module disagree on the profile: init refuses instead of stamping either.
    await expect(
      createWasmScrubEngine({ pin: mirrorPin({ scrub_profile: "relayscrubber/pinned-label" }), loadArtifact: async () => ({ bytes: MIRROR_BYTES }), instantiate: async () => mirrorExports() }),
    ).rejects.toMatchObject({ reason: "profile_mismatch" });
    const engine = await mirrorEngine();
    expect(engine.scrubRecords(enc.encode('{"a":1}\n')).scrub).toEqual({ ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION, profile: MIRROR_PROFILE });
  });
});

describe("trace isSafeIdentifier through the seam (predicate-hook.ts)", () => {
  it("TypeScript engine: same verdicts as scrub.ts", () => {
    // An email is identifier-shaped but scrub.ts redacts it, so only the scrub check rejects it.
    expect(isSafeIdentifier("someone@example.com")).toBe(tsScrubText("someone@example.com") === "someone@example.com");
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(true);
  });

  it("wasm selected but not initialised: no identifier is judged safe (never the TypeScript engine)", () => {
    seam.selectScrubEngine({ SCRUB_ENGINE: "wasm" });
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
  });

  it("wasm initialised: the engine judges, with the same verdict as TypeScript for the mirror", async () => {
    await initScrubEngine(mirrorInit());
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(true);
  });

  it("wasm engine poisoned: identifiers are dropped", async () => {
    await initScrubEngine(mirrorInit({ trapOn: "scrub_text" }));
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
  });
});
