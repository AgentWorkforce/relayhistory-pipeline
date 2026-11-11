/**
 * The routed raw-chunk sealer against the REAL vendored relayscrubber 0.5.0-alpha.8
 * wasm, under the AA env triple (SCRUB_ENGINE=wasm-bindgen,
 * SCRUB_ENGINE_REQUIRED=wasm-bindgen, READER_MAX_RULESET=10).
 *
 * The blank predicate is PR-A's Rust-exact one (hasRecordContent): NEL-only input is
 * refused as chunk_empty before the crate and without poisoning; U+FEFF is content, so
 * FEFF-only input reaches the crate and the crate's own answer stands (alpha.8 answers
 * a tagged invalid_json call refusal, which refuses that one call and never poisons).
 */
import { zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { objectKey, readAll } from "../src/lib/pipeline/object-layout.js";
import {
  activeScrubEngine,
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  scrubEngineReady,
} from "../src/lib/scrub-engine/index.js";
import { TRIPLE, faultGlue, sealInput, tempStores } from "./support/real-bindgen.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET"] as const;
const stores = tempStores();
let saved: Record<string, string | undefined>;
let glue: ReturnType<typeof faultGlue>;

beforeEach(async () => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  Object.assign(process.env, TRIPLE);
  resetScrubEngineForTests();
  glue = faultGlue();
  // The exact checked-in pin and real vendored bytes; the wrapper only counts calls.
  await initScrubEngine({ env: TRIPLE, bindgen: { importGlue: glue.importGlue } });
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await stores.cleanup();
});

const show = (t: string) => t.replace(/[^\x21-\x7e]/g, (c) => `U+${c.charCodeAt(0).toString(16).padStart(4, "0")}`);
async function rejection(p: Promise<unknown>): Promise<Record<string, unknown>> {
  try { await p; } catch (error) { return error as Record<string, unknown>; }
  throw new Error("expected rejection");
}
function sealer() {
  const s = routedRawChunkSealer();
  if (!s) throw new Error("expected a routed sealer under wasm-bindgen");
  return s;
}
async function expectHealthyAfterwards() {
  expect(scrubEngineReady()).toBe(true);
  const store = await stores.store();
  const sealed = await sealer()(store, sealInput(enc.encode('{"ok":"token=ghp_abcdefghijklmnopqrstuvwxyz0123456789"}\n'), 99));
  expect(sealed.created).toBe(true);
  expect(sealed.entry.records).toBe(1);
}
async function stored(store: Awaited<ReturnType<typeof stores.store>>, key: string) {
  const object = await store.getObject(objectKey(key));
  return { text: dec.decode(zstdDecompressSync(await readAll(object.body))), metadata: object.meta.metadata };
}
const concat = (...parts: Uint8Array[]) => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0)); let at = 0;
  for (const p of parts) { out.set(p, at); at += p.byteLength; }
  return out;
};
const spaces = (n: number) => new Uint8Array(n).fill(0x20);

describe("the routed sealer is bound to the exact verified instance", () => {
  it("is the same function for every caller and carries the instance's own marker", () => {
    const a = sealer(), b = sealer();
    expect(a).toBe(b);
    expect(a.marker).toStrictEqual({ ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" });
    expect(a.marker).toStrictEqual(activeScrubEngine().marker);
  });

  it("seals with the real crate: redacted body, scrub-version 10 and the crate profile in metadata", async () => {
    const store = await stores.store();
    const source = enc.encode('{"a":"AKIAIOSFODNN7EXAMPLE"}\r\n{"k":"token=ghp_abcdefghijklmnopqrstuvwxyz0123456789"}\n');
    const sealed = await sealer()(store, sealInput(source));
    expect(glue.calls.scrubRecords).toBe(1);
    expect(sealed.created).toBe(true);
    expect(sealed.scrub).toStrictEqual({ ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" });
    expect(sealed.entry.records).toBe(2);
    const { text, metadata } = await stored(store, sealed.key);
    expect(text).toBe('{"a":"[REDACTED]"}\n{"k":"token=[REDACTED]"}\n');
    expect(text).not.toContain("AKIA");
    expect(metadata["scrub-ruleset"]).toBe("relayhistory-scrub");
    expect(metadata["scrub-version"]).toBe("10");
    expect(metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
  });
});

describe("NEL-only input: refused as blank before the crate, never poisons", () => {
  for (const text of ["\u0085", "\u0085\n", "\u0085\u0085\r\n", "\n\u0085\n　 \r\n \n", " \u0085\t\n"])
    it(`${show(text)}: chunk_empty, zero crate calls, nothing stored, still ready`, async () => {
      const store = await stores.store();
      const error = await rejection(sealer()(store, sealInput(enc.encode(text))));
      expect(error.code).toBe("chunk_empty");
      expect(glue.calls.scrubRecords).toBe(0);
      expect(store.puts).toEqual([]);
      expect((activeScrubEngine() as unknown as { failed: boolean }).failed).toBe(false);
      await expectHealthyAfterwards();
    });
});

describe("FEFF-only input: reaches the crate; the crate's answer stands", () => {
  for (const text of ["﻿", "﻿\n", "﻿﻿\n", " ﻿ \n", "\u0085﻿\n"])
    it(`${show(text)}: one real crate call, tagged invalid_json call refusal, not poisoned`, async () => {
      const store = await stores.store();
      const error = await rejection(sealer()(store, sealInput(enc.encode(text))));
      expect(glue.calls.scrubRecords).toBe(1);
      expect(error).toMatchObject({ code: "invalid_json", disposition: "call_refusal", record: 1 });
      expect(store.puts).toEqual([]);
      await expectHealthyAfterwards();
    });

  it("FEFF inside a JSON string is content the crate keeps: one record, U+FEFF preserved", async () => {
    const store = await stores.store();
    const sealed = await sealer()(store, sealInput(enc.encode('{"a":"﻿"}\n')));
    expect(sealed.entry.records).toBe(1);
    expect((await stored(store, sealed.key)).text).toBe('{"a":"﻿"}\n');
  });

  it("a FEFF-only line after a record: refused at that record (2), no partial object", async () => {
    const store = await stores.store();
    const error = await rejection(sealer()(store, sealInput(enc.encode('{"a":1}\n﻿\n'))));
    expect(error).toMatchObject({ code: "invalid_json", record: 2 });
    expect(store.puts).toEqual([]);
    await expectHealthyAfterwards();
  });
});

describe("mixed boundaries", () => {
  const cases: [string, number, string][] = [
    ['{"a":1}\r\n\u0085\n{"b":"AKIAIOSFODNN7EXAMPLE"}\n　\n', 2, '{"a":1}\n{"b":"[REDACTED]"}\n'],
    ['{"a":1}', 1, '{"a":1}\n'],
    ['\n\n{"a":1}\n\n', 1, '{"a":1}\n'],
    [' {"a":1} \r\n \n{"c":[1,2]}\r\n', 2, '{"a":1}\n{"c":[1,2]}\n'],
    ['"x"\n1\nnull\n', 3, '"x"\n1\nnull\n'],
  ];
  for (const [text, records, out] of cases)
    it(`${show(text)}: ${records} record(s)`, async () => {
      const store = await stores.store();
      const sealed = await sealer()(store, sealInput(enc.encode(text)));
      expect(sealed.entry.records).toBe(records);
      expect((await stored(store, sealed.key)).text).toBe(out);
    });

  it("a non-JSON line between records: tagged invalid_json for that record, not poisoned", async () => {
    const store = await stores.store();
    const error = await rejection(sealer()(store, sealInput(enc.encode('{"a":1}\nnot json\n{"b":2}\n'))));
    expect(error).toMatchObject({ code: "invalid_json", disposition: "call_refusal", record: 2 });
    await expectHealthyAfterwards();
  });

  it("invalid UTF-8 is refused in JS admission before the crate (chunk_not_utf8), not poisoned", async () => {
    const store = await stores.store();
    const error = await rejection(sealer()(store, sealInput(Uint8Array.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xc3, 0x22, 0x7d, 0x0a]))));
    expect(error.code).toBe("chunk_not_utf8");
    expect(glue.calls.scrubRecords).toBe(0);
    await expectHealthyAfterwards();
  });
});

describe("chunked UTF-8 decoding: code points straddling the 64 KiB decode boundary", () => {
  const BOUNDARY = 65_536;
  /** Put `ch`'s encoding so that its first byte is at BOUNDARY - 1 (it straddles). */
  const straddle = (ch: string, fill: (n: number) => Uint8Array, tail: Uint8Array = enc.encode("\n")) =>
    concat(fill(BOUNDARY - 1), enc.encode(ch), tail);

  for (const ch of ["\u0085", "　", " ", " "])
    it(`whitespace ${show(ch)} straddling among spaces: still blank (chunk_empty), zero crate calls`, async () => {
      const source = straddle(ch, spaces);
      expect(source[BOUNDARY - 1]).toBeGreaterThanOrEqual(0xc2);
      const error = await rejection(sealer()(await stores.store(), sealInput(source)));
      expect(error.code).toBe("chunk_empty");
      expect(glue.calls.scrubRecords).toBe(0);
      await expectHealthyAfterwards();
    });

  it("U+FEFF straddling among spaces: content, reaches the crate (invalid_json), not poisoned", async () => {
    const error = await rejection(sealer()(await stores.store(), sealInput(straddle("﻿", spaces))));
    expect(glue.calls.scrubRecords).toBe(1);
    expect(error).toMatchObject({ code: "invalid_json", disposition: "call_refusal" });
    await expectHealthyAfterwards();
  });

  for (const ch of ["€", "\u{1F600}", "é"])
    it(`content ${show(ch)} straddling inside a JSON string: sealed whole, byte-exact`, async () => {
      const head = enc.encode('{"a":"');
      const pad = new Uint8Array(BOUNDARY - 1 - head.byteLength).fill(0x78);
      const source = concat(head, pad, enc.encode(`${ch}"}\n`));
      expect(source.byteLength).toBeGreaterThan(BOUNDARY);
      const store = await stores.store();
      const sealed = await sealer()(store, sealInput(source));
      expect(sealed.entry.records).toBe(1);
      expect((await stored(store, sealed.key)).text).toBe(dec.decode(source));
    });

  it("a code point split at the end of the stream is invalid UTF-8: chunk_not_utf8 before the crate", async () => {
    const source = concat(spaces(BOUNDARY - 1), enc.encode("€").subarray(0, 2));
    const error = await rejection(sealer()(await stores.store(), sealInput(source)));
    expect(error.code).toBe("chunk_not_utf8");
    expect(glue.calls.scrubRecords).toBe(0);
  });
});
