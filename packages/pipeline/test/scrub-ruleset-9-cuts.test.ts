import { describe, expect, it } from "vitest";
import { MAX_QUOTED_VALUE, MAX_SCRUB_CHARS, scrubJson, scrubText, unsplittableQuotedValueSpans } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import {
  scrubJson as v8Json,
  scrubText as v8,
  unsplittableQuotedValueSpans as v8Spans,
} from "./fixtures/scrub-ruleset-8.js";

// Rule set 9: segment cuts near a quoted secret key whose value's opener sits behind a long
// backslash run, and the quoted key/value rule's added value forms (encoded openers, line
// breaks around the separator, values opening with `[`). Synthetic values only:
// SECRET_ALPHA / SECRET_BETA are markers, never real secrets. The literal backslash is
// written as BS (Bun cooks `\u` escapes inside String.raw). The end-to-end proof that rule
// set 8's scrubLong and scrubRecords store the value is in `scrub-v8-v9-fuzz.test.ts`
// (it needs vitest's module mock); here the proof is the missing span and the keyless tail.

const BS = "\\";
const encoder = new TextEncoder(), decoder = new TextDecoder();
const records = (text: string) => decoder.decode(scrubRecords(encoder.encode(text)).scrubbed);
const leaks = (out: string) => /SECRET_(ALPHA|BETA)/.test(out);
const VALUE = "SECRET_ALPHA SECRET_BETA";
const TAIL = " tail " + "z ".repeat(5000);

/** Filler of `n` units ending in a space, no rule matches it. */
const filler = (n: number) => "lorem ipsum ".repeat(Math.ceil(n / 12) + 1).slice(0, n - 1) + " ";

/** The F1 shape: a quoted key `off` units before MAX_SCRUB_CHARS, `bs` backslashes, then the quoted value. */
function f1(bs: number, off: number, sep: string, q: string) {
  const run = BS.repeat(bs);
  const head = filler(MAX_SCRUB_CHARS - off);
  return { head, input: `${head}${q}password${q}${sep}${run}${q}${VALUE}${run}${q}${TAIL}` };
}

const paths: Array<[string, (x: string) => string, (out: string) => string, string | undefined]> = [
  ["scrubLong", (x) => scrubLong(x), (o) => scrubLong(o), undefined],
  ["scrubRecords {text}", (x) => records(JSON.stringify({ text: x })), records, undefined],
  ["scrubRecords {idempotency_key, after}", (x) => records(JSON.stringify({ idempotency_key: x, after: "keep" })), records, '"after":"keep"}'],
];

describe("rule set 9: no segment cut between a quoted key and a value behind a long backslash run", () => {
  for (const bs of [9000, 20000]) {
    for (const off of [2000, 500]) {
      for (const sep of [": ", " : ", " = "]) {
        for (const q of ['"', "'"]) {
          const { head, input } = f1(bs, off, sep, q);
          for (const [name, first, again, peer] of paths) {
            it(`bs ${bs}, key ${off} before the bound, ${JSON.stringify(sep)}, ${q}: ${name}`, () => {
              const out = first(input);
              expect(leaks(out)).toBe(false);
              expect(out).toContain("[REDACTED]");
              expect(again(out)).toBe(out);
              if (peer) expect(out.endsWith(peer)) .toBe(true);
              if (name === "scrubLong") {
                // Only the value goes: the head, the key and the text after the close are kept.
                expect(out.startsWith(`${head}${q}password${q}${sep}`)).toBe(true);
                expect(out.endsWith(`${q}${TAIL}`)).toBe(true);
              }
            });
          }
        }
      }
    }
  }

  it("bs 8000 (below the context check's reach) stays clean on every path", () => {
    for (const sep of [": ", " = "]) {
      const { input } = f1(8000, 2000, sep, '"');
      for (const [, first, again] of paths) {
        const out = first(input);
        expect(leaks(out)).toBe(false);
        expect(again(out)).toBe(out);
      }
    }
  });

  it("a prefix ending in the backslash run after the separator is recorded open to its end (rule set 8 recorded nothing)", () => {
    const { head } = f1(9000, 2000, ": ", '"');
    for (const tail of ["", BS.repeat(1), BS.repeat(5000)]) {
      const window = `${head}"password": ${tail}`;
      expect(unsplittableQuotedValueSpans(window, { prefix: true })).toEqual([[head.length, window.length]]);
      expect(v8Spans(window, { prefix: true })).toEqual([]);
    }
    // Not a prefix: nothing follows, nothing to protect.
    expect(unsplittableQuotedValueSpans(`${head}"password": ${BS.repeat(50)}`)).toEqual([]);
    // What a cut there stored under rule set 8: the keyless rest keeps the value.
    const keyless = `${BS.repeat(9000)}"${VALUE}${BS.repeat(9000)}"${TAIL}`;
    expect(leaks(v8(keyless))).toBe(true);
    expect(leaks(scrubText(keyless))).toBe(true);
  });

  it("a closed value opened more than MAX_QUOTED_VALUE units past its key is recorded whole", () => {
    const run = BS.repeat(9000);
    const text = `x ${run}"password${run}": "${VALUE}" tail`;
    const valueEnd = text.indexOf(VALUE) + VALUE.length;
    expect(text.indexOf(VALUE) - 2).toBeGreaterThan(MAX_QUOTED_VALUE);
    expect(unsplittableQuotedValueSpans(text, { prefix: true })).toEqual([[2, valueEnd]]);
    expect(v8Spans(text, { prefix: true })).toEqual([]);
  });

  for (const off of [19000, 25000]) {
    for (const sep of [": ", ":"]) {
      it(`a 9000-backslash key run, key ${off} before the bound, ${JSON.stringify(sep)}: scrubLong and scrubRecords`, () => {
        const run = BS.repeat(9000);
        const head = filler(MAX_SCRUB_CHARS - off);
        const input = `${head}${run}"password${run}"${sep}"${VALUE}"${"x".repeat(30000)} tail ${"z ".repeat(3000)}`;
        for (const [, first, again] of paths.slice(0, 2)) {
          const out = first(input);
          expect(leaks(out)).toBe(false);
          expect(again(out)).toBe(out);
        }
      });
    }
  }
});

/** [name, input, exact scrubText output]: rule set 8 keeps the value in every case. */
const valueForms: Array<[string, string, string]> = [
  // O3: line breaks around the separator.
  ["LF after the colon", `"password":\n"${VALUE}" tail`, `"password":\n"[REDACTED]" tail`],
  ["CRLF after the colon", `"password":\r\n"${VALUE}" tail`, `"password":\r\n"[REDACTED]" tail`],
  ["space, colon, LF and indent", `"password" :\n  "${VALUE}" tail`, `"password" :\n  "[REDACTED]" tail`],
  ["LF before the colon", `'token'\n: '${VALUE}' tail`, `'token'\n: '[REDACTED]' tail`],
  // O1: a value opening with `[`.
  ["`[` unclosed", `"password"=[SECRET_ALPHA tail`, `"password"=[REDACTED] tail`],
  ["`[` closed", `"password": [SECRET_ALPHA] tail`, `"password": [REDACTED] tail`],
  ["marker then a percent quote then a secret", `"password"=[REDACTED]%22SECRET_ALPHA tail`, `"password"=[REDACTED] tail`],
  ["percent quote, marker, percent quote, secret", `"password"=%22[REDACTED]%22SECRET_ALPHA tail`, `"password"=[REDACTED] tail`],
  // O2: encoded openers, closed by the same form at the same depth.
  ["backslash-u0022", `"password": ${BS}u0022${VALUE}${BS}u0022 tail`, `"password": ${BS}u0022[REDACTED]${BS}u0022 tail`],
  ["backslash-x22", `"password": ${BS}x22${VALUE}${BS}x22 tail`, `"password": ${BS}x22[REDACTED]${BS}x22 tail`],
  ["single-quoted key, backslash-u0027", `'password': ${BS}u0027${VALUE}${BS}u0027 tail`, `'password': ${BS}u0027[REDACTED]${BS}u0027 tail`],
  ["two backslashes, U0060", `"password": ${BS}${BS}U0060${VALUE}${BS}${BS}u0060 tail`, `"password": ${BS}${BS}U0060[REDACTED]${BS}${BS}u0060 tail`],
  ["%22", `"password": %22${VALUE}%22 tail`, `"password": %22[REDACTED]%22 tail`],
  ["%2527", `"password": %2527${VALUE}%2527 tail`, `"password": %2527[REDACTED]%2527 tail`],
  ["%u0022 / %U0022", `"password":%u0022${VALUE}%U0022 tail`, `"password":%u0022[REDACTED]%U0022 tail`],
  ["%22 unclosed: to the end", `"password": %22${VALUE} tail`, `"password": %22[REDACTED]`],
  ["%22 closed by %27 only: to the end", `"password": %22${VALUE}%27 tail`, `"password": %22[REDACTED]`],
  ["backslash-x22 across a line break", `"password": ${BS}x22SECRET_ALPHA\nSECRET_BETA${BS}x22 tail`, `"password": ${BS}x22[REDACTED]${BS}x22 tail`],
];

/** Kept by both rule sets (and stable): no secret, or already redacted. */
const keptForms = [
  `"password"=[REDACTED]`,
  `"password"=[REDACTED]%22`,
  `"password": %22[REDACTED]%22 tail`,
  `"password": ${BS}u0022[REDACTED]${BS}u0022 tail`,
  `"password"=[]`,
  `"password"=[1, 2]`,
  `"password": [true]`,
  `"password": ["a"]`,
  `"auth":\n{"a":1}`,
];

describe("rule set 9: quoted key/value rule, encoded openers, line breaks and `[` values", () => {
  for (const [name, input, expected] of valueForms) {
    it(`${name}: scrubText, scrubLong, scrubJson and scrubRecords; rule set 8 keeps it`, () => {
      expect(scrubText(input)).toBe(expected);
      expect(scrubText(expected)).toBe(expected);
      expect(scrubLong(input)).toBe(expected);
      expect(scrubLong(expected)).toBe(expected);
      expect(scrubJson({ text: input })).toEqual({ text: expected });
      expect(scrubJson({ text: expected })).toEqual({ text: expected });
      const rec = records(JSON.stringify({ text: input, peer: "keep" }));
      expect(rec).toBe(JSON.stringify({ text: expected, peer: "keep" }));
      expect(records(rec)).toBe(rec);
      expect(leaks(v8(input))).toBe(true);
      expect(leaks(JSON.stringify(v8Json({ text: input })))).toBe(true);
    });
  }

  it("kept values stay byte-identical on every path", () => {
    for (const input of keptForms) {
      expect(scrubText(input)).toBe(input);
      expect(scrubLong(input)).toBe(input);
      expect(scrubJson({ text: input })).toEqual({ text: input });
      const rec = JSON.stringify({ text: input });
      expect(records(rec)).toBe(rec);
    }
  });

  it("a value of MAX_QUOTED_VALUE units or more opening with `[` is redacted whatever it holds", () => {
    const input = `"password": [${"1".repeat(MAX_QUOTED_VALUE)} tail`;
    expect(scrubText(input)).toBe(`"password": [REDACTED] tail`);
  });
});
