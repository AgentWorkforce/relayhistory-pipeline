/**
 * Rule set 4 promise: a quoted value whose total length to its matching closing quote is
 * within the scan bound (`MAX_QUOTED_VALUE`, 4096 UTF-16 units, counted across line
 * breaks) scrubs byte-identically to rule set 3. This differential fuzz runs rule set 3's
 * `lib/scrub.ts` against the current one on random key/value inputs, including
 * multi-line values whose first line is short and that cross the bound only after one or
 * more breaks (CR, LF, CRLF, U+2028, U+2029, U+0085), and values whose total length lands
 * either side of the bound. A difference is allowed only where rule set 4 reports an
 * over-bound value (`overCapQuotedValueSpans`) and the input could hold one (some value
 * opens more than 4096 units before the end of the input); any other difference fails.
 * 2,000 inputs by default; SCRUB_FUZZ_INPUTS raises it, SCRUB_FUZZ_SEED changes the seed.
 *
 * Rule set 3 is frozen in `fixtures/scrub-ruleset-3.ts`: byte for byte
 * `git show fc10bb03:packages/pipeline/src/lib/scrub.ts` (it has no imports), pinned by
 * its sha256 below, so the test runs in every checkout, shallow ones and CI included.
 * Never edit the fixture: a rule set that has shipped does not change.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { scrubText as v3 } from "./fixtures/scrub-ruleset-3.js";
import { overCapQuotedValueSpans, scrubText as v4 } from "../src/lib/scrub.js";

/** sha256 of fixtures/scrub-ruleset-3.ts (LF line endings), rule set 3's scrub.ts at fc10bb03. */
const V3_FIXTURE_SHA256 = "e912c396277d383ef07a77b5b3dbd54d9ad83e59d3d5d1bafdab62b2b619daa1";
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 2000);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 12345);

describe("rule set 3 vs 4 within the scan bound (differential fuzz)", () => {
  it("runs against the frozen rule set 3 source", () => {
    const fixture = readFileSync(
      fileURLToPath(new URL("./fixtures/scrub-ruleset-3.ts", import.meta.url).href),
      "utf8",
    ).replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe(V3_FIXTURE_SHA256);
  });

  it(`scrubs ${INPUTS} random inputs byte-identically wherever every value closes within 4096 units`, () => {
    let seed = SEED;
    const rnd = (n: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return Math.floor((seed / 0x80000000) * n);
    };
    const pick = <T,>(a: readonly T[]) => a[rnd(a.length)] as T;
    const keys = ['"password"', "'token'", "`secret`", '\\"api_key\\"', '\\\\\\"auth\\\\\\"', '"name"', '"access_token"', '"db password"'];
    const seps = [": ", ":", " = ", " => ", "="];
    const quotes = ['"', "'", "`", '\\"', '\\\\\\"', ""];
    const atoms = ["a b", '\\"', "\\\\", '"', "'", "`", "\n", "\r\n", "\r", "\u2028", "\u2029", "\u0085", " ", "op://", "capy_" + "Ab1".repeat(7), "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", ",", "{", "}", ":", "="];
    const breaks = ["\n", "\r\n", "\r", "\u2028", "\u2029", "\u0085"];
    const filler = (n: number) => {
      let s = "";
      while (s.length < n) s += rnd(10) < 7 ? "v".repeat(1 + rnd(300)) : pick(atoms);
      return s.slice(0, n);
    };
    const value = () => {
      const r = rnd(12);
      if (r === 10) {
        // Multi-line: a short first line, then one or more breaks; the total lands on
        // either side of the bound, so it crosses it (if at all) only after a break.
        let s = "v".repeat(rnd(40));
        const total = 4080 + rnd(40) + (rnd(2) ? rnd(3000) : 0);
        while (s.length < total) s += pick(breaks) + "v".repeat(1 + rnd(2500));
        return s.slice(0, total);
      }
      const n = r < 6 ? rnd(40) : r < 9 ? rnd(4097) : r === 9 ? 4090 + rnd(7) : rnd(8000);
      return filler(n);
    };

    let unexpected = 0;
    let first = "";
    let differ = 0;
    let withSpan = 0;
    let acrossLines = 0;
    for (let k = 0; k < INPUTS; k += 1) {
      let text = "";
      let earliest = Infinity;
      for (let p = 1 + rnd(3); p > 0; p -= 1) {
        const q = pick(quotes);
        text += pick(["", " ", "\n", "{", ", "]) + pick(keys) + pick(seps) + q;
        earliest = Math.min(earliest, text.length);
        text += value() + (rnd(5) < 4 ? q : "");
      }
      const found = overCapQuotedValueSpans(text);
      // A span runs from a key start past more than 4096 units of value.
      if (found.some(([start, end]) => end - start <= 4096)) {
        unexpected += 1;
        if (!first) first = `input ${k} (seed ${SEED}): short span`;
      }
      const spans = found.length > 0;
      if (spans) withSpan += 1;
      if (v3(text) === v4(text)) continue;
      differ += 1;
      // Over the bound only in total, every line within it: what rule set 4 caps across lines.
      if (!text.split(/\r\n|[\r\n\u2028\u2029\u0085]/).some((line) => line.length > 4096)) acrossLines += 1;
      // Allowed only for an over-bound value, which needs more than 4096 units after its opening quote.
      if (spans && text.length - earliest > 4096) continue;
      unexpected += 1;
      if (!first) first = `input ${k} (seed ${SEED}), ${text.length} units`;
    }
    console.log(`scrub fuzz: ${INPUTS} inputs, ${withSpan} with an over-bound value, ${differ} differ from rule set 3 (${acrossLines} with every line within 4096 units), ${unexpected} unexpected`);
    // Inputs are random filler, never secrets; the message names the input, not its text.
    expect(unexpected, first).toBe(0);
    // The fuzz reaches over-bound values, multi-line ones included, and their differences.
    expect(withSpan).toBeGreaterThan(0);
    expect(differ).toBeGreaterThan(0);
    expect(acrossLines).toBeGreaterThan(0);
  }, 120_000);
});
