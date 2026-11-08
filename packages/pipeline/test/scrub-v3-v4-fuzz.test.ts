/**
 * Rule set 4 promise: a quoted value within the scan bound (`MAX_QUOTED_VALUE`, 4096
 * units) scrubs byte-identically to rule set 3. This differential fuzz runs rule set 3's
 * `lib/scrub.ts` against the current one on random key/value inputs, and allows a
 * difference only where some line holds more than 4096 units (where an over-bound value
 * can be: a value is over the bound only when its key's own line runs 4096 units past
 * its opening quote, though from there it runs across later line breaks, CR, LF, CRLF,
 * U+2028, U+2029 and U+0085 alike, to its matching closing quote or the end of input).
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
import { scrubText as v4 } from "../src/lib/scrub.js";

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

  it(`scrubs ${INPUTS} random inputs byte-identically wherever no line passes 4096 units`, () => {
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
    const value = () => {
      const r = rnd(10);
      const n = r < 6 ? rnd(40) : r < 9 ? rnd(4097) : 4090 + rnd(7);
      let s = "";
      while (s.length < n) s += rnd(10) < 7 ? "v".repeat(1 + rnd(300)) : pick(atoms);
      return s.slice(0, n);
    };

    let unexpected = 0;
    let first = "";
    for (let k = 0; k < INPUTS; k += 1) {
      let text = "";
      for (let p = 1 + rnd(3); p > 0; p -= 1) {
        const q = pick(quotes);
        text += pick(["", " ", "\n", "{", ", "]) + pick(keys) + pick(seps) + q + value() + (rnd(5) < 4 ? q : "");
      }
      if (v3(text) === v4(text)) continue;
      if (text.split(/\r\n|[\r\n]/).some((line) => line.length > 4096)) continue;
      unexpected += 1;
      if (!first) first = `input ${k} (seed ${SEED}), ${text.length} units`;
    }
    // Inputs are random filler, never secrets; the message names the input, not its text.
    expect(unexpected, first).toBe(0);
  }, 120_000);
});
