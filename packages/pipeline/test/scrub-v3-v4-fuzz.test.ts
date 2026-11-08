/**
 * Rule set 4 promise: a quoted value within the scan bound (`MAX_QUOTED_VALUE`, 4096
 * units) scrubs byte-identically to rule set 3. This differential fuzz runs rule set 3's
 * `lib/scrub.ts` (read from git at its last commit, `fc10bb03`) against the current one
 * on random key/value inputs, and allows a difference only where some line holds more
 * than 4096 units (where an over-bound value can be). 2,000 inputs by default;
 * SCRUB_FUZZ_INPUTS raises it, SCRUB_FUZZ_SEED changes the seed. Skipped when the
 * rule set 3 source is not in the clone (a shallow checkout).
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { scrubText as v4 } from "../src/lib/scrub.js";

const V3_COMMIT = "fc10bb03";
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 2000);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 12345);

function v3Source(): string | null {
  try {
    return execFileSync("git", ["show", `${V3_COMMIT}:packages/pipeline/src/lib/scrub.ts`], {
      cwd: resolve(__dirname, ".."),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      maxBuffer: 8 * 1024 * 1024,
    });
  } catch {
    return null;
  }
}

const source = v3Source();
const dir = source ? mkdtempSync(join(tmpdir(), "scrub-v3-")) : "";
afterAll(() => {
  if (dir) rmSync(dir, { recursive: true, force: true });
});

describe.skipIf(!source)("rule set 3 vs 4 within the scan bound (differential fuzz)", () => {
  it(`scrubs ${INPUTS} random inputs byte-identically wherever no line passes 4096 units`, async () => {
    const file = join(dir, "scrub-v3.ts");
    writeFileSync(file, source!);
    const { scrubText: v3 } = (await import(pathToFileURL(file).href)) as { scrubText: (s: string) => string };

    let seed = SEED;
    const rnd = (n: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return Math.floor((seed / 0x80000000) * n);
    };
    const pick = <T,>(a: readonly T[]) => a[rnd(a.length)] as T;
    const keys = ['"password"', "'token'", "`secret`", '\\"api_key\\"', '\\\\\\"auth\\\\\\"', '"name"', '"access_token"', '"db password"'];
    const seps = [": ", ":", " = ", " => ", "="];
    const quotes = ['"', "'", "`", '\\"', '\\\\\\"', ""];
    const atoms = ["a b", '\\"', "\\\\", '"', "'", "`", "\n", "\r\n", "\r", " ", "op://", "capy_" + "Ab1".repeat(7), "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", ",", "{", "}", ":", "="];
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
