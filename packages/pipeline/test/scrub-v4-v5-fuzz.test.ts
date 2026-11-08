/**
 * Rule set 5 promise: output equals rule set 4's except where rule set 5 redacts more, and
 * rule set 5 never emits text that rule set 4 redacted. This differential fuzz runs rule
 * set 4's `lib/scrub.ts` against the current one.
 *
 * - Text: random quoted-key/value inputs, including over-cap multi-line values made of
 *   many shrinkable `capy_` tokens (which rule set 4 measured only after the token rules
 *   had shrunk them), values whose total length lands either side of the bound, and
 *   values within the bound that span line breaks (CR, LF, CRLF, U+2028, U+2029, U+0085)
 *   or are left unclosed. Output must be byte-identical unless the original text holds an
 *   over-bound value, a quoted secret value within the bound that holds a line break, or
 *   one with no matching close (`unsplittableQuotedValueSpans` with `prefix: true`): rule
 *   set 5 redacts those whole, where rule set 4 stopped at the first line's whitespace.
 * - Records: random `scrubJson` objects, objects and arrays nested under secret keys
 *   included. With no secret key at any depth output must be byte-identical; with secret
 *   keys, only the values below them may differ.
 * - Both: every input carries unique marker words; any marker absent from rule set 4's
 *   output must be absent from rule set 5's (rule set 5 never re-emits a redacted byte).
 *
 * 2,000 inputs each by default; SCRUB_FUZZ_INPUTS raises it, SCRUB_FUZZ_SEED changes the seed.
 *
 * Rule set 4 is frozen in `fixtures/scrub-ruleset-4.ts`: byte for byte
 * `git show 5cc6e9db:packages/pipeline/src/lib/scrub.ts` (it has no imports), pinned by
 * its sha256 below. Never edit the fixture: a rule set that has shipped does not change.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { scrubJson as v4Json, scrubText as v4 } from "./fixtures/scrub-ruleset-4.js";
import {
  isSecretKeyName,
  overCapQuotedValueSpans,
  scrubJson as v5Json,
  scrubText as v5,
  unsplittableQuotedValueSpans,
} from "../src/lib/scrub.js";

/** sha256 of fixtures/scrub-ruleset-4.ts (LF line endings), rule set 4's scrub.ts at 5cc6e9db. */
const V4_FIXTURE_SHA256 = "b0eb083bda55524542ce7ba04ccaecfc01d7891f13fb50a14a06fd61c80761ce";
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 2000);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 54321);

function rng(start: number) {
  let seed = start;
  const rnd = (n: number) => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return Math.floor((seed / 0x80000000) * n);
  };
  const pick = <T,>(a: readonly T[]) => a[rnd(a.length)] as T;
  return { rnd, pick };
}

/** A plain lowercase word no rule matches on its own; unique per call. */
function markers() {
  let n = 0;
  const all: string[] = [];
  const next = () => {
    let k = n++;
    let s = "";
    do {
      s += String.fromCharCode(97 + (k % 26));
      k = Math.floor(k / 26);
    } while (k > 0);
    const m = `zqmark${s}zq`;
    all.push(m);
    return m;
  };
  return { next, all };
}

const CAPY = "capy_" + "Ab1".repeat(12);

describe("rule set 4 vs 5 (differential fuzz)", () => {
  it("runs against the frozen rule set 4 source", () => {
    const fixture = readFileSync(
      fileURLToPath(new URL("./fixtures/scrub-ruleset-4.ts", import.meta.url).href),
      "utf8",
    ).replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe(V4_FIXTURE_SHA256);
  });

  it(`scrubs ${INPUTS} random texts as rule set 4 does, except over-bound, multi-line or unclosed values, never re-emitting a redacted marker`, () => {
    const { rnd, pick } = rng(SEED);
    const keys = ['"password"', "'token'", "`secret`", '\\"api_key\\"', '\\\\\\"auth\\\\\\"', '"name"', '"access_token"', '"db password"', '"note"'];
    const seps = [": ", ":", " = ", " => ", "="];
    const quotes = ['"', "'", "`", '\\"', '\\\\\\"', ""];
    const atoms = ["a b", '\\"', "\\\\", '"', "'", "`", "\n", "\r\n", "\r", " ", " ", "\u0085", " ", "op://", CAPY, `${CAPY} `, "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", ",", "{", "}", ":", "="];
    const breaks = ["\n", "\r\n", "\r", " ", " ", "\u0085"];

    let unexpected = 0;
    let leaked = 0;
    let first = "";
    let differ = 0;
    let withSpan = 0;
    let multiLine = 0;
    let multiLineDiffer = 0;
    let shrinkCases = 0;
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      const filler = (n: number) => {
        let s = "";
        while (s.length < n) {
          const r = rnd(20);
          s += r < 13 ? "v".repeat(1 + rnd(300)) : r === 13 ? ` ${mk.next()} ` : pick(atoms);
        }
        return s.slice(0, n);
      };
      const value = () => {
        const r = rnd(14);
        if (r >= 12) {
          // Over the bound on the original, under it once the capy tokens shrink, then a marker line.
          shrinkCases += 1;
          const count = 105 + rnd(40);
          return `${CAPY} `.repeat(count) + pick(breaks) + mk.next() + (rnd(2) ? pick(breaks) + mk.next() : "");
        }
        if (r === 10) {
          let s = "v".repeat(rnd(40));
          const total = 4080 + rnd(40) + (rnd(2) ? rnd(3000) : 0);
          while (s.length < total) s += pick(breaks) + (rnd(3) ? "v".repeat(1 + rnd(2500)) : mk.next());
          return s.slice(0, total);
        }
        const n = r < 6 ? rnd(40) : r < 9 ? rnd(4097) : r === 9 ? 4090 + rnd(7) : rnd(8000);
        return filler(n);
      };

      let text = "";
      for (let p = 1 + rnd(3); p > 0; p -= 1) {
        const q = pick(quotes);
        text += pick(["", " ", "\n", "{", ", "]) + pick(keys) + pick(seps) + q;
        text += value() + (rnd(5) < 4 ? q : "") + (rnd(3) ? "" : ` ${mk.next()}`);
      }
      const a = v4(text);
      const b = v5(text);
      // Rule set 5 never emits a marker rule set 4 redacted.
      const reEmitted = mk.all.filter((m) => !a.includes(m) && b.includes(m));
      if (reEmitted.length > 0) {
        leaked += 1;
        if (!first) first = `input ${k} (seed ${SEED}): re-emitted ${reEmitted.length} marker(s)`;
      }
      const overBound = overCapQuotedValueSpans(text).length;
      if (overBound > 0) withSpan += 1;
      // Rule set 5 also redacts whole a value within the bound that holds a line break
      // (an extra span here) or has no matching close (open to the end as a prefix).
      const whole = unsplittableQuotedValueSpans(text, { prefix: true });
      const lineOrOpen = whole.length > overBound || (whole.at(-1)?.[1] ?? -1) >= text.length;
      if (unsplittableQuotedValueSpans(text).length > overBound) multiLine += 1;
      if (a === b) continue;
      differ += 1;
      if (overBound > 0) continue;
      if (lineOrOpen) {
        multiLineDiffer += 1;
        continue;
      }
      unexpected += 1;
      if (!first) first = `input ${k} (seed ${SEED}), ${text.length} units, no over-bound, multi-line or unclosed value`;
    }
    console.log(`scrub v4/v5 text fuzz: ${INPUTS} inputs, ${shrinkCases} shrinkable over-cap values, ${withSpan} with an over-bound value, ${multiLine} with a multi-line value within the bound, ${differ} differ from rule set 4 (${multiLineDiffer} only by a multi-line or unclosed value), ${unexpected} unexpected, ${leaked} re-emitting a redacted marker`);
    expect(unexpected, first).toBe(0);
    expect(leaked, first).toBe(0);
    expect(withSpan).toBeGreaterThan(0);
    expect(multiLine).toBeGreaterThan(0);
    expect(multiLineDiffer).toBeGreaterThan(0);
    expect(differ).toBeGreaterThan(0);
  }, 300_000);

  it(`scrubs ${INPUTS} random records as rule set 4 does, except secret-keyed values, never re-emitting a redacted marker`, () => {
    const { rnd, pick } = rng(SEED ^ 0x5a5a);
    const plainKeys = ["name", "tokens", "author", "oauth", "key", "content", "type", "note", "path"];
    const secretKeys = ["password", "token", "api_key", "client_secret", "x-auth-token", "db_password", "apiKey", " secret "];
    const strAtoms = ["a b", "op://vault/item/field", "[REDACTED]", "", CAPY, "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", "\n", "ok", "Bearer abc"];

    let unexpected = 0;
    let leaked = 0;
    let first = "";
    let differ = 0;
    let withSecret = 0;
    let nestedUnderSecret = 0;
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      const allowSecret = rnd(2) === 0;
      let hasSecret = false;
      let nested = false;
      const str = () => {
        const r = rnd(6);
        return r === 0 ? mk.next() : r === 1 ? `${pick(strAtoms)} ${mk.next()}` : pick(strAtoms);
      };
      const node = (depth: number): unknown => {
        const r = rnd(10);
        if (depth > 3 || r < 4) return r === 0 ? rnd(1000) : r === 1 ? null : r === 2 ? true : str();
        if (r < 6) return Array.from({ length: rnd(4) }, () => node(depth + 1));
        const o: Record<string, unknown> = {};
        for (let i = rnd(4); i >= 0; i -= 1) {
          let key = pick(plainKeys);
          let secret = false;
          if (allowSecret && rnd(3) === 0) {
            key = pick(secretKeys);
            hasSecret = true;
            secret = true;
          }
          // Under a secret key, an object (with plain keys, so only the inherited secret
          // context can redact it) half the time.
          const child = secret && rnd(2) === 0 ? { [pick(plainKeys)]: node(depth + 1), [pick(plainKeys)]: str() } : node(depth + 1);
          if (secret && child && typeof child === "object" && !Array.isArray(child)) nested = true;
          o[key] = child;
        }
        return o;
      };
      const input = node(0);
      if (hasSecret) withSecret += 1;
      if (nested) nestedUnderSecret += 1;
      const a = JSON.stringify(v4Json(input));
      const b = JSON.stringify(v5Json(input));
      const reEmitted = mk.all.filter((m) => !a.includes(m) && b.includes(m));
      if (reEmitted.length > 0) {
        leaked += 1;
        if (!first) first = `record ${k} (seed ${SEED}): re-emitted ${reEmitted.length} marker(s)`;
      }
      if (a === b) continue;
      differ += 1;
      if (hasSecret) continue;
      unexpected += 1;
      if (!first) first = `record ${k} (seed ${SEED}): differs with no secret key`;
    }
    console.log(`scrub v4/v5 record fuzz: ${INPUTS} records, ${withSecret} with a secret key, ${nestedUnderSecret} with an object under one, ${differ} differ from rule set 4, ${unexpected} unexpected, ${leaked} re-emitting a redacted marker`);
    expect(isSecretKeyName("password")).toBe(true);
    expect(unexpected, first).toBe(0);
    expect(leaked, first).toBe(0);
    expect(withSecret).toBeGreaterThan(0);
    expect(nestedUnderSecret).toBeGreaterThan(0);
    expect(differ).toBeGreaterThan(0);
  }, 300_000);
});
