/**
 * Rule set 6 promise: output equals rule set 5's except on the two shapes rule set 6
 * corrects, and there rule set 6 neither keeps a secret byte rule set 5 redacted nor
 * drops text after a private-key block that it can keep. This differential fuzz runs rule
 * set 5's `lib/scrub.ts` (and the raw-chunk record path on top of it) against the current
 * one.
 *
 * - Defect 1 (leak): an unquoted value after a quoted secret key of `MAX_QUOTED_VALUE`
 *   units or more. Rule set 5 classified its first 4096 units only (4096 digits then a
 *   tail passed as a number); rule set 6 redacts it whole. Detected as an extra
 *   `overCapQuotedValueSpans` span under rule set 6. On such input rule set 6 only
 *   redacts more: it never emits a marker rule set 5 redacted.
 * - Defect 2 (data loss): a private-key header or footer paired across the edge of a
 *   quoted secret value. Rule set 5's over-cap pass took the footer with the value, so the
 *   private-key rule redacted from the header to the end of the input (and a value holding
 *   only the footer, within the bound, kept its tail). On input holding a private-key
 *   boundary, rule set 6 may emit a marker rule set 5 dropped only after the first
 *   boundary, and never emits more private-key headers or footers than rule set 5.
 * - Everything else: byte-identical, in `scrubText`, `scrubJson` and `scrubRecords`.
 * - The explicit proof below: rule set 5 LEAKS `SECRETTAIL` (defect 1, under `password`,
 *   `api_key` and `token`) and LOSES `HISTORY_TAIL`/`KEEPTRAIL` (defect 2) in `scrubText`,
 *   `scrubJson` and `scrubRecords`; rule set 6 does neither.
 *
 * 2,000 inputs each by default; SCRUB_FUZZ_INPUTS raises it, SCRUB_FUZZ_SEED changes the seed.
 *
 * Rule set 5 is frozen in `fixtures/scrub-ruleset-5.ts`: byte for byte
 * `git show 62a7996d:packages/pipeline/src/lib/scrub.ts` (it has no imports), pinned by
 * its sha256 below. Never edit the fixture: a rule set that has shipped does not change.
 * Rule set 5's `scrubRecords` is the current raw-chunks.ts (its segmenter is unchanged in
 * rule set 6) loaded with `lib/scrub.ts` mocked to that fixture.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import {
  overCapQuotedValueSpans as v5Spans,
  scrubJson as v5Json,
  scrubText as v5,
} from "./fixtures/scrub-ruleset-5.js";
import {
  MAX_QUOTED_VALUE,
  overCapQuotedValueSpans as v6Spans,
  scrubJson as v6Json,
  scrubText as v6,
} from "../src/lib/scrub.js";
import { scrubRecords as v6RecordsRaw } from "../src/lib/pipeline/raw-chunks.js";

/** sha256 of fixtures/scrub-ruleset-5.ts (LF line endings), rule set 5's scrub.ts at 62a7996d. */
const V5_FIXTURE_SHA256 = "8fbf9136b61d8fae03f076f87e514f910bfcfc03841313f550f5bdcb861d4de1";
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 2000);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 65432);

const enc = new TextEncoder();
const dec = new TextDecoder();
type Records = (input: Uint8Array) => { scrubbed: Uint8Array };
let v5RecordsRaw: Records;
const v5Records = (text: string) => dec.decode(v5RecordsRaw(enc.encode(text)).scrubbed);
const v6Records = (text: string) => dec.decode(v6RecordsRaw(enc.encode(text)).scrubbed);

beforeAll(async () => {
  vi.resetModules();
  vi.doMock("../src/lib/scrub.js", () => import("./fixtures/scrub-ruleset-5.js"));
  const mod = await import("../src/lib/pipeline/raw-chunks.js");
  v5RecordsRaw = mod.scrubRecords as Records;
  vi.doUnmock("../src/lib/scrub.js");
  vi.resetModules();
});

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

const H = "-----BEGIN RSA PRIVATE KEY-----";
const F = "-----END RSA PRIVATE KEY-----";
const PEM_ANY = /-----(?:BEGIN|END) [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/;
/** How many private-key headers and footers `text` holds. */
const boundaries = (text: string) => text.match(/-----(?:BEGIN|END) [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g)?.length ?? 0;
const CAPY = "capy_" + "Ab1".repeat(12);

/** Index of the first private-key header or footer in `text`, or -1. */
const firstBoundary = (text: string) => {
  const m = PEM_ANY.exec(text);
  return m ? m.index : -1;
};

/**
 * Compare one output pair. `allowed` is set when the input holds a defect shape; then a
 * marker rule set 5 dropped may come back only after the first private-key boundary, and
 * rule set 6 never emits more private-key headers or footers than rule set 5 (a lone
 * boundary outside every value is kept by both).
 */
function judge(text: string, a: string, b: string, markerList: string[], bareShape: boolean, pemShape: boolean) {
  const from = firstBoundary(text);
  const reEmitted = markerList.filter((m) => !a.includes(m) && b.includes(m) && !(pemShape && text.indexOf(m) > from));
  const dangling = boundaries(b) > boundaries(a);
  const unexpected = a !== b && !bareShape && !pemShape;
  return { reEmitted: reEmitted.length, dangling, unexpected };
}

describe("rule set 5 vs 6 (differential fuzz)", () => {
  it("runs against the frozen rule set 5 source", () => {
    const fixture = readFileSync(
      fileURLToPath(new URL("./fixtures/scrub-ruleset-5.ts", import.meta.url).href),
      "utf8",
    ).replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe(V5_FIXTURE_SHA256);
    // The fixture is the rule set 5 implementation, not the current one.
    const d = "7".repeat(MAX_QUOTED_VALUE);
    expect(v5(`"token": ${d}SECRETTAIL`)).toContain("SECRETTAIL");
    expect(v6(`"token": ${d}SECRETTAIL`)).not.toContain("SECRETTAIL");
  });

  it(`scrubs ${INPUTS} random texts as rule set 5 does, except the two corrected shapes`, () => {
    const { rnd, pick } = rng(SEED);
    const keys = ['"password"', "'token'", "`secret`", '\\"api_key\\"', '"name"', '"access_token"', '"api_key"', '"note"'];
    const seps = [": ", ":", " = ", " => ", "="];
    const quotes = ['"', "'", "`", '\\"', "", "", ""];
    const atoms = ["a b", '\\"', "\\\\", '"', "'", "\n", "\r\n", " ", " ", CAPY, "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", ",", "{", "}", ":", "7777"];

    let unexpected = 0;
    let leaked = 0;
    let dangling = 0;
    let first = "";
    let differ = 0;
    let bareCases = 0;
    let bareDiffer = 0;
    let pemCases = 0;
    let pemDiffer = 0;
    let recordsChecked = 0;
    let plain = 0;
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      const filler = (n: number) => {
        let s = "";
        while (s.length < n) {
          const r = rnd(22);
          s += r < 13 ? "v".repeat(1 + rnd(300)) : r === 13 ? ` ${mk.next()} ` : r === 14 && rnd(10) === 0 ? pick([`\n${H}\n`, `\n${F}\n`]) : pick(atoms);
        }
        return s.slice(0, n);
      };
      const value = (quoted: boolean) => {
        if (!quoted) {
          // Unquoted: a digit run either side of the bound, then maybe a tail; or a short token.
          const r = rnd(4);
          if (r === 0) return pick(["512", "true", "null", "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ", mk.next()]);
          const digits = "7".repeat(MAX_QUOTED_VALUE - 3 + rnd(7) - (r === 1 ? rnd(200) : 0));
          return digits + pick(["", mk.next(), `SECRETTAIL${mk.next()}`, "Zq8Xw2Lp"]);
        }
        const r = rnd(10);
        const n = r < 5 ? rnd(40) : r < 8 ? rnd(4097) : 4090 + rnd(14);
        return filler(n);
      };

      let text = rnd(12) === 0 ? `${mk.next()}\n${H}\n` : "";
      for (let p = 1 + rnd(3); p > 0; p -= 1) {
        const q = pick(quotes);
        text += pick(["", " ", "\n", "{", ", "]) + pick(keys) + pick(seps) + q;
        text += value(q !== "") + (rnd(10) === 0 ? `\n${F}` : "") + (q && rnd(5) < 4 ? q : "") + (rnd(3) ? "" : ` ${mk.next()}`);
        if (rnd(16) === 0) text += pick([`\n${H}\n`, `\n${F}\n`]);
      }
      const bareShape = v6Spans(text).length > v5Spans(text).length;
      const pemShape = PEM_ANY.test(text);
      if (bareShape) bareCases += 1;
      if (pemShape) pemCases += 1;
      if (!bareShape && !pemShape) plain += 1;
      const outs: Array<[string, string]> = [[v5(text), v6(text)]];
      // The raw-chunk record path on a slice of the inputs (it decodes and re-scrubs; slower).
      if (k % 4 === 0) {
        recordsChecked += 1;
        outs.push([v5Records(`${text}\n`), v6Records(`${text}\n`)]);
        const line = `${JSON.stringify({ type: "user", message: { content: [{ type: "tool_result", content: text }] } })}\n`;
        outs.push([v5Records(line), v6Records(line)]);
      }
      let differs = false;
      for (const [a, b] of outs) {
        const j = judge(text, a, b, mk.all, bareShape, pemShape);
        if (a !== b) differs = true;
        if (j.reEmitted > 0) leaked += 1;
        if (j.dangling) dangling += 1;
        if (j.unexpected) unexpected += 1;
        if (!first && (j.reEmitted > 0 || j.dangling || j.unexpected)) {
          first = `input ${k} (seed ${SEED}), ${text.length} units: reEmitted ${j.reEmitted}, dangling ${j.dangling}, unexpected ${j.unexpected}`;
        }
      }
      if (!differs) continue;
      differ += 1;
      if (bareShape) bareDiffer += 1;
      else if (pemShape) pemDiffer += 1;
    }
    console.log(`scrub v5/v6 text fuzz: ${INPUTS} inputs (${recordsChecked} also through scrubRecords), ${bareCases} with an over-bound bare value, ${pemCases} with a private-key boundary, ${plain} with neither (byte-identical required), ${differ} differ from rule set 5 (${bareDiffer} by an over-bound bare value, ${pemDiffer} by a private-key boundary), ${unexpected} unexpected, ${leaked} re-emitting a redacted marker, ${dangling} with a dangling boundary`);
    expect(unexpected, first).toBe(0);
    expect(leaked, first).toBe(0);
    expect(dangling, first).toBe(0);
    expect(bareCases).toBeGreaterThan(0);
    expect(plain).toBeGreaterThan(INPUTS / 4);
    expect(bareDiffer).toBeGreaterThan(0);
    expect(pemDiffer).toBeGreaterThan(0);
  }, 300_000);

  it(`scrubs ${INPUTS} random records as rule set 5 does, except the two corrected shapes`, () => {
    const { rnd, pick } = rng(SEED ^ 0x5a5a);
    const plainKeys = ["name", "tokens", "author", "content", "type", "note", "path"];
    const secretKeys = ["password", "token", "api_key", "client_secret"];
    const digits = () => "7".repeat(MAX_QUOTED_VALUE - 2 + rnd(5));

    let unexpected = 0;
    let leaked = 0;
    let dangling = 0;
    let first = "";
    let differ = 0;
    let shaped = 0;
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      const strings: string[] = [];
      const str = (): string => {
        const r = rnd(9);
        const s =
          r === 0 ? mk.next()
          : r === 1 ? `"${pick(secretKeys)}": ${digits()}${pick(["", mk.next()])} ${mk.next()}`
          : r === 2 ? `${H}\n"password": "${"a".repeat(rnd(2) ? 20 : MAX_QUOTED_VALUE + 1)}\n${F}" ${mk.next()}`
          : r === 3 ? `"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}\n${H}\nab" ${mk.next()}\n${F}\n${mk.next()}`
          : r === 4 ? `"${pick(plainKeys)}": ${digits()} ${mk.next()}`
          : `${pick(["a b", "ok", CAPY, "512"])} ${mk.next()}`;
        strings.push(s);
        return s;
      };
      const node = (depth: number): unknown => {
        const r = rnd(10);
        if (depth > 3 || r < 4) return r === 0 ? rnd(1000) : r === 1 ? null : str();
        if (r < 6) return Array.from({ length: rnd(4) }, () => node(depth + 1));
        const o: Record<string, unknown> = {};
        for (let i = rnd(4); i >= 0; i -= 1) o[rnd(4) === 0 ? pick(secretKeys) : pick(plainKeys)] = node(depth + 1);
        return o;
      };
      const input = node(0);
      const bareShape = strings.some((s) => v6Spans(s).length > v5Spans(s).length);
      const pemShape = strings.some((s) => PEM_ANY.test(s));
      if (bareShape || pemShape) shaped += 1;
      const line = `${JSON.stringify(input)}\n`;
      const pairs: Array<[string, string]> = [
        [JSON.stringify(v5Json(input)), JSON.stringify(v6Json(input))],
        [v5Records(line), v6Records(line)],
      ];
      let differs = false;
      for (const [a, b] of pairs) {
        if (a !== b) differs = true;
        // Markers are only ever compared within the string they were placed in, so the
        // boundary rule here is per input: with a boundary anywhere, no ordering is checked.
        const reEmitted = mk.all.filter((m) => !a.includes(m) && b.includes(m)).length;
        if (reEmitted > 0 && !pemShape) leaked += 1;
        if (boundaries(b) > boundaries(a)) dangling += 1;
        if (a !== b && !bareShape && !pemShape) unexpected += 1;
        if (!first && ((reEmitted > 0 && !pemShape) || (a !== b && !bareShape && !pemShape) || boundaries(b) > boundaries(a))) {
          first = `record ${k} (seed ${SEED}): reEmitted ${reEmitted}`;
        }
      }
      if (differs) differ += 1;
    }
    console.log(`scrub v5/v6 record fuzz: ${INPUTS} records, ${shaped} with a corrected shape, ${differ} differ from rule set 5, ${unexpected} unexpected, ${leaked} re-emitting a redacted marker, ${dangling} with a dangling boundary`);
    expect(unexpected, first).toBe(0);
    expect(leaked, first).toBe(0);
    expect(dangling, first).toBe(0);
    expect(shaped).toBeGreaterThan(0);
    expect(differ).toBeGreaterThan(0);
  }, 300_000);
});

describe("rule set 5 fails and rule set 6 passes on both defects", () => {
  const R = "[REDACTED]";
  const record = (content: string) => `${JSON.stringify({ type: "user", message: { content: [{ type: "tool_result", content }] } })}\n`;
  /** scrubText, scrubJson (a string under a plain key) and scrubRecords (raw line and record line). */
  const paths = (text: string, json: typeof v6Json, text_: typeof v6, records: (t: string) => string) => ({
    scrubText: text_(text),
    scrubJson: (json({ content: text }) as { content: string }).content,
    "scrubRecords raw": records(`${text}\n`),
    "scrubRecords record": records(record(text)),
  });

  for (const key of ["password", "api_key", "token"]) {
    for (const n of [MAX_QUOTED_VALUE - 1, MAX_QUOTED_VALUE, MAX_QUOTED_VALUE + 1]) {
      it(`defect 1: "${key}": ${n} digits then SECRETTAIL`, () => {
        const text = `"${key}": ${"7".repeat(n)}SECRETTAILx9Q2 KEEPTRAIL`;
        for (const [path, out] of Object.entries(paths(text, v5Json, v5, v5Records))) {
          expect(out.includes("SECRETTAIL"), `rule set 5 ${path} leaks SECRETTAIL`).toBe(true);
        }
        for (const [path, out] of Object.entries(paths(text, v6Json, v6, v6Records))) {
          expect(out.includes("SECRETTAIL"), `rule set 6 ${path}`).toBe(false);
          expect(out.includes("KEEPTRAIL"), `rule set 6 ${path} keeps KEEPTRAIL`).toBe(true);
          expect(out.includes("7777"), `rule set 6 ${path}`).toBe(false);
        }
        expect(v6(text)).toBe(`"${key}": ${R} KEEPTRAIL`);
      });
    }
  }

  it("defect 2: header, over-cap \"password\" value holding the footer, closing quote, HISTORY_TAIL", () => {
    const text = `${H}\n"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}${F}" HISTORY_TAIL KEEPTRAIL\n"user": "bob"`;
    for (const [path, out] of Object.entries(paths(text, v5Json, v5, v5Records))) {
      expect(out.includes("HISTORY_TAIL") || out.includes("KEEPTRAIL"), `rule set 5 ${path} loses the tail`).toBe(false);
    }
    expect(v5(text)).toBe(R);
    expect(v5Records(`${text}\n`)).toBe(R);
    for (const [path, out] of Object.entries(paths(text, v6Json, v6, v6Records))) {
      expect(out.includes("HISTORY_TAIL") && out.includes("KEEPTRAIL") && out.includes("bob"), `rule set 6 ${path} keeps the tail`).toBe(true);
      expect(out.includes("aaaaaaaaaaaa") || PEM_ANY.test(out), `rule set 6 ${path} keeps no key material`).toBe(false);
    }
    expect(v6(text)).toBe(`${R} HISTORY_TAIL KEEPTRAIL\n"user": "bob"`);
  });
});
