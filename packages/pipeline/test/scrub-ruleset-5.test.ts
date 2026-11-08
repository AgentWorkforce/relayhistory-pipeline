/**
 * Rule set 5: two leaks reproduced in rule set 4 (relayscrubber review).
 *
 * 1. Key context on the record path. `scrubJson`, `scrubRecord` and `scrubRecords`
 *    tokenized a record value by value, so a string under a secret key name
 *    (`isSecretKeyName`) was scrubbed alone, with no key in view, and kept unless it
 *    matched a provider shape: `{"password":"<arbitrary>"}` was stored as it was.
 * 2. Over-cap quoted values shrunk by an earlier rule. A multi-line quoted secret value
 *    over `MAX_QUOTED_VALUE` (4096 units) made of provider-shaped tokens was shrunk under
 *    the bound by the token rules before the quoted rule measured it, which then took it
 *    for an ordinary value and kept everything after its first line.
 * 3. Multi-line quoted values within the bound (relayscrubber PASS gate, blocker 1). A
 *    quoted secret value within `MAX_QUOTED_VALUE` that held a CR, LF or CRLF before its
 *    close, or had no close, was redacted only up to the first whitespace (rule set 3's
 *    fallback); the rest of the value, later lines included, was kept. It is now redacted
 *    through its matching close, or to the end of the input when unterminated, across
 *    every line break (CR, LF, CRLF, U+2028, U+2029, U+0085), and the raw-chunk segmenter
 *    never cuts inside such a value.
 * 4. Composite values under a secret key (blocker 2). `{"password":{"value":"..."}}`,
 *    `{"credentials":[{"value":"..."}]}`: an object or array under a secret key reset the
 *    secret context, so a string below it was kept unless its own key was a secret name.
 *    The context now runs through every descendant, in `scrubJson` and the raw-chunk
 *    record-line tokenizer alike.
 * 5. Escaped line separators in an `op://` value (exact-head review of ff5fc268). A value
 *    starting `op://` was kept unless it held a literal line separator, so one that held
 *    it as an escape sequence (`op://vault/item\nSECRETTAIL`, the two characters `\` and
 *    `n`, in nested or escaped JSON) was kept tail and all, in `scrubText`, `scrubJson`
 *    and `scrubRecords`. Only a complete one-line reference (`OP_REFERENCE`, the whole
 *    value) is kept now; any other value starting `op://` is redacted whole.
 *
 * Every value here is synthetic; `SECRETTAIL`/`SECRETTAILMARK` mark the bytes that must
 * not survive.
 */
/*
 * Rule set 5 shipped: production objects are sealed under it (source digest ed8d5c35…), so
 * its behaviour is frozen. This file runs every case against the frozen source
 * (`fixtures/scrub-ruleset-5.ts`, byte for byte `git show 62a7996d:packages/pipeline/src/lib/scrub.ts`,
 * pinned in scrub-v5-v6-fuzz.test.ts) with the raw-chunk paths (`scrubLong`, `scrubRecords`)
 * wired to it, and again against the current `lib/scrub.ts`, which must keep all of it.
 * PR #354 edited lib/scrub.ts in place under version 5 and added two tests here asserting
 * the new behaviour (a bare value classified whole, a PEM footer inside an over-cap value);
 * rule set 6 ships those fixes, so they live in scrub-ruleset-6.test.ts, as does the shared
 * corpus's record-path run (the corpus holds rule set 6 cases). Likewise #362's chained-PEM
 * test (the frozen rule set 5 source redacts that input to the end), now in
 * scrub-ruleset-6.test.ts.
 */
import { describe, expect, it, vi } from "vitest";
import * as currentChunks from "../src/lib/pipeline/raw-chunks.js";
import * as currentScrub from "../src/lib/scrub.js";
import * as frozenScrub from "./fixtures/scrub-ruleset-5.js";
import { scrubJson as v4Json, scrubText as v4Text } from "./fixtures/scrub-ruleset-4.js";

// raw-chunks.ts with its scrub.js import bound to the frozen rule set 5 source (as in
// scrub-v5-v6-fuzz.test.ts); the segmenter is unchanged between rule sets 5 and 6.
vi.resetModules();
vi.doMock("../src/lib/scrub.js", () => import("./fixtures/scrub-ruleset-5.js"));
const frozenChunks = await import("../src/lib/pipeline/raw-chunks.js");
vi.doUnmock("../src/lib/scrub.js");
vi.resetModules();

type Scrub = Pick<
  typeof frozenScrub,
  | "MAX_QUOTED_VALUE"
  | "MAX_SCRUB_CHARS"
  | "OP_REFERENCE"
  | "scrubJson"
  | "scrubRecord"
  | "scrubText"
  | "unsplittableQuotedValueSpans"
>;
type Chunks = Pick<typeof currentChunks, "scrubLong" | "scrubRecords">;

const IMPLEMENTATIONS: Array<[string, Scrub, Chunks]> = [
  ["frozen rule set 5 (fixtures/scrub-ruleset-5.ts)", frozenScrub, frozenChunks],
  ["current lib/scrub.ts (keeps rule set 5's fixes)", currentScrub, currentChunks],
];

const R = "[REDACTED]";
const enc = new TextEncoder();
const dec = new TextDecoder();

/** A synthetic Capy-shaped token (the `capy_` rule matches it; it is not a credential). */
const CAPY = `capy_${"Ab1".repeat(12)}`;
/** Leak 2: 130 Capy-shaped tokens (over the bound) then a line break and the tail. */
const SHRINKABLE = `${`${CAPY} `.repeat(130)}\nSECRETTAILMARK`;

describe("the frozen rule set 5 binding", () => {
  it("is rule set 5, raw-chunk paths included, not the current source", () => {
    // Rule set 5 classified a bare value on its first 4096 units: this tail was kept.
    const text = `"token": ${"7".repeat(4096)}SECRETTAIL KEEPTRAIL`;
    expect(frozenScrub.scrubText(text)).toBe(text);
    expect(frozenChunks.scrubLong(text)).toBe(text);
    expect(dec.decode(frozenChunks.scrubRecords(enc.encode(`${text}\n`)).scrubbed)).toBe(`${text}\n`);
    expect(currentScrub.scrubText(text)).toBe(`"token": ${R} KEEPTRAIL`);
    expect(currentChunks.scrubLong(text)).toBe(`"token": ${R} KEEPTRAIL`);
  });
});

for (const [implementation, scrub, chunks] of IMPLEMENTATIONS) {
  describe(implementation, () => ruleSet5(scrub, chunks));
}

/** Every rule set 5 case, against one implementation. */
function ruleSet5(scrub: Scrub, chunks: Chunks) {
  const {
    MAX_QUOTED_VALUE,
    MAX_SCRUB_CHARS,
    OP_REFERENCE,
    scrubJson,
    scrubRecord,
    scrubText,
    unsplittableQuotedValueSpans,
  } = scrub;
  const { scrubLong, scrubRecords } = chunks;
  const records = (text: string) => dec.decode(scrubRecords(enc.encode(text)).scrubbed);

  describe("rule set 5: secret-keyed record values (leak 1)", () => {
    const arbitrary = "SECRETTAIL plain words, not generated-looking";

    it("redacts a secret-keyed value in scrubJson whatever it looks like", () => {
      const out = scrubJson({ password: arbitrary, user: "ops" });
      expect(JSON.stringify(out)).not.toContain("SECRETTAIL");
      expect(out).toEqual({ password: R, user: "ops" });
    });

    it("redacts secret-keyed values at every nesting depth, arrays included", () => {
      const input = {
        type: "tool_use",
        trace: { config: [{ db: { db_password: `${arbitrary} 1` } }], apiKey: `${arbitrary} 2` },
        tokens: 12,
        auth: { token: [`${arbitrary} 3`, [`${arbitrary} 4`]] },
      };
      const out = JSON.stringify(scrubJson(input));
      expect(out).not.toContain("SECRETTAIL");
      expect(out).toBe(
        JSON.stringify({
          type: "tool_use",
          trace: { config: [{ db: { db_password: R } }], apiKey: R },
          tokens: 12,
          auth: { token: [R, [R]] },
        }),
      );
    });

    it("redacts a secret-keyed value in scrubRecord (allowlisted top-level key)", () => {
      const out = scrubRecord({ trace: { client_secret: arbitrary }, task: { password: arbitrary } });
      expect(JSON.stringify(out)).not.toContain("SECRETTAIL");
      expect(out).toEqual({ trace: { client_secret: R }, task: { password: R } });
    });

    it("redacts a secret-keyed value in scrubRecords, kept valid JSON", () => {
      const line = JSON.stringify({ password: arbitrary, user: "ops", nested: { "x-auth-token": arbitrary } });
      const out = records(`${line}\n`);
      expect(out).not.toContain("SECRETTAIL");
      expect(out).toBe(`${JSON.stringify({ password: R, user: "ops", nested: { "x-auth-token": R } })}\n`);
    });

    it("stays redacted when the scrubbed record is serialized and rescanned", () => {
      const input = { message: { content: [{ type: "text", password: arbitrary }] } };
      const once = JSON.stringify(scrubJson(input));
      expect(once).not.toContain("SECRETTAIL");
      expect(scrubText(once)).toBe(once);
      expect(records(`${once}\n`)).toBe(`${once}\n`);
    });

    it("keeps non-string, empty, already-redacted and op:// values, as the text rule does", () => {
      const input = { password: "", token: 512, secret: null, api_key: "op://vault/item/field", auth: true };
      expect(scrubJson(input)).toEqual(input);
      const line = JSON.stringify(input);
      expect(records(`${line}\n`)).toBe(`${line}\n`);
    });

    it("leaves records without a secret key name unchanged", () => {
      const input = { tokens: "plain words", author: "ops", oauth: "flow", key: "cache/key" };
      expect(scrubJson(input)).toEqual(input);
      const line = JSON.stringify(input);
      expect(records(`${line}\n`)).toBe(`${line}\n`);
    });
  });

  describe("rule set 5: over-cap values are measured on the original text (leak 2)", () => {
    it("the reproduction is over the bound before the token rules and under it after", () => {
      expect(SHRINKABLE.length).toBeGreaterThan(MAX_QUOTED_VALUE);
      expect(SHRINKABLE.replaceAll(CAPY, R).length).toBeLessThan(MAX_QUOTED_VALUE);
    });

    it("redacts the whole original span in scrubText", () => {
      const input = `"password": "${SHRINKABLE}" after`;
      const out = scrubText(input);
      expect(out).not.toContain("SECRETTAILMARK");
      expect(out).toBe(`"password": "${R}" after`);
    });

    it("redacts it unterminated, single-quoted, backtick and escaped too", () => {
      for (const [input, expected] of [
        [`"token": "${SHRINKABLE}`, `"token": "${R}`],
        [`'secret': '${SHRINKABLE}' after`, `'secret': '${R}' after`],
        [`\`secret\`: \`${SHRINKABLE}\` after`, `\`secret\`: \`${R}\` after`],
        [`{\\"api_key\\": \\"${SHRINKABLE}\\"}`, `{\\"api_key\\": \\"${R}\\"}`],
      ] as const) {
        const out = scrubText(input);
        expect(out).not.toContain("SECRETTAILMARK");
        expect(out).toBe(expected);
      }
    });

    it("redacts it in scrubRecords, in a text run and inside a record string", () => {
      const run = records(`log line\n"password": "${SHRINKABLE}" after\nnext\n`);
      expect(run).not.toContain("SECRETTAILMARK");
      expect(run).toBe(`log line\n"password": "${R}" after\nnext\n`);
      const line = JSON.stringify({ type: "tool_result", content: `"password": "${SHRINKABLE}" after` });
      const rec = records(`${line}\n`);
      expect(rec).not.toContain("SECRETTAILMARK");
      expect(rec).toBe(`${JSON.stringify({ type: "tool_result", content: `"password": "${R}" after` })}\n`);
    });

    it("redacts it in scrubJson", () => {
      const out = JSON.stringify(scrubJson({ content: `"password": "${SHRINKABLE}" after` }));
      expect(out).not.toContain("SECRETTAILMARK");
    });
  });

  /**
   * "v5 over v4 output": what re-scrubbing objects already stored under rule set 4 can fix.
   * Leak 1 survives in stored v4 output with its key still in view, so rule set 5 catches it.
   * Leak 2 survives in stored v4 output as a shrunk value under the bound, but still with
   * the line break after which rule set 4 stopped, so rule set 5's multi-line rule (leak 3)
   * catches it too. Leak 4 survives with its secret key still above the value.
   */
  describe("rule set 5 over rule set 4 output", () => {
    it("catches leak 1 in stored v4 records (the secret key is still there)", () => {
      const original = { trace: { password: "SECRETTAIL plain words" }, auth: { token: ["SECRETTAIL x"] }, user: "ops" };
      const stored = v4Json(original);
      expect(JSON.stringify(stored)).toContain("SECRETTAIL");
      const rescanned = scrubJson(stored);
      expect(JSON.stringify(rescanned)).not.toContain("SECRETTAIL");
      expect(rescanned).toEqual({ trace: { password: R }, auth: { token: [R] }, user: "ops" });
      const line = `${JSON.stringify(stored)}\n`;
      expect(records(line)).not.toContain("SECRETTAIL");
      expect(JSON.stringify(scrubRecord({ task: stored }))).not.toContain("SECRETTAIL");
    });

    it("catches leak 2 in stored v4 output too: the shrunk value is under the bound but still multi-line", () => {
      const text = `"password": "${SHRINKABLE}" after`;
      const stored = v4Text(text);
      expect(stored).toContain("SECRETTAILMARK");
      expect(stored.length).toBeLessThan(MAX_QUOTED_VALUE);
      // Rule set 5 redacts an under-bound value holding a line break whole (leak 3).
      expect(scrubText(stored)).toBe(`"password": "${R}" after`);
      expect(JSON.stringify(scrubJson({ content: stored }))).not.toContain("SECRETTAILMARK");
      // The original bytes, re-scrubbed, are clean.
      expect(scrubText(text)).toBe(`"password": "${R}" after`);
    });
  });

  /** Quote styles of the quoted rule (key quoted the same way as its value). */
  const QUOTE_STYLES: Array<[string, string]> = [
    ['"password"', '"'],
    ["'token'", "'"],
    ["`secret`", "`"],
  ];
  const SEPARATORS: Array<[string, string]> = [
    ["CR", "\r"],
    ["LF", "\n"],
    ["CRLF", "\r\n"],
    ["U+2028", " "],
    ["U+2029", " "],
    ["U+0085", "\u0085"],
  ];
  /** A multi-line value within the bound: the first line has spaces, the rest is the tail. */
  const multiLine = (sep: string) => `SECRETTAIL one two${sep}SECRETTAIL three four${sep}SECRETTAIL five`;

  describe("rule set 5: multi-line quoted secret values within the bound (blocker 1)", () => {
    for (const [key, q] of QUOTE_STYLES) {
      for (const [name, sep] of SEPARATORS) {
        it(`scrubText ${key} ${name}, terminated: redacted through the close, the rest kept`, () => {
          const input = `${key}: ${q}${multiLine(sep)}${q} KEPTAFTER${sep}{"user": "ops"}`;
          const out = scrubText(input);
          expect(out).not.toContain("SECRETTAIL");
          expect(out).toBe(`${key}: ${q}${R}${q} KEPTAFTER${sep}{"user": "ops"}`);
        });
        it(`scrubText ${key} ${name}, unterminated: redacted to the end of input`, () => {
          const out = scrubText(`${key}: ${q}${multiLine(sep)}`);
          expect(out).not.toContain("SECRETTAIL");
          expect(out).toBe(`${key}: ${q}${R}`);
        });
      }
    }

    it("keeps the empty-value, [REDACTED] and op:// exceptions and the unquoted path", () => {
      expect(scrubText(`"password": "" next\nline`)).toBe(`"password": "" next\nline`);
      expect(scrubText(`"password": "${R}" kept\nline`)).toBe(`"password": "${R}" kept\nline`);
      expect(scrubText(`"api_key": "op://vault/item/field" kept\nline`)).toBe(`"api_key": "op://vault/item/field" kept\nline`);
      // An op:// reference spanning a line break is no reference: redacted whole.
      expect(scrubText(`"api_key": "op://vault/item\nSECRETTAIL" kept`)).toBe(`"api_key": "${R}" kept`);
      expect(scrubText(`password=SECRETTAIL\nnext line`)).toBe(`password=${R}\nnext line`);
    });

    it("scrubJson: a long string holding a multi-line quoted value, one case per quote style", () => {
      const filler = "lorem ipsum dolor sit amet ".repeat(2000);
      for (const [key, q] of QUOTE_STYLES) {
        for (const sep of ["\n", "\r\n", " "]) {
          const content = `${filler}${key}: ${q}${multiLine(sep)}${q} KEPTAFTER\n${filler}`;
          const out = scrubJson({ type: "tool_result", content });
          expect(JSON.stringify(out), key).not.toContain("SECRETTAIL");
          expect(out, key).toEqual({ type: "tool_result", content: `${filler}${key}: ${q}${R}${q} KEPTAFTER\n${filler}` });
          // Unterminated: to the end of the string.
          const open = scrubJson({ content: `${filler}${key}: ${q}${multiLine(sep)}` });
          expect(open, key).toEqual({ content: `${filler}${key}: ${q}${R}` });
        }
      }
    });

    /**
     * Words up to just below `MAX_SCRUB_CHARS`, then a key whose multi-line value (within
     * the bound) straddles it: the last whitespace below the bound, where a segmenter would
     * cut, lies inside the value, more than the rule's look-ahead from nothing but the key.
     */
    function straddling(key: string, q: string, sep: string, terminated: boolean) {
      const words = (n: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(n / 27)).slice(0, n);
      const head = `${words(MAX_SCRUB_CHARS - 600)} `;
      const value = `SECRETTAIL one${sep}${words(1500)}${sep}SECRETTAIL two`;
      expect(value.length).toBeLessThan(MAX_QUOTED_VALUE);
      const keyAt = head.length;
      const valueEnd = keyAt + key.length + 2 + q.length + value.length;
      expect(keyAt).toBeLessThan(MAX_SCRUB_CHARS);
      expect(valueEnd).toBeGreaterThan(MAX_SCRUB_CHARS);
      if (!terminated) return { text: `${head}${key}: ${q}${value}`, expected: `${head}${key}: ${q}${R}` };
      const tail = ` after${sep}${words(5000)}`;
      return { text: `${head}${key}: ${q}${value}${q}${tail}`, expected: `${head}${key}: ${q}${R}${q}${tail}` };
    }

    for (const [name, sep] of [["LF", "\n"], ["CRLF", "\r\n"], ["U+2028", " "]] as const) {
      for (const terminated of [true, false]) {
        const label = `${name}, ${terminated ? "terminated" : "unterminated (EOI)"}`;
        it(`scrubLong: a value crossing the segment cut is never cut inside (${label})`, () => {
          for (const [key, q] of QUOTE_STYLES) {
            const { text, expected } = straddling(key, q, sep, terminated);
            expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
            // The value is protected from its key's start to its close (or the end).
            const spans = unsplittableQuotedValueSpans(text);
            expect(spans.some(([a, b]) => a < MAX_SCRUB_CHARS && b > MAX_SCRUB_CHARS), key).toBe(true);
            const out = scrubLong(text);
            expect(out.includes("SECRETTAIL"), `${key} ${label}`).toBe(false);
            expect(out === expected, `${key} ${label}: differs from the whole-value scrub`).toBe(true);
          }
        });
      }
    }

    it("scrubRecords: a multi-line value crossing a line-run cut is never cut inside", () => {
      const line = "lorem ipsum dolor sit amet ".repeat(37);
      for (const sep of ["\n", "\r\n"]) {
        const lines: string[] = [];
        while (lines.join(sep).length < MAX_SCRUB_CHARS - 1500) lines.push(line);
        const value = ["SECRETTAIL one", ...Array.from({ length: 3 }, () => line), "SECRETTAIL two"].join(sep);
        expect(value.length).toBeLessThan(MAX_QUOTED_VALUE);
        const text = `${lines.join(sep)}${sep}"password": "${value}" after${sep}${lines.slice(0, 20).join(sep)}${sep}`;
        expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
        const out = records(text);
        expect(out.includes("SECRETTAIL"), JSON.stringify(sep)).toBe(false);
        expect(out).toBe(`${lines.join(sep)}${sep}"password": "${R}" after${sep}${lines.slice(0, 20).join(sep)}${sep}`);
      }
    });
  });

  describe("rule set 5: secret context runs through composite values (blocker 2)", () => {
    const cases: Array<[string, unknown, unknown]> = [
      ["password:{value}", { password: { value: "SECRETTAIL one" } }, { password: { value: R } }],
      ["auth:{value}", { auth: { value: "SECRETTAIL two" } }, { auth: { value: R } }],
      ["token:{nested:{deeper}}", { token: { nested: { deeper: "SECRETTAIL three" } } }, { token: { nested: { deeper: R } } }],
      ["credentials:[{value}]", { credentials: [{ value: "SECRETTAIL four" }] }, { credentials: [{ value: R }] }],
      [
        "a secret object whose child keys are not secret names",
        { client_secret: { label: "SECRETTAIL five", list: ["SECRETTAIL six", { note: "SECRETTAIL seven" }] } },
        { client_secret: { label: R, list: [R, { note: R }] } },
      ],
      [
        "numbers, booleans, null, [REDACTED] and op:// leaves under a secret key",
        { password: { n: 3, ok: true, no: false, z: null, r: R, o: "op://vault/item/field", e: "", s: "SECRETTAIL eight" } },
        { password: { n: 3, ok: true, no: false, z: null, r: R, o: "op://vault/item/field", e: "", s: R } },
      ],
      [
        "a non-secret sibling object is scrubbed by its own keys only",
        { password: { value: "SECRETTAIL nine" }, settings: { mode: "visible words", depth: 2, inner: { token: "SECRETTAIL ten", other: "kept too" } } },
        { password: { value: R }, settings: { mode: "visible words", depth: 2, inner: { token: R, other: "kept too" } } },
      ],
    ];
    for (const [label, input, expected] of cases) {
      it(`scrubJson: ${label}`, () => {
        const out = scrubJson(input);
        expect(JSON.stringify(out)).not.toContain("SECRETTAIL");
        expect(out).toEqual(expected);
        // Stable when rescanned.
        expect(scrubJson(out)).toEqual(out);
      });
      it(`scrubRecords: ${label}`, () => {
        const line = JSON.stringify(input);
        const out = records(`${line}\n`);
        expect(out).not.toContain("SECRETTAIL");
        expect(out).toBe(`${JSON.stringify(expected)}\n`);
        expect(records(out)).toBe(out);
      });
    }

    it("scrubRecord: the same under an allowlisted top-level key", () => {
      const out = scrubRecord({ task: { password: { value: "SECRETTAIL one" } }, trace: { credentials: [{ value: "SECRETTAIL two" }] } });
      expect(JSON.stringify(out)).not.toContain("SECRETTAIL");
      expect(out).toEqual({ task: { password: { value: R } }, trace: { credentials: [{ value: R }] } });
    });

    it("the context ends with the secret-keyed value: later siblings and records are not secret", () => {
      const line = `${JSON.stringify({ password: { a: ["x"] }, after: "kept", list: [{ b: "kept" }] })}\n${JSON.stringify({ c: "kept" })}\n`;
      expect(records(line)).toBe(`${JSON.stringify({ password: { a: [R] }, after: "kept", list: [{ b: "kept" }] })}\n${JSON.stringify({ c: "kept" })}\n`);
    });
  });

  // Self-adversarial set: each is no strict reference and must be redacted whole (SECRET marks a tail).
  const ADVERSARIAL: Array<[string, string]> = [
    ["one segment (op://vault)", "op://vault"],
    ["one segment (op://SECRET)", "op://SECRET"],
    ["a query suffix", "op://v/i/f?attribute=otp"],
    ["a trailing slash", "op://v/i/f/"],
    ["an empty segment after the vault", "op://v//i/f"],
    ["three slashes after the scheme", "op:///v/i/f"],
    ["an empty segment in the middle", "op://v/i//f"],
    ["a percent-encoded slash", "op://v/i/f%2Fsecret"],
    ["a percent-encoded space", "op://v/i/f%20x"],
    ["an uppercase scheme", "OP://v/i/f"],
    ["a scheme without slashes", "op:vault/item/field"],
    ["leading whitespace", " op://v/i/f"],
    ["trailing whitespace", "op://v/i/f "],
    ["a leading space inside a segment", "op:// v/i/f"],
    ["a non-ASCII segment", "op://v/i/f\u00efeld"],
  ];

  describe("rule set 5: only a strict one-line op:// reference is kept (escaped separators)", () => {
    /** The quote that opens a value at JSON escaping depth `d`: 2^d - 1 backslashes, then `"`. */
    const q = (d: number) => `${"\\".repeat(2 ** d - 1)}"`;
    /** `body` as the value of `api_key`, `d` levels deep in `{"content":"…"}` strings; `eoi` drops every closing quote and brace. */
    const doc = (d: number, body: string, eoi: boolean) => {
      let inner = `{${q(d)}api_key${q(d)}:${q(d)}${body}${eoi ? "" : `${q(d)}}`}`;
      for (let k = d - 1; k >= 0; k -= 1) inner = `{${q(k)}content${q(k)}:${q(k)}${inner}${eoi ? "" : `${q(k)}}`}`;
      return inner;
    };
    /** An escape sequence written at depth `d`: each backslash doubled `d` times. */
    const at = (d: number, esc: string) => esc.replace(/\\/g, "\\".repeat(2 ** d));
    /** Long input for scrubLong: plain lines past the scrub bound, then the document. */
    const PAD = `${"lorem ipsum dolor sit amet\n".repeat(Math.ceil(MAX_SCRUB_CHARS / 27) + 10)}`;
    const parses = (t: string) => {
      try {
        JSON.parse(t);
        return true;
      } catch {
        return false;
      }
    };

    /** One document through scrubText, scrubLong, scrubJson, scrubRecord and scrubRecords. */
    const allPaths = (text: string, expected: string) => {
      expect(scrubText(text)).toBe(expected);
      expect(scrubLong(text)).toBe(expected);
      const long = scrubLong(PAD + text);
      expect(long.length).toBeLessThan(PAD.length + text.length + 1);
      expect(long).toBe(PAD + expected);
      expect(scrubJson({ content: text })).toEqual({ content: expected });
      expect(scrubRecord({ trace: { content: text } })).toEqual({ trace: { content: expected } });
      const record = { type: "user", message: { content: [{ type: "tool_result", content: text }] } };
      expect(records(`${JSON.stringify(record)}\n`)).toBe(
        `${JSON.stringify({ ...record, message: { content: [{ type: "tool_result", content: expected }] } })}\n`,
      );
      if (parses(text) && parses(expected)) {
        expect(scrubJson(JSON.parse(text))).toEqual(JSON.parse(expected));
        expect(records(`${text}\n`)).toBe(`${JSON.stringify(JSON.parse(expected))}\n`);
      }
      if (expected !== text) {
        const outs = [scrubText(text), long, records(`${text}\n`), records(`${JSON.stringify(record)}\n`)];
        outs.push(JSON.stringify(scrubJson({ content: text })), JSON.stringify(scrubRecord({ trace: { content: text } })));
        for (const out of outs) expect(out).not.toContain("SECRETTAIL");
      }
    };
    /** The key-context path: the decoded value under a secret key, in every record function. */
    const keyed = (value: string, kept: boolean) => {
      const out = kept ? value : R;
      expect(scrubJson({ api_key: value })).toEqual({ api_key: out });
      expect(scrubRecord({ trace: { api_key: value } })).toEqual({ trace: { api_key: out } });
      expect(records(`${JSON.stringify({ api_key: value })}\n`)).toBe(`${JSON.stringify({ api_key: out })}\n`);
      expect(scrubText(`"api_key": "${value}"`)).toBe(`"api_key": "${out}"`);
      expect(scrubLong(`${PAD}"api_key": "${value}"`)).toBe(`${PAD}"api_key": "${out}"`);
    };

    const separators: Array<[string, string, string]> = [
      ["LF", "\n", "\\n"],
      ["CR", "\r", "\\r"],
      ["CRLF", "\r\n", "\\r\\n"],
      ["U+2028", " ", "\\u2028"],
      ["U+2029", " ", "\\u2029"],
      ["U+0085", "\u0085", "\\u0085"],
    ];
    // [label, body at depth d]: every one must be redacted whole.
    const nonconforming: Array<[string, (d: number) => string]> = [];
    for (const [name, literal, esc] of separators) {
      nonconforming.push([`a literal ${name}`, () => `op://vault/item${literal}SECRETTAIL`]);
      nonconforming.push([`an encoded ${name} (${esc})`, (d) => `op://vault/item${at(d, esc)}SECRETTAIL`]);
    }
    // An inner (escaped) quote at depth d has 2^(d+1) - 1 backslashes; an escaped backslash 2^(d+1).
    nonconforming.push(["an escaped quote", (d) => `op://vault/item${"\\".repeat(2 ** (d + 1) - 1)}"SECRETTAIL`]);
    nonconforming.push(["an escaped backslash", (d) => `op://vault/item${"\\".repeat(2 ** (d + 1))}SECRETTAIL`]);
    nonconforming.push(["a double space", () => "op://vault/item  SECRETTAIL"]);
    nonconforming.push(["a tab", () => "op://vault/item\tSECRETTAIL"]);
    nonconforming.push(["a trailing space", () => "op://vault/item SECRETTAIL "]);
    nonconforming.push(["no item segment", () => "op://SECRETTAIL"]);
    // The grammar is exactly vault/item/field or vault/item/section/field: 2 or 5+ segments are no reference.
    nonconforming.push(["two segments", () => "op://vault/item"]);
    nonconforming.push(["five segments", () => "op://vault/item/section/field/SECRETTAIL"]);
    nonconforming.push(["six segments", () => "op://vault/item/section/field/x/SECRETTAIL"]);
    nonconforming.push(["a long tail", () => "op://a/b/c/d/e/f/SECRETTAIL"]);
    for (const [label, v] of ADVERSARIAL) nonconforming.push([label, () => v]);
    // Depth 0 decodes `\/` to `/`; the tail has five segments so the decoded value is no reference either way (op://v\/i\/f/SECRET decodes to a legitimate four-segment reference).
    nonconforming.push(["a backslash-escaped slash", (d) => `op://v${at(d, "\\/")}i${at(d, "\\/")}f/x/SECRET`]);

    for (const [label, body] of nonconforming) {
      for (const d of [0, 1, 2]) {
        for (const eoi of [false, true]) {
          it(`redacts an op:// value with ${label}, depth ${d}, ${eoi ? "EOI" : "terminated"}, in every path`, () => {
            allPaths(doc(d, body(d), eoi), doc(d, R, eoi));
          });
        }
      }
    }

    // The decoded value under a secret key (scrubJson, scrubRecord, scrubRecords), literal and encoded.
    for (const [name, literal, esc] of separators) {
      for (const [form, value] of [["literal", `op://vault/item${literal}SECRETTAIL`], ["encoded", `op://vault/item${esc}SECRETTAIL`]]) {
        it(`key context: redacts a ${form} ${name} op:// value in every path`, () => keyed(value as string, false));
      }
    }
    for (const [label, value] of [
      ["an escaped quote", 'op://vault/item\\"SECRETTAIL'],
      ["a backslash", "op://vault/item\\SECRETTAIL"],
      ["a double space", "op://vault/item  SECRETTAIL"],
      ["two segments", "op://vault/item"],
      ["five segments", "op://vault/item/section/field/SECRETTAIL"],
      ["six segments", "op://vault/item/section/field/x/SECRETTAIL"],
      ["a long tail", "op://a/b/c/d/e/f/SECRETTAIL"],
      ...ADVERSARIAL,
      ["a backslash-escaped slash", "op://v\\/i\\/f/x/SECRET"],
      ["a literal newline", "op://v/i/f\nSECRET"],
      ["a literal CR", "op://v/i/f\rSECRET"],
    ]) {
      it(`key context: redacts an op:// value with ${label} in every path`, () => keyed(value as string, false));
    }

    for (const ref of ["op://vault/item/field", "op://Engineering/cloudflare/credential", "op://My Vault/item name/field", "op://vault/item/section/field"]) {
      it(`keeps ${ref} byte for byte at depths 0, 1, 2 (terminated) and under a secret key, in every path`, () => {
        expect(OP_REFERENCE.test(ref)).toBe(true);
        for (const d of [0, 1, 2]) allPaths(doc(d, ref, false), doc(d, ref, false));
        keyed(ref, true);
      });
    }
  });

  describe("OP_REFERENCE grammar: exactly three or four segments", () => {
    it.each(["op://vault/item/field", "op://vault/item/section/field", "op://My Vault/item name/section 2/field"])("accepts %s", (ref) => {
      expect(OP_REFERENCE.test(ref)).toBe(true);
    });
    it.each([
      "op://vault",
      "op://vault/item",
      "op://vault/item/section/field/SECRETTAIL",
      "op://vault/item/section/field/x/SECRETTAIL",
      "op://a/b/c/d/e/f/SECRETTAIL",
      "op://vault/item/field?attr=otp",
      ...ADVERSARIAL.map(([, v]) => v),
      "op://v\\/i\\/f/x/SECRET",
      "op://v/i/f\nSECRET",
      "op://v/i/f\rSECRET",
      "op://vault/item/field/",
      "op://vault//field",
    ])("rejects %s", (ref) => {
      expect(OP_REFERENCE.test(ref)).toBe(false);
    });
  });
}
