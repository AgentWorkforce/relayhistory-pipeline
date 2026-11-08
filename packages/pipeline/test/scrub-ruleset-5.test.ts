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
 *
 * Every value here is synthetic; `SECRETTAIL`/`SECRETTAILMARK` mark the bytes that must
 * not survive.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  MAX_QUOTED_VALUE,
  MAX_SCRUB_CHARS,
  scrubJson,
  scrubRecord,
  scrubText,
  unsplittableQuotedValueSpans,
} from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubJson as v4Json, scrubText as v4Text } from "./fixtures/scrub-ruleset-4.js";

const R = "[REDACTED]";
const enc = new TextEncoder();
const dec = new TextDecoder();
const records = (text: string) => dec.decode(scrubRecords(enc.encode(text)).scrubbed);

/** A synthetic Capy-shaped token (the `capy_` rule matches it; it is not a credential). */
const CAPY = `capy_${"Ab1".repeat(12)}`;
/** Leak 2: 130 Capy-shaped tokens (over the bound) then a line break and the tail. */
const SHRINKABLE = `${`${CAPY} `.repeat(130)}\nSECRETTAILMARK`;

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

describe("rule set 5: the shared corpus's record cases through the raw-chunk record path", () => {
  // The shared corpus test (kept identical in relayhistory-cloud) runs record cases through
  // scrubJson and scrubRecord; the record-line tokenizer lives only here, so it is run here.
  const corpus = JSON.parse(
    readFileSync(fileURLToPath(new URL("./fixtures/scrub-secret-corpus.json", import.meta.url).href), "utf8"),
  ) as {
    generators: Record<string, { prefix?: string; parts: [string, number | string][] }>;
    cases: { id: string; input: string; expected: string; record?: boolean; expectedRecord?: string }[];
  };
  // Deterministic synthetic fill: the record-line tokenizer is what is under test here; the
  // corpus test fills the same cases with fresh random values.
  const fill = (s: string) =>
    s.replace(/\{\{([a-z0-9_]+)\}\}/g, (_m, name: string) => {
      const g = corpus.generators[name];
      if (!g) throw new Error(`unknown placeholder ${name}`);
      let body = "";
      for (const [kind, n] of g.parts) body += kind === "lit" ? String(n) : "Zq7".repeat(Number(n)).slice(0, Number(n));
      return JSON.stringify(`${g.prefix ?? ""}${body}`).slice(1, -1);
    });
  const cases = corpus.cases.filter((c) => c.record);

  it("has record cases for both leaks", () => {
    expect(cases.length).toBeGreaterThanOrEqual(5);
  });

  for (const c of cases) {
    it(c.id, () => {
      const input = fill(c.input);
      const expected = fill(c.expectedRecord ?? c.expected);
      const out = records(`${input}\n`);
      expect(out).not.toContain("SECRETTAIL");
      expect(out).toBe(`${expected}\n`);
      expect(records(out)).toBe(out);
    });
  }
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
