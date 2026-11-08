/**
 * Rule set 4: a quoted secret value whose matching closing quote is not within the quoted
 * rule's scan bound (`MAX_QUOTED_VALUE`, 4096 UTF-16 code units in `lib/scrub.ts`, counted
 * across line breaks) is redacted whole, up to that close or, with none, to the end of the
 * input. No line break (CR, LF, CRLF, U+2028, U+2029, U+0085) ends it, and a short first
 * line does not keep it within the bound. Rule set 3 redacted only the first 4096 units
 * (or up to the first space or line break in them) and stored the rest. The bound is a
 * scan bound only; a value whose total length to its close is within it scrubs as in
 * rule set 3 when it is on one line and closed. Rule set 5: a quoted secret value within the bound that holds a line break, or
 * has no matching close, is redacted whole too (to its close, or to the end of input);
 * rule set 4 stopped those at the first whitespace and kept the rest.
 */
import { describe, expect, it } from "vitest";
import * as scrub from "../src/lib/scrub.js";
import { MAX_SCRUB_CHARS, scrubText } from "../src/lib/scrub.js";
import { canScrubLong, scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";

/** `MAX_QUOTED_VALUE` in lib/scrub.ts. */
const CAP = scrub.MAX_QUOTED_VALUE;
const R = "[REDACTED]";

/** Low-entropy filler no other rule matches, with no whitespace. */
const solid = (n: number) => "v".repeat(n);
/** Filler with spaces, distinct from the text around it. */
const spaced = (n: number) => "zeta kappa omicron ".repeat(Math.ceil(n / 19)).slice(0, n);

/** No 12-unit window of `value` survives in `out`. */
function expectNoFragment(out: string, value: string, label: string) {
  const seen = new Set<string>();
  for (let i = 0; i + 12 <= value.length; i += 1) {
    const f = value.slice(i, i + 12);
    if (seen.has(f)) continue;
    seen.add(f);
    if (/^\s*$/.test(f)) continue;
    expect(out.includes(f), `${label}: kept "${f}"`).toBe(false);
  }
}

describe("quoted secret values past the scan bound (rule set 4)", () => {
  it("keeps rule set 3 output for a value exactly at the bound", () => {
    expect(scrubText(`{"password": "${solid(CAP)}"}`)).toBe(`{"password": "${R}"}`);
    expect(scrubText(`{"password": "${spaced(CAP)}"}`)).toBe(`{"password": "${R}"}`);
    // Unterminated at the end of input, within the bound: to the end of input (rule set 5;
    // rule set 4 stopped at the next whitespace and kept " tail").
    expect(scrubText(`"token": "${solid(CAP)}`)).toBe(`"token": "${R}`);
    expect(scrubText(`"token": "${solid(100)} tail`)).toBe(`"token": "${R}`);
  });

  it("redacts a value one unit over the bound whole", () => {
    expect(scrubText(`{"password": "${solid(CAP + 1)}"}`)).toBe(`{"password": "${R}"}`);
    expect(scrubText(`{"password": "${spaced(CAP + 1)}"}`)).toBe(`{"password": "${R}"}`);
  });

  it("redacts a 100 KiB value whole, with or without spaces", () => {
    for (const value of [solid(100 * 1024), spaced(100 * 1024)]) {
      const out = scrubText(`{"user": "ops", "api_key": "${value}", "n": 1}`);
      expect(out).toBe(`{"user": "ops", "api_key": "${R}", "n": 1}`);
    }
  });

  it("redacts an unterminated value over the bound to the end of input", () => {
    for (const value of [solid(CAP + 1), solid(CAP * 3), spaced(CAP * 3)]) {
      expect(scrubText(`"token": "${value}`)).toBe(`"token": "${R}`);
    }
  });

  it("handles single, double and backtick quotes", () => {
    const v = spaced(CAP * 2);
    expect(scrubText(`{'secret': '${v}', 'k': 'x'}`)).toBe(`{'secret': '${R}', 'k': 'x'}`);
    expect(scrubText(`{"secret": "${v}", "k": "x"}`)).toBe(`{"secret": "${R}", "k": "x"}`);
    expect(scrubText(`{\`secret\`: \`${v}\`, \`k\`: \`x\`}`)).toBe(
      `{\`secret\`: \`${R}\`, \`k\`: \`x\`}`,
    );
    expect(scrubText(`'secret' => '${v}'`)).toBe(`'secret' => '${R}'`);
  });

  it("handles escaped quotes inside the value and escaped (nested JSON) quoting", () => {
    // Depth 0, inner `\"` quotes do not end the value.
    const inner = `${spaced(CAP)} \\"quoted\\" ${spaced(CAP)}`;
    expect(scrubText(`{"password": "${inner}", "user": "ops"}`)).toBe(
      `{"password": "${R}", "user": "ops"}`,
    );
    // Depth 1 (a JSON document in a JSON string), with depth-2 inner quotes.
    const nested = `${spaced(CAP)} \\\\\\"x\\\\\\" ${spaced(CAP)}`;
    expect(scrubText(`{\\"password\\": \\"${nested}\\", \\"user\\": \\"ops\\"}`)).toBe(
      `{\\"password\\": \\"${R}\\", \\"user\\": \\"ops\\"}`,
    );
  });

  /** Every line break a value may hold: none ends an over-bound value. */
  const BREAKS: Array<[string, string]> = [
    ["LF", "\n"],
    ["CRLF", "\r\n"],
    ["CR", "\r"],
    ["U+2028", "\u2028"],
    ["U+2029", "\u2029"],
    ["U+0085", "\u0085"],
  ];
  const QUOTES: Array<[string, string]> = [
    ['"password"', '"'],
    ["'token'", "'"],
    ["`secret`", "`"],
    ['\\"api_key\\"', '\\"'],
  ];

  it("runs an over-bound backtick value past LF and CRLF to its closing backtick (regression 1)", () => {
    for (const nl of ["\n", "\r\n"]) {
      const tail = `TAILAFTERBREAK${nl === "\n" ? "LF" : "CRLF"}Q7`;
      for (const head of [solid(CAP * 2), spaced(CAP * 2)]) {
        const input = `const cfg = {\`secret\`: \`${head}${nl}${tail} more template text${nl}last line\`, kept: 1}${nl}after the literal`;
        const out = scrubText(input);
        expect(out).toBe(`const cfg = {\`secret\`: \`${R}\`, kept: 1}${nl}after the literal`);
        expect(out.includes(tail)).toBe(false);
        expect(scrub.overCapQuotedValueSpans(input)).toEqual([
          [13, input.indexOf("`, kept")],
        ]);
      }
    }
  });

  it("runs an over-bound value of every quote past every line break to its matching quote", () => {
    for (const [key, q] of QUOTES) {
      for (const [name, nl] of BREAKS) {
        const tail = `UNIQUETAIL${name.replace(/\W/g, "")}`;
        const input = `${key}: ${q}${solid(CAP + 5)}${nl}${tail}${q} kept after${nl}next "token": "x"`;
        const out = scrubText(input);
        expect(out, `${key} ${name}`).toBe(`${key}: ${q}${R}${q} kept after${nl}next "token": "${R}"`);
        expect(out.includes(tail), `${key} ${name}`).toBe(false);
      }
    }
  });

  it("redacts an over-bound value with a line break and no closing quote to the end of input (regression 2)", () => {
    for (const [key, q] of QUOTES) {
      for (const [name, nl] of BREAKS) {
        for (const head of [solid(CAP + 1), spaced(CAP * 3)]) {
          const tail = `NOCLOSETAIL${name.replace(/\W/g, "")}`;
          const input = `x ${key}: ${q}${head}${nl}${tail} more words${nl}and another line`;
          const out = scrubText(input);
          expect(out, `${key} ${name}`).toBe(`x ${key}: ${q}${R}`);
          expect(scrub.overCapQuotedValueSpans(input), `${key} ${name}`).toEqual([[2, input.length]]);
        }
      }
    }
  });

  it("keeps the text after a closing quote that follows a line break (regression 4)", () => {
    for (const [, nl] of BREAKS) {
      const input = `a: 1${nl}"password": "${solid(CAP * 2)}${nl}line two of it${nl}" after the close${nl}b: 2 "user": "ops"`;
      expect(scrubText(input)).toBe(`a: 1${nl}"password": "${R}" after the close${nl}b: 2 "user": "ops"`);
    }
    // Several over-bound values in one text: each ends at its own closing quote.
    const two = `"password": "${solid(CAP + 9)}\nP1TAIL" mid\n'token': '${spaced(CAP + 9)}\r\nP2TAIL' end`;
    expect(scrubText(two)).toBe(`"password": "${R}" mid\n'token': '${R}' end`);
  });

  it("caps the total length across lines: the exact backtick reproduction", () => {
    // First line short, the bound crossed only after the break: rule set 4 as of 7bc5e51d
    // decided on the key's line alone and stored everything after "short".
    const input = `\`secret\`: \`short\n${solid(CAP + 1)}UNIQUE_TAIL_SHOULD_NOT_SURVIVE\` after`;
    const out = scrubText(input);
    expect(out).toBe(`\`secret\`: \`${R}\` after`);
    expect(out.includes("UNIQUE_TAIL_SHOULD_NOT_SURVIVE")).toBe(false);
    expect(out.includes("short")).toBe(false);
    expect(scrub.overCapQuotedValueSpans(input)).toEqual([[0, input.indexOf("` after")]]);
  });

  it("caps the total length of every quote across every break, to the close or the end of input", () => {
    for (const [key, q] of QUOTES) {
      for (const [name, nl] of BREAKS) {
        const label = `${key} ${name}`;
        const tail = `TOTALTAIL${name.replace(/\W/g, "")}`;
        // One break, then the rest of the bound; and several breaks, no line near the bound.
        for (const body of [
          `short${nl}${solid(CAP)}${tail}`,
          `short${nl}${solid(2000)}${nl}${spaced(1500)}${nl}${solid(700)}${nl}${tail}`,
        ]) {
          const closed = `${key}: ${q}${body}${q} kept after${nl}next "token": "x"`;
          const out = scrubText(closed);
          expect(out, label).toBe(`${key}: ${q}${R}${q} kept after${nl}next "token": "${R}"`);
          expect(out.includes(tail), label).toBe(false);
          // No close: to the end of input.
          const open = `x ${key}: ${q}${body} more words${nl}and another line`;
          expect(scrubText(open), label).toBe(`x ${key}: ${q}${R}`);
          expect(scrub.overCapQuotedValueSpans(open), label).toEqual([[2, open.length]]);
        }
      }
    }
  });

  it("redacts a multi-line value whose total length to its close is within the bound whole (rule set 5)", () => {
    for (const [key, q] of QUOTES) {
      for (const [name, nl] of BREAKS) {
        const label = `${key} ${name}`;
        // Within the bound: the closing quote character at most CAP units past the value's
        // start (an escaped close's backslashes count as value units, as in rule set 3).
        const top = CAP - (q.length - 1);
        for (const [first, total] of [[5, 300], [100, top - 1], [0, top], [top - 50, top]]) {
          const value = `${solid(first)}${nl}${spaced(total - first - nl.length)}`;
          expect(value.length).toBe(total);
          for (const after of [`${q} after${nl}"user": "ops"`, `${q}`]) {
            const input = `${key}: ${q}${value}${after}`;
            // Rule set 4 kept rule set 3's output here (up to the first whitespace).
            expect(scrubText(input), `${label} ${first}/${total}`).toBe(`${key}: ${q}${R}${after}`);
            expect(scrub.overCapQuotedValueSpans(input), label).toEqual([]);
            // Not over the bound, but never cut inside by the segmenter (key to value end).
            const start = input.indexOf(q, key.length) + q.length;
            expect(scrub.unsplittableQuotedValueSpans(input), label).toEqual([[0, start + value.length]]);
          }
        }
        // One unit more is over the bound in total, though every line is short.
        const over = `${key}: ${q}${solid(5)}${nl}${spaced(top + 1 - 5 - nl.length)}${q} after`;
        expect(scrubText(over), label).toBe(`${key}: ${q}${R}${q} after`);
        // An unclosed value whose input ends within the bound: to the end of input.
        const ends = `${key}: ${q}short${nl}${spaced(CAP - 100)}`;
        expect(scrubText(ends), label).toBe(`${key}: ${q}${R}`);
      }
    }
    // A line break inside the bound with the close past it no longer ends the value.
    expect(scrubText(`"password": "${solid(CAP - 1)}\n${solid(CAP * 2)} kept"`)).toBe(`"password": "${R}"`);
    // Within the bound: the next line, up to its close, is part of the value (rule set 5).
    expect(scrubText(`"password": "${solid(100)}\nnext line kept"`)).toBe(`"password": "${R}"`);
  });

  it("redacts a value whose surrogate pair straddles the bound whole, leaving no lone surrogate", () => {
    const lone = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
    for (const before of [CAP - 2, CAP - 1, CAP]) {
      const input = `"password": "${solid(before)}\u{1F600}" kept`;
      const out = scrubText(input);
      expect(out).toBe(`"password": "${R}" kept`);
      expect(lone.test(out)).toBe(false);
      // CAP - 2 + 2 units is within the bound (rule set 3 path); the others are over it.
      expect(scrub.overCapQuotedValueSpans(input).length).toBe(before === CAP - 2 ? 0 : 1);
    }
  });

  it("treats an unquoted value of exactly 4096 units as rule set 3 did", () => {
    // Low entropy: kept as is, at, below and above 4096 units.
    for (const n of [CAP - 1, CAP, CAP + 1]) {
      expect(scrubText(`'token' => ${solid(n)} end`)).toBe(`'token' => ${solid(n)} end`);
    }
    // Generated: redacted whole at exactly 4096 units.
    const chunk = "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ";
    const value = chunk.repeat(CAP / chunk.length);
    expect(value.length).toBe(CAP);
    expect(scrubText(`'token' => ${value} end`)).toBe(`'token' => ${R} end`);
  });

  it("redacts an over-bound op:// reference rather than keeping it", () => {
    expect(scrubText(`"password": "op://${solid(CAP * 2)}"`)).toBe(`"password": "${R}"`);
    expect(scrubText(`"password": "op://vault/item/field"`)).toBe(
      `"password": "op://vault/item/field"`,
    );
  });

  it("redacts a generated unquoted value after a quoted key whole", () => {
    const chunk = "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ";
    const value = chunk.repeat(Math.ceil((CAP * 2) / chunk.length));
    const out = scrubText(`'token' => ${value}; next`);
    expect(out).toBe(`'token' => ${R}; next`);
  });

  it("records a span from the key for a generated unquoted value of 4096+ units, not for a shorter one", () => {
    const chunk = "aB3dE5fG7hJ9kL1mN2pQ4rS6tU8wX0yZ";
    for (const n of [CAP, CAP * 2]) {
      const input = `x 'token' => ${chunk.repeat(n / chunk.length)}; next`;
      expect(scrub.overCapQuotedValueSpans(input)).toEqual([[2, input.length - 6]]);
    }
    expect(scrub.overCapQuotedValueSpans(`'token' => ${chunk.repeat(8)}; next`)).toEqual([]);
    // Low entropy: not taken, so no span either.
    expect(scrub.overCapQuotedValueSpans(`'token' => ${solid(CAP * 2)}; next`)).toEqual([]);
  });

  it("leaves no byte of the value in output for every shape", () => {
    const value = spaced(CAP * 5);
    const inputs = [
      `{"password": "${value}"}`,
      `"password": "${value}`,
      `{\\"password\\": \\"${value}\\"}`,
      `'password': '${value}'`,
      `\`password\`: \`${value}\``,
    ];
    for (const input of inputs) expectNoFragment(scrubText(input), value, input.slice(0, 16));
  });
});

describe("rule set 4 stays linear", () => {
  it("scrubs a 1 MiB quoted value in bounded time", () => {
    const MiB = 1024 * 1024;
    for (const value of [solid(MiB), spaced(MiB)]) {
      const input = `{"password": "${value}"}`;
      let start = performance.now();
      const spans = scrub.overCapQuotedValueSpans(input);
      let ms = performance.now() - start;
      expect(spans).toEqual([[1, 14 + MiB]]);
      expect(ms, `span scan took ${ms.toFixed(0)} ms`).toBeLessThan(2_000);
      start = performance.now();
      const out = scrubText(input);
      ms = performance.now() - start;
      expect(ms, `scrubText took ${ms.toFixed(0)} ms`).toBeLessThan(2_000);
      // scrubText bounds its input at MAX_SCRUB_CHARS first (cut after whitespace, the
      // rest dropped and announced); what it keeps of the value is redacted.
      expectNoFragment(out, value.slice(0, 64), "1 MiB");
    }
  }, 60_000);

  it("stays fast on many over-bound values and adversarial quote runs", () => {
    const inputs = [
      `"token": "${solid(CAP + 10)}"\n`.repeat(Math.floor(MAX_SCRUB_CHARS / (CAP + 22))),
      `"token": "${solid(CAP + 10)} `.repeat(Math.floor(MAX_SCRUB_CHARS / (CAP + 22))),
      `"token": "${`\\"token\\": \\"${solid(CAP)}`.repeat(Math.floor(MAX_SCRUB_CHARS / (CAP + 14)))}`,
      `'token': "${`'token': '${spaced(CAP + 1)} `.repeat(Math.floor(MAX_SCRUB_CHARS / (CAP + 14)))}`,
    ];
    for (const input of inputs) {
      const start = performance.now();
      scrubText(input);
      scrub.overCapQuotedValueSpans(input);
      const ms = performance.now() - start;
      expect(ms, `${input.slice(0, 24)}… took ${ms.toFixed(0)} ms`).toBeLessThan(2_000);
    }
  }, 60_000);
});

describe("the segmenter never cuts inside an over-bound value (rule set 4)", () => {
  it("keeps a long spaced value in one segment, so no suffix is stored", () => {
    // The value straddles MAX_SCRUB_CHARS and its key is far more than the context
    // check's reach (CUT_CONTEXT_CHARS) before any cut inside it.
    const value = spaced(40_000);
    const head = "lorem ipsum dolor sit amet ".repeat(Math.ceil((MAX_SCRUB_CHARS - 20_000) / 27));
    const text = `${head}"password": "${value}" tail words here`;
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const out = scrubLong(text);
    expect(out).toBe(`${head}"password": "${R}" tail words here`);
    expectNoFragment(out, value, "segmented");
  });

  it("does not cut between the key and an over-bound value (the key/value seam)", () => {
    // Reviewer probe: earlier rules shrink each capy_ token to [REDACTED], and a cut
    // in the separator after the key left the value keyless in the next segment, so
    // the tail of the value was stored. The protected span starts at the key.
    const marker = "SECRETTAILMARK";
    const tokens = `${"capy_" + "Ab1".repeat(10)} `.repeat(260);
    const head = "lorem ipsum dolor ".repeat(Math.floor((MAX_SCRUB_CHARS - 5000) / 18));
    const text = `${head}"password": "${tokens}${marker} ${tokens}" end words`;
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const out = scrubLong(text);
    expect(out.includes(marker)).toBe(false);
    expect(out.endsWith(`"password": "${R}" end words`)).toBe(true);
  });

  // Delta review N1: longCut found over-bound values only in the text below the segment
  // bound, so a value whose length passes the scan bound only past it (its key less than
  // about 4,250 units below the bound, or straddling it) had no span, and the cut could
  // land in the key, at the seam or in the value, storing the value's tail keyless.
  const MARK = "SECRETTAILMARK";
  const capyTokens = `${"capy_" + "Ab1".repeat(10)} `.repeat(260);
  /** Filler ending in a non-space, so the only whitespace before the key is the one added. */
  const lorem = (n: number) => "lorem ipsum dolor ".repeat(Math.ceil(n / 18)).slice(0, n).replace(/ $/, "x");
  /** A text whose key starts `off` units from MAX_SCRUB_CHARS. */
  const keyAt = (off: number, key: string, sep: string, close: string) =>
    `${lorem(MAX_SCRUB_CHARS + off - 1)} ${key}${sep}${capyTokens}${MARK} ${capyTokens}${close} end words`;
  const KEY_STYLES: Array<[string, string, string]> = [
    ['"password"', ': "', '"'],
    ['"db password"', ': "', '"'],
    ["'token'", ": '", "'"],
    ['\\"api_key\\"', ': \\"', '\\"'],
    ['"secret"', ' : "', '"'],
  ];

  it("protects a value whose key starts anywhere from 4,200 units below the bound to the bound", () => {
    // Every 200 units up to the scan's reach (4096 + 150 from the key), then each unit
    // where the cut can fall in the key or the separator.
    const offsets = [-4250, -4100, -4096, -4000, -100, -64, -56, -48];
    for (let off = -3800; off < -100; off += 200) offsets.push(off);
    for (let off = -40; off <= 0; off += 1) offsets.push(off);
    for (const [key, sep, close] of KEY_STYLES) {
      for (const off of offsets) {
        const text = keyAt(off, key, sep, close);
        expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
        expect(scrubLong(text).includes(MARK), `${key} at ${off}`).toBe(false);
      }
    }
  }, 120_000);

  it("does not cut inside the key, at the colon, at the opening quote or just after it", () => {
    // Each was the cut rule set 4 made before the fix (key start + 4, + 11, + 12, + 14).
    const cases: Array<[string, string, number, string]> = [
      ['"db password"', ': "', -5, "inside the key"],
      ['"password"', ' : "', -11, "before the colon"],
      ['"password"', ': "', -14, "before the opening quote"],
      ['"password"', ': " ', -14, "just after the opening quote"],
    ];
    for (const [key, sep, off, where] of cases) {
      const text = keyAt(off, key, sep, '"');
      const out = scrubLong(text);
      expect(out.includes(MARK), where).toBe(false);
      expect(out.endsWith(`${key}${sep.trimEnd()}${R}" end words`), where).toBe(true);
    }
  });

  it("refuses a text whose only cuts fall inside an over-bound value", () => {
    const value = spaced(MAX_SCRUB_CHARS + 1024);
    expect(() => scrubLong(`"password": "${value}"`)).toThrow(/scrub limit/);
  });

  it("refuses a multi-line over-bound value crossing MAX_SCRUB_CHARS, with no content (regression 3)", () => {
    const MARK3 = "MULTILINETAILMARK";
    for (const nl of ["\n", "\r\n", "\u2028"]) {
      const lines = `zeta kappa ${MARK3} omicron${nl}`.repeat(Math.ceil((MAX_SCRUB_CHARS + 2048) / 30));
      for (const close of ['" after the close', ""]) {
        const text = `"password": "${solid(CAP + 8)}${nl}${lines}${close}`;
        expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
        // Every whitespace (each line break included) is inside the protected span.
        expect(scrub.overCapQuotedValueSpans(text)).toEqual([[0, close ? text.length - close.length : text.length]]);
        expect(canScrubLong(text)).toBe(false);
        let error: unknown;
        try {
          scrubLong(text);
        } catch (e) {
          error = e;
        }
        expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
        const message = (error as Error).message;
        expect(message.includes(MARK3) || message.includes("zeta") || message.includes("vvvv")).toBe(false);
      }
    }
  });

  it("does not cut a multi-line over-bound value whose key is far before the bound", () => {
    // Key ~50,000 units below the bound, value across many lines past it: longCut must
    // go back to the key; the line breaks inside the value are no cut.
    const lines = `zeta kappa SPANTAILMARK omicron\n`.repeat(Math.ceil(60_000 / 32));
    const head = "lorem ipsum dolor sit amet ".repeat(Math.ceil((MAX_SCRUB_CHARS - 50_000) / 27));
    const text = `${head}"password": "${solid(CAP + 8)}\n${lines}" tail words here`;
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const out = scrubLong(text);
    expect(out).toBe(`${head}"password": "${R}" tail words here`);
  });

  it("refuses a value whose first line is short and that crosses MAX_SCRUB_CHARS after a break, with no content", () => {
    const MARKN = "TOTALCAPTAILMARK";
    for (const nl of ["\n", "\r\n", "\r", "\u2028", "\u2029", "\u0085"]) {
      const lines = `zeta kappa ${MARKN} omicron${nl}`.repeat(Math.ceil((MAX_SCRUB_CHARS + 2048) / 30));
      for (const close of ["` after the close", ""]) {
        const text = `\`secret\`: \`short${nl}${lines}${close}`;
        expect(scrub.overCapQuotedValueSpans(text)).toEqual([[0, close ? text.length - close.length : text.length]]);
        expect(canScrubLong(text)).toBe(false);
        let error: unknown;
        try {
          scrubLong(text);
        } catch (e) {
          error = e;
        }
        expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
        const message = (error as Error).message;
        expect(message.includes(MARKN) || message.includes("zeta") || message.includes("short")).toBe(false);
      }
    }
  });

  it("cuts before the key of a value whose first line is short and that crosses the bound after a break", () => {
    const lines = `zeta kappa TOTALCUTMARK omicron\n`.repeat(Math.ceil(60_000 / 32));
    const head = "lorem ipsum dolor sit amet ".repeat(Math.ceil((MAX_SCRUB_CHARS - 50_000) / 27));
    for (const close of ['" tail words here', ""]) {
      const text = `${head}"password": "short\n${lines}${close}`;
      expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
      expect(scrubLong(text)).toBe(`${head}"password": "${R}${close ? close : ""}`);
    }
    // The key just below the bound, the value's first line short: decided past the bound.
    const near = "lorem ipsum dolor sit amet ".repeat(Math.ceil((MAX_SCRUB_CHARS - 40) / 27)).slice(0, MAX_SCRUB_CHARS - 40);
    const text = `${near} "password": "short\n${spaced(CAP)}\nNEARTAILMARK" after`;
    expect(scrubLong(text)).toBe(`${near} "password": "${R}" after`);
  });
});

describe("raw-chunk runs never end inside an over-bound value (rule set 4)", () => {
  const enc = (s: string) => new TextEncoder().encode(s);
  const dec = (b: Uint8Array) => new TextDecoder().decode(b);

  it("cuts a long run before the key line, not at a line break inside the value", () => {
    // Text lines (no records): the run overflows inside the value; the only safe line
    // boundaries are before the key line, which is within the cut search window.
    const head = "lorem ipsum dolor sit amet\n".repeat(Math.ceil((MAX_SCRUB_CHARS - 50_000) / 27));
    const lines = "zeta kappa RUNTAILMARK omicron\n".repeat(Math.ceil(100_000 / 31));
    const text = `${head}"password": "${solid(CAP + 8)}\n${lines}" after the close\nlast line\n`;
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const out = dec(scrubRecords(enc(text)).scrubbed);
    expect(out.includes("RUNTAILMARK")).toBe(false);
    expect(out).toBe(`${head}"password": "${R}" after the close\nlast line\n`);
  });

  it("refuses a run whose only line boundaries fall inside an over-bound value", () => {
    const lines = "zeta kappa RUNTAILMARK omicron\n".repeat(Math.ceil((MAX_SCRUB_CHARS + 4096) / 31));
    const text = `"password": "${solid(CAP + 8)}\n${lines}" after\n`;
    let error: unknown;
    try {
      scrubRecords(enc(text));
    } catch (e) {
      error = e;
    }
    expect(error).toMatchObject({ code: "chunk_unsegmentable", retryable: false });
    expect((error as Error).message.includes("RUNTAILMARK")).toBe(false);
  });

  it("keeps a record or key line inside an open over-bound value in the run", () => {
    // A JSON record line and a private-key header line after the key line would end
    // the run; the value is still open, so they join it and the tail stays redacted.
    for (const between of ['{"a": 1}', "-----BEGIN PRIVATE KEY-----"]) {
      const text = `x\n\`secret\`: \`${solid(CAP + 8)}\n${between}\nRECTAILMARK words\n\` after the close\n{"b": 2}\n`;
      const out = dec(scrubRecords(enc(text)).scrubbed);
      expect(out.includes("RECTAILMARK"), between).toBe(false);
      expect(out.includes("vvvvvvvv"), between).toBe(false);
      // The record line: text after the closing quote is kept. (A key header with no
      // footer keeps the run open to the end, where the private-key rule takes over.)
      if (between.startsWith("{"))
        expect(out).toBe(`x\n\`secret\`: \`${R}\` after the close\n{"b": 2}\n`);
    }
    // A closed value ends the run as before: the record line after it is its own record.
    const closed = `\`secret\`: \`${solid(CAP + 8)}\`\n{"note": "x"}\n`;
    expect(dec(scrubRecords(enc(closed)).scrubbed)).toBe(`\`secret\`: \`${R}\`\n{"note": "x"}\n`);
  });

  it("keeps a record or key line in a short run that ends inside an undecided secret value (fail closed)", () => {
    // The run ends at the record line holding only "short" of the value: whether it is
    // over the bound is decided by lines after the record, so the record joins the run.
    for (const between of ['{"a": 1}', "-----BEGIN PRIVATE KEY-----"]) {
      for (const nl of ["\n", "\r\n"]) {
        const text = `x${nl}\`secret\`: \`short${nl}${between}${nl}${solid(CAP)}RUNTOTALTAIL${nl}\` after the close${nl}{"b": 2}${nl}`;
        const out = dec(scrubRecords(enc(text)).scrubbed);
        expect(out.includes("RUNTOTALTAIL"), between).toBe(false);
        expect(out.includes("vvvvvvvv"), between).toBe(false);
        expect(out.includes("short"), between).toBe(false);
        if (between.startsWith("{")) expect(out).toBe(`x${nl}\`secret\`: \`${R}\` after the close${nl}{"b": 2}${nl}`);
      }
    }
    // Within the bound in total, the value is redacted whole to its close (rule set 5;
    // rule set 4 scrubbed as rule set 3, to the next whitespace, and kept the lines after).
    const within = `x\n\`secret\`: \`short\n{"a": 1}\nmore\` after\n{"b": 2}\n`;
    expect(dec(scrubRecords(enc(within)).scrubbed)).toBe(`x\n\`secret\`: \`${R}\` after\n{"b": 2}\n`);
  });

  it("cuts a long run before the key line of a value that crosses the bound only after a break", () => {
    const head = "lorem ipsum dolor sit amet\n".repeat(Math.ceil((MAX_SCRUB_CHARS - 50_000) / 27));
    const lines = "zeta kappa RUNTOTALMARK omicron\n".repeat(Math.ceil(100_000 / 32));
    const text = `${head}"password": "short\n${lines}" after the close\nlast line\n`;
    const out = dec(scrubRecords(enc(text)).scrubbed);
    expect(out.includes("RUNTOTALMARK")).toBe(false);
    expect(out).toBe(`${head}"password": "${R}" after the close\nlast line\n`);
    // With no line boundary before the key in reach: refused, with no content.
    const bare = `"password": "short\n${"zeta kappa RUNTOTALMARK omicron\n".repeat(Math.ceil((MAX_SCRUB_CHARS + 4096) / 32))}" after\n`;
    let error: unknown;
    try {
      scrubRecords(enc(bare));
    } catch (e) {
      error = e;
    }
    expect(error).toMatchObject({ code: "chunk_unsegmentable", retryable: false });
    expect((error as Error).message.includes("RUNTOTALMARK")).toBe(false);
  });
});
