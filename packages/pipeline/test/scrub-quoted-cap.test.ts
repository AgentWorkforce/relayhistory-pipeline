/**
 * Rule set 4: a quoted secret value longer than the quoted rule's scan bound
 * (`MAX_QUOTED_VALUE`, 4096 UTF-16 code units in `lib/scrub.ts`) is redacted whole, up to
 * its closing quote or, with none, to the first line break (LF, CRLF or lone CR) or the
 * end of the input. U+2028 and U+0085 are not line breaks to it. Rule set 3
 * redacted only the first 4096 units (or up to the first space in them) and stored the
 * rest in clear. The bound is a scan bound only; values within it scrub as in rule set 3.
 */
import { describe, expect, it } from "vitest";
import * as scrub from "../src/lib/scrub.js";
import { MAX_SCRUB_CHARS, scrubText } from "../src/lib/scrub.js";
import { scrubLong } from "../src/lib/pipeline/raw-chunks.js";

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
    // Unterminated at the end of input, within the bound: up to the next whitespace.
    expect(scrubText(`"token": "${solid(CAP)}`)).toBe(`"token": "${R}`);
    expect(scrubText(`"token": "${solid(100)} tail`)).toBe(`"token": "${R} tail`);
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

  it("stops at the end of the line: a quoted value never spans a line break", () => {
    for (const nl of ["\n", "\r\n"]) {
      const v = solid(CAP * 2);
      expect(scrubText(`"password": "${v}${nl}next line kept "token": "x"`)).toBe(
        `"password": "${R}${nl}next line kept "token": "${R}"`,
      );
      expect(scrubText(`a: 1${nl}"password": "${spaced(CAP * 2)}"${nl}b: 2`)).toBe(
        `a: 1${nl}"password": "${R}"${nl}b: 2`,
      );
    }
  });

  it("stops at a lone CR too, but not at U+2028 or U+0085 (not line breaks to the scan)", () => {
    // LF, CRLF and a lone CR end the value; the rest of the line after the break is kept.
    expect(scrubText(`"password": "${solid(CAP + 5)}\rafter line"`)).toBe(`"password": "${R}\rafter line"`);
    // U+2028 and U+0085 are ordinary value units: the value runs on to its closing quote.
    for (const sep of ["\u2028", "\u0085"]) {
      const input = `"password": "${solid(CAP + 5)}${sep}after line" kept`;
      expect(scrubText(input)).toBe(`"password": "${R}" kept`);
      expect(scrub.overCapQuotedValueSpans(input)).toEqual([[0, input.length - 6]]);
    }
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
});
