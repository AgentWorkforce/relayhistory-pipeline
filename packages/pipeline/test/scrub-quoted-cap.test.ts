/**
 * Rule set 4: a quoted secret value longer than the quoted rule's scan bound
 * (`MAX_QUOTED_VALUE`, 4096 UTF-16 code units in `lib/scrub.ts`) is redacted whole, up to
 * its closing quote or, with none, to the end of its line or of the input. Rule set 3
 * redacted only the first 4096 units (or up to the first space in them) and stored the
 * rest in clear. The bound is a scan bound only; values within it scrub as in rule set 3.
 */
import { describe, expect, it } from "vitest";
import * as scrub from "../src/lib/scrub.js";
import { MAX_SCRUB_CHARS, scrubText } from "../src/lib/scrub.js";
import { scrubLong } from "../src/lib/pipeline/raw-chunks.js";

/** `MAX_QUOTED_VALUE` in lib/scrub.ts (not exported). */
const CAP = 4096;
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
      expect(spans).toEqual([[14, 14 + MiB]]);
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

  it("refuses a text whose only cuts fall inside an over-bound value", () => {
    const value = spaced(MAX_SCRUB_CHARS + 1024);
    expect(() => scrubLong(`"password": "${value}"`)).toThrow(/scrub limit/);
  });
});
