import { describe, expect, it } from "vitest";
import { MAX_QUOTED_VALUE, MAX_SCRUB_CHARS, scrubJson, scrubText, unsplittableQuotedValueSpans } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubText as v8 } from "./fixtures/scrub-ruleset-8.js";

// Rule set 9, quoted key/value rule (rule 5) and arbitrarily layered percent encoding.
// Synthetic values only: SECRET_ALPHA / SECRET_BETA are markers, never real secrets.
// The literal backslash is written as BS (Bun cooks `\u` escapes inside String.raw).

const BS = "\\";
const encoder = new TextEncoder(), decoder = new TextDecoder();
const recordsRaw = (value: unknown) => decoder.decode(scrubRecords(encoder.encode(JSON.stringify(value))).scrubbed);
const leaks = (out: string) => /SECRET_(ALPHA|BETA|TAIL)/.test(out);
const FAR = "z".repeat(MAX_QUOTED_VALUE + 904);

/**
 * [name, input, exact safe output on scrubText, kind]: "closed" within the bound, "open"
 * (no matching close: redacted to the end of the input), "far" (closed past the bound:
 * redacted whole to its close, as rule set 4 does, the text after the close kept).
 */
type Kind = "closed" | "open" | "far";
type Case = [string, string, string, Kind];

const keyCases: Case[] = [];
for (const n of [0, 1, 16, 17, 31, 40]) {
  const r = BS.repeat(n);
  const head = `x ${r}"password${r}": ${r}"`;
  keyCases.push([`key run ${n}, closed`, `${head}SECRET_ALPHA SECRET_BETA${r}" tail`, `${head}[REDACTED]${r}" tail`, "closed"]);
  keyCases.push([`key run ${n}, unclosed`, `${head}SECRET_ALPHA SECRET_BETA tail`, `${head}[REDACTED]`, "open"]);
  keyCases.push([`key run ${n}, close beyond ${MAX_QUOTED_VALUE}`, `${head}SECRET_ALPHA ${FAR} SECRET_BETA${r}" tail`, `${head}[REDACTED]${r}" tail`, "far"]);
  keyCases.push([`key run ${n}, unclosed beyond ${MAX_QUOTED_VALUE}`, `${head}SECRET_ALPHA ${FAR} SECRET_BETA`, `${head}[REDACTED]`, "open"]);
  keyCases.push([`key run ${n}, multi-line closed`, `${head}SECRET_ALPHA\nSECRET_BETA${r}" tail`, `${head}[REDACTED]${r}" tail`, "closed"]);
}

// Value depth on its own: a depth-1 key (`\"password\"`) with a value opener behind 16, 17
// or 31 backslashes (rule set 8 scanned at most 16 before the value's quote).
const valueCases: Case[] = [];
for (const n of [1, 16, 17, 31, 40]) {
  const r = BS.repeat(n);
  const head = `{${BS}"password${BS}": ${r}"`;
  valueCases.push([`value run ${n}, closed`, `${head}SECRET_ALPHA SECRET_BETA${r}"}`, `${head}[REDACTED]${r}"}`, "closed"]);
  valueCases.push([`value run ${n}, unclosed`, `${head}SECRET_ALPHA SECRET_BETA}`, `${head}[REDACTED]`, "open"]);
  valueCases.push([`value run ${n}, close beyond ${MAX_QUOTED_VALUE}`, `${head}SECRET_ALPHA ${FAR} SECRET_BETA${r}"}`, `${head}[REDACTED]${r}"}`, "far"]);
  valueCases.push([`value run ${n}, multi-line closed`, `${head}SECRET_ALPHA\r\nSECRET_BETA${r}"}`, `${head}[REDACTED]${r}"}`, "closed"]);
}

const percentCases: Case[] = [
  ["triple %25253D + %252522", "x password%25253D%252522SECRET_ALPHA SECRET_BETA%252522 tail", "x password%25253D%252522[REDACTED]%252522 tail", "closed"],
  ["triple, lower hex", "x password%25253d%252522SECRET_ALPHA SECRET_BETA%252522 tail", "x password%25253d%252522[REDACTED]%252522 tail", "closed"],
  ["triple colon, %252527", "x api_key%25253a%252527SECRET_ALPHA SECRET_BETA%252527 tail", "x api_key%25253a%252527[REDACTED]%252527 tail", "closed"],
  ["six layers %2525252525253A + backtick", "x secret%2525252525253A%25252525252560SECRET_ALPHA SECRET_BETA%25252525252560 ok", "x secret%2525252525253A%25252525252560[REDACTED]%25252525252560 ok", "closed"],
  ["fewer layers is not the close", "x password%25253D%252522SECRET_ALPHA SECRET_BETA%2522 SECRET_BETA", "x password%25253D%252522[REDACTED]", "open"],
  ["more layers is not the close", "x password%253D%2522SECRET_ALPHA SECRET_BETA%252522 SECRET_BETA", "x password%253D%2522[REDACTED]", "open"],
  ["triple unclosed", "x password%25253D%252522SECRET_ALPHA SECRET_BETA", "x password%25253D%252522[REDACTED]", "open"],
  ["triple close beyond bound", `x password%25253D%252522SECRET_ALPHA ${FAR}%252522 SECRET_BETA`, "x password%25253D%252522[REDACTED]", "open"],
  ["%u0022 multi-word", "x password%3D%u0022SECRET_ALPHA SECRET_BETA%u0022 tail", "x password%3D%u0022[REDACTED]%u0022 tail", "closed"],
  ["%U0027 multi-word, mixed-case close", "x token=%U0027SECRET_ALPHA SECRET_BETA%u0027 tail", "x token=%U0027[REDACTED]%u0027 tail", "closed"],
  ["%u0060 behind %25 layers", "x secret%253D%25u0060SECRET_ALPHA SECRET_BETA%25U0060 tail", "x secret%253D%25u0060[REDACTED]%25U0060 tail", "closed"],
  ["%u0022 with %u003d separator", "x password%u003d%u0022SECRET_ALPHA SECRET_BETA%u0022 tail", "x password%u003d%u0022[REDACTED]%u0022 tail", "closed"],
  ["%u0022 multi-line", "x password=%u0022SECRET_ALPHA\nSECRET_BETA%u0022 tail", "x password=%u0022[REDACTED]%u0022 tail", "closed"],
  ["%u0022 closed by %22 is not the close", "x password=%u0022SECRET_ALPHA SECRET_BETA%22 SECRET_BETA", "x password=%u0022[REDACTED]", "open"],
  ["%u0022 unclosed", "x password=%u0022SECRET_ALPHA SECRET_BETA", "x password=%u0022[REDACTED]", "open"],
  ["%u0022 close beyond bound", `x password=%u0022SECRET_ALPHA ${FAR}%u0022 SECRET_BETA`, "x password=%u0022[REDACTED]", "open"],
];

/** `JSON.stringify` applied `depth` times. */
const atDepth = (s: string, depth: number) => {
  for (let d = 0; d < depth; d++) s = JSON.stringify(s);
  return s;
};

/** Secret absence and byte-identical second passes on the text, long, JSON and NDJSON paths. */
function expectSafeEverywhere(name: string, input: string) {
  const text = scrubText(input), long = scrubLong(input);
  const json = (scrubJson({ text: input }) as { text: string }).text;
  const raw = recordsRaw({ text: input });
  for (const out of [text, long, json, raw]) expect(leaks(out), name).toBe(false);
  expect(scrubText(text) === text, name).toBe(true);
  expect(scrubLong(long) === long, name).toBe(true);
  expect((scrubJson({ text: json }) as { text: string }).text === json, name).toBe(true);
  // The record path re-scrubs the escaped literal too: its second pass must not change it.
  expect(recordsRaw(JSON.parse(raw)) === raw, name).toBe(true);
}

describe("ruleset 9 rule 5: frozen R8 negative proof", () => {
  it("the R8 fixture leaks every new class", () => {
    const r17 = BS.repeat(17);
    const proofs = [
      `x ${r17}"password${r17}": ${r17}"SECRET_ALPHA SECRET_BETA${r17}" tail`,
      `x ${r17}"password${r17}": ${r17}"SECRET_ALPHA SECRET_BETA tail`,
      `{${BS}"password${BS}": ${r17}"SECRET_ALPHA SECRET_BETA${r17}"}`,
      atDepth(JSON.stringify({ password: "SECRET_ALPHA SECRET_BETA" }), 5),
      "x password%25253D%252522SECRET_ALPHA SECRET_BETA%252522 tail",
      "x password%3D%u0022SECRET_ALPHA SECRET_BETA%u0022 tail",
      "x token=%U0027SECRET_ALPHA SECRET_BETA%u0027 tail",
    ];
    for (const input of proofs) expect(leaks(v8(input)), input.slice(0, 60)).toBe(true);
  });
});

describe("ruleset 9 rule 5: uncapped key and value backslash runs, fail closed", () => {
  for (const [name, input, safe, kind] of [...keyCases, ...valueCases]) {
    it(`${name}: exact output, all paths safe, idempotent`, () => {
      expect(scrubText(input)).toBe(safe);
      expect(scrubLong(input)).toBe(safe);
      expect(scrubJson({ text: input })).toEqual({ text: safe });
      expect(JSON.parse(recordsRaw({ text: input }))).toEqual({ text: safe });
      expectSafeEverywhere(name, input);
      // Wrapped one escape level down (the stored form inside another string).
      expectSafeEverywhere(`${name} (wrapped)`, JSON.stringify({ note: input }));
    });

    it(`${name}: unsplittable span, with and without prefix`, () => {
      const keyStart = input.startsWith("{") ? 1 : 2;
      const spans = unsplittableQuotedValueSpans(input);
      const prefixed = unsplittableQuotedValueSpans(input, { prefix: true });
      const valueEnd = input.indexOf("SECRET_BETA") + "SECRET_BETA".length;
      if (kind === "open") {
        // [key start, end of text), with and without a prefix.
        expect(spans).toContainEqual([keyStart, input.length]);
        expect(prefixed).toContainEqual([keyStart, input.length]);
      } else if (kind === "far" || input.includes("\n")) {
        // Over the bound, or multi-line: [key start, value end) (the close's run excluded).
        expect(spans).toContainEqual([keyStart, valueEnd]);
        expect(prefixed).toContainEqual([keyStart, valueEnd]);
      } else {
        // A closed single-line value is decided within the text: never open to its end.
        expect(spans).not.toContainEqual([keyStart, input.length]);
        expect(prefixed).not.toContainEqual([keyStart, input.length]);
      }
    });
  }

  it("natural JSON.stringify depths 0-6 redact on every path (depth 5 = 31 backslashes)", () => {
    let s = JSON.stringify({ password: "SECRET_ALPHA SECRET_BETA" });
    for (let d = 0; d <= 6; d++) {
      expectSafeEverywhere(`depth ${d}`, s);
      expectSafeEverywhere(`depth ${d} unclosed`, s.slice(0, s.indexOf("SECRET_BETA") + 4));
      expectSafeEverywhere(`depth ${d} far`, s.replace("SECRET_ALPHA", `SECRET_ALPHA ${FAR}`));
      expectSafeEverywhere(`depth ${d} far unclosed`, s.replace("SECRET_ALPHA", `SECRET_ALPHA ${FAR}`).slice(0, s.indexOf("SECRET_BETA") + FAR.length + 5));
      s = JSON.stringify(s);
    }
    const depth5 = atDepth(JSON.stringify({ password: "SECRET_ALPHA SECRET_BETA" }), 5);
    expect(depth5).toContain(`${BS.repeat(31)}"password${BS.repeat(31)}"`);
    expect(scrubText(depth5)).toBe(depth5.replace("SECRET_ALPHA SECRET_BETA", "[REDACTED]"));
  });

  it("key and value runs on each side of the old cap: 16 and 17 both redact", () => {
    for (const n of [15, 16, 17, 18]) {
      const r = BS.repeat(n);
      expect(scrubText(`${r}"password${r}": ${r}"SECRET_ALPHA SECRET_BETA${r}"`)).toBe(`${r}"password${r}": ${r}"[REDACTED]${r}"`);
      expect(scrubText(`${BS}"password${BS}": ${r}"SECRET_ALPHA SECRET_BETA${r}"`)).toBe(`${BS}"password${BS}": ${r}"[REDACTED]${r}"`);
    }
  });

  it("keeps public replay selectors (B1/B2) and the P1 positive", () => {
    for (const benign of ["idempotency_key=public-replay-42", 'IDEMPOTENCY_KEY : "public-replay-42"']) {
      for (let d = 0; d <= 4; d++) {
        const input = atDepth(benign, d);
        expect(scrubText(input)).toBe(input);
        expect(scrubLong(input)).toBe(input);
        expect(scrubJson({ text: input })).toEqual({ text: input });
        expect(recordsRaw({ text: input })).toBe(JSON.stringify({ text: input }));
      }
    }
    // The exemption takes the same layered separators as the rule.
    for (const sep of ["%3D", "%253d", "%25253D", "%2525252525253a", "%u003D", "%25u003d"]) {
      expect(scrubText(`idempotency_key${sep}public-replay-42`)).toBe(`idempotency_key${sep}public-replay-42`);
      expect(scrubText(`password${sep}SECRET_ALPHA`)).toBe(`password${sep}[REDACTED]`);
    }
    expect(scrubText("idempotency_key=password=fixture-only")).toBe("idempotency_key=password=[REDACTED]");
  });

  it("does not let a segment cut leave the tail of a deep-key value keyless", () => {
    const r = BS.repeat(17);
    for (const off of [-3000, -1500, -40]) {
      const head = ` ${r}"password${r}": ${r}"`;
      const pad = "ordinary ".repeat(MAX_SCRUB_CHARS).slice(0, MAX_SCRUB_CHARS + off - head.length);
      const value = Array.from({ length: 300 }, (_, i) => `SECRET_W${i}`).join(" ");
      const unclosed = `${pad}${head}SECRET_ALPHA ${value} after ${"tail ".repeat(200)}`;
      expect(scrubLong(unclosed) === `${pad}${head}[REDACTED]`, `${off}`).toBe(true);
      expect(/SECRET_/.test(JSON.parse(recordsRaw({ text: unclosed })).text), `${off}`).toBe(false);
      // A multi-line closed value across the bound is redacted whole, the tail kept.
      const closed = `${pad}${head}SECRET_ALPHA\n${value}${r}" after`;
      expect(scrubLong(closed) === `${pad}${head}[REDACTED]${r}" after`, `${off}`).toBe(true);
      expect(/SECRET_/.test(JSON.parse(recordsRaw({ text: closed })).text), `${off}`).toBe(false);
    }
  });
});

describe("ruleset 9: arbitrarily layered percent encoding and percent-u quotes", () => {
  for (const [name, input, safe, kind] of percentCases) {
    it(`${name}: exact output, all paths safe, idempotent, spans`, () => {
      expect(scrubText(input)).toBe(safe);
      expect(scrubLong(input)).toBe(safe);
      expect(scrubJson({ text: input })).toEqual({ text: safe });
      expect(JSON.parse(recordsRaw({ text: input }))).toEqual({ text: safe });
      expectSafeEverywhere(name, input);
      for (let d = 1; d <= 4; d++) expectSafeEverywhere(`${name} depth ${d}`, atDepth(input, d));
      if (kind === "open") {
        expect(unsplittableQuotedValueSpans(input)).toContainEqual([2, input.length]);
        expect(unsplittableQuotedValueSpans(input, { prefix: true })).toContainEqual([2, input.length]);
      }
    });
  }

  it("long-bound tail: a layered percent value failing closed near the cut", () => {
    for (const off of [-3000, -40]) {
      const head = " password%25253D%252522";
      const pad = "ordinary ".repeat(MAX_SCRUB_CHARS).slice(0, MAX_SCRUB_CHARS + off - head.length);
      const input = `${pad}${head}SECRET_ALPHA ${"SECRET_BETA ".repeat(400)}`;
      expect(scrubLong(input) === `${pad}${head}[REDACTED]`, `${off}`).toBe(true);
      expect(/SECRET_/.test(JSON.parse(recordsRaw({ text: input })).text), `${off}`).toBe(false);
    }
  });
});

describe("ruleset 9 rule 5 and percent layers stay linear on 64 KiB adversarial inputs", () => {
  const K = 65536;
  const inputs: Array<[string, string]> = [
    ["growing key runs, unclosed", Array.from({ length: 400 }, (_, i) => { const r = BS.repeat(17 + i); return `${r}"password${r}": ${r}"x `; }).join("")],
    ["growing key runs, closed", Array.from({ length: 400 }, (_, i) => { const r = BS.repeat(17 + i); return `${r}"password${r}": ${r}"x${r}" `; }).join("")],
    ["mismatched key runs", Array.from({ length: 2000 }, (_, i) => `${BS.repeat(17 + (i % 30))}"password${BS.repeat(18 + (i % 30))}": `).join("")],
    ["one long run then key", `${BS.repeat(K - 40)}"password${BS}": "x`],
    ["key then one long value run", `"password": ${BS.repeat(K - 40)}"x`],
    ["key with long run, near-closes", `${BS.repeat(17)}"password${BS.repeat(17)}": ${BS.repeat(17)}"${`${BS.repeat(16)}"`.repeat(3800)}`],
    ["repeated deep keys, values closing late", Array.from({ length: 16 }, () => `${BS.repeat(31)}"token${BS.repeat(31)}": ${BS.repeat(31)}"${"y".repeat(3900)}${BS.repeat(31)}" `).join("")],
    ["deep keys, values closing past the bound", Array.from({ length: 12 }, () => `${BS.repeat(31)}"token${BS.repeat(31)}": ${BS.repeat(31)}"${"y".repeat(5000)}${BS.repeat(31)}" `).join("")],
    ["deep key, long runs before candidate closes", `${BS.repeat(17)}"password${BS.repeat(17)}": ${BS.repeat(17)}"${`${BS.repeat(1000)}"`.repeat(65)}`],
    ["many deep keys in one unclosed value", `"password": "${Array.from({ length: 1500 }, (_, i) => `${BS.repeat(17 + (i % 20))}"token${BS.repeat(17 + (i % 20))}": `).join("")}`],
    ["percent: one huge separator layer run", `password%${"25".repeat((K - 20) / 2)}3D`],
    ["percent: huge opener layers, unclosed", `password%3D%${"25".repeat((K - 20) / 2)}22`],
    ["percent: opener then near-close candidates", `password%3D%25252522${"%252525".repeat(K / 7)}`],
    ["percent: alternating layer counts", Array.from({ length: 3000 }, (_, i) => `pwd%${"25".repeat(i % 9)}3D%${"25".repeat(i % 7)}22x `).join("")],
    ["percent: exemption with huge layers", `idempotency_key%${"25".repeat((K - 30) / 2)}3d`],
    ["percent-u: openers, never closed", "token=%u0022".repeat(K / 12)],
    ["percent: many %25 with no hex", "password%3D%22" + "%25".repeat(K / 3)],
  ];
  for (const [name, input] of inputs) {
    it(`${name}: under 2000 ms`, () => {
      const text = input.slice(0, K);
      const started = performance.now();
      const out = scrubText(text);
      const ms = performance.now() - started;
      const started2 = performance.now();
      unsplittableQuotedValueSpans(text, { prefix: true });
      const ms2 = performance.now() - started2;
      console.log(`[timing] ${name}: ${text.length} chars, scrubText ${ms.toFixed(1)} ms, spans ${ms2.toFixed(1)} ms`);
      expect(typeof out).toBe("string");
      expect(ms).toBeLessThan(2000);
      expect(ms2).toBeLessThan(2000);
    });
  }
});
