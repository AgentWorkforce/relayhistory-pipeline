/**
 * Rule set 6: two defects reproduced in rule set 5 (fixture `fixtures/scrub-ruleset-5.ts`).
 *
 * 1. Bare values classified by their first `MAX_QUOTED_VALUE` units (leak). An unquoted
 *    value after a quoted secret key (`"password": 0000…0SECRETTAIL`) was classified on
 *    its first 4096 units only: 4095 or more digits then an alphanumeric tail passed as a
 *    number (or a low-entropy value) and the tail was stored. The whole bare value is
 *    classified now, and a bare value at or over the bound is redacted whole.
 * 2. Private-key boundaries consumed by the over-cap pass (data loss). A
 *    `-----BEGIN … PRIVATE KEY-----` header before an over-cap quoted secret value whose
 *    `-----END … PRIVATE KEY-----` footer was inside the value: the first (over-cap) pass
 *    redacted the value, footer included, so the private-key rule paired the header with
 *    nothing and redacted to the end of the input. The redaction now runs from the header
 *    through the value's close and nothing after it. The same pairing also covers a
 *    within-bound value holding the footer (rule set 5 kept the value's tail after the
 *    footer) and a block opened inside a value and closed after it.
 *
 * 3. Found while forward-merging main (#354): a quoted secret value holding a header
 *    whose footer is inside a LATER quoted secret value (`"password": "…-----BEGIN …" …
 *    "token": "…-----END …-----SECRETTAIL"`). Redacting to that footer removes the later
 *    value's key; putting this value's closing quote back then left the rest of the later
 *    value (`SECRETTAIL`) with no key in view, and it was kept (rule set 5 and #354 did not
 *    keep it). When a quote lies between the value and the footer, the close is no longer
 *    put back (or, with a header before the value too, the redaction runs to the end).
 *
 * 4. Found in review of f6a97053: the same with the footer inside a later UNQUOTED
 *    credential (`token=…`, `Authorization: Bearer …`, `?access_token=…`). With no quote
 *    between, the close was put back and the rest of that credential, glued to the footer,
 *    was kept with its label gone. See scrub-ruleset-6-later-unquoted.test.ts.
 *
 * PR #354 made the fixes for 1 and 2 in place under version 5 with tests in
 * scrub-ruleset-5.test.ts; those tests are folded in here (its `"token": <digits>` kept
 * assertion is superseded: rule set 6 redacts any bare value at or over the bound), and
 * scrub-ruleset-5.test.ts now runs against the frozen rule set 5 source.
 *
 * Every value here is synthetic; `SECRETTAIL` marks bytes that must not survive,
 * `KEEPTRAIL`/`HISTORY_TAIL` bytes that must.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  MAX_QUOTED_VALUE,
  MAX_SCRUB_CHARS,
  overCapQuotedValueSpans,
  scrubJson,
  scrubRecord,
  scrubText,
  unsplittableQuotedValueSpans,
} from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";

const R = "[REDACTED]";
const enc = new TextEncoder();
const dec = new TextDecoder();
const records = (text: string) => dec.decode(scrubRecords(enc.encode(text)).scrubbed);
/** Plain lines past the scrub bound, for the segmented path. */
const PAD = "lorem ipsum dolor sit amet\n".repeat(Math.ceil(MAX_SCRUB_CHARS / 27) + 10);
const H = "-----BEGIN RSA PRIVATE KEY-----";
const F = "-----END RSA PRIVATE KEY-----";
/** A synthetic base64-looking body line (not key material). */
const BODY = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo";

/** No secret bytes left: the marker, a run of the value filler, a body line or a PEM boundary. */
function noKeyMaterial(out: string, label: string) {
  expect(out.includes("SECRETTAIL"), label).toBe(false);
  expect(out.includes("a".repeat(12)), label).toBe(false);
  expect(out.includes(BODY), label).toBe(false);
  expect(/-----(?:BEGIN|END) [A-Z ]*PRIVATE KEY/.test(out), label).toBe(false);
}

/** One text through scrubText, scrubLong (alone and after PAD), scrubJson, scrubRecord and scrubRecords. */
function allPaths(text: string, expected: string, kept: string[]) {
  const record = (content: string) => ({ type: "user", message: { content: [{ type: "tool_result", content }] } });
  const outs: Array<[string, string, string]> = [
    ["scrubText", scrubText(text), expected],
    ["scrubLong", scrubLong(text), expected],
    ["scrubLong+PAD", scrubLong(PAD + text), PAD + expected],
    ["scrubJson", JSON.stringify(scrubJson({ content: text })), JSON.stringify({ content: expected })],
    ["scrubRecord", JSON.stringify(scrubRecord({ trace: { content: text } })), JSON.stringify({ trace: { content: expected } })],
    ["scrubRecords", records(`${JSON.stringify(record(text))}\n`), `${JSON.stringify(record(expected))}\n`],
    ["scrubRecords raw", records(`${text}\n`), `${expected}\n`],
  ];
  for (const [path, out, want] of outs) {
    expect(out === want, `${path}: ${JSON.stringify(out.slice(-160))}`).toBe(true);
    noKeyMaterial(out, path);
    // In the JSON paths a kept fragment appears string-escaped.
    for (const k of kept) expect(out.includes(k) || out.includes(JSON.stringify(k).slice(1, -1)), `${path} keeps ${k}`).toBe(true);
  }
  // Stable: a second scrub changes nothing.
  expect(scrubText(expected)).toBe(expected);
  expect(scrubLong(PAD + expected)).toBe(PAD + expected);
}

describe("rule set 6: a bare value after a quoted secret key is classified whole (defect 1)", () => {
  const CONTROLS = `"max_tokens": 512, "token": 4096, "enabled": true`;
  for (const key of ["password", "api_key", "token"]) {
    for (const n of [MAX_QUOTED_VALUE - 1, MAX_QUOTED_VALUE, MAX_QUOTED_VALUE + 1]) {
      it(`"${key}": ${n} digits then an alphanumeric tail is redacted whole`, () => {
        const bare = `${"7".repeat(n)}SECRETTAILx9Q2`;
        // Line-terminated, delimiter-terminated and unterminated (end of input).
        allPaths(`"${key}": ${bare} KEEPTRAIL\n${CONTROLS}`, `"${key}": ${R} KEEPTRAIL\n${CONTROLS}`, ["KEEPTRAIL", CONTROLS]);
        allPaths(`{"${key}": ${bare}, "note": "KEEPTRAIL"}`, `{"${key}": ${R}, "note": "KEEPTRAIL"}`, ["KEEPTRAIL"]);
        allPaths(`${CONTROLS}\n"${key}": ${bare}`, `${CONTROLS}\n"${key}": ${R}`, [CONTROLS]);
      });
    }
  }

  it("an over-cap bare value is redacted whole even when it is only digits (fail closed)", () => {
    allPaths(`"token": ${"7".repeat(5000)} KEEPTRAIL`, `"token": ${R} KEEPTRAIL`, ["KEEPTRAIL"]);
  });

  it("an over-cap digit run followed by a long generated tail is redacted whole (cross-checked against #354)", () => {
    const tail = "Q7xK2mP9vR4tW8nB3cY6".repeat(310);
    allPaths(`"token": ${"7".repeat(MAX_QUOTED_VALUE)}${tail} after`, `"token": ${R} after`, ["after"]);
    // #354's reproduction: a 4096-unit digit prefix, then a long alphanumeric tail.
    const prefix = "0123456789".repeat(409) + "012345";
    expect(prefix.length).toBe(MAX_QUOTED_VALUE);
    const tail354 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".repeat(100);
    allPaths(`"token": ${prefix}${tail354} after`, `"token": ${R} after`, ["after"]);
  });

  it("within the bound, literals and numbers stay kept", () => {
    for (const text of [CONTROLS, `"token": ${"7".repeat(MAX_QUOTED_VALUE - 1)}`, `"password": null`]) {
      expect(scrubText(text)).toBe(text);
      expect(scrubLong(PAD + text)).toBe(PAD + text);
      expect(records(`${text}\n`)).toBe(`${text}\n`);
    }
    // A JSON record keeps its number lexemes byte for byte (only card numbers are redacted).
    const json = `{"max_tokens":512,"token":4096,"enabled":true}\n`;
    expect(records(json)).toBe(json);
  });

  it("a bare value straddling the segment cut is redacted whole", () => {
    const head = "lorem ipsum dolor sit amet ".repeat(Math.ceil(MAX_SCRUB_CHARS / 27)).slice(0, MAX_SCRUB_CHARS - 1000);
    const text = `${head} "password": ${"7".repeat(MAX_QUOTED_VALUE)}SECRETTAIL KEEPTRAIL ${head}`;
    const out = scrubLong(text);
    expect(out.includes("SECRETTAIL")).toBe(false);
    expect(out).toBe(`${head} "password": ${R} KEEPTRAIL ${head}`);
  });
});

describe("rule set 6: private-key boundaries around an over-cap quoted value (defect 2)", () => {
  const value = `${"a".repeat(MAX_QUOTED_VALUE + 1)}${F}`;
  const NEXT = `\n"user": "bob", "note": "KEEPTRAIL two"\n`;

  it("header before the value, footer inside it: header through the close, tail kept", () => {
    const text = `${H}\n"password": "${value}" HISTORY_TAIL KEEPTRAIL${NEXT}`;
    expect(overCapQuotedValueSpans(text).length).toBe(1);
    allPaths(text, `${R} HISTORY_TAIL KEEPTRAIL${NEXT}`, ["HISTORY_TAIL", "KEEPTRAIL", "KEEPTRAIL two"]);
  });

  it("the same with a body and text before the header", () => {
    const text = `before KEEPTRAIL zero\n${H}\n${BODY}\n"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}\n${F}\n" HISTORY_TAIL${NEXT}`;
    allPaths(text, `before KEEPTRAIL zero\n${R} HISTORY_TAIL${NEXT}`, ["KEEPTRAIL zero", "HISTORY_TAIL", "KEEPTRAIL two"]);
  });

  it("two over-cap values, the first holding a footer for a header before it", () => {
    const text = `${H}\n"password": "${value}" HISTORY_TAIL\n"api_key": "${"a".repeat(MAX_QUOTED_VALUE + 1)}" KEEPTRAIL${NEXT}`;
    allPaths(text, `${R} HISTORY_TAIL\n"api_key": "${R}" KEEPTRAIL${NEXT}`, ["HISTORY_TAIL", "KEEPTRAIL", "KEEPTRAIL two"]);
  });

  it("within the bound, a value holding the footer for a header before it", () => {
    for (const sep of [" ", "\n"]) {
      const text = `${H}\n${BODY}\n"password": "short${sep}${F}${sep}SECRETTAIL" KEEPTRAIL${NEXT}`;
      allPaths(text, `${R} KEEPTRAIL${NEXT}`, ["KEEPTRAIL", "KEEPTRAIL two"]);
    }
  });

  it("a block wholly inside an over-cap value: the value only", () => {
    const text = `"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}\n${H}\n${BODY}\n${F}\n" KEEPTRAIL${NEXT}`;
    allPaths(text, `"password": "${R}" KEEPTRAIL${NEXT}`, ["KEEPTRAIL", "KEEPTRAIL two"]);
  });

  it("a block opened inside an over-cap value and closed after it: to the footer, text after kept", () => {
    const text = `"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}\n${H}\n${BODY}" ${BODY}\n${F}\nKEEPTRAIL${NEXT}`;
    allPaths(text, `"password": "${R}"\nKEEPTRAIL${NEXT}`, ["KEEPTRAIL", "KEEPTRAIL two"]);
  });

  it("unadorned header before the value, footer then a suffix inside it: no stray quote (cross-checked against #354)", () => {
    const h = "-----BEGIN PRIVATE KEY-----";
    const f = "-----END PRIVATE KEY-----";
    for (const suffix of ["", "SECRETTAIL", "Q7xK2mP9vR4tW8nB3cY6".repeat(5)]) {
      const text = `${h}\n"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}${f}${suffix}" KEEPTRAIL${NEXT}`;
      allPaths(text, `${R} KEEPTRAIL${NEXT}`, ["KEEPTRAIL", "KEEPTRAIL two"]);
    }
  });

  it("the segmenter never cuts between the header and the value's end", () => {
    const words = (n: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(n / 27)).slice(0, n);
    for (const before of [MAX_SCRUB_CHARS - 600, MAX_SCRUB_CHARS - 3000, MAX_SCRUB_CHARS - 5000]) {
      const head = `${words(before)}\n`;
      const block = `${H}\n${BODY}\n"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)} ${words(2000)}\n${F}"`;
      const text = `${head}${block} HISTORY_TAIL KEEPTRAIL${NEXT}${words(5000)}`;
      expect(head.length).toBeLessThan(MAX_SCRUB_CHARS);
      expect(head.length + block.length).toBeGreaterThan(MAX_SCRUB_CHARS);
      const expected = `${head}${R} HISTORY_TAIL KEEPTRAIL${NEXT}${words(5000)}`;
      const out = scrubLong(text);
      noKeyMaterial(out, `scrubLong ${before}`);
      expect(out === expected, `scrubLong ${before}`).toBe(true);
      expect(records(`${text}\n`) === `${expected}\n`, `scrubRecords ${before}`).toBe(true);
    }
    // The value itself stays one unsplittable span.
    const text = `${H}\n"password": "${value}" HISTORY_TAIL`;
    expect(unsplittableQuotedValueSpans(text).some(([a, b]) => a <= H.length + 1 && b >= text.length - 14)).toBe(true);
  });
});

describe("rule set 6: a private-key footer inside a later quoted secret value (defect 3)", () => {
  const h = "-----BEGIN PRIVATE KEY-----";
  const f = "-----END PRIVATE KEY-----";
  for (const q of ['"', "'", "`"]) {
    for (const [label, n] of [["within the bound", 3], ["over the bound", MAX_QUOTED_VALUE + 1]] as const) {
      it(`header in one ${q}-quoted value, footer in the next, ${label}: the next value's rest is not kept`, () => {
        const later = `${q}token${q}: ${q}${"a".repeat(n)}${f}SECRETTAIL${q}`;
        const text = `${q}password${q}: ${q}abc${h}\n${BODY}${q} KEEPMID ${later} KEEPTRAIL`;
        for (const out of [scrubText(text), scrubLong(text), scrubLong(PAD + text), records(`${text}\n`)]) {
          noKeyMaterial(out, label);
          expect(out.startsWith(`${q}password${q}: ${q}${R}`) || out.startsWith(`${PAD}${q}password${q}: ${q}${R}`)).toBe(true);
        }
        const json = JSON.stringify(scrubJson({ content: text }));
        noKeyMaterial(json, `scrubJson ${label}`);
      });
    }
  }

  it("with a header before the value too: redacted to the end of the input", () => {
    const later = `"token": "${"a".repeat(MAX_QUOTED_VALUE + 1)}${F}SECRETTAIL${"Q7xK2mP9vR4tW8nB3cY6".repeat(3)}"`;
    const text = `before KEEPTRAIL\n${H}\n${BODY}\n"password": "${F}${H}\nSECRETTAIL" mid ${later} after`;
    expect(scrubText(text)).toBe(`before KEEPTRAIL\n${R}`);
    expect(scrubLong(text)).toBe(`before KEEPTRAIL\n${R}`);
    // The raw record path redacts through the line's end, its line break included.
    for (const out of [scrubLong(PAD + text), records(`${text}\n`), JSON.stringify(scrubJson({ content: text }))]) {
      noKeyMaterial(out, "path");
      expect(out.includes("mid") || out.includes("after")).toBe(false);
    }
  });

  it("a footer after the value with no quote between still keeps the text after it", () => {
    const text = `"password": "${"a".repeat(MAX_QUOTED_VALUE + 1)}\n${H}\n${BODY}" ${BODY}\n${F}\nKEEPTRAIL`;
    allPaths(text, `"password": "${R}"\nKEEPTRAIL`, ["KEEPTRAIL"]);
  });
});

describe("rule set 6: the shared corpus's record cases through the raw-chunk record path", () => {
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

  // Moved from scrub-ruleset-5.test.ts: the corpus holds rule set 6 cases, run here against
  // the current source (scrub-ruleset-5.test.ts runs the frozen rule set 5 source).
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
