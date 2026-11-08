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
 *
 * Every value here is synthetic; `SECRETTAIL`/`SECRETTAILMARK` mark the bytes that must
 * not survive.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MAX_QUOTED_VALUE, scrubJson, scrubRecord, scrubText } from "../src/lib/scrub.js";
import { scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
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
 * Leak 2 does not: v4 output already holds the shrunk value, under the bound, so the
 * original length is gone and rule set 5 keeps it exactly as rule set 4 stored it. Only
 * the original raw bytes can be re-scrubbed for that class.
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

  it("does not catch leak 2 in stored v4 output: the shrunk value is under the bound (needs raw bytes)", () => {
    const text = `"password": "${SHRINKABLE}" after`;
    const stored = v4Text(text);
    expect(stored).toContain("SECRETTAILMARK");
    expect(stored.length).toBeLessThan(MAX_QUOTED_VALUE);
    // Rule set 5 sees an under-bound value beginning with [REDACTED] and keeps it as v4 did.
    expect(scrubText(stored)).toBe(stored);
    expect(JSON.stringify(scrubJson({ content: stored }))).toContain("SECRETTAILMARK");
    // The original bytes, re-scrubbed, are clean.
    expect(scrubText(text)).toBe(`"password": "${R}" after`);
  });
});
