import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { scrubJson, scrubText } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubText as v8, scrubJson as v8Json } from "./fixtures/scrub-ruleset-8.js";

const enc = new TextEncoder(), dec = new TextDecoder();
const records = <T>(input: T): T => JSON.parse(dec.decode(scrubRecords(enc.encode(JSON.stringify(input))).scrubbed));
const noTail = (value: unknown) => {
  const text = JSON.stringify(value);
  expect(text).not.toContain("SECRET_ALPHA");
  expect(text).not.toContain("SECRET_BETA");
  expect(text).toContain("[REDACTED]");
};
const allPaths = (inner: string) => {
  const text = `idempotency_key=${inner}`;
  for (const scrub of [scrubText, scrubLong]) {
    const clean = scrub(text);
    noTail(clean);
    expect(scrub(clean)).toBe(clean);
  }
  for (const input of [{ idempotency_key: inner }, { text }]) {
    const json = scrubJson(input), record = records(input);
    noTail(json);
    noTail(record);
    expect(scrubJson(json)).toEqual(json);
    expect(records(record)).toEqual(record);
  }
};

describe("ruleset 9 assignment extents", () => {
  it("pins the immutable v8 witness and demonstrates both bypasses", () => {
    const source = readFileSync(fileURLToPath(new URL("./fixtures/scrub-ruleset-8.ts", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(source).digest("hex")).toBe("fce75866a2dbf312f1865210fe343e1c372d216c3318dfacc27de6a3f8a82cc7");
    const segmenter = readFileSync(fileURLToPath(new URL("./fixtures/scrub-segmenter-8.txt", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(segmenter).digest("hex")).toBe("26d0733aabe8a219a7b1833e71eaa5f0aa6dfbfe4999b4d7d9870d0b993f8a68");
    expect(v8('idempotency_key=password="SECRET_ALPHA SECRET_BETA"')).toContain("SECRET_BETA");
    expect(v8(String.raw`idempotency_key=password\u003dSECRET_ALPHA`)).toContain("SECRET_ALPHA");
    expect(JSON.stringify(v8Json({ idempotency_key: 'password="SECRET_ALPHA SECRET_BETA"' }))).toContain("SECRET_BETA");
    expect(JSON.stringify(v8Json({ idempotency_key: String.raw`password\u003dSECRET_ALPHA` }))).toContain("SECRET_ALPHA");
  });

  it("redacts terminated and EOI multiword values in every entry point", () => {
    for (const label of ["password", "token", "api_key", "client_secret", "credentials"]) {
      for (const separator of ["=", ":", " : ", String.raw`\u003d`, String.raw`\u003D`, String.raw`\u003a`, String.raw`\u003A`, String.raw`\x3D`]) {
        for (const quote of ['"', "'", String.fromCharCode(96)]) {
          for (const close of [quote, ""]) {
            allPaths(`${label}${separator}${quote}SECRET_ALPHA SECRET_BETA${close}`);
          }
        }
        allPaths(`"${label}${separator}SECRET_ALPHA SECRET_BETA"`);
        allPaths(`"${label}${separator}SECRET_ALPHA SECRET_BETA`);
        // Parsing a JSON property removes its enclosing quotes. The public
        // property context must still prevent keeping an unlabelled tail.
        allPaths(`${label}${separator}SECRET_ALPHA SECRET_BETA`);
        allPaths(`"${label}"${separator}"SECRET_ALPHA SECRET_BETA"`);
      }
    }
  });

  it("handles JSON escaping depths and encoded separators without decoding output", () => {
    for (const label of ["password", "token", "api_key", "client_secret", "credentials"]) {
      for (const separator of ["=", String.raw`\u003d`, String.raw`\u003D`, String.raw`\u003a`, String.raw`\u003A`]) {
        let text = `idempotency_key=${label}${separator}"SECRET_ALPHA SECRET_BETA"`;
        for (let depth = 0; depth < 5; depth++) {
          for (const scrub of [scrubText, scrubLong]) {
            const clean = scrub(text);
            noTail(clean);
            expect(scrub(clean)).toBe(clean);
          }
          const json = scrubJson({ text }), record = records({ text });
          noTail(json);
          noTail(record);
          expect(scrubJson(json)).toEqual(json);
          expect(records(record)).toEqual(record);
          text = JSON.stringify(text);
        }
      }
    }
  });

  it("fails closed beyond the bounded quote escape depth", () => {
    for (const depth of [32, 63, 64, 127]) {
      const inner = 'password=' + "\\".repeat(depth) + '"SECRET_ALPHA SECRET_BETA';
      allPaths(inner);
      noTail(scrubText(inner));
    }
  });

  it("does not keep a secret tail after an existing redaction marker", () => {
    for (const separator of ["=", String.raw`\u003D`, String.raw`\u003A`]) {
      for (const quote of ['"', "'", String.fromCharCode(96)]) {
        allPaths(`password${separator}${quote}[REDACTED] SECRET_ALPHA SECRET_BETA${quote}`);
        allPaths(`password${separator}${quote}[REDACTED] SECRET_ALPHA SECRET_BETA`);
      }
      allPaths(`password${separator}[REDACTED] SECRET_ALPHA SECRET_BETA`);
    }
  });

  it("preserves public controls across all entry points and escaping depths", () => {
    for (const value of ["public-replay-42", '"public-replay-42"', "opaque=selector-42", "idempotency_key=".repeat(512) + "public-replay-42"]) {
      expect(scrubJson({ idempotency_key: value })).toEqual({ idempotency_key: value });
      expect(records({ idempotency_key: value })).toEqual({ idempotency_key: value });
      let text = `idempotency_key=${value}`;
      for (let depth = 0; depth < 5; depth++) {
        expect(scrubText(text)).toBe(text);
        expect(scrubLong(text)).toBe(text);
        expect(records({ text })).toEqual({ text });
        text = JSON.stringify(text);
      }
    }
  });

  it("retains public property context after a benign prefix and through descendants", () => {
    const value = 'ordinary prefix password=SECRET_ALPHA SECRET_BETA';
    noTail(scrubJson({ idempotency_key: value }));
    noTail(records({ idempotency_key: value }));
    noTail(scrubJson({ idempotency_key: [value, { text: value }] }));
    noTail(records({ idempotency_key: [value, { text: value }] }));
    noTail(scrubText('idempotency_key="' + value + '"'));
  });

  it("does not apply the preceding public property's context to an object key", () => {
    const input = { idempotency_key: "public-replay-42", "token=ordinary tail": "plain" };
    expect(records(input)).toEqual(scrubJson(input));
    expect(records(input)).toHaveProperty("token=[REDACTED] tail", "plain");
    expect(scrubText('{"key":"public-value"}')).toBe('{"key":"public-value"}');
  });

  it("scrubs a shadowed duplicate public property without silently dropping it", () => {
    const source = '{"idempotency_key":"password=SECRET_ALPHA SECRET_BETA","idempotency_key":"public-replay-42"}';
    const clean = dec.decode(scrubRecords(enc.encode(source)).scrubbed);
    noTail(clean);
    expect(clean.match(/"idempotency_key"/g)).toHaveLength(2);
    expect(clean).toContain("public-replay-42");
  });

  it("never detaches a multiword tail across the segment bound", () => {
    const prefix = "ordinary ".repeat(Math.floor(262144 / 9) - 3);
    for (const label of ["password", "token", "api_key", "client_secret", "credentials"]) {
      for (const close of ['"', ""]) {
        const text = prefix + `idempotency_key=${label}="SECRET_ALPHA SECRET_BETA${close}`;
        noTail(scrubLong(text));
        noTail(records({ text }));
      }
    }
    // A tail farther than the context window must not become an independent
    // safe-looking segment. A refusal is safe; returning any secret is not.
    const long = 'idempotency_key=password="SECRET_ALPHA ' + "padding ".repeat(45000) + 'SECRET_BETA"';
    try { noTail(scrubLong(long)); } catch (error) {
      expect(["chunk_unsegmentable", "chunk_string_too_long"]).toContain((error as { code: string }).code);
    }
  });
});
