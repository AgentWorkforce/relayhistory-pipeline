import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { scrubJson, scrubText } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubText as v7 } from "./fixtures/scrub-ruleset-7.js";

const encoder = new TextEncoder(), decoder = new TextDecoder();
const record = (text: string) => JSON.parse(decoder.decode(
  scrubRecords(encoder.encode(JSON.stringify({ text }))).scrubbed,
)).text as string;

describe("ruleset 8 does not consume public assignment values", () => {
  it("freezes v7 and records its demonstrated nested-assignment bypass", () => {
    const source = readFileSync(fileURLToPath(new URL("./fixtures/scrub-ruleset-7.ts", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(source).digest("hex")).toBe("8432a10f5802b1ea880f447c697482601f51a0b6aec9cde9bb785837c2e016c4");
    const input = "idempotency_key=password=fixture-only";
    expect(v7(input)).toBe(input);
    expect(scrubText(input)).toBe("idempotency_key=password=[REDACTED]");
  });

  it("retains benign public replay selectors with the previous syntax", () => {
    for (const key of ["idempotency_key", "IDEMPOTENCY_KEY", "Idempotency_Key"]) {
      for (const separator of ["=", ":", " : ", "\t=\t", "\u00a0=\ufeff"]) {
        for (const quote of ["", '"', "'", String.fromCharCode(96)]) {
          const input = key + separator + quote + "public-replay-42" + quote;
          expect(scrubText(input)).toBe(input);
          expect(scrubText(input)).toBe(v7(input));
          expect(record(input)).toBe(input);
        }
      }
    }
  });

  it("finds labelled credentials inside an exempt assignment in all entry points", () => {
    for (const key of ["idempotency_key", "IDEMPOTENCY_KEY"]) {
      for (const separator of ["=", ":", " : ", "\t=\t"]) {
        for (const quote of ["", '"', "'", String.fromCharCode(96)]) {
          for (const label of ["password", "token", "api_key", "client_secret", "credentials"]) {
            const input = key + separator + quote + label + "=fixture-only" + quote;
            expect(scrubText(input)).not.toContain("fixture-only");
            expect(scrubLong(input)).not.toContain("fixture-only");
            expect(scrubJson({ text: input }).text).not.toContain("fixture-only");
            expect(record(input)).not.toContain("fixture-only");
          }
        }
      }
    }
  });

  it("keeps scanning nested public assignments without recursion or a depth cutoff", () => {
    const prefix = "idempotency_key=".repeat(512);
    expect(scrubText(prefix + "password=fixture-only")).toBe(prefix + "password=[REDACTED]");
    expect(scrubText(prefix + "public-replay-42")).toBe(prefix + "public-replay-42");
  });

  it("retains the fix inside JSON-escaped tool text", () => {
    let input = "idempotency_key=password=fixture-only";
    for (let depth = 0; depth < 5; depth++) {
      expect(scrubText(input)).not.toContain("fixture-only");
      expect(record(input)).not.toContain("fixture-only");
      input = JSON.stringify(input);
    }
  });

  it("retains the fix when an assignment crosses the segment bound", () => {
    const prefix = "ordinary ".repeat(Math.floor(262144 / 9));
    const input = prefix + "idempotency_key=password=fixture-only after";
    const clean = scrubLong(input);
    expect(clean).not.toContain("fixture-only");
    expect(clean).toContain("idempotency_key=password=[REDACTED]");
    expect(scrubLong(clean)).toBe(clean);
  });
});
