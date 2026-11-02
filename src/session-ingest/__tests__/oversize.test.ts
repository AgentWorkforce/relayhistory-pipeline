/**
 * `scrubOversize` (raw-chunks.ts), the ingest's answer to
 * `chunk_string_too_long`: an over-bound text is scrubbed in segments, never
 * truncated and never stored unscrubbed.
 */
import { describe, expect, test } from "bun:test";
import { MAX_SCRUB_CHARS, scrubText } from "@rhc/lib/scrub.ts";
import { oversizeRedaction, scrubOversize, scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";

const KEY = "AKIAABCDEFGHIJKLMNOP";
const PEM = `-----BEGIN PRIVATE KEY-----\n${"QUJD".repeat(400)}\n-----END PRIVATE KEY-----`;

describe("scrubOversize", () => {
  test("within the bound it is scrubText", () => {
    expect(scrubOversize(`a ${KEY} b`)).toBe(scrubText(`a ${KEY} b`));
  });

  test("keeps every character of a long text and redacts secrets in every segment", () => {
    const text = Array.from({ length: 50_000 }, (_, i) => (i % 997 === 0 ? KEY : `w${i}`)).join(" ") + ` ${KEY}`;
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const out = scrubOversize(text);
    expect(out).not.toContain(KEY);
    expect(out.split(" ").length).toBe(text.split(" ").length);
    expect(out).toBe(scrubOversize(text)); // deterministic: a replay seals the same bytes
  });

  test("never cuts through a secret that sits on the natural boundary", () => {
    // The last separator before the bound is the space inside "Bearer <token>".
    const token = "A".repeat(40);
    const text = "x".repeat(MAX_SCRUB_CHARS - 8) + " Bearer " + token + " " + "y ".repeat(10_000);
    expect(text[MAX_SCRUB_CHARS - 1]).toBe(" ");
    expect(scrubText(text.slice(0, MAX_SCRUB_CHARS))).toContain("Bearer");
    const out = scrubOversize(text);
    expect(out).not.toContain(token);
  });

  test("never splits a private key across segments", () => {
    const filler = "z, ".repeat(Math.ceil((MAX_SCRUB_CHARS - 1000) / 3));
    const out = scrubOversize(filler + PEM + " tail".repeat(5_000));
    expect(out).not.toContain("QUJDQUJD");
  });

  test("never splits a private key larger than the cut's scrub context", () => {
    // 12 KiB of base64 lines: the cut's 4 KiB + 4 KiB window sees neither delimiter.
    const body = Array.from({ length: 190 }, (_, i) => `M${String(i).padStart(3, "0")}${"QUJD".repeat(15)}`).join("\n");
    const big = `-----BEGIN PRIVATE KEY-----\n${body}\n-----END PRIVATE KEY-----`;
    expect(big.length).toBeGreaterThan(12_000);
    const filler = "z, ".repeat(Math.ceil((MAX_SCRUB_CHARS - 6_000) / 3));
    const out = scrubOversize(filler + big + " tail".repeat(5_000));
    expect(out).not.toContain("QUJDQUJD");
  });

  test("with no safe boundary the whole text is redacted, never stored unscrubbed", () => {
    const text = "q".repeat(MAX_SCRUB_CHARS + 10);
    expect(scrubOversize(text)).toBe(oversizeRedaction(text.length));
  });
});

describe("sealRawChunk oversize option", () => {
  const line = JSON.stringify({ evidence: Array.from({ length: 60_000 }, (_, i) => `v${i}`).join(",") }) + "\n";
  const source = new TextEncoder().encode(line);

  test("default (every client-facing writer): an over-bound string is still refused", () => {
    expect(() => scrubRecords(source)).toThrow(/scrub limit/);
    expect(() => scrubRecords(source, { oversize: "refuse" })).toThrow(/scrub limit/);
  });

  test("segment: the line is scrubbed whole and kept", () => {
    const { scrubbed } = scrubRecords(source, { oversize: "segment" });
    expect(new TextDecoder().decode(scrubbed)).toBe(line);
  });
});
