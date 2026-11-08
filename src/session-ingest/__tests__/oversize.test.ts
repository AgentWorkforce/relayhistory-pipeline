/**
 * oversize.ts: before sealing, a string the pipeline segmenter (`scrubLong`,
 * main's rule set 2) cannot cut becomes one redaction marker, so a delivery
 * batch always lands and nothing unscrubbed is stored. A string it can cut is
 * left for it. The pipeline itself still refuses for every other writer.
 */
import { describe, expect, test } from "bun:test";
import { MAX_SCRUB_CHARS } from "@rhc/lib/scrub.ts";
import { scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import { fitForScrub, oversizeRedaction } from "../oversize.ts";
import { liveLine } from "../writer.ts";

const KEY = "AKIAABCDEFGHIJKLMNOP";
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const base = { schema_version: 1 as const, origin_id: "o", record_id: "r1", revision_id: "r1:1", revision: 1, kind: "tool_call" as const, source: "claude", session_id: "s", operation: "upsert" as const, digest: "d" };
// Canonical JSON evidence has no whitespace: no safe cut for scrubLong.
const unbroken = JSON.stringify({ items: Array.from({ length: 60_000 }, (_, i) => `v${i}`) });

describe("fitForScrub", () => {
  test("the pipeline alone still refuses a string with no safe cut (every other writer)", () => {
    expect(() => scrubRecords(enc(liveLine({ ...base, payload: { input_json: unbroken } }, "t") + "\n"))).toThrow(/scrub limit/);
  });

  test("redacts only that string; the record, its other fields and their scrubbing are kept", () => {
    const fitted = fitForScrub({ ...base, payload: { input_json: unbroken, note: `k ${KEY}`, n: 3 } });
    const out = JSON.parse(dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed));
    expect(out.record_id).toBe("r1");
    expect(out.payload.input_json).toBe(oversizeRedaction(unbroken.length));
    expect(out.payload.n).toBe(3);
    expect(out.payload.note).not.toContain(KEY);
  });

  test("rule set 4: a string cut only inside an over-bound quoted value becomes the marker; the refusal holds no content", () => {
    // Every whitespace in the string falls inside one quoted secret value longer than
    // MAX_QUOTED_VALUE, which rule set 4 never splits, so the segmenter refuses it.
    const value = "zeta kappa omicron SECRETTAILMARK ".repeat(Math.ceil((MAX_SCRUB_CHARS + 1024) / 34));
    const text = `"password": "${value}"`;
    let error: unknown;
    try {
      scrubRecords(enc(liveLine({ ...base, payload: { text } }, "t") + "\n"));
    } catch (e) {
      error = e;
    }
    expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
    expect((error as Error).message).not.toContain("SECRETTAILMARK");
    expect((error as Error).message).not.toContain("zeta");
    const fitted = fitForScrub({ ...base, payload: { text, n: 1 } });
    const out = dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed);
    expect(JSON.parse(out).payload.text).toBe(oversizeRedaction(text.length));
    expect(JSON.parse(out).payload.n).toBe(1);
    expect(out).not.toContain("SECRETTAILMARK");
  });

  test("rule set 4: a multi-line over-bound quoted value crossing the bound becomes the marker; the refusal holds no content", () => {
    // The value passes the quoted rule's 4096-unit scan bound on its first line, then runs
    // across many lines (LF, CRLF, U+2028) past MAX_SCRUB_CHARS. Line breaks do not end
    // it, so every cut candidate is inside its protected span and scrubLong refuses.
    for (const nl of ["\n", "\r\n", "\u2028"]) {
      const lines = `zeta kappa MULTILINETAILMARK omicron${nl}`.repeat(Math.ceil((MAX_SCRUB_CHARS + 2048) / 36));
      for (const close of ['` after the close', ""]) {
        const text = `\`secret\`: \`${"v".repeat(4104)}${nl}${lines}${close}`;
        let error: unknown;
        try {
          scrubRecords(enc(liveLine({ ...base, payload: { text } }, "t") + "\n"));
        } catch (e) {
          error = e;
        }
        expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
        expect((error as Error).message).not.toContain("MULTILINETAILMARK");
        expect((error as Error).message).not.toContain("zeta");
        const fitted = fitForScrub({ ...base, payload: { text, n: 1 } });
        const out = dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed);
        expect(JSON.parse(out).payload.text).toBe(oversizeRedaction(text.length));
        expect(JSON.parse(out).payload.n).toBe(1);
        expect(out).not.toContain("MULTILINETAILMARK");
        expect(out).not.toContain("zeta");
      }
    }
  });

  test("rule set 4: a quoted value with a short first line that crosses the bound after a break becomes the marker", () => {
    // The total length caps the value, not its first line: "short", a break, then lines
    // past MAX_SCRUB_CHARS. No cut outside the value's span, so scrubLong refuses.
    for (const nl of ["\n", "\r\n", "\r", "\u2028", "\u2029", "\u0085"]) {
      const lines = `zeta kappa TOTALTAILMARK omicron${nl}`.repeat(Math.ceil((MAX_SCRUB_CHARS + 2048) / 32));
      for (const close of ["` after the close", ""]) {
        const text = `\`secret\`: \`short${nl}${lines}${close}`;
        let error: unknown;
        try {
          scrubRecords(enc(liveLine({ ...base, payload: { text } }, "t") + "\n"));
        } catch (e) {
          error = e;
        }
        expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
        expect((error as Error).message).not.toContain("TOTALTAILMARK");
        expect((error as Error).message).not.toContain("zeta");
        const fitted = fitForScrub({ ...base, payload: { text, n: 1 } });
        const out = dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed);
        expect(JSON.parse(out).payload.text).toBe(oversizeRedaction(text.length));
        expect(JSON.parse(out).payload.n).toBe(1);
        expect(out).not.toContain("TOTALTAILMARK");
        expect(out).not.toContain("short");
      }
    }
  });

  test("a long text scrubLong can cut is left whole for it, and scrubbed", () => {
    const text = Array.from({ length: 60_000 }, (_, i) => (i % 997 === 0 ? KEY : `w${i}`)).join(" ");
    expect(text.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const fitted = fitForScrub({ ...base, payload: { text } });
    expect(fitted.payload!.text).toBe(text);
    const out = JSON.parse(dec(scrubRecords(enc(liveLine(fitted, "t") + "\n")).scrubbed)).payload.text as string;
    expect(out).not.toContain(KEY);
    expect(out.split(" ").length).toBe(text.split(" ").length);
  });

  test("a long string whose only whitespace is newlines (base64, CSV) is redacted, not refused (re-review High)", () => {
    // Decoded, the newlines are safe cuts; JSON-escaped (what the scrubber also checks), they are not.
    const wrapped = Array.from({ length: 5_000 }, () => "QUJDRA".repeat(13)).join("\n");
    expect(wrapped.length).toBeGreaterThan(MAX_SCRUB_CHARS);
    const line = (payload: Record<string, unknown>) => enc(liveLine({ ...base, payload }, "t") + "\n");
    expect(() => scrubRecords(line({ blob: wrapped }))).toThrow(/scrub limit/);
    const fitted = fitForScrub({ ...base, payload: { blob: wrapped } });
    const out = JSON.parse(dec(scrubRecords(line(fitted.payload!)).scrubbed));
    expect(out.payload.blob).toBe(oversizeRedaction(wrapped.length));
  });
});
