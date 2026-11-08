import { describe, expect, test } from "bun:test";
import { ALLOWED_LOG_FIELDS, BOUNDED_COUNT_FIELDS, errorFields, logLine } from "../log.ts";
import { spanAttrs } from "../telemetry.ts";

describe("telemetry allowlist", () => {
  test("drops anything that could carry content", () => {
    const attrs = spanAttrs({ stage: "summary-short", prompt: "secret text", summary: "x", path: "/home/u", session_uuid: "abc", count: 3 });
    expect(Object.keys(attrs).sort()).toEqual(["count", "session_uuid", "stage"]);
  });
  test("never allowlists content-shaped fields", () => {
    for (const f of ["prompt", "summary", "message", "body", "path", "manifest", "content", "text", "error_message"]) expect(ALLOWED_LOG_FIELDS.has(f)).toBe(false);
  });
  test("scrubs secret-shaped values at the span exporter boundary", () => {
    const planted = "AKIAABCDEFGHIJKLMNOP";
    const attrs = spanAttrs({ harness: planted, session_uuid: "safe-session-id" });
    expect(attrs.harness).not.toBe(planted);
    expect(JSON.stringify(attrs)).not.toContain(planted);
    expect(attrs.session_uuid).toBe("safe-session-id");
  });
  test("buffered gate counts survive logLine as bounded integers; anything else is dropped", () => {
    const line = (fields: Record<string, unknown>) => JSON.parse(logLine("info", "request", { route: "/v1/search", reason: "buffered_gate", ...fields }));
    const ok = line({ buffered_checked: 12, buffered_dropped: 3 });
    expect([ok.buffered_checked, ok.buffered_dropped, ok.reason]).toEqual([12, 3, "buffered_gate"]);
    expect(line({ buffered_checked: 0, buffered_dropped: 0 })).toMatchObject({ buffered_checked: 0, buffered_dropped: 0 });
    // Clamped to the ceiling.
    expect(BOUNDED_COUNT_FIELDS.get("buffered_checked")).toBe(100_000);
    const big = line({ buffered_checked: 10_000_000, buffered_dropped: Number.MAX_SAFE_INTEGER });
    expect([big.buffered_checked, big.buffered_dropped]).toEqual([100_000, 100_000]);
    // Never free text, an identifier, a fraction, a negative or a non-finite value.
    for (const bad of ["7", "org_123", "7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a01", 1.5, -1, Number.NaN, Number.POSITIVE_INFINITY, null, true, { n: 1 }, [1], 10n]) {
      const out = line({ buffered_checked: bad, buffered_dropped: bad });
      expect(["buffered_checked" in out, "buffered_dropped" in out]).toEqual([false, false]);
    }
  });
  test("error fields are code-only", () => {
    const f = errorFields(Object.assign(new Error("Authorization: Bearer sk-live-123"), { code: "chunk_empty", retryable: false }));
    expect(f).toEqual({ error_code: "chunk_empty", retryable: false });
    expect(JSON.stringify(f)).not.toContain("sk-live");
  });
});
