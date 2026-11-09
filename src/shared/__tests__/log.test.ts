import { describe, expect, test } from "bun:test";
import { ALLOWED_LOG_FIELDS, BOUNDED_COUNT_FIELDS, errorFields, logLine, sanitizeLogField, searchIndexErrorDetail } from "../log.ts";
import { SearchIndexError } from "@rhc/search-index/contracts.ts";
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
    for (const bad of ["7", "org_123", "7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a01", 1.5, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, null, true, false, { parity_missing: 1 }, [1], 10n]) {
      const out = line({ buffered_checked: bad, buffered_dropped: bad });
      expect(["buffered_checked" in out, "buffered_dropped" in out]).toEqual([false, false]);
    }
  });
  test("buffered gate counts at the span boundary: same bounded-count rule as logLine", () => {
    const ok = spanAttrs({ route: "/v1/search", buffered_checked: 12, buffered_dropped: 3 });
    expect([ok.buffered_checked, ok.buffered_dropped, ok.route]).toEqual([12, 3, "/v1/search"]);
    expect(spanAttrs({ buffered_checked: 0, buffered_dropped: 0 })).toEqual({ buffered_checked: 0, buffered_dropped: 0 });
    const big = spanAttrs({ buffered_checked: 10_000_000, buffered_dropped: Number.MAX_SAFE_INTEGER });
    expect([big.buffered_checked, big.buffered_dropped]).toEqual([100_000, 100_000]);
    // The alias channel the reviewer named, and every other non-count value.
    expect(spanAttrs({ buffered_checked: "private-id", buffered_dropped: true })).toEqual({});
    for (const bad of ["7", "org_123", true, false, { parity_missing: 1 }, [1], 1.5, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, null, 10n]) {
      const out = spanAttrs({ buffered_checked: bad, buffered_dropped: bad });
      expect(["buffered_checked" in out, "buffered_dropped" in out]).toEqual([false, false]);
    }
  });
  test("one field sanitizer: logLine and spanAttrs agree on every count value", () => {
    for (const v of [0, 3, 100_000, 100_001, Number.MAX_SAFE_INTEGER, "private-id", "7", true, false, { parity_missing: 1 }, [1], 1.5, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, null]) {
      for (const k of ["buffered_checked", "buffered_dropped"]) {
        const want = sanitizeLogField(k, v);
        expect(want === undefined || (typeof want === "number" && Number.isInteger(want) && want >= 0 && want <= 100_000)).toBe(true);
        expect(JSON.parse(logLine("info", "m", { [k]: v }))[k]).toBe(want as never);
        expect(spanAttrs({ [k]: v })[k]).toBe(want as never);
      }
    }
    expect(sanitizeLogField("prompt", "x")).toBeUndefined();
  });
  test("error fields are code-only", () => {
    const f = errorFields(Object.assign(new Error("Authorization: Bearer sk-live-123"), { code: "chunk_empty", retryable: false }));
    expect(f).toEqual({ error_code: "chunk_empty", retryable: false });
    expect(JSON.stringify(f)).not.toContain("sk-live");
  });
});

describe("searchIndexErrorDetail", () => {
  test("carries a SearchIndexError's bounded detail into the log line as error_detail", () => {
    const error = new SearchIndexError("unavailable", true, "http_429:es_rejected_execution_exception:bulk_items_812:bulk_bytes_4190000");
    const line = JSON.parse(logLine("error", "search index failed", { stage: "search-index", ...errorFields(error), ...searchIndexErrorDetail(error) }));
    expect(line).toMatchObject({ error_code: "unavailable", retryable: true, error_detail: "http_429:es_rejected_execution_exception:bulk_items_812:bulk_bytes_4190000" });
  });
  test("never takes another error's detail or a detail that is not a bounded code", () => {
    const pg = Object.assign(new Error("duplicate key"), { code: "23505", detail: "Key (content)=(secret transcript) already exists." });
    expect(searchIndexErrorDetail(pg)).toEqual({});
    expect(searchIndexErrorDetail(Object.assign(new Error("x"), { name: "SearchIndexError", detail: "Has spaces: secret" }))).toEqual({});
    expect(searchIndexErrorDetail(new SearchIndexError("unavailable", true, "")).error_detail).toBeUndefined();
    // A long code is truncated, not dropped.
    expect(searchIndexErrorDetail(new SearchIndexError("unavailable", true, "x".repeat(300))).error_detail).toBe("x".repeat(256));
    expect(searchIndexErrorDetail(new SearchIndexError("aborted"))).toEqual({});
    expect(searchIndexErrorDetail(undefined)).toEqual({});
  });
});
