import { describe, expect, test } from "bun:test";
import { ALLOWED_LOG_FIELDS, BOOLEAN_FIELDS, BOUNDED_COUNT_FIELDS, FRAME_NAME_SHAPE, STORE_CAUSE_VALUES, errorFields, log, logLine, sanitizeLogField, searchIndexErrorDetail } from "../log.ts";
import { STORE_TRANSPORT_CAUSES } from "../vendor.ts";
import { send } from "../../../packages/object-store/src/core.ts";
import { ObjectStoreError, storeTransportCause } from "../../../packages/object-store/src/errors.ts";
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
  test("store_cause is one closed class or nothing, and the log's set equals object-store's", () => {
    expect([...STORE_CAUSE_VALUES].sort()).toEqual([...STORE_TRANSPORT_CAUSES].sort());
    expect(sanitizeLogField("store_cause", "econnreset")).toBe("econnreset");
    for (const bad of ["ECONNRESET", "socket hang up at https://bucket/96bulls/key", "", 104, null]) expect(sanitizeLogField("store_cause", bad)).toBeUndefined();
  });

  test("the streaming reconcile startup fields: streaming is on / off or nothing, the budgets bounded counts", () => {
    expect(sanitizeLogField("streaming", "off")).toBe("off");
    expect(sanitizeLogField("streaming", "on")).toBe("on");
    for (const bad of ["OFF", "true", "", 1, null]) expect(sanitizeLogField("streaming", bad)).toBeUndefined();
    for (const f of ["max_read_bytes", "max_read_events", "largest_streamed_scope_bytes", "largest_streamed_scope_events"]) {
      expect(sanitizeLogField(f, 2048 * 1024 * 1024)).toBe(2048 * 1024 * 1024);
      for (const bad of ["2GiB", -1, 1.5]) expect(sanitizeLogField(f, bad)).toBeUndefined();
    }
  });

  test("the Bob Costas summary input fields are bounded counts only", () => {
    for (const f of ["chunks_read", "chunks_total", "bytes_read", "names_dropped", "turns_dropped",
      "max_chunks", "max_input_bytes", "max_chunk_bytes"]) {
      expect(ALLOWED_LOG_FIELDS.has(f)).toBe(true);
      expect(BOUNDED_COUNT_FIELDS.has(f)).toBe(true);
      expect(sanitizeLogField(f, 3_489)).toBe(3_489);
      for (const bad of ["3489", "prompt text", -1, 1.5, null]) expect(sanitizeLogField(f, bad)).toBeUndefined();
    }
  });

  test("storeTransportCause classifies by code / name / errno only (one cause level), never the message", () => {
    const e = (code?: string, name?: string, message = "https://bucket/96bulls/secret") => Object.assign(new Error(message), code ? { code } : {}, name ? { name } : {});
    expect(storeTransportCause(e("ECONNRESET"))).toBe("econnreset");
    expect(storeTransportCause(e("ConnectionReset"))).toBe("econnreset");
    expect(storeTransportCause(e("ETIMEDOUT"))).toBe("etimedout");
    expect(storeTransportCause(e("ConnectionRefused"))).toBe("econnrefused");
    expect(storeTransportCause(e("EPIPE"))).toBe("epipe");
    expect(storeTransportCause(e(undefined, "AbortError"))).toBe("abort");
    expect(storeTransportCause(e(undefined, "TimeoutError"))).toBe("timeout");
    expect(storeTransportCause(e("ENOTFOUND"))).toBe("dns");
    expect(storeTransportCause(e("UNABLE_TO_VERIFY_LEAF_SIGNATURE_CERT"))).toBe("tls");
    expect(storeTransportCause(e("UND_ERR_SOCKET"))).toBe("socket");
    expect(storeTransportCause(e("ECONNABORTED"))).toBe("socket");
    expect(storeTransportCause(new TypeError("fetch failed", { cause: e("ECONNRESET") }))).toBe("econnreset");
    // The message is never read: an ECONNRESET in the text alone is "other".
    expect(storeTransportCause(new Error("ECONNRESET at https://bucket/96bulls/key"))).toBe("other");
    expect(storeTransportCause("nope")).toBe("other");
  });

  test("send() wraps a fetch failure as a status-less transient carrying its closed class", async () => {
    const failing = (async () => { throw Object.assign(new Error("reset"), { code: "ECONNRESET" }); }) as unknown as typeof fetch;
    const err = await send("s3", "96bulls/key", "https://example.invalid/x", {}, failing).catch((x: unknown) => x);
    expect(err).toBeInstanceOf(ObjectStoreError);
    expect((err as ObjectStoreError).code).toBe("transient");
    expect((err as ObjectStoreError).status).toBeUndefined();
    expect((err as ObjectStoreError).transportCause).toBe("econnreset");
  });

  test("store_status (a ledger_store_* retry's HTTP status) is a bounded integer only", () => {
    expect(sanitizeLogField("store_status", 503)).toBe(503);
    expect(sanitizeLogField("store_status", 9999)).toBe(599);
    expect(sanitizeLogField("store_status", "503 SlowDown on 96bulls/key")).toBeUndefined();
  });

  test("age_ms (the in-flight lease line's oldest holder age) is a bounded integer only", () => {
    expect(ALLOWED_LOG_FIELDS.has("age_ms")).toBe(true);
    expect(BOUNDED_COUNT_FIELDS.get("age_ms")).toBe(3_600_000);
    expect(sanitizeLogField("age_ms", 0)).toBe(0);
    expect(sanitizeLogField("age_ms", 89_999)).toBe(89_999);
    expect(sanitizeLogField("age_ms", Number.MAX_SAFE_INTEGER)).toBe(3_600_000);
    for (const bad of ["3489", "org_123", -1, 1.5, Number.NaN, null, true]) expect(sanitizeLogField("age_ms", bad)).toBeUndefined();
    expect(JSON.parse(logLine("info", "request", { route: "ingest.inflight_lease", age_ms: 1_234 })).age_ms).toBe(1_234);
  });

  test("same_origin (the in-flight lease line's overlap label) is a boolean only, never an id", () => {
    expect(ALLOWED_LOG_FIELDS.has("same_origin")).toBe(true);
    expect(BOOLEAN_FIELDS.has("same_origin")).toBe(true);
    expect(sanitizeLogField("same_origin", true)).toBe(true);
    expect(sanitizeLogField("same_origin", false)).toBe(false);
    for (const bad of ["true", "false", "origin-1", "", 0, 1, Number.NaN, null, {}, [true]]) expect(sanitizeLogField("same_origin", bad)).toBeUndefined();
    for (const v of [true, false]) {
      const line = JSON.parse(logLine("info", "request", { route: "ingest.inflight_lease", outcome: "would_refuse", count: 1, age_ms: 7, same_origin: v }));
      expect(line).toMatchObject({ route: "ingest.inflight_lease", outcome: "would_refuse", count: 1, age_ms: 7, same_origin: v });
    }
    expect(Object.hasOwn(JSON.parse(logLine("info", "request", { same_origin: "origin-1" })), "same_origin")).toBe(false);
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

test("receipt_tag / attempt_tag: 8 lowercase hex only, anything else dropped", () => {
  for (const f of ["receipt_tag", "attempt_tag"]) {
    expect(sanitizeLogField(f, "0a1b2c3d")).toBe("0a1b2c3d");
    for (const bad of ["0A1B2C3D", "0a1b2c3", "0a1b2c3d4", "96bulls/.admission-inbox/org/v1", 12345678, null]) expect(sanitizeLogField(f, bad)).toBeUndefined();
  }
});

test("error_frame: a frame-shaped value the scrubber changes is dropped, and the line is still written", () => {
  // The synthetic key-shaped value the span scrub test above plants: a valid identifier, but scrubbed.
  const planted = "AKIAABCDEFGHIJKLMNOP";
  expect(FRAME_NAME_SHAPE.test(planted)).toBe(true);
  expect(sanitizeLogField("error_frame", planted)).toBeUndefined();
  expect(sanitizeLogField("error_frame", "CloudflareSocket._listen")).toBe("CloudflareSocket._listen");
  const fields = { error_frame: planted, error_name: "TypeError", db_error_detail: "socket_closed" };
  const parsed = JSON.parse(logLine("error", "session ingest db error", fields)) as Record<string, unknown>;
  expect(parsed).not.toHaveProperty("error_frame");
  expect(parsed).toMatchObject({ level: "error", message: "session ingest db error", error_name: "TypeError", db_error_detail: "socket_closed" });
  // log.error writes exactly one line to stderr, without the planted value.
  const written: string[] = [];
  const original = process.stderr.write;
  process.stderr.write = ((chunk: string | Uint8Array) => { written.push(String(chunk)); return true; }) as typeof process.stderr.write;
  try {
    log.error("session ingest db error", fields);
  } finally {
    process.stderr.write = original;
  }
  expect(written).toHaveLength(1);
  expect(written[0]).toContain("session ingest db error");
  expect(written[0]).not.toContain(planted);
  expect(JSON.parse(written[0]!)).not.toHaveProperty("error_frame");
});
