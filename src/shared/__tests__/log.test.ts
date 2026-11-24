import { describe, expect, test } from "bun:test";
import { ALLOWED_LOG_FIELDS, EXACT_VALUE_FIELDS, NON_NEGATIVE_INTEGER_FIELDS, errorFields, logLine } from "../log.ts";
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
  test("error fields are code-only", () => {
    const f = errorFields(Object.assign(new Error("Authorization: Bearer sk-live-123"), { code: "chunk_empty", retryable: false }));
    expect(f).toEqual({ error_code: "chunk_empty", retryable: false });
    expect(JSON.stringify(f)).not.toContain("sk-live");
  });
  test("event_age_ms and s3_event_age_ms survive the real logLine only as non-negative safe integers", () => {
    for (const field of ["event_age_ms", "s3_event_age_ms"]) {
      expect(ALLOWED_LOG_FIELDS.has(field)).toBe(true);
      expect(NON_NEGATIVE_INTEGER_FIELDS.has(field)).toBe(true);
      const line = (v: unknown) => JSON.parse(logLine("info", "search index reconciled", { stage: "search-index", [field]: v })) as Record<string, unknown>;
      for (const v of [0, 1, 7_200_000, Number.MAX_SAFE_INTEGER]) expect(line(v)[field]).toBe(v);
      for (const v of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, "5", "AKIAABCDEFGHIJKLMNOP", true, null, { ms: 1 }, [1]]) {
        const out = line(v);
        expect([field, v, field in out]).toEqual([field, v, false]);
        expect(out.stage).toBe("search-index");
      }
    }
    expect(ALLOWED_LOG_FIELDS.has("oldest_event_age_ms")).toBe(false);
  });
  test("queue_origin survives the real logLine only as one of s3, requeue, sweep", () => {
    expect(ALLOWED_LOG_FIELDS.has("queue_origin")).toBe(true);
    expect([...EXACT_VALUE_FIELDS.get("queue_origin")!].sort()).toEqual(["requeue", "s3", "sweep"]);
    const line = (v: unknown) => JSON.parse(logLine("info", "search index reconciled", { stage: "search-index", queue_origin: v })) as Record<string, unknown>;
    for (const v of ["s3", "requeue", "sweep"]) expect(line(v).queue_origin).toBe(v);
    for (const v of ["S3", "s3 ", "", "other", "sqs", "AKIAABCDEFGHIJKLMNOP", "059f36b3-02ba-4e7d-a2c1-8f1e1c0d9a77", 0, 1, true, null, ["s3"], { s3: 1 }]) {
      const out = line(v);
      expect([v, "queue_origin" in out]).toEqual([v, false]);
      expect(out.stage).toBe("search-index");
    }
  });
  test("the value rules apply at every level, not only info", () => {
    for (const level of ["debug", "info", "warn", "error"] as const) {
      const out = JSON.parse(logLine(level, "search index partial", { stage: "search-index", event_age_ms: -1, s3_event_age_ms: "5", queue_origin: "other" })) as Record<string, unknown>;
      expect([level, "event_age_ms" in out, "s3_event_age_ms" in out, "queue_origin" in out]).toEqual([level, false, false, false]);
      expect(out).toMatchObject({ level, stage: "search-index" });
      const kept = JSON.parse(logLine(level, "search index partial", { stage: "search-index", event_age_ms: 3, s3_event_age_ms: 4, queue_origin: "sweep" })) as Record<string, unknown>;
      expect(kept).toMatchObject({ level, event_age_ms: 3, s3_event_age_ms: 4, queue_origin: "sweep" });
    }
  });
  test("contender_seats survives the real logLine only as one of its four words, at every level", () => {
    const WORDS = ["all_unseated", "seated_unseated", "both", "catalog_key"];
    expect(ALLOWED_LOG_FIELDS.has("contender_seats")).toBe(true);
    expect([...EXACT_VALUE_FIELDS.get("contender_seats")!].sort()).toEqual([...WORDS].sort());
    for (const level of ["debug", "info", "warn", "error"] as const) {
      const line = (v: unknown) => JSON.parse(logLine(level, "search index reconciled", { stage: "search-index", contender_seats: v })) as Record<string, unknown>;
      for (const v of WORDS) expect([level, line(v).contender_seats]).toEqual([level, v]);
      for (const v of [
        "ALL_UNSEATED", "both ", " catalog_key", "", "other", "seated", "unseated", "all_unseated,both", "s3",
        "AKIAABCDEFGHIJKLMNOP", "059f36b3-02ba-4e7d-a2c1-8f1e1c0d9a77", 0, 2, -1, true, null, ["both"], { both: 1 },
      ]) {
        const out = line(v);
        expect([level, v, "contender_seats" in out]).toEqual([level, v, false]);
        expect(out).toMatchObject({ level, stage: "search-index" });
      }
    }
  });
  test("batch_deadline_deferrals and the batch-deadline reasons survive the real logLine; the count only as a non-negative safe integer, at every level", () => {
    expect(ALLOWED_LOG_FIELDS.has("batch_deadline_deferrals")).toBe(true);
    expect(NON_NEGATIVE_INTEGER_FIELDS.has("batch_deadline_deferrals")).toBe(true);
    for (const level of ["info", "error"] as const) {
      for (const reason of ["batch_deadline", "batch_deadline_deferrals_exhausted"]) {
        const line = (v: unknown) => JSON.parse(logLine(level, "search index past the batch deadline, requeued", { stage: "search-index", reason, batch_deadline_deferrals: v })) as Record<string, unknown>;
        for (const v of [0, 1, 3, 4]) expect(line(v)).toMatchObject({ level, stage: "search-index", reason, batch_deadline_deferrals: v });
        for (const v of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, "3", true, null, [3], { n: 3 }]) {
          const out = line(v);
          expect([level, v, "batch_deadline_deferrals" in out]).toEqual([level, v, false]);
          expect(out).toMatchObject({ level, stage: "search-index", reason });
        }
      }
    }
  });
  test("the value rule leaves other allowlisted fields as they were", () => {
    const out = JSON.parse(logLine("info", "m", { count: -1, duration_ms: 1.5, ok: true, reason: "x" })) as Record<string, unknown>;
    expect(out).toMatchObject({ count: -1, duration_ms: 1.5, ok: true, reason: "x" });
  });
});
