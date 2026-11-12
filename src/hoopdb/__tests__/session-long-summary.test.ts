/**
 * The long summary writer (src/hoopdb/session-long-summary.ts) without a database: the scrub and
 * bound refusals, which SQL each path issues, and the exact parameters. The database side
 * (constraints, trigger, reader, rollback) is session-long-summary.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { ScrubberError } from "@rhc/lib/scrub-engine/index.ts";
import { SCRUB_RULESET } from "../../shared/vendor.ts";
import {
  LONG_SUMMARY_MAX_CHARS,
  LongSummaryRejected,
  clearLongSummary,
  scrubLongSummary,
  writeLongSummary,
} from "../session-long-summary.ts";

const KEY = { orgId: "org-a", workspaceId: "ws-a", harness: "claude", sessionId: "s-1" };
const AT = new Date("2026-11-09T00:00:00Z");

function recorder(rows: unknown[] = [{ written: true }]) {
  const calls: { text: string; params: unknown[] }[] = [];
  const db = {
    query: async (text: string, params: unknown[] = []) => {
      calls.push({ text, params });
      return { rows };
    },
  } as never;
  return { db, calls };
}
const reason = async (p: Promise<unknown>) =>
  p.then(() => null, (e: unknown) => (e instanceof LongSummaryRejected ? e.reason : e));
const identity = (v: string) => v;

describe("scrubLongSummary", () => {
  test("bound is 8000 code points: exactly 8000 accepted (astral too), 8001 refused, never truncated", () => {
    expect(LONG_SUMMARY_MAX_CHARS).toBe(8000);
    expect(scrubLongSummary("a".repeat(8000), identity).text).toHaveLength(8000);
    const astral = "\u{1F402}".repeat(8000);
    expect(scrubLongSummary(astral, identity).text).toBe(astral);
    expect(() => scrubLongSummary("a".repeat(8001), identity)).toThrow("long_summary rejected: over_length");
    expect(() => scrubLongSummary("\u{1F402}".repeat(8001), identity)).toThrow("long_summary rejected: over_length");
  });

  test("empty, failing and unstable scrubs are refused; the refusal never carries the text", () => {
    expect(() => scrubLongSummary("   \n ", identity)).toThrow("long_summary rejected: empty");
    expect(() => scrubLongSummary("secret text", () => { throw new Error("boom secret text"); })).toThrow("long_summary rejected: scrub_failed");
    let n = 0;
    try { scrubLongSummary("secret text", (v) => `${v}${n++}`); throw new Error("not refused"); }
    catch (e) { expect((e as LongSummaryRejected).reason).toBe("scrub_unstable"); expect((e as Error).message).not.toContain("secret"); }
    // A scrub that empties the text is refused as empty, not stored.
    expect(() => scrubLongSummary("abc", () => "  ")).toThrow("long_summary rejected: empty");
  });

  test("the active scrub engine is the default and records its ruleset", () => {
    const out = scrubLongSummary("The agent fixed the flaky build.");
    expect(out.scrubRuleset).toBe(SCRUB_RULESET);
    expect(out.scrubVersion).toBeGreaterThanOrEqual(1);
  });
});

describe("writeLongSummary", () => {
  test("one fenced UPDATE with the scrubbed text and full provenance", async () => {
    const { db, calls } = recorder();
    expect(await writeLongSummary(db, { ...KEY, summary: "  Long text.  ", source: "bob-costas/extractive/v1", generatedAt: AT }, identity)).toMatchObject({ written: true });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.text).toContain("UPDATE sessions.session_directory");
    expect(calls[0]!.text).toContain("d.deleted_at IS NULL AND d.short_summary IS NOT NULL");
    expect(calls[0]!.text).toContain("d.long_summary_generated_at <= $9::timestamptz");
    expect(calls[0]!.text).not.toContain("projected_at");
    expect(calls[0]!.text).not.toContain("search_text");
    expect(calls[0]!.params).toEqual([KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, "Long text.", SCRUB_RULESET, expect.any(Number), "bob-costas/extractive/v1", AT.toISOString()]);
  });

  test("no row updated: written false", async () => {
    const { db } = recorder([]);
    expect((await writeLongSummary(db, { ...KEY, summary: "Long text.", source: "bob-costas/v1", generatedAt: AT }, identity)).written).toBe(false);
  });

  test("a refusal issues only the generated_at-fenced clear, then rethrows", async () => {
    for (const [input, expected] of [
      [{ summary: "x".repeat(8001), source: "bob-costas/v1" }, "over_length"],
      [{ summary: "  ", source: "bob-costas/v1" }, "empty"],
      [{ summary: "Long text.", source: "Not A Source" }, "invalid_source"],
    ] as const) {
      const { db, calls } = recorder([]);
      expect(await reason(writeLongSummary(db, { ...KEY, ...input, generatedAt: AT }, identity))).toBe(expected);
      expect(calls).toHaveLength(1);
      expect(calls[0]!.text).toContain("SET long_summary = NULL");
      expect(calls[0]!.text).toContain("d.long_summary_generated_at <= $5::timestamptz");
      expect(calls[0]!.params).toEqual([KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, AT.toISOString()]);
      expect(JSON.stringify(calls[0]!.params)).not.toContain("xxxx");
    }
  });

  test("a retryable scrubber failure is rethrown as is and issues no SQL", async () => {
    const failure = new ScrubberError("scrub_admission_unavailable", "call_refusal", undefined, true);
    const { db, calls } = recorder();
    await expect(writeLongSummary(db, { ...KEY, summary: "Long text.", source: "bob-costas/v1", generatedAt: AT }, () => { throw failure; })).rejects.toBe(failure);
    expect(calls).toEqual([]);
  });

  test("clearLongSummary clears unconditionally and reports whether a row changed", async () => {
    const hit = recorder([{ cleared: true }]);
    expect(await clearLongSummary(hit.db, KEY)).toBe(true);
    expect(hit.calls[0]!.text).toContain("SET long_summary = NULL");
    expect(hit.calls[0]!.params).toEqual([KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId]);
    expect(await clearLongSummary(recorder([]).db, KEY)).toBe(false);
  });
});
