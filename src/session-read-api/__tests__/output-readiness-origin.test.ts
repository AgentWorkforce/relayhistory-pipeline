/**
 * outputWindowReadiness counts only CANONICAL transcript events as needing facts: facts are
 * built from the canonical S3 read only, so a buffered-origin row (buffered-search-feed.ts)
 * never gets a facts state row and must not turn a window into `session_unindexed` (a 501 on
 * /v1/sessions/output and /v1/digest/daily). A table without the origin column is read
 * unfiltered only while a probe proves the column is still absent (page.ts's convention).
 * Fake ClickHouse; the real-ClickHouse cases are in output-route.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { outputWindowReadiness } from "../output-query.ts";
import { ClickHouseError, type ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { OUTPUT_FACTS_SESSION_STATE_TABLE, OUTPUT_FACTS_STATE_TABLE } from "../../session-pipeline/output-facts-schema.ts";
import { EVENTS_TABLE } from "../../session-pipeline/vendor/search-index/schema.ts";

interface Event { session: string; source: string; origin: "canonical" | "buffered" }
interface Fake {
  events: Event[];
  /** Complete facts state rows, as `${source}/${session}`. */
  complete: Set<string>;
  /** Whether transcript_events has the origin column (before/after the probe). */
  hasOrigin: boolean;
  hasOriginAtProbe?: boolean;
  sql: string[];
}

/** Answers the four readiness queries by shape; the events query honours the origin predicate. */
function fakeClickHouse(f: Fake): ClickHouseClient {
  return {
    database: "testdb",
    async command() { throw new Error("read path issues no commands"); },
    async insert() { throw new Error("read path issues no inserts"); },
    async select<T>(query: string): Promise<T[]> {
      f.sql.push(query);
      if (query.includes("system.columns")) return [{ n: (f.hasOriginAtProbe ?? f.hasOrigin) ? 2 : 0 }] as T[];
      if (query.includes(`FROM ${OUTPUT_FACTS_STATE_TABLE} FINAL`)) return [{ complete_since: "2026-11-01 00:00:00.000000" }] as T[];
      if (query.includes(`FROM ${EVENTS_TABLE} FINAL`)) {
        const canonicalOnly = /\borigin = 'canonical'/.test(query);
        if (/\borigin\b/.test(query) && !f.hasOrigin) throw new ClickHouseError(404, "47", "UNKNOWN_IDENTIFIER");
        const keys = new Set(f.events.filter((e) => !canonicalOnly || e.origin === "canonical").map((e) => `${e.source}/${e.session}`));
        return [{ n: [...keys].filter((k) => !f.complete.has(k)).length }] as T[];
      }
      if (query.includes(`FROM ${OUTPUT_FACTS_SESSION_STATE_TABLE} FINAL`)) return [{ n: 0 }] as T[];
      throw new Error(`unexpected query: ${query}`);
    },
  };
}

const since = new Date("2026-11-05T00:00:00Z"), until = new Date("2026-11-05T23:59:59.999Z");
const readiness = (f: Fake) => outputWindowReadiness(fakeClickHouse(f), "org-1", "ws-1", since, until);

describe("outputWindowReadiness: canonical rows only", () => {
  test("a buffered-only session in the window, with no facts, is ready (was session_unindexed)", async () => {
    const f: Fake = { events: [{ session: "s-buf", source: "codex", origin: "buffered" }], complete: new Set(), hasOrigin: true, sql: [] };
    expect(await readiness(f)).toBe("ready");
    const eventsSql = f.sql.filter((q) => q.includes(`FROM ${EVENTS_TABLE} FINAL`));
    expect(eventsSql).toHaveLength(1);
    expect(eventsSql[0]).toContain("AND origin = 'canonical'");
    // Answered by the filtered query itself: no fallback, no probe.
    expect(f.sql.some((q) => q.includes("system.columns"))).toBe(false);
  });

  test("a canonical session without facts still blocks; buffered rows never hide it", async () => {
    const f: Fake = {
      events: [
        { session: "s-can", source: "codex", origin: "canonical" },
        { session: "s-can", source: "codex", origin: "buffered" },
        { session: "s-buf", source: "codex", origin: "buffered" },
      ],
      complete: new Set(), hasOrigin: true, sql: [],
    };
    expect(await readiness(f)).toBe("session_unindexed");
    f.complete.add("codex/s-can");
    expect(await readiness(f)).toBe("ready");
  });

  test("no origin column: unfiltered count, served only after the probe proves it still absent", async () => {
    const f: Fake = { events: [{ session: "s-old", source: "codex", origin: "canonical" }], complete: new Set(), hasOrigin: false, sql: [] };
    expect(await readiness(f)).toBe("session_unindexed");
    const eventsSql = f.sql.filter((q) => q.includes(`FROM ${EVENTS_TABLE} FINAL`));
    expect(eventsSql).toHaveLength(2);
    expect(eventsSql[1]).not.toMatch(/\borigin\b/);
    expect(f.sql.at(-1)).toContain("system.columns");
    f.complete.add("codex/s-old");
    expect(await readiness(f)).toBe("ready");
  });

  test("the origin column appearing between the reads is a 503, never an unfiltered answer", async () => {
    const f: Fake = { events: [{ session: "s-buf", source: "codex", origin: "buffered" }], complete: new Set(), hasOrigin: false, hasOriginAtProbe: true, sql: [] };
    const err = await readiness(f).then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(ClickHouseError);
    expect((err as ClickHouseError).status).toBe(503);
    expect((err as Error).message).toContain("origin_column_changed");
  });

  test("other ClickHouse errors propagate unchanged (no fallback)", async () => {
    const ch: ClickHouseClient = {
      ...fakeClickHouse({ events: [], complete: new Set(), hasOrigin: true, sql: [] }),
      async select<T>(query: string): Promise<T[]> {
        if (query.includes(`FROM ${OUTPUT_FACTS_STATE_TABLE} FINAL`)) return [{ complete_since: "2026-11-01 00:00:00.000000" }] as T[];
        if (query.includes(`FROM ${EVENTS_TABLE} FINAL`)) throw new ClickHouseError(500, "159", "TIMEOUT_EXCEEDED");
        return [{ n: 0 }] as T[];
      },
    };
    const err = await outputWindowReadiness(ch, "org-1", "ws-1", since, until).then(() => null, (e: unknown) => e);
    expect((err as ClickHouseError).code).toBe("159");
  });
});
