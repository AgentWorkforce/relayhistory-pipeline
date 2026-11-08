/**
 * The origin marker against a real ClickHouse (skipped unless
 * READ_API_TEST_CLICKHOUSE_URL is set). Whenever the buffered search gate is
 * not wired (the Lambda composition, or the gate off), reads serve canonical
 * rows only; a table without the origin columns is canonical-only by
 * construction, and is answered only after a probe proves the columns are
 * still absent.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { ClickHouseError } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { createBufferedSearchGate, type AdmissionStore, type ControlHeads } from "../buffered-search-gate.ts";
import { decodeCursor } from "../cursor.ts";
import { readEventPage, type PageDeps } from "../page.ts";
import type { EventQuery } from "../queries.ts";
import { CH_URL, freshClickHouse, memoryLiveness, row } from "./support.ts";

const suite = CH_URL ? describe : describe.skip;
const CLAIM = "11111111-2222-4333-8444-555555555555";
/** The feeder's DDL (docs/proposals/buffered-search-feed.clickhouse.draft.sql, #339). */
const ORIGIN_DDL = [
  "ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin LowCardinality(String) DEFAULT 'canonical'",
  "ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS origin_claim_token String DEFAULT ''",
];

type Ch = Awaited<ReturnType<typeof freshClickHouse>>;
const q = (over: Partial<EventQuery> = {}): EventQuery => ({ orgId: "org-a", workspaceId: "ws-a", order: "desc", limit: 50, q: "needle", ...over });
const buffered = (r: ReturnType<typeof row>) => ({ ...r, origin: "buffered", origin_claim_token: CLAIM });

async function all(deps: PageDeps, input: EventQuery) {
  const ids: string[] = [];
  let cursor = input.cursor;
  for (let i = 0; i < 100; i++) {
    const page = await readEventPage(deps, { ...input, cursor });
    ids.push(...page.events.map((e) => e.eventId));
    if (!page.nextCursor) return ids.sort();
    cursor = decodeCursor(page.nextCursor);
  }
  throw new Error("did not terminate");
}

suite("origin marker on ClickHouse", () => {
  let ch: Ch;
  let legacy: Ch;
  beforeAll(async () => {
    ch = await freshClickHouse();
    await ch.command("SYSTEM STOP MERGES transcript_events");
    for (const ddl of ORIGIN_DDL) await ch.command(ddl);
    // One INSERT per row, so versions coexist (page.clickhouse.test.ts).
    for (const r of [
      row({ id: "c-1", ts: "2026-10-01 10:00:00.000", content: "canonical needle" }), // no origin named: the column default
      buffered(row({ id: "b-1", ts: "2026-10-01 11:00:00.000", user: "u-2", content: "buffered needle of u-2" })),
      buffered(row({ id: "b-2", ts: "2026-10-01 12:00:00.000", session: "s-2", content: "buffered needle in another session" })),
      // A canonical version later overwritten by a buffered one: never served through the stale canonical version.
      row({ id: "x-1", ts: "2026-10-01 13:00:00.000", content: "was canonical needle", version: 1 }),
      buffered(row({ id: "x-1", ts: "2026-10-01 13:00:00.000", content: "now buffered needle", version: 2 })),
      // A buffered version later replaced by its canonical publication: served.
      buffered(row({ id: "p-1", ts: "2026-10-01 14:00:00.000", content: "published needle", version: 1 })),
      row({ id: "p-1", ts: "2026-10-01 14:00:00.000", content: "published needle", version: 2 }),
    ]) await ch.insert("transcript_events", [r]);
    legacy = await freshClickHouse();
    await legacy.insert("transcript_events", [row({ id: "l-1", ts: "2026-10-01 10:00:00.000", content: "legacy needle" })]);
  });
  afterAll(async () => {
    await ch?.drop();
    await legacy?.drop();
  });

  const liveness = () => memoryLiveness().liveness;

  test("gate not wired (the Lambda composition, or the gate off): no buffered row, on any route shape", async () => {
    const deps: PageDeps = { ch, liveness: liveness() };
    expect(await all(deps, q())).toEqual(["c-1", "p-1"]);
    expect(await all(deps, q({ q: undefined, sessionId: "s-1", order: "asc" }))).toEqual(["c-1", "p-1"]);
    // Tiny pages: buffered rows never fill a page or leak through a cursor.
    expect(await all(deps, q({ limit: 1 }))).toEqual(["c-1", "p-1"]);
  });

  test("gate wired: buffered rows reach the gate, which judges each one", async () => {
    const heads: ControlHeads = async () => null; // every head unavailable
    const store: AdmissionStore = { admissions: async () => new Map(), parity: async () => new Set() };
    const deps: PageDeps = { ch, liveness: liveness(), bufferedGate: createBufferedSearchGate({ controlHeads: heads, store }) };
    const page = await readEventPage(deps, q());
    expect(page.events.map((e) => e.eventId).sort()).toEqual(["c-1", "p-1"]);
    expect(page.buffered).toEqual({ checked: 3, dropped: { control_head_unavailable: 3 } });
  });

  test("a table without the origin columns is canonical-only by construction and still answers", async () => {
    expect(await all({ ch: legacy, liveness: liveness() }, q())).toEqual(["l-1"]);
  });

  test("the missing-column fallback is answered only if the probe still finds no origin column", async () => {
    // A client that reports the column missing for the canonical query while the table has it:
    // the probe sees the column, so the read refuses instead of serving unfiltered rows.
    const racing: Ch = Object.assign(Object.create(ch) as Ch, {
      database: ch.database,
      async select<T>(query: string, options?: Parameters<Ch["select"]>[1]): Promise<T[]> {
        if (query.includes("origin = 'canonical'")) throw new ClickHouseError(400, "47", "UNKNOWN_IDENTIFIER");
        return ch.select<T>(query, options);
      },
    });
    await expect(readEventPage({ ch: racing, liveness: liveness() }, q())).rejects.toThrow("origin_column_changed");
  });

  test("other ClickHouse errors are not retried without the filter", async () => {
    let calls = 0;
    const failing: Ch = Object.assign(Object.create(ch) as Ch, {
      database: ch.database,
      async select<T>(): Promise<T[]> { calls++; throw new ClickHouseError(500, "159", "TIMEOUT_EXCEEDED"); },
    });
    await expect(readEventPage({ ch: failing, liveness: liveness() }, q())).rejects.toThrow("TIMEOUT_EXCEEDED");
    expect(calls).toBe(1);
  });
});
