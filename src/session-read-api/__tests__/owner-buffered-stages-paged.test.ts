/**
 * 047020 paged exact-session stages on the reader (READ_API_OWNER_BUFFERED_STAGES_PAGED=on):
 * the flag parser, the single SQL switch, page traversal (cursor round trip, concatenation in
 * page order, one envelope per call) and every fail-closed path (page error mid-way, malformed
 * envelope, snapshot drift, duplicate stage, stuck cursor, page cap, shared deadline, abort).
 * A fake tenant runner answers scripted envelopes; nothing reaches a database.
 */
import { describe, expect, test } from "bun:test";
import {
  BUFFERED_PAGED_STAGES_MAX_PAGES,
  BUFFERED_PAGED_STAGES_PAGE_CLAIMS,
  BUFFERED_PAGED_STAGES_SQL,
  BUFFERED_STAGES_SQL,
  BUFFERED_TARGET_STAGES_SQL,
  BUFFERED_USER_SQL,
  BufferedFenced,
  bufferedStagesMode,
  ownerBufferedStagesPagedEnabled,
  sqlOwnerBufferedReads,
  type BufferedStageRow,
} from "../owner-buffered.ts";
import { E1, SCOPE, SESSION, authority } from "./owner-buffered-support.ts";
import { MAX_DELIVERY_RECORDS } from "../../session-ingest/vendor/delivery/delivery-contracts.ts";

const SNAP = "2026-11-05T12:30:00.000Z";
function stage(n: number): BufferedStageRow {
  return {
    batch_id: `b-${n}`, stage_key: `96bulls/buffered-batches/v1/ws=ws-a/m=srv_test/b=${n}.zst`, stage_sha256: String(n % 10).repeat(64), stage_bytes: 100 + n,
    group_indexes: [0], group_ordinals: [[n * 10, n * 10 + 1]], kept_ordinals: [n * 10, n * 10 + 1], reset_epoch: "0",
    accepted_at: `2026-11-05T12:${String(n).padStart(2, "0")}:00.000Z`, admission_epoch: E1, admission_version: "7",
  };
}
type Envelope = { stages: unknown; cursor: unknown; has_more: unknown; snapshot_at: unknown };
const env = (stages: BufferedStageRow[], cursor: Record<string, unknown> | null, snapshot = SNAP): Envelope =>
  ({ stages, cursor, has_more: cursor !== null, snapshot_at: snapshot });
const cur = (n: number, snapshot = SNAP) => ({ v: 1, snapshot_at: snapshot, accepted_at: `2026-11-05T12:${String(n).padStart(2, "0")}:00Z`, batch_id: `b-${n}`, stage_key: `k${n}`, stage_token: `t${n}` });
/** Pages of 2 stages each over n stages, as the RPC would answer with p_page_claims=2. */
function pagesOf(n: number, snapshot = SNAP): Envelope[] {
  const out: Envelope[] = [];
  for (let i = 1; i <= n; i += 2) {
    const rows = [stage(i), ...(i + 1 <= n ? [stage(i + 1)] : [])];
    out.push(env(rows, i + 1 < n ? cur(i + 1, snapshot) : null, snapshot));
  }
  return out;
}

/** A fake tenant runner: one scripted answer per RPC call (an Error is thrown; a function runs). */
function runner(answers: Array<Envelope | Envelope[] | Error | (() => Envelope | Promise<Envelope>)>) {
  const seen: Array<{ sql: string; params?: unknown[] }> = [];
  const rpc: Array<{ sql: string; params: unknown[]; timeout: string }> = [];
  let calls = 0;
  let timeout = "";
  const run = (async (_org: string, _ws: string, work: (q: { query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
    work({
      async query(sql, params) {
        seen.push({ sql, params });
        if (sql.startsWith("SET LOCAL statement_timeout")) { timeout = sql; return { rows: [] }; }
        if (sql === BUFFERED_USER_SQL) return { rows: [] };
        rpc.push({ sql, params: params ?? [], timeout });
        const a = answers[Math.min(calls++, answers.length - 1)]!;
        if (a instanceof Error) throw a;
        const v = typeof a === "function" ? await a() : a;
        return { rows: Array.isArray(v) ? v : [v] };
      },
    })) as never;
  return { run, seen, rpc };
}
const paged = (run: never, extra: Parameters<typeof sqlOwnerBufferedReads>[1] = {}) => sqlOwnerBufferedReads(run, { exactSession: true, stagesPaged: true, ...extra });
const reason = (p: Promise<unknown>) => p.then(() => "resolved", (e) => (e instanceof BufferedFenced ? e.reason : `other:${String(e)}`));

describe("READ_API_OWNER_BUFFERED_STAGES_PAGED flag and the single SQL switch", () => {
  test("only the exact string on enables; default off", () => {
    for (const v of [undefined, "", "off", "true", "1", "ON", " on", "on ", "yes"]) expect(ownerBufferedStagesPagedEnabled(v), String(v)).toBe(false);
    expect(ownerBufferedStagesPagedEnabled("on")).toBe(true);
  });
  test("paged only together with exact-session; the paged SQL names the new RPC with cursor and page size", () => {
    expect(bufferedStagesMode({})).toBe("frozen");
    expect(bufferedStagesMode({ stagesPaged: true })).toBe("frozen");
    expect(bufferedStagesMode({ exactSession: false, stagesPaged: true })).toBe("frozen");
    expect(bufferedStagesMode({ exactSession: true })).toBe("target");
    expect(bufferedStagesMode({ exactSession: true, stagesPaged: false })).toBe("target");
    expect(bufferedStagesMode({ exactSession: true, stagesPaged: true })).toBe("paged");
    expect(BUFFERED_PAGED_STAGES_SQL).toContain("sessions.buffered_session_stages_for_target_paged(");
    expect(BUFFERED_PAGED_STAGES_SQL).toContain("p_source => $9, p_session_id => $10, p_cursor => $11::jsonb, p_page_claims => $12::integer)");
    expect(BUFFERED_PAGED_STAGES_SQL.match(/=> \$/g)!.length).toBe(12);
    expect(BUFFERED_PAGED_STAGES_PAGE_CLAIMS).toBe(2);
    expect(BUFFERED_PAGED_STAGES_MAX_PAGES).toBe(64);
  });
  test("page bound: 2 claims x the ingest 500-records-per-claim invariant = 1,000 records per Worker page", () => {
    // A change to either constant must revisit the documented bound (runbook "Paged stages (047020)").
    expect(MAX_DELIVERY_RECORDS).toBe(500);
    expect(BUFFERED_PAGED_STAGES_PAGE_CLAIMS).toBeGreaterThanOrEqual(1);
    expect(BUFFERED_PAGED_STAGES_PAGE_CLAIMS).toBeLessThanOrEqual(4);
    expect(BUFFERED_PAGED_STAGES_PAGE_CLAIMS * MAX_DELIVERY_RECORDS).toBe(1_000);
  });
  test("flag off (or paged without exact-session): the paged SQL is never sent", async () => {
    for (const [opts, want] of [[{}, BUFFERED_STAGES_SQL], [{ stagesPaged: true }, BUFFERED_STAGES_SQL], [{ exactSession: true }, BUFFERED_TARGET_STAGES_SQL], [{ exactSession: true, stagesPaged: false }, BUFFERED_TARGET_STAGES_SQL]] as const) {
      const { run, rpc } = runner([[stage(1) as never]]);
      const rows = await sqlOwnerBufferedReads(run, opts).stages(SCOPE, authority("7"), "claude", SESSION);
      expect(rows).toHaveLength(1);
      expect(rpc.map((r) => r.sql)).toEqual([want]);
      expect(rpc[0]!.params).toHaveLength(10);
    }
  });
});

describe("paged traversal", () => {
  test("on: pages concatenated in order; cursor NULL first, then each returned cursor; page size 2", async () => {
    const pages = pagesOf(7);
    const { run, rpc, seen } = runner(pages);
    const rows = await paged(run).stages(SCOPE, authority("7"), "claude", SESSION);
    expect(rows.map((r) => r.batch_id)).toEqual(["b-1", "b-2", "b-3", "b-4", "b-5", "b-6", "b-7"]);
    expect(rpc).toHaveLength(4);
    for (const r of rpc) expect(r.sql).toBe(BUFFERED_PAGED_STAGES_SQL);
    expect(rpc.map((r) => r.params[10])).toEqual([null, JSON.stringify(cur(2)), JSON.stringify(cur(4)), JSON.stringify(cur(6))]);
    for (const r of rpc) {
      expect(r.params.slice(0, 10)).toEqual(["ws-a", E1, E1, "7", "3".repeat(64), "selection", false, "4".repeat(64), "claude", SESSION]);
      expect(r.params[11]).toBe(2);
    }
    // Each page is its own transaction: timeout, user binding, the RPC.
    expect(seen.filter((s) => s.sql === BUFFERED_USER_SQL)).toHaveLength(4);
  });
  test("jsonb returned as text (no type parser) is accepted the same way", async () => {
    const pages = pagesOf(3).map((p) => ({ ...p, stages: JSON.stringify(p.stages), cursor: p.cursor === null ? null : JSON.stringify(p.cursor) }));
    const rows = await paged(runner(pages).run).stages(SCOPE, authority("7"), "claude", SESSION);
    expect(rows.map((r) => r.batch_id)).toEqual(["b-1", "b-2", "b-3"]);
  });
  test("fully filtered pages (empty stages, has_more true) keep paging to the end", async () => {
    const pages = [env([], cur(2)), env([], cur(4)), env([stage(5)], cur(6)), env([], null)];
    const { run, rpc } = runner(pages);
    expect((await paged(run).stages(SCOPE, authority("7"), "claude", SESSION)).map((r) => r.batch_id)).toEqual(["b-5"]);
    expect(rpc).toHaveLength(4);
  });
  test("each traversal is fresh: a second stages() call (the recheck) starts again from cursor NULL", async () => {
    const pages = pagesOf(3, SNAP);
    const later = pagesOf(3, "2026-11-05T12:31:00.000Z");
    const { run, rpc } = runner([...pages, ...later]);
    const reads = paged(run);
    await reads.stages(SCOPE, authority("7"), "claude", SESSION);
    await reads.stages(SCOPE, authority("7"), "claude", SESSION);
    expect(rpc.map((r) => r.params[10])).toEqual([null, JSON.stringify(cur(2)), null, JSON.stringify(cur(2, "2026-11-05T12:31:00.000Z"))]);
  });
});

describe("paged traversal fails closed, never a partial set", () => {
  test("a page error mid-way: rpc_error / rpc_timeout / rpc_fenced like the single RPC, nothing returned", async () => {
    const cases: Array<[Error, string]> = [
      [Object.assign(new Error("function does not exist"), { code: "42883" }), "rpc_error"],
      [Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }), "rpc_timeout"],
      [Object.assign(new Error("buffered read fenced"), { code: "55006" }), "rpc_fenced"],
      [Object.assign(new Error("gate changed between pages"), { code: "55000" }), "rpc_error"],
    ];
    for (const [err, want] of cases) {
      const [p1, p2] = pagesOf(7);
      const { run, rpc } = runner([p1!, p2!, err]);
      expect(await reason(paged(run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe(want);
      expect(rpc).toHaveLength(3);
    }
  });
  test("malformed envelopes are rpc_error", async () => {
    const good = env([stage(1)], cur(1));
    const bad: Array<Envelope | Envelope[]> = [
      [],
      [good, good],
      { ...good, stages: null },
      { ...good, stages: [1] },
      { ...good, stages: [[]] },
      { ...good, has_more: "true" },
      { ...good, has_more: null },
      { ...good, cursor: null },
      { ...good, has_more: false },
      { ...good, cursor: [1] },
      { ...good, cursor: "not json" },
      { ...good, snapshot_at: "not a time" },
      { ...good, snapshot_at: null },
    ];
    for (const b of bad) expect(await reason(paged(runner([b]).run).stages(SCOPE, authority("7"), "claude", SESSION)), JSON.stringify(b).slice(0, 80)).toBe("rpc_error");
    // A row-shape error stays the existing buffered_row_invalid (500), as on the single RPC.
    expect(await reason(paged(runner([{ ...good, stages: [{ ...stage(1), stage_sha256: "x" }] }]).run).stages(SCOPE, authority("7"), "claude", SESSION))).toContain("buffered_row_invalid");
    expect(await reason(paged(runner([[]]).run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
  });
  test("snapshot drift between pages, a repeated stage or a cursor that does not advance is rpc_error", async () => {
    const drift = [env([stage(1)], cur(1)), env([stage(2)], null, "2026-11-05T12:31:00.000Z")];
    expect(await reason(paged(runner(drift).run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
    const repeated = [env([stage(1)], cur(1)), env([stage(1)], null)];
    expect(await reason(paged(runner(repeated).run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
    const stuck = [env([stage(1)], cur(1)), env([], cur(1))];
    expect(await reason(paged(runner(stuck).run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
  });
  test("cap overflow: more than maxPages pages is rpc_error after exactly maxPages calls", async () => {
    const endless = Array.from({ length: 70 }, (_, i) => env([], cur(i + 1)));
    const { run, rpc } = runner(endless);
    expect(await reason(paged(run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
    expect(rpc).toHaveLength(BUFFERED_PAGED_STAGES_MAX_PAGES);
    // Exactly maxPages pages is still served.
    const fits = [...Array.from({ length: 3 }, (_, i) => env([stage(i + 1)], cur(i + 1))), env([stage(4)], null)];
    expect(await paged(runner(fits).run, { maxPages: 4 }).stages(SCOPE, authority("7"), "claude", SESSION)).toHaveLength(4);
    expect(await reason(paged(runner(fits).run, { maxPages: 3 }).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_error");
  });
  test("one statement budget for the whole traversal: each page gets what is left; overrun is rpc_timeout", async () => {
    let t = 0;
    const now = () => t;
    const step = (ms: number, e: Envelope) => () => { t += ms; return e; };
    const pages = pagesOf(5);
    const { run, rpc } = runner([step(700, pages[0]!), step(700, pages[1]!), step(100, pages[2]!)]);
    expect(await paged(run, { statementTimeoutMs: 2000, now }).stages(SCOPE, authority("7"), "claude", SESSION)).toHaveLength(5);
    expect(rpc.map((r) => r.timeout)).toEqual(["SET LOCAL statement_timeout = 2000", "SET LOCAL statement_timeout = 1300", "SET LOCAL statement_timeout = 600"]);
    t = 0;
    const slow = runner([step(1500, pages[0]!), step(600, pages[1]!), step(0, pages[2]!)]);
    expect(await reason(paged(slow.run, { statementTimeoutMs: 2000, now }).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_timeout");
    expect(slow.rpc).toHaveLength(2);
    t = 0;
    const exhausted = runner([step(2000, pages[0]!), step(0, pages[1]!)]);
    expect(await reason(paged(exhausted.run, { statementTimeoutMs: 2000, now }).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_timeout");
    expect(exhausted.rpc).toHaveLength(1);
  });
  test("the default traversal clock is monotonic: a wall-clock jump does not expire the budget", async () => {
    const wall = Date.now;
    let jump = 0;
    Date.now = () => wall() + (jump += 3_600_000);
    try {
      const { run, rpc } = runner(pagesOf(5));
      expect(await paged(run, { statementTimeoutMs: 2000 }).stages(SCOPE, authority("7"), "claude", SESSION)).toHaveLength(5);
      expect(rpc).toHaveLength(3);
    } finally {
      Date.now = wall;
    }
  });
  test("abort before or during a page is request_aborted; no further page is read", async () => {
    const before = new AbortController();
    before.abort();
    const a = runner(pagesOf(5));
    expect(await reason(paged(a.run).stages(SCOPE, authority("7"), "claude", SESSION, before.signal))).toBe("request_aborted");
    expect(a.rpc).toHaveLength(0);
    const during = new AbortController();
    const pages = pagesOf(5);
    const b = runner([pages[0]!, () => { during.abort(); return pages[1]!; }, pages[2]!]);
    expect(await reason(paged(b.run).stages(SCOPE, authority("7"), "claude", SESSION, during.signal))).toBe("request_aborted");
    expect(b.rpc).toHaveLength(2);
  });
});
