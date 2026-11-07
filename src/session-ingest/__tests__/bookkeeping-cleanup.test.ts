import { expect, test } from "bun:test";
import { BookkeepingCleanupError, runBookkeepingCleanup, type CleanupSql } from "../bookkeeping-cleanup.ts";

/** A scripted SQL double: records every statement per transaction and answers pages in order. */
function fake(pages: Record<string, unknown>[]) {
  const txs: string[][] = [];
  const sql: CleanupSql = { transaction: async (work) => {
    const seen: string[] = []; txs.push(seen);
    return work(async (s, p) => { seen.push(s + (p.length ? " " + String(p[0]) : "")); return { rows: s.startsWith("SELECT") ? [{ r: pages.shift() }] : [] }; });
  } };
  return { sql, txs };
}
const page = (o: Record<string, unknown>) => ({ phase: "decisions", busy: false, mode: "dry-run", scannedRows: 3, eligibleRows: 2, deleted: 0,
  blocked: { liveStage: 1, uncommittedOrYoung: 0, babysitter: 0 }, done: false, ...o });
const base = { phase: "decisions" as const, limit: 500, maxPages: 10, execute: false, pauseMs: 0, statementTimeoutMs: 3000 };
const u = (n: number) => `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`;

test("pages one short transaction each with a bounded statement_timeout, follows the cursor, sums counts", async () => {
  const f = fake([page({ next: u(1) }), page({ next: u(2), done: true })]);
  const r = await runBookkeepingCleanup({ ...base, sql: f.sql });
  expect(r).toMatchObject({ mode: "dry-run", pages: 2, scannedRows: 6, eligibleRows: 4, deleted: 0, done: true, next: null, blocked: { liveStage: 2 } });
  expect(f.txs).toHaveLength(2);
  for (const t of f.txs) { expect(t[0]).toBe("SET LOCAL statement_timeout=3000"); expect(t).toHaveLength(2); }
  expect(JSON.parse(f.txs[0]![1]!.slice(f.txs[0]![1]!.indexOf("{")))).toEqual({ after: null, rowBudget: 500, execute: false });
  expect(JSON.parse(f.txs[1]![1]!.slice(f.txs[1]![1]!.indexOf("{")))).toEqual({ after: u(1), rowBudget: 500, execute: false });
});
test("stops on busy without waiting, and pauses between pages", async () => {
  const slept: number[] = [];
  const f = fake([page({ next: u(1) }), { phase: "decisions", busy: true }]);
  const r = await runBookkeepingCleanup({ ...base, sql: f.sql, pauseMs: 250, sleep: async (ms) => { slept.push(ms); } });
  expect(r).toMatchObject({ busy: true, pages: 1, next: u(1), done: false });
  expect(slept).toEqual([250]);
});
test("refuses a non-advancing cursor, a mode mismatch, deletes in a dry run, and bad options", async () => {
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ next: u(2) }), page({ next: u(2) })]).sql })).rejects.toBeInstanceOf(BookkeepingCleanupError);
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ mode: "execute", next: null })]).sql })).rejects.toMatchObject({ code: "result_shape" });
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ deleted: 1, next: null })]).sql })).rejects.toMatchObject({ code: "result_counts" });
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ next: "not-a-uuid" })]).sql })).rejects.toMatchObject({ code: "cursor" });
  for (const bad of [{ limit: 499 }, { statementTimeoutMs: 0 }, { statementTimeoutMs: 5001 }, { maxPages: 0 }, { phase: "frozen" as const, execute: true }])
    await expect(runBookkeepingCleanup({ ...base, ...bad, sql: fake([]).sql })).rejects.toMatchObject({ code: "options" });
  await expect(runBookkeepingCleanup({ ...base, after: "x", sql: fake([]).sql })).rejects.toMatchObject({ code: "cursor" });
});
test("seats cursor advances numerically, not lexically", async () => {
  const p = (next: string | null, done = false) => ({ phase: "seats", busy: false, mode: "dry-run", scannedRows: 1, eligibleRows: 0, deleted: 0, blocked: {}, next, done });
  const r = await runBookkeepingCleanup({ ...base, phase: "seats", limit: 10, sql: fake([p("9"), p("10", true)]).sql });
  expect(r).toMatchObject({ pages: 2, done: true });
});
