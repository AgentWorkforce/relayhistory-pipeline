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
  blocked: { liveStage: 1, uncommittedOrYoung: 0, babysitter: 0 }, skipped: 0, end: false, done: false, ...o });
const base = { phase: "decisions" as const, limit: 500, maxPages: 10, execute: false, pauseMs: 0, statementTimeoutMs: 3000 };
const u = (n: number) => `00000000-0000-0000-0000-${String(n).padStart(12, "0")}`;

test("pages one short transaction each with a bounded statement_timeout, follows the cursor, sums counts", async () => {
  const f = fake([page({ next: u(1) }), page({ next: u(2), end: true, done: true })]);
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
  expect(r).toMatchObject({ busy: true, pages: 1, next: u(1), done: false, incomplete: "busy" });
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
  const p = (next: string | null, done = false) => ({ phase: "seats", busy: false, mode: "dry-run", scannedRows: 1, eligibleRows: 0, deleted: 0, blocked: {}, skipped: 0, next, end: done, done });
  const r = await runBookkeepingCleanup({ ...base, phase: "seats", limit: 10, sql: fake([p("9"), p("10", true)]).sql });
  expect(r).toMatchObject({ pages: 2, done: true });
});
test("a skip anywhere makes the run incomplete and resumable from the earliest skipping page, never done", async () => {
  const x = (o: Record<string, unknown>) => page({ mode: "execute", deleted: 1, ...o });
  const f = fake([x({ next: u(1) }), x({ next: u(2), skipped: 1 }), x({ next: u(3) }), x({ next: u(4), end: true, done: true })]);
  const r = await runBookkeepingCleanup({ ...base, execute: true, sql: f.sql });
  expect(r).toMatchObject({ done: false, incomplete: "locked", skipped: 1, next: null, resumeFrom: u(1), pages: 4 });
  // SQL must not claim a page done while it skipped rows, nor skip anything in a dry run.
  await expect(runBookkeepingCleanup({ ...base, execute: true, sql: fake([x({ next: null, skipped: 1, end: true, done: true })]).sql })).rejects.toMatchObject({ code: "result_shape" });
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ next: null, skipped: 1, end: true })]).sql })).rejects.toMatchObject({ code: "result_shape" });
  // Running out of pages is not done either.
  expect(await runBookkeepingCleanup({ ...base, maxPages: 1, sql: fake([page({ next: u(1) })]).sql })).toMatchObject({ done: false, incomplete: "max_pages", resumeFrom: u(1) });
});
import { parseCleanupArgs, redactedReason } from "../bookkeeping-cleanup.ts";
const hx = (c: string) => c.repeat(64);
const pb = (phase: string, o: Record<string, unknown>) => ({ phase, busy: false, mode: "execute", scannedRows: 2, eligibleRows: 1, deleted: 1, blocked: { young: 0 },
  skipped: 0, end: false, done: false, ...o });
test("execute mode on seats and custody sums deletes and sends execute:true with the page limit", async () => {
  const s = fake([pb("seats", { next: "5" }), pb("seats", { next: "9", end: true, done: true })]);
  expect(await runBookkeepingCleanup({ ...base, phase: "seats", limit: 10, execute: true, sql: s.sql })).toMatchObject({ mode: "execute", deleted: 2, done: true });
  expect(JSON.parse(s.txs[1]![1]!.slice(s.txs[1]![1]!.indexOf("{")))).toEqual({ after: "5", limit: 10, execute: true });
  const c = fake([pb("custody", { next: hx("1") }), pb("custody", { next: hx("2"), end: true, done: true })]);
  expect(await runBookkeepingCleanup({ ...base, phase: "custody", limit: 10, execute: true, sql: c.sql })).toMatchObject({ mode: "execute", deleted: 2, done: true });
});
test("busy on the very first page is reported, never done, resumable from the start cursor", async () => {
  const r = await runBookkeepingCleanup({ ...base, sql: fake([{ phase: "decisions", busy: true }]).sql });
  expect(r).toMatchObject({ busy: true, pages: 0, done: false, incomplete: "busy", resumeFrom: null });
});
test("a backwards custody cursor and a next=null page that is not the end are refused", async () => {
  await expect(runBookkeepingCleanup({ ...base, phase: "custody", limit: 10, execute: true,
    sql: fake([pb("custody", { next: hx("2") }), pb("custody", { next: hx("1") })]).sql })).rejects.toMatchObject({ code: "cursor_order" });
  await expect(runBookkeepingCleanup({ ...base, sql: fake([page({ next: null })]).sql })).rejects.toMatchObject({ code: "result_shape" });
});
test("frozen census: read-only mode, counts bothConditions as eligible, never deletes, execute refused up front", async () => {
  const fz = (o: Record<string, unknown>) => ({ phase: "frozen", mode: "census", scannedRows: 4, slotInitialized: 1, ownedCovered: 3, bothConditions: 1,
    skipped: 0, end: false, done: false, ...o });
  const f = fake([fz({ next: ["a", "b", "c", "d"] }), fz({ next: ["a", "b", "c", "e"], end: true, done: true })]);
  const r = await runBookkeepingCleanup({ ...base, phase: "frozen", limit: 100, sql: f.sql });
  expect(r).toMatchObject({ mode: "census", eligibleRows: 2, deleted: 0, done: true, blocked: { slotInitialized: 2, ownedCovered: 6 } });
  expect(JSON.parse(f.txs[0]![1]!.slice(f.txs[0]![1]!.indexOf("{")))).toEqual({ after: null, limit: 100 });
  await expect(runBookkeepingCleanup({ ...base, phase: "frozen", limit: 100, execute: true, sql: fake([]).sql })).rejects.toMatchObject({ code: "options" });
});
test("CLI args: known flags only, values never another flag, no repeats; errors print only a closed reason", () => {
  expect(parseCleanupArgs(["--phase", "seats", "--after", '"7"', "--execute"])).toEqual({ phase: "seats", limit: 1000, maxPages: 1, pauseMs: 500,
    statementTimeoutMs: 3000, after: "7", execute: true });
  for (const [argv, code] of [[["--phase", "seats", "--after", "--execute"], "args_value"], [["--phase", "seats", "--bogus", "1"], "args_unknown"],
    [["--phase", "seats", "--execute", "--execute"], "args_repeat"], [["--phase", "nope"], "args_phase"], [["--phase", "seats", "--limit", "-1"], "args_limit"],
    [["--phase", "seats", "--limit", "1e3"], "args_limit"], [["--phase", "decisions", "--after", '"x"'], "cursor"], [["--phase", "seats", "--after", "{"], "args_after"]] as const)
    expect(() => parseCleanupArgs([...argv])).toThrow(expect.objectContaining({ code }));
  expect(redactedReason(Object.assign(Error("connect ECONNREFUSED db.secret-host.example:5432"), { code: "ECONNREFUSED" }))).toBe("internal_error");
  expect(redactedReason(Object.assign(Error("x at db.secret-host"), { code: "55000" }))).toBe("sqlstate_55000");
  expect(redactedReason(new Error("db.secret-host"))).toBe("internal_error");
});
import { readFileSync } from "node:fs";
test("the ingest-bookkeeping-cleanup.ts CLI builds its owner client from the pinned helper, never a connectionString", () => {
  const src = readFileSync(new URL("../../../scripts/ops/ingest-bookkeeping-cleanup.ts", import.meta.url), "utf8").replace(/^\s*\/\/.*$/gm, "");
  expect({ conn: /connectionString/.test(src), helper: /ownerClientConfig\(/.test(src) }).toEqual({ conn: false, helper: true });
});
