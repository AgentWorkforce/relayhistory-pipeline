/**
 * Stage 2 truncation edges (src/session-intel/synthesis/stage2.ts selectStage2Sessions): the 2-session floor of the
 * binary search, and the (source, sessionId) tie-break on equal analyzedAt.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { analyzedSessions, synthesize, type AnalyzedSession } from "../synthesis/synthesize.ts";
import { buildStage2Prompt, estimateInputTokens, memoryBudgetLedger, runStage2, selectStage2Sessions, type Stage2ModelTransport } from "../synthesis/stage2.ts";

const cohortText = readFileSync(new URL("./fixtures/synthesis/cohort.json", import.meta.url), "utf8");
const cohort = () => JSON.parse(cohortText) as { sessions: Array<Record<string, any>> };
const OPTS = { now: "2026-11-08T00:00:00.000Z" };
const byKey = (x: AnalyzedSession, y: AnalyzedSession) => (x.source < y.source ? -1 : x.source > y.source ? 1 : x.sessionId < y.sessionId ? -1 : x.sessionId > y.sessionId ? 1 : 0);
const newest = (all: AnalyzedSession[]) => [...all].sort((x, y) => y.analyzedAtMs - x.analyzedAtMs || byKey(x, y));

describe("the truncation floor", () => {
  test("an input cap that fits exactly ONE session: insufficient_sessions, no call, no reservation", async () => {
    const c = cohort();
    const v1 = synthesize(c, OPTS);
    const all = analyzedSessions(c, OPTS);
    const order = newest(all);
    const one = estimateInputTokens(buildStage2Prompt(v1, order.slice(0, 1), all));
    const two = estimateInputTokens(buildStage2Prompt(v1, order.slice(0, 2), all));
    // The cap admits one session and not two: the precondition the floor is tested under.
    expect(one).toBeLessThan(two);
    expect(selectStage2Sessions(v1, all, one)).toBeNull();
    expect(selectStage2Sessions(v1, all, two)!.kept.length).toBe(2);
    let calls = 0;
    const transport: Stage2ModelTransport = { async complete() { calls++; throw new Error("fx: must not be called"); } };
    const ledger = memoryBudgetLedger();
    const r = await runStage2({
      v1, cohort: c, options: OPTS, orgId: "fx-org",
      deps: { enabled: true, trigger: "manual", transport, ledger, clock: () => new Date("2026-11-24T10:00:00.000Z"), caps: { maxInputTokensPerRun: one } },
    });
    expect([r.document.stage2, r.document.stage2Reason, r.called, calls, ledger.reservations.size, ledger.runs.size]).toEqual([null, "insufficient_sessions", false, 0, 0, 0]);
  });
});

describe("the tie-break", () => {
  test("sessions tied on analyzedAt are kept in (source, sessionId) order", () => {
    // Three sessions tied as the newest: fx-claude/fx-s33, fx-codex/fx-s24, fx-codex/fx-s34. A cap that fits two
    // keeps fx-claude/fx-s33 (source first) and fx-codex/fx-s24 (then sessionId), never fx-codex/fx-s34.
    const c = cohort();
    const tied = new Set(["fx-claude\u0000fx-s33", "fx-codex\u0000fx-s24", "fx-codex\u0000fx-s34"]);
    for (const e of c.sessions) if (tied.has(`${e.source}\u0000${e.sessionId}`)) e.result.analysis.analyzedAt = "2026-11-07T23:00:00.000Z";
    const v1 = synthesize(c, OPTS);
    const all = analyzedSessions(c, OPTS);
    expect(all.filter((a) => a.analyzedAtMs === Date.parse("2026-11-07T23:00:00.000Z")).length).toBe(3);
    const expected = all.filter((a) => (a.source === "fx-claude" && a.sessionId === "fx-s33") || (a.source === "fx-codex" && a.sessionId === "fx-s24"));
    const cap = estimateInputTokens(buildStage2Prompt(v1, expected, all));
    for (const order of [all, [...all].reverse()]) {
      const picked = selectStage2Sessions(v1, order, cap);
      expect(picked!.kept.map((a) => `${a.source}/${a.sessionId}`)).toEqual(["fx-claude/fx-s33", "fx-codex/fx-s24"]);
    }
  });
});
