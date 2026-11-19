/** The live claims scheduler (M1-3): gates, settings, the tick with fakes, the runner wrapper, fail-open and the flag-off golden. */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  CANDIDATES_SQL, CLAIMS_SCHEDULER_BOUNDS, CLAIMS_SCHEDULER_CANDIDATES_MAX, CLAIMS_SCHEDULER_INPUT_BYTES,
  CLAIMS_SCHEDULER_LIVE_SECONDS, CLAIMS_SCHEDULER_RESERVE_TOKENS, claimsSchedulerEnabled, claimsSchedulerSettings,
  claimsSchedulerTick, SCHEDULE_SQL, withClaimsScheduler,
} from "../claims-scheduler.ts";
import { runnerTick } from "../runner.ts";
import { controlsOff } from "../../shared/session-intel-controls.ts";

const ALL_ON = {
  SESSION_ANALYSIS_ENABLED: "true", SUMMARY_DECISION_RECORDS_ENABLED: "true",
  SESSION_INTEL_CLAIMS_ENABLED: "true", SESSION_INTEL_CLAIMS_SCHEDULER_ENABLED: "true",
};

describe("gates", () => {
  test("every flag exactly true", () => {
    expect(claimsSchedulerEnabled({})).toBe(false);
    expect(claimsSchedulerEnabled(ALL_ON)).toBe(true);
    for (const name of Object.keys(ALL_ON)) {
      expect(claimsSchedulerEnabled({ ...ALL_ON, [name]: undefined })).toBe(false);
      for (const v of ["1", "TRUE", " true", "yes", ""]) expect(claimsSchedulerEnabled({ ...ALL_ON, [name]: v })).toBe(false);
    }
  });
});

describe("settings: defaults, and out-of-range or malformed values fall back", () => {
  test("defaults", () => {
    expect(claimsSchedulerSettings({})).toEqual({
      intervalSeconds: 120, minIntervalSeconds: 600, workspaceHourlyCap: 5, globalDailyCap: 40, dailyTokenCeiling: 5_600_000,
    });
  });
  const names = {
    intervalSeconds: "SESSION_INTEL_CLAIMS_SCHEDULER_INTERVAL_SECONDS",
    minIntervalSeconds: "SESSION_INTEL_CLAIMS_SCHEDULER_MIN_INTERVAL_SECONDS",
    workspaceHourlyCap: "SESSION_INTEL_CLAIMS_WORKSPACE_HOURLY_CAP",
    globalDailyCap: "SESSION_INTEL_CLAIMS_GLOBAL_DAILY_CAP",
    dailyTokenCeiling: "SESSION_INTEL_CLAIMS_DAILY_TOKEN_CEILING",
  } as const;
  for (const [field, name] of Object.entries(names) as [keyof typeof names, string][]) {
    const b = CLAIMS_SCHEDULER_BOUNDS[field];
    test(`${name}: ${b.min}..${b.max}, default ${b.fallback}`, () => {
      expect(claimsSchedulerSettings({ [name]: String(b.min) })[field]).toBe(b.min);
      expect(claimsSchedulerSettings({ [name]: String(b.max) })[field]).toBe(b.max);
      for (const bad of [String(b.min - 1), String(b.max + 1), "-1", "1e3", " 10", "ten", ""])
        expect(claimsSchedulerSettings({ [name]: bad })[field]).toBe(b.fallback);
    });
  }
  test("the token ceiling is bounded at 50,000,000; the reservation is the worst case of one job", () => {
    expect(CLAIMS_SCHEDULER_BOUNDS.dailyTokenCeiling.max).toBe(50_000_000);
    expect(CLAIMS_SCHEDULER_RESERVE_TOKENS).toBe(Math.ceil((480 * 1024) / 4) + 16_384);
  });
  test("the input cap is the engine's own (engine/session-evidence.ts MAXIMUM_INPUT_BYTES)", () => {
    const engine = readFileSync(join(import.meta.dir, "../engine/session-evidence.ts"), "utf8");
    const m = /const MAXIMUM_INPUT_BYTES = (\d+) \* 1024;/.exec(engine);
    expect(m && Number(m[1]) * 1024).toBe(CLAIMS_SCHEDULER_INPUT_BYTES);
    // The full analysis call has no output cap of its own (only the summary sets one), so the allowance applies.
    const model = readFileSync(join(import.meta.dir, "../engine/model.ts"), "utf8");
    expect(model.match(/maxOutputTokens: \d+/g)).toEqual(["maxOutputTokens: 256"]);
  });
});

type Call = [string, unknown[]];
function fakeDb(rows: { org_id: string; workspace_id: string; session_id: string }[], outcomes: Record<string, unknown>, over: { candidates?: () => never } = {}) {
  const calls: Call[] = [];
  return {
    calls,
    db: {
      query: async (text: string, params: unknown[]) => {
        calls.push([text, params]);
        if (text === CANDIDATES_SQL) { if (over.candidates) over.candidates(); return { rows }; }
        const o = outcomes[params[2] as string];
        if (o instanceof Error) throw o;
        return { rows: [{ r: o }] };
      },
    } as never,
  };
}
const on = new Map(controlsOff()).set("claims", true);
const SETTINGS = claimsSchedulerSettings({});
const c = (ws: string, s: string) => ({ org_id: "org-a", workspace_id: ws, session_id: s });

describe("claimsSchedulerTick", () => {
  test("candidates, then one admission per session with the budgets as arguments; outcomes are counted", async () => {
    const { db, calls } = fakeDb([c("ws-a", "s-1"), c("ws-a", "s-2"), c("ws-a", "s-3"), c("ws-a", "s-4"), c("ws-a", "s-5")], {
      "s-1": { outcome: "enqueued" }, "s-2": { outcome: "over_budget", reason: "global_daily" }, "s-3": { outcome: "in_flight" },
      "s-4": { outcome: "surprise" }, "s-5": new Error("boom"),
    });
    const counts = await claimsSchedulerTick({ db, controls: { read: async () => on }, settings: SETTINGS });
    expect(counts).toEqual({ selected: 5, enqueued: 1, skipped_budget: 1, skipped_control: 0, skipped: 1, failed: 2 });
    expect(calls[0]).toEqual([CANDIDATES_SQL, [CLAIMS_SCHEDULER_LIVE_SECONDS, 600, CLAIMS_SCHEDULER_CANDIDATES_MAX]]);
    expect(calls[1]).toEqual([SCHEDULE_SQL, ["org-a", "ws-a", "s-1", 5, 40, 600, 5_600_000, CLAIMS_SCHEDULER_RESERVE_TOKENS]]);
  });
  test("controls fail closed: off, missing, a thrown read: no admission call, counted; read once per workspace", async () => {
    let reads = 0;
    for (const read of [async () => controlsOff(), async () => new Map(), async () => { throw new Error("down"); }]) {
      const { db, calls } = fakeDb([c("ws-a", "s-1"), c("ws-a", "s-2")], { "s-1": { outcome: "enqueued" }, "s-2": { outcome: "enqueued" } });
      const counts = await claimsSchedulerTick({ db, controls: { read: async (o, w) => { reads += 1; return (await read()) as never; } }, settings: SETTINGS });
      expect(counts).toEqual({ selected: 2, enqueued: 0, skipped_budget: 0, skipped_control: 2, skipped: 0, failed: 0 });
      expect(calls.map((x) => x[0])).toEqual([CANDIDATES_SQL]);
    }
    expect(reads).toBe(3);
  });
  test("a failed candidates read is one failure and nothing else", async () => {
    const { db, calls } = fakeDb([], {}, { candidates: () => { throw new Error("down"); } });
    expect(await claimsSchedulerTick({ db, controls: { read: async () => on }, settings: SETTINGS }))
      .toEqual({ selected: 0, enqueued: 0, skipped_budget: 0, skipped_control: 0, skipped: 0, failed: 1 });
    expect(calls.length).toBe(1);
  });
});

describe("withClaimsScheduler: fail-open around the analysis tick", () => {
  test("runs at most once per interval, after the analysis tick, whose result passes through", async () => {
    let t = 0, runs = 0;
    const tick = withClaimsScheduler(async () => ({ completed: 1 }), { run: async () => { runs += 1; return { selected: 0, enqueued: 0, skipped_budget: 0, skipped_control: 0, skipped: 0, failed: 0 }; }, intervalMs: 120_000, now: () => t });
    expect(await tick()).toEqual({ completed: 1 });
    t = 60_000; await tick();
    expect(runs).toBe(1);
    t = 120_000; await tick();
    expect(runs).toBe(2);
  });
  test("a scheduler failure never reaches the analysis tick (summaries, briefs and analyses are its results)", async () => {
    const tick = withClaimsScheduler(async () => ({ completed: 2, failed: 0 }), { run: async () => { throw new Error("scheduler down"); }, intervalMs: 1 });
    expect(await tick()).toEqual({ completed: 2, failed: 0 });
  });
  test("an analysis tick failure still throws (the runner counts it), and the scheduler pass still runs", async () => {
    let runs = 0;
    const tick = withClaimsScheduler(async () => { throw new Error("tick"); }, { run: async () => { runs += 1; return { selected: 0, enqueued: 0, skipped_budget: 0, skipped_control: 0, skipped: 0, failed: 0 }; }, intervalMs: 1 });
    await expect(tick()).rejects.toThrow("tick");
    expect(runs).toBe(1);
  });
});

describe("flag-off golden: the runner's tick is the analysis tick itself", () => {
  const analysis = async () => ({ completed: 0 });
  const db = { query: async () => { throw new Error("no query when off"); } } as never;
  test("any gate off, or no model: the very same function, and no query", async () => {
    expect(runnerTick({}, analysis, db, true)).toBe(analysis);
    for (const name of Object.keys(ALL_ON)) expect(runnerTick({ ...ALL_ON, [name]: "false" }, analysis, db, true)).toBe(analysis);
    expect(runnerTick(ALL_ON, analysis, db, false)).toBe(analysis);
    // Tuning alone changes nothing.
    expect(runnerTick({ SESSION_INTEL_CLAIMS_WORKSPACE_HOURLY_CAP: "9", SESSION_INTEL_CLAIMS_SCHEDULER_INTERVAL_SECONDS: "60" }, analysis, db, true)).toBe(analysis);
  });
  test("every gate on with a model: wrapped, and a failing database leaves the analysis result alone", async () => {
    const wrapped = runnerTick(ALL_ON, analysis, db, true);
    expect(wrapped).not.toBe(analysis);
    expect(await wrapped()).toEqual({ completed: 0 });
  });
});
