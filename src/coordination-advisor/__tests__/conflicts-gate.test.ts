/**
 * M2-4 gate: with SESSION_INTEL_CONFLICTS_ENABLED off (absent or false), or the `conflicts` control off, the scan
 * is byte-identical to the scan before M2-4: the same store, directory, backend and sender calls with the same
 * arguments, and the same stats. The golden (fixtures/scan-golden.json) was recorded from scan.ts before the flag
 * existed. A claims spy proves no claims read.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_INTEL_FEATURES } from "../../shared/session-intel-controls.ts";
import type { CoordinationBackend, CoordinationFacts } from "../../session-read-api/coordination-backend.ts";
import { CONTROLS_SQL } from "../../shared/session-intel-controls.ts";
import type { WorkerOptions } from "../../shared/worker.ts";
import { CLAIMS_SQL } from "../conflicts.ts";
import { advisorConfig, SESSION_INTEL_CONFLICTS_ENABLED } from "../config.ts";
import { advisorMain, type AdvisorIo } from "../main.ts";
import { CANDIDATES_SQL } from "../scan.ts";
import { ALL_ON, RecordingStore, runGoldenScenario } from "./scan-golden-scenario.ts";

const GOLDEN = JSON.parse(readFileSync(join(import.meta.dir, "fixtures", "scan-golden.json"), "utf8")) as { log: unknown[]; stats: unknown[] };
const canon = (v: unknown) => JSON.parse(JSON.stringify(v));

/** A store with a claims reader that records (and would fail the test) if called. */
class SpyStore extends RecordingStore {
  claimCalls = 0;
  async claims() { this.claimCalls++; return []; }
}

describe("the conflicts gate", () => {
  const cases: Array<[string, Record<string, unknown>, boolean]> = [
    ["flag absent", {}, true],
    ["flag false", { conflicts: false }, true],
    ["flag absent, conflicts control on", {}, true],
    ["flag on, conflicts control off", { conflicts: true }, false],
  ];
  for (const [name, extra, conflictsControl] of cases) test(`${name}: byte-identical to the pre-M2-4 scan, no claims read`, async () => {
    const ctl = new Map(SESSION_INTEL_FEATURES.map((f) => [f, f === "conflicts" ? conflictsControl : true]));
    const store = new SpyStore(ctl);
    const r = await runGoldenScenario({ extra, store });
    expect(store.claimCalls).toBe(0);
    const stats = extra.conflicts === true
      ? (r.stats as Array<Record<string, unknown>>).map(({ conflicts, conflictsSkipped, ...rest }) => { expect([conflicts, conflictsSkipped]).toEqual([0, 0]); return rest; })
      : r.stats;
    expect(canon(r.log)).toEqual(GOLDEN.log);
    expect(canon(stats)).toEqual(GOLDEN.stats);
    if (extra.conflicts !== true) expect(JSON.stringify(canon(r.stats))).toBe(JSON.stringify(GOLDEN.stats));
  });

  test("a store without a claims reader is identical too", async () => {
    const r = await runGoldenScenario({ store: new RecordingStore(ALL_ON) });
    expect(canon(r)).toEqual(GOLDEN);
  });

  test("main wires the flag: only SESSION_INTEL_CONFLICTS_ENABLED=true reads claims (062150) in the tick", async () => {
    const now = new Date("2026-11-17T12:00:00.000Z");
    for (const [flag, want] of [[undefined, 0], ["false", 0], ["1", 0], ["true", 1]] as const) {
      let tick: WorkerOptions["tick"];
      const io: AdvisorIo = {
        runWorker: async (opts) => { tick = opts.tick; }, idle: async () => {},
        backend: () => () => ({ kind: "clickhouse", windowReadiness: async () => { throw new Error("unused"); }, isSchemaMissing: () => false,
          readFacts: async () => ({ lanes: [], sessionsTruncated: false, notes: [] }) as unknown as CoordinationFacts }) as CoordinationBackend,
        sender: () => undefined,
      };
      expect(await advisorMain({ COORDINATION_ADVISOR_ENABLED: "true", READ_API_DIRECTORY_WORKSPACES: "*", ...(flag === undefined ? {} : { [SESSION_INTEL_CONFLICTS_ENABLED]: flag }) }, io)).toBe("ran");
      let claimReads = 0;
      const lane = (s: string) => ({ org_id: "org", workspace_id: "ws", harness: "claude", session_id: s, project_id: "p", git_repo: null,
        short_summary: null, last_activity_at: new Date(now.getTime() - 60_000), parent_session_ids: [] });
      const db = { query: async (sql: string) => {
        if (sql === CANDIDATES_SQL) return { rows: [lane("a"), lane("b")] };
        if (sql === CONTROLS_SQL) return { rows: SESSION_INTEL_FEATURES.map((f) => ({ feature: f, enabled: true })) };
        if (sql === CLAIMS_SQL) { claimReads++; return { rows: [] }; }
        return { rows: [] };
      } };
      await tick!(db as unknown as Parameters<NonNullable<WorkerOptions["tick"]>>[0], now);
      expect([flag, claimReads]).toEqual([flag, want]);
    }
  });

  test("the flag is exactly \"true\" only, default off", () => {
    expect(SESSION_INTEL_CONFLICTS_ENABLED).toBe("SESSION_INTEL_CONFLICTS_ENABLED");
    const base = { COORDINATION_ADVISOR_ENABLED: "true" };
    expect(advisorConfig(base).conflicts).toBe(false);
    for (const v of ["1", "on", "TRUE", "yes", " true", ""]) expect(advisorConfig({ ...base, [SESSION_INTEL_CONFLICTS_ENABLED]: v }).conflicts).toBe(false);
    expect(advisorConfig({ ...base, [SESSION_INTEL_CONFLICTS_ENABLED]: "true" }).conflicts).toBe(true);
  });
});
