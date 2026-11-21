/**
 * COORDINATION_ADVISOR_INCARNATION_ENABLED (20261105062220): exactly "true" makes the worker write through
 * coordination_advisories_upsert_v2; anything else keeps upsert_v1 exactly as before. With the relay DM flag also on
 * the worker refuses to start (instance mode has no DM), before HoopDb, the backend or the sender is touched.
 */
import { describe, expect, test } from "bun:test";
import { CONTROLS_SQL, SESSION_INTEL_FEATURES } from "../../shared/session-intel-controls.ts";
import type { WorkerOptions } from "../../shared/worker.ts";
import {
  advisorConfig, COORDINATION_ADVISOR_INCARNATION_ENABLED, COORDINATION_ADVISOR_RELAY_DM_ENABLED,
  COORDINATION_ADVISOR_RELAY_TOKEN, COORDINATION_ADVISOR_RELAY_URL,
} from "../config.ts";
import { advisorMain, type AdvisorIo } from "../main.ts";
import { CANDIDATES_SQL, sqlAdvisorStore, UPSERT_SQL, UPSERT_V2_SQL } from "../scan.ts";
import { recordingBackend } from "./scan-golden-scenario.ts";

const BASE = { COORDINATION_ADVISOR_ENABLED: "true", READ_API_DIRECTORY_WORKSPACES: "*" };
const DM = { [COORDINATION_ADVISOR_RELAY_DM_ENABLED]: "true", [COORDINATION_ADVISOR_RELAY_TOKEN]: "t", [COORDINATION_ADVISOR_RELAY_URL]: "https://relay.example" };
const ROW = { advisory_id: "0123456789abcdef", harness: "claude", session_id: "a", peer_harness: "claude", peer_session_id: "b",
  urgency: "high" as const, kind: "concurrent_edit", artifact: "src/x.ts", evidence: [] };

/** A fake HoopDb that answers the scan's reads and records every statement. */
function fakeDb(now: Date) {
  const sql: string[] = [];
  const lane = (s: string) => ({ org_id: "org", workspace_id: "ws", harness: "claude", session_id: s, project_id: "proj", git_repo: "o/r",
    short_summary: null, last_activity_at: new Date(now.getTime() - 5 * 60_000), parent_session_ids: [] });
  const db = { query: async (text: string) => {
    sql.push(text);
    if (text === CANDIDATES_SQL) return { rows: [lane("a"), lane("b")] };
    if (text === CONTROLS_SQL) return { rows: SESSION_INTEL_FEATURES.map((f) => ({ feature: f, enabled: true })) };
    if (text === UPSERT_SQL || text === UPSERT_V2_SQL) return { rows: [{ inserted: 1, upgraded: 0, skipped: 0 }] };
    return { rows: [] };
  } };
  return { db, sql };
}

function io(calls: string[]): AdvisorIo & { tick?: WorkerOptions["tick"] } {
  const self: AdvisorIo & { tick?: WorkerOptions["tick"] } = {
    runWorker: async (opts) => { calls.push("runWorker"); self.tick = opts.tick; },
    idle: async () => { calls.push("idle"); },
    backend: () => { calls.push("backend"); return () => recordingBackend([]); },
    sender: () => { calls.push("sender"); return undefined; },
  };
  return self;
}

describe("the advisory instance writer flag", () => {
  test("exactly \"true\" only, default off", () => {
    expect(COORDINATION_ADVISOR_INCARNATION_ENABLED).toBe("COORDINATION_ADVISOR_INCARNATION_ENABLED");
    expect(advisorConfig(BASE).incarnation).toBe(false);
    for (const v of ["1", "on", "TRUE", "yes", " true", "", "false"])
      expect([v, advisorConfig({ ...BASE, [COORDINATION_ADVISOR_INCARNATION_ENABLED]: v }).incarnation]).toEqual([v, false]);
    expect(advisorConfig({ ...BASE, [COORDINATION_ADVISOR_INCARNATION_ENABLED]: "true" }).incarnation).toBe(true);
    // Each flag alone is fine.
    expect(advisorConfig({ ...BASE, ...DM }).relayDm.enabled).toBe(true);
    expect(advisorConfig({ ...BASE, ...DM, [COORDINATION_ADVISOR_INCARNATION_ENABLED]: "1" }).relayDm.enabled).toBe(true);
  });

  test("both flags on: the config refuses, and advisorMain refuses before the backend, the sender or the worker starts", async () => {
    const env = { ...BASE, ...DM, [COORDINATION_ADVISOR_INCARNATION_ENABLED]: "true" };
    expect(() => advisorConfig(env)).toThrow(/COORDINATION_ADVISOR_INCARNATION_ENABLED and COORDINATION_ADVISOR_RELAY_DM_ENABLED must not both be on/);
    const calls: string[] = [];
    await expect(advisorMain(env, io(calls))).rejects.toThrow(/instance mode has no DM/);
    expect(calls).toEqual([]);
    // The error names the variables, never their values.
    expect(() => advisorConfig(env)).not.toThrow(/relay\.example/);
  });

  test("the store: off is upsert_v1 (the exact pre-062220 statement), on is upsert_v2 with the same arguments", async () => {
    for (const [opts, want] of [[undefined, UPSERT_SQL], [{}, UPSERT_SQL], [{ incarnation: false }, UPSERT_SQL], [{ incarnation: true }, UPSERT_V2_SQL]] as const) {
      const seen: Array<[string, unknown[] | undefined]> = [];
      const db = { query: async (text: string, params?: unknown[]) => { seen.push([text, params]); return { rows: [{ inserted: 1, upgraded: 0, skipped: 0 }] }; } };
      const store = opts === undefined ? sqlAdvisorStore(db as never) : sqlAdvisorStore(db as never, opts);
      expect(await store.upsert("org", "ws", ROW)).toEqual({ inserted: 1, upgraded: 0, skipped: 0 });
      expect(seen).toEqual([[want, ["org", "ws", JSON.stringify([ROW])]]]);
    }
    expect(UPSERT_SQL).toBe("SELECT inserted, upgraded, skipped FROM sessions.coordination_advisories_upsert_v1($1, $2, $3::jsonb)");
    expect(UPSERT_V2_SQL).toBe("SELECT inserted, upgraded, skipped FROM sessions.coordination_advisories_upsert_v2($1, $2, $3::jsonb)");
  });

  test("main wires the flag: the tick writes through v2 only with it on, v1 otherwise; never both", async () => {
    const now = new Date("2026-11-17T12:00:00.000Z");
    for (const [flag, want] of [[undefined, UPSERT_SQL], ["false", UPSERT_SQL], ["1", UPSERT_SQL], ["true", UPSERT_V2_SQL]] as const) {
      const calls: string[] = [];
      const w = io(calls);
      expect(await advisorMain({ ...BASE, ...(flag === undefined ? {} : { [COORDINATION_ADVISOR_INCARNATION_ENABLED]: flag }) }, w)).toBe("ran");
      const { db, sql } = fakeDb(now);
      await w.tick!(db as unknown as Parameters<NonNullable<WorkerOptions["tick"]>>[0], now);
      const upserts = sql.filter((s) => s === UPSERT_SQL || s === UPSERT_V2_SQL);
      expect(upserts.length).toBeGreaterThan(0);
      expect([flag, [...new Set(upserts)]]).toEqual([flag, [want]]);
    }
  });
});
