import { describe, expect, test } from "bun:test";
import type { SqlQuery } from "../../session-read-api/liveness.ts";
import { CONTROLS_SQL, SESSION_INTEL_FEATURES, sessionIntelControls } from "../session-intel-controls.ts";

const sql = (read: (args?: unknown[]) => Promise<unknown>): SqlQuery => ({
  query: async (_text, args) => await read(args) as never,
});
const allOff = (m: ReadonlyMap<string, boolean>) => expect([...m.entries()]).toEqual(SESSION_INTEL_FEATURES.map((f) => [f, false]));

describe("session intelligence controls", () => {
  test("binds the tenant, defaults missing features off, and does not cache a previous tick", async () => {
    let on = true;
    const calls: unknown[] = [];
    const reader = sessionIntelControls({ query: async (text, args) => {
      expect(text).toBe(CONTROLS_SQL); calls.push(args);
      return { rows: [{ feature: "advisor", enabled: on }] } as never;
    } });
    const first = await reader.read("org-a", "ws-a");
    expect(first.get("advisor")).toBe(true);
    expect(first.get("claims")).toBe(false);
    on = false;
    allOff(await reader.read("org-b", "ws-b"));
    expect(calls).toEqual([["org-a", "ws-a"], ["org-b", "ws-b"]]);
  });

  test("query rejection, a missing function, and malformed rows fail closed without foreign error text", async () => {
    for (const code of ["42883", "08006"]) {
      const warnings: string[] = [];
      const reader = sessionIntelControls(sql(async () => { throw Object.assign(new Error("private error"), { code }); }), { warn: (r) => warnings.push(r) });
      allOff(await reader.read("org", "ws"));
      expect(warnings).toEqual(["unavailable"]);
    }
    for (const rows of [[{ feature: "advisor", enabled: "true" }], [{ feature: "foreign", enabled: true }],
      [{ feature: "advisor", enabled: true }, { feature: "advisor", enabled: false }]]) {
      const warnings: string[] = [];
      allOff(await sessionIntelControls(sql(async () => ({ rows })), { warn: (r) => warnings.push(r) }).read("org", "ws"));
      expect(warnings).toEqual(["malformed"]);
    }
  });

  for (const rejects of [false, true]) test(`a timed-out query is bounded and a late ${rejects ? "rejection" : "completion"} permits a fresh read`, async () => {
    let finish!: (result: unknown) => void;
    let calls = 0;
    const warnings: string[] = [];
    const reader = sessionIntelControls(sql(async () => {
      calls++;
      return calls === 1 ? new Promise((resolve, reject) => {
        finish = (result) => rejects ? reject(new Error("late private error")) : resolve(result);
      }) : { rows: [{ feature: "advisor", enabled: true }] };
    }), { timeoutMs: 10, warn: (r) => warnings.push(r) });
    allOff(await reader.read("org", "ws"));
    allOff(await reader.read("other-org", "other-ws"));
    expect(calls).toBe(1);
    expect(warnings).toEqual(["timeout", "busy"]);
    finish({ rows: [{ feature: "advisor", enabled: true }] });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect((await reader.read("org", "ws")).get("advisor")).toBe(true);
    expect(calls).toBe(2);
  });
});
