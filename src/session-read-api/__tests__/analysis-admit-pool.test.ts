/**
 * perRequestAdmitPool (the Worker's AdmitPool over one Hyperdrive client per admission) under
 * sqlAnalysisAdmit: the admission's statements run on the admission principal's own client in
 * one transaction, COMMIT on success, ROLLBACK on any failure, and every client is closed.
 */
import { describe, expect, test } from "bun:test";
import { ANALYSIS_ADMIT_SQL, perRequestAdmitPool, sqlAnalysisAdmit } from "../analysis-source.ts";
import { TENANT_SETTINGS_SQL } from "../liveness.ts";

const identity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read", "rth:sync"] } as never;

function fakeClient(answer: (text: string) => unknown[] | Error) {
  const seen: Array<{ text: string; params?: unknown[] }> = [];
  let ended = 0;
  const client = {
    async query(text: string, params?: unknown[]) {
      seen.push({ text, params });
      const rows = answer(text);
      if (rows instanceof Error) throw rows;
      return { rows };
    },
  };
  return { client, seen, ended: () => ended, end: async () => void ended++ };
}

const accepted = { outcome: "accepted", request: { id: "r-1", sessionId: "s-1", status: "queued" } };

describe("perRequestAdmitPool + sqlAnalysisAdmit", () => {
  test("success: BEGIN, tenant binding, admission, COMMIT on one client; the client is closed", async () => {
    const f = fakeClient((text) => (text === ANALYSIS_ADMIT_SQL ? [{ r: accepted }] : []));
    const admit = perRequestAdmitPool(async () => f.client, f.end);
    const out = await sqlAnalysisAdmit(admit.pool)(identity, "s-1");
    expect(out).toEqual(accepted as never);
    expect(f.seen.map((q) => q.text)).toEqual(["BEGIN", TENANT_SETTINGS_SQL, ANALYSIS_ADMIT_SQL, "COMMIT"]);
    expect(f.seen[1]!.params).toEqual(["org-a", "ws-a"]);
    expect(f.seen[2]!.params).toEqual(["org-a", "ws-a", "u-1", "s-1"]);
    await admit.close();
    expect(f.ended()).toBe(1);
  });

  test("a failing admission statement rolls back, rethrows, and still closes the client", async () => {
    const boom = Object.assign(new Error("tenant binding missing or not this tenant"), { code: "42501" });
    const f = fakeClient((text) => (text === ANALYSIS_ADMIT_SQL ? boom : []));
    const admit = perRequestAdmitPool(async () => f.client, f.end);
    await expect(sqlAnalysisAdmit(admit.pool)(identity, "s-1")).rejects.toBe(boom);
    expect(f.seen.map((q) => q.text)).toEqual(["BEGIN", TENANT_SETTINGS_SQL, ANALYSIS_ADMIT_SQL, "ROLLBACK"]);
    expect(f.seen.some((q) => q.text === "COMMIT")).toBe(false);
    await admit.close();
    expect(f.ended()).toBe(1);
  });

  test("a failing tenant binding never reaches the admission", async () => {
    const f = fakeClient((text) => (text === TENANT_SETTINGS_SQL ? new Error("no") : []));
    const admit = perRequestAdmitPool(async () => f.client, f.end);
    await expect(sqlAnalysisAdmit(admit.pool)(identity, "s-1")).rejects.toThrow("no");
    expect(f.seen.map((q) => q.text)).toEqual(["BEGIN", TENANT_SETTINGS_SQL, "ROLLBACK"]);
    await admit.close();
    expect(f.ended()).toBe(1);
  });

  test("a failing COMMIT is a failure (rolled back, rethrown)", async () => {
    const f = fakeClient((text) => (text === "COMMIT" ? new Error("serialization") : text === ANALYSIS_ADMIT_SQL ? [{ r: accepted }] : []));
    const admit = perRequestAdmitPool(async () => f.client, f.end);
    await expect(sqlAnalysisAdmit(admit.pool)(identity, "s-1")).rejects.toThrow("serialization");
    expect(f.seen.at(-1)!.text).toBe("ROLLBACK");
    await admit.close();
    expect(f.ended()).toBe(1);
  });

  test("an unknown outcome is a contract error, not an answer", async () => {
    const f = fakeClient((text) => (text === ANALYSIS_ADMIT_SQL ? [{ r: { outcome: "maybe" } }] : []));
    const admit = perRequestAdmitPool(async () => f.client, f.end);
    await expect(sqlAnalysisAdmit(admit.pool)(identity, "s-1")).rejects.toThrow("analysis admit contract");
    await admit.close();
    expect(f.ended()).toBe(1);
  });

  test("a connect failure surfaces and leaves nothing to close", async () => {
    let ended = 0;
    const admit = perRequestAdmitPool(async () => { throw new Error("ECONNREFUSED"); }, async () => void ended++);
    await expect(sqlAnalysisAdmit(admit.pool)(identity, "s-1")).rejects.toThrow("ECONNREFUSED");
    await admit.close();
    expect(ended).toBe(0);
  });

  test("each admission gets its own client; a released client refuses further statements; close is idempotent", async () => {
    const clients = [fakeClient(() => []), fakeClient(() => [])];
    let i = 0;
    let ended = 0;
    const admit = perRequestAdmitPool(async () => clients[i++]!.client, async () => void ended++);
    const a = await admit.pool.connect();
    const b = await admit.pool.connect();
    await a.query("SELECT 1");
    await b.query("SELECT 2");
    expect(clients[0]!.seen.map((q) => q.text)).toEqual(["SELECT 1"]);
    expect(clients[1]!.seen.map((q) => q.text)).toEqual(["SELECT 2"]);
    a.release();
    a.release();
    await expect(a.query("SELECT 3")).rejects.toThrow("used after release");
    // b was never released: close() closes it too.
    await admit.close();
    await admit.close();
    expect(ended).toBe(2);
  });

  test("a close failure is swallowed (the response is already final)", async () => {
    const f = fakeClient((text) => (text === ANALYSIS_ADMIT_SQL ? [{ r: accepted }] : []));
    const admit = perRequestAdmitPool(async () => f.client, async () => { throw new Error("socket closed"); });
    await sqlAnalysisAdmit(admit.pool)(identity, "s-1");
    await expect(admit.close()).resolves.toBeUndefined();
  });
});
