import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import {
  LOCKER_LIFECYCLE_RULES,
  LOCKER_NONCURRENT_RETENTION_DAYS,
  ensureCloudSqlProtection,
  ensureLockerLifecycle,
  lifecycleMatches,
} from "../data-retention.ts";

const json = (status: number, body: unknown = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const BUCKET = "awf-conformance-locker-agentrelay-96bulls-soc2-2610";

describe("locker lifecycle", () => {
  it("is exactly one rule: delete noncurrent versions after 30 days, never a live object", () => {
    expect(LOCKER_NONCURRENT_RETENTION_DAYS).toBe(30);
    expect(LOCKER_LIFECYCLE_RULES).toEqual([{ action: { type: "Delete" }, condition: { isLive: false, daysSinceNoncurrentTime: 30 } }]);
  });

  it("matches only the exact rule set, in any key order", () => {
    expect(lifecycleMatches({ rule: [{ condition: { daysSinceNoncurrentTime: 30, isLive: false }, action: { type: "Delete" } }] })).toBe(true);
    expect(lifecycleMatches(undefined)).toBe(false);
    expect(lifecycleMatches({ rule: [] })).toBe(false);
    expect(lifecycleMatches({ rule: [{ action: { type: "Delete" }, condition: { isLive: false, daysSinceNoncurrentTime: 7 } }] })).toBe(false);
    expect(lifecycleMatches({ rule: [{ action: { type: "Delete" }, condition: { age: 30 } }] })).toBe(false);
    expect(
      lifecycleMatches({ rule: [...LOCKER_LIFECYCLE_RULES, { action: { type: "Delete" }, condition: { age: 1 } }] }),
    ).toBe(false);
  });

  it("writes only when it differs, then reads it back", async () => {
    let state: unknown = undefined;
    const calls: string[] = [];
    const t = async (url: string, init: RequestInit) => {
      calls.push(`${init.method} ${url}`);
      if (init.method === "PATCH") {
        state = JSON.parse(String(init.body)).lifecycle;
        return json(200, { lifecycle: state });
      }
      return json(200, state ? { lifecycle: state } : {});
    };
    expect(await ensureLockerLifecycle(BUCKET, { bearer: "t" }, t)).toEqual({ changed: true });
    expect(calls).toEqual([
      `GET https://storage.googleapis.com/storage/v1/b/${BUCKET}?fields=lifecycle`,
      `PATCH https://storage.googleapis.com/storage/v1/b/${BUCKET}?fields=lifecycle`,
      `GET https://storage.googleapis.com/storage/v1/b/${BUCKET}?fields=lifecycle`,
    ]);
    calls.length = 0;
    expect(await ensureLockerLifecycle(BUCKET, { bearer: "t" }, t)).toEqual({ changed: false });
    expect(calls).toHaveLength(1);
  });

  it("fails closed on a read or write error, a bad read-back, or a malformed bucket", async () => {
    await expect(ensureLockerLifecycle(BUCKET, { bearer: "t" }, async () => json(403))).rejects.toThrow("read failed with HTTP 403");
    await expect(
      ensureLockerLifecycle(BUCKET, { bearer: "t" }, async (_u, i) => (i.method === "PATCH" ? json(400) : json(200, {}))),
    ).rejects.toThrow("write failed with HTTP 400");
    await expect(ensureLockerLifecycle(BUCKET, { bearer: "t" }, async () => json(200, {}))).rejects.toThrow("did not read back");
    await expect(ensureLockerLifecycle('b") x', { bearer: "t" }, async () => json(200))).rejects.toThrow("malformed");
  });
});

describe("Cloud SQL protection", () => {
  it("patches only the two settings, awaits the operation and reads back", async () => {
    let s: Record<string, boolean> = { deletionProtectionEnabled: false };
    let polls = 0;
    const patched: unknown[] = [];
    const t = async (url: string, init: RequestInit) => {
      if (init.method === "PATCH") {
        patched.push(JSON.parse(String(init.body)));
        return json(200, { name: "op-123" });
      }
      if (url.endsWith("/operations/op-123")) {
        polls++;
        if (polls < 2) return json(200, { status: "RUNNING" });
        s = { deletionProtectionEnabled: true, retainBackupsOnDelete: true };
        return json(200, { status: "DONE" });
      }
      return json(200, { settings: s });
    };
    expect(await ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "awf-conformance-postgres", { bearer: "t" }, t, 0)).toEqual({ changed: true });
    expect(patched).toEqual([{ settings: { deletionProtectionEnabled: true, retainBackupsOnDelete: true } }]);
    expect(await ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "awf-conformance-postgres", { bearer: "t" }, t, 0)).toEqual({ changed: false });
  });

  it("fails closed on an operation error, a missing read-back, a timeout, or a malformed name", async () => {
    const base = (op: unknown, after: Record<string, boolean> = {}) => async (url: string, init: RequestInit) =>
      init.method === "PATCH" ? json(200, { name: "op-1" }) : url.includes("/operations/") ? json(200, op) : json(200, { settings: after });
    await expect(ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, base({ status: "DONE", error: { errors: [] } }), 0)).rejects.toThrow("finished with an error");
    await expect(ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, base({ status: "DONE" }), 0)).rejects.toThrow("did not read back");
    await expect(ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, base({ status: "RUNNING" }), 0, 3)).rejects.toThrow("did not finish");
    await expect(ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, async () => json(404), 0)).rejects.toThrow("read failed with HTTP 404");
    await expect(ensureCloudSqlProtection("Bad_Project", "pg", { bearer: "t" }, async () => json(200), 0)).rejects.toThrow("malformed");
  });

  it("never writes after the apply lease expired or was aborted", async () => {
    let patches = 0;
    const t = async (_u: string, init: RequestInit) => (init.method === "PATCH" ? (patches++, json(200, { name: "op" })) : json(200, { settings: {} }));
    await expect(
      ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, t, 0, 3, { expiresAtMs: Date.now() - 1 }),
    ).rejects.toThrow("lease expired");
    const ac = new AbortController();
    ac.abort();
    await expect(ensureCloudSqlProtection("agentrelay-96bulls-soc2-2610", "pg", { bearer: "t" }, t, 0, 3, { signal: ac.signal })).rejects.toThrow("interrupted");
    await expect(ensureLockerLifecycle(BUCKET, { bearer: "t" }, async (_u, i) => (i.method === "PATCH" ? (patches++, json(200)) : json(200, {})), { expiresAtMs: Date.now() - 1 })).rejects.toThrow("lease expired");
    expect(patches).toBe(0);
  });

  it("is wired into the stack: Cloud SQL protection right after the poster, lifecycle on every locker", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    const poster = stack.indexOf('EnsurePrivateCloudSql("private-cloud-sql"');
    const protection = stack.indexOf('EnsureCloudSqlProtection("cloud-sql-protection"');
    expect(protection).toBeGreaterThan(poster);
    // Nothing else is declared between the poster and its protection.
    expect(stack.slice(poster, protection).match(/yield\* /g)?.length).toBe(1);
    expect(stack).toContain('EnsureLockerLifecycle("locker-lifecycle", { bucket: locker.bucketName, authorization: applyAuthorization })');
    // The lifecycle sits outside the managed-data-plane branch.
    expect(stack.indexOf('EnsureLockerLifecycle("locker-lifecycle"')).toBeLessThan(stack.indexOf("const dataPlane = i.managedDataPlane.provision"));
    for (const name of ["ensureCloudSqlProtection", "ensureLockerLifecycle"]) expect(stack).toMatch(new RegExp(`${name}\\([\\s\\S]{0,260}expiresAtMs: input\\.authorization\\.leaseExpiresAt \\* 1000,\\s*signal`));
  });
});
