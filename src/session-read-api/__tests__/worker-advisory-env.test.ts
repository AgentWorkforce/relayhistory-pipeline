/**
 * The read Worker's advisory env plumbing (packages/session-read-worker/src/index.ts): the M3-7 priority switch
 * (READ_API_ADVISORY_PRIORITY_ENABLED), the 062220 instance switch (READ_API_ADVISORY_INSTANCE_ENABLED) and the notify
 * rows reach the read API, the v1-fallback warning is wired, and a
 * malformed READ_API_ADVISORY_NOTIFY_ROWS fails only the advisory routes (503) while every other route is served.
 */
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { handle, workerAdvisoryDeps, type Env } from "../../../packages/session-read-worker/src/index.ts";
import { PENDING_SQL, PENDING_V2_SQL, PENDING_V3_SQL } from "../coordination-advisories.ts";
import type { TenantRunner } from "../liveness.ts";

const recipient = { orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" };
function runner(v2Missing = false) {
  const texts: string[] = [];
  const run: TenantRunner = async (_o, _w, work) => work({
    async query<R>(text: string) {
      texts.push(text);
      if (text === PENDING_V2_SQL && v2Missing) throw Object.assign(new Error("missing"), { code: "42883" });
      if (text.includes("to_regprocedure")) return { rows: [{ installed: false } as R] };
      return { rows: [] };
    },
  });
  return { run, texts };
}

describe("workerAdvisoryDeps", () => {
  test("advisories off: nothing; on with the switch unset: v1 only and no priority; switch exactly true: v2 and advisoryPriority", async () => {
    expect(workerAdvisoryDeps({}, runner().run, new Set())).toEqual({});
    for (const value of [undefined, "", "1", "TRUE", "false"]) {
      const r = runner();
      const deps = workerAdvisoryDeps({ READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_ADVISORY_PRIORITY_ENABLED: value }, r.run, new Set());
      expect(Object.keys(deps)).toEqual(["coordinationAdvisories"]);
      await deps.coordinationAdvisories!.source.pending(recipient);
      expect(r.texts).toEqual([PENDING_SQL]);
    }
    const r = runner();
    const rows = new Set(["duplicate_work:high"]);
    const deps = workerAdvisoryDeps({ READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_ADVISORY_PRIORITY_ENABLED: "true" }, r.run, rows);
    expect(deps).toMatchObject({ advisoryNotifyRows: rows, advisoryPriority: true });
    await deps.coordinationAdvisories!.source.pending(recipient);
    expect(r.texts).toEqual([PENDING_V2_SQL]);
  });

  test("the 062220 instance switch: exactly true sets advisoryInstance (the GET reads pending_for_v3); anything else leaves the deps and the statement unchanged", async () => {
    for (const value of [undefined, "", "1", "TRUE", "false"]) {
      const r = runner();
      const deps = workerAdvisoryDeps({ READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_ADVISORY_INSTANCE_ENABLED: value }, r.run, new Set());
      expect([value, Object.keys(deps)]).toEqual([value, ["coordinationAdvisories"]]);
      await deps.coordinationAdvisories!.source.pending(recipient);
      expect(r.texts).toEqual([PENDING_SQL]);
    }
    expect(workerAdvisoryDeps({ READ_API_ADVISORY_INSTANCE_ENABLED: "true" }, runner().run, new Set())).toEqual({ advisoryInstance: true });
    const r = runner();
    const deps = workerAdvisoryDeps({ READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_ADVISORY_INSTANCE_ENABLED: "true" }, r.run, new Set());
    expect(deps).toMatchObject({ advisoryInstance: true });
    expect("advisoryPriority" in deps).toBe(false);
    await deps.coordinationAdvisories!.source.pendingInstances!(recipient);
    expect(r.texts).toEqual([PENDING_V3_SQL]);
    // The Worker never acks: the instance ack is refused like the v1 one.
    await expect(deps.coordinationAdvisories!.source.ackInstance!(recipient, "9f3c2a71d04be6e5", "00000000-0000-4000-8000-000000000000")).rejects.toThrow(/self-host service only/);
  });

  test("the v1 fallback warning is wired to the warn log, and the absent-v2 cache is shared across per-request sources", async () => {
    const warns: Record<string, unknown>[] = [];
    const env = { READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_ADVISORY_PRIORITY_ENABLED: "true" };
    const first = runner(true);
    await workerAdvisoryDeps(env, first.run, new Set(), (f) => { warns.push(f); }).coordinationAdvisories!.source.pending(recipient);
    expect(warns).toEqual([expect.objectContaining({ reason: "coordination_advisories_v1_fallback", error_code: "42883" })]);
    // A second request's source: no v2 statement within the TTL, no second warning.
    const second = runner(true);
    await workerAdvisoryDeps(env, second.run, new Set(), (f) => { warns.push(f); }).coordinationAdvisories!.source.pending(recipient);
    expect(second.texts.some((t) => t === PENDING_V2_SQL)).toBe(false);
    expect(warns.length).toBe(1);
  });
});

describe("a malformed READ_API_ADVISORY_NOTIFY_ROWS is scoped to the advisory routes", () => {
  afterEach(() => { (console.error as { mockRestore?: () => void }).mockRestore?.(); });
  const env = {
    CLICKHOUSE_URL: "https://ch.test.example", CLICKHOUSE_USER: "u", CLICKHOUSE_PASSWORD: "p", CLICKHOUSE_DATABASE: "d",
    READ_API_ORIGIN: "https://read.test.example", READ_API_COMPLETE_SINCE: "epoch",
    RELAYAUTH_JWKS_URL: "https://auth.test.example/jwks", RELAYAUTH_ISSUER: "https://relayauth.dev",
    READ_API_COORDINATION_ADVISORIES_ENABLED: "true", READ_API_SESSION_INTEL_ENABLED: "true",
    READ_API_ADVISORY_NOTIFY_ROWS: "duplicate_work:HIGH",
  } as unknown as Env;
  const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as never;
  const overrides = { connect: async () => { throw new Error("no database in this test"); }, fetcher: (async () => new Response("{}", { status: 500 })) as never };
  const status = async (path: string) => (await handle(new Request(`https://read.test.example${path}`), env, ctx, overrides as never)).status;

  test("the advisories GET, an ack path and the workspace view answer 503; other routes are served (401 without a token)", async () => {
    const errors = spyOn(console, "error").mockImplementation(() => {});
    expect(await status("/v1/coordination/advisories?workspace=ws&source=codex&sessionId=s-1")).toBe(503);
    expect(await status("/v1/coordination/advisories/9f3c2a71d04be6e5/ack?workspace=ws")).toBe(503);
    expect(await status("/v1/session-intel/workspace?workspace=ws")).toBe(503);
    expect(errors.mock.calls.some((c) => String(c[0]).includes("missing_READ_API_ADVISORY_NOTIFY_ROWS"))).toBe(true);
    for (const path of ["/v1/sessions?workspace=ws", "/v1/coordination/overlaps?workspace=ws", "/v1/session-intel/workspacex"])
      expect([path, await status(path)]).toEqual([path, 401]);
    // A well-formed list: the advisory routes are served too (401 without a token, not 503).
    const ok = { ...env, READ_API_ADVISORY_NOTIFY_ROWS: "duplicate_work:high" } as Env;
    expect((await handle(new Request("https://read.test.example/v1/coordination/advisories?workspace=ws&source=codex&sessionId=s-1"), ok, ctx, overrides as never)).status).toBe(401);
  });
});
