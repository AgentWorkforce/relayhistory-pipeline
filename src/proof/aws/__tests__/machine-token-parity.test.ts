import { describe, expect, test } from "bun:test";
import { newCtx, tokensFor } from "../checks.ts";
import { defaultConfig } from "../config.ts";
import { Runner } from "../exec.ts";
import { PROOF_CONTEXT, PROOF_FENCE, PROOF_TENANT_ORGS } from "../fence.ts";
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";

function context() {
  const runner = new Runner({
    mode: "live",
    identity: {
      confirmLive: true,
      accountId: PROOF_ACCOUNT_ID,
      clusterName: PROOF_CLUSTER,
      kubeContext: PROOF_CONTEXT,
      kubeContextCluster: PROOF_CONTEXT,
      clusterBound: true,
    },
    fence: PROOF_FENCE,
    print: () => {},
  });
  return newCtx(
    { ...defaultConfig(), sessions: 4 },
    runner,
    "aws-proof-machine-token-0001",
    {
      PROOF_RELAYAUTH_BOOTSTRAP_KEY: "synthetic-proof-key",
      PROOF_SPONSOR_ID: "proof-operator",
    },
  );
}

describe("machine-bound proof assertions", () => {
  test("mints one assertion per machine and never reuses a tenant token for another machine", async () => {
    const ctx = context();
    const tenantA = ctx.corpus.find(
      (p) => p.tenant.orgId === PROOF_TENANT_ORGS[0],
    )!;
    ctx.corpus[2] = {
      ...ctx.corpus[2]!,
      machineId: `${tenantA.machineId}-second`,
    };
    const bodies: Array<Record<string, unknown>> = [];
    const result = await tokensFor(
      ctx,
      "http://127.0.0.1:18787",
      async (_url, init) => {
        const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
        bodies.push(body);
        return Response.json(
          { accessToken: `token-for-${String(body.machineId)}` },
          { status: 201 },
        );
      },
    );
    expect(result).toBeUndefined();
    expect(bodies.map((body) => body.machineId).sort()).toEqual(
      [...new Set(ctx.corpus.map((p) => p.machineId))].sort(),
    );
    expect(ctx.machineTokens.size).toBe(3);
    expect(ctx.tokens.size).toBe(2);
    for (const [machineId, token] of ctx.machineTokens)
      expect(token).toBe(`token-for-${machineId}`);
  });

  test("refuses one machine id assigned to two tenants before minting", async () => {
    const ctx = context();
    ctx.corpus[1] = {
      ...ctx.corpus[1]!,
      machineId: ctx.corpus[0]!.machineId,
    };
    let calls = 0;
    const result = await tokensFor(ctx, "http://127.0.0.1:18787", async () => {
      calls += 1;
      return Response.json({ accessToken: "unreachable" }, { status: 201 });
    });
    expect(result).toMatch(/two tenants/);
    expect(calls).toBe(0);
    expect(ctx.machineTokens.size).toBe(0);
  });
});
