import { describe, expect, test } from "bun:test";
import { provePrivateKubernetesReachability, relaycastProofAnnotations } from "../kubernetes-preflight.ts";
import type { GcpApplyAuthorizationProof } from "../apply-authorization.ts";

describe("private GKE executor preflight", () => {
  const executor = {
    name: "gcp-vpn-runner-01",
    mode: "vpn" as const,
    identity: "gcp-vpn-runner@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com",
  };
  const authorization: GcpApplyAuthorizationProof = {
    proof: "gcp-apply-authorized",
    stage: "gcp-dev",
    projectId: "agentrelay-96bulls-soc2-2610",
    executor: executor.name,
    executorIdentity: executor.identity,
    nonceDigest: "a".repeat(64),
    expiresAt: Math.floor(Date.now() / 1000) + 3600,
    leaseExpiresAt: Math.floor(Date.now() / 1000) + 900,
  };

  test("returns a non-secret proof only after a named private-route executor connects", async () => {
    const calls: unknown[][] = [];
    const proof = await provePrivateKubernetesReachability(
      "10.52.0.2",
      executor,
      authorization,
      async (...args) => {
        calls.push(args);
      },
      2500,
    );
    expect(calls).toEqual([["10.52.0.2", 443, 2500]]);
    expect(proof).toEqual({
      executor: "gcp-vpn-runner-01",
      executorIdentity: executor.identity,
      mode: "vpn",
      stage: authorization.stage,
      projectId: authorization.projectId,
      proof: "private-control-plane-tcp-443",
    });
  });

  test("normalizes the GKE provider's canonical HTTPS endpoint to its private IPv4 host", async () => {
    const calls: unknown[][] = [];
    await provePrivateKubernetesReachability(
      "https://172.20.0.2",
      executor,
      authorization,
      async (...args) => {
        calls.push(args);
      },
      2500,
    );
    expect(calls).toEqual([["172.20.0.2", 443, 2500]]);
  });

  test("rejects public, malformed, whitespace, and unreachable endpoints", async () => {
    const inVpcExecutor = { ...executor, name: "gcp-vpc-runner-01", mode: "in-vpc" as const };
    const inVpcAuthorization = { ...authorization, executor: inVpcExecutor.name };
    for (const endpoint of [
      "34.1.2.3",
      "10.52.0.2 ",
      "http://10.52.0.2",
      "https://10.52.0.2:443",
      "https://10.52.0.2/api",
      "https://user@10.52.0.2",
      "https://34.1.2.3",
      "https://10.52.0.2 ",
      "not-an-ip",
      "127.0.0.1",
    ])
      await expect(provePrivateKubernetesReachability(endpoint, inVpcExecutor, inVpcAuthorization, async () => {})).rejects.toThrow(
        /concrete private IPv4/,
      );
    await expect(
      provePrivateKubernetesReachability("10.52.0.2", inVpcExecutor, inVpcAuthorization, async () => {
        throw new Error("secret route detail");
      }),
    ).rejects.toThrow(/cannot reach.*No Kubernetes objects were applied/);
  });

  test("rejects expired or differently bound apply authorization", async () => {
    await expect(
      provePrivateKubernetesReachability("10.52.0.2", executor, { ...authorization, leaseExpiresAt: 0 }, async () => {}),
    ).rejects.toThrow(/mutation lease expired/);
    await expect(
      provePrivateKubernetesReachability(
        "10.52.0.2",
        executor,
        { ...authorization, executorIdentity: "other@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com" },
        async () => {},
      ),
    ).rejects.toThrow(/executor identity is invalid/);
  });

  test("keeps different valid one-use authorization digests out of persisted pod inputs", async () => {
    const first = await provePrivateKubernetesReachability("10.52.0.2", executor, authorization, async () => {});
    const second = await provePrivateKubernetesReachability(
      "10.52.0.2",
      executor,
      { ...authorization, nonceDigest: "b".repeat(64) },
      async () => {},
    );
    expect(first).toEqual(second);
    expect(relaycastProofAnnotations(first)).toEqual(relaycastProofAnnotations(second));
    expect(JSON.stringify(relaycastProofAnnotations(first))).not.toContain(authorization.nonceDigest);
  });
});
