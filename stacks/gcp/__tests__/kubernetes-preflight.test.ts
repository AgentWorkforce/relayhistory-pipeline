import { describe, expect, test } from "bun:test";
import { provePrivateKubernetesReachability } from "../kubernetes-preflight.ts";

describe("private GKE executor preflight", () => {
  const applyId = "c05b704a-62d1-4d70-a1f9-a98c9e45bb25";

  test("returns a non-secret proof only after a named private-route executor connects", async () => {
    const calls: unknown[][] = [];
    const proof = await provePrivateKubernetesReachability(
      "10.52.0.2",
      { name: "gcp-vpn-runner-01", mode: "vpn", applyId },
      async (...args) => {
        calls.push(args);
      },
      2500,
    );
    expect(calls).toEqual([["10.52.0.2", 443, 2500]]);
    expect(proof).toEqual({
      executor: "gcp-vpn-runner-01",
      mode: "vpn",
      applyId,
      proof: "private-control-plane-tcp-443",
    });
  });

  test("rejects public, malformed, whitespace, and unreachable endpoints", async () => {
    const executor = { name: "gcp-vpc-runner-01", mode: "in-vpc" as const, applyId };
    for (const endpoint of ["34.1.2.3", "10.52.0.2 ", "https://10.52.0.2", "not-an-ip", "127.0.0.1"])
      await expect(provePrivateKubernetesReachability(endpoint, executor, async () => {})).rejects.toThrow(
        /concrete private IPv4/,
      );
    await expect(
      provePrivateKubernetesReachability("10.52.0.2", executor, async () => {
        throw new Error("secret route detail");
      }),
    ).rejects.toThrow(/cannot reach.*No Kubernetes objects were applied/);
  });
});
