import { isIP } from "node:net";
import { connect } from "node:net";
import type { KubernetesExecutorInputs } from "./config.ts";

const EXECUTOR_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{2,62}$/;
const APPLY_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isPrivateIpv4(value: string): boolean {
  if (isIP(value) !== 4 || value !== value.trim()) return false;
  const [a, b] = value.split(".").map(Number);
  return a === 10 || (a === 172 && b! >= 16 && b! <= 31) || (a === 192 && b === 168);
}

export interface KubernetesReachabilityProof {
  executor: string;
  mode: KubernetesExecutorInputs["mode"];
  applyId: string;
  proof: "private-control-plane-tcp-443";
}

export type KubernetesEndpointDialer = (host: string, port: number, timeoutMs: number) => Promise<void>;

const dialTcp: KubernetesEndpointDialer = (host, port, timeoutMs) =>
  new Promise<void>((resolve, reject) => {
    const socket = connect({ host, port });
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(() => finish(new Error("deadline exceeded")), timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      finish();
    });
    socket.once("error", () => {
      clearTimeout(timer);
      finish(new Error("connection failed"));
    });
  });

/** Apply-time route proof. It performs no authenticated request and emits no endpoint. */
export async function provePrivateKubernetesReachability(
  endpoint: string,
  executor: KubernetesExecutorInputs,
  dial: KubernetesEndpointDialer = dialTcp,
  timeoutMs = 5000,
): Promise<KubernetesReachabilityProof> {
  if (
    !EXECUTOR_NAME.test(executor.name) ||
    (executor.mode !== "in-vpc" && executor.mode !== "vpn") ||
    !APPLY_ID.test(executor.applyId)
  )
    throw new Error("96bulls: Kubernetes executor identity is invalid. No Kubernetes objects were applied.");
  if (!isPrivateIpv4(endpoint))
    throw new Error("96bulls: GKE control-plane endpoint is not a concrete private IPv4 address. No Kubernetes objects were applied.");
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000)
    throw new Error("96bulls: Kubernetes reachability deadline is invalid. No Kubernetes objects were applied.");
  try {
    await dial(endpoint, 443, timeoutMs);
  } catch {
    throw new Error(
      `96bulls: named ${executor.mode} executor ${executor.name} cannot reach the private GKE control plane. No Kubernetes objects were applied.`,
    );
  }
  return {
    executor: executor.name,
    mode: executor.mode,
    applyId: executor.applyId,
    proof: "private-control-plane-tcp-443",
  };
}
