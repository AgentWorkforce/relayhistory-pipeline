import { isIP } from "node:net";
import { connect } from "node:net";
import type { KubernetesExecutorInputs } from "./config.ts";
import type { GcpApplyAuthorizationProof } from "./apply-authorization.ts";

const EXECUTOR_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{2,62}$/;

function isPrivateIpv4(value: string): boolean {
  if (isIP(value) !== 4 || value !== value.trim()) return false;
  const [a, b] = value.split(".").map(Number);
  return a === 10 || (a === 172 && b! >= 16 && b! <= 31) || (a === 192 && b === 168);
}

export interface KubernetesReachabilityProof {
  executor: string;
  executorIdentity: string;
  mode: KubernetesExecutorInputs["mode"];
  stage: string;
  projectId: string;
  nonceDigest: string;
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
  authorization: GcpApplyAuthorizationProof,
  dial: KubernetesEndpointDialer = dialTcp,
  timeoutMs = 5000,
): Promise<KubernetesReachabilityProof> {
  if (
    !EXECUTOR_NAME.test(executor.name) ||
    (executor.mode !== "in-vpc" && executor.mode !== "vpn") ||
    authorization.proof !== "gcp-apply-authorized" ||
    authorization.executor !== executor.name ||
    authorization.executorIdentity !== executor.identity ||
    authorization.expiresAt <= Math.floor(Date.now() / 1000)
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
    executorIdentity: executor.identity,
    mode: executor.mode,
    stage: authorization.stage,
    projectId: authorization.projectId,
    nonceDigest: authorization.nonceDigest,
    proof: "private-control-plane-tcp-443",
  };
}
