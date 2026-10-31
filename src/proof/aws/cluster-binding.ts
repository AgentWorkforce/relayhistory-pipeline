/**
 * Binds the harness to the live proof cluster as AWS reports it, never as a
 * kubeconfig names it.
 *
 *   1. `aws eks describe-cluster --name 96bulls-aws-proof --region us-west-2`
 *      (the caller already verified as account 500268966557) gives the cluster
 *      ARN, API endpoint and certificate authority.
 *   2. The operator's kubeconfig context must point at exactly that endpoint
 *      with exactly that CA, with no TLS override; otherwise the run fails
 *      closed. This catches a context that was renamed onto another server.
 *   3. Every kubectl/helm call then uses a kubeconfig the harness generates
 *      from the describe result (one cluster, one context, credentials from
 *      `aws eks get-token`), through KUBECONFIG. The operator's kubeconfig is
 *      never consulted by a step.
 */
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_REGION } from "./guard.ts";

export const PROOF_CLUSTER_ARN = `arn:aws:eks:${PROOF_REGION}:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;

export interface DescribedCluster {
  arn: string;
  endpoint: string;
  caData: string;
  createdAtMs: number;
}

const ENDPOINT = /^https:\/\/[A-Z0-9]+\.[a-z0-9]+\.us-west-2\.eks\.amazonaws\.com$/;
const BASE64 = /^[A-Za-z0-9+/]+={0,2}$/;

export function parseDescribeCluster(json: string): DescribedCluster | { error: string } {
  let c: { cluster?: { arn?: string; name?: string; endpoint?: string; status?: string; createdAt?: string; certificateAuthority?: { data?: string } } };
  try {
    c = JSON.parse(json);
  } catch {
    return { error: "describe-cluster output unreadable" };
  }
  const k = c.cluster;
  if (!k) return { error: "describe-cluster returned no cluster" };
  if (k.arn !== PROOF_CLUSTER_ARN || k.name !== PROOF_CLUSTER) return { error: `described cluster is not ${PROOF_CLUSTER_ARN}` };
  if (k.status !== "ACTIVE") return { error: `cluster status is ${k.status ?? "unknown"}, not ACTIVE` };
  if (!k.endpoint || !ENDPOINT.test(k.endpoint)) return { error: "cluster endpoint is not an EKS endpoint in us-west-2" };
  const ca = k.certificateAuthority?.data;
  if (!ca || !BASE64.test(ca)) return { error: "cluster certificate authority missing" };
  const createdAtMs = Date.parse(k.createdAt ?? "");
  if (Number.isNaN(createdAtMs)) return { error: "cluster createdAt missing" };
  return { arn: k.arn, endpoint: k.endpoint, caData: ca, createdAtMs };
}

interface KubeconfigView {
  contexts?: Array<{ name: string; context?: { cluster?: string } }>;
  clusters?: Array<{ name: string; cluster?: Record<string, unknown> }>;
}

/**
 * The operator's kubeconfig context must reach exactly the described server
 * with exactly its CA. `kubectl config view --raw -o json` is parsed in memory
 * only; nothing from it is logged or stored.
 */
export function bindKubeconfig(viewJson: string, contextName: string, d: DescribedCluster): { ok: true } | { error: string } {
  let v: KubeconfigView;
  try {
    v = JSON.parse(viewJson);
  } catch {
    return { error: "kubeconfig unreadable" };
  }
  const ctxs = (v.contexts ?? []).filter((c) => c.name === contextName);
  if (ctxs.length !== 1) return { error: `kube context ${contextName} ${ctxs.length ? "is ambiguous" : "is missing"}` };
  const clusterName = ctxs[0]!.context?.cluster;
  const cls = (v.clusters ?? []).filter((c) => c.name === clusterName);
  if (cls.length !== 1) return { error: "kube context's cluster entry missing or ambiguous" };
  const c = cls[0]!.cluster ?? {};
  if (c["insecure-skip-tls-verify"] || c["tls-server-name"] || c["proxy-url"] || c["certificate-authority"]) return { error: "kubeconfig cluster entry overrides TLS or proxies; refusing" };
  if (c.server !== d.endpoint) return { error: "kubeconfig server does not equal the described cluster endpoint" };
  if (c["certificate-authority-data"] !== d.caData) return { error: "kubeconfig certificate-authority-data does not equal the described cluster CA" };
  return { ok: true };
}

/** The only kubeconfig any step uses: built from the describe result. Holds no secret (the CA is public; tokens come from the exec plugin). */
export function proofKubeconfig(d: DescribedCluster, contextName: string): string {
  return JSON.stringify(
    {
      apiVersion: "v1",
      kind: "Config",
      clusters: [{ name: d.arn, cluster: { server: d.endpoint, "certificate-authority-data": d.caData } }],
      users: [
        {
          name: d.arn,
          user: {
            exec: {
              apiVersion: "client.authentication.k8s.io/v1beta1",
              command: "aws",
              args: ["eks", "get-token", "--cluster-name", PROOF_CLUSTER, "--region", PROOF_REGION, "--output", "json"],
              interactiveMode: "Never",
            },
          },
        },
      ],
      contexts: [{ name: contextName, context: { cluster: d.arn, user: d.arn } }],
      "current-context": contextName,
      preferences: {},
    },
    null,
    2,
  );
}

/** Environment for every spawned tool: the generated kubeconfig, and nothing that retargets helm or the AWS CLI. */
export function toolEnv(base: Record<string, string | undefined>, kubeconfigPath: string | undefined): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(base)) {
    if (v === undefined) continue;
    if (k === "KUBECONFIG" || k.startsWith("HELM_KUBE") || k === "HELM_KUBECONTEXT" || k.startsWith("AWS_ENDPOINT_URL") || k === "AWS_CA_BUNDLE" || k === "KUBECTL_EXTERNAL_DIFF") continue;
    env[k] = v;
  }
  if (kubeconfigPath) env.KUBECONFIG = kubeconfigPath;
  return env;
}
