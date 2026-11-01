/**
 * Binds the harness to the live proof cluster as AWS reports it, never as a
 * kubeconfig names it.
 *
 *   1. `aws eks describe-cluster --name <target cluster> --region us-west-2`
 *      (the caller already verified as the target's account: 500268966557 for
 *      the proof stage, 361835732085 + the agent-session role for the Pippen
 *      sandbox) gives the cluster ARN, API endpoint and certificate authority.
 *   2. The operator's kubeconfig context must point at exactly that endpoint
 *      with exactly that CA, with no TLS override; otherwise the run fails
 *      closed. This catches a context that was renamed onto another server.
 *   3. Every kubectl/helm call then uses a kubeconfig the harness generates
 *      from the describe result (one cluster, one context, credentials from
 *      `aws eks get-token`), through KUBECONFIG. The operator's kubeconfig is
 *      never consulted by a step.
 */
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_REGION, SESSIONS_DEV_PROOF_TARGET, type ProofTarget } from "./guard.ts";

export const PROOF_CLUSTER_ARN = `arn:aws:eks:${PROOF_REGION}:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
/** The EKS cluster ARN of a hardcoded proof target. */
export const clusterArnOf = (t: ProofTarget) => `arn:aws:eks:${t.region}:${t.accountId}:cluster/${t.clusterName}`;

export interface DescribedCluster {
  arn: string;
  endpoint: string;
  caData: string;
  createdAtMs: number;
}

const ENDPOINT = /^https:\/\/[A-Z0-9]+\.[a-z0-9]+\.us-west-2\.eks\.amazonaws\.com$/;
const BASE64 = /^[A-Za-z0-9+/]+={0,2}$/;

export function parseDescribeCluster(json: string, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): DescribedCluster | { error: string } {
  const wantArn = clusterArnOf(t);
  let c: { cluster?: { arn?: string; name?: string; endpoint?: string; status?: string; createdAt?: string; certificateAuthority?: { data?: string } } };
  try {
    c = JSON.parse(json);
  } catch {
    return { error: "describe-cluster output unreadable" };
  }
  const k = c.cluster;
  if (!k) return { error: "describe-cluster returned no cluster" };
  if (k.arn !== wantArn || k.name !== t.clusterName) return { error: `described cluster is not ${wantArn}` };
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
export function proofKubeconfig(d: DescribedCluster, contextName: string, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): string {
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
              args: ["eks", "get-token", "--cluster-name", t.clusterName, "--region", t.region, "--output", "json"],
              // Inherits the minimal tool env; these repeat the pins in case a kubectl ever adds its own.
              env: [
                { name: "AWS_CONFIG_FILE", value: "/dev/null" },
                { name: "AWS_SHARED_CREDENTIALS_FILE", value: "/dev/null" },
                { name: "AWS_STS_REGIONAL_ENDPOINTS", value: "regional" },
                { name: "AWS_REGION", value: t.region },
              ],
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

/**
 * The complete environment of every spawned aws/kubectl/helm (and the
 * kubeconfig's exec plugin, which inherits it). Built from scratch, never
 * filtered from the operator's environment, so nothing can retarget a call:
 *
 *   - AWS credentials only as explicit env vars (AWS_ACCESS_KEY_ID,
 *     AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN), copied by name;
 *   - AWS_CONFIG_FILE and AWS_SHARED_CREDENTIALS_FILE are /dev/null, so no
 *     profile, endpoint_url, ca_bundle, credential_process, role chain or
 *     sso session from a config file applies; no AWS_PROFILE;
 *   - region pinned; regional STS; no FIPS/dual-stack; no instance metadata;
 *     no endpoint, CA-bundle or proxy variables at all;
 *   - KUBECONFIG is the generated file (or absent: kubectl/helm are refused);
 *   - only PATH, HOME, LANG, TMPDIR pass through for the tools to start.
 */
export const TOOL_ENV_PASSTHROUGH = ["PATH", "HOME", "LANG", "TMPDIR"] as const;
export const AWS_CREDENTIAL_VARS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"] as const;

export function toolEnv(base: Record<string, string | undefined>, kubeconfigPath: string | undefined): Record<string, string> {
  const env: Record<string, string> = {};
  for (const k of [...TOOL_ENV_PASSTHROUGH, ...AWS_CREDENTIAL_VARS]) {
    const v = base[k];
    if (v !== undefined && v !== "") env[k] = v;
  }
  Object.assign(env, {
    AWS_CONFIG_FILE: "/dev/null",
    AWS_SHARED_CREDENTIALS_FILE: "/dev/null",
    AWS_REGION: PROOF_REGION,
    AWS_DEFAULT_REGION: PROOF_REGION,
    AWS_STS_REGIONAL_ENDPOINTS: "regional",
    AWS_USE_FIPS_ENDPOINT: "false",
    AWS_USE_DUALSTACK_ENDPOINT: "false",
    AWS_EC2_METADATA_DISABLED: "true",
    AWS_PAGER: "",
  });
  if (kubeconfigPath) env.KUBECONFIG = kubeconfigPath;
  return env;
}

/** Live runs need explicit credentials: the minimal env deliberately ignores profiles and config files. */
export function credentialIssue(base: Record<string, string | undefined>): string | undefined {
  if (!base.AWS_ACCESS_KEY_ID || !base.AWS_SECRET_ACCESS_KEY)
    return "AWS credentials must be explicit env vars (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY[/AWS_SESSION_TOKEN]), e.g. `eval \"$(aws configure export-credentials --format env)\"`; profiles and config files are ignored";
  return undefined;
}
