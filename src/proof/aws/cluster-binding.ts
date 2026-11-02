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
import { readFileSync } from "node:fs";
import { isAbsolute } from "node:path";
import { PIPPEN_SANDBOX_AGENT, PIPPEN_SESSION_ID, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_REGION, SESSIONS_DEV_PROOF_TARGET, type ProofTarget } from "./guard.ts";

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
export function proofKubeconfig(d: DescribedCluster, contextName: string, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET, creds: ToolCredentials = STATIC_CREDENTIALS): string {
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
              env:
                creds.kind === "pippen-profile"
                  ? [
                      // `pippen sandbox exec`'s credential_process profile, so every get-token refreshes through it.
                      { name: "AWS_CONFIG_FILE", value: creds.configFile },
                      { name: "AWS_PROFILE", value: PIPPEN_EXEC_PROFILE },
                      { name: "AWS_SHARED_CREDENTIALS_FILE", value: "/dev/null" },
                      { name: "AWS_STS_REGIONAL_ENDPOINTS", value: "regional" },
                      { name: "AWS_REGION", value: t.region },
                    ]
                  : [
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
 *
 * The one exception is the Pippen sandbox target run under `pippen sandbox
 * exec` (resolveToolCredentials, kind "pippen-profile"): there AWS_CONFIG_FILE
 * is the exec's private, validated config (only `[profile pippen-sandbox]`
 * with `region` and the `pippen sandbox credentials-process` line), with
 * AWS_PROFILE=pippen-sandbox and AWS_SDK_LOAD_CONFIG=1, so aws, kubectl's
 * `aws eks get-token` and helm refresh credentials through it. Static keys are
 * never copied in that mode, and every other pin stays.
 */
export const TOOL_ENV_PASSTHROUGH = ["PATH", "HOME", "LANG", "TMPDIR"] as const;
export const AWS_CREDENTIAL_VARS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"] as const;

export function toolEnv(base: Record<string, string | undefined>, kubeconfigPath: string | undefined, creds: ToolCredentials = STATIC_CREDENTIALS): Record<string, string> {
  const env: Record<string, string> = {};
  for (const k of creds.kind === "pippen-profile" ? TOOL_ENV_PASSTHROUGH : [...TOOL_ENV_PASSTHROUGH, ...AWS_CREDENTIAL_VARS]) {
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
  if (creds.kind === "pippen-profile") Object.assign(env, { AWS_CONFIG_FILE: creds.configFile, AWS_PROFILE: PIPPEN_EXEC_PROFILE, AWS_SDK_LOAD_CONFIG: "1" });
  if (kubeconfigPath) env.KUBECONFIG = kubeconfigPath;
  return env;
}

/** Live runs need explicit credentials: the minimal env deliberately ignores profiles and config files. */
export function credentialIssue(base: Record<string, string | undefined>): string | undefined {
  if (!base.AWS_ACCESS_KEY_ID || !base.AWS_SECRET_ACCESS_KEY)
    return "AWS credentials must be explicit env vars (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY[/AWS_SESSION_TOKEN]), e.g. `eval \"$(aws configure export-credentials --format env)\"`; profiles and config files are ignored";
  return undefined;
}

/**
 * Where a live run's AWS credentials come from.
 *   - "static": explicit AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY[/AWS_SESSION_TOKEN]
 *     (credentialIssue). The only kind the proof (sessions-dev) target accepts.
 *   - "pippen-profile": the Pippen sandbox target only, run under
 *     `pippen sandbox exec aws-proof -- ...` (pippen-tools 327c263, default
 *     refreshing mode): a private AWS config holding only
 *     `[profile pippen-sandbox]` with `region` and a `credential_process` that
 *     runs `pippen sandbox credentials-process aws-proof`.
 */
export type ToolCredentials = { readonly kind: "static" } | { readonly kind: "pippen-profile"; readonly configFile: string };
export const STATIC_CREDENTIALS: ToolCredentials = Object.freeze({ kind: "static" });
/** The profile name `pippen sandbox exec` writes and exports as AWS_PROFILE. */
export const PIPPEN_EXEC_PROFILE = "pippen-sandbox";
/** The only keys the exec's profile may carry: anything else (sso_*, role_arn, source_profile, endpoint_url, ca_bundle, ...) is refused. */
const PIPPEN_PROFILE_KEYS = new Set(["region", "credential_process"]);
const CREDENTIALS_PROCESS = new RegExp(`(^|[\\s"'])sandbox["']?\\s+["']?credentials-process["']?\\s+["']?${PIPPEN_SANDBOX_AGENT}["']?\\s+["']?--client-env(["'\\s]|$)`);

/**
 * Validate `pippen sandbox exec`'s AWS config: exactly one section,
 * `[profile pippen-sandbox]`, whose keys are only `region` and a
 * `credential_process` running `pippen sandbox credentials-process aws-proof`.
 * The file is secret-free by pippen's contract; it is parsed in memory and
 * nothing from it is logged.
 */
export function pippenProfileConfigIssue(text: string): string | undefined {
  const sections: string[] = [];
  const keys = new Map<string, string>();
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (line === "" || line.startsWith("#") || line.startsWith(";")) continue;
    const sec = /^\[\s*([^\]]*?)\s*\]$/.exec(line);
    if (sec) {
      sections.push(sec[1]!.replace(/\s+/g, " "));
      if (sections.length > 1) break;
      continue;
    }
    if (sections.length === 0) return "AWS config has settings outside a section";
    const kv = /^([A-Za-z0-9_.-]+)\s*=\s*(.*)$/.exec(line);
    if (!kv) return "AWS config has a line that is not key = value (nested settings are not allowed)";
    const key = kv[1]!.toLowerCase();
    if (keys.has(key)) return `AWS config repeats ${key}`;
    keys.set(key, kv[2]!);
  }
  if (sections.length !== 1 || sections[0] !== `profile ${PIPPEN_EXEC_PROFILE}`) return `AWS config must hold exactly one section, [profile ${PIPPEN_EXEC_PROFILE}]`;
  for (const k of keys.keys()) {
    if (k.startsWith("sso_")) return "AWS config profile is an SSO profile; refusing";
    if (!PIPPEN_PROFILE_KEYS.has(k)) return `AWS config profile sets ${k}; only region and credential_process are allowed`;
  }
  const cp = keys.get("credential_process");
  if (!cp || !CREDENTIALS_PROCESS.test(cp)) return `AWS config credential_process is not \`pippen sandbox credentials-process ${PIPPEN_SANDBOX_AGENT}\``;
  return undefined;
}

const set = (v: string | undefined) => v !== undefined && v !== "";

/**
 * The credentials a live run against target `t` may use, or why it is refused.
 *
 * The proof (sessions-dev) target: exactly credentialIssue, static keys only.
 *
 * The Pippen sandbox target additionally accepts `pippen sandbox exec`'s
 * refreshing profile: AWS_PROFILE=pippen-sandbox, AWS_CONFIG_FILE an absolute
 * path to a config that passes pippenProfileConfigIssue,
 * AWS_SHARED_CREDENTIALS_FILE=/dev/null, no AWS_DEFAULT_PROFILE and no static
 * keys. Refused: any other (ambient, default, saved or SSO) profile, static keys
 * mixed with a profile, and a PIPPEN_SANDBOX_SESSION_ID / PIPPEN_SANDBOX_ACCOUNT_ID
 * that names another lease or account than `--pippen-session` and the target.
 * Either kind still has to pass the caller-ARN pin (observeIdentity, guard.ts).
 */
export function resolveToolCredentials(
  base: Record<string, string | undefined>,
  t: ProofTarget,
  o: { pippenSession?: string; readFile?: (path: string) => string } = {},
): ToolCredentials | { error: string } {
  if (t.callerRolePrefix === undefined) {
    const issue = credentialIssue(base);
    return issue ? { error: issue } : STATIC_CREDENTIALS;
  }
  const envSession = base.PIPPEN_SANDBOX_SESSION_ID;
  if (envSession !== undefined) {
    if (!PIPPEN_SESSION_ID.test(envSession)) return { error: "PIPPEN_SANDBOX_SESSION_ID is not a pippen lease id" };
    if (o.pippenSession !== undefined && envSession !== o.pippenSession) return { error: "PIPPEN_SANDBOX_SESSION_ID (from `pippen sandbox exec`) is not --pippen-session" };
  }
  if (base.PIPPEN_SANDBOX_ACCOUNT_ID !== undefined && base.PIPPEN_SANDBOX_ACCOUNT_ID !== t.accountId) return { error: "PIPPEN_SANDBOX_ACCOUNT_ID is not the sandbox account" };
  const hasStatic = AWS_CREDENTIAL_VARS.some((k) => set(base[k]));
  const hasProfile = set(base.AWS_PROFILE) || set(base.AWS_DEFAULT_PROFILE);
  if (hasStatic && hasProfile) return { error: "AWS credentials are both static keys and a profile; give exactly one (`pippen sandbox exec` sets only the profile)" };
  if (!hasProfile) {
    const issue = credentialIssue(base);
    return issue ? { error: `${issue}; or run under \`pippen sandbox exec ${PIPPEN_SANDBOX_AGENT} -- ...\`` } : STATIC_CREDENTIALS;
  }
  if (set(base.AWS_DEFAULT_PROFILE)) return { error: "AWS_DEFAULT_PROFILE is set; only `pippen sandbox exec`'s AWS_PROFILE=pippen-sandbox is accepted" };
  if (base.AWS_PROFILE !== PIPPEN_EXEC_PROFILE) return { error: `AWS_PROFILE is not ${PIPPEN_EXEC_PROFILE}; ambient and saved profiles are refused (use \`pippen sandbox exec ${PIPPEN_SANDBOX_AGENT} -- ...\`)` };
  if (base.AWS_SHARED_CREDENTIALS_FILE !== "/dev/null") return { error: "AWS_SHARED_CREDENTIALS_FILE must be /dev/null under `pippen sandbox exec`" };
  const configFile = base.AWS_CONFIG_FILE;
  if (!configFile || configFile === "/dev/null" || !isAbsolute(configFile)) return { error: "AWS_CONFIG_FILE must be the absolute path `pippen sandbox exec` wrote" };
  let text: string;
  try {
    text = (o.readFile ?? ((p: string) => readFileSync(p, "utf8")))(configFile);
  } catch {
    return { error: "AWS_CONFIG_FILE unreadable" };
  }
  const bad = pippenProfileConfigIssue(text);
  return bad ? { error: bad } : Object.freeze({ kind: "pippen-profile", configFile });
}
