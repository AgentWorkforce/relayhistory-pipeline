/**
 * Proof inputs. Non-secret only: names, versions, digests, URLs. Secrets
 * (the RelayAuth bootstrap key) come from the environment at run time, piped
 * from 1Password, and are never read from or written to this file.
 *
 * Read from `.96bulls/aws-proof/inputs.json` (gitignored) when present; the
 * Pippen sandbox target reads `.96bulls/aws-proof-sandbox/inputs.json`.
 */
import { existsSync, readFileSync } from "node:fs";
import { LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE, SANDBOX_ACCOUNT_ID, SANDBOX_CLUSTER, SANDBOX_REGION, SANDBOX_STAGE } from "./guard.ts";
import type { Tenant } from "./leak-scan.ts";
import { validateTranscriptConfig, type TranscriptConfig } from "./transcript.ts";
import { PROOF_CHART_REPO, PROOF_HOST, PROOF_KEY_PREFIX_BASE, PROOF_LOCKER_BUCKET, PROOF_ORIGIN, PROOF_TENANT_ORGS, proofConfigIssues, SANDBOX_CONTEXT, SANDBOX_HOST, SANDBOX_LOCKER_BUCKET, SANDBOX_ORIGIN, type HarnessTarget } from "./fence.ts";

export interface ProofConfig {
  stage: string;
  accountId: string;
  region: string;
  clusterName: string;
  kubeContext: string;
  namespace: string;
  release: string;
  chartRepo: string;
  chartVersion: string;
  /** The chart version upgraded to, then rolled back from. */
  candidateChartVersion?: string;
  /** Expected digests by image repository (from the released chart values + versions.lock.json + aws-inputs.json). */
  expectedImages: Record<string, string>;
  coreDeployments: string[];
  hoopdb: { cluster: string; database: string };
  locker: { bucket: string };
  /** The ingest derives `<base>/<org>` key prefixes (PIPELINE_KEY_PREFIX + PIPELINE_ORG_FROM_PREFIX). */
  objectKeyPrefixBase: string;
  /** Bucket holding CNPG barman backups; required for the backup drill. */
  backupBucket?: string;
  relayauth: { namespace: string; service: string; port: number };
  relaycast: { namespace: string; service: string; port: number };
  ingest: { service: string; port: number };
  /**
   * The public, customer-facing ingest: HTTPS :443 only behind the proof ALB.
   * The WAF burst is bounded and runs only with --confirm-live after the host is
   * proven to resolve to an ALB in the proof account.
   */
  ingress: { hostname: string; burst: { maxRequests?: number; concurrency?: number; deadlineMs?: number; path?: string } };
  historyOrigin: string;
  tenants: Tenant[];
  sessions: number;
  /** Sessions per tenant re-ingested for the #38 stale-summary case. */
  staleSessionsPerTenant: number;
  /**
   * How V1 search is reached.
   * `postgrest-jwt` (default, the only gate evidence): the tenant's RelayAuth
   * token is exchanged for a search token (RelayAuth #116) and PostgREST runs
   * the RPC (#73). Blocked until that path is deployed.
   * `rpc-sql`: diagnostic only. psql inside the CNPG primary as the Worker role
   * with harness-set claims; checks that rely on it can never pass.
   * `absent`: search checks are blocked.
   */
  search: { mode: "postgrest-jwt" | "rpc-sql" | "absent" };
  /** The transcript-search gate's V2 backend (ClickHouse or ObsessionDB). "none" leaves the proof INCOMPLETE. */
  transcriptSearch: TranscriptConfig;
  /** Digest-pinned image for in-cluster probes (curl + sh). Required for the Relaycast roundtrip. */
  probeImage?: string;
  /** Deployment runs BOB_COSTAS_MODEL=off, so short_summary text is predictable. */
  extractiveSummaries: boolean;
  pollIntervalMs: number;
  stageDeadlineMs: number;
}

const DIGEST_REF = /@sha256:[0-9a-f]{64}$/;

export function defaultConfig(): ProofConfig {
  return {
    stage: PROOF_STAGE,
    accountId: PROOF_ACCOUNT_ID,
    region: "us-west-2",
    clusterName: PROOF_CLUSTER,
    kubeContext: `arn:aws:eks:us-west-2:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`,
    namespace: "96bulls",
    release: "96bulls",
    chartRepo: PROOF_CHART_REPO,
    chartVersion: "0.3.0",
    expectedImages: {},
    coreDeployments: ["96bulls-alleyoop-ingest", "96bulls-worker-hoopdb-indexer", "96bulls-worker-referee", "96bulls-worker-bob-costas"],
    hoopdb: { cluster: "hoopdb", database: "hoopdb" },
    // stacks/aws/alchemy.aws.run.ts: `96bulls-<stage>-locker-<account>`, versioned, KMS-encrypted.
    locker: { bucket: PROOF_LOCKER_BUCKET },
    objectKeyPrefixBase: PROOF_KEY_PREFIX_BASE,
    relayauth: { namespace: "relayauth", service: "relayauth", port: 8787 },
    relaycast: { namespace: "relaycast", service: "relaycast", port: 8787 },
    ingest: { service: "96bulls-alleyoop", port: 8080 },
    ingress: { hostname: PROOF_HOST, burst: {} },
    historyOrigin: PROOF_ORIGIN,
    // #75 eks.tenantOrgIds; the fence allowlists exactly these.
    tenants: PROOF_TENANT_ORGS.map((orgId) => ({ orgId, workspaceId: "default" })),
    sessions: 100,
    staleSessionsPerTenant: 2,
    search: { mode: "postgrest-jwt" },
    transcriptSearch: { backend: "none", feed: "pipeline", credentials: { tenants: "PROOF_TRANSCRIPT_TENANT_CREDENTIALS", writer: "PROOF_TRANSCRIPT_WRITER" } },
    extractiveSummaries: true,
    pollIntervalMs: 2_000,
    stageDeadlineMs: 300_000,
  };
}

/**
 * The Pippen sandbox of agent aws-proof (account 361835732085): the proof
 * stage's shape with the sandbox's own stage, cluster, context, bucket and host.
 */
export function sandboxConfig(): ProofConfig {
  return {
    ...defaultConfig(),
    stage: SANDBOX_STAGE,
    accountId: SANDBOX_ACCOUNT_ID,
    region: SANDBOX_REGION,
    clusterName: SANDBOX_CLUSTER,
    kubeContext: SANDBOX_CONTEXT,
    locker: { bucket: SANDBOX_LOCKER_BUCKET },
    ingress: { hostname: SANDBOX_HOST, burst: {} },
    historyOrigin: SANDBOX_ORIGIN,
  };
}

/** Default non-secret inputs path per target (gitignored). */
export const defaultInputsPath = (target: HarnessTarget) => (target === "pippen-sandbox" ? `.96bulls/${SANDBOX_STAGE}/inputs.json` : ".96bulls/aws-proof/inputs.json");

/** The local kind cluster, read-only: only the fresh-install inspection runs there. */
export function kindReadOnlyConfig(): ProofConfig {
  return {
    ...defaultConfig(),
    stage: "kind-readonly",
    accountId: "",
    clusterName: LOCAL_READONLY_CONTEXT,
    kubeContext: LOCAL_READONLY_CONTEXT,
    namespace: "96bulls-e2e",
    release: "bulls",
    relayauth: { namespace: "relayauth-e2e", service: "relayauth", port: 8787 },
    relaycast: { namespace: "relaycast-proof", service: "relaycast", port: 8787 },
    coreDeployments: ["bulls-alleyoop-ingest", "bulls-worker-hoopdb-indexer", "bulls-worker-referee", "bulls-worker-bob-costas"],
  };
}

export function validateConfig(c: ProofConfig): string[] {
  const issues: string[] = [];
  if (c.sessions < 2 || !Number.isInteger(c.sessions)) issues.push("sessions must be an integer >= 2");
  if (c.tenants.length < 2) issues.push("at least two tenants are required");
  if (new Set(c.tenants.map((t) => t.orgId)).size !== c.tenants.length) issues.push("tenants need distinct orgs");
  for (const [repo, digest] of Object.entries(c.expectedImages))
    if (!/^sha256:[0-9a-f]{64}$/.test(digest)) issues.push(`expectedImages.${repo} must be sha256:<64 hex>`);
  if (c.probeImage && !DIGEST_REF.test(c.probeImage)) issues.push("probeImage must be digest-pinned (@sha256:...)");
  if (!["postgrest-jwt", "rpc-sql", "absent"].includes(c.search.mode)) issues.push("search.mode must be postgrest-jwt, rpc-sql (diagnostic only) or absent");
  if (!/^https:\/\//.test(c.historyOrigin)) issues.push("historyOrigin must be https");
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(c.ingress.hostname)) issues.push("ingress.hostname must be a DNS name");
  issues.push(...validateTranscriptConfig(c.transcriptSearch));
  // Hard fence: every target is the proof stage's, no URL credentials, nothing production-shaped.
  issues.push(...proofConfigIssues(c as unknown as Parameters<typeof proofConfigIssues>[0]));
  if (c.staleSessionsPerTenant < 1) issues.push("staleSessionsPerTenant must be >= 1");
  return issues;
}

export function loadConfig(path: string | undefined, preset: HarnessTarget): ProofConfig {
  const base = preset === "kind-readonly" ? kindReadOnlyConfig() : preset === "pippen-sandbox" ? sandboxConfig() : defaultConfig();
  if (!path || !existsSync(path)) return base;
  const secretish: string[] = [];
  const raw = JSON.parse(readFileSync(path, "utf8"), (k, v) => {
    if (/secret|password|passwd|token|bootstrap|api[_-]?key|private/i.test(k)) secretish.push(k);
    return v;
  }) as Partial<ProofConfig> & Record<string, unknown>;
  if (secretish.length) throw new Error(`inputs.json must not carry secrets (${secretish.join(", ")}); use the environment`);
  // The guard constants are not configurable: a file cannot widen the target.
  const { accountId: _a, clusterName: _c, stage: _s, ...rest } = raw;
  return { ...base, ...rest } as ProofConfig;
}
