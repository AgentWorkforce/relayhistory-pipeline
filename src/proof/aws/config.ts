/**
 * Proof inputs. Non-secret only: names, versions, digests, URLs. Secrets
 * (the RelayAuth bootstrap key) come from the environment at run time, piped
 * from 1Password, and are never read from or written to this file.
 *
 * Read from `.96bulls/aws-proof/inputs.json` (gitignored) when present.
 */
import { existsSync, readFileSync } from "node:fs";
import { LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE } from "./guard.ts";
import type { Tenant } from "./leak-scan.ts";
import { validateTranscriptConfig, type TranscriptConfig } from "./transcript.ts";

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
  locker: { bucket: string; prefix: string };
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
   * How V1 search is reached. `rpc-sql`: call sessions.search_session_directory
   * inside the CNPG primary as the Worker role with claims set by the harness
   * (tenant binding is exercised; JWT signatures are not, see #63).
   * `postgrest`: POST /rest/v1/rpc/search_session_directory with a per-tenant
   * JWT supplied by env (#63 issuer). `absent`: search checks are blocked.
   */
  search: { mode: "rpc-sql" | "postgrest" | "absent"; url?: string };
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
    chartRepo: "https://AgentWorkforce.github.io/helm-charts",
    chartVersion: "0.3.0",
    expectedImages: {},
    coreDeployments: ["96bulls-alleyoop-ingest", "96bulls-worker-hoopdb-indexer", "96bulls-worker-referee", "96bulls-worker-bob-costas"],
    hoopdb: { cluster: "hoopdb", database: "hoopdb" },
    // stacks/aws/alchemy.aws.run.ts: `96bulls-<stage>-locker-<account>`, versioned, KMS-encrypted.
    locker: { bucket: `96bulls-${PROOF_STAGE}-locker-${PROOF_ACCOUNT_ID}`, prefix: "" },
    relayauth: { namespace: "relayauth", service: "relayauth", port: 8787 },
    relaycast: { namespace: "relaycast", service: "relaycast", port: 8787 },
    ingest: { service: "96bulls-alleyoop", port: 8080 },
    ingress: { hostname: "sessions-dev.agentrelay.com", burst: {} },
    historyOrigin: "https://aws-proof.invalid",
    tenants: [
      { orgId: "org_proof_a", workspaceId: "default" },
      { orgId: "org_proof_b", workspaceId: "default" },
    ],
    sessions: 100,
    staleSessionsPerTenant: 2,
    search: { mode: "rpc-sql" },
    transcriptSearch: { backend: "none", feed: "harness-synthetic", credentials: { tenants: "PROOF_TRANSCRIPT_TENANT_CREDENTIALS", writer: "PROOF_TRANSCRIPT_WRITER" } },
    extractiveSummaries: true,
    pollIntervalMs: 2_000,
    stageDeadlineMs: 300_000,
  };
}

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
  if (c.search.mode === "postgrest" && !c.search.url) issues.push("search.url is required for postgrest mode");
  if (!/^https:\/\//.test(c.historyOrigin)) issues.push("historyOrigin must be https");
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(c.ingress.hostname)) issues.push("ingress.hostname must be a DNS name");
  issues.push(...validateTranscriptConfig(c.transcriptSearch));
  if (c.staleSessionsPerTenant < 1) issues.push("staleSessionsPerTenant must be >= 1");
  return issues;
}

export function loadConfig(path: string | undefined, preset: "proof" | "kind-readonly"): ProofConfig {
  const base = preset === "kind-readonly" ? kindReadOnlyConfig() : defaultConfig();
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
