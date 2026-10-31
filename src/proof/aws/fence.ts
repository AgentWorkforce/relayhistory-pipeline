/**
 * The target fence. The guard (guard.ts) proves *who* we are and *which
 * cluster* we talk to; the fence proves *what* each step touches.
 *
 * Every allowed value is HARDCODED here for the aws-proof stage. Nothing in the
 * fence is derived from inputs.json: config may only restate these values
 * (proofConfigIssues refuses any difference), and every step's argv is checked
 * against the constants in every mode, dry-run included (ops.ts admitStep:
 * every command is parsed into a typed operation and must match an explicit
 * allowlist; mutations need an exact (verb, kind, namespace, name) entry).
 * Changing a target means a reviewed code change to this file.
 *
 * Production-shaped names are refused everywhere, even inside the proof account.
 */
import { LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE } from "./guard.ts";

export { PROOF_REGION } from "./guard.ts";
import { PROOF_REGION } from "./guard.ts";
export const PROOF_CONTEXT = `arn:aws:eks:${PROOF_REGION}:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
/** The proof stage's public ingest host and RelayHistory origin. Never sessions-dev or a production host. */
export const PROOF_HOST = "proof-sessions.agentrelay.com";
export const PROOF_ORIGIN = `https://${PROOF_HOST}`;
/** stacks/aws/alchemy.aws.run.ts `name()`: 96bulls-<stage>-<base>[-<account>]. */
export const PROOF_NAME_PREFIX = `96bulls-${PROOF_STAGE}-`;
export const PROOF_LOCKER_BUCKET = `${PROOF_NAME_PREFIX}locker-${PROOF_ACCOUNT_ID}`;
export const PROOF_BACKUP_BUCKET = `${PROOF_NAME_PREFIX}hoopdb-backups-${PROOF_ACCOUNT_ID}`;
export const PROOF_RELEASE = "96bulls";
export const PROOF_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";
export const PROOF_CHART_HOST = "agentworkforce.github.io";

/** Namespaces and their services, fixed by stacks/aws/eks.ts and relayauth.ts. */
export const PROOF_NAMESPACE = "96bulls";
export const PROOF_RELAYAUTH = { namespace: "relayauth", service: "relayauth" } as const;
export const PROOF_RELAYCAST = { namespace: "relaycast", service: "relaycast" } as const;
export const PROOF_CNPG_NAMESPACE = "cnpg-system";
export const PROOF_INGEST_SERVICE = "96bulls-alleyoop";
export const PROOF_CORE_DEPLOYMENTS = ["96bulls-alleyoop-ingest", "96bulls-worker-hoopdb-indexer", "96bulls-worker-referee", "96bulls-worker-bob-costas"] as const;
export const PROOF_HOOPDB = { cluster: "hoopdb", database: "hoopdb" } as const;
export const PROOF_RESTORE_CLUSTER = "hoopdb-restore-proof";
/**
 * The self-hosted PostgREST of #73's profile (session directory search over
 * HTTP). Not deployed in Stage A (sessionDirectory.enabled=false), so the
 * metadata search gate is BLOCKED until it is; the service name is fixed here.
 */
export const PROOF_POSTGREST = { namespace: "96bulls", service: "session-directory-postgrest", port: 3000, clientLabel: ["96bulls.agentrelay.com/search-client", "true"] } as const;
/** The in-cluster search client pod: carries both caller labels, so NetworkPolicy admits it to RelayAuth TLS and PostgREST. */
export const PROOF_SEARCH_CLIENT_POD = "proof-search-client";
/**
 * The only images the in-cluster probe pod may run: the Stage A services image
 * (versions.lock.json images.services-aws.approved on #75, publish-image-ecr
 * run 41154325003, source 2fb88cd). It ships bun, which the probe needs.
 */
export const PROOF_PROBE_IMAGES: readonly string[] = [
  "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services@sha256:59d9468551ded11f3c6f5d36558ab7ef87c3e50e99772de352cc309823cb00e0",
];
/** The proof stage's tenants (#75 eks.tenantOrgIds). Any other org must get 403 org_not_allowed at ingest. */
export const PROOF_TENANT_ORGS = ["org_aws_proof_1", "org_aws_proof_2"] as const;
/** A deliberately unlisted org for the negative ingest check. */
export const PROOF_UNLISTED_ORG = "org_aws_proof_unlisted";
/** Object keys land under `96bulls/<org>/v1/...` (#75). */
export const PROOF_KEY_PREFIX_BASE = "96bulls";
/**
 * RelayAuth's TLS Service (#75). The search-token exchange must go here, from a
 * caller pod labelled relayauth.agentrelay.com/client=tls that trusts the CA in
 * ConfigMap relayauth-ca. Never the plain relayauth:8787.
 */
export const PROOF_RELAYAUTH_TLS = { namespace: "relayauth", service: "relayauth-tls", port: 8443, caConfigMap: "relayauth-ca", clientLabel: ["relayauth.agentrelay.com/client", "tls"] } as const;
/** Namespaces that hold objects the proof creates (and teardown may delete). */
export const PROOF_WRITABLE_NAMESPACES = [PROOF_NAMESPACE, PROOF_RELAYCAST.namespace] as const;

/**
 * The transcript backend. Empty until the synthetic ClickHouse Cloud service is
 * provisioned and its exact host is added here in a reviewed commit; until then
 * every configured transcript backend is refused and the proof stays INCOMPLETE.
 */
export const PROOF_TRANSCRIPT_HOSTS: readonly string[] = [];
export const PROOF_TRANSCRIPT_TABLE = { database: "proof", table: "transcripts" } as const;

/** Production- or shared-shaped tokens. Matched as whole segments, case-insensitive. */
export const FORBIDDEN_NAME = /(^|[^a-z0-9])(prod|production|sessions-prod|sessions-dev|staging|hosted)([^a-z0-9]|$)/i;
/** Any URL carrying userinfo (`scheme://user[:pass]@host`). */
export const URL_USERINFO = /\b[a-z][a-z0-9+.-]*:\/\/[^\s/?#@]+@/i;

export interface Fence {
  /** Which hardcoded target this is. The kind fence admits reads only. */
  target: "proof" | "kind-readonly";
  context: string;
  region?: string;
  namespaces: ReadonlySet<string>;
  /** Namespaces a delete may name. Deletes without an explicit one of these are refused. */
  deleteNamespaces: ReadonlySet<string>;
  buckets: ReadonlySet<string>;
  deployments: ReadonlySet<string>;
  /** Services reachable by port-forward or in-cluster DNS. */
  services: ReadonlySet<string>;
  /** CNPG Cluster names (`clusters.postgresql.cnpg.io <name>`). */
  pgClusters: ReadonlySet<string>;
  /** psql `-d` databases. */
  databases: ReadonlySet<string>;
  releases: ReadonlySet<string>;
  hosts: ReadonlySet<string>;
  /** Account every ARN must carry; undefined for the kind fence (no ARN is allowed at all). */
  accountId?: string;
}

export const PROOF_FENCE: Fence = Object.freeze({
  target: "proof",
  context: PROOF_CONTEXT,
  region: PROOF_REGION,
  namespaces: new Set([PROOF_NAMESPACE, PROOF_RELAYAUTH.namespace, PROOF_RELAYCAST.namespace, PROOF_CNPG_NAMESPACE]),
  deleteNamespaces: new Set(PROOF_WRITABLE_NAMESPACES),
  buckets: new Set([PROOF_LOCKER_BUCKET, PROOF_BACKUP_BUCKET]),
  deployments: new Set([...PROOF_CORE_DEPLOYMENTS, PROOF_RELAYAUTH.service, PROOF_RELAYCAST.service]),
  services: new Set([PROOF_INGEST_SERVICE, PROOF_POSTGREST.service, PROOF_RELAYAUTH.service, PROOF_RELAYCAST.service]),
  pgClusters: new Set([PROOF_HOOPDB.cluster, PROOF_RESTORE_CLUSTER]),
  databases: new Set([PROOF_HOOPDB.database]),
  releases: new Set([PROOF_RELEASE]),
  hosts: new Set(["127.0.0.1", PROOF_HOST, PROOF_CHART_HOST, ...PROOF_TRANSCRIPT_HOSTS]),
  accountId: PROOF_ACCOUNT_ID,
});

/** The local kind cluster: reads only (the guard refuses every mutation), and no AWS at all. */
export const KIND_FENCE: Fence = Object.freeze({
  target: "kind-readonly",
  context: LOCAL_READONLY_CONTEXT,
  namespaces: new Set(["96bulls-e2e", "relayauth-e2e", "relaycast-proof", "cnpg-system"]),
  deleteNamespaces: new Set<string>(),
  buckets: new Set<string>(),
  deployments: new Set(["bulls-alleyoop-ingest", "bulls-worker-hoopdb-indexer", "bulls-worker-referee", "bulls-worker-bob-costas", "relayauth", "relaycast"]),
  services: new Set(["bulls-alleyoop", "relayauth", "relaycast"]),
  pgClusters: new Set(["hoopdb"]),
  databases: new Set(["hoopdb"]),
  releases: new Set(["bulls"]),
  hosts: new Set(["127.0.0.1", PROOF_CHART_HOST]),
});

export const fenceFor = (target: "proof" | "kind-readonly"): Fence => (target === "kind-readonly" ? KIND_FENCE : PROOF_FENCE);

interface FenceSource {
  stage: string;
  kubeContext: string;
  region: string;
  namespace: string;
  release: string;
  chartRepo: string;
  coreDeployments: string[];
  hoopdb: { cluster: string; database: string };
  locker: { bucket: string };
  backupBucket?: string;
  relayauth: { namespace: string; service: string };
  relaycast: { namespace: string; service: string };
  ingest: { service: string };
  ingress: { hostname: string };
  historyOrigin: string;
  transcriptSearch: { backend: string; endpoint?: string; database?: string; table?: string };
  probeImage?: string;
}

/** Every string anywhere in a value, with its path. */
function strings(v: unknown, path = ""): Array<[string, string]> {
  if (typeof v === "string") return [[path, v]];
  if (Array.isArray(v)) return v.flatMap((x, i) => strings(x, `${path}[${i}]`));
  if (v && typeof v === "object") return Object.entries(v).flatMap(([k, x]) => strings(x, path ? `${path}.${k}` : k));
  return [];
}

const hostOf = (u: string | undefined) => {
  try {
    return u ? new URL(u).hostname.toLowerCase() : undefined;
  } catch {
    return undefined;
  }
};

/**
 * Config-level fence. For the proof stage every target field must equal its
 * hardcoded constant; config cannot introduce a target. Empty array = ok.
 */
export function proofConfigIssues(c: FenceSource & Record<string, unknown>): string[] {
  const issues: string[] = [];
  for (const [path, v] of strings(c)) {
    if (FORBIDDEN_NAME.test(v)) issues.push(`${path} is production- or shared-shaped`);
    if (URL_USERINFO.test(v)) issues.push(`${path} carries URL credentials (userinfo); credentials come from the environment only`);
  }
  if (c.stage !== PROOF_STAGE) return issues;
  const want = (path: string, got: unknown, exp: unknown) => {
    if (JSON.stringify(got) !== JSON.stringify(exp)) issues.push(`${path} must be ${JSON.stringify(exp)} (got ${JSON.stringify(got)})`);
  };
  want("region", c.region, PROOF_REGION);
  want("kubeContext", c.kubeContext, PROOF_CONTEXT);
  want("namespace", c.namespace, PROOF_NAMESPACE);
  want("release", c.release, PROOF_RELEASE);
  want("chartRepo", c.chartRepo, PROOF_CHART_REPO);
  want("coreDeployments", c.coreDeployments, PROOF_CORE_DEPLOYMENTS);
  want("hoopdb", { cluster: c.hoopdb.cluster, database: c.hoopdb.database }, PROOF_HOOPDB);
  want("locker.bucket", c.locker.bucket, PROOF_LOCKER_BUCKET);
  if (c.backupBucket !== undefined) want("backupBucket", c.backupBucket, PROOF_BACKUP_BUCKET);
  if (c.probeImage !== undefined && !PROOF_PROBE_IMAGES.includes(c.probeImage as string)) issues.push(`probeImage must be one of the allowlisted images (fence.ts PROOF_PROBE_IMAGES)`);
  want("relayauth", { namespace: c.relayauth.namespace, service: c.relayauth.service }, PROOF_RELAYAUTH);
  want("relaycast", { namespace: c.relaycast.namespace, service: c.relaycast.service }, PROOF_RELAYCAST);
  want("ingest.service", c.ingest.service, PROOF_INGEST_SERVICE);
  want("ingress.hostname", c.ingress.hostname, PROOF_HOST);
  want("historyOrigin", c.historyOrigin, PROOF_ORIGIN);
  want("tenants", (c as { tenants?: Array<{ orgId: string }> }).tenants?.map((t) => t.orgId), PROOF_TENANT_ORGS);
  want("objectKeyPrefixBase", (c as { objectKeyPrefixBase?: string }).objectKeyPrefixBase, PROOF_KEY_PREFIX_BASE);
  const t = c.transcriptSearch;
  if (t.backend !== "none") {
    const h = hostOf(t.endpoint);
    if (!h || !PROOF_TRANSCRIPT_HOSTS.includes(h))
      issues.push(`transcriptSearch.endpoint host ${h ?? "?"} is not an allowlisted proof transcript host (fence.ts PROOF_TRANSCRIPT_HOSTS: ${PROOF_TRANSCRIPT_HOSTS.join(", ") || "none yet"})`);
    want("transcriptSearch database/table", { database: t.database, table: t.table }, PROOF_TRANSCRIPT_TABLE);
  }
  return issues;
}

/**
 * The stack's `eks.harness` outputs (#75) restate the harness endpoints. They
 * are read and must equal the hardcoded fence; they never widen it. The shape
 * below is the harness's expectation of #75's output; any difference refuses.
 */
export const EXPECTED_HARNESS_OUTPUTS = {
  relayauthExchangeUrl: `https://${PROOF_RELAYAUTH_TLS.service}.${PROOF_RELAYAUTH_TLS.namespace}.svc:${PROOF_RELAYAUTH_TLS.port}`,
  relayauthCaConfigMap: PROOF_RELAYAUTH_TLS.caConfigMap,
  relayauthClientLabel: PROOF_RELAYAUTH_TLS.clientLabel.join("="),
  postgrestUrl: `http://${PROOF_POSTGREST.service}.${PROOF_POSTGREST.namespace}.svc:${PROOF_POSTGREST.port}`,
  searchClientLabel: PROOF_POSTGREST.clientLabel.join("="),
  tenantOrgIds: [...PROOF_TENANT_ORGS],
  objectKeyPrefix: PROOF_KEY_PREFIX_BASE,
} as const;

export function harnessOutputIssues(outputsJson: string): string[] {
  let h: Record<string, unknown> | undefined;
  try {
    h = (JSON.parse(outputsJson) as { eks?: { harness?: Record<string, unknown> } }).eks?.harness;
  } catch {
    return ["stack outputs are not JSON"];
  }
  if (!h) return ["stack outputs have no eks.harness"];
  const issues: string[] = [];
  for (const [k, want] of Object.entries(EXPECTED_HARNESS_OUTPUTS))
    if (JSON.stringify(h[k]) !== JSON.stringify(want)) issues.push(`eks.harness.${k} is ${JSON.stringify(h[k])}, the fence requires ${JSON.stringify(want)}`);
  return issues;
}
