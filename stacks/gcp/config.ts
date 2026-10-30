import { createHash } from "node:crypto";
import { isIP } from "node:net";

export const GCP_CONFIRM_PROJECT_ENV = "NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT";

/** The only project this lane may plan. Any other id, number, org, billing account, or region is refused. */
export const GCP_APPROVED_TARGET = {
  projectId: "agentrelay-96bulls-soc2-2610",
  projectNumber: "955297012355",
  organizationId: "929835122282",
  billingAccount: "014D50-E96D1A-DE6360",
  region: "us-central1",
} as const;

/** Published helm index entry relaycast-0.1.0. The image digest is the chart default. */
export const RELAYCAST_PUBLISHED_CHART = {
  version: "0.1.0",
  repository: "ghcr.io/agentworkforce/relaycast-self-host",
  imageDigest: "sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98",
  packageDigest: "cdfc41cb6202fd81beb17cb57d5fbbdecaf27fa427f51faa07c5ec0fc116744f",
} as const;

/** Non-secret client contract. AUTH stays off; TLS is server authentication. No password is accepted. */
export const GCP_REDIS_CLIENT = {
  authEnabled: "false",
  transitEncryption: "SERVER_AUTHENTICATION",
} as const;

/**
 * Shape the installed Alchemy provider can actually post.
 * Autopilot create omits node pools, so a node service account would not be
 * sent and `disallowDefaultComputeServiceAccount` would deny the cluster.
 * Standard mode posts that account on the default pool. NAT uses one reserved
 * address and logs every translation. The control-plane endpoint is private;
 * operator CIDRs are the only authorized networks.
 */
export const GCP_RUNTIME_SHAPE = {
  privateEndpoint: true,
  autopilot: false,
  machineType: "e2-medium",
  initialNodeCount: 1,
  natIpAllocateOption: "MANUAL_ONLY",
  natLogFilter: "ALL",
  nodeOauthScope: "https://www.googleapis.com/auth/cloud-platform",
} as const;

/** Node identity only. Not the Workload Identity runtime account, and not a key. */
export const GCP_NODE_ROLES = [
  "roles/logging.logWriter",
  "roles/monitoring.metricWriter",
  "roles/monitoring.viewer",
  "roles/stackdriver.resourceMetadata.writer",
  "roles/artifactregistry.reader",
] as const;

export function isPublishedHelmRepo(repo: string | undefined): boolean {
  if (!repo) return true;
  let url: URL;
  try {
    url = new URL(repo);
  } catch {
    return false;
  }
  return (
    url.protocol === "https:" &&
    !url.username &&
    !url.password &&
    !url.search &&
    !url.hash &&
    url.hostname.toLowerCase() === "agentworkforce.github.io" &&
    url.pathname.replace(/\/$/, "") === "/helm-charts"
  );
}

export interface ManagedDataPlaneInputs {
  /**
   * Managed services are provisioned for adapter development and proof only.
   * Relaycast does not consume them until its HA adapter is released.
   */
  provision: boolean;
  acknowledgement?: "adapter-proof-only";
  cloudSqlTier?: string;
  cloudSqlHa?: boolean;
  redisMemoryGb?: number;
  redisHa?: boolean;
}

export interface RelaycastChartInputs {
  chartVersion: string;
  baseUrl: string;
  storageClass?: string;
}

export interface RelayAuthChartInputs {
  chartVersion: string;
  existingSecretName: string;
  baseUrl?: string;
  storageClass?: string;
}

export interface RelayfileChartInputs {
  chartVersion: string;
  existingSecretName: string;
  relayauthJwksUrl: string;
}

export interface RelayflowsChartInputs {
  chartVersion: string;
  flowConfigMap: string;
  flowConfigMapKey?: string;
}

export interface GcpInputs {
  projectId: string;
  projectNumber: string;
  organizationId: string;
  billingAccount: string;
  region: string;
  /** Operator/VPN egress ranges allowed to reach the public GKE API endpoint. */
  controlPlaneAuthorizedCidrs: Array<{ cidrBlock: string; displayName?: string }>;
  /** Khaliq's words and date recording that a real GCP plan/deploy is cleared. */
  clearedBy: string;
  chartRepo?: string;
  chartStorageClass?: string;
  managedDataPlane: ManagedDataPlaneInputs;
  relaycast?: RelaycastChartInputs;
  relayauth?: RelayAuthChartInputs;
  relayfile?: RelayfileChartInputs;
  relayflows?: RelayflowsChartInputs;
  /**
   * Conformance install of the 96Bulls chart. Allowed only with the managed
   * data plane. Secret payloads stay in `existingSecretName`.
   */
  bulls96?: {
    enabled: boolean;
    chartVersion: string;
    tenantOrgId: string;
    existingSecretName: string;
    /** Public HTTPS or the in-cluster RelayAuth service. Alleyoop refuses to render without it. */
    relayauthJwksUrl: string;
  };
}

const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const PROJECT_NUMBER = /^[0-9]{6,20}$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const SEMVER = /^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$/;
const KUBERNETES_NAME = /^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$/;

/** Keep long stage names distinct while preserving a human-readable suffix. */
export function gcpResourceName(stage: string, suffix: string, max = 40): string {
  const stagePart = stage
    .replace(/^gcp-/, "")
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  const suffixPart = suffix
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  const full = `awf-${stagePart}-${suffixPart}`;
  if (full.length <= max) return full;
  const digest = createHash("sha256").update(full).digest("hex").slice(0, 8);
  const fixed = `awf--${digest}-${suffixPart}`;
  const stageBudget = max - fixed.length;
  if (stageBudget < 1)
    throw new Error("96bulls: GCP resource name limit cannot preserve uniqueness and suffix");
  return `awf-${stagePart.slice(0, stageBudget).replace(/-$/, "")}-${digest}-${suffixPart}`;
}

export function pubsubServiceAgent(projectNumber: string): string {
  if (!PROJECT_NUMBER.test(projectNumber))
    throw new Error("96bulls: gcp projectNumber is invalid");
  return `service-${projectNumber}@gcp-sa-pubsub.iam.gserviceaccount.com`;
}

function requireSemver(label: string, version: string): void {
  if (!SEMVER.test(version)) throw new Error(`96bulls: ${label} must be an explicit semantic version`);
}

function requireSecretName(label: string, name: string): void {
  if (!KUBERNETES_NAME.test(name) || name.length > 253)
    throw new Error(`96bulls: ${label} must be a Kubernetes Secret name, never secret material`);
}

function requirePublicHttpsOrigin(label: string, value: string): URL {
  const origin = new URL(value);
  if (
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  )
    throw new Error(`96bulls: ${label} must be an HTTPS origin without credentials, path, query, or fragment`);
  const hostname = origin.hostname.replace(/\.$/, "").toLowerCase();
  if (
    !hostname.includes(".") ||
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    isIP(hostname) !== 0
  )
    throw new Error(`96bulls: ${label} must use a public multi-label DNS hostname`);
  return origin;
}

export function assertGcpInputs(stage: string, inputs: GcpInputs): void {
  if (!stage.startsWith("gcp-")) throw new Error(`96bulls: GCP config requires a gcp-* stage, got ${stage}`);
  if (!PROJECT_ID.test(inputs.projectId)) throw new Error("96bulls: gcp projectId is invalid");
  if (!PROJECT_NUMBER.test(inputs.projectNumber)) throw new Error("96bulls: gcp projectNumber is invalid");
  if (!/^[0-9]{6,20}$/.test(inputs.organizationId)) throw new Error("96bulls: gcp organizationId is invalid");
  if (!/^[0-9A-F]{6}-[0-9A-F]{6}-[0-9A-F]{6}$/.test(inputs.billingAccount))
    throw new Error("96bulls: gcp billingAccount is invalid");
  if (!REGION.test(inputs.region)) throw new Error("96bulls: gcp region is invalid");
  if (
    inputs.projectId !== GCP_APPROVED_TARGET.projectId ||
    inputs.projectNumber !== GCP_APPROVED_TARGET.projectNumber ||
    inputs.organizationId !== GCP_APPROVED_TARGET.organizationId ||
    inputs.billingAccount !== GCP_APPROVED_TARGET.billingAccount ||
    inputs.region !== GCP_APPROVED_TARGET.region
  ) {
    throw new Error("96bulls: GCP target does not match the approved project. Nothing was created.");
  }
  if (!inputs.clearedBy?.trim()) throw new Error("96bulls: GCP stage has no clearedBy record; nothing was created");
  if (!inputs.controlPlaneAuthorizedCidrs?.length)
    throw new Error("96bulls: controlPlaneAuthorizedCidrs must allow at least one operator or VPN range");
  for (const { cidrBlock } of inputs.controlPlaneAuthorizedCidrs) {
    const match = /^([0-9A-Fa-f:.]+)\/([0-9]{1,3})$/.exec(cidrBlock);
    if (!match || Number(match[2]) > (match[1]!.includes(":") ? 128 : 32) || cidrBlock === "0.0.0.0/0" || cidrBlock === "::/0")
      throw new Error(`96bulls: unsafe or invalid control-plane CIDR ${cidrBlock}`);
  }
  if (inputs.chartRepo) {
    const repo = new URL(inputs.chartRepo);
    if (repo.protocol !== "https:" || repo.username || repo.password)
      throw new Error("96bulls: chartRepo must be HTTPS and must not contain credentials");
  }
  if (inputs.chartStorageClass && !KUBERNETES_NAME.test(inputs.chartStorageClass))
    throw new Error("96bulls: chartStorageClass is not a valid Kubernetes name");

  if (inputs.managedDataPlane.provision && inputs.managedDataPlane.acknowledgement !== "adapter-proof-only")
    throw new Error(
      "96bulls: managedDataPlane.provision requires acknowledgement=adapter-proof-only because Relaycast HA is not released",
    );
  if ((inputs.managedDataPlane.redisMemoryGb ?? 1) < 1)
    throw new Error("96bulls: managedDataPlane.redisMemoryGb must be at least 1");

  if (inputs.relaycast) {
    requireSemver("relaycast.chartVersion", inputs.relaycast.chartVersion);
    if (inputs.relaycast.chartVersion !== RELAYCAST_PUBLISHED_CHART.version || !isPublishedHelmRepo(inputs.chartRepo))
      throw new Error("96bulls: relaycast must use published chart 0.1.0 from the AgentWorkforce helm repository. Nothing was created.");
    requirePublicHttpsOrigin("relaycast.baseUrl", inputs.relaycast.baseUrl);
  }
  if (inputs.relayauth) {
    requireSemver("relayauth.chartVersion", inputs.relayauth.chartVersion);
    requireSecretName("relayauth.existingSecretName", inputs.relayauth.existingSecretName);
    if (inputs.relayauth.baseUrl) requirePublicHttpsOrigin("relayauth.baseUrl", inputs.relayauth.baseUrl);
  }
  if (inputs.relayfile) {
    requireSemver("relayfile.chartVersion", inputs.relayfile.chartVersion);
    requireSecretName("relayfile.existingSecretName", inputs.relayfile.existingSecretName);
    requireRelayauthJwksUrl("relayfile.relayauthJwksUrl", inputs.relayfile.relayauthJwksUrl);
  }
  if (inputs.relayflows) {
    requireSemver("relayflows.chartVersion", inputs.relayflows.chartVersion);
    if (!KUBERNETES_NAME.test(inputs.relayflows.flowConfigMap))
      throw new Error("96bulls: relayflows.flowConfigMap must be a Kubernetes ConfigMap name");
  }
  if (inputs.bulls96?.enabled) {
    if (!inputs.managedDataPlane.provision)
      throw new Error(
        "96bulls: bulls96.enabled requires managedDataPlane.provision with acknowledgement=adapter-proof-only",
      );
    requireSemver("bulls96.chartVersion", inputs.bulls96.chartVersion);
    const [major, minor] = inputs.bulls96.chartVersion.split(".").map(Number);
    if (!(major! > 0 || (major === 0 && minor! >= 3)))
      throw new Error("96bulls: bulls96.chartVersion must be 0.3.0 or newer so the worker chart accepts pod identity and extraEnv");
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(inputs.bulls96.tenantOrgId))
      throw new Error("96bulls: bulls96.tenantOrgId must be an org id, never a URL or credential");
    requireSecretName("bulls96.existingSecretName", inputs.bulls96.existingSecretName);
    requireRelayauthJwksUrl("bulls96.relayauthJwksUrl", inputs.bulls96.relayauthJwksUrl);
  }
}

/** Defense in depth for runtime wiring; the stack's poster remains apply-only. */
export function assertGcpRuntimeDeployable(inputs: GcpInputs): void {
  if (
    inputs.bulls96?.enabled &&
    (!inputs.managedDataPlane.provision || inputs.managedDataPlane.acknowledgement !== "adapter-proof-only")
  )
    throw new Error("96bulls: GCP runtime wiring requires the acknowledged managed data plane. Nothing was created.");
}

export function relaycastChartValues(i: RelaycastChartInputs, defaultStorageClass = "standard-rwo") {
  if (i.chartVersion !== RELAYCAST_PUBLISHED_CHART.version)
    throw new Error("96bulls: relaycast chart is not the published 0.1.0 contract. Nothing was created.");
  const host = new URL(i.baseUrl).hostname;
  return {
    replicaCount: 1,
    strategy: { type: "Recreate" },
    image: {
      repository: RELAYCAST_PUBLISHED_CHART.repository,
      digest: RELAYCAST_PUBLISHED_CHART.imageDigest,
    },
    runtime: { backend: "sqlite" },
    baseUrl: i.baseUrl,
    persistence: { storageClass: i.storageClass ?? defaultStorageClass, accessMode: "ReadWriteOnce", size: "20Gi" },
    service: { type: "ClusterIP", annotations: { "cloud.google.com/neg": '{"ingress": true}' } },
    ingress: {
      enabled: true,
      className: "",
      annotations: {
        "kubernetes.io/ingress.class": "gce",
        "networking.gke.io/managed-certificates": "relaycast",
      },
      hosts: [host],
      tls: [],
    },
  };
}

export function relayauthChartValues(i: RelayAuthChartInputs, defaultStorageClass = "standard-rwo") {
  return {
    replicaCount: 1,
    existingSecret: i.existingSecretName,
    ...(i.baseUrl ? { baseUrl: i.baseUrl, issuer: i.baseUrl } : {}),
    persistence: { storageClass: i.storageClass ?? defaultStorageClass },
    jwksIngress: i.baseUrl
      ? {
          enabled: true,
          className: "",
          annotations: {
            "kubernetes.io/ingress.class": "gce",
            "networking.gke.io/managed-certificates": "relayauth",
          },
          hosts: [new URL(i.baseUrl).hostname],
          tls: [],
        }
      : { enabled: false },
  };
}

export function relayfileChartValues(i: RelayfileChartInputs) {
  return {
    server: { backendProfile: "production" },
    secrets: { existingSecret: i.existingSecretName },
    auth: { jwksUrl: i.relayauthJwksUrl },
    ingress: { enabled: false },
  };
}

export interface Bulls96ReleaseOutputs {
  bucket: unknown;
  region: string;
  cloudSqlConnectionName: unknown;
  redisHost: unknown;
  redisPort: unknown;
  pubsubTopic: unknown;
  pubsubSubscription: unknown;
  workloadIdentityEmail: string;
}

const GSA_EMAIL = /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

function requireRelayauthJwksUrl(label: string, value: string): void {
  let jwks: URL;
  try {
    jwks = new URL(value);
  } catch {
    throw new Error(`96bulls: ${label} must be public HTTPS or the in-cluster RelayAuth service`);
  }
  if (jwks.username || jwks.password)
    throw new Error(`96bulls: ${label} must not contain credentials`);
  const hostname = jwks.hostname.replace(/\.$/, "").toLowerCase();
  const inCluster =
    jwks.protocol === "http:" &&
    /^relayauth\.[a-z0-9-]+\.svc(?:\.cluster\.local)?$/.test(hostname);
  const publicHttps =
    jwks.protocol === "https:" &&
    hostname.includes(".") &&
    hostname !== "localhost" &&
    !hostname.endsWith(".localhost") &&
    !hostname.endsWith(".local") &&
    isIP(hostname) === 0;
  if ((!inCluster && !publicHttps) || jwks.hash || jwks.search)
    throw new Error(`96bulls: ${label} must be public HTTPS or the in-cluster RelayAuth service`);
}

function plainSetting(label: string, value: unknown): void {
  if (typeof value !== "string") return;
  if (value.includes("://") || value.includes("@") || value.includes("BEGIN "))
    throw new Error(`96bulls: ${label} must not carry a URL, credential, or key. Nothing was created.`);
}

/**
 * Non-secret outputs for the published 96bulls chart. In-chart MinIO and
 * CloudNativePG stay off. HOOPDB_URL remains a key in the existing Secret.
 * Redis AUTH and HMAC keys are not accepted here.
 */
export function bulls96ReleaseValues(
  i: NonNullable<GcpInputs["bulls96"]>,
  managed: Bulls96ReleaseOutputs,
): Record<string, unknown> {
  if (!GSA_EMAIL.test(managed.workloadIdentityEmail))
    throw new Error("96bulls: workload identity email is not a service-account email. Nothing was created.");
  for (const [label, value] of [
    ["bucket", managed.bucket],
    ["region", managed.region],
    ["cloudSqlConnectionName", managed.cloudSqlConnectionName],
    ["redisHost", managed.redisHost],
    ["pubsubTopic", managed.pubsubTopic],
    ["pubsubSubscription", managed.pubsubSubscription],
  ] as const) {
    plainSetting(label, value);
  }
  if (typeof managed.redisPort === "string" && !/^[0-9]+$/.test(managed.redisPort))
    throw new Error("96bulls: redis port must be numeric. Nothing was created.");
  const redisPort = typeof managed.redisPort === "number" ? String(managed.redisPort) : managed.redisPort;
  requireRelayauthJwksUrl("bulls96.relayauthJwksUrl", i.relayauthJwksUrl);
  const identity = { "iam.gke.io/gcp-service-account": managed.workloadIdentityEmail };
  const dataEnv = () => [
    { name: "LOCKER_PROVIDER", value: "gcs" },
    { name: "PIPELINE_RAW_PROVIDER", value: "gcs" },
    { name: "PIPELINE_RAW_S3_BUCKET", value: managed.bucket },
    { name: "CLOUD_SQL_CONNECTION_NAME", value: managed.cloudSqlConnectionName },
    { name: "REDIS_HOST", value: managed.redisHost },
    { name: "REDIS_PORT", value: redisPort },
    { name: "REDIS_AUTH_ENABLED", value: GCP_REDIS_CLIENT.authEnabled },
    { name: "REDIS_TRANSIT_ENCRYPTION", value: GCP_REDIS_CLIENT.transitEncryption },
    { name: "PUBSUB_TOPIC", value: managed.pubsubTopic },
    { name: "PUBSUB_SUBSCRIPTION", value: managed.pubsubSubscription },
    { name: "WORKER_DATA_PATH_PROBE", value: "1" },
  ];
  const locker = () => ({
    endpoint: "",
    bucket: managed.bucket,
    region: managed.region,
    credentials: { mode: "podIdentity" as const },
  });
  const hoopdb = () => ({ existingSecret: i.existingSecretName, key: "HOOPDB_URL" });
  return {
    global: { tenant: { orgId: i.tenantOrgId } },
    worker: {
      existingSecret: i.existingSecretName,
      hoopdb: hoopdb(),
      locker: locker(),
      lockerInit: { enabled: false },
      migrations: { supabaseShim: true },
      summaryEncryption: { existingSecret: i.existingSecretName, key: "SUMMARY_MASTER_KEYS" },
      serviceAccount: { annotations: { ...identity } },
      tenant: { orgId: i.tenantOrgId, workspaceId: "default" },
      extraEnv: dataEnv(),
    },
    alleyoop: {
      existingSecret: i.existingSecretName,
      hoopdb: hoopdb(),
      locker: locker(),
      auth: { jwksUrl: i.relayauthJwksUrl },
      serviceAccount: { annotations: { ...identity } },
      tenant: { orgId: i.tenantOrgId, workspaceId: "default" },
      extraEnv: dataEnv(),
    },
    hoopdb: { enabled: false },
    locker: { enabled: false },
  };
}

export function relayflowsChartValues(i: RelayflowsChartInputs, defaultStorageClass = "standard-rwo") {
  return {
    mode: "standalone",
    standalone: {
      flow: {
        existingConfigMap: i.flowConfigMap,
        configMapKey: i.flowConfigMapKey ?? "flow.yaml",
      },
    },
    persistence: { storageClass: defaultStorageClass },
  };
}
