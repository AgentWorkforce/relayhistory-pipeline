import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { isIP } from "node:net";
import versionsLock from "../../versions.lock.json";

export const GCP_CONFIRM_PROJECT_ENV = "NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT";
export const GCP_CONFIRM_SQL_POST_ENV = "NINETY_SIX_BULLS_CONFIRM_GCP_SQL_POST";
export const GCP_APPLY_NONCE_ENV = "NINETY_SIX_BULLS_GCP_APPLY_NONCE";
export const GCP_APPLICATION_PROOF_STAGE = "gcp-conformance";

export type AlchemyStackCommand =
  | "provider"
  | "deploy"
  | "dev"
  | "destroy"
  | "plan"
  | "logs"
  | "profile"
  | "state"
  | "drift"
  | "unsafe";

const ALCHEMY_STACK_COMMANDS = new Set<AlchemyStackCommand>([
  "provider",
  "deploy",
  "dev",
  "destroy",
  "plan",
  "logs",
  "profile",
  "state",
  "drift",
  "unsafe",
]);

/** Mirror Alchemy's first-known-command scan without importing its CLI. */
export function requestedAlchemyCommand(argv: readonly string[] = process.argv.slice(2)): AlchemyStackCommand | undefined {
  return argv.find((value): value is AlchemyStackCommand => ALCHEMY_STACK_COMMANDS.has(value as AlchemyStackCommand));
}

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

/**
 * The only services image the GCP proof may run. Updating a digest alone is
 * insufficient: the release commit must record the exact source revision and
 * explicitly make that pair eligible after its image build succeeds.
 */
export const GCP_APPROVED_SERVICES_IMAGE = {
  digest: versionsLock.images.services.digest,
  sourceRevision: versionsLock.images.services.sourceRevision,
  eligible: versionsLock.images.services.gcpProofEligible,
} as const;

export const GCP_REDIS_CLIENT = {
  authEnabled: "false",
  transitEncryption: "SERVER_AUTHENTICATION",
} as const;

/** DNS-1035-safe names shared by Helm rendering and Workload Identity IAM. */
export const GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS = {
  worker: "bulls96-worker",
  alleyoop: "bulls96-alleyoop",
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
  nodeLocation: "us-central1-a",
  machineType: "e2-medium",
  initialNodeCount: 1,
  natIpAllocateOption: "MANUAL_ONLY",
  natLogFilter: "ALL",
  nodeOauthScope: "https://www.googleapis.com/auth/cloud-platform",
} as const;

/** Node identity only. Not the Workload Identity runtime account, and not a key. */
export const GCP_NODE_ROLES = [
  "roles/container.defaultNodeServiceAccount",
  "roles/artifactregistry.reader",
] as const;

export interface CapturedRelaycastChartPackage {
  readonly bytes: Buffer;
  readonly digest: string;
  readonly sourcePath: string;
}

/** Read once, then verify those exact bytes. Callers must never reopen sourcePath. */
export function captureRelaycastChartPackage(path: string): CapturedRelaycastChartPackage {
  let bytes: Buffer;
  try {
    bytes = readFileSync(path);
  } catch {
    throw new Error(
      `96bulls: Relaycast chart package is missing; expected the published 0.1.0 artifact at ${path}. Nothing was created.`,
    );
  }
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (digest !== RELAYCAST_PUBLISHED_CHART.packageDigest)
    throw new Error("96bulls: Relaycast chart package digest does not match published 0.1.0. Nothing was created.");
  return { bytes, digest, sourcePath: path };
}

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

export interface KubernetesExecutorInputs {
  /** A human-auditable runner name, not a credential or address. */
  name: string;
  /** The runner must have a route to the cluster's private control plane. */
  mode: "in-vpc" | "vpn";
  /** Google service-account identity whose signed OIDC token authorizes this apply. */
  identity: string;
}

export interface GcpInputs {
  projectId: string;
  projectNumber: string;
  organizationId: string;
  billingAccount: string;
  region: string;
  /** Operator/VPN ranges allowed to reach the private GKE API endpoint. */
  controlPlaneAuthorizedCidrs: Array<{ cidrBlock: string; displayName?: string }>;
  /** Required before this stack declares any Kubernetes object or Helm chart. */
  kubernetesExecutor: KubernetesExecutorInputs;
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
    /** Digest of the services image built from the reviewed runtime commit. */
    imageDigest: string;
    /** Full Git revision from which imageDigest was built. Must match versions.lock.json. */
    imageSourceRevision: string;
    /** Public HTTPS or the in-cluster RelayAuth service. Alleyoop refuses to render without it. */
    relayauthJwksUrl: string;
    /** Exact token issuer: public HTTPS or the in-cluster RelayAuth service origin. */
    relayauthIssuer: string;
    /** Exact History origin placed in RelayAuth assertion audiences and enforced by AlleyOop. */
    relayhistoryOrigin: string;
  };
}

const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const GCP_STAGE = /^gcp-[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const PROJECT_NUMBER = /^[0-9]{6,20}$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const SEMVER = /^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$/;
const KUBERNETES_NAME = /^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$/;
const EXECUTOR_IDENTITY = /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

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

export function assertGcpInputs(
  stage: string,
  inputs: GcpInputs,
  requestedCommand?: AlchemyStackCommand,
): void {
  if (!GCP_STAGE.test(stage)) throw new Error(`96bulls: GCP config requires a safe gcp-* stage, got ${stage}`);
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
    const family = match ? isIP(match[1]!) : 0;
    if (
      !match ||
      cidrBlock !== cidrBlock.trim() ||
      family === 0 ||
      Number(match[2]) > (family === 6 ? 128 : 32) ||
      cidrBlock === "0.0.0.0/0" ||
      cidrBlock === "::/0"
    )
      throw new Error(`96bulls: unsafe or invalid control-plane CIDR ${cidrBlock}`);
  }
  if (!inputs.kubernetesExecutor)
    throw new Error("96bulls: every GCP apply requires a named executor identity and fresh signed nonce");
  {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{2,62}$/.test(inputs.kubernetesExecutor.name))
      throw new Error("96bulls: kubernetesExecutor.name must identify a named runner, never an address or credential");
    if (inputs.kubernetesExecutor.mode !== "in-vpc" && inputs.kubernetesExecutor.mode !== "vpn")
      throw new Error("96bulls: kubernetesExecutor.mode must be in-vpc or vpn");
    if (!EXECUTOR_IDENTITY.test(inputs.kubernetesExecutor.identity))
      throw new Error("96bulls: kubernetesExecutor.identity must be a Google service-account email");
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
    if (!/^sha256:[a-f0-9]{64}$/.test(inputs.bulls96.imageDigest))
      throw new Error("96bulls: bulls96.imageDigest must be an immutable sha256 digest");
    if (!/^[a-f0-9]{40}$/.test(inputs.bulls96.imageSourceRevision))
      throw new Error("96bulls: bulls96.imageSourceRevision must be a full Git commit SHA");
    requireRelayauthJwksUrl("bulls96.relayauthJwksUrl", inputs.bulls96.relayauthJwksUrl);
    requireRelayauthIssuer("bulls96.relayauthIssuer", inputs.bulls96.relayauthIssuer);
    requireRelayhistoryOrigin("bulls96.relayhistoryOrigin", inputs.bulls96.relayhistoryOrigin);
    if (inputs.relayauth?.baseUrl && inputs.bulls96.relayauthIssuer !== inputs.relayauth.baseUrl)
      throw new Error("96bulls: bulls96.relayauthIssuer must match relayauth.baseUrl for the in-stage RelayAuth release");
    if (requestedCommand !== "destroy" && stage !== GCP_APPLICATION_PROOF_STAGE)
      throw new Error(
        `96bulls: the approved services image is published only for ${GCP_APPLICATION_PROOF_STAGE}; ${stage} cannot install 96Bulls. Nothing was created.`,
      );
    if (requestedCommand !== "destroy")
      requireApprovedGcpServicesImage(inputs.bulls96.imageDigest, inputs.bulls96.imageSourceRevision);
  }
}

function requireApprovedGcpServicesImage(digest: string, sourceRevision: string): void {
  if (
    digest !== GCP_APPROVED_SERVICES_IMAGE.digest ||
    sourceRevision !== GCP_APPROVED_SERVICES_IMAGE.sourceRevision
  )
    throw new Error(
      "96bulls: GCP runtime image digest/source revision does not match the reviewed release in versions.lock.json. Nothing was created.",
    );
  if (!GCP_APPROVED_SERVICES_IMAGE.eligible)
    throw new Error(
      "96bulls: the recorded services image is not approved for the GCP application proof. Publish and review a current image first. Nothing was created.",
    );
}

/** Defense in depth for runtime wiring; the stack's poster remains apply-only. */
export function assertGcpRuntimeDeployable(
  stage: string,
  inputs: GcpInputs,
  requestedCommand?: AlchemyStackCommand,
): void {
  if (
    inputs.bulls96?.enabled &&
    (!inputs.managedDataPlane.provision || inputs.managedDataPlane.acknowledgement !== "adapter-proof-only")
  )
    throw new Error("96bulls: GCP runtime wiring requires the acknowledged managed data plane. Nothing was created.");
  if (inputs.bulls96?.enabled && requestedCommand !== "destroy" && stage !== GCP_APPLICATION_PROOF_STAGE)
    throw new Error(
      `96bulls: the approved services image is published only for ${GCP_APPLICATION_PROOF_STAGE}; ${stage} cannot install 96Bulls. Nothing was created.`,
    );
  if (inputs.bulls96?.enabled && requestedCommand !== "destroy")
    requireApprovedGcpServicesImage(inputs.bulls96.imageDigest, inputs.bulls96.imageSourceRevision);
}

export function assertGcpSqlPostConfirmation(
  projectId: string,
  instanceName: string,
  confirmation: string | undefined,
): void {
  const expected = `${projectId}/${instanceName}`;
  if (confirmation !== expected)
    throw new Error(
      `96bulls: set ${GCP_CONFIRM_SQL_POST_ENV}=${expected} for the apply-time Cloud SQL insert. Nothing was created.`,
    );
}

export function relaycastChartValues(i: RelaycastChartInputs, defaultStorageClass = "standard-rwo") {
  if (i.chartVersion !== RELAYCAST_PUBLISHED_CHART.version)
    throw new Error("96bulls: relaycast chart is not the published 0.1.0 contract. Nothing was created.");
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
    service: { type: "ClusterIP", annotations: {} },
    ingress: {
      enabled: false,
      className: "",
      annotations: {},
      hosts: [],
      tls: [],
    },
    networkPolicy: { enabled: true, ingressFrom: [] },
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
  /** Digest-pinned repository in the stage's private Artifact Registry. */
  imageRepository: unknown;
  cloudSqlConnectionName: unknown;
  redisHost: unknown;
  redisPort: unknown;
  /** Public Memorystore service-CA bundle, never a private key or AUTH value. */
  redisCaPem: unknown;
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

function requireRelayauthIssuer(label: string, value: string): void {
  let issuer: URL;
  try {
    issuer = new URL(value);
  } catch {
    throw new Error(`96bulls: ${label} must be a public HTTPS or in-cluster RelayAuth origin`);
  }
  const hostname = issuer.hostname.replace(/\.$/, "").toLowerCase();
  const inCluster =
    issuer.protocol === "http:" &&
    /^relayauth\.[a-z0-9-]+\.svc(?:\.cluster\.local)?$/.test(hostname);
  const publicHttps =
    issuer.protocol === "https:" &&
    hostname.includes(".") &&
    hostname !== "localhost" &&
    !hostname.endsWith(".localhost") &&
    !hostname.endsWith(".local") &&
    isIP(hostname) === 0;
  if (
    issuer.username ||
    issuer.password ||
    (!inCluster && !publicHttps) ||
    issuer.pathname !== "/" ||
    issuer.search ||
    issuer.hash
  ) throw new Error(`96bulls: ${label} must be a public HTTPS or in-cluster RelayAuth origin`);
}

function requireRelayhistoryOrigin(label: string, value: string): void {
  let origin: URL;
  try {
    origin = new URL(value);
  } catch {
    throw new Error(`96bulls: ${label} must be a public HTTPS or in-cluster Kubernetes origin`);
  }
  const hostname = origin.hostname.replace(/\.$/, "").toLowerCase();
  const inCluster =
    origin.protocol === "http:" &&
    /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\.svc(?:\.cluster\.local)?$/.test(hostname);
  const publicHttps =
    origin.protocol === "https:" &&
    hostname.includes(".") &&
    hostname !== "localhost" &&
    !hostname.endsWith(".localhost") &&
    !hostname.endsWith(".local") &&
    isIP(hostname) === 0;
  const canonical = value.replace(/\/$/, "").toLowerCase() === origin.origin;
  if (
    origin.username ||
    origin.password ||
    (!inCluster && !publicHttps) ||
    !canonical ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  ) throw new Error(`96bulls: ${label} must be a public HTTPS or in-cluster Kubernetes origin`);
}

function plainSetting(label: string, value: unknown): void {
  if (typeof value !== "string") return;
  if (value.includes("://") || value.includes("@") || value.includes("BEGIN "))
    throw new Error(`96bulls: ${label} must not carry a URL, credential, or key. Nothing was created.`);
}

function publicCaBundle(value: unknown): void {
  if (typeof value !== "string") return;
  if (
    value.length > 131_072 ||
    value.includes("PRIVATE KEY") ||
    !value.includes("-----BEGIN CERTIFICATE-----") ||
    !value.includes("-----END CERTIFICATE-----")
  ) throw new Error("96bulls: Redis service CA bundle is malformed. Nothing was created.");
}

/** Published chart values backed by pod identity, an external Secret, and a live Redis TLS readiness probe. */
export function bulls96ReleaseValues(
  i: NonNullable<GcpInputs["bulls96"]>,
  managed: Bulls96ReleaseOutputs,
): Record<string, unknown> {
  if (!GSA_EMAIL.test(managed.workloadIdentityEmail))
    throw new Error("96bulls: workload identity email is not a service-account email. Nothing was created.");
  for (const [label, value] of [
    ["bucket", managed.bucket],
    ["region", managed.region],
    ["imageRepository", managed.imageRepository],
    ["cloudSqlConnectionName", managed.cloudSqlConnectionName],
    ["redisHost", managed.redisHost],
    ["pubsubTopic", managed.pubsubTopic],
    ["pubsubSubscription", managed.pubsubSubscription],
  ] as const) plainSetting(label, value);
  publicCaBundle(managed.redisCaPem);
  if (typeof managed.redisPort === "string" && !/^[0-9]+$/.test(managed.redisPort))
    throw new Error("96bulls: redis port must be numeric. Nothing was created.");
  const redisPort = typeof managed.redisPort === "number" ? String(managed.redisPort) : managed.redisPort;
  requireRelayauthJwksUrl("bulls96.relayauthJwksUrl", i.relayauthJwksUrl);
  requireRelayauthIssuer("bulls96.relayauthIssuer", i.relayauthIssuer);
  requireRelayhistoryOrigin("bulls96.relayhistoryOrigin", i.relayhistoryOrigin);
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
    { name: "REDIS_TLS_CA_PEM", value: managed.redisCaPem },
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
  const imagePullSecrets: Array<{ name: string }> = [];
  const image = { repository: managed.imageRepository, digest: i.imageDigest };
  return {
    global: { tenant: { orgId: i.tenantOrgId } },
    worker: {
      fullnameOverride: GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS.worker,
      existingSecret: i.existingSecretName,
      imagePullSecrets,
      image,
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
      fullnameOverride: GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS.alleyoop,
      existingSecret: i.existingSecretName,
      imagePullSecrets,
      image,
      hoopdb: hoopdb(),
      locker: locker(),
      auth: { jwksUrl: i.relayauthJwksUrl, issuer: i.relayauthIssuer },
      serviceAccount: { annotations: { ...identity } },
      tenant: { orgId: i.tenantOrgId, workspaceId: "default" },
      extraEnv: [
        ...dataEnv(),
        { name: "RELAYHISTORY_ORIGIN", value: i.relayhistoryOrigin },
      ],
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
