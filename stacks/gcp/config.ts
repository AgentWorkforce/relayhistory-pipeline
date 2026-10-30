import { isIP } from "node:net";

export const GCP_CONFIRM_PROJECT_ENV = "NINETY_SIX_BULLS_CONFIRM_GCP_PROJECT";

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
  /** Intentionally unsupported until the 96Bulls runtime selects GCS. */
  bulls96?: { enabled: boolean; chartVersion: string };
}

const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const PROJECT_NUMBER = /^[0-9]{6,20}$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const SEMVER = /^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$/;
const KUBERNETES_NAME = /^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$/;

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
  if (!REGION.test(inputs.region)) throw new Error("96bulls: gcp region is invalid");
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
    const jwks = new URL(inputs.relayfile.relayauthJwksUrl);
    if (jwks.username || jwks.password)
      throw new Error("96bulls: relayfile.relayauthJwksUrl must not contain credentials");
    if (!inputs.relayfile.relayauthJwksUrl.startsWith("https://") && !inputs.relayfile.relayauthJwksUrl.startsWith("http://relayauth."))
      throw new Error("96bulls: relayfile.relayauthJwksUrl must be public HTTPS or the in-cluster RelayAuth service");
  }
  if (inputs.relayflows) {
    requireSemver("relayflows.chartVersion", inputs.relayflows.chartVersion);
    if (!KUBERNETES_NAME.test(inputs.relayflows.flowConfigMap))
      throw new Error("96bulls: relayflows.flowConfigMap must be a Kubernetes ConfigMap name");
  }
  if (inputs.bulls96?.enabled)
    throw new Error(
      "96bulls: the GCP 96Bulls add-on is blocked: service Locker wiring is S3-only even though the object-store package has a GCS adapter",
    );
}

export function relaycastChartValues(i: RelaycastChartInputs, defaultStorageClass = "standard-rwo") {
  const host = new URL(i.baseUrl).hostname;
  return {
    replicaCount: 1,
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
