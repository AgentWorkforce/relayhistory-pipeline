/**
 * Private Cloud SQL body and Auth Proxy route.
 *
 * `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` writes only
 * `ipConfiguration.ipv4Enabled`. Extra fields are dropped, so this stack must
 * not call that resource. The body below is the SQL Admin insert shape a
 * later poster has to send: public IPv4 off, the VPC link, the Private
 * Service Access range, and no authorized networks. `posted` stays false
 * until that poster exists. Nothing in this module creates an instance.
 */

export interface SqlProviderCapabilities {
  /** True only when `GCP.SQL.Instance` accepts `privateNetwork` and passes it to Cloud SQL. */
  privateNetwork: boolean;
}

/** Capability of the pin used by this lane. Do not flip this until the provider passes the field through. */
export const ALCHEMY_GCP_BETA_84_SQL: SqlProviderCapabilities = { privateNetwork: false };

export const CLOUD_SQL_PROXY_IMAGE = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.26.3";

export interface CloudSqlIpConfiguration {
  ipv4Enabled: false;
  /** `/projects/{project}/global/networks/{name}`. */
  privateNetwork: string;
  /** Private Service Access range name, not a CIDR. */
  allocatedIpRange: string;
}

export interface PrivateCloudSqlSpec {
  /** This module does not call Cloud SQL. A later resource must post `insertBody` unchanged. */
  posted: false;
  instanceName: string;
  connectionName: string;
  databaseVersion: "POSTGRES_17";
  ipv4Enabled: false;
  privateNetwork: string;
  allocatedIpRange: string;
  insertBody: {
    name: string;
    region: string;
    databaseVersion: "POSTGRES_17";
    settings: {
      tier: string;
      availabilityType: "ZONAL" | "REGIONAL";
      ipConfiguration: CloudSqlIpConfiguration;
    };
  };
  /** Positional connection name is last. `--private-ip` is required. */
  proxyArgs: readonly ["--private-ip", "--address=0.0.0.0", "--port=5432", string];
}

const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const SHORT_NAME = /^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const TIER = /^[a-z][a-z0-9-]{1,62}$/;

function refuse(message: string): never {
  throw new Error(`96bulls: ${message} Nothing was created.`);
}

function shortName(label: string, value: unknown): string {
  if (typeof value !== "string" || !SHORT_NAME.test(value))
    refuse(`Cloud SQL private attachment needs a ${label} name.`);
  return value;
}

export function privateCloudSqlSpec(input: {
  projectId: string;
  region: string;
  instanceName: string;
  networkName: string;
  allocatedIpRange: string;
  tier?: string;
  highAvailability?: boolean;
}): PrivateCloudSqlSpec {
  if (!PROJECT_ID.test(input.projectId)) refuse("Cloud SQL private attachment needs a project id.");
  if (!REGION.test(input.region)) refuse("Cloud SQL private attachment needs a region.");
  const instanceName = shortName("instance", input.instanceName);
  const networkName = shortName("VPC network", input.networkName);
  const allocatedIpRange = shortName("Private Service Access range", input.allocatedIpRange);
  const tier = input.tier ?? "db-custom-1-3840";
  if (!TIER.test(tier)) refuse("Cloud SQL tier is not a Cloud SQL tier name.");
  const privateNetwork = `/projects/${input.projectId}/global/networks/${networkName}`;
  const connectionName = `${input.projectId}:${input.region}:${instanceName}`;
  const ipConfiguration: CloudSqlIpConfiguration = {
    ipv4Enabled: false,
    privateNetwork,
    allocatedIpRange,
  };
  if ("authorizedNetworks" in ipConfiguration || ipConfiguration.ipv4Enabled !== false)
    refuse("Cloud SQL ipConfiguration drifted off the private-only body.");
  const proxyArgs = ["--private-ip", "--address=0.0.0.0", "--port=5432", connectionName] as const;
  if (proxyArgs[0] !== "--private-ip") refuse("Cloud SQL proxy lost --private-ip.");
  return {
    posted: false,
    instanceName,
    connectionName,
    databaseVersion: "POSTGRES_17",
    ipv4Enabled: false,
    privateNetwork,
    allocatedIpRange,
    insertBody: {
      name: instanceName,
      region: input.region,
      databaseVersion: "POSTGRES_17",
      settings: {
        tier,
        availabilityType: input.highAvailability ? "REGIONAL" : "ZONAL",
        ipConfiguration,
      },
    },
    proxyArgs,
  };
}

export interface CloudSqlProxyWorkload {
  serviceAccount: {
    apiVersion: "v1";
    kind: "ServiceAccount";
    metadata: {
      name: "cloud-sql-proxy";
      namespace: "96bulls";
      annotations: { "iam.gke.io/gcp-service-account": string };
    };
  };
  service: {
    apiVersion: "v1";
    kind: "Service";
    metadata: { name: "cloud-sql-proxy"; namespace: "96bulls" };
    spec: { selector: { app: "cloud-sql-proxy" }; ports: [{ name: "postgres"; port: 5432; targetPort: 5432 }] };
  };
  deployment: {
    apiVersion: "apps/v1";
    kind: "Deployment";
    metadata: { name: "cloud-sql-proxy"; namespace: "96bulls" };
    spec: {
      replicas: 1;
      selector: { matchLabels: { app: "cloud-sql-proxy" } };
      template: {
        metadata: { labels: { app: "cloud-sql-proxy" } };
        spec: {
          serviceAccountName: "cloud-sql-proxy";
          containers: [
            {
              name: "cloud-sql-proxy";
              image: typeof CLOUD_SQL_PROXY_IMAGE;
              args: PrivateCloudSqlSpec["proxyArgs"];
              ports: [{ name: "postgres"; containerPort: 5432 }];
              readinessProbe: { tcpSocket: { port: 5432 }; periodSeconds: 10 };
              livenessProbe: { tcpSocket: { port: 5432 }; periodSeconds: 20 };
              securityContext: { runAsNonRoot: true; allowPrivilegeEscalation: false; readOnlyRootFilesystem: true };
            },
          ];
        };
      };
    };
  };
}

const GSA_EMAIL = /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

/**
 * Apply the proxy only after a resource posts `insertBody`. The pinned
 * provider never does, so this stays false and the stack must not create
 * a proxy for an instance that does not exist.
 */
export function cloudSqlProxyApplies(spec: PrivateCloudSqlSpec): boolean {
  return spec.posted !== false && spec.ipv4Enabled === false && spec.proxyArgs[0] === "--private-ip";
}

/** In-cluster proxy. The instance connection name is a positional arg, and `--private-ip` is the first flag. */
export function cloudSqlProxyWorkload(spec: PrivateCloudSqlSpec, workloadIdentityEmail: string): CloudSqlProxyWorkload {
  if (spec.posted !== false || spec.proxyArgs[0] !== "--private-ip" || spec.ipv4Enabled !== false)
    refuse("Cloud SQL proxy requires the private-only spec.");
  if (!GSA_EMAIL.test(workloadIdentityEmail))
    refuse("Cloud SQL proxy needs the runtime Workload Identity email, not a key.");
  return {
    serviceAccount: {
      apiVersion: "v1",
      kind: "ServiceAccount",
      metadata: {
        name: "cloud-sql-proxy",
        namespace: "96bulls",
        annotations: { "iam.gke.io/gcp-service-account": workloadIdentityEmail },
      },
    },
    service: {
      apiVersion: "v1",
      kind: "Service",
      metadata: { name: "cloud-sql-proxy", namespace: "96bulls" },
      spec: { selector: { app: "cloud-sql-proxy" }, ports: [{ name: "postgres", port: 5432, targetPort: 5432 }] },
    },
    deployment: {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: { name: "cloud-sql-proxy", namespace: "96bulls" },
      spec: {
        replicas: 1,
        selector: { matchLabels: { app: "cloud-sql-proxy" } },
        template: {
          metadata: { labels: { app: "cloud-sql-proxy" } },
          spec: {
            serviceAccountName: "cloud-sql-proxy",
            containers: [
              {
                name: "cloud-sql-proxy",
                image: CLOUD_SQL_PROXY_IMAGE,
                args: spec.proxyArgs,
                ports: [{ name: "postgres", containerPort: 5432 }],
                readinessProbe: { tcpSocket: { port: 5432 }, periodSeconds: 10 },
                livenessProbe: { tcpSocket: { port: 5432 }, periodSeconds: 20 },
                securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, readOnlyRootFilesystem: true },
              },
            ],
          },
        },
      },
    },
  };
}
