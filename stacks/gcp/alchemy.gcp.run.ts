/**
 * GCP reference lane for the self-hosted AgentWorkforce stack.
 *
 * This is code-only until Khaliq separately clears a project. It deliberately
 * keeps secret payloads out of Alchemy inputs/state and refuses unsupported
 * runtime claims: Relaycast is single-node SQLite, and 96Bulls remains blocked
 * until its service wiring can select the existing GCS object-store adapter.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as GCP from "@alchemy.run/gcp";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  GCP_CONFIRM_PROJECT_ENV,
  assertGcpInputs,
  relayauthChartValues,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  type GcpInputs,
} from "./config.ts";
import { assertGcpDeploymentTarget, assertGcpProjectConfirmation } from "./deploy-guard.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const DEFAULT_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";
const CLOUD_SQL_PROXY_IMAGE =
  "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.18.2@sha256:fc224915ef435afeb5b2a9421260a0d31986d5c8b7c7f5783c7f5d5885700cd2";

function readInputs(stage: string): GcpInputs {
  const path = join(ROOT, ".96bulls", stage, "gcp-inputs.json");
  let inputs: GcpInputs;
  try {
    inputs = JSON.parse(readFileSync(path, "utf8")) as GcpInputs;
  } catch {
    throw new Error(`96bulls: GCP stage ${stage} is not cleared: ${path} does not exist. Nothing was created.`);
  }
  assertGcpInputs(stage, inputs);
  assertGcpProjectConfirmation(inputs.projectId, process.env[GCP_CONFIRM_PROJECT_ENV]);
  return inputs;
}

const compactName = (stage: string, suffix: string, max = 40) =>
  `awf-${stage.slice("gcp-".length)}-${suffix}`.toLowerCase().replace(/[^a-z0-9-]/g, "-").replace(/-+/g, "-").slice(0, max).replace(/-$/, "");

const labels = (stage: string, component: string) => ({
  stack: "agentworkforce",
  stage: stage.replace(/[^a-z0-9_-]/g, "_"),
  component: component.replace(/[^a-z0-9_-]/g, "_"),
});

const workloadIdentityPrincipal = (i: GcpInputs, namespace: string, serviceAccount: string) =>
  `principal://iam.googleapis.com/projects/${i.projectNumber}/locations/global/workloadIdentityPools/${i.projectId}.svc.id.goog/subject/ns/${namespace}/sa/${serviceAccount}`;

const managedCertificate = (name: string, namespace: string, domain: string) => ({
  apiVersion: "networking.gke.io/v1",
  kind: "ManagedCertificate",
  metadata: { name, namespace },
  spec: { domains: [domain] },
});

export default Alchemy.Stack(
  "agentworkforce-gcp",
  {
    providers: Layer.mergeAll(GCP.providers(), Kubernetes.providers()),
    state: GCP.state({ name: "agentworkforce", prefix: "96bulls" }),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    const i = readInputs(stage);
    // Before the first resource. Confirmation is operator intent; the resolved
    // GCP environment is the project and region these credentials will use.
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    assertGcpDeploymentTarget(
      { projectId: i.projectId, region: i.region },
      { projectId: live.project, region: live.region },
    );
    const chartRepo = i.chartRepo ?? DEFAULT_CHART_REPO;
    const storageClass = i.chartStorageClass ?? "standard-rwo";

    // Keep project-level APIs enabled on destroy because they may be shared by
    // other stacks. Service Usage itself is the one bootstrap prerequisite.
    for (const service of [
      "artifactregistry.googleapis.com",
      "compute.googleapis.com",
      "container.googleapis.com",
      "iam.googleapis.com",
      "iamcredentials.googleapis.com",
      "pubsub.googleapis.com",
      "redis.googleapis.com",
      "secretmanager.googleapis.com",
      "servicenetworking.googleapis.com",
      "sqladmin.googleapis.com",
    ]) {
      yield* GCP.ServiceUsage.ProjectService(`api-${service.split(".")[0]}`, {
        project: i.projectId,
        service,
        disableOnDestroy: false,
      });
    }

    const network = yield* GCP.Compute.Network("network", {
      networkName: compactName(stage, "vpc", 63),
      autoCreateSubnetworks: false,
      routingMode: "REGIONAL",
      description: `AgentWorkforce ${stage} private network`,
    });
    const subnet = yield* GCP.Compute.Subnetwork("subnet", {
      subnetworkName: compactName(stage, "private", 63),
      region: i.region,
      network: network.networkName,
      ipCidrRange: "10.52.0.0/20",
      privateIpGoogleAccess: true,
      secondaryIpRanges: [
        { rangeName: "gke-pods", ipCidrRange: "10.60.0.0/14" },
        { rangeName: "gke-services", ipCidrRange: "10.64.0.0/20" },
      ],
      logConfig: { enable: true, aggregationInterval: "INTERVAL_5_SEC", flowSampling: 0.5, metadata: "INCLUDE_ALL_METADATA" },
    });
    yield* GCP.Compute.Router("nat", {
      routerName: compactName(stage, "nat", 63),
      region: i.region,
      network: network.networkName,
      description: `AgentWorkforce ${stage} private-node egress`,
      nats: [
        {
          name: compactName(stage, "egress", 63),
          sourceSubnetworkIpRangesToNat: "LIST_OF_SUBNETWORKS",
          subnetworks: [{ name: subnet.subnetworkName, sourceIpRangesToNat: ["ALL_IP_RANGES"] }],
          natIpAllocateOption: "AUTO_ONLY",
          logConfig: { enable: true, filter: "ERRORS_ONLY" },
        },
      ],
    });

    const psaRange = yield* GCP.Compute.GlobalAddress("private-services-range", {
      addressName: compactName(stage, "psa", 63),
      addressType: "INTERNAL",
      purpose: "VPC_PEERING",
      network: network.networkName,
      prefixLength: 20,
      description: `AgentWorkforce ${stage} managed-service peering`,
    });
    yield* GCP.ServiceNetworking.Connection("private-services", {
      network: network.networkName,
      reservedPeeringRanges: [psaRange.addressName],
      deletionPolicy: "DELETE",
    });

    const cluster = yield* GCP.Container.Cluster("cluster", {
      clusterId: compactName(stage, "gke"),
      location: i.region,
      description: `AgentWorkforce ${stage}`,
      labels: labels(stage, "gke"),
      network: network.networkName,
      subnetwork: subnet.subnetworkName,
      ipAllocationPolicy: {
        useIpAliases: true,
        clusterSecondaryRangeName: "gke-pods",
        servicesSecondaryRangeName: "gke-services",
      },
      privateClusterConfig: {
        enablePrivateNodes: true,
        enablePrivateEndpoint: false,
        masterIpv4CidrBlock: "172.20.0.0/28",
        masterGlobalAccessConfig: { enabled: false },
      },
      masterAuthorizedNetworksConfig: {
        enabled: true,
        cidrBlocks: i.controlPlaneAuthorizedCidrs,
      },
      autopilot: true,
      datapathProvider: "ADVANCED_DATAPATH",
      enableShieldedNodes: true,
      releaseChannel: "STABLE",
      loggingService: "logging.googleapis.com/kubernetes",
      monitoringService: "monitoring.googleapis.com/kubernetes",
      deletionProtection: stage === "gcp-prod",
    });

    const locker = yield* GCP.Storage.Bucket("locker", {
      bucketName: `${compactName(stage, "locker", 30)}-${i.projectId}`.slice(0, 63).replace(/-$/, ""),
      location: i.region,
      storageClass: "STANDARD",
      versioning: true,
      uniformBucketLevelAccess: true,
      forceDestroy: false,
      labels: labels(stage, "locker"),
    });
    const runtimeSecret = yield* GCP.SecretManager.Secret("runtime-secret", {
      secretId: compactName(stage, "runtime", 63),
      labels: labels(stage, "runtime"),
      replication: { userManaged: { replicas: [{ location: i.region }] } },
      versionDestroyTtl: "604800s",
    });
    const images = yield* GCP.ArtifactRegistry.Repository("images", {
      repositoryId: compactName(stage, "images", 63),
      location: i.region,
      format: "DOCKER",
      description: `AgentWorkforce ${stage} private images`,
      labels: labels(stage, "images"),
      dockerConfig: { immutableTags: true },
    });

    const runtimeIdentity = yield* GCP.IAM.ServiceAccount("runtime-identity", {
      accountId: compactName(stage, "runtime", 30),
      displayName: `AgentWorkforce ${stage} runtime`,
      description: "Workload Identity target for managed data-plane clients",
    });
    for (const [namespace, serviceAccount] of [
      ["96bulls", "96bulls-worker"],
      ["96bulls", "96bulls-alleyoop"],
      ["agentworkforce-system", "cloud-sql-proxy"],
    ] as const) {
      yield* GCP.IAM.Member(`workload-identity-${namespace}-${serviceAccount}`, {
        kind: "iam.serviceAccount",
        name: runtimeIdentity.name,
        role: "roles/iam.workloadIdentityUser",
        member: workloadIdentityPrincipal(i, namespace, serviceAccount),
      });
    }
    yield* GCP.IAM.Member("runtime-locker", {
      kind: "storage.bucket",
      name: locker.bucketName,
      role: "roles/storage.objectUser",
      member: `serviceAccount:${runtimeIdentity.email}`,
    });
    yield* GCP.IAM.Member("runtime-secret-access", {
      kind: "secretmanager.secret",
      name: runtimeSecret.name,
      role: "roles/secretmanager.secretAccessor",
      member: `serviceAccount:${runtimeIdentity.email}`,
    });
    yield* GCP.IAM.Member("runtime-cloudsql", {
      kind: "project",
      name: i.projectId,
      role: "roles/cloudsql.client",
      member: `serviceAccount:${runtimeIdentity.email}`,
    });

    const dataPlane = i.managedDataPlane.provision
      ? yield* Effect.gen(function* () {
          const sql = yield* GCP.SQL.Instance("postgres", {
            instanceName: compactName(stage, "postgres", 63),
            region: i.region,
            databaseVersion: "POSTGRES_17",
            tier: i.managedDataPlane.cloudSqlTier ?? "db-custom-2-7680",
            availabilityType: i.managedDataPlane.cloudSqlHa ? "REGIONAL" : "ZONAL",
            backupEnabled: true,
            dataDiskSizeGb: 20,
            dataDiskType: "PD_SSD",
            ipv4Enabled: true,
            deletionProtectionEnabled: stage === "gcp-prod",
            labels: labels(stage, "postgres"),
            databaseFlags: [{ name: "cloudsql.iam_authentication", value: "on" }],
          });
          const hoopdb = yield* GCP.SQL.Database("hoopdb", {
            instance: sql.instanceName,
            databaseName: "hoopdb",
          });
          const relayfile = yield* GCP.SQL.Database("relayfile-database", {
            instance: sql.instanceName,
            databaseName: "relayfile",
          });
          const redis = yield* GCP.Redis.Instance("redis", {
            instanceId: compactName(stage, "redis"),
            location: i.region,
            displayName: `AgentWorkforce ${stage}`,
            labels: labels(stage, "redis"),
            redisVersion: "REDIS_7_2",
            tier: i.managedDataPlane.redisHa ? "STANDARD_HA" : "BASIC",
            memorySizeGb: i.managedDataPlane.redisMemoryGb ?? 1,
            authorizedNetwork: network.networkName,
            connectMode: "PRIVATE_SERVICE_ACCESS",
            reservedIpRange: psaRange.addressName,
            authEnabled: true,
            transitEncryptionMode: "SERVER_AUTHENTICATION",
            persistenceConfig: i.managedDataPlane.redisHa
              ? { persistenceMode: "RDB", rdbSnapshotPeriod: "SIX_HOURS" }
              : { persistenceMode: "DISABLED" },
          });
          const deadLetters = yield* GCP.PubSub.Topic("dead-letters", {
            topicId: compactName(stage, "dead-letters", 63),
            labels: labels(stage, "queue"),
            messageRetentionDuration: "604800s",
          });
          const events = yield* GCP.PubSub.Topic("events", {
            topicId: compactName(stage, "events", 63),
            labels: labels(stage, "queue"),
            messageRetentionDuration: "604800s",
          });
          const subscription = yield* GCP.PubSub.Subscription("events-worker", {
            subscriptionId: compactName(stage, "events-worker", 63),
            topic: events.name,
            labels: labels(stage, "queue"),
            ackDeadlineSeconds: 60,
            messageRetentionDuration: "604800s",
            enableExactlyOnceDelivery: true,
            retryPolicy: { minimumBackoff: "10s", maximumBackoff: "600s" },
            deadLetterPolicy: { deadLetterTopic: deadLetters.name, maxDeliveryAttempts: 10 },
            expirationPolicy: {},
          });
          for (const [id, kind, name, role] of [
            ["events-publish", "pubsub.topic", events.name, "roles/pubsub.publisher"],
            ["events-consume", "pubsub.subscription", subscription.name, "roles/pubsub.subscriber"],
          ] as const) {
            yield* GCP.IAM.Member(`runtime-${id}`, {
              kind,
              name,
              role,
              member: `serviceAccount:${runtimeIdentity.email}`,
            });
          }

          yield* Kubernetes.Manifest("system-namespace", {
            cluster,
            manifest: { apiVersion: "v1", kind: "Namespace", metadata: { name: "agentworkforce-system" } },
          });
          const proxy = yield* Kubernetes.Deployment("cloud-sql-proxy", {
            cluster,
            name: "cloud-sql-proxy",
            namespace: "agentworkforce-system",
            image: CLOUD_SQL_PROXY_IMAGE,
            replicas: 2,
            port: 5432,
            serviceType: "ClusterIP",
            args: ["--address=0.0.0.0", "--port=5432", sql.connectionName.as<string>()],
            identity: { gcpServiceAccount: runtimeIdentity.email },
            resources: {
              requests: { cpu: "100m", memory: "128Mi" },
              limits: { cpu: "1", memory: "512Mi" },
            },
            podTemplate: {
              spec: {
                automountServiceAccountToken: true,
                securityContext: { runAsNonRoot: true, seccompProfile: { type: "RuntimeDefault" } },
              },
            },
          });
          return {
            sql: { connectionName: sql.connectionName, hoopdb: hoopdb.databaseName, relayfile: relayfile.databaseName, proxyService: proxy.serviceName },
            redis: { host: redis.host, port: redis.port },
            pubsub: { topic: events.name, subscription: subscription.name, deadLetters: deadLetters.name },
          };
        })
      : null;

    const relaycastConfig = i.relaycast;
    const relaycast = relaycastConfig
      ? yield* Effect.gen(function* () {
          yield* Kubernetes.Manifest("relaycast-namespace", {
            cluster,
            manifest: { apiVersion: "v1", kind: "Namespace", metadata: { name: "relaycast" } },
          });
          yield* Kubernetes.Manifest("relaycast-certificate", {
            cluster,
            manifest: managedCertificate("relaycast", "relaycast", new URL(relaycastConfig.baseUrl).hostname),
          });
          return yield* Kubernetes.HelmChart("relaycast", {
            cluster,
            chart: "relaycast",
            repo: chartRepo,
            version: relaycastConfig.chartVersion,
            releaseName: "relaycast",
            namespace: "relaycast",
            values: relaycastChartValues(relaycastConfig, storageClass),
          });
        })
      : null;

    const relayauthConfig = i.relayauth;
    const relayauth = relayauthConfig
      ? yield* Effect.gen(function* () {
          yield* Kubernetes.Manifest("relayauth-namespace", {
            cluster,
            manifest: { apiVersion: "v1", kind: "Namespace", metadata: { name: "relayauth" } },
          });
          if (relayauthConfig.baseUrl)
            yield* Kubernetes.Manifest("relayauth-certificate", {
              cluster,
              manifest: managedCertificate("relayauth", "relayauth", new URL(relayauthConfig.baseUrl).hostname),
            });
          return yield* Kubernetes.HelmChart("relayauth", {
            cluster,
            chart: "relayauth",
            repo: chartRepo,
            version: relayauthConfig.chartVersion,
            releaseName: "relayauth",
            namespace: "relayauth",
            values: relayauthChartValues(relayauthConfig, storageClass),
          });
        })
      : null;

    const relayfile = i.relayfile
      ? yield* Kubernetes.HelmChart("relayfile", {
          cluster,
          chart: "relayfile",
          repo: chartRepo,
          version: i.relayfile.chartVersion,
          releaseName: "relayfile",
          namespace: "relayfile",
          createNamespace: true,
          values: relayfileChartValues(i.relayfile),
        })
      : null;

    const relayflows = i.relayflows
      ? yield* Kubernetes.HelmChart("relayflows", {
          cluster,
          chart: "relayflows",
          repo: chartRepo,
          version: i.relayflows.chartVersion,
          releaseName: "relayflows",
          namespace: "relayflows",
          createNamespace: true,
          values: relayflowsChartValues(i.relayflows, storageClass),
        })
      : null;

    return {
      stage,
      projectId: i.projectId,
      region: i.region,
      cluster: { name: cluster.clusterId, endpoint: cluster.endpoint, workloadPool: cluster.workloadPool },
      locker: { bucket: locker.bucketName },
      runtimeSecret: runtimeSecret.name,
      runtimeIdentity: runtimeIdentity.email,
      imageRegistry: images.registryUri,
      managedDataPlane: dataPlane,
      charts: {
        relaycast: relaycast?.releaseName ?? null,
        relayauth: relayauth?.releaseName ?? null,
        relayfile: relayfile?.releaseName ?? null,
        relayflows: relayflows?.releaseName ?? null,
        bulls96: null,
      },
    };
  }),
);
