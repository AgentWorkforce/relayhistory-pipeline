/**
 * GCP reference lane for the self-hosted AgentWorkforce stack.
 *
 * This is code-only until Khaliq separately clears a project. It deliberately
 * keeps secret payloads out of Alchemy inputs/state. Relaycast stays
 * single-node SQLite. 96Bulls installs only as conformance wiring: GCS,
 * private Cloud SQL (not posted through GCP.SQL.Instance), Redis, and Pub/Sub.
 * Production stays Supabase and AWS S3.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as Output from "alchemy/Output";
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
  assertGcpRuntimeDeployable,
  bulls96ReleaseValues,
  gcpResourceName,
  pubsubServiceAgent,
  relayauthChartValues,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  type GcpInputs,
} from "./config.ts";
import { assertGcpDeploymentTarget, assertGcpProjectConfirmation } from "./deploy-guard.ts";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  cloudSqlProxyApplies,
  cloudSqlProxyWorkload,
  privateCloudSqlSpec,
  sqlAdminInsertRequest,
} from "./cloud-sql.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const DEFAULT_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";

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
    assertGcpRuntimeDeployable(i);
    // Before the first resource. Confirmation is operator intent; the resolved
    // GCP environment is the project and region these credentials will use.
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    assertGcpDeploymentTarget(
      { projectId: i.projectId, region: i.region },
      { projectId: live.project, region: live.region },
    );
    const chartRepo = i.chartRepo ?? DEFAULT_CHART_REPO;
    const storageClass = i.chartStorageClass ?? "standard-rwo";
    const networkName = gcpResourceName(stage, "vpc", 63);
    const psaName = gcpResourceName(stage, "psa", 63);
    const runtimeAccountId = gcpResourceName(stage, "runtime", 30);
    const runtimeEmail = `${runtimeAccountId}@${i.projectId}.iam.gserviceaccount.com`;
    const lockerBucket = `${gcpResourceName(stage, "locker", 30)}-${i.projectId}`.slice(0, 63).replace(/-$/, "");
    const eventsTopicId = gcpResourceName(stage, "events", 63);
    const eventsSubscriptionId = gcpResourceName(stage, "events-worker", 63);
    const eventsTopicName = `projects/${i.projectId}/topics/${eventsTopicId}`;
    const eventsSubscriptionName = `projects/${i.projectId}/subscriptions/${eventsSubscriptionId}`;

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
      networkName,
      autoCreateSubnetworks: false,
      routingMode: "REGIONAL",
      description: `AgentWorkforce ${stage} private network`,
    });
    const subnet = yield* GCP.Compute.Subnetwork("subnet", {
      subnetworkName: gcpResourceName(stage, "private", 63),
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
      routerName: gcpResourceName(stage, "nat", 63),
      region: i.region,
      network: network.networkName,
      description: `AgentWorkforce ${stage} private-node egress`,
      nats: [
        {
          name: gcpResourceName(stage, "egress", 63),
          sourceSubnetworkIpRangesToNat: "LIST_OF_SUBNETWORKS",
          subnetworks: [{ name: subnet.subnetworkName, sourceIpRangesToNat: ["ALL_IP_RANGES"] }],
          natIpAllocateOption: "AUTO_ONLY",
          logConfig: { enable: true, filter: "ERRORS_ONLY" },
        },
      ],
    });

    const psaRange = yield* GCP.Compute.GlobalAddress("private-services-range", {
      addressName: psaName,
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
      clusterId: gcpResourceName(stage, "gke"),
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
      bucketName: lockerBucket,
      location: i.region,
      storageClass: "STANDARD",
      versioning: true,
      uniformBucketLevelAccess: true,
      forceDestroy: false,
      labels: labels(stage, "locker"),
    });
    const runtimeSecret = yield* GCP.SecretManager.Secret("runtime-secret", {
      secretId: gcpResourceName(stage, "runtime", 63),
      labels: labels(stage, "runtime"),
      replication: { userManaged: { replicas: [{ location: i.region }] } },
      versionDestroyTtl: "604800s",
    });
    const images = yield* GCP.ArtifactRegistry.Repository("images", {
      repositoryId: gcpResourceName(stage, "images", 63),
      location: i.region,
      format: "DOCKER",
      description: `AgentWorkforce ${stage} private images`,
      labels: labels(stage, "images"),
      dockerConfig: { immutableTags: true },
    });

    const runtimeIdentity = yield* GCP.IAM.ServiceAccount("runtime-identity", {
      accountId: runtimeAccountId,
      displayName: `AgentWorkforce ${stage} runtime`,
      description: "Workload Identity target for managed data-plane clients",
    });
    // email is an Output until deploy. A JS template coerces it and throws.
    const runtimeMember = Output.interpolate`serviceAccount:${runtimeIdentity.email}`;
    // The proxy Kubernetes service account is created only once Cloud SQL is
    // posted. Binding it earlier would let a later account of that name assume
    // the runtime identity.
    for (const [namespace, serviceAccount] of [
      ["96bulls", "96bulls-worker"],
      ["96bulls", "96bulls-alleyoop"],
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
      member: runtimeMember,
    });
    yield* GCP.IAM.Member("runtime-cloudsql", {
      kind: "project",
      name: i.projectId,
      role: "roles/cloudsql.client",
      member: runtimeMember,
    });

    const dataPlane = i.managedDataPlane.provision
      ? yield* Effect.gen(function* () {
          // GCP.SQL.Instance would drop privateNetwork and can post a public instance.
          // It is not called. The SQL Admin request is built and not sent.
          if (ALCHEMY_GCP_BETA_84_SQL.privateNetwork)
            throw new Error(
              "96bulls: Alchemy reports privateNetwork support. Review the insert body before posting it. Nothing was created.",
            );
          const sql = privateCloudSqlSpec({
            projectId: i.projectId,
            region: i.region,
            instanceName: gcpResourceName(stage, "postgres", 63),
            networkName,
            allocatedIpRange: psaName,
            tier: i.managedDataPlane.cloudSqlTier,
            highAvailability: i.managedDataPlane.cloudSqlHa === true,
          });
          const sqlInsert = sqlAdminInsertRequest(sql);
          if (sqlInsert.sent !== false || sqlInsert.method !== "POST")
            throw new Error("96bulls: Cloud SQL insert seam tried to send. Nothing was created.");
          const redis = yield* GCP.Redis.Instance("redis", {
            instanceId: gcpResourceName(stage, "redis"),
            location: i.region,
            displayName: `AgentWorkforce ${stage}`,
            labels: labels(stage, "redis"),
            redisVersion: "REDIS_7_2",
            tier: i.managedDataPlane.redisHa ? "STANDARD_HA" : "BASIC",
            memorySizeGb: i.managedDataPlane.redisMemoryGb ?? 1,
            authorizedNetwork: network.networkName,
            connectMode: "PRIVATE_SERVICE_ACCESS",
            reservedIpRange: psaRange.addressName,
            // The workload probe rejects an inline AUTH string, and this stack
            // does not write secret payloads. TLS server authentication stays on.
            authEnabled: false,
            transitEncryptionMode: "SERVER_AUTHENTICATION",
            persistenceConfig: i.managedDataPlane.redisHa
              ? { persistenceMode: "RDB", rdbSnapshotPeriod: "SIX_HOURS" }
              : { persistenceMode: "DISABLED" },
          });
          const deadLetters = yield* GCP.PubSub.Topic("dead-letters", {
            topicId: gcpResourceName(stage, "dead-letters", 63),
            labels: labels(stage, "queue"),
            messageRetentionDuration: "604800s",
          });
          const events = yield* GCP.PubSub.Topic("events", {
            topicId: eventsTopicId,
            labels: labels(stage, "queue"),
            messageRetentionDuration: "604800s",
          });
          const subscription = yield* GCP.PubSub.Subscription("events-worker", {
            subscriptionId: eventsSubscriptionId,
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
              member: runtimeMember,
            });
          }
          const pubsubAgent = `serviceAccount:${pubsubServiceAgent(i.projectNumber)}`;
          for (const [id, kind, name, role] of [
            ["dead-letter-publish", "pubsub.topic", deadLetters.name, "roles/pubsub.publisher"],
            ["dead-letter-consume", "pubsub.subscription", subscription.name, "roles/pubsub.subscriber"],
          ] as const) {
            yield* GCP.IAM.Member(`pubsub-${id}`, {
              kind,
              name,
              role,
              member: pubsubAgent,
            });
          }

          return {
            sql,
            sqlInsert,
            redis: { host: redis.host, port: redis.port },
            pubsub: {
              topic: eventsTopicName,
              subscription: eventsSubscriptionName,
              deadLetters: deadLetters.name,
            },
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

    const bulls96 = i.bulls96?.enabled
      ? yield* Effect.gen(function* () {
          if (!dataPlane)
            throw new Error("96bulls: 96Bulls install requires the managed data plane. Nothing was created.");
          yield* Kubernetes.Manifest("96bulls-namespace", {
            cluster,
            manifest: { apiVersion: "v1", kind: "Namespace", metadata: { name: "96bulls" } },
          });
          // The private body is specified. The proxy is rendered by cloudSqlProxyWorkload
          // and applied only once that body is posted. This pin does not post it.
          if (cloudSqlProxyApplies(dataPlane.sql)) {
            yield* GCP.IAM.Member("workload-identity-96bulls-cloud-sql-proxy", {
              kind: "iam.serviceAccount",
              name: runtimeIdentity.name,
              role: "roles/iam.workloadIdentityUser",
              member: workloadIdentityPrincipal(i, "96bulls", "cloud-sql-proxy"),
            });
            const proxy = cloudSqlProxyWorkload(dataPlane.sql, runtimeEmail);
            yield* Kubernetes.Manifest("cloud-sql-proxy-account", { cluster, manifest: proxy.serviceAccount });
            yield* Kubernetes.Manifest("cloud-sql-proxy-service", { cluster, manifest: proxy.service });
            yield* Kubernetes.Manifest("cloud-sql-proxy", { cluster, manifest: proxy.deployment });
          }
          const release = yield* Kubernetes.HelmChart("96bulls", {
            cluster,
            chart: "96bulls",
            repo: chartRepo,
            version: i.bulls96!.chartVersion,
            releaseName: "96bulls",
            namespace: "96bulls",
            values: bulls96ReleaseValues(i.bulls96!, {
              bucket: lockerBucket,
              region: i.region,
              cloudSqlConnectionName: dataPlane.sql.connectionName,
              redisHost: dataPlane.redis.host,
              redisPort: dataPlane.redis.port,
              pubsubTopic: eventsTopicName,
              pubsubSubscription: eventsSubscriptionName,
              workloadIdentityEmail: runtimeEmail,
            }),
          });
          return release.releaseName;
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
        bulls96: bulls96 ?? null,
      },
    };
  }),
);
