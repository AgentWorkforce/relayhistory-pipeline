/**
 * GCP reference lane for the self-hosted AgentWorkforce stack.
 *
 * This is code-only until Khaliq separately clears a project. It deliberately
 * keeps secret payloads out of Alchemy inputs/state. Relaycast stays
 * single-node SQLite. Managed GCS, private Cloud SQL (not posted through
 * GCP.SQL.Instance), Redis, and Pub/Sub remain adapter-test infrastructure;
 * 96Bulls installation is held until the Redis TLS client contract is proven.
 * Production stays Supabase and AWS S3.
 */
import * as Alchemy from "alchemy";
import type { Input } from "alchemy/Input";
import { Stage } from "alchemy";
import * as Output from "alchemy/Output";
import * as GCP from "@alchemy.run/gcp";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  GCP_CONFIRM_PROJECT_ENV,
  GCP_CONFIRM_KUBERNETES_APPLY_ENV,
  GCP_CONFIRM_SQL_POST_ENV,
  GCP_NODE_ROLES,
  GCP_RUNTIME_SHAPE,
  assertGcpInputs,
  assertGcpKubernetesApplyConfirmation,
  assertGcpRuntimeDeployable,
  assertGcpSqlPostConfirmation,
  bulls96ReleaseValues,
  gcpResourceName,
  pubsubServiceAgent,
  relayauthChartValues,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  verifyRelaycastChartPackage,
  type GcpInputs,
} from "./config.ts";
import { assertGcpDeploymentTarget, assertGcpProjectConfirmation } from "./deploy-guard.ts";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  cloudSqlProxyWorkload,
  ensurePrivateCloudSql,
  privateCloudSqlSpec,
  sqlAdminFetchTransport,
  type PrivateCloudSqlSpec,
} from "./cloud-sql.ts";
import { provePrivateKubernetesReachability } from "./kubernetes-preflight.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const DEFAULT_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";

interface EnsurePrivateCloudSqlInput {
  spec: PrivateCloudSqlSpec;
  /** Resolved PSA output: establishes that the peering exists before the POST. */
  privateServiceConnection: string;
  proxyPrincipal: string;
}

interface EnsurePrivateCloudSqlOutput {
  connectionName: string;
  proxyPrincipal: string;
  created: boolean;
  operation: string | null;
}

interface RequireKubernetesReachabilityInput {
  endpoint: string;
  executor: NonNullable<GcpInputs["kubernetesExecutor"]>;
}

const EnsurePrivateCloudSql = Alchemy.Action<
  "96Bulls.GCP.EnsurePrivateCloudSql",
  EnsurePrivateCloudSqlInput,
  EnsurePrivateCloudSqlOutput,
  GCP.GcpEnvironment
>("96Bulls.GCP.EnsurePrivateCloudSql", (input) =>
  Effect.gen(function* () {
    if (!input.privateServiceConnection)
      throw new Error("96bulls: private service connection was not resolved. Nothing was created.");
    const project = /^\/projects\/([^/]+)\/global\/networks\//.exec(input.spec.privateNetwork)?.[1];
    if (!project)
      throw new Error("96bulls: Cloud SQL poster project is malformed. Nothing was created.");
    assertGcpSqlPostConfirmation(
      project,
      input.spec.instanceName,
      process.env[GCP_CONFIRM_SQL_POST_ENV],
    );
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    if (project !== live.project)
      throw new Error("96bulls: Cloud SQL poster project differs from the resolved GCP project. Nothing was created.");
    const result = yield* Effect.tryPromise(() =>
      ensurePrivateCloudSql(
        input.spec,
        { bearer: Redacted.value(live.accessToken) },
        sqlAdminFetchTransport(),
      ),
    );
    return {
      connectionName: input.spec.connectionName,
      proxyPrincipal: input.proxyPrincipal,
      created: result.created,
      operation: result.operation,
    };
  }),
);

const RequireKubernetesReachability = Alchemy.Action<
  "96Bulls.GCP.RequireKubernetesReachability",
  RequireKubernetesReachabilityInput,
  Awaited<ReturnType<typeof provePrivateKubernetesReachability>>,
  GCP.GcpEnvironment
>("96Bulls.GCP.RequireKubernetesReachability", (input) =>
  Effect.tryPromise(() => {
    assertGcpKubernetesApplyConfirmation(
      input.executor.applyId,
      process.env[GCP_CONFIRM_KUBERNETES_APPLY_ENV],
    );
    return provePrivateKubernetesReachability(input.endpoint, input.executor);
  }),
);

function prepareRelaycastChart(stage: string): string {
  const artifact = verifyRelaycastChartPackage(
    join(ROOT, ".96bulls", stage, "artifacts", "relaycast-0.1.0.tgz"),
  );
  const extracted = mkdtempSync(join(tmpdir(), "96bulls-relaycast-0.1.0-"));
  try {
    execFileSync("tar", ["-xzf", artifact, "-C", extracted], { stdio: "ignore" });
    const chart = join(extracted, "relaycast");
    if (!statSync(chart).isDirectory())
      throw new Error("published package did not contain relaycast/");
    process.once("exit", () => rmSync(extracted, { recursive: true, force: true }));
    return chart;
  } catch {
    rmSync(extracted, { recursive: true, force: true });
    throw new Error("96bulls: verified Relaycast package could not be extracted. Nothing was created.");
  }
}

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

const managedCertificate = (
  name: string,
  namespace: string,
  domain: string,
  reachability: Input<string>,
) => ({
  apiVersion: "networking.gke.io/v1",
  kind: "ManagedCertificate",
  metadata: {
    name,
    namespace,
    annotations: { "96bulls.agentworkforce.dev/reachability": reachability },
  },
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
    // Verify and freeze the exact published package before provider resolution
    // or any resource declaration. Helm renders the extracted immutable bytes.
    const relaycastChart = i.relaycast ? prepareRelaycastChart(stage) : null;
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
    const natAddress = yield* GCP.Compute.Address("nat-address", {
      addressName: gcpResourceName(stage, "nat", 63),
      region: i.region,
      addressType: "EXTERNAL",
      networkTier: "PREMIUM",
      description: `AgentWorkforce ${stage} auditable NAT address`,
      labels: labels(stage, "nat"),
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
          natIpAllocateOption: GCP_RUNTIME_SHAPE.natIpAllocateOption,
          natIps: [natAddress.selfLink.as<string>()],
          logConfig: { enable: true, filter: GCP_RUNTIME_SHAPE.natLogFilter },
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
    // Project-local Private Service Access peering for this isolated project.
    const privateServices = yield* GCP.ServiceNetworking.Connection("private-services", {
      network: network.networkName,
      reservedPeeringRanges: [psaRange.addressName],
      deletionPolicy: "DELETE",
    });

    const nodeAccountId = gcpResourceName(stage, "nodes", 30);
    const nodeIdentity = yield* GCP.IAM.ServiceAccount("node-identity", {
      accountId: nodeAccountId,
      displayName: `AgentWorkforce ${stage} nodes`,
      description: "Dedicated GKE node identity. No keys.",
    });
    const nodeMember = Output.interpolate`serviceAccount:${nodeIdentity.email}`;
    for (const role of GCP_NODE_ROLES) {
      yield* GCP.IAM.Member(`node-${role.split("/")[1]!.replaceAll(".", "-")}`, {
        kind: "project",
        name: i.projectId,
        role,
        member: nodeMember,
      });
    }

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
        enablePrivateEndpoint: GCP_RUNTIME_SHAPE.privateEndpoint,
        masterIpv4CidrBlock: "172.20.0.0/28",
        masterGlobalAccessConfig: { enabled: false },
      },
      masterAuthorizedNetworksConfig: {
        enabled: true,
        cidrBlocks: i.controlPlaneAuthorizedCidrs,
      },
      // The provider omits node pools when autopilot is true, so this stays false.
      autopilot: GCP_RUNTIME_SHAPE.autopilot,
      serviceAccount: nodeIdentity.email,
      machineType: GCP_RUNTIME_SHAPE.machineType,
      initialNodeCount: GCP_RUNTIME_SHAPE.initialNodeCount,
      nodeLocations: [GCP_RUNTIME_SHAPE.nodeLocation],
      oauthScopes: [GCP_RUNTIME_SHAPE.nodeOauthScope],
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
          // The apply-time Action below uses the exact private SQL Admin body instead.
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
          const sqlPoster = yield* EnsurePrivateCloudSql("private-cloud-sql", {
            spec: sql,
            privateServiceConnection: privateServices.name,
            proxyPrincipal: workloadIdentityPrincipal(i, "96bulls", "cloud-sql-proxy"),
          });
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
            sqlPoster,
            redis: { host: redis.host, port: redis.port },
            pubsub: {
              topic: eventsTopicName,
              subscription: eventsSubscriptionName,
              deadLetters: deadLetters.name,
            },
          };
        })
      : null;

    const kubernetesRequested = Boolean(
      i.relaycast || i.relayauth || i.relayfile || i.relayflows || i.bulls96?.enabled,
    );
    // This Action must succeed during apply before execution can advance to
    // the first Kubernetes resource. It proves this named executor can route
    // to the concrete private control-plane endpoint; it sends no credential.
    const kubernetesReachability = kubernetesRequested
      ? yield* RequireKubernetesReachability("kubernetes-reachability", {
          endpoint: cluster.endpoint.as<string>(),
          executor: i.kubernetesExecutor!,
        })
      : null;

    const relaycastConfig = i.relaycast;
    const relaycast = relaycastConfig
      ? yield* Effect.gen(function* () {
          yield* Kubernetes.Manifest("relaycast-namespace", {
            cluster,
            manifest: {
              apiVersion: "v1",
              kind: "Namespace",
              metadata: {
                name: "relaycast",
                annotations: { "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof },
              },
            },
          });
          return yield* Kubernetes.HelmChart("relaycast", {
            cluster,
            chart: relaycastChart!,
            releaseName: "relaycast",
            namespace: "relaycast",
            values: {
              ...relaycastChartValues(relaycastConfig, storageClass),
              podAnnotations: {
                "96bulls.agentworkforce.dev/executor": kubernetesReachability!.executor,
                "96bulls.agentworkforce.dev/apply-id": kubernetesReachability!.applyId,
                "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof,
              },
            },
          });
        })
      : null;

    const relayauthConfig = i.relayauth;
    const relayauth = relayauthConfig
      ? yield* Effect.gen(function* () {
          yield* Kubernetes.Manifest("relayauth-namespace", {
            cluster,
            manifest: {
              apiVersion: "v1",
              kind: "Namespace",
              metadata: {
                name: "relayauth",
                annotations: { "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof },
              },
            },
          });
          if (relayauthConfig.baseUrl)
            yield* Kubernetes.Manifest("relayauth-certificate", {
              cluster,
              manifest: managedCertificate(
                "relayauth",
                "relayauth",
                new URL(relayauthConfig.baseUrl).hostname,
                kubernetesReachability!.proof,
              ),
            });
          return yield* Kubernetes.HelmChart("relayauth", {
            cluster,
            chart: "relayauth",
            repo: chartRepo,
            version: relayauthConfig.chartVersion,
            releaseName: "relayauth",
            namespace: Output.map(kubernetesReachability!.proof, () => "relayauth"),
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
          namespace: Output.map(kubernetesReachability!.proof, () => "relayfile"),
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
          namespace: Output.map(kubernetesReachability!.proof, () => "relayflows"),
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
            manifest: {
              apiVersion: "v1",
              kind: "Namespace",
              metadata: {
                name: "96bulls",
                annotations: { "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof },
              },
            },
          });
          // The poster output is an explicit dependency: neither this IAM
          // edge nor the proxy deployment can apply before Cloud SQL is
          // confirmed RUNNABLE with the exact private-only body.
          yield* GCP.IAM.Member("workload-identity-96bulls-cloud-sql-proxy", {
            kind: "iam.serviceAccount",
            name: runtimeIdentity.name,
            role: "roles/iam.workloadIdentityUser",
            member: dataPlane.sqlPoster.proxyPrincipal,
          });
          const proxy = cloudSqlProxyWorkload(
            dataPlane.sql,
            runtimeEmail,
            dataPlane.sqlPoster.connectionName,
          );
          const reachabilityAnnotation = {
            "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof,
          };
          yield* Kubernetes.Manifest("cloud-sql-proxy-account", {
            cluster,
            manifest: {
              ...proxy.serviceAccount,
              metadata: {
                ...proxy.serviceAccount.metadata,
                annotations: { ...proxy.serviceAccount.metadata.annotations, ...reachabilityAnnotation },
              },
            },
          });
          yield* Kubernetes.Manifest("cloud-sql-proxy-service", {
            cluster,
            manifest: {
              ...proxy.service,
              metadata: { ...proxy.service.metadata, annotations: reachabilityAnnotation },
            },
          });
          yield* Kubernetes.Manifest("cloud-sql-proxy", {
            cluster,
            manifest: {
              ...proxy.deployment,
              metadata: { ...proxy.deployment.metadata, annotations: reachabilityAnnotation },
            },
          });
          const release = yield* Kubernetes.HelmChart("96bulls", {
            cluster,
            chart: "96bulls",
            repo: chartRepo,
            version: i.bulls96!.chartVersion,
            releaseName: "96bulls",
            namespace: Output.map(kubernetesReachability!.proof, () => "96bulls"),
            values: bulls96ReleaseValues(i.bulls96!, {
              bucket: lockerBucket,
              region: i.region,
              cloudSqlConnectionName: dataPlane.sqlPoster.connectionName,
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
