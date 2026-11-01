/**
 * GCP reference lane for the self-hosted AgentWorkforce stack.
 *
 * This is code-only until Khaliq separately clears a project. It deliberately
 * keeps secret payloads out of Alchemy inputs/state. Relaycast stays
 * single-node SQLite. Managed GCS, private Cloud SQL (not posted through
 * GCP.SQL.Instance), Redis, and Pub/Sub remain adapter-test infrastructure.
 * The optional 96Bulls release resolves Memorystore's public CA and requires
 * a live TLS PING in workload readiness; secret payloads remain external.
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
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  GCP_CONFIRM_PROJECT_ENV,
  GCP_CONFIRM_SQL_POST_ENV,
  GCP_APPLY_NONCE_ENV,
  GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS,
  GCP_NODE_ROLES,
  GCP_RUNTIME_SHAPE,
  assertGcpInputs,
  assertGcpRuntimeDeployable,
  assertGcpSqlPostConfirmation,
  bulls96ReleaseValues,
  gcpResourceName,
  pubsubServiceAgent,
  relayauthChartValues,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  requestedAlchemyCommand,
  captureRelayauthChartPackage,
  captureRelaycastChartPackage,
  type GcpInputs,
} from "./config.ts";
import { prepareRelayauthChart, prepareRelaycastChart } from "./chart-cache.ts";
import { assertGcpDeploymentTarget, assertGcpProjectConfirmation } from "./deploy-guard.ts";
import {
  ALCHEMY_GCP_BETA_84_SQL,
  cloudSqlProxyWorkload,
  ensurePrivateCloudSql,
  privateCloudSqlSpec,
  sqlAdminFetchTransport,
  type PrivateCloudSqlSpec,
} from "./cloud-sql.ts";
import { provePrivateKubernetesReachability, relaycastProofAnnotations } from "./kubernetes-preflight.ts";
import {
  assertGcpApplyLeaseActive,
  applyNonceDigest,
  boundGcpApplyLease,
  consumeGcpApplyNonce,
  consumeGcpApplyNonceGlobally,
  verifyGcpApplyNonce,
  type GcpApplyAuthorizationInput,
  type GcpApplyAuthorizationProof,
} from "./apply-authorization.ts";
import { requireServiceAccountActAs, type ServiceAccountActAsProof } from "./iam-preflight.ts";
import { fetchRedisServerCa, redisAdminFetchTransport } from "./redis-ca.ts";
import { PRIVATE_GOOGLE_APIS_RECORDS, assertInClusterJwks, bulls96NetworkPolicies } from "./egress.ts";
import {
  alleyoopWriteCondition,
  ensureConditionalBucketBindings,
  workerWriteCondition,
  type ConditionalBinding,
} from "./bucket-iam.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const DEFAULT_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";
const PSA_CIDR = "10.80.0.0/20";

interface EnsurePrivateCloudSqlInput {
  spec: PrivateCloudSqlSpec;
  /** Resolved PSA output: establishes that the peering exists before the POST. */
  privateServiceConnection: string;
  proxyPrincipal: string;
  authorization: GcpApplyAuthorizationProof;
}

interface EnsureLockerWriteScopesInput {
  bucket: string;
  bindings: ConditionalBinding[];
  /** Members stripped from every bucket binding (the retired shared identity). */
  forbidden: string[];
  authorization: GcpApplyAuthorizationProof;
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
  authorization: GcpApplyAuthorizationProof;
}
interface RequireNodeActAsInput {
  projectId: string;
  serviceAccountEmail: string;
  authorization: GcpApplyAuthorizationProof;
}
interface ResolveRedisServerCaInput {
  projectId: string;
  region: string;
  instanceId: string;
  authorization: GcpApplyAuthorizationProof;
}

function readInputs(stage: string, requestedCommand: ReturnType<typeof requestedAlchemyCommand>): GcpInputs {
  const path = join(ROOT, ".96bulls", stage, "gcp-inputs.json");
  let inputs: GcpInputs;
  try {
    inputs = JSON.parse(readFileSync(path, "utf8")) as GcpInputs;
  } catch {
    throw new Error(`96bulls: GCP stage ${stage} is not cleared: ${path} does not exist. Nothing was created.`);
  }
  assertGcpInputs(stage, inputs, requestedCommand);
  assertGcpProjectConfirmation(inputs.projectId, process.env[GCP_CONFIRM_PROJECT_ENV]);
  return inputs;
}

function bootstrapStage(): string {
  if (process.env.CI !== "true")
    throw new Error(
      "96bulls: set CI=true so Alchemy cannot create a missing GCP state bucket before apply authorization. Nothing was created.",
    );
  const stage = process.env.ALCHEMY_STAGE;
  if (!stage)
    throw new Error(
      "96bulls: set ALCHEMY_STAGE explicitly so inputs and chart bytes are captured before Alchemy initializes. Nothing was created.",
    );
  return stage;
}

// Synchronous, local-only bootstrap runs before any Alchemy Action, provider,
// or state construction below. It captures the chart bytes exactly once.
const BOOTSTRAP_STAGE = bootstrapStage();
const BOOTSTRAP_COMMAND = requestedAlchemyCommand();
const BOOTSTRAP_INPUTS = readInputs(BOOTSTRAP_STAGE, BOOTSTRAP_COMMAND);
assertGcpRuntimeDeployable(BOOTSTRAP_STAGE, BOOTSTRAP_INPUTS, BOOTSTRAP_COMMAND);
// This value is deliberately not persisted between invocations. It prevents
// Alchemy from reusing a prior successful Action output for the same token;
// the atomic ledger then rejects that token as already consumed.
const APPLY_INVOCATION_ID = randomUUID();
const BOOTSTRAP_RELAYCAST_CHART = BOOTSTRAP_INPUTS.relaycast
  ? prepareRelaycastChart(
      captureRelaycastChartPackage(
        join(ROOT, ".96bulls", BOOTSTRAP_STAGE, "artifacts", "relaycast-0.1.0.tgz"),
      ),
    )
  : null;
const BOOTSTRAP_RELAYAUTH_CHART =
  BOOTSTRAP_INPUTS.relayauth && BOOTSTRAP_COMMAND !== "destroy"
    ? prepareRelayauthChart(
        captureRelayauthChartPackage(
          join(ROOT, ".96bulls", BOOTSTRAP_STAGE, "artifacts", "relayauth-0.1.1.tgz"),
        ),
      )
    : null;

const RequireGcpApplyAuthorization = Alchemy.Action<
  "96Bulls.GCP.RequireApplyAuthorization",
  GcpApplyAuthorizationInput,
  GcpApplyAuthorizationProof,
  GCP.GcpEnvironment
>("96Bulls.GCP.RequireApplyAuthorization", (input) =>
  Effect.gen(function* () {
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    if (live.project !== input.projectId)
      throw new Error("96bulls: apply nonce project differs from the resolved GCP project. Nothing was created.");
    const token = process.env[GCP_APPLY_NONCE_ENV];
    if (applyNonceDigest(token) !== input.nonceDigest)
      throw new Error("96bulls: GCP apply nonce changed after planning. Nothing was created.");
    const verified = yield* Effect.tryPromise(() =>
      verifyGcpApplyNonce(token, {
        stage: input.stage,
        projectId: input.projectId,
        executor: input.executor,
      }),
    );
    const locallyConsumed = consumeGcpApplyNonce(verified, join(ROOT, ".96bulls"));
    return yield* Effect.tryPromise(() =>
      consumeGcpApplyNonceGlobally(
        locallyConsumed,
        { bearer: Redacted.value(live.accessToken) },
      ),
    );
  }),
);

const RequireNodeActAs = Alchemy.Action<
  "96Bulls.GCP.RequireNodeActAs",
  RequireNodeActAsInput,
  ServiceAccountActAsProof,
  GCP.GcpEnvironment
>("96Bulls.GCP.RequireNodeActAs", (input) =>
  Effect.gen(function* () {
    if (input.authorization.projectId !== input.projectId)
      throw new Error("96bulls: node actAs preflight is not bound to this project. GKE was not created or changed.");
    assertGcpApplyLeaseActive(input.authorization);
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    return yield* Effect.tryPromise(() =>
      requireServiceAccountActAs(
        input.projectId,
        input.serviceAccountEmail,
        { bearer: Redacted.value(live.accessToken) },
      ),
    );
  }),
);

const EnsurePrivateCloudSql = Alchemy.Action<
  "96Bulls.GCP.EnsurePrivateCloudSql",
  EnsurePrivateCloudSqlInput,
  EnsurePrivateCloudSqlOutput,
  GCP.GcpEnvironment
>("96Bulls.GCP.EnsurePrivateCloudSql", (input) =>
  Effect.gen(function* () {
    if (!input.privateServiceConnection)
      throw new Error("96bulls: private service connection was not resolved. Nothing was created.");
    const project = /^projects\/([^/]+)\/global\/networks\//.exec(input.spec.privateNetwork)?.[1];
    if (!project || input.authorization.projectId !== project)
      throw new Error("96bulls: Cloud SQL poster project is malformed. Nothing was created.");
    assertGcpApplyLeaseActive(input.authorization);
    assertGcpSqlPostConfirmation(project, input.spec.instanceName, process.env[GCP_CONFIRM_SQL_POST_ENV]);
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    if (project !== live.project)
      throw new Error("96bulls: Cloud SQL poster project differs from the resolved GCP project. Nothing was created.");
    const result = yield* Effect.tryPromise((signal) =>
      ensurePrivateCloudSql(
        input.spec,
        { bearer: Redacted.value(live.accessToken) },
        sqlAdminFetchTransport(),
        undefined,
        { expiresAtMs: input.authorization.leaseExpiresAt * 1000, signal },
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

const EnsureLockerWriteScopes = Alchemy.Action<
  "96Bulls.GCP.EnsureLockerWriteScopes",
  EnsureLockerWriteScopesInput,
  { bucket: string; changed: boolean },
  GCP.GcpEnvironment
>("96Bulls.GCP.EnsureLockerWriteScopes", (input) =>
  Effect.gen(function* () {
    assertGcpApplyLeaseActive(input.authorization);
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    if (live.project !== input.authorization.projectId)
      throw new Error("96bulls: locker write scopes project differs from the resolved GCP project. Nothing was changed.");
    const result = yield* Effect.tryPromise(() =>
      ensureConditionalBucketBindings(
        input.bucket,
        input.bindings,
        { bearer: Redacted.value(live.accessToken) },
        undefined,
        undefined,
        input.forbidden,
      ),
    );
    return { bucket: input.bucket, changed: result.changed };
  }),
);

const ResolveRedisServerCa = Alchemy.Action<
  "96Bulls.GCP.ResolveRedisServerCa",
  ResolveRedisServerCaInput,
  Awaited<ReturnType<typeof fetchRedisServerCa>>,
  GCP.GcpEnvironment
>("96Bulls.GCP.ResolveRedisServerCa", (input) =>
  Effect.gen(function* () {
    if (input.authorization.projectId !== input.projectId)
      throw new Error("96bulls: Redis service CA lookup project is not authorized. Nothing was created or changed.");
    if (input.authorization.expiresAt <= Math.floor(Date.now() / 1000))
      throw new Error("96bulls: GCP apply authorization expired before the Redis service CA lookup. Nothing was created or changed.");
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    if (live.project !== input.projectId)
      throw new Error("96bulls: Redis service CA lookup project differs from the resolved GCP project. Nothing was created or changed.");
    return yield* Effect.tryPromise(() =>
      fetchRedisServerCa(
        input,
        { bearer: Redacted.value(live.accessToken) },
        redisAdminFetchTransport(),
      ),
    );
  }),
);

const RequireKubernetesReachability = Alchemy.Action<
  "96Bulls.GCP.RequireKubernetesReachability",
  RequireKubernetesReachabilityInput,
  Awaited<ReturnType<typeof provePrivateKubernetesReachability>>,
  GCP.GcpEnvironment
>("96Bulls.GCP.RequireKubernetesReachability", (input) =>
  Effect.tryPromise(() =>
    provePrivateKubernetesReachability(input.endpoint, input.executor, input.authorization),
  ),
);

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
  boundGcpApplyLease(Effect.gen(function* () {
    const stage = yield* Stage;
    if (stage !== BOOTSTRAP_STAGE)
      throw new Error("96bulls: Alchemy stage changed after local bootstrap. Nothing was created.");
    const i = BOOTSTRAP_INPUTS;
    const relaycastChart = BOOTSTRAP_RELAYCAST_CHART;
    // Before the first resource. Confirmation is operator intent; the resolved
    // GCP environment is the project and region these credentials will use.
    const live = yield* GCP.GcpEnvironment.current.pipe(Effect.orDie);
    assertGcpDeploymentTarget(
      { projectId: i.projectId, region: i.region },
      { projectId: live.project, region: live.region },
    );
    const applyAuthorization = yield* RequireGcpApplyAuthorization("apply-authorization", {
      stage,
      projectId: i.projectId,
      executor: i.kubernetesExecutor,
      nonceDigest: applyNonceDigest(),
      invocationId: APPLY_INVOCATION_ID,
    });
    // Output.map preserves the proof dependency while returning stable resource
    // inputs, so a fresh nonce gates mutations without creating perpetual diffs.
    const authorized = <T>(value: T): Input<T> =>
      Output.map(applyAuthorization.proof, () => value) as Input<T>;
    const chartRepo = i.chartRepo ?? DEFAULT_CHART_REPO;
    const storageClass = i.chartStorageClass ?? "standard-rwo";
    const networkName = gcpResourceName(stage, "vpc", 63);
    const psaName = gcpResourceName(stage, "psa", 63);
    const runtimeAccountId = gcpResourceName(stage, "runtime", 30);
    const runtimeEmail = `${runtimeAccountId}@${i.projectId}.iam.gserviceaccount.com`;
    const workerAccountId = gcpResourceName(stage, "worker", 30);
    const workerEmail = `${workerAccountId}@${i.projectId}.iam.gserviceaccount.com`;
    const alleyoopAccountId = gcpResourceName(stage, "alleyoop", 30);
    const alleyoopEmail = `${alleyoopAccountId}@${i.projectId}.iam.gserviceaccount.com`;
    const lockerBucket = `${gcpResourceName(stage, "locker", 30)}-${i.projectId}`.slice(0, 63).replace(/-$/, "");
    const eventsTopicId = gcpResourceName(stage, "events", 63);
    const eventsSubscriptionId = gcpResourceName(stage, "events-worker", 63);
    const eventsTopicName = `projects/${i.projectId}/topics/${eventsTopicId}`;
    const eventsSubscriptionName = `projects/${i.projectId}/subscriptions/${eventsSubscriptionId}`;

    // Keep project-level APIs enabled on destroy because they may be shared by
    // other stacks. Service Usage itself is the one bootstrap prerequisite.
    const apiGates: Record<string, Output.Output<string>> = {};
    for (const service of [
      "artifactregistry.googleapis.com",
      "compute.googleapis.com",
      "container.googleapis.com",
      "dns.googleapis.com",
      "iam.googleapis.com",
      "iamcredentials.googleapis.com",
      "pubsub.googleapis.com",
      "redis.googleapis.com",
      "secretmanager.googleapis.com",
      "servicenetworking.googleapis.com",
      "sqladmin.googleapis.com",
    ]) {
      const api = yield* GCP.ServiceUsage.ProjectService(`api-${service.split(".")[0]}`, {
        project: authorized(i.projectId),
        service,
        disableOnDestroy: false,
      });
      apiGates[service] = api.state;
    }
    const afterApi = <T>(service: string, value: T): Input<T> =>
      Output.map(apiGates[service]!, (state) => {
        if (state !== "ENABLED")
          throw new Error(`96bulls: required GCP API ${service} is not enabled. Nothing was created.`);
        return value;
      }) as Input<T>;

    const network = yield* GCP.Compute.Network("network", {
      networkName: afterApi("compute.googleapis.com", networkName),
      autoCreateSubnetworks: false,
      routingMode: "REGIONAL",
      description: authorized(`AgentWorkforce ${stage} private network`),
    });
    const subnet = yield* GCP.Compute.Subnetwork("subnet", {
      subnetworkName: gcpResourceName(stage, "private", 63),
      region: i.region,
      network: network.networkName,
      ipCidrRange: "10.52.0.0/20",
      privateIpGoogleAccess: true,
      description: authorized(`AgentWorkforce ${stage} private subnet`),
      secondaryIpRanges: [
        { rangeName: "gke-pods", ipCidrRange: "10.60.0.0/14" },
        { rangeName: "gke-services", ipCidrRange: "10.64.0.0/20" },
      ],
      logConfig: { enable: true, aggregationInterval: "INTERVAL_5_SEC", flowSampling: 0.5, metadata: "INCLUDE_ALL_METADATA" },
    });
    // Every *.googleapis.com name in the stage VPC resolves to private.googleapis.com,
    // reached through Private Google Access, so workload egress needs no public
    // internet route for Google APIs (egress.ts).
    const googleApisZone = yield* GCP.DNS.ManagedZone("private-googleapis", {
      zoneName: afterApi("dns.googleapis.com", gcpResourceName(stage, "googleapis", 63)),
      dnsName: "googleapis.com.",
      visibility: "private",
      networks: [network.networkName],
      description: authorized(`AgentWorkforce ${stage}: Google APIs on private.googleapis.com`),
      labels: labels(stage, "dns"),
    });
    for (const record of PRIVATE_GOOGLE_APIS_RECORDS) {
      yield* GCP.DNS.ResourceRecordSet(record.id, {
        managedZone: googleApisZone.zoneName,
        name: record.name,
        type: record.type,
        ttl: 300,
        rrdatas: [...record.rrdatas],
      });
    }
    const natAddress = yield* GCP.Compute.Address("nat-address", {
      addressName: afterApi("compute.googleapis.com", gcpResourceName(stage, "nat", 63)),
      region: i.region,
      addressType: "EXTERNAL",
      networkTier: "PREMIUM",
      description: authorized(`AgentWorkforce ${stage} auditable NAT address`),
      labels: labels(stage, "nat"),
    });
    yield* GCP.Compute.Router("nat", {
      routerName: gcpResourceName(stage, "nat", 63),
      region: i.region,
      network: network.networkName,
      description: authorized(`AgentWorkforce ${stage} private-node egress`),
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
      address: "10.80.0.0",
      addressType: "INTERNAL",
      purpose: "VPC_PEERING",
      network: network.selfLink.as<string>(),
      prefixLength: 20,
      description: authorized(`AgentWorkforce ${stage} managed-service peering`),
    });
    // Project-local Private Service Access peering for this isolated project.
    const privateServices = yield* GCP.ServiceNetworking.Connection("private-services", {
      // The network resource already carries the nonce dependency.
      network: network.networkName,
      service: afterApi("servicenetworking.googleapis.com", "servicenetworking.googleapis.com"),
      reservedPeeringRanges: [psaRange.addressName],
      deletionPolicy: "DELETE",
    });

    const nodeAccountId = gcpResourceName(stage, "nodes", 30);
    const nodeIdentity = yield* GCP.IAM.ServiceAccount("node-identity", {
      accountId: afterApi("iam.googleapis.com", nodeAccountId),
      displayName: `AgentWorkforce ${stage} nodes`,
      description: authorized("Dedicated GKE node identity. No keys."),
    });
    const nodeMember = Output.interpolate`serviceAccount:${nodeIdentity.email}`;
    yield* GCP.IAM.Member("node-container-defaultNodeServiceAccount", {
      kind: "project",
      name: i.projectId,
      role: authorized(GCP_NODE_ROLES[0]),
      member: nodeMember,
    });
    const nodeArtifactRegistryReader = yield* GCP.IAM.Member("node-artifactregistry-reader", {
      kind: "project",
      name: i.projectId,
      role: authorized(GCP_NODE_ROLES[1]),
      member: nodeMember,
    });

    const nodeActAs = yield* RequireNodeActAs("node-act-as", {
      projectId: i.projectId,
      serviceAccountEmail: nodeIdentity.email,
      authorization: applyAuthorization,
    });

    const cluster = yield* GCP.Container.Cluster("cluster", {
      clusterId: afterApi("container.googleapis.com", gcpResourceName(stage, "gke")),
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
      serviceAccount: nodeActAs.serviceAccountEmail,
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
      bucketName: authorized(lockerBucket),
      location: i.region,
      storageClass: "STANDARD",
      versioning: true,
      uniformBucketLevelAccess: true,
      forceDestroy: false,
      labels: labels(stage, "locker"),
    });
    const runtimeSecret = yield* GCP.SecretManager.Secret("runtime-secret", {
      secretId: authorized(gcpResourceName(stage, "runtime", 63)),
      labels: labels(stage, "runtime"),
      replication: { userManaged: { replicas: [{ location: i.region }] } },
      versionDestroyTtl: afterApi("secretmanager.googleapis.com", "604800s"),
    });
    const images = yield* GCP.ArtifactRegistry.Repository("images", {
      repositoryId: authorized(gcpResourceName(stage, "images", 63)),
      location: i.region,
      format: afterApi("artifactregistry.googleapis.com", "DOCKER"),
      description: `AgentWorkforce ${stage} private images`,
      labels: labels(stage, "images"),
      dockerConfig: { immutableTags: true },
    });

    // runtime-identity is the Cloud SQL proxy's identity only: Cloud SQL client, no
    // bucket, no Pub/Sub. The service pods each get their own account below, so a
    // compromised proxy can no longer read or rewrite transcripts, and AlleyOop and
    // the worker can each write only their own objects (bucket-iam.ts).
    const runtimeIdentity = yield* GCP.IAM.ServiceAccount("runtime-identity", {
      accountId: runtimeAccountId,
      displayName: afterApi("iam.googleapis.com", `AgentWorkforce ${stage} runtime`),
      description: authorized("Workload Identity target for the Cloud SQL proxy"),
    });
    // email is an Output until deploy. A JS template coerces it and throws.
    const runtimeMember = Output.interpolate`serviceAccount:${runtimeIdentity.email}`;
    const workerIdentity = yield* GCP.IAM.ServiceAccount("worker-identity", {
      accountId: workerAccountId,
      displayName: afterApi("iam.googleapis.com", `AgentWorkforce ${stage} 96bulls worker`),
      description: authorized("Workload Identity target for the 96Bulls worker pods and Jobs"),
    });
    const alleyoopIdentity = yield* GCP.IAM.ServiceAccount("alleyoop-identity", {
      accountId: alleyoopAccountId,
      displayName: afterApi("iam.googleapis.com", `AgentWorkforce ${stage} AlleyOop`),
      description: authorized("Workload Identity target for the AlleyOop ingest pods"),
    });
    const workerMember = Output.interpolate`serviceAccount:${workerIdentity.email}`;
    const alleyoopMember = Output.interpolate`serviceAccount:${alleyoopIdentity.email}`;
    // The proxy Kubernetes service account is created only once Cloud SQL is
    // posted. Binding it earlier would let a later account of that name assume
    // the runtime identity.
    const serviceBindings: Array<{ role: Output.Output<string> }> = [];
    for (const [logicalName, serviceAccount, identity] of [
      ["96bulls-worker", GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS.worker, workerIdentity],
      ["96bulls-alleyoop", GCP_BULLS96_KUBERNETES_SERVICE_ACCOUNTS.alleyoop, alleyoopIdentity],
    ] as const) {
      serviceBindings.push(
        yield* GCP.IAM.Member(`workload-identity-${logicalName}-own`, {
          kind: "iam.serviceAccount",
          name: identity.name,
          role: "roles/iam.workloadIdentityUser",
          member: Output.map(cluster.workloadPool, () =>
            workloadIdentityPrincipal(i, "96bulls", serviceAccount),
          ),
        }),
      );
    }
    for (const [id, member] of [
      ["worker", workerMember],
      ["alleyoop", alleyoopMember],
    ] as const) {
      serviceBindings.push(
        yield* GCP.IAM.Member(`${id}-locker-read`, {
          kind: "storage.bucket",
          name: locker.bucketName,
          role: authorized("roles/storage.objectViewer"),
          member,
        }),
      );
    }
    const lockerWriteScopes = yield* EnsureLockerWriteScopes("locker-write-scopes", {
      bucket: locker.bucketName,
      bindings: Output.map(
        Output.all(locker.bucketName, workerIdentity.email, alleyoopIdentity.email),
        ([bucket, worker, alleyoop]) => [
          { role: "roles/storage.objectUser", member: `serviceAccount:${worker}`, condition: workerWriteCondition(bucket) },
          { role: "roles/storage.objectUser", member: `serviceAccount:${alleyoop}`, condition: alleyoopWriteCondition(bucket) },
        ],
      ) as unknown as ConditionalBinding[],
      // The Cloud SQL proxy identity must hold nothing on the bucket.
      forbidden: [`serviceAccount:${runtimeEmail}`],
      authorization: applyAuthorization,
    });
    yield* GCP.IAM.Member("runtime-cloudsql", {
      kind: "project",
      name: i.projectId,
      role: authorized("roles/cloudsql.client"),
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
            allocatedIpCidr: PSA_CIDR,
            tier: i.managedDataPlane.cloudSqlTier,
            highAvailability: i.managedDataPlane.cloudSqlHa === true,
          });
          const sqlPoster = yield* EnsurePrivateCloudSql("private-cloud-sql", {
            spec: sql,
            privateServiceConnection: privateServices.name,
            proxyPrincipal: workloadIdentityPrincipal(i, "96bulls", "cloud-sql-proxy"),
            authorization: applyAuthorization,
          });
          const redis = yield* GCP.Redis.Instance("redis", {
            instanceId: authorized(gcpResourceName(stage, "redis")),
            location: i.region,
            displayName: afterApi("redis.googleapis.com", `AgentWorkforce ${stage}`),
            labels: labels(stage, "redis"),
            redisVersion: "REDIS_7_2",
            tier: i.managedDataPlane.redisHa ? "STANDARD_HA" : "BASIC",
            memorySizeGb: i.managedDataPlane.redisMemoryGb ?? 1,
            // Memorystore rejects a short network name. Mapping the resolved
            // network output preserves the resource dependency while emitting
            // the canonical API resource name required by Redis.
            authorizedNetwork: Output.map(
              network.networkName,
              (name) => `projects/${i.projectId}/global/networks/${name}`,
            ),
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
          const redisCa = yield* ResolveRedisServerCa("redis-service-ca", {
            projectId: i.projectId,
            region: i.region,
            instanceId: redis.instanceId,
            authorization: applyAuthorization,
          });
          const deadLetters = yield* GCP.PubSub.Topic("dead-letters", {
            topicId: authorized(gcpResourceName(stage, "dead-letters", 63)),
            labels: labels(stage, "queue"),
            messageRetentionDuration: afterApi("pubsub.googleapis.com", "604800s"),
          });
          const events = yield* GCP.PubSub.Topic("events", {
            topicId: authorized(eventsTopicId),
            labels: labels(stage, "queue"),
            messageRetentionDuration: afterApi("pubsub.googleapis.com", "604800s"),
          });
          const subscription = yield* GCP.PubSub.Subscription("events-worker", {
            subscriptionId: authorized(eventsSubscriptionId),
            topic: events.name,
            labels: labels(stage, "queue"),
            ackDeadlineSeconds: afterApi("pubsub.googleapis.com", 60),
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
            yield* GCP.IAM.Member(`worker-${id}`, {
              kind,
              name,
              role,
              // The worker identity and the named topic/subscription are
              // already nonce-gated dependencies of this binding.
              member: workerMember,
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
              member: authorized(pubsubAgent),
            });
          }

          return {
            sql,
            sqlPoster,
            redis: { host: redis.host, port: redis.port, caPem: redisCa.caPem },
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
          authorization: applyAuthorization,
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
              podAnnotations: relaycastProofAnnotations(kubernetesReachability!),
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
            ...(BOOTSTRAP_COMMAND === "destroy"
              ? {
                  chart: "relayauth",
                  repo: chartRepo,
                  version: relayauthConfig.chartVersion,
                }
              : { chart: BOOTSTRAP_RELAYAUTH_CHART! }),
            releaseName: "relayauth",
            namespace: Output.map(kubernetesReachability!.proof, () => "relayauth"),
            values: relayauthChartValues(
              relayauthConfig,
              storageClass,
              BOOTSTRAP_COMMAND,
            ),
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
          // Default deny plus the exact egress the services use (egress.ts). Redis
          // and Cloud SQL both take their private IPs from the PSA range.
          if (BOOTSTRAP_COMMAND !== "destroy")
            assertInClusterJwks("bulls96.relayauthJwksUrl", i.bulls96!.relayauthJwksUrl);
          for (const policy of bulls96NetworkPolicies({ redisRange: PSA_CIDR, cloudSqlRange: PSA_CIDR })) {
            yield* Kubernetes.Manifest(`netpol-${policy.metadata.name}`, {
              cluster,
              manifest: {
                ...policy,
                metadata: {
                  ...policy.metadata,
                  annotations: { "96bulls.agentworkforce.dev/reachability": kubernetesReachability!.proof },
                },
              },
            });
          }
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
              // Resolve the IAM member as part of the repository value so
              // Helm cannot create image-pulling pods before the reader edge.
              // The service grants and write scopes ride the same value, so pods
              // cannot roll onto an identity before it can read and write.
              imageRepository: Output.map(
                Output.all(
                  images.registryUri,
                  nodeArtifactRegistryReader.role,
                  lockerWriteScopes.bucket,
                  Output.all(...serviceBindings.map((b) => b.role)),
                ),
                ([registry]) => `${registry}/96bulls-services`,
              ),
              cloudSqlConnectionName: dataPlane.sqlPoster.connectionName,
              redisHost: dataPlane.redis.host,
              redisPort: dataPlane.redis.port,
              redisCaPem: dataPlane.redis.caPem,
              pubsubTopic: eventsTopicName,
              pubsubSubscription: eventsSubscriptionName,
              workloadIdentityEmails: { worker: workerEmail, alleyoop: alleyoopEmail },
            }, BOOTSTRAP_COMMAND),
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
      workerIdentity: workerIdentity.email,
      alleyoopIdentity: alleyoopIdentity.email,
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
  })),
);
