/**
 * First cloud compute target (Khaliq, 2026-10-28): EKS running helm-chart-lead's
 * charts. CODE ONLY, reached only through the gated AWS stage.
 *
 * Declares: a VPC with private subnets and one NAT, an EKS Auto Mode cluster
 * (private API endpoint, public endpoint off or limited to listed operator
 * CIDRs, all five control-plane log types), one Pod Identity role per service
 * account, NetworkPolicies (default deny) and the `96bulls` umbrella chart
 * (worker + alleyoop + OTel collector) pinned to the published services image
 * digest. HoopDb is external (Supabase, the production design) or, for the
 * isolated proof stage, an in-cluster CloudNativePG Postgres 17. Relaycast is
 * optional. Secrets reach the pods through existing Kubernetes Secrets synced
 * from 1Password, never through chart values. Pure contracts: hardening.ts and
 * proof-addons.ts.
 */
import { claimlessWindowEnv, type ClaimlessWindow } from "../claimless-window.ts";
import * as AWS from "@alchemy.run/aws";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import { ingressStage } from "./ingress.ts";
import { resolveTenantOrgs, tenantWiring } from "./tenants.ts";
import { RELAYAUTH_TLS_CLIENT_LABEL, RELAYAUTH_TLS_NAMESPACE, RELAYAUTH_TLS_PORT, RELAYAUTH_TLS_SERVICE, RELAYAUTH_TLS_URL } from "./relayauth-tls.ts";
import { relayauthStage } from "./relayauth.ts";
import {
  CONTROL_PLANE_LOG_RETENTION,
  NAMESPACE,
  PINNED_IMAGES,
  SERVICE_ACCOUNTS,
  VPC_CIDR,
  assertServicesImage,
  clusterEndpointAccess,
  controlPlaneLogGroupName,
  controlPlaneLogging,
  hoopdbBackend,
  podIdentityTrust,
  podRoles,
  telemetryPolicy,
  type HoopDbBackend,
} from "./hardening.ts";
import {
  CNPG_NAMESPACE,
  CNPG_OPERATOR_CHART,
  RELAYCAST_NAMESPACE,
  assertRelaycastInputs,
  assertSessionDirectoryPreconditions,
  SEARCH_CLIENT_LABEL,
  SESSION_DIRECTORY_POSTGREST,
  cnpgHoopdbValues,
  cnpgOperatorValues,
  networkPolicies,
  networkPolicyControllerConfigMap,
  relaycastValues,
  storageClassManifest,
  type IngestExposureInput,
  type RelaycastInputs,
} from "./proof-addons.ts";

export { telemetryPolicy } from "./hardening.ts";

/**
 * HOOPDB_DB_MODE, an explicit shim choice and the dedicated runtime role for the
 * migrator. There is no runtime-role default in either mode: in-cluster (selfhosted) the
 * role is created by the privileged supabase/bootstrap/hoopdb_runtime_role.sql step, run
 * as the CNPG superuser before migrate (#75 wires it); external (supabase) it is
 * pre-provisioned by an operator. Unset, the migrator refuses to run.
 */
export function hoopdbModeEnv(inCluster: boolean, runtimeRole?: string, runtimeLogin = true): Array<{ name: string; value: string }> {
  return [
    { name: "HOOPDB_DB_MODE", value: inCluster ? "selfhosted" : "supabase" },
    { name: "HOOPDB_SUPABASE_SHIM", value: inCluster ? "true" : "false" },
    ...(runtimeRole
      ? [
          { name: "HOOPDB_RUNTIME_ROLE", value: runtimeRole },
          { name: "HOOPDB_RUNTIME_ROLE_LOGIN", value: runtimeLogin ? "true" : "false" },
        ]
      : []),
  ];
}

export interface EksStageInputs {
  stage: string;
  /** The account the stage deploys into; Pod Identity trust is bound to it. */
  accountId: string;
  /** Chart repository and versions (helm-charts, published via chart-releaser). */
  chartRepo?: string;
  chartVersion: string;
  /** Kubernetes Secret holding HOOPDB_URL, LOCKER_* and the JWKS settings; created by the secrets sync, not by IaC. */
  runtimeSecretName: string;
  /**
   * HOOPDB_RUNTIME_ROLE: the dedicated, non-owner role the services connect as. No default
   * (the owner `hoop` is refused in selfhosted and supabase mode): unset, the migrator
   * refuses to run (src/hoopdb/migrate.ts). In-cluster it is created by
   * supabase/bootstrap/hoopdb_runtime_role.sql; external it is pre-provisioned.
   */
  hoopdbRuntimeRole?: string;
  /** HOOPDB_RUNTIME_ROLE_LOGIN: whether that role logs in itself (default true). */
  hoopdbRuntimeRoleLogin?: boolean;
  lockerBucket: string;
  lockerRegion: string;
  /** One tenant org (single-tenant mode). */
  tenantOrgId?: string;
  /** Several tenant orgs (org-derived key prefixes; tenants.ts). tenantOrgIds[0] must equal tenantOrgId if both are set. */
  tenantOrgIds?: string[];
  relayauthJwksUrl: string;
  relayhistoryOrigin: string;
  /** Default off. See runbooks/manifest-activation.md. */
  claimlessWindow?: ClaimlessWindow;
  /** The `iss` RelayAuth stamps; must match exactly or every token is refused. */
  relayauthIssuer?: string;
  /**
   * Self-hosted RelayAuth in this cluster (see relayauth.ts). When set, AlleyOop's
   * issuer and JWKS URL are the in-cluster RelayAuth and relayauthJwksUrl /
   * relayauthIssuer must not point elsewhere.
   */
  relayauth?: { chartVersion: string; secretName: string };
  /** The account's private ECR repository; nodes pull with their node role, no pull secret. */
  imageRepository: string;
  /** The digest published to this account's repository (per account, from aws-inputs.json). */
  imageDigest: string;
  /** The full commit the imageDigest image was built from (publish-image-ecr records it). */
  imageSourceRevision: string;
  /**
   * EKS API access. The private endpoint is always on. The public endpoint is
   * off unless operator CIDRs are listed here (never 0.0.0.0/0).
   */
  apiAccess?: { publicAccessCidrs?: string[] };
  /** `external` (default): HOOPDB_URL from runtimeSecretName (Supabase). `cnpg`: in-cluster proof database. */
  hoopdb?: { backend?: HoopDbBackend };
  /** Portable Relaycast core (helm-charts `relaycast`), ClusterIP only. */
  relaycast?: RelaycastInputs;
  /**
   * Fence the session-directory search path (NetworkPolicies only). The PostgREST
   * Deployment comes from 96BullSessions #73; requires cnpg + self-hosted RelayAuth (TLS).
   */
  sessionDirectory?: { enabled: boolean };
  /** Step 2 of the NetworkPolicy rollout: switch enforcement on once the policies are applied and checked. */
  networkPolicyEnforcement?: boolean;
  /** "keep": keep the old shared pods role (unattached) until the per-SA roles are verified. */
  legacyPodsRole?: "keep";
  /** Proof/sandbox teardown: buckets emptied and volumes deleted on destroy (alchemy.aws.run.ts gates it). */
  purgeOnDestroy?: boolean;
  /** The Locker bucket ARN and its KMS key ARN, for the pods' Pod Identity role. */
  lockerBucketArn: string;
  lockerKmsKeyArn: string;
  /** Sandbox stages: every role we create carries this permissions boundary and the bounded tag. */
  permissionsBoundaryArn?: string;
  /** Public hostname for the ingest (Cloudflare DNS). Omit to keep the ingest cluster-internal. */
  ingress?: { hostname: string; cloudflareZoneId: string; zoneDomain: string; cloudflareTokenSecret: string; certValidation?: { name: string; value: string } } & IngestExposureInput;
  tags: Record<string, string>;
}

export const OTEL_SERVICE_ACCOUNT = SERVICE_ACCOUNTS.otel;
export const OTEL_ENDPOINT = "http://otel-collector:4318";

/**
 * Values kept as a pure function so the AWS observability contract can be
 * tested without loading either the AWS or Kubernetes providers.
 */
export function eksChartValues(
  i: Pick<EksStageInputs, "stage" | "claimlessWindow" | "runtimeSecretName" | "lockerBucket" | "lockerRegion" | "tenantOrgId" | "tenantOrgIds" | "relayhistoryOrigin" | "imageRepository" | "imageDigest" | "hoopdbRuntimeRole" | "hoopdbRuntimeRoleLogin">,
  sandbox: boolean,
  authIssuer: string,
  authJwksUrl: string,
  /** Set when HoopDb is the in-cluster CNPG cluster (proof stage); the value orders it after the operator. */
  cnpg?: { operatorRelease: string },
) {
  // Sandboxes and the CNPG proof stage run HoopDb in the cluster; the apps then read
  // the Secret CloudNativePG generates (umbrella default hoopdb-app/uri).
  const inCluster = sandbox || cnpg !== undefined;
  const tenants = tenantWiring(resolveTenantOrgs(i));
  return {
    global: {
      imageRegistry: "", // the ECR repository below is fully qualified
      tenant: { orgId: tenants.primaryOrg },
      otel: { endpoint: OTEL_ENDPOINT },
    },
    worker: {
      image: { repository: i.imageRepository, digest: i.imageDigest },
      hoopdb: inCluster ? {} : { existingSecret: i.runtimeSecretName, key: "HOOPDB_URL" },
      locker: { endpoint: "", bucket: i.lockerBucket, region: i.lockerRegion, credentials: { mode: "podIdentity" } },
      // Supabase already has anon/authenticated/service_role; the plain-Postgres shim is for in-cluster DBs only.
      migrations: { supabaseShim: inCluster },
      // The bucket is created and versioned by IaC; the pods' role has no CreateBucket.
      lockerInit: { enabled: false },
      deployment: { name: "hosted", id: i.stage },
      // Bob Costas alone gets the summary master key (src/shared/keyring.ts): worker 0.4.0 projects
      // this Secret key as a read-only file into Bob Costas only (SUMMARY_MASTER_KEYS_FILE).
      summaryEncryption: { existingSecret: i.runtimeSecretName, key: "SUMMARY_MASTER_KEYS" },
      // The migrator's explicit database-mode contract (src/hoopdb/migrate.ts); never inferred from the host.
      extraEnv: [...hoopdbModeEnv(inCluster, i.hoopdbRuntimeRole, i.hoopdbRuntimeRoleLogin), ...tenants.extraEnv],
    },
    alleyoop: {
      image: { repository: i.imageRepository, digest: i.imageDigest },
      hoopdb: inCluster ? {} : { existingSecret: i.runtimeSecretName, key: "HOOPDB_URL" },
      locker: { endpoint: "", bucket: i.lockerBucket, region: i.lockerRegion, credentials: { mode: "podIdentity" } },
      auth: { jwksUrl: authJwksUrl, issuer: authIssuer },
      extraEnv: [{ name: "RELAYHISTORY_ORIGIN", value: i.relayhistoryOrigin }, ...claimlessWindowEnv(i.claimlessWindow), ...tenants.extraEnv],
      deployment: { name: "hosted", id: i.stage },
    },
    otel: {
      enabled: true,
      fullnameOverride: "otel-collector",
      // The chart pulls by digest when one is set; the tag stays for readability.
      image: { repository: PINNED_IMAGES.otelCollector.repository, tag: PINNED_IMAGES.otelCollector.tag, digest: PINNED_IMAGES.otelCollector.digest },
      // No Kubernetes API access is needed (no k8sattributes): no API token in the pod. EKS Pod
      // Identity mounts its own audience-bound token, so the S3 exporter is unaffected.
      serviceAccount: { automountServiceAccountToken: false },
      podSecurityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: { type: "RuntimeDefault" } },
      securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } },
      config: {
        exporters: {
          nop: {},
          "awss3/traces": {
            s3uploader: {
              region: i.lockerRegion,
              s3_bucket: i.lockerBucket,
              s3_base_prefix: "telemetry",
              s3_prefix: "traces",
              s3_partition_timezone: "UTC",
              unique_key_func_name: "uuidv7",
              compression: "gzip",
            },
            marshaler: "otlp_json",
            timeout: "20s",
            sending_queue: { enabled: true, num_consumers: 2, queue_size: 100 },
          },
        },
        processors: {
          "attributes/drop_forbidden": {
            actions: ["prompt", "summary", "message", "path", "body"].map((key) => ({ key, action: "delete" })),
          },
          redaction: {
            allow_all_keys: true,
            blocked_values: [
              "AKIA[0-9A-Z]{16}",
              "ASIA[0-9A-Z]{16}",
              "sk-[A-Za-z0-9_-]{16,}",
              "xox[abpr]-[A-Za-z0-9-]{10,}",
              "-----BEGIN [A-Z ]*PRIVATE KEY-----",
              "eyJ[A-Za-z0-9_-]{10,}[.][A-Za-z0-9_-]{10,}[.][A-Za-z0-9_-]{10,}",
              "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}",
            ],
            summary: "silent",
          },
        },
        service: {
          pipelines: {
            traces: {
              receivers: ["otlp"],
              processors: ["memory_limiter", "attributes/drop_forbidden", "redaction", "batch"],
              exporters: ["awss3/traces"],
            },
            // The services currently emit traces; keep unused signals explicit and non-exporting.
            metrics: { receivers: ["otlp"], processors: ["memory_limiter", "batch"], exporters: ["nop"] },
            logs: { receivers: ["otlp"], processors: ["memory_limiter", "batch"], exporters: ["nop"] },
          },
        },
      },
    },
    hoopdb: cnpg ? cnpgHoopdbValues(cnpg.operatorRelease) : { enabled: sandbox },
    locker: { enabled: false },
  };
}

/** The pre-split shared role, byte for byte, so keeping it is a no-op on a stage that has it. */
export function legacyPodsRoleProps(i: Pick<EksStageInputs, "stage" | "lockerBucketArn" | "lockerKmsKeyArn" | "permissionsBoundaryArn" | "tags">) {
  return {
    roleName: `96bulls-${i.stage}-pods`,
    description: `96bulls ${i.stage}: worker and ingest pods (EKS Pod Identity)`,
    assumeRolePolicyDocument: {
      Version: "2012-10-17" as const,
      Statement: [{ Effect: "Allow" as const, Principal: { Service: "pods.eks.amazonaws.com" }, Action: ["sts:AssumeRole", "sts:TagSession"] }],
    },
    inlinePolicies: {
      locker: {
        Version: "2012-10-17" as const,
        Statement: [
          { Effect: "Allow" as const, Action: ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetBucketVersioning"], Resource: [i.lockerBucketArn] },
          { Effect: "Allow" as const, Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"], Resource: [`${i.lockerBucketArn}/*`] },
          { Effect: "Allow" as const, Action: ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"], Resource: [i.lockerKmsKeyArn] },
        ],
      },
    },
    maxSessionDuration: "1 hour" as const,
    ...(i.permissionsBoundaryArn ? { permissionsBoundary: i.permissionsBoundaryArn } : {}),
    tags: { ...i.tags, ...(i.permissionsBoundaryArn ? { "96bulls:bounded": "true" } : {}) },
  };
}

export const eksStage = (i: EksStageInputs) =>
  Effect.gen(function* () {
    // Refuse before declaring anything billable.
    if (i.relayauth && i.relayauthIssuer)
      throw new Error("96bulls: eks.relayauth (self-hosted) and eks.relayauthIssuer are both set; the self-hosted issuer is the in-cluster RelayAuth. Remove relayauthIssuer. Nothing was created.");
    // Umbrella 0.3.0 (worker 0.4.0) is the first to give Bob Costas the summary key; older
    // charts would start it without one and it refuses to run.
    const [maj, min] = i.chartVersion.split(".").map(Number);
    if (!(maj! > 0 || (maj === 0 && min! >= 3)))
      throw new Error(`96bulls: eks.chartVersion ${i.chartVersion} is too old; 0.3.0+ (worker 0.4.0) is required for summary encryption. Nothing was created.`);
    assertServicesImage(i);
    const endpointAccess = clusterEndpointAccess(i.apiAccess?.publicAccessCidrs);
    const backend = hoopdbBackend(i.stage, i.hoopdb?.backend);
    if (i.relaycast) assertRelaycastInputs(i.relaycast);
    const sessionDirectory = Boolean(i.sessionDirectory?.enabled);
    if (sessionDirectory) assertSessionDirectoryPreconditions({ hoopdb: backend, relayauthTls: Boolean(i.relayauth) });
    const network = yield* AWS.EC2.Network("network", {
      cidrBlock: VPC_CIDR,
      availabilityZones: 2,
      nat: "single",
      gatewayEndpoints: ["s3"],
      tags: i.tags,
    });
    // Control-plane logs: the group exists (with retention) before EKS starts writing,
    // otherwise EKS creates it with no expiry. The cluster tag below orders the two.
    const clusterName = `96bulls-${i.stage}`;
    const controlPlaneLogs = yield* AWS.Logs.LogGroup("control-plane-logs", {
      logGroupName: controlPlaneLogGroupName(clusterName),
      retention: CONTROL_PLANE_LOG_RETENTION,
      tags: i.tags,
    });
    // Endpoint access and logging are UpdateClusterConfig changes: applied in place, no replacement.
    const cluster = yield* AWS.EKS.Cluster("cluster", {
      clusterName,
      compute: "auto",
      resourcesVpcConfig: { subnetIds: network.privateSubnetIds, ...endpointAccess },
      logging: controlPlaneLogging(),
      // Protection only for production; dev and sandboxes must tear down cleanly with `96bulls down`.
      deletionProtection: i.stage === "aws-prod",
      tags: { ...i.tags, "96bulls:control-plane-logs": controlPlaneLogs.logGroupName as unknown as string },
    });
    const sandbox = i.stage.startsWith("aws-sandbox-");

    // EKS Pod Identity: one role per service account, none shared (hardening.ts `podRoles`).
    // Trust is bound to this account and this cluster. No static keys anywhere.
    const keyBase = tenantWiring(resolveTenantOrgs(i)).keyBase;
    const refs = { lockerBucketArn: i.lockerBucketArn, lockerKmsKeyArn: i.lockerKmsKeyArn, lockerRegion: i.lockerRegion, ...(keyBase ? { keyBase } : {}) };
    for (const r of podRoles(refs)) {
      const role = yield* AWS.IAM.Role(`${r.id}-role`, {
        roleName: `96bulls-${i.stage}-${r.id}`,
        description: `96bulls ${i.stage}: ${r.description} (Pod Identity ${NAMESPACE}/${r.serviceAccount})`,
        assumeRolePolicyDocument: podIdentityTrust(i.accountId, cluster.clusterArn as unknown as string),
        inlinePolicies: { [r.policyName]: r.policy as never },
        maxSessionDuration: "1 hour",
        ...(i.permissionsBoundaryArn ? { permissionsBoundary: i.permissionsBoundaryArn } : {}),
        tags: {
          ...i.tags,
          "96bulls:service-account": `${NAMESPACE}/${r.serviceAccount}`,
          ...(i.permissionsBoundaryArn ? { "96bulls:bounded": "true" } : {}),
        },
      });
      // Logical ids kept from the shared-role layout so the existing associations update in place.
      const associationId = r.serviceAccount === SERVICE_ACCOUNTS.otel ? "pod-identity-otel" : `pod-identity-${r.serviceAccount}`;
      yield* AWS.EKS.PodIdentityAssociation(associationId, {
        clusterName: cluster.clusterName,
        namespace: NAMESPACE,
        serviceAccount: r.serviceAccount,
        roleArn: role.roleArn,
        tags: i.tags,
      });
    }

    // Encrypted gp3 for every in-cluster volume (RelayAuth, HoopDb, Relaycast).
    const inClusterVolumes = Boolean(i.relayauth) || backend === "cnpg" || Boolean(i.relaycast) || sandbox;
    if (inClusterVolumes) yield* Kubernetes.Manifest("storageclass-gp3", { cluster: cluster as never, manifest: storageClassManifest(i.purgeOnDestroy ? "Delete" : "Retain") });

    // Rollback window for the IAM split: the old shared role stays (attached to nothing) until the
    // per-SA roles are verified, and goes in a later, separate apply that drops `legacyPodsRole`.
    if (i.legacyPodsRole === "keep") yield* AWS.IAM.Role("pods-role", legacyPodsRoleProps(i));

    // In-cluster HoopDb for the isolated proof stage only (hoopdbBackend refuses production).
    const cnpgOperator = backend === "cnpg"
      ? yield* Kubernetes.HelmChart("cnpg-operator", {
          cluster,
          chart: CNPG_OPERATOR_CHART.chart,
          repo: CNPG_OPERATOR_CHART.repo,
          version: CNPG_OPERATOR_CHART.version,
          releaseName: "cnpg",
          namespace: CNPG_NAMESPACE,
          createNamespace: true,
          values: cnpgOperatorValues(),
        })
      : undefined;

    // Self-hosted RelayAuth first, so AlleyOop's issuer and JWKS come from this environment.
    const selfAuth = i.relayauth
      ? yield* relayauthStage({ cluster, chartVersion: i.relayauth.chartVersion, secretName: i.relayauth.secretName, clientNamespace: "96bulls" })
      : undefined;
    const authIssuer = selfAuth?.issuer ?? i.relayauthIssuer ?? "https://relayauth.dev";
    const authJwksUrl = selfAuth?.jwksUrl ?? i.relayauthJwksUrl;

    const chart = yield* Kubernetes.HelmChart("96bulls", {
      cluster,
      chart: "96bulls",
      repo: i.chartRepo ?? "https://AgentWorkforce.github.io/helm-charts",
      version: i.chartVersion,
      releaseName: "96bulls",
      namespace: "96bulls",
      createNamespace: true,
      values: eksChartValues(i, sandbox, authIssuer, authJwksUrl, cnpgOperator ? { operatorRelease: cnpgOperator.releaseName as unknown as string } : undefined),
    });

    const relaycast = i.relaycast
      ? yield* Kubernetes.HelmChart("relaycast", {
          cluster,
          chart: "relaycast",
          repo: i.chartRepo ?? "https://AgentWorkforce.github.io/helm-charts",
          version: i.relaycast.chartVersion,
          releaseName: "relaycast",
          namespace: RELAYCAST_NAMESPACE,
          createNamespace: true,
          values: relaycastValues(i.relaycast),
        })
      : undefined;

    // Default-deny + explicit allows, applied after the namespaces exist (the chart
    // release names in the annotation order each policy after its release).
    const releases: Record<string, string> = { [NAMESPACE]: chart.releaseName as unknown as string };
    if (selfAuth) releases.relayauth = selfAuth.release as unknown as string;
    if (relaycast) releases[RELAYCAST_NAMESPACE] = relaycast.releaseName as unknown as string;
    for (const policy of networkPolicies({ hoopdb: backend, relayauth: Boolean(selfAuth), relaycast: Boolean(relaycast), sessionDirectory, ingestScheme: i.ingress?.scheme ?? "internet-facing" })) {
      const { namespace, name } = policy.metadata;
      yield* Kubernetes.Manifest(`netpol-${namespace}-${name}`, {
        cluster: cluster as never,
        manifest: { ...policy, metadata: { ...policy.metadata, annotations: { "96bulls.agentrelay.com/after-release": releases[namespace]! } } },
      });
    }
    // Enforcement is a separate, later apply (step 2): with it off, the policies above are accepted and
    // ignored, so an interrupted first apply can never leave workloads behind a half-written deny.
    if (i.networkPolicyEnforcement)
      yield* Kubernetes.Manifest("network-policy-controller", { cluster: cluster as never, manifest: networkPolicyControllerConfigMap() });
    const publicIngest = i.ingress
      ? yield* ingressStage({
          stage: i.stage,
          accountId: i.accountId,
          ...(i.purgeOnDestroy ? { purgeOnDestroy: true } : {}),
          region: i.lockerRegion,
          cluster,
          hostname: i.ingress.hostname,
          cloudflareZoneId: i.ingress.cloudflareZoneId,
          zoneDomain: i.ingress.zoneDomain,
          namespace: "96bulls",
          serviceName: "96bulls-alleyoop",
          servicePort: 8080,
          cloudflareTokenSecret: i.ingress.cloudflareTokenSecret,
          ...(i.ingress.certValidation ? { certValidation: i.ingress.certValidation } : {}),
          exposure: { ...(i.ingress.scheme ? { scheme: i.ingress.scheme } : {}), ...(i.ingress.rateLimitPer5Min !== undefined ? { rateLimitPer5Min: i.ingress.rateLimitPer5Min } : {}) },
          tags: i.tags,
        })
      : undefined;
    return {
      clusterName: cluster.clusterName,
      endpoint: cluster.endpoint,
      endpointAccess,
      chartRelease: chart.releaseName,
      hoopdbBackend: backend,
      relaycast: relaycast?.releaseName ?? null,
      hostname: publicIngest?.hostname ?? null,
      ingestScheme: publicIngest?.scheme ?? null,
      relayauthIssuer: authIssuer,
      // Endpoints the proof harness calls (in-cluster only).
      harness: {
        tenantOrgIds: resolveTenantOrgs(i),
        // Secret-bearing RelayAuth calls (relayauth #116 exchange) go here, TLS 1.3, never to the plain Service.
        relayauthTls: selfAuth
          ? { service: RELAYAUTH_TLS_SERVICE, namespace: RELAYAUTH_TLS_NAMESPACE, port: RELAYAUTH_TLS_PORT, url: RELAYAUTH_TLS_URL, clientLabel: RELAYAUTH_TLS_CLIENT_LABEL,
              // Not created by the stack: the operator publishes it from the relayauth-tls Secret (docs/aws-stage.md, "Transport").
              caConfigMap: { name: "relayauth-ca", key: "ca.crt", namespace: NAMESPACE, publishedBy: "operator" } }
          : null,
        sessionDirectoryPostgrest: sessionDirectory
          ? { service: SESSION_DIRECTORY_POSTGREST.service, namespace: SESSION_DIRECTORY_POSTGREST.namespace, port: SESSION_DIRECTORY_POSTGREST.port, url: SESSION_DIRECTORY_POSTGREST.url, clientLabel: SEARCH_CLIENT_LABEL }
          : null,
      },
    };
  });
