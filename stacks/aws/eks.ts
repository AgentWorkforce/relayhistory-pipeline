/**
 * First cloud compute target (Khaliq, 2026-10-28): EKS running helm-chart-lead's
 * charts. CODE ONLY, reached only through the gated AWS stage.
 *
 * Declares: a VPC with private subnets and one NAT, an EKS Auto Mode cluster,
 * and the `96bulls` umbrella chart (worker + alleyoop; HoopDb and the Locker come
 * from outside the cluster for non-sandbox stages) pinned to the published
 * services image digest. Secrets reach the pods through an existing Kubernetes
 * Secret synced from 1Password/Secrets Manager, never through chart values.
 */
import * as AWS from "@alchemy.run/aws";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import versions from "../../versions.lock.json" with { type: "json" };

export interface EksStageInputs {
  stage: string;
  /** Chart repository and versions (helm-charts, published via chart-releaser). */
  chartRepo?: string;
  chartVersion: string;
  /** Kubernetes Secret holding HOOPDB_URL, LOCKER_* and the JWKS settings; created by the secrets sync, not by IaC. */
  runtimeSecretName: string;
  lockerBucket: string;
  lockerRegion: string;
  tenantOrgId: string;
  relayauthJwksUrl: string;
  relayhistoryOrigin: string;
  /** The account's private ECR repository; nodes pull with their node role, no pull secret. */
  imageRepository: string;
  tags: Record<string, string>;
}

export const eksStage = (i: EksStageInputs) =>
  Effect.gen(function* () {
    const network = yield* AWS.EC2.Network("network", {
      cidrBlock: "10.42.0.0/16",
      availabilityZones: 2,
      nat: "single",
      gatewayEndpoints: ["s3"],
      tags: i.tags,
    });
    const cluster = yield* AWS.EKS.Cluster("cluster", {
      clusterName: `96bulls-${i.stage}`,
      compute: "auto",
      resourcesVpcConfig: { subnetIds: network.privateSubnetIds },
      // Protection only for production; dev and sandboxes must tear down cleanly with `96bulls down`.
      deletionProtection: i.stage === "aws-prod",
      tags: i.tags,
    });
    const sandbox = i.stage.startsWith("aws-sandbox-");
    // The hosted image is the ECR build; its digest is pinned after publish-image-ecr runs.
    const image = (versions.images as unknown as Record<string, { digest?: string | null }>)["services-ecr"];
    if (!image?.digest)
      throw new Error("96bulls: versions.lock.json has no services-ecr digest; run publish-image-ecr after the stage creates the repository, then pin it. Nothing was deployed to EKS.");
    const chart = yield* Kubernetes.HelmChart("96bulls", {
      cluster,
      chart: "96bulls",
      repo: i.chartRepo ?? "https://AgentWorkforce.github.io/helm-charts",
      version: i.chartVersion,
      releaseName: "96bulls",
      namespace: "96bulls",
      createNamespace: true,
      values: {
        global: {
          // Digest of the image pushed to ECR by publish-image-ecr (same build as versions.lock.json's digest).
          image: { repository: i.imageRepository, digest: image.digest },
          existingSecret: i.runtimeSecretName,
          env: {
            TENANT_ORG_ID: i.tenantOrgId,
            LOCKER_BUCKET: i.lockerBucket,
            LOCKER_REGION: i.lockerRegion,
            DEPLOYMENT: "hosted",
            DEPLOYMENT_ID: i.stage,
          },
        },
        // Keys per helm-chart-lead (umbrella helm-charts#15, 96bulls-worker 0.2.0).
        alleyoop: {
          auth: { jwksUrl: i.relayauthJwksUrl },
          relayhistoryOrigin: i.relayhistoryOrigin,
          ...(sandbox ? {} : { hoopdb: { existingSecret: i.runtimeSecretName } }),
        },
        worker: {
          // Supabase already has anon/authenticated/service_role; the plain-Postgres shim is for in-cluster DBs only.
          migrations: { supabaseShim: sandbox },
          ...(sandbox ? {} : { hoopdb: { existingSecret: i.runtimeSecretName } }),
        },
        // Sandboxes run HoopDb in-cluster (Khaliq: container Postgres); other stages use Supabase + S3.
        hoopdb: { enabled: sandbox },
        locker: { enabled: false },
      },
    });
    return { clusterName: cluster.clusterName, endpoint: cluster.endpoint, chartRelease: chart.releaseName };
  });
