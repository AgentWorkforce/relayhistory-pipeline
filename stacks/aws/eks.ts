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
import { ingressStage } from "./ingress.ts";
import { relayauthStage } from "./relayauth.ts";

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
  /** The Locker bucket ARN and its KMS key ARN, for the pods' Pod Identity role. */
  lockerBucketArn: string;
  lockerKmsKeyArn: string;
  /** Sandbox stages: every role we create carries this permissions boundary and the bounded tag. */
  permissionsBoundaryArn?: string;
  /** Public hostname for the ingest (Cloudflare DNS). Omit to keep the ingest cluster-internal. */
  ingress?: { hostname: string; cloudflareZoneId: string; zoneDomain: string; cloudflareTokenSecret: string; certValidation?: { name: string; value: string } };
  tags: Record<string, string>;
}

export const eksStage = (i: EksStageInputs) =>
  Effect.gen(function* () {
    // Refuse before declaring anything billable.
    if (i.relayauth && i.relayauthIssuer)
      throw new Error("96bulls: eks.relayauth (self-hosted) and eks.relayauthIssuer are both set; the self-hosted issuer is the in-cluster RelayAuth. Remove relayauthIssuer. Nothing was created.");
    if (!/^sha256:[0-9a-f]{64}$/.test(i.imageDigest ?? ""))
      throw new Error("96bulls: eks.imageDigest is missing or invalid; run publish-image-ecr for this account and put its digest in aws-inputs.json. Nothing was created.");
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

    // EKS Pod Identity: one role for the 96bulls pods, Locker objects + its KMS key only.
    // No static keys anywhere; the services read short-lived credentials from the pod identity agent.
    const podRole = yield* AWS.IAM.Role("pods-role", {
      roleName: `96bulls-${i.stage}-pods`,
      description: `96bulls ${i.stage}: worker and ingest pods (EKS Pod Identity)`,
      assumeRolePolicyDocument: {
        Version: "2012-10-17",
        Statement: [{ Effect: "Allow", Principal: { Service: "pods.eks.amazonaws.com" }, Action: ["sts:AssumeRole", "sts:TagSession"] }],
      },
      inlinePolicies: {
        locker: {
          Version: "2012-10-17",
          Statement: [
            { Effect: "Allow", Action: ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetBucketVersioning"], Resource: [i.lockerBucketArn] },
            { Effect: "Allow", Action: ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"], Resource: [`${i.lockerBucketArn}/*`] },
            { Effect: "Allow", Action: ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"], Resource: [i.lockerKmsKeyArn] },
          ],
        },
      },
      maxSessionDuration: "1 hour",
      ...(i.permissionsBoundaryArn ? { permissionsBoundary: i.permissionsBoundaryArn } : {}),
      tags: { ...i.tags, ...(i.permissionsBoundaryArn ? { "96bulls:bounded": "true" } : {}) },
    });
    for (const sa of ["96bulls-worker", "96bulls-alleyoop"]) {
      yield* AWS.EKS.PodIdentityAssociation(`pod-identity-${sa}`, {
        clusterName: cluster.clusterName,
        namespace: "96bulls",
        serviceAccount: sa,
        roleArn: podRole.roleArn,
        tags: i.tags,
      });
    }

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
      // Keys per the released umbrella chart (helm-charts 96bulls 0.1.0; worker/alleyoop 0.2.0).
      values: {
        global: {
          imageRegistry: "", // the ECR repository below is fully qualified
          tenant: { orgId: i.tenantOrgId },
        },
        worker: {
          image: { repository: i.imageRepository, digest: i.imageDigest },
          hoopdb: sandbox ? {} : { existingSecret: i.runtimeSecretName, key: "HOOPDB_URL" },
          locker: { endpoint: "", bucket: i.lockerBucket, region: i.lockerRegion, credentials: { mode: "podIdentity" } },
          // Supabase already has anon/authenticated/service_role; the plain-Postgres shim is for in-cluster DBs only.
          migrations: { supabaseShim: sandbox },
          // The bucket is created and versioned by IaC; the pods' role has no CreateBucket.
          lockerInit: { enabled: false },
          deployment: { name: "hosted", id: i.stage },
        },
        alleyoop: {
          image: { repository: i.imageRepository, digest: i.imageDigest },
          hoopdb: sandbox ? {} : { existingSecret: i.runtimeSecretName, key: "HOOPDB_URL" },
          locker: { endpoint: "", bucket: i.lockerBucket, region: i.lockerRegion, credentials: { mode: "podIdentity" } },
          // The chart's issuer default is the local self-signed one; hosted tokens come from RelayAuth.
          auth: { jwksUrl: authJwksUrl, issuer: authIssuer },
          // alleyoop 0.3.0 has no relayhistoryOrigin key: the origin rides in extraEnv, so only
          // assertions minted for this deployment's origin are accepted.
          extraEnv: [{ name: "RELAYHISTORY_ORIGIN", value: i.relayhistoryOrigin }],
          deployment: { name: "hosted", id: i.stage },
        },
        // Sandboxes run HoopDb in-cluster (Khaliq: container Postgres); other stages use Supabase + S3.
        hoopdb: { enabled: sandbox },
        locker: { enabled: false },
      },
    });
    const publicIngest = i.ingress
      ? yield* ingressStage({
          stage: i.stage,
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
          tags: i.tags,
        })
      : undefined;
    return { clusterName: cluster.clusterName, endpoint: cluster.endpoint, chartRelease: chart.releaseName, hostname: publicIngest?.hostname ?? null, relayauthIssuer: authIssuer };
  });
