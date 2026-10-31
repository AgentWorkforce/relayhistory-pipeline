/**
 * Pure hardening contracts for the AWS proof stage: EKS API access, control-plane
 * logging, image provenance, per-service-account IAM and the CloudTrail key.
 *
 * Nothing here loads a provider or touches the network, so every contract is
 * unit-tested (stacks/aws/__tests__/hardening.test.ts) and the stack calls the
 * same functions before it declares a resource.
 *
 * The S3 grants are derived from the code that uses the Locker, not guessed:
 *
 *   key layout   packages/object-store/src/layout.ts   `{LAYOUT_VERSION}/ws=…/s=…/` + manifest.json | raw/… | summary/…
 *   AlleyOop     src/alleyoop/manifests.ts              head + get manifests and chunks; put raw/ chunks and manifest.json (conditional)
 *   Bob Costas   src/bob-costas/summary-objects.ts     head + get session objects; put summary/{short,long}.json.enc (conditional)
 *   indexer      src/hoopdb/indexer.ts                  list `v1` (reconcile) + get manifests
 *   referee      src/referee/main.ts                    get manifests
 *
 * The ObjectStore interface has no delete, so no pod role carries s3:DeleteObject.
 * The deployment sets no PIPELINE_KEY_PREFIX, so every session key starts at `v1/`.
 */
import { LAYOUT_VERSION } from "../../packages/object-store/src/layout.ts";

/** The proof VPC (stacks/aws/eks.ts `AWS.EC2.Network`). NetworkPolicies and the API allowlist refer to it. */
export const VPC_CIDR = "10.42.0.0/16";

// ---------------------------------------------------------------------------
// Image provenance
// ---------------------------------------------------------------------------

export const DIGEST_RE = /^sha256:[0-9a-f]{64}$/;

/**
 * A digest, or a full reference that ends in one. Tag-only references
 * (`repo:tag`, `latest`, an empty value) are refused: a tag can be repointed,
 * a digest cannot.
 */
export function assertDigestPinned(field: string, ref: string | undefined): string {
  const value = (ref ?? "").trim();
  const at = value.lastIndexOf("@");
  const digest = at >= 0 ? value.slice(at + 1) : value;
  if (!DIGEST_RE.test(digest)) {
    const why = value === "" ? "is missing" : at < 0 && !value.startsWith("sha256:") ? `is a tag-only reference (${JSON.stringify(value)})` : `is not an immutable sha256 digest (${JSON.stringify(value)})`;
    throw new Error(`96bulls: ${field} ${why}; pin it by digest (sha256:<64 hex>). Nothing was created.`);
  }
  return digest;
}

/**
 * The services image must be a published digest with the commit it was built
 * from, so a reviewer can tie the running bytes to reviewed source.
 */
export function assertServicesImage(i: { imageDigest?: string; imageSourceRevision?: string }) {
  if (!DIGEST_RE.test(i.imageDigest ?? "")) {
    assertDigestPinned("eks.imageDigest", i.imageDigest);
    throw new Error("96bulls: eks.imageDigest must be the bare digest (sha256:<64 hex>); the repository is this account's ECR. Nothing was created.");
  }
  if (!/^[0-9a-f]{40}$/.test(i.imageSourceRevision ?? ""))
    throw new Error("96bulls: eks.imageSourceRevision must be the full 40-hex commit the eks.imageDigest image was built from (publish-image-ecr records it). Nothing was created.");
}

/**
 * Third-party images the AWS stage runs, pinned by digest. The same values are
 * recorded in versions.lock.json; a test keeps the two equal.
 */
export const PINNED_IMAGES = {
  otelCollector: {
    repository: "otel/opentelemetry-collector-contrib",
    tag: "0.163.2",
    digest: "sha256:7f22d2a6748110ffb6a59bc0df448bccbf501cdb667d610ede4c61d3863d5849",
  },
  cnpgOperator: {
    repository: "ghcr.io/cloudnative-pg/cloudnative-pg",
    tag: "1.30.1",
    digest: "sha256:923c267ec29636db3bee20f993d0ec4973fa22998e1adad37da79e4d32b5bc07",
  },
  cnpgPostgres: {
    repository: "ghcr.io/cloudnative-pg/postgresql",
    tag: "17-standard-trixie",
    digest: "sha256:ed6923f9ba2399cf9070379a353c710c29ced8a294be73986a2c3dae7e599ce3",
  },
} as const;

// ---------------------------------------------------------------------------
// EKS API endpoint
// ---------------------------------------------------------------------------

/** Broadest public allowlist entry accepted: a single office or VPN range, never the internet. */
export const MIN_PUBLIC_CIDR_PREFIX = 16;
/** EKS's own limit on publicAccessCidrs. */
const MAX_PUBLIC_CIDRS = 40;

/**
 * Private endpoint always on. The public endpoint is OFF unless the stage lists
 * explicit operator CIDRs (`eks.apiAccess.publicAccessCidrs`). There is no
 * 0.0.0.0/0 default and it is refused if written.
 */
export function clusterEndpointAccess(publicAccessCidrs: readonly string[] | undefined) {
  const cidrs = [...(publicAccessCidrs ?? [])];
  if (cidrs.length > MAX_PUBLIC_CIDRS) throw new Error(`96bulls: eks.apiAccess.publicAccessCidrs has ${cidrs.length} entries; EKS allows ${MAX_PUBLIC_CIDRS}. Nothing was created.`);
  for (const cidr of cidrs) {
    const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/.exec(cidr);
    if (!m || m.slice(1, 5).some((o) => Number(o) > 255) || Number(m[5]) > 32)
      throw new Error(`96bulls: eks.apiAccess.publicAccessCidrs entry ${JSON.stringify(cidr)} is not an IPv4 CIDR. Nothing was created.`);
    if (Number(m[5]) < MIN_PUBLIC_CIDR_PREFIX)
      throw new Error(`96bulls: eks.apiAccess.publicAccessCidrs entry ${cidr} is broader than /${MIN_PUBLIC_CIDR_PREFIX}; list the operator's address (/32) or a bastion range, never the internet. Nothing was created.`);
  }
  if (new Set(cidrs).size !== cidrs.length) throw new Error("96bulls: eks.apiAccess.publicAccessCidrs has duplicates. Nothing was created.");
  return cidrs.length
    ? { endpointPrivateAccess: true, endpointPublicAccess: true, publicAccessCidrs: cidrs.sort() }
    : { endpointPrivateAccess: true, endpointPublicAccess: false };
}

// ---------------------------------------------------------------------------
// Control-plane logging
// ---------------------------------------------------------------------------

export const CONTROL_PLANE_LOG_TYPES = ["api", "audit", "authenticator", "controllerManager", "scheduler"] as const;
/** Audit evidence for the proof window plus a quarter; CloudWatch's default would be "never expire". */
export const CONTROL_PLANE_LOG_RETENTION = "90 days";
/** The group EKS writes to. Declared first so retention applies from the first line. */
export const controlPlaneLogGroupName = (clusterName: string) => `/aws/eks/${clusterName}/cluster`;

export function controlPlaneLogging() {
  return { clusterLogging: [{ types: [...CONTROL_PLANE_LOG_TYPES], enabled: true }] };
}

// ---------------------------------------------------------------------------
// Per-service-account IAM (EKS Pod Identity)
// ---------------------------------------------------------------------------

export const NAMESPACE = "96bulls";

/**
 * One IAM role per Kubernetes service account. Worker 0.4.0 runs every worker
 * component (hoopdb-indexer, referee, bob-costas, migrate) under one service
 * account, so that is the finest split Pod Identity can express today; the
 * worker role is the union of what those components do and nothing more.
 */
export const SERVICE_ACCOUNTS = {
  worker: "96bulls-worker",
  alleyoop: "96bulls-alleyoop",
  /** session_directory_search_worker (src/hoopdb search RPC): HoopDb only, through PostgREST. */
  searchWorker: "96bulls-search-worker",
  otel: "otel-collector",
} as const;

export interface LockerRefs {
  lockerBucketArn: string;
  lockerKmsKeyArn: string;
  lockerRegion: string;
}

const sessionObjects = (bucketArn: string, tail: string) => `${bucketArn}/${LAYOUT_VERSION}/*${tail}`;

/**
 * S3 HEAD on a missing key answers 404 only when the caller holds s3:ListBucket
 * on the bucket (otherwise 403), and both conditional writers HEAD first. A
 * `s3:prefix` condition is absent from HEAD/GET request context, so it cannot be
 * used to narrow this grant without turning every miss into 403.
 */
const listBucket = (bucketArn: string) => ({ Sid: "ListForMissingKeySemantics", Effect: "Allow" as const, Action: ["s3:ListBucket"], Resource: [bucketArn] });

/** SSE-KMS through S3 only: GenerateDataKey to write, Decrypt to read (and for bucket keys). No kms:Encrypt or DescribeKey. */
const lockerKms = (i: LockerRefs, write: boolean) => ({
  Sid: write ? "LockerKeyReadWriteViaS3" : "LockerKeyReadViaS3",
  Effect: "Allow" as const,
  Action: write ? ["kms:Decrypt", "kms:GenerateDataKey"] : ["kms:Decrypt"],
  Resource: [i.lockerKmsKeyArn],
  Condition: { StringEquals: { "kms:ViaService": `s3.${i.lockerRegion}.amazonaws.com` } },
});

/**
 * Worker service account: hoopdb-indexer (list + read), referee (read),
 * bob-costas (read + write encrypted summaries), migrate (no S3).
 */
export function workerPolicy(i: LockerRefs) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      listBucket(i.lockerBucketArn),
      { Sid: "ReadSessionObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: [sessionObjects(i.lockerBucketArn, "")] },
      { Sid: "WriteEncryptedSummaries", Effect: "Allow" as const, Action: ["s3:PutObject"], Resource: [sessionObjects(i.lockerBucketArn, "/summary/*")] },
      lockerKms(i, true),
    ],
  };
}

/** AlleyOop (ingest + courier routes): read session objects, write raw chunks and manifests. */
export function alleyoopPolicy(i: LockerRefs) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      listBucket(i.lockerBucketArn),
      { Sid: "ReadSessionObjects", Effect: "Allow" as const, Action: ["s3:GetObject"], Resource: [sessionObjects(i.lockerBucketArn, "")] },
      {
        Sid: "WriteRawChunksAndManifests",
        Effect: "Allow" as const,
        Action: ["s3:PutObject"],
        Resource: [sessionObjects(i.lockerBucketArn, "/raw/*"), sessionObjects(i.lockerBucketArn, "/manifest.json")],
      },
      lockerKms(i, true),
    ],
  };
}

/**
 * The search worker reads HoopDb through a tenant-bound RPC and needs no AWS
 * access at all. It still gets its own role, so its provenance is explicit and
 * nothing can quietly attach it to a Locker-capable one: the policy denies every
 * S3 action on the Locker and every KMS action on its key, which survives a
 * later Allow added by mistake.
 */
export function searchWorkerPolicy(i: LockerRefs) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "NoLockerAccess", Effect: "Deny" as const, Action: ["s3:*"], Resource: [i.lockerBucketArn, `${i.lockerBucketArn}/*`] },
      { Sid: "NoLockerKey", Effect: "Deny" as const, Action: ["kms:*"], Resource: [i.lockerKmsKeyArn] },
    ],
  };
}

/** Unchanged contract (docs/aws-stage.md "AWS observability"): append-only under telemetry/traces/. */
export function telemetryPolicy(i: LockerRefs) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Effect: "Allow" as const,
        Action: ["s3:PutObject", "s3:AbortMultipartUpload"],
        Resource: [`${i.lockerBucketArn}/telemetry/traces/*`],
      },
      {
        Effect: "Allow" as const,
        Action: ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"],
        Resource: [i.lockerKmsKeyArn],
        Condition: { StringEquals: { "kms:ViaService": `s3.${i.lockerRegion}.amazonaws.com` } },
      },
    ],
  };
}

/**
 * Pod Identity trust, bound to this account and this cluster (confused-deputy
 * guard from the EKS Pod Identity documentation).
 */
export function podIdentityTrust(accountId: string, clusterArn: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Effect: "Allow" as const,
        Principal: { Service: "pods.eks.amazonaws.com" },
        Action: ["sts:AssumeRole", "sts:TagSession"],
        Condition: { StringEquals: { "aws:SourceAccount": accountId }, ArnEquals: { "aws:SourceArn": clusterArn } },
      },
    ],
  };
}

export interface PodRoleSpec {
  /** Alchemy logical id; also the role name suffix. */
  id: string;
  serviceAccount: string;
  description: string;
  policyName: string;
  policy: { Version: "2012-10-17"; Statement: unknown[] };
}

/** Every pod role the stage declares, one per service account, none shared. */
export function podRoles(i: LockerRefs): PodRoleSpec[] {
  return [
    { id: "worker", serviceAccount: SERVICE_ACCOUNTS.worker, description: "worker components (indexer, referee, bob-costas, migrate): read sessions, write encrypted summaries", policyName: "locker", policy: workerPolicy(i) },
    { id: "alleyoop", serviceAccount: SERVICE_ACCOUNTS.alleyoop, description: "AlleyOop ingest: read sessions, write raw chunks and manifests", policyName: "locker", policy: alleyoopPolicy(i) },
    { id: "search-worker", serviceAccount: SERVICE_ACCOUNTS.searchWorker, description: "session directory search worker: HoopDb only, explicitly no Locker or KMS", policyName: "no-locker", policy: searchWorkerPolicy(i) },
    { id: "telemetry", serviceAccount: SERVICE_ACCOUNTS.otel, description: "OpenTelemetry collector writes trace batches to S3", policyName: "telemetry", policy: telemetryPolicy(i) },
  ];
}

// ---------------------------------------------------------------------------
// CloudTrail: versioned bucket encrypted with its own key
// ---------------------------------------------------------------------------

/** Key administration without key use: PutKeyPolicy stays, so the account can never lock itself out. */
export const KMS_ADMIN_ACTIONS = [
  "kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*", "kms:Update*", "kms:Revoke*", "kms:Disable*",
  "kms:Get*", "kms:Delete*", "kms:TagResource", "kms:UntagResource", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion", "kms:RotateKeyOnDemand",
] as const;

/**
 * Key policy for the trail's own KMS key, least privilege: the account root may
 * administer the key (no lockout) but not use it; CloudTrail may generate data
 * keys for this trail only; account principals may decrypt trail log files.
 */
export function trailKeyPolicy(accountId: string, region: string, trailName: string) {
  const trailArn = `arn:aws:cloudtrail:${region}:${accountId}:trail/${trailName}`;
  const context = { "kms:EncryptionContext:aws:cloudtrail:arn": `arn:aws:cloudtrail:*:${accountId}:trail/*` };
  return {
    Version: "2012-10-17" as const,
    Statement: [
      // Administration only: the account can manage (and repair) the key through IAM, but using it
      // to encrypt or decrypt is granted below, to CloudTrail and for trail log files only.
      { Sid: "AccountAdministersKey", Effect: "Allow" as const, Principal: { AWS: `arn:aws:iam::${accountId}:root` }, Action: [...KMS_ADMIN_ACTIONS], Resource: "*" },
      {
        Sid: "CloudTrailEncryptsLogs",
        Effect: "Allow" as const,
        Principal: { Service: "cloudtrail.amazonaws.com" },
        Action: ["kms:GenerateDataKey*"],
        Resource: "*",
        Condition: { StringEquals: { "aws:SourceArn": trailArn }, StringLike: context },
      },
      {
        Sid: "CloudTrailDescribesKey",
        Effect: "Allow" as const,
        Principal: { Service: "cloudtrail.amazonaws.com" },
        Action: ["kms:DescribeKey"],
        Resource: "*",
        Condition: { StringEquals: { "aws:SourceArn": trailArn } },
      },
      {
        Sid: "AccountReadsLogs",
        Effect: "Allow" as const,
        Principal: { AWS: "*" },
        Action: ["kms:Decrypt", "kms:ReEncryptFrom"],
        Resource: "*",
        Condition: { StringEquals: { "kms:CallerAccount": accountId }, StringLike: context },
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// HoopDb backend
// ---------------------------------------------------------------------------

/**
 * `external`: HOOPDB_URL from the runtime Secret (hosted production is Supabase,
 * docs/decisions/0001-production-data-plane.md). `cnpg`: an in-cluster
 * CloudNativePG Postgres 17, for the isolated proof stage only.
 */
export type HoopDbBackend = "external" | "cnpg";

export function hoopdbBackend(stage: string, backend: string | undefined): HoopDbBackend {
  const b = backend ?? "external";
  if (b !== "external" && b !== "cnpg") throw new Error(`96bulls: eks.hoopdb.backend must be "external" or "cnpg", got ${JSON.stringify(backend)}. Nothing was created.`);
  if (b === "cnpg" && /prod/.test(stage))
    throw new Error(`96bulls: stage ${stage} looks like production; production HoopDb is Supabase (docs/decisions/0001-production-data-plane.md), never in-cluster CNPG. Nothing was created.`);
  return b;
}

// ---------------------------------------------------------------------------
// Buckets and the GitHub publisher
// ---------------------------------------------------------------------------

/** Every bucket refuses plain-HTTP requests. */
export function denyInsecureTransport(bucketArn: string) {
  return [
    {
      Sid: "DenyInsecureTransport",
      Effect: "Deny" as const,
      Principal: "*",
      Action: ["s3:*"],
      Resource: [bucketArn, `${bucketArn}/*`],
      Condition: { Bool: { "aws:SecureTransport": "false" } },
    },
  ];
}

/**
 * Locker lifecycle. Manifests are overwritten per revision, so versioning keeps
 * every superseded manifest; nothing reads old versions (no GetObjectVersion
 * anywhere), so they expire after 90 days. Incomplete multipart uploads go after 7.
 */
export function lockerLifecycleRules() {
  return [
    { ID: "expire-noncurrent-versions", Status: "Enabled", Filter: { Prefix: "" }, NoncurrentVersionExpiration: { NoncurrentDays: 90 } },
    { ID: "abort-incomplete-uploads", Status: "Enabled", Filter: { Prefix: "" }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 } },
  ];
}

/**
 * The GitHub publisher may push the services image to this account's ECR
 * repository and nothing else: no Locker access, no KMS.
 */
export function ecrPushPolicy(region: string, accountId: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "EcrLogin", Effect: "Allow" as const, Action: ["ecr:GetAuthorizationToken"], Resource: ["*"] },
      {
        Sid: "PushServicesImage",
        Effect: "Allow" as const,
        Action: ["ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage", "ecr:DescribeImages"],
        Resource: [`arn:aws:ecr:${region}:${accountId}:repository/96bulls-services`],
      },
    ],
  };
}

/**
 * Trusted only for jobs of this repository (immutable numeric subject) running
 * in the `image-publish` environment. No per-stage deploy subject: no workflow
 * deploys the stage. The environment's branch policy (main only) is a GitHub
 * setting and must be on; the subject cannot carry the ref for environment jobs.
 */
export function githubImagePublishTrust(oidcProviderArn: string, subPrefix: string) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      {
        Effect: "Allow" as const,
        Principal: { Federated: oidcProviderArn },
        Action: ["sts:AssumeRoleWithWebIdentity"],
        Condition: {
          StringEquals: {
            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
            "token.actions.githubusercontent.com:sub": `${subPrefix}:environment:image-publish`,
          },
        },
      },
    ],
  };
}
