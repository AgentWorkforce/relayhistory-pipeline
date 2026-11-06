/** Pure, unqualified C-auth policy proposal. No SDK, credentials, environment or resources. */
import { createHash } from "node:crypto";

export const C_AUTH_SOURCE = "3b8d86c3045cda422d3a20994a4cf400dc9ec62a";
export const E_CANDIDATE_ROOT = "96bulls/upload-inbox/v1";
const FIELDS = ["version", "accountId", "region", "bucketArn", "keyRoot", "kmsKeyArn", "runtimePrincipalArn", "ownerBootstrapPrincipalArn", "keyAdminPrincipalArn", "ePrincipalArn", "eBucketArn", "eAuthorityKeyRoot", "eCandidateKeyRoot", "transcriptBucketArn"] as const;
export interface RhcAuthStorePolicyInput {
  version: 1;
  accountId: string;
  region: "us-east-1";
  bucketArn: string;
  keyRoot: string;
  kmsKeyArn: string;
  /** Current C consumes primitive access-key/secret-key strings: no role broker is assumed. */
  runtimePrincipalArn: string;
  ownerBootstrapPrincipalArn: string;
  keyAdminPrincipalArn: string;
  ePrincipalArn: string;
  eBucketArn: string;
  eAuthorityKeyRoot: string;
  eCandidateKeyRoot: typeof E_CANDIDATE_ROOT;
  transcriptBucketArn: string;
}
export class RhcAuthStorePolicyError extends Error {
  constructor(readonly code: string) { super("C auth policy proposal refused: " + code); }
}
const refuse = (code: string): never => { throw new RhcAuthStorePolicyError(code); };
const credential = /(?:AKIA|ASIA)[A-Z0-9]{16}|-----BEGIN|postgres(?:ql)?:\/\//;
function plain(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  try {
    const prototype = Object.getPrototypeOf(value);
    return (prototype === Object.prototype || prototype === null) && Reflect.ownKeys(value).every(k => typeof k === "string" && "value" in Object.getOwnPropertyDescriptor(value, k)!);
  } catch { return false; }
}
function root(value: string): boolean {
  return value.length <= 200 && /^[A-Za-z0-9][A-Za-z0-9_/-]*$/.test(value) && !value.endsWith("/") && !value.includes("//");
}
function bucket(value: string): boolean {
  const name = value.replace(/^arn:aws:s3:::/, "");
  return value === "arn:aws:s3:::" + name && name.length >= 3 && name.length <= 63
    && /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(name) && !name.includes("..") && !name.includes(".-") && !name.includes("-.")
    && !/^\d+\.\d+\.\d+\.\d+$/.test(name) && !name.startsWith("xn--") && !name.endsWith("-s3alias") && !name.endsWith("--ol-s3");
}
const overlaps = (a: string, b: string) => a === b || a.startsWith(b + "/") || b.startsWith(a + "/");
function principal(value: string, account: string, userOnly = false): boolean {
  const prefix = `arn:aws:iam::${account}:`;
  if (!value.startsWith(prefix)) return false;
  const tail = value.slice(prefix.length);
  return (userOnly ? /^user\// : /^(user|role)\//).test(tail) && /^(user|role)\/[A-Za-z0-9_+=,.@/-]+$/.test(tail)
    && !tail.endsWith("/") && !tail.includes("//") && !tail.split("/").some(s => s === "." || s === "..");
}
export function validateRhcAuthStorePolicyInput(value: unknown): RhcAuthStorePolicyInput {
  if (!plain(value)) return refuse("input_shape");
  let data: Record<string, unknown>;
  try {
    const descriptors = Object.getOwnPropertyDescriptors(value);
    if (Object.keys(descriptors).sort().join("\0") !== [...FIELDS].sort().join("\0")) return refuse("input_fields");
    data = Object.fromEntries(FIELDS.map(k => [k, descriptors[k]!.value]));
  } catch (e) { if (e instanceof RhcAuthStorePolicyError) throw e; return refuse("input_shape"); }
  if (data.version !== 1 || FIELDS.some(k => k !== "version" && typeof data[k] !== "string")) return refuse("input_shape");
  for (const v of Object.values(data)) if (typeof v === "string" && credential.test(v)) return refuse("credential_input");
  const v = data as unknown as RhcAuthStorePolicyInput;
  if (!/^\d{12}$/.test(v.accountId)) return refuse("account");
  if (v.region !== "us-east-1") return refuse("region");
  if (![v.bucketArn, v.eBucketArn, v.transcriptBucketArn].every(bucket)) return refuse("bucket_arn");
  if (![v.keyRoot, v.eAuthorityKeyRoot, v.eCandidateKeyRoot].every(root)) return refuse("key_root");
  if (v.eCandidateKeyRoot !== E_CANDIDATE_ROOT || overlaps(v.eAuthorityKeyRoot, v.eCandidateKeyRoot)) return refuse("namespace_separation");
  if (v.bucketArn === v.eBucketArn || v.bucketArn === v.transcriptBucketArn || overlaps(v.keyRoot, v.eAuthorityKeyRoot) || overlaps(v.keyRoot, v.eCandidateKeyRoot)) return refuse("namespace_separation");
  const keyPrefix = `arn:aws:kms:${v.region}:${v.accountId}:key/`;
  if (!v.kmsKeyArn.startsWith(keyPrefix) || !/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(v.kmsKeyArn.slice(keyPrefix.length))) return refuse("kms_arn");
  if (!principal(v.runtimePrincipalArn, v.accountId, true) || ![v.ownerBootstrapPrincipalArn, v.keyAdminPrincipalArn, v.ePrincipalArn].every(p => principal(p, v.accountId))) return refuse("principal_arn");
  if ([v.ownerBootstrapPrincipalArn, v.keyAdminPrincipalArn, v.ePrincipalArn].includes(v.runtimePrincipalArn) || [v.ownerBootstrapPrincipalArn, v.keyAdminPrincipalArn].includes(v.ePrincipalArn)) return refuse("principal_separation");
  // Copy only data descriptors after validation; callers cannot mutate the captured input.
  return { ...v };
}
export function canonicalPolicyJson(value: unknown): string {
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length || !Object.keys(value).every((k, n) => k === String(n))) return refuse("non_json_plan");
    return "[" + value.map(canonicalPolicyJson).join(",") + "]";
  }
  if (plain(value)) return "{" + Object.keys(value).sort().map(k => JSON.stringify(k) + ":" + canonicalPolicyJson(value[k])).join(",") + "}";
  if (value === null || typeof value === "string" || typeof value === "boolean" || (typeof value === "number" && Number.isFinite(value))) return JSON.stringify(value);
  return refuse("non_json_plan");
}
function freeze<T>(value: T): Readonly<T> {
  if (value !== null && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}
const denyActions = ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:ListBucketVersions", "s3:PutObjectAcl", "s3:PutBucketAcl", "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutEncryptionConfiguration", "s3:PutBucketVersioning", "s3:PutBucketPublicAccessBlock", "s3:PutBucketOwnershipControls", "s3:PutObjectRetention", "s3:PutObjectLegalHold", "s3:DeleteBucket"];
const keyAdminActions = ["kms:DescribeKey", "kms:GetKeyPolicy", "kms:PutKeyPolicy", "kms:EnableKey", "kms:DisableKey", "kms:EnableKeyRotation", "kms:DisableKeyRotation", "kms:GetKeyRotationStatus", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion", "kms:TagResource", "kms:UntagResource", "kms:ListResourceTags"];
export function buildRhcAuthStorePlan(value: unknown) {
  const i = validateRhcAuthStorePolicyInput(value), base = `${i.bucketArn}/${i.keyRoot}`;
  const seal = `${base}/authority.json`;
  const mutable = [`${base}/sessions/cli/*/*.json`, `${base}/sessions/service/*/*.json`, ...["id", "access", "refresh"].map(k => `${base}/index/${k}/*.json`), `${base}/auth-transitions/*/*.json`];
  const readable = [seal, ...mutable];
  const encryption = { StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": i.kmsKeyArn } };
  const via = { StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId }, StringLike: { "kms:EncryptionContext:aws:s3:arn": `${base}/*` } };
  const policy = <T>(Statement: T[]) => ({ Version: "2012-10-17" as const, Statement });
  const runtimePolicy = policy<Record<string, unknown>>([
    { Sid: "ReadCurrentAuthObjects", Effect: "Allow", Action: ["s3:GetObject"], Resource: readable },
    { Sid: "WriteRuntimeAuthObjects", Effect: "Allow", Action: ["s3:PutObject"], Resource: mutable, Condition: encryption },
    { Sid: "ListDedicatedAuthBucket", Effect: "Allow", Action: ["s3:ListBucket"], Resource: [i.bucketArn] },
    { Sid: "UseExactAuthKeyViaS3", Effect: "Allow", Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: [i.kmsKeyArn], Condition: via },
    { Sid: "NeverMutateOwnerSeal", Effect: "Deny", Action: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: [seal] },
    { Sid: "NoAuthHistoryDeletionOrAdministration", Effect: "Deny", Action: denyActions, Resource: [i.bucketArn, `${i.bucketArn}/*`] },
    { Sid: "NoReadsOutsideAuthFamilies", Effect: "Deny", Action: ["s3:GetObject"], NotResource: readable },
    { Sid: "NoWritesOutsideRuntimeAuthFamilies", Effect: "Deny", Action: ["s3:PutObject"], NotResource: mutable },
    { Sid: "NoOtherBucketListing", Effect: "Deny", Action: ["s3:ListBucket"], NotResource: [i.bucketArn] },
    { Sid: "NoOtherCryptoKeys", Effect: "Deny", Action: ["kms:GenerateDataKey", "kms:Decrypt"], NotResource: [i.kmsKeyArn] },
  ]);
  const ownerBootstrapPolicy = policy<Record<string, unknown>>([
    { Sid: "OwnerReadsAuthInventory", Effect: "Allow", Action: ["s3:GetObject"], Resource: readable },
    { Sid: "OwnerImportsAndSeals", Effect: "Allow", Action: ["s3:PutObject"], Resource: readable, Condition: encryption },
    { Sid: "OwnerInspectsAuthInventory", Effect: "Allow", Action: ["s3:ListBucket"], Resource: [i.bucketArn] },
    { Sid: "OwnerUsesAuthKeyViaS3", Effect: "Allow", Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: [i.kmsKeyArn], Condition: via },
  ]);
  const bucketPolicy = policy<Record<string, unknown>>([
    { Sid: "EHasNoCAuthAccess", Effect: "Deny", Principal: { AWS: i.ePrincipalArn }, Action: ["s3:*"], Resource: [i.bucketArn, `${i.bucketArn}/*`] },
    { Sid: "RequireTls", Effect: "Deny", Principal: "*", Action: ["s3:GetObject", "s3:PutObject", "s3:ListBucket"], Resource: [i.bucketArn, `${i.bucketArn}/*`], Condition: { Bool: { "aws:SecureTransport": "false" } } },
    { Sid: "RequireExplicitKms", Effect: "Deny", Principal: "*", Action: ["s3:PutObject"], Resource: [`${i.bucketArn}/*`], Condition: { StringNotEquals: { "s3:x-amz-server-side-encryption": "aws:kms" } } },
    { Sid: "RequireExactKmsKey", Effect: "Deny", Principal: "*", Action: ["s3:PutObject"], Resource: [`${i.bucketArn}/*`], Condition: { StringNotEquals: { "s3:x-amz-server-side-encryption-aws-kms-key-id": i.kmsKeyArn } } },
    { Sid: "OnlyOwnerMayWriteSeal", Effect: "Deny", Principal: "*", Action: ["s3:PutObject"], Resource: [seal], Condition: { ArnNotEquals: { "aws:PrincipalArn": i.ownerBootstrapPrincipalArn } } },
    { Sid: "RuntimeCannotMutateSeal", Effect: "Deny", Principal: { AWS: i.runtimePrincipalArn }, Action: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"], Resource: [seal] },
  ]);
  const kmsKeyPolicy = policy<Record<string, unknown>>([
    { Sid: "NamedOwnerAdministersAuthKey", Effect: "Allow", Principal: { AWS: i.keyAdminPrincipalArn }, Action: keyAdminActions, Resource: "*" },
    ...[["RuntimeUsesAuthKeyViaS3", i.runtimePrincipalArn], ["OwnerImportsAndSealsViaS3", i.ownerBootstrapPrincipalArn]].map(([Sid, arn]) => ({ Sid, Effect: "Allow", Principal: { AWS: arn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*", Condition: via })),
  ]);
  const proposal = {
    protocol: "rhc-auth-store-offline-plan-v1", status: "unqualified-proposal", deployable: false,
    sourceContract: C_AUTH_SOURCE, input: i,
    resources: { bucket: { arn: i.bucketArn, dedicatedPurpose: "C-auth-only", blockPublicAccess: true, objectOwnership: "BucketOwnerEnforced", bucketKeyEnabled: false, objectLockEnabled: false, proposedVersioning: "Enabled", lifecycleRules: [] }, key: { arn: i.kmsKeyArn }, runtimePrincipal: { arn: i.runtimePrincipalArn, credentialInterface: "primitive-access-key-and-secret-key" } },
    keyFamilies: { seal, readable, mutable, serviceListPrefix: `${i.keyRoot}/sessions/service/<sha256(org)>/` },
    policies: { runtimePolicy, ownerBootstrapPolicy, bucketPolicy, kmsKeyPolicy },
    holds: ["independent-frozen-C-auth-and-error-mapping", "effective-S3-404-403-and-conditional-write-proof", "exact-provider-identity-and-root-ownership", "C-runtime-account-validator-compatibility", "RAM-only-deploy-and-binding-preservation", "genuine-baseline-old-writer-drain-consent-and-barrier", "versioning-retention-erasure-and-restore", "managed-public-auth-upload-with-SQL-outage"],
  };
  const sha256 = createHash("sha256").update(canonicalPolicyJson(proposal)).digest("hex");
  return freeze({ ...proposal, sha256 });
}
