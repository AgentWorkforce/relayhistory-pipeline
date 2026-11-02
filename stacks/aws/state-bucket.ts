/**
 * Where the stage's Alchemy state lives, and, in a Pippen sandbox, how its
 * bucket is made.
 *
 * Every stage but a Pippen sandbox keeps Alchemy's default account store,
 * `alchemy-state-<account>-<region>-an` (@alchemy.run/aws StateStore/State.ts
 * createStateBucketName), unchanged: `AWS.state({ prefix: "96bulls" })`.
 *
 * A Pippen sandbox (`inputs.pippenSandbox`) cannot use it: pippen-sweeper may
 * delete only `pippen-sbx-*` buckets and is explicitly denied `alchemy-state-*`
 * (pippen-tools iam/drafts/pippen-sweeper-role.json
 * NeverDeleteBucketsThatMustSurvive), so a default store created in a fresh
 * sandbox would outlive `pippen sandbox down` while the sweep reports clean.
 * There the store is `pippen-sbx-alchemy-state-<account>-<region>-an`: the
 * pippen-sbx- prefix the sweeper deletes (that deny is prefix-anchored, so it
 * does not match), still in Alchemy's account-regional naming
 * (`<prefix>-<account>-<region>-an`, required by the account-regional
 * namespace AWS.state creates buckets in). It is tagged pippen:sandbox=true at
 * CreateBucket (no untagged window) and given the default store's settings:
 * versioning, SSE-S3 (AES256, as Alchemy's default), all public access
 * blocked, BucketOwnerEnforced. It is never protected from the sweeper, which
 * empties every version and delete marker before DeleteBucket.
 *
 * `ensurePippenStateBucket` creates it when absent and converges those
 * settings otherwise. `96bulls plan --capture` runs it before snapshotting
 * (a fresh account has no prior state: the bucket is made, and its empty
 * listing is the captured state), and the stack's state layer runs it before
 * Alchemy's first state operation, so a direct `alchemy` run never lets
 * AWS.state create the bucket untagged.
 */
import { PIPPEN_BUCKET_PREFIX, PIPPEN_SANDBOX_TAG } from "./pippen-sandbox.ts";

export const ALCHEMY_STATE_KEY_PREFIX = "96bulls";
const REPO = "AgentWorkforce/sessions-pipeline";

/** Alchemy's default AWS state bucket (StateStore/State.ts createStateBucketName). */
export const alchemyStateBucket = (accountId: string, region: string) => `alchemy-state-${accountId}-${region}-an`;
/** This stack's key prefix in the store. */
export const alchemyStatePrefix = (stage: string) => `${ALCHEMY_STATE_KEY_PREFIX}/96bulls-aws/${stage}`;

const S3_NAME = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/;

/** `pippen-sbx-alchemy-state-<account>-<region>-an`: swept by pippen, account-regional like Alchemy's own. */
export function pippenStateBucket(accountId: string, region: string): string {
  if (!/^\d{12}$/.test(accountId)) throw new Error(`96bulls: account id ${JSON.stringify(accountId)} must be 12 digits. Nothing was created.`);
  if (!/^[a-z]{2}(-[a-z]+)+-\d$/.test(region)) throw new Error(`96bulls: ${JSON.stringify(region)} is not an AWS region. Nothing was created.`);
  const name = `${PIPPEN_BUCKET_PREFIX}alchemy-state-${accountId}-${region}-an`;
  if (!S3_NAME.test(name) || name.includes("--")) throw new Error(`96bulls: state bucket name ${name} is not a valid S3 bucket name (3-63 characters). Nothing was created.`);
  return name;
}

export interface StateTarget {
  accountId: string;
  region: string;
  pippenSandbox?: unknown;
}

/** The stage's state bucket: the pippen-sbx- store in a Pippen sandbox, Alchemy's default otherwise. */
export const stateBucketFor = (i: StateTarget) => (i.pippenSandbox ? pippenStateBucket(i.accountId, i.region) : alchemyStateBucket(i.accountId, i.region));

/**
 * `AWS.state` options. Without pippenSandbox exactly `{ prefix: "96bulls" }`, as
 * before (Alchemy derives the default bucket itself).
 */
export function stackStateOptions(i: StateTarget | undefined): { prefix: string; bucketName?: string } {
  if (!i?.pippenSandbox) return { prefix: ALCHEMY_STATE_KEY_PREFIX };
  return { prefix: ALCHEMY_STATE_KEY_PREFIX, bucketName: pippenStateBucket(i.accountId, i.region) };
}

/** pippen:sandbox=true first: the sweeper lists by it (tag:GetResources). */
export const pippenStateBucketTags = (stage: string) => [
  ...Object.entries(PIPPEN_SANDBOX_TAG).map(([Key, Value]) => ({ Key, Value })),
  { Key: "96bulls:stack", Value: stage },
  { Key: "96bulls:component", Value: "alchemy-state" },
  { Key: "96bulls:repo", Value: REPO },
];

/** The `aws` CLI: returns stdout, throws an Error carrying stderr on a non-zero exit. */
export type AwsRun = (args: string[]) => string;

const MISSING_BUCKET = /NoSuchBucket|\(404\)|Not Found/;
const isMissing = (e: unknown) => MISSING_BUCKET.test(e instanceof Error ? e.message : String(e));
const json = (s: string) => (s.trim() ? JSON.parse(s) : {});

/** Whether the bucket exists. Only a 404 is "absent"; a 403 or a throttle is an error (fail closed). */
export function bucketExists(aws: AwsRun, bucket: string, region: string): boolean {
  try {
    aws(["s3api", "head-bucket", "--bucket", bucket, "--region", region]);
    return true;
  } catch (e) {
    if (isMissing(e)) return false;
    throw e;
  }
}

/**
 * Create the Pippen sandbox state bucket if absent (tagged at create), then
 * converge versioning, encryption, public access block, ownership and the
 * pippen tag: each read first, written only on drift. Touches no object, so
 * the state's version listing (the capture's digest) is unaffected.
 */
export function ensurePippenStateBucket(aws: AwsRun, i: { accountId: string; region: string; stage: string }): { bucket: string; created: boolean } {
  const bucket = pippenStateBucket(i.accountId, i.region);
  const r = ["--bucket", bucket, "--region", i.region];
  const tags = pippenStateBucketTags(i.stage);
  // The credentials must be the sandbox's before anything is created there.
  const account = (json(aws(["sts", "get-caller-identity", "--output", "json"])) as { Account?: string }).Account;
  if (account !== i.accountId) throw new Error(`96bulls: STS caller is in ${account}, the state bucket belongs to ${i.accountId}. Nothing was created.`);
  let created = false;
  if (!bucketExists(aws, bucket, i.region)) {
    const config = { ...(i.region === "us-east-1" ? {} : { LocationConstraint: i.region }), Tags: tags };
    try {
      aws(["s3api", "create-bucket", ...r, "--bucket-namespace", "account-regional", "--create-bucket-configuration", JSON.stringify(config)]);
      created = true;
    } catch (e) {
      if (!/BucketAlreadyOwnedByYou/.test(e instanceof Error ? e.message : String(e))) throw e;
    }
    aws(["s3api", "wait", "bucket-exists", ...r]);
  }
  if ((json(aws(["s3api", "get-bucket-versioning", ...r, "--output", "json"])) as { Status?: string }).Status !== "Enabled")
    aws(["s3api", "put-bucket-versioning", ...r, "--versioning-configuration", "Status=Enabled"]);
  let rule: { ApplyServerSideEncryptionByDefault?: { SSEAlgorithm?: string }; BucketKeyEnabled?: boolean } | undefined;
  try {
    rule = (json(aws(["s3api", "get-bucket-encryption", ...r, "--output", "json"])) as { ServerSideEncryptionConfiguration?: { Rules?: (typeof rule)[] } }).ServerSideEncryptionConfiguration?.Rules?.[0];
  } catch (e) {
    if (!/ServerSideEncryptionConfigurationNotFound/.test(e instanceof Error ? e.message : String(e))) throw e;
  }
  if (rule?.ApplyServerSideEncryptionByDefault?.SSEAlgorithm !== "AES256" || rule.BucketKeyEnabled === true)
    aws(["s3api", "put-bucket-encryption", ...r, "--server-side-encryption-configuration", JSON.stringify({ Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: "AES256" }, BucketKeyEnabled: false }] })]);
  const blockAll = { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true };
  let pab: Record<string, boolean> | undefined;
  try {
    pab = (json(aws(["s3api", "get-public-access-block", ...r, "--output", "json"])) as { PublicAccessBlockConfiguration?: Record<string, boolean> }).PublicAccessBlockConfiguration;
  } catch (e) {
    if (!/NoSuchPublicAccessBlockConfiguration/.test(e instanceof Error ? e.message : String(e))) throw e;
  }
  if (!Object.entries(blockAll).every(([k, v]) => pab?.[k] === v)) aws(["s3api", "put-public-access-block", ...r, "--public-access-block-configuration", JSON.stringify(blockAll)]);
  let ownership: string | undefined;
  try {
    ownership = (json(aws(["s3api", "get-bucket-ownership-controls", ...r, "--output", "json"])) as { OwnershipControls?: { Rules?: { ObjectOwnership?: string }[] } }).OwnershipControls?.Rules?.[0]?.ObjectOwnership;
  } catch (e) {
    if (!/OwnershipControlsNotFoundError/.test(e instanceof Error ? e.message : String(e))) throw e;
  }
  if (ownership !== "BucketOwnerEnforced") aws(["s3api", "put-bucket-ownership-controls", ...r, "--ownership-controls", JSON.stringify({ Rules: [{ ObjectOwnership: "BucketOwnerEnforced" }] })]);
  // An existing bucket (e.g. made by an older direct `alchemy` run) gets the tags merged in.
  let tagSet: { Key: string; Value: string }[] = [];
  try {
    tagSet = (json(aws(["s3api", "get-bucket-tagging", ...r, "--output", "json"])) as { TagSet?: typeof tagSet }).TagSet ?? [];
  } catch (e) {
    if (!/NoSuchTagSet/.test(e instanceof Error ? e.message : String(e))) throw e;
  }
  if (!tags.every((t) => tagSet.some((x) => x.Key === t.Key && x.Value === t.Value))) {
    const merged = [...tagSet.filter((x) => !tags.some((t) => t.Key === x.Key)), ...tags];
    aws(["s3api", "put-bucket-tagging", ...r, "--tagging", JSON.stringify({ TagSet: merged })]);
  }
  return { bucket, created };
}

/**
 * The state target, read before the stack runs: Alchemy builds the state layer
 * from the Stack options, before the stage's Effect (and readInputs) runs, so
 * the stage comes from Alchemy's own argv (`--stage <s>` or `--stage=<s>`) and
 * accountId/region/pippenSandbox from that stage's aws-inputs.json. Anything
 * missing or unreadable gives undefined: the default store, and readInputs
 * then refuses the stage as before.
 */
/** The aws-* stage Alchemy was started with (`--stage X` or `--stage=X`), or undefined. */
export function stageFromArgv(argv: readonly string[]): string | undefined {
  const at = argv.indexOf("--stage");
  const stage = at >= 0 ? argv[at + 1] : argv.find((a) => a.startsWith("--stage="))?.slice("--stage=".length);
  return stage && /^aws-[a-z0-9-]+$/.test(stage) ? stage : undefined;
}

export function stateTargetFromArgv(argv: readonly string[], readInputsText: (stage: string) => string | undefined): (StateTarget & { stage: string }) | undefined {
  const stage = stageFromArgv(argv);
  if (!stage) return undefined;
  let inputs: Partial<StateTarget>;
  try {
    inputs = JSON.parse(readInputsText(stage) ?? "null") ?? {};
  } catch {
    return undefined;
  }
  if (typeof inputs.accountId !== "string" || typeof inputs.region !== "string") return undefined;
  return { stage, accountId: inputs.accountId, region: inputs.region, ...(inputs.pippenSandbox ? { pippenSandbox: inputs.pippenSandbox } : {}) };
}
