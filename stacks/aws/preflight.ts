/**
 * Read-only AWS checks the stack makes before declaring anything, through an
 * injected `aws` CLI runner (the stack passes one that uses the same exported
 * credentials as the providers; tests pass a fake). Every check fails closed.
 *
 *   - the services image digest exists in its ECR repository and carries the
 *     tag `sha-<imageSourceRevision>` (publish-image-ecr tags by commit);
 *   - an `accountShared.ownerStage` really owns the account's ECR repository;
 *   - on the Pippen sandbox proof stage, the caller is exactly the pippen
 *     agent-session role session in the sandbox account;
 *   - NetworkPolicy enforcement (phase 2) only after Alchemy state shows every
 *     current policy applied, unchanged, by an earlier apply.
 */
import { createHash } from "node:crypto";

/** Read-only AWS access: `json` runs `aws <args> --output json`; `s3Json` reads one JSON object. Both throw on error. */
export interface AwsCli {
  json(args: string[]): unknown;
  s3Json(bucket: string, key: string, region: string): unknown;
}

const fail = (why: string): never => {
  throw new Error(`96bulls: ${why}. Nothing was changed.`);
};

export function parseEcrUri(uri: string) {
  const m = /^(\d{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com\/([a-z0-9._/-]+)$/.exec(uri);
  if (!m) return fail(`${uri} is not an ECR repository URI`);
  return { registryId: m[1]!, region: m[2]!, repositoryName: m[3]! };
}

export function verifyServicesImage(aws: AwsCli, i: { repositoryUri: string; imageDigest: string; imageSourceRevision: string }) {
  const r = parseEcrUri(i.repositoryUri);
  let out: { imageDetails?: { imageDigest?: string; imageTags?: string[] }[] };
  try {
    out = aws.json(["ecr", "describe-images", "--registry-id", r.registryId, "--region", r.region, "--repository-name", r.repositoryName, "--image-ids", `imageDigest=${i.imageDigest}`]) as typeof out;
  } catch (e) {
    return fail(`services image ${i.imageDigest} is not in ${i.repositoryUri} (${(e as Error).message.split("\n")[0]})`);
  }
  const d = out.imageDetails?.[0];
  if (d?.imageDigest !== i.imageDigest) fail(`ECR did not return ${i.imageDigest}`);
  const want = `sha-${i.imageSourceRevision}`;
  if (!(d!.imageTags ?? []).includes(want))
    fail(`services image ${i.imageDigest} is tagged ${JSON.stringify(d!.imageTags ?? [])}, not ${want}: the digest was not built from imageSourceRevision`);
}

export function verifyAccountOwner(aws: AwsCli, i: { accountId: string; region: string; ownerStage: string }) {
  let repo: { repositories?: { repositoryArn?: string }[] };
  try {
    repo = aws.json(["ecr", "describe-repositories", "--region", i.region, "--registry-id", i.accountId, "--repository-names", "96bulls-services"]) as typeof repo;
  } catch {
    return fail(`accountShared.ownerStage ${i.ownerStage}: no 96bulls-services repository in ${i.accountId}/${i.region}`);
  }
  const arn = repo.repositories?.[0]?.repositoryArn ?? fail("ECR returned no repository ARN");
  const tags = (aws.json(["ecr", "list-tags-for-resource", "--region", i.region, "--resource-arn", arn]) as { tags?: { Key: string; Value: string }[] }).tags ?? [];
  const owner = tags.find((t) => t.Key === "alchemy::stage")?.Value;
  if (owner !== i.ownerStage) fail(`96bulls-services is owned by stage ${JSON.stringify(owner)}, not accountShared.ownerStage ${i.ownerStage}`);
}

/** The caller must be exactly this assumed-role session (`aws sts get-caller-identity` Arn). */
export function verifyCallerArn(aws: AwsCli, expectedArn: string) {
  let who: { Arn?: string };
  try {
    who = aws.json(["sts", "get-caller-identity"]) as typeof who;
  } catch {
    return fail("sts get-caller-identity failed; the caller is unverified");
  }
  if (who.Arn !== expectedArn) fail(`caller ${JSON.stringify(who.Arn ?? null)} is not ${expectedArn}`);
}

/** Canonical JSON (sorted keys), so a hash depends only on content. */
export function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v && typeof v === "object")
    return `{${Object.keys(v as object).sort().map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`).join(",")}}`;
  return JSON.stringify(v);
}

export const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export interface PolicyLike {
  metadata: { name: string; namespace: string };
  spec: unknown;
}

export const policyLogicalId = (p: PolicyLike) => `netpol-${p.metadata.namespace}-${p.metadata.name}`;

/** The hash an operator records after checking the phase-1 policies (`networkPolicyEnforcement.policySetSha256`). */
export function policySetSha256(policies: PolicyLike[]) {
  return sha256(canonical(policies.map((p) => ({ id: policyLogicalId(p), spec: p.spec })).sort((a, b) => a.id.localeCompare(b.id))));
}

/**
 * Phase 2 gate. Refused unless (a) the operator's verification record names the
 * exact current policy set, and (b) Alchemy state for this stage holds every one
 * of those policies as applied, with the same spec: they went in, and were
 * checked, in an earlier apply.
 */
export function verifyNetworkPolicyPhase(
  aws: AwsCli,
  i: { stateBucket: string; statePrefix: string; region: string; policies: PolicyLike[]; record: { policySetSha256?: string; verifiedBy?: string; verifiedOn?: string } },
) {
  const want = policySetSha256(i.policies);
  if (i.record.policySetSha256 !== want)
    fail(`networkPolicyEnforcement.policySetSha256 must be ${want}, the policy set applied and checked in phase 1 (got ${JSON.stringify(i.record.policySetSha256)})`);
  if (!i.record.verifiedBy?.trim() || !/^\d{4}-\d{2}-\d{2}$/.test(i.record.verifiedOn ?? "")) fail("networkPolicyEnforcement needs verifiedBy and verifiedOn (YYYY-MM-DD)");
  for (const p of i.policies) {
    const key = `${i.statePrefix}/${policyLogicalId(p)}.json`;
    let state: { status?: string; props?: { manifest?: { spec?: unknown } } };
    try {
      state = aws.s3Json(i.stateBucket, key, i.region) as typeof state;
    } catch {
      return fail(`NetworkPolicy ${policyLogicalId(p)} is not in Alchemy state: apply phase 1 first`);
    }
    if (state.status !== "created" && state.status !== "updated") fail(`NetworkPolicy ${policyLogicalId(p)} is ${JSON.stringify(state.status)} in state, not applied`);
    if (canonical(state.props?.manifest?.spec) !== canonical(p.spec)) fail(`NetworkPolicy ${policyLogicalId(p)} in state differs from the current policy: apply and check it in phase 1 first`);
  }
}

/** The runner the stack uses: the `aws` CLI with the environment's (exported SSO) credentials. */
export function awsCliRunner(spawn: (cmd: string, args: string[]) => { status: number | null; stdout: string; stderr: string }): AwsCli {
  const run = (args: string[]) => {
    const r = spawn("aws", [...args, "--output", "json"]);
    if (r.status !== 0) throw new Error(r.stderr.trim() || `aws ${args.slice(0, 2).join(" ")} failed`);
    return r.stdout.trim() ? JSON.parse(r.stdout) : {};
  };
  return {
    json: run,
    s3Json: (bucket, key, region) => {
      const r = spawn("aws", ["s3", "cp", `s3://${bucket}/${key}`, "-", "--region", region]);
      if (r.status !== 0) throw new Error(r.stderr.trim() || `s3://${bucket}/${key} unreadable`);
      return JSON.parse(r.stdout);
    },
  };
}

/** Alchemy's default AWS state bucket and this stack's key prefix (StateStore/State.ts). */
export const alchemyStateBucket = (accountId: string, region: string) => `alchemy-state-${accountId}-${region}-an`;
export const alchemyStatePrefix = (stage: string) => `96bulls/96bulls-aws/${stage}`;
