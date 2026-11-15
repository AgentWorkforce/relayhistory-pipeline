/**
 * Read-only pre-apply proof for `transcriptStore.adoptIdentities` (stacks/aws/adopt-identities.ts; docs/aws-stage.md
 * "Adopt identities"). Adoption reconciles each live identity to the declaration: it detaches, deletes and rewrites
 * whatever differs. So before the plan is captured, this fetches every identity the stage inputs adopt, with IAM and
 * KMS read calls only (get/list/describe), renders the same identities the stack declares, and refuses on any
 * difference. Output names fields only, never document contents.
 *
 *   bun scripts/adopt-identities-preapply.ts [--inputs <path>] [--allow-equivalent-policy-version]
 *
 * Exit 0: clean (adopting changes nothing but tags the stack sets). Exit 1: refused (differences listed). Exit 2: error.
 * Credentials come from the operator's environment (a read-only session is enough); nothing is written anywhere.
 */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  adoptedIdentities, adoptedIdentityDifferences, assertAdoptIdentitiesInputs,
  type LiveIdentity, type RenderedIdentity,
} from "../stacks/aws/adopt-identities.ts";
import { TRANSCRIPT_STAGE, transcriptNames } from "../stacks/aws/transcript-store.ts";

export type Run = (args: string[]) => { status: number; stdout: string; stderr: string };

const awsRun: Run = (args) => {
  const r = spawnSync("aws", [...args, "--output", "json"], { encoding: "utf8", env: process.env, maxBuffer: 64 << 20 });
  return { status: r.status ?? 1, stdout: r.stdout ?? "", stderr: r.stderr ?? "" };
};

class Missing extends Error {}
/** One read call. NoSuchEntity is "absent", any other failure aborts the proof. */
function call(run: Run, args: string[]): any {
  if (!/^(get|list|describe)-/.test(args[1] ?? "")) throw new Error(`refusing a non-read call: ${args.slice(0, 2).join(" ")}`);
  const r = run(args);
  if (r.status !== 0) {
    if (/NoSuchEntity|NotFoundException/.test(r.stderr)) throw new Missing();
    throw new Error(`aws ${args.slice(0, 2).join(" ")} failed: ${(r.stderr || "").trim().split("\n").pop()}`);
  }
  return JSON.parse(r.stdout || "{}");
}
const doc = (d: unknown) => (typeof d === "string" ? JSON.parse(decodeURIComponent(d)) : d);
const tagRecord = (tags: { Key: string; Value: string }[] | undefined) => Object.fromEntries((tags ?? []).map((t) => [t.Key, t.Value]));
const absent = <T>(f: () => T): T | { exists: false } => { try { return f(); } catch (e) { if (e instanceof Missing) return { exists: false }; throw e; } };

/** The live side of one rendered identity, from read calls only. */
export function fetchLive(run: Run, r: RenderedIdentity): LiveIdentity {
  return absent((): LiveIdentity => {
    if (r.kind === "oidc-provider") {
      const p = call(run, ["iam", "get-open-id-connect-provider", "--open-id-connect-provider-arn", r.arn]);
      const t = call(run, ["iam", "list-open-id-connect-provider-tags", "--open-id-connect-provider-arn", r.arn]);
      return { exists: true, url: p.Url, clientIDList: p.ClientIDList ?? [], tags: tagRecord(t.Tags) };
    }
    if (r.kind === "managed-policy") {
      const p = call(run, ["iam", "get-policy", "--policy-arn", r.arn]).Policy;
      const v = call(run, ["iam", "get-policy-version", "--policy-arn", r.arn, "--version-id", p.DefaultVersionId]).PolicyVersion;
      const t = call(run, ["iam", "list-policy-tags", "--policy-arn", r.arn]);
      return { exists: true, path: p.Path, document: doc(v.Document), tags: tagRecord(t.Tags) };
    }
    const [noun, nameFlag, name] = r.kind === "role" ? ["role", "--role-name", r.roleName] : ["user", "--user-name", r.userName];
    const got = call(run, ["iam", `get-${noun}`, nameFlag, name])[r.kind === "role" ? "Role" : "User"];
    const names: string[] = call(run, ["iam", `list-${noun}-policies`, nameFlag, name]).PolicyNames ?? [];
    const inlinePolicies = Object.fromEntries(names.map((n) => [n, doc(call(run, ["iam", `get-${noun}-policy`, nameFlag, name, "--policy-name", n]).PolicyDocument)]));
    const attached: { PolicyArn: string }[] = call(run, ["iam", `list-attached-${noun}-policies`, nameFlag, name]).AttachedPolicies ?? [];
    const t = call(run, ["iam", `list-${noun}-tags`, nameFlag, name]);
    return {
      exists: true, path: got.Path, inlinePolicies, managedPolicyArns: attached.map((a) => a.PolicyArn),
      permissionsBoundary: got.PermissionsBoundary?.PermissionsBoundaryArn, tags: tagRecord(t.Tags),
      ...(r.kind === "role" ? { trust: doc(got.AssumeRolePolicyDocument), maxSessionDuration: got.MaxSessionDuration, description: got.Description } : {}),
    };
  }) as LiveIdentity;
}

export interface PreApplyReport {
  adoptIdentitiesPreApply: "clean" | "refused";
  resources: { resource: string; differences: string[]; notes: string[] }[];
}

/** Renders what the stage inputs adopt, fetches each live identity, and judges every one. */
export function preApplyReport(inputs: any, run: Run, o: { allowEquivalentPolicyVersion?: boolean } = {}): PreApplyReport {
  const refuse = (why: string): never => { throw new Error(why); };
  const { accountId, region } = inputs ?? {};
  if (typeof accountId !== "string" || typeof region !== "string") refuse("the inputs need accountId and region");
  const n = transcriptNames(TRANSCRIPT_STAGE, accountId);
  const adopt = assertAdoptIdentitiesInputs(inputs.transcriptStore?.adoptIdentities, { accountId, region, bucketName: n.bucket }, refuse);
  if (!adopt) return refuse("transcriptStore.adoptIdentities is not set: nothing to adopt");
  const caller = call(run, ["sts", "get-caller-identity"]);
  if (caller.Account !== accountId) refuse("the credentials are for another account than the inputs' accountId");
  const keyArn: string = call(run, ["kms", "describe-key", "--key-id", n.keyAlias, "--region", region]).KeyMetadata?.Arn;
  if (typeof keyArn !== "string") refuse("the transcripts key alias does not resolve");
  const rendered = adoptedIdentities(adopt, { stage: TRANSCRIPT_STAGE, accountId, region, bucketName: n.bucket, keyAlias: n.keyAlias, keyArn });
  const resources = rendered.map((r) => ({ resource: r.resource, ...adoptedIdentityDifferences(r, fetchLive(run, r), o) }));
  return { adoptIdentitiesPreApply: resources.some((r) => r.differences.length > 0) ? "refused" : "clean", resources };
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2);
    const at = argv.indexOf("--inputs");
    const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
    const path = at >= 0 ? argv[at + 1]! : join(root, ".96bulls", TRANSCRIPT_STAGE, "aws-inputs.json");
    const report = preApplyReport(JSON.parse(readFileSync(path, "utf8")), awsRun, { allowEquivalentPolicyVersion: argv.includes("--allow-equivalent-policy-version") });
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.adoptIdentitiesPreApply === "clean" ? 0 : 1;
  } catch (e) {
    console.error(`adopt-identities pre-apply refused: ${(e as Error).message}`);
    process.exitCode = 2;
  }
}
