/**
 * The live side of `transcriptStore.adoptIdentities` (adopt-identities.ts): fetch each identity the stage inputs adopt
 * with IAM, STS and KMS read calls only (get/list/describe), render the same identities the stack declares, and judge
 * every one. The report's sha256 is stable (canonical JSON over the rendered identities and the result), so the
 * capture records it in the provenance and the deploy refuses unless a fresh run is clean with the same hash.
 * Used by `96bulls plan aws-sessions-prod --capture`, the deploy (alchemy.aws.run.ts) and
 * scripts/adopt-identities-preapply.ts. Reports name fields only, never document contents.
 */
import { spawnSync } from "node:child_process";
import { sha256 } from "../../src/cli/plan-provenance.ts";
import { adoptedIdentities, adoptedIdentityDifferences, assertAdoptIdentitiesInputs, type LiveIdentity, type RenderedIdentity } from "./adopt-identities.ts";
import { TRANSCRIPT_STAGE, transcriptNames } from "./transcript-store.ts";

export type Run = (args: string[]) => { status: number; stdout: string; stderr: string };

/** The aws CLI with the given environment (the operator's or the deploy's credentials). */
export const awsCliRun = (env: Record<string, string | undefined> = process.env): Run => (args) => {
  const r = spawnSync("aws", [...args, "--output", "json"], { encoding: "utf8", env, maxBuffer: 64 << 20 });
  return { status: r.status ?? 1, stdout: r.stdout ?? "", stderr: r.stderr ?? "" };
};

class Missing extends Error {}
/** One read call. NoSuchEntity is "absent"; any other failure aborts. Anything but get/list/describe is refused. */
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

/** The live side of one rendered identity, from read calls only. */
export function fetchLive(run: Run, r: RenderedIdentity): LiveIdentity {
  try {
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
    const [noun, flag, name] = r.kind === "role" ? ["role", "--role-name", r.roleName] : ["user", "--user-name", r.userName];
    const got = call(run, ["iam", `get-${noun}`, flag, name])[r.kind === "role" ? "Role" : "User"];
    const names: string[] = call(run, ["iam", `list-${noun}-policies`, flag, name]).PolicyNames ?? [];
    const inlinePolicies = Object.fromEntries(names.map((n) => [n, doc(call(run, ["iam", `get-${noun}-policy`, flag, name, "--policy-name", n]).PolicyDocument)]));
    const attached: { PolicyArn: string }[] = call(run, ["iam", `list-attached-${noun}-policies`, flag, name]).AttachedPolicies ?? [];
    const t = call(run, ["iam", `list-${noun}-tags`, flag, name]);
    return {
      exists: true, path: got.Path, inlinePolicies, managedPolicyArns: attached.map((a) => a.PolicyArn),
      permissionsBoundary: got.PermissionsBoundary?.PermissionsBoundaryArn, tags: tagRecord(t.Tags),
      ...(r.kind === "role" ? { trust: doc(got.AssumeRolePolicyDocument), maxSessionDuration: got.MaxSessionDuration, description: got.Description } : {}),
    };
  } catch (e) {
    if (e instanceof Missing) return { exists: false };
    throw e;
  }
}

/** JSON with every object's keys sorted: the same report always hashes the same. */
export const canonicalJson = (v: unknown): string => JSON.stringify(v, (_k, x) =>
  x !== null && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, x[k]])) : x);

export interface PreApplyReport {
  adoptIdentitiesPreApply: "clean" | "refused";
  /** sha256 of canonical JSON over { schema, stage, account, region, key, rendered identities, results }. */
  sha256: string;
  resources: { resource: string; differences: string[] }[];
}

/** Renders what the stage inputs adopt, fetches each live identity, and judges every one. Throws on unusable inputs. */
export function preApplyReport(inputs: any, run: Run): PreApplyReport {
  const refuse = (why: string): never => { throw new Error(`adopt identities pre-apply: ${why}`); };
  const { accountId, region } = inputs ?? {};
  if (typeof accountId !== "string" || typeof region !== "string") refuse("the inputs need accountId and region");
  const n = transcriptNames(TRANSCRIPT_STAGE, accountId);
  if (inputs.transcriptStore?.adoptIdentities === undefined) refuse("transcriptStore.adoptIdentities is not set: nothing to adopt");
  const caller = call(run, ["sts", "get-caller-identity"]);
  if (caller.Account !== accountId) refuse("the credentials are for another account than the inputs' accountId");
  const keyArn: unknown = call(run, ["kms", "describe-key", "--key-id", n.keyAlias, "--region", region]).KeyMetadata?.Arn;
  if (typeof keyArn !== "string") return refuse("the transcripts key alias does not resolve");
  // With the key's live ARN known, the pasted role policies must name exactly that key.
  const adopt = assertAdoptIdentitiesInputs(inputs.transcriptStore.adoptIdentities, { accountId, region, bucketName: n.bucket, keyArn }, refuse)!;
  const rendered = adoptedIdentities(adopt, { stage: TRANSCRIPT_STAGE, accountId, region, bucketName: n.bucket, keyAlias: n.keyAlias, keyArn });
  const resources = rendered.map((r) => ({ resource: r.resource, differences: adoptedIdentityDifferences(r, fetchLive(run, r)) }));
  const status = resources.some((r) => r.differences.length > 0) ? "refused" : "clean";
  const hash = sha256(canonicalJson({ schema: 1, stage: TRANSCRIPT_STAGE, accountId, region, keyArn, identities: rendered, status, resources }));
  return { adoptIdentitiesPreApply: status, sha256: hash, resources };
}

/** A clean report or an error that names every difference (fields only). */
export function assertCleanPreApply(report: PreApplyReport, when: string): string {
  if (report.adoptIdentitiesPreApply === "clean") return report.sha256;
  const what = report.resources.filter((r) => r.differences.length).map((r) => `${r.resource}: ${r.differences.join("; ")}`).join(" | ");
  throw new Error(`96bulls: adopt identities: ${when}: the live identities differ from the declaration (the apply would rewrite them): ${what}. Nothing was changed.`);
}
