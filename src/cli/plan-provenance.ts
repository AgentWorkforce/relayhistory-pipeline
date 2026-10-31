/**
 * Plan provenance for the AWS stage: `96bulls plan <stage> --capture` writes the
 * plan, the detailed plan, sanitized inputs and a provenance record to
 * docs/plans/; `96bulls up <stage>` refuses unless that record still matches the
 * exact source, inputs, Alchemy state and plan files it is about to apply.
 *
 * What a record binds: the source tree the stack is built from (and that it was
 * clean), the inputs file (sha256), the stage's Alchemy state objects (digest of
 * keys + ETags), the tool versions, the STS caller, account and region, and the
 * sha256 of each plan file. The state bucket must already exist: Alchemy creates
 * a missing one on first use, so capture refuses rather than letting a "plan"
 * create it.
 */
import { createHash } from "node:crypto";

export const sha256 = (s: string | Uint8Array) => createHash("sha256").update(s).digest("hex");

/** Everything the AWS stack imports or reads at plan time. */
export const STACK_SOURCE_PATHS = ["stacks", "src", "packages", "supabase", "package.json", "bun.lock", "versions.lock.json", "tsconfig.json"] as const;

export interface Provenance {
  schema: 1;
  stage: string;
  generatedAt: string;
  target: { accountId: string; region: string; callerArn: string };
  source: { commit: string; stackSourceSha256: string; clean: true };
  inputs: { sha256: string; sanitizedPath: string; sanitizedSha256: string };
  state: { bucket: string; prefix: string; bucketExisted: true; objects: number; digest: string };
  tools: Record<string, string>;
  outputs: { plan: { path: string; sha256: string }; detailed: { path: string; sha256: string } };
  summary: string;
  note: string;
}

/** Digest of the stage's state objects: sorted `key etag` lines. Any change to state changes it. */
export function stateDigest(objects: { Key: string; ETag: string }[]) {
  const lines = objects.map((o) => `${o.Key} ${o.ETag}`).sort();
  return { objects: lines.length, digest: sha256(lines.join("\n")) };
}

/** Inputs with the operator's address removed, safe to commit. */
export function sanitizeInputs(inputs: unknown): unknown {
  return JSON.parse(JSON.stringify(inputs), (k, v) => (k === "publicAccessCidrs" && Array.isArray(v) ? v.map(() => "operator-address-redacted/32") : v));
}

/** Plan output with timestamps, the cluster CA and the operator address removed. */
export function sanitizePlan(text: string, operatorCidrs: string[]) {
  let out = text
    .split("\n")
    .map((l) => l.replace(/^\[[0-9:.]+\] INFO \(#[0-9]+\): ?/, "").replace(/(certificateAuthorityData: ).*/, "$1<public cluster CA omitted>"))
    .filter((l) => !l.startsWith("S3 Bucket diff:"))
    .join("\n");
  for (const c of operatorCidrs) out = out.split(c).join("operator-address-redacted/32");
  return out;
}

export interface Observed {
  stage: string;
  accountId: string;
  region: string;
  commit: string;
  stackSourceSha256: string;
  clean: boolean;
  inputsSha256: string;
  state: { objects: number; digest: string };
  planSha256: string;
  detailedSha256: string;
}

/** Every reason the recorded plan no longer describes what `up` would apply. Empty = verified. */
export function provenanceMismatches(p: Provenance, o: Observed): string[] {
  const out: string[] = [];
  const cmp = (what: string, recorded: unknown, now: unknown) => {
    if (recorded !== now) out.push(`${what}: plan has ${JSON.stringify(recorded)}, now ${JSON.stringify(now)}`);
  };
  cmp("stage", p.stage, o.stage);
  cmp("account", p.target.accountId, o.accountId);
  cmp("region", p.target.region, o.region);
  if (!o.clean) out.push("stack source has uncommitted changes");
  cmp("stack source", p.source.stackSourceSha256, o.stackSourceSha256);
  cmp("inputs", p.inputs.sha256, o.inputsSha256);
  cmp("state objects", p.state.objects, o.state.objects);
  cmp("state digest", p.state.digest, o.state.digest);
  cmp("plan file", p.outputs.plan.sha256, o.planSha256);
  cmp("detailed plan file", p.outputs.detailed.sha256, o.detailedSha256);
  return out;
}
