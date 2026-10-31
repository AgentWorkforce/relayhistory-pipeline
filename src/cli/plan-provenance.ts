/**
 * Plan provenance for the AWS stage: `96bulls plan <stage> --capture` writes the
 * plan, the detailed plan, sanitized inputs and a provenance record to
 * docs/plans/; `96bulls up <stage>` refuses unless that record still matches the
 * exact source, inputs, Alchemy state and plan files it is about to apply.
 *
 * What a record binds: the source tree the stack is built from (and that it was
 * clean), the inputs file (sha256), the stage's Alchemy state objects (digest of
 * keys + ETags), the tool versions, the STS caller, account and region, and the
 * sha256 of each plan file.
 *
 * Read-only by construction: `AWS.state` writes a lease (and may reconcile the
 * bucket) even for `alchemy plan`, so capture never plans against it. It reads
 * the stage's state objects with GETs pinned to version ids into a local
 * snapshot and plans against that (stacks/aws/state-snapshot.ts). The state
 * bucket must be versioned, and its full version listing for the stage (every
 * version and delete marker) must be identical before and after: a transient
 * write by anyone during the window leaves a version and is caught. `up`
 * requires the listing to be unchanged since capture.
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
  state: {
    bucket: string;
    prefix: string;
    versioning: "Enabled";
    method: "snapshot";
    /** Current (latest, not deleted) objects planned against. */
    objects: number;
    /** sha256 of the snapshot file's objects (key, version id, text). */
    snapshotSha256: string;
    /** sha256 of every version and delete marker under the prefix; equal before and after the run. */
    versionsDigest: string;
    versionEntries: number;
  };
  tools: Record<string, string>;
  outputs: { plan: { path: string; sha256: string }; detailed: { path: string; sha256: string } };
  summary: string;
  note: string;
}

export interface VersionListing {
  Versions?: { Key: string; VersionId: string; IsLatest: boolean; ETag: string }[];
  DeleteMarkers?: { Key: string; VersionId: string; IsLatest: boolean }[];
}

/**
 * Digest of every version and delete marker under the prefix. A write that
 * was later undone (a lease PUT then DELETE) still leaves a version and a
 * marker, so it changes this even though the final keys and ETags do not.
 */
export function versionsDigest(l: VersionListing) {
  const lines = [
    ...(l.Versions ?? []).map((v) => `V ${v.Key} ${v.VersionId} ${v.ETag} ${v.IsLatest}`),
    ...(l.DeleteMarkers ?? []).map((d) => `D ${d.Key} ${d.VersionId} ${d.IsLatest}`),
  ].sort();
  return { entries: lines.length, digest: sha256(lines.join("\n")) };
}

/** The objects a plan reads: the latest version of each key that is not deleted. */
export function currentObjects(l: VersionListing) {
  const deleted = new Set((l.DeleteMarkers ?? []).filter((d) => d.IsLatest).map((d) => d.Key));
  return (l.Versions ?? []).filter((v) => v.IsLatest && !deleted.has(v.Key)).map((v) => ({ key: v.Key, versionId: v.VersionId })).sort((a, b) => a.key.localeCompare(b.key));
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
  state: { versioning: string; versionsDigest: string; versionEntries: number };
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
  cmp("state bucket versioning", p.state.versioning, o.state.versioning);
  cmp("state versions", p.state.versionEntries, o.state.versionEntries);
  cmp("state digest", p.state.versionsDigest, o.state.versionsDigest);
  cmp("plan file", p.outputs.plan.sha256, o.planSha256);
  cmp("detailed plan file", p.outputs.detailed.sha256, o.detailedSha256);
  return out;
}
