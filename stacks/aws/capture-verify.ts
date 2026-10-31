/**
 * Every deploy, whatever starts it (`bin/96bulls up` or a direct `alchemy
 * deploy`), applies exactly the reviewed capture or nothing. Called by the
 * stack before it declares a resource, and by the CLI before it runs Alchemy.
 *
 * Checks, against docs/plans/<stage>.provenance.json:
 *   - the stack source is clean and its tree digest is the capture's;
 *   - the checked-out commit is the capture's, or a descendant that changes only
 *     the capture's own files;
 *   - the stage's live Alchemy state (every version and delete marker, lease
 *     excluded) is exactly what the capture planned against;
 *   - the plan and detailed-plan files are the ones the capture recorded;
 *   - the inputs being applied are the captured inputs;
 *   - account and region.
 * The deploy approval separately names this exact provenance file
 * (deploy-guard.ts assertApprovalBinding).
 */
import { createHash } from "node:crypto";
import { join } from "node:path";
import { inputsDigest } from "./deploy-guard.ts";
import { alchemyStateBucket, alchemyStatePrefix } from "./preflight.ts";
import { STACK_SOURCE_PATHS, provenanceMismatches, versionsDigest, type Observed, type Provenance, type VersionListing } from "../../src/cli/plan-provenance.ts";

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

/** The side effects verification needs, injected so tests can stand in for git, aws and the files. */
export interface CaptureIO {
  root: string;
  /** Runs a command; returns stdout, or throws on a non-zero exit. */
  run(cmd: "git" | "aws", args: string[]): string;
  /** File text, or undefined if absent. */
  readFile(path: string): string | undefined;
}

export const capturePaths = (root: string, stage: string) => {
  const dir = join(root, "docs", "plans");
  return {
    dir,
    plan: join(dir, `${stage}.plan.txt`),
    detailed: join(dir, `${stage}.plan.detailed.txt`),
    inputs: join(dir, `${stage}.inputs.sanitized.json`),
    provenance: join(dir, `${stage}.provenance.json`),
  };
};

export function observeSource(io: CaptureIO, captureCommit?: string) {
  const paths = [...STACK_SOURCE_PATHS];
  const clean = io.run("git", ["status", "--porcelain", "--", ...paths]).trim() === "";
  const commit = io.run("git", ["rev-parse", "HEAD"]).trim();
  let commitChangedFiles: string[] | null = [];
  if (captureCommit && captureCommit !== commit) {
    let descends = true;
    try {
      io.run("git", ["merge-base", "--is-ancestor", captureCommit, commit]);
    } catch {
      descends = false;
    }
    commitChangedFiles = descends ? io.run("git", ["diff", "--name-only", captureCommit, commit]).split("\n").map((l) => l.trim()).filter(Boolean) : null;
  }
  return { commit, commitChangedFiles, stackSourceSha256: sha256(io.run("git", ["ls-tree", "-r", "HEAD", "--", ...paths])), clean };
}

const awsJson = (io: CaptureIO, args: string[]) => JSON.parse(io.run("aws", [...args, "--output", "json"]) || "{}");

export function listVersions(io: CaptureIO, bucket: string, prefix: string, region: string): VersionListing {
  const out: VersionListing = { Versions: [], DeleteMarkers: [] };
  let token: string | undefined;
  do {
    const page = awsJson(io, ["s3api", "list-object-versions", "--bucket", bucket, "--prefix", prefix, "--region", region, ...(token ? ["--starting-token", token] : [])]) as VersionListing & { NextToken?: string };
    out.Versions!.push(...(page.Versions ?? []));
    out.DeleteMarkers!.push(...(page.DeleteMarkers ?? []));
    token = page.NextToken;
  } while (token);
  return out;
}

/** Read-only view of the stage's state: bucket versioning and every version and delete marker. */
export function observeState(io: CaptureIO, accountId: string, region: string, stage: string) {
  const bucket = alchemyStateBucket(accountId, region);
  const prefix = `${alchemyStatePrefix(stage)}/`;
  const versioning = (awsJson(io, ["s3api", "get-bucket-versioning", "--bucket", bucket, "--region", region]) as { Status?: string }).Status ?? "Disabled";
  const listing = listVersions(io, bucket, prefix, region);
  const v = versionsDigest(listing);
  return { bucket, prefix, versioning, listing, versionsDigest: v.digest, versionEntries: v.entries };
}

/** Verify that this deploy applies the reviewed capture; throws with every mismatch. */
export function verifyDeployCapture(io: CaptureIO, stage: string, inputsText: string): Provenance {
  const p = capturePaths(io.root, stage);
  const provenanceText = io.readFile(p.provenance);
  const planText = io.readFile(p.plan);
  const detailedText = io.readFile(p.detailed);
  if (provenanceText === undefined || planText === undefined || detailedText === undefined)
    throw new Error(`96bulls: no reviewed capture for ${stage} (docs/plans/${stage}.{provenance.json,plan.txt,plan.detailed.txt}); run \`96bulls plan ${stage} --capture\`. Nothing was changed.`);
  const rec = JSON.parse(provenanceText) as Provenance;
  const inputs = JSON.parse(inputsText) as { accountId: string; region: string };
  const src = observeSource(io, rec.source?.commit);
  const caller = awsJson(io, ["sts", "get-caller-identity"]) as { Account: string };
  const st = observeState(io, inputs.accountId, inputs.region, stage);
  const observed: Observed = {
    stage,
    accountId: caller.Account,
    region: inputs.region,
    commit: src.commit,
    commitChangedFiles: src.commitChangedFiles,
    stackSourceSha256: src.stackSourceSha256,
    clean: src.clean,
    inputsSha256: inputsDigest(inputsText),
    state: { versioning: st.versioning, versionsDigest: st.versionsDigest, versionEntries: st.versionEntries },
    planSha256: sha256(planText),
    detailedSha256: sha256(detailedText),
  };
  const bad = provenanceMismatches(rec, observed);
  if (bad.length) throw new Error(`96bulls: this deploy does not match the reviewed capture; capture and review again:\n  - ${bad.join("\n  - ")}\nNothing was changed.`);
  return rec;
}
