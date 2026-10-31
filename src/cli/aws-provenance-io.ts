/**
 * The impure half of plan provenance (src/cli/plan-provenance.ts): git, the aws
 * CLI and the plan runs. Read-only against AWS.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { alchemyStateBucket, alchemyStatePrefix } from "../../stacks/aws/preflight.ts";
import { STACK_SOURCE_PATHS, currentObjects, provenanceMismatches, sanitizeInputs, sanitizePlan, sha256, versionsDigest, type Observed, type Provenance, type VersionListing } from "./plan-provenance.ts";
import type { StateSnapshot } from "../../stacks/aws/state-snapshot.ts";
import { inputsDigest } from "../../stacks/aws/deploy-guard.ts";
import { tmpdir } from "node:os";
import { mkdtempSync, rmSync } from "node:fs";
import { awsInvocation } from "./aws.ts";

const run = (cmd: string, args: string[], cwd: string, env: Record<string, string> = {}) => {
  const r = spawnSync(cmd, args, { cwd, encoding: "utf8", env: { ...process.env, ...env }, maxBuffer: 256 * 1024 * 1024 });
  if (r.status !== 0) throw new Error(`${cmd} ${args.slice(0, 3).join(" ")} failed: ${(r.stderr || r.stdout).trim().split("\n").slice(-3).join(" | ")}`);
  return r.stdout;
};
const awsJson = (root: string, args: string[]) => JSON.parse(run("aws", [...args, "--output", "json"], root) || "{}");

export const planPaths = (root: string, stage: string) => {
  const dir = join(root, "docs", "plans");
  return {
    dir,
    plan: join(dir, `${stage}.plan.txt`),
    detailed: join(dir, `${stage}.plan.detailed.txt`),
    inputs: join(dir, `${stage}.inputs.sanitized.json`),
    provenance: join(dir, `${stage}.provenance.json`),
  };
};

function observeSource(root: string) {
  const paths = [...STACK_SOURCE_PATHS];
  const clean = run("git", ["status", "--porcelain", "--", ...paths], root).trim() === "";
  return { commit: run("git", ["rev-parse", "HEAD"], root).trim(), stackSourceSha256: sha256(run("git", ["ls-tree", "-r", "HEAD", "--", ...paths], root)), clean };
}

function listVersions(root: string, bucket: string, prefix: string, region: string): VersionListing {
  const out: VersionListing = { Versions: [], DeleteMarkers: [] };
  let token: string | undefined;
  do {
    const page = awsJson(root, ["s3api", "list-object-versions", "--bucket", bucket, "--prefix", prefix, "--region", region, ...(token ? ["--starting-token", token] : [])]) as VersionListing & { NextToken?: string };
    out.Versions!.push(...(page.Versions ?? []));
    out.DeleteMarkers!.push(...(page.DeleteMarkers ?? []));
    token = page.NextToken;
  } while (token);
  return out;
}

/** Read-only view of the stage's state: bucket versioning and every version and delete marker. */
function observeState(root: string, accountId: string, region: string, stage: string) {
  const bucket = alchemyStateBucket(accountId, region);
  const prefix = `${alchemyStatePrefix(stage)}/`;
  const versioning = (awsJson(root, ["s3api", "get-bucket-versioning", "--bucket", bucket, "--region", region]) as { Status?: string }).Status ?? "Disabled";
  const listing = listVersions(root, bucket, prefix, region);
  const v = versionsDigest(listing);
  return { bucket, prefix, versioning, listing, versionsDigest: v.digest, versionEntries: v.entries };
}

/** GET each current state object at its exact version id. */
function snapshotState(root: string, stage: string, region: string, st: ReturnType<typeof observeState>): StateSnapshot {
  const dir = mkdtempSync(join(tmpdir(), "96bulls-state-"));
  try {
    const objects = currentObjects(st.listing).map((o, n) => {
      const file = join(dir, String(n));
      run("aws", ["s3api", "get-object", "--bucket", st.bucket, "--key", o.key, "--version-id", o.versionId, "--region", region, file], root);
      return { key: o.key, versionId: o.versionId, text: readFileSync(file, "utf8") };
    });
    return { schema: 1, stack: "96bulls-aws", stage, bucket: st.bucket, prefix: st.prefix, objects };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

export function capturePlan(root: string, stage: string, env: Record<string, string | undefined>) {
  const inputsPath = join(root, ".96bulls", stage, "aws-inputs.json");
  const rawInputs = readFileSync(inputsPath, "utf8");
  const inputs = JSON.parse(rawInputs) as { accountId: string; region: string; eks?: { apiAccess?: { publicAccessCidrs?: string[] } } };
  const src = observeSource(root);
  if (!src.clean) throw new Error("commit the stack source first: a plan must describe committed code");
  const caller = awsJson(root, ["sts", "get-caller-identity"]) as { Account: string; Arn: string };
  if (caller.Account !== inputs.accountId) throw new Error(`STS caller is ${caller.Account}, inputs say ${inputs.accountId}`);
  const state = observeState(root, inputs.accountId, inputs.region, stage);
  // Transient writes are detectable only in a versioned bucket.
  if (state.versioning !== "Enabled") throw new Error(`state bucket ${state.bucket} is not versioned; a plan's writes could not be detected`);
  const snap = snapshotState(root, stage, inputs.region, state);
  const snapPath = join(root, ".96bulls", stage, "state-snapshot.json");
  const snapText = JSON.stringify(snap);
  writeFileSync(snapPath, snapText, { mode: 0o600 });
  const cidrs = inputs.eks?.apiAccess?.publicAccessCidrs ?? [];
  const exec = (detailed: boolean) => {
    const inv = awsInvocation("plan", stage, env, { detailed });
    // In-memory state from the snapshot: the plan writes nothing to the state bucket.
    const r = spawnSync("bunx", inv.args, { cwd: root, encoding: "utf8", env: { ...process.env, ...inv.env, NINETY_SIX_BULLS_STATE_SNAPSHOT: snapPath }, maxBuffer: 256 * 1024 * 1024 });
    if (r.status !== 0) throw new Error(`plan failed:\n${r.stdout}\n${r.stderr}`);
    return sanitizePlan(`${r.stdout}${r.stderr}`, cidrs);
  };
  const plan = exec(false);
  const detailed = exec(true);
  // Every version and delete marker must be unchanged: no write at all in the window, by anyone.
  const after = observeState(root, inputs.accountId, inputs.region, stage);
  if (after.versionsDigest !== state.versionsDigest) throw new Error("Alchemy state bucket changed during the capture (a version or delete marker appeared); capture again");
  const p = planPaths(root, stage);
  mkdirSync(p.dir, { recursive: true });
  const sanitized = `${JSON.stringify(sanitizeInputs(inputs), null, 2)}\n`;
  writeFileSync(p.inputs, sanitized);
  writeFileSync(p.plan, plan);
  writeFileSync(p.detailed, detailed);
  const version = (pkg: string) => (JSON.parse(readFileSync(join(root, "node_modules", pkg, "package.json"), "utf8")) as { version: string }).version;
  const record: Provenance = {
    schema: 1,
    stage,
    generatedAt: new Date().toISOString(),
    target: { accountId: inputs.accountId, region: inputs.region, callerArn: caller.Arn },
    source: { commit: src.commit, stackSourceSha256: src.stackSourceSha256, clean: true },
    // Excludes the approval record, which names this capture (deploy-guard.ts inputsDigest).
    inputs: { sha256: inputsDigest(rawInputs), sanitizedPath: `docs/plans/${stage}.inputs.sanitized.json`, sanitizedSha256: sha256(sanitized) },
    state: {
      bucket: state.bucket,
      prefix: state.prefix,
      versioning: "Enabled",
      method: "snapshot",
      objects: snap.objects.length,
      snapshotSha256: sha256(snapText),
      versionsDigest: state.versionsDigest,
      versionEntries: state.versionEntries,
    },
    tools: {
      alchemy: version("alchemy"),
      "@alchemy.run/aws": version("@alchemy.run/aws"),
      "@alchemy.run/kubernetes": version("@alchemy.run/kubernetes"),
      bun: run("bun", ["--version"], root).trim(),
      "aws-cli": run("aws", ["--version"], root).trim(),
      helm: run("helm", ["version", "--short"], root).trim(),
    },
    outputs: {
      plan: { path: `docs/plans/${stage}.plan.txt`, sha256: sha256(plan) },
      detailed: { path: `docs/plans/${stage}.plan.detailed.txt`, sha256: sha256(detailed) },
    },
    summary: /Plan: .*/.exec(plan)?.[0] ?? "no summary line",
    note: "Planned against a read-only snapshot of the state (GETs pinned to version ids, in-memory Alchemy state): no state-bucket writes, no lease. The version listing (all versions and delete markers) was identical before and after. AWS provider reads only (Describe/List/Get). `96bulls up` re-checks everything before any change.",
  };
  writeFileSync(p.provenance, `${JSON.stringify(record, null, 2)}\n`);
  return record;
}

/** `up` runs this first: the committed plan must still describe exactly what would be applied. */
export function verifyCapturedPlan(root: string, stage: string) {
  const p = planPaths(root, stage);
  if (!existsSync(p.provenance)) throw new Error(`no captured plan: run \`96bulls plan ${stage} --capture\`, review it, commit it`);
  const rec = JSON.parse(readFileSync(p.provenance, "utf8")) as Provenance;
  const inputs = JSON.parse(readFileSync(join(root, ".96bulls", stage, "aws-inputs.json"), "utf8")) as { accountId: string; region: string };
  const src = observeSource(root);
  const caller = awsJson(root, ["sts", "get-caller-identity"]) as { Account: string };
  const observed: Observed = {
    stage,
    accountId: caller.Account,
    region: inputs.region,
    commit: src.commit,
    stackSourceSha256: src.stackSourceSha256,
    clean: src.clean,
    inputsSha256: inputsDigest(readFileSync(join(root, ".96bulls", stage, "aws-inputs.json"), "utf8")),
    state: (({ versioning, versionsDigest, versionEntries }) => ({ versioning, versionsDigest, versionEntries }))(observeState(root, inputs.accountId, inputs.region, stage)),
    planSha256: sha256(readFileSync(p.plan, "utf8")),
    detailedSha256: sha256(readFileSync(p.detailed, "utf8")),
  };
  const bad = provenanceMismatches(rec, observed);
  if (bad.length) throw new Error(`the captured plan no longer matches; capture and review again:\n  - ${bad.join("\n  - ")}`);
  return rec;
}
