/**
 * The impure half of plan provenance (src/cli/plan-provenance.ts): git, the aws
 * CLI and the plan runs. Read-only against AWS.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { alchemyStateBucket, alchemyStatePrefix } from "../../stacks/aws/preflight.ts";
import { currentObjects, sanitizeInputs, sanitizePlan, sha256, type Provenance } from "./plan-provenance.ts";
import { capturePaths, observeSource, observeState, verifyDeployCapture, type CaptureIO } from "../../stacks/aws/capture-verify.ts";
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

/** The real side effects: git and the aws CLI in the repo root, files from disk. */
export const realIO = (root: string): CaptureIO => ({
  root,
  run: (cmd, args) => run(cmd, args, root),
  readFile: (path) => (existsSync(path) ? readFileSync(path, "utf8") : undefined),
});
export const planPaths = capturePaths;
const observeSourceAt = (root: string) => observeSource(realIO(root));
const observeStateAt = (root: string, accountId: string, region: string, stage: string) => observeState(realIO(root), accountId, region, stage);

/** GET each current state object at its exact version id. */
function snapshotState(root: string, stage: string, region: string, st: ReturnType<typeof observeStateAt>): StateSnapshot {
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
  const src = observeSourceAt(root);
  if (!src.clean) throw new Error("commit the stack source first: a plan must describe committed code");
  const caller = awsJson(root, ["sts", "get-caller-identity"]) as { Account: string; Arn: string };
  if (caller.Account !== inputs.accountId) throw new Error(`STS caller is ${caller.Account}, inputs say ${inputs.accountId}`);
  const state = observeStateAt(root, inputs.accountId, inputs.region, stage);
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
  const after = observeStateAt(root, inputs.accountId, inputs.region, stage);
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

/** `up` runs this first (and the stack runs it again on every deploy, however started). */
export function verifyCapturedPlan(root: string, stage: string) {
  const inputsText = readFileSync(join(root, ".96bulls", stage, "aws-inputs.json"), "utf8");
  return verifyDeployCapture(realIO(root), stage, inputsText);
}
