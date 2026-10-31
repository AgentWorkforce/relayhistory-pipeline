/**
 * The impure half of plan provenance (src/cli/plan-provenance.ts): git, the aws
 * CLI and the plan runs. Read-only against AWS.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { alchemyStateBucket, alchemyStatePrefix } from "../../stacks/aws/preflight.ts";
import { STACK_SOURCE_PATHS, provenanceMismatches, sanitizeInputs, sanitizePlan, sha256, stateDigest, type Observed, type Provenance } from "./plan-provenance.ts";
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

function observeState(root: string, accountId: string, region: string, stage: string) {
  const bucket = alchemyStateBucket(accountId, region);
  // Refuse rather than let Alchemy create a missing state bucket during a "plan".
  run("aws", ["s3api", "head-bucket", "--bucket", bucket, "--region", region], root);
  const prefix = `${alchemyStatePrefix(stage)}/`;
  const listed = awsJson(root, ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix, "--region", region]) as { Contents?: { Key: string; ETag: string }[] };
  return { bucket, prefix, ...stateDigest(listed.Contents ?? []) };
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
  const cidrs = inputs.eks?.apiAccess?.publicAccessCidrs ?? [];
  const exec = (detailed: boolean) => {
    const inv = awsInvocation("plan", stage, env, { detailed });
    const r = spawnSync("bunx", inv.args, { cwd: root, encoding: "utf8", env: { ...process.env, ...inv.env }, maxBuffer: 256 * 1024 * 1024 });
    if (r.status !== 0) throw new Error(`plan failed:\n${r.stdout}\n${r.stderr}`);
    return sanitizePlan(`${r.stdout}${r.stderr}`, cidrs);
  };
  const plan = exec(false);
  const detailed = exec(true);
  // State must not have moved while planning (nothing else was applying).
  const after = observeState(root, inputs.accountId, inputs.region, stage);
  if (after.digest !== state.digest) throw new Error("Alchemy state changed while planning; capture again");
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
    inputs: { sha256: sha256(rawInputs), sanitizedPath: `docs/plans/${stage}.inputs.sanitized.json`, sanitizedSha256: sha256(sanitized) },
    state: { bucket: state.bucket, prefix: state.prefix, bucketExisted: true, objects: state.objects, digest: state.digest },
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
    note: "Read-only alchemy plan (state bucket existed; state digest unchanged across the run). Verified again by `96bulls up` before any change.",
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
    inputsSha256: sha256(readFileSync(join(root, ".96bulls", stage, "aws-inputs.json"), "utf8")),
    state: observeState(root, inputs.accountId, inputs.region, stage),
    planSha256: sha256(readFileSync(p.plan, "utf8")),
    detailedSha256: sha256(readFileSync(p.detailed, "utf8")),
  };
  const bad = provenanceMismatches(rec, observed);
  if (bad.length) throw new Error(`the captured plan no longer matches; capture and review again:\n  - ${bad.join("\n  - ")}`);
  return rec;
}
