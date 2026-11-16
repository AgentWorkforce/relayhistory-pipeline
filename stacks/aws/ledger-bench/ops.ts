/**
 * The whole life of the ledger-bench throwaway stack, as code (docs/design/hosted-ledger-benchmark-plan.md).
 * Nothing here runs without Khaliq's own OK for these resources (AGENTS.md); every mutating command also
 * needs `--confirm-account <id>` to equal the account the credentials really are for.
 *
 *   bun stacks/aws/ledger-bench/ops.ts render   --inputs <file> --run-id <id> [--profile smoke] [--steps 1,2]
 *   bun stacks/aws/ledger-bench/ops.ts up-data      --inputs <file> --confirm-account <data account>
 *   bun stacks/aws/ledger-bench/ops.ts up-identity  --inputs <file> --confirm-account <cluster account>
 *   bun stacks/aws/ledger-bench/ops.ts up-namespace --inputs <file>
 *   bun stacks/aws/ledger-bench/ops.ts run      --inputs <file> --run-id <id> [--profile smoke] [--lanes r2,r3]
 *   bun stacks/aws/ledger-bench/ops.ts collect  --inputs <file> --run-id <id> --out <dir outside the repo>
 *   bun stacks/aws/ledger-bench/ops.ts down     --inputs <file> --confirm-account <data account> --confirm-cluster-account <cluster account>
 *   bun stacks/aws/ledger-bench/ops.ts verify   --inputs <file>
 *
 * The inputs file holds names and ids (accounts, cluster, image digest). It is not a credential, but it is
 * not public either: keep it outside the repository. Credentials come from the two AWS CLI profiles and the
 * kube context the inputs name; this program stores none.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { LANES, PROFILES, type Lane } from "../../../benchmarks/ledger-bench/config.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "../../..");
export const NAMESPACE = "ledger-bench";
export const STACKS = { west: "ledger-bench-data-west", east: "ledger-bench-data-east", identity: "ledger-bench-cluster-identity" } as const;

export interface Inputs {
  /** AWS CLI profile for the throwaway (sessions-dev) account. */
  dataProfile: string;
  dataAccountId: string;
  /** AWS CLI profile for the EKS cluster's account. */
  clusterProfile: string;
  clusterAccountId: string;
  clusterName: string;
  kubeContext: string;
  /** The compute's Region (R2..R6) and the baseline's (R1). */
  region: string;
  eastRegion: string;
  /** Zone ids for R4 (the nodes' zone) and R5. */
  expressZoneA: string;
  expressZoneB: string;
  /** The ingest-edge image, by digest. */
  image: string;
  nodeSelector: Record<string, string>;
  tolerations: Record<string, unknown>[];
  cpu: string;
  memory: string;
  owner: string;
  /** YYYY-MM-DD, apply date + 2 days. */
  ttl: string;
  budgetEmail?: string;
}

const ACCOUNT = /^\d{12}$/;

export function parseInputs(text: string): Inputs {
  const o = JSON.parse(text) as Partial<Inputs>;
  const need = (name: keyof Inputs, pattern: RegExp) => {
    const v = o[name];
    if (typeof v !== "string" || !pattern.test(v)) throw new Error(`ledger-bench: inputs.${name} is missing or malformed`);
  };
  need("dataProfile", /^[\w.@-]{1,64}$/);
  need("clusterProfile", /^[\w.@-]{1,64}$/);
  need("dataAccountId", ACCOUNT);
  need("clusterAccountId", ACCOUNT);
  need("clusterName", /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/);
  need("kubeContext", /^[\w.:@/-]{1,200}$/);
  need("region", /^[a-z]{2}(-[a-z]+)+-\d$/);
  need("eastRegion", /^[a-z]{2}(-[a-z]+)+-\d$/);
  need("expressZoneA", /^[a-z0-9]+-az\d+$/);
  need("expressZoneB", /^[a-z0-9]+-az\d+$/);
  // A tag can move; the benchmark must run the bytes ingest-edge runs.
  need("image", /^[a-z0-9.\-/:]+@sha256:[0-9a-f]{64}$/);
  need("cpu", /^\d+m?$/);
  need("memory", /^\d+(Mi|Gi)$/);
  need("owner", /^[a-z0-9-]{3,40}$/);
  need("ttl", /^\d{4}-\d{2}-\d{2}$/);
  if (o.dataAccountId === o.clusterAccountId) throw new Error("ledger-bench: the data account must not be the cluster's account");
  if (o.expressZoneA === o.expressZoneB) throw new Error("ledger-bench: R4 and R5 must be in different zones");
  if (o.region === o.eastRegion) throw new Error("ledger-bench: R1 must be in another Region than the compute");
  if (o.budgetEmail !== undefined && !/^[^@\s]+@[^@\s]+$/.test(o.budgetEmail)) throw new Error("ledger-bench: inputs.budgetEmail is malformed");
  return { ...(o as Inputs), nodeSelector: o.nodeSelector ?? {}, tolerations: o.tolerations ?? [] };
}

/** Resource names follow from the account and Regions, exactly as the templates derive them. */
export function resourceNames(i: Inputs) {
  return {
    R1: `ledger-bench-r1-${i.dataAccountId}-${i.eastRegion}`,
    R2: `ledger-bench-r2-${i.dataAccountId}-${i.region}`,
    R3: `ledger-bench-r3-${i.dataAccountId}-${i.region}`,
    R4: `ledger-bench-r4-${i.dataAccountId}--${i.expressZoneA}--x-s3`,
    R5: `ledger-bench-r5-${i.dataAccountId}--${i.expressZoneB}--x-s3`,
    R6: "ledger_bench",
    clientRole: "ledger-bench-client",
    podRole: "ledger-bench-pod",
  };
}

/** The client's LEDGER_BENCH_CONFIG. The key ARNs come from the stacks' outputs. */
export function clientConfig(i: Inputs, keys: { east: string; west: string }): Record<string, unknown> {
  const n = resourceNames(i);
  return {
    stsRegion: i.region,
    assumeRoleArn: `arn:aws:iam::${i.dataAccountId}:role/${n.clientRole}`,
    targets: {
      R1: { bucket: n.R1, region: i.eastRegion, kmsKeyArn: keys.east },
      R2: { bucket: n.R2, region: i.region, kmsKeyArn: keys.west },
      R3: { bucket: n.R3, region: i.region, kmsKeyArn: keys.west },
      R4: { bucket: n.R4, region: i.region },
      R5: { bucket: n.R5, region: i.region },
      R6: { table: n.R6, region: i.region },
    },
  };
}

export interface JobParams {
  runId: string;
  lane: Lane;
  profile: string;
  steps: string;
}

/** One lane's Job manifest. Every substituted value is validated, and JSON-encoded where it is structured. */
export function renderJob(template: string, i: Inputs, p: JobParams): string {
  if (!/^[a-z0-9][a-z0-9-]{2,30}$/.test(p.runId)) throw new Error("ledger-bench: run id must be 3-31 characters of [a-z0-9-]");
  if (!LANES.includes(p.lane)) throw new Error(`ledger-bench: unknown lane ${p.lane}`);
  if (!PROFILES[p.profile]) throw new Error(`ledger-bench: unknown profile ${p.profile}`);
  if (!/^(\d(,\d)*)?$/.test(p.steps)) throw new Error("ledger-bench: steps are a comma-separated list of 1..6");
  const values: Record<string, string> = {
    RUN_ID: p.runId,
    LANE: p.lane,
    PROFILE: p.profile,
    STEPS: p.steps,
    IMAGE: i.image,
    CPU: i.cpu,
    MEMORY: i.memory,
    NODE_SELECTOR_JSON: JSON.stringify(i.nodeSelector),
    TOLERATIONS_JSON: JSON.stringify(i.tolerations),
  };
  // An empty step list means every step: the client reads an empty LEDGER_BENCH_STEPS as unset.
  return template.replace(/\$\{([A-Z_]+)\}/g, (_, name: string) => {
    const v = values[name];
    if (v === undefined) throw new Error(`ledger-bench: the Job template names an unknown value ${name}`);
    return v;
  });
}

interface Run {
  status: number;
  stdout: string;
  stderr: string;
}

function exec(cmd: string, args: string[], input?: string): Run {
  const r = spawnSync(cmd, args, { encoding: "utf8", input, maxBuffer: 512 * 1024 * 1024 });
  if (r.error) throw new Error(`ledger-bench: cannot run ${cmd}: ${r.error.message}`);
  return { status: r.status ?? 1, stdout: r.stdout ?? "", stderr: r.stderr ?? "" };
}

function must(cmd: string, args: string[], input?: string): string {
  const r = exec(cmd, args, input);
  // The last stderr line names the failure; ids in it stay on the operator's terminal, not in a file.
  if (r.status !== 0) throw new Error(`ledger-bench: ${cmd} ${args.slice(0, 3).join(" ")} failed: ${r.stderr.trim().split("\n").pop() ?? ""}`);
  return r.stdout;
}

const aws = (profile: string, region: string, args: string[]) => ["--profile", profile, "--region", region, "--output", "json", ...args];
const kubectl = (i: Inputs, args: string[]) => ["--context", i.kubeContext, ...args];

/** The credentials must be for the account the operator typed: a wrong profile is refused before any change. */
function confirmAccount(profile: string, region: string, expected: string, confirmed: string | undefined, what: string): void {
  if (confirmed !== expected) throw new Error(`ledger-bench: ${what}: pass the account id you intend to change; it must equal the inputs' id. Nothing was changed.`);
  const live = (JSON.parse(must("aws", aws(profile, region, ["sts", "get-caller-identity"]))) as { Account: string }).Account;
  if (live !== expected) throw new Error(`ledger-bench: ${what}: profile ${profile} is for another account than the inputs name. Nothing was changed.`);
}

function deploy(profile: string, region: string, stack: string, template: string, parameters: Record<string, string>, iam: boolean, i: Inputs): void {
  must("aws", aws(profile, region, [
    "cloudformation", "deploy", "--stack-name", stack, "--template-file", join(HERE, template), "--no-fail-on-empty-changeset",
    ...(iam ? ["--capabilities", "CAPABILITY_NAMED_IAM"] : []),
    "--tags", "purpose=ledger-bench", `ttl=${i.ttl}`, `owner=${i.owner}`,
    ...(Object.keys(parameters).length > 0 ? ["--parameter-overrides", ...Object.entries(parameters).map(([k, v]) => `${k}=${v}`)] : []),
  ]));
}

function output(profile: string, region: string, stack: string, name: string): string {
  const stacks = JSON.parse(must("aws", aws(profile, region, ["cloudformation", "describe-stacks", "--stack-name", stack]))) as { Stacks: { Outputs?: { OutputKey: string; OutputValue: string }[] }[] };
  const value = stacks.Stacks[0]?.Outputs?.find((o) => o.OutputKey === name)?.OutputValue;
  if (!value) throw new Error(`ledger-bench: stack ${stack} has no output ${name}`);
  return value;
}

function upData(i: Inputs, confirmed: string | undefined): void {
  confirmAccount(i.dataProfile, i.region, i.dataAccountId, confirmed, "up-data");
  deploy(i.dataProfile, i.eastRegion, STACKS.east, "data-east.yaml", { Owner: i.owner, Ttl: i.ttl }, false, i);
  deploy(i.dataProfile, i.region, STACKS.west, "data-west.yaml", {
    ClusterAccountId: i.clusterAccountId, EastRegion: i.eastRegion, ExpressZoneA: i.expressZoneA, ExpressZoneB: i.expressZoneB, Owner: i.owner, Ttl: i.ttl,
    ...(i.budgetEmail ? { BudgetEmail: i.budgetEmail } : {}),
  }, true, i);
  console.log("up-data: both data stacks are deployed");
}

function upIdentity(i: Inputs, confirmed: string | undefined): void {
  confirmAccount(i.clusterProfile, i.region, i.clusterAccountId, confirmed, "up-identity");
  deploy(i.clusterProfile, i.region, STACKS.identity, "cluster-identity.yaml", { ClusterName: i.clusterName, DataAccountId: i.dataAccountId, Owner: i.owner, Ttl: i.ttl }, true, i);
  console.log("up-identity: the pod role and its Pod Identity association are deployed");
}

/** Builds the single-file client. Its hash goes into the results, so a run names the exact code it ran. */
export function buildBundle(): { path: string; sha256: string; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), "ledger-bench-"));
  const path = join(dir, "main.js");
  must("bun", ["build", join(ROOT, "benchmarks/ledger-bench/main.ts"), "--target=bun", "--minify-whitespace", "--outfile", path]);
  const bytes = readFileSync(path);
  if (bytes.length > 900_000) throw new Error("ledger-bench: the bundle no longer fits a ConfigMap");
  return { path, dir, sha256: createHash("sha256").update(bytes).digest("hex") };
}

function upNamespace(i: Inputs): void {
  must("kubectl", kubectl(i, ["apply", "-f", join(HERE, "namespace.yaml")]));
  console.log("up-namespace: namespace, ServiceAccount, NetworkPolicy and quota applied");
}

function run(i: Inputs, runId: string, profile: string, lanes: Lane[], steps: string): void {
  const keys = { east: output(i.dataProfile, i.eastRegion, STACKS.east, "KeyArn"), west: output(i.dataProfile, i.region, STACKS.west, "KeyArn") };
  const bundle = buildBundle();
  try {
    const apply = (manifest: string) => must("kubectl", kubectl(i, ["apply", "-f", "-"]), manifest);
    apply(must("kubectl", kubectl(i, ["-n", NAMESPACE, "create", "configmap", "ledger-bench-bundle", `--from-file=main.js=${bundle.path}`, "--dry-run=client", "-o", "yaml"])));
    apply(must("kubectl", kubectl(i, ["-n", NAMESPACE, "create", "configmap", "ledger-bench-config", `--from-literal=config.json=${JSON.stringify(clientConfig(i, keys))}`, "--dry-run=client", "-o", "yaml"])));
    const template = readFileSync(join(HERE, "job.template.yaml"), "utf8");
    for (const lane of lanes) apply(renderJob(template, i, { runId, lane, profile, steps }));
    console.log(JSON.stringify({ run: runId, profile, lanes, bundle_sha256: bundle.sha256 }));
  } finally {
    rmSync(bundle.dir, { recursive: true, force: true });
  }
}

function collect(i: Inputs, runId: string, out: string): void {
  const dir = resolve(out);
  if (dir.startsWith(ROOT)) throw new Error("ledger-bench: results go outside this (public) repository");
  mkdirSync(dir, { recursive: true });
  for (const lane of LANES) {
    const r = exec("kubectl", kubectl(i, ["-n", NAMESPACE, "logs", `job/ledger-bench-${runId}-${lane}`, "--tail=-1"]));
    if (r.status !== 0) {
      console.log(`collect: no log for lane ${lane}`);
      continue;
    }
    const lines = r.stdout.split("\n").filter((l) => l.startsWith('{"ledger_bench":1'));
    writeFileSync(join(dir, `${runId}-${lane}.jsonl`), `${lines.join("\n")}\n`);
    console.log(`collect: lane ${lane}: ${lines.length} records, ${lines.some((l) => l.includes('"event":"end"')) ? "ended" : "NOT ended"}`);
  }
}

const gone = (r: Run, marker: RegExp) => r.status !== 0 && marker.test(r.stderr);

function emptyBucket(i: Inputs, region: string, bucket: string): void {
  const r = exec("aws", aws(i.dataProfile, region, ["s3", "rm", `s3://${bucket}`, "--recursive", "--only-show-errors"]));
  if (r.status !== 0 && !/NoSuchBucket/.test(r.stderr)) throw new Error(`ledger-bench: could not empty a bucket in ${region}: ${r.stderr.trim().split("\n").pop() ?? ""}`);
}

function deleteStack(profile: string, region: string, stack: string): void {
  must("aws", aws(profile, region, ["cloudformation", "delete-stack", "--stack-name", stack]));
  must("aws", aws(profile, region, ["cloudformation", "wait", "stack-delete-complete", "--stack-name", stack]));
}

function down(i: Inputs, confirmedData: string | undefined, confirmedCluster: string | undefined): void {
  confirmAccount(i.dataProfile, i.region, i.dataAccountId, confirmedData, "down");
  confirmAccount(i.clusterProfile, i.region, i.clusterAccountId, confirmedCluster, "down (cluster account)");
  const n = resourceNames(i);
  // 1. The client first, so nothing is writing while the buckets are emptied.
  must("kubectl", kubectl(i, ["delete", "namespace", NAMESPACE, "--ignore-not-found", "--wait=true", "--timeout=300s"]));
  // 2. Every object, directory buckets included (a stack cannot delete a non-empty bucket).
  emptyBucket(i, i.eastRegion, n.R1);
  for (const bucket of [n.R2, n.R3, n.R4, n.R5]) emptyBucket(i, i.region, bucket);
  // 3. The stacks: identity first (the pod loses its role), then the data.
  deleteStack(i.clusterProfile, i.region, STACKS.identity);
  deleteStack(i.dataProfile, i.region, STACKS.west);
  deleteStack(i.dataProfile, i.eastRegion, STACKS.east);
  console.log("down: namespace deleted, buckets emptied, three stacks deleted; run verify");
}

export interface Verification {
  at: string;
  checks: { what: string; gone: boolean; detail?: string }[];
  /** Tagged resources still listed: only KMS keys pending deletion are expected. */
  tagged_remaining: { region: string; type: string; state: string }[];
  clean: boolean;
}

/** Proves the teardown: every resource is looked up by name and by tag. Prints no id, ARN or bucket name. */
function verify(i: Inputs): Verification {
  const n = resourceNames(i);
  const checks: Verification["checks"] = [];
  const check = (what: string, r: Run, marker: RegExp) => checks.push({ what, gone: gone(r, marker), ...(gone(r, marker) ? {} : { detail: r.status === 0 ? "still exists" : "lookup failed" }) });
  const data = (region: string, args: string[]) => exec("aws", aws(i.dataProfile, region, args));
  check("stack data-west", data(i.region, ["cloudformation", "describe-stacks", "--stack-name", STACKS.west]), /does not exist/);
  check("stack data-east", data(i.eastRegion, ["cloudformation", "describe-stacks", "--stack-name", STACKS.east]), /does not exist/);
  check("stack cluster-identity", exec("aws", aws(i.clusterProfile, i.region, ["cloudformation", "describe-stacks", "--stack-name", STACKS.identity])), /does not exist/);
  check("R1 bucket", data(i.eastRegion, ["s3api", "head-bucket", "--bucket", n.R1]), /404|Not Found|NoSuchBucket/);
  for (const id of ["R2", "R3", "R4", "R5"] as const) check(`${id} bucket`, data(i.region, ["s3api", "head-bucket", "--bucket", n[id]]), /404|Not Found|NoSuchBucket/);
  check("R6 table", data(i.region, ["dynamodb", "describe-table", "--table-name", n.R6]), /ResourceNotFoundException/);
  check("client role", data(i.region, ["iam", "get-role", "--role-name", n.clientRole]), /NoSuchEntity/);
  check("pod role", exec("aws", aws(i.clusterProfile, i.region, ["iam", "get-role", "--role-name", n.podRole])), /NoSuchEntity/);
  check("budget", data("us-east-1", ["budgets", "describe-budget", "--account-id", i.dataAccountId, "--budget-name", "ledger-bench"]), /NotFoundException/);
  check("namespace", exec("kubectl", kubectl(i, ["get", "namespace", NAMESPACE])), /NotFound/);
  const associations = exec("aws", aws(i.clusterProfile, i.region, ["eks", "list-pod-identity-associations", "--cluster-name", i.clusterName, "--namespace", NAMESPACE]));
  const left = associations.status === 0 ? ((JSON.parse(associations.stdout) as { associations?: unknown[] }).associations ?? []).length : -1;
  checks.push({ what: "pod identity association", gone: left === 0, ...(left === 0 ? {} : { detail: left < 0 ? "lookup failed" : "still exists" }) });
  const directory = data(i.region, ["s3api", "list-directory-buckets"]);
  const directoryLeft = directory.status === 0 ? ((JSON.parse(directory.stdout) as { Buckets?: { Name: string }[] }).Buckets ?? []).filter((b) => b.Name.startsWith("ledger-bench-")).length : -1;
  checks.push({ what: "directory buckets named ledger-bench-*", gone: directoryLeft === 0, ...(directoryLeft === 0 ? {} : { detail: directoryLeft < 0 ? "lookup failed" : "still exists" }) });

  const tagged: Verification["tagged_remaining"] = [];
  let tagLookupFailed = false;
  for (const [profile, region] of [[i.dataProfile, i.region], [i.dataProfile, i.eastRegion], [i.clusterProfile, i.region]] as const) {
    const r = exec("aws", aws(profile, region, ["resourcegroupstaggingapi", "get-resources", "--tag-filters", "Key=purpose,Values=ledger-bench"]));
    if (r.status !== 0) {
      tagLookupFailed = true;
      continue;
    }
    for (const m of (JSON.parse(r.stdout) as { ResourceTagMappingList?: { ResourceARN: string }[] }).ResourceTagMappingList ?? []) {
      const type = m.ResourceARN.split(":").slice(2, 3).join("");
      let state = "exists";
      if (type === "kms") {
        const key = exec("aws", aws(profile, region, ["kms", "describe-key", "--key-id", m.ResourceARN]));
        state = key.status === 0 ? (JSON.parse(key.stdout) as { KeyMetadata: { KeyState: string } }).KeyMetadata.KeyState : "lookup failed";
      }
      tagged.push({ region, type, state });
    }
  }
  if (tagLookupFailed) checks.push({ what: "tag listing", gone: false, detail: "lookup failed" });
  const clean = checks.every((c) => c.gone) && tagged.every((t) => t.type === "kms" && t.state === "PendingDeletion");
  return { at: new Date().toISOString(), checks, tagged_remaining: tagged, clean };
}

if (import.meta.main) {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      inputs: { type: "string" }, "run-id": { type: "string" }, profile: { type: "string", default: "standard" }, lanes: { type: "string" }, steps: { type: "string", default: "" },
      out: { type: "string" }, "confirm-account": { type: "string" }, "confirm-cluster-account": { type: "string" },
    },
  });
  const command = positionals[0];
  if (!values.inputs) throw new Error("ledger-bench: --inputs <file> is required");
  if (resolve(values.inputs).startsWith(ROOT)) throw new Error("ledger-bench: keep the inputs file outside this (public) repository");
  const inputs = parseInputs(readFileSync(values.inputs, "utf8"));
  const runId = values["run-id"] ?? "";
  const lanes = (values.lanes ? values.lanes.split(",") : [...LANES]) as Lane[];
  switch (command) {
    case "render": {
      const template = readFileSync(join(HERE, "job.template.yaml"), "utf8");
      console.log(readFileSync(join(HERE, "namespace.yaml"), "utf8"));
      for (const lane of lanes) console.log(`---\n${renderJob(template, inputs, { runId, lane, profile: values.profile!, steps: values.steps! })}`);
      break;
    }
    case "up-data": upData(inputs, values["confirm-account"]); break;
    case "up-identity": upIdentity(inputs, values["confirm-account"]); break;
    case "up-namespace": upNamespace(inputs); break;
    case "run": run(inputs, runId, values.profile!, lanes, values.steps!); break;
    case "collect": collect(inputs, runId, values.out ?? ""); break;
    case "down": down(inputs, values["confirm-account"], values["confirm-cluster-account"]); break;
    case "verify": {
      const v = verify(inputs);
      console.log(JSON.stringify(v, null, 2));
      if (!v.clean) process.exit(1);
      break;
    }
    default:
      console.error("usage: ops.ts render|up-data|up-identity|up-namespace|run|collect|down|verify --inputs <file> ...");
      process.exit(2);
  }
}
