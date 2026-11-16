/**
 * The whole life of the ledger-bench throwaway stack, as code (docs/design/hosted-ledger-benchmark-plan.md).
 * Nothing here runs without Khaliq's own OK for these resources (AGENTS.md). The data side and the cluster
 * side can be run by two operators with different access: each command needs only its own side's access,
 * and every command that touches an account first checks that the credentials are for the account the
 * operator typed.
 *
 *   Data side (the throwaway account):
 *     ops.ts up-data      --inputs <file> --confirm-account <data account>
 *     ops.ts outputs-data --inputs <file>                 # the key ARNs the cluster side's inputs need
 *     ops.ts down-data    --inputs <file> --confirm-account <data account>
 *     ops.ts verify-data  --inputs <file> --confirm-account <data account>
 *   Cluster side (the EKS cluster and its account):
 *     ops.ts up-identity  --inputs <file> --confirm-account <cluster account>
 *     ops.ts up-namespace --inputs <file>
 *     ops.ts run          --inputs <file> --run-id <id> [--profile smoke] [--lanes r2,r3] [--steps 1,2]
 *     ops.ts collect      --inputs <file> --run-id <id> --out <dir outside the repo>
 *     ops.ts down-cluster --inputs <file> --confirm-account <cluster account> (--collected <dir> | --discard-results)
 *     ops.ts verify-cluster --inputs <file> --confirm-account <cluster account>
 *   Either:
 *     ops.ts render       --inputs <file> --run-id <id>   # prints the manifests; contacts nothing
 *
 * The inputs file holds names and ids (accounts, cluster, image digest). It is not a credential, but it is
 * not public either: keep it outside the repository. Credentials come from the AWS CLI profiles and the
 * kube context the inputs name; this program stores none.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { LANES, PROFILES, type Lane } from "../../../benchmarks/ledger-bench/config.ts";
import { APPROVED_CAP_USD, FIXED_USD, laneCostBound } from "../../../benchmarks/ledger-bench/cost.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "../../..");
export const NAMESPACE = "ledger-bench";
export const STACKS = { west: "ledger-bench-data-west", east: "ledger-bench-data-east", identity: "ledger-bench-cluster-identity" } as const;

export interface KeyArns {
  /** K1 (R1's key), K2 (the key of R2..R5) and the probe key no bucket uses. */
  east: string;
  west: string;
  probe: string;
}

export interface Inputs {
  /** AWS CLI profile for the throwaway (sessions-dev) account. Needed by the data-side commands only. */
  dataProfile?: string;
  dataAccountId: string;
  /** AWS CLI profile for the EKS cluster's account. Needed by the cluster-side commands only. */
  clusterProfile?: string;
  clusterAccountId: string;
  clusterName: string;
  /** Needed by the cluster-side commands only. */
  kubeContext?: string;
  /** From `outputs-data`. Needed by `run`. */
  keys?: KeyArns;
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
  /** CPU request and limit; the limit caps what the client can take from a node it shares with production pods. */
  cpu: string;
  cpuLimit: string;
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
  const optional = (name: keyof Inputs, pattern: RegExp) => {
    if (o[name] !== undefined) need(name, pattern);
  };
  optional("dataProfile", /^[\w.@-]{1,64}$/);
  optional("clusterProfile", /^[\w.@-]{1,64}$/);
  optional("kubeContext", /^[\w.:@/-]{1,200}$/);
  if (o.keys !== undefined) {
    const arn = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;
    for (const k of ["east", "west", "probe"] as const) if (typeof o.keys[k] !== "string" || !arn.test(o.keys[k])) throw new Error(`ledger-bench: inputs.keys.${k} is not a KMS key ARN`);
    if (new Set([o.keys.east, o.keys.west, o.keys.probe]).size !== 3) throw new Error("ledger-bench: inputs.keys must name three different keys");
  }
  need("dataAccountId", ACCOUNT);
  need("clusterAccountId", ACCOUNT);
  need("clusterName", /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/);
  need("region", /^[a-z]{2}(-[a-z]+)+-\d$/);
  need("eastRegion", /^[a-z]{2}(-[a-z]+)+-\d$/);
  need("expressZoneA", /^[a-z0-9]+-az\d+$/);
  need("expressZoneB", /^[a-z0-9]+-az\d+$/);
  // A tag can move; the benchmark must run the bytes ingest-edge runs.
  need("image", /^[a-z0-9.\-/:]+@sha256:[0-9a-f]{64}$/);
  need("cpu", /^\d+m?$/);
  need("cpuLimit", /^\d+m?$/);
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
export function clientConfig(i: Inputs, keys: KeyArns): Record<string, unknown> {
  const n = resourceNames(i);
  return {
    stsRegion: i.region,
    assumeRoleArn: `arn:aws:iam::${i.dataAccountId}:role/${n.clientRole}`,
    foreignKmsKeyArn: keys.probe,
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
  /** SHA-256 of the client bundle the Job runs; recorded in the lane's `start` record. */
  bundleSha256: string;
}

/** One lane's Job manifest. Every substituted value is validated, and JSON-encoded where it is structured. */
export function renderJob(template: string, i: Inputs, p: JobParams): string {
  if (!/^[a-z0-9][a-z0-9-]{1,29}[a-z0-9]$/.test(p.runId)) throw new Error("ledger-bench: run id must be 3-31 characters of [a-z0-9-], ending in a letter or digit");
  if (!LANES.includes(p.lane)) throw new Error(`ledger-bench: unknown lane ${p.lane}`);
  if (!Object.hasOwn(PROFILES, p.profile)) throw new Error(`ledger-bench: unknown profile ${p.profile}`);
  if (!/^([0-9a-f]{64})?$/.test(p.bundleSha256)) throw new Error("ledger-bench: bundle hash must be a SHA-256");
  if (!/^(\d(,\d)*)?$/.test(p.steps)) throw new Error("ledger-bench: steps are a comma-separated list of 1..6");
  const values: Record<string, string> = {
    RUN_ID: p.runId,
    LANE: p.lane,
    PROFILE: p.profile,
    STEPS: p.steps,
    BUNDLE_SHA256: p.bundleSha256,
    IMAGE: i.image,
    CPU: i.cpu,
    CPU_LIMIT: i.cpuLimit,
    MEMORY: i.memory,
    NODE_SELECTOR_JSON: JSON.stringify(i.nodeSelector),
    TOLERATIONS_JSON: JSON.stringify(i.tolerations),
  };
  // An empty step list means every step: the client reads an empty LEDGER_BENCH_STEPS as unset.
  return template.replace(/\$\{([A-Z0-9_]+)\}/g, (_, name: string) => {
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

function dataProfile(i: Inputs): string {
  if (!i.dataProfile) throw new Error("ledger-bench: this command needs inputs.dataProfile (the data side)");
  return i.dataProfile;
}
function clusterProfile(i: Inputs): string {
  if (!i.clusterProfile) throw new Error("ledger-bench: this command needs inputs.clusterProfile (the cluster side)");
  return i.clusterProfile;
}
function kubectl(i: Inputs, args: string[]): string[] {
  if (!i.kubeContext) throw new Error("ledger-bench: this command needs inputs.kubeContext (the cluster side)");
  return ["--context", i.kubeContext, ...args];
}

/** The credentials must be for the account the operator typed: a wrong profile is refused before anything else. */
function confirmAccount(profile: string, region: string, expected: string, confirmed: string | undefined, what: string): void {
  if (confirmed !== expected) throw new Error(`ledger-bench: ${what}: pass --confirm-account with the account id you mean; it must equal the inputs' id. Nothing was changed.`);
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
  const profile = dataProfile(i);
  confirmAccount(profile, i.region, i.dataAccountId, confirmed, "up-data");
  deploy(profile, i.eastRegion, STACKS.east, "data-east.yaml", { Owner: i.owner, Ttl: i.ttl }, false, i);
  deploy(profile, i.region, STACKS.west, "data-west.yaml", {
    ClusterAccountId: i.clusterAccountId, EastRegion: i.eastRegion, ExpressZoneA: i.expressZoneA, ExpressZoneB: i.expressZoneB, Owner: i.owner, Ttl: i.ttl,
    ...(i.budgetEmail ? { BudgetEmail: i.budgetEmail } : {}),
  }, true, i);
  console.log("up-data: both data stacks are deployed; run outputs-data for the key ARNs");
}

function outputsData(i: Inputs): KeyArns {
  const profile = dataProfile(i);
  return { east: output(profile, i.eastRegion, STACKS.east, "KeyArn"), west: output(profile, i.region, STACKS.west, "KeyArn"), probe: output(profile, i.region, STACKS.west, "ProbeKeyArn") };
}

function upIdentity(i: Inputs, confirmed: string | undefined): void {
  const profile = clusterProfile(i);
  confirmAccount(profile, i.region, i.clusterAccountId, confirmed, "up-identity");
  deploy(profile, i.region, STACKS.identity, "cluster-identity.yaml", { ClusterName: i.clusterName, DataAccountId: i.dataAccountId, Owner: i.owner, Ttl: i.ttl }, true, i);
  console.log("up-identity: the pod role and its Pod Identity association are deployed");
}

/**
 * A ConfigMap value is capped near 1 MiB, and the object is applied server-side: a client-side apply would
 * copy it into an annotation, which is capped at 256 KiB.
 */
export const BUNDLE_MAX_BYTES = 900_000;

/** Builds the single-file client. Its hash goes into the results, so a run names the exact code it ran. */
export function buildBundle(): { path: string; sha256: string; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), "ledger-bench-"));
  const path = join(dir, "main.js");
  must("bun", ["build", join(ROOT, "benchmarks/ledger-bench/main.ts"), "--target=bun", "--minify-whitespace", "--outfile", path]);
  const bytes = readFileSync(path);
  if (bytes.length > BUNDLE_MAX_BYTES) throw new Error("ledger-bench: the bundle no longer fits a ConfigMap");
  return { path, dir, sha256: createHash("sha256").update(bytes).digest("hex") };
}

function upNamespace(i: Inputs): void {
  must("kubectl", kubectl(i, ["apply", "-f", join(HERE, "namespace.yaml")]));
  console.log("up-namespace: namespace, ServiceAccount, NetworkPolicy and quota applied");
}

export interface LedgerEntry {
  run: string;
  lane: Lane;
  profile: string;
  usd: number;
}

/**
 * The spend ledger: every lane run started, at the most it can cost. A run that would take the total past
 * the approved cap is refused, so the cap holds over smoke runs, repeats and re-runs together.
 */
export function ledgerAdd(entries: LedgerEntry[], run: string, profile: string, lanes: Lane[], cap = APPROVED_CAP_USD): LedgerEntry[] {
  if (!Object.hasOwn(PROFILES, profile)) throw new Error(`ledger-bench: unknown profile ${profile}`);
  const added = lanes.map((lane) => ({ run, lane, profile, usd: laneCostBound(PROFILES[profile]!, lane) }));
  const next = [...entries, ...added];
  const total = next.reduce((a, e) => a + e.usd, 0) + FIXED_USD;
  if (total > cap) throw new Error(`ledger-bench: this run would take the benchmark's cost bound to ${Math.ceil(total)} USD, over the approved ${cap} USD. Nothing was started.`);
  return next;
}

const LEDGER = "ledger-bench-ledger";

function readLedger(i: Inputs): LedgerEntry[] {
  const r = exec("kubectl", kubectl(i, ["-n", NAMESPACE, "get", "configmap", LEDGER, "-o", "json"]));
  if (r.status !== 0) {
    if (/NotFound/.test(r.stderr)) return [];
    throw new Error("ledger-bench: cannot read the spend ledger; nothing was started");
  }
  return JSON.parse((JSON.parse(r.stdout) as { data?: { entries?: string } }).data?.entries ?? "[]") as LedgerEntry[];
}

/** Server-side apply of a ConfigMap built by kubectl itself (so no value is ever interpolated into YAML). */
function applyConfigMap(i: Inputs, name: string, from: string): void {
  const manifest = must("kubectl", kubectl(i, ["-n", NAMESPACE, "create", "configmap", name, from, "--dry-run=client", "-o", "yaml"]));
  must("kubectl", kubectl(i, ["apply", "--server-side", "--force-conflicts", "--field-manager=ledger-bench", "-f", "-"]), manifest);
}

function run(i: Inputs, runId: string, profile: string, lanes: Lane[], steps: string): void {
  if (!i.keys) throw new Error("ledger-bench: run needs inputs.keys (from outputs-data)");
  const entries = readLedger(i);
  if (entries.some((e) => e.run === runId && lanes.includes(e.lane))) throw new Error("ledger-bench: that run id already ran one of these lanes; choose a new run id. Nothing was started.");
  const ledger = ledgerAdd(entries, runId, profile, lanes);
  const bundle = buildBundle();
  try {
    // The ledger is written before any Job exists: a crash here over-counts, never under-counts.
    applyConfigMap(i, LEDGER, `--from-literal=entries=${JSON.stringify(ledger)}`);
    applyConfigMap(i, "ledger-bench-bundle", `--from-file=main.js=${bundle.path}`);
    applyConfigMap(i, "ledger-bench-config", `--from-literal=config.json=${JSON.stringify(clientConfig(i, i.keys))}`);
    const template = readFileSync(join(HERE, "job.template.yaml"), "utf8");
    for (const lane of lanes) must("kubectl", kubectl(i, ["apply", "-f", "-"]), renderJob(template, i, { runId, lane, profile, steps, bundleSha256: bundle.sha256 }));
    console.log(JSON.stringify({ run: runId, profile, lanes, bundle_sha256: bundle.sha256, cost_bound_so_far_usd: Math.ceil(ledger.reduce((a, e) => a + e.usd, 0) + FIXED_USD) }));
  } finally {
    rmSync(bundle.dir, { recursive: true, force: true });
  }
}

/** True for a path inside this repository, also when reached through a symlink or (on a case-insensitive volume) another letter case. */
export function insideRepo(path: string): boolean {
  let p = resolve(path);
  // The nearest existing ancestor decides: the path itself may not exist yet.
  while (!existsSync(p) && dirname(p) !== p) p = dirname(p);
  const real = realpathSync(p).toLowerCase();
  const root = realpathSync(ROOT).toLowerCase();
  return real === root || real.startsWith(`${root}/`);
}

function collect(i: Inputs, runId: string, out: string): void {
  if (!out || insideRepo(out)) throw new Error("ledger-bench: results go outside this (public) repository");
  const dir = resolve(out);
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

/** Lane runs in the ledger with no collected `end` record in `dir`: what a teardown would lose. */
export function uncollected(entries: LedgerEntry[], dir: string): string[] {
  const files = existsSync(dir) ? readdirSync(dir) : [];
  return entries
    .map((e) => `${e.run}-${e.lane}`)
    .filter((name) => !files.includes(`${name}.jsonl`) || !readFileSync(join(dir, `${name}.jsonl`), "utf8").includes('"event":"end"'));
}

const gone = (r: Run, marker: RegExp) => r.status !== 0 && marker.test(r.stderr);

function emptyBucket(profile: string, region: string, bucket: string): void {
  const r = exec("aws", aws(profile, region, ["s3", "rm", `s3://${bucket}`, "--recursive", "--only-show-errors"]));
  if (r.status !== 0 && !/NoSuchBucket|does not exist|Not Found|404/.test(r.stderr)) throw new Error(`ledger-bench: could not empty a bucket in ${region}: ${r.stderr.trim().split("\n").pop() ?? ""}`);
}

function deleteStack(profile: string, region: string, stack: string): void {
  must("aws", aws(profile, region, ["cloudformation", "delete-stack", "--stack-name", stack]));
  must("aws", aws(profile, region, ["cloudformation", "wait", "stack-delete-complete", "--stack-name", stack]));
}

/** The client first: with the namespace and the pod role gone, nothing can write while the data is removed. */
function downCluster(i: Inputs, confirmed: string | undefined, collected: string | undefined, discard: boolean): void {
  const profile = clusterProfile(i);
  confirmAccount(profile, i.region, i.clusterAccountId, confirmed, "down-cluster");
  if (!discard) {
    // The Jobs' logs are the results; deleting the namespace deletes them.
    const missing = collected ? uncollected(readLedger(i), collected) : ["(no --collected directory given)"];
    if (missing.length > 0) throw new Error(`ledger-bench: down-cluster would lose results not yet collected: ${missing.join(", ")}. Collect them, or pass --discard-results. Nothing was changed.`);
  }
  must("kubectl", kubectl(i, ["delete", "namespace", NAMESPACE, "--ignore-not-found", "--wait=true", "--timeout=300s"]));
  deleteStack(profile, i.region, STACKS.identity);
  console.log("down-cluster: namespace and identity stack deleted; run verify-cluster");
}

function downData(i: Inputs, confirmed: string | undefined): void {
  const profile = dataProfile(i);
  confirmAccount(profile, i.region, i.dataAccountId, confirmed, "down-data");
  const n = resourceNames(i);
  // Every object, directory buckets included (a stack cannot delete a non-empty bucket).
  emptyBucket(profile, i.eastRegion, n.R1);
  for (const bucket of [n.R2, n.R3, n.R4, n.R5]) emptyBucket(profile, i.region, bucket);
  deleteStack(profile, i.region, STACKS.west);
  deleteStack(profile, i.eastRegion, STACKS.east);
  console.log("down-data: buckets emptied, both data stacks deleted; run verify-data");
}

export interface Check {
  what: string;
  gone: boolean;
  detail?: string;
}

export interface Verification {
  side: "data" | "cluster";
  at: string;
  checks: Check[];
  /** Tagged resources still listed: only KMS keys pending deletion are expected. */
  tagged_remaining: { region: string; type: string; state: string }[];
  clean: boolean;
}

/**
 * Clean means every lookup positively said "gone" (a lookup that failed for another reason is not gone),
 * and nothing tagged remains but KMS keys in their pending-deletion window.
 */
export function isClean(checks: Check[], tagged: Verification["tagged_remaining"]): boolean {
  return checks.length > 0 && checks.every((c) => c.gone) && tagged.every((t) => t.type === "kms" && t.state === "PendingDeletion");
}

function taggedRemaining(profile: string, regions: string[], checks: Check[]): Verification["tagged_remaining"] {
  const tagged: Verification["tagged_remaining"] = [];
  for (const region of regions) {
    const r = exec("aws", aws(profile, region, ["resourcegroupstaggingapi", "get-resources", "--tag-filters", "Key=purpose,Values=ledger-bench"]));
    if (r.status !== 0) {
      checks.push({ what: `tag listing ${region}`, gone: false, detail: "lookup failed" });
      continue;
    }
    checks.push({ what: `tag listing ${region}`, gone: true });
    for (const m of (JSON.parse(r.stdout) as { ResourceTagMappingList?: { ResourceARN: string }[] }).ResourceTagMappingList ?? []) {
      const type = m.ResourceARN.split(":")[2] ?? "unknown";
      let state = "exists";
      if (type === "kms") {
        const key = exec("aws", aws(profile, region, ["kms", "describe-key", "--key-id", m.ResourceARN]));
        state = key.status === 0 ? (JSON.parse(key.stdout) as { KeyMetadata: { KeyState: string } }).KeyMetadata.KeyState : "lookup failed";
      }
      tagged.push({ region, type, state });
    }
  }
  return tagged;
}

const checkOf = (what: string, r: Run, marker: RegExp): Check => ({ what, gone: gone(r, marker), ...(gone(r, marker) ? {} : { detail: r.status === 0 ? "still exists" : "lookup failed" }) });

/** Proves the data side's teardown: every resource by name, then by tag. Prints no id, ARN or bucket name. */
function verifyData(i: Inputs, confirmed: string | undefined): Verification {
  const profile = dataProfile(i);
  // A profile for another account would answer "not found" to everything.
  confirmAccount(profile, i.region, i.dataAccountId, confirmed, "verify-data");
  const n = resourceNames(i);
  const data = (region: string, args: string[]) => exec("aws", aws(profile, region, args));
  const checks: Check[] = [
    checkOf("stack data-west", data(i.region, ["cloudformation", "describe-stacks", "--stack-name", STACKS.west]), /does not exist/),
    checkOf("stack data-east", data(i.eastRegion, ["cloudformation", "describe-stacks", "--stack-name", STACKS.east]), /does not exist/),
    checkOf("R1 bucket", data(i.eastRegion, ["s3api", "head-bucket", "--bucket", n.R1]), /\b404\b|Not Found|NoSuchBucket/),
    ...(["R2", "R3", "R4", "R5"] as const).map((id) => checkOf(`${id} bucket`, data(i.region, ["s3api", "head-bucket", "--bucket", n[id]]), /\b404\b|Not Found|NoSuchBucket/)),
    checkOf("R6 table", data(i.region, ["dynamodb", "describe-table", "--table-name", n.R6]), /ResourceNotFoundException/),
    checkOf("client role", data(i.region, ["iam", "get-role", "--role-name", n.clientRole]), /NoSuchEntity/),
    checkOf("budget", data("us-east-1", ["budgets", "describe-budget", "--account-id", i.dataAccountId, "--budget-name", "ledger-bench"]), /NotFoundException/),
  ];
  const directory = data(i.region, ["s3api", "list-directory-buckets"]);
  const left = directory.status === 0 ? ((JSON.parse(directory.stdout) as { Buckets?: { Name: string }[] }).Buckets ?? []).filter((b) => b.Name.startsWith("ledger-bench-")).length : -1;
  checks.push({ what: "directory buckets named ledger-bench-*", gone: left === 0, ...(left === 0 ? {} : { detail: left < 0 ? "lookup failed" : "still exists" }) });
  const tagged = taggedRemaining(profile, [i.region, i.eastRegion], checks);
  return { side: "data", at: new Date().toISOString(), checks, tagged_remaining: tagged, clean: isClean(checks, tagged) };
}

function verifyCluster(i: Inputs, confirmed: string | undefined): Verification {
  const profile = clusterProfile(i);
  confirmAccount(profile, i.region, i.clusterAccountId, confirmed, "verify-cluster");
  const n = resourceNames(i);
  const checks: Check[] = [
    checkOf("stack cluster-identity", exec("aws", aws(profile, i.region, ["cloudformation", "describe-stacks", "--stack-name", STACKS.identity])), /does not exist/),
    checkOf("pod role", exec("aws", aws(profile, i.region, ["iam", "get-role", "--role-name", n.podRole])), /NoSuchEntity/),
    checkOf("namespace", exec("kubectl", kubectl(i, ["get", "namespace", NAMESPACE])), /NotFound/),
  ];
  const associations = exec("aws", aws(profile, i.region, ["eks", "list-pod-identity-associations", "--cluster-name", i.clusterName, "--namespace", NAMESPACE]));
  const left = associations.status === 0 ? ((JSON.parse(associations.stdout) as { associations?: unknown[] }).associations ?? []).length : -1;
  checks.push({ what: "pod identity association", gone: left === 0, ...(left === 0 ? {} : { detail: left < 0 ? "lookup failed" : "still exists" }) });
  const tagged = taggedRemaining(profile, [i.region], checks);
  return { side: "cluster", at: new Date().toISOString(), checks, tagged_remaining: tagged, clean: isClean(checks, tagged) };
}

if (import.meta.main) {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      inputs: { type: "string" }, "run-id": { type: "string" }, profile: { type: "string", default: "standard" }, lanes: { type: "string" }, steps: { type: "string", default: "" },
      out: { type: "string" }, collected: { type: "string" }, "discard-results": { type: "boolean", default: false }, "confirm-account": { type: "string" },
    },
  });
  const command = positionals[0];
  if (!values.inputs) throw new Error("ledger-bench: --inputs <file> is required");
  if (insideRepo(values.inputs)) throw new Error("ledger-bench: keep the inputs file outside this (public) repository");
  const inputs = parseInputs(readFileSync(values.inputs, "utf8"));
  const runId = values["run-id"] ?? "";
  const lanes = (values.lanes ? values.lanes.split(",") : [...LANES]) as Lane[];
  const report = (v: Verification) => {
    console.log(JSON.stringify(v, null, 2));
    if (!v.clean) process.exit(1);
  };
  switch (command) {
    case "render": {
      const template = readFileSync(join(HERE, "job.template.yaml"), "utf8");
      console.log(readFileSync(join(HERE, "namespace.yaml"), "utf8"));
      for (const lane of lanes) console.log(`---\n${renderJob(template, inputs, { runId, lane, profile: values.profile!, steps: values.steps!, bundleSha256: "" })}`);
      break;
    }
    case "up-data": upData(inputs, values["confirm-account"]); break;
    case "outputs-data": console.log(JSON.stringify({ keys: outputsData(inputs) })); break;
    case "up-identity": upIdentity(inputs, values["confirm-account"]); break;
    case "up-namespace": upNamespace(inputs); break;
    case "run": run(inputs, runId, values.profile!, lanes, values.steps!); break;
    case "collect": collect(inputs, runId, values.out ?? ""); break;
    case "down-cluster": downCluster(inputs, values["confirm-account"], values.collected, values["discard-results"]!); break;
    case "down-data": downData(inputs, values["confirm-account"]); break;
    case "verify-data": report(verifyData(inputs, values["confirm-account"])); break;
    case "verify-cluster": report(verifyCluster(inputs, values["confirm-account"])); break;
    default:
      console.error("usage: ops.ts render|up-data|outputs-data|down-data|verify-data|up-identity|up-namespace|run|collect|down-cluster|verify-cluster --inputs <file> ...");
      process.exit(2);
  }
}
