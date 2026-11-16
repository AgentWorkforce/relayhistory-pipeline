/**
 * Hosted-ledger benchmark client (docs/design/hosted-ledger-benchmark-plan.md, decision 0008).
 *
 *   bun benchmarks/ledger-bench/main.ts plan            # the matrix and the cost bound; touches nothing
 *   bun benchmarks/ledger-bench/main.ts run             # one lane, from LEDGER_BENCH_CONFIG (in the Job)
 *
 * `run` writes one JSON line per cell to stdout (progress goes to stderr) and, when R2 is configured, the
 * same lines to `bench/<runId>/results/<lane>.jsonl` there. It only ever writes to resources whose names
 * carry the `ledger-bench` marker (config.ts), under the key prefix `bench/<runId>/<lane>/`.
 */
import { spawn } from "node:child_process";
import { objectKey } from "../../packages/object-store/src/key.ts";
import { assumeRole, baseCredentials, Dynamo, expressStore, meteredFetch, RequestMeter, standardStore, type CredentialProvider } from "./aws.ts";
import { LANE_TARGETS, LANES, parseConfig, PROFILES, type RunConfig, type TargetId } from "./config.ts";
import { readFileSync } from "node:fs";
import type { Cell, Ctx } from "./context.ts";
import { costBound } from "./cost.ts";
import { errorClass } from "./load.ts";
import { step3Log, step3P13, step3Race, step4, step5 } from "./steps-protocols.ts";
import { ambiguousChild, endpointRoundTrips, step1, step2, step6, type SpawnAmbiguousChild } from "./steps-s3.ts";

type BucketId = Exclude<TargetId, "R6">;

function credentialsFor(config: RunConfig): CredentialProvider {
  const base = baseCredentials();
  return config.assumeRoleArn ? assumeRole(base, { roleArn: config.assumeRoleArn, region: config.stsRegion, sessionName: `ledger-bench-${config.lane}` }) : base;
}

function buildContext(config: RunConfig, credentials: CredentialProvider, meter: RequestMeter, emit: (cell: Cell) => void): Ctx {
  const ctx: Ctx = { profile: config.profile, meter, root: `bench/${config.runId}/${config.lane}`, stores: {}, sseS3: {}, expressStats: {}, emit, log: (m) => console.error(`[ledger-bench ${config.lane}] ${m}`) };
  for (const id of ["R1", "R2", "R3"] as const) {
    const target = config.targets[id];
    if (!target) continue;
    ctx.stores[id] = standardStore(target, credentials, meteredFetch(meter, id));
    if (id !== "R3") ctx.sseS3[id] = standardStore(target, credentials, meteredFetch(meter, `${id}-sse-s3`), "AES256");
  }
  for (const id of ["R4", "R5"] as const) {
    const target = config.targets[id];
    if (!target) continue;
    const express = expressStore(target, credentials, meteredFetch(meter, id));
    ctx.stores[id] = express.store;
    ctx.expressStats[id] = express.stats;
  }
  if (config.targets.R6) ctx.dynamo = new Dynamo(config.targets.R6, credentials, meteredFetch(meter, "R6"));
  return ctx;
}

/** Requests one ambiguous-commit child may make. The parent takes them from its own budget when it starts the child. */
const CHILD_REQUESTS = 500;

/** CPU throttling of this container (cgroup v2), when readable: a throttled client bounds the fast cells itself. */
function cpuStat(): Record<string, number> | null {
  try {
    const out: Record<string, number> = {};
    for (const line of readFileSync("/sys/fs/cgroup/cpu.stat", "utf8").split("\n")) {
      const [name, value] = line.split(" ");
      if (name && ["nr_periods", "nr_throttled", "throttled_usec", "usage_usec"].includes(name)) out[name] = Number(value);
    }
    return out;
  } catch {
    return null;
  }
}

/** Starts this same program as the ambiguous-commit child, lets it write for a moment, and SIGKILLs it mid-PUT. */
const spawnAmbiguousChild: SpawnAmbiguousChild = (target, prefix, runTag) =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [process.argv[1]!, "ambiguous-child"], {
      env: { ...process.env, LEDGER_BENCH_CHILD: JSON.stringify({ target, prefix, runTag }) },
      stdio: ["ignore", "pipe", "inherit"],
    });
    const started: number[] = [];
    const done: number[] = [];
    let buffer = "";
    let armed = false;
    // A child that never gets to its third write (a crash, a denied request) is ended and reported as an empty round.
    const giveUp = setTimeout(() => child.kill("SIGKILL"), 60_000);
    child.stdout.on("data", (chunk: Buffer) => {
      buffer += chunk.toString("utf8");
      for (;;) {
        const nl = buffer.indexOf("\n");
        if (nl < 0) break;
        const [word, n] = buffer.slice(0, nl).split(" ");
        buffer = buffer.slice(nl + 1);
        if (word === "start") started.push(Number(n));
        if (word === "done") done.push(Number(n));
        // After a few whole writes, kill at a random point inside a later one.
        if (word === "start" && started.length === 3 && !armed) {
          armed = true;
          setTimeout(() => child.kill("SIGKILL"), 5 + Math.random() * 400);
        }
      }
    });
    child.on("error", reject);
    child.on("close", () => {
      clearTimeout(giveUp);
      resolve({ started, done });
    });
  });

async function runChild(config: RunConfig): Promise<void> {
  const { target, prefix, runTag } = JSON.parse(process.env.LEDGER_BENCH_CHILD ?? "{}") as { target: BucketId; prefix: string; runTag: string };
  if (!prefix?.startsWith(`bench/${config.runId}/`)) throw new Error("ledger-bench: the child writes only under the run's prefix");
  // The parent reserved exactly this many requests in its own budget before starting the child.
  const ctx = buildContext(config, credentialsFor(config), new RequestMeter(CHILD_REQUESTS), () => {});
  const store = ctx.stores[target];
  if (!store) throw new Error("ledger-bench: the child's target is not configured");
  await ambiguousChild(store, prefix, runTag, (line) => process.stdout.write(`${line}\n`));
}

async function runLane(config: RunConfig): Promise<boolean> {
  for (const id of LANE_TARGETS[config.lane]) if (!config.targets[id]) throw new Error(`ledger-bench: lane ${config.lane} needs target ${id}`);
  const meter = new RequestMeter(config.profile.requestBudgetPerLane, config.profile.byteBudgetPerLane);
  const credentials = credentialsFor(config);
  const lines: string[] = [];
  const write = (record: Record<string, unknown>) => {
    const line = JSON.stringify({ ledger_bench: 1, run: config.runId, lane: config.lane, at: new Date().toISOString(), ...record });
    lines.push(line);
    console.log(line);
  };
  const ctx = buildContext(config, credentials, meter, (cell) => write({ ...cell }));
  const wanted = (step: number) => !config.steps || config.steps.includes(step);
  const { targets } = config;
  // The ambiguous-commit children are separate processes; their requests come out of this lane's budget.
  const spawnChild: SpawnAmbiguousChild = (target, prefix, runTag) => {
    meter.take(CHILD_REQUESTS);
    return spawnAmbiguousChild(target, prefix, runTag);
  };
  write({ event: "start", profile_name: config.profileName, bundle_sha256: process.env.LEDGER_BENCH_BUNDLE_SHA256 ?? null, bun_max_http_requests: process.env.BUN_CONFIG_MAX_HTTP_REQUESTS ?? "default", profile: config.profile, bun: process.versions.bun ?? null, node: process.versions.node, steps: config.steps ?? "all", targets: Object.keys(targets) });

  let failure: string | undefined;
  let stagesFailed = 0;
  const stage = async (step: number, name: string, work: () => Promise<void>) => {
    if (!wanted(step) || failure) return;
    if (meter.exhausted) {
      write({ event: "skipped", step, stage: name, reason: "request budget spent" });
      return;
    }
    ctx.log(`stage ${name} (requests so far ${meter.used})`);
    try {
      await work();
    } catch (error) {
      // A failed stage is recorded and the lane goes on: one unsupported operation must not lose the rest.
      // Only the closed error class is written: a store's message can quote ARNs and bucket names.
      stagesFailed++;
      write({ event: "stage_failed", step, stage: name, error: errorClass(error) });
      if (meter.exhausted) failure = "request budget spent";
    }
  };

  switch (config.lane) {
    case "r1":
      await stage(1, "endpoints R1", () => endpointRoundTrips(ctx, "R1", [{ service: "s3", region: targets.R1!.region }, { service: "kms", region: targets.R1!.region }], meteredFetch(meter, "endpoint")));
      await stage(1, "step1 R1", () => step1(ctx, "R1"));
      await stage(3, "p13 R1", () => step3P13(ctx, "R1"));
      break;
    case "r2":
      await stage(1, "endpoints R2", () => endpointRoundTrips(ctx, "R2", ["s3", "kms", "dynamodb", "sts"].map((service) => ({ service, region: targets.R2!.region })), meteredFetch(meter, "endpoint")));
      await stage(1, "step1 R2", () => step1(ctx, "R2"));
      await stage(2, "step2 R2", () => step2(ctx, "R2", spawnChild));
      await stage(3, "p13 R2", () => step3P13(ctx, "R2"));
      await stage(3, "plog R2", () => step3Log(ctx, "R2", "plog"));
      await stage(3, "racing writers R2", () => step3Race(ctx, "R2"));
      await stage(3, "pddb R2+R6", () => step3Log(ctx, "R2", "pddb"));
      await stage(6, "pool ramp R2", () => step6(ctx, "R2"));
      break;
    case "r3":
      await stage(1, "step1 R3", () => step1(ctx, "R3"));
      await stage(2, "step2 R3", () => step2(ctx, "R3", spawnChild));
      await stage(3, "p13 R3", () => step3P13(ctx, "R3"));
      await stage(3, "plog R3", () => step3Log(ctx, "R3", "plog"));
      await stage(3, "racing writers R3", () => step3Race(ctx, "R3"));
      break;
    case "express":
      await stage(1, "step1 R4", () => step1(ctx, "R4"));
      await stage(2, "step2 R4", () => step2(ctx, "R4", spawnChild));
      await stage(2, "step2 R5", () => step2(ctx, "R5"));
      await stage(3, "plog R4", () => step3Log(ctx, "R4", "plog"));
      await stage(3, "racing writers R4", () => step3Race(ctx, "R4"));
      await stage(4, "step4 express", () => step4(ctx, credentials, meteredFetch(meter, "R4-session"), { R4: targets.R4!, R5: targets.R5! }, config.foreignKmsKeyArn));
      break;
    case "dynamo":
      await stage(5, "step5 R6", () => step5(ctx));
      break;
  }

  const complete = failure === undefined && !meter.exhausted && stagesFailed === 0;
  write({ event: "end", complete, stages_failed: stagesFailed, requests: meter.used, request_budget: meter.limit, bytes: meter.bytes, byte_budget: meter.byteLimit, statuses: meter.statuses.toJSON(), express_sessions: ctx.expressStats, cpu_stat: cpuStat(), max_rss_kib: Math.round(process.resourceUsage().maxRSS) });
  // A second copy of the results next to the data, in case the pod's log is lost. Outside the meter (one
  // request), so it is written even when the budget is spent. Best effort.
  if (targets.R2) {
    await standardStore(targets.R2, credentials, fetch)
      .put(objectKey(`bench/${config.runId}/results/${config.lane}.jsonl`), new TextEncoder().encode(`${lines.join("\n")}\n`), { contentType: "application/x-ndjson" })
      .catch((error) => ctx.log(`results copy failed: ${errorClass(error)}`));
  }
  return complete;
}

function printPlan(): void {
  const profile = PROFILES[process.env.LEDGER_BENCH_PROFILE ?? "standard"] ?? PROFILES.standard!;
  const repeats = Number(process.env.LEDGER_BENCH_REPEATS ?? 3);
  console.log(JSON.stringify({ profile, lanes: LANES, lane_targets: LANE_TARGETS, repeats, cost_bound: costBound(profile, repeats) }, null, 2));
}

const mode = process.argv[2];
if (mode === "plan") printPlan();
else if (mode === "run" || mode === "ambiguous-child") {
  const config = parseConfig(process.env.LEDGER_BENCH_CONFIG ?? "");
  if (mode === "run") process.exit((await runLane(config)) ? 0 : 1);
  await runChild(config);
  process.exit(0);
} else {
  console.error("usage: main.ts plan | run");
  process.exit(2);
}
