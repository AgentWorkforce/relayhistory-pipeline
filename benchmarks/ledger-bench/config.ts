/**
 * The benchmark's run configuration. Resource names never live in this (public) repository: the stack
 * writes them to the Job's ConfigMap at deploy time and the client reads them from `LEDGER_BENCH_CONFIG`.
 * Results name targets only by the plan's ids (R1..R6).
 */
import type { DynamoTarget, ExpressTarget, S3Target } from "./aws.ts";

/** R1 us-east-1 (Bucket Keys off), R2 us-west-2 (off), R3 us-west-2 (on), R4/R5 directory buckets, R6 DynamoDB. */
export const TARGET_IDS = ["R1", "R2", "R3", "R4", "R5", "R6"] as const;
export type TargetId = (typeof TARGET_IDS)[number];

export interface Targets {
  R1?: S3Target;
  R2?: S3Target;
  R3?: S3Target;
  R4?: ExpressTarget;
  R5?: ExpressTarget;
  R6?: DynamoTarget;
}

/** Each lane is one pod of the Job; lanes run side by side, a lane's cells one after another. */
export const LANES = ["r1", "r2", "r3", "express", "dynamo"] as const;
export type Lane = (typeof LANES)[number];

export interface Profile {
  /** Steady-state seconds per latency cell (the plan's figure is 300). */
  cellSeconds: number;
  warmupSeconds: number;
  /** Seconds per step 3 protocol cell. */
  protocolSeconds: number;
  concurrencies: number[];
  /** Rounds per racing-writer correctness check. */
  raceRounds: number;
  /** The Express session-refresh soak; must exceed the 300 s session lifetime to cross a refresh. Paced: it needs time, not volume. */
  refreshSoakSeconds: number;
  /** In-flight levels for the step 6 pool ramp. */
  poolRamp: number[];
  /** A cell ends early once it has started this many operations; enough for a p99, and it bounds fast cells. */
  maxOpsPerCell: number;
  /** Hard cap on HTTP requests per lane: the cost bound (cost.ts). */
  requestBudgetPerLane: number;
  /** Hard cap on bytes sent and received per lane: bounds what is priced per byte (cost.ts). */
  byteBudgetPerLane: number;
  /** A write that was in flight at the kill and is absent is read again after this long: it may land late. */
  ambiguousRecheckSeconds: number;
}

/**
 * The profile a real run uses. Cells are 60 s rather than the plan's 300 s so that one lane finishes inside
 * the Job's 4 h deadline (the full matrix at 300 s is about 9 h per lane); the three repeats at different
 * times of day are three Job runs. `plan` mode restores the plan's 300 s for a single lane when asked.
 */
export const STANDARD: Profile = {
  cellSeconds: 60,
  warmupSeconds: 10,
  protocolSeconds: 120,
  concurrencies: [1, 8, 16, 64],
  raceRounds: 200,
  refreshSoakSeconds: 660,
  poolRamp: [64, 128, 256, 384, 512],
  maxOpsPerCell: 40_000,
  requestBudgetPerLane: 2_200_000,
  byteBudgetPerLane: 48 * 1024 ** 3,
  ambiguousRecheckSeconds: 45,
};

/** A seconds-long pass over every cell, to prove the harness and the permissions before the real run. */
export const SMOKE: Profile = {
  cellSeconds: 3,
  warmupSeconds: 1,
  protocolSeconds: 5,
  concurrencies: [1, 8],
  raceRounds: 10,
  refreshSoakSeconds: 0,
  poolRamp: [64],
  maxOpsPerCell: 2_000,
  requestBudgetPerLane: 100_000,
  byteBudgetPerLane: 8 * 1024 ** 3,
  ambiguousRecheckSeconds: 5,
};

export const PROFILES: Record<string, Profile> = { standard: STANDARD, smoke: SMOKE };

export interface RunConfig {
  /** Names this run's key prefix and results; `[a-z0-9-]`, set by the operator per Job run. */
  runId: string;
  lane: Lane;
  profile: Profile;
  targets: Targets;
  /** The bench role in the throwaway account, assumed from the pod's role. Absent for a same-account or local run. */
  assumeRoleArn?: string;
  /** A bench key that is not any bucket's default key: the single-key probe names it on a directory-bucket put. */
  foreignKmsKeyArn?: string;
  profileName: string;
  /** Region of the STS endpoint used for the hop (the cluster's region). */
  stsRegion: string;
  /** Step filter (`1`..`6`), for re-running one step. Default: every step of the lane. */
  steps?: number[];
}

const KMS_KEY_ARN = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const REGION = /^[a-z]{2}(?:-[a-z]+)+-\d$/;

function s3Target(id: string, v: unknown): S3Target {
  const o = v as Partial<S3Target> | undefined;
  if (!o || typeof o.bucket !== "string" || !BUCKET.test(o.bucket)) throw new Error(`ledger-bench: ${id}.bucket is not a bucket name`);
  if (typeof o.region !== "string" || !REGION.test(o.region)) throw new Error(`ledger-bench: ${id}.region is not a region`);
  if (o.kmsKeyArn !== undefined && !KMS_KEY_ARN.test(o.kmsKeyArn)) throw new Error(`ledger-bench: ${id}.kmsKeyArn is not a KMS key ARN`);
  if (o.endpoint !== undefined && !localEndpoint(o.endpoint)) throw new Error(`ledger-bench: ${id}.endpoint may only be a loopback emulator`);
  return { bucket: o.bucket, region: o.region, ...(o.kmsKeyArn ? { kmsKeyArn: o.kmsKeyArn } : {}), ...(o.endpoint ? { endpoint: o.endpoint } : {}) };
}

/** An endpoint override is for a local emulator only, so a typo can never send the run somewhere else. */
export function localEndpoint(endpoint: string): boolean {
  try {
    const u = new URL(endpoint);
    return u.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(u.hostname);
  } catch {
    return false;
  }
}

/**
 * A target the benchmark may write to must look like one of its own: the stack names every bucket and the
 * table with this marker. The pod's role cannot reach anything else anyway; this refuses a mistyped config
 * before the first request.
 */
export const NAME_MARKER = "ledger-bench";

export function parseConfig(json: string, env: NodeJS.ProcessEnv = process.env): RunConfig {
  let raw: Record<string, unknown>;
  try {
    raw = JSON.parse(json) as Record<string, unknown>;
  } catch {
    throw new Error("ledger-bench: LEDGER_BENCH_CONFIG is not JSON");
  }
  const lane = (env.LEDGER_BENCH_LANE ?? raw.lane) as Lane;
  if (!LANES.includes(lane)) throw new Error(`ledger-bench: lane must be one of ${LANES.join(", ")}`);
  const runId = String(env.LEDGER_BENCH_RUN_ID ?? raw.runId ?? "");
  if (!/^[a-z0-9][a-z0-9-]{2,40}$/.test(runId)) throw new Error("ledger-bench: runId must be 3-41 characters of [a-z0-9-]");
  const profileName = String(env.LEDGER_BENCH_PROFILE ?? raw.profile ?? "standard");
  const profile = Object.hasOwn(PROFILES, profileName) ? PROFILES[profileName] : undefined;
  if (!profile) throw new Error(`ledger-bench: unknown profile ${profileName}`);
  const t = (raw.targets ?? {}) as Record<string, unknown>;
  const targets: Targets = {};
  for (const id of ["R1", "R2", "R3"] as const) if (t[id] !== undefined) targets[id] = s3Target(id, t[id]);
  for (const id of ["R4", "R5"] as const) {
    if (t[id] === undefined) continue;
    const o = t[id] as Partial<ExpressTarget>;
    if (typeof o.bucket !== "string" || !/--x-s3$/.test(o.bucket)) throw new Error(`ledger-bench: ${id}.bucket is not a directory bucket name`);
    if (typeof o.region !== "string" || !REGION.test(o.region)) throw new Error(`ledger-bench: ${id}.region is not a region`);
    targets[id] = { bucket: o.bucket, region: o.region };
  }
  if (t.R6 !== undefined) {
    const o = t.R6 as Partial<DynamoTarget>;
    if (typeof o.table !== "string" || !/^[A-Za-z0-9_.-]{3,255}$/.test(o.table)) throw new Error("ledger-bench: R6.table is not a table name");
    if (typeof o.region !== "string" || !REGION.test(o.region)) throw new Error("ledger-bench: R6.region is not a region");
    if (o.endpoint !== undefined && !localEndpoint(o.endpoint)) throw new Error("ledger-bench: R6.endpoint may only be a loopback emulator");
    targets.R6 = { table: o.table, region: o.region, ...(o.endpoint ? { endpoint: o.endpoint } : {}) };
  }
  for (const [id, target] of Object.entries(targets)) {
    const name = "bucket" in target ? target.bucket : target.table;
    if (!name.replaceAll("_", "-").includes(NAME_MARKER)) throw new Error(`ledger-bench: ${id} is not a ${NAME_MARKER} resource; refusing to write to it`);
  }
  const assumeRoleArn = raw.assumeRoleArn === undefined ? undefined : String(raw.assumeRoleArn);
  const foreignKmsKeyArn = raw.foreignKmsKeyArn === undefined ? undefined : String(raw.foreignKmsKeyArn);
  if (foreignKmsKeyArn !== undefined && !KMS_KEY_ARN.test(foreignKmsKeyArn)) throw new Error("ledger-bench: foreignKmsKeyArn is not a KMS key ARN");
  if (foreignKmsKeyArn !== undefined && Object.values(targets).some((t) => "kmsKeyArn" in t && t.kmsKeyArn === foreignKmsKeyArn)) throw new Error("ledger-bench: foreignKmsKeyArn must not be a bucket's own key");
  const stsRegion = String(raw.stsRegion ?? "us-west-2");
  if (!REGION.test(stsRegion)) throw new Error("ledger-bench: stsRegion is not a region");
  const stepsRaw = env.LEDGER_BENCH_STEPS ?? (Array.isArray(raw.steps) ? raw.steps.join(",") : undefined);
  const steps = stepsRaw ? String(stepsRaw).split(",").map((s) => Number(s.trim())) : undefined;
  if (steps && steps.some((s) => !Number.isInteger(s) || s < 1 || s > 6)) throw new Error("ledger-bench: steps are 1..6");
  return { runId, lane, profile, profileName, targets, stsRegion, ...(assumeRoleArn ? { assumeRoleArn } : {}), ...(foreignKmsKeyArn ? { foreignKmsKeyArn } : {}), ...(steps ? { steps } : {}) };
}

/** The targets a lane needs; a lane refuses to start without them. */
export const LANE_TARGETS: Record<Lane, TargetId[]> = {
  r1: ["R1"],
  r2: ["R2", "R6"],
  r3: ["R3"],
  express: ["R4", "R5", "R2"],
  dynamo: ["R6"],
};
