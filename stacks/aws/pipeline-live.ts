/**
 * The live session pipeline function, read back for the deploy plan contract
 * (transcript-store.ts TranscriptStorePlanCapture.liveSnapshotText): its name, RevisionId and env,
 * and every event source mapping on it (UUID, source, function, state, settings), by the aws CLI,
 * read-only (lambda:GetFunctionConfiguration, lambda:ListEventSourceMappings). Held in memory only.
 *
 * Alchemy writes the function's whole env on every update and plans against its state, not the
 * live function, so a variable set by hand is visible only here.
 *
 * Failures carry fixed codes only (PipelineLiveError): no CLI output, no exception text and no env
 * value is ever put in a message, because the env is in that output.
 */
import { createHash } from "node:crypto";
import { sessionPipelineNames } from "./session-pipeline.ts";

/** One CLI call: never throws with its output; the caller of readPipelineLive sees only fixed codes. */
export type AwsRun = (cmd: string, args: string[]) => { status: number | null; stdout: string; stderr: string };

export type PipelineLiveErrorCode = "live_cli_failed" | "live_output_empty" | "live_parse_failed" | "live_incomplete" | "live_env_error" | "live_wrong_function";

export class PipelineLiveError extends Error {
  constructor(readonly code: PipelineLiveErrorCode) {
    super(`96bulls: the live session pipeline readback failed (${code}). Nothing was changed.`);
    this.name = "PipelineLiveError";
  }
}

export interface PipelineLiveMapping {
  uuid: string;
  eventSourceArn: string;
  functionArn: string;
  state: string;
  batchSize: number;
  maximumBatchingWindowInSeconds: number;
  functionResponseTypes: string[];
  /** Absent: the mapping has no MaximumConcurrency (uncapped). */
  maximumConcurrency?: number;
}

export interface PipelineLive {
  schema: 2;
  function: { name: string; revisionId: string; env: Record<string, string> };
  mappings: PipelineLiveMapping[];
}

const call = (run: AwsRun, args: string[]): unknown => {
  let r: ReturnType<AwsRun>;
  try {
    r = run("aws", [...args, "--output", "json"]);
  } catch {
    throw new PipelineLiveError("live_cli_failed");
  }
  if (r.status !== 0) {
    // Only the error class is looked at, never echoed.
    if (/\bResourceNotFoundException\b/.test(r.stderr ?? "")) return undefined;
    throw new PipelineLiveError("live_cli_failed");
  }
  if (!r.stdout || !r.stdout.trim()) throw new PipelineLiveError("live_output_empty");
  try {
    return JSON.parse(r.stdout);
  } catch {
    throw new PipelineLiveError("live_parse_failed");
  }
};

const isString = (v: unknown): v is string => typeof v === "string" && v.length > 0;

/**
 * The live snapshot text, or undefined when the function does not exist yet (a first rollout).
 * Refused (fixed codes): a CLI failure, empty or unparseable output, a config without its name,
 * RevisionId or env Variables, an env the CLI could not read (Environment.Error), a non-string env
 * value, another function's config, and a mapping missing any of its fields.
 */
export function readPipelineLive(run: AwsRun, stage: string, accountId: string, region: string): string | undefined {
  const fn = sessionPipelineNames(stage, accountId, region).function;
  const config = call(run, ["lambda", "get-function-configuration", "--function-name", fn, "--region", region]) as
    | { FunctionName?: unknown; RevisionId?: unknown; Environment?: { Variables?: unknown; Error?: unknown } }
    | undefined;
  if (config === undefined) return undefined;
  if (config === null || typeof config !== "object") throw new PipelineLiveError("live_incomplete");
  if (config.FunctionName !== fn) throw new PipelineLiveError("live_wrong_function");
  if (!isString(config.RevisionId)) throw new PipelineLiveError("live_incomplete");
  if (config.Environment?.Error !== undefined) throw new PipelineLiveError("live_env_error");
  const vars = config.Environment?.Variables;
  if (vars === null || typeof vars !== "object" || Array.isArray(vars)) throw new PipelineLiveError("live_incomplete");
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(vars as Record<string, unknown>)) {
    if (typeof v !== "string") throw new PipelineLiveError("live_incomplete");
    env[k] = v;
  }
  const listed = call(run, ["lambda", "list-event-source-mappings", "--function-name", fn, "--region", region]) as { EventSourceMappings?: unknown } | undefined;
  if (listed === undefined || !Array.isArray(listed.EventSourceMappings)) throw new PipelineLiveError("live_incomplete");
  const mappings = listed.EventSourceMappings.map((m: Record<string, unknown>): PipelineLiveMapping => {
    const scaling = m.ScalingConfig as { MaximumConcurrency?: unknown } | undefined;
    const max = scaling?.MaximumConcurrency;
    if (
      !isString(m.UUID) ||
      !isString(m.EventSourceArn) ||
      !isString(m.FunctionArn) ||
      !isString(m.State) ||
      !Number.isInteger(m.BatchSize) ||
      !Number.isInteger(m.MaximumBatchingWindowInSeconds ?? 0) ||
      (max !== undefined && !Number.isInteger(max)) ||
      (m.FunctionResponseTypes !== undefined && !(Array.isArray(m.FunctionResponseTypes) && m.FunctionResponseTypes.every(isString)))
    )
      throw new PipelineLiveError("live_incomplete");
    return {
      uuid: m.UUID,
      eventSourceArn: m.EventSourceArn,
      functionArn: m.FunctionArn,
      state: m.State,
      batchSize: m.BatchSize as number,
      maximumBatchingWindowInSeconds: (m.MaximumBatchingWindowInSeconds as number | undefined) ?? 0,
      functionResponseTypes: (m.FunctionResponseTypes as string[] | undefined) ?? [],
      ...(max !== undefined ? { maximumConcurrency: max as number } : {}),
    };
  });
  const live: PipelineLive = { schema: 2, function: { name: fn, revisionId: config.RevisionId, env }, mappings };
  return JSON.stringify(live);
}

/**
 * What the capture records of the live function (provenance `pipelineLive`) and the deploy requires
 * unchanged: the function's RevisionId, a digest of its env (never the values), and each mapping's
 * UUID, source, state and settings. Any change between capture and deploy (a hand edit, another
 * apply) refuses the deploy.
 */
export function pipelineLiveFingerprint(live: PipelineLive) {
  const env = Object.keys(live.function.env)
    .sort()
    .map((k) => [k, live.function.env[k]]);
  return {
    function: live.function.name,
    revisionId: live.function.revisionId,
    envSha256: createHash("sha256").update(JSON.stringify(env)).digest("hex"),
    mappings: [...live.mappings]
      .sort((a, b) => (a.uuid < b.uuid ? -1 : a.uuid > b.uuid ? 1 : 0))
      .map((m) => ({ uuid: m.uuid, eventSourceArn: m.eventSourceArn, functionArn: m.functionArn, state: m.state, batchSize: m.batchSize, maximumBatchingWindowInSeconds: m.maximumBatchingWindowInSeconds, functionResponseTypes: m.functionResponseTypes, maximumConcurrency: m.maximumConcurrency ?? null })),
  };
}

/** spawnSync as an AwsRun: the child's output is returned, never thrown. */
export const spawnAwsRun =
  (spawn: (cmd: string, args: string[]) => { status: number | null; stdout: string | Buffer; stderr: string | Buffer }): AwsRun =>
  (cmd, args) => {
    const r = spawn(cmd, args);
    return { status: r.status, stdout: String(r.stdout ?? ""), stderr: String(r.stderr ?? "") };
  };
