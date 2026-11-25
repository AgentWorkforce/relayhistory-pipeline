/**
 * The live session pipeline function, read back for the deploy plan contract
 * (transcript-store.ts TranscriptStorePlanCapture.liveSnapshotText): its name, RevisionId and env,
 * and every event source mapping on it (UUID, source, function, state, settings), by the aws CLI,
 * read-only (lambda:GetFunctionConfiguration, lambda:ListAliases, lambda:ListVersionsByFunction,
 * lambda:ListEventSourceMappings). Held in memory only.
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

export type PipelineLiveErrorCode = "live_cli_failed" | "live_output_empty" | "live_parse_failed" | "live_incomplete" | "live_env_error" | "live_wrong_function" | "live_not_settled";

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
  state: PipelineLiveMappingState;
  batchSize: number;
  maximumBatchingWindowInSeconds: number;
  functionResponseTypes: string[];
  /** Absent: the mapping has no MaximumConcurrency (uncapped). */
  maximumConcurrency?: number;
}

export interface PipelineLive {
  schema: 3;
  function: { name: string; functionArn: string; role: string; revisionId: string; state: "Active"; lastUpdateStatus: "Successful"; env: Record<string, string> };
  /** Every alias and published version of the function (each is searched for mappings too). */
  aliases: string[];
  versions: string[];
  mappings: PipelineLiveMapping[];
}

/** The states a mapping can report; anything else is refused before it is used anywhere. */
export const MAPPING_STATES = ["Creating", "Enabling", "Enabled", "Disabling", "Disabled", "Updating", "Deleting"] as const;
export type PipelineLiveMappingState = (typeof MAPPING_STATES)[number];
/** Allow-lists for every external field the guard may name in a refusal (validated here, before any use). */
export const LIVE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SQS_ARN = /^arn:aws[a-z-]*:sqs:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]{1,80}$/;
const LAMBDA_ARN = /^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]{1,64}(?::[A-Za-z0-9_$-]{1,128})?$/;
const ROLE_ARN = /^arn:aws[a-z-]*:iam::[0-9]{12}:role\/[A-Za-z0-9_+=,.@/-]{1,512}$/;
const ALIAS = /^[A-Za-z0-9_-]{1,128}$/;
const VERSION = /^(?:\$LATEST|[0-9]{1,10})$/;
const RESPONSE_TYPE = /^[A-Za-z]{1,64}$/;
const ENV_KEY = /^[A-Za-z][A-Za-z0-9_]{0,255}$/;
/** Pages any one listing may take before the readback refuses rather than return a partial list. */
const MAX_PAGES = 100;

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

/** Every page of a NextMarker listing (`--no-paginate`, then `--marker`), to the end, or a refusal. */
function listAll(run: AwsRun, args: string[], field: string): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  let marker: string | undefined;
  for (let page = 0; page < MAX_PAGES; page++) {
    const body = call(run, [...args, "--no-paginate", ...(marker ? ["--marker", marker] : [])]) as Record<string, unknown> | undefined;
    if (body === undefined || body === null || typeof body !== "object" || !Array.isArray(body[field])) throw new PipelineLiveError("live_incomplete");
    out.push(...(body[field] as Record<string, unknown>[]));
    const next = body.NextMarker;
    if (next === undefined || next === null || next === "") return out;
    if (typeof next !== "string" || next === marker) throw new PipelineLiveError("live_incomplete");
    marker = next;
  }
  throw new PipelineLiveError("live_incomplete");
}

const isString = (v: unknown): v is string => typeof v === "string" && v.length > 0;

function mappingOf(m: Record<string, unknown>): PipelineLiveMapping {
  const scaling = m.ScalingConfig as { MaximumConcurrency?: unknown } | undefined;
  const max = scaling?.MaximumConcurrency;
  const types = m.FunctionResponseTypes ?? [];
  if (
    !isString(m.UUID) || !LIVE_UUID.test(m.UUID) ||
    !isString(m.EventSourceArn) || !SQS_ARN.test(m.EventSourceArn) ||
    !isString(m.FunctionArn) || !LAMBDA_ARN.test(m.FunctionArn) ||
    !(MAPPING_STATES as readonly unknown[]).includes(m.State) ||
    !Number.isInteger(m.BatchSize) ||
    !Number.isInteger(m.MaximumBatchingWindowInSeconds ?? 0) ||
    (max !== undefined && !Number.isInteger(max)) ||
    !(Array.isArray(types) && types.every((t) => typeof t === "string" && RESPONSE_TYPE.test(t)))
  )
    throw new PipelineLiveError("live_incomplete");
  return {
    uuid: m.UUID,
    eventSourceArn: m.EventSourceArn,
    functionArn: m.FunctionArn,
    state: m.State as PipelineLiveMappingState,
    batchSize: m.BatchSize as number,
    maximumBatchingWindowInSeconds: (m.MaximumBatchingWindowInSeconds as number | undefined) ?? 0,
    functionResponseTypes: types as string[],
    ...(max !== undefined ? { maximumConcurrency: max as number } : {}),
  };
}

/**
 * The live snapshot text, or undefined when the function does not exist yet (a first rollout).
 * Refused (fixed codes): a CLI failure, empty or unparseable output, a config without its exact
 * name and FunctionArn, Role, RevisionId or env Variables, a config not Active with LastUpdateStatus
 * Successful, an env the CLI could not read (Environment.Error), a non-string value or a malformed
 * key, and any listing that does not reach its end. Mappings are searched on the function, on every
 * alias and on every version (a name-filtered listing need not show an alias's mapping), each
 * validated against its allow-list, and deduplicated by UUID.
 */
export function readPipelineLive(run: AwsRun, stage: string, accountId: string, region: string): string | undefined {
  const fn = sessionPipelineNames(stage, accountId, region).function;
  const functionArn = `arn:aws:lambda:${region}:${accountId}:function:${fn}`;
  const config = call(run, ["lambda", "get-function-configuration", "--function-name", fn, "--region", region]) as
    | { FunctionName?: unknown; FunctionArn?: unknown; Role?: unknown; RevisionId?: unknown; State?: unknown; LastUpdateStatus?: unknown; Environment?: { Variables?: unknown; Error?: unknown } }
    | undefined;
  if (config === undefined) return undefined;
  if (config === null || typeof config !== "object") throw new PipelineLiveError("live_incomplete");
  if (config.FunctionName !== fn || config.FunctionArn !== functionArn) throw new PipelineLiveError("live_wrong_function");
  if (config.State !== "Active" || config.LastUpdateStatus !== "Successful") throw new PipelineLiveError("live_not_settled");
  if (!isString(config.RevisionId) || !/^[0-9a-f-]{1,64}$/.test(config.RevisionId)) throw new PipelineLiveError("live_incomplete");
  if (!isString(config.Role) || !ROLE_ARN.test(config.Role)) throw new PipelineLiveError("live_incomplete");
  if (config.Environment?.Error !== undefined) throw new PipelineLiveError("live_env_error");
  const vars = config.Environment?.Variables;
  if (vars === null || typeof vars !== "object" || Array.isArray(vars)) throw new PipelineLiveError("live_incomplete");
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(vars as Record<string, unknown>)) {
    if (typeof v !== "string" || !ENV_KEY.test(k)) throw new PipelineLiveError("live_incomplete");
    env[k] = v;
  }
  const aliases = listAll(run, ["lambda", "list-aliases", "--function-name", fn, "--region", region], "Aliases").map((a) => {
    if (!isString(a.Name) || !ALIAS.test(a.Name)) throw new PipelineLiveError("live_incomplete");
    return a.Name;
  });
  const versions = listAll(run, ["lambda", "list-versions-by-function", "--function-name", fn, "--region", region], "Versions").map((v) => {
    if (!isString(v.Version) || !VERSION.test(v.Version)) throw new PipelineLiveError("live_incomplete");
    return v.Version;
  });
  const targets = [fn, ...aliases.map((a) => `${functionArn}:${a}`), ...versions.filter((v) => v !== "$LATEST").map((v) => `${functionArn}:${v}`)];
  const byUuid = new Map<string, PipelineLiveMapping>();
  for (const target of targets)
    for (const m of listAll(run, ["lambda", "list-event-source-mappings", "--function-name", target, "--region", region], "EventSourceMappings")) {
      const mapping = mappingOf(m);
      const seen = byUuid.get(mapping.uuid);
      if (seen && JSON.stringify(seen) !== JSON.stringify(mapping)) throw new PipelineLiveError("live_incomplete");
      byUuid.set(mapping.uuid, mapping);
    }
  const live: PipelineLive = {
    schema: 3,
    function: { name: fn, functionArn, role: config.Role, revisionId: config.RevisionId, state: "Active", lastUpdateStatus: "Successful", env },
    aliases,
    versions,
    mappings: [...byUuid.values()].sort((a, b) => (a.uuid < b.uuid ? -1 : 1)),
  };
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
    functionArn: live.function.functionArn,
    role: live.function.role,
    revisionId: live.function.revisionId,
    aliases: [...live.aliases].sort(),
    versions: [...live.versions].sort(),
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
