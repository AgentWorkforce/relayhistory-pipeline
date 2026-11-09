/**
 * Renders wrangler.deploy.jsonc for .github/workflows/deploy-session-read-worker.yml.
 *
 * owner_buffered=off (the default, and every dispatch without inputs): exactly the old
 * `sed` substitution of the Hyperdrive placeholder, byte for byte. No service binding, no
 * owner-buffered var: the deployed Worker is the one deployed before this script existed.
 *
 * owner_buffered=on (Option C canary, src/session-read-api/owner-buffered.ts): the same
 * config plus
 *   - services: HISTORY_CONTROL_HEAD -> relayhistory-api, entrypoint SessionReadPrivacyControls
 *     (relayhistory-cloud's named entrypoint, control-head contract v5; pinned here, never an
 *     input, so no dispatch can point the privacy authority at another Worker);
 *   - vars: RELAYHISTORY_READ_OWNER_BUFFERED=true, READ_API_OWNER_BUFFERED_WORKSPACES (the
 *     canary allowlist: explicit ids, each also on READ_API_DIRECTORY_WORKSPACES, or the
 *     all-workspaces mode below), INGEST_S3_BUCKET and INGEST_S3_REGION (from GitHub
 *     environment variables).
 *
 * All-workspaces mode: owner_buffered_workspaces is exactly "*" and SESSION_READ_DIRECTORY_WORKSPACES
 * is itself exactly "*"; renders READ_API_OWNER_BUFFERED_WORKSPACES="*". Refused: "*" mixed with
 * ids, "*" while the directory allowlist is an explicit list (or unset), "*" with owner_buffered
 * off. The mode only widens the workspace allowlist; every buffered response still passes the
 * per-user privacy gate (the caller's own G2 parity row, rule 4, in
 * sessions.owner_buffered_read_scope, and the control-head authority gate).
 * The S3 credentials are the Worker's existing PIPELINE_RAW_S3_ACCESS_KEY_ID /
 * _SECRET_ACCESS_KEY secrets (wrangler secret put); the workflow checks they exist by name.
 *
 * buffered_search_gate=on (src/session-read-api/buffered-search-gate.ts, PR #338): the per-hit
 * privacy gate for buffered-origin search rows. Adds vars READ_API_BUFFERED_SEARCH_GATE=on (the
 * Worker enables the gate only on exactly "on") and the same pinned HISTORY_CONTROL_HEAD binding
 * the gate reads; independent of owner_buffered (no workspace list, no S3). With owner_buffered=on
 * as well the binding is rendered once. buffered_search_gate=off (the default, empty = off) changes
 * nothing: every render is byte-identical to the render before this input existed. Refused: any
 * value other than off/on, and a template that already declares services, env or the gate var.
 *
 * owner_buffered_events=on (owner-buffered session events, src/session-read-api/owner-buffered.ts
 * readOwnerBufferedEvents): valid only with owner_buffered=on; adds vars
 * READ_API_OWNER_BUFFERED_EVENTS=on (the Worker enables it only on exactly "on") to the
 * owner_buffered=on render. owner_buffered_events=off (the default, empty = off) changes nothing:
 * every render is byte-identical to the render before this input existed. Refused: any value other
 * than off/on, on with owner_buffered off, and a template that already declares the var (in every
 * mode and for every owner_buffered_events value, including the default and gate-only renders).
 *
 * owner_buffered_stage_concurrency=2..32 and owner_buffered_phase_log=on (owner-buffered events
 * read latency): valid only with owner_buffered_events=on (they act on that read only); add vars
 * READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY=<n> (at most n stage GetObjects in flight) and
 * READ_API_OWNER_BUFFERED_PHASE_LOG=1 (per-phase *_ms fields on the request log line).
 * off (the default, empty = off) changes nothing: every render is byte-identical to the render
 * before these inputs existed. Refused: any other value, either set without
 * owner_buffered_events=on, and a template that already declares either var (in every mode).
 *
 * agent_match=on (agent-name prefix search, docs/agent-name-search-contract.md, migration
 * 20261105050020): adds vars READ_API_AGENT_MATCH="true" (the Worker enables it only on exactly
 * "true") to whichever render the other inputs select; no binding, no workspace list. Independent
 * of every other input. agent_match=off (the default, empty = off) changes nothing: every render is
 * byte-identical to the render before this input existed. Refused: any value other than off/on,
 * and a template that already declares the var (in every mode and for every agent_match value).
 * Turn it on only after 20261105050020 is applied to the read database: before that a prefix
 * request is 501 agent_match_not_migrated.
 *
 * The binding is production-scoped by living only in this production deploy's on-render, not in
 * wrangler.jsonc: a wrangler `env` block would rename the script (legacy_env appends -<env>) and
 * does not inherit hyperdrive/vars/services, so it would either deploy a second Worker or
 * duplicate every binding; and a top-level binding would change the default deploy.
 *
 * CLI: reads wrangler.jsonc and the env below, writes wrangler.deploy.jsonc, prints no value.
 *   HYPERDRIVE_ID, OWNER_BUFFERED (off|on; empty = off), OWNER_BUFFERED_WORKSPACES,
 *   OWNER_BUFFERED_S3_BUCKET, OWNER_BUFFERED_S3_REGION, DIRECTORY_WORKSPACES,
 *   BUFFERED_SEARCH_GATE (off|on; empty = off), OWNER_BUFFERED_EVENTS (off|on; empty = off),
 *   OWNER_BUFFERED_STAGE_CONCURRENCY (off|2..32; empty = off), OWNER_BUFFERED_PHASE_LOG (off|on; empty = off),
 *   AGENT_MATCH (off|on; empty = off).
 */
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";

export const HYPERDRIVE_PLACEHOLDER = '"id": "00000000000000000000000000000000"';

/** The privacy authority binding the owner-buffered gate reads (owner-buffered.ts serviceBindingControlHead). */
export const CONTROL_HEAD_BINDING = Object.freeze({
  binding: "HISTORY_CONTROL_HEAD",
  service: "relayhistory-api",
  entrypoint: "SessionReadPrivacyControls",
});

/** The buffered search gate flag (#338 index.ts Env; parseBufferedSearchGate accepts only "on"). */
export const BUFFERED_SEARCH_GATE_VAR = "READ_API_BUFFERED_SEARCH_GATE";

/** The owner-buffered events flag (index.ts Env; ownerBufferedEventsEnabled accepts only "on"). */
export const OWNER_BUFFERED_EVENTS_VAR = "READ_API_OWNER_BUFFERED_EVENTS";
/** The owner-buffered events read's stage concurrency (index.ts Env; ownerBufferedStageConcurrency accepts 2..32). */
export const OWNER_BUFFERED_STAGE_CONCURRENCY_VAR = "READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY";
/** The owner-buffered events read's per-phase log fields (index.ts Env; ownerBufferedPhaseLogEnabled accepts only "1"). */
export const OWNER_BUFFERED_PHASE_LOG_VAR = "READ_API_OWNER_BUFFERED_PHASE_LOG";
/** The agent-name prefix search flag (src/session-read-api/config.ts agentMatchEnabled accepts only "true"). */
export const AGENT_MATCH_VAR = "READ_API_AGENT_MATCH";
/** Vars only this render adds; a template declaring any of them is refused in every mode. */
const RENDER_ONLY_VARS = [OWNER_BUFFERED_EVENTS_VAR, OWNER_BUFFERED_STAGE_CONCURRENCY_VAR, OWNER_BUFFERED_PHASE_LOG_VAR, AGENT_MATCH_VAR] as const;

export interface RenderInputs {
  hyperdriveId: string;
  /** The workflow_dispatch input; "" (no input) = off. */
  ownerBuffered?: string;
  ownerBufferedWorkspaces?: string;
  s3Bucket?: string;
  s3Region?: string;
  /** READ_API_DIRECTORY_WORKSPACES as deployed; owner-buffered is served only where both lists allow. */
  directoryWorkspaces?: string;
  /** The buffered_search_gate workflow_dispatch input; "" (no input) = off. */
  bufferedSearchGate?: string;
  /** The owner_buffered_events workflow_dispatch input; "" (no input) = off; on needs owner_buffered=on. */
  ownerBufferedEvents?: string;
  /** The owner_buffered_stage_concurrency workflow_dispatch input; "" or "off" = off, else 2..32; needs owner_buffered_events=on. */
  ownerBufferedStageConcurrency?: string;
  /** The owner_buffered_phase_log workflow_dispatch input; "" (no input) = off; on needs owner_buffered_events=on. */
  ownerBufferedPhaseLog?: string;
  /** The agent_match workflow_dispatch input; "" (no input) = off. */
  agentMatch?: string;
}

export class RenderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RenderError";
  }
}

const WORKSPACE_ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;

/** The all-workspaces value of owner_buffered_workspaces (valid only with a directory allowlist of "*"). */
export const ALL_WORKSPACES = "*";

/** A canary list: comma-separated explicit workspace ids, no "*", no blanks, no duplicates. */
export function parseCanaryWorkspaces(raw: string): string[] {
  const ids = raw.split(",").map((v) => v.trim());
  if (ids.some((id) => id === "*")) throw new RenderError('owner_buffered_workspaces lists workspace ids or is exactly "*" (all-workspaces mode); "*" cannot be mixed with ids');
  if (ids.some((id) => !WORKSPACE_ID.test(id))) throw new RenderError("owner_buffered_workspaces must be comma-separated workspace ids (letters, digits, _ . : -)");
  if (new Set(ids).size !== ids.length) throw new RenderError("owner_buffered_workspaces lists a workspace twice");
  return ids;
}

function renderDefault(template: string, hyperdriveId: string): string {
  // sed 's/<placeholder>/<id>/': the first match on each line.
  return template
    .split("\n")
    .map((line) => line.replace(HYPERDRIVE_PLACEHOLDER, () => `"id": "${hyperdriveId}"`))
    .join("\n");
}

type JsoncConfig = Record<string, unknown> & { vars?: Record<string, string>; services?: unknown; env?: unknown };

/** The default render parsed, refusing a template that already binds services or declares env. */
function parseRendered(template: string, hyperdriveId: string): JsoncConfig {
  const parsed = ts.parseConfigFileTextToJson("wrangler.jsonc", renderDefault(template, hyperdriveId));
  if (parsed.error) throw new RenderError("wrangler.jsonc is not valid JSONC");
  const config = parsed.config as JsoncConfig;
  if (config.services !== undefined || config.env !== undefined) throw new RenderError("wrangler.jsonc must not declare services or env; the binding is added here only");
  return config;
}

function leaveUnset(vars: Record<string, string>, keys: readonly string[]): void {
  for (const k of keys) {
    if (k in vars) throw new RenderError(`wrangler.jsonc must leave ${k} unset`);
  }
}

/** Refuses a template whose vars already declare one of `keys` (a render-only flag), in every mode; an unparsable template is left to the paths that parse it. */
function refusePredeclared(template: string, hyperdriveId: string, keys: readonly string[]): void {
  const parsed = ts.parseConfigFileTextToJson("wrangler.jsonc", renderDefault(template, hyperdriveId));
  if (parsed.error) return;
  const vars = (parsed.config as JsoncConfig | undefined)?.vars;
  if (vars === null || typeof vars !== "object") return;
  for (const k of keys) if (k in vars) throw new RenderError(`wrangler.jsonc must leave ${k} unset`);
}

export function renderDeployConfig(template: string, inputs: RenderInputs): string {
  const agentMode = (inputs.agentMatch ?? "").trim() || "off";
  if (agentMode !== "off" && agentMode !== "on") throw new RenderError('agent_match must be "off" or "on"');
  const out = renderWithoutAgentMatch(template, inputs);
  if (agentMode === "off") return out;
  // agent_match=on: the selected render plus the flag, nothing else. The off renders are JSON or the
  // default JSONC; both parse here, and the default one is checked for services/env like the gate-only render.
  const parsed = ts.parseConfigFileTextToJson("wrangler.jsonc", out);
  if (parsed.error) throw new RenderError("wrangler.jsonc is not valid JSONC");
  const config = parsed.config as JsoncConfig;
  if (out === renderDefault(template, inputs.hyperdriveId) && (config.services !== undefined || config.env !== undefined)) throw new RenderError("wrangler.jsonc must not declare services or env; the binding is added here only");
  const vars = config.vars ?? {};
  leaveUnset(vars, [AGENT_MATCH_VAR]);
  return `${JSON.stringify({ ...config, vars: { ...vars, [AGENT_MATCH_VAR]: "true" } }, null, 2)}\n`;
}

function renderWithoutAgentMatch(template: string, inputs: RenderInputs): string {
  if (!/^[0-9a-f]{32}$/.test(inputs.hyperdriveId) || /^0+$/.test(inputs.hyperdriveId)) throw new RenderError("SESSION_READ_HYPERDRIVE_ID must be the Hyperdrive config id (32 hex)");
  if (!template.includes(HYPERDRIVE_PLACEHOLDER)) throw new RenderError("wrangler.jsonc has no Hyperdrive placeholder");
  const gateMode = (inputs.bufferedSearchGate ?? "").trim() || "off";
  if (gateMode !== "off" && gateMode !== "on") throw new RenderError('buffered_search_gate must be "off" or "on"');
  const gate = gateMode === "on";
  const mode = (inputs.ownerBuffered ?? "").trim() || "off";
  const eventsMode = (inputs.ownerBufferedEvents ?? "").trim() || "off";
  if (eventsMode !== "off" && eventsMode !== "on") throw new RenderError('owner_buffered_events must be "off" or "on"');
  const events = eventsMode === "on";
  const concurrencyMode = (inputs.ownerBufferedStageConcurrency ?? "").trim() || "off";
  if (concurrencyMode !== "off" && !/^(?:[2-9]|[12][0-9]|3[0-2])$/.test(concurrencyMode)) throw new RenderError('owner_buffered_stage_concurrency must be "off" or an integer 2..32');
  const concurrency = concurrencyMode === "off" ? undefined : concurrencyMode;
  const phaseLogMode = (inputs.ownerBufferedPhaseLog ?? "").trim() || "off";
  if (phaseLogMode !== "off" && phaseLogMode !== "on") throw new RenderError('owner_buffered_phase_log must be "off" or "on"');
  const phaseLog = phaseLogMode === "on";
  refusePredeclared(template, inputs.hyperdriveId, RENDER_ONLY_VARS);
  if (concurrency !== undefined && !events) throw new RenderError("owner_buffered_stage_concurrency needs owner_buffered_events=on; refusing a flag that would have no effect");
  if (phaseLog && !events) throw new RenderError("owner_buffered_phase_log=on needs owner_buffered_events=on; refusing a flag that would have no effect");
  const workspacesRaw = (inputs.ownerBufferedWorkspaces ?? "").trim();
  if (events && mode !== "on") throw new RenderError("owner_buffered_events=on needs owner_buffered=on; refusing a flag that would have no effect");
  if (mode === "off") {
    if (workspacesRaw) throw new RenderError("owner_buffered_workspaces is set but owner_buffered is off; refusing an ambiguous dispatch");
    if (!gate) return renderDefault(template, inputs.hyperdriveId);
    // Gate only: the pinned binding and the flag, nothing owner-buffered.
    const config = parseRendered(template, inputs.hyperdriveId);
    const vars = config.vars ?? {};
    leaveUnset(vars, [BUFFERED_SEARCH_GATE_VAR]);
    return `${JSON.stringify({ ...config, services: [{ ...CONTROL_HEAD_BINDING }], vars: { ...vars, [BUFFERED_SEARCH_GATE_VAR]: "on" } }, null, 2)}\n`;
  }
  if (mode !== "on") throw new RenderError('owner_buffered must be "off" or "on"');

  if (!workspacesRaw) throw new RenderError('owner_buffered=on needs owner_buffered_workspaces (at least one canary workspace id, or exactly "*" for all workspaces)');
  const directory = (inputs.directoryWorkspaces ?? "").trim();
  const all = workspacesRaw === ALL_WORKSPACES;
  if (all && directory !== ALL_WORKSPACES) throw new RenderError('owner_buffered_workspaces="*" (all-workspaces mode) needs SESSION_READ_DIRECTORY_WORKSPACES to be exactly "*"; with an explicit directory list, list the canary ids instead');
  const workspaces = all ? [ALL_WORKSPACES] : parseCanaryWorkspaces(workspacesRaw);
  if (!all && directory !== ALL_WORKSPACES) {
    const listed = new Set(directory.split(",").map((v) => v.trim()).filter(Boolean));
    const missing = workspaces.filter((id) => !listed.has(id));
    if (missing.length) throw new RenderError(`${missing.length} owner_buffered_workspaces id(s) are not on SESSION_READ_DIRECTORY_WORKSPACES; owner-buffered is served only where both allow`);
  }
  const bucket = (inputs.s3Bucket ?? "").trim();
  if (!BUCKET.test(bucket)) throw new RenderError("owner_buffered=on needs SESSION_READ_INGEST_S3_BUCKET (the ingest bucket holding buffered-batches/)");
  const region = (inputs.s3Region ?? "").trim();
  if (!REGION.test(region)) throw new RenderError("owner_buffered=on needs SESSION_READ_INGEST_S3_REGION (e.g. us-east-1)");

  const config = parseRendered(template, inputs.hyperdriveId);
  const vars = config.vars ?? {};
  leaveUnset(vars, ["RELAYHISTORY_READ_OWNER_BUFFERED", "READ_API_OWNER_BUFFERED_WORKSPACES", "INGEST_S3_BUCKET", "INGEST_S3_REGION"]);
  if (gate) leaveUnset(vars, [BUFFERED_SEARCH_GATE_VAR]);
  if (events) leaveUnset(vars, [OWNER_BUFFERED_EVENTS_VAR]);
  if (concurrency !== undefined) leaveUnset(vars, [OWNER_BUFFERED_STAGE_CONCURRENCY_VAR]);
  if (phaseLog) leaveUnset(vars, [OWNER_BUFFERED_PHASE_LOG_VAR]);
  const rendered = {
    ...config,
    services: [{ ...CONTROL_HEAD_BINDING }],
    vars: {
      ...vars,
      RELAYHISTORY_READ_OWNER_BUFFERED: "true",
      READ_API_OWNER_BUFFERED_WORKSPACES: workspaces.join(","),
      INGEST_S3_BUCKET: bucket,
      INGEST_S3_REGION: region,
      // One binding serves both readers; the gate adds only its flag.
      ...(gate ? { [BUFFERED_SEARCH_GATE_VAR]: "on" } : {}),
      ...(events ? { [OWNER_BUFFERED_EVENTS_VAR]: "on" } : {}),
      ...(concurrency !== undefined ? { [OWNER_BUFFERED_STAGE_CONCURRENCY_VAR]: concurrency } : {}),
      ...(phaseLog ? { [OWNER_BUFFERED_PHASE_LOG_VAR]: "1" } : {}),
    },
  };
  return `${JSON.stringify(rendered, null, 2)}\n`;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
  try {
    const out = renderDeployConfig(readFileSync(here("../wrangler.jsonc"), "utf8"), {
      hyperdriveId: process.env.HYPERDRIVE_ID ?? "",
      ownerBuffered: process.env.OWNER_BUFFERED,
      ownerBufferedWorkspaces: process.env.OWNER_BUFFERED_WORKSPACES,
      s3Bucket: process.env.OWNER_BUFFERED_S3_BUCKET,
      s3Region: process.env.OWNER_BUFFERED_S3_REGION,
      directoryWorkspaces: process.env.DIRECTORY_WORKSPACES,
      bufferedSearchGate: process.env.BUFFERED_SEARCH_GATE,
      ownerBufferedEvents: process.env.OWNER_BUFFERED_EVENTS,
      ownerBufferedStageConcurrency: process.env.OWNER_BUFFERED_STAGE_CONCURRENCY,
      ownerBufferedPhaseLog: process.env.OWNER_BUFFERED_PHASE_LOG,
      agentMatch: process.env.AGENT_MATCH,
    });
    writeFileSync(here("../wrangler.deploy.jsonc"), out);
    const on = ((process.env.OWNER_BUFFERED ?? "").trim() || "off") === "on";
    const all = (process.env.OWNER_BUFFERED_WORKSPACES ?? "").trim() === ALL_WORKSPACES;
    const gate = ((process.env.BUFFERED_SEARCH_GATE ?? "").trim() || "off") === "on";
    const binding = `${CONTROL_HEAD_BINDING.binding} -> ${CONTROL_HEAD_BINDING.service}#${CONTROL_HEAD_BINDING.entrypoint}`;
    console.log(on ? `deploy config: owner-buffered ON (${all ? "all workspaces" : "canary list"}), ${binding}` : gate ? "deploy config: owner-buffered off" : "deploy config: owner-buffered off (Hyperdrive id only)");
    if (((process.env.OWNER_BUFFERED_EVENTS ?? "").trim() || "off") === "on") console.log(`deploy config: owner-buffered events ON, ${OWNER_BUFFERED_EVENTS_VAR}=on`);
    const concurrencyOut = (process.env.OWNER_BUFFERED_STAGE_CONCURRENCY ?? "").trim() || "off";
    if (concurrencyOut !== "off") console.log(`deploy config: owner-buffered events stage concurrency ON, ${OWNER_BUFFERED_STAGE_CONCURRENCY_VAR}=${concurrencyOut}`);
    if (((process.env.OWNER_BUFFERED_PHASE_LOG ?? "").trim() || "off") === "on") console.log(`deploy config: owner-buffered events phase log ON, ${OWNER_BUFFERED_PHASE_LOG_VAR}=1`);
    if (((process.env.AGENT_MATCH ?? "").trim() || "off") === "on") console.log(`deploy config: agent-name prefix search ON, ${AGENT_MATCH_VAR}=true`);
    if (gate) console.log(`deploy config: buffered search gate ON, ${BUFFERED_SEARCH_GATE_VAR}=on, ${binding}${on ? " (shared with owner-buffered, rendered once)" : ""}`);
  } catch (error) {
    console.error(`::error::${error instanceof RenderError ? error.message : "render failed"}`);
    process.exit(1);
  }
}
