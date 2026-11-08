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
 *     canary allowlist, explicit ids only, each also on READ_API_DIRECTORY_WORKSPACES),
 *     INGEST_S3_BUCKET and INGEST_S3_REGION (from GitHub environment variables).
 * The S3 credentials are the Worker's existing PIPELINE_RAW_S3_ACCESS_KEY_ID /
 * _SECRET_ACCESS_KEY secrets (wrangler secret put); the workflow checks they exist by name.
 *
 * The binding is production-scoped by living only in this production deploy's on-render, not in
 * wrangler.jsonc: a wrangler `env` block would rename the script (legacy_env appends -<env>) and
 * does not inherit hyperdrive/vars/services, so it would either deploy a second Worker or
 * duplicate every binding; and a top-level binding would change the default deploy.
 *
 * CLI: reads wrangler.jsonc and the env below, writes wrangler.deploy.jsonc, prints no value.
 *   HYPERDRIVE_ID, OWNER_BUFFERED (off|on; empty = off), OWNER_BUFFERED_WORKSPACES,
 *   OWNER_BUFFERED_S3_BUCKET, OWNER_BUFFERED_S3_REGION, DIRECTORY_WORKSPACES.
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

export interface RenderInputs {
  hyperdriveId: string;
  /** The workflow_dispatch input; "" (no input) = off. */
  ownerBuffered?: string;
  ownerBufferedWorkspaces?: string;
  s3Bucket?: string;
  s3Region?: string;
  /** READ_API_DIRECTORY_WORKSPACES as deployed; owner-buffered is served only where both lists allow. */
  directoryWorkspaces?: string;
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

/** A canary list: comma-separated explicit workspace ids, no "*", no blanks, no duplicates. */
export function parseCanaryWorkspaces(raw: string): string[] {
  const ids = raw.split(",").map((v) => v.trim());
  if (ids.some((id) => id === "*")) throw new RenderError('owner_buffered_workspaces must list workspace ids; "*" is not allowed for the canary');
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

export function renderDeployConfig(template: string, inputs: RenderInputs): string {
  if (!/^[0-9a-f]{32}$/.test(inputs.hyperdriveId) || /^0+$/.test(inputs.hyperdriveId)) throw new RenderError("SESSION_READ_HYPERDRIVE_ID must be the Hyperdrive config id (32 hex)");
  if (!template.includes(HYPERDRIVE_PLACEHOLDER)) throw new RenderError("wrangler.jsonc has no Hyperdrive placeholder");
  const mode = (inputs.ownerBuffered ?? "").trim() || "off";
  const workspacesRaw = (inputs.ownerBufferedWorkspaces ?? "").trim();
  if (mode === "off") {
    if (workspacesRaw) throw new RenderError("owner_buffered_workspaces is set but owner_buffered is off; refusing an ambiguous dispatch");
    return renderDefault(template, inputs.hyperdriveId);
  }
  if (mode !== "on") throw new RenderError('owner_buffered must be "off" or "on"');

  if (!workspacesRaw) throw new RenderError("owner_buffered=on needs owner_buffered_workspaces (at least one canary workspace id)");
  const workspaces = parseCanaryWorkspaces(workspacesRaw);
  const directory = (inputs.directoryWorkspaces ?? "").trim();
  if (directory !== "*") {
    const listed = new Set(directory.split(",").map((v) => v.trim()).filter(Boolean));
    const missing = workspaces.filter((id) => !listed.has(id));
    if (missing.length) throw new RenderError(`${missing.length} owner_buffered_workspaces id(s) are not on SESSION_READ_DIRECTORY_WORKSPACES; owner-buffered is served only where both allow`);
  }
  const bucket = (inputs.s3Bucket ?? "").trim();
  if (!BUCKET.test(bucket)) throw new RenderError("owner_buffered=on needs SESSION_READ_INGEST_S3_BUCKET (the ingest bucket holding buffered-batches/)");
  const region = (inputs.s3Region ?? "").trim();
  if (!REGION.test(region)) throw new RenderError("owner_buffered=on needs SESSION_READ_INGEST_S3_REGION (e.g. us-east-1)");

  const parsed = ts.parseConfigFileTextToJson("wrangler.jsonc", renderDefault(template, inputs.hyperdriveId));
  if (parsed.error) throw new RenderError("wrangler.jsonc is not valid JSONC");
  const config = parsed.config as Record<string, unknown> & { vars?: Record<string, string>; services?: unknown; env?: unknown };
  if (config.services !== undefined || config.env !== undefined) throw new RenderError("wrangler.jsonc must not declare services or env; the binding is added here only");
  const vars = config.vars ?? {};
  for (const k of ["RELAYHISTORY_READ_OWNER_BUFFERED", "READ_API_OWNER_BUFFERED_WORKSPACES", "INGEST_S3_BUCKET", "INGEST_S3_REGION"]) {
    if (k in vars) throw new RenderError(`wrangler.jsonc must leave ${k} unset`);
  }
  const rendered = {
    ...config,
    services: [{ ...CONTROL_HEAD_BINDING }],
    vars: {
      ...vars,
      RELAYHISTORY_READ_OWNER_BUFFERED: "true",
      READ_API_OWNER_BUFFERED_WORKSPACES: workspaces.join(","),
      INGEST_S3_BUCKET: bucket,
      INGEST_S3_REGION: region,
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
    });
    writeFileSync(here("../wrangler.deploy.jsonc"), out);
    const on = ((process.env.OWNER_BUFFERED ?? "").trim() || "off") === "on";
    console.log(on ? `deploy config: owner-buffered ON, ${CONTROL_HEAD_BINDING.binding} -> ${CONTROL_HEAD_BINDING.service}#${CONTROL_HEAD_BINDING.entrypoint}` : "deploy config: owner-buffered off (Hyperdrive id only)");
  } catch (error) {
    console.error(`::error::${error instanceof RenderError ? error.message : "render failed"}`);
    process.exit(1);
  }
}
