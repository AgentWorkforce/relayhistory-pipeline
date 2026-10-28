#!/usr/bin/env node
/**
 * List or replay session pipeline dead letters through the operator route.
 *
 *   node scripts/pipeline-dlq-replay.mjs --url http://127.0.0.1:8787 \
 *     --secret-file ./.pipeline-ops-secret [--stage file-normalize] [--trace <uuid>] \
 *     [--workspace <id>] [--limit 100] [--list] [--execute] [--force]
 *
 * Dry run unless `--execute` is given: it prints what would be re-sent and
 * changes nothing. The secret is read from a file (mode 0600), never argv or
 * stdout, matching the repo rule for operator credentials.
 */
import { readFileSync, statSync } from "node:fs";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    url: { type: "string" },
    "secret-file": { type: "string" },
    stage: { type: "string" },
    trace: { type: "string" },
    workspace: { type: "string" },
    limit: { type: "string" },
    list: { type: "boolean", default: false },
    execute: { type: "boolean", default: false },
    force: { type: "boolean", default: false },
  },
});

function fail(message) {
  console.error(message);
  process.exit(2);
}

if (!values.url) fail("--url is required");
if (!values["secret-file"]) fail("--secret-file is required");
const mode = statSync(values["secret-file"]).mode & 0o077;
if (mode)
  fail("--secret-file must not be readable by group or others (chmod 600)");
const secret = readFileSync(values["secret-file"], "utf8").trim();
if (!secret) fail("--secret-file is empty");

const base = values.url.replace(/\/+$/, "");
const headers = {
  Authorization: `Bearer ${secret}`,
  "Content-Type": "application/json",
};

let response;
if (values.list) {
  const query = new URLSearchParams();
  if (values.stage) query.set("stage", values.stage);
  if (values.trace) query.set("traceId", values.trace);
  if (values.limit) query.set("limit", values.limit);
  response = await fetch(`${base}/v1/internal/pipeline/dead-letters?${query}`, {
    headers,
  });
} else {
  response = await fetch(`${base}/v1/internal/pipeline/dead-letters/replay`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      ...(values.stage ? { stage: values.stage } : {}),
      ...(values.trace ? { traceId: values.trace } : {}),
      ...(values.workspace ? { workspaceId: values.workspace } : {}),
      ...(values.limit ? { limit: Number(values.limit) } : {}),
      dryRun: !values.execute,
      force: values.force,
    }),
  });
}

const body = await response.text();
if (!response.ok) fail(`HTTP ${response.status}: ${body.slice(0, 500)}`);
console.log(JSON.stringify(JSON.parse(body), null, 2));
if (!values.list && !values.execute)
  console.error("dry run: nothing was sent. Re-run with --execute to replay.");
