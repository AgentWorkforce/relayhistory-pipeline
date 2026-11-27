/**
 * sweeper-pass-verify: the READ-ONLY re-arm sweeper pass verifier for backlog-ops (contract:
 * src/session-ingest/sweeper-pass-verify.ts). CLI only; not a backlog-ops dispatch mode, never run by the workflow.
 *
 *   bun scripts/backlog-sweeper-verify.ts --bucket <locker> --region <r>   < expectation.json
 *
 * stdin: one JSON line, the SweeperPassVerifyRequest (the serving sweeper's expectations, obtained independently of
 * the store; on stdin so no org, CronJob, Job, Pod or digest reaches argv or a log). It is validated before any
 * credential is read or any request made. AWS_* credentials from the environment only: run it with the backlog-ops
 * READ-ONLY role.
 *
 * It builds exactly one store, s3GetOnly (a frozen { getObject }: no ObjectStore, no BaseStore, no S3 transport, so
 * nothing that can put, delete, list or presign is reachable from it). It never loads the authority path
 * (backlog-authority.ts, authority-evidence.ts, the ledger tool stores) and calls no store factory; a test pins both.
 * (Its reader, rearm-sweeper-receipt.ts, still loads rearm-sweeper.ts and the vendor barrel for shared helpers;
 * those define writers this tool never calls.) No writer-provenance provider is
 * wired and none can be supplied, so a pass that meets every other check is refused `writer_unverified`.
 *
 * Output: one JSON line, the closed summary (validSweeperPassSummary), for success and for every refusal, usage and
 * tool errors included. Exit 0 only when the verdict is `verified`; 1 otherwise.
 */
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { s3GetOnly } from "../src/session-ingest/s3-get-only.ts";
import {
  refusedSweeperPassSummary, validSweeperPassRequest, validSweeperPassSummary, verifySweeperPass, type SweeperPassReason, type SweeperPassSummary,
} from "../src/session-ingest/sweeper-pass-verify.ts";

class ToolRefusal extends Error { constructor(readonly code: SweeperPassReason) { super(code); } }
const fail = (code: SweeperPassReason): never => { throw new ToolRefusal(code); };
const STDIN_LIMIT = 64 * 1024;

export function parseSweeperVerifyArgs(argv: readonly string[]): { bucket: string; region: string } {
  let values;
  try {
    ({ values } = parseArgs({ args: [...argv], options: { bucket: { type: "string" }, region: { type: "string" } }, strict: true, allowPositionals: false }));
  } catch { return fail("usage"); }
  if (typeof values.bucket !== "string" || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(values.bucket) || values.bucket.includes("..")) fail("usage");
  if (typeof values.region !== "string" || !/^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u.test(values.region)) fail("usage");
  return { bucket: values.bucket!, region: values.region! };
}

export function parseSweeperVerifyStdin(raw: string): unknown {
  if (raw.length > STDIN_LIMIT) fail("stdin_too_large");
  const lines = raw.split("\n").filter((l) => l.trim() !== "");
  if (lines.length !== 1) fail("stdin_invalid");
  try { return JSON.parse(lines[0]!); } catch { return fail("stdin_invalid"); }
}

export interface SweeperVerifyDeps {
  /** Tests only: the HTTP client s3GetOnly uses (default the global fetch). */
  fetch?: typeof fetch;
  /** Tests only: the verifier clock (default Date.now). */
  now?: () => number;
}

/** The production entrypoint: never throws, always a closed summary. */
export async function main(argv: readonly string[], env: NodeJS.ProcessEnv, stdin: () => string, deps: SweeperVerifyDeps = {}): Promise<SweeperPassSummary> {
  let summary: SweeperPassSummary;
  try {
    const target = parseSweeperVerifyArgs(argv);
    const request = parseSweeperVerifyStdin(stdin());
    // The request is checked before credentials or the network (verifySweeperPass checks it again).
    if (!validSweeperPassRequest(request)) fail("invalid_request");
    const accessKeyId = env.AWS_ACCESS_KEY_ID, secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) fail("aws_credentials_required");
    const store = s3GetOnly({ bucket: target.bucket, region: target.region,
      credentials: { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey!, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) },
      ...(deps.fetch ? { fetch: deps.fetch } : {}) });
    summary = await verifySweeperPass(store, request, (deps.now ?? Date.now)());
  } catch (error) {
    summary = refusedSweeperPassSummary(error instanceof ToolRefusal ? error.code : "internal_error");
  }
  return validSweeperPassSummary(summary) ? summary : refusedSweeperPassSummary("internal_error");
}

export const exitCodeFor = (s: SweeperPassSummary) => (s.verdict === "verified" ? 0 : 1);

if (import.meta.main) {
  main(process.argv.slice(2), process.env, () => readFileSync(0, "utf8")).then((summary) => {
    process.stdout.write(JSON.stringify(summary) + "\n");
    process.exitCode = exitCodeFor(summary);
  }, () => { process.stdout.write(JSON.stringify(refusedSweeperPassSummary("internal_error")) + "\n"); process.exitCode = 1; });
}
