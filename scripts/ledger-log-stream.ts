#!/usr/bin/env bun
/**
 * Operator tool for one ledger-log stream (src/session-ingest/ledger-log-stream-ops.ts). Dry run unless --apply.
 *
 *   bun scripts/ledger-log-stream.ts status --bucket <b> --org <o> --workspace <w> --user <u> --origin <origin>
 *   bun scripts/ledger-log-stream.ts switch  ... [--apply --kms-key-arn <arn> --i-know-this-is-not-prod]   drain check, then entry 0
 *   bun scripts/ledger-log-stream.ts close   ... [--apply --kms-key-arn <arn> --i-know-this-is-not-prod]   every entry applied, then the close marker
 *
 * --apply on switch/close is refused without --i-know-this-is-not-prod, and for any production-like bucket name.
 *
 * Credentials: --credentials keys (default: AWS_* environment) | podIdentity, as scripts/ledger-reshard-revisions.ts.
 * Prints one JSON result; never a key, token or header value.
 */
import { parseArgs } from "node:util";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { closeLogStream, streamStatus, switchToLogMode } from "../src/session-ingest/ledger-log-stream-ops.ts";
import { reshardCredentials, type ReshardCredentialMode } from "./ledger-reshard-revisions.ts";

const ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;

/**
 * The CLI's guard for a MUTATING step (switch or close with --apply): an allowlist, not a denylist. It needs the
 * explicit --i-know-this-is-not-prod flag, and it still refuses any bucket whose name looks production-like (a
 * belt on top: the operation itself also requires the bucket's non-production marker, ledger-log-stream-ops.ts).
 * Returns the refusal code, or undefined when allowed. Read-only steps (status, dry runs) are always allowed.
 */
export function mutationRefusal(command: string | undefined, bucket: string, apply: boolean, declaredNonProd: boolean): string | undefined {
  if (!apply || (command !== "switch" && command !== "close")) return undefined;
  if (!declaredNonProd) return "not_declared_nonprod";
  if (/prod|prd|live|hosted|customer/i.test(bucket)) return "bucket_name_production_like";
  return undefined;
}

if (import.meta.main) {
  const [command, ...rest] = process.argv.slice(2);
  let values;
  try {
    ({ values } = parseArgs({ args: rest, options: {
      bucket: { type: "string" }, region: { type: "string", default: "us-east-1" }, "key-root": { type: "string", default: "96bulls" },
      org: { type: "string" }, workspace: { type: "string" }, user: { type: "string" }, origin: { type: "string" },
      apply: { type: "boolean", default: false }, "kms-key-arn": { type: "string" }, credentials: { type: "string", default: "keys" },
      "i-know-this-is-not-prod": { type: "boolean", default: false },
    } }));
  } catch { values = undefined; }
  const ids = [values?.org, values?.workspace, values?.user, values?.origin];
  // Log mode is not cleared for production (docs/design/s3-ledger-log-commit.md, Remaining). A mutating step needs
  // the explicit allowlist flag (and the operation itself needs the bucket's non-production marker). status and dry
  // runs stay allowed.
  const refusal = mutationRefusal(command, values?.bucket ?? "", values?.apply ?? false, values?.["i-know-this-is-not-prod"] ?? false);
  if (refusal) {
    console.error(JSON.stringify({ refused: refusal }));
    process.exit(2);
  }
  if (!values?.bucket || !["status", "switch", "close"].includes(command ?? "") || ids.some((v) => !v || !ID.test(v))
      || (values.apply && !values["kms-key-arn"]?.startsWith("arn:aws:kms:")) || !/^[a-z0-9_.-]{1,64}$/.test(values["key-root"]!)
      || (values.credentials !== "keys" && values.credentials !== "podIdentity")) {
    console.error("usage: status|switch|close --bucket <b> --org <o> --workspace <w> --user <u> --origin <origin> [--region r] [--key-root 96bulls] [--credentials keys|podIdentity] [--apply --kms-key-arn <arn>]");
    process.exit(2);
  }
  let credentials: Awaited<ReturnType<typeof reshardCredentials>>;
  try { credentials = await reshardCredentials(values.credentials as ReshardCredentialMode, process.env); }
  catch (error) {
    const m = (error as Error)?.message;
    console.error(JSON.stringify({ refused: typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "credentials_unavailable" }));
    process.exit(2);
  }
  const store = createS3Store({ bucket: values.bucket, region: values.region!, credentials,
    ...(values.apply ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: values["kms-key-arn"]! } : {}) }) as unknown as ObjectStore;
  const keyRoot = values["key-root"]!;
  const tenant = { orgId: values.org!, workspaceId: values.workspace!, userId: values.user! };
  let result;
  try {
    result = command === "status" ? await streamStatus(store, keyRoot, tenant, values.origin!)
      : command === "switch" ? await switchToLogMode(store, keyRoot, tenant, values.origin!, { apply: values.apply!, writer: `operator_${crypto.randomUUID()}` })
      : await closeLogStream(store, keyRoot, tenant, values.origin!, { apply: values.apply! });
  } catch (error) {
    // A code only: never a key, a bucket or a message.
    const code = (error as { code?: unknown })?.code;
    console.error(JSON.stringify({ failed: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "operation_failed" }));
    process.exit(1);
  }
  console.log(JSON.stringify(result));
  process.exit("refused" in result && result.refused ? 1 : 0);
}
