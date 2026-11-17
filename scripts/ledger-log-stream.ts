#!/usr/bin/env bun
/**
 * Operator tool for one ledger-log stream (src/session-ingest/ledger-log-stream-ops.ts). Dry run unless --apply.
 *
 *   bun scripts/ledger-log-stream.ts status --bucket <b> --org <o> --workspace <w> --user <u> --origin <origin>
 *   bun scripts/ledger-log-stream.ts measure --bucket <b> --org <o> --workspace <w> --user <u> [--origin <origin>]
 *       [--window-seconds 600] [--max-pages 500]
 *       READ ONLY: batch arrival rate per origin (by origin key hash) over the window; without --origin, every origin
 *       of the user, hottest first. Always allowed (production too).
 *   bun scripts/ledger-log-stream.ts switch  ... [--apply --kms-key-arn <arn> --i-know-this-is-not-prod]   drain check, then entry 0
 *   bun scripts/ledger-log-stream.ts close   ... [--apply --kms-key-arn <arn> --i-know-this-is-not-prod]   every entry applied, then the close marker
 *   bun scripts/ledger-log-stream.ts migrate-registry --bucket <b> [--apply --kms-key-arn <arn> --i-know-this-is-not-prod]
 *       rewrites v1 stream registry docs (raw ids outside the user's directory) as hash-keyed pointers plus the
 *       identity doc inside the user's directory, then deletes the v1 docs (ledger-log-materialize.ts
 *       migrateLogStreamRegistry). Bucket-wide: takes no stream ids.
 *
 * --apply on switch/close/migrate-registry is refused without --i-know-this-is-not-prod, and for any production-like
 * bucket name.
 *
 * PRODUCTION (switch and close only; docs/design/s3-ledger-log-commit.md "Production pilot"):
 *   bun scripts/ledger-log-stream.ts switch ... --production                       plan: preflight + the confirmation
 *   bun scripts/ledger-log-stream.ts switch ... --production --apply --kms-key-arn <arn> --confirm '<phrase>'
 *   (close the same way.) --production replaces --i-know-this-is-not-prod (the two together refuse) and skips the
 *   bucket-name check; the step itself then requires LEDGER_LOG_PRODUCTION_ENABLED, a clean worktree at a commit on
 *   origin/main, a passing preflight, and the confirmation rebuilt byte for byte from that preflight, unexpired.
 *
 * Credentials: --credentials keys (default: AWS_* environment) | podIdentity, as scripts/ledger-reshard-revisions.ts.
 * Prints one JSON result; never a key, token, header value, bucket name or stream id (only digests and counts).
 */
import { execFileSync } from "node:child_process";
import { parseArgs } from "node:util";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { assertLedgerHomeHere, LedgerHomeRefused } from "../src/session-ingest/ledger-store.ts";
import { closeLogStream, measureArrivalRate, streamStatus, switchToLogMode } from "../src/session-ingest/ledger-log-stream-ops.ts";
import { migrateLogStreamRegistry } from "../src/session-ingest/ledger-log-materialize.ts";
import { LEDGER_LOG_PRODUCTION_ENABLED, operatorCodeSha, type ProductionContext } from "../src/session-ingest/ledger-log-stream-production.ts";
import { reshardCredentials, type ReshardCredentialMode } from "./ledger-reshard-revisions.ts";

const ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const MUTATING = ["switch", "close", "migrate-registry"];

/**
 * The CLI's guard for a MUTATING step (switch or close with --apply): an allowlist, not a denylist. It needs the
 * explicit --i-know-this-is-not-prod flag, and it still refuses any bucket whose name looks production-like (a
 * belt on top: the operation itself also requires the bucket's non-production marker, ledger-log-stream-ops.ts).
 * With --production instead (switch and close only), the bucket-name check is skipped and an apply needs --confirm;
 * the operation then verifies the confirmation itself. Returns the refusal code, or undefined when allowed. Read-only
 * steps (status, measure, dry runs without --production) are always allowed.
 */
export function mutationRefusal(command: string | undefined, bucket: string, apply: boolean, declaredNonProd: boolean,
  production = false, confirmation = false, enabled = LEDGER_LOG_PRODUCTION_ENABLED): string | undefined {
  if (production) {
    if (declaredNonProd) return "production_flags_conflict";
    if (command !== "switch" && command !== "close") return "production_command_unsupported";
    if (!apply) return confirmation ? "production_confirmation_on_plan" : undefined;
    if (!enabled) return "production_dormant";
    return confirmation ? undefined : "production_confirmation_required";
  }
  if (confirmation) return "confirmation_without_production";
  if (!apply || !MUTATING.includes(command ?? "")) return undefined;
  if (!declaredNonProd) return "not_declared_nonprod";
  if (/prod|prd|live|hosted|customer/i.test(bucket)) return "bucket_name_production_like";
  return undefined;
}

const posInt = (v: string | undefined, max: number) => (v !== undefined && /^[1-9][0-9]{0,9}$/.test(v) && Number(v) <= max ? Number(v) : undefined);

if (import.meta.main) {
  const [command, ...rest] = process.argv.slice(2);
  let values;
  try {
    ({ values } = parseArgs({ args: rest, options: {
      bucket: { type: "string" }, region: { type: "string", default: "us-east-1" }, "key-root": { type: "string", default: "96bulls" },
      org: { type: "string" }, workspace: { type: "string" }, user: { type: "string" }, origin: { type: "string" },
      apply: { type: "boolean", default: false }, "kms-key-arn": { type: "string" }, credentials: { type: "string", default: "keys" },
      "i-know-this-is-not-prod": { type: "boolean", default: false },
      production: { type: "boolean", default: false }, confirm: { type: "string" },
      "window-seconds": { type: "string", default: "600" }, "max-pages": { type: "string", default: "500" },
    } }));
  } catch { values = undefined; }
  // Log mode is not cleared for production by default (docs/design/s3-ledger-log-commit.md). A mutating step needs the
  // explicit allowlist flag (and the operation itself needs the bucket's non-production marker), or the deliberate
  // production path. status, measure and dry runs stay allowed.
  const refusal = mutationRefusal(command, values?.bucket ?? "", values?.apply ?? false, values?.["i-know-this-is-not-prod"] ?? false,
    values?.production ?? false, values?.confirm !== undefined);
  if (refusal) {
    console.error(JSON.stringify({ refused: refusal }));
    process.exit(2);
  }
  const bucketWide = command === "migrate-registry";
  const measure = command === "measure";
  const windowSeconds = posInt(values?.["window-seconds"], 7 * 24 * 3600), maxPages = posInt(values?.["max-pages"], 100_000);
  if (!values?.bucket || !["status", "measure", "switch", "close", "migrate-registry"].includes(command ?? "")
      || (!bucketWide && [values.org, values.workspace, values.user].some((v) => !v || !ID.test(v)))
      || (!bucketWide && !(measure && values.origin === undefined) && (!values.origin || !ID.test(values.origin)))
      || (measure && (windowSeconds === undefined || maxPages === undefined || values.apply))
      || (values.apply && !values["kms-key-arn"]?.startsWith("arn:aws:kms:")) || !/^[a-z0-9_.-]{1,64}$/.test(values["key-root"]!)
      || (values.credentials !== "keys" && values.credentials !== "podIdentity")) {
    console.error("usage: status|switch|close --bucket <b> --org <o> --workspace <w> --user <u> --origin <origin> [--region r] [--key-root 96bulls] [--credentials keys|podIdentity] [--apply --kms-key-arn <arn>]\n       switch|close ... --production [--apply --kms-key-arn <arn> --confirm '<phrase from the plan>']\n       measure --bucket <b> --org <o> --workspace <w> --user <u> [--origin <origin>] [--window-seconds 600] [--max-pages 500]\n       migrate-registry --bucket <b> [--region r] [--key-root 96bulls] [--credentials keys|podIdentity] [--apply --kms-key-arn <arn>]");
    process.exit(2);
  }
  // Production: the commit actually running, before any credential is read (a dirty tree or unlanded code refuses).
  let production: ProductionContext | undefined;
  if (values.production) {
    try {
      const codeSha = operatorCodeSha(process.env, (args) => execFileSync("git", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }));
      production = { bucket: values.bucket, codeSha, planRef: "local", ...(values.confirm !== undefined ? { confirmation: values.confirm } : {}) };
    } catch (error) {
      const code = (error as { code?: unknown })?.code;
      console.error(JSON.stringify({ refused: typeof code === "string" && /^[a-z0-9_]{1,64}$/.test(code) ? code : "production_code_sha" }));
      process.exit(2);
    }
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
  // INGEST_LEDGER_HOME_GUARD=on (docs/ledger-relocation.md step 0; default off, no request): a relocated ledger's
  // marker names another bucket, so this bucket's ledger/** objects are stale copies: refuse (a closed code).
  try { await assertLedgerHomeHere(store, keyRoot, values.bucket!); } catch (error) {
    console.error(JSON.stringify({ failed: error instanceof LedgerHomeRefused ? error.code : "ledger_home_unavailable" }));
    process.exit(1);
  }
  const tenant = { orgId: values.org!, workspaceId: values.workspace!, userId: values.user! };
  let result;
  try {
    result = command === "migrate-registry" ? { ...(await migrateLogStreamRegistry(store, keyRoot, { apply: values.apply! })), applied: values.apply! }
      : command === "measure" ? await measureArrivalRate(store, keyRoot, tenant, values.origin, { windowMs: windowSeconds! * 1000, maxPages: maxPages! })
      : command === "status" ? await streamStatus(store, keyRoot, tenant, values.origin!)
      : command === "switch" ? await switchToLogMode(store, keyRoot, tenant, values.origin!, { apply: values.apply!, writer: `operator_${crypto.randomUUID()}`, ...(production ? { production } : {}) })
      : await closeLogStream(store, keyRoot, tenant, values.origin!, { apply: values.apply!, ...(production ? { production } : {}) });
  } catch (error) {
    // A code only: never a key, a bucket or a message.
    const code = (error as { code?: unknown })?.code;
    console.error(JSON.stringify({ failed: typeof code === "string" && /^[A-Za-z0-9_]{1,64}$/.test(code) ? code : "operation_failed" }));
    process.exit(1);
  }
  console.log(JSON.stringify(result));
  process.exit("refused" in result && result.refused ? 1 : 0);
}
