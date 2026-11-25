/**
 * Per-message SQS dead-letter resend (src/session-pipeline/dlq-resend.ts; usage docs/handoffs/dlq-resend.md).
 *
 *   bun scripts/dlq-resend.ts --dlq-url <url> --target-url <url> --max <1..100> \
 *     (--select <hashes-file> [--classes <classes.json>] | --select-class <name> --classes <classes.json>) \
 *     [--rate <per-second, default 1, max 5>] [--visibility-seconds 120] [--empty-receives 3] \
 *     [--max-scan <n>] [--wait-seconds 2] [--execute]
 *
 * DRY RUN unless --execute; --execute also needs DLQ_RESEND_CONFIRM=resend in the environment.
 * Credentials: all three of AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN, or none of
 * them and AWS_PROFILE (read through `aws configure export-credentials`, held in memory only).
 * Output: JSON lines on stdout, hashes and fixed codes only. Exit 0 done, 1 finished with errors or a
 * duplicate risk, 2 refused or failed.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import type { AwsSessionCredentials } from "../src/session-pipeline/aws-json.ts";
import { awsSqsClient, DlqResendRefusal, operatorCredentials, resolveOptions, runDlqResend } from "../src/session-pipeline/dlq-resend.ts";

/** The profile's current session, straight from the AWS CLI's stdout into memory; nothing is written. */
const exportProfile = (profile: string) =>
  execFileSync("aws", ["configure", "export-credentials", "--profile", profile, "--format", "process"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
    timeout: 30_000,
  });

const emit = (record: Record<string, unknown>) => process.stdout.write(JSON.stringify(record) + "\n");

if (import.meta.main) {
  try {
    const { values } = parseArgs({
      strict: true,
      options: {
        "dlq-url": { type: "string" },
        "target-url": { type: "string" },
        select: { type: "string" },
        "select-class": { type: "string" },
        classes: { type: "string" },
        max: { type: "string" },
        rate: { type: "string" },
        execute: { type: "boolean", default: false },
        "visibility-seconds": { type: "string" },
        "empty-receives": { type: "string" },
        "max-scan": { type: "string" },
        "wait-seconds": { type: "string" },
      },
    });
    const options = resolveOptions(values, process.env, (path) => readFileSync(path, "utf8"));
    // Resolved once, before any AWS call, so a credential refusal happens before the queue checks.
    const credentials: AwsSessionCredentials = operatorCredentials(process.env, exportProfile);
    const sqs = awsSqsClient(async () => credentials);
    const summary = await runDlqResend(options, { sqs, emit });
    process.exit(summary.errors > 0 || summary.duplicateRisk > 0 ? 1 : 0);
  } catch (error) {
    // A refusal names its fixed reason; anything else prints its class only (an error message could echo a URL).
    const code = (error as { code?: unknown })?.code;
    emit(error instanceof DlqResendRefusal ? { event: "refused", reason: error.reason } : { event: "failed", error: error instanceof Error ? error.name : "error", code: typeof code === "string" ? code : null });
    process.exit(2);
  }
}
