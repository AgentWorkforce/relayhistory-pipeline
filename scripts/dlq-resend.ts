/**
 * Per-message SQS dead-letter resend (src/session-pipeline/dlq-resend.ts; runbook docs/handoffs/dlq-resend.md).
 *
 *   bun scripts/dlq-resend.ts --dlq-url <url> --target-url <url> --max <1..100> \
 *     (--select <hashes-file> [--classes <classes.json>] | --select-class <name> --classes <classes.json>) \
 *     [--rate <per-second, default 1, max 5>] [--visibility-seconds 120] [--empty-receives 3] \
 *     [--max-scan <n>] [--wait-seconds 2] [--execute]
 *
 * DRY RUN unless --execute; --execute also needs DLQ_RESEND_CONFIRM=resend in the environment.
 * Credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN from the environment, or,
 * when those are absent, `aws configure export-credentials --profile $AWS_PROFILE --format process`.
 * Region: from each queue URL, else AWS_REGION / AWS_DEFAULT_REGION.
 * Output: JSON lines on stdout, hashes only. Exit 0 done, 1 finished with errors, 2 refused or failed.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { lambdaCredentials, type AwsSessionCredentials } from "../src/session-pipeline/aws-json.ts";
import { awsSqsClient, DlqResendRefusal, resolveOptions, runDlqResend } from "../src/session-pipeline/dlq-resend.ts";

/** Environment keys first (an assumed-role session), then the named profile through the AWS CLI. */
export function operatorCredentials(env: Record<string, string | undefined> = process.env): AwsSessionCredentials {
  if (env.AWS_ACCESS_KEY_ID && env.AWS_SECRET_ACCESS_KEY) return lambdaCredentials(env as NodeJS.ProcessEnv);
  if (!env.AWS_PROFILE) throw new DlqResendRefusal("no_aws_credentials");
  const out = execFileSync("aws", ["configure", "export-credentials", "--profile", env.AWS_PROFILE, "--format", "process"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  const parsed = JSON.parse(out) as { AccessKeyId?: string; SecretAccessKey?: string; SessionToken?: string };
  if (!parsed.AccessKeyId || !parsed.SecretAccessKey) throw new DlqResendRefusal("profile_credentials_incomplete");
  return { accessKeyId: parsed.AccessKeyId, secretAccessKey: parsed.SecretAccessKey, ...(parsed.SessionToken ? { sessionToken: parsed.SessionToken } : {}) };
}

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
    let cached: AwsSessionCredentials | undefined;
    const sqs = awsSqsClient(async () => (cached ??= operatorCredentials()), {
      fallbackRegion: process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION,
    });
    const summary = await runDlqResend(options, { sqs, emit });
    process.exit(summary.errors > 0 || summary.duplicateRisk > 0 ? 1 : 0);
  } catch (error) {
    // A refusal names its fixed reason; anything else prints its class only (an error message could echo a URL).
    emit(error instanceof DlqResendRefusal ? { event: "refused", reason: error.reason } : { event: "failed", error: error instanceof Error ? error.name : "error", code: (error as { code?: unknown })?.code ?? null });
    process.exit(2);
  }
}
