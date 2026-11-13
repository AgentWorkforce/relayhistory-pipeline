/**
 * One-time reshard of the S3 upload ledger's revision index from 64 to 4096 shards (src/session-ingest/revision-reshard.ts).
 *
 *   bun scripts/ledger-reshard-revisions.ts --bucket <name> [--region us-west-2] [--org <orgId> ...] [--key-root 96bulls]
 *   bun scripts/ledger-reshard-revisions.ts --execute --kms-key-arn <arn> --bucket <name> [--org <orgId> ...]
 *
 * Dry run by default (lists and reads only). --execute merges each legacy entry into the new layout under the user's
 * current fence (idempotent; safe to rerun and to run while admissions run). Legacy shards are never deleted.
 * AWS_* credentials in the environment. Prints one JSON summary per org: counts only, never keys or ids.
 */
import { parseArgs } from "node:util";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { reshardRevisions } from "../src/session-ingest/revision-reshard.ts";

const ORG = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

if (import.meta.main) {
  let values;
  try {
    ({ values } = parseArgs({ options: {
      bucket: { type: "string" }, region: { type: "string", default: "us-east-1" }, org: { type: "string", multiple: true },
      "key-root": { type: "string", default: "96bulls" }, execute: { type: "boolean", default: false }, "kms-key-arn": { type: "string" },
      concurrency: { type: "string", default: "8" },
    } }));
  } catch {
    values = undefined;
  }
  const kms = values?.["kms-key-arn"];
  if (!values?.bucket || (values.execute && !kms?.startsWith("arn:aws:kms:")) || (values.org ?? []).some((o) => !ORG.test(o)) || !/^[a-z0-9_.-]{1,64}$/.test(values["key-root"]!)) {
    console.error("usage: --bucket <name> [--region r] [--org <orgId> ...] [--key-root 96bulls] [--concurrency 8] [--execute --kms-key-arn <arn>]; dry run unless --execute; AWS_* credentials in the environment");
    process.exit(2);
  }
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) {
    console.error("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be set in the environment");
    process.exit(2);
  }
  const keyRoot = values["key-root"]!;
  const store = createS3Store({
    bucket: values.bucket, region: values.region!,
    credentials: { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) },
    ...(values.execute ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: kms! } : {}),
  }) as unknown as ObjectStore;
  const prefixes = values.org?.length ? values.org.map((o) => `${keyRoot}/.admission-inbox/${o}/`) : [`${keyRoot}/.admission-inbox/`];
  let failed = false;
  for (const [i, prefix] of prefixes.entries()) {
    try {
      const summary = await reshardRevisions(store, keyRoot, { prefix, execute: values.execute!, concurrency: Number(values.concurrency) });
      process.stdout.write(JSON.stringify({ org_index: i, mode: values.execute ? "execute" : "dry_run", ...summary }) + "\n");
      if (summary.failed > 0 || summary.invalid > 0) failed = true;
    } catch (error) {
      const m = (error as Error)?.message;
      process.stdout.write(JSON.stringify({ org_index: i, refused: typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "internal_error" }) + "\n");
      failed = true;
    }
  }
  process.exitCode = failed ? 1 : 0;
}
