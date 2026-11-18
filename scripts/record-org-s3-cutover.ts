/**
 * Records one org's S3 cutover in sessions.org_s3_cutover (migration 20261105062120), the input of the
 * session analysis's scoped completeness proof (condition (d), docs/session-analysis-completeness.md).
 * Khaliq-gated: run it for a real org only with his explicit OK, after the migration is applied.
 *
 *   bun scripts/record-org-s3-cutover.ts --org <id> \
 *     --uploads-cutover-at <iso> --turns-cutover-at <iso> --recorded-by <operator> \
 *     --pointer-fix-deployed-at <iso> \
 *     --attest-uploads-fenced --attest-turns-forwarded [--note <text>] \
 *     [--correct [--khaliq-approved-lowering]] [--execute]
 *
 * --correct changes an existing row (owner-only sessions.org_s3_cutover_correct_v1; the prior row goes
 * to sessions.org_s3_cutover_history). It may only move cutover_at LATER. Moving it earlier needs
 * Khaliq's explicit approval, passed as --khaliq-approved-lowering, and prints a loud warning. Deleting a
 * row is not offered by this script (runbook-only, Khaliq-gated). On --correct, --pointer-fix-deployed-at
 * must equal the time stored with the row (cutover_pointer_fix_mismatch otherwise); a correction keeps it.
 *
 * Dry-run by default (reads counts, writes nothing). --execute inserts exactly one immutable row. The
 * connection is the guarded schema release's owner client (SUPABASE_MIGRATION_OWNER_URL, from 1Password
 * into the environment only) and must prove the pinned owner identity before any query: the table is
 * owner-only. Prints one JSON receipt of counts; never an org, workspace or session id. Errors print a
 * fixed code only.
 */
import { releaseClient, releaseErrorCode, requireReleaseOwner } from "../src/hoopdb/schema-release-inspect.ts";
import {
  ORG_S3_CUTOVER_USAGE,
  OrgS3CutoverRefusal,
  parseOrgS3CutoverArgs,
  recordOrgS3Cutover,
} from "../src/session-analysis/org-s3-cutover.ts";

async function main() {
  const input = parseOrgS3CutoverArgs(process.argv.slice(2));
  if (input.approvedLowering)
    console.error(
      "WARNING: --khaliq-approved-lowering moves this org's S3 cutover EARLIER. That widens the analysis\n" +
        "WARNING: completeness proof to sessions nobody attested before. Proceed only with Khaliq's explicit,\n" +
        "WARNING: written approval for exactly this org and these times.",
    );
  const client = releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);
  await client.connect();
  try {
    await requireReleaseOwner(client);
    console.log(JSON.stringify(await recordOrgS3Cutover(client, input)));
  } finally {
    await client.end().catch(() => {});
  }
}

if (import.meta.main)
  main().catch((error) => {
    const code = error instanceof OrgS3CutoverRefusal ? error.code : releaseErrorCode(error);
    console.error("record_org_s3_cutover_refused " + code);
    if (code === "cutover_args") console.error(ORG_S3_CUTOVER_USAGE);
    process.exitCode = 1;
  });
