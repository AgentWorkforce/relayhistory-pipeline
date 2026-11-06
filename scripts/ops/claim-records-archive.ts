// Operator step only. Retires terminal ingest_claim_attempts.records to S3.
// stdin (one JSON line, piped from 1Password, never written to disk): {"ownerUrl": "..."}
// AWS: temporary session credentials in AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN.
// Flags: --bucket <b> --region us-east-1 --kms-key-arn <arn> [--limit 50] [--max-pages 1] [--after <uuid>] [--execute]
// Default is DRY-RUN: lists and verifies candidates, writes nothing to S3 or SQL.
// Output is one sanitized JSON report (counts and the resume cursor only).
import { readFileSync } from "node:fs";
import pg from "pg";
import { createS3Store } from "../../packages/object-store/src/index.ts";
import { archiveClaimAttemptRecords } from "../../src/session-ingest/claim-records-archive.ts";
const args = process.argv.slice(2), flag = (n: string) => { const i = args.indexOf("--" + n); return i < 0 ? undefined : args[i + 1]; };
const execute = args.includes("--execute"), bucket = flag("bucket"), region = flag("region"), kmsKeyArn = flag("kms-key-arn");
const limit = Number(flag("limit") ?? "50"), maxPages = Number(flag("max-pages") ?? "1"), after = flag("after") ?? null;
if (!bucket || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || region !== "us-east-1"
  || !kmsKeyArn || !/^arn:aws:kms:us-east-1:\d{12}:key\/[0-9a-f-]{36}$/.test(kmsKeyArn)) throw Error("target_required");
if (after !== null && !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(after)) throw Error("after_invalid");
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]!) as { ownerUrl?: string };
if (typeof ownerUrl !== "string" || !ownerUrl) throw Error("owner_url_required");
const client = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "96bulls-claim-records-archive", statement_timeout: 15000 });
ownerUrl = "";
await client.connect();
try {
  // Temporary role-session credentials from the operator's environment only (never a file).
  const { AWS_ACCESS_KEY_ID: accessKeyId, AWS_SECRET_ACCESS_KEY: secretAccessKey, AWS_SESSION_TOKEN: sessionToken } = process.env;
  if (!accessKeyId || !secretAccessKey || !sessionToken || !/^ASIA[A-Z0-9]{16}$/.test(accessKeyId)) throw Error("temporary_aws_credentials_required");
  const store = createS3Store({ bucket, region, serverSideEncryption: "aws:kms", sseKmsKeyId: kmsKeyArn,
    credentials: async () => ({ accessKeyId, secretAccessKey, sessionToken }) });
  const report = await archiveClaimAttemptRecords({ sql: { query: (s, p) => client.query(s, p) }, store, keyRoot: "96bulls", limit, maxPages, execute }, after);
  console.log(JSON.stringify({ mode: execute ? "execute" : "dry-run", ...report }));
} finally { await client.end(); }
