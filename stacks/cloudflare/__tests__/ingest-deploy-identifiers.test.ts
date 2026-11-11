/**
 * The session-ingest deploy path must carry no production identifier: the account id and the
 * live version come from the environment secret and the live read at run time; the Hyperdrive id,
 * queue ids, Secrets Store id, bucket and KMS key are copied from live into a runner-temp config,
 * never committed. This scans every committed file of that path.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "..", "..", "..");
export const DEPLOY_PATH_FILES = [
  ".github/workflows/deploy-session-ingest.yml",
  "deploy/session-ingest/production-profile.json",
  "runbooks/sessions-prod-session-ingest.md",
  "stacks/cloudflare/ingest-deploy/plan.ts",
  "stacks/cloudflare/ingest-deploy/cli.ts",
  "stacks/cloudflare/__tests__/ingest-deploy-plan.test.ts",
  "stacks/cloudflare/__tests__/ingest-deploy-identifiers.test.ts",
];

/** Cloudflare account, Hyperdrive, queue and Secrets Store ids are 32 hex; versions are UUIDs;
 * AWS accounts are 12 digits (bucket names and KMS ARNs embed them). */
const PATTERNS: [string, RegExp][] = [
  ["32-hex id (account, Hyperdrive, queue, Secrets Store)", /(?<![0-9A-Za-z])[0-9a-fA-F]{32}(?![0-9A-Za-z])/],
  ["UUID (Worker version)", /(?<![0-9A-Za-z])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?![0-9A-Za-z])/],
  ["12-digit AWS account", /(?<!\d)\d{12}(?!\d)/],
  ["AWS ARN", /arn:aws[a-z-]*:/],
];

const hits = (text: string) => PATTERNS.filter(([, re]) => re.test(text)).map(([what]) => what);

test("the patterns catch what they are meant to (samples built at run time)", () => {
  expect(hits(`account_id: ${"ab".repeat(16)}`)).toContain(PATTERNS[0]![0]);
  expect(hits(`"id": "${"0".repeat(32)}"`)).toContain(PATTERNS[0]![0]);
  expect(hits(["1".repeat(8), "2".repeat(4), "3".repeat(4), "4".repeat(4), "5".repeat(12)].join("-"))).toContain(PATTERNS[1]![0]);
  expect(hits(`bucket-${"9".repeat(12)}`)).toContain(PATTERNS[2]![0]);
  expect(hits(["arn", "aws", "kms"].join(":") + ":"))
    .toContain(PATTERNS[3]![0]);
  // A 40-hex commit sha and a 64-hex sha256 are not ids.
  expect(hits(`commit ${"c".repeat(40)} sha256 ${"d".repeat(64)}`)).toEqual([]);
});

test("no committed file of the session-ingest deploy path contains a production-looking identifier", () => {
  for (const file of DEPLOY_PATH_FILES) {
    const text = readFileSync(join(ROOT, file), "utf8");
    expect({ file, hits: hits(text) }).toEqual({ file, hits: [] });
  }
});
