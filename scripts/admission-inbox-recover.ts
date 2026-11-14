/**
 * Read-only operator recovery for S3-first async admission (docs/design/s3-first-async-ingest.md §g).
 *
 * Lists every acknowledged admission inbox object that still holds content (not tombstoned) and is
 * older than --older-than-minutes, and prints the INGEST_ADMISSION message that re-drives it, one
 * JSON object per line. The queue is therefore never the authority over an acknowledged upload:
 * after a lost or expired message the operator publishes these lines to the admission queue (the
 * Cloudflare Queues "send messages" API). A message for an already-terminal object only acks, and
 * a re-run admission replays the claim, so over-sending is safe.
 *
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]   from the environment only
 *     (CloudShell / a role session; never a file), with s3:ListBucket on the transcripts bucket
 *   bun run scripts/admission-inbox-recover.ts --bucket <name> [--region us-east-1] [--org <org>] [--older-than-minutes 60]
 *
 * It writes nothing and prints only object keys (tenant ids and hashes, no content). Every argument
 * that reaches the request host is validated strictly before any credential is read.
 */
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { pendingAdmissions } from "../src/session-ingest/async-admission.ts";

export async function recoverLines(store: ObjectStore, opts: { keyRoot: string; nowMs: number; olderThanMs: number; orgId?: string }): Promise<string[]> {
  const out: string[] = [];
  for await (const message of pendingAdmissions(store, opts.keyRoot, opts)) out.push(JSON.stringify(message));
  return out;
}

const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;
const BUCKET = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
/** writer.ts assertTenantSegment: a segment of [A-Za-z0-9._-]{1,128}, never starting with a dot. */
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;

export interface RecoverArgs { bucket: string; region: string; orgId?: string; olderThanMs: number }

/** Strict CLI parsing: a region/bucket that is not a plain AWS name never reaches the S3 host. */
export function parseRecoverArgs(argv: string[]): RecoverArgs {
  const arg = (name: string, fallback?: string) => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 ? argv[i + 1] : fallback;
  };
  const bucket = arg("bucket") ?? "", region = arg("region", "us-east-1")!, org = arg("org"), minutes = Number(arg("older-than-minutes", "60"));
  if (!BUCKET.test(bucket)) throw new Error("--bucket must be a plain S3 bucket name");
  if (!REGION.test(region)) throw new Error("--region must be an AWS region name like us-east-1");
  if (org !== undefined && !ORG.test(org)) throw new Error("--org must be an org id");
  if (!Number.isFinite(minutes) || minutes < 0) throw new Error("--older-than-minutes must be a non-negative number");
  return { bucket, region, ...(org !== undefined ? { orgId: org } : {}), olderThanMs: minutes * 60_000 };
}

if (import.meta.main) {
  let args: RecoverArgs;
  try {
    args = parseRecoverArgs(process.argv);
  } catch (error) {
    console.error(`${(error as Error).message}\nusage: --bucket <name> [--region r] [--org o] [--older-than-minutes n]; AWS_* credentials in the environment`);
    process.exit(2);
  }
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) {
    console.error("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be set in the environment");
    process.exit(2);
  }
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine();
  const store = createS3Store({
    bucket: args.bucket,
    region: args.region,
    credentials: { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) },
  }) as unknown as ObjectStore;
  for (const line of await recoverLines(store, { keyRoot: "96bulls", nowMs: Date.now(), olderThanMs: args.olderThanMs, ...(args.orgId ? { orgId: args.orgId } : {}) }))
    process.stdout.write(line + "\n");
}
