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
 * It writes nothing and prints only object keys (tenant ids and hashes, no content).
 */
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { pendingAdmissions } from "../src/session-ingest/async-admission.ts";

export async function recoverLines(store: ObjectStore, opts: { keyRoot: string; nowMs: number; olderThanMs: number; orgId?: string }): Promise<string[]> {
  const out: string[] = [];
  for await (const message of pendingAdmissions(store, opts.keyRoot, opts)) out.push(JSON.stringify(message));
  return out;
}

function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

if (import.meta.main) {
  const bucket = arg("bucket");
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  if (!bucket || !accessKeyId || !secretAccessKey) {
    console.error("usage: --bucket <name> [--region r] [--org o] [--older-than-minutes n]; AWS_* credentials in the environment");
    process.exit(2);
  }
  const minutes = Number(arg("older-than-minutes", "60"));
  if (!Number.isFinite(minutes) || minutes < 0) throw new Error("--older-than-minutes must be a non-negative number");
  const store = createS3Store({
    bucket,
    region: arg("region", "us-east-1")!,
    credentials: { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) },
  }) as unknown as ObjectStore;
  for (const line of await recoverLines(store, { keyRoot: "96bulls", nowMs: Date.now(), olderThanMs: minutes * 60_000, ...(arg("org") ? { orgId: arg("org")! } : {}) }))
    process.stdout.write(line + "\n");
}
