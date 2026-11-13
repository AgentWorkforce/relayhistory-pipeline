/**
 * Operator CLI for the one-off buffered backlog drain (src/session-ingest/buffered-drain.ts,
 * docs/decisions/0005-s3-native-upload-ledger.md "The buffered backlog"). S3 only: no SQL.
 *
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]   from the environment only
 *     (CloudShell / a role session; never a file).
 *   bun run scripts/buffered-drain.ts --dry-run  --bucket <name> [--region us-east-1] [--org <org> ...]
 *       [--concurrency 4] [--max-objects N] [--start-after <key> | --checkpoint-file <path>]
 *       [--assume-empty-fence]
 *   bun run scripts/buffered-drain.ts --execute  --bucket <name> --kms-key-arn <arn> [same flags, never
 *       --assume-empty-fence]
 *
 * ONE DRAIN PROCESS AT A TIME. --execute takes a create-only lease at `96bulls/.drain/lock.json`
 * (holder + expiry, renewed while it runs, expired in place on exit) and refuses to start while an
 * unexpired one exists; --dry-run writes no lock but also refuses beside a live one. A crashed run's
 * lease expires after its TTL (15 minutes).
 *
 * A user without a backfilled `upload-fences.json` (0005 step 0) is always refused (`fence_absent`):
 * the backlog's deletions and resets were recorded in SQL, and the drain reads S3 alone.
 * --assume-empty-fence (dry run only; refused with --execute) classifies such a user as if the fence
 * were empty, counted in `fence_assumed_empty`, so a dry run before the backfill still measures the
 * backlog.
 *
 * Exactly one of --dry-run / --execute is required. --dry-run needs only s3:ListBucket and s3:GetObject
 * (and kms:Decrypt); every put/delete is refused in code as well. --execute writes the canonical layout,
 * the 0005 revision index, re-arm markers and the drained markers, and needs the owner's explicit OK.
 *
 * Without --org, orgs are discovered with one delimiter listing of `96bulls/` (S3 ListObjectsV2,
 * read-only). Output: one JSON summary line on stdout, and counts-only progress on stderr. Never a key,
 * tenant id or content. --checkpoint-file holds the resume key (which names an org); it is operator
 * state, not output, and is read on start if it exists.
 */
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { signRequest, type AwsCredentials } from "../packages/object-store/src/sigv4.ts";
import { xmlElements, xmlText } from "../packages/object-store/src/xml.ts";
import { BUFFER_STAGE_MAX_BYTES } from "../src/session-ingest/staging.ts";
import { MAX_DRAIN_CONCURRENCY, drainBufferedBatches, parseStagedKey, type DrainSummary } from "../src/session-ingest/buffered-drain.ts";

export const KEY_ROOT = "96bulls";
const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;
const BUCKET = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const KMS = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;

export interface DrainArgs {
  mode: "dry_run" | "execute";
  bucket: string;
  region: string;
  orgs: string[];
  concurrency: number;
  /** Dry run only (refused with --execute here and in drainBufferedBatches). */
  assumeEmptyFence?: true;
  maxObjects?: number;
  startAfter?: string;
  checkpointFile?: string;
  kmsKeyArn?: string;
}

const VALUE_FLAGS = new Set(["--bucket", "--region", "--org", "--concurrency", "--max-objects", "--start-after", "--checkpoint-file", "--kms-key-arn"]);
const BOOL_FLAGS = new Set(["--dry-run", "--execute", "--assume-empty-fence"]);

/** Strict parsing: nothing unvalidated reaches the S3 host, and the mode is never implied. */
export function parseDrainArgs(argv: string[]): DrainArgs {
  const orgs: string[] = [];
  const one = new Map<string, string>();
  const bools = new Set<string>();
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (BOOL_FLAGS.has(a)) { bools.add(a); continue; }
    if (!VALUE_FLAGS.has(a)) throw new Error(`unknown flag ${a.startsWith("--") ? a : "(positional)"}`);
    const v = argv[++i];
    if (v === undefined || v.startsWith("--")) throw new Error(`${a} needs a value`);
    if (a === "--org") orgs.push(v);
    else if (one.has(a)) throw new Error(`${a} given twice`);
    else one.set(a, v);
  }
  if (bools.has("--dry-run") === bools.has("--execute")) throw new Error("exactly one of --dry-run / --execute is required");
  const mode = bools.has("--execute") ? "execute" : "dry_run";
  const bucket = one.get("--bucket") ?? "", region = one.get("--region") ?? "us-east-1";
  if (!BUCKET.test(bucket)) throw new Error("--bucket must be a plain S3 bucket name");
  if (!REGION.test(region)) throw new Error("--region must be an AWS region name like us-east-1");
  for (const org of orgs) if (!ORG.test(org)) throw new Error("--org must be an org id");
  const int = (name: string, min: number, max: number) => {
    const v = one.get(name);
    if (v === undefined) return undefined;
    if (!/^[0-9]{1,9}$/.test(v) || Number(v) < min || Number(v) > max) throw new Error(`${name} must be an integer in [${min}, ${max}]`);
    return Number(v);
  };
  const concurrency = int("--concurrency", 1, MAX_DRAIN_CONCURRENCY) ?? 4;
  const maxObjects = int("--max-objects", 1, 100_000_000);
  const startAfter = one.get("--start-after"), checkpointFile = one.get("--checkpoint-file"), kmsKeyArn = one.get("--kms-key-arn");
  if (startAfter !== undefined && checkpointFile !== undefined) throw new Error("use --start-after or --checkpoint-file, not both");
  if (startAfter !== undefined && !validResumeKey(startAfter)) throw new Error("--start-after must be a buffered staged key or its drained marker");
  if (mode === "execute" && (kmsKeyArn === undefined || !KMS.test(kmsKeyArn))) throw new Error("--execute requires --kms-key-arn (the bucket's KMS key ARN)");
  if (mode === "dry_run" && kmsKeyArn !== undefined) throw new Error("--kms-key-arn is only for --execute");
  const assumeEmptyFence = bools.has("--assume-empty-fence");
  if (assumeEmptyFence && mode !== "dry_run") throw new Error("--assume-empty-fence is only for --dry-run");
  return {
    mode, bucket, region, orgs, concurrency,
    ...(assumeEmptyFence ? { assumeEmptyFence: true as const } : {}),
    ...(maxObjects !== undefined ? { maxObjects } : {}),
    ...(startAfter !== undefined ? { startAfter } : {}),
    ...(checkpointFile !== undefined ? { checkpointFile } : {}),
    ...(kmsKeyArn !== undefined ? { kmsKeyArn } : {}),
  };
}

function validResumeKey(key: string): boolean {
  const staged = key.endsWith(".drained.json") ? key.slice(0, -".drained.json".length) : key;
  const parts = parseStagedKey(staged);
  return parts !== null && parts.keyRoot === KEY_ROOT;
}

/** Org ids: the `96bulls/<org>/` common prefixes of one read-only delimiter listing. */
export async function discoverOrgs(args: { bucket: string; region: string; credentials: AwsCredentials; fetcher?: typeof fetch }): Promise<string[]> {
  const orgs: string[] = [];
  let token: string | undefined;
  do {
    const url = new URL(`https://${args.bucket}.s3.${args.region}.amazonaws.com/`);
    url.searchParams.set("list-type", "2");
    url.searchParams.set("prefix", `${KEY_ROOT}/`);
    url.searchParams.set("delimiter", "/");
    if (token) url.searchParams.set("continuation-token", token);
    const headers = new Headers();
    await signRequest({ method: "GET", url, headers, payloadSha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", region: args.region, credentials: args.credentials });
    const res = await (args.fetcher ?? fetch)(url, { method: "GET", headers });
    const text = await res.text();
    if (!res.ok) throw new Error(`org_discovery_http_${res.status}`);
    for (const cp of xmlElements(text, "CommonPrefixes")) {
      const prefix = xmlText(cp, "Prefix") ?? "";
      const org = prefix.slice(KEY_ROOT.length + 1, -1);
      if (prefix.startsWith(`${KEY_ROOT}/`) && prefix.endsWith("/") && ORG.test(org)) orgs.push(org);
    }
    token = xmlText(text, "IsTruncated") === "true" ? xmlText(text, "NextContinuationToken") : undefined;
  } while (token);
  return orgs;
}

const progressLine = (s: DrainSummary) =>
  JSON.stringify({ progress: true, listed: s.listed, ...Object.fromEntries(Object.entries(s.objects).filter(([, n]) => n > 0)), lines_written: s.lines.written });

if (import.meta.main) {
  let args: DrainArgs;
  try {
    args = parseDrainArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${(error as Error).message}\nusage: (--dry-run | --execute --kms-key-arn <arn>) --bucket <name> [--region r] [--org o ...] [--concurrency n] [--max-objects n] [--start-after key | --checkpoint-file path] [--assume-empty-fence (dry run only)]; one drain process at a time (lease at 96bulls/.drain/lock.json); AWS_* credentials in the environment`);
    process.exit(2);
  }
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) {
    console.error("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be set in the environment");
    process.exit(2);
  }
  const credentials: AwsCredentials = { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) };
  try {
    let startAfter = args.startAfter;
    if (args.checkpointFile && existsSync(args.checkpointFile)) {
      const saved = readFileSync(args.checkpointFile, "utf8").trim();
      if (!validResumeKey(saved)) throw new Error("checkpoint_invalid");
      startAfter = saved;
    }
    const store = createS3Store({
      bucket: args.bucket, region: args.region, credentials,
      ...(args.mode === "execute" ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: args.kmsKeyArn! } : {}),
    }) as unknown as ObjectStore;
    const orgs = args.orgs.length ? args.orgs : await discoverOrgs({ bucket: args.bucket, region: args.region, credentials });
    let lastProgress = 0;
    const summary = await drainBufferedBatches({
      store, keyRoot: KEY_ROOT, orgs, dryRun: args.mode === "dry_run", concurrency: args.concurrency,
      ...(args.assumeEmptyFence ? { assumeEmptyFence: true } : {}),      decompress: (bytes) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: BUFFER_STAGE_MAX_BYTES })),
      ...(args.maxObjects !== undefined ? { maxObjects: args.maxObjects } : {}),
      ...(startAfter !== undefined ? { startAfter } : {}),
      ...(args.checkpointFile ? {
        onCheckpoint: (key: string) => {
          const tmp = `${args.checkpointFile}.tmp`;
          writeFileSync(tmp, key + "\n", { mode: 0o600 });
          renameSync(tmp, args.checkpointFile!);
        },
      } : {}),
      onProgress: (s) => {
        if (Date.now() - lastProgress < 30_000) return;
        lastProgress = Date.now();
        process.stderr.write(progressLine(s) + "\n");
      },
    });
    process.stdout.write(JSON.stringify(summary) + "\n");
  } catch (error) {
    // A closed reason only: an S3 error message can carry a key.
    const m = (error as Error)?.message;
    process.stdout.write(JSON.stringify({ refused: typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "internal_error" }) + "\n");
    process.exitCode = 1;
  }
}
