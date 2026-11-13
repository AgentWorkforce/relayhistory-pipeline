/**
 * Operator CLI for the one-time accepted-stage export (src/session-ingest/accepted-stage-export.ts,
 * docs/decisions/0005-s3-native-upload-ledger.md "The buffered backlog", step 0b).
 *
 *   stdin (one JSON line, piped from 1Password, never written to disk):
 *     {"dbUrl": "postgresql://user:password@host:port/db?sslmode=verify-full"}
 *     A read-only login is recommended; every page is its own short READ ONLY transaction (checked with
 *     SHOW transaction_read_only, always rolled back), and the session sets default_transaction_read_only=on.
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN] from the environment only.
 *
 *   bun run scripts/accepted-stage-export.ts --dry-run  --bucket <name> [--region us-east-1] [--org <org> ...]
 *       [--page-size 1000] [--concurrency 8] [--statement-timeout-ms 30000]
 *   bun run scripts/accepted-stage-export.ts --execute  --bucket <name> --kms-key-arn <arn> [same flags]
 *
 * Exactly one of --dry-run / --execute is required. --dry-run reads SQL and the existing sidecars and writes
 * nothing (refused in code as well). --execute writes each candidate stage's `<stage>.accepted.json` sidecar (v2:
 * SQL's own liveness verdict, the lines SQL keeps, and the user's S3 fence as seen; replaced by CAS when a rerun's
 * verdict differs) and the create-only per-org completion objects the drain requires, under a fenced lease
 * (96bulls/.drain/export-lock.json, one export at a time). It runs AFTER the fence backfill, and again right before
 * the drain. It needs the owner's explicit OK. Rerunning converges.
 *
 * Output: one JSON line of counts only (never a key, tenant id, hash or URL).
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { MAX_EXPORT_CONCURRENCY, exportAcceptedStages } from "../src/session-ingest/accepted-stage-export.ts";

const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;
const BUCKET = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const KMS = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;

export interface ExportArgs {
  mode: "dry_run" | "execute";
  bucket: string;
  region: string;
  orgs: string[];
  pageSize: number;
  concurrency: number;
  statementTimeoutMs: number;
  kmsKeyArn?: string;
}

const VALUE_FLAGS = new Set(["--bucket", "--region", "--org", "--page-size", "--concurrency", "--statement-timeout-ms", "--kms-key-arn"]);
const BOOL_FLAGS = new Set(["--dry-run", "--execute"]);

/** Strict parsing: the mode is never implied, and nothing unvalidated reaches a host. */
export function parseExportArgs(argv: string[]): ExportArgs {
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
  const kmsKeyArn = one.get("--kms-key-arn");
  if (mode === "execute" && (kmsKeyArn === undefined || !KMS.test(kmsKeyArn))) throw new Error("--execute requires --kms-key-arn (the bucket's KMS key ARN)");
  if (mode === "dry_run" && kmsKeyArn !== undefined) throw new Error("--kms-key-arn is only for --execute");
  return {
    mode, bucket, region, orgs,
    pageSize: int("--page-size", 1, 10_000) ?? 1_000,
    concurrency: int("--concurrency", 1, MAX_EXPORT_CONCURRENCY) ?? 8,
    statementTimeoutMs: int("--statement-timeout-ms", 1_000, 600_000) ?? 30_000,
    ...(kmsKeyArn !== undefined ? { kmsKeyArn } : {}),
  };
}

// A closed, redacted reason only: a pg or S3 error message can carry a host, key or tenant id.
const redactedReason = (e: unknown): string => {
  const code = (e as { code?: unknown } | null)?.code;
  if (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code)) return "sqlstate_" + code;
  if ((e as { name?: unknown })?.name === "OwnerUrlError" && typeof code === "string") return "db_url_" + code;
  const m = (e as Error)?.message;
  return typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "internal_error";
};

if (import.meta.main) {
  let args: ExportArgs;
  try {
    args = parseExportArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${(error as Error).message}\nusage: (--dry-run | --execute --kms-key-arn <arn>) --bucket <name> [--region r] [--org o ...] [--page-size n] [--concurrency n] [--statement-timeout-ms n]; {"dbUrl": …} on stdin; AWS_* credentials in the environment`);
    process.exit(2);
  }
  let client: pg.Client | undefined;
  try {
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) throw new Error("aws_credentials_required");
    const line = readFileSync(0, "utf8").split("\n")[0] ?? "";
    if (line.length > 64 * 1024) throw new Error("stdin_too_large");
    let parsed: unknown;
    try { parsed = JSON.parse(line); } catch { throw new Error("stdin_invalid"); }
    let dbUrl = (parsed as { dbUrl?: unknown } | null)?.dbUrl;
    if (typeof dbUrl !== "string" || !dbUrl || Object.keys(parsed as object).length !== 1) throw new Error("db_url_required");
    client = new pg.Client({ ...ownerClientConfig(dbUrl, "96bulls-accepted-stage-export"), options: "-c default_transaction_read_only=on" });
    dbUrl = "";
    client.on("error", () => {});
    await client.connect();
    const store = createS3Store({
      bucket: args.bucket, region: args.region,
      credentials: { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) },
      ...(args.mode === "execute" ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: args.kmsKeyArn! } : {}),
    }) as unknown as ObjectStore;
    const db = client;
    const summary = await exportAcceptedStages({
      sql: { query: async (text, params) => ({ rows: (await db.query(text, params)).rows }) },
      store, keyRoot: "96bulls", dryRun: args.mode === "dry_run",
      ...(args.orgs.length ? { orgs: args.orgs } : {}),
      pageSize: args.pageSize, concurrency: args.concurrency, statementTimeoutMs: args.statementTimeoutMs,
    });
    process.stdout.write(JSON.stringify(summary) + "\n");
  } catch (e) {
    process.stdout.write(JSON.stringify({ refused: redactedReason(e) }) + "\n");
    process.exitCode = 1;
  } finally {
    await client?.end().catch(() => {});
  }
}
