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
 * verdict differs), a create-only `<stage>.superseded.json` for an earlier live sidecar whose stage is no longer a
 * candidate, and the per-org completion objects the drain requires (revoked at the start of every execute run,
 * rewritten only for an org the run ends clean for), under a fenced lease
 * (96bulls/.drain/export-lock.json, one export at a time). The verdict applies the reader's per-user gate (privacy
 * authority, lazy policy, mirrored control head and its receipts, the owner's G2 parity row): a closed gate is not
 * live, counted by reason in `gated`. It refuses to run (`export_claim_sessions_gate_off`) while the v2
 * claim-sessions reader gate is off. The login must be able to run the owner-only owner_buffered_parity_expected.
 * It runs AFTER the fence backfill, and again right before the drain. It needs the owner's explicit OK. Rerunning
 * converges. Before draining: `failed`, `fence_late` and `orgs.incomplete` must all be 0.
 *
 * Path B (default off; docs/decisions/0005-s3-native-upload-ledger.md step 0b): --no-head-parity-org <uuid> (repeatable,
 * lowercase UUIDs, each also given as --org) with --no-head-parity-max-age <seconds | ISO duration like PT2H>, at most
 * 6 h, required with the list and refused without it. The list may instead come on stdin as "noHeadParityOrgs": [...]
 * beside dbUrl (how the backlog-ops workflow passes it, so the org ids never reach argv or a log); never both. For those
 * orgs only, a user with no control head is proven at version 0 by a no_head parity row at most that old (checked at
 * this run's verdict), with no control receipt and no active privacy authority needed (src/session-ingest/
 * accepted-stage-export.ts "path B"). Empty: byte-identical to before.
 *
 * Output: one JSON line of counts only (never a key, tenant id, hash or URL).
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { MAX_EXPORT_CONCURRENCY, NO_HEAD_PARITY_MAX_AGE_SECONDS, exportAcceptedStages } from "../src/session-ingest/accepted-stage-export.ts";

const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;
const BUCKET = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const KMS = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export interface ExportArgs {
  mode: "dry_run" | "execute";
  bucket: string;
  region: string;
  orgs: string[];
  pageSize: number;
  concurrency: number;
  statementTimeoutMs: number;
  kmsKeyArn?: string;
  /** Path B's orgs from argv (the workflow passes them on stdin instead; see resolveNoHeadParity). */
  noHeadParityOrgs: string[];
  /** Path B's maximum parity row age, seconds; null when not given. */
  noHeadParityMaxAgeSeconds: number | null;
}

/** Seconds, or an ISO 8601 duration of hours / minutes / seconds only (PT6H, PT90M, PT1H30M); null when malformed. */
export function parseMaxAge(v: string): number | null {
  if (/^[1-9][0-9]{0,5}$/.test(v)) return Number(v);
  const m = /^PT(?:([0-9]{1,2})H)?(?:([0-9]{1,4})M)?(?:([0-9]{1,6})S)?$/.exec(v);
  if (!m || v === "PT") return null;
  return Number(m[1] ?? 0) * 3600 + Number(m[2] ?? 0) * 60 + Number(m[3] ?? 0);
}

/**
 * Path B's final list and age: argv's or stdin's list (never both), every org a lowercase UUID inside the run's --org
 * scope, the age required (1 s to 6 h) exactly when the list is non-empty. Null: off.
 */
export function resolveNoHeadParity(args: ExportArgs, stdinOrgs: unknown): { orgs: string[]; maxAgeSeconds: number } | null {
  if (stdinOrgs !== undefined && (!Array.isArray(stdinOrgs) || !stdinOrgs.every((o) => typeof o === "string"))) throw new Error("no_head_parity_orgs_invalid");
  if (stdinOrgs !== undefined && args.noHeadParityOrgs.length) throw new Error("no_head_parity_orgs_twice");
  const orgs = (stdinOrgs as string[] | undefined) ?? args.noHeadParityOrgs;
  if (orgs.length === 0) {
    if (args.noHeadParityMaxAgeSeconds !== null) throw new Error("no_head_parity_max_age_without_orgs");
    return null;
  }
  if (!orgs.every((o) => ORG_UUID.test(o)) || new Set(orgs).size !== orgs.length) throw new Error("no_head_parity_orgs_invalid");
  if (!orgs.every((o) => args.orgs.includes(o))) throw new Error("no_head_parity_outside_scope");
  const age = args.noHeadParityMaxAgeSeconds;
  if (age === null) throw new Error("no_head_parity_max_age_required");
  return { orgs: [...orgs].sort(), maxAgeSeconds: age };
}

const VALUE_FLAGS = new Set(["--bucket", "--region", "--org", "--page-size", "--concurrency", "--statement-timeout-ms", "--kms-key-arn",
  "--no-head-parity-org", "--no-head-parity-max-age"]);
const BOOL_FLAGS = new Set(["--dry-run", "--execute"]);

/** Strict parsing: the mode is never implied, and nothing unvalidated reaches a host. */
export function parseExportArgs(argv: string[]): ExportArgs {
  const orgs: string[] = [], nhp: string[] = [];
  const one = new Map<string, string>();
  const bools = new Set<string>();
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (BOOL_FLAGS.has(a)) { bools.add(a); continue; }
    if (!VALUE_FLAGS.has(a)) throw new Error(`unknown flag ${a.startsWith("--") ? a : "(positional)"}`);
    const v = argv[++i];
    if (v === undefined || v.startsWith("--")) throw new Error(`${a} needs a value`);
    if (a === "--org") orgs.push(v);
    else if (a === "--no-head-parity-org") nhp.push(v);
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
  for (const org of nhp) if (!ORG_UUID.test(org)) throw new Error("--no-head-parity-org must be a lowercase org UUID");
  if (new Set(nhp).size !== nhp.length) throw new Error("--no-head-parity-org given twice for one org");
  if (!nhp.every((o) => orgs.includes(o))) throw new Error("--no-head-parity-org must also be given as --org");
  const ageRaw = one.get("--no-head-parity-max-age");
  const age = ageRaw === undefined ? null : parseMaxAge(ageRaw);
  if (ageRaw !== undefined && (age === null || age < 1 || age > NO_HEAD_PARITY_MAX_AGE_SECONDS))
    throw new Error(`--no-head-parity-max-age must be seconds or an ISO duration (PT2H), 1 s to ${NO_HEAD_PARITY_MAX_AGE_SECONDS} s`);
  return {
    mode, bucket, region, orgs, noHeadParityOrgs: nhp, noHeadParityMaxAgeSeconds: age,
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
    console.error(`${(error as Error).message}\nusage: (--dry-run | --execute --kms-key-arn <arn>) --bucket <name> [--region r] [--org o ...] [--page-size n] [--concurrency n] [--statement-timeout-ms n] [--no-head-parity-org uuid ... --no-head-parity-max-age seconds|PT2H]; {"dbUrl": …[, "noHeadParityOrgs": [...]]} on stdin; AWS_* credentials in the environment`);
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
    const keys = parsed && typeof parsed === "object" ? Object.keys(parsed) : [];
    if (typeof dbUrl !== "string" || !dbUrl || !keys.every((k) => k === "dbUrl" || k === "noHeadParityOrgs")) throw new Error("db_url_required");
    const noHeadParity = resolveNoHeadParity(args, (parsed as { noHeadParityOrgs?: unknown }).noHeadParityOrgs);
    // Keep the base options (idle_in_transaction_session_timeout) and add read-only by default; TCP keepalives so a
    // NAT or pooler does not idle the socket out between pages (as upload-fence-backfill.ts).
    const base = ownerClientConfig(dbUrl, "96bulls-accepted-stage-export");
    client = new pg.Client({ ...base, options: `${base.options ?? ""} -c default_transaction_read_only=on`.trim(),
      keepAlive: true, keepAliveInitialDelayMillis: 10_000 });
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
      ...(noHeadParity ? { noHeadParity } : {}),
    });
    process.stdout.write(JSON.stringify(summary) + "\n");
  } catch (e) {
    process.stdout.write(JSON.stringify({ refused: redactedReason(e) }) + "\n");
    process.exitCode = 1;
  } finally {
    await client?.end().catch(() => {});
  }
}
