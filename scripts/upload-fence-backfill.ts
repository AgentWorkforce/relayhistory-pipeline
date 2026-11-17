/**
 * Operator CLI for the one-time S3 ledger backfill (src/session-ingest/upload-fence-backfill.ts,
 * docs/decisions/0005-s3-native-upload-ledger.md "The buffered backlog", step 0): fences, ownership stubs
 * and revision index shards, in #580's ledger-s3 formats, from read-only SQL.
 *
 *   stdin (one JSON line, piped from 1Password, never written to disk):
 *     {"dbUrl": "postgresql://user:password@host:port/db?sslmode=verify-full"}
 *     A read-only login is recommended; every page is its own short READ ONLY transaction (checked with
 *     SHOW transaction_read_only, always rolled back; no long snapshot on the primary), and the session sets
 *     default_transaction_read_only=on.
 *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN] from the environment only.
 *
 *   bun run scripts/upload-fence-backfill.ts --dry-run  --bucket <name> [--region us-east-1] [--org <org> ...]
 *       [--section fences|ownership|revisions ...] [--page-size 500] [--row-page-size 5000] [--concurrency 4]
 *       [--statement-timeout-ms 30000] [--revisions-since-ms <epoch ms>]
 *   bun run scripts/upload-fence-backfill.ts --execute  --bucket <name> --kms-key-arn <arn> [same flags]
 *
 * Exactly one of --dry-run / --execute is required. --dry-run reads SQL and the existing fences and
 * writes nothing (refused in code as well). --execute writes through ledger-s3 (fence CAS edits, ownership
 * stubs, revision shard merges) and needs the owner's explicit OK. Rerunning is a no-op for what is there.
 *
 * --slots-by-org (backlog-ops slots_read=org, any backfill mode): a scoped run reads each org's owned slots once through the org index
 * instead of the heap block windows (upload-fence-backfill.ts SLOTS_ORG_CURSOR), and takes the owned-slot fingerprint from
 * that read instead of a separate FINGERPRINT_SLOTS read.
 * --slot-pace-ms (0..5000, default 0) and --slot-fetch-rows (100..50000; default 5000, or 2000 when paced), only with
 * --slots-by-org: pause between the org cursor's FETCHes and their size; a paced org read held open past 30 min fails
 * closed for that org (slot_read_too_long, its sections incomplete). backlog-ops passes --slot-pace-ms for slot_pace_ms.
 * --revisions-since-ms (a catch-up after a full run; needs the revisions section): only the ingest_record_revisions
 * rows updated at or after that epoch ms are applied; owned lazy slots, fences and ownership are read in full
 * (src/session-ingest/upload-fence-backfill.ts header). The summary records it as `revisions_since_ms`.
 *
 * Output: one JSON line of counts only (never a tenant id, session id, key or URL), with each section's
 * completion (`sections.fences|ownership|revisions`: complete | incomplete | not_run), which the
 * fence-authority marker's `fenceBackfill` / `ownershipBackfill` / `revisionBackfill` record, and the run's input
 * `fingerprint` (counts and content hashes of the revision, ownership and owned-slot rows, read first in one
 * read-only snapshot; with --slots-by-org on a scoped run with a slot pass, the owned-slot values are summed from each
 * org's slot cursor, in that cursor's snapshot; BackfillFingerprint), which the backlog-ops drain compares between a full run and a refresh.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { assertLedgerHomeGuardValid, assertLedgerHomeHere } from "../src/session-ingest/ledger-store.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { BACKFILL_SECTIONS, MAX_BACKFILL_CONCURRENCY, SLOT_FETCH_ROWS_MAX, SLOT_FETCH_ROWS_MIN, SLOT_PACE_MAX_MS, backfillUploadFences, type BackfillSection } from "../src/session-ingest/upload-fence-backfill.ts";

const REGION = /^[a-z]{2}(-[a-z]+)+-\d$/;
const BUCKET = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const KMS = /^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-f-]{36}$/;

export interface BackfillArgs {
  mode: "dry_run" | "execute";
  bucket: string;
  region: string;
  orgs: string[];
  sections: BackfillSection[];
  pageSize: number;
  rowPageSize: number;
  concurrency: number;
  statementTimeoutMs: number;
  kmsKeyArn?: string;
  revisionsSinceMs?: number;
  slotsByOrg?: boolean;
  slotPaceMs?: number;
  slotFetchRows?: number;
}

const VALUE_FLAGS = new Set(["--bucket", "--region", "--org", "--section", "--page-size", "--row-page-size", "--concurrency", "--statement-timeout-ms", "--kms-key-arn", "--revisions-since-ms", "--slot-pace-ms", "--slot-fetch-rows"]);
const BOOL_FLAGS = new Set(["--dry-run", "--execute", "--slots-by-org"]);

/** Strict parsing: the mode is never implied, and nothing unvalidated reaches a host. */
export function parseBackfillArgs(argv: string[]): BackfillArgs {
  const orgs: string[] = [];
  const sections: BackfillSection[] = [];
  const one = new Map<string, string>();
  const bools = new Set<string>();
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (BOOL_FLAGS.has(a)) { bools.add(a); continue; }
    if (!VALUE_FLAGS.has(a)) throw new Error(`unknown flag ${a.startsWith("--") ? a : "(positional)"}`);
    const v = argv[++i];
    if (v === undefined || v.startsWith("--")) throw new Error(`${a} needs a value`);
    if (a === "--org") orgs.push(v);
    else if (a === "--section") {
      if (!BACKFILL_SECTIONS.includes(v as BackfillSection)) throw new Error("--section must be fences, ownership or revisions");
      sections.push(v as BackfillSection);
    }
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
  const pageSize = int("--page-size", 1, 5_000) ?? 500;
  const concurrency = int("--concurrency", 1, MAX_BACKFILL_CONCURRENCY) ?? 4;
  const statementTimeoutMs = int("--statement-timeout-ms", 1_000, 600_000) ?? 30_000;
  const rowPageSize = int("--row-page-size", 1, 50_000) ?? 5_000;
  const kmsKeyArn = one.get("--kms-key-arn");
  if (mode === "execute" && (kmsKeyArn === undefined || !KMS.test(kmsKeyArn))) throw new Error("--execute requires --kms-key-arn (the bucket's KMS key ARN)");
  if (mode === "dry_run" && kmsKeyArn !== undefined) throw new Error("--kms-key-arn is only for --execute");
  const chosen = sections.length ? [...new Set(sections)] : [...BACKFILL_SECTIONS];
  const sinceRaw = one.get("--revisions-since-ms");
  let revisionsSinceMs: number | undefined;
  if (sinceRaw !== undefined) {
    if (!/^[1-9][0-9]{0,15}$/.test(sinceRaw) || !Number.isSafeInteger(Number(sinceRaw))) throw new Error("--revisions-since-ms must be a positive epoch ms");
    if (!chosen.includes("revisions")) throw new Error("--revisions-since-ms needs the revisions section");
    revisionsSinceMs = Number(sinceRaw);
  }
  // Range-checked here and again by the library (backfill_slot_pace_invalid); a "0" pace is accepted and means unpaced.
  const slotPaceMs = int("--slot-pace-ms", 0, SLOT_PACE_MAX_MS);
  const slotFetchRows = int("--slot-fetch-rows", SLOT_FETCH_ROWS_MIN, SLOT_FETCH_ROWS_MAX);
  if ((slotPaceMs !== undefined || slotFetchRows !== undefined) && !bools.has("--slots-by-org")) throw new Error("--slot-pace-ms and --slot-fetch-rows need --slots-by-org");
  return {
    mode, bucket, region, orgs, sections: chosen, pageSize, rowPageSize, concurrency, statementTimeoutMs,
    ...(kmsKeyArn !== undefined ? { kmsKeyArn } : {}),
    ...(revisionsSinceMs !== undefined ? { revisionsSinceMs } : {}),
    ...(bools.has("--slots-by-org") ? { slotsByOrg: true } : {}),
    ...(slotPaceMs !== undefined ? { slotPaceMs } : {}),
    ...(slotFetchRows !== undefined ? { slotFetchRows } : {}),
  };
}

const dbConnectionLost = (e: unknown): boolean => {
  const m = (e as Error)?.message;
  return typeof m === "string" && /Connection terminated|not queryable|connection error/i.test(m);
};

/**
 * The revisions pass merges a whole buffer (minutes of S3 work) between two SQL reads, so the one connection can sit
 * idle far longer than a network path keeps an idle TCP flow (GitHub's hosted runners drop idle outbound flows after
 * about 4 minutes). TCP keepalive plus a SELECT 1 every 30 s while the tool runs keep it in use. A heartbeat can
 * interleave inside a read-only transaction, which is harmless.
 */
export const DB_HEARTBEAT_MS = 30_000;

// A closed, redacted reason only: a pg or S3 error message can carry a host, key or tenant id.
export const redactedReason = (e: unknown): string => {
  const code = (e as { code?: unknown } | null)?.code;
  if (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code)) return "sqlstate_" + code;
  // A dropped database connection (network idle drop, server-side termination): pg's messages, never quoted.
  if (code === "ECONNRESET" || code === "EPIPE" || code === "ETIMEDOUT" || dbConnectionLost(e)) return "db_connection_lost";
  if ((e as { name?: unknown })?.name === "OwnerUrlError" && typeof code === "string") return "db_url_" + code;
  const m = (e as Error)?.message;
  return typeof m === "string" && /^[a-z0-9_]{1,64}$/.test(m) ? m : "internal_error";
};

if (import.meta.main) {
  let args: BackfillArgs;
  try {
    args = parseBackfillArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${(error as Error).message}\nusage: (--dry-run | --execute --kms-key-arn <arn>) --bucket <name> [--region r] [--org o ...] [--section s ...] [--page-size n] [--row-page-size n] [--concurrency n] [--statement-timeout-ms n] [--revisions-since-ms ms] [--slots-by-org [--slot-pace-ms n] [--slot-fetch-rows n]]; {"dbUrl": …} on stdin; AWS_* credentials in the environment`);
    process.exit(2);
  }
  let client: pg.Client | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  try {
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID, secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) throw new Error("aws_credentials_required");
    // A bad INGEST_LEDGER_HOME_GUARD refuses before the database or stdin is touched (ledger_home_guard_invalid).
    assertLedgerHomeGuardValid();
    const line = readFileSync(0, "utf8").split("\n")[0] ?? "";
    if (line.length > 64 * 1024) throw new Error("stdin_too_large");
    let parsed: unknown;
    try { parsed = JSON.parse(line); } catch { throw new Error("stdin_invalid"); }
    let dbUrl = (parsed as { dbUrl?: unknown } | null)?.dbUrl;
    if (typeof dbUrl !== "string" || !dbUrl || Object.keys(parsed as object).length !== 1) throw new Error("db_url_required");
    // Discrete pinned fields only (verified TLS, no PG* env or .pgpass fallback), and read-only by default.
    // Keep the base options (idle_in_transaction_session_timeout) and add read-only by default.
    const base = ownerClientConfig(dbUrl, "96bulls-upload-fence-backfill");
    client = new pg.Client({ ...base, options: `${base.options ?? ""} -c default_transaction_read_only=on`.trim(),
      keepAlive: true, keepAliveInitialDelayMillis: 10_000 });
    dbUrl = "";
    client.on("error", () => {});
    await client.connect();
    const beat = client;
    heartbeat = setInterval(() => { beat.query("SELECT 1").catch(() => {}); }, DB_HEARTBEAT_MS);
    heartbeat.unref?.();
    const store = createS3Store({
      bucket: args.bucket, region: args.region,
      credentials: { accessKeyId, secretAccessKey, ...(process.env.AWS_SESSION_TOKEN ? { sessionToken: process.env.AWS_SESSION_TOKEN } : {}) },
      ...(args.mode === "execute" ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: args.kmsKeyArn! } : {}),
    }) as unknown as ObjectStore;
    // INGEST_LEDGER_HOME_GUARD=on (docs/ledger-relocation.md step 0; default off, no request): a relocated ledger's
    // marker names another bucket, so this bucket's ledger/** objects are stale copies: refuse (a closed code).
    // Only the ownership and revision sections write ledger/** (the fences never move: ledger-store.ts).
    if (args.sections.some((s) => s !== "fences")) await assertLedgerHomeHere(store, "96bulls", args.bucket);
    const db = client;
    const summary = await backfillUploadFences({
      sql: { query: async (text, params) => ({ rows: (await db.query(text, params)).rows }) },
      store, keyRoot: "96bulls", dryRun: args.mode === "dry_run",
      ...(args.orgs.length ? { orgs: args.orgs } : {}),
      sections: args.sections, pageSize: args.pageSize, rowPageSize: args.rowPageSize, concurrency: args.concurrency, statementTimeoutMs: args.statementTimeoutMs,
      ...(args.revisionsSinceMs !== undefined ? { revisionsSinceMs: args.revisionsSinceMs } : {}),
      ...(args.slotsByOrg ? { slotsByOrg: true } : {}),
      ...(args.slotPaceMs !== undefined ? { slotPaceMs: args.slotPaceMs } : {}),
      ...(args.slotFetchRows !== undefined ? { slotFetchRows: args.slotFetchRows } : {}),
    });
    process.stdout.write(JSON.stringify(summary) + "\n");
  } catch (e) {
    process.stdout.write(JSON.stringify({ refused: redactedReason(e) }) + "\n");
    process.exitCode = 1;
  } finally {
    if (heartbeat) clearInterval(heartbeat);
    await client?.end().catch(() => {});
  }
}
