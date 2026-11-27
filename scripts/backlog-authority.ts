/**
 * The backlog-ops authority tool (contract: src/session-ingest/backlog-authority.ts; run only by scripts/backlog-ops.ts
 * with fixed argv, an allowlisted environment and its secrets on stdin):
 *
 *   bun scripts/backlog-authority.ts reshard-verify --bucket <locker> --region <r> --org <org> ...
 *       read-only: the revision reshard's verify per org, against the revision home (INGEST_LEDGER_BUCKET / _REGION
 *       routed as the edge routes them; the tool fork guard first). No stdin.
 *   bun scripts/backlog-authority.ts evidence --org <org> ...
 *       stdin {"dbUrl": ..., "cloudflareToken": ...}: the unbound live evidence (authority-dry).
 *   bun scripts/backlog-authority.ts execute --bucket <locker> --region <r> --kms-key-arn <arn> --org <org> ...
 *       stdin {"dbUrl": ..., "cloudflareToken": ..., "baseline": {...}, "ledgerBucket": <name> | null}: the bound
 *       evidence, the sweeper, then the markers (authority-execute). Refused unless BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED.
 *
 * AWS_* credentials from the environment only. Every SQL statement runs in its own REPEATABLE READ READ ONLY
 * transaction (checked by the evidence module) on a session with default_transaction_read_only=on. Output: one JSON
 * line, counts, codes and hashes only; refusals as {"refused": <code>}.
 */
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import pg from "pg";
import { createS3Store, type ObjectStore } from "../src/shared/vendor.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { AUTHORITY_EVIDENCE, authorityEvidence, authorityOrgs, type AuthorityEvidenceSources } from "../src/session-ingest/authority-evidence.ts";
import { BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED } from "../src/session-ingest/backlog-ops.ts";
import {
  AUTHORITY_TOOL_REFUSALS,
  BACKLOG_AUTHORITY_KEY_ROOT,
  CLOUDFLARE_TOKEN,
  authorityExecute,
  cloudflareGet,
  reshardVerifyOrgs,
  toReceiptShape,
} from "../src/session-ingest/backlog-authority.ts";
import { assertToolLedgerHome, toolLedgerBucket, toolStores } from "../src/session-ingest/ledger-tool-store.ts";
import { assertLedgerHomeGuardValid, LedgerHomeRefused } from "../src/session-ingest/ledger-store.ts";

const STATEMENT_TIMEOUT_MS = 600_000;
const fail = (code: string): never => { throw Object.assign(new Error(code), { code }); };

export interface AuthorityArgs { command: "reshard-verify" | "evidence" | "execute"; bucket?: string; region?: string; kmsKeyArn?: string; orgs: string[] }
export function parseAuthorityArgs(argv: readonly string[]): AuthorityArgs {
  const [command, ...rest] = argv;
  if (command !== "reshard-verify" && command !== "evidence" && command !== "execute") fail("usage");
  let values;
  try {
    ({ values } = parseArgs({ args: [...rest], options: { bucket: { type: "string" }, region: { type: "string" }, "kms-key-arn": { type: "string" },
      org: { type: "string", multiple: true } }, strict: true, allowPositionals: false }));
  } catch { return fail("usage"); }
  const orgs = authorityOrgsOr(values.org ?? []);
  const s3 = command !== "evidence";
  if (s3 !== (values.bucket !== undefined) || s3 !== (values.region !== undefined) || (command === "execute") !== (values["kms-key-arn"] !== undefined)) fail("usage");
  if (s3 && (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(values.bucket!) || !/^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u.test(values.region!))) fail("usage");
  if (command === "execute" && !/^arn:aws:kms:[a-z0-9-]+:\d{12}:key\/[0-9a-z-]{36,40}$/u.test(values["kms-key-arn"]!)) fail("usage");
  return { command, orgs, ...(s3 ? { bucket: values.bucket!, region: values.region! } : {}), ...(values["kms-key-arn"] ? { kmsKeyArn: values["kms-key-arn"] } : {}) };
}
function authorityOrgsOr(orgs: readonly string[]): string[] {
  try { return authorityOrgs(orgs); } catch { return fail("orgs"); }
}

/** The stdin payload: exactly the keys `keys`, each checked. */
export function authorityStdin(line: string, command: AuthorityArgs["command"]): { dbUrl: string; cloudflareToken: string; baseline?: unknown; ledgerBucket?: string | null } {
  if (line.length > 4 * 1024 * 1024) fail("stdin_too_large");
  let p: unknown;
  try { p = JSON.parse(line); } catch { return fail("stdin_invalid"); }
  const keys = command === "execute" ? ["dbUrl", "cloudflareToken", "baseline", "ledgerBucket"] : ["dbUrl", "cloudflareToken"];
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.keys(p).length !== keys.length || !keys.every((k) => Object.hasOwn(p as object, k))) fail("stdin_invalid");
  const o = p as Record<string, unknown>;
  if (typeof o.dbUrl !== "string" || o.dbUrl === "") fail("db_url_required");
  if (typeof o.cloudflareToken !== "string" || !CLOUDFLARE_TOKEN.test(o.cloudflareToken)) fail("cloudflare_token_invalid");
  if (command === "execute" && (o.ledgerBucket !== null && (typeof o.ledgerBucket !== "string" || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(o.ledgerBucket))))
    fail("stdin_invalid");
  return o as { dbUrl: string; cloudflareToken: string; baseline?: unknown; ledgerBucket?: string | null };
}

/** The SQL transport: each statement alone in a REPEATABLE READ READ ONLY transaction, one at a time. */
export function readOnlySql(client: { query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }> }): AuthorityEvidenceSources["sql"] {
  let tail: Promise<unknown> = Promise.resolve();
  return (text, params) => {
    const run = tail.then(async () => {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      try {
        await client.query(`SET LOCAL statement_timeout = ${STATEMENT_TIMEOUT_MS}`);
        return (await client.query(text, [...params])).rows;
      } finally {
        await client.query("ROLLBACK").catch(() => {});
      }
    });
    tail = run.catch(() => {});
    return run;
  };
}

function awsCredentials(env: NodeJS.ProcessEnv) {
  const accessKeyId = env.AWS_ACCESS_KEY_ID, secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) fail("aws_credentials_required");
  return { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey!, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) };
}

export async function main(argv: readonly string[], env: NodeJS.ProcessEnv): Promise<{ summary: unknown; ok: boolean }> {
  const args = parseAuthorityArgs(argv);
  if (args.command === "execute" && BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED !== true) fail("authority_execute_held");
  if (args.command === "reshard-verify") {
    assertLedgerHomeGuardValid(env);
    const ledgerBucket = toolLedgerBucket(env, { bucket: args.bucket!, region: args.region! });
    const stores = toolStores({ env, bucket: args.bucket!, region: args.region!, keyRoot: BACKLOG_AUTHORITY_KEY_ROOT, credentials: awsCredentials(env), ledgerBucket });
    await assertToolLedgerHome(stores, BACKLOG_AUTHORITY_KEY_ROOT, args.bucket!, env);
    const summary = await reshardVerifyOrgs(stores.store, BACKLOG_AUTHORITY_KEY_ROOT, args.orgs);
    return { summary, ok: summary.verify_passed };
  }
  const input = authorityStdin(readFileSync(0, "utf8").split("\n")[0] ?? "", args.command);
  const base = ownerClientConfig(input.dbUrl, "96bulls-backlog-authority");
  const client = new pg.Client({ ...base, options: `${base.options ?? ""} -c default_transaction_read_only=on`.trim(), keepAlive: true });
  input.dbUrl = "";
  client.on("error", () => {});
  await client.connect();
  try {
    const sources: AuthorityEvidenceSources = {
      cfGet: cloudflareGet(AUTHORITY_EVIDENCE.accountId, input.cloudflareToken), sql: readOnlySql(client),
      nowMs: () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    };
    if (args.command === "evidence") {
      const report = await authorityEvidence(args.orgs, sources);
      // Ready to bind (unbound, every live check passed) is the only success of a dry run.
      return { summary: { mode: "dry_run", evidence: toReceiptShape(report) }, ok: report.status === "authority_evidence_unbound" };
    }
    const store = createS3Store({ bucket: args.bucket!, region: args.region!, credentials: awsCredentials(env),
      serverSideEncryption: "aws:kms", sseKmsKeyId: args.kmsKeyArn! }) as unknown as ObjectStore;
    const summary = await authorityExecute({ orgs: args.orgs, sources, baseline: input.baseline as never, store, keyRoot: BACKLOG_AUTHORITY_KEY_ROOT,
      bucket: args.bucket!, ledgerBucket: input.ledgerBucket ?? null, now: () => Date.now() });
    return { summary, ok: summary.status === "authority_written" };
  } finally {
    await client.end().catch(() => {});
  }
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env)
    .then(({ summary, ok }) => { process.stdout.write(JSON.stringify(summary) + "\n"); process.exitCode = ok ? 0 : 1; })
    .catch((error) => {
      // A closed set only: never a message (a store, pg or fetch error's text can be code-shaped).
      // (An exact match of a message against the closed list is safe: e.g. toolLedgerBucket's ledger_bucket_invalid.)
      const c = error instanceof LedgerHomeRefused ? error.code : (error as { code?: unknown })?.code ?? (error as Error)?.message;
      process.stdout.write(JSON.stringify({ refused: typeof c === "string" && (AUTHORITY_TOOL_REFUSALS as readonly string[]).includes(c) ? c : "internal_error" }) + "\n");
      process.exitCode = 1;
    });
}
