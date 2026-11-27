/**
 * reshard-verify: the READ-ONLY revision reshard verify for a wave of orgs, as an operator CLI (the same verify the
 * workflow's reshard-dry runs: src/session-ingest/reshard-wave.ts reshardVerifyWave over backlog-authority.ts
 * reshardVerifyOrgs). Not a dispatch mode; it binds nothing and nothing binds it (a marker binds only a reshard-dry
 * receipt from the workflow).
 *
 *   bun scripts/backlog-reshard-verify.ts --bucket <locker> --region <r> [--concurrency n]   < {"orgs": [...]}
 *
 * The orgs come on stdin (one JSON line, capped in bytes while reading), never argv, so no org reaches a process list
 * or a log; 1..64 lowercase org UUIDs, a duplicate refused. The revision home is routed exactly as the workflow's
 * reshard-dry routes it (INGEST_LEDGER_BUCKET / _REGION from the environment, the tool fork guard first). AWS_*
 * credentials from the environment only: run it with the backlog-ops READ-ONLY role. Output: one JSON line, the
 * reshard receipt schema's summary (backlog-authority-schema.ts RESHARD_SCHEMA), or {"refused": <code>}. Exit 0 only
 * when every org verified.
 */
import { parseArgs } from "node:util";
import { BACKLOG_AUTHORITY_KEY_ROOT } from "../src/session-ingest/backlog-authority.ts";
import { authorityReceiptSummary } from "../src/session-ingest/backlog-authority-schema.ts";
import { assertToolLedgerHome, toolLedgerBucket, toolStores } from "../src/session-ingest/ledger-tool-store.ts";
import { assertLedgerHomeGuardValid, LedgerHomeRefused } from "../src/session-ingest/ledger-store.ts";
import { RESHARD_WAVE_CONCURRENCY, reshardVerifyWave } from "../src/session-ingest/reshard-wave.ts";
import { WAVE_MAX_CONCURRENCY, waveOrgList } from "../src/session-ingest/org-wave.ts";
import { readCappedStdin } from "./backlog-sweeper-verify.ts";

const fail = (code: string): never => { throw Object.assign(new Error(code), { code }); };
export const RESHARD_VERIFY_REFUSALS = ["usage", "stdin_invalid", "stdin_too_large", "aws_credentials_required", "wave_orgs_empty", "wave_orgs_oversized",
  "wave_org_invalid", "wave_org_duplicate", "internal_error", "ledger_bucket_invalid", "ledger_home_guard_invalid", "ledger_home_mismatch",
  "ledger_home_missing", "ledger_home_moved", "ledger_home_invalid", "ledger_home_unavailable", "ledger_home_store_error", "access_denied"] as const;

export function parseReshardVerifyArgs(argv: readonly string[]) {
  let values;
  try {
    ({ values } = parseArgs({ args: [...argv], options: { bucket: { type: "string" }, region: { type: "string" }, concurrency: { type: "string" } },
      strict: true, allowPositionals: false }));
  } catch { return fail("usage"); }
  if (typeof values.bucket !== "string" || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u.test(values.bucket) || values.bucket.includes("..")) fail("usage");
  if (typeof values.region !== "string" || !/^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u.test(values.region)) fail("usage");
  const c = values.concurrency === undefined ? RESHARD_WAVE_CONCURRENCY : Number(values.concurrency);
  if (values.concurrency !== undefined && !/^[1-9][0-9]?$/u.test(values.concurrency)) fail("usage");
  if (c > WAVE_MAX_CONCURRENCY) fail("usage");
  return { bucket: values.bucket!, region: values.region!, concurrency: c };
}

/** stdin: exactly {"orgs": [...]} on one line. */
export function parseReshardVerifyStdin(raw: string): string[] {
  const lines = raw.split("\n").filter((l) => l.trim() !== "");
  if (lines.length !== 1) fail("stdin_invalid");
  let p: unknown;
  try { p = JSON.parse(lines[0]!); } catch { return fail("stdin_invalid"); }
  if (!p || typeof p !== "object" || Array.isArray(p) || Object.keys(p).length !== 1 || !Object.hasOwn(p, "orgs")) fail("stdin_invalid");
  try { return waveOrgList((p as { orgs: unknown }).orgs); } catch (e) { return fail((e as { code: string }).code); }
}

export async function main(argv: readonly string[], env: NodeJS.ProcessEnv, stdin: () => string | Promise<string>,
  deps: { stores?: typeof toolStores } = {}): Promise<{ summary: unknown; ok: boolean }> {
  try {
    const a = parseReshardVerifyArgs(argv);
    const orgs = parseReshardVerifyStdin(await stdin());
    const accessKeyId = env.AWS_ACCESS_KEY_ID, secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) fail("aws_credentials_required");
    assertLedgerHomeGuardValid(env);
    const ledgerBucket = toolLedgerBucket(env, { bucket: a.bucket, region: a.region });
    const stores = (deps.stores ?? toolStores)({ env, bucket: a.bucket, region: a.region, keyRoot: BACKLOG_AUTHORITY_KEY_ROOT,
      credentials: { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey!, ...(env.AWS_SESSION_TOKEN ? { sessionToken: env.AWS_SESSION_TOKEN } : {}) }, ledgerBucket });
    await assertToolLedgerHome(stores, BACKLOG_AUTHORITY_KEY_ROOT, a.bucket, env);
    const out = authorityReceiptSummary("reshard", await reshardVerifyWave(stores.store, BACKLOG_AUTHORITY_KEY_ROOT, orgs, a.concurrency));
    return { summary: out.summary, ok: out.ok && (out.summary as { verify_passed?: unknown }).verify_passed === true };
  } catch (error) {
    const c = error instanceof LedgerHomeRefused ? error.code : (error as { code?: unknown })?.code;
    return { summary: { refused: typeof c === "string" && (RESHARD_VERIFY_REFUSALS as readonly string[]).includes(c) ? c : "internal_error" }, ok: false };
  }
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env, () => readCappedStdin(Bun.stdin.stream())).then(({ summary, ok }) => {
    process.stdout.write(JSON.stringify(summary) + "\n");
    process.exitCode = ok ? 0 : 1;
  }, () => { process.stdout.write(JSON.stringify({ refused: "internal_error" }) + "\n"); process.exitCode = 1; });
}
