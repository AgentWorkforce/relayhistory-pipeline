/**
 * Operator per-user erase: process entry (one-shot Job / CLI, SERVICE=user-erase), DEFAULT OFF.
 *
 *   OPERATOR_USER_ERASE_ENABLED                exactly "1" enables; unset, empty or "0" is off (exits 0 with
 *                                              {"enabled":false}: no config, credential or request is read, no
 *                                              database or store is touched); any other value fails startup
 *   OPERATOR_USER_ERASE_DATABASE_URL[_FILE]    exactly one: the sessions schema OWNER login
 *                                              (postgresql://user:password@host[:port]/db, optional
 *                                              ?sslmode=verify-full; TLS is always verified, src/shared/owner-pg-config.ts)
 *   OPERATOR_USER_ERASE_REQUEST_FILE           absolute path of the request JSON (or `--request <path>`, else stdin):
 *                                              {"request_id","org_id","workspace_id","user_id",
 *                                               "confirmation":"ERASE ALL UPLOADS OF THIS USER"}
 *   OPERATOR_USER_ERASE_SETTLE_MS              wait before each byte verification, default 60000
 *   SUMMARY_DECISION_RECORDS_ENABLED           exactly "true" adds the relayfacts step before begin
 *                                              (sessions.relayfacts_user_erase_v1, 20261105062100); any other value or
 *                                              unset: no relayfacts SQL, result and logs unchanged
 *   SESSION_ERASE_S3_BUCKET / _REGION / _ENDPOINT / _KEY_ROOT and SESSION_ERASE_S3_ACCESS_KEY_ID[_FILE] /
 *   SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE]  the transcript bucket and its ERASER key (the only principal the
 *                                              production bucket policy lets delete), as the per-session byte eraser
 *   LOCKER_PROVIDER=azure                      the user's bytes in Azure Blob instead (session-erase/azure-target.ts):
 *                                              SESSION_ERASE_AZURE_ACCOUNT / _CONTAINER / _ENDPOINT / _ACCOUNT_KEY_FILE,
 *                                              which must be the writer store exactly; the data-plane preflight runs
 *                                              before any SQL (azure_blob_<reason> refusals, exit 2). Azure errors a
 *                                              rerun cannot fix (soft-delete retention, a forbidden permanent delete or
 *                                              list, a missing container, a redirect) are refusals
 *
 * Exit codes: 0 verified (or disabled); 3 not verified yet (rerun the same request: every phase resumes);
 * 2 refused or misconfigured (do not retry unchanged); 4 failed with any other error (rerun). With the relayfacts flag
 * on, the job first checks that sessions.relayfacts_user_erase_v1 exists and refuses relayfacts_migration_missing (2)
 * when 20261105062100 is not applied. A failed relayfacts step prints relayfacts_erase_failed with its SQLSTATE:
 * 2 for 22023 (the request id belongs to another user or workspace, or a malformed id), else 4.
 * Output: one JSON result line with counts only. Runbook: docs/operator-user-erase.md.
 */
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import pg from "pg";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import type { AzureBlobEraserConfig } from "../migration/azure-blob-erasure/eraser.ts";
import { azureEraseFailureCode, azureErasePreflight, azureEraseRefusal, azureEraseTarget, createAzureEraseEraser, type AzureErasePreflightPorts } from "./azure-target.ts";
import { ownerClientConfig, OwnerUrlError } from "../shared/owner-pg-config.ts";
import { sessionEraseConfig, sessionEraseCredentials } from "./selfhost-eraser-main.ts";
import { SessionEraseRefused } from "./selfhost-eraser.ts";
import { parseUserEraseRequest, RelayfactsEraseFailed, relayfactsUserEraseApplied, runUserErase, sqlUserEraseRelayfacts, UserEraseRefused, type UserEraseDb, type UserEraseResult } from "./user-erase.ts";

export const ENABLE_FLAG = "OPERATOR_USER_ERASE_ENABLED";
const APPLICATION_NAME = "96bulls-operator-user-erase";
const MAX_FILE = 1 << 16;
const refuse = (code: string): never => {
  throw new UserEraseRefused(code);
};

export function userEraseEnabled(env: NodeJS.ProcessEnv): boolean {
  const raw = env[ENABLE_FLAG];
  if (raw === undefined || raw === "" || raw === "0") return false;
  if (raw === "1") return true;
  return refuse("flag_invalid");
}

/** The relayfacts step (20261105062100). Same rule as decisionRecordsEnabled in session-analysis/decision-records.ts
 * (not imported: it would pull the extractor into the erase job): exactly "true". */
export function relayfactsUserEraseEnabled(env: NodeJS.ProcessEnv): boolean {
  return env.SUMMARY_DECISION_RECORDS_ENABLED === "true";
}

function readBounded(path: string): string {
  if (!isAbsolute(path)) refuse("file_invalid");
  try {
    const st = statSync(path);
    if (!st.isFile() || st.size < 1 || st.size > MAX_FILE) refuse("file_invalid");
    return readFileSync(path, "utf8");
  } catch (e) {
    if (e instanceof UserEraseRefused) throw e;
    return refuse("file_invalid");
  }
}

export function databaseUrl(env: NodeJS.ProcessEnv): string {
  const value = env.OPERATOR_USER_ERASE_DATABASE_URL, file = env.OPERATOR_USER_ERASE_DATABASE_URL_FILE;
  if (value !== undefined && file !== undefined) refuse("secret_source_ambiguous");
  const text = value ?? (file ? readBounded(file).replace(/\r?\n$/, "") : "");
  if (!text) refuse("database_url_missing");
  return text;
}

export function settleMs(env: NodeJS.ProcessEnv): number {
  const raw = env.OPERATOR_USER_ERASE_SETTLE_MS;
  if (raw === undefined || raw === "") return 60_000;
  if (!/^\d{1,7}$/.test(raw) || Number(raw) > 3_600_000) refuse("config_invalid");
  return Number(raw);
}

export function requestPath(argv: readonly string[], env: NodeJS.ProcessEnv): string | undefined {
  const at = argv.indexOf("--request");
  if (at < 0) return env.OPERATOR_USER_ERASE_REQUEST_FILE || undefined;
  const value = argv[at + 1];
  if (!value || value.startsWith("--")) refuse("request_invalid");
  return value;
}

export interface UserEraseOverrides {
  /** Test-only ports; the process entry never takes them from input. */
  db?: UserEraseDb;
  eraser?: (config: S3EraserConfig) => PagedEraser;
  /** LOCKER_PROVIDER=azure test ports: the eraser and the preflight. */
  azureEraser?: (config: AzureBlobEraserConfig) => PagedEraser;
  preflight?: AzureErasePreflightPorts;
  sleep?: (ms: number) => Promise<void>;
  log?: (event: Record<string, unknown>) => void;
}

/** With the flag off this returns before reading any config, credential or request, or touching a database or store. */
export async function runUserEraseMain(
  env: NodeJS.ProcessEnv,
  request: () => Promise<string> | string,
  overrides: UserEraseOverrides = {},
): Promise<{ enabled: false } | ({ enabled: true } & UserEraseResult)> {
  if (!userEraseEnabled(env)) return { enabled: false };
  // Per-user erasure has no dry run on any backend. Check before config, credentials and request input.
  if (env.SESSION_ERASE_DRY_RUN === "1") refuse("dry_run_unsupported");
  const s3cfg = sessionEraseConfig(env);
  const settle = settleMs(env);
  const req = parseUserEraseRequest(await request());
  let eraser: PagedEraser;
  if (s3cfg.azure) {
    // The writer store exactly, then the data-plane preflight, all before any SQL (begin revokes and fences).
    const target = azureEraseTarget(env, refuse);
    await azureErasePreflight(target, s3cfg.keyRoot, refuse, overrides.preflight);
    eraser = createAzureEraseEraser(target, overrides.azureEraser);
  } else {
    const credentials = sessionEraseCredentials(env);
    eraser = (overrides.eraser ?? createS3Eraser)({ bucket: s3cfg.bucket, region: s3cfg.region, ...(s3cfg.endpoint ? { endpoint: s3cfg.endpoint } : {}), credentials });
  }
  let client: pg.Client | undefined;
  let db = overrides.db;
  if (!db) {
    // keepAlive: a quiet connection through a stateful firewall/NAT must not lose a result mid-run.
    client = new pg.Client({ ...ownerClientConfig(databaseUrl(env), APPLICATION_NAME), keepAlive: true, keepAliveInitialDelayMillis: 30_000 });
    await client.connect();
    db = client;
  }
  try {
    const relayfacts = relayfactsUserEraseEnabled(env);
    // Flag on but 20261105062100 not applied: refuse before any other statement, instead of failing 42883 on
    // every rerun (exit 4) without ever reaching begin.
    if (relayfacts && !(await relayfactsUserEraseApplied(db))) refuse("relayfacts_migration_missing");
    let result: UserEraseResult;
    try {
      result = await runUserErase(db, req, {
        keyRoot: s3cfg.keyRoot, eraser, settleMs: settle,
        ...(relayfacts ? { relayfacts: sqlUserEraseRelayfacts(db) } : {}),
        ...(overrides.sleep ? { sleep: overrides.sleep } : {}),
        log: overrides.log ?? ((e) => console.error(JSON.stringify(e))),
      });
    } catch (error) {
      // Azure only (S3 errors carry no such codes): an error a rerun cannot fix is a refusal.
      const code = s3cfg.azure ? azureEraseRefusal(error) : undefined;
      if (code) refuse(code);
      throw error;
    }
    return { enabled: true, ...result };
  } finally {
    await client?.end().catch(() => {});
  }
}

export function exitCodeForResult(r: { enabled: false } | ({ enabled: true } & UserEraseResult)): 0 | 3 {
  return !r.enabled || r.verified ? 0 : 3;
}

/** 2: refused or misconfigured (never retried unchanged); 4: anything else, possibly partial (rerun). */
export function exitCodeForError(e: unknown): 2 | 4 {
  if (e instanceof UserEraseRefused || e instanceof SessionEraseRefused || e instanceof OwnerUrlError) return 2;
  // The relayfacts step ran before begin: 22023 is a refusal; anything else (RH015, a lock timeout) is rerun.
  if (e instanceof RelayfactsEraseFailed) return e.sqlstate === "22023" ? 2 : 4;
  const code = (e as { code?: unknown })?.code;
  // RH040 (published sessions: use the per-session erase), 22023 (malformed, or the request id belongs to another
  // user), 55000 (another request is active for this user, or the purge found rows it could not remove).
  return code === "RH040" || code === "22023" || code === "55000" ? 2 : 4;
}

/** The one printed error line: a code, plus the SQLSTATE of a failed relayfacts step; never an id or database text. */
export function errorEvent(e: unknown): { event: "user_erase"; error: string; sqlstate?: string } {
  if (e instanceof RelayfactsEraseFailed) return { event: "user_erase", error: e.message, ...(e.sqlstate ? { sqlstate: e.sqlstate } : {}) };
  const code = e instanceof UserEraseRefused || e instanceof SessionEraseRefused ? e.message
    : e instanceof OwnerUrlError ? e.code
    : azureEraseFailureCode(e) ?? (typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "user_erase_failed");
  return { event: "user_erase", error: code };
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += (chunk as Buffer).length;
    if (size > MAX_FILE) refuse("request_invalid");
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

if (import.meta.main) {
  // The path is resolved inside the request thunk, so with the flag off nothing is read.
  const request = () => {
    const path = requestPath(process.argv.slice(2), process.env);
    return path ? readBounded(path) : readStdin();
  };
  runUserEraseMain(process.env, request)
    .then((r) => {
      console.log(JSON.stringify(r));
      process.exit(exitCodeForResult(r));
    })
    .catch((e) => {
      console.error(JSON.stringify(errorEvent(e)));
      process.exit(exitCodeForError(e));
    });
}
