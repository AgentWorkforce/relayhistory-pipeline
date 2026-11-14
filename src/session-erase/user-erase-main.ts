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
 *   SESSION_ERASE_S3_BUCKET / _REGION / _ENDPOINT / _KEY_ROOT and SESSION_ERASE_S3_ACCESS_KEY_ID[_FILE] /
 *   SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE]  the transcript bucket and its ERASER key (the only principal the
 *                                              production bucket policy lets delete), as the per-session byte eraser
 *
 * Exit codes: 0 verified (or disabled); 3 not verified yet (rerun the same request: every phase resumes);
 * 2 refused or misconfigured (do not retry unchanged); 4 failed with any other error (rerun).
 * Output: one JSON result line with counts only. Runbook: docs/operator-user-erase.md.
 */
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import pg from "pg";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import { ownerClientConfig, OwnerUrlError } from "../shared/owner-pg-config.ts";
import { sessionEraseConfig, sessionEraseCredentials } from "./selfhost-eraser-main.ts";
import { SessionEraseRefused } from "./selfhost-eraser.ts";
import { parseUserEraseRequest, runUserErase, UserEraseRefused, type UserEraseDb, type UserEraseResult } from "./user-erase.ts";

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
  const s3cfg = sessionEraseConfig(env);
  const settle = settleMs(env);
  const req = parseUserEraseRequest(await request());
  const credentials = sessionEraseCredentials(env);
  const eraser = (overrides.eraser ?? createS3Eraser)({ bucket: s3cfg.bucket, region: s3cfg.region, ...(s3cfg.endpoint ? { endpoint: s3cfg.endpoint } : {}), credentials });
  let client: pg.Client | undefined;
  let db = overrides.db;
  if (!db) {
    // keepAlive: a quiet connection through a stateful firewall/NAT must not lose a result mid-run.
    client = new pg.Client({ ...ownerClientConfig(databaseUrl(env), APPLICATION_NAME), keepAlive: true, keepAliveInitialDelayMillis: 30_000 });
    await client.connect();
    db = client;
  }
  try {
    const result = await runUserErase(db, req, {
      keyRoot: s3cfg.keyRoot, eraser, settleMs: settle,
      ...(overrides.sleep ? { sleep: overrides.sleep } : {}),
      log: overrides.log ?? ((e) => console.error(JSON.stringify(e))),
    });
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
  const code = (e as { code?: unknown })?.code;
  // RH040 (published sessions: use the per-session erase), 22023 (malformed, or the request id belongs to another
  // user), 55000 (another request is active for this user, or the purge found rows it could not remove).
  return code === "RH040" || code === "22023" || code === "55000" ? 2 : 4;
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
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  initScrubEngine().then(() => runUserEraseMain(process.env, request))
    .then((r) => {
      console.log(JSON.stringify(r));
      process.exit(exitCodeForResult(r));
    })
    .catch((e) => {
      const code = e instanceof UserEraseRefused || e instanceof SessionEraseRefused ? e.message
        : e instanceof OwnerUrlError ? e.code
        : typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "user_erase_failed";
      console.error(JSON.stringify({ event: "user_erase", error: code }));
      process.exit(exitCodeForError(e));
    });
}
