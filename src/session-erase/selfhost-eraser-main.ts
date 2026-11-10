/**
 * Self-host per-session byte eraser: process entry (one-shot Job / CLI), DEFAULT OFF.
 *
 *   SESSION_ERASE_BYTES_ENABLED              exactly "1" enables; unset, empty or "0" is off
 *                                            (exits 0 with {"enabled":false}, the store untouched);
 *                                            any other value fails startup
 *   SESSION_ERASE_S3_BUCKET                  versioned S3-compatible bucket (the ingest bucket)
 *   SESSION_ERASE_S3_REGION                  signing region
 *   SESSION_ERASE_S3_ENDPOINT                bare origin (MinIO etc.); unset = AWS S3. https, or
 *                                            loopback / ALLOW_IN_CLUSTER_HTTP Service http
 *   SESSION_ERASE_S3_ACCESS_KEY_ID[_FILE]    exactly one of the pair; the ERASER key
 *   SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE] (ListBucketVersions + DeleteObjectVersion only)
 *   SESSION_ERASE_KEY_ROOT                   object key root, default "96bulls" (= INGEST_KEY_ROOT)
 *   SESSION_ERASE_DRY_RUN                    "1": list and count only
 *   SESSION_ERASE_CONCURRENCY                1..16, default 4
 *   SESSION_ERASE_REQUEST_FILE               absolute path of the request JSON (or `--request <path>`;
 *                                            otherwise stdin). `--request` with no value is refused
 *
 * Exit codes (`exitCodeForResult` / `exitCodeForError`): 0 verified (or disabled, or a dry run);
 * 3 not verified (rerun); 2 refused or misconfigured (SessionEraseRefused only: do not retry
 * unchanged); 4 failed with any other error (S3 5xx / 403, network, inventory): the run may have
 * deleted part of the session, rerun it.
 * Output: one JSON result line with counts only. Refusal codes never carry a value.
 * See docs/operator-session-erase-selfhost-eraser.md.
 */
import { readFileSync, statSync } from "node:fs";
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { isAbsolute } from "node:path";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { eraseSessionBytes, SessionEraseRefused, type SessionEraseBytesRequest, type SessionEraseBytesResult } from "./selfhost-eraser.ts";

export const ENABLE_FLAG = "SESSION_ERASE_BYTES_ENABLED";
const MAX_FILE = 1 << 20;
const MAX_SECRET_FILE = 65536;
const refuse = (code: string): never => {
  throw new SessionEraseRefused(code);
};

/** Exactly "1" enables. Unset / "" / "0" is off; anything else is a startup error (a typo never means either). */
export function sessionEraseBytesEnabled(env: NodeJS.ProcessEnv): boolean {
  const raw = env[ENABLE_FLAG];
  if (raw === undefined || raw === "" || raw === "0") return false;
  if (raw === "1") return true;
  return refuse("flag_invalid");
}

export interface SessionEraseConfig {
  bucket: string;
  region: string;
  endpoint?: string;
  keyRoot: string;
  dryRun: boolean;
  concurrency: number;
}

export function sessionEraseConfig(env: NodeJS.ProcessEnv): SessionEraseConfig {
  const bucket = env.SESSION_ERASE_S3_BUCKET ?? "", region = env.SESSION_ERASE_S3_REGION ?? "";
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || !/^[a-z0-9-]{1,63}$/.test(region)) refuse("config_invalid");
  let endpoint: string | undefined;
  if (env.SESSION_ERASE_S3_ENDPOINT) {
    try {
      endpoint = assertDestinationUrl("SESSION_ERASE_S3_ENDPOINT", env.SESSION_ERASE_S3_ENDPOINT, destinationPolicy(env), { allowPath: false }).origin;
    } catch {
      refuse("endpoint_invalid");
    }
  }
  const keyRoot = env.SESSION_ERASE_KEY_ROOT?.trim() || "96bulls";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(keyRoot)) refuse("config_invalid");
  const dry = env.SESSION_ERASE_DRY_RUN;
  if (dry !== undefined && dry !== "" && dry !== "0" && dry !== "1") refuse("config_invalid");
  const concurrency = Number(env.SESSION_ERASE_CONCURRENCY ?? "4");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 16) refuse("config_invalid");
  return { bucket, region, ...(endpoint ? { endpoint } : {}), keyRoot, dryRun: dry === "1", concurrency };
}

function readBounded(path: string, max: number): string {
  if (!isAbsolute(path)) refuse("file_invalid");
  let text = "";
  try {
    const st = statSync(path);
    if (!st.isFile() || st.size < 1 || st.size > max) refuse("file_invalid");
    text = readFileSync(path, "utf8");
  } catch (e) {
    if (e instanceof SessionEraseRefused) throw e;
    refuse("file_invalid");
  }
  return text;
}

function secret(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name], file = env[`${name}_FILE`];
  if (value !== undefined && file !== undefined) refuse("secret_source_ambiguous");
  let text = value ?? (file ? readBounded(file, MAX_SECRET_FILE).replace(/\r?\n$/, "") : "");
  // eslint-disable-next-line no-control-regex
  if (!text || /[\u0000-\u001f\u007f]/.test(text)) refuse("secret_invalid");
  return text;
}

export function sessionEraseCredentials(env: NodeJS.ProcessEnv): S3EraserConfig["credentials"] {
  const accessKeyId = secret(env, "SESSION_ERASE_S3_ACCESS_KEY_ID"), secretAccessKey = secret(env, "SESSION_ERASE_S3_SECRET_ACCESS_KEY");
  if (!/^[A-Za-z0-9._~+=-]{3,128}$/.test(accessKeyId) || secretAccessKey.length < 8 || secretAccessKey.length > 256 || /\s/.test(secretAccessKey)) refuse("secret_invalid");
  return { accessKeyId, secretAccessKey };
}

export function parseRequest(text: string): SessionEraseBytesRequest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return refuse("request_invalid");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) refuse("request_invalid");
  return parsed as SessionEraseBytesRequest;
}

export interface SessionEraseOverrides {
  /** Test-only port; the process entry never takes it from input. */
  eraser?: (config: S3EraserConfig) => PagedEraser;
  now?: () => number;
  log?: (line: Record<string, unknown>) => void;
}

/**
 * The entry point. With the flag off it returns `{enabled:false}` before reading any config,
 * credential or request, and before constructing an eraser (the store is never touched).
 */
export async function runSessionEraseBytes(
  env: NodeJS.ProcessEnv,
  request: () => Promise<string> | string,
  overrides: SessionEraseOverrides = {},
): Promise<{ enabled: false } | ({ enabled: true } & SessionEraseBytesResult)> {
  if (!sessionEraseBytesEnabled(env)) return { enabled: false };
  const cfg = sessionEraseConfig(env);
  const req = parseRequest(await request());
  const credentials = sessionEraseCredentials(env);
  const s3: S3EraserConfig = { bucket: cfg.bucket, region: cfg.region, ...(cfg.endpoint ? { endpoint: cfg.endpoint } : {}), credentials };
  const eraser = (overrides.eraser ?? createS3Eraser)(s3);
  const result = await eraseSessionBytes(eraser, req, {
    keyRoot: cfg.keyRoot, bucket: cfg.bucket, dryRun: cfg.dryRun, concurrency: cfg.concurrency,
    ...(overrides.now ? { now: overrides.now } : {}), ...(overrides.log ? { log: overrides.log } : {}),
  });
  return { enabled: true, ...result };
}

/** Exit code of a completed run: 0 verified, disabled or dry run; 3 not verified (rerun). */
export function exitCodeForResult(r: { enabled: false } | ({ enabled: true } & SessionEraseBytesResult)): 0 | 3 {
  return !r.enabled || r.dryRun || r.verified ? 0 : 3;
}

/**
 * Exit code of a failed run: 2 only for a refusal or a config error (every one is a
 * SessionEraseRefused); 4 for anything else (an S3 error, a network error, an inventory
 * error), which may follow partial deletes and must be rerun, never treated as permanent.
 */
export function exitCodeForError(e: unknown): 2 | 4 {
  return e instanceof SessionEraseRefused ? 2 : 4;
}

/** The request file: `--request <path>` (a missing value is refused), else SESSION_ERASE_REQUEST_FILE, else stdin (undefined). */
export function requestPathFrom(argv: readonly string[], env: NodeJS.ProcessEnv): string | undefined {
  const at = argv.indexOf("--request");
  if (at < 0) return env.SESSION_ERASE_REQUEST_FILE || undefined;
  const value = argv[at + 1];
  if (!value || value.startsWith("--")) refuse("request_invalid");
  return value;
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += (chunk as Buffer).length;
    if (size > MAX_FILE) refuse("request_invalid");
    chunks.push(chunk as Buffer);
  }
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text || text.length > MAX_FILE) refuse("request_invalid");
  return text;
}

if (import.meta.main) {
  // The path is resolved inside the request thunk, so with the flag off nothing is parsed.
  const request = () => {
    const path = requestPathFrom(process.argv.slice(2), process.env);
    return path ? readBounded(path, MAX_FILE) : readStdin();
  };
  // SCRUB_ENGINE (docs/scrub-engine.md): every service selects (and for wasm verifies) at startup.
  initScrubEngine()
    .then(() => runSessionEraseBytes(process.env, request))
    .then((r) => {
      console.log(JSON.stringify(r));
      process.exit(exitCodeForResult(r));
    })
    .catch((e) => {
      const code = e instanceof SessionEraseRefused ? e.message : typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "session_erase_failed";
      console.error(JSON.stringify({ event: "session_erase_bytes", error: code }));
      process.exit(exitCodeForError(e));
    });
}
