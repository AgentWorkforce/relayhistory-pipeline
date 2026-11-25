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
 *   SESSION_ERASE_S3_SESSION_TOKEN_FILE     optional mounted STS session token
 *   SESSION_ERASE_S3_SECRET_ACCESS_KEY[_FILE] (ListBucketVersions + DeleteObjectVersion only)
 *   SESSION_ERASE_KEY_ROOT                   object key root, default "96bulls" (= INGEST_KEY_ROOT)
 *   SESSION_ERASE_DRY_RUN                    "1": list and count only
 *   SESSION_ERASE_CONCURRENCY                1..16, default 4
 *   SESSION_ERASE_REQUEST_FILE               absolute path of the request JSON (or `--request <path>`;
 *                                            otherwise stdin). `--request` with no value is refused
 *
 * LOCKER_PROVIDER=azure (Azure Blob; unset or s3 is the S3 path above, unchanged; src/session-erase/azure-target.ts):
 *   SESSION_ERASE_AZURE_ACCOUNT / _CONTAINER / optional _ENDPOINT   the WRITER store exactly (LOCKER_AZURE_ACCOUNT,
 *                                            LOCKER_BUCKET, LOCKER_ENDPOINT must match: azure_target_not_writer_store)
 *   SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE     the Shared Key, a mounted file only; any SESSION_ERASE_S3_* refuses
 *   INGEST_KEY_ROOT                          REQUIRED on azure: the writer's pinned key root (the same value the
 *                                            ingest service runs with, 96bulls by default, set explicitly here);
 *                                            SESSION_ERASE_KEY_ROOT, when set, must equal it (azure_key_root_not_writer)
 *   Execute runs the data-plane preflight first (versioning, container, key, soft delete / permanent delete) and
 *   refuses azure_blob_<reason>; a dry run is list-only, runs no probe and reports preflight "skipped" (it cannot
 *   certify that execute will pass). Unfixable Azure errors mid-run are refusals (exit 2): soft-delete retention, a
 *   forbidden permanent delete, a redirect, a missing container or a forbidden list.
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
import type { AzureBlobEraserConfig } from "../migration/azure-blob-erasure/eraser.ts";
import type { AzureBlobSettings } from "../shared/azure-blob-settings.ts";
import { azureEraseFailureCode, azureErasePreflight, azureEraseRefusal, azureEraseSettings, azureEraseTarget, createAzureEraseEraser, eraseOnAzure, type AzureErasePreflightPorts } from "./azure-target.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { eraseSessionBytes, planTargets, SessionEraseRefused, type SessionEraseBytesRequest, type SessionEraseBytesResult } from "./selfhost-eraser.ts";

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
  /** On Azure, the container. */
  bucket: string;
  /** On Azure, "". */
  region: string;
  endpoint?: string;
  keyRoot: string;
  dryRun: boolean;
  concurrency: number;
  /** LOCKER_PROVIDER=azure only: the erase target (the writer store, matched exactly). Absent on the S3 path. */
  azure?: AzureBlobSettings;
}

/** The provider-independent run settings, read at the same point on both paths. */
function eraseRunSettings(env: NodeJS.ProcessEnv): Pick<SessionEraseConfig, "keyRoot" | "dryRun" | "concurrency"> {
  const keyRoot = env.SESSION_ERASE_KEY_ROOT?.trim() || "96bulls";
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(keyRoot)) refuse("config_invalid");
  const dry = env.SESSION_ERASE_DRY_RUN;
  if (dry !== undefined && dry !== "" && dry !== "0" && dry !== "1") refuse("config_invalid");
  const concurrency = Number(env.SESSION_ERASE_CONCURRENCY ?? "4");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 16) refuse("config_invalid");
  return { keyRoot, dryRun: dry === "1", concurrency };
}

/**
 * LOCKER_PROVIDER=azure's erase config, bound to the writer's key root (the standalone Jobs pass INGEST_KEY_ROOT; the
 * in-service runtime passes the root its ingest service runs with). Every prefix is derived from that root, and a
 * different root on the same container would leave the real bytes untouched while empty prefixes verify, so
 * SESSION_ERASE_KEY_ROOT, when set, must equal it.
 */
export function azureEraseConfig(env: NodeJS.ProcessEnv, writerRoot: string): SessionEraseConfig {
  const azure = azureEraseSettings(env, refuse);
  const run = eraseRunSettings(env);
  if (writerRoot !== writerRoot.trim() || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(writerRoot)) refuse("azure_writer_key_root_invalid");
  const own = env.SESSION_ERASE_KEY_ROOT;
  if (own !== undefined && own !== "" && own.trim() !== writerRoot) refuse("azure_key_root_not_writer");
  return { bucket: azure.container, region: "", ...run, keyRoot: writerRoot, azure };
}

export function sessionEraseConfig(env: NodeJS.ProcessEnv): SessionEraseConfig {
  if (eraseOnAzure(env)) {
    // The standalone Job carries the writer's pinned INGEST_KEY_ROOT explicitly (never assumed to be the default).
    const writerRoot = env.INGEST_KEY_ROOT;
    if (writerRoot === undefined || writerRoot === "") refuse("azure_writer_key_root_missing");
    return azureEraseConfig(env, writerRoot!);
  }
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
  const { keyRoot, dryRun, concurrency } = eraseRunSettings(env);
  return { bucket, region, ...(endpoint ? { endpoint } : {}), keyRoot, dryRun, concurrency };
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
  if (env.SESSION_ERASE_S3_SESSION_TOKEN !== undefined) refuse("secret_invalid");
  if (env.SESSION_ERASE_S3_SESSION_TOKEN_FILE === undefined) return { accessKeyId, secretAccessKey };
  const sessionToken = secret(env, "SESSION_ERASE_S3_SESSION_TOKEN");
  if (sessionToken.length > MAX_SECRET_FILE || /[^!-~]/.test(sessionToken)) refuse("secret_invalid");
  return { accessKeyId, secretAccessKey, sessionToken };
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
  /** LOCKER_PROVIDER=azure test ports: the eraser and the preflight (blob-versioning.ts). */
  azureEraser?: (config: AzureBlobEraserConfig) => PagedEraser;
  preflight?: AzureErasePreflightPorts;
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
): Promise<{ enabled: false } | ({ enabled: true } & SessionEraseBytesResult & AzurePreflightReport)> {
  if (!sessionEraseBytesEnabled(env)) return { enabled: false };
  const cfg = sessionEraseConfig(env);
  const req = parseRequest(await request());
  if (cfg.azure) return runAzure(env, cfg, req, overrides);
  const credentials = sessionEraseCredentials(env);
  const s3: S3EraserConfig = { bucket: cfg.bucket, region: cfg.region, ...(cfg.endpoint ? { endpoint: cfg.endpoint } : {}), credentials };
  const eraser = (overrides.eraser ?? createS3Eraser)(s3);
  const result = await eraseSessionBytes(eraser, req, {
    keyRoot: cfg.keyRoot, bucket: cfg.bucket, dryRun: cfg.dryRun, concurrency: cfg.concurrency,
    ...(overrides.now ? { now: overrides.now } : {}), ...(overrides.log ? { log: overrides.log } : {}),
  });
  return { enabled: true, ...result };
}

/** Azure only: whether the execute preflight ran. A dry run never probes, so it never certifies execute. */
export interface AzurePreflightReport {
  preflight?: "passed" | "skipped";
  /** Present on an Azure dry run: execute refuses unless the preflight passes. */
  executeRequiresPreflight?: true;
}

async function runAzure(env: NodeJS.ProcessEnv, cfg: SessionEraseConfig, req: SessionEraseBytesRequest, overrides: SessionEraseOverrides) {
  const target = azureEraseTarget(env, refuse);
  // Every pure check first: the request's plan (ids, rows, provider, bucket) is validated before the preflight, which
  // writes and deletes a probe blob. An invalid request makes no request at all.
  planTargets(req, { keyRoot: cfg.keyRoot, bucket: cfg.bucket, provider: "azure" });
  if (!cfg.dryRun) await azureErasePreflight(target, cfg.keyRoot, refuse, overrides.preflight);
  const eraser = createAzureEraseEraser(target, overrides.azureEraser);
  let result: SessionEraseBytesResult;
  try {
    result = await eraseSessionBytes(eraser, req, {
      keyRoot: cfg.keyRoot, bucket: cfg.bucket, provider: "azure", dryRun: cfg.dryRun, concurrency: cfg.concurrency,
      ...(overrides.now ? { now: overrides.now } : {}), ...(overrides.log ? { log: overrides.log } : {}),
    });
  } catch (error) {
    const code = azureEraseRefusal(error);
    if (code) refuse(code);
    throw error;
  }
  return { enabled: true as const, ...result, ...(cfg.dryRun ? { preflight: "skipped" as const, executeRequiresPreflight: true as const } : { preflight: "passed" as const }) };
}

/** The printed error code: a refusal's code, a named Azure failure, an error's own code, or a generic one. */
export function errorCodeOf(e: unknown): string {
  if (e instanceof SessionEraseRefused) return e.message;
  return azureEraseFailureCode(e) ?? (typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "session_erase_failed");
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
      console.error(JSON.stringify({ event: "session_erase_bytes", error: errorCodeOf(e) }));
      process.exit(exitCodeForError(e));
    });
}
