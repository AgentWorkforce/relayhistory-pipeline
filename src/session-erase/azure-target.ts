/**
 * The operator erase jobs on LOCKER_PROVIDER=azure (selfhost-eraser-main.ts, user-erase-main.ts, selfhost-runtime.ts):
 * one target, one preflight and one error classification, so the three entries cannot differ.
 *
 *   target     SESSION_ERASE_AZURE_ACCOUNT / _CONTAINER / optional _ENDPOINT / _ACCOUNT_KEY_FILE, through THE shared
 *              parser (src/shared/azure-blob-settings.ts, prefix SESSION_ERASE_AZURE). Any SESSION_ERASE_S3_* setting
 *              alongside refuses (azure_s3_settings_conflict). The target must be the WRITER store, exactly:
 *              LOCKER_AZURE_ACCOUNT, LOCKER_BUCKET and LOCKER_ENDPOINT (the Locker's own settings, locker-selection.ts)
 *              must name the same account, container and endpoint (unset endpoint on both, or the same normalized
 *              one), or azure_target_not_writer_store, before any SQL, request or delete. The Locker's key is not
 *              read.
 *   preflight  execute only (a dry run is strictly read-only and reports preflight "skipped"): the runner's data-plane
 *              proof (session-analysis/blob-versioning.ts) under <keyRoot>/.runner-probe/session-erase/, in its own
 *              <uuid>/ directory with age-gated leftover cleanup, so concurrent erase runs and the runner never touch
 *              each other's probe. Refusals azure_blob_<verdict> (exit 2); a transient failure throws
 *              erase_blob_versioning_unavailable (exit 4, rerun).
 *   codes      bare (the per-session job's SessionEraseRefused message adds its session_erase_ prefix; the per-user
 *              job prints them as they are).
 *   errors     classified by the operation that failed (the Azure eraser tags list / delete): a list 404 is the
 *              container missing and a list 403 a forbidden key (refusals); soft-delete retention, a forbidden
 *              permanent delete and a redirect are refusals. A delete 404 never reaches here (the eraser treats it as
 *              already gone); a delete 403 stays a failure (erase_azure_delete_forbidden, exit 4). Nothing is ever
 *              reported as absent because a request failed.
 */
import { azureAccountKey, azureBlobSettings, type AzureBlobSettings, type SettingsFail } from "../shared/azure-blob-settings.ts";
import { createAzureBlobEraser, type AzureBlobEraserConfig } from "../migration/azure-blob-erasure/eraser.ts";
import type { PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";
import { probeBlobVersioning, type BlobVersioningCheck, type BlobVersioningVerdict } from "../session-analysis/blob-versioning.ts";

/** LOCKER_PROVIDER=azure selects the Azure erase path; unset, "s3" or anything else keeps the S3 path unchanged. */
export function eraseOnAzure(env: NodeJS.ProcessEnv): boolean {
  return env.LOCKER_PROVIDER?.trim() === "azure";
}

const S3_ERASE_VARS = ["SESSION_ERASE_S3_BUCKET", "SESSION_ERASE_S3_REGION", "SESSION_ERASE_S3_ENDPOINT",
  "SESSION_ERASE_S3_ACCESS_KEY_ID", "SESSION_ERASE_S3_SECRET_ACCESS_KEY",
  "SESSION_ERASE_S3_ACCESS_KEY_ID_FILE", "SESSION_ERASE_S3_SECRET_ACCESS_KEY_FILE",
  "SESSION_ERASE_S3_SESSION_TOKEN", "SESSION_ERASE_S3_SESSION_TOKEN_FILE"] as const;

const INVALID = Symbol("invalid writer endpoint");

/**
 * The Locker writer's endpoint as the shared parser normalizes one (origin plus an optional /<account>), or INVALID.
 * The writer's own rule (locker-selection.ts trustedAzureEndpoint) is applied here too, explicitly: https anywhere or
 * http to loopback only, and never userinfo, a query or a fragment. Nothing is dropped before the comparison, so a
 * malformed writer URL can never compare equal to the erase target.
 */
function normalizedEndpoint(raw: string | undefined, account: string): string | null | typeof INVALID {
  if (raw === undefined || raw === "") return null;
  if (raw !== raw.trim() || /[?#@\s]/.test(raw)) return INVALID;
  let url: URL;
  try { url = new URL(raw); } catch { return INVALID; }
  if (url.username || url.password || url.search || url.hash) return INVALID;
  const host = url.hostname.toLowerCase();
  const loopback = host === "localhost" || host === "[::1]" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) return INVALID;
  const path = url.pathname.replace(/\/+$/, "");
  return path === "" || path === `/${account}` ? `${url.origin}${path}` : INVALID;
}

/** The writer's LOCKER_ENDPOINT normalized (null when unset), or undefined when it breaks the writer's rule. */
export function writerAzureEndpoint(raw: string | undefined, account: string): string | null | undefined {
  const e = normalizedEndpoint(raw, account);
  return e === INVALID ? undefined : e;
}

/** The settings only (no secret): conflicts, the shared parser, and the exact writer-store match. */
export function azureEraseSettings(env: NodeJS.ProcessEnv, fail: SettingsFail): AzureBlobSettings {
  if (S3_ERASE_VARS.some((name) => env[name] !== undefined && env[name] !== "")) fail("azure_s3_settings_conflict");
  const settings = azureBlobSettings(env, fail, "SESSION_ERASE_AZURE");
  // Exact match against the writer's own settings. The Locker's key (LOCKER_AZURE_ACCOUNT_KEY) is never read here:
  // the erase job holds only its own key file.
  const writerAccount = env.LOCKER_AZURE_ACCOUNT?.trim() ?? "";
  const writerEndpoint = normalizedEndpoint(env.LOCKER_ENDPOINT, writerAccount);
  if (writerAccount !== settings.account || (env.LOCKER_BUCKET ?? "") !== settings.container
      || writerEndpoint === INVALID || writerEndpoint !== (settings.endpoint ?? null))
    fail("azure_target_not_writer_store");
  return settings;
}

export interface AzureEraseTarget {
  readonly settings: AzureBlobSettings;
  readonly accountKey: () => string;
}

/**
 * Settings plus the key (SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE only, behind a closure). It parses the settings again on
 * purpose: sessionEraseConfig validates them (no secret) at config time, and this is where the key is read; the parse
 * is pure, so both see the same result.
 */
export function azureEraseTarget(env: NodeJS.ProcessEnv, fail: SettingsFail): AzureEraseTarget {
  const settings = azureEraseSettings(env, fail);
  const { accountKey } = azureAccountKey(env, fail, "SESSION_ERASE_AZURE");
  return Object.freeze({ settings, accountKey });
}

export function azureEraserConfig(target: AzureEraseTarget, fetchImpl?: typeof fetch): AzureBlobEraserConfig {
  const { account, container, endpoint } = target.settings;
  return { account, container, ...(endpoint ? { endpoint } : {}), accountKey: target.accountKey, ...(fetchImpl ? { fetch: fetchImpl } : {}) };
}

export function createAzureEraseEraser(target: AzureEraseTarget, make: (config: AzureBlobEraserConfig) => PagedEraser = createAzureBlobEraser): PagedEraser {
  return make(azureEraserConfig(target));
}

/** The erase jobs' private probe directory under the key root (never a session or org key: a dot segment). */
export function eraseProbePrefix(keyRoot: string): string {
  return `${keyRoot.replace(/^\/+|\/+$/g, "")}/.runner-probe/session-erase/`;
}

export interface AzureErasePreflightPorts {
  probe?: (check: BlobVersioningCheck) => Promise<BlobVersioningVerdict>;
  eraserFactory?: (config: AzureBlobEraserConfig) => PagedEraser;
  fetch?: typeof fetch;
}

/** Execute-only: the data-plane proof. Refuses by `fail(azure_blob_<verdict>)`; transient failures throw (exit 4). */
export async function azureErasePreflight(target: AzureEraseTarget, keyRoot: string, fail: SettingsFail, ports: AzureErasePreflightPorts = {}): Promise<void> {
  let verdict: BlobVersioningVerdict;
  try {
    verdict = await (ports.probe ?? probeBlobVersioning)({
      ...azureEraserConfig(target, ports.fetch), prefix: eraseProbePrefix(keyRoot),
      ...(ports.eraserFactory ? { eraserFactory: ports.eraserFactory } : {}),
    });
  } catch (error) {
    if ((error as { code?: string })?.code === "blob_versioning_unavailable")
      throw Object.assign(new Error("erase_blob_versioning_unavailable"), { code: "erase_blob_versioning_unavailable" });
    return fail("azure_blob_versioning_unverified");
  }
  if (verdict !== "Enabled") fail(`azure_blob_${verdict === "redirect" ? "versioning_redirect" : verdict}`);
}

/**
 * The in-service worker's preflight, OFF the ingest startup path (an operator-only feature must never block uploads):
 * run lazily before the first byte work of the process, cached once it passes, and run again after any Azure refusal
 * mid-run. While it fails, `gate` throws (the worker records the pass as failed_retryable and erases nothing) and one
 * coded line is logged: "operator erase refused: <code>". The pure checks (config, writer store, key file, root) stay
 * at startup, as on S3.
 */
export function createAzurePreflightGate(target: AzureEraseTarget, keyRoot: string,
  log: (fields: Record<string, unknown>) => void = () => {}, ports: AzureErasePreflightPorts = {}) {
  let passed = false;
  const ensure = async () => {
    if (passed) return;
    try {
      await azureErasePreflight(target, keyRoot, (code) => { throw Object.assign(new Error(`operator erase refused: ${code}`), { code }); }, ports);
      passed = true;
    } catch (error) {
      log({ event: "operator_session_erase_preflight", outcome: "refused", reason: (error as { code?: string })?.code ?? "unavailable" });
      throw error;
    }
  };
  return {
    async gate<T>(work: () => Promise<T>): Promise<T> {
      await ensure();
      try { return await work(); }
      catch (error) { if (azureEraseRefusal(error)) passed = false; throw error; }
    },
    get passed() { return passed; },
  };
}

/**
 * A mid-run Azure error that a rerun cannot fix: its refusal code (exit 2). Classified by code AND operation; anything
 * else (a 5xx, a network failure, a delete 403, an inventory error) returns undefined and stays a failure (exit 4).
 */
export function azureEraseRefusal(error: unknown): string | undefined {
  const e = error as { code?: unknown; op?: unknown } | null;
  const code = typeof e?.code === "string" ? e.code : "";
  if (code === "azure_soft_delete_retained") return "azure_blob_soft_delete_retained";
  if (code === "azure_permanent_delete_forbidden") return "azure_blob_permanent_delete_forbidden";
  if (code === "azure_redirect") return "azure_redirect_refused";
  if (e?.op === "list" && code === "azure_404") return "azure_blob_container_missing";
  if (e?.op === "list" && code === "azure_403") return "azure_forbidden";
  return undefined;
}

/** The printed code of an Azure failure that stays exit 4 (a delete 403 is named, for a revoked or rotated key). */
export function azureEraseFailureCode(error: unknown): string | undefined {
  const e = error as { code?: unknown; op?: unknown } | null;
  if (e?.op === "delete" && e.code === "azure_403") return "erase_azure_delete_forbidden";
  return undefined;
}
