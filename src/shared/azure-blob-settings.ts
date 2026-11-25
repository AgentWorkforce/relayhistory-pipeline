/**
 * THE parser of the Azure Blob settings that the session-analysis runner (runner.ts, LOCKER_PROVIDER=azure), the
 * read API's analysis source (session-read-api/lambda.ts) and the operator erase jobs (session-erase/*, with the
 * SESSION_ERASE_AZURE_* family) all use, so none of them can validate differently. Shown with the default family:
 *
 *   SESSION_ANALYSIS_AZURE_ACCOUNT           3-24 lowercase letters or digits          azure_account_invalid
 *   SESSION_ANALYSIS_AZURE_CONTAINER         an Azure container name                   azure_container_invalid
 *   SESSION_ANALYSIS_AZURE_ENDPOINT          optional; the shared destination policy   azure_endpoint_invalid,
 *                                            (destination-url.ts), path "" or           allow_in_cluster_http_invalid
 *                                            exactly /<account> (Azurite)
 *   SESSION_ANALYSIS_AZURE_ACCOUNT_KEY_FILE  the Shared Key, from a mounted file only;   azure_account_key_plaintext,
 *                                            strict padded base64 of >= 32 bytes        _missing, _invalid, _unreadable
 *
 * azureNoRedirectFetch is the fetch both give their Azure store (never follow a redirect with the Shared Key).
 *
 * Every failure calls the caller's `fail(code)` (the runner refuses with exit 2; the read API logs a coded warning and
 * leaves analysis unconfigured). Codes only: never a setting's value, and never the key, which is returned behind a
 * closure.
 */
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { fromBase64 } from "../../packages/object-store/src/crypto.ts";
import { assertDestinationUrl, destinationPolicy } from "./destination-url.ts";

export type SettingsFail = (code: string) => never;

export interface AzureBlobSettings {
  account: string;
  container: string;
  /** The blob endpoint as validated (origin, plus /<account> for path-style). Absent: the public Azure endpoint. */
  endpoint?: string;
}

/**
 * The variable family: SESSION_ANALYSIS_AZURE (the runner and the read API, the default) or SESSION_ERASE_AZURE (the
 * operator erase jobs). The rules and the refusal codes are the same for both.
 */
export type AzureSettingsPrefix = "SESSION_ANALYSIS_AZURE" | "SESSION_ERASE_AZURE";
export const DEFAULT_AZURE_SETTINGS_PREFIX: AzureSettingsPrefix = "SESSION_ANALYSIS_AZURE";

/** Account, container and endpoint (no secret is read; the plaintext key variable is refused by presence). */
export function azureBlobSettings(env: NodeJS.ProcessEnv, fail: SettingsFail, prefix: AzureSettingsPrefix = DEFAULT_AZURE_SETTINGS_PREFIX): AzureBlobSettings {
  const account = env[`${prefix}_ACCOUNT`] ?? "";
  if (!/^[a-z0-9]{3,24}$/.test(account)) fail("azure_account_invalid");
  const container = env[`${prefix}_CONTAINER`] ?? "";
  if (!/^(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$/.test(container)) fail("azure_container_invalid");
  const plain = env[`${prefix}_ACCOUNT_KEY`];
  if (plain !== undefined && plain !== "") fail("azure_account_key_plaintext");
  const raw = env[`${prefix}_ENDPOINT`];
  if (raw === undefined || raw === "") return { account, container };
  let policy: ReturnType<typeof destinationPolicy>;
  try { policy = destinationPolicy(env); } catch { return fail("allow_in_cluster_http_invalid"); }
  let url: URL;
  try { url = assertDestinationUrl(`${prefix}_ENDPOINT`, raw, policy); } catch { return fail("azure_endpoint_invalid"); }
  const path = url.pathname.replace(/\/+$/, "");
  if (path !== "" && path !== `/${account}`) fail("azure_endpoint_invalid");
  return { account, container, endpoint: `${url.origin}${path}` };
}

/**
 * One secret from exactly one of `<name>` or `<name>_FILE` (an absolute path to a regular file of 1..65536 bytes; one
 * trailing newline is dropped). Codes `<code>_ambiguous`, `_invalid`, `_unreadable`, `_missing` name the variable's
 * role, never its value.
 */
export function envSecret(env: NodeJS.ProcessEnv, name: string, code: string, fail: SettingsFail): string {
  const value = env[name], file = env[`${name}_FILE`];
  if (value !== undefined && value !== "" && file !== undefined && file !== "") fail(`${code}_ambiguous`);
  let text = value ?? "";
  if (file !== undefined && file !== "") {
    if (!isAbsolute(file)) fail(`${code}_invalid`);
    let bad: string | undefined;
    try {
      const st = statSync(file);
      if (!st.isFile() || st.size < 1 || st.size > 65_536) bad = `${code}_invalid`;
      else text = readFileSync(file, "utf8").replace(/\r?\n$/, "");
    } catch {
      bad = `${code}_unreadable`;
    }
    if (bad) fail(bad);
  }
  if (!text) fail(`${code}_missing`);
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f\s]/.test(text)) fail(`${code}_invalid`);
  return text;
}

/** The Shared Key from <prefix>_ACCOUNT_KEY_FILE only, behind a closure. */
export function azureAccountKey(env: NodeJS.ProcessEnv, fail: SettingsFail, prefix: AzureSettingsPrefix = DEFAULT_AZURE_SETTINGS_PREFIX): { accountKey: () => string } {
  const plain = env[`${prefix}_ACCOUNT_KEY`];
  if (plain !== undefined && plain !== "") fail("azure_account_key_plaintext");
  const file = env[`${prefix}_ACCOUNT_KEY_FILE`];
  if (file === undefined || file === "") fail("azure_account_key_missing");
  const key = envSecret(env, `${prefix}_ACCOUNT_KEY`, "azure_account_key", fail);
  let bytes = 0;
  if (key.length % 4 === 0 && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(key)) {
    try { bytes = fromBase64(key).length; } catch { bytes = 0; }
  }
  if (bytes < 32) fail("azure_account_key_invalid");
  return { accountKey: () => key };
}

/**
 * A fetch for the Azure STORE (createAzureBlobStore) that never follows a redirect: every request is sent with
 * redirect "manual", and a 3xx (or an opaque redirect) is cancelled and thrown as `azure_redirect`, so a Shared Key
 * signed request (a put body included) is never replayed to another location. core.ts `send` wraps the throw as a
 * transient store error whose `cause` carries the code.
 */
export function azureNoRedirectFetch(inner?: typeof fetch): typeof fetch {
  const doFetch = inner ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  return (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const res = await doFetch(input, { ...init, redirect: "manual" });
    if (res.type === "opaqueredirect" || res.redirected || (res.status >= 300 && res.status < 400)) {
      await res.body?.cancel().catch(() => {});
      throw Object.assign(new Error("Azure answered a redirect"), { code: "azure_redirect" });
    }
    return res;
  }) as typeof fetch;
}
