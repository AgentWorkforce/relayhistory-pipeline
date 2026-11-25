/**
 * The session-analysis runner's startup versioning proof for LOCKER_PROVIDER=azure (runner.ts). Blob versioning is
 * an account (ARM) setting the data plane cannot read, so it is proven on the data plane, with the runner's own key,
 * through the very eraser the sweeper uses:
 *
 *   1. Erase any earlier probe leftovers under the reserved prefix <PIPELINE_KEY_PREFIX>/.runner-probe/session-analysis/
 *      (a pod killed mid-probe leaves at most one probe blob; a dot segment is never an org or a session key).
 *   2. PUT one small create-only probe blob there. The answer must carry x-ms-version-id in Azure's timestamp shape:
 *      none is `not_versioned` (the probe is then deleted with a plain conditional delete, best effort).
 *   3. List it (the inventory must see it), erase it through the strict Azure eraser (createAzureBlobEraser) and
 *      verifyErased must report erased. On a soft-delete account the deleted version is retained, so erasure needs
 *      the account's permanent delete; refused, the verdict is `soft_delete_retained`.
 *
 * Verdicts mirror bucket-versioning.ts: "Enabled" passes; `not_versioned`, `unverified`, `redirect` (any 3xx, never
 * followed), `soft_delete_retained` and `auth_refused` (403) refuse. A 5xx, a network failure or a timeout throws a
 * plain coded error `blob_versioning_unavailable` (transient). Errors carry codes only, never the account, the
 * container, a key or a body.
 */
import { azureSharedKeyAuthorization, AZURE_API_VERSION } from "../../packages/object-store/src/azure.ts";
import { encodeKeyPath, rfc3986 } from "../../packages/object-store/src/crypto.ts";
import { createAzureBlobEraser, type AzureBlobEraserConfig } from "../migration/azure-blob-erasure/eraser.ts";
import type { EraseCheckpoint, ListedVersion, PagedEraser } from "../migration/neon-s3-erasure/eraser.ts";

export type BlobVersioningVerdict =
  | "Enabled" | "not_versioned" | "unverified" | "redirect" | "soft_delete_retained" | "permanent_delete_forbidden"
  | "auth_refused" | "container_missing";

export interface BlobVersioningCheck {
  account: string;
  container: string;
  endpoint?: string;
  accountKey: () => string;
  /** The reserved probe prefix (runnerProbePrefix), ending in "/". */
  prefix: string;
  fetch?: typeof fetch;
  timeoutMs?: number;
  /** Test port; production is createAzureBlobEraser over the same settings. */
  eraserFactory?: (config: AzureBlobEraserConfig) => PagedEraser;
  /** Test port for the probe's unique segment. */
  id?: () => string;
  /** Test port for the clock that ages leftovers. */
  now?: () => number;
}

/**
 * Leftovers older than this are erased before the probe (a pod killed mid-probe). A younger probe blob is another
 * runner's probe in flight (each probe writes its own <uuid>/ directory) and is never touched, so runners that
 * start together cannot delete each other's probe.
 */
export const PROBE_LEFTOVER_AGE_MS = 15 * 60_000;

/** The reserved probe directory under the runner's key root (never an org: a dot segment). */
export function runnerProbePrefix(keyPrefix: string): string {
  return `${keyPrefix.replace(/^\/+|\/+$/g, "")}/.runner-probe/session-analysis/`;
}

/** Azure version ids are UTC timestamps with up to 7 fractional digits. */
export const AZURE_VERSION_ID = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?Z$/;
const PROBE_BODY = new TextEncoder().encode("session-analysis versioning probe\n");
const SCOPE = "session-analysis-versioning-probe";
const MAX_PASSES = 3;
const MAX_STEPS = 10;

const transient = () => Object.assign(new Error("blob_versioning_unavailable"), { code: "blob_versioning_unavailable" });

class Verdict extends Error {
  constructor(readonly verdict: BlobVersioningVerdict) { super(verdict); }
}

/** Map an eraser failure to a verdict, or rethrow a transient one. */
function fromEraser(error: unknown): never {
  const code = (error as { code?: string })?.code ?? "";
  if (code === "azure_soft_delete_retained") throw new Verdict("soft_delete_retained");
  if (code === "azure_permanent_delete_forbidden") throw new Verdict("permanent_delete_forbidden");
  if (code === "azure_404") throw new Verdict("container_missing");
  if (code === "azure_redirect") throw new Verdict("redirect");
  if (code === "azure_403") throw new Verdict("auth_refused");
  if (code === "azure_unavailable" || /^azure_5\d\d$/.test(code)) throw transient();
  throw new Verdict("unverified");
}

/** eraseStep to traversal and verifyErased, up to MAX_PASSES (a soft-deleted version needs a second pass). */
async function eraseAndVerify(eraser: PagedEraser, prefix: string, exactKey?: string, select?: (v: ListedVersion) => boolean): Promise<void> {
  const authorize = async () => {};
  const exact = { ...(exactKey ? { exactKey } : {}), ...(select ? { select } : {}) };
  try {
    for (let pass = 0; pass < MAX_PASSES; pass++) {
      let checkpoint: EraseCheckpoint | null = null;
      for (let step = 0; ; step++) {
        if (step >= MAX_STEPS) throw new Verdict("unverified");
        const r = await eraser.eraseStep(prefix, { scope: SCOPE, authorize, checkpoint, ...exact });
        if (r.status === "traversed") break;
        checkpoint = r.checkpoint;
      }
      const proof = await eraser.verifyErased(prefix, { authorize, ...exact });
      if (proof.status === "erased") return;
    }
  } catch (error) {
    if (error instanceof Verdict) throw error;
    fromEraser(error);
  }
  throw new Verdict("unverified");
}

export async function probeBlobVersioning(cfg: BlobVersioningCheck): Promise<BlobVersioningVerdict> {
  if (!cfg.prefix.endsWith("/") || !cfg.prefix.includes("/.runner-probe/")) return "unverified";
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const eraserConfig: AzureBlobEraserConfig = {
    account: cfg.account, container: cfg.container, ...(cfg.endpoint ? { endpoint: cfg.endpoint } : {}),
    accountKey: cfg.accountKey, fetch: doFetch, ...(cfg.timeoutMs ? { timeoutMs: cfg.timeoutMs } : {}),
  };
  const eraser = (cfg.eraserFactory ?? createAzureBlobEraser)(eraserConfig);
  const base = (cfg.endpoint ?? `https://${cfg.account}.blob.core.windows.net`).replace(/\/+$/, "");
  const signed = async (method: "PUT" | "DELETE", key: string, extra: Record<string, string>, body?: Uint8Array): Promise<Response> => {
    const url = new URL(`${base}/${rfc3986(cfg.container)}/${encodeKeyPath(key)}`);
    const headers = new Headers(extra);
    headers.set("x-ms-version", AZURE_API_VERSION);
    headers.set("x-ms-date", new Date().toUTCString());
    headers.set("authorization", await azureSharedKeyAuthorization(cfg.account, cfg.accountKey(), method, url, headers, body?.length ?? 0));
    let res: Response;
    try {
      res = await doFetch(url, { method, headers, redirect: "manual", signal: AbortSignal.timeout(cfg.timeoutMs ?? 10_000),
        ...(body ? { body: body as Uint8Array<ArrayBuffer> } : {}) });
    } catch {
      throw transient();
    }
    await res.body?.cancel().catch(() => {});
    return res;
  };
  try {
    // 1. Leftovers of an earlier, interrupted probe: only copies older than PROBE_LEFTOVER_AGE_MS (an unknown age is
    //    kept), never another runner's probe in flight.
    const cutoff = (cfg.now ?? Date.now)() - PROBE_LEFTOVER_AGE_MS;
    await eraseAndVerify(eraser, cfg.prefix, undefined, (v) => v.lastModified !== undefined && v.lastModified < cutoff);
    // 2. The probe write.
    const dir = `${cfg.prefix}${(cfg.id ?? (() => crypto.randomUUID()))()}/`;
    const key = `${dir}probe`;
    const res = await signed("PUT", key, { "x-ms-blob-type": "BlockBlob", "content-type": "application/octet-stream", "if-none-match": "*" }, PROBE_BODY);
    if (res.type === "opaqueredirect" || res.redirected || (res.status >= 300 && res.status < 400)) return "redirect";
    if (res.status === 403) return "auth_refused";
    if (res.status === 404) return "container_missing";
    if (res.status >= 500) throw transient();
    if (!res.ok) return "unverified";
    const version = res.headers.get("x-ms-version-id");
    if (!version) {
      // Unversioned: a plain conditional delete is this account's only erase; best effort, the refusal stands.
      const etag = res.headers.get("etag");
      if (etag) await signed("DELETE", key, { "if-match": etag }).catch(() => {});
      return "not_versioned";
    }
    if (!AZURE_VERSION_ID.test(version)) { await eraseAndVerify(eraser, dir, key).catch(() => {}); return "unverified"; }
    // 3. The inventory must see the probe version, then the strict erase must remove every copy.
    const before = await eraser.verifyErased(dir, { authorize: async () => {}, exactKey: key, select: (v) => v.versionId === version }).catch(fromEraser);
    if (before.status !== "pending" || before.remaining < 1) return "unverified";
    await eraseAndVerify(eraser, dir, key);
    return "Enabled";
  } catch (error) {
    if (error instanceof Verdict) return error.verdict;
    throw error;
  }
}
