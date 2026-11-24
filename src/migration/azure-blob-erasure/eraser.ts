/**
 * Strict, paged, resumable erasure for Azure Blob Storage: the same PagedEraser contract the
 * S3 eraser meets (eraser.ts eraseStep / verifyErased), over THE shared traversal
 * (createPagedEraser), so page strictness, budgets, hard ceilings, checkpoint binding and cycle
 * checks are one implementation. Reusable: no runner state, so another consumer (the self-host
 * operator erase, later) can compose it unchanged.
 *
 * Inventory: List Blobs with include=versions,deleted,snapshots. Every recoverable copy under
 * the prefix is an entry, by exact identity (ListedVersion.versionId):
 *   live version             the raw x-ms-version-id (what a put acked, so owned versions match)
 *   soft-deleted version     "deleted:" + id          (deleteMarker: true, never readable)
 *   snapshot                 "snapshot:" + time       (soft-deleted: "deleted:snapshot:" + time)
 *   unversioned base blob    "null"                   (soft-deleted: "deleted:null")
 * Deletes, per page: snapshots, then the current version (the base blob, If-Match on its listed
 * ETag, then the version id: Azure refuses deleting the current version by id), then other
 * versions, then soft-deleted entries with deletetype=permanent. A 412 on the base delete is
 * `azure_current_changed` (a successor became current: nothing deleted, the step fails, the next
 * sweep relists). A refused permanent delete, or a soft-deleted base blob (which only undelete
 * can touch), is `azure_soft_delete_retained`: erasure cannot complete, so verifyErased keeps
 * reporting pending, never erased. Every request is Shared Key signed with redirect "manual":
 * a 3xx is `azure_redirect` and is never followed. Errors carry codes and statuses only, never a
 * key, a container, an account, a body or the account key.
 *
 * Cursors: Azure's continuation is an opaque marker, not a key marker. Within one eraser
 * instance every truncated page records its last entry's position with its NextMarker; a request
 * after a position resumes from the greatest recorded marker at or before it and filters client
 * side. Without one (a checkpoint resumed by a fresh instance) it lists from the start of the
 * prefix and skips, at most AZURE_RESUME_MAX_PAGES provider pages, then `azure_resume_unbounded`.
 * The sweeper runs exact-key mode (the prefix is one key), so that is one or two pages.
 */
import { azureSharedKeyAuthorization, AZURE_API_VERSION } from "../../../packages/object-store/src/azure.ts";
import { encodeKeyPath, rfc3986 } from "../../../packages/object-store/src/crypto.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";
import { assertInventoryXml, coded, createPagedEraser, type ListedVersion, type PagedEraser, type StrictPage } from "../neon-s3-erasure/eraser.ts";

export interface AzureBlobEraserConfig {
  account: string;
  container: string;
  /** Blob endpoint (Azurite: http://127.0.0.1:10000/<account>). Unset: https://<account>.blob.core.windows.net. */
  endpoint?: string;
  /** The account key, read per request (never stored on the eraser object). */
  accountKey: () => string;
  fetch?: typeof fetch;
  /** Per request (default 30 s). */
  timeoutMs?: number;
  /** List Blobs maxresults (default 1000, the service maximum). */
  pageSize?: number;
}

export const AZURE_RESUME_MAX_PAGES = 100;
const MAX_INVENTORY_BYTES = 16 * 1024 * 1024;
const DELETED = "deleted:";
const SNAPSHOT = "snapshot:";
const cmpC = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const CONTROL = /[\x00-\x1f\x7f]/u;

/** The checkpoint binding label: account and container, so a checkpoint never crosses either. */
export function azureEraserLabel(account: string, container: string): string {
  return `azure:${account}/${container}`;
}

interface RawPage { entries: ListedVersion[]; nextMarker: string | null }

export function createAzureBlobEraser(cfg: AzureBlobEraserConfig): PagedEraser {
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const base = (cfg.endpoint ?? `https://${cfg.account}.blob.core.windows.net`).replace(/\/+$/, "");
  const pageSize = Math.min(Math.max(1, Math.floor(cfg.pageSize ?? 1000)), 1000);
  const containerUrl = () => new URL(`${base}/${rfc3986(cfg.container)}`);
  const blobUrl = (key: string) => new URL(`${base}/${rfc3986(cfg.container)}/${encodeKeyPath(key)}`);

  /** One signed request; never follows a redirect; the caller maps statuses. */
  async function request(method: "GET" | "DELETE", url: URL, extra: Record<string, string> = {}): Promise<Response> {
    const headers = new Headers(extra);
    headers.set("x-ms-version", AZURE_API_VERSION);
    headers.set("x-ms-date", new Date().toUTCString());
    headers.set("authorization", await azureSharedKeyAuthorization(cfg.account, cfg.accountKey(), method, url, headers, 0));
    let res: Response;
    try {
      res = await doFetch(url, { method, headers, redirect: "manual", signal: AbortSignal.timeout(cfg.timeoutMs ?? 30_000) });
    } catch {
      throw coded("azure_unavailable", `Azure ${method} failed`);
    }
    if (res.type === "opaqueredirect" || res.redirected || (res.status >= 300 && res.status < 400)) {
      await res.body?.cancel().catch(() => {});
      throw coded("azure_redirect", `Azure ${method} answered a redirect`);
    }
    return res;
  }

  async function boundedText(res: Response): Promise<string> {
    const reader = res.body?.getReader(), chunks: Uint8Array[] = [];
    let size = 0;
    try {
      if (!reader) throw coded("erasure_inventory", "empty blob inventory");
      for (;;) {
        const r = await reader.read();
        if (r.done) break;
        size += r.value.byteLength;
        if (size > MAX_INVENTORY_BYTES) throw coded("erasure_inventory", "blob inventory byte bound");
        chunks.push(r.value);
      }
    } finally {
      await reader?.cancel().catch(() => {});
      reader?.releaseLock();
    }
    const body = new Uint8Array(size);
    let at = 0;
    for (const c of chunks) { body.set(c, at); at += c.byteLength; }
    try { return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body); }
    catch { throw coded("erasure_inventory", "blob inventory encoding"); }
  }

  const bool = (el: string, tag: string): boolean | undefined => {
    const v = xmlText(el, tag);
    if (v === undefined) return undefined;
    if (v !== "true" && v !== "false") throw coded("erasure_inventory", "incomplete blob entry");
    return v === "true";
  };

  /** One List Blobs page, strictly parsed, in provider order. */
  async function listPage(prefix: string, marker: string | undefined): Promise<RawPage> {
    const u = containerUrl();
    u.searchParams.set("restype", "container");
    u.searchParams.set("comp", "list");
    u.searchParams.set("prefix", prefix);
    u.searchParams.set("include", "deleted,snapshots,versions");
    u.searchParams.set("maxresults", String(pageSize));
    if (marker !== undefined) u.searchParams.set("marker", marker);
    const res = await request("GET", u);
    if (!res.ok) { await res.body?.cancel().catch(() => {}); throw coded(`azure_${res.status}`, "Azure list failed"); }
    const xml = await boundedText(res);
    assertInventoryXml(xml, "EnumerationResults");
    // An Encoded name (characters XML cannot carry) is refused rather than guessed.
    if (/<Name\s+[^>]*Encoded\s*=\s*["']true["']/u.test(xml)) throw coded("erasure_inventory", "encoded blob name");
    const entries: ListedVersion[] = [];
    for (const el of xmlElements(xml, "Blob")) {
      const key = xmlText(el, "Name");
      const version = xmlText(el, "VersionId");
      const snapshot = xmlText(el, "Snapshot");
      const current = bool(el, "IsCurrentVersion");
      const deleted = bool(el, "Deleted") ?? false;
      const props = xmlElements(el, "Properties")[0];
      if (!key || props === undefined || (version !== undefined && (!version || CONTROL.test(version)))
          || (snapshot !== undefined && (!snapshot || CONTROL.test(snapshot))) || (snapshot !== undefined && current === true))
        throw coded("erasure_inventory", "incomplete blob entry");
      const etag = xmlText(props, "Etag")?.replace(/^"|"$/g, "");
      const modified = Date.parse(xmlText(props, "Last-Modified") ?? "");
      const id = snapshot !== undefined ? `${SNAPSHOT}${snapshot}` : version ?? "null";
      // Unversioned base blobs are current by definition; versions say so explicitly.
      const latest = !deleted && snapshot === undefined && (version === undefined ? true : current === true);
      entries.push({
        key,
        versionId: deleted ? `${DELETED}${id}` : id,
        latest,
        deleteMarker: deleted,
        ...(Number.isFinite(modified) ? { lastModified: modified } : {}),
        ...(etag && !deleted ? { etag } : {}),
      });
    }
    // Pagination must be proven: NextMarker present (empty when done).
    if (!/<NextMarker\s*\/>|<NextMarker>[\s\S]*?<\/NextMarker>/u.test(xml)) throw coded("erasure_inventory", "blob pagination is unproven");
    const next = xmlText(xml, "NextMarker") ?? "";
    if (CONTROL.test(next)) throw coded("erasure_inventory", "blob pagination marker");
    for (let i = 1; i < entries.length; i++)
      if (cmpC(entries[i]!.key, entries[i - 1]!.key) < 0) throw coded("erasure_inventory", "inventory keys out of order");
    return { entries, nextMarker: next === "" ? null : next };
  }

  // Recorded resume points, per listed prefix and for the CURRENT traversal only: the last entry of each
  // truncated page and the marker after it, in order. An Azure marker names the next item as of the moment it
  // was issued, so a marker from an earlier traversal could skip an entry created since; every traversal that
  // starts from the beginning (eraseStep without a checkpoint, every verifyErased) therefore drops them.
  const boundsByPrefix = new Map<string, { key: string; versionId: string; marker: string }[]>();

  const fetchPage = async (prefix: string, keyMarker?: string, versionMarker?: string): Promise<StrictPage> => {
    if (keyMarker === undefined) boundsByPrefix.delete(prefix);
    let bounds = boundsByPrefix.get(prefix);
    if (!bounds) boundsByPrefix.set(prefix, (bounds = []));
    // Resume: right after an exactly recorded in-key bound (no filter needed); else from the last
    // recorded bound before the position (or the start of the prefix), filtering client side.
    // Newest first: the most recent marker for a position is the freshest view of the listing.
    const exact = keyMarker !== undefined && versionMarker !== undefined
      ? [...bounds].reverse().find((b) => b.key === keyMarker && b.versionId === versionMarker) : undefined;
    const before = keyMarker === undefined || exact ? undefined
      : [...bounds].reverse().find((b) => (versionMarker === undefined ? cmpC(b.key, keyMarker) <= 0 : cmpC(b.key, keyMarker) < 0));
    let marker = exact?.marker ?? before?.marker, skipped = 0, passed = false;
    const after = (entries: ListedVersion[]): ListedVersion[] => {
      if (keyMarker === undefined || exact) return entries;
      if (versionMarker === undefined) return entries.filter((v) => cmpC(v.key, keyMarker) > 0);
      // In-key: drop the key's entries up to and including the marker version. If the marker
      // version is gone, keep every entry of the key (a relist; deleted entries do not return).
      const markerSeen = passed || entries.some((v) => v.key === keyMarker && v.versionId === versionMarker);
      const kept: ListedVersion[] = [];
      for (const v of entries) {
        const c = cmpC(v.key, keyMarker);
        if (c < 0) continue;
        if (c === 0 && markerSeen && !passed) { if (v.versionId === versionMarker) passed = true; continue; }
        kept.push(v);
      }
      return kept;
    };
    for (;;) {
      const raw = await listPage(prefix, marker);
      const entries = after(raw.entries);
      if (raw.nextMarker === null) return { entries, truncated: false };
      if (entries.length) {
        const last = entries[entries.length - 1]!;
        bounds.push({ key: last.key, versionId: last.versionId, marker: raw.nextMarker });
        return { entries, truncated: true, nextKey: last.key, nextVersion: last.versionId };
      }
      if (++skipped >= AZURE_RESUME_MAX_PAGES) throw coded("azure_resume_unbounded", "the resume point is too far into the listing");
      marker = raw.nextMarker;
    }
  };

  async function del(url: URL, extra: Record<string, string> = {}, onRefused?: string): Promise<void> {
    const res = await request("DELETE", url, extra);
    await res.body?.cancel().catch(() => {});
    if (res.ok || res.status === 404) return;
    // A permanent delete refused: 403 is a permission the key or account lacks; 400/409 is the account's retention
    // (soft delete without permanent delete enabled). Both keep the copy, so both block erasure.
    if (onRefused && res.status === 403) throw coded("azure_permanent_delete_forbidden", "Azure forbade a permanent delete");
    if (onRefused && (res.status === 400 || res.status === 409)) throw coded(onRefused, "Azure refused a permanent delete");
    if (res.status === 412) throw coded("azure_current_changed", "the current version changed");
    throw coded(`azure_${res.status}`, "Azure DELETE failed");
  }

  /** Deletion class: snapshots, the current version, other versions, soft-deleted entries. */
  const rank = (v: ListedVersion) => (v.deleteMarker ? 3 : v.versionId.startsWith(SNAPSHOT) ? 0 : v.latest ? 1 : 2);

  async function removeExact(list: ListedVersion[]): Promise<number> {
    if (list.some((v) => v.key.split("/").some((seg) => seg === "." || seg === "..")))
      throw coded("erasure_key_shape", "a key with a dot segment would be normalized away");
    const order = [...list].sort((a, b) =>
      Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")) || rank(a) - rank(b));
    for (const v of order) {
      const id = v.versionId;
      if (v.deleteMarker) {
        const inner = id.slice(DELETED.length);
        // A soft-deleted base blob has no version or snapshot to delete permanently.
        if (!id.startsWith(DELETED) || inner === "null") throw coded("azure_soft_delete_retained", "a soft-deleted base blob remains");
        const u = blobUrl(v.key);
        if (inner.startsWith(SNAPSHOT)) u.searchParams.set("snapshot", inner.slice(SNAPSHOT.length));
        else u.searchParams.set("versionid", inner);
        u.searchParams.set("deletetype", "permanent");
        await del(u, {}, "azure_soft_delete_retained");
        continue;
      }
      if (id.startsWith(SNAPSHOT)) {
        const u = blobUrl(v.key);
        u.searchParams.set("snapshot", id.slice(SNAPSHOT.length));
        await del(u);
        continue;
      }
      if (v.latest) {
        // The current copy: the base blob, conditional on the listed ETag (never a successor).
        if (!v.etag) throw coded("erasure_inventory", "a current blob without an ETag");
        await del(blobUrl(v.key), { "if-match": `"${v.etag.replace(/^"|"$/g, "")}"` });
        if (id === "null") continue;
      }
      if (id === "null") throw coded("erasure_inventory", "a non-current unversioned blob");
      const u = blobUrl(v.key);
      u.searchParams.set("versionid", id);
      await del(u);
    }
    return list.length;
  }

  return createPagedEraser(azureEraserLabel(cfg.account, cfg.container), fetchPage, removeExact);
}
