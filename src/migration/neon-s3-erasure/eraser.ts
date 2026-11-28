/**
 * Permanent erasure for the sessions-prod transcript bucket, with only the
 * eraser key (96BullSessions #100: ListBucket/ListBucketVersions under
 * `96bulls/`, DeleteObject/DeleteObjectVersion, no GetObject, no key use).
 *
 * Every version and delete marker of a key (or under a prefix) is deleted by
 * version id, so nothing waits for lifecycle: a plain DELETE on a versioned
 * bucket would only add a delete marker and leave the bytes for a day.
 */
import { encodeKeyPath, sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { signRequest, type AwsCredentials } from "../../../packages/object-store/src/sigv4.ts";
import { refuseIfExpired } from "../short-lived-credentials.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";

export interface Eraser {
  /** Every version of exactly this key. Returns versions removed. */
  eraseObject(key: string): Promise<number>;
  /** Every version of every key under `prefix` (which ends in `/`), manifests first. */
  erasePrefix(prefix: string): Promise<number>;
  /**
   * Every version of every key under `base` (ends in `/`) that sits in the
   * session directory `base + d=yyyy/mm/dd/ + sessionSegment + /`, whatever
   * its date. Used for relayhistory-api's live objects, whose `d=` the eraser
   * cannot read (it holds no GetObject).
   */
  eraseSessionAnyDate(base: string, sessionSegment: string, before?: Date): Promise<{ deleted: number; kept: number }>;
  /** Every version of exactly this key last modified before `before` (all when unset). */
  eraseObjectBefore(key: string, before: Date): Promise<{ deleted: number; kept: number }>;
  /** Keys with any version under `prefix` (ends in `/`). */
  listKeys(prefix: string): Promise<string[]>;
}

/** True when `key` is inside `base` + `d=yyyy/mm/dd/` + `sessionSegment` + `/`. */
export function inSessionAnyDate(base: string, sessionSegment: string, key: string): boolean {
  if (!key.startsWith(base)) return false;
  const rest = key.slice(base.length);
  const m = /^d=\d{4}\/\d{2}\/\d{2}\/([^/]+)\//.exec(rest);
  return !!m && m[1] === sessionSegment;
}

/**
 * THE shared strict, paged, resumable eraser primitive (Babysitter retention, the F1 analysis
 * sweeper and the E3 job all compose this one implementation; consumers persist the returned
 * checkpoint in their own durable state and must not fork the traversal).
 *
 * `eraseStep` runs ONE bounded invocation: strict pages (complete Key/VersionId/IsLatest,
 * exact key+version identity, recognized pagination with both markers equal to the last
 * entry, every key under the prefix, no duplicate in a page, monotonic cursor, no cycle) and
 * exact-version deletes of the selected entries, with `authorize()` re-checked before every
 * page's deletes. It never reports erasure: `traversed` means one full pass ended; only
 * `verifyErased` (a fresh strict zero inventory, under the consumer's current fence) proves it.
 * On budget exhaustion it returns `incomplete` with a checkpoint covering exactly the progress
 * whose deletes succeeded. A crash or lost response simply reuses the previous checkpoint: the
 * relist is idempotent (deleted versions are gone; delete-by-version of a gone one is a no-op).
 *
 * The checkpoint is content-free beyond object keys (hash-named) and is BOUND to the bucket,
 * the prefix and the consumer's `scope` (operation, tenant/session, generation, fence, lease
 * owner digest); a mismatched checkpoint is refused, never adopted. Resumption is at a
 * completed-key boundary (`afterKey`, S3 key-marker only), so a deleted marker version can
 * never strand the cursor; only a single key with more versions than one page uses the
 * provider's in-key markers, and a provider that rejects them (400: its marker version was
 * deleted) restarts at `afterKey` within the same bounded run.
 */
export interface EraseCheckpoint {
  v: 1;
  bucket: string;
  prefix: string;
  scope: string;
  /** Every version of every key <= afterKey (C order) was processed. null: from the start. */
  afterKey: string | null;
  /** In-key provider continuation (a key with more versions than one page). */
  inKey: { key: string; versionId: string } | null;
  /** Exact-key mode: the one key this traversal lists (absent/null: the whole prefix). */
  exactKey?: string | null;
}
export interface ListedVersion {
  key: string;
  versionId: string;
  latest: boolean;
  deleteMarker: boolean;
  lastModified?: number;
  /** The provider's ETag without quotes (absent for delete markers / unversioned stores). */
  etag?: string;
}
export interface EraseBudget {
  /** Versions listed per invocation (hard ceiling 10,000). */
  versions?: number;
  /** Pages per invocation (hard ceiling 100). */
  pages?: number;
  /** Absolute ms; no new page or delete batch starts at or after it. */
  deadlineAt?: number;
}
export interface EraseStepOptions {
  /** Consumer scope digest: operation, tenant/session, generation, fence, lease owner. */
  scope: string;
  checkpoint?: EraseCheckpoint | null;
  /** Which listed versions to delete (default: all). */
  select?: (v: ListedVersion) => boolean;
  /** Re-checks current authority (lease, generation, fence); throws to stop before any delete. */
  authorize: () => Promise<void>;
  /**
   * Durable evidence hook: runs after authorize() and BEFORE a page's deletes with exactly the
   * versions about to be deleted (write-obligation.ts records "an unowned write was found"
   * here, so a lost DELETE acknowledgement can never erase that proof). Throwing stops the run
   * before that page deletes anything.
   */
  beforeDelete?: (doomed: readonly ListedVersion[]) => Promise<void>;
  /**
   * Exact-key mode: list and erase only this one key's versions (it must lie under `prefix`).
   * The provider is listed with Prefix = the key itself, so the key's versions come first and
   * the traversal stops at the first other key: a key whose directory holds any number of kept
   * siblings stays within budget. Every strict-page rule still applies.
   */
  exactKey?: string;
  budget?: EraseBudget;
  now?: () => number;
}
export type EraseStepResult =
  | { status: "incomplete"; checkpoint: EraseCheckpoint; deleted: number; listed: number; reason: "versions" | "pages" | "deadline" }
  | { status: "traversed"; deleted: number; listed: number };
export type ErasedProof =
  | { status: "erased"; listed: number }
  | { status: "pending"; reason: "remaining" | "budget"; remaining: number; listed: number };
export interface PagedEraser {
  eraseStep(prefix: string, opts: EraseStepOptions): Promise<EraseStepResult>;
  /**
   * A fresh, strict, full traversal from the start: erased only when zero selected versions
   * remain. With `exactKey`, the traversal covers exactly that key's versions (see EraseStepOptions).
   */
  verifyErased(prefix: string, opts: { select?: (v: ListedVersion) => boolean; authorize: () => Promise<void>; budget?: EraseBudget; now?: () => number; exactKey?: string }): Promise<ErasedProof>;
}
export const ERASE_RUN_MAX_VERSIONS = 10_000;
export const ERASE_RUN_MAX_PAGES = 100;
const MAX_CURSOR_RESTARTS = 3;

/** One strict inventory page; when truncated, nextKey/nextVersion name its last entry. */
export interface StrictPage { entries: ListedVersion[]; truncated: boolean; nextKey?: string; nextVersion?: string }
/** One bounded provider listing after (keyMarker[, versionMarker]) under `prefix`. */
export type PageFetcher = (prefix: string, keyMarker?: string, versionMarker?: string) => Promise<StrictPage>;
const cmpC = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/**
 * Validates one strict page against the listed prefix and the requested cursor. In exact-key
 * mode the listed prefix IS the key, which itself is a valid entry.
 */
function checkPage(prefix: string, page: StrictPage, keyMarker?: string, versionMarker?: string, exact = false): void {
  const seen = new Set<string>();
  let prev: string | undefined;
  for (const v of page.entries) {
    if (!v.key.startsWith(prefix) || (!exact && v.key.length === prefix.length) || v.key.split("/").some((seg) => seg === "." || seg === ".."))
      throw coded("erasure_foreign_key", "an inventory key is outside the requested prefix");
    const id = JSON.stringify([v.key, v.versionId]);
    if (seen.has(id)) throw coded("erasure_inventory", "duplicate version in one inventory page");
    seen.add(id);
    if (prev !== undefined && cmpC(v.key, prev) < 0) throw coded("erasure_inventory", "inventory keys out of order");
    prev = v.key;
    // Monotonic: a key-marker-only page starts strictly after the marker; an in-key page at or after it.
    if (keyMarker !== undefined && (versionMarker === undefined ? cmpC(v.key, keyMarker) <= 0 : cmpC(v.key, keyMarker) < 0))
      throw coded("erasure_inventory", "inventory cursor went backwards");
  }
  if (page.truncated) {
    const last = page.entries[page.entries.length - 1];
    if (!last || page.nextKey !== last.key || page.nextVersion !== last.versionId)
      throw coded("erasure_inventory", "version pagination markers do not match the page");
  }
}

function checkCheckpoint(bucket: string, prefix: string, scope: string, cp: EraseCheckpoint | null | undefined, exactKey: string | null = null): EraseCheckpoint | null {
  if (cp === null || cp === undefined) return null;
  if (!cp || cp.v !== 1 || cp.bucket !== bucket || cp.prefix !== prefix || cp.scope !== scope || (cp.exactKey ?? null) !== exactKey)
    throw coded("erasure_checkpoint_scope", "the checkpoint is bound to another bucket, prefix, key or scope");
  if (cp.afterKey !== null && (typeof cp.afterKey !== "string" || !cp.afterKey.startsWith(prefix)))
    throw coded("erasure_checkpoint_scope", "the checkpoint cursor is outside the prefix");
  if (cp.inKey !== null && (!cp.inKey || typeof cp.inKey.key !== "string" || !cp.inKey.key.startsWith(prefix) || typeof cp.inKey.versionId !== "string" || !cp.inKey.versionId
      || (cp.afterKey !== null && cmpC(cp.inKey.key, cp.afterKey) <= 0)))
    throw coded("erasure_checkpoint_scope", "the checkpoint in-key cursor is malformed");
  return cp;
}

function budgetOf(b: EraseBudget | undefined) {
  const versions = Math.min(b?.versions ?? ERASE_RUN_MAX_VERSIONS, ERASE_RUN_MAX_VERSIONS);
  const pages = Math.min(b?.pages ?? ERASE_RUN_MAX_PAGES, ERASE_RUN_MAX_PAGES);
  if (!(versions > 0) || !(pages > 0)) throw coded("erasure_budget", "an erase run needs a positive budget");
  return { versions, pages, deadlineAt: b?.deadlineAt ?? Number.POSITIVE_INFINITY };
}

/** The exact key of an exact-key run, validated against its directory prefix (or null). */
function exactKeyOf(prefix: string, exactKey: string | undefined): string | null {
  if (exactKey === undefined) return null;
  if (typeof exactKey !== "string" || !exactKey.startsWith(prefix) || exactKey.length === prefix.length || exactKey.endsWith("/")
      || exactKey.split("/").some((seg) => seg === "." || seg === ".."))
    throw coded("erasure_prefix_shape", "an exact key lies under its prefix and names one object");
  return exactKey;
}

/** In exact-key mode a page is cut at the first other key (it sorts after every version of the key). */
function cutToKey(page: StrictPage, exact: string | null): StrictPage {
  if (exact === null) return page;
  const at = page.entries.findIndex((v) => v.key !== exact);
  return at < 0 ? page : { entries: page.entries.slice(0, at), truncated: false };
}

/**
 * The one traversal every store shares (S3 versions, Azure Blob versions, or an unversioned
 * store's keys). Exported so another provider's eraser composes it instead of forking it:
 * `bucket` is the checkpoint binding label, `fetchPage` one strict page, `removeExact` the
 * exact-identity deletes of one page's selected entries.
 */
export function createPagedEraser(bucket: string, fetchPage: PageFetcher, removeExact: (list: ListedVersion[]) => Promise<number>): PagedEraser {
  return {
    async eraseStep(prefix, opts) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      if (typeof opts.scope !== "string" || !opts.scope) throw coded("erasure_checkpoint_scope", "an erase run needs a scope");
      const exact = exactKeyOf(prefix, opts.exactKey);
      const cp = checkCheckpoint(bucket, prefix, opts.scope, opts.checkpoint, exact);
      const budget = budgetOf(opts.budget), now = opts.now ?? Date.now, base = opts.select ?? (() => true);
      const select = exact === null ? base : (v: ListedVersion) => v.key === exact && base(v);
      const listPrefix = exact ?? prefix;
      let afterKey = cp?.afterKey ?? null, inKey = cp?.inKey ?? null;
      let pages = 0, listed = 0, deleted = 0, restarts = 0;
      const gone = new Set<string>(), states = new Set<string>();
      const checkpoint = (): EraseCheckpoint => ({ v: 1, bucket, prefix, scope: opts.scope, afterKey, inKey, ...(exact !== null ? { exactKey: exact } : {}) });
      for (;;) {
        if (now() >= budget.deadlineAt) return { status: "incomplete", checkpoint: checkpoint(), deleted, listed, reason: "deadline" };
        if (pages >= budget.pages) return { status: "incomplete", checkpoint: checkpoint(), deleted, listed, reason: "pages" };
        if (listed >= budget.versions) return { status: "incomplete", checkpoint: checkpoint(), deleted, listed, reason: "versions" };
        const state = JSON.stringify([afterKey, inKey]);
        const keyMarker = inKey ? inKey.key : afterKey ?? undefined, versionMarker = inKey?.versionId;
        let page: StrictPage;
        try {
          page = await fetchPage(listPrefix, keyMarker, versionMarker);
        } catch (e) {
          // A provider that rejects its own in-key marker (the marker version was deleted):
          // reconcile from the last completed key, inside this bounded run; never guess a marker.
          if (inKey && (e as { code?: string }).code === "s3_400" && restarts < MAX_CURSOR_RESTARTS) { restarts++; inKey = null; continue; }
          throw e;
        }
        pages++;
        checkPage(listPrefix, page, keyMarker, versionMarker, exact !== null);
        page = cutToKey(page, exact);
        for (const v of page.entries)
          if (gone.has(JSON.stringify([v.key, v.versionId]))) throw coded("erasure_inventory_changed", "a deleted version reappeared");
        listed += page.entries.length;
        const doomed = page.entries.filter(select);
        if (doomed.length) {
          if (now() >= budget.deadlineAt) return { status: "incomplete", checkpoint: checkpoint(), deleted, listed, reason: "deadline" };
          await opts.authorize();
          assertUnderPrefix(prefix, doomed);
          if (opts.beforeDelete) await opts.beforeDelete(doomed);
          deleted += await removeExact(doomed);
          for (const v of doomed) gone.add(JSON.stringify([v.key, v.versionId]));
        }
        if (!page.truncated) return { status: "traversed", deleted, listed };
        const lastKey = page.nextKey!;
        const complete = page.entries.filter((v) => cmpC(v.key, lastKey) < 0);
        if (complete.length) { afterKey = complete[complete.length - 1]!.key; inKey = null; }
        else if (doomed.length) inKey = null;          // progress by deletion: relist the key from afterKey
        else inKey = { key: lastKey, versionId: page.nextVersion! };
        // A state seen again in this run with nothing deleted since is a provider cycle.
        if (!doomed.length && states.has(JSON.stringify([afterKey, inKey]))) throw coded("erasure_inventory", "version pagination cycle");
        states.add(state);
      }
    },
    async verifyErased(prefix, opts) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const exact = exactKeyOf(prefix, opts.exactKey);
      const budget = budgetOf(opts.budget), now = opts.now ?? Date.now, base = opts.select ?? (() => true);
      const select = exact === null ? base : (v: ListedVersion) => v.key === exact && base(v);
      const listPrefix = exact ?? prefix;
      await opts.authorize();
      let keyMarker: string | undefined, versionMarker: string | undefined, pages = 0, listed = 0, remaining = 0;
      const cursors = new Set<string>();
      for (;;) {
        if (now() >= budget.deadlineAt || pages >= budget.pages || listed >= budget.versions) return { status: "pending", reason: "budget", remaining, listed };
        const listedPage = await fetchPage(listPrefix, keyMarker, versionMarker);
        pages++;
        checkPage(listPrefix, listedPage, keyMarker, versionMarker, exact !== null);
        const page = cutToKey(listedPage, exact);
        listed += page.entries.length;
        remaining += page.entries.filter(select).length;
        if (remaining) return { status: "pending", reason: "remaining", remaining, listed };
        if (!page.truncated) return { status: "erased", listed };
        keyMarker = page.nextKey; versionMarker = page.nextVersion;
        const c = JSON.stringify([keyMarker, versionMarker]);
        if (cursors.has(c)) throw coded("erasure_inventory", "version pagination cycle");
        cursors.add(c);
      }
    },
  };
}

export interface S3EraserConfig {
  bucket: string;
  region: string;
  /** Path-style override (MinIO). */
  endpoint?: string;
  credentials: AwsCredentials;
  fetch?: typeof fetch;
  /** Per-request deadline (ms, default S3_REQUEST_DEADLINE_MS): every signed request is aborted at it. */
  requestDeadlineMs?: number;
  /** TEST SEAM: the signer (default sigv4 signRequest); a test can make signing slow. */
  signer?: typeof signRequest;
}
/**
 * The last gate of ONE request: runs AFTER signing and IMMEDIATELY before fetch (nothing awaits between). Throwing
 * means fetch is never called. Returns the milliseconds left to the caller's ABSOLUTE protected deadline; the request
 * is aborted at the earlier of that and the per-request deadline.
 */
export type BeforeFetch = () => number;
/** Default per-request deadline. Requests are never retried here (attempts = 1). */
export const S3_REQUEST_DEADLINE_MS = 30_000;
export const S3_REQUEST_ATTEMPTS = 1;

/** One exact (key, version) identity. */
export interface VersionRef {
  key: string;
  versionId: string;
}

/** Refuses a snapshot entry outside its own prefix (no dot segments either). */
export function assertUnderPrefix(prefix: string, list: readonly VersionRef[]): void {
  if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
  for (const v of list)
    if (typeof v.key !== "string" || !v.key.startsWith(prefix) || v.key.length === prefix.length || v.key.split("/").some((seg) => seg === "." || seg === ".."))
      throw coded("erasure_foreign_key", "a snapshot key is outside its bucket prefix");
}
export function isPagedEraser(e: unknown): e is PagedEraser {
  return !!e && typeof (e as PagedEraser).eraseStep === "function" && typeof (e as PagedEraser).verifyErased === "function";
}

/**
 * A read-only existence check: true only when the key's CURRENT version exists and is not a delete marker. Used by
 * the stale-prefix-only mode (job.ts) to prove a live sibling copy before an old prefix is erased.
 */
export interface LiveObjectProbe {
  objectLive(key: string): Promise<boolean>;
}

export function isLiveObjectProbe(e: unknown): e is LiveObjectProbe {
  return !!e && typeof (e as LiveObjectProbe).objectLive === "function";
}

export interface PreservedVersionEraser {
  /** Keep the current non-delete barrier version; permanently remove only older versions. */
  eraseObjectExcept(key: string, preservedVersionId: string): Promise<{ deleted: number; kept: number }>;
}

interface Version {
  key: string;
  versionId: string;
  lastModified?: number;
  latest: boolean;
  deleteMarker: boolean;
}

/** Split versions into those strictly older than `before` (erasable) and the rest (kept). */
function cut<T extends { lastModified?: number }>(list: T[], before?: Date): { erase: T[]; kept: number } {
  if (!before) return { erase: list, kept: 0 };
  const limit = before.getTime();
  // A version with no readable time is kept: never erase what might be new.
  const erase = list.filter((v) => v.lastModified !== undefined && v.lastModified < limit);
  return { erase, kept: list.length - erase.length };
}

export const coded = (code: string, message: string) => Object.assign(new Error(message), { code });

// S3 inventory is destructive authority. The lightweight value extractor alone
// cannot distinguish a complete document from truncated/malformed XML.
export function assertInventoryXml(xml: string, root = "ListVersionsResult"): void {
  const stack: string[] = []; let at = 0, roots = 0;
  const tags = /<[^>]*>/g; let token: RegExpExecArray | null;
  while ((token = tags.exec(xml))) {
    const text = xml.slice(at, token.index);
    if (text.includes("<") || (!stack.length && text.trim())) throw coded("erasure_inventory", "malformed version inventory");
    const tag = token[0]; at = tags.lastIndex;
    if (/^<\?xml\s[^?]*\?>$/.test(tag) && !roots && !stack.length) continue;
    const close = /^<\/([A-Za-z_][\w:.-]*)\s*>$/.exec(tag);
    if (close) { if (stack.pop() !== close[1]) throw coded("erasure_inventory", "unbalanced version inventory"); continue; }
    const open = /^<([A-Za-z_][\w:.-]*)(?:\s+[A-Za-z_][\w:.-]*=(?:"[^"<>]*"|'[^'<>]*'))*\s*(\/?)>$/.exec(tag);
    if (!open) throw coded("erasure_inventory", "malformed version inventory");
    if (!stack.length && (++roots !== 1 || open[1] !== root)) throw coded("erasure_inventory", "unknown version inventory");
    if (!open[2]) stack.push(open[1]!);
  }
  if (stack.length || roots !== 1 || xml.slice(at).trim()) throw coded("erasure_inventory", "incomplete version inventory");
}

/**
 * The signed S3 plumbing every S3 role here shares (the eraser, and the read-only stale-prefix verifier): one URL
 * builder, one signed request (status-only errors: S3 error bodies can echo keys), and THE strict ListObjectVersions
 * page parser. `label` names the credential in a short-lived refusal (`short_lived_expired.<label>`).
 */
function s3Core(cfg: S3EraserConfig, label: string) {
  const deadlineMs = cfg.requestDeadlineMs ?? S3_REQUEST_DEADLINE_MS;
  if (!(Number.isSafeInteger(deadlineMs) && deadlineMs > 0)) throw coded("erasure_config", "a positive request deadline is required");
  const doFetch = cfg.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const url = (key = "") => {
    if (cfg.endpoint) {
      const u = new URL(cfg.endpoint);
      u.pathname = `${u.pathname.replace(/\/+$/, "")}/${cfg.bucket}/${key ? encodeKeyPath(key) : ""}`;
      return u;
    }
    return new URL(`https://${cfg.bucket}.s3.${cfg.region}.amazonaws.com/${key ? encodeKeyPath(key) : ""}`);
  };
  const sign = cfg.signer ?? signRequest;
  const request = async (method: string, u: URL, passStatuses: readonly number[] = [], beforeFetch?: BeforeFetch) => {
    const headers = new Headers();
    refuseIfExpired(cfg.credentials, label, Date.now()); // a short-lived key is never used to sign past expiry
    await sign({ method, url: u, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: cfg.region, credentials: cfg.credentials });
    // The caller's final gate AFTER signing (signing and hashing can be slow) and synchronously before fetch.
    const left = beforeFetch ? beforeFetch() : Number.POSITIVE_INFINITY;
    const timeout = Math.min(deadlineMs, left);
    if (!(timeout > 0)) throw coded("stale_prefix_deadline", "no protected time left for this request");
    const res = await doFetch(u, { method, headers, signal: AbortSignal.timeout(timeout) });
    // Status only: S3 error bodies can echo keys.
    if (!res.ok && !(method === "DELETE" && res.status === 404) && !passStatuses.includes(res.status)) throw coded(`s3_${res.status}`, `S3 ${method} failed`);
    return res;
  };

  /** One ListObjectVersions page (strict: assertInventoryXml, complete entries, recognized pagination). */
  async function fetchPage(prefix: string, keyMarker?: string, versionMarker?: string, strict = true, beforeFetch?: BeforeFetch): Promise<StrictPage> {
    const u = url();
    u.searchParams.set("versions", "");
    u.searchParams.set("prefix", prefix);
    u.searchParams.set("max-keys", "1000");
    if (keyMarker !== undefined) u.searchParams.set("key-marker", keyMarker);
    if (versionMarker !== undefined) u.searchParams.set("version-id-marker", versionMarker);
    const response = await request("GET", u, [], beforeFetch);
    let xml: string;
    if (!strict) xml = await response.text();
    else {
      const reader = response.body?.getReader(), chunks: Uint8Array[] = []; let size = 0;
      try {
        if (!reader) throw coded("erasure_inventory", "empty version inventory");
        for (;;) {
          const r = await reader.read(); if (r.done) break;
          size += r.value.byteLength;
          if (size > 16 * 1024 * 1024) throw coded("erasure_inventory", "version inventory byte bound");
          chunks.push(r.value);
        }
        const body = new Uint8Array(size); let at = 0;
        for (const chunk of chunks) { body.set(chunk, at); at += chunk.byteLength; }
        try { xml = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body); }
        catch { throw coded("erasure_inventory", "version inventory encoding"); }
      } finally { await reader?.cancel().catch(() => {}); reader?.releaseLock(); }
    }
    if (strict) assertInventoryXml(xml);
    const entries: ListedVersion[] = [];
    // Document order (S3 returns key order; versions newest first within a key).
    const els: { el: string; marker: boolean }[] = [];
    for (const m of xml.matchAll(/<(Version|DeleteMarker)(?:\s[^>]*)?>([\s\S]*?)<\/\1>/g)) els.push({ el: m[2]!, marker: m[1] === "DeleteMarker" });
    for (const { el, marker } of els) {
      const key = xmlText(el, "Key");
      const versionId = xmlText(el, "VersionId");
      const modified = Date.parse(xmlText(el, "LastModified") ?? "");
      const latest = xmlText(el, "IsLatest");
      const etag = xmlText(el, "ETag")?.replace(/^"|"$/g, "");
      if (strict && (!key || !versionId || /[\x00-\x1f\x7f]/u.test(versionId) || (latest !== "true" && latest !== "false")))
        throw coded("erasure_inventory", "incomplete version entry");
      // xmlText already decodes entities; decoding again would mangle a literal `&amp;`.
      if (key !== undefined && versionId !== undefined)
        entries.push({ key, versionId, latest: latest === "true", deleteMarker: marker, ...(Number.isFinite(modified) ? { lastModified: modified } : {}), ...(etag && !marker ? { etag } : {}) });
    }
    const truncated = xmlText(xml, "IsTruncated");
    if (strict && truncated !== "true" && truncated !== "false") throw coded("erasure_inventory", "version pagination is unproven");
    if (truncated !== "true") return { entries, truncated: false };
    const nextKey = xmlText(xml, "NextKeyMarker"), nextVersion = xmlText(xml, "NextVersionIdMarker");
    if (strict && (nextKey === undefined || nextVersion === undefined)) throw coded("erasure_inventory", "version pagination is incomplete or unbounded");
    return { entries, truncated: true, ...(nextKey !== undefined ? { nextKey } : {}), ...(nextVersion !== undefined ? { nextVersion } : {}) };
  }

  // Strict by default: every inventory here is destructive authority (assertInventoryXml,
  // required Key/VersionId/IsLatest, proven pagination with cursor progress, the 10k bound).
  // A whole-prefix listing in one call; large prefixes use the resumable `eraseStep`.
  async function versions(prefix: string, strict = true): Promise<Version[]> {
    const out: Version[] = [];
    const pages = new Set<string>();
    let keyMarker: string | undefined;
    let versionMarker: string | undefined;
    for (;;) {
      const page = await fetchPage(prefix, keyMarker, versionMarker, strict);
      out.push(...page.entries);
      if (strict && out.length > 10000) throw coded("erasure_inventory", "version inventory exceeds bound");
      if (!page.truncated) return out;
      keyMarker = page.nextKey;
      versionMarker = page.nextVersion;
      if (strict) {
        const cursor = JSON.stringify([keyMarker, versionMarker]);
        if (keyMarker === undefined || versionMarker === undefined || pages.has(cursor) || pages.size >= 100 || out.length > 10000)
          throw coded("erasure_inventory", "version pagination is incomplete or unbounded");
        pages.add(cursor);
      }
      if (keyMarker === undefined) return out;
    }
  }

  const strictPage: GatedPageFetcher = (prefix, k, v, beforeFetch) => fetchPage(prefix, k, v, true, beforeFetch);
  return { url, request, fetchPage, strictPage, versions, deadlineMs };
}

export function createS3Eraser(cfg: S3EraserConfig): Eraser & PreservedVersionEraser & PagedEraser & LiveObjectProbe & ReceiptFence {
  const { url, request, fetchPage, strictPage, versions, deadlineMs } = s3Core(cfg, "eraser");
  async function remove(list: Version[]): Promise<number> {
    if (list.some((v) => v.key.split("/").some((seg) => seg === "." || seg === "..")))
      throw coded("erasure_key_shape", "a key with a dot segment would be normalized away");
    // Manifests first, so no reader finds a manifest whose chunks are gone.
    list.sort((a, b) => Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")));
    for (const v of list) {
      const u = url(v.key);
      u.searchParams.set("versionId", v.versionId);
      await request("DELETE", u);
    }
    return list.length;
  }

  const paged = createPagedEraser(cfg.bucket, (prefix, k, v) => fetchPage(prefix, k, v), (list) =>
    remove(list.map((v) => ({ key: v.key, versionId: v.versionId, latest: v.latest, deleteMarker: v.deleteMarker }))));
  return {
    eraseStep: paged.eraseStep,
    verifyErased: paged.verifyErased,
    eraseObjectExcept: async (key, preservedVersionId) => {
      if (!preservedVersionId || /[\x00-\x1f\x7f]/u.test(preservedVersionId))
        throw coded("erasure_preserved_version", "a concrete preserved version is required");
      const all = (await versions(key)).filter((v) => v.key === key);
      const latest = all.filter((v) => v.latest);
      if (latest.length !== 1 || latest[0]!.deleteMarker || latest[0]!.versionId !== preservedVersionId)
        throw coded("erasure_barrier_not_current", "the preserved non-delete barrier must be current");
      const erase = all.filter((v) => v.versionId !== preservedVersionId);
      const deleted = await remove(erase);
      const remaining = (await versions(key)).filter((v) => v.key === key);
      if (remaining.length !== 1 || remaining[0]!.versionId !== preservedVersionId || !remaining[0]!.latest || remaining[0]!.deleteMarker)
        throw coded("erasure_inventory_changed", "permanent erasure must leave only the verified current barrier");
      return { deleted, kept: 1 };
    },
    eraseObject: async (key) => {
      const deleted = await remove((await versions(key)).filter((v) => v.key === key));
      // Acknowledged only after a post-delete strict inventory shows zero versions of the key.
      if ((await versions(key)).some((v) => v.key === key)) throw coded("erasure_inventory_changed", "versions survived permanent erasure");
      return deleted;
    },
    erasePrefix: async (prefix) => {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const list = await versions(prefix);
      if (list.some((v) => !v.key.startsWith(prefix))) throw coded("erasure_foreign_key", "an inventory key is outside the requested prefix");
      const deleted = await remove(list);
      if ((await versions(prefix)).length) throw coded("erasure_inventory_changed", "versions survived permanent erasure");
      return deleted;
    },
    eraseSessionAnyDate: async (base, segment, before) => {
      if (!base.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const { erase, kept } = cut((await versions(`${base}d=`)).filter((v) => inSessionAnyDate(base, segment, v.key)), before);
      return { deleted: await remove(erase), kept };
    },
    eraseObjectBefore: async (key, before) => {
      const { erase, kept } = cut((await versions(key)).filter((v) => v.key === key), before);
      return { deleted: await remove(erase), kept };
    },
    listKeys: async (prefix) => {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      return [...new Set((await versions(prefix)).map((v) => v.key))];
    },
    listVersions: (prefix, opts) => strictInventory(strictPage, prefix, opts?.beforeRequest),
    requestDeadlineMs: deadlineMs,
    requestAttempts: S3_REQUEST_ATTEMPTS,
    // Exactly one version by id: never a plain DELETE (that would only add a delete marker on a versioned bucket).
    deleteVersion: async (key, versionId, opts) => {
      assertUnderPrefix(key.slice(0, key.lastIndexOf("/") + 1), [{ key, versionId }]);
      if (!versionId || /[\x00-\x1f\x7f]/u.test(versionId)) throw coded("erasure_key_shape", "an exact version id is required");
      const u = url(key);
      u.searchParams.set("versionId", versionId);
      await request("DELETE", u, [], opts?.beforeFetch);
    },
    bucketLifecycle: () => readLifecycle(url, request),
    bucketVersioning: () => readVersioning(url, request),
    // One strict ListObjectVersions page at the exact key (no GetObject/HeadObject grant needed). The exact key sorts
    // first under its own prefix and S3 lists its versions newest first, so its IsLatest entry is on the first page.
    objectLive: async (key) => {
      if (!key || key.endsWith("/") || key.split("/").some((seg) => seg === "." || seg === ".."))
        throw coded("erasure_key_shape", "an existence check names one object");
      const page = await fetchPage(key);
      checkPage(key, page, undefined, undefined, true);
      const latest = page.entries.filter((v) => v.key === key && v.latest);
      return latest.length === 1 && !latest[0]!.deleteMarker;
    },
  };
}

// ------------------------------------------------------------------ stale-prefix verification and receipt fence

/** A strict, complete, bounded version inventory of one directory prefix (every version and delete marker). */
export interface VersionLister {
  /**
   * `beforeRequest` runs for EVERY page request after it is signed and immediately before it is sent (the fence's
   * hard deadline and page budget); throwing stops the listing with that page unsent.
   */
  listVersions(prefix: string, opts?: { beforeRequest?: BeforeFetch }): Promise<ListedVersion[]>;
}
/** Read-only: the lister plus GET of one exact object version (bounded bytes). Needs s3:GetObjectVersion. */
export interface VersionReader extends VersionLister {
  getVersion(key: string, versionId: string, maxBytes: number, opts?: { beforeFetch?: BeforeFetch }): Promise<Uint8Array>;
}
/** The bucket's lifecycle configuration as read (raw XML only when rules exist); never guessed. */
export type LifecycleRead = { status: "none" } | { status: "rules"; xml: string } | { status: "unreadable"; code: string };
export type VersioningRead = { status: "Enabled" | "Suspended" | "Unversioned" } | { status: "unreadable"; code: string };
export interface BucketProtectionReader {
  bucketLifecycle(): Promise<LifecycleRead>;
  bucketVersioning(): Promise<VersioningRead>;
}
/**
 * What the receipt-fenced stale-prefix execute needs from the store, and nothing more: the strict inventory (old-set
 * drift and survivor pin checks are ListObjectVersions, so the eraser key needs no GetObject), exact-version deletes,
 * and the read-only bucket protection facts for the preflight.
 */
export interface ReceiptFence extends VersionLister, BucketProtectionReader {
  /** `beforeFetch` runs after signing, immediately before the DELETE is sent; throwing means it is never sent. */
  deleteVersion(key: string, versionId: string, opts?: { beforeFetch?: BeforeFetch }): Promise<void>;
  /** Worst-case time of one request (its abort deadline) and attempts per request: the lease-TTL bound uses both. */
  readonly requestDeadlineMs: number;
  readonly requestAttempts: number;
}
export function isReceiptFence(e: unknown): e is ReceiptFence {
  return !!e && ["listVersions", "deleteVersion", "bucketLifecycle", "bucketVersioning"].every((m) => typeof (e as Record<string, unknown>)[m] === "function")
    && Number.isSafeInteger((e as ReceiptFence).requestDeadlineMs) && Number.isSafeInteger((e as ReceiptFence).requestAttempts);
}

/**
 * THE strict whole-prefix inventory, over any strict page fetcher: the same page rules as the paged eraser (checkPage:
 * every key under the prefix, no dot segments, C order, no duplicate in a page, monotonic cursor, markers equal to the
 * last entry), no duplicate across pages, no cursor cycle, and the same hard bounds (10,000 versions, 100 pages); any
 * breach throws, never a short list.
 */
/** A strict page fetcher that runs `beforeFetch` after signing, immediately before sending (S3); others call it first. */
export type GatedPageFetcher = (prefix: string, keyMarker?: string, versionMarker?: string, beforeFetch?: BeforeFetch) => Promise<StrictPage>;
export async function strictInventory(fetchPage: GatedPageFetcher, prefix: string, beforeRequest?: BeforeFetch): Promise<ListedVersion[]> {
  if (!prefix.endsWith("/") || prefix.split("/").some((seg) => seg === "." || seg === ".."))
    throw coded("erasure_prefix_shape", "a prefix ends in / and has no dot segment");
  const out: ListedVersion[] = [];
  const seen = new Set<string>(), cursors = new Set<string>();
  let keyMarker: string | undefined, versionMarker: string | undefined, pages = 0;
  for (;;) {
    if (pages >= ERASE_RUN_MAX_PAGES) throw coded("erasure_inventory", "version pagination is unbounded");
    const page = await fetchPage(prefix, keyMarker, versionMarker, beforeRequest);
    pages++;
    checkPage(prefix, page, keyMarker, versionMarker);
    for (const v of page.entries) {
      const id = JSON.stringify([v.key, v.versionId]);
      if (seen.has(id)) throw coded("erasure_inventory", "duplicate version across inventory pages");
      seen.add(id);
      out.push(v);
    }
    if (out.length > ERASE_RUN_MAX_VERSIONS) throw coded("erasure_inventory", "version inventory exceeds bound");
    if (!page.truncated) return out;
    keyMarker = page.nextKey; versionMarker = page.nextVersion;
    const c = JSON.stringify([keyMarker, versionMarker]);
    if (cursors.has(c)) throw coded("erasure_inventory", "version pagination cycle");
    cursors.add(c);
  }
}

/** A response body read to at most `max` bytes (refused beyond it). */
async function boundedBody(res: Response, max: number, code: string): Promise<Uint8Array> {
  const reader = res.body?.getReader(), chunks: Uint8Array[] = []; let size = 0;
  try {
    if (!reader) return new Uint8Array();
    for (;;) {
      const r = await reader.read(); if (r.done) break;
      size += r.value.byteLength;
      if (size > max) throw coded(code, "response byte bound");
      chunks.push(r.value);
    }
  } finally { await reader?.cancel().catch(() => {}); reader?.releaseLock(); }
  const body = new Uint8Array(size); let at = 0;
  for (const c of chunks) { body.set(c, at); at += c.byteLength; }
  return body;
}
const utf8Strict = (b: Uint8Array, code: string) => {
  try { return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(b); } catch { throw coded(code, "response encoding"); }
};
type Requester = (method: string, u: URL, pass?: readonly number[]) => Promise<Response>;

/** GetBucketLifecycleConfiguration, read-only. Only S3's own NoSuchLifecycleConfiguration means "no rules". */
async function readLifecycle(url: (k?: string) => URL, request: Requester): Promise<LifecycleRead> {
  const u = url(); u.searchParams.set("lifecycle", "");
  let res: Response;
  try { res = await request("GET", u, [404]); } catch (e) { return { status: "unreadable", code: (e as { code?: string }).code ?? "s3_error" }; }
  try {
    const xml = utf8Strict(await boundedBody(res, 1024 * 1024, "lifecycle_unreadable"), "lifecycle_unreadable");
    if (res.status === 404) return /<Code>NoSuchLifecycleConfiguration<\/Code>/.test(xml) ? { status: "none" } : { status: "unreadable", code: "s3_404" };
    return { status: "rules", xml };
  } catch (e) { return { status: "unreadable", code: (e as { code?: string }).code ?? "lifecycle_unreadable" }; }
}

/** GetBucketVersioning, read-only: no Status element means the bucket was never versioned. */
async function readVersioning(url: (k?: string) => URL, request: Requester): Promise<VersioningRead> {
  const u = url(); u.searchParams.set("versioning", "");
  try {
    const xml = utf8Strict(await boundedBody(await request("GET", u), 64 * 1024, "versioning_unreadable"), "versioning_unreadable");
    assertInventoryXml(xml, "VersioningConfiguration");
    const st = xmlText(xml, "Status");
    if (st === undefined) return { status: "Unversioned" };
    if (st === "Enabled" || st === "Suspended") return { status: st };
    return { status: "unreadable", code: "versioning_unknown" };
  } catch (e) { return { status: "unreadable", code: (e as { code?: string }).code ?? "versioning_unreadable" }; }
}

/**
 * The stale-prefix verifier's READ-ONLY client (ListBucketVersions + GetObjectVersion): the same signed core and
 * strict inventory as the eraser, plus GET of one exact version. It has no delete method at all.
 */
export function createS3VersionReader(cfg: S3EraserConfig): VersionReader & BucketProtectionReader {
  const { url, request, strictPage } = s3Core(cfg, "reader");
  return {
    listVersions: (prefix, opts) => strictInventory(strictPage, prefix, opts?.beforeRequest),
    async getVersion(key, versionId, maxBytes, opts) {
      if (!key || key.endsWith("/") || key.split("/").some((seg) => seg === "." || seg === "..")) throw coded("erasure_key_shape", "a GET names one object");
      if (!versionId || /[\x00-\x1f\x7f]/u.test(versionId)) throw coded("erasure_key_shape", "an exact version id is required");
      const u = url(key);
      u.searchParams.set("versionId", versionId);
      const res = await request("GET", u, [], opts?.beforeFetch);
      // The provider must answer with the very version asked for.
      const got = res.headers.get("x-amz-version-id");
      if (got !== versionId) { await res.body?.cancel().catch(() => {}); throw coded("reader_version_mismatch", "the response is not the requested version"); }
      return boundedBody(res, maxBytes, "reader_object_too_large");
    },
    bucketLifecycle: () => readLifecycle(url, request),
    bucketVersioning: () => readVersioning(url, request),
  };
}

/**
 * The same contract over a plain (unversioned) store, for TESTS AND LOCAL RUNS ONLY.
 * Its paged listing exhausts the backend's `list` cursors for the directory and materializes,
 * filters and sorts the result before it returns one synthetic page, so the outer page/version
 * budgets bound the TRAVERSAL, not the backend list work. It is a functional model of the
 * contract, NOT a bounded remote-store implementation and NOT evidence of a production budget
 * (the versioned S3 eraser issues one bounded, key-prefix ListObjectVersions request per page).
 */
export function storeEraser(store: ObjectStore): Eraser & PagedEraser & LiveObjectProbe {
  // Unversioned: one "null" version per key, pages of 1000 keys after the key marker.
  const fetchPage: PageFetcher = async (prefix, keyMarker) => {
    const items: { key: string; lastModified?: number }[] = [];
    let cursor: string | undefined;
    do {
      // A directory prefix lists itself; an exact key lists its directory, filtered to the key.
      const base = prefix.endsWith("/") ? prefix.slice(0, -1) : prefix.slice(0, prefix.lastIndexOf("/"));
      const page = await store.list(objectKey(base), cursor);
      for (const i of page.items) {
        const key = String(i.key);
        if (!key.startsWith(prefix)) continue;
        const at = i.lastModified instanceof Date ? i.lastModified.getTime() : NaN;
        // Same shape as the S3 eraser's entries: lastModified only when readable.
        items.push({ key, ...(Number.isFinite(at) ? { lastModified: at } : {}) });
      }
      cursor = page.nextCursor;
    } while (cursor);
    const after = items.sort((a, b) => cmpC(a.key, b.key)).filter((i) => keyMarker === undefined || cmpC(i.key, keyMarker) > 0);
    const entries = after.slice(0, 1000).map((i) => ({ ...i, versionId: "null", latest: true, deleteMarker: false }));
    const truncated = after.length > 1000;
    return truncated ? { entries, truncated, nextKey: entries[entries.length - 1]!.key, nextVersion: "null" } : { entries, truncated };
  };
  const paged = createPagedEraser("store", fetchPage, async (list) => { for (const v of list) await store.delete(objectKey(v.key)); return list.length; });
  return {
    eraseStep: paged.eraseStep,
    verifyErased: paged.verifyErased,
    async eraseObject(key) {
      const had = await store.head(objectKey(key));
      await store.delete(objectKey(key));
      return had ? 1 : 0;
    },
    async erasePrefix(prefix) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => i.key));
        cursor = page.nextCursor;
      } while (cursor);
      keys.sort((a, b) => Number(b.endsWith("/manifest.json")) - Number(a.endsWith("/manifest.json")));
      for (const k of keys) await store.delete(objectKey(k));
      return keys.length;
    },
    async eraseSessionAnyDate(base, segment, before) {
      if (!base.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const items: { key: string; lastModified: number }[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(base.slice(0, -1)), cursor);
        for (const i of page.items)
          if (inSessionAnyDate(base, segment, i.key)) items.push({ key: i.key, lastModified: i.lastModified.getTime() });
        cursor = page.nextCursor;
      } while (cursor);
      const { erase, kept } = cut(items, before);
      erase.sort((a, b) => Number(b.key.endsWith("/manifest.json")) - Number(a.key.endsWith("/manifest.json")));
      for (const i of erase) await store.delete(objectKey(i.key));
      return { deleted: erase.length, kept };
    },
    async eraseObjectBefore(key, before) {
      const meta = await store.head(objectKey(key));
      if (!meta) return { deleted: 0, kept: 0 };
      if (meta.lastModified.getTime() >= before.getTime()) return { deleted: 0, kept: 1 };
      await store.delete(objectKey(key));
      return { deleted: 1, kept: 0 };
    },
    async listKeys(prefix) {
      if (!prefix.endsWith("/")) throw coded("erasure_prefix_shape", "a prefix ends in /");
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => i.key));
        cursor = page.nextCursor;
      } while (cursor);
      return keys;
    },
    async objectLive(key) {
      if (!key || key.endsWith("/") || key.split("/").some((seg) => seg === "." || seg === ".."))
        throw coded("erasure_key_shape", "an existence check names one object");
      return !!(await store.head(objectKey(key)));
    },
  };
}
