/**
 * SearchIndexAdapter over the OpenSearch / Elasticsearch REST API.
 *
 * One adapter, two dialects: OpenSearch (forked from Elasticsearch 7.10) and
 * Elasticsearch 7.10+/8.x share every endpoint used here (`_bulk` with
 * `version_type=external_gte`, `refresh=wait_for`, `_delete_by_query`,
 * `search_after`, `wildcard` on `keyword`). The dialect only selects the
 * product check `ensureSchema` performs, so pointing the adapter at the wrong
 * product fails loudly instead of half-working. Plain `fetch` against the
 * configured base URL; no vendor client and no other outbound request.
 *
 * Layout follows the postgres adapter: one index (`<prefix>-transcript-events`)
 * holds every tenant, and every read or delete carries org + workspace as
 * `filter` clauses (never `should`). A document's `_id` is a SHA-256 over the
 * tenant + position key, so an upsert of the same key replaces the row.
 *
 * q semantics are exact, not analyzer-based: the contract's JavaScript
 * lowercase of the content prefix (first CONTENT_PREFIX_CHARS code points) and
 * of the task title is stored as overlapping keyword windows, and q (lowercased
 * the same way) is a `wildcard` `*literal*` query with `*`, `?` and `\`
 * escaped, or, for a folded literal longer than WILDCARD_MAX_LITERAL_BYTES, a
 * params-bound painless `contains` over the same window values (see
 * qFilter). Any folded q (at most 2 * MAX_QUERY_CHARS code points) fits wholly
 * inside one window, so a match equals `matchesQuery` on either path.
 *
 * Errors carry bounded codes only (`http_<status>`, `timeout`, `network`, and
 * for an HTTP error the server's `error.type` / `error.caused_by.type` names),
 * never response bodies or `reason` text, which can quote document content or q.
 * A failed `_bulk` write also names its item count and body bytes, and for
 * failed items the first failing item's status and error type names
 * (e.g. `http_429:bulk_items_812:bulk_bytes_4190000`).
 */
import {
  CONTENT_PREFIX_CHARS,
  MAX_QUERY_CHARS,
  MAX_UPSERT_BATCH,
  SearchIndexError,
  assertDocument,
  assertPosition,
  assertQuery,
  assertScope,
  assertTenant,
  positionOf,
  truncateContent,
  type SearchCallContext,
  type SearchEventDocument,
  type SearchFingerprint,
  type SearchHit,
  type SearchIndexAdapter,
  type SearchIndexCapabilities,
  type SearchIndexHealth,
  type SearchPage,
  type SearchPosition,
  type SearchQuery,
  type SearchScope,
} from "./contracts.js";
// The S3 signer, generalized by service. Imported by source like the other
// object-store primitives in this package (destinations/local.ts).
import { signRequest, type AwsCredentials } from "../../../object-store/src/sigv4.js";
import { sha256Hex } from "../../../object-store/src/crypto.js";
import { cachedProvider } from "../../../object-store/src/core.js";

export type OpenSearchDialect = "opensearch" | "elasticsearch";
export const OPENSEARCH_DIALECTS: readonly OpenSearchDialect[] = ["opensearch", "elasticsearch"];
export type OpenSearchAuth =
  | { readonly kind: "none" }
  | { readonly kind: "basic"; readonly username: string; readonly password: string }
  | { readonly kind: "sigv4"; readonly region: string; readonly credentials: () => Promise<AwsCredentials> };

export type FetchLike = (input: string, init: RequestInit & { tls?: { ca?: string } }) => Promise<Response>;

export interface OpenSearchSearchIndexConfig {
  readonly id?: string;
  /** Base URL (http or https, optional path prefix, no credentials/query). */
  readonly url: string;
  readonly dialect?: OpenSearchDialect;
  /** Index name prefix; the index is `<prefix>-transcript-events`. Default `relayhistory`. */
  readonly indexPrefix?: string;
  readonly auth?: OpenSearchAuth;
  /** PEM CA bundle (contents, not a path) trusted for an https URL. Bun runtime only. */
  readonly ca?: string;
  /** Per request. Default 10000. */
  readonly requestTimeoutMs?: number;
  /**
   * Upper bound (bytes) of one `_bulk` NDJSON body. Default
   * DEFAULT_OPENSEARCH_BULK_MAX_BYTES. A single document larger than this is
   * still written, alone in its own request.
   */
  readonly bulkMaxBytes?: number;
  /** Injected for tests; defaults to the global fetch. */
  readonly fetch?: FetchLike;
  /** Injected for tests; waits between deleted-key write retries. */
  readonly sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

export const DEFAULT_OPENSEARCH_INDEX_PREFIX = "relayhistory";
export const DEFAULT_OPENSEARCH_TIMEOUT_MS = 10_000;
/** Upper bound for requestTimeoutMs (and SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS). */
export const MAX_OPENSEARCH_TIMEOUT_MS = 600_000;
const PREFIX = /^[a-z0-9][a-z0-9_-]{0,100}$/;
/** Folded-text window (code points) and stride: any needle up to WINDOW - STRIDE + 1 fits in one window. */
const WINDOW = 4096;
const STRIDE = 2048;
/** Rows per search_after page of a fingerprint scan (under index.max_result_window). */
const SCAN_PAGE = 5000;
/**
 * Default bound of one `_bulk` request body (bytes, the serialized NDJSON).
 * Amazon OpenSearch Service caps an HTTP request body at 10 MiB on its small
 * instance types (and answers 429 / 502 near that size), well below the
 * engine's default http.max_content_length of 100mb. Override with
 * `bulkMaxBytes` (SEARCH_INDEX_OPENSEARCH_BULK_MAX_BYTES).
 */
export const DEFAULT_OPENSEARCH_BULK_MAX_BYTES = 4 * 1024 * 1024;
const ERASE_ATTEMPTS = 5;
/**
 * Waits before re-writing a key whose only conflict is a delete tombstone.
 * The engine prunes a tombstone on a refresh once it is older than
 * gc_deletes (0s) on its coarse clock (thread_pool.estimated_time_interval,
 * 200ms by default), so a write right after a delete still conflicts.
 */
const TOMBSTONE_RETRY_DELAYS_MS = [250, 250, 500, 1000, 2000];

function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new SearchIndexError("aborted"));
    const onAbort = () => {
      clearTimeout(timer);
      reject(new SearchIndexError("aborted"));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

// The folded-q bound the window arithmetic relies on (İ lowercases to two code points).
if (2 * MAX_QUERY_CHARS > WINDOW - STRIDE + 1) throw new Error("opensearch window too small for MAX_QUERY_CHARS");

const KEY_FIELDS = ["ts_ms", "session_id", "source", "machine_id", "kind", "event_id"] as const;
const id = { type: "keyword" } as const;
const stored = { type: "keyword", index: false, doc_values: false } as const;

/** Explicit mapping for every field; `dynamic: strict` rejects anything else. */
export const OPENSEARCH_MAPPINGS = Object.freeze({
  dynamic: "strict",
  properties: {
    org_id: id,
    workspace_id: id,
    ts_ms: { type: "long" },
    session_id: id,
    source: id,
    machine_id: id,
    kind: id,
    event_id: id,
    type: stored,
    user_id: stored,
    actor_name: stored,
    actor_role: stored,
    subagent_id: stored,
    project_id: id,
    task_title: stored,
    task_ref: stored,
    task_ref_id: stored,
    tags: stored,
    files_touched: stored,
    tool_name: stored,
    tool_status: stored,
    delivery_record_id: stored,
    content: stored,
    content_bytes: { type: "long", index: false },
    // doc_values: the long-q script filter reads the window values from doc values.
    content_prefix_folded: { type: "keyword" },
    task_title_folded: { type: "keyword" },
    fingerprint: stored,
    version: { type: "long", index: false },
  },
});

/** Index settings. gc_deletes 0: a deleted key can be re-written at any version, as a deleted postgres row can. */
export const OPENSEARCH_SETTINGS = Object.freeze({ index: { gc_deletes: "0s" } });
/** The spellings of a zero gc_deletes that `ensureSchema` accepts on an existing index. */
const ZERO_GC_DELETES = new Set(["0", "0s", "0ms"]);

interface Source {
  org_id: string;
  workspace_id: string;
  ts_ms: number;
  session_id: string;
  source: string;
  machine_id: string;
  kind: string;
  event_id: string;
  type: string;
  user_id: string;
  actor_name: string;
  actor_role: string;
  subagent_id: string;
  project_id: string;
  task_title: string;
  task_ref: string;
  tags: string[];
  files_touched: string[];
  tool_name: string;
  tool_status: string;
  content: string;
  fingerprint: string;
}

/** Overlapping code-point windows of an already folded string. */
export function foldedWindows(folded: string): string[] {
  const cps = [...folded];
  if (cps.length <= WINDOW) return folded ? [folded] : [];
  const out: string[] = [];
  for (let start = 0; ; start += STRIDE) {
    out.push(cps.slice(start, start + WINDOW).join(""));
    if (start + WINDOW >= cps.length) return out;
  }
}

/** A literal for a wildcard query: `*`, `?` and `\` are escaped. */
export function wildcardLiteral(q: string): string {
  return `*${q.replace(/[\\*?]/g, (c) => `\\${c}`)}*`;
}

/**
 * Longest folded q (UTF-8 bytes) sent as a `wildcard`; anything longer uses
 * the script filter. Lucene compiles `*L*` to a code-point automaton, converts
 * it to UTF-8 bytes and determinizes it by subset construction under
 * Operations.DEFAULT_DETERMINIZE_WORK_LIMIT = 10,000 (exceeding it is
 * TooComplexToDeterminizeException, an HTTP 400 on both products). The worst
 * L is a period-1 byte run (`aaaa...`): with B literal bytes, the DFA state
 * after a partial match of k bytes holds the loop state plus k + 1 literal
 * positions, and the trailing `*` repeats that chain once more after a full
 * match, so the work (the sum of subset sizes, Lucene 9's effort measure, and
 * an upper bound on the DFA state count, the older one) is about
 * 2 * sum_{k=0..B} (k + 1) = (B + 1)(B + 2), plus at most ~4 UTF-8 loop
 * states per subset: 2 * (B + 1) * 4. Periodic or multi-byte literals (`abab`,
 * `İİ` folded to `i\u0307i\u0307`) keep about k / period + 1 positions per
 * subset, so they cost less per byte. For B = 64: 65 * 66 + 2 * 65 * 4 =
 * 4,290 + 520 = 4,810 <= 10,000 / 2, a 2x margin against the limit read as
 * 10,000 work units (Lucene 9 actually budgets 10x that effort; not relied on).
 * At B = 500 (`a` x 500) the same estimate is ~250,000: the CI failure.
 */
export const WILDCARD_MAX_LITERAL_BYTES = 64;

/** Fixed script source: q only ever travels as params.q, never in the source. */
export const Q_CONTAINS_SCRIPT =
  "for (def v : doc['content_prefix_folded']) { if (v.contains(params.q)) { return true; } } " +
  "for (def v : doc['task_title_folded']) { if (v.contains(params.q)) { return true; } } " +
  "return false;";

/**
 * The q filter clause for an already folded q. Short literals: a `wildcard`
 * per field. Long literals: one `script` filter doing String.contains over
 * every window value of both fields (Java `contains` and JS `includes` both
 * compare UTF-16 code units, and each window value is a whole code-point
 * run, so this equals the wildcard / `matchesQuery`). The script needs no
 * index or cluster setting beyond defaults (inline painless allowed, doc
 * values on the two fields) and compiles once: its source never varies.
 */
export function qFilter(folded: string): Record<string, unknown> {
  if (new TextEncoder().encode(folded).length <= WILDCARD_MAX_LITERAL_BYTES) {
    const value = wildcardLiteral(folded);
    return {
      bool: {
        should: [{ wildcard: { content_prefix_folded: { value } } }, { wildcard: { task_title_folded: { value } } }],
        minimum_should_match: 1,
      },
    };
  }
  return { script: { script: { lang: "painless", source: Q_CONTAINS_SCRIPT, params: { q: folded } } } };
}

const ERROR_TYPE = /^[a-z][a-z0-9_.]{0,99}$/;

/** `http_<status>` plus the server's error type names (never `reason`, which can quote q or content). */
function httpDetail(status: number, body: unknown): string {
  const error = (body as { error?: unknown } | undefined)?.error;
  if (!error || typeof error !== "object") return `http_${status}`;
  const named = (v: unknown) => (typeof v === "string" && ERROR_TYPE.test(v) ? v : undefined);
  const type = named((error as { type?: unknown }).type);
  const cause = named((error as { caused_by?: { type?: unknown } }).caused_by?.type);
  return [`http_${status}`, type, cause].filter((x) => x !== undefined).join(":");
}

/** The bounded error type names of a bulk item error (never `reason`, which can quote document content). */
function itemErrorTypes(error: unknown): string[] {
  if (!error || typeof error !== "object") return [];
  const named = (v: unknown) => (typeof v === "string" && ERROR_TYPE.test(v) ? v : undefined);
  const type = named((error as { type?: unknown }).type);
  const cause = named((error as { caused_by?: { type?: unknown } }).caused_by?.type);
  return [type, cause].filter((x): x is string => x !== undefined);
}

function toSource(d: SearchEventDocument) {
  return {
    org_id: d.orgId,
    workspace_id: d.workspaceId,
    ts_ms: d.tsMs,
    session_id: d.sessionId,
    source: d.source,
    machine_id: d.machineId,
    kind: d.kind,
    event_id: d.eventId,
    type: d.type,
    user_id: d.userId,
    actor_name: d.actorName,
    actor_role: d.actorRole,
    subagent_id: d.subagentId,
    project_id: d.projectId,
    task_title: d.taskTitle,
    task_ref: d.taskRef,
    task_ref_id: d.taskRefId,
    tags: [...d.tags],
    files_touched: [...d.filesTouched],
    tool_name: d.toolName,
    tool_status: d.toolStatus,
    delivery_record_id: d.deliveryRecordId,
    content: d.content,
    content_bytes: new TextEncoder().encode(d.content).length,
    content_prefix_folded: foldedWindows([...d.content].slice(0, CONTENT_PREFIX_CHARS).join("").toLowerCase()),
    task_title_folded: foldedWindows(d.taskTitle.toLowerCase()),
    fingerprint: d.fingerprint,
    version: d.version,
  };
}

function toHit(s: Source, maxContentChars: number | undefined): SearchHit {
  return {
    orgId: s.org_id,
    workspaceId: s.workspace_id,
    tsMs: Number(s.ts_ms),
    sessionId: s.session_id,
    source: s.source,
    machineId: s.machine_id,
    kind: s.kind,
    eventId: s.event_id,
    type: s.type,
    userId: s.user_id,
    actorName: s.actor_name,
    actorRole: s.actor_role,
    subagentId: s.subagent_id,
    projectId: s.project_id,
    taskTitle: s.task_title,
    taskRef: s.task_ref,
    tags: s.tags,
    filesTouched: s.files_touched,
    toolName: s.tool_name,
    toolStatus: s.tool_status,
    content: truncateContent(s.content, maxContentChars),
  };
}

const HIT_FIELDS = [
  "org_id", "workspace_id", "ts_ms", "session_id", "source", "machine_id", "kind", "event_id", "type", "user_id",
  "actor_name", "actor_role", "subagent_id", "project_id", "task_title", "task_ref", "tags", "files_touched",
  "tool_name", "tool_status", "content",
];

/** The stable `_id` of a row: SHA-256 hex of the tenant + position key. */
export async function documentId(tenant: { orgId: string; workspaceId: string }, p: SearchPosition): Promise<string> {
  const key = JSON.stringify([tenant.orgId, tenant.workspaceId, p.tsMs, p.sessionId, p.source, p.machineId, p.kind, p.eventId]);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)));
  return [...digest].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function sortClause(order: "asc" | "desc") {
  return KEY_FIELDS.map((f) => ({ [f]: { order } }));
}

const tenantFilter = (orgId: string, workspaceId: string) => [{ term: { org_id: orgId } }, { term: { workspace_id: workspaceId } }];

function scopeFilter(scope: SearchScope) {
  return [...tenantFilter(scope.orgId, scope.workspaceId), { term: { source: scope.source } }, { terms: { session_id: [...scope.sessionIds] } }];
}

export function normalizeBaseUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new SearchIndexError("invalid_argument", false, "url");
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password || url.search || url.hash) {
    throw new SearchIndexError("invalid_argument", false, "url");
  }
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}

function isAbort(error: unknown): boolean {
  return (error as { name?: unknown })?.name === "AbortError" || (error as { name?: unknown })?.name === "TimeoutError";
}

const retryableStatus = (status: number) => status === 429 || status >= 500;

/** Loopback hosts, where plain http never leaves the machine. */
function isLoopbackHost(hostname: string): boolean {
  // WHATWG URL normalizes IPv4 shorthands (127.1) to dotted quads and brackets IPv6.
  return hostname === "localhost" || hostname === "[::1]" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(hostname);
}

export function createOpenSearchSearchIndex(config: OpenSearchSearchIndexConfig): SearchIndexAdapter {
  const base = normalizeBaseUrl(config.url);
  const dialect = config.dialect ?? "opensearch";
  if (!OPENSEARCH_DIALECTS.includes(dialect)) throw new SearchIndexError("invalid_argument", false, "dialect");
  const prefix = config.indexPrefix ?? DEFAULT_OPENSEARCH_INDEX_PREFIX;
  if (!PREFIX.test(prefix)) throw new SearchIndexError("invalid_argument", false, "index_prefix");
  const index = `${prefix}-transcript-events`;
  const timeoutMs = config.requestTimeoutMs ?? DEFAULT_OPENSEARCH_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_OPENSEARCH_TIMEOUT_MS) throw new SearchIndexError("invalid_argument", false, "timeout");
  const bulkMaxBytes = config.bulkMaxBytes ?? DEFAULT_OPENSEARCH_BULK_MAX_BYTES;
  if (!Number.isSafeInteger(bulkMaxBytes) || bulkMaxBytes < 1) throw new SearchIndexError("invalid_argument", false, "bulk_max_bytes");
  const auth = config.auth ?? { kind: "none" };
  let authorization: string | undefined;
  let sigv4: { region: string; credentials: () => Promise<AwsCredentials> } | undefined;
  if (auth.kind === "basic") {
    if (!auth.username || auth.username.includes(":") || typeof auth.password !== "string" || !auth.password) {
      throw new SearchIndexError("invalid_argument", false, "basic_auth");
    }
    // Basic credentials are replayable: never send them in cleartext off the machine.
    const { protocol, hostname } = new URL(base);
    if (protocol !== "https:" && !isLoopbackHost(hostname)) {
      throw new SearchIndexError("invalid_argument", false, "basic_auth_needs_https");
    }
    const bytes = new TextEncoder().encode(`${auth.username}:${auth.password}`);
    authorization = `Basic ${btoa(String.fromCharCode(...bytes))}`;
  } else if (auth.kind === "sigv4") {
    // Amazon OpenSearch Service (service "es"). The session token is a bearer
    // secret too: never send it in cleartext off the machine.
    if (typeof auth.region !== "string" || !/^[a-z0-9-]{1,32}$/.test(auth.region) || typeof auth.credentials !== "function") {
      throw new SearchIndexError("invalid_argument", false, "sigv4_config");
    }
    const { protocol, hostname } = new URL(base);
    if (protocol !== "https:" && !isLoopbackHost(hostname)) {
      throw new SearchIndexError("invalid_argument", false, "sigv4_needs_https");
    }
    sigv4 = { region: auth.region, credentials: cachedProvider(auth.credentials) };
  } else if (auth.kind !== "none") {
    throw new SearchIndexError("invalid_argument", false, "auth");
  }
  if (config.ca !== undefined) {
    if (typeof config.ca !== "string" || !config.ca.includes("-----BEGIN")) throw new SearchIndexError("invalid_argument", false, "ca");
    if (!base.startsWith("https:")) throw new SearchIndexError("invalid_argument", false, "ca_needs_https");
    // Only Bun's fetch takes a per-request CA; elsewhere it would be silently ignored.
    if (config.fetch === undefined && typeof (globalThis as { Bun?: unknown }).Bun === "undefined") {
      throw new SearchIndexError("unsupported", false, "ca_needs_bun");
    }
  }
  const doFetch: FetchLike = config.fetch ?? ((input, init) => fetch(input, init as RequestInit));

  interface Reply {
    status: number;
    headers: Headers;
    body: unknown;
  }

  /** One request to `<base>/<path>`. Never follows redirects; maps failures to bounded codes. */
  async function request(
    method: string,
    path: string,
    body: unknown,
    context: SearchCallContext | undefined,
    options: { ndjson?: string; accept?: number[] } = {},
  ): Promise<Reply> {
    if (context?.signal?.aborted) throw new SearchIndexError("aborted");
    const timeout = AbortSignal.timeout(timeoutMs);
    const signal = context?.signal ? AbortSignal.any([context.signal, timeout]) : timeout;
    const headers: Record<string, string> = { accept: "application/json" };
    if (authorization) headers.authorization = authorization;
    let payload: string | undefined;
    if (options.ndjson !== undefined) {
      headers["content-type"] = "application/x-ndjson";
      payload = options.ndjson;
    } else if (body !== undefined) {
      headers["content-type"] = "application/json";
      payload = JSON.stringify(body);
    }
    let signed: Headers | undefined;
    if (sigv4) {
      signed = new Headers(headers);
      try {
        await signRequest({
          method,
          url: new URL(`${base}/${path}`),
          headers: signed,
          payloadSha256: await sha256Hex(payload ?? ""),
          region: sigv4.region,
          service: "es",
          credentials: await sigv4.credentials(),
        });
      } catch {
        // The credential source's message is not ours to echo.
        throw new SearchIndexError("unavailable", true, "credentials");
      }
    }
    let response: Response;
    let text: string;
    try {
      response = await doFetch(`${base}/${path}`, {
        method,
        headers: signed ?? headers,
        body: payload,
        redirect: "error",
        signal,
        ...(config.ca !== undefined ? { tls: { ca: config.ca } } : {}),
      });
      text = method === "HEAD" ? "" : await response.text();
    } catch (error) {
      if (context?.signal?.aborted) throw new SearchIndexError("aborted");
      if (timeout.aborted || isAbort(error)) throw new SearchIndexError("unavailable", true, "timeout");
      throw new SearchIndexError("unavailable", true, "network");
    }
    let parsed: unknown;
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = undefined;
      }
    }
    const status = response.status;
    if ((status >= 200 && status < 300) || options.accept?.includes(status)) return { status, headers: response.headers, body: parsed };
    const type = (parsed as { error?: { type?: unknown } } | undefined)?.error?.type;
    if (status === 404 && (type === "index_not_found_exception" || method === "HEAD")) throw new SearchIndexError("schema_missing");
    throw new SearchIndexError("unavailable", retryableStatus(status), httpDetail(status, parsed));
  }

  function invalidReply(): never {
    throw new SearchIndexError("unavailable", false, "bad_response");
  }

  async function search(body: Record<string, unknown>, context?: SearchCallContext): Promise<Source[]> {
    const reply = await request("POST", `${index}/_search`, body, context);
    const result = reply.body as { timed_out?: unknown; _shards?: { failed?: unknown }; hits?: { hits?: { _source?: Source }[] } } | undefined;
    const hits = result?.hits?.hits;
    if (!Array.isArray(hits)) invalidReply();
    // A partial result (a shard failed or timed out) would page with gaps.
    if (result!.timed_out === true || (typeof result!._shards?.failed === "number" && result!._shards.failed > 0)) {
      throw new SearchIndexError("unavailable", true, "partial_results");
    }
    return hits.map((h) => {
      if (!h || typeof h._source !== "object" || h._source === null) invalidReply();
      return h._source;
    });
  }

  async function deleteByQuery(filter: unknown[], context?: SearchCallContext) {
    const reply = await request("POST", `${index}/_delete_by_query?refresh=true&conflicts=proceed`, { query: { bool: { filter } } }, context);
    const r = reply.body as { deleted?: unknown; version_conflicts?: unknown; failures?: unknown; timed_out?: unknown } | undefined;
    if (!r || typeof r.deleted !== "number") invalidReply();
    if (r.timed_out === true || (Array.isArray(r.failures) && r.failures.length > 0)) {
      throw new SearchIndexError("unavailable", true, "delete_incomplete");
    }
    return { deleted: r.deleted, conflicts: typeof r.version_conflicts === "number" ? r.version_conflicts : 0 };
  }

  const capabilities: SearchIndexCapabilities = Object.freeze({
    backend: "opensearch",
    filters: Object.freeze(["session", "source", "kind", "project", "since", "until"] as const),
    // No q length is an indexed lookup: a short q walks the folded terms (leading
    // wildcard), a long one is a per-candidate script filter.
    minIndexedQueryChars: Number.MAX_SAFE_INTEGER,
    readAfterWrite: "immediate",
    synchronousErasure: true,
    versionedUpserts: true,
  });

  return {
    id: config.id ?? dialect,
    backend: "opensearch",
    capabilities: () => capabilities,

    async ensureSchema(context) {
      // Product check: the dialect must name the product actually answering.
      const root = await request("GET", "", undefined, context);
      const info = root.body as { version?: { distribution?: unknown; number?: unknown } } | undefined;
      if (!info || typeof info.version?.number !== "string") invalidReply();
      const product =
        info.version?.distribution === "opensearch"
          ? "opensearch"
          : root.headers.get("x-elastic-product") === "Elasticsearch" || /^7\.(?:[0-9]|1[0-3])\./.test(info.version.number)
            ? "elasticsearch"
            : undefined;
      if (product !== dialect) throw new SearchIndexError("unsupported", false, "dialect_mismatch");
      const created = await request(
        "PUT",
        index,
        { settings: OPENSEARCH_SETTINGS, mappings: OPENSEARCH_MAPPINGS },
        context,
        { accept: [400] },
      );
      if (created.status === 400) {
        const type = (created.body as { error?: { type?: unknown } } | undefined)?.error?.type;
        if (type !== "resource_already_exists_exception") throw new SearchIndexError("unavailable", false, "http_400");
        // Existing index: the same mapping must apply cleanly (a conflict is a 400).
        const merged = await request("PUT", `${index}/_mapping`, OPENSEARCH_MAPPINGS, context, { accept: [400] });
        if (merged.status === 400) throw new SearchIndexError("schema_missing", false, "mapping_conflict");
      }
      // Verify (never apply) gc_deletes: an existing or template-created index may
      // keep the 60s default, and the tombstone handling in upsert assumes 0.
      const settings = await request("GET", `${index}/_settings/index.gc_deletes?flat_settings=true`, undefined, context);
      const entries = Object.values((settings.body ?? {}) as Record<string, { settings?: Record<string, unknown> } | undefined>);
      if (entries.length !== 1) invalidReply();
      const gcDeletes = entries[0]?.settings?.["index.gc_deletes"];
      if (typeof gcDeletes !== "string" || !ZERO_GC_DELETES.has(gcDeletes)) {
        throw new SearchIndexError("schema_mismatch", false, "gc_deletes");
      }
    },

    async upsert(docs, context) {
      if (docs.length > MAX_UPSERT_BATCH) throw new SearchIndexError("invalid_argument");
      if (docs.length === 0) return { written: 0 };
      for (const doc of docs) assertDocument(doc);
      // One action per key (the newest version), as the postgres adapter writes one row per key.
      const newest = new Map<string, SearchEventDocument>();
      for (const doc of docs) {
        const key = JSON.stringify([doc.orgId, doc.workspaceId, doc.tsMs, doc.sessionId, doc.source, doc.machineId, doc.kind, doc.eventId]);
        const held = newest.get(key);
        if (!held || held.version <= doc.version) newest.set(key, doc);
      }
      const entries: { id: string; line: string }[] = [];
      for (const doc of newest.values()) {
        const id = await documentId(doc, doc);
        const action = { index: { _index: index, _id: id, version: doc.version, version_type: "external_gte" } };
        entries.push({ id, line: `${JSON.stringify(action)}\n${JSON.stringify(toSource(doc))}\n` });
      }
      let written = 0;
      /**
       * One _bulk call; returns the lines that hit a version conflict. A failure
       * keeps its code and retryable flag; its detail gains the request's item
       * count and body bytes (and, for failed items, the first failing item's
       * status and error type names and the failed count). Never a reason or
       * any document text.
       */
      const bulk = async (batch: { id: string; line: string }[]) => {
        const ndjson = batch.map((b) => b.line).join("");
        const shape = `bulk_items_${batch.length}:bulk_bytes_${new TextEncoder().encode(ndjson).length}`;
        let reply: Awaited<ReturnType<typeof request>>;
        try {
          reply = await request("POST", "_bulk?refresh=wait_for", undefined, context, { ndjson });
        } catch (error) {
          if (error instanceof SearchIndexError && error.code === "unavailable") {
            throw new SearchIndexError(error.code, error.retryable, `${error.detail ?? "error"}:${shape}`);
          }
          throw error;
        }
        const items = (reply.body as { items?: { index?: { status?: unknown; error?: unknown } }[] } | undefined)?.items;
        if (!Array.isArray(items) || items.length !== batch.length) invalidReply();
        let failure: { status: number; types: string[] } | undefined;
        let failed = 0;
        const conflicts: { id: string; line: string }[] = [];
        items.forEach((item, i) => {
          const status = item?.index?.status;
          if (typeof status !== "number") invalidReply();
          if (status >= 200 && status < 300) written += 1;
          else if (status === 409 && (item.index!.error as { type?: unknown } | undefined)?.type === "version_conflict_engine_exception") conflicts.push(batch[i]!);
          else {
            failed += 1;
            // The first failure, or the first retryable one: a retryable item keeps the whole call retryable.
            if (failure === undefined || (retryableStatus(status) && !retryableStatus(failure.status))) {
              failure = { status, types: itemErrorTypes(item.index!.error) };
            }
          }
        });
        if (failure !== undefined) {
          const detail = [`bulk_item_${failure.status}`, ...failure.types, `bulk_failed_${failed}`, shape].join(":");
          throw new SearchIndexError("unavailable", retryableStatus(failure.status), detail);
        }
        return conflicts;
      };
      /** The conflicts with no live document: the conflict is a delete tombstone, not a newer row. */
      const deletedOnly = async (conflicts: { id: string; line: string }[]) => {
        if (conflicts.length === 0) return conflicts;
        const reply = await request("POST", `${index}/_mget?realtime=true&_source=false`, { ids: conflicts.map((c) => c.id) }, context);
        const found = (reply.body as { docs?: { found?: unknown }[] } | undefined)?.docs;
        if (!Array.isArray(found) || found.length !== conflicts.length) invalidReply();
        // An item without a boolean `found` (an error item) is neither live nor deleted.
        if (found.some((item) => typeof item?.found !== "boolean")) invalidReply();
        return conflicts.filter((_, i) => found[i]!.found === false);
      };
      const flush = async (batch: { id: string; line: string }[]) => {
        if (batch.length === 0) return;
        // A 409 against a live row means a newer version is stored: not written.
        // A 409 against a delete tombstone is retried until the tombstone is
        // pruned, as postgres re-inserts a deleted row at any version.
        let pending = await deletedOnly(await bulk(batch));
        for (const delay of TOMBSTONE_RETRY_DELAYS_MS) {
          if (pending.length === 0) return;
          await (config.sleep ?? abortableSleep)(delay, context?.signal);
          await request("POST", `${index}/_refresh`, undefined, context);
          pending = await deletedOnly(await bulk(pending));
        }
        if (pending.length > 0) throw new SearchIndexError("unavailable", true, "deleted_key_conflict");
      };
      // Batches bounded by bytes (the exact NDJSON body size: the lines are
      // joined as-is) and by MAX_UPSERT_BATCH actions. A batch is flushed
      // before an entry that would push it past either bound; the first entry
      // of a batch is always taken, so a document larger than the byte bound is
      // sent alone rather than dropped or retried forever.
      let batch: { id: string; line: string }[] = [];
      let batchBytes = 0;
      for (const entry of entries) {
        const bytes = new TextEncoder().encode(entry.line).length;
        if (batch.length > 0 && (batchBytes + bytes > bulkMaxBytes || batch.length >= MAX_UPSERT_BATCH)) {
          await flush(batch);
          batch = [];
          batchBytes = 0;
        }
        batch.push(entry);
        batchBytes += bytes;
      }
      await flush(batch);
      return { written };
    },

    async deleteEvents(tenant, positions, context) {
      assertTenant(tenant);
      if (positions.length > MAX_UPSERT_BATCH) throw new SearchIndexError("invalid_argument");
      if (positions.length === 0) return { deleted: 0 };
      for (const p of positions) assertPosition(p);
      const ids = await Promise.all(positions.map((p) => documentId(tenant, p)));
      const { deleted } = await deleteByQuery([...tenantFilter(tenant.orgId, tenant.workspaceId), { ids: { values: [...new Set(ids)] } }], context);
      return { deleted };
    },

    async eraseScope(scope, context) {
      assertScope(scope);
      // A conflict means a row changed under the delete: run again until none
      // is left, so the scope is empty when this resolves.
      for (let attempt = 0; attempt < ERASE_ATTEMPTS; attempt++) {
        const { conflicts } = await deleteByQuery(scopeFilter(scope), context);
        if (conflicts === 0) return;
      }
      throw new SearchIndexError("unavailable", true, "erase_conflict");
    },

    async scanFingerprints(scope: SearchScope, options, context) {
      assertScope(scope);
      if (!Number.isSafeInteger(options.fromMs) || !Number.isInteger(options.limit) || options.limit < 1) {
        throw new SearchIndexError("invalid_argument");
      }
      const want = options.limit + 1;
      const rows: SearchFingerprint[] = [];
      let after: unknown[] | undefined;
      while (rows.length < want) {
        const size = Math.min(SCAN_PAGE, want - rows.length);
        const page = await search(
          {
            size,
            track_total_hits: false,
            _source: ["workspace_id", ...KEY_FIELDS, "fingerprint"],
            query: { bool: { filter: [...scopeFilter(scope), { range: { ts_ms: { gte: options.fromMs } } }] } },
            sort: sortClause("asc"),
            ...(after ? { search_after: after } : {}),
          },
          context,
        );
        for (const s of page) {
          rows.push({
            tsMs: Number(s.ts_ms),
            sessionId: s.session_id,
            source: s.source,
            machineId: s.machine_id,
            kind: s.kind,
            eventId: s.event_id,
            workspaceId: s.workspace_id,
            fingerprint: s.fingerprint,
          });
        }
        if (page.length < size) break;
        const last = rows[rows.length - 1]!;
        after = [last.tsMs, last.sessionId, last.source, last.machineId, last.kind, last.eventId];
      }
      const truncated = rows.length > options.limit;
      return { truncated, rows: rows.slice(0, options.limit) };
    },

    async query(query: SearchQuery, context): Promise<SearchPage> {
      assertQuery(query);
      const filter: unknown[] = tenantFilter(query.tenant.orgId, query.tenant.workspaceId);
      if (query.sessionId !== undefined) filter.push({ term: { session_id: query.sessionId } });
      if (query.source !== undefined) filter.push({ term: { source: query.source } });
      if (query.kind !== undefined) filter.push({ term: { kind: query.kind } });
      if (query.projectId !== undefined) filter.push({ term: { project_id: query.projectId } });
      if (query.sinceMs !== undefined || query.untilMs !== undefined) {
        filter.push({
          range: { ts_ms: { ...(query.sinceMs !== undefined ? { gte: query.sinceMs } : {}), ...(query.untilMs !== undefined ? { lte: query.untilMs } : {}) } },
        });
      }
      if (query.q !== undefined) {
        // q is one more filter clause (a bool of two `should` wildcards, or a
        // script) nested INSIDE `filter`, so it can only narrow the tenant's rows.
        filter.push(qFilter(query.q.toLowerCase()));
      }
      const c = query.cursor;
      const sources = await search(
        {
          size: query.limit + 1,
          track_total_hits: false,
          _source: HIT_FIELDS,
          query: { bool: { filter } },
          sort: sortClause(query.order),
          ...(c ? { search_after: [c.tsMs, c.sessionId, c.source, c.machineId, c.kind, c.eventId] } : {}),
        },
        context,
      );
      const hits = sources.slice(0, query.limit).map((s) => toHit(s, query.maxContentChars));
      const more = sources.length > query.limit;
      return more ? { hits, next: positionOf(hits[hits.length - 1]!) } : { hits };
    },

    async health(context): Promise<SearchIndexHealth> {
      try {
        await request("HEAD", index, undefined, context);
        return { ok: true, code: "ok" };
      } catch (error) {
        if (error instanceof SearchIndexError && error.code === "schema_missing") return { ok: false, code: "schema_missing" };
        return { ok: false, code: "unreachable" };
      }
    },
  };
}
