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
 * escaped. Any folded q (at most 2 * MAX_QUERY_CHARS code points) fits wholly
 * inside one window, so a match equals `matchesQuery`.
 *
 * Errors carry bounded codes only (`http_<status>`, `timeout`, `network`),
 * never response bodies, which can quote document content.
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

export type OpenSearchDialect = "opensearch" | "elasticsearch";
export const OPENSEARCH_DIALECTS: readonly OpenSearchDialect[] = ["opensearch", "elasticsearch"];
export type OpenSearchAuth =
  | { readonly kind: "none" }
  | { readonly kind: "basic"; readonly username: string; readonly password: string }
  | { readonly kind: "sigv4" };

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
  /** Injected for tests; defaults to the global fetch. */
  readonly fetch?: FetchLike;
}

export const DEFAULT_OPENSEARCH_INDEX_PREFIX = "relayhistory";
export const DEFAULT_OPENSEARCH_TIMEOUT_MS = 10_000;
const PREFIX = /^[a-z0-9][a-z0-9_-]{0,100}$/;
/** Folded-text window (code points) and stride: any needle up to WINDOW - STRIDE + 1 fits in one window. */
const WINDOW = 4096;
const STRIDE = 2048;
/** Rows per search_after page of a fingerprint scan (under index.max_result_window). */
const SCAN_PAGE = 5000;
/** Bulk request body bound (bytes); well under the default http.max_content_length of 100mb. */
const BULK_BYTES = 8 * 1024 * 1024;
const ERASE_ATTEMPTS = 5;

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
    content_prefix_folded: { type: "keyword", doc_values: false },
    task_title_folded: { type: "keyword", doc_values: false },
    fingerprint: stored,
    version: { type: "long", index: false },
  },
});

/** Index settings. gc_deletes 0: a deleted key can be re-written at any version, as a deleted postgres row can. */
export const OPENSEARCH_SETTINGS = Object.freeze({ index: { gc_deletes: "0s" } });

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

export function createOpenSearchSearchIndex(config: OpenSearchSearchIndexConfig): SearchIndexAdapter {
  const base = normalizeBaseUrl(config.url);
  const dialect = config.dialect ?? "opensearch";
  if (!OPENSEARCH_DIALECTS.includes(dialect)) throw new SearchIndexError("invalid_argument", false, "dialect");
  const prefix = config.indexPrefix ?? DEFAULT_OPENSEARCH_INDEX_PREFIX;
  if (!PREFIX.test(prefix)) throw new SearchIndexError("invalid_argument", false, "index_prefix");
  const index = `${prefix}-transcript-events`;
  const timeoutMs = config.requestTimeoutMs ?? DEFAULT_OPENSEARCH_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 600_000) throw new SearchIndexError("invalid_argument", false, "timeout");
  const auth = config.auth ?? { kind: "none" };
  let authorization: string | undefined;
  if (auth.kind === "basic") {
    if (!auth.username || auth.username.includes(":") || typeof auth.password !== "string" || !auth.password) {
      throw new SearchIndexError("invalid_argument", false, "basic_auth");
    }
    const bytes = new TextEncoder().encode(`${auth.username}:${auth.password}`);
    authorization = `Basic ${btoa(String.fromCharCode(...bytes))}`;
  } else if (auth.kind === "sigv4") {
    // TODO(sigv4): sign requests with AWS SigV4 (service "es"/"aoss") for
    // Amazon OpenSearch Service. Not implemented: refuse at construction so a
    // misconfigured install never sends unsigned requests.
    throw new SearchIndexError("unsupported", false, "sigv4_not_implemented");
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
    let response: Response;
    let text: string;
    try {
      response = await doFetch(`${base}/${path}`, {
        method,
        headers,
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
    throw new SearchIndexError("unavailable", retryableStatus(status), `http_${status}`);
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
    // A leading-wildcard query walks the folded terms for every q length.
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
      const lines: string[] = [];
      for (const doc of newest.values()) {
        const action = { index: { _index: index, _id: await documentId(doc, doc), version: doc.version, version_type: "external_gte" } };
        lines.push(`${JSON.stringify(action)}\n${JSON.stringify(toSource(doc))}\n`);
      }
      let written = 0;
      let batch = "";
      let batchBytes = 0;
      const flush = async () => {
        if (!batch) return;
        const reply = await request("POST", "_bulk?refresh=wait_for", undefined, context, { ndjson: batch });
        batch = "";
        batchBytes = 0;
        const items = (reply.body as { items?: { index?: { status?: unknown; error?: { type?: unknown } } }[] } | undefined)?.items;
        if (!Array.isArray(items)) invalidReply();
        let failure: number | undefined;
        for (const item of items) {
          const status = item?.index?.status;
          if (typeof status !== "number") invalidReply();
          if (status >= 200 && status < 300) written += 1;
          else if (status === 409 && item.index!.error?.type === "version_conflict_engine_exception") continue; // a newer row is stored
          else failure = failure === undefined || retryableStatus(status) ? status : failure;
        }
        if (failure !== undefined) throw new SearchIndexError("unavailable", retryableStatus(failure), `bulk_item_${failure}`);
      };
      for (const line of lines) {
        const bytes = new TextEncoder().encode(line).length;
        if (batch && batchBytes + bytes > BULK_BYTES) await flush();
        batch += line;
        batchBytes += bytes;
      }
      await flush();
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
        const value = wildcardLiteral(query.q.toLowerCase());
        // q is one more filter clause: a bool with two `should` alternatives
        // nested INSIDE `filter`, so it can only narrow the tenant's rows.
        filter.push({
          bool: {
            should: [{ wildcard: { content_prefix_folded: { value } } }, { wildcard: { task_title_folded: { value } } }],
            minimum_should_match: 1,
          },
        });
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
