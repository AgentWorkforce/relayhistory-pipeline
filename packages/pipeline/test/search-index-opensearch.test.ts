import { describe, expect, it } from "vitest";
import { createSearchIndexRegistry, openSearchConfigFromInput } from "../src/adapters/defaults.js";
import { syntheticDoc } from "../src/search-index/conformance.js";
import { SearchIndexError, matchesQuery } from "../src/search-index/contracts.js";
import {
  OPENSEARCH_MAPPINGS,
  createOpenSearchSearchIndex,
  documentId,
  foldedWindows,
  wildcardLiteral,
  type FetchLike,
} from "../src/search-index/opensearch.js";

// Request-shape tests over a fake fetch. The real-server run of the shared
// conformance suite is search-index-opensearch-real.test.ts.

interface Call {
  method: string;
  url: string;
  headers: Record<string, string>;
  body?: string;
  redirect?: string;
}

type Responder = (call: Call) => Response | Promise<Response>;

function fake(responder: Responder) {
  const calls: Call[] = [];
  const fetch: FetchLike = async (url, init) => {
    const call: Call = {
      method: init.method ?? "GET",
      url,
      headers: init.headers as Record<string, string>,
      body: init.body as string | undefined,
      redirect: init.redirect,
    };
    calls.push(call);
    return responder(call);
  };
  return { calls, fetch };
}

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

const BASE = "http://search.test:9200";
const INDEX = "relayhistory-transcript-events";
const tenant = { orgId: "org-a", workspaceId: "ws-a" };

async function rejects(p: Promise<unknown>, code: string, detail?: string) {
  const error = await p.then(() => null, (e: unknown) => e);
  expect(error).toBeInstanceOf(SearchIndexError);
  expect((error as SearchIndexError).code).toBe(code);
  if (detail !== undefined) expect((error as SearchIndexError).detail).toBe(detail);
  return error as SearchIndexError;
}

describe("opensearch adapter request shapes", () => {
  it("upserts through _bulk with external_gte versions and refresh=wait_for, newest version per key", async () => {
    const { calls, fetch } = fake(() => json(200, { errors: true, items: [{ index: { status: 201 } }, { index: { status: 409, error: { type: "version_conflict_engine_exception" } } }] }));
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    const d1 = syntheticDoc({ orgId: "org-a", eventId: "e1", version: 5, content: "Hello WORLD" });
    const d2 = syntheticDoc({ orgId: "org-a", eventId: "e2", version: 2 });
    const result = await a.upsert([{ ...d1, version: 3 }, d1, d2]);
    expect(result).toEqual({ written: 1 }); // the 409 (a newer stored row) is not written
    expect(calls).toHaveLength(1);
    const call = calls[0]!;
    expect(call.method).toBe("POST");
    expect(call.url).toBe(`${BASE}/_bulk?refresh=wait_for`);
    expect(call.headers["content-type"]).toBe("application/x-ndjson");
    expect(call.redirect).toBe("error");
    const lines = call.body!.trimEnd().split("\n").map((l) => JSON.parse(l));
    expect(lines).toHaveLength(4);
    expect(lines[0]).toEqual({ index: { _index: INDEX, _id: await documentId(d1, d1), version: 5, version_type: "external_gte" } });
    expect(lines[1]).toMatchObject({ org_id: "org-a", workspace_id: "ws-a", event_id: "e1", version: 5, content: "Hello WORLD", content_prefix_folded: ["hello world"] });
    expect(lines[2].index.version).toBe(2);
    // Every stored field is mapped explicitly (dynamic: strict).
    expect(Object.keys(lines[1]).sort()).toEqual(Object.keys(OPENSEARCH_MAPPINGS.properties).sort());
  });

  it("raises bounded errors for failed bulk items, retryable for 429/5xx", async () => {
    const items = (status: number) => () => json(200, { errors: true, items: [{ index: { status, error: { type: "x", reason: "secret content" } } }] });
    await rejects(createOpenSearchSearchIndex({ url: BASE, fetch: fake(items(400)).fetch }).upsert([syntheticDoc({ orgId: "o" })]), "unavailable", "bulk_item_400");
    const e = await rejects(createOpenSearchSearchIndex({ url: BASE, fetch: fake(items(429)).fetch }).upsert([syntheticDoc({ orgId: "o" })]), "unavailable", "bulk_item_429");
    expect(e.retryable).toBe(true);
    expect(e.message).not.toContain("secret");
  });

  it("deletes exact rows with _delete_by_query, refresh=true, conflicts=proceed and tenant filters", async () => {
    const { calls, fetch } = fake(() => json(200, { deleted: 1, version_conflicts: 0, failures: [] }));
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    const doc = syntheticDoc({ orgId: "org-a" });
    expect(await a.deleteEvents(tenant, [doc, doc])).toEqual({ deleted: 1 });
    const call = calls[0]!;
    expect(call.url).toBe(`${BASE}/${INDEX}/_delete_by_query?refresh=true&conflicts=proceed`);
    const body = JSON.parse(call.body!);
    expect(Object.keys(body.query.bool)).toEqual(["filter"]);
    expect(body.query.bool.filter).toEqual([
      { term: { org_id: "org-a" } },
      { term: { workspace_id: "ws-a" } },
      { ids: { values: [await documentId(tenant, doc)] } },
    ]);
  });

  it("erases a scope by tenant + source + session ids, repeating on version conflicts and failing when incomplete", async () => {
    let n = 0;
    const { calls, fetch } = fake(() => json(200, { deleted: 2, version_conflicts: n++ < 1 ? 1 : 0, failures: [] }));
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    await a.eraseScope({ ...tenant, source: "claude", sessionIds: ["s1", "s2"] });
    expect(calls).toHaveLength(2);
    const body = JSON.parse(calls[0]!.body!);
    expect(calls[0]!.url).toBe(`${BASE}/${INDEX}/_delete_by_query?refresh=true&conflicts=proceed`);
    expect(body).toEqual({
      query: {
        bool: {
          filter: [
            { term: { org_id: "org-a" } },
            { term: { workspace_id: "ws-a" } },
            { term: { source: "claude" } },
            { terms: { session_id: ["s1", "s2"] } },
          ],
        },
      },
    });
    const stuck = createOpenSearchSearchIndex({ url: BASE, fetch: fake(() => json(200, { deleted: 0, version_conflicts: 1, failures: [] })).fetch });
    await rejects(stuck.eraseScope({ ...tenant, source: "claude", sessionIds: ["s1"] }), "unavailable", "erase_conflict");
    const failed = createOpenSearchSearchIndex({ url: BASE, fetch: fake(() => json(200, { deleted: 0, version_conflicts: 0, failures: [{ cause: "x" }] })).fetch });
    await rejects(failed.eraseScope({ ...tenant, source: "claude", sessionIds: ["s1"] }), "unavailable", "delete_incomplete");
  });

  it("queries with tenant filters in filter clauses only, q nested in filter, exact sort and search_after", async () => {
    const source = (eventId: string) => ({
      org_id: "org-a", workspace_id: "ws-a", ts_ms: 10, session_id: "s", source: "claude", machine_id: "", kind: "message", event_id: eventId,
      type: "t", user_id: "u", actor_name: "n", actor_role: "r", subagent_id: "", project_id: "p", task_title: "", task_ref: "{}",
      tags: [], files_touched: [], tool_name: "", tool_status: "", content: "héllo wörld",
    });
    const { calls, fetch } = fake(() => json(200, { timed_out: false, _shards: { failed: 0 }, hits: { hits: [{ _source: source("a") }, { _source: source("b") }] } }));
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    const cursor = { tsMs: 5, sessionId: "s", source: "claude", machineId: "", kind: "message", eventId: "z" };
    const page = await a.query({ tenant, q: "Wö*?\\", sessionId: "s", sinceMs: 1, untilMs: 99, order: "desc", limit: 1, cursor, maxContentChars: 4 });
    expect(page.hits).toHaveLength(1);
    expect(page.hits[0]!.content).toBe("héll");
    expect(page.next).toEqual({ tsMs: 10, sessionId: "s", source: "claude", machineId: "", kind: "message", eventId: "a" });
    const body = JSON.parse(calls[0]!.body!);
    expect(calls[0]!.url).toBe(`${BASE}/${INDEX}/_search`);
    expect(body.size).toBe(2);
    expect(Object.keys(body.query.bool)).toEqual(["filter"]);
    expect(body.query.bool.filter.slice(0, 2)).toEqual([{ term: { org_id: "org-a" } }, { term: { workspace_id: "ws-a" } }]);
    expect(body.query.bool.filter).toContainEqual({ range: { ts_ms: { gte: 1, lte: 99 } } });
    expect(body.query.bool.filter.at(-1)).toEqual({
      bool: {
        should: [
          { wildcard: { content_prefix_folded: { value: "*wö\\*\\?\\\\*" } } },
          { wildcard: { task_title_folded: { value: "*wö\\*\\?\\\\*" } } },
        ],
        minimum_should_match: 1,
      },
    });
    expect(body.sort).toEqual(["ts_ms", "session_id", "source", "machine_id", "kind", "event_id"].map((f) => ({ [f]: { order: "desc" } })));
    expect(body.search_after).toEqual([5, "s", "claude", "", "message", "z"]);
    expect(body._source).not.toContain("fingerprint");
  });

  it("refuses partial search results instead of paging with gaps", async () => {
    const a = createOpenSearchSearchIndex({ url: BASE, fetch: fake(() => json(200, { timed_out: false, _shards: { failed: 1 }, hits: { hits: [] } })).fetch });
    await rejects(a.query({ tenant, order: "asc", limit: 5 }), "unavailable", "partial_results");
  });

  it("scans fingerprints with search_after pages and flags truncation", async () => {
    const row = (i: number) => ({ _source: { workspace_id: "ws-a", ts_ms: i, session_id: "s", source: "claude", machine_id: "m", kind: "k", event_id: `e${i}`, fingerprint: `f${i}` } });
    const { calls, fetch } = fake((call) => {
      const body = JSON.parse(call.body!);
      const start = body.search_after ? body.search_after[0] + 1 : 0;
      return json(200, { hits: { hits: Array.from({ length: Math.min(body.size, 12_000 - start) }, (_, i) => row(start + i)) } });
    });
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    const scan = await a.scanFingerprints({ ...tenant, source: "claude", sessionIds: ["s"] }, { fromMs: 0, limit: 11_000 });
    expect(scan.truncated).toBe(true);
    expect(scan.rows).toHaveLength(11_000);
    expect(scan.rows[10_999]!.eventId).toBe("e10999");
    expect(calls.map((c) => JSON.parse(c.body!).size)).toEqual([5000, 5000, 1001]);
    expect(JSON.parse(calls[0]!.body!).query.bool.filter).toContainEqual({ range: { ts_ms: { gte: 0 } } });
  });

  it("rejects invalid tenants, scopes and queries before any request", async () => {
    const { calls, fetch } = fake(() => json(200, {}));
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    await rejects(a.query({ tenant: { orgId: "", workspaceId: "w" }, order: "asc", limit: 1 }), "invalid_argument");
    await rejects(a.deleteEvents({ orgId: "o", workspaceId: "" }, [syntheticDoc({ orgId: "o" })]), "invalid_argument");
    await rejects(a.upsert([syntheticDoc({ orgId: "" })]), "invalid_argument");
    await rejects(a.eraseScope({ ...tenant, source: "claude", sessionIds: [] }), "invalid_argument");
    await rejects(a.scanFingerprints({ orgId: "o", workspaceId: "", source: "c", sessionIds: ["s"] }, { fromMs: 0, limit: 1 }), "invalid_argument");
    expect(calls).toHaveLength(0);
  });
});

describe("opensearch adapter transport errors", () => {
  const doc = syntheticDoc({ orgId: "org-a" });

  it("maps statuses to bounded codes without backend text", async () => {
    const cases: [number, unknown, string, boolean, string | undefined][] = [
      [404, { error: { type: "index_not_found_exception", reason: "no such index [secret]" } }, "schema_missing", false, undefined],
      [401, { error: "secret body" }, "unavailable", false, "http_401"],
      [403, {}, "unavailable", false, "http_403"],
      [429, {}, "unavailable", true, "http_429"],
      [503, { error: { reason: "content quoted here" } }, "unavailable", true, "http_503"],
    ];
    for (const [status, body, code, retryable, detail] of cases) {
      const a = createOpenSearchSearchIndex({ url: BASE, fetch: fake(() => json(status, body)).fetch });
      const e = await rejects(a.query({ tenant, order: "asc", limit: 1 }), code, detail);
      expect(e.retryable).toBe(retryable);
      expect(e.message).not.toMatch(/secret|quoted/);
    }
  });

  it("maps network failure, timeout and caller abort", async () => {
    const down = createOpenSearchSearchIndex({ url: BASE, fetch: async () => { throw new TypeError("connect ECONNREFUSED 10.0.0.1"); } });
    const e = await rejects(down.upsert([doc]), "unavailable", "network");
    expect(e.retryable).toBe(true);
    expect(e.message).not.toContain("10.0.0.1");
    const hang: FetchLike = (_url, init) =>
      new Promise((_resolve, reject) => init.signal!.addEventListener("abort", () => reject(init.signal!.reason)));
    await rejects(createOpenSearchSearchIndex({ url: BASE, fetch: hang, requestTimeoutMs: 20 }).query({ tenant, order: "asc", limit: 1 }), "unavailable", "timeout");
    const controller = new AbortController();
    const pending = createOpenSearchSearchIndex({ url: BASE, fetch: hang }).query({ tenant, order: "asc", limit: 1 }, { signal: controller.signal });
    controller.abort();
    await rejects(pending, "aborted");
    expect(await createOpenSearchSearchIndex({ url: BASE, fetch: hang }).health({ signal: controller.signal })).toEqual({ ok: false, code: "unreachable" });
  });

  it("reports health in the postgres shape", async () => {
    const health = (responder: Responder) => createOpenSearchSearchIndex({ url: BASE, fetch: fake(responder).fetch }).health();
    expect(await health(() => new Response(null, { status: 200 }))).toEqual({ ok: true, code: "ok" });
    expect(await health(() => new Response(null, { status: 404 }))).toEqual({ ok: false, code: "schema_missing" });
    expect(await health(() => new Response(null, { status: 503 }))).toEqual({ ok: false, code: "unreachable" });
    expect(await health(() => { throw new TypeError("down"); })).toEqual({ ok: false, code: "unreachable" });
    const { calls, fetch } = fake(() => new Response(null, { status: 200 }));
    await createOpenSearchSearchIndex({ url: `${BASE}/prefix/`, fetch }).health();
    expect(calls[0]).toMatchObject({ method: "HEAD", url: `${BASE}/prefix/${INDEX}` });
  });
});

describe("opensearch adapter schema and dialects", () => {
  const root = (product: "opensearch" | "elasticsearch") =>
    product === "opensearch"
      ? json(200, { version: { distribution: "opensearch", number: "2.19.0" } })
      : json(200, { version: { number: "8.15.3" }, tagline: "You Know, for Search" }, { "x-elastic-product": "Elasticsearch" });

  for (const dialect of ["opensearch", "elasticsearch"] as const) {
    it(`creates the index with explicit mappings after the ${dialect} product check`, async () => {
      const { calls, fetch } = fake((call) => (call.method === "GET" ? root(dialect) : json(200, { acknowledged: true })));
      await createOpenSearchSearchIndex({ url: BASE, dialect, fetch }).ensureSchema();
      expect(calls.map((c) => `${c.method} ${c.url}`)).toEqual([`GET ${BASE}/`, `PUT ${BASE}/${INDEX}`]);
      const body = JSON.parse(calls[1]!.body!);
      expect(body.mappings).toEqual(OPENSEARCH_MAPPINGS);
      expect(body.mappings.dynamic).toBe("strict");
      expect(body.settings).toEqual({ index: { gc_deletes: "0s" } });
      const other = dialect === "opensearch" ? "elasticsearch" : "opensearch";
      const wrong = fake((call) => (call.method === "GET" ? root(other) : json(200, {})));
      await rejects(createOpenSearchSearchIndex({ url: BASE, dialect, fetch: wrong.fetch }).ensureSchema(), "unsupported", "dialect_mismatch");
      expect(wrong.calls).toHaveLength(1);
    });
  }

  it("verifies the mapping of an existing index and reports conflicts", async () => {
    const exists = json(400, { error: { type: "resource_already_exists_exception" } });
    let mappingStatus = 200;
    const { calls, fetch } = fake((call) =>
      call.method === "GET" ? root("opensearch") : call.url.endsWith("/_mapping") ? json(mappingStatus, {}) : exists.clone(),
    );
    const a = createOpenSearchSearchIndex({ url: BASE, fetch });
    await a.ensureSchema();
    expect(calls.map((c) => `${c.method} ${c.url}`).at(-1)).toBe(`PUT ${BASE}/${INDEX}/_mapping`);
    mappingStatus = 400;
    await rejects(a.ensureSchema(), "schema_missing", "mapping_conflict");
  });

  it("sends basic auth only to the configured URL and refuses URLs with credentials or queries", async () => {
    const { calls, fetch } = fake(() => new Response(null, { status: 200 }));
    await createOpenSearchSearchIndex({ url: BASE, fetch, auth: { kind: "basic", username: "reader", password: "pässword" } }).health();
    expect(calls[0]!.headers.authorization).toBe(`Basic ${Buffer.from("reader:pässword").toString("base64")}`);
    for (const url of ["http://u:p@search.test", "ftp://search.test", "http://search.test/?a=1", "http://search.test/#x", "not a url"]) {
      expect(() => createOpenSearchSearchIndex({ url, fetch })).toThrow(SearchIndexError);
    }
    expect(() => createOpenSearchSearchIndex({ url: BASE, fetch, auth: { kind: "basic", username: "a:b", password: "p" } })).toThrow(SearchIndexError);
    expect(() => createOpenSearchSearchIndex({ url: BASE, fetch, indexPrefix: "Bad/Prefix" })).toThrow(SearchIndexError);
  });

  it("refuses SigV4 at construction (not implemented)", () => {
    let error: unknown;
    try {
      createOpenSearchSearchIndex({ url: BASE, auth: { kind: "sigv4" } });
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(SearchIndexError);
    expect((error as SearchIndexError).code).toBe("unsupported");
    expect((error as SearchIndexError).detail).toBe("sigv4_not_implemented");
  });

  it("refuses a CA bundle on plain http or where fetch cannot take one", () => {
    const pem = "-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n";
    expect(() => createOpenSearchSearchIndex({ url: BASE, ca: pem })).toThrow(/ca_needs_https/);
    if (typeof (globalThis as { Bun?: unknown }).Bun === "undefined") {
      expect(() => createOpenSearchSearchIndex({ url: "https://search.test", ca: pem })).toThrow(/ca_needs_bun/);
    }
  });
});

describe("opensearch env parsing", () => {
  it("fails fast without a URL and parses every SEARCH_INDEX_OPENSEARCH_ setting", () => {
    expect(() => openSearchConfigFromInput({ env: {} })).toThrow(/needs SEARCH_INDEX_OPENSEARCH_URL/);
    expect(() => createSearchIndexRegistry().create({ SEARCH_INDEX_BACKEND: "opensearch" }, { env: { SEARCH_INDEX_OPENSEARCH_URL: " " } })).toThrow(
      /needs SEARCH_INDEX_OPENSEARCH_URL/,
    );
    const config = openSearchConfigFromInput({
      env: {
        SEARCH_INDEX_OPENSEARCH_URL: BASE,
        SEARCH_INDEX_OPENSEARCH_DIALECT: "elasticsearch",
        SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX: "custom",
        SEARCH_INDEX_OPENSEARCH_AUTH: "basic",
        SEARCH_INDEX_OPENSEARCH_USERNAME: "reader",
        SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS: "2500",
        SEARCH_INDEX_OPENSEARCH_PASSWORD: "ignored-from-env",
      },
      secrets: { SEARCH_INDEX_OPENSEARCH_PASSWORD: "from-secret" },
    });
    expect(config).toEqual({ url: BASE, dialect: "elasticsearch", indexPrefix: "custom", requestTimeoutMs: 2500, auth: { kind: "basic", username: "reader", password: "from-secret" } });
    expect(openSearchConfigFromInput({ env: { SEARCH_INDEX_OPENSEARCH_URL: BASE } })).toEqual({ url: BASE, dialect: "opensearch", auth: { kind: "none" } });
    // The password is a secret: env alone never supplies it.
    expect(() =>
      openSearchConfigFromInput({ env: { SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_AUTH: "basic", SEARCH_INDEX_OPENSEARCH_USERNAME: "u", SEARCH_INDEX_OPENSEARCH_PASSWORD: "p" } }),
    ).toThrow(/SEARCH_INDEX_OPENSEARCH_PASSWORD secret/);
    expect(() => openSearchConfigFromInput({ env: { SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_DIALECT: "solr" } })).toThrow(/DIALECT/);
    expect(() => openSearchConfigFromInput({ env: { SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_AUTH: "token" } })).toThrow(/AUTH/);
    expect(() => openSearchConfigFromInput({ env: { SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS: "-1" } })).toThrow(/TIMEOUT/);
    expect(() =>
      createSearchIndexRegistry().create({ SEARCH_INDEX_BACKEND: "opensearch" }, { env: { SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_AUTH: "sigv4" } }),
    ).toThrow(/sigv4_not_implemented/);
  });
});

describe("opensearch q folding", () => {
  it("windows the folded text so every folded q fits wholly in one window", () => {
    expect(foldedWindows("")).toEqual([]);
    expect(foldedWindows("abc")).toEqual(["abc"]);
    const long = "a".repeat(5000) + "NEEDLE".toLowerCase() + "b".repeat(5000);
    const windows = foldedWindows(long);
    expect(windows.every((w) => [...w].length <= 4096)).toBe(true);
    expect(windows.some((w) => w.includes("needle"))).toBe(true);
    // A needle of the maximum folded length straddling every window boundary is still found.
    const text = [...Array(10_000)].map((_, i) => String.fromCharCode(97 + (i % 26))).join("");
    for (const start of [0, 2047, 2048, 4095, 6000, 9000]) {
      const needle = text.slice(start, start + 1000);
      expect(foldedWindows(text).some((w) => w.includes(needle)), `start ${start}`).toBe(
        matchesQuery({ content: "", taskTitle: text }, needle),
      );
    }
    expect(wildcardLiteral("a*b?c\\d%_")).toBe("*a\\*b\\?c\\\\d%_*");
  });
});
