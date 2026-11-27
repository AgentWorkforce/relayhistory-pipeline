/**
 * The operator lines the self-host search pieces print before a stack trace:
 *   - the search-indexer-bootstrap Job (consumer-bootstrap.ts) names an OpenSearch
 *     failure and says whether the search Postgres grants were applied;
 *   - the search indexer Deployment (consumer-service.ts) names the unfinished
 *     bootstrap when it starts without its sessions privileges (42501).
 * The 42501 cases run the real startup check against real PGlite catalogs, and the
 * network case runs the real OpenSearch adapter against a closed port.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { SearchIndexError } from "@rhc/search-index/contracts.ts";
import { createOpenSearchSearchIndex } from "@rhc/search-index/opensearch.ts";
import {
  openSearchHost,
  searchBootstrapFailureLine,
  withSearchBootstrapDiagnosis,
} from "../consumer-bootstrap.ts";
import { assertServiceSchema, searchIndexerStartupHint } from "../consumer-service.ts";

const URL_ENV = { SEARCH_INDEX_OPENSEARCH_URL: "https://search-relay.us-east-1.es.amazonaws.com" };
const NOT_APPLIED =
  "this run's search Postgres changes (state tables and grants for search_consumer_runtime) were rolled back with it, so this run applied none of them";
const NOT_BEGUN = "this run applied no Postgres changes (it failed before its Postgres transaction began)";
const INDEX = "relayhistory-transcript-events";
const HOST = "search-relay.us-east-1.es.amazonaws.com";

/**
 * A fake OpenSearch that answers ensureSchema's real request sequence: GET / (product),
 * PUT <index>, PUT <index>/_mapping when the index exists, GET <index>/_settings.
 */
function fakeOpenSearch(answers: { create?: Response; mapping?: Response; settings?: Response }) {
  const json = (status: number, body: unknown) =>
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
  return createOpenSearchSearchIndex({
    url: URL_ENV.SEARCH_INDEX_OPENSEARCH_URL,
    fetch: async (input, init) => {
      const path = new URL(input).pathname;
      if (path === "/") return json(200, { version: { distribution: "opensearch", number: "2.17.0" } });
      if (init.method === "PUT" && path === `/${INDEX}`) return answers.create ?? json(200, { acknowledged: true });
      if (init.method === "PUT" && path === `/${INDEX}/_mapping`) return answers.mapping ?? json(200, { acknowledged: true });
      if (path.startsWith(`/${INDEX}/_settings`))
        return answers.settings ?? json(200, { [INDEX]: { settings: { "index.gc_deletes": "0s" } } });
      throw new Error(`unexpected ${init.method} ${path}`);
    },
  });
}
const reply = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
/** Phrases that belong to exactly one schema case each. */
const SCHEMA_PHRASES = {
  absent: "was not found (schema_missing)",
  conflict: "mapping conflicts with this release's mapping",
  gc: "index.gc_deletes other than 0",
};

async function caught(work: () => Promise<unknown>): Promise<unknown> {
  try {
    await work();
  } catch (error) {
    return error;
  }
  throw new Error("expected a throw");
}

describe("search bootstrap: one plain line for an OpenSearch failure", () => {
  test("the real adapter, OpenSearch unreachable (network): the exact line, grants NOT applied", async () => {
    // Port 1 on loopback: nothing listens, so fetch fails with a connection error.
    const env = { SEARCH_INDEX_OPENSEARCH_URL: "http://127.0.0.1:1" };
    const index = createOpenSearchSearchIndex({ url: env.SEARCH_INDEX_OPENSEARCH_URL, requestTimeoutMs: 5_000 });
    const error = await caught(() => index.ensureSchema());
    expect(error).toBeInstanceOf(SearchIndexError);
    expect((error as Error).message).toBe("search index: unavailable (network)");
    expect(searchBootstrapFailureLine(error, env, "rolled_back")).toBe(
      `search bootstrap failed: OpenSearch 127.0.0.1:1 unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); ${NOT_APPLIED}`,
    );
  });

  test("after the Postgres half committed, the line says it succeeded", () => {
    const line = searchBootstrapFailureLine(new SearchIndexError("unavailable", true, "network"), URL_ENV, "committed");
    expect(line).toBe(
      "search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); this run's search Postgres changes (state tables and grants for search_consumer_runtime) were committed before this failure",
    );
  });

  test("a 403 from the real adapter names the role mapping and Pod Identity", async () => {
    const index = createOpenSearchSearchIndex({
      url: URL_ENV.SEARCH_INDEX_OPENSEARCH_URL,
      fetch: async () =>
        new Response(JSON.stringify({ error: { type: "security_exception", reason: "no permissions for [indices:admin/get]" } }), {
          status: 403,
          headers: { "content-type": "application/json" },
        }),
    });
    const error = await caught(() => index.ensureSchema());
    expect(searchBootstrapFailureLine(error, URL_ENV, "rolled_back")).toBe(
      `search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com refused the request (http_403): check the OpenSearch role mapping for the search indexer's Pod Identity role, and the Pod Identity association; ${NOT_APPLIED}`,
    );
    // The backend's reason text never reaches the line.
    expect(searchBootstrapFailureLine(error, URL_ENV, "rolled_back")).not.toContain("no permissions");
  });

  test("every SearchIndexError kind gets its own pointer", () => {
    const cause = (code: ConstructorParameters<typeof SearchIndexError>[0], detail?: string) =>
      searchBootstrapFailureLine(new SearchIndexError(code, false, detail), URL_ENV, "rolled_back")!
        .replace("search bootstrap failed: ", "")
        .replace(`; ${NOT_APPLIED}`, "");
    const host = "search-relay.us-east-1.es.amazonaws.com";
    expect(cause("unavailable", "timeout")).toBe(
      `OpenSearch ${host} did not answer in time (timeout): check search.opensearch.url, the VPC/security group and the domain's health`,
    );
    expect(cause("unavailable", "credentials")).toBe(
      `OpenSearch ${host}: no AWS credentials to sign requests (credentials): check the search indexer's Pod Identity association`,
    );
    expect(cause("unavailable", "http_401")).toBe(
      `OpenSearch ${host} rejected the credentials (http_401): check search.opensearch.auth, the OpenSearch role mapping and Pod Identity`,
    );
    expect(cause("unavailable", "http_503")).toBe(
      `OpenSearch ${host} answered HTTP 503 (http_503): check search.opensearch.url and the domain's health`,
    );
    expect(cause("unavailable", "bad_response")).toBe(
      `OpenSearch ${host} sent a malformed or unexpected reply (bad_response; for example index settings or a mapping the adapter could not read): check search.opensearch.url, the domain's health and the index named here`,
    );
    expect(cause("unsupported", "dialect_mismatch")).toBe(
      `OpenSearch ${host} is not the configured product (dialect_mismatch): check search.opensearch.dialect`,
    );
    expect(cause("invalid_argument", "sigv4_needs_https")).toBe(
      "the OpenSearch settings are invalid (invalid_argument: sigv4_needs_https): check search.opensearch.* in your values file",
    );
    expect(cause("schema_mismatch", "some_future_detail")).toBe(
      `the OpenSearch index ${INDEX} on ${host} failed its schema check (schema_mismatch: some_future_detail): see docs/search-and-runtime-adapters.md (OpenSearch implementation) or contact support with this line`,
    );
    expect(cause("aborted")).toBe(`OpenSearch ${host} failed (aborted)`);
  });

  test("a real index_not_found_exception (schema_missing, no detail): absent, created by the bootstrap; never 'exists', never removal", async () => {
    const index = fakeOpenSearch({
      settings: reply(404, { error: { type: "index_not_found_exception", reason: "no such index [relayhistory-transcript-events]" } }),
    });
    const error = (await caught(() => index.ensureSchema())) as SearchIndexError;
    expect([error.code, error.detail]).toEqual(["schema_missing", undefined]);
    const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
    expect(line).toBe(
      `search bootstrap failed: the OpenSearch index ${INDEX} on ${HOST} was not found (schema_missing): the bootstrap creates it, so it is absent or was removed while this run checked it; rerun the Job, and if this repeats, check search.opensearch.indexPrefix and that the OpenSearch role mapping allows creating indices; ${NOT_APPLIED}`,
    );
    expect(line).not.toContain(SCHEMA_PHRASES.conflict);
    expect(line).not.toContain(SCHEMA_PHRASES.gc);
    expect(line).not.toMatch(/exists|remove the|delete|recreate|stale/i);
  });

  test("a 400 on PUT _mapping with no conflict evidence: rejected, never 'conflict', no upstream strings", async () => {
    for (const body of [
      {},
      { error: { type: "mapper_parsing_exception", reason: "No handler for type [flattened]" } },
      { error: { type: "synthetic_private_marker", reason: "synthetic_private_reason" } },
      { error: { type: "illegal_argument_exception", reason: "x", caused_by: { type: "synthetic_nested_marker", reason: "synthetic_nested_reason" } } },
    ]) {
      // A fresh fake per call: a Response body can be read once.
      const fresh = () =>
        fakeOpenSearch({
          create: reply(400, { error: { type: "resource_already_exists_exception" } }),
          mapping: reply(400, body),
        });
      const error = (await caught(() => fresh().ensureSchema())) as SearchIndexError;
      expect(error.code).toBe("schema_missing");
      expect(error.detail).toBe("mapping_rejected:http_400");
      // What serve() actually writes to stderr, through the real wrapper.
      const written: string[] = [];
      const index = fresh();
      await caught(() =>
        withSearchBootstrapDiagnosis(URL_ENV, (openSearch) => openSearch(() => index.ensureSchema()), (l) => written.push(l)),
      );
      expect(written).toHaveLength(1);
      const line = written[0]!;
      expect(line).toContain("already exists and rejected this release's mapping update (HTTP 400; schema_missing: mapping_rejected:http_400)");
      expect(line).not.toMatch(/conflict/i);
      expect(line).not.toMatch(/remove the|delete|recreate/i);
      for (const text of [line, error.detail ?? "", error.message])
        expect(text).not.toMatch(/synthetic_|mapper_parsing|No handler/);
    }
  });

  test("gc_deletes: zero passes, a confirmed nonzero string is 'other than 0', anything else could not be verified", async () => {
    const settings = (value: unknown) =>
      fakeOpenSearch({ settings: reply(200, { [INDEX]: { settings: value === undefined ? {} : { "index.gc_deletes": value } } }) });
    for (const zero of [0, "0", "0s"]) await settings(zero).ensureSchema();
    for (const value of [undefined, { value: "60s" }, "garbage", 60]) {
      const error = (await caught(() => settings(value).ensureSchema())) as SearchIndexError;
      expect([error.code, error.detail], JSON.stringify(value)).toEqual(["schema_mismatch", "gc_deletes_unverified"]);
      const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
      expect(line).toContain("index.gc_deletes could not be verified as 0s (absent from the settings response or in an unsupported form");
      expect(line).not.toContain(SCHEMA_PHRASES.gc);
    }
    const nonzero = (await caught(() => settings("60s").ensureSchema())) as SearchIndexError;
    expect([nonzero.code, nonzero.detail]).toEqual(["schema_mismatch", "gc_deletes"]);
    expect(searchBootstrapFailureLine(nonzero, URL_ENV, "rolled_back")).toContain(SCHEMA_PHRASES.gc);
  });

  test("an HTTP failure line shows the status only, never upstream type names", async () => {
    const index = fakeOpenSearch({
      create: reply(403, { error: { type: "synthetic_private_marker", caused_by: { type: "synthetic_nested_marker" } } }),
    });
    const error = (await caught(() => index.ensureSchema())) as SearchIndexError;
    expect(error.code).toBe("unavailable");
    const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
    expect(line).toContain("refused the request (http_403)");
    expect(line).not.toContain("synthetic_");
  });

  test("bad_response after a valid product check (malformed settings): malformed or unexpected, never 'not OpenSearch'", async () => {
    const index = fakeOpenSearch({ settings: reply(200, {}) });
    const error = (await caught(() => index.ensureSchema())) as SearchIndexError;
    expect([error.code, error.detail]).toEqual(["unavailable", "bad_response"]);
    const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
    expect(line).toContain("sent a malformed or unexpected reply (bad_response");
    expect(line).not.toMatch(/not OpenSearch|is not the configured product/);
  });

  test("a real mapping conflict (schema_missing: mapping_conflict): names the index, no field invented, no deletion advice", async () => {
    const index = fakeOpenSearch({
      create: reply(400, { error: { type: "resource_already_exists_exception" } }),
      mapping: reply(400, { error: { type: "illegal_argument_exception", reason: "mapper [content_prefix_folded] cannot be changed" } }),
    });
    const error = (await caught(() => index.ensureSchema())) as SearchIndexError;
    expect([error.code, error.detail]).toEqual(["schema_missing", "mapping_conflict"]);
    const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
    expect(line).toBe(
      `search bootstrap failed: the OpenSearch index ${INDEX} on ${HOST} already exists and its mapping conflicts with this release's mapping (schema_missing: mapping_conflict; the conflicting field is not reported): see docs/search-and-runtime-adapters.md (OpenSearch implementation) or contact support with this line; ${NOT_APPLIED}`,
    );
    expect(line).not.toContain(SCHEMA_PHRASES.absent);
    expect(line).not.toContain(SCHEMA_PHRASES.gc);
    expect(line).not.toContain("content_prefix_folded"); // backend reason text never reaches the line
    expect(line).not.toMatch(/remove|delete|recreate|drop|stale/i);
  });

  test("a real gc_deletes mismatch (schema_mismatch: gc_deletes): its own line, the documented dynamic setting", async () => {
    const index = fakeOpenSearch({ settings: reply(200, { [INDEX]: { settings: { "index.gc_deletes": "60s" } } }) });
    const error = (await caught(() => index.ensureSchema())) as SearchIndexError;
    expect([error.code, error.detail]).toEqual(["schema_mismatch", "gc_deletes"]);
    const line = searchBootstrapFailureLine(error, URL_ENV, "rolled_back")!;
    expect(line).toBe(
      `search bootstrap failed: the OpenSearch index ${INDEX} on ${HOST} has index.gc_deletes other than 0 (schema_mismatch: gc_deletes): the bootstrap checks but never changes settings; set index.gc_deletes: 0s on that index (a dynamic setting) and rerun the Job, see docs/search-and-runtime-adapters.md (OpenSearch implementation); ${NOT_APPLIED}`,
    );
    expect(line).not.toContain(SCHEMA_PHRASES.absent);
    expect(line).not.toContain(SCHEMA_PHRASES.conflict);
    // "gc_deletes" is the setting's own name; no delete/remove/recreate advice.
    expect(line).not.toMatch(/\b(remove|delete|recreate)\b|stale/i);
  });

  test("the index name follows SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX and never echoes an invalid prefix", () => {
    const error = new SearchIndexError("schema_mismatch", false, "gc_deletes");
    expect(searchBootstrapFailureLine(error, { ...URL_ENV, SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX: "acme" }, "rolled_back")).toContain(
      "index acme-transcript-events on",
    );
    const bad = searchBootstrapFailureLine(error, { ...URL_ENV, SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX: "Bad Prefix/../x" }, "rolled_back")!;
    expect(bad).toContain("index (the configured index) on");
    expect(bad).not.toContain("Bad Prefix");
  });

  test("the grants clause speaks only for this run: no claim about earlier grants, no 42501 prediction", () => {
    for (const applied of ["not_begun", "rolled_back", "committed"] as const) {
      const line = searchBootstrapFailureLine(new SearchIndexError("unavailable", true, "network"), URL_ENV, applied)!;
      expect(line).toContain(applied === "not_begun" ? NOT_BEGUN : "this run's search Postgres changes");
      expect(line).not.toMatch(/permission denied|42501|will report|NOT applied|until this Job succeeds/);
    }
  });

  test("never a URL with credentials, a path or a query: the host only", () => {
    const env = { SEARCH_INDEX_OPENSEARCH_URL: " https://admin:hunter2@search-relay.example.com:9200/prefix?token=abc " };
    expect(openSearchHost(env)).toBe("search-relay.example.com:9200");
    const line = searchBootstrapFailureLine(new SearchIndexError("unavailable", true, "network"), env, "rolled_back")!;
    for (const secret of ["admin", "hunter2", "token", "abc", "prefix", "https://"]) expect(line).not.toContain(secret);
    expect(openSearchHost({})).toBe("(SEARCH_INDEX_OPENSEARCH_URL unset)");
    expect(openSearchHost({ SEARCH_INDEX_OPENSEARCH_URL: "not a url:hunter2" })).toBe("(SEARCH_INDEX_OPENSEARCH_URL is not a URL)");
  });

  test("anything that is not a SearchIndexError gets no line", () => {
    expect(searchBootstrapFailureLine(new Error("search index: unavailable (network)"), URL_ENV, "rolled_back")).toBeUndefined();
    expect(searchBootstrapFailureLine(Object.assign(new Error("x"), { code: "unavailable" }), URL_ENV, "rolled_back")).toBeUndefined();
  });
});

describe("search bootstrap: the wrapper serve() runs", () => {
  const run = async (body: Parameters<typeof withSearchBootstrapDiagnosis<never>>[1]) => {
    const written: string[] = [];
    const error = await caught(() => withSearchBootstrapDiagnosis(URL_ENV, body, (line) => written.push(line)));
    return { error, written };
  };

  test("an OpenSearch-half failure before BEGIN (composing the index) says no Postgres changes, not a rollback", async () => {
    const thrown = new SearchIndexError("unavailable", true, "network");
    const { error, written } = await run(async (openSearch) => {
      await openSearch(async () => {
        throw thrown;
      });
      throw new Error("unreachable");
    });
    expect(error).toBe(thrown);
    expect(written).toEqual([
      `search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); ${NOT_BEGUN}\n`,
    ]);
    expect(written[0]).not.toContain("rolled back");
  });

  test("an OpenSearch-half failure inside the owner transaction says it was rolled back", async () => {
    const { written } = await run(async (openSearch, progress) => {
      progress.postgresBegun = true;
      return openSearch(async () => {
        throw new SearchIndexError("schema_missing");
      });
    });
    expect(written).toHaveLength(1);
    expect(written[0]).toEndWith(`; ${NOT_APPLIED}\n`);
    expect(written[0]).not.toContain("applied no Postgres changes");
  });

  test("a failure after the Postgres commit says the Postgres part succeeded", async () => {
    const { written } = await run(async (openSearch, progress) => {
      progress.postgresCommitted = true;
      return openSearch(async () => {
        throw new SearchIndexError("unavailable", false, "http_403");
      });
    });
    expect(written).toHaveLength(1);
    expect(written[0]).toContain("refused the request (http_403)");
    expect(written[0]).toContain("were committed before this failure");
  });

  test("a SearchIndexError from the Postgres half (not OpenSearch) prints nothing", async () => {
    const thrown = new SearchIndexError("schema_mismatch");
    const { error, written } = await run(async (openSearch) => {
      await openSearch(() => "ok");
      throw thrown;
    });
    expect(error).toBe(thrown);
    expect(written).toEqual([]);
  });

  test("a non-search failure in the OpenSearch half (a plain config Error) prints nothing", async () => {
    const { written } = await run((openSearch) =>
      openSearch(() => {
        throw new Error("SEARCH_INDEX_BACKEND=opensearch needs SEARCH_INDEX_OPENSEARCH_URL");
      }),
    );
    expect(written).toEqual([]);
  });
});

describe("search indexer: startup hint for a 42501 on the sessions schema", () => {
  const open: PGlite[] = [];
  afterEach(async () => {
    await Promise.all(open.splice(0).map((d) => d.close().catch(() => undefined)));
  });
  const HINT = (ns: string, denied = "schema sessions") =>
    `search indexer: this login was denied on ${denied} (42501). Likely the search bootstrap has not completed (the search-indexer-bootstrap Job grants this login its sessions privileges), or those grants changed after it ran. Check: kubectl -n ${ns} get jobs | grep search-indexer-bootstrap; if it failed, read its log, fix the cause (often OpenSearch reachability), delete the Job and run helm upgrade again; if it succeeded, compare this login's grants with what the Job grants (a successful rerun reapplies them).`;

  /** The runtime login before the bootstrap granted it anything. */
  async function ungranted(grant = ""): Promise<PGlite> {
    const db = new PGlite();
    open.push(db);
    await db.exec(`CREATE ROLE hoop NOLOGIN;
      CREATE SCHEMA sessions AUTHORIZATION hoop;
      CREATE TABLE sessions.search_consumer_pending(k text);
      CREATE TABLE sessions.search_consumer_progress(k text);
      ALTER TABLE sessions.search_consumer_pending OWNER TO hoop;
      ALTER TABLE sessions.search_consumer_progress OWNER TO hoop;
      CREATE ROLE search_consumer_runtime LOGIN NOINHERIT;
      ${grant}
      SET SESSION AUTHORIZATION search_consumer_runtime`);
    return db;
  }

  test("the real startup check fails with 42501 permission denied for schema sessions, and the hint names the Job", async () => {
    const db = await ungranted();
    const error = await caught(() => assertServiceSchema({ query: (text, params) => db.query(text, params as never) as never }));
    expect((error as { code?: string }).code).toBe("42501");
    expect((error as Error).message).toBe("permission denied for schema sessions");
    expect(searchIndexerStartupHint(error, {})).toBe(HINT("<namespace>"));
    expect(searchIndexerStartupHint(error, { POD_NAMESPACE: "agent-relay" })).toBe(HINT("agent-relay"));
  });

  test("a table-level 42501 (privilege drift after a completed bootstrap): names the table, offers both causes", async () => {
    const db = await ungranted("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime;");
    const error = await caught(() => db.query("SELECT 1 FROM sessions.search_consumer_pending"));
    expect((error as Error).message).toBe("permission denied for table search_consumer_pending");
    const hint = searchIndexerStartupHint(error, {})!;
    expect(hint).toBe(HINT("<namespace>", "table search_consumer_pending"));
    // A likely cause, not a certain one: the drift case is named, and the line never says the bootstrap failed.
    expect(hint).toContain("or those grants changed after it ran");
    expect(hint).toMatch(/^search indexer: this login was denied on table search_consumer_pending \(42501\)\. Likely /);
    expect(hint).not.toMatch(/bootstrap failed|denied on schema/);
  });

  test("other errors, other schemas and an unsafe POD_NAMESPACE do not get through", () => {
    const pg = (message: string, code = "42501") => Object.assign(new Error(message), { code });
    expect(searchIndexerStartupHint(pg("permission denied for schema public"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("permission denied for schema sessions", "42P01"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("password authentication failed for user \"x\"", "28P01"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(new Error("pipeline consumer: refusing to run as SUPERUSER"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(undefined, {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("permission denied for schema search_index"), { POD_NAMESPACE: "ns; rm -rf /" })).toBe(
      HINT("<namespace>", "schema search_index"),
    );
  });
});
