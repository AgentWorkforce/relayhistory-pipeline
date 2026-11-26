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
  'the search Postgres grants were NOT applied, so the search indexer will report "permission denied for schema sessions" until this Job succeeds';

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
    expect(searchBootstrapFailureLine(error, env, false)).toBe(
      `search bootstrap failed: OpenSearch 127.0.0.1:1 unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); ${NOT_APPLIED}`,
    );
  });

  test("after the Postgres half committed, the line says it succeeded", () => {
    const line = searchBootstrapFailureLine(new SearchIndexError("unavailable", true, "network"), URL_ENV, true);
    expect(line).toBe(
      "search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); the search Postgres part (state tables and grants for search_consumer_runtime) already succeeded and is kept",
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
    expect(searchBootstrapFailureLine(error, URL_ENV, false)).toBe(
      `search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com refused the request (http_403:security_exception): check the OpenSearch role mapping for the search indexer's Pod Identity role, and the Pod Identity association; ${NOT_APPLIED}`,
    );
    // The backend's reason text never reaches the line.
    expect(searchBootstrapFailureLine(error, URL_ENV, false)).not.toContain("no permissions");
  });

  test("every SearchIndexError kind gets its own pointer", () => {
    const cause = (code: ConstructorParameters<typeof SearchIndexError>[0], detail?: string) =>
      searchBootstrapFailureLine(new SearchIndexError(code, false, detail), URL_ENV, false)!
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
      `OpenSearch ${host} sent a reply that is not OpenSearch (bad_response): check search.opensearch.url`,
    );
    expect(cause("unsupported", "dialect_mismatch")).toBe(
      `OpenSearch ${host} is not the configured product (dialect_mismatch): check search.opensearch.dialect`,
    );
    expect(cause("invalid_argument", "sigv4_needs_https")).toBe(
      "the OpenSearch settings are invalid (invalid_argument: sigv4_needs_https): check search.opensearch.* in your values file",
    );
    expect(cause("schema_mismatch", "gc_deletes")).toBe(
      `the OpenSearch index on ${host} exists with a conflicting mapping or settings (schema_mismatch: gc_deletes): check search.opensearch.indexPrefix, or remove the stale index`,
    );
    expect(cause("aborted")).toBe(`OpenSearch ${host} failed (aborted)`);
  });

  test("never a URL with credentials, a path or a query: the host only", () => {
    const env = { SEARCH_INDEX_OPENSEARCH_URL: " https://admin:hunter2@search-relay.example.com:9200/prefix?token=abc " };
    expect(openSearchHost(env)).toBe("search-relay.example.com:9200");
    const line = searchBootstrapFailureLine(new SearchIndexError("unavailable", true, "network"), env, false)!;
    for (const secret of ["admin", "hunter2", "token", "abc", "prefix", "https://"]) expect(line).not.toContain(secret);
    expect(openSearchHost({})).toBe("(SEARCH_INDEX_OPENSEARCH_URL unset)");
    expect(openSearchHost({ SEARCH_INDEX_OPENSEARCH_URL: "not a url:hunter2" })).toBe("(SEARCH_INDEX_OPENSEARCH_URL is not a URL)");
  });

  test("anything that is not a SearchIndexError gets no line", () => {
    expect(searchBootstrapFailureLine(new Error("search index: unavailable (network)"), URL_ENV, false)).toBeUndefined();
    expect(searchBootstrapFailureLine(Object.assign(new Error("x"), { code: "unavailable" }), URL_ENV, false)).toBeUndefined();
  });
});

describe("search bootstrap: the wrapper serve() runs", () => {
  const run = async (body: Parameters<typeof withSearchBootstrapDiagnosis<never>>[1]) => {
    const written: string[] = [];
    const error = await caught(() => withSearchBootstrapDiagnosis(URL_ENV, body, (line) => written.push(line)));
    return { error, written };
  };

  test("an OpenSearch-half failure prints the line once, first, and still rethrows the same error", async () => {
    const thrown = new SearchIndexError("unavailable", true, "network");
    const { error, written } = await run(async (openSearch) => {
      await openSearch(async () => {
        throw thrown;
      });
      throw new Error("unreachable");
    });
    expect(error).toBe(thrown);
    expect(written).toEqual([
      `search bootstrap failed: OpenSearch search-relay.us-east-1.es.amazonaws.com unreachable (network): check search.opensearch.url, the VPC/security group and Pod Identity (the OpenSearch role mapping); ${NOT_APPLIED}\n`,
    ]);
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
    expect(written[0]).toContain("already succeeded and is kept");
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
  const HINT = (ns: string) =>
    `search indexer: the search bootstrap has not completed (the search-indexer-bootstrap Job grants this login its sessions privileges). Check: kubectl -n ${ns} get jobs | grep search-indexer-bootstrap; if it failed, read its log, fix the cause (often OpenSearch reachability), delete the Job and run helm upgrade again.`;

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

  test("a 42501 on a table counts; with schema USAGE but no table grant", async () => {
    const db = await ungranted("GRANT USAGE ON SCHEMA sessions TO search_consumer_runtime;");
    const error = await caught(() => db.query("SELECT 1 FROM sessions.search_consumer_pending"));
    expect((error as Error).message).toBe("permission denied for table search_consumer_pending");
    expect(searchIndexerStartupHint(error, {})).toBe(HINT("<namespace>"));
  });

  test("other errors, other schemas and an unsafe POD_NAMESPACE do not get through", () => {
    const pg = (message: string, code = "42501") => Object.assign(new Error(message), { code });
    expect(searchIndexerStartupHint(pg("permission denied for schema public"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("permission denied for schema sessions", "42P01"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("password authentication failed for user \"x\"", "28P01"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(new Error("pipeline consumer: refusing to run as SUPERUSER"), {})).toBeUndefined();
    expect(searchIndexerStartupHint(undefined, {})).toBeUndefined();
    expect(searchIndexerStartupHint(pg("permission denied for schema search_index"), { POD_NAMESPACE: "ns; rm -rf /" })).toBe(
      HINT("<namespace>"),
    );
  });
});
