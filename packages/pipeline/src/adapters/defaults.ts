/**
 * Adapter registries for compositions. `createSearchIndexRegistry` registers
 * Postgres only (the default backend); `createOpenSearchCapableSearchIndexRegistry`
 * registers Postgres and OpenSearch and must be chosen explicitly by a
 * composition that intends to run OpenSearch. `createPipelineRuntimeRegistry`
 * registers the worker runtime only. A composition adds its own factories to
 * these registries before selecting; registering a name does not widen the
 * worker entry points' allow list, while the Lambda entries allow the names
 * their composition's registry holds. Provider implementation placement and
 * customer distribution are separate reviewed changes.
 */
import type { PipelineRuntimeAdapter } from "../runtime/contracts.js";
import { createWorkerRuntime, type WorkSource } from "../runtime/workers.js";
import type { SearchIndexAdapter } from "../search-index/contracts.js";
import { createPostgresSearchIndex, type SqlClient, type TenantSqlRunner } from "../search-index/postgres.js";
import {
  createOpenSearchSearchIndex,
  MAX_OPENSEARCH_TIMEOUT_MS,
  OPENSEARCH_DIALECTS,
  type FetchLike,
  type OpenSearchDialect,
  type OpenSearchSearchIndexConfig,
} from "../search-index/opensearch.js";
import {
  AdapterRegistry,
  DEFAULT_PIPELINE_RUNTIME,
  DEFAULT_SEARCH_INDEX_BACKEND,
  PIPELINE_RUNTIMES,
  SEARCH_INDEX_BACKENDS,
  type PipelineRuntimeName,
  type SearchIndexBackendName,
} from "./registry.js";

/** What a search factory receives. Secrets arrive resolved (file-mounted), never via env. */
export interface SearchFactoryInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  /** The HoopDb (or a dedicated) Postgres for the postgres backend. */
  readonly sql?: SqlClient;
  /** Tenant transactions for the postgres backend's tenant-scoped statements
   * (a reader's pool runner). Omitted: the statements bind the tenant on `sql`. */
  readonly tenantRunner?: TenantSqlRunner;
  readonly secrets?: Readonly<Record<string, string | undefined>>;
  /** Composition-specific clients (ClickHouse client, AWS credential provider, fetch). */
  readonly extras?: Readonly<Record<string, unknown>>;
}

export interface RuntimeFactoryInput {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly source?: WorkSource;
  readonly extras?: Readonly<Record<string, unknown>>;
}

export type SearchIndexRegistry = AdapterRegistry<SearchIndexBackendName, SearchFactoryInput, SearchIndexAdapter>;
export type PipelineRuntimeRegistry = AdapterRegistry<PipelineRuntimeName, RuntimeFactoryInput, PipelineRuntimeAdapter>;

function registerPostgres(registry: SearchIndexRegistry): void {
  registry.register({
    name: "postgres",
    create(input) {
      if (!input.sql) throw new Error("SEARCH_INDEX_BACKEND=postgres needs a Postgres client");
      return createPostgresSearchIndex({
        client: input.sql,
        ...(input.tenantRunner ? { tenantRunner: input.tenantRunner } : {}),
        ...(input.env.SEARCH_INDEX_POSTGRES_SCHEMA ? { schema: input.env.SEARCH_INDEX_POSTGRES_SCHEMA } : {}),
      });
    },
  });
}

/**
 * OpenSearch / Elasticsearch settings from env (`SEARCH_INDEX_OPENSEARCH_*`).
 * The basic-auth password is a secret: it comes from `input.secrets`, never env.
 * CA_FILE is a path read once here (contents are passed to the adapter).
 */
export function openSearchConfigFromInput(input: SearchFactoryInput): OpenSearchSearchIndexConfig {
  const env = input.env;
  const value = (name: string) => {
    const raw = env[`SEARCH_INDEX_OPENSEARCH_${name}`]?.trim();
    return raw === undefined || raw === "" ? undefined : raw;
  };
  const url = value("URL");
  if (!url) throw new Error("SEARCH_INDEX_BACKEND=opensearch needs SEARCH_INDEX_OPENSEARCH_URL");
  const dialect = (value("DIALECT") ?? "opensearch") as OpenSearchDialect;
  if (!OPENSEARCH_DIALECTS.includes(dialect)) {
    throw new Error(`SEARCH_INDEX_OPENSEARCH_DIALECT must be one of ${OPENSEARCH_DIALECTS.join(", ")}`);
  }
  const timeout = value("REQUEST_TIMEOUT_MS");
  if (timeout !== undefined && (!/^[1-9][0-9]{0,5}$/.test(timeout) || Number(timeout) > MAX_OPENSEARCH_TIMEOUT_MS)) {
    throw new Error(`SEARCH_INDEX_OPENSEARCH_REQUEST_TIMEOUT_MS must be an integer from 1 to ${MAX_OPENSEARCH_TIMEOUT_MS} (ms)`);
  }
  const authKind = value("AUTH") ?? "none";
  let auth: OpenSearchSearchIndexConfig["auth"];
  if (authKind === "none") auth = { kind: "none" };
  else if (authKind === "basic") {
    const username = value("USERNAME");
    const password = input.secrets?.SEARCH_INDEX_OPENSEARCH_PASSWORD;
    if (!username || !password) {
      throw new Error("SEARCH_INDEX_OPENSEARCH_AUTH=basic needs SEARCH_INDEX_OPENSEARCH_USERNAME and the SEARCH_INDEX_OPENSEARCH_PASSWORD secret");
    }
    auth = { kind: "basic", username, password };
  } else if (authKind === "sigv4") auth = { kind: "sigv4" };
  else throw new Error("SEARCH_INDEX_OPENSEARCH_AUTH must be one of none, basic, sigv4");
  const caFile = value("CA_FILE");
  let ca: string | undefined;
  if (caFile !== undefined) {
    const fs = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process?.getBuiltinModule?.("node:fs") as
      | { readFileSync(path: string, encoding: "utf8"): string }
      | undefined;
    if (!fs) throw new Error("SEARCH_INDEX_OPENSEARCH_CA_FILE needs a runtime with node:fs");
    ca = fs.readFileSync(caFile, "utf8");
  }
  const fetch = input.extras?.fetch as FetchLike | undefined;
  return {
    url,
    dialect,
    auth,
    ...(value("INDEX_PREFIX") ? { indexPrefix: value("INDEX_PREFIX") } : {}),
    ...(timeout !== undefined ? { requestTimeoutMs: Number(timeout) } : {}),
    ...(ca !== undefined ? { ca } : {}),
    ...(typeof fetch === "function" ? { fetch } : {}),
  };
}

function registerOpenSearch(registry: SearchIndexRegistry): void {
  registry.register({
    name: "opensearch",
    create(input) {
      return createOpenSearchSearchIndex(openSearchConfigFromInput(input));
    },
  });
}

function newSearchIndexRegistry(): SearchIndexRegistry {
  return new AdapterRegistry("SEARCH_INDEX_BACKEND", SEARCH_INDEX_BACKENDS, DEFAULT_SEARCH_INDEX_BACKEND);
}

/** Postgres only (the default backend). The composition ABI and every shipped entry point use this. */
export function createSearchIndexRegistry(): SearchIndexRegistry {
  const registry = newSearchIndexRegistry();
  registerPostgres(registry);
  return registry;
}

/**
 * Postgres and opensearch (OpenSearch or Elasticsearch). Opt-in: only a
 * composition that intends to run OpenSearch should use it, because an entry
 * point built on a composition allows every name its registry holds.
 */
export function createOpenSearchCapableSearchIndexRegistry(): SearchIndexRegistry {
  const registry = newSearchIndexRegistry();
  registerPostgres(registry);
  registerOpenSearch(registry);
  return registry;
}

export function createPipelineRuntimeRegistry(): PipelineRuntimeRegistry {
  const registry: PipelineRuntimeRegistry = new AdapterRegistry("PIPELINE_RUNTIME", PIPELINE_RUNTIMES, DEFAULT_PIPELINE_RUNTIME);
  registry.register({
    name: "workers",
    create(input) {
      if (!input.source) throw new Error("PIPELINE_RUNTIME=workers needs a work source");
      return createWorkerRuntime({ source: input.source });
    },
  });
  return registry;
}
