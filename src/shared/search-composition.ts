import { createSearchIndexRegistry, type SearchFactoryInput, type SearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import type { SearchIndexBackendName } from "@rhc/adapters/registry.ts";
import { assertSearchServiceBinding, type SearchProjectionState, type SearchServiceBinding } from "../session-pipeline/search-adapter.ts";
import { ledgeredSearchState, postgresSearchState } from "../session-pipeline/postgres-search-state.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { admitPortableConnection, type PortableRole } from "./portable-connection-identity.ts";

/** A reviewed entrypoint supplies implementations, not a private import in the
 * public engine. Registering a name alone never provides completeness state. */
export interface SearchComposition {
  readonly registry: SearchIndexRegistry;
  readonly extras?: SearchFactoryInput["extras"];
  state(input: SearchFactoryInput & { readonly backend: SearchIndexBackendName; readonly role: "writer" | "reader"; readonly tenantRunner?: TenantRunner }): SearchProjectionState;
  /** Optional, additive: awaited once by the Lambda assemblies (async init) before
   * any work, with the pool the composition's state uses. Throwing refuses the
   * service. Absent on v1 compositions, which keep their previous behavior. */
  admit?(sql: SqlExecutor, role: PortableRole): Promise<void>;
}

export function composeSearchService(composition: SearchComposition, input: SearchFactoryInput, role: "writer" | "reader", tenantRunner?: TenantRunner): SearchServiceBinding {
  const registered = composition.registry.registered();
  const backend = composition.registry.select(input.env, registered);
  const options = { ...input, extras: composition.extras };
  const index = composition.registry.create(input.env, options, registered);
  const binding = { index, state: composition.state({ ...options, backend, role, tenantRunner }) };
  assertSearchServiceBinding(binding, backend);
  return binding;
}

/** Portable existing Postgres index plus epoch-fenced completeness. Storage
 * initialization and role privileges remain explicit operator prerequisites. */
export function createPostgresSearchComposition(options: { backendId: string; fromMs: number }): SearchComposition {
  const { backendId, fromMs } = options;
  return {
    registry: createSearchIndexRegistry(),
    state(input) { return postgresSearchState({ schema: input.env.SEARCH_INDEX_POSTGRES_SCHEMA, backendId, fromMs, reader: input.tenantRunner }); },
  };
}

/** Provider-specific values are explicitly resolved by the existing secret
 * loader; factories never discover credentials from the build host or env. */
export function searchSecret(value: unknown): Readonly<Record<string, string | undefined>> | undefined {
  if (value === undefined) return;
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.values(value).some(v => typeof v !== "string")) throw new Error("search secret configuration invalid");
  return value as Record<string, string>;
}

/** The self-host profile: content-free completeness state in the migration-owned
 * sessions.search_projection_state (20261105047000), on the HoopDb runtime pool and
 * transaction. The registry must come from a reviewed provider composition (for
 * example OpenSearch). There is no ledgered Postgres content index, so the
 * `postgres` index backend is refused, as is any SEARCH_INDEX_POSTGRES_SCHEMA:
 * storage names are fixed by the migration, never by environment. Privileged
 * (SUPERUSER, BYPASSRLS, storage-owner) connections are refused at assembly. */
export function createLedgeredSearchComposition(options: { backendId: string; fromMs: number; registry: SearchIndexRegistry; extras?: SearchFactoryInput["extras"] }): SearchComposition {
  const { backendId, fromMs, registry, extras } = options;
  if (!registry) throw new Error("ledgered search composition needs a provider registry");
  return {
    registry,
    ...(extras ? { extras } : {}),
    state(input) {
      if (input.env.SEARCH_INDEX_POSTGRES_SCHEMA !== undefined) throw new Error("SEARCH_INDEX_POSTGRES_SCHEMA is not accepted by the ledgered profile");
      if (input.backend === "postgres") throw new Error("the ledgered profile has no Postgres content index");
      return ledgeredSearchState({ backendId, fromMs, reader: input.tenantRunner });
    },
    admit: (sql, role) => admitPortableConnection(sql, role),
  };
}
