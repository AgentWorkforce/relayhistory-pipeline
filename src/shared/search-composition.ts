import { createSearchIndexRegistry, type SearchFactoryInput, type SearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import type { SearchIndexBackendName } from "@rhc/adapters/registry.ts";
import { assertSearchServiceBinding, type SearchProjectionState, type SearchServiceBinding } from "../session-pipeline/search-adapter.ts";
import { postgresSearchState } from "../session-pipeline/postgres-search-state.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";

/** A reviewed entrypoint supplies implementations, not a private import in the
 * public engine. Registering a name alone never provides completeness state. */
export interface SearchComposition {
  readonly registry: SearchIndexRegistry;
  readonly extras?: SearchFactoryInput["extras"];
  state(input: SearchFactoryInput & { readonly backend: SearchIndexBackendName; readonly role: "writer" | "reader"; readonly tenantRunner?: TenantRunner }): SearchProjectionState;
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
