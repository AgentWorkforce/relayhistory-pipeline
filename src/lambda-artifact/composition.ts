/** Explicit, versioned service ABI for a reviewed composition packaged beside
 * this immutable artifact. No discovery, provider import or registration side effect. */
export { derivedSchemaAssets } from "../session-pipeline/derived-schema.ts";
export { createSearchIndexRegistry, createPipelineRuntimeRegistry } from "@rhc/adapters/defaults.ts";
export { composeSearchService, createPostgresSearchComposition } from "../shared/search-composition.ts";
export { postgresSearchState } from "../session-pipeline/postgres-search-state.ts";
export { postgresTlsOptions, postgresConnectionString } from "../shared/postgres-tls.ts";
export { poolTenantRunner, sqlLiveness } from "../session-read-api/liveness.ts";
export { createReadApi } from "../session-read-api/app.ts";
export { indexSearchScope } from "../session-pipeline/search-index.ts";
export { readEventPage } from "../session-read-api/page.ts";
export { dispatchRegisteredLambda } from "../session-pipeline/lambda-runtime.ts";
