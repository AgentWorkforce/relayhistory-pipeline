/**
 * The self-host OpenSearch search composition: the registry's OpenSearch
 * adapter for the index, Postgres projection state for completeness (the same
 * state the Postgres composition uses, under its own backend id).
 *
 * Service entries only (session-read-api, session-pipeline-consumer and its
 * bootstrap). It is deliberately outside search-composition.ts: that module is
 * in the Cloudflare read Worker's graph and the Lambda ABI, neither of which may
 * reach this registry or the Node credential loader in shared/locker.ts.
 *
 * With SEARCH_INDEX_OPENSEARCH_AUTH=sigv4 the credentials come only from EKS Pod
 * Identity, so the signature is the ServiceAccount's own role. The Locker's
 * static keys are never considered: they are a different identity.
 */
import { createOpenSearchCapableSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { postgresSearchState } from "../session-pipeline/postgres-search-state.ts";
import type { SearchComposition } from "./search-composition.ts";
import { podIdentityCredentials } from "./locker.ts";

export function createOpenSearchSearchComposition(options: {
  env: NodeJS.ProcessEnv;
  backendId: string;
  fromMs: number;
}): SearchComposition {
  const { env, backendId, fromMs } = options;
  const sigv4 = env.SEARCH_INDEX_OPENSEARCH_AUTH?.trim() === "sigv4";
  return {
    registry: createOpenSearchCapableSearchIndexRegistry(),
    ...(sigv4 ? { extras: { awsCredentials: () => podIdentityCredentials(env) } } : {}),
    state(input) {
      return postgresSearchState({ schema: input.env.SEARCH_INDEX_POSTGRES_SCHEMA, backendId, fromMs, reader: input.tenantRunner });
    },
  };
}
