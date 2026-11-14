/**
 * INGEST_ACCEPT_GENERATIONS parsing for the session ingest Worker (worker.ts re-exports it).
 *
 * Kept in its own module, free of the Worker's module-load TS grant (worker.ts imports
 * hosted-worker-grant.ts first), so that tooling which only needs this rule, such as the schema
 * release rehearsal (scripts/pg-proof/schema-rehearsal.ts, stage R6), never gains the hosted-worker
 * grant (docs/scrub-engine.md, "Default flip"; scripts/scrub-default-scan.ts "hosted-grant-reach").
 * Import nothing here that reaches a grant module.
 */

/**
 * INGEST_ACCEPT_GENERATIONS: unset, or a comma list of 1-2 distinct explicit lowercase hex64 generations that
 * includes INGEST_EXPECT_GENERATION, in buffered mode only. Anything else (a wildcard, a third value, a value
 * without the expected generation, whitespace inside a value, an explicitly empty value) is a configuration
 * error, never a silent narrowing or widening.
 */
export function acceptedGenerations(env: { INGEST_ACCEPT_GENERATIONS?: string; INGEST_EXPECT_GENERATION?: string }, acceptanceMode: string): readonly string[] | undefined {
  const raw = env.INGEST_ACCEPT_GENERATIONS;
  if (raw === undefined) return undefined;
  if (acceptanceMode !== "buffered") throw new Error("session ingest: INGEST_ACCEPT_GENERATIONS needs buffered acceptance");
  const list = raw.split(",");
  if (list.length < 1 || list.length > 2 || !list.every((g) => /^[0-9a-f]{64}$/.test(g)) || new Set(list).size !== list.length
      || !list.includes(env.INGEST_EXPECT_GENERATION ?? ""))
    throw new Error("session ingest: INGEST_ACCEPT_GENERATIONS must be 1 or 2 distinct hex64 generations including INGEST_EXPECT_GENERATION");
  return Object.freeze(list);
}
