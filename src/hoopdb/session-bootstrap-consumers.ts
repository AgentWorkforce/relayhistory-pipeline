/**
 * Reviewed, static manifest: each bootstrap chain role (SESSION_BOOTSTRAP_ROLES) and its
 * FIRST consumer, the first supabase/migrations file, in name order, whose text contains
 * the role name as a whole word. A target-bound release requires (and its bootstrap
 * stage provisions) only the roles whose first consumer is at or before the bound
 * target; a role first consumed above the target is not required.
 *
 * The migration files are frozen, so this list only grows. When a new chain role lands,
 * add it here with its first consumer; session-bootstrap-consumers.test.ts recomputes
 * this list from the migration files and from SESSION_BOOTSTRAP_ROLES and fails on any
 * difference. Kept as pairs (not a record) so a duplicate or conflicting entry is
 * representable and refused (release_bootstrap_manifest) rather than silently merged.
 */
export const SESSION_BOOTSTRAP_CONSUMERS: readonly (readonly [role: string, firstConsumer: string])[] = [
  ["session_directory_search_worker", "20261030020000_session_directory_short_summary.sql"],
  ["session_token_verifier", "20261104020100_auth_session_verify.sql"],
  ["bulls96_session_ingest", "20261104022000_session_ingest_ledger.sql"],
  ["bulls96_session_ingest_seeder", "20261104022000_session_ingest_ledger.sql"],
  ["bulls96_canonical_feature_producer", "20261105025000_canonical_feature_journal.sql"],
  ["bulls96_project_events", "20261105033000_canonical_project_events.sql"],
  ["bulls96_read_api", "20261105034000_session_directory_read_api.sql"],
  ["bulls96_privacy_eraser", "20261105041000_ingest_privacy_authority.sql"],
  ["bulls96_babysitter_retention", "20261105042000_babysitter_evidence_retention.sql"],
  ["bulls96_acquisition_installer", "20261105043850_ingest_acquisition_owner.sql"],
  ["buffered_search_feed_runtime", "20261105043870_buffered_search_feed_cursor.sql"],
  ["search_consumer_runtime", "20261105043900_session_consumer_function_grants.sql"],
  ["bulls96_digest_narrative", "20261105045000_digest_narrative_cache.sql"],
  ["bulls96_analysis_admit", "20261105046000_analysis_jobs.sql"],
];

/** A first consumer's schema step name, as the migrator ledgers it (upstream lane). */
export const consumerStep = (file: string) => `upstream:${file}`;

/**
 * Every role name a chain file's text can provision: literal CREATE ROLE names, v_name /
 * v_role text literals and FOREACH ... IN ARRAY ARRAY[...] lists, with `--` comment lines
 * ignored. The bootstrap stage compares this set, computed on the anchor's bytes, with
 * SESSION_BOOTSTRAP_ROLES for the file; session-bootstrap-roles.test.ts pins it on HEAD.
 */
export function chainFileRoles(text: string): Set<string> {
  const sql = text
    .split("\n")
    .filter((l) => !/^\s*--/.test(l))
    .join("\n");
  const names = new Set<string>();
  for (const m of sql.matchAll(/CREATE ROLE\s+([a-z_][a-z0-9_]*)/g))
    if (m[1] !== "IF") names.add(m[1]);
  for (const m of sql.matchAll(/\bv_(?:name|role)\s+text\s*:=\s*'([^']+)'/g)) names.add(m[1]);
  for (const m of sql.matchAll(/FOREACH\s+v_(?:name|role)\s+IN\s+ARRAY\s+ARRAY\[([^\]]*)\]/g))
    for (const n of m[1].matchAll(/'([^']+)'/g)) names.add(n[1]);
  return names;
}
