/**
 * The single ordered bootstrap chain every migration applier runs before the
 * migration chain itself. Migrations never create roles (a CNPG/Cloud SQL
 * migration owner has no CREATEROLE), and the function-grants migration
 * FAILs closed when its grantee is absent — so every path that applies the
 * full supabase/migrations chain must provision these files first, in this
 * order, exactly like migrate.ts plan() does. Add a file here, not in each
 * applier; every consumer below reads this list.
 *
 * Paths are repo-relative. Test-only shim
 * (supabase/tests/supabase_shim.sql) is intentionally NOT listed: only the
 * suites that emulate Supabase add it, before this chain.
 */
export const SESSION_BOOTSTRAP_FILES = [
  // Privileged and idempotent worker role. 20261101000100 verifies it and
  // fails closed if absent (42704); it never creates it.
  "supabase/bootstrap/session_directory_search_worker.sql",
  // The auth token verifier role: the same contract, its own file.
  // 20261101000100 verifies it and fails closed if absent (42704).
  "supabase/bootstrap/session_token_verifier.sql",
  // The session ingest roles (bulls96_session_ingest, _seeder): the same
  // contract. 20261102010000 verifies them and fails closed if absent (42704).
  "supabase/bootstrap/session_ingest_roles.sql",
  // The session read API role (bulls96_read_api), bare: the same contract.
  // 20261105034000 verifies it and fails closed if absent (42704); it never
  // creates it. session_read_api_role.sql (after migrate, operator) keeps
  // LOGIN, settings and RLS.
  "supabase/bootstrap/session_read_api_roles.sql",
  // The session pipeline consumer role (search_consumer_runtime), bare: the
  // same contract, its own file. The consumer grant migration installs the
  // twelve function EXECUTEs; this step only provisions the NOLOGIN role so
  // the grants have a grantee on every migrate. session_consumer_role.sql
  // (after migrate, operator) promotes it to LOGIN and owns the table
  // grants, RLS and verify.
  "supabase/bootstrap/session_consumer_roles.sql",
] as const;
