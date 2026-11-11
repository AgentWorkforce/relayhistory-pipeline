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

/**
 * A chain role's contract as its chain file accepts an existing role (never stricter
 * than the file on membership, so a release plan does not refuse a shape every migrate
 * accepts; the flags additionally pin rolconnlimit -1). Every chain role is
 * NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS, member of
 * no role (each file refuses an outbound membership).
 *
 * login:    "never"       NOLOGIN only (the file refuses rolcanlogin);
 *           "allowed"     NOLOGIN, or LOGIN made later by its operator file (the chain
 *                         file never alters an existing role and accepts it);
 *           "claimed-60s" session_consumer_roles.sql: bare NOLOGIN with no settings,
 *                         or LOGIN with exactly one role-wide statement_timeout=60s.
 * inbound:  "none"        PG16+: only the Supabase creator edge (member postgres,
 *                         grantor superuser supabase_admin, ADMIN, no INHERIT, no SET)
 *                         and only in supabase db mode; before PG16 no member at all
 *                         (bulls96_read_api's block in session_read_api_roles.sql);
 *           "worker"      "none" plus an authenticator member that is NOINHERIT, without
 *                         ADMIN and (PG16+) without INHERIT (session_directory_search_worker.sql);
 *           "verifier"    "none" plus an authenticator member without ADMIN; PG16+ no
 *                         INHERIT and SET TRUE, before PG16 the authenticator NOINHERIT
 *                         (session_token_verifier.sql);
 *           "consumer"    at most the creator edge: member postgres (the release owner,
 *                         the file's current_user) WITH ADMIN, PG16+ no INHERIT and no SET
 *                         (session_consumer_roles.sql);
 *           "unchecked"   the chain file does not check members OF the role; its own
 *                         phase 2 verifier owns that contract (e.g. the seeder is granted
 *                         to an operator login by design).
 * settings: "unchecked"   the chain file does not check role settings;
 *           "worker-2s"   exactly one role-wide row, statement_timeout=2s;
 *           "consumer"    none while NOLOGIN; exactly one role-wide statement_timeout=60s
 *                         while LOGIN.
 */
export interface BootstrapRoleContract {
  readonly login: "never" | "allowed" | "claimed-60s";
  readonly inbound: "none" | "worker" | "verifier" | "consumer" | "unchecked";
  readonly settings: "unchecked" | "worker-2s" | "consumer";
}
const bare = (
  login: BootstrapRoleContract["login"],
  inbound: BootstrapRoleContract["inbound"],
): BootstrapRoleContract => ({ login, inbound, settings: "unchecked" });
/**
 * For each chain file, in chain order, the roles it provisions and their contracts.
 * The guarded production release's bootstrap stage creates only these roles, only by
 * running their file (release-sha bytes), and only when one is absent. A unit test
 * keeps this map equal to the CREATE ROLE / v_name / v_role literals of each file.
 */
export const SESSION_BOOTSTRAP_ROLES: {
  readonly [F in (typeof SESSION_BOOTSTRAP_FILES)[number]]: Readonly<
    Record<string, BootstrapRoleContract>
  >;
} = {
  "supabase/bootstrap/session_directory_search_worker.sql": {
    session_directory_search_worker: {
      login: "never",
      inbound: "worker",
      settings: "worker-2s",
    },
  },
  "supabase/bootstrap/session_token_verifier.sql": {
    session_token_verifier: bare("never", "verifier"),
  },
  "supabase/bootstrap/session_ingest_roles.sql": {
    // Made LOGIN after migrate by their operator files; members owned by their verifiers.
    bulls96_session_ingest: bare("allowed", "unchecked"),
    // The file refuses a seeder that can log in; granted to an operator login by design.
    bulls96_session_ingest_seeder: bare("never", "unchecked"),
    bulls96_canonical_feature_producer: bare("allowed", "unchecked"),
    bulls96_project_events: bare("allowed", "unchecked"),
    bulls96_privacy_eraser: bare("allowed", "unchecked"),
    bulls96_babysitter_retention: bare("allowed", "unchecked"),
    // No login may SET ROLE to these two: only the creator edge (the file's dogpatch P1).
    bulls96_acquisition_installer: bare("allowed", "none"),
    buffered_search_feed_runtime: bare("allowed", "none"),
  },
  "supabase/bootstrap/session_read_api_roles.sql": {
    // Production already has it LOGIN (session_read_api_role.sql); no member.
    bulls96_read_api: bare("allowed", "none"),
    // Lane F1 (20261105046000); its block checks flags and member-of only.
    bulls96_analysis_admit: bare("allowed", "unchecked"),
    // Lane F4 (20261105045000); its block checks flags and member-of only.
    bulls96_digest_narrative: bare("allowed", "unchecked"),
  },
  "supabase/bootstrap/session_consumer_roles.sql": {
    search_consumer_runtime: {
      login: "claimed-60s",
      inbound: "consumer",
      settings: "consumer",
    },
  },
};
