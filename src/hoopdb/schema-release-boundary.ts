/** CI-only Supabase event-trigger refresh, bound to the approved read-only plan. */
import { createHash } from "node:crypto";
import {
  functionBoundaryEvidence,
  type FunctionBoundaryEvidence,
} from "./migrate-guards.ts";
import { wrapperSource, type WrapperSource } from "./owner-operator-apply.ts";
import { SCHEMA_RELEASE as P, refuse } from "./schema-release-contract.ts";
import type { MigrationClient } from "./migrate.ts";

type Boundary = FunctionBoundaryEvidence["event_trigger"];

export function releaseBoundaryDigest(boundary: unknown): string {
  return createHash("sha256").update(JSON.stringify(boundary)).digest("hex");
}

/**
 * The plan may legitimately observe the preceding release's function body. Every
 * other trust-boundary property must already be exact before CI is allowed to
 * replace that body. The body digest is retained in the plan and compared again
 * under the migrator advisory lock.
 */
export function requireSafeReleaseBoundary(boundary: any): Boundary {
  if (
    !boundary?.present ||
    boundary.event !== "ddl_command_end" ||
    !boundary.tags_exact ||
    !boundary.enabled_ok ||
    boundary.owner !== P.owner ||
    !boundary.owner_is_migration_owner ||
    !boundary.function_is_reviewed ||
    boundary.function_owner !== P.owner ||
    !boundary.function_owner_is_migration_owner ||
    !boundary.schema_owner_is_migration_owner ||
    typeof boundary.prosrc_sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(boundary.prosrc_sha256) ||
    !boundary.proconfig_exact ||
    boundary.security_definer !== false ||
    boundary.volatility !== "v" ||
    boundary.language !== "plpgsql" ||
    boundary.returns !== "event_trigger" ||
    JSON.stringify(boundary.function_acl) !==
      JSON.stringify(["<owner>:EXECUTE"]) ||
    boundary.function_public_execute !== false ||
    !boundary.runtime_role_setting_matches ||
    !boundary.schema_private
  )
    refuse("release_boundary_identity");
  return boundary as Boundary;
}

export async function inspectReleaseBoundary(
  client: MigrationClient,
): Promise<Boundary> {
  const evidence = await functionBoundaryEvidence(
    client,
    "global-default-revoke",
    "supabase",
    P.runtime,
  );
  // The global default is the catalog-proven control while the reviewed event
  // trigger body moves from the previous release to this exact source.
  if (!evidence.satisfied) refuse("release_boundary_control");
  return requireSafeReleaseBoundary(evidence.event_trigger);
}

async function exactTargetBoundary(client: MigrationClient): Promise<Boundary> {
  const evidence = await functionBoundaryEvidence(
    client,
    "event-trigger",
    "supabase",
    P.runtime,
  );
  const boundary = requireSafeReleaseBoundary(evidence.event_trigger);
  if (!evidence.satisfied || !boundary.verified || !boundary.prosrc_exact)
    refuse("release_boundary_refresh_unproven");
  return boundary;
}

/** Run only from runMigrations.beforeWrite, under the migrator advisory lock. */
export async function refreshReleaseBoundary(
  client: MigrationClient,
  planned: unknown,
  suppliedWrapper?: WrapperSource,
): Promise<Boundary> {
  const expectedWrapper = await wrapperSource();
  const wrapper = suppliedWrapper ?? expectedWrapper;
  if (
    wrapper.sql !== expectedWrapper.sql ||
    wrapper.sha256 !== expectedWrapper.sha256 ||
    wrapper.prosrcSha256 !== expectedWrapper.prosrcSha256
  )
    refuse("release_boundary_wrapper");
  const approved = requireSafeReleaseBoundary(planned);
  const current = await inspectReleaseBoundary(client);
  if (releaseBoundaryDigest(current) !== releaseBoundaryDigest(approved))
    refuse("release_boundary_changed");
  await client.query("BEGIN");
  try {
    await client.query(wrapper.sql);
    const exact = await exactTargetBoundary(client);
    await client.query("COMMIT");
    return exact;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

export async function verifyReleaseBoundary(
  client: MigrationClient,
): Promise<Boundary> {
  return exactTargetBoundary(client);
}
