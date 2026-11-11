/** Production schema release pins. No environment/credential fallback. */
export const SCHEMA_RELEASE = {
  repository: "AgentWorkforce/relayhistory-pipeline",
  environment: "session-schema-production",
  project: "pyamkrhoyszgzirqqffq",
  host: "aws-0-us-east-1.pooler.supabase.com",
  port: 5432,
  database: "postgres",
  // Supabase's privileged project owner is the proven production migration
  // authority. The workflow pins its exact non-superuser attribute shape and
  // never grants or creates a role to make an inspection pass.
  owner: "postgres",
  runtime: "bulls96_runtime",
  dispatcher: { login: "khaliqgant", id: 1724137 },
  workflow: ".github/workflows/apply-session-schema.yml",
} as const;
/** One schema step name exactly as the migrator ledgers it (migrate.ts schemaSteps). */
export const RELEASE_TARGET_STEP =
  /^(?:upstream:[0-9]{14}|96bulls:[0-9]{4})_[a-z0-9_]+\.sql$/;
export function refuse(code: string): never {
  throw Object.assign(new Error(code), { code });
}
export function releaseInputs(
  env: Record<string, string | undefined> = process.env,
) {
  const {
    RELEASE_MODE: mode,
    RELEASE_SHA: sha,
    RELEASE_GENERATION: generation,
    RELEASE_BOUNDARY: boundary,
    // Required in both modes: the last schema step this release may apply.
    RELEASE_TARGET: target,
    RELEASE_PLAN_RUN: planRun = "",
    RELEASE_APPROVAL: approval = "",
  } = env;
  if (
    (mode !== "plan" && mode !== "apply") ||
    !/^[0-9a-f]{40}$/.test(sha ?? "") ||
    !/^[0-9a-f]{64}$/.test(generation ?? "") ||
    boundary !== "global-default-revoke" ||
    !RELEASE_TARGET_STEP.test(target ?? "")
  )
    refuse("release_inputs");
  if (
    mode === "apply" &&
    (!/^[1-9][0-9]*$/.test(planRun) ||
      approval !==
        `APPLY ${SCHEMA_RELEASE.project} ${sha} ${generation} TARGET ${target} PLAN ${planRun}`)
  )
    refuse("release_typed_approval");
  if (mode === "plan" && (planRun || approval)) refuse("release_plan_inputs");
  return {
    mode,
    sha: sha!,
    generation: generation!,
    boundary,
    target: target!,
    planRun,
  } as const;
}
export function releaseConnection(value: string | undefined) {
  let u: URL;
  try {
    u = new URL(value ?? "");
  } catch {
    return refuse("release_connection");
  }
  if (u.protocol !== "postgresql:" && u.protocol !== "postgres:")
    refuse("release_connection");
  if (
    u.hostname !== SCHEMA_RELEASE.host ||
    u.port !== String(SCHEMA_RELEASE.port) ||
    u.pathname !== "/" + SCHEMA_RELEASE.database ||
    decodeURIComponent(u.username) !==
      `${SCHEMA_RELEASE.owner}.${SCHEMA_RELEASE.project}` ||
    !u.password ||
    u.hash
  )
    refuse("release_connection");
  for (const [k, v] of u.searchParams) {
    if (
      !(
        (k === "sslmode" && (v === "require" || v === "verify-full")) ||
        (k === "channel_binding" && v === "require")
      )
    )
      refuse("release_connection_options");
  }
  return {
    host: SCHEMA_RELEASE.host,
    port: SCHEMA_RELEASE.port,
    database: SCHEMA_RELEASE.database,
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
  };
}
