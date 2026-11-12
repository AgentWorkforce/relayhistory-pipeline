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
/**
 * The protected branch that anchors a held-step release: release/schema-<slot>, where
 * the slot is the target's 6-digit migration slot for an upstream step
 * (upstream:20261105047000_x.sql -> release/schema-047000) and 96bulls-NNNN for a
 * history-lane step (96bulls:0005_x.sql -> release/schema-96bulls-0005). target_sha
 * must be the tip of this branch whenever it differs from the release sha
 * (src/hoopdb/release-target.ts requireReleaseAnchor; the GitHub gate). Returns
 * undefined for a name that is not a schema step.
 */
export const RELEASE_ANCHOR_BRANCH_PREFIX = "release/schema-";
export function releaseAnchorBranch(target: string): string | undefined {
  if (!RELEASE_TARGET_STEP.test(target)) return undefined;
  const up = /^upstream:[0-9]{8}([0-9]{6})_/.exec(target);
  if (up) return RELEASE_ANCHOR_BRANCH_PREFIX + up[1];
  const hist = /^96bulls:([0-9]{4})_/.exec(target);
  return hist ? `${RELEASE_ANCHOR_BRANCH_PREFIX}96bulls-${hist[1]}` : undefined;
}
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
    // Required with the target, both modes: the full 40-hex commit (the release sha or
    // on its first-parent main history, with green push CI of its own) whose tree ends
    // at the target; the release is anchored to it (src/hoopdb/release-target.ts,
    // scripts/schema-release-github.ts).
    RELEASE_TARGET_SHA: targetSha,
    // Apply and bootstrap: the target generation the plan recorded, the generation of
    // target_sha's tree (what --generation prints there); the only generation an
    // apply may mark PASS. Equal to RELEASE_GENERATION when nothing is held.
    RELEASE_TARGET_GENERATION: targetGeneration = "",
    RELEASE_PLAN_RUN: planRun = "",
    RELEASE_APPROVAL: approval = "",
  } = env;
  if (
    (mode !== "plan" && mode !== "bootstrap" && mode !== "apply") ||
    !/^[0-9a-f]{40}$/.test(sha ?? "") ||
    !/^[0-9a-f]{64}$/.test(generation ?? "") ||
    boundary !== "global-default-revoke" ||
    !RELEASE_TARGET_STEP.test(target ?? "")
  )
    refuse("release_inputs");
  // target is always set, so target_sha is always required; never abbreviated.
  if (!/^[0-9a-f]{40}$/.test(targetSha ?? "")) refuse("release_target_sha");
  // The plan's manifest digest is carried only inside the typed approval; the whole
  // string must then equal the canonical form exactly.
  const manifestDigest =
    mode === "apply" ? (/ MANIFEST ([0-9a-f]{64}) PLAN /.exec(approval)?.[1] ?? "") : "";
  if (
    mode === "apply" &&
    (!/^[1-9][0-9]*$/.test(planRun) ||
      !/^[0-9a-f]{64}$/.test(targetGeneration) ||
      !manifestDigest ||
      approval !==
        `APPLY ${SCHEMA_RELEASE.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN ${planRun}`)
  )
    refuse("release_typed_approval");
  if (mode === "plan" && (planRun || approval || targetGeneration))
    refuse("release_plan_inputs");
  // Bootstrap only: the plan's chain digest and the sorted absent roles it will create
  // are carried only inside the typed approval; the whole string must equal the
  // canonical form exactly (src/hoopdb/schema-release-bootstrap.ts bootstrapApproval).
  // The project ref is matched here and never printed.
  let chainDigest = "";
  let bootstrapRoles: string[] = [];
  if (mode === "bootstrap") {
    const m =
      / CHAIN ([0-9a-f]{64}) ROLES (none|[a-z_][a-z0-9_]{0,62}(?:,[a-z_][a-z0-9_]{0,62})*) PLAN /.exec(
        approval,
      );
    chainDigest = m?.[1] ?? "";
    bootstrapRoles = m && m[2] !== "none" ? m[2].split(",") : [];
    const sorted = [...bootstrapRoles].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    if (
      !/^[1-9][0-9]*$/.test(planRun) ||
      !/^[0-9a-f]{64}$/.test(targetGeneration) ||
      !m ||
      new Set(bootstrapRoles).size !== bootstrapRoles.length ||
      sorted.join(",") !== bootstrapRoles.join(",") ||
      approval !==
        `BOOTSTRAP ${SCHEMA_RELEASE.project} ${sha} TARGET ${target} ${targetSha} CHAIN ${chainDigest} ROLES ${m[2]} PLAN ${planRun}`
    )
      refuse("release_bootstrap_approval");
  }
  return {
    mode,
    sha: sha!,
    generation: generation!,
    boundary,
    target: target!,
    targetSha: targetSha!,
    targetGeneration,
    manifestDigest,
    planRun,
    chainDigest,
    bootstrapRoles,
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
