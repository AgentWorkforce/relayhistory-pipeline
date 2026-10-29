/**
 * Sandbox rules settled by Khaliq on 2026-10-28 (plan section 13/13.1), as data.
 * Read by the AWS stage, the `96bulls sandbox` commands and the sweeper. Plan
 * and code only: nothing here creates anything.
 */
export const SANDBOX_RULES = {
  /** Sandbox HoopDb (Khaliq, 2026-10-28): Postgres (pgvector) as a container in the sandbox compute; RDS only when a test needs managed Postgres. */
  databaseDefault: "container" as "container" | "rds",
  /** Regions a 96bulls stage may use; production defaults to us-west-2. */
  regionAllowlist: ["us-east-1", "us-west-1", "us-west-2"] as const,
  productionRegion: "us-west-2",
  /** One reusable sandbox account per agent, under this OU. */
  sandboxOu: "Sandbox",
  /** No OrganizationAccountAccessRole: only the agent-session role, bounded. */
  agentSessionRole: "96bulls-agent-session",
  permissionsBoundary: "96bulls-sandbox-boundary",
  /** Cost: $50 ceiling per sandbox account, alarm at $25. */
  budgetUsd: 50,
  budgetAlarmUsd: 25,
  /** Quiet timeout and extensions: 3h default, 3h blocks, no cap; any activity or deployment resets the clock. */
  quietTimeoutHours: 3,
  extensionBlockHours: 3,
  extensionCap: null as number | null,
  /** The sweeper's three checks; any failure quarantines the account. */
  sweeperChecks: ["zero_sandbox_tagged_resources", "zero_non_baseline_iam_entities", "empty_workspace_bucket_prefix"] as const,
  tags: { stack: "96bulls:stack", owner: "96bulls:owner", sessionUuid: "96bulls:session_uuid", expiresAt: "96bulls:expires_at", lastDeploy: "96bulls:last_deploy" },
} as const;

export type SandboxRegion = (typeof SANDBOX_RULES.regionAllowlist)[number];

export function assertRegion(region: string): asserts region is SandboxRegion {
  if (!(SANDBOX_RULES.regionAllowlist as readonly string[]).includes(region))
    throw new Error(`96bulls: region ${region} is not in the allowlist ${SANDBOX_RULES.regionAllowlist.join(", ")}`);
}
