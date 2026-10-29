/**
 * `96bulls sandbox up|down|extend|status <agent>`: the per-agent sandbox
 * lifecycle Khaliq settled on 2026-10-28. PLAN/CODE ONLY: every command here
 * prints what it would do and exits; the calls go to the Pippin Tools
 * provisioning API once it exists, and nothing runs against AWS until Khaliq
 * clears it. Rules live in stacks/aws/sandbox-rules.ts.
 */
import { SANDBOX_RULES } from "../../stacks/aws/sandbox-rules.ts";

export interface SandboxPlan {
  command: "up" | "down" | "extend" | "status";
  agent: string;
  stage: string;
  steps: string[];
}

export function planSandbox(command: SandboxPlan["command"], agent: string, opts: { hours?: number } = {}): SandboxPlan {
  if (!/^[a-z0-9][a-z0-9-]{1,40}$/.test(agent)) throw new Error("96bulls: agent name must be [a-z0-9-]");
  const stage = `aws-sandbox-${agent}`;
  const r = SANDBOX_RULES;
  const steps: Record<SandboxPlan["command"], string[]> = {
    up: [
      `Pippin Tools: provision or reattach the sandbox account for ${agent} under OU ${r.sandboxOu} (one per agent, reused).`,
      `Verify the account is empty by the sweeper's checks (${r.sweeperChecks.join(", ")}); refuse a quarantined account.`,
      `Assume ${r.agentSessionRole} (bounded by ${r.permissionsBoundary}); no OrganizationAccountAccessRole.`,
      `Tag the account: ${r.tags.owner}=${agent}, ${r.tags.expiresAt}=now+${r.quietTimeoutHours}h, ${r.tags.lastDeploy}=now.`,
      `alchemy deploy --config stacks/aws/alchemy.aws.run.ts --stage ${stage} (budget $${r.budgetUsd}, alarm $${r.budgetAlarmUsd}).`,
    ],
    down: [
      `alchemy destroy --config stacks/aws/alchemy.aws.run.ts --stage ${stage} (reverse dependency order).`,
      `Run the sweeper: delete everything tagged ${r.tags.stack}=${stage}; then the three checks; quarantine on any failure.`,
      `Keep the account open (per-agent accounts are reused).`,
    ],
    extend: [
      `Add ${opts.hours ?? r.extensionBlockHours}h in ${r.extensionBlockHours}h blocks to ${r.tags.expiresAt} (no cap); log the extension with the session uuid.`,
    ],
    status: [`Read the account tags and the sweeper's last report for ${agent}.`],
  };
  return { command, agent, stage, steps: steps[command] };
}

if (import.meta.main) {
  const [cmd, agent, ...rest] = process.argv.slice(2);
  const hours = Number(rest[rest.indexOf("--hours") + 1]) || undefined;
  if (!cmd || !agent || !["up", "down", "extend", "status"].includes(cmd)) {
    console.error("usage: 96bulls sandbox up|down|extend|status <agent> [--hours N]");
    process.exit(2);
  }
  const plan = planSandbox(cmd as SandboxPlan["command"], agent, { hours });
  console.log(`96bulls sandbox ${plan.command} ${plan.agent} (stage ${plan.stage}) -- PLAN ONLY, nothing executed:`);
  for (const s of plan.steps) console.log(`  - ${s}`);
}
