/**
 * `96bulls plan|up|down aws-<stage>`: the gated AWS stage (stacks/aws/alchemy.aws.run.ts).
 *
 * The stack itself refuses, before declaring anything and whatever started it
 * (stacks/aws/deploy-guard.ts): placeholders, account confirmation, live STS
 * account/region, Khaliq's approval record for the exact operation, typed
 * stage, and destroy of a long-lived stage. This wrapper checks the same typed
 * confirmations early and binds `up` to a captured, verified plan.
 *
 *   plan [--detailed] [--capture]   read only; --capture writes docs/plans/ with provenance
 *   up    verifies docs/plans/<stage>.provenance.json, then alchemy deploy
 *   down  alchemy destroy (refused for aws-sessions-dev and *prod*)
 */
import { NON_DESTROYABLE } from "../../stacks/aws/deploy-guard.ts";

export { NON_DESTROYABLE };
export const AWS_CONFIG = "stacks/aws/alchemy.aws.run.ts";

export interface AwsInvocation {
  args: string[];
  env: Record<string, string>;
}

export function awsInvocation(verb: "plan" | "up" | "down", stage: string, env: Record<string, string | undefined>, flags: { detailed?: boolean } = {}): AwsInvocation {
  if (!/^aws-[a-z0-9-]+$/.test(stage)) throw new Error(`not an AWS stage: ${stage}`);
  if (!env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT) throw new Error("set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id>; nothing was run");
  if (!env.AWS_REGION && !env.AWS_DEFAULT_REGION) throw new Error("set AWS_REGION explicitly (it must equal the inputs' region); nothing was run");
  const common = ["--config", AWS_CONFIG, "--stage", stage, "--no-input"];
  const childEnv = { ALCHEMY_PLAIN: "1" };
  if (verb === "plan") return { args: ["alchemy", "plan", ...common, ...(flags.detailed ? ["--detailed"] : [])], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "plan" } };
  if (verb === "up") {
    if (env.NINETY_SIX_BULLS_CONFIRM_STAGE !== stage) throw new Error(`set NINETY_SIX_BULLS_CONFIRM_STAGE=${stage} to apply; nothing was run`);
    return { args: ["alchemy", "deploy", ...common, "--yes"], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "deploy" } };
  }
  if (NON_DESTROYABLE(stage)) throw new Error(`${stage} is not destroyed from the CLI; revert the change and run \`96bulls up ${stage}\`; nothing was run`);
  if (env.NINETY_SIX_BULLS_CONFIRM_DESTROY !== stage) throw new Error(`set NINETY_SIX_BULLS_CONFIRM_DESTROY=${stage} to destroy; nothing was run`);
  return { args: ["alchemy", "destroy", ...common, "--yes"], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "destroy" } };
}
