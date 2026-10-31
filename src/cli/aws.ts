/**
 * `96bulls plan|up|down aws-<stage>`: the gated AWS stage (stacks/aws/alchemy.aws.run.ts).
 *
 * The stack itself refuses, before declaring anything, unless the inputs carry
 * no placeholders, NINETY_SIX_BULLS_CONFIRM_ACCOUNT equals their account, the
 * live STS caller and provider region equal their account and region, and (up
 * and down) `clearedBy` holds Khaliq's dated words. This file adds the typed
 * stage confirmation and keeps destroy away from long-lived stages.
 *
 *   plan  read only; fails closed on any error
 *   up    alchemy deploy;  needs NINETY_SIX_BULLS_CONFIRM_STAGE=<stage>
 *   down  alchemy destroy; needs NINETY_SIX_BULLS_CONFIRM_DESTROY=<stage>; refused
 *         for aws-sessions-dev and any *prod* stage (roll those back by reverting
 *         and running `up` again)
 */
export const AWS_CONFIG = "stacks/aws/alchemy.aws.run.ts";

/** Stages whose state carries data or live users: never destroyed from the CLI. */
export const NON_DESTROYABLE = (stage: string) => stage === "aws-sessions-dev" || /prod/.test(stage);

export interface AwsInvocation {
  args: string[];
  env: Record<string, string>;
}

export function awsInvocation(verb: "plan" | "up" | "down", stage: string, env: Record<string, string | undefined>): AwsInvocation {
  if (!/^aws-[a-z0-9-]+$/.test(stage)) throw new Error(`not an AWS stage: ${stage}`);
  if (!env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT) throw new Error("set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id>; nothing was run");
  if (!env.AWS_REGION && !env.AWS_DEFAULT_REGION) throw new Error("set AWS_REGION explicitly (it must equal the inputs' region); nothing was run");
  const common = ["--config", AWS_CONFIG, "--stage", stage, "--no-input"];
  const childEnv = { ALCHEMY_PLAIN: "1" };
  if (verb === "plan") return { args: ["alchemy", "plan", ...common], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "plan" } };
  if (verb === "up") {
    if (env.NINETY_SIX_BULLS_CONFIRM_STAGE !== stage) throw new Error(`set NINETY_SIX_BULLS_CONFIRM_STAGE=${stage} to apply; nothing was run`);
    return { args: ["alchemy", "deploy", ...common, "--yes"], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "deploy" } };
  }
  if (NON_DESTROYABLE(stage)) throw new Error(`${stage} is not destroyed from the CLI; revert the change and run \`96bulls up ${stage}\`; nothing was run`);
  if (env.NINETY_SIX_BULLS_CONFIRM_DESTROY !== stage) throw new Error(`set NINETY_SIX_BULLS_CONFIRM_DESTROY=${stage} to destroy; nothing was run`);
  return { args: ["alchemy", "destroy", ...common, "--yes"], env: { ...childEnv, NINETY_SIX_BULLS_AWS_MODE: "destroy" } };
}
