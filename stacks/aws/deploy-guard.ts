/**
 * Fail-closed guards that run before the AWS stage declares a resource.
 * Pure, so they are unit-tested (stacks/aws/__tests__/deploy-guard.test.ts).
 */
export interface AwsDeploymentTarget {
  accountId: string;
  region: string;
}

/** What the stage is being run for. Unset (a direct `alchemy deploy`) is treated as deploy. */
export type AwsMode = "plan" | "deploy" | "destroy";

export function awsMode(env: Record<string, string | undefined>): AwsMode {
  const m = env.NINETY_SIX_BULLS_AWS_MODE ?? "deploy";
  if (m !== "plan" && m !== "deploy" && m !== "destroy") throw new Error(`96bulls: NINETY_SIX_BULLS_AWS_MODE must be plan, deploy or destroy, got ${JSON.stringify(m)}. Nothing was created.`);
  return m;
}

/**
 * The live STS caller and the resolved provider region must be exactly the
 * inputs' account and region, or nothing is declared.
 */
export function assertAwsDeploymentTarget(expected: AwsDeploymentTarget, actual: AwsDeploymentTarget): void {
  if (actual.accountId !== expected.accountId)
    throw new Error(`96bulls: live AWS STS caller is account ${actual.accountId}, not the inputs' account ${expected.accountId}. Nothing was created.`);
  if (actual.region !== expected.region)
    throw new Error(`96bulls: resolved AWS provider region is ${actual.region}, not the inputs' region ${expected.region}. Nothing was created.`);
}

/** `<...>`, TODO, REPLACE or CHANGEME anywhere in the inputs means they were copied, not written. */
const PLACEHOLDER = /<[^<>]*>|\bTODO\b|\bREPLACE(_?ME)?\b|\bCHANGEME\b/i;

export function findPlaceholders(value: unknown, path = "$"): string[] {
  if (typeof value === "string") return PLACEHOLDER.test(value) ? [path] : [];
  if (Array.isArray(value)) return value.flatMap((v, i) => findPlaceholders(v, `${path}[${i}]`));
  if (value && typeof value === "object")
    return Object.entries(value).flatMap(([k, v]) => (k === "$comment" ? [] : findPlaceholders(v, `${path}.${k}`)));
  return [];
}

/**
 * Clearance: deploy and destroy need Khaliq's words with a date (YYYY-MM-DD) in
 * `clearedBy`, the account confirmed on the command line, and no placeholder
 * anywhere. A plan reads only, so it needs the account confirmation and no
 * placeholders, but not the clearance.
 */
export function assertClearance(mode: AwsMode, inputs: { accountId: string; clearedBy?: string }, env: Record<string, string | undefined>) {
  const placeholders = findPlaceholders(inputs);
  if (placeholders.length) throw new Error(`96bulls: aws-inputs.json still has placeholder values at ${placeholders.join(", ")}. Nothing was created.`);
  if (env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT !== inputs.accountId)
    throw new Error("96bulls: set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id> to run the AWS stage. Nothing was created.");
  if (mode === "plan") return;
  const c = inputs.clearedBy?.trim() ?? "";
  if (!/\b20\d\d-\d\d-\d\d\b/.test(c) || !/khaliq/i.test(c) || c.length < 24)
    throw new Error(`96bulls: ${mode} needs clearedBy with Khaliq's words and the date (YYYY-MM-DD). Nothing was created.`);
}
