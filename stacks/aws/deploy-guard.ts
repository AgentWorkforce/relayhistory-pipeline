/**
 * Fail-closed guards that run inside the AWS stack, before it declares a
 * resource, whatever started it (bin/96bulls or a direct `alchemy` command).
 * Pure, so they are unit-tested (stacks/aws/__tests__/deploy-guard.test.ts).
 */
export interface AwsDeploymentTarget {
  accountId: string;
  region: string;
}

export type AwsMode = "plan" | "deploy" | "destroy";

/** Stages whose state carries data or live users: never destroyed, by any route. */
export const NON_DESTROYABLE = (stage: string) => stage === "aws-sessions-dev" || /prod/.test(stage);

/**
 * The operation Alchemy is running. `alchemy destroy` evaluates this stack too,
 * so the subcommand in argv decides; NINETY_SIX_BULLS_AWS_MODE must agree with
 * it, which stops a direct `alchemy destroy` from passing as a deploy. Anything
 * that is not plan or destroy is a deploy (the strictest gate besides destroy).
 */
export function awsMode(env: Record<string, string | undefined>, argv: readonly string[]): AwsMode {
  const sub = argv.find((a) => a === "plan" || a === "deploy" || a === "destroy");
  const fromArgv: AwsMode = sub === "plan" ? "plan" : sub === "destroy" ? "destroy" : "deploy";
  const declared = env.NINETY_SIX_BULLS_AWS_MODE;
  if (declared !== undefined && declared !== "plan" && declared !== "deploy" && declared !== "destroy")
    throw new Error(`96bulls: NINETY_SIX_BULLS_AWS_MODE must be plan, deploy or destroy, got ${JSON.stringify(declared)}. Nothing was changed.`);
  if (fromArgv === "destroy" && declared !== "destroy")
    throw new Error("96bulls: `alchemy destroy` needs NINETY_SIX_BULLS_AWS_MODE=destroy (use `bin/96bulls down <stage>`). Nothing was changed.");
  if (declared !== undefined && declared !== fromArgv)
    throw new Error(`96bulls: NINETY_SIX_BULLS_AWS_MODE=${declared} does not match the alchemy ${fromArgv} being run. Nothing was changed.`);
  return fromArgv;
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

/** `<...>`, TODO, REPLACE or CHANGEME anywhere in the inputs, `$comment` included, means they were copied, not written. */
const PLACEHOLDER = /<[^<>]*>|\bTODO\b|\bREPLACE(_?ME)?\b|\bCHANGEME\b/i;

export function findPlaceholders(value: unknown, path = "$"): string[] {
  if (typeof value === "string") return PLACEHOLDER.test(value) ? [path] : [];
  if (Array.isArray(value)) return value.flatMap((v, i) => findPlaceholders(v, `${path}[${i}]`));
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => findPlaceholders(v, `${path}.${k}`));
  return [];
}

/** A strict YYYY-MM-DD that is a real calendar day (2026-02-30 and 2026-99-99 are refused). */
export function parseCalendarDate(s: string | undefined): Date | undefined {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s ?? "");
  if (!m) return undefined;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const t = new Date(Date.UTC(y, mo - 1, d));
  return t.getUTCFullYear() === y && t.getUTCMonth() === mo - 1 && t.getUTCDate() === d ? t : undefined;
}

/**
 * Khaliq's approval for one exact operation on one exact target, as a record,
 * not prose to be pattern-matched. `words` is his message verbatim, kept for
 * the audit trail; it must not countermand the record.
 */
export interface Approval {
  by: string;
  /** YYYY-MM-DD, a real date, not in the future, at most APPROVAL_MAX_AGE_DAYS old. */
  date: string;
  operations: AwsMode[];
  stage: string;
  accountId: string;
  region: string;
  words: string;
}

export const APPROVER = "Khaliq Gant";
export const APPROVAL_MAX_AGE_DAYS = 30;
const COUNTERMAND = /\b(do\s+not|don'?t|never|not\s+(?:yet|approved|cleared|deploy|apply|destroy)|hold|stop|wait|revoke[sd]?|cancel(?:led)?|no\s+go)\b/i;

export function assertApproval(mode: AwsMode, stage: string, inputs: { accountId: string; region: string; approval?: Approval }, now: Date): void {
  if (mode === "plan") return;
  const a = inputs.approval;
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${mode} of ${stage} is not approved: ${why}. Nothing was changed.`);
  };
  if (!a) return refuse("aws-inputs.json has no `approval` record");
  if (a.by !== APPROVER) refuse(`approval.by must be "${APPROVER}"`);
  const day = parseCalendarDate(a.date);
  if (!day) refuse(`approval.date ${JSON.stringify(a.date)} is not a real YYYY-MM-DD date`);
  const today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  if (day!.getTime() > today) refuse(`approval.date ${a.date} is in the future`);
  if (today - day!.getTime() > APPROVAL_MAX_AGE_DAYS * 86_400_000) refuse(`approval.date ${a.date} is older than ${APPROVAL_MAX_AGE_DAYS} days`);
  if (!Array.isArray(a.operations) || !a.operations.includes(mode)) refuse(`approval.operations does not include "${mode}"`);
  if (a.stage !== stage) refuse(`approval.stage is ${JSON.stringify(a.stage)}`);
  if (a.accountId !== inputs.accountId || a.region !== inputs.region) refuse("approval.accountId/region do not match the inputs");
  if (!a.words?.trim() || a.words.trim().length < 8) refuse("approval.words must hold Khaliq's message verbatim");
  if (COUNTERMAND.test(a.words)) refuse("approval.words countermand the approval");
}

/**
 * Everything the stack checks about who asked for this run, before reading
 * credentials: no placeholders, the typed account, and for deploy/destroy the
 * approval record; destroy additionally the typed stage and a destroyable stage.
 */
export function assertClearance(
  mode: AwsMode,
  stage: string,
  inputs: { accountId: string; region: string; approval?: Approval },
  env: Record<string, string | undefined>,
  now: Date = new Date(),
) {
  const placeholders = findPlaceholders(inputs);
  if (placeholders.length) throw new Error(`96bulls: aws-inputs.json still has placeholder values at ${placeholders.join(", ")}. Nothing was changed.`);
  if (env.NINETY_SIX_BULLS_CONFIRM_ACCOUNT !== inputs.accountId)
    throw new Error("96bulls: set NINETY_SIX_BULLS_CONFIRM_ACCOUNT=<account id> to run the AWS stage. Nothing was changed.");
  if (mode === "destroy") {
    if (NON_DESTROYABLE(stage)) throw new Error(`96bulls: ${stage} is never destroyed; revert the change and deploy again. Nothing was changed.`);
    if (env.NINETY_SIX_BULLS_CONFIRM_DESTROY !== stage) throw new Error(`96bulls: set NINETY_SIX_BULLS_CONFIRM_DESTROY=${stage} to destroy. Nothing was changed.`);
  }
  if (mode === "deploy" && env.NINETY_SIX_BULLS_CONFIRM_STAGE !== stage)
    throw new Error(`96bulls: set NINETY_SIX_BULLS_CONFIRM_STAGE=${stage} to deploy. Nothing was changed.`);
  assertApproval(mode, stage, inputs, now);
}
