/**
 * What credentials Alchemy can deploy stage aws-proof-sandbox with.
 *
 * @alchemy.run/aws (2.0.0-beta.84, AuthProvider.ts + alchemy Auth/Resolve.ts)
 * resolves AWS credentials from exactly two places: static env keys
 * (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY [+ AWS_SESSION_TOKEN], with
 * AWS_REGION) whenever they are present, otherwise its OWN profile store
 * (~/.alchemy, methods stored / sso / console-login). It never reads
 * AWS_PROFILE and has no credential_process path. So `pippen sandbox exec`'s
 * default, refreshing mode (AWS_CONFIG_FILE + AWS_PROFILE=pippen-sandbox, no
 * keys) reaches Alchemy as "no env credentials", and it fails with
 * "Provider 'AWS' is not configured in profile 'default'" (proof run 3).
 *
 * The rule here, checked by bin/96bulls before anything runs and again by the
 * stack itself (so a direct `alchemy` run is held to it): exactly one
 * credential source, and it is static env keys with a session token (the
 * agent-session role session is STS). A profile mixed with keys is refused.
 * The caller ARN pin (sandbox-rules.ts pippenProofSandboxCallerArn) still
 * applies on top. Pure: env in, message out.
 */
import { PIPPEN_PROOF_SANDBOX } from "./sandbox-rules.ts";

export const STATIC_KEY_VARS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"] as const;
export const PROFILE_VARS = ["AWS_PROFILE", "AWS_DEFAULT_PROFILE"] as const;
/** The profile `pippen sandbox exec` writes in its default mode. */
export const PIPPEN_EXEC_PROFILE = "pippen-sandbox";
/** An Alchemy step must not run out of credentials part-way: an EKS Auto Mode create alone is ~15-30 min. */
export const MIN_ALCHEMY_CREDENTIAL_MINUTES = 50;

const set = (v: string | undefined) => v !== undefined && v !== "";

/** The command an operator should run instead, e.g. `pippen sandbox exec aws-proof --static-credentials -- bin/96bulls plan aws-proof-sandbox`. */
export const staticExecCommand = (verb = "plan|up|down") =>
  `pippen sandbox exec ${PIPPEN_PROOF_SANDBOX.agent} --static-credentials -- bin/96bulls ${verb} ${PIPPEN_PROOF_SANDBOX.stage}`;

export type SandboxCredentialShape = "static" | "profile" | "mixed" | "partial" | "none";

export function sandboxCredentialShape(env: Record<string, string | undefined>): SandboxCredentialShape {
  const keys = STATIC_KEY_VARS.filter((k) => set(env[k]));
  const profile = PROFILE_VARS.some((k) => set(env[k]));
  if (keys.length && profile) return "mixed";
  if (profile) return "profile";
  if (keys.length === STATIC_KEY_VARS.length) return "static";
  return keys.length ? "partial" : "none";
}

/**
 * Why Alchemy cannot run stage aws-proof-sandbox with this env, or undefined.
 * `now` is for the optional expiry check (AWS_CREDENTIAL_EXPIRATION, which
 * `pippen sandbox exec --static-credentials` sets).
 */
export function sandboxAlchemyCredentialIssue(env: Record<string, string | undefined>, verb?: string, now = Date.now()): string | undefined {
  const fix = `run it as \`${staticExecCommand(verb)}\` (fresh static credentials, at most 1 hour, one Alchemy step per invocation)`;
  switch (sandboxCredentialShape(env)) {
    case "mixed":
      return `AWS credentials are both static keys and a profile (${PROFILE_VARS.filter((k) => set(env[k])).join(", ")}); give exactly one: ${fix}`;
    case "profile":
      return `Alchemy cannot use an AWS profile or credential_process (\`pippen sandbox exec\`'s default mode): @alchemy.run/aws reads only static env keys or its own stored/SSO/console-login profiles, and would fail with "Provider 'AWS' is not configured in profile 'default'"; ${fix}`;
    case "partial":
      return `AWS static credentials need all of ${STATIC_KEY_VARS.join(", ")} (the agent-session role session is STS); missing ${STATIC_KEY_VARS.filter((k) => !set(env[k])).join(", ")}; ${fix}`;
    case "none":
      return `no AWS credentials in the environment; ${fix}`;
  }
  const exp = env.AWS_CREDENTIAL_EXPIRATION;
  if (set(exp)) {
    const ms = Date.parse(exp!);
    if (Number.isNaN(ms)) return `AWS_CREDENTIAL_EXPIRATION ${JSON.stringify(exp)} is not a timestamp`;
    const left = Math.floor((ms - now) / 60_000);
    if (left < MIN_ALCHEMY_CREDENTIAL_MINUTES)
      return `the AWS credentials expire in ${left} min (< ${MIN_ALCHEMY_CREDENTIAL_MINUTES}); start a fresh invocation (\`${staticExecCommand(verb)}\`), renewing the pippen lease first if it caps them`;
  }
  return undefined;
}

export function assertSandboxAlchemyCredentials(env: Record<string, string | undefined>, verb?: string, now = Date.now()): void {
  const issue = sandboxAlchemyCredentialIssue(env, verb, now);
  if (issue) throw new Error(`96bulls: stage ${PIPPEN_PROOF_SANDBOX.stage}: ${issue}. Nothing was changed.`);
}
