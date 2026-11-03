/**
 * `bin/96bulls plan|up|down aws-proof-sandbox` under `pippen sandbox exec`.
 *
 * Alchemy reads only static env keys (stacks/aws/sandbox-credentials.ts), but
 * `pippen sandbox exec aws-proof -- ...` by default gives a refreshing
 * credential_process profile. So, before Alchemy starts:
 *
 *   - static keys, no profile (`exec --static-credentials`): used as they are,
 *     after the same checks the stack makes (all three keys, expiry >= 50 min).
 *   - the pippen profile, no keys (default `exec`): resolved ONCE with
 *     `aws configure export-credentials --profile pippen-sandbox --format process`
 *     (which runs pippen's own `credentials-process`, a fresh agent-session
 *     session per call) into the env of the child processes only: never to
 *     disk, never in argv, never printed (stdout is parsed, not echoed; errors
 *     carry no output). Refused unless the profile is exactly the one
 *     `pippen sandbox exec` writes (src/proof/aws resolveToolCredentials, the
 *     harness's own check, so the two cannot drift) and the credentials have
 *     >= 50 min left. The child then has the keys, AWS_CONFIG_FILE and
 *     AWS_SHARED_CREDENTIALS_FILE=/dev/null and no AWS_PROFILE, so nothing in
 *     it can mix sources. No refresh: one Alchemy step per invocation, < 1 hour.
 *   - anything else (another profile, keys mixed with a profile, partial keys,
 *     nothing, or a resolution that fails): refused before Alchemy runs, with
 *     the `pippen sandbox exec aws-proof --static-credentials -- ...` command,
 *     instead of Alchemy's "Provider 'AWS' is not configured in profile 'default'".
 *
 * Only stage aws-proof-sandbox; every other stage is untouched.
 */
import { PIPPEN_SANDBOX_PROOF_TARGET } from "../proof/aws/guard.ts";
import { resolveToolCredentials } from "../proof/aws/cluster-binding.ts";
import { PIPPEN_PROOF_SANDBOX } from "../../stacks/aws/sandbox-rules.ts";
import {
  MIN_ALCHEMY_CREDENTIAL_MINUTES, PIPPEN_EXEC_PROFILE, PROFILE_VARS, sandboxAlchemyCredentialIssue, sandboxCredentialShape, staticExecCommand,
} from "../../stacks/aws/sandbox-credentials.ts";

export type Env = Record<string, string | undefined>;

export interface ResolveDeps {
  /** Runs `aws` with exactly this env; returns its exit status and stdout. Never logs. */
  runAws: (args: string[], env: Env) => { status: number | null; stdout: string };
  now?: () => number;
  readFile?: (path: string) => string;
}

export const EXPORT_CREDENTIALS_ARGS = Object.freeze(["configure", "export-credentials", "--profile", PIPPEN_EXEC_PROFILE, "--format", "process"]);

const refuse = (verb: string, why: string): never => {
  throw new Error(`stage ${PIPPEN_PROOF_SANDBOX.stage}: ${why}. Run it as \`${staticExecCommand(verb)}\`; nothing was run`);
};

/**
 * The env the Alchemy child (and the CLI's own aws/git calls for it) must run
 * with: `env` itself for any other stage or already-static credentials, or a
 * new object with the profile resolved to static keys. Throws (nothing run)
 * when neither is possible.
 */
export function sandboxAlchemyEnv(verb: string, stage: string, env: Env, deps: ResolveDeps): Env {
  if (stage !== PIPPEN_PROOF_SANDBOX.stage) return env;
  const now = deps.now ?? Date.now;
  const shape = sandboxCredentialShape(env);
  if (shape !== "profile") {
    const issue = sandboxAlchemyCredentialIssue(env, verb, now());
    if (issue) throw new Error(`stage ${PIPPEN_PROOF_SANDBOX.stage}: ${issue}; nothing was run`);
    return env;
  }
  // A profile and no keys: only `pippen sandbox exec`'s own, for this lease, is resolved.
  const profile = resolveToolCredentials(env, PIPPEN_SANDBOX_PROOF_TARGET, {
    ...(env[PIPPEN_PROOF_SANDBOX.sessionEnv] ? { pippenSession: env[PIPPEN_PROOF_SANDBOX.sessionEnv] } : {}),
    ...(deps.readFile ? { readFile: deps.readFile } : {}),
  });
  if ("error" in profile) return refuse(verb, `Alchemy cannot use this AWS profile (${profile.error})`);
  if (profile.kind !== "pippen-profile") return refuse(verb, "Alchemy cannot use this AWS profile");
  let r: { status: number | null; stdout: string };
  try {
    r = deps.runAws([...EXPORT_CREDENTIALS_ARGS], env);
  } catch {
    return refuse(verb, `\`aws ${EXPORT_CREDENTIALS_ARGS.join(" ")}\` could not run (AWS CLI v2.9+ is required)`);
  }
  if (r.status !== 0) return refuse(verb, `\`aws ${EXPORT_CREDENTIALS_ARGS.join(" ")}\` failed (exit ${r.status})`);
  let c: { Version?: unknown; AccessKeyId?: unknown; SecretAccessKey?: unknown; SessionToken?: unknown; Expiration?: unknown };
  try {
    c = JSON.parse(r.stdout) as typeof c;
  } catch {
    // Never echo the output: it is a credential document.
    return refuse(verb, "the pippen profile's credentials were not a credential_process document");
  }
  const str = (v: unknown): v is string => typeof v === "string" && v !== "";
  if (c?.Version !== 1 || !str(c.AccessKeyId) || !str(c.SecretAccessKey) || !str(c.SessionToken) || !str(c.Expiration))
    return refuse(verb, "the pippen profile's credentials lack a key, a session token or an expiration");
  const expMs = Date.parse(c.Expiration);
  if (Number.isNaN(expMs)) return refuse(verb, "the pippen profile's credential expiration is not a timestamp");
  const left = Math.floor((expMs - now()) / 60_000);
  if (left < MIN_ALCHEMY_CREDENTIAL_MINUTES)
    return refuse(verb, `the pippen profile's credentials expire in ${left} min (< ${MIN_ALCHEMY_CREDENTIAL_MINUTES}), and Alchemy cannot refresh them; renew the pippen lease if it caps them`);
  const out: Env = { ...env };
  for (const k of PROFILE_VARS) delete out[k];
  Object.assign(out, {
    AWS_ACCESS_KEY_ID: c.AccessKeyId,
    AWS_SECRET_ACCESS_KEY: c.SecretAccessKey,
    AWS_SESSION_TOKEN: c.SessionToken,
    AWS_CREDENTIAL_EXPIRATION: c.Expiration,
    // No fallback to the profile, a saved config, a credentials file or IMDS.
    AWS_CONFIG_FILE: "/dev/null",
    AWS_SHARED_CREDENTIALS_FILE: "/dev/null",
    AWS_EC2_METADATA_DISABLED: "true",
  });
  // The stack checks the result again; a miss here is a bug, not an operator error.
  const issue = sandboxAlchemyCredentialIssue(out, verb, now());
  if (issue) return refuse(verb, issue);
  return out;
}
