/** Deployment identity pins (AWS account, Supabase project ref, EC2 instance).
 * Always explicit operator configuration: this module has no default target and
 * no fallback, so an unset or malformed value fails closed in the caller.
 * Values are validated to a fixed alphabet before they are used in a pattern,
 * a role username or a host name, so no caller can inject regex or URL syntax.
 */
export const AWS_ACCOUNT_ID = /^[0-9]{12}$/;
export const SUPABASE_PROJECT_REF = /^[a-z0-9]{20}$/;
export const EC2_INSTANCE_ID = /^i-[0-9a-f]{17}$/;
export type DeployTargetFailure = "required" | "invalid";
/** Reads `env[name]` and checks it against `pattern`; calls `fail` when unset or invalid. */
export function deployTargetValue(
  env: Record<string, string | undefined>,
  name: string,
  pattern: RegExp,
  fail: (reason: DeployTargetFailure, name: string) => never,
): string {
  const value = env[name];
  if (value === undefined || value === "") return fail("required", name);
  if (!pattern.test(value)) return fail("invalid", name);
  return value;
}
/** Throws `{code}` errors for operator scripts (`<name>_required` / `<name>_invalid`). */
export function requiredSupabaseProjectRef(env: Record<string, string | undefined>, name = "SUPABASE_PROJECT_REF"): string {
  return deployTargetValue(env, name, SUPABASE_PROJECT_REF, (reason) => {
    throw Object.assign(new Error(`${name.toLowerCase()}_${reason}`), { code: `${name.toLowerCase()}_${reason}` });
  });
}
