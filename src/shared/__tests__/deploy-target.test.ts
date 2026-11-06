import { describe, expect, test } from "bun:test";
import { AWS_ACCOUNT_ID, EC2_INSTANCE_ID, SUPABASE_PROJECT_REF, deployTargetValue, requiredSupabaseProjectRef } from "../deploy-target.ts";
import { parseCliLogin } from "../supabase-cli-login.ts";

// Synthetic identities only: the AWS documentation account and placeholder refs.
const REF = "abcdefghijklmnopqrst";
const codeOf = (f: () => unknown) => { try { f(); } catch (e) { return (e as { code?: string }).code; } return "no_throw"; };
const fail = (reason: string, name: string): never => { throw Object.assign(new Error(reason), { code: `${name}:${reason}` }); };

describe("deployment identity configuration", () => {
  test("has no default: unset and empty values are refused", () => {
    for (const env of [{}, { X: "" }]) expect(codeOf(() => deployTargetValue(env, "X", AWS_ACCOUNT_ID, fail))).toBe("X:required");
    expect(codeOf(() => requiredSupabaseProjectRef({}))).toBe("supabase_project_ref_required");
    expect(codeOf(() => requiredSupabaseProjectRef({ SEED_REF: "" }, "SEED_REF"))).toBe("seed_ref_required");
  });
  test("returns exactly the configured value when it is well formed", () => {
    expect(deployTargetValue({ X: "111122223333" }, "X", AWS_ACCOUNT_ID, fail)).toBe("111122223333");
    expect(requiredSupabaseProjectRef({ SUPABASE_PROJECT_REF: REF })).toBe(REF);
    expect(deployTargetValue({ X: "i-0123456789abcdef0" }, "X", EC2_INSTANCE_ID, fail)).toBe("i-0123456789abcdef0");
  });
  test("refuses values that could widen a pattern, a username or a host name", () => {
    for (const v of ["11112222333", "1111222233334", ".*", "111122223333|.*", " 111122223333"]) expect(AWS_ACCOUNT_ID.test(v)).toBe(false);
    for (const v of ["abcdefghijklmnopqrs", "ABCDEFGHIJKLMNOPQRST", "abcdefghij.lmnopqrst", "abcdefghij/lmnopqrst", "abcdefghijklmnopqrst\n"]) expect(SUPABASE_PROJECT_REF.test(v)).toBe(false);
    for (const v of ["i-0123", "0123456789abcdef0", "i-0123456789ABCDEF0"]) expect(EC2_INSTANCE_ID.test(v)).toBe(false);
    expect(codeOf(() => requiredSupabaseProjectRef({ SUPABASE_PROJECT_REF: "not-a-ref" }))).toBe("supabase_project_ref_invalid");
  });
});

describe("Supabase CLI login parser uses the configured project ref", () => {
  const login = (ref: string) => [`export PGHOST='db.${ref}.supabase.co'`, "export PGPORT='5432'", "export PGUSER='cli_login_postgres'", "export PGPASSWORD='fake-unit-only-credential-value'", "export PGDATABASE='postgres'"].join("\n");
  test("accepts the CLI login for the configured project", () => expect(parseCliLogin(login(REF), REF).password).toBe("fake-unit-only-credential-value"));
  test("refuses a login for any other project", () => expect(codeOf(() => parseCliLogin(login("zyxwvutsrqponmlkjihg"), REF))).toBe("cli_target_pin"));
  test("refuses a missing or malformed configured ref before parsing", () => {
    for (const ref of ["", "db.evil", undefined as unknown as string]) expect(codeOf(() => parseCliLogin(login(REF), ref))).toBe("cli_target_config");
  });
});
