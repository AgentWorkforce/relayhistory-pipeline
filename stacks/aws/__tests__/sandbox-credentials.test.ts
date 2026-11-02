/**
 * aws-proof-sandbox credentials for Alchemy (sandbox-credentials.ts): static env
 * keys only, never a profile, never both; the stack refuses anything else
 * before any provider or state layer runs.
 */
import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  assertSandboxAlchemyCredentials, MIN_ALCHEMY_CREDENTIAL_MINUTES, sandboxAlchemyCredentialIssue, sandboxCredentialShape, staticExecCommand,
} from "../sandbox-credentials.ts";
import { pippenProofSandboxCallerArn } from "../sandbox-rules.ts";
import { stageFromArgv } from "../state-bucket.ts";

const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
const NOW = Date.parse("2026-11-01T12:00:00Z");
const KEYS = { AWS_ACCESS_KEY_ID: "ASIAEXAMPLEEXAMPLE00", AWS_SECRET_ACCESS_KEY: "s".repeat(40), AWS_SESSION_TOKEN: "tok" };
/** The child env `pippen sandbox exec aws-proof --static-credentials -- ...` gives (pippen-tools 327c263 sandboxChildEnv). */
const staticExecEnv = (o: Record<string, string | undefined> = {}): Record<string, string | undefined> => ({
  PATH: "/usr/bin:/bin", AWS_REGION: "us-west-2", AWS_DEFAULT_REGION: "us-west-2", ...KEYS,
  AWS_CREDENTIAL_EXPIRATION: "2026-11-01T13:00:00Z", AWS_CONFIG_FILE: "/dev/null", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_EC2_METADATA_DISABLED: "true",
  PIPPEN_SANDBOX_AGENT_ID: "aws-proof", PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "361835732085", PIPPEN_SANDBOX_CREDENTIALS_EXPIRE_AT: "2026-11-01T13:00:00Z",
  NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION,
  ...o,
});
/** The default (refreshing) `pippen sandbox exec` env: a credential_process profile and no keys. */
const profileExecEnv = (o: Record<string, string | undefined> = {}) =>
  staticExecEnv({ AWS_ACCESS_KEY_ID: undefined, AWS_SECRET_ACCESS_KEY: undefined, AWS_SESSION_TOKEN: undefined, AWS_CREDENTIAL_EXPIRATION: undefined, AWS_CONFIG_FILE: "/run/user/1000/pippen-exec-Ab12Cd/config", AWS_PROFILE: "pippen-sandbox", AWS_SDK_LOAD_CONFIG: "1", ...o });
const issue = (env: Record<string, string | undefined>) => sandboxAlchemyCredentialIssue(env, "plan", NOW);

describe("aws-proof-sandbox: which credentials Alchemy can use", () => {
  it("detects the shape: static, profile, mixed, partial, none", () => {
    expect(sandboxCredentialShape(staticExecEnv())).toBe("static");
    expect(sandboxCredentialShape(profileExecEnv())).toBe("profile");
    expect(sandboxCredentialShape(profileExecEnv({ AWS_PROFILE: undefined, AWS_DEFAULT_PROFILE: "x" }))).toBe("profile");
    expect(sandboxCredentialShape(staticExecEnv({ AWS_PROFILE: "pippen-sandbox" }))).toBe("mixed");
    expect(sandboxCredentialShape(profileExecEnv({ AWS_ACCESS_KEY_ID: "AKIA" }))).toBe("mixed");
    expect(sandboxCredentialShape(staticExecEnv({ AWS_SESSION_TOKEN: undefined }))).toBe("partial");
    expect(sandboxCredentialShape(staticExecEnv({ AWS_SESSION_TOKEN: "" }))).toBe("partial");
    expect(sandboxCredentialShape({})).toBe("none");
    expect(sandboxCredentialShape({ AWS_PROFILE: "" })).toBe("none");
  });

  it("accepts `pippen sandbox exec --static-credentials`'s env, and its caller pin is this lease's agent-session", () => {
    expect(issue(staticExecEnv())).toBeUndefined();
    expect(issue(staticExecEnv({ AWS_CREDENTIAL_EXPIRATION: undefined }))).toBeUndefined();
    expect(pippenProofSandboxCallerArn(staticExecEnv())).toBe(`arn:aws:sts::361835732085:assumed-role/agent-session/agent-${SESSION}`);
    expect(() => pippenProofSandboxCallerArn(staticExecEnv({ PIPPEN_SANDBOX_SESSION_ID: "9a1c2b3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d" }))).toThrow(/mismatched lease/);
  });

  it("refuses the credential_process profile with the --static-credentials command, not Alchemy's 'profile default' error", () => {
    const m = issue(profileExecEnv()) ?? "accepted";
    expect(m).toContain("Alchemy cannot use an AWS profile or credential_process");
    expect(m).toContain("pippen sandbox exec aws-proof --static-credentials -- bin/96bulls plan aws-proof-sandbox");
    expect(m).toContain("not configured in profile 'default'");
    expect(() => assertSandboxAlchemyCredentials(profileExecEnv(), "up", NOW)).toThrow(/^96bulls: stage aws-proof-sandbox: .*--static-credentials -- bin\/96bulls up aws-proof-sandbox.*Nothing was changed\.$/);
  });

  it("refuses mixing a profile with static keys, partial keys and no credentials", () => {
    expect(issue(staticExecEnv({ AWS_PROFILE: "pippen-sandbox" }))).toMatch(/both static keys and a profile \(AWS_PROFILE\); give exactly one/);
    expect(issue(staticExecEnv({ AWS_DEFAULT_PROFILE: "default" }))).toMatch(/both static keys and a profile \(AWS_DEFAULT_PROFILE\)/);
    expect(issue(staticExecEnv({ AWS_SESSION_TOKEN: undefined }))).toMatch(/need all of .*missing AWS_SESSION_TOKEN/);
    expect(issue({ AWS_REGION: "us-west-2" })).toMatch(/^no AWS credentials in the environment; run it as `pippen sandbox exec aws-proof --static-credentials/);
    for (const m of [issue(staticExecEnv({ AWS_PROFILE: "p" })), issue(staticExecEnv({ AWS_SESSION_TOKEN: undefined }))]) {
      expect(m).not.toContain(KEYS.AWS_SECRET_ACCESS_KEY);
      expect(m).not.toContain(KEYS.AWS_ACCESS_KEY_ID);
    }
  });

  it(`refuses credentials with less than ${MIN_ALCHEMY_CREDENTIAL_MINUTES} minutes left`, () => {
    const at = (min: number) => new Date(NOW + min * 60_000).toISOString();
    expect(issue(staticExecEnv({ AWS_CREDENTIAL_EXPIRATION: at(50) }))).toBeUndefined();
    expect(issue(staticExecEnv({ AWS_CREDENTIAL_EXPIRATION: at(49.9) }))).toMatch(/expire in 49 min \(< 50\)/);
    expect(issue(staticExecEnv({ AWS_CREDENTIAL_EXPIRATION: at(-5) }))).toMatch(/expire in -5 min/);
    expect(issue(staticExecEnv({ AWS_CREDENTIAL_EXPIRATION: "soon" }))).toMatch(/not a timestamp/);
  });

  it("names the command for each verb", () => {
    expect(staticExecCommand("down")).toBe("pippen sandbox exec aws-proof --static-credentials -- bin/96bulls down aws-proof-sandbox");
  });
});

describe("the stack checks it itself, before any provider or state layer", () => {
  const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
  it("asserts at module load on stage aws-proof-sandbox, however Alchemy was started", () => {
    const guard = stack.indexOf("if (stageFromArgv(process.argv) === PIPPEN_PROOF_SANDBOX.stage) assertSandboxAlchemyCredentials(process.env);");
    expect(guard).toBeGreaterThan(0);
    expect(guard).toBeLessThan(stack.indexOf("const stateTarget = stateTargetFromArgv("));
    expect(guard).toBeLessThan(stack.indexOf('export default Alchemy.Stack('));
  });
  it("reads the stage from Alchemy's argv in either form", () => {
    expect(stageFromArgv(["bun", "alchemy", "plan", "--stage", "aws-proof-sandbox"])).toBe("aws-proof-sandbox");
    expect(stageFromArgv(["alchemy", "deploy", "--stage=aws-proof-sandbox"])).toBe("aws-proof-sandbox");
    expect(stageFromArgv(["alchemy", "deploy", "--stage", "local"])).toBeUndefined();
    expect(stageFromArgv(["alchemy", "deploy"])).toBeUndefined();
  });
});
