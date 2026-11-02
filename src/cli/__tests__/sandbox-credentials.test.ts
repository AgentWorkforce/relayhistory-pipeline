/**
 * bin/96bulls plan|up|down aws-proof-sandbox under `pippen sandbox exec`
 * (src/cli/sandbox-credentials.ts): static keys pass through; the default
 * credential_process profile is resolved once into the child env (>= 50 min
 * left), or refused with the --static-credentials command; nothing else is
 * touched.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { EXPORT_CREDENTIALS_ARGS, sandboxAlchemyEnv, type Env, type ResolveDeps } from "../sandbox-credentials.ts";

const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
const OTHER = "9a1c2b3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d";
const NOW = Date.parse("2026-11-01T12:00:00Z");
const CFG = "/run/user/1000/pippen-exec-Ab12Cd/config";
const PIPPEN_CONFIG = `[profile pippen-sandbox]\nregion = us-west-2\ncredential_process = "/usr/bin/node" "/opt/pippen-tools/bin/pippen.js" "sandbox" "credentials-process" "aws-proof" "--client-env" "/run/user/1000/pippen-exec-Ab12Cd/client.json"\n`;
const SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const TOKEN = "IQoJb3JpZ2luX2VjEXAMPLETOKEN";
const KEY_ID = "ASIAEXAMPLEEXAMPLE00";
const at = (min: number) => new Date(NOW + min * 60_000).toISOString();
const doc = (o: Record<string, unknown> = {}) => JSON.stringify({ Version: 1, AccessKeyId: KEY_ID, SecretAccessKey: SECRET, SessionToken: TOKEN, Expiration: at(59), ...o });

/** Default `pippen sandbox exec aws-proof -- ...` env (pippen-tools 327c263), plus the operator's 96bulls vars. */
const profileEnv = (o: Env = {}): Env => ({
  PATH: "/usr/bin:/bin", HOME: "/home/op", AWS_REGION: "us-west-2", AWS_DEFAULT_REGION: "us-west-2",
  AWS_CONFIG_FILE: CFG, AWS_PROFILE: "pippen-sandbox", AWS_SDK_LOAD_CONFIG: "1", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_EC2_METADATA_DISABLED: "true",
  PIPPEN_SANDBOX_AGENT_ID: "aws-proof", PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "361835732085",
  NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, NINETY_SIX_BULLS_CONFIRM_ACCOUNT: "361835732085",
  ...o,
});
const staticEnv = (o: Env = {}): Env =>
  profileEnv({ AWS_PROFILE: undefined, AWS_SDK_LOAD_CONFIG: undefined, AWS_CONFIG_FILE: "/dev/null", AWS_ACCESS_KEY_ID: KEY_ID, AWS_SECRET_ACCESS_KEY: SECRET, AWS_SESSION_TOKEN: TOKEN, AWS_CREDENTIAL_EXPIRATION: at(59), ...o });

function deps(out: { status?: number | null; stdout?: string; throws?: boolean } = {}, config = PIPPEN_CONFIG) {
  const calls: Array<{ args: string[]; env: Env }> = [];
  const d: ResolveDeps = {
    runAws: (args, env) => {
      calls.push({ args, env });
      if (out.throws) throw new Error("spawn aws ENOENT");
      return { status: out.status === undefined ? 0 : out.status, stdout: out.stdout ?? doc() };
    },
    now: () => NOW,
    readFile: (p) => {
      if (p !== CFG) throw new Error("ENOENT");
      return config;
    },
  };
  return { d, calls };
}
const errorOf = (f: () => unknown) => {
  try {
    f();
    return "accepted";
  } catch (e) {
    return (e as Error).message;
  }
};
const FIX = "pippen sandbox exec aws-proof --static-credentials -- bin/96bulls plan aws-proof-sandbox";

describe("other stages: untouched", () => {
  test("the env is returned as is and aws is never run", () => {
    for (const stage of ["aws-proof", "aws-sessions-dev", "aws-sessions-prod"]) {
      const { d, calls } = deps();
      const env = profileEnv({ AWS_PROFILE: "sessions-dev" });
      expect(sandboxAlchemyEnv("plan", stage, env, d)).toBe(env);
      expect(calls).toEqual([]);
    }
  });
});

describe("aws-proof-sandbox, static credentials (`exec --static-credentials`)", () => {
  test("are used as they are; nothing is resolved", () => {
    const { d, calls } = deps();
    const env = staticEnv();
    expect(sandboxAlchemyEnv("up", "aws-proof-sandbox", env, d)).toBe(env);
    expect(calls).toEqual([]);
  });
  test("mixed with a profile, partial, missing or short-lived: refused before anything runs", () => {
    const { d, calls } = deps();
    expect(errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", staticEnv({ AWS_PROFILE: "pippen-sandbox" }), d))).toMatch(/both static keys and a profile.*nothing was run$/);
    expect(errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", profileEnv({ AWS_ACCESS_KEY_ID: KEY_ID }), d))).toMatch(/both static keys and a profile/);
    expect(errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", staticEnv({ AWS_SESSION_TOKEN: undefined }), d))).toMatch(/missing AWS_SESSION_TOKEN/);
    expect(errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", profileEnv({ AWS_PROFILE: undefined }), d))).toContain(`no AWS credentials in the environment; run it as \`${FIX}\``);
    expect(errorOf(() => sandboxAlchemyEnv("up", "aws-proof-sandbox", staticEnv({ AWS_CREDENTIAL_EXPIRATION: at(30) }), d))).toMatch(/expire in 30 min \(< 50\)/);
    expect(calls).toEqual([]);
  });
});

describe("aws-proof-sandbox, `pippen sandbox exec`'s default credential_process profile", () => {
  test("is resolved once, with exactly the export-credentials call, into a new env with only static keys", () => {
    const { d, calls } = deps();
    const env = profileEnv();
    const before = JSON.stringify(env);
    const out = sandboxAlchemyEnv("up", "aws-proof-sandbox", env, d);
    expect(calls.length).toBe(1);
    expect(calls[0]!.args).toEqual(["configure", "export-credentials", "--profile", "pippen-sandbox", "--format", "process"]);
    expect(calls[0]!.args).toEqual([...EXPORT_CREDENTIALS_ARGS]);
    // The profile env runs the export; the keys appear nowhere in argv.
    expect(calls[0]!.env).toBe(env);
    expect(calls[0]!.args.join(" ")).not.toContain(SECRET);
    expect(JSON.stringify(env)).toBe(before);
    expect(out).not.toBe(env);
    expect(out).toMatchObject({
      AWS_ACCESS_KEY_ID: KEY_ID, AWS_SECRET_ACCESS_KEY: SECRET, AWS_SESSION_TOKEN: TOKEN, AWS_CREDENTIAL_EXPIRATION: at(59),
      AWS_CONFIG_FILE: "/dev/null", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_EC2_METADATA_DISABLED: "true",
      AWS_REGION: "us-west-2", PIPPEN_SANDBOX_SESSION_ID: SESSION, NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION,
    });
    expect("AWS_PROFILE" in out).toBe(false);
    expect("AWS_DEFAULT_PROFILE" in out).toBe(false);
  });

  test(`needs at least 50 minutes left (Alchemy cannot refresh)`, () => {
    expect(errorOf(() => sandboxAlchemyEnv("up", "aws-proof-sandbox", profileEnv(), deps({ stdout: doc({ Expiration: at(50) }) }).d))).toBe("accepted");
    const m = errorOf(() => sandboxAlchemyEnv("up", "aws-proof-sandbox", profileEnv(), deps({ stdout: doc({ Expiration: at(49) }) }).d));
    expect(m).toMatch(/expire in 49 min \(< 50\), and Alchemy cannot refresh them/);
    expect(m).toContain("pippen sandbox exec aws-proof --static-credentials -- bin/96bulls up aws-proof-sandbox");
    expect(errorOf(() => sandboxAlchemyEnv("up", "aws-proof-sandbox", profileEnv(), deps({ stdout: doc({ Expiration: "tomorrow" }) }).d))).toMatch(/not a timestamp/);
  });

  test("refuses, with the --static-credentials command, when it cannot be resolved; never echoing the output", () => {
    const cases: Array<[Parameters<typeof deps>[0], RegExp]> = [
      [{ status: 255 }, /failed \(exit 255\)/],
      [{ throws: true }, /could not run \(AWS CLI v2\.9\+ is required\)/],
      [{ stdout: `not json ${SECRET}` }, /not a credential_process document/],
      [{ stdout: doc({ SessionToken: undefined }) }, /lack a key, a session token or an expiration/],
      [{ stdout: doc({ Expiration: undefined }) }, /lack a key, a session token or an expiration/],
      [{ stdout: doc({ Version: 2 }) }, /lack a key/],
      [{ stdout: doc({ AccessKeyId: "" }) }, /lack a key/],
    ];
    for (const [o, re] of cases) {
      const m = errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", profileEnv(), deps(o).d));
      expect(m).toMatch(re);
      expect(m).toContain(`Run it as \`${FIX}\`; nothing was run`);
      expect(m).not.toContain(SECRET);
      expect(m).not.toContain(TOKEN);
      expect(m).not.toContain(KEY_ID);
    }
  });

  test("only pippen's own profile for this lease is resolved; any other profile is refused without running aws", () => {
    const refused: Array<[Env, string, RegExp]> = [
      [profileEnv({ AWS_PROFILE: "default" }), PIPPEN_CONFIG, /AWS_PROFILE is not pippen-sandbox/],
      [profileEnv({ AWS_PROFILE: undefined, AWS_DEFAULT_PROFILE: "pippen-sandbox" }), PIPPEN_CONFIG, /AWS_DEFAULT_PROFILE is set/],
      [profileEnv({ AWS_SHARED_CREDENTIALS_FILE: undefined }), PIPPEN_CONFIG, /AWS_SHARED_CREDENTIALS_FILE must be \/dev\/null/],
      [profileEnv({ AWS_CONFIG_FILE: "/home/op/.aws/config" }), PIPPEN_CONFIG, /AWS_CONFIG_FILE unreadable/],
      [profileEnv(), "[profile pippen-sandbox]\nregion = us-west-2\ncredential_process = /bin/sh -c 'cat creds'\n", /credential_process is not `pippen sandbox credentials-process aws-proof`/],
      [profileEnv(), `${PIPPEN_CONFIG}role_arn = arn:aws:iam::1:role/x\n`, /sets role_arn/],
      [profileEnv({ PIPPEN_SANDBOX_SESSION_ID: OTHER }), PIPPEN_CONFIG, /is not --pippen-session/],
      [profileEnv({ PIPPEN_SANDBOX_ACCOUNT_ID: "500268966557" }), PIPPEN_CONFIG, /not the sandbox account/],
    ];
    for (const [env, config, re] of refused) {
      const { d, calls } = deps({}, config);
      const m = errorOf(() => sandboxAlchemyEnv("plan", "aws-proof-sandbox", env, d));
      expect(m).toMatch(/^stage aws-proof-sandbox: Alchemy cannot use this AWS profile \(/);
      expect(m).toMatch(re);
      expect(m).toContain(FIX);
      expect(calls).toEqual([]);
    }
  });
});

describe("bin/96bulls wiring", () => {
  const cli = readFileSync(new URL("../main.ts", import.meta.url), "utf8");
  const runner = cli.slice(cli.indexOf("async function aws("), cli.indexOf("async function main()"));
  test("resolves before the capture, the verify and the Alchemy run, and every one of them gets the resolved env", () => {
    const at = runner.indexOf("baseEnv = sandboxAlchemyEnv(verb, stage, process.env, {");
    expect(at).toBeGreaterThan(0);
    expect(at).toBeLessThan(runner.indexOf("capturePlan(ROOT, stage, baseEnv)"));
    expect(at).toBeLessThan(runner.indexOf("verifyCapturedPlan(ROOT, stage, baseEnv)"));
    expect(at).toBeLessThan(runner.indexOf('sh("bunx", inv.args, { env: inv.env, baseEnv });'));
    // Inside the try: a refusal dies before anything runs.
    expect(at).toBeLessThan(runner.indexOf("} catch (err) {"));
  });
  test("the export's stdout is captured, never inherited", () => {
    expect(runner).toContain('stdio: ["ignore", "pipe", "inherit"]');
    expect(cli).toContain("env: { ...(opts.baseEnv ?? process.env), ...opts.env },");
  });
  test("capture spawns everything with the env it was given, not process.env", () => {
    const io = readFileSync(new URL("../aws-provenance-io.ts", import.meta.url), "utf8");
    const capture = io.slice(io.indexOf("export function capturePlan("), io.indexOf("export function verifyCapturedPlan("));
    expect(capture).not.toContain("process.env");
    expect(capture).toContain("env: { ...env, ...inv.env, NINETY_SIX_BULLS_STATE_SNAPSHOT: snapPath }");
    expect(io).toContain("env: baseEnv, maxBuffer");
  });
});
