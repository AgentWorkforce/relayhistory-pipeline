/**
 * Unblocking the Pippen-sandbox proof: the 7d7229b services image is approved
 * (versions.lock.json + PROOF_PROBE_IMAGES, still equal to the plan's digest),
 * and the harness accepts `pippen sandbox exec`'s refreshing credential_process
 * profile on the sandbox target only. The sessions-dev proof target is
 * unchanged.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { observeIdentity } from "../cli.ts";
import {
  credentialIssue, PIPPEN_EXEC_PROFILE, pippenProfileConfigIssue, PROOF_CLUSTER_ARN, proofKubeconfig, resolveToolCredentials, STATIC_CREDENTIALS, toolEnv, type ToolCredentials,
} from "../cluster-binding.ts";
import { sandboxConfig } from "../config.ts";
import { Runner } from "../exec.ts";
import { fenceFor, PROOF_CONTEXT, PROOF_PROBE_IMAGES, probeImagePlanIssue, proofConfigIssues, SANDBOX_CONTEXT } from "../fence.ts";
import { PIPPEN_SANDBOX_PROOF_TARGET, SESSIONS_DEV_PROOF_TARGET } from "../guard.ts";
import { probePod } from "../ops.ts";

const SBX = PIPPEN_SANDBOX_PROOF_TARGET;
const DEV = SESSIONS_DEV_PROOF_TARGET;
const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
const OTHER_SESSION = "9a1c2b3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d";
const CALLER = `arn:aws:sts::361835732085:assumed-role/agent-session/agent-${SESSION}`;
const CFG = "/run/user/1000/pippen-exec-Ab12Cd/config";
/** Exactly what pippen-tools 327c263 src/cli/exec.ts writes (each argument POSIX double-quoted). */
const PIPPEN_CONFIG = `[profile pippen-sandbox]\nregion = us-west-2\ncredential_process = "/usr/bin/node" "/opt/pippen-tools/bin/pippen.js" "sandbox" "credentials-process" "aws-proof" "--client-env" "/run/user/1000/pippen-exec-Ab12Cd/client.json"\n`;
/** The child env `pippen sandbox exec aws-proof -- ...` gives in its default (refreshing) mode. */
const execEnv = (o: Record<string, string | undefined> = {}): Record<string, string | undefined> => ({
  PATH: "/usr/bin:/bin", HOME: "/home/op", AWS_REGION: "us-west-2", AWS_DEFAULT_REGION: "us-west-2",
  AWS_CONFIG_FILE: CFG, AWS_PROFILE: "pippen-sandbox", AWS_SDK_LOAD_CONFIG: "1", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_EC2_METADATA_DISABLED: "true",
  PIPPEN_SANDBOX_AGENT_ID: "aws-proof", PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "361835732085",
  ...o,
});
const STATIC_KEYS = { AWS_ACCESS_KEY_ID: "ASIAEXAMPLEEXAMPLE00", AWS_SECRET_ACCESS_KEY: "s".repeat(40), AWS_SESSION_TOKEN: "tok" };
const reader = (text: string) => (p: string) => {
  if (p !== CFG) throw new Error("ENOENT");
  return text;
};
const resolveSbx = (env: Record<string, string | undefined>, text = PIPPEN_CONFIG, session: string | undefined = SESSION) =>
  resolveToolCredentials(env, SBX, { ...(session ? { pippenSession: session } : {}), readFile: reader(text) });
const errorOf = (r: ToolCredentials | { error: string }) => ("error" in r ? r.error : "accepted");
const PROFILE: ToolCredentials = { kind: "pippen-profile", configFile: CFG };
const D = { arn: PROOF_CLUSTER_ARN, endpoint: "https://ABC.gr7.us-west-2.eks.amazonaws.com", caData: "Y2E=", createdAtMs: 0 };

describe("services image 7d7229b: approved in versions.lock.json and the probe fence", () => {
  const lock = JSON.parse(readFileSync(new URL("../../../../versions.lock.json", import.meta.url), "utf8")) as {
    images: { "services-aws": { approved: Array<{ repository: string; digest: string; sourceRevision: string; generation?: string; ghcr?: { ref: string; digest: string; run: string }; note: string }> } };
  };
  const approved = lock.images["services-aws"].approved;
  const ECR = "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services";
  const DIGEST = "sha256:5bb6075240afa02a1b1797a81c07bffa029954eac7803d702ecba20db35d4db9";
  test("the entry parses with its provenance, and the earlier entries are kept", () => {
    expect(approved.map((a) => a.sourceRevision)).toEqual(["ffb7ef0f684a92c129bbf14c2da2066fbe6fe6b2", "2fb88cdaca3ef42f5eea42050d43448c883ac5e4", "7d7229bb31c121ecb98cd4e35c248fc2cf5669ad"]);
    const e = approved.find((a) => a.digest === DIGEST)!;
    expect(e).toMatchObject({ repository: ECR, sourceRevision: "7d7229bb31c121ecb98cd4e35c248fc2cf5669ad", generation: "eff8b3be46565b57ad8fa5e4a066dbb6d1fe608313d9c1680bfbcd484add259b" });
    expect(e.ghcr).toEqual({ ref: "ghcr.io/agentworkforce/96bulls-services", digest: "sha256:6f22ea17dff336e1431664542759fd27958649388b90a0b2f0881883caa7c6b2", run: "41375745173" });
    for (const s of ["41375747474", "41375745173", "sha-7d7229bb31c121ecb98cd4e35c248fc2cf5669ad", "Khaliq 2026-11-01"]) expect(e.note).toContain(s);
    for (const a of approved) {
      expect(a.digest).toMatch(/^sha256:[0-9a-f]{64}$/);
      expect(a.sourceRevision).toMatch(/^[0-9a-f]{40}$/);
    }
  });
  test("the stack accepts the 7d7229b image on the sandbox stage and still refuses an unlisted one", async () => {
    const { validateAwsInputs } = await import("../../../../stacks/aws/inputs.ts");
    const example = JSON.parse(readFileSync(new URL("../../../../stacks/aws/examples/aws-proof-sandbox.inputs.example.json", import.meta.url), "utf8"));
    const withImage = (digest: string, rev: string) => {
      const i = structuredClone(example);
      const fill = (o: unknown): unknown => (typeof o === "string" ? o.replace(/<[^>]+>/g, "x") : Array.isArray(o) ? o.map(fill) : o && typeof o === "object" ? Object.fromEntries(Object.entries(o).map(([k, v]) => [k, fill(v)])) : o);
      const f = fill(i) as typeof example;
      f.eks.imageDigest = digest;
      f.eks.imageSourceRevision = rev;
      return f;
    };
    const msg = (f: () => void) => {
      try {
        f();
        return "ok";
      } catch (e) {
        return (e as Error).message;
      }
    };
    expect(msg(() => validateAwsInputs("aws-proof-sandbox", withImage(DIGEST, "7d7229bb31c121ecb98cd4e35c248fc2cf5669ad"), approved))).not.toMatch(/not an approved services-aws entry/);
    expect(msg(() => validateAwsInputs("aws-proof-sandbox", withImage(DIGEST, "2fb88cdaca3ef42f5eea42050d43448c883ac5e4"), approved))).toMatch(/not an approved services-aws entry/);
  });
  test("every probe image is an approved entry; the new one is allowlisted", () => {
    expect(PROOF_PROBE_IMAGES).toEqual([
      `${ECR}@sha256:59d9468551ded11f3c6f5d36558ab7ef87c3e50e99772de352cc309823cb00e0`,
      `${ECR}@${DIGEST}`,
    ]);
    for (const img of PROOF_PROBE_IMAGES) expect(approved.map((a) => `${a.repository}@${a.digest}`)).toContain(img);
    expect(proofConfigIssues({ ...sandboxConfig(), probeImage: `${ECR}@${DIGEST}` })).not.toContainEqual(expect.stringMatching(/probeImage/));
    expect(proofConfigIssues({ ...sandboxConfig(), probeImage: `${ECR}@sha256:${"a".repeat(64)}` })).toContainEqual(expect.stringMatching(/probeImage must be one of the allowlisted/));
    expect(() => probePod("search", `${ECR}@${DIGEST}`)).not.toThrow();
  });
  test("the probe image must still equal the stage plan's services digest", () => {
    expect(probeImagePlanIssue(`${ECR}@${DIGEST}`, DIGEST)).toBeUndefined();
    expect(probeImagePlanIssue(undefined, DIGEST)).toBeUndefined();
    // Both allowlisted, but a run whose plan is one image may not probe with the other.
    expect(probeImagePlanIssue(`${ECR}@${DIGEST}`, "sha256:59d9468551ded11f3c6f5d36558ab7ef87c3e50e99772de352cc309823cb00e0")).toBe("probeImage is not the stage plan's services image");
    expect(probeImagePlanIssue(PROOF_PROBE_IMAGES[0], DIGEST)).toBe("probeImage is not the stage plan's services image");
    const cli = readFileSync(new URL("../cli.ts", import.meta.url), "utf8");
    expect(cli).toContain("const probeIssue = stagePlan ? probeImagePlanIssue(cfg.probeImage, stagePlan.servicesDigest) : undefined;");
  });
});

describe("sessions-dev proof target: credential handling is unchanged", () => {
  const cases: Array<Record<string, string | undefined>> = [
    {},
    STATIC_KEYS,
    { AWS_ACCESS_KEY_ID: "a" },
    { AWS_PROFILE: "sessions-dev" },
    execEnv(),
    { ...execEnv(), ...STATIC_KEYS },
    { AWS_PROFILE: "pippen-sandbox", AWS_CONFIG_FILE: CFG, AWS_SHARED_CREDENTIALS_FILE: "/dev/null" },
  ];
  test("resolveToolCredentials is exactly credentialIssue (a pippen exec profile is refused there)", () => {
    for (const env of cases) {
      const issue = credentialIssue(env);
      const r = resolveToolCredentials(env, DEV, { pippenSession: SESSION, readFile: reader(PIPPEN_CONFIG) });
      expect(r).toEqual(issue ? { error: issue } : STATIC_CREDENTIALS);
    }
    expect(errorOf(resolveToolCredentials(execEnv(), DEV, { readFile: reader(PIPPEN_CONFIG) }))).toMatch(/^AWS credentials must be explicit env vars/);
  });
  test("toolEnv and the kubeconfig are byte-identical with the default (static) credentials", () => {
    for (const env of cases) {
      expect(JSON.stringify(toolEnv(env, "/run/k.json", STATIC_CREDENTIALS))).toBe(JSON.stringify(toolEnv(env, "/run/k.json")));
      expect(toolEnv(env, undefined)).not.toHaveProperty("AWS_PROFILE");
      expect(toolEnv(env, undefined).AWS_CONFIG_FILE).toBe("/dev/null");
    }
    expect(proofKubeconfig(D, PROOF_CONTEXT, DEV, STATIC_CREDENTIALS)).toBe(proofKubeconfig(D, PROOF_CONTEXT));
    expect(JSON.stringify(JSON.parse(proofKubeconfig(D, PROOF_CONTEXT)).users[0].user.exec.env)).toBe(
      '[{"name":"AWS_CONFIG_FILE","value":"/dev/null"},{"name":"AWS_SHARED_CREDENTIALS_FILE","value":"/dev/null"},{"name":"AWS_STS_REGIONAL_ENDPOINTS","value":"regional"},{"name":"AWS_REGION","value":"us-west-2"}]',
    );
    const r = new Runner({ mode: "dry-run", identity: { confirmLive: false, clusterName: DEV.clusterName, kubeContext: PROOF_CONTEXT }, fence: fenceFor("proof"), baseEnv: { ...execEnv(), ...STATIC_KEYS } });
    expect(r.env()).toEqual(toolEnv({ ...execEnv(), ...STATIC_KEYS }, undefined));
    expect(r.env()).toMatchObject({ AWS_CONFIG_FILE: "/dev/null", AWS_ACCESS_KEY_ID: STATIC_KEYS.AWS_ACCESS_KEY_ID });
  });
});

describe("pippen sandbox target: accepts `pippen sandbox exec`'s refreshing profile", () => {
  test("the exact exec env and config are accepted", () => {
    expect(resolveSbx(execEnv())).toEqual(PROFILE);
    // Without the PIPPEN_* markers (e.g. a wrapper that drops them) the profile itself still has to validate, and does.
    expect(resolveSbx(execEnv({ PIPPEN_SANDBOX_SESSION_ID: undefined, PIPPEN_SANDBOX_ACCOUNT_ID: undefined }))).toEqual(PROFILE);
    // A standalone pippen binary, single-quoted, and comments are fine.
    expect(pippenProfileConfigIssue(`# pippen\n[profile pippen-sandbox]\nregion=us-west-2\ncredential_process = '/usr/local/bin/pippen' 'sandbox' 'credentials-process' 'aws-proof' '--client-env' '/tmp/x/client.json'\n`)).toBeUndefined();
  });
  test("a real file written like pippen's is read and accepted", () => {
    const dir = mkdtempSync(join(tmpdir(), "pippen-exec-"));
    const cfg = join(dir, "config");
    writeFileSync(cfg, PIPPEN_CONFIG, { mode: 0o600 });
    expect(resolveToolCredentials(execEnv({ AWS_CONFIG_FILE: cfg }), SBX, { pippenSession: SESSION })).toEqual({ kind: "pippen-profile", configFile: cfg });
    expect(errorOf(resolveToolCredentials(execEnv({ AWS_CONFIG_FILE: join(dir, "missing") }), SBX, { pippenSession: SESSION }))).toBe("AWS_CONFIG_FILE unreadable");
  });
  test("--static-credentials mode (and exported keys) still work on the sandbox", () => {
    const staticExec = { ...execEnv({ AWS_CONFIG_FILE: "/dev/null", AWS_PROFILE: undefined, AWS_SDK_LOAD_CONFIG: undefined }), ...STATIC_KEYS };
    expect(resolveSbx(staticExec)).toEqual(STATIC_CREDENTIALS);
    expect(resolveSbx(STATIC_KEYS)).toEqual(STATIC_CREDENTIALS);
  });
  test("every child tool inherits the profile so credential_process refreshes: aws, kubectl (aws eks get-token), helm", () => {
    const env = toolEnv({ ...execEnv(), KUBECONFIG: "/home/op/.kube/config", AWS_ENDPOINT_URL: "https://attacker.example", AWS_CA_BUNDLE: "/tmp/evil.pem" }, "/run/k.json", PROFILE);
    expect(env).toEqual({
      PATH: "/usr/bin:/bin", HOME: "/home/op",
      AWS_CONFIG_FILE: CFG, AWS_PROFILE: PIPPEN_EXEC_PROFILE, AWS_SDK_LOAD_CONFIG: "1", AWS_SHARED_CREDENTIALS_FILE: "/dev/null",
      AWS_REGION: "us-west-2", AWS_DEFAULT_REGION: "us-west-2", AWS_STS_REGIONAL_ENDPOINTS: "regional", AWS_USE_FIPS_ENDPOINT: "false", AWS_USE_DUALSTACK_ENDPOINT: "false", AWS_EC2_METADATA_DISABLED: "true", AWS_PAGER: "",
      KUBECONFIG: "/run/k.json",
    });
    // Static keys never ride along in profile mode, even if something put them in the base env.
    expect(toolEnv({ ...execEnv(), ...STATIC_KEYS }, undefined, PROFILE)).not.toHaveProperty("AWS_ACCESS_KEY_ID");
    const exec = JSON.parse(proofKubeconfig({ ...D, arn: SANDBOX_CONTEXT }, SANDBOX_CONTEXT, SBX, PROFILE)).users[0].user.exec;
    expect(exec.command).toBe("aws");
    expect(exec.args).toEqual(["eks", "get-token", "--cluster-name", "96bulls-aws-proof-sandbox", "--region", "us-west-2", "--output", "json"]);
    expect(Object.fromEntries((exec.env as Array<{ name: string; value: string }>).map((e) => [e.name, e.value]))).toEqual({
      AWS_CONFIG_FILE: CFG, AWS_PROFILE: "pippen-sandbox", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_STS_REGIONAL_ENDPOINTS: "regional", AWS_REGION: "us-west-2",
    });
    const r = new Runner({
      mode: "read-only", identity: { confirmLive: true, clusterName: SBX.clusterName, kubeContext: SANDBOX_CONTEXT }, fence: fenceFor("pippen-sandbox"),
      baseEnv: execEnv(), credentials: PROFILE, kubeconfigPath: "/run/k.json",
    });
    expect(r.env()).toMatchObject({ AWS_CONFIG_FILE: CFG, AWS_PROFILE: "pippen-sandbox", AWS_SDK_LOAD_CONFIG: "1", KUBECONFIG: "/run/k.json" });
  });
  test("the caller pin is unchanged: the profile's identity must still be agent-<--pippen-session>", () => {
    const fake = (arn: string) => (argv: string[]) => {
      if (argv.includes("Account")) return { stdout: "361835732085\n", status: 0 };
      if (argv.includes("Arn")) return { stdout: `${arn}\n`, status: 0 };
      return { stdout: "", status: 1 };
    };
    expect(observeIdentity(sandboxConfig(), fake(`arn:aws:sts::361835732085:assumed-role/agent-session/agent-${OTHER_SESSION}`), SBX, SESSION).bindError).toBe("caller is not the pippen agent-session role session for --pippen-session");
    expect(observeIdentity(sandboxConfig(), fake(CALLER), SBX, SESSION).bindError).toBe("describe-cluster failed");
  });
});

describe("pippen sandbox target: the refusal matrix", () => {
  const refused: Array<[string, Record<string, string | undefined>, string | undefined, RegExp]> = [
    ["no credentials at all", { PATH: "/bin" }, undefined, /must be explicit env vars.*pippen sandbox exec aws-proof/],
    ["ambient default profile", execEnv({ AWS_PROFILE: "default" }), undefined, /AWS_PROFILE is not pippen-sandbox/],
    ["a saved operator profile", execEnv({ AWS_PROFILE: "sessions-dev" }), undefined, /AWS_PROFILE is not pippen-sandbox/],
    ["AWS_DEFAULT_PROFILE alone", { AWS_DEFAULT_PROFILE: "pippen-sandbox", AWS_CONFIG_FILE: CFG, AWS_SHARED_CREDENTIALS_FILE: "/dev/null" }, undefined, /AWS_DEFAULT_PROFILE is set/],
    ["AWS_DEFAULT_PROFILE beside the exec profile", execEnv({ AWS_DEFAULT_PROFILE: "prod" }), undefined, /AWS_DEFAULT_PROFILE is set/],
    ["static keys mixed with the profile", { ...execEnv(), ...STATIC_KEYS }, undefined, /both static keys and a profile/],
    ["a lone session token mixed with the profile", execEnv({ AWS_SESSION_TOKEN: "tok" }), undefined, /both static keys and a profile/],
    ["shared credentials file not /dev/null", execEnv({ AWS_SHARED_CREDENTIALS_FILE: "/home/op/.aws/credentials" }), undefined, /AWS_SHARED_CREDENTIALS_FILE must be \/dev\/null/],
    ["shared credentials file unset (falls back to ~/.aws/credentials)", execEnv({ AWS_SHARED_CREDENTIALS_FILE: undefined }), undefined, /AWS_SHARED_CREDENTIALS_FILE must be \/dev\/null/],
    ["no config file (falls back to ~/.aws/config)", execEnv({ AWS_CONFIG_FILE: undefined }), undefined, /AWS_CONFIG_FILE must be the absolute path/],
    ["config file /dev/null with a profile", execEnv({ AWS_CONFIG_FILE: "/dev/null" }), undefined, /AWS_CONFIG_FILE must be the absolute path/],
    ["relative config file", execEnv({ AWS_CONFIG_FILE: "config" }), undefined, /AWS_CONFIG_FILE must be the absolute path/],
    ["unreadable config file", execEnv({ AWS_CONFIG_FILE: "/nonexistent/config" }), undefined, /AWS_CONFIG_FILE unreadable/],
    ["an SSO profile", execEnv(), "[profile pippen-sandbox]\nsso_session = corp\nsso_account_id = 361835732085\nsso_role_name = Admin\nregion = us-west-2\n", /SSO profile/],
    ["an sso-session section", execEnv(), `${PIPPEN_CONFIG}[sso-session corp]\nsso_start_url = https://x.awsapps.com/start\n`, /exactly one section/],
    ["a default section beside the profile", execEnv(), `[default]\nregion = eu-west-1\n${PIPPEN_CONFIG}`, /exactly one section/],
    ["a role chain", execEnv(), `${PIPPEN_CONFIG}role_arn = arn:aws:iam::361835732085:role/Admin\nsource_profile = default\n`, /sets role_arn/],
    ["an endpoint override", execEnv(), `${PIPPEN_CONFIG}endpoint_url = https://attacker.example\n`, /sets endpoint_url/],
    ["a CA bundle", execEnv(), `${PIPPEN_CONFIG}ca_bundle = /tmp/evil.pem\n`, /sets ca_bundle/],
    ["nested service settings", execEnv(), `${PIPPEN_CONFIG}sts =\n  endpoint_url = https://attacker.example\n`, /sets sts|not key = value/],
    ["a repeated credential_process", execEnv(), `${PIPPEN_CONFIG}credential_process = /bin/sh -c 'echo {}'\n`, /repeats credential_process/],
    ["another program as credential_process", execEnv(), "[profile pippen-sandbox]\ncredential_process = /usr/local/bin/aws-vault export --format=json prod\n", /credential_process is not/],
    ["another agent's credentials-process", execEnv(), '[profile pippen-sandbox]\ncredential_process = "/usr/bin/node" "/opt/pippen.js" "sandbox" "credentials-process" "agent-b" "--client-env" "/x/client.json"\n', /credential_process is not/],
    ["no credential_process", execEnv(), "[profile pippen-sandbox]\nregion = us-west-2\n", /credential_process is not/],
    ["a differently named profile section", execEnv(), PIPPEN_CONFIG.replace("[profile pippen-sandbox]", "[profile other]"), /exactly one section/],
    ["exec issued for another lease", execEnv({ PIPPEN_SANDBOX_SESSION_ID: OTHER_SESSION }), undefined, /PIPPEN_SANDBOX_SESSION_ID .* is not --pippen-session/],
    ["static exec issued for another lease", { ...STATIC_KEYS, AWS_CONFIG_FILE: "/dev/null", PIPPEN_SANDBOX_SESSION_ID: OTHER_SESSION }, undefined, /is not --pippen-session/],
    ["a malformed exec session id", execEnv({ PIPPEN_SANDBOX_SESSION_ID: SESSION.toUpperCase() }), undefined, /not a pippen lease id/],
    ["exec issued for another account", execEnv({ PIPPEN_SANDBOX_ACCOUNT_ID: "500268966557" }), undefined, /PIPPEN_SANDBOX_ACCOUNT_ID is not the sandbox account/],
  ];
  for (const [name, env, text, want] of refused)
    test(`refuses ${name}`, () => {
      expect(errorOf(resolveSbx(env, text ?? PIPPEN_CONFIG))).toMatch(want);
    });
});

describe("the stack's sandbox caller pin agrees with `pippen sandbox exec`", () => {
  test("PIPPEN_SANDBOX_SESSION_ID / _ACCOUNT_ID, when present, must name the same lease and the sandbox account", async () => {
    const { pippenProofSandboxCallerArn } = await import("../../../../stacks/aws/sandbox-rules.ts");
    expect(pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION })).toBe(CALLER);
    expect(pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "361835732085" })).toBe(CALLER);
    expect(() => pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_SESSION_ID: OTHER_SESSION })).toThrow(/PIPPEN_SANDBOX_SESSION_ID .* is not NINETY_SIX_BULLS_PIPPEN_SESSION/);
    expect(() => pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "500268966557" })).toThrow(/PIPPEN_SANDBOX_ACCOUNT_ID is not the sandbox account/);
  });
});
