import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ecrPullPolicy, MIGRATION_URL_MIN_CHART, RUNTIME_ROLE_DENYLIST, sharedServicesRepository, validateAwsInputs, type ApprovedServicesImage, type AwsInputs } from "../inputs.ts";
import { PIPPEN_PROOF_SANDBOX, pippenProofSandboxCallerArn } from "../sandbox-rules.ts";
import { verifyCallerArn } from "../preflight.ts";

const lock = JSON.parse(readFileSync(fileURLToPath(new URL("../../../versions.lock.json", import.meta.url)), "utf8")) as { images: { "services-aws": { approved: ApprovedServicesImage[] } } };
const approved = lock.images["services-aws"].approved;

const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-proof.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;
const filled = (patch: (i: AwsInputs) => void = () => {}) => {
  const i = structuredClone(example);
  i.approval = { ...i.approval!, decision: "approved", date: "2026-10-30", quote: "approved applying stage A (aws-proof, create-only)", capture: { provenanceSha256: "a".repeat(64), planSha256: "b".repeat(64), inputsSha256: "c".repeat(64), sourceCommit: "d".repeat(40) } };
  i.eks!.apiAccess = { publicAccessCidrs: ["203.0.113.7/32"] };
  patch(i);
  return i;
};

describe("aws-proof: a fresh, fully hardened stage beside sessions-dev", () => {
  it("validates once the placeholders are filled, with every hardening default on", () => {
    const r = validateAwsInputs("aws-proof", filled(), approved);
    expect(r.hoopdb).toBe("cnpg");
    expect(r.endpointAccess).toEqual({ endpointPrivateAccess: true, endpointPublicAccess: true, publicAccessCidrs: ["203.0.113.7/32"] });
    expect(example.eks!.relayauth).toBeDefined(); // RelayAuth with in-cluster TLS
    expect(example.eks!.ingress!.hostname).toBe("proof-sessions.agentrelay.com");
    expect(example.eks!.ingress!.scheme).toBe("internet-facing");
  });

  it("refuses the example as committed: every <...> placeholder is named", () => {
    expect(() => validateAwsInputs("aws-proof", example)).toThrow(/\$\.approval\.decision, \$\.approval\.date, \$\.approval\.quote, \$\.approval\.capture\.provenanceSha256, \$\.approval\.capture\.planSha256, \$\.approval\.capture\.inputsSha256, \$\.approval\.capture\.sourceCommit, \$\.eks\.apiAccess\.publicAccessCidrs\[0\]/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.$comment = "fill <this>")))).toThrow(/\$\.\$comment/);
  });

  it("pins the services image to an approved versions.lock.json entry (digest + source commit + repository)", () => {
    expect(approved).toContainEqual(expect.objectContaining({ digest: example.eks!.imageDigest, sourceRevision: example.eks!.imageSourceRevision }));
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.imageDigest = `sha256:${"c".repeat(64)}`)), approved)).toThrow(/not an approved services-aws entry/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.imageSourceRevision = "d".repeat(40))), approved)).toThrow(/not an approved services-aws entry/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.imageRepository = "111122223333.dkr.ecr.us-west-2.amazonaws.com/96bulls-services")), approved)).toThrow(/not an approved services-aws entry/);
  });

  it("allows bucket purge on destroy only for proof and sandbox stages", () => {
    expect(example.teardown).toEqual({ purgeBucketsOnDestroy: true });
    expect(() => validateAwsInputs("aws-sessions-dev", filled((i) => { i.eks!.hoopdb = { backend: "external" }; }), approved)).toThrow(/purgeBucketsOnDestroy is only for proof and sandbox/);
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack.match(/\.\.\.\(purge \? \{ forceDestroy: true \} : \{\}\)/g)).toHaveLength(2); // locker + trail
    expect(readFileSync(fileURLToPath(new URL("../ingress.ts", import.meta.url)), "utf8")).toContain("...(i.purgeOnDestroy ? { forceDestroy: true } : {})");
  });

  it("never purges buckets on a production-looking stage, even one matching the proof/sandbox pattern", () => {
    for (const stage of ["aws-proof-prod", "aws-production", "aws-sandbox-prod"])
      expect(() => validateAwsInputs(stage, filled((i) => { delete i.eks!.hoopdb; }), approved)).toThrow(/never allowed on a production-looking stage/);
  });

  it("refuses a boolean NetworkPolicy enforcement switch: phase 2 needs a verification record", () => {
    expect(() => validateAwsInputs("aws-proof", filled((i) => ((i.eks as Record<string, unknown>).networkPolicyEnforcement = true)))).toThrow(/verification record, not a boolean/);
  });

  it("also fits a fresh Pippen sandbox account: own ECR/OIDC skipped, boundary and sweeper tag on", () => {
    const sandbox = filled((i) => {
      delete i.accountShared;
      i.accountId = "111122223333";
      i.pippenSandbox = { permissionsBoundaryArn: "arn:aws:iam::111122223333:policy/pippen-agent-session-boundary" };
      i.eks!.imageRepository = "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services";
    });
    expect(() => validateAwsInputs("aws-proof", sandbox)).not.toThrow();
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.pippenSandbox = { permissionsBoundaryArn: "arn:aws:iam::999999999999:policy/x" })))).toThrow(/this account|cannot share/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.imageRepository = "ghcr.io/x/y")))).toThrow(/ECR/);
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack).toContain('sandboxTag = inputs.pippenSandbox ? { "pippen:sandbox": "true" } : {};');
  });

  it("shares the account's ECR repository, OIDC provider and publisher with the owner stage", () => {
    expect(example.accountShared).toEqual({ ownerStage: "aws-sessions-dev" });
    expect(sharedServicesRepository("500268966557", "us-west-2")).toBe("500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services");
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.accountShared = { ownerStage: "aws-proof" })))).toThrow(/another aws-\* stage/);
  });

  it("is refused if it would expose the API to the internet or run CNPG on production", () => {
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.apiAccess = { publicAccessCidrs: ["0.0.0.0/0"] })))).toThrow(/broader than/);
    expect(() => validateAwsInputs("aws-prod", filled((i) => delete i.teardown))).toThrow(/Supabase/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.eks!.relayauthIssuer = "https://relayauth.dev")))).toThrow(/both set/);
  });

  it("names a cluster the #74 harness guard expects", () => {
    // eks.ts: clusterName = `96bulls-${stage}`
    expect(readFileSync(fileURLToPath(new URL("../eks.ts", import.meta.url)), "utf8")).toContain("const clusterName = `96bulls-${i.stage}`;");
    expect(`96bulls-${"aws-proof"}`).toBe("96bulls-aws-proof");
  });
});

describe("aws-proof-sandbox: the E2E proof in the Pippen sandbox of agent aws-proof", () => {
  const sbxExample = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-proof-sandbox.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;
  const sbx = (patch: (i: AwsInputs) => void = () => {}) => {
    const i = structuredClone(sbxExample);
    i.approval = { ...i.approval!, decision: "approved", date: "2026-11-01", quote: "run the EKS proof in a pippen sandbox", capture: { provenanceSha256: "a".repeat(64), planSha256: "b".repeat(64), inputsSha256: "c".repeat(64), sourceCommit: "d".repeat(40) } };
    i.eks!.apiAccess = { publicAccessCidrs: ["203.0.113.7/32"] };
    i.pippenSandbox = { permissionsBoundaryArn: "arn:aws:iam::361835732085:policy/pippen-sandbox-boundary" };
    patch(i);
    return i;
  };

  it("validates once the placeholders are filled: pinned account, pippenSandbox, no accountShared, approved image", () => {
    expect(sbxExample.accountId).toBe(PIPPEN_PROOF_SANDBOX.accountId);
    expect(sbxExample.region).toBe("us-west-2");
    expect(sbxExample.accountShared).toBeUndefined();
    expect(sbxExample.eks!.ingress!.hostname).toBe("proof-sandbox-sessions.agentrelay.com");
    expect(sbxExample.eks!.relayhistoryOrigin).toBe("https://proof-sandbox-sessions.agentrelay.com");
    const r = validateAwsInputs("aws-proof-sandbox", sbx(), approved);
    expect(r.hoopdb).toBe("cnpg");
  });

  it("refuses ecr.pullAccounts in a Pippen sandbox: its boundary denies ecr:SetRepositoryPolicy (pippen-tools #37)", () => {
    expect(sbxExample.ecr).toBeUndefined();
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => (i.ecr = { pullAccounts: ["111122223333"] })), approved)).toThrow(/declares no ECR repository policy/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => (i.ecr = { pullAccounts: [] })), approved)).not.toThrow();
  });

  it("refuses the example as committed: the boundary policy name and approval are placeholders", () => {
    expect(() => validateAwsInputs("aws-proof-sandbox", sbxExample)).toThrow(/\$\.pippenSandbox\.permissionsBoundaryArn/);
  });

  it("refuses the sandbox stage without pippenSandbox, with accountShared, or in another account/region", () => {
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => delete i.pippenSandbox), approved)).toThrow(/needs pippenSandbox\.permissionsBoundaryArn/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => (i.accountShared = { ownerStage: "aws-sessions-dev" })), approved)).toThrow(/cannot share|cannot set accountShared/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => { delete i.pippenSandbox; i.accountShared = { ownerStage: "aws-sessions-dev" }; }), approved)).toThrow(/needs pippenSandbox/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => { i.accountId = "500268966557"; i.pippenSandbox = { permissionsBoundaryArn: "arn:aws:iam::500268966557:policy/b" }; }), approved)).toThrow(/runs only in the Pippen sandbox 361835732085\/us-west-2/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => (i.region = "us-east-1")), approved)).toThrow(/runs only in the Pippen sandbox/);
    expect(() => validateAwsInputs("aws-proof-sandbox", sbx((i) => (i.pippenSandbox = { permissionsBoundaryArn: "arn:aws:iam::500268966557:policy/b" })), approved)).toThrow(/policy ARN in this account/);
  });

  it("the sandbox account hosts no other stage", () => {
    for (const stage of ["aws-proof", "aws-sessions-dev", "aws-sandbox-aws-proof"])
      expect(() => validateAwsInputs(stage, sbx((i) => delete i.teardown), approved)).toThrow(/only stage aws-proof-sandbox may target it/);
  });

  it("the existing aws-proof stage is unaffected", () => {
    expect(() => validateAwsInputs("aws-proof", filled(), approved)).not.toThrow();
  });

  it("under pippen sandbox exec, the exec's lease and account must match the pinned session", () => {
    const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
    const ARN = `arn:aws:sts::361835732085:assumed-role/agent-session/agent-${SESSION}`;
    const exec = { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, AWS_PROFILE: "pippen-sandbox", AWS_CONFIG_FILE: "/run/user/1000/pippen-exec-x/config", AWS_SHARED_CREDENTIALS_FILE: "/dev/null" };
    expect(pippenProofSandboxCallerArn({ ...exec, PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: "361835732085" })).toBe(ARN);
    expect(pippenProofSandboxCallerArn(exec)).toBe(ARN);
    expect(() => pippenProofSandboxCallerArn({ ...exec, PIPPEN_SANDBOX_SESSION_ID: "9a1c2b3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d" })).toThrow(/PIPPEN_SANDBOX_SESSION_ID .* is not NINETY_SIX_BULLS_PIPPEN_SESSION/);
    expect(() => pippenProofSandboxCallerArn({ ...exec, PIPPEN_SANDBOX_SESSION_ID: "" })).toThrow(/is not NINETY_SIX_BULLS_PIPPEN_SESSION/);
    expect(() => pippenProofSandboxCallerArn({ ...exec, PIPPEN_SANDBOX_ACCOUNT_ID: "500268966557" })).toThrow(/PIPPEN_SANDBOX_ACCOUNT_ID is not the sandbox account/);
  });

  it("the stack checks the caller is the pippen agent-session role before declaring anything", () => {
    const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
    const ARN = pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION });
    expect(ARN).toBe(`arn:aws:sts::361835732085:assumed-role/agent-session/agent-${SESSION}`);
    for (const bad of [undefined, "", "aws-proof", SESSION.toUpperCase(), SESSION.replace("-4e6b-", "-1e6b-"), `${SESSION}x`])
      expect(() => pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: bad })).toThrow(/needs NINETY_SIX_BULLS_PIPPEN_SESSION/);
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    // In the stack's Effect (the state layer, before it, runs the same check before making the sandbox's state bucket).
    const guard = stack.indexOf("verifyCallerArn(", stack.indexOf("export default Alchemy.Stack("));
    expect(stack.indexOf("verifyCallerArn(")).toBeLessThan(stack.indexOf("ensurePippenStateBucket(\n"));
    expect(guard).toBeGreaterThan(stack.indexOf("assertAwsDeploymentTarget("));
    expect(guard).toBeLessThan(stack.indexOf("AWS.KMS.Key("));
    expect(stack).toContain("if (stage === PIPPEN_PROOF_SANDBOX.stage)");
    expect(stack).toContain("pippenProofSandboxCallerArn(process.env)");
    const calls: string[][] = [];
    const aws = (arn: string | undefined) => ({ json: (a: string[]) => { calls.push(a); if (arn === undefined) throw new Error("denied"); return { Arn: arn }; }, s3Json: () => ({}) });
    expect(() => verifyCallerArn(aws(ARN), ARN)).not.toThrow();
    expect(() => verifyCallerArn(aws("arn:aws:sts::361835732085:assumed-role/agent-session/agent-aws-proof"), ARN)).toThrow(/is not arn:aws:sts::361835732085:assumed-role\/agent-session\/agent-3f2b8c1e/);
    expect(() => verifyCallerArn(aws(undefined), ARN)).toThrow(/unverified/);
    expect(calls[0]).toEqual(["sts", "get-caller-identity"]);
  });

  it("names the cluster the harness's sandbox target expects", () => {
    expect(`96bulls-${PIPPEN_PROOF_SANDBOX.stage}`).toBe("96bulls-aws-proof-sandbox");
  });
});

describe("ecr.pullAccounts: read-only cross-account pull from the services repository", () => {
  // Applied live by the coordinator on Khaliq's OK (2026-11-01, "give my OK") to the sessions-dev repository, with
  // ecr:DescribeImages added (the sandbox stage's image check); the coordinator applies this 4-action policy after merge.
  const LIVE = {"Version":"2012-10-17","Statement":[{"Sid":"PippenSandboxAwsProofPull","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::361835732085:root"},"Action":["ecr:BatchGetImage","ecr:GetDownloadUrlForLayer","ecr:BatchCheckLayerAvailability","ecr:DescribeImages"]}]};
  const devAccount = "500268966557";
  const canon = (v: unknown): unknown => (Array.isArray(v) ? v.map(canon) : v && typeof v === "object" ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b)).map(([k, x]) => [k, canon(x)])) : v);

  it("one account renders exactly the live policy (after normalization)", () => {
    const text = ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085"] } })!;
    expect(JSON.stringify(canon(JSON.parse(text)))).toBe(JSON.stringify(canon(LIVE)));
    expect(text).toBe(JSON.stringify(LIVE)); // and byte for byte as rendered
  });

  it("grants read-only actions only: pull and image metadata, never push, delete or policy changes", () => {
    const actions = JSON.parse(ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085"] } })!).Statement[0].Action as string[];
    expect(actions).toEqual(["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability", "ecr:DescribeImages"]);
    expect(actions.every((a) => /^ecr:(BatchGet|GetDownload|BatchCheck|Describe)/.test(a))).toBe(true);
  });

  it("other accounts get one generic read-only statement each", () => {
    const p = JSON.parse(ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085", "111122223333"] } })!);
    expect(p.Statement).toHaveLength(2);
    expect(p.Statement[1]).toEqual({ Sid: "CrossAccountPull111122223333", Effect: "Allow", Principal: { AWS: "arn:aws:iam::111122223333:root" }, Action: LIVE.Statement[0]!.Action });
  });

  it("absent or empty means no policy (today's behavior)", () => {
    expect(ecrPullPolicy({ accountId: devAccount })).toBeUndefined();
    expect(ecrPullPolicy({ accountId: devAccount, ecr: {} })).toBeUndefined();
    expect(ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: [] } })).toBeUndefined();
  });

  it("refuses invalid ids, wildcards, duplicates, the own account and anything but pullAccounts", () => {
    for (const bad of ["*", "36183573208", "3618357320855", "arn:aws:iam::361835732085:root", " 361835732085", "36183573208x", 361835732085 as unknown as string])
      expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: [bad] } })).toThrow(/12-digit account ids/);
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: "361835732085" as unknown as string[] } })).toThrow(/array/);
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085", "361835732085"] } })).toThrow(/repeat/);
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: [devAccount] } })).toThrow(/own account/);
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085"], actions: ["ecr:*"] } as never })).toThrow(/only key/);
  });

  it("never grants the production account", () => {
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["941433935742"] } })).toThrow(/production account 941433935742/);
    expect(() => ecrPullPolicy({ accountId: devAccount, ecr: { pullAccounts: ["361835732085", "941433935742"] } })).toThrow(/production account/);
  });

  it("is refused on a stage that does not own the repository", () => {
    expect(() => ecrPullPolicy({ accountId: devAccount, accountShared: { ownerStage: "aws-sessions-dev" }, ecr: { pullAccounts: ["361835732085"] } })).toThrow(/accountShared/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => (i.ecr = { pullAccounts: ["361835732085"] })), approved)).toThrow(/accountShared/);
  });

  it("validateAwsInputs runs it, and the stack sets it as the services repository policy only when present", () => {
    expect(() => validateAwsInputs("aws-proof", filled((i) => { delete i.accountShared; i.ecr = { pullAccounts: ["941433935742"] }; }), approved)).toThrow(/production account/);
    const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(stack).toContain("const pullPolicy = ecrPullPolicy(inputs);");
    const repo = stack.slice(stack.indexOf('AWS.ECR.Repository("services-image"'), stack.indexOf("tags: tags(stage, \"ecr\")"));
    expect(repo).toContain("...(pullPolicy ? { policy: pullPolicy } : {}),");
  });
});

describe("HoopDb runtime role, function boundary and migration owner inputs", () => {
  const profile = { hoopdbRuntimeRole: "example_runtime", hoopdbRuntimeRoleLogin: true, hoopdbFunctionBoundaryMode: "event-trigger", hoopdbMigrationUrlKey: "HOOPDB_MIGRATION_URL" } as const;
  const external = (patch: (e: NonNullable<AwsInputs["eks"]>) => void) =>
    filled((i) => {
      i.eks!.hoopdb = { backend: "external" };
      i.eks!.chartVersion = "0.3.2";
      delete i.eks!.sessionDirectory;
      patch(i.eks!);
    });
  const check = (patch: (e: NonNullable<AwsInputs["eks"]>) => void) => () => validateAwsInputs("aws-proof", external(patch), approved);

  it("accepts the full external profile, and the inputs unset", () => {
    expect(validateAwsInputs("aws-proof", filled(), approved).hoopdb).toBe("cnpg");
    expect(check((e) => Object.assign(e, profile))().hoopdb).toBe("external");
  });

  it("refuses an unsafe, denylisted or half-set runtime role", () => {
    for (const role of RUNTIME_ROLE_DENYLIST)
      expect(check((e) => Object.assign(e, profile, { hoopdbRuntimeRole: role }))).toThrow(/dedicated non-owner role/);
    expect([...RUNTIME_ROLE_DENYLIST]).toEqual(["postgres", "hoop", "anon", "authenticated", "service_role", "authenticator", "supabase_admin"]);
    expect(check((e) => Object.assign(e, profile, { hoopdbRuntimeRole: "bad role" }))).toThrow(/dedicated non-owner role/);
    expect(check((e) => Object.assign(e, profile, { hoopdbRuntimeRoleLogin: undefined }))).toThrow(/hoopdbRuntimeRoleLogin must be true or false/);
    expect(check((e) => { e.hoopdbRuntimeRoleLogin = true; })).toThrow(/without eks\.hoopdbRuntimeRole/);
  });

  it("refuses a NOLOGIN runtime role: there is no workload-role input", () => {
    expect(check((e) => Object.assign(e, profile, { hoopdbRuntimeRoleLogin: false }))).toThrow(/hoopdbRuntimeRoleLogin=false is not supported.*workload login role/);
  });

  it("requires the owner URL key with a runtime role on external HoopDb (not in-cluster)", () => {
    expect(check((e) => Object.assign(e, profile, { hoopdbMigrationUrlKey: undefined }))).toThrow(/hoopdbMigrationUrlKey is required with eks\.hoopdbRuntimeRole/);
    // CNPG: migrate and the services read the CNPG Secret; no owner key needed or allowed.
    expect(validateAwsInputs("aws-proof", filled((i) => Object.assign(i.eks!, { hoopdbRuntimeRole: "example_runtime", hoopdbRuntimeRoleLogin: true })), approved).hoopdb).toBe("cnpg");
  });

  it("keeps the boundary mode and the owner URL key to external HoopDb", () => {
    expect(() => validateAwsInputs("aws-proof", filled((i) => { i.eks!.hoopdbFunctionBoundaryMode = "event-trigger"; }), approved)).toThrow(/external \(Supabase\) HoopDb only/);
    expect(check((e) => Object.assign(e, profile, { hoopdbFunctionBoundaryMode: "none" }))).toThrow(/external \(Supabase\) HoopDb only/);
    expect(() => validateAwsInputs("aws-proof", filled((i) => { i.eks!.hoopdbMigrationUrlKey = "HOOPDB_MIGRATION_URL"; }), approved)).toThrow(/hoopdbMigrationUrlKey is for external/);
    expect(check((e) => Object.assign(e, profile, { hoopdbMigrationUrlKey: "HOOPDB_URL" }))).toThrow(/other than HOOPDB_URL/);
  });

  it("needs chart 0.3.2+ as an exact release for the owner URL key", () => {
    for (const v of ["0.3.0", "0.3.1", "0.3.2-rc.1", "0.3.2+build.1", "0.3", "v0.3.2", "0.03.2"])
      expect(check((e) => Object.assign(e, profile, { chartVersion: v }))).toThrow(/needs eks\.chartVersion 0\.3\.2\+ as an exact release/);
    for (const v of ["0.3.2", "0.3.10", "0.4.0"]) expect(check((e) => Object.assign(e, profile, { chartVersion: v }))().hoopdb).toBe("external");
    expect(MIGRATION_URL_MIN_CHART).toEqual([0, 3, 2]);
  });
});
