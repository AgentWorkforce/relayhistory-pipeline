import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { observeState, verifyDeployCapture, type CaptureIO } from "../capture-verify.ts";
import { alchemyStateBucket as preflightStateBucket } from "../preflight.ts";
import {
  alchemyStateBucket,
  alchemyStatePrefix,
  ensurePippenStateBucket,
  pippenStateBucket,
  pippenStateBucketTags,
  stackStateOptions,
  stateBucketFor,
  stateTargetFromArgv,
  type AwsRun,
} from "../state-bucket.ts";
import { versionsDigest } from "../../../src/cli/plan-provenance.ts";

const src = (f: string) => readFileSync(fileURLToPath(new URL(`../${f}`, import.meta.url)), "utf8");
const SBX = { accountId: "361835732085", region: "us-west-2", stage: "aws-proof-sandbox" };
const BUCKET = "pippen-sbx-alchemy-state-361835732085-us-west-2-an";
const S3_NAME = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/;

describe("Alchemy state store: every other stage is byte-for-byte unchanged", () => {
  it("keeps Alchemy's default account store and the 96bulls prefix", () => {
    expect(stackStateOptions(undefined)).toEqual({ prefix: "96bulls" });
    expect(JSON.stringify(stackStateOptions({ accountId: "500268966557", region: "us-west-2" }))).toBe('{"prefix":"96bulls"}');
    expect(stateBucketFor({ accountId: "500268966557", region: "us-west-2" })).toBe("alchemy-state-500268966557-us-west-2-an");
    expect(stateBucketFor({ accountId: "941433935742", region: "us-east-1" })).toBe("alchemy-state-941433935742-us-east-1-an");
    expect(alchemyStateBucket("500268966557", "us-west-2")).toBe(preflightStateBucket("500268966557", "us-west-2"));
    expect(alchemyStatePrefix("aws-proof")).toBe("96bulls/96bulls-aws/aws-proof");
  });

  it("the stack's state layer: the snapshot for captured plans, AWS.state({ prefix: \"96bulls\" }) unless pippenSandbox", () => {
    const run = src("alchemy.aws.run.ts");
    expect(run).toContain(`    state: process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT
      ? snapshotStateLayer(process.env.NINETY_SIX_BULLS_STATE_SNAPSHOT)
      : stateTarget?.pippenSandbox
        ? pippenStateLayer(stateTarget)
        : AWS.state({ prefix: "96bulls" }),`);
    expect(run).toContain("stateBucket: stateBucketFor(inputs),");
  });

  it("observeState still errors on a missing default store (no fresh-account path outside a sandbox)", () => {
    const io: CaptureIO = { root: "/r", readFile: () => undefined, run: (_c, a) => { if (a[1] === "get-bucket-versioning") throw new Error("aws s3api get-bucket-versioning failed: An error occurred (NoSuchBucket)"); throw new Error(a.join(" ")); } };
    expect(() => observeState(io, "500268966557", "us-west-2", "aws-proof")).toThrow(/NoSuchBucket/);
  });
});

describe("Pippen sandbox: the state store is a pippen-sbx- bucket the sweeper deletes", () => {
  it("is pippen-sbx-alchemy-state-<account>-<region>-an: valid S3, <= 63, account-regional, outside the sweeper's alchemy-state-* deny", () => {
    const b = pippenStateBucket(SBX.accountId, SBX.region);
    expect(b).toBe(BUCKET);
    expect(b.length).toBeLessThanOrEqual(63);
    expect(b).toMatch(S3_NAME);
    expect(b.startsWith("pippen-sbx-")).toBe(true);
    // pippen-tools NeverDeleteBucketsThatMustSurvive denies arn:aws:s3:::alchemy-state-*; IAM ARN globs are anchored.
    expect(b.startsWith("alchemy-state-")).toBe(false);
    expect(b.endsWith(`-${SBX.accountId}-${SBX.region}-an`)).toBe(true);
    // The longest region name still fits.
    expect(pippenStateBucket(SBX.accountId, "ap-southeast-4").length).toBeLessThanOrEqual(63);
    expect(() => pippenStateBucket("36183573208", SBX.region)).toThrow(/12 digits/);
    expect(() => pippenStateBucket(SBX.accountId, "US-WEST-2")).toThrow(/not an AWS region/);
  });

  it("AWS.state gets the bucket by name; capture, deploy check and NetworkPolicy check use the same one", () => {
    const t = { accountId: SBX.accountId, region: SBX.region, pippenSandbox: { permissionsBoundaryArn: "arn:aws:iam::361835732085:policy/b" } };
    expect(stackStateOptions(t)).toEqual({ prefix: "96bulls", bucketName: BUCKET });
    expect(stateBucketFor(t)).toBe(BUCKET);
  });

  it("is tagged pippen:sandbox=true (the sweeper lists by it) and for the stage", () => {
    expect(pippenStateBucketTags(SBX.stage)).toEqual([
      { Key: "pippen:sandbox", Value: "true" },
      { Key: "96bulls:stack", Value: "aws-proof-sandbox" },
      { Key: "96bulls:component", Value: "alchemy-state" },
      { Key: "96bulls:repo", Value: "AgentWorkforce/sessions-pipeline" },
    ]);
  });

  it("reads the stage and inputs from Alchemy's argv before the stack runs", () => {
    const inputs = JSON.stringify({ accountId: SBX.accountId, region: SBX.region, pippenSandbox: { permissionsBoundaryArn: "x" } });
    const read = (stage: string) => (stage === SBX.stage ? inputs : undefined);
    expect(stateTargetFromArgv(["bun", "alchemy", "plan", "--config", "c", "--stage", SBX.stage], read)).toEqual({ ...SBX, pippenSandbox: { permissionsBoundaryArn: "x" } });
    expect(stateTargetFromArgv(["alchemy", "deploy", `--stage=${SBX.stage}`], read)?.pippenSandbox).toBeTruthy();
    expect(stateTargetFromArgv(["alchemy", "plan", "--stage", "aws-proof"], read)).toBeUndefined();
    expect(stateTargetFromArgv(["alchemy", "plan", "--stage", "aws-proof"], () => JSON.stringify({ accountId: "500268966557", region: "us-west-2" }))).toEqual({ stage: "aws-proof", accountId: "500268966557", region: "us-west-2" });
    expect(stateTargetFromArgv(["alchemy", "plan"], read)).toBeUndefined();
    expect(stateTargetFromArgv(["alchemy", "plan", "--stage", SBX.stage], () => "{not json")).toBeUndefined();
  });
});

/** A fake account: the bucket's settings, recording every call. */
function fakeAccount(o: { exists?: boolean; account?: string; headError?: string; tags?: { Key: string; Value: string }[]; versioning?: string; encryption?: string; pab?: boolean; ownership?: string } = {}) {
  const calls: string[][] = [];
  let exists = o.exists ?? false;
  const aws: AwsRun = (args) => {
    calls.push(args);
    const [svc, cmd] = args;
    if (svc === "sts") return JSON.stringify({ Account: o.account ?? SBX.accountId, Arn: "arn" });
    if (cmd === "head-bucket") {
      if (o.headError) throw new Error(o.headError);
      if (!exists) throw new Error("aws s3api failed: An error occurred (404) when calling the HeadBucket operation: Not Found");
      return "";
    }
    if (cmd === "create-bucket") {
      exists = true;
      return JSON.stringify({ Location: `/${BUCKET}` });
    }
    if (cmd === "wait") return "";
    if (!exists) throw new Error("An error occurred (NoSuchBucket)");
    if (cmd === "get-bucket-versioning") return o.exists ? JSON.stringify(o.versioning ? { Status: o.versioning } : {}) : "{}";
    if (cmd === "get-bucket-encryption") {
      if (o.encryption === "none") throw new Error("An error occurred (ServerSideEncryptionConfigurationNotFoundError)");
      return JSON.stringify({ ServerSideEncryptionConfiguration: { Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: o.encryption ?? "AES256" }, BucketKeyEnabled: false }] } });
    }
    if (cmd === "get-public-access-block") {
      if (o.exists && !o.pab) throw new Error("An error occurred (NoSuchPublicAccessBlockConfiguration)");
      return JSON.stringify({ PublicAccessBlockConfiguration: { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true } });
    }
    if (cmd === "get-bucket-ownership-controls") return JSON.stringify({ OwnershipControls: { Rules: [{ ObjectOwnership: o.ownership ?? "BucketOwnerEnforced" }] } });
    if (cmd === "get-bucket-tagging") {
      if (!o.exists) return JSON.stringify({ TagSet: pippenStateBucketTags(SBX.stage) });
      if (!o.tags) throw new Error("An error occurred (NoSuchTagSet)");
      return JSON.stringify({ TagSet: o.tags });
    }
    if (cmd?.startsWith("put-")) return "";
    throw new Error(`unexpected aws ${args.join(" ")}`);
  };
  const opt = (c: string[], flag: string) => c[c.indexOf(flag) + 1]!;
  return { aws, calls, opt, writes: () => calls.filter((c) => c[1] === "create-bucket" || c[1]?.startsWith("put-")).map((c) => c[1]) };
}

describe("ensurePippenStateBucket: a fresh account gets the store on the first plan", () => {
  it("creates it tagged in the CreateBucket call (no untagged window), account-regional, versioned, public access blocked", () => {
    const f = fakeAccount();
    expect(ensurePippenStateBucket(f.aws, SBX)).toEqual({ bucket: BUCKET, created: true });
    const create = f.calls.find((c) => c[1] === "create-bucket")!;
    expect(f.opt(create, "--bucket")).toBe(BUCKET);
    expect(f.opt(create, "--region")).toBe("us-west-2");
    expect(f.opt(create, "--bucket-namespace")).toBe("account-regional");
    const config = JSON.parse(f.opt(create, "--create-bucket-configuration"));
    expect(config.LocationConstraint).toBe("us-west-2");
    expect(config.Tags ?? []).toContainEqual({ Key: "pippen:sandbox", Value: "true" });
    // The STS account check comes before any S3 call.
    expect(f.calls[0]![0]).toBe("sts");
    // A new bucket already blocks public access, is SSE-S3 and BucketOwnerEnforced: those are read, not rewritten.
    expect(f.writes()).toEqual(["create-bucket", "put-bucket-versioning"]);
    expect(f.opt(f.calls.find((c) => c[1] === "put-bucket-versioning")!, "--versioning-configuration")).toBe("Status=Enabled");
    for (const read of ["get-bucket-encryption", "get-public-access-block", "get-bucket-ownership-controls", "get-bucket-tagging"]) expect(f.calls.some((c) => c[1] === read)).toBe(true);
    // Never a bucket policy, lifecycle rule or object write: nothing protects it from the sweeper.
    expect(f.calls.some((c) => /policy|lifecycle|put-object|retention|legal-hold/.test(c[1] ?? ""))).toBe(false);
  });

  it("converges an existing bucket only where it drifted, merging the pippen tag into existing tags", () => {
    const f = fakeAccount({ exists: true, versioning: "Suspended", encryption: "none", pab: false, ownership: "ObjectWriter", tags: [{ Key: "owner", Value: "x" }] });
    expect(ensurePippenStateBucket(f.aws, SBX)).toEqual({ bucket: BUCKET, created: false });
    expect(f.writes()).toEqual(["put-bucket-versioning", "put-bucket-encryption", "put-public-access-block", "put-bucket-ownership-controls", "put-bucket-tagging"]);
    expect(JSON.parse(f.opt(f.calls.find((c) => c[1] === "put-public-access-block")!, "--public-access-block-configuration"))).toEqual({ BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true });
    expect(JSON.parse(f.opt(f.calls.find((c) => c[1] === "put-bucket-encryption")!, "--server-side-encryption-configuration"))).toEqual({ Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: "AES256" }, BucketKeyEnabled: false }] });
    const tagSet = JSON.parse(f.opt(f.calls.find((c) => c[1] === "put-bucket-tagging")!, "--tagging")).TagSet;
    expect(tagSet).toContainEqual({ Key: "owner", Value: "x" });
    expect(tagSet).toContainEqual({ Key: "pippen:sandbox", Value: "true" });
  });

  it("writes nothing to a bucket already in shape", () => {
    const f = fakeAccount({ exists: true, versioning: "Enabled", pab: true, tags: pippenStateBucketTags(SBX.stage) });
    ensurePippenStateBucket(f.aws, SBX);
    expect(f.writes()).toEqual([]);
  });

  it("fails closed: another account's credentials, or a 403 on head-bucket, create nothing", () => {
    const wrong = fakeAccount({ account: "500268966557" });
    expect(() => ensurePippenStateBucket(wrong.aws, SBX)).toThrow(/STS caller is in 500268966557/);
    expect(wrong.calls.filter((c) => c[0] === "s3api")).toEqual([]);
    const denied = fakeAccount({ headError: "An error occurred (403) when calling the HeadBucket operation: Forbidden" });
    expect(() => ensurePippenStateBucket(denied.aws, SBX)).toThrow(/403/);
    expect(denied.writes()).toEqual([]);
  });
});

describe("capture and deploy check in a Pippen sandbox", () => {
  const pippenInputs = JSON.stringify({ accountId: SBX.accountId, region: SBX.region, pippenSandbox: { permissionsBoundaryArn: "arn:aws:iam::361835732085:policy/b" } });
  const io = (missing: boolean, seen: string[][]): CaptureIO => ({
    root: "/r",
    readFile: (p) => (p.endsWith(".json") ? JSON.stringify({ schema: 1, stage: SBX.stage, target: { accountId: SBX.accountId, region: SBX.region }, source: {}, inputs: {}, state: { versioning: "Enabled", versionsDigest: versionsDigest({ Versions: [], DeleteMarkers: [] }).digest }, outputs: { plan: {}, detailed: {} } }) : "plan"),
    run: (cmd, a) => {
      seen.push([cmd, ...a]);
      if (cmd === "git") return a[0] === "rev-parse" ? "c\n" : "";
      if (a[0] === "sts") return JSON.stringify({ Account: SBX.accountId });
      if (missing) throw new Error("aws s3api get-bucket-versioning failed: An error occurred (NoSuchBucket) when calling the GetBucketVersioning operation");
      if (a[1] === "get-bucket-versioning") return JSON.stringify({ Status: "Enabled" });
      if (a[1] === "list-object-versions") return "{}";
      throw new Error(a.join(" "));
    },
  });

  it("an absent store is a fresh account: empty prior state, not an error", () => {
    let st: ReturnType<typeof observeState> | undefined;
    expect(() => (st = observeState(io(true, []), SBX.accountId, SBX.region, SBX.stage, true))).not.toThrow();
    if (!st) return;
    expect(st.bucket).toBe(BUCKET);
    expect(st.versioning).toBe("Absent");
    expect(st.listing).toEqual({ Versions: [], DeleteMarkers: [] });
    expect(st.versionsDigest).toBe(versionsDigest({ Versions: [], DeleteMarkers: [] }).digest);
  });

  it("the deploy check reads the pippen-sbx- store, and refuses when it has vanished since the capture", () => {
    const seen: string[][] = [];
    try {
      verifyDeployCapture(io(false, seen), SBX.stage, pippenInputs);
    } catch {
      // Other provenance fields are not under test here.
    }
    const buckets = seen.filter((c) => c[0] === "aws" && c.includes("--bucket")).map((c) => c[c.indexOf("--bucket") + 1]);
    expect(buckets.length).toBeGreaterThan(0);
    expect(new Set(buckets)).toEqual(new Set([BUCKET]));
    expect(() => verifyDeployCapture(io(true, []), SBX.stage, pippenInputs)).toThrow(/versioning/);
  });

  it("`plan --capture` makes the store before it snapshots, and only in a Pippen sandbox", () => {
    const cap = readFileSync(fileURLToPath(new URL("../../../src/cli/aws-provenance-io.ts", import.meta.url)), "utf8");
    const ensure = cap.indexOf("if (pippen) ensurePippenStateBucket(");
    expect(ensure).toBeGreaterThan(0);
    expect(ensure).toBeLessThan(cap.indexOf("const state = observeStateAt(root, inputs.accountId, inputs.region, stage, pippen);"));
    expect(cap).toContain("const after = observeStateAt(root, inputs.accountId, inputs.region, stage, pippen);");
  });
});
