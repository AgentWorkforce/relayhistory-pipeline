import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parseConfig } from "../../../benchmarks/ledger-bench/config.ts";
import { clientConfig, parseInputs, renderJob, resourceNames, type Inputs } from "../ledger-bench/ops.ts";

const DIR = join(import.meta.dir, "..", "ledger-bench");
const read = (name: string) => readFileSync(join(DIR, name), "utf8");
const INPUTS: Inputs = {
  dataProfile: "data", dataAccountId: "111111111111", clusterProfile: "cluster", clusterAccountId: "222222222222", clusterName: "example-cluster", kubeContext: "example",
  region: "us-west-2", eastRegion: "us-east-1", expressZoneA: "usw2-az1", expressZoneB: "usw2-az3", image: `registry.example/services@sha256:${"a".repeat(64)}`,
  nodeSelector: { "example/role": "general" }, tolerations: [], cpu: "500m", cpuLimit: "1", memory: "1536Mi", owner: "s3-spike-finn", ttl: "2026-11-18",
};
const KEYS = { east: `arn:aws:kms:us-east-1:111111111111:key/${"0".repeat(8)}-0000-0000-0000-${"0".repeat(12)}`, west: `arn:aws:kms:us-west-2:111111111111:key/${"0".repeat(8)}-0000-0000-0000-${"0".repeat(12)}` };

describe("ledger-bench inputs", () => {
  test("accepts a complete record and defaults the scheduling fields", () => {
    const { nodeSelector, tolerations, ...rest } = INPUTS;
    expect(parseInputs(JSON.stringify(rest))).toMatchObject({ nodeSelector: {}, tolerations: [] });
    expect(nodeSelector).toBeDefined();
    expect(tolerations).toBeDefined();
  });
  test("refuses the cluster's account as the data account, one zone twice, one Region twice, and an image by tag", () => {
    const bad = (patch: Partial<Inputs>) => () => parseInputs(JSON.stringify({ ...INPUTS, ...patch }));
    expect(bad({ dataAccountId: INPUTS.clusterAccountId })).toThrow(/must not be the cluster/);
    expect(bad({ expressZoneB: INPUTS.expressZoneA })).toThrow(/different zones/);
    expect(bad({ eastRegion: INPUTS.region })).toThrow(/another Region/);
    expect(bad({ image: "registry.example/services:latest" })).toThrow(/image/);
    expect(bad({ ttl: "soon" })).toThrow(/ttl/);
    expect(bad({ dataAccountId: "<account-id>" })).toThrow(/dataAccountId/);
  });
});

describe("ledger-bench client config", () => {
  test("is what the client accepts, and every target carries the marker", () => {
    const config = parseConfig(JSON.stringify({ ...clientConfig(INPUTS, KEYS), runId: "run-1", lane: "express" }), {});
    expect(Object.keys(config.targets)).toEqual(["R1", "R2", "R3", "R4", "R5", "R6"]);
    expect(config.assumeRoleArn).toBe("arn:aws:iam::111111111111:role/ledger-bench-client");
    expect(config.targets.R4!.bucket).toBe("ledger-bench-r4-111111111111--usw2-az1--x-s3");
    expect(config.targets.R4!.bucket.length).toBeLessThanOrEqual(63);
  });
  test("names follow the templates' own derivation", () => {
    const names = resourceNames(INPUTS);
    const west = read("data-west.yaml");
    const east = read("data-east.yaml");
    const sub = (pattern: string) => pattern.replace("${AWS::AccountId}", INPUTS.dataAccountId).replace("${AWS::Region}", INPUTS.region).replace("${ExpressZoneA}", INPUTS.expressZoneA).replace("${ExpressZoneB}", INPUTS.expressZoneB).replace("${EastRegion}", INPUTS.eastRegion);
    for (const id of ["R2", "R3", "R4", "R5"] as const) {
      const pattern = new RegExp(`BucketName: !Sub "(ledger-bench-${id.toLowerCase()}-[^"]+)"`).exec(west)![1]!;
      expect(sub(pattern)).toBe(names[id]);
    }
    expect(/BucketName: !Sub "(ledger-bench-r1-[^"]+)"/.exec(east)![1]!.replace("${AWS::AccountId}", INPUTS.dataAccountId).replace("${AWS::Region}", INPUTS.eastRegion)).toBe(names.R1);
    // The client role's grant on R1 uses the same derivation.
    expect(west).toContain("s3:::ledger-bench-r1-${AWS::AccountId}-${EastRegion}/bench/*");
    expect(west).toContain(`TableName: ${names.R6}`);
    expect(west).toContain(`RoleName: ${names.clientRole}`);
    expect(read("cluster-identity.yaml")).toContain(`role/${names.clientRole}`);
  });
});

describe("ledger-bench Job", () => {
  const template = read("job.template.yaml");
  test("renders every value, pins the deadline and runs without privileges", () => {
    const job = renderJob(template, INPUTS, { runId: "run-1", lane: "r2", profile: "standard", steps: "" });
    expect(job).not.toContain("${");
    expect(job).toContain("name: ledger-bench-run-1-r2");
    expect(job).toContain("activeDeadlineSeconds: 14400");
    expect(job).toContain("ttlSecondsAfterFinished: 3600");
    expect(job).toContain("backoffLimit: 0");
    expect(job).toContain(`image: ${INPUTS.image}`);
    expect(job).toContain('nodeSelector: {"example/role":"general"}');
    expect(job).toContain('limits: { cpu: "1", memory: "1536Mi" }');
    expect(job).toContain("readOnlyRootFilesystem: true");
    expect(job).toContain("automountServiceAccountToken: false");
    expect(job).toContain("namespace: ledger-bench");
  });
  test("refuses a run id, lane, profile or step list that could alter the manifest", () => {
    const render = (p: Partial<Parameters<typeof renderJob>[2]>) => () => renderJob(template, INPUTS, { runId: "run-1", lane: "r2", profile: "standard", steps: "", ...p });
    expect(render({ runId: 'x"\n  hostNetwork: true' })).toThrow(/run id/);
    expect(render({ lane: "prod" as never })).toThrow(/lane/);
    expect(render({ profile: "huge" })).toThrow(/profile/);
    expect(render({ steps: "1;rm" })).toThrow(/steps/);
  });
});

describe("ledger-bench templates", () => {
  const west = read("data-west.yaml");
  const identity = read("cluster-identity.yaml");
  test("carry no account id, ARN of a real account, or bucket name", () => {
    for (const name of ["data-west.yaml", "data-east.yaml", "cluster-identity.yaml", "job.template.yaml", "namespace.yaml"]) expect(read(name)).not.toMatch(/\d{12}/);
  });
  test("the pod role can only assume the client role, and only the bench ServiceAccount gets it", () => {
    const policies = identity.slice(identity.indexOf("      Policies:"), identity.indexOf("      Tags:"));
    expect(policies.match(/Action:/g)!.length).toBe(1);
    expect(policies).toContain('Action: "sts:AssumeRole"');
    expect(policies).toContain("role/ledger-bench-client");
    expect(policies).not.toContain("*");
    expect(identity).toContain("AllowedValues: [ledger-bench]");
    expect(identity).toContain('"aws:RequestTag/kubernetes-namespace": !Ref Namespace');
    expect(identity).toContain('"aws:RequestTag/kubernetes-service-account": !Ref ServiceAccount');
  });
  test("the client role is trusted only by that pod role and reaches only bench resources", () => {
    expect(west).toContain('"aws:PrincipalArn": !Sub "arn:${AWS::Partition}:iam::${ClusterAccountId}:role/${PodRoleName}"');
    const policy = west.slice(west.indexOf("        - PolicyName: ledger-bench-targets"), west.indexOf("  # A backstop"));
    // Every S3 object grant is under bench/, every bucket is a ledger-bench bucket, KMS is the bench keys only (by ARN and by tag).
    for (const line of policy.split("\n").filter((l) => l.includes("s3:::"))) expect(line).toContain("ledger-bench-r1-");
    expect(policy.match(/\/bench\/\*/g)!.length).toBe(3);
    expect(policy).toContain('"aws:ResourceTag/purpose": ledger-bench');
    expect(policy).not.toMatch(/Action: "?(s3|dynamodb|kms|iam|sts):\*/);
    expect(policy).not.toContain('Resource: "*"');
  });
  test("every data resource is deleted with its stack, buckets expire their objects, and the table is capped", () => {
    for (const name of ["data-west.yaml", "data-east.yaml"]) {
      const t = read(name);
      expect(t).not.toContain("DeletionPolicy: Retain");
      expect(t.match(/Type: AWS::S3::Bucket\n/g)!.length).toBe(t.match(/ExpirationInDays: 1, AbortIncompleteMultipartUpload/g)!.length);
      expect(t).toContain("PendingWindowInDays: 7");
    }
    expect(west).toContain("MaxWriteRequestUnits: 10000");
    expect(west).toContain("BillingMode: PAY_PER_REQUEST");
    expect(west.match(/BucketKeyEnabled: false/g)!.length).toBe(1);
  });
});
