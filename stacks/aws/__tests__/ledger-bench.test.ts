import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parseConfig } from "../../../benchmarks/ledger-bench/config.ts";
import { mkdtempSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { STANDARD } from "../../../benchmarks/ledger-bench/config.ts";
import { BUNDLE_MAX_BYTES, clientConfig, insideRepo, isClean, ledgerAdd, parseInputs, renderJob, resourceNames, uncollected, type Inputs, type LedgerEntry } from "../ledger-bench/ops.ts";

const DIR = join(import.meta.dir, "..", "ledger-bench");
const read = (name: string) => readFileSync(join(DIR, name), "utf8");
const INPUTS: Inputs = {
  dataProfile: "data", dataAccountId: "111111111111", clusterProfile: "cluster", clusterAccountId: "222222222222", clusterName: "example-cluster", kubeContext: "example",
  region: "us-west-2", eastRegion: "us-east-1", expressZoneA: "usw2-az1", expressZoneB: "usw2-az3", image: `registry.example/services@sha256:${"a".repeat(64)}`,
  nodeSelector: { "example/role": "general" }, tolerations: [], cpu: "500m", cpuLimit: "1", memory: "1536Mi", owner: "s3-spike-finn", ttl: "2026-11-18",
};
const keyArn = (region: string, n: string) => `arn:aws:kms:${region}:111111111111:key/${n.repeat(8)}-${n.repeat(4)}-${n.repeat(4)}-${n.repeat(4)}-${n.repeat(12)}`;
const KEYS = { east: keyArn("us-east-1", "0"), west: keyArn("us-west-2", "1"), probe: keyArn("us-west-2", "2") };
const SHA = "b".repeat(64);

describe("ledger-bench inputs", () => {
  test("accepts a complete record and defaults the scheduling fields", () => {
    const { nodeSelector: _n, tolerations: _t, dataProfile: _d, ...rest } = INPUTS;
    // The cluster side's operator has no data profile, and the reverse.
    expect(parseInputs(JSON.stringify(rest))).toMatchObject({ nodeSelector: {}, tolerations: [] });
    expect(parseInputs(JSON.stringify({ ...INPUTS, keys: KEYS })).keys).toEqual(KEYS);
  });
  test("refuses key ARNs that are malformed or not three different keys", () => {
    expect(() => parseInputs(JSON.stringify({ ...INPUTS, keys: { ...KEYS, probe: KEYS.west } }))).toThrow(/three different keys/);
    expect(() => parseInputs(JSON.stringify({ ...INPUTS, keys: { ...KEYS, east: "alias/x" } }))).toThrow(/keys.east/);
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
    // The single-key probe names a key that is no bucket's default.
    expect(config.foreignKmsKeyArn).toBe(KEYS.probe);
    expect(Object.values(config.targets).map((t) => ("kmsKeyArn" in t ? t.kmsKeyArn : undefined))).not.toContain(KEYS.probe);
    expect(config.targets.R4!.bucket).toBe("ledger-bench-r4-111111111111--usw2-az1--x-s3");
    expect(config.targets.R4!.bucket.length).toBeLessThanOrEqual(63);
  });
  test("names follow the templates' own derivation", () => {
    const names = resourceNames(INPUTS);
    const west = read("data-west.yaml");
    const east = read("data-east.yaml");
    const sub = (pattern: string) => pattern.replaceAll("${AWS::AccountId}", INPUTS.dataAccountId).replaceAll("${AWS::Region}", INPUTS.region).replace("${ExpressZoneA}", INPUTS.expressZoneA).replace("${ExpressZoneB}", INPUTS.expressZoneB).replace("${EastRegion}", INPUTS.eastRegion);
    for (const id of ["R2", "R3", "R4", "R5"] as const) {
      const pattern = new RegExp(`BucketName: !Sub "(ledger-bench-${id.toLowerCase()}-[^"]+)"`).exec(west)![1]!;
      expect(sub(pattern)).toBe(names[id]);
    }
    expect(/BucketName: !Sub "(ledger-bench-r1-[^"]+)"/.exec(east)![1]!.replace("${AWS::AccountId}", INPUTS.dataAccountId).replace("${AWS::Region}", INPUTS.eastRegion)).toBe(names.R1);
    // The client role's grant on R1 uses the same derivation.
    // The client role is its own stack, so it names every target by the same derivation rather than by reference.
    const role = read("client-role.yaml");
    const roleSub = (pattern: string) => sub(pattern).replace("${AWS::Partition}", "aws");
    for (const id of ["R1", "R2", "R3"] as const) expect(role.split("\n").map(roleSub)).toContain(`                  - !Sub "arn:aws:s3:::${names[id]}/bench/*"`);
    for (const id of ["R4", "R5"] as const) expect(role.split("\n").map(roleSub)).toContain(`                  - !Sub "arn:aws:s3express:${INPUTS.region}:${INPUTS.dataAccountId}:bucket/${names[id]}"`);
    expect(role).toContain(`table/${names.R6}`);
    expect(west).toContain(`TableName: ${names.R6}`);
    expect(role).toContain(`RoleName: ${names.clientRole}`);
    // The storage stacks hold no IAM, so an identity that may not create roles can deploy them.
    for (const name of ["data-west.yaml", "data-east.yaml"]) expect(read(name)).not.toContain("AWS::IAM::");
    expect(read("cluster-identity.yaml")).toContain(`role/${names.clientRole}`);
  });
});

describe("ledger-bench Job", () => {
  const template = read("job.template.yaml");
  test("renders every value, pins the deadline and runs without privileges", () => {
    const job = renderJob(template, INPUTS, { runId: "run-1", lane: "r2", profile: "standard", steps: "", bundleSha256: SHA });
    expect(job).not.toContain("${");
    expect(job).toContain("name: ledger-bench-run-1-r2");
    expect(job).toContain("activeDeadlineSeconds: 14400");
    // The finished Job is the results: it must outlive a night, not an hour.
    expect(job).toContain("ttlSecondsAfterFinished: 172800");
    expect(job).toContain(`{ name: LEDGER_BENCH_BUNDLE_SHA256, value: "${SHA}" }`);
    expect(job).toContain("backoffLimit: 0");
    expect(job).toContain(`image: ${INPUTS.image}`);
    expect(job).toContain('nodeSelector: {"example/role":"general"}');
    expect(job).toContain('limits: { cpu: "1", memory: "1536Mi" }');
    expect(job).toContain("readOnlyRootFilesystem: true");
    expect(job).toContain("affinity: {}");
    // With pods to avoid, the Job may never be scheduled onto their node.
    const apart = renderJob(template, { ...INPUTS, avoidPods: { namespaces: ["example-prod"], matchLabels: { "app.kubernetes.io/component": "ingest-edge" } } }, { runId: "run-1", lane: "r2", profile: "standard", steps: "", bundleSha256: SHA });
    expect(apart).toContain('affinity: {"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"topologyKey":"kubernetes.io/hostname","namespaces":["example-prod"],"labelSelector":{"matchLabels":{"app.kubernetes.io/component":"ingest-edge"}}}]}}');
    expect(job).toContain("automountServiceAccountToken: false");
    expect(job).toContain("namespace: ledger-bench");
  });
  test("refuses a run id, lane, profile or step list that could alter the manifest", () => {
    const render = (p: Partial<Parameters<typeof renderJob>[2]>) => () => renderJob(template, INPUTS, { runId: "run-1", lane: "r2", profile: "standard", steps: "", bundleSha256: SHA, ...p });
    expect(render({ runId: 'x"\n  hostNetwork: true' })).toThrow(/run id/);
    expect(render({ lane: "prod" as never })).toThrow(/lane/);
    expect(render({ profile: "huge" })).toThrow(/profile/);
    expect(render({ steps: "1;rm" })).toThrow(/steps/);
    expect(render({ runId: "run-1-" })).toThrow(/run id/);
    expect(render({ profile: "constructor" })).toThrow(/profile/);
    expect(render({ bundleSha256: 'x"' })).toThrow(/bundle hash/);
  });
});

describe("ledger-bench templates", () => {
  const west = read("data-west.yaml");
  const identity = read("cluster-identity.yaml");
  test("carry no account id, ARN of a real account, or bucket name", () => {
    for (const name of ["data-west.yaml", "data-east.yaml", "client-role.yaml", "cluster-identity.yaml", "job.template.yaml", "namespace.yaml"]) expect(read(name)).not.toMatch(/\d{12}/);
  });
  test("the pod role can only assume the client role, and only the bench ServiceAccount gets it", () => {
    const policies = identity.slice(identity.indexOf("      Policies:"), identity.indexOf("      Tags:"));
    expect(policies.match(/Action:/g)!.length).toBe(1);
    // TagSession with AssumeRole: Pod Identity session tags are transitive, and the hop fails without it.
    expect(policies).toContain('Action: ["sts:AssumeRole", "sts:TagSession"]');
    expect(policies).toContain("role/ledger-bench-client");
    expect(policies).not.toContain("*");
    expect(identity).toContain("AllowedValues: [ledger-bench]");
    expect(identity).toContain('"aws:RequestTag/kubernetes-namespace": !Ref Namespace');
    expect(identity).toContain('"aws:RequestTag/kubernetes-service-account": !Ref ServiceAccount');
  });
  test("the client role is trusted only by that pod role and reaches only bench resources", () => {
    const role = read("client-role.yaml");
    const trust = role.slice(role.indexOf("      AssumeRolePolicyDocument:"), role.indexOf("        - PolicyName: ledger-bench-targets"));
    expect(trust).toContain('Action: ["sts:AssumeRole", "sts:TagSession"]');
    expect(trust.match(/Effect: Allow/g)!.length).toBe(1);
    expect(role).toContain('"aws:PrincipalArn": !Sub "arn:${AWS::Partition}:iam::${ClusterAccountId}:role/${PodRoleName}"');
    const policy = role.slice(role.indexOf("        - PolicyName: ledger-bench-targets"), role.indexOf("Outputs:"));
    // Every S3 object grant is under bench/, every bucket is a ledger-bench bucket, KMS is the tagged bench keys only.
    for (const line of policy.split("\n").filter((l) => l.includes("s3:::") || l.includes(":bucket/"))) expect(line).toMatch(/ledger-bench-r[1-5]-/);
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
    // The probe key is a key of its own: no bucket's encryption refers to it.
    expect(west).toContain("ProbeKeyArn: { Value: !GetAtt ProbeKey.Arn }");
    expect(west).not.toContain("KMSMasterKeyID: !GetAtt ProbeKey.Arn");
  });
  test("the pod cannot open a connection to another pod: every private and shared range is excluded", () => {
    const ns = read("namespace.yaml");
    for (const cidr of ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "169.254.0.0/16"]) expect(ns).toContain(cidr);
    expect(ns).toContain("cidr: 169.254.170.23/32");
    expect(ns).toContain("ingress: []");
    expect(ns).toContain("pod-security.kubernetes.io/enforce: restricted");
  });
});

describe("ledger-bench ops guards", () => {
  const entry = (run: string, lane: LedgerEntry["lane"], usd: number): LedgerEntry => ({ run, lane, profile: "standard", usd });
  test("the spend ledger holds the approved cap over every run together", () => {
    const lanes = ["r1", "r2", "r3", "express", "dynamo"] as const;
    let ledger: LedgerEntry[] = [];
    for (const run of ["smoke-1", "a", "b", "c"]) ledger = ledgerAdd(ledger, run, run === "smoke-1" ? "smoke" : "standard", [...lanes]);
    expect(ledger.length).toBe(20);
    // Three repeats and a smoke run fit; a fourth repeat, or one more lane, does not.
    expect(() => ledgerAdd(ledger, "d", "standard", [...lanes])).toThrow(/over the approved 300 USD/);
    expect(() => ledgerAdd(ledger, "d", "standard", ["r2"])).toThrow(/Nothing was started/);
    expect(ledgerAdd(ledger, "smoke-2", "smoke", ["r2"]).length).toBe(21);
    expect(() => ledgerAdd([], "a", "constructor", ["r2"])).toThrow(/profile/);
    expect(ledger.find((e) => e.run === "a" && e.lane === "r1")!.usd).toBeCloseTo(STANDARD.requestBudgetPerLane * 0.000008, 1);
  });
  test("teardown refuses while a lane's results are not collected", () => {
    const dir = mkdtempSync(join(tmpdir(), "ledger-bench-test-"));
    const ledger = [entry("a", "r1", 1), entry("a", "r2", 1), entry("a", "r3", 1)];
    writeFileSync(join(dir, "a-r1.jsonl"), '{"ledger_bench":1,"event":"end"}\n');
    writeFileSync(join(dir, "a-r2.jsonl"), '{"ledger_bench":1,"event":"start"}\n'); // collected mid-run
    expect(uncollected(ledger, dir)).toEqual(["a-r2", "a-r3"]);
    expect(uncollected(ledger, join(dir, "missing"))).toEqual(["a-r1", "a-r2", "a-r3"]);
    expect(uncollected([entry("a", "r1", 1)], dir)).toEqual([]);
  });
  test("verify is clean only when every lookup said gone and only keys pending deletion remain", () => {
    const gone = [{ what: "a", gone: true }, { what: "b", gone: true }];
    expect(isClean(gone, [])).toBe(true);
    expect(isClean(gone, [{ region: "us-west-2", type: "kms", state: "PendingDeletion" }])).toBe(true);
    expect(isClean(gone, [{ region: "us-west-2", type: "kms", state: "Enabled" }])).toBe(false);
    expect(isClean(gone, [{ region: "us-west-2", type: "s3", state: "exists" }])).toBe(false);
    expect(isClean([...gone, { what: "c", gone: false, detail: "lookup failed" }], [])).toBe(false);
    // No check at all proves nothing.
    expect(isClean([], [])).toBe(false);
  });
  test("results and inputs are kept out of the repository, also through a symlink", () => {
    expect(insideRepo(join(import.meta.dir, "new-results-dir"))).toBe(true);
    const link = join(mkdtempSync(join(tmpdir(), "ledger-bench-test-")), "link");
    symlinkSync(join(import.meta.dir, "..", "ledger-bench"), link);
    expect(insideRepo(join(link, "results"))).toBe(true);
    expect(insideRepo(tmpdir())).toBe(false);
  });
  test("the bundle limit is the ConfigMap's, under 1 MiB", () => {
    expect(BUNDLE_MAX_BYTES).toBeLessThan(1024 * 1024);
  });
});
