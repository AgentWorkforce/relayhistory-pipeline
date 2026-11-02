import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { legacyPodsRoleProps, podRoleProps } from "../eks.ts";
import { NAMESPACE, podIdentityTrust, podRoles } from "../hardening.ts";
import {
  AUTO_MODE_CLUSTER_POLICIES,
  AUTO_MODE_NODE_POLICIES,
  PIPPEN_BUCKET_PREFIX,
  PIPPEN_SANDBOX_TAG,
  PIPPEN_WORKLOAD_ROLE_PATH,
  autoModeRoleProps,
  ingressClassTags,
  stageBucketName,
  stageBudgetName,
} from "../pippen-sandbox.ts";
import { STORAGE_CLASS, storageClassManifest } from "../proof-addons.ts";
import { SANDBOX_BACKUP_BUCKET, SANDBOX_LOCKER_BUCKET } from "../../../src/proof/aws/fence.ts";

const src = (f: string) => readFileSync(fileURLToPath(new URL(`../${f}`, import.meta.url)), "utf8");
const BOUNDARY = "arn:aws:iam::361835732085:policy/pippen-agent-session-boundary";
const SBX_TAGS = { "96bulls:stack": "aws-proof-sandbox", "96bulls:component": "eks", "96bulls:repo": "AgentWorkforce/sessions-pipeline", ...PIPPEN_SANDBOX_TAG };
const DEV_TAGS = { "96bulls:stack": "aws-sessions-dev", "96bulls:component": "eks", "96bulls:repo": "AgentWorkforce/sessions-pipeline" };
const refs = { lockerBucketArn: "arn:aws:s3:::b", lockerKmsKeyArn: "arn:aws:kms:us-west-2:1:key/k", lockerRegion: "us-west-2" };
const CLUSTER_ARN = "arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-sessions-dev";

describe("pippen sandbox: other stages are byte-for-byte unchanged", () => {
  it("bucket names: `96bulls-<stage>-<base>-<account>` exactly as before", () => {
    expect(stageBucketName("aws-sessions-dev", "locker", "500268966557", false)).toBe("96bulls-aws-sessions-dev-locker-500268966557");
    expect(stageBucketName("aws-proof", "trail", "500268966557", false)).toBe("96bulls-aws-proof-trail-500268966557");
    expect(stageBucketName("aws-proof", "alb-logs", "500268966557", false)).toBe("96bulls-aws-proof-alb-logs-500268966557");
    for (const stage of ["aws-proof", "aws-sessions-dev", "aws-prod", "aws-sandbox-claude-code-agent"]) expect(stageBudgetName(stage, false)).toBe(`96bulls-${stage}-monthly`);
  });

  it("Pod Identity roles: the same props as the inline literal they replace (no path key at all)", () => {
    for (const bounded of [undefined, "arn:aws:iam::500268966557:policy/96bulls-sandbox-boundary"]) {
      for (const r of podRoles(refs)) {
        const i = { stage: "aws-sessions-dev", accountId: "500268966557", tags: DEV_TAGS, ...(bounded ? { permissionsBoundaryArn: bounded } : {}) };
        const before = {
          roleName: `96bulls-${i.stage}-${r.id}`,
          description: `96bulls ${i.stage}: ${r.description} (Pod Identity ${NAMESPACE}/${r.serviceAccount})`,
          assumeRolePolicyDocument: podIdentityTrust(i.accountId, CLUSTER_ARN),
          inlinePolicies: { [r.policyName]: r.policy },
          maxSessionDuration: "1 hour",
          ...(bounded ? { permissionsBoundary: bounded } : {}),
          tags: { ...DEV_TAGS, "96bulls:service-account": `${NAMESPACE}/${r.serviceAccount}`, ...(bounded ? { "96bulls:bounded": "true" } : {}) },
        };
        expect(JSON.stringify(podRoleProps(i, r, CLUSTER_ARN))).toBe(JSON.stringify(before));
      }
    }
  });

  it("the legacy pods role, the StorageClass and the cluster props are unchanged without pippenSandbox", () => {
    const legacy = legacyPodsRoleProps({ stage: "aws-proof", lockerBucketArn: "arn:aws:s3:::b", lockerKmsKeyArn: "k", tags: DEV_TAGS });
    expect("path" in legacy).toBe(false);
    expect(Object.keys(legacy)).toEqual(["roleName", "description", "assumeRolePolicyDocument", "inlinePolicies", "maxSessionDuration", "tags"]);
    for (const reclaim of ["Retain", "Delete"] as const) {
      expect(storageClassManifest(reclaim)).toEqual({
        apiVersion: "storage.k8s.io/v1",
        kind: "StorageClass",
        metadata: { name: STORAGE_CLASS },
        provisioner: "ebs.csi.eks.amazonaws.com",
        volumeBindingMode: "WaitForFirstConsumer",
        allowVolumeExpansion: true,
        reclaimPolicy: reclaim,
        parameters: { type: "gp3", encrypted: "true" },
      });
    }
    expect(storageClassManifest()).toEqual(storageClassManifest("Retain"));
    // Cluster roles, budget tags, IngressClassParams tags and the StorageClass tags are all behind the flag.
    const eks = src("eks.ts");
    expect(eks).toContain("const autoModeRoles = i.pippenSandbox");
    expect(eks).toContain("...(autoModeRoles\n");
    expect(eks).toContain('storageClassManifest(i.purgeOnDestroy ? "Delete" : "Retain", i.pippenSandbox ? PIPPEN_SANDBOX_TAG : undefined)');
    expect(src("ingress.ts")).toContain("...(i.pippenSandbox ? { tags: ingressClassTags(PIPPEN_SANDBOX_TAG) } : {}),");
    const stack = src("alchemy.aws.run.ts");
    expect(stack).toContain('...(pippen ? { tags: tags(stage, "budget") } : {}),');
    expect(stack).toContain("const pippen = Boolean(inputs.pippenSandbox);");
    expect(stack).toContain("permissionsBoundaryArn: inputs.pippenSandbox.permissionsBoundaryArn, pippenSandbox: true");
  });
});

describe("pippen sandbox: what the stage changes", () => {
  it("buckets are pippen-sbx-*, the only ones pippen-sweeper may delete, and the harness fence agrees", () => {
    expect(stageBucketName("aws-proof-sandbox", "locker", "361835732085", true)).toBe("pippen-sbx-96bulls-aws-proof-sandbox-locker-361835732085");
    expect(stageBucketName("aws-proof-sandbox", "trail", "361835732085", true)).toBe("pippen-sbx-96bulls-aws-proof-sandbox-trail-361835732085");
    expect(stageBucketName("aws-proof-sandbox", "alb-logs", "361835732085", true)).toBe("pippen-sbx-96bulls-aws-proof-sandbox-alb-logs-361835732085");
    expect(SANDBOX_LOCKER_BUCKET).toBe(stageBucketName("aws-proof-sandbox", "locker", "361835732085", true));
    expect(SANDBOX_BACKUP_BUCKET.startsWith(PIPPEN_BUCKET_PREFIX)).toBe(true);
    expect(() => stageBucketName("aws-proof-sandbox-with-a-much-longer-stage-name", "alb-logs", "361835732085", true)).toThrow(/63 characters/);
  });

  it("EVERY bucket name the sandbox stage and its proof render is a valid S3 name starting pippen-sbx-", () => {
    // S3 general purpose bucket naming: 3-63 chars, lowercase letters, digits, hyphens and dots, alphanumeric at both ends.
    const valid = (b: string) => b.length >= 3 && b.length <= 63 && /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(b) && !b.includes("..") && !/^\d+\.\d+\.\d+\.\d+$/.test(b);
    const stack = ["locker", "trail", "alb-logs"].map((b) => stageBucketName("aws-proof-sandbox", b, "361835732085", true));
    const all = [...stack, SANDBOX_LOCKER_BUCKET, SANDBOX_BACKUP_BUCKET];
    for (const b of all) {
      expect(valid(b), `${b} (${b.length} chars)`).toBe(true);
      expect(b.startsWith(PIPPEN_BUCKET_PREFIX), b).toBe(true);
    }
    expect(new Set(all).size).toBe(4); // locker shared by stack and harness; the rest unique
    expect(SANDBOX_BACKUP_BUCKET).toBe("pippen-sbx-96bulls-aws-proof-sandbox-backups-361835732085");
    // The bucket names the stack builds are exactly stageBucketName's (every bucketName in the stack goes through it).
    for (const f of ["alchemy.aws.run.ts", "ingress.ts", "eks.ts"]) {
      for (const m of src(f).matchAll(/bucketName: ([^,\n]+)/g)) expect(["lockerBucketName", "trailBucketName", "logBucketName"], `${f}: ${m[1]}`).toContain(m[1]!.trim());
    }
    // The old name really was invalid (regression).
    expect(valid("pippen-sbx-96bulls-aws-proof-sandbox-hoopdb-backups-361835732085")).toBe(false);
  });

  it("the budget is pippen-sbx-* (Budgets is scoped by name in the pippen boundary; ecr.pullAccounts refusal: inputs.test.ts)", () => {
    expect(stageBudgetName("aws-proof-sandbox", true)).toBe("pippen-sbx-96bulls-aws-proof-sandbox-monthly");
  });

  it("every Pod Identity role and the legacy role are under /pippen/workload/ with the boundary", () => {
    const i = { stage: "aws-proof-sandbox", accountId: "361835732085", tags: SBX_TAGS, permissionsBoundaryArn: BOUNDARY, pippenSandbox: true };
    for (const r of podRoles(refs)) {
      const p = podRoleProps(i, r, "arn:aws:eks:us-west-2:361835732085:cluster/96bulls-aws-proof-sandbox");
      expect(p.path).toBe(PIPPEN_WORKLOAD_ROLE_PATH);
      expect(p.permissionsBoundary).toBe(BOUNDARY);
      expect(p.tags["pippen:sandbox"]).toBe("true");
    }
    const legacy = legacyPodsRoleProps({ ...i, lockerBucketArn: "arn:aws:s3:::b", lockerKmsKeyArn: "k" });
    expect(legacy.path).toBe(PIPPEN_WORKLOAD_ROLE_PATH);
    expect(legacy.permissionsBoundary).toBe(BOUNDARY);
  });

  it("the Auto Mode cluster and node roles are the stage's own, bounded, with Alchemy's Auto Mode policies", () => {
    const { cluster, node } = autoModeRoleProps({ stage: "aws-proof-sandbox", permissionsBoundaryArn: BOUNDARY, tags: SBX_TAGS });
    expect(cluster.roleName).toBe("96bulls-aws-proof-sandbox-cluster");
    expect(node.roleName).toBe("96bulls-aws-proof-sandbox-node");
    for (const r of [cluster, node]) {
      expect(r.path).toBe(PIPPEN_WORKLOAD_ROLE_PATH);
      expect(r.permissionsBoundary).toBe(BOUNDARY);
      expect(r.tags["pippen:sandbox"]).toBe("true");
      expect(r.tags["96bulls:bounded"]).toBe("true");
    }
    expect(cluster.assumeRolePolicyDocument.Statement).toEqual([{ Effect: "Allow", Principal: { Service: "eks.amazonaws.com" }, Action: ["sts:AssumeRole", "sts:TagSession"] }]);
    expect(node.assumeRolePolicyDocument.Statement).toEqual([{ Effect: "Allow", Principal: { Service: "ec2.amazonaws.com" }, Action: ["sts:AssumeRole"] }]);
    // The same lists Alchemy's own Auto Mode roles attach (@alchemy.run/aws EKS/Cluster.ts).
    const alchemy = readFileSync(fileURLToPath(new URL("../../../node_modules/@alchemy.run/aws/src/EKS/Cluster.ts", import.meta.url)), "utf8");
    const list = (name: string) => [...(alchemy.split(`const ${name} = [`)[1]!.split("];")[0]!.matchAll(/"([^"]+)"/g))].map((m) => m[1]);
    expect(cluster.managedPolicyArns).toEqual(list("autoClusterManagedPolicyArns"));
    expect(node.managedPolicyArns).toEqual(list("autoNodeManagedPolicyArns"));
    expect([...AUTO_MODE_CLUSTER_POLICIES, ...AUTO_MODE_NODE_POLICIES].length).toBe(7);
  });

  it("the ALB and EBS volumes Auto Mode creates carry pippen:sandbox", () => {
    expect(ingressClassTags(PIPPEN_SANDBOX_TAG)).toEqual([{ key: "pippen:sandbox", value: "true" }]);
    expect(storageClassManifest("Delete", PIPPEN_SANDBOX_TAG).parameters).toEqual({ type: "gp3", encrypted: "true", tagSpecification_1: "pippen:sandbox=true" });
  });

  it("every AWS resource the stage declares passes the stage tags (pippen:sandbox in a sandbox), except untaggable KMS aliases", () => {
    // Props given by a helper that sets `tags` from its input (checked above).
    const viaHelper = /^(podRoleProps|legacyPodsRoleProps|props\.(cluster|node))\(?/;
    const seen: string[] = [];
    for (const file of ["alchemy.aws.run.ts", "eks.ts", "ingress.ts"]) {
      const text = src(file);
      for (const m of text.matchAll(/AWS\.(\w+)\.(\w+)\(\s*([^,()]+),\s*/g)) {
        const [, service, kind, id] = m;
        seen.push(`${service}.${kind}`);
        if (`${service}.${kind}` === "KMS.Alias") continue;
        // The call's argument text, to its closing parenthesis.
        let depth = 1;
        let j = m.index! + m[0].length;
        const start = j;
        for (; j < text.length && depth > 0; j++) depth += text[j] === "(" ? 1 : text[j] === ")" ? -1 : 0;
        const props = text.slice(start, j - 1);
        if (viaHelper.test(props.trim())) continue;
        // The transcript-store stage and the GitHub OIDC deployer never run with pippenSandbox.
        expect(props.includes("tags:") || props.includes("tags,") || props.includes('tags(stage, "budget")'), `${file}: AWS.${service}.${kind}(${id}) has no tags`).toBe(true);
      }
    }
    for (const kind of ["KMS.Key", "S3.Bucket", "CloudTrail.Trail", "Budgets.Budget", "ECR.Repository", "EC2.Network", "Logs.LogGroup", "EKS.Cluster", "IAM.Role", "EKS.PodIdentityAssociation", "WAFv2.WebACL", "ACM.Certificate"]) {
      expect(seen, kind).toContain(kind);
    }
  });

  it("the GitHub OIDC provider and deployer role are skipped in a Pippen sandbox", () => {
    expect(src("alchemy.aws.run.ts")).toContain("const deployer = isSandbox || inputs.accountShared || inputs.pippenSandbox\n      ? undefined");
  });

  it("refuses a Pippen sandbox EKS stage without the boundary", () => {
    expect(src("eks.ts")).toContain("if (i.pippenSandbox && !i.permissionsBoundaryArn)");
  });
});
