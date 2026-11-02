import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { controlPlaneLogGroupName, podRoles } from "../hardening.ts";
import { ingestWafIdentity, ingestWafRules } from "../proof-addons.ts";
import { legacyPodsRoleProps } from "../eks.ts";

/**
 * AWS restricts names and descriptions per service, and a violation only
 * surfaces as a ValidationException halfway through an apply (Stage A: a
 * parenthesis in the WAF description). Every such string the stack builds is
 * checked here against the service's documented pattern, for every stage shape.
 */
const AWS = {
  // WAFv2 CreateWebACL / Rule
  wafName: /^[\w-]{1,128}$/,
  wafDescription: /^[\w+=:#@/\-,.][\w+=:#@/\-,.\s]+[\w+=:#@/\-,.]$/,
  wafMetricName: /^[\w#:.\-/]{1,255}$/,
  // IAM CreateRole
  iamRoleName: /^[\w+=,.@-]{1,64}$/,
  iamDescription: /^[\u0009\u000A\u000D -~¡-ÿ]{0,1000}$/,
  // KMS CreateAlias
  kmsAlias: /^alias\/[a-zA-Z0-9/_-]{1,250}$/,
  // CloudWatch Logs CreateLogGroup
  logGroup: /^[.\-_/#A-Za-z0-9]{1,512}$/,
  // Budgets: no ':' or '\', at most 100 characters
  budgetName: /^[^:\\]{1,100}$/,
  // S3 bucket names
  bucket: /^(?!xn--)(?!.*-s3alias$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/,
};

const STAGES = ["aws-proof", "aws-proof-sandbox", "aws-sessions-dev", "aws-sandbox-claude-code-agent", "aws-prod"];
const ACCOUNT = "500268966557";
const src = (f: string) => readFileSync(fileURLToPath(new URL(`../${f}`, import.meta.url)), "utf8");
const refs = { lockerBucketArn: "arn:aws:s3:::b", lockerKmsKeyArn: "arn:aws:kms:us-west-2:1:key/k", lockerRegion: "us-west-2" };

describe("AWS name and description charsets", () => {
  for (const stage of STAGES) {
    it(`${stage}: WAF web ACL name, description, metric and every rule name/metric`, () => {
      const w = ingestWafIdentity(stage);
      expect(w.webAclName).toMatch(AWS.wafName);
      expect(w.description).toMatch(AWS.wafDescription);
      expect(w.description.length).toBeLessThanOrEqual(256);
      expect(w.metricName).toMatch(AWS.wafMetricName);
      for (const r of ingestWafRules(2000) as { Name: string; VisibilityConfig: { MetricName: string } }[]) {
        expect(r.Name).toMatch(AWS.wafName);
        expect(r.VisibilityConfig.MetricName).toMatch(AWS.wafMetricName);
      }
    });

    it(`${stage}: IAM role names and descriptions, KMS aliases, log group, budget, bucket names`, () => {
      for (const r of podRoles(refs)) {
        expect(`96bulls-${stage}-${r.id}`).toMatch(AWS.iamRoleName);
        expect(`96bulls ${stage}: ${r.description} (Pod Identity 96bulls/${r.serviceAccount})`).toMatch(AWS.iamDescription);
      }
      const legacy = legacyPodsRoleProps({ stage, lockerBucketArn: "a", lockerKmsKeyArn: "k", tags: {} });
      expect(legacy.roleName).toMatch(AWS.iamRoleName);
      expect(legacy.description).toMatch(AWS.iamDescription);
      expect(`96bulls-${stage}-deployer`).toMatch(AWS.iamRoleName);
      expect(`96bulls ${stage}: ECR push of the services image, assumed only by AgentWorkforce/96BullSessions jobs in the image-publish environment`).toMatch(AWS.iamDescription);
      for (const a of ["locker", "trail"]) expect(`alias/96bulls-${stage}-${a}`).toMatch(AWS.kmsAlias);
      expect(controlPlaneLogGroupName(`96bulls-${stage}`)).toMatch(AWS.logGroup);
      expect(`96bulls-${stage}-monthly`).toMatch(AWS.budgetName);
      for (const b of ["locker", "trail", "alb-logs"]) {
        expect(`96bulls-${stage}-${b}-${ACCOUNT}`).toMatch(AWS.bucket);
        // Only the Pippen sandbox stage gets the pippen-sbx- prefix (stageBucketName).
        if (stage === "aws-proof-sandbox") expect(`pippen-sbx-96bulls-${stage}-${b}-361835732085`).toMatch(AWS.bucket);
      }
    });
  }

  it("the stack builds those strings with exactly these templates", () => {
    const ingress = src("ingress.ts");
    expect(ingress).toContain("webAclName: ingestWafIdentity(i.stage).webAclName,");
    expect(ingress).toContain("description: ingestWafIdentity(i.stage).description,");
    expect(ingress).toContain("MetricName: ingestWafIdentity(i.stage).metricName");
    // stageBucketName (pippen-sandbox.ts): `96bulls-<stage>-<base>-<account>`, pippen-sbx- first only in a Pippen sandbox.
    expect(ingress).toContain('const logBucketName = stageBucketName(i.stage, "alb-logs", i.accountId, Boolean(i.pippenSandbox));');
    const eks = src("eks.ts");
    expect(eks).toContain("roleName: `96bulls-${i.stage}-${r.id}`,");
    expect(eks).toContain("description: `96bulls ${i.stage}: ${r.description} (Pod Identity ${NAMESPACE}/${r.serviceAccount})`,");
    const stack = src("alchemy.aws.run.ts");
    expect(stack).toContain('const name = (base: string) => `96bulls-${stage}-${base}`;');
    expect(stack).toContain('const lockerBucketName = stageBucketName(stage, "locker", inputs.accountId, pippen);');
    expect(stack).toContain('const trailBucketName = stageBucketName(stage, "trail", inputs.accountId, pippen);');
    expect(stack).toContain('aliasName: `alias/${name("locker")}`');
    expect(stack).toContain('aliasName: `alias/${name("trail")}`');
    expect(stack).toContain("budgetName: stageBudgetName(stage, pippen),");
    expect(stack).toContain('roleName: name("deployer"),');
    // No other WAF description in the stack bypasses ingestWafIdentity.
    expect((ingress + eks + stack).match(/WAFv2\.(WebACL|RuleGroup|IPSet)\(/g)).toEqual(["WAFv2.WebACL("]);
  });

  it("the Stage A description was invalid (regression)", () => {
    expect("96bulls aws-proof: ingest ALB (managed rules, per-IP rate limit, operator routes blocked)").not.toMatch(AWS.wafDescription);
  });
});
