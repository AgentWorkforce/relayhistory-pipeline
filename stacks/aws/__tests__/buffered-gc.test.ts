import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { stagedKey } from "../../../src/session-ingest/staging.ts";

const t = JSON.parse(readFileSync(new URL("../buffered-gc.template.json", import.meta.url), "utf8"));
const statements = t.Resources.Role.Properties.Policies[0].PolicyDocument.Statement;
const parameters: Record<string, string> = {
  BufferBucket: "gc-scope-fixture",
  KmsKeyArn: "arn:aws:kms:us-east-1:941433935742:key/11111111-1111-4111-8111-111111111111",
  SecretArn: "arn:aws:secretsmanager:us-east-1:941433935742:secret:96bulls/proof/buffered-gc-abcdef",
};
const value = (v: any): string => typeof v === "string" ? v.replace(/\$\{([^}]+)\}/g, (_, k) => parameters[k]!) : v.Ref ? parameters[v.Ref]! : value(v["Fn::Sub"]);
// IAM wildcards span '/', unlike filesystem globs. This checks only the finite
// identity-policy grammar here; effective AWS/resource-policy proof is separate.
const glob = (pattern: string, text: string) => new RegExp("^" + pattern.split("*").map(s => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join(".*") + "$").test(text);
function decision(action: string, resource: string, context: Record<string, string> = {}) {
  const matches = statements.filter((s: any) => {
    if (!s.Action.some((p: string) => glob(p, action)) || ![].concat(s.Resource).some((r: any) => glob(value(r), resource))) return false;
    return Object.entries(s.Condition ?? {}).every(([op, entries]) => {
      if (op !== "StringEquals" && op !== "StringLike") throw new Error("unsupported policy condition");
      return Object.entries(entries as object).every(([key, expected]) => typeof context[key] === "string" && (op === "StringEquals" ? context[key] === value(expected) : glob(value(expected), context[key]!)));
    });
  });
  return matches.some((s: any) => s.Effect === "Deny") ? "explicitDeny" : matches.some((s: any) => s.Effect === "Allow") ? "allowed" : "implicitDeny";
}
const key = stagedKey("96bulls", { org_id: "org", workspace_id: "ws/escaped", origin_id: "origin", user_id: "user", batch_digest: "a".repeat(64), claim_token: "b".repeat(64) }, "c".repeat(64), "buffered-batches");
const arn = (key: string) => `arn:aws:s3:::${parameters.BufferBucket}/${key}`;
const canonical = "96bulls/org/upload-generations/g=11111111-1111-4111-8111-111111111111/v1/ws=w/m=srv_1/h=h/d=d/s=s/publications/22222222-2222-4222-8222-222222222222/derived/";

test("one dormant switch arms the permanent job and schedule", () => {
  expect(t.Parameters.EnableGc.Default).toBe("false");
  expect(t.Resources.Function.Properties.Environment.Variables.INGEST_GC_ENABLED).toEqual({ Ref: "EnableGc" });
  expect(t.Resources.Schedule.Properties.State).toEqual({ "Fn::If": ["Armed", "ENABLED", "DISABLED"] });
  expect(t.Resources.Schedule.Properties.ScheduleExpression).toBe("rate(1 minute)");
  expect(t.Resources.Function.Properties.Timeout).toBe(60);
  expect(t.Resources.Function.Properties.ReservedConcurrentExecutions).toBe(1);
  expect(t.Resources.Function.Properties.Code.S3ObjectVersion).toEqual({ Ref: "CodeVersion" });
  expect(t.Resources.HealthyAlarm.Properties.TreatMissingData).toEqual({ "Fn::If": ["Armed", "breaching", "notBreaching"] });
});
test("actual producer stage permits needed operations but no plain deletion", () => {
  for (const action of ["s3:GetObject", "s3:DeleteObjectVersion"]) expect(decision(action, arn(key))).toBe("allowed");
  expect(decision("s3:PutObject", arn(key), { "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": parameters.KmsKeyArn! })).toBe("allowed");
  expect(decision("s3:PutObject", arn(key))).toBe("implicitDeny");
  expect(decision("s3:DeleteObject", arn(key))).toBe("implicitDeny");
  const kmsContext = {"kms:CallerAccount":"941433935742", "kms:ViaService":"s3.us-east-1.amazonaws.com", "kms:EncryptionContext:aws:s3:arn":arn(key)};
  expect(decision("kms:Decrypt", parameters.KmsKeyArn!, kmsContext)).toBe("allowed");
  expect(decision("kms:GenerateDataKey", parameters.KmsKeyArn!, kmsContext)).toBe("allowed");
  expect(decision("kms:Decrypt", parameters.KmsKeyArn!, {...kmsContext,"kms:ViaService":"kms.us-east-1.amazonaws.com"})).toBe("implicitDeny");
  expect(decision("s3:ListBucketVersions", `arn:aws:s3:::${parameters.BufferBucket}`, { "s3:prefix": key })).toBe("allowed");
});
test("canonical derived and staged-looking canonical paths remain explicitly denied", () => {
  const paths = [canonical + "buffered-batches/v1/ws=w/m=srv_1/b=lookalike.staged.ndjson.zst", canonical + "buffered-batches/33333333-3333-4333-8333-333333333333.bin"];
  for (const path of paths) {
    for (const action of ["s3:GetObject", "s3:PutObject", "s3:DeleteObjectVersion"]) expect(decision(action, arn(path))).toBe("explicitDeny");
    expect(decision("s3:ListBucketVersions", `arn:aws:s3:::${parameters.BufferBucket}`, { "s3:prefix": path })).toBe("explicitDeny");
    expect(decision("kms:Decrypt", parameters.KmsKeyArn!, { "kms:CallerAccount": "941433935742", "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:EncryptionContext:aws:s3:arn": arn(path) })).toBe("explicitDeny");
  }
});
test("unregistered buffered objects, legacy objects, broad secrets and bucket listing stay outside permissions", () => {
  for (const path of ["96bulls/org/buffered-batches/unregistered.bin", "96bulls/org/v1/manifest.json", "96bulls/org/live-batches/v1/ws=w/m=srv_1/b=old.staged.ndjson.zst"]) expect(decision("s3:GetObject", arn(path))).toBe("implicitDeny");
  expect(decision("s3:ListBucket", `arn:aws:s3:::${parameters.BufferBucket}`)).toBe("implicitDeny");
  expect(decision("secretsmanager:GetSecretValue", parameters.SecretArn!)).toBe("allowed");
  expect(decision("secretsmanager:GetSecretValue", parameters.SecretArn! + "other")).toBe("implicitDeny");
  expect(decision("iam:PassRole", "*")).toBe("implicitDeny");
});
