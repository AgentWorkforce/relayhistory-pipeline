import {test,expect} from "bun:test";
import {readFileSync} from "node:fs";
const t=JSON.parse(readFileSync(new URL("../buffered-gc.template.json",import.meta.url),"utf8"));
test("one default-off switch arms both permanent Lambda and minute schedule",()=>{
 expect(t.Parameters.EnableGc.Default).toBe("false");expect(t.Resources.Function.Properties.Environment.Variables.INGEST_GC_ENABLED).toEqual({Ref:"EnableGc"});
 expect(t.Resources.Schedule.Properties.State).toEqual({"Fn::If":["Armed","ENABLED","DISABLED"]});
 expect(t.Resources.Schedule.Properties.ScheduleExpression).toBe("rate(1 minute)");
 expect(t.Resources.Function.Properties.Timeout).toBe(60);expect(t.Resources.Function.Properties.ReservedConcurrentExecutions).toBe(1);
 expect(t.Resources.Function.Properties.Code.S3ObjectVersion).toEqual({Ref:"CodeVersion"});
 expect(t.Resources.HealthyAlarm.Properties.TreatMissingData).toEqual({"Fn::If":["Armed","breaching","notBreaching"]});
});
test("dedicated role has no bucket-wide listing, canonical writes, broad secrets or plain deletion",()=>{
 const s=t.Resources.Role.Properties.Policies[0].PolicyDocument.Statement,actions=s.flatMap((x:any)=>x.Action);
 for(const action of ["s3:ListBucket","s3:DeleteObject","secretsmanager:ListSecrets","iam:PassRole"])expect(actions).not.toContain(action);
 for(const p of s.filter((x:any)=>x.Action.some((a:string)=>a.startsWith("s3:")&&a!=="s3:ListBucketVersions")))expect(p.Resource["Fn::Sub"]).toBe("arn:aws:s3:::${BufferBucket}/96bulls/*/buffered-batches/*");
 const put=s.find((x:any)=>x.Action.includes("s3:PutObject"));expect(put.Condition.StringEquals["s3:x-amz-server-side-encryption"]).toBe("aws:kms");
 expect(s.find((x:any)=>x.Action.includes("secretsmanager:GetSecretValue")).Resource).toEqual({Ref:"SecretArn"});
 const list=s.find((x:any)=>x.Action.includes("s3:ListBucketVersions"));expect(list.Condition.StringLike["s3:prefix"]).toBe("96bulls/*/buffered-batches/*");
});
