import{test,expect}from"bun:test";
import{readFileSync}from"node:fs";
const t=JSON.parse(readFileSync(new URL("../full-model.template.json",import.meta.url),"utf8"));
test("permanent model runtime is dormant with version-addressed code and finite serial work",()=>{
 expect(t.Parameters.EnableModel.Default).toBe("false");expect(t.Parameters.Model.Default).toBe("off");
 const p=t.Resources.Function.Properties;
 expect(p.Code.S3ObjectVersion).toEqual({Ref:"CodeVersion"});expect(p.Runtime).toBe("nodejs24.x");
 expect(p.Timeout).toBe(300);expect(p.ReservedConcurrentExecutions).toBe(1);
 expect(p.Environment.Variables.SESSION_MODEL_EXPECT_GENERATION).toEqual({Ref:"ExpectedGeneration"});
 expect(t.Resources.Schedule.Properties.State["Fn::If"]).toEqual(["Armed","ENABLED","DISABLED"]);
 expect(JSON.parse(t.Resources.Schedule.Properties.Targets[0].Input)).toEqual({fullModel:true});
 expect(t.Resources.Permission.Properties.SourceAccount).toBe("941433935742");
});
test("model role has no data mutation, delete, listing, admin or secret-write authority",()=>{
 const statements=t.Resources.Role.Properties.Policies.flatMap((p:any)=>p.PolicyDocument.Statement),actions=new Set(statements.flatMap((s:any)=>s.Action));
 expect([...actions].sort()).toEqual(["kms:Decrypt","kms:GenerateDataKey","logs:CreateLogStream","logs:PutLogEvents","s3:GetObject","s3:PutObject","secretsmanager:GetSecretValue"].sort());
 const gets=statements.find((s:any)=>s.Action.includes("s3:GetObject")).Resource.map((x:any)=>x["Fn::Sub"]);
 const puts=statements.find((s:any)=>s.Action.includes("s3:PutObject"));
 expect(gets).toHaveLength(4);expect(gets.every((s:string)=>s.includes("/upload-generations/")&&s.includes("/derived/"))).toBe(true);
 expect(puts.Resource.map((x:any)=>x["Fn::Sub"].split("/derived/")[1]).sort()).toEqual(["bob-slice/*.bin","summary-long/*.bin","summary-short/*.bin"]);
 expect(puts.Condition.StringEquals["s3:x-amz-server-side-encryption"]).toBe("aws:kms");
 expect(puts.Condition.StringEquals["s3:x-amz-server-side-encryption-aws-kms-key-id"]).toEqual({Ref:"KmsKeyArn"});
 expect(statements.find((s:any)=>s.Action.includes("secretsmanager:GetSecretValue")).Resource).toEqual({Ref:"SecretArn"});
});
