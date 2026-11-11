import { describe, expect, it } from "bun:test";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { bucketNotificationConfiguration, sessionPipelineNames } from "../session-pipeline.ts";
import {
  assertTranscriptStoreInputs, srvWriterObjects, transcriptBucketPolicy, transcriptKeyPolicy,
  transcriptReaderPolicy, transcriptSrvWriterPolicy, transcriptWriterPolicy,
  TRANSCRIPT_ACCOUNT_ID, TRANSCRIPT_REGION, TRANSCRIPT_STAGE,
} from "../transcript-store.ts";
import {
  UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS, UPLOAD_ADMISSION_VISIBILITY_TIMEOUT_SECONDS, UPLOAD_INBOX_NOTIFICATION_ID,
  UPLOAD_INBOX_PREFIX, UPLOAD_INBOX_RESOURCES, desktopUploadPolicy, desktopUploadTrustPolicy, parseHistoryUploadDirectInfra,
  uploadAdmissionPolicy, uploadAdmissionQueuePolicy, uploadInboxNames,
} from "../upload-inbox.ts";

// Synthetic identifiers only.
const accountId = "111111111111", region = "us-east-1", stage = "aws-synthetic";
const bucketName = `96bulls-${stage}-transcripts-${accountId}`, bucketArn = `arn:aws:s3:::${bucketName}`;
const keyArn = `arn:aws:kms:${region}:${accountId}:key/synthetic`, keyAlias = `alias/96bulls-${stage}-transcripts`;
const controlRoleArn = "arn:aws:iam::222222222222:role/synthetic-relay-control";
const eraserArn = `arn:aws:iam::${accountId}:user/96bulls/synthetic-eraser`;
const writerArn = `arn:aws:iam::${accountId}:user/96bulls/synthetic-writer`;
const readerArn = `arn:aws:iam::${accountId}:user/96bulls/synthetic-reader`;
const srvWriterArn = `arn:aws:iam::${accountId}:user/96bulls/synthetic-srv-writer`;
const pipelineRoleArn = `arn:aws:iam::${accountId}:role/synthetic-pipeline`;
const gcRoleArn = `arn:aws:iam::${accountId}:role/synthetic-buffered-gc`;
const u = uploadInboxNames(stage, accountId, region);
const roles = { uploadRoleArn: u.uploadRoleArn, consumerRoleArn: u.consumerRoleArn };

const inboxKey = `${bucketArn}/96bulls/.upload-inbox/org-a/v1/ws=w1/u=abc/0001.json.zst`;
const sessionKey = `${bucketArn}/96bulls/org-a/v1/ws=w1/m=m1/h=claude/d=2026/11/11/s=s1/manifest.json`;
const srvKey = `${bucketArn}/96bulls/org-a/v1/ws=w1/m=srv_abc/h=claude/d=2026/11/11/s=s1/manifest.json`;
const admissionKey = `${bucketArn}/96bulls/.admission-inbox/org-a/v1/ws=w1/m=srv_abc/r=x/a=1.ndjson.zst`;

// A small evaluator for the statements these policies use (same-account, no SCPs).
type Statement = Record<string, any>;
const many = (v: any): any[] => (Array.isArray(v) ? v : [v]);
const glob = (p: string, v: string) => new RegExp("^" + p.replace(/[.+^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*").replaceAll("?", ".") + "$", "s").test(v);
function matches(s: Statement, action: string, resource: string, ctx: Record<string, string>) {
  if (s.Principal && s.Principal !== "*" && !many(s.Principal.AWS).includes(ctx["aws:PrincipalArn"])) return false;
  if (s.Action && !many(s.Action).some((p) => glob(p.toLowerCase(), action.toLowerCase()))) return false;
  if (s.Resource && !many(s.Resource).some((p) => glob(p, resource))) return false;
  if (s.NotResource && many(s.NotResource).some((p) => glob(p, resource))) return false;
  return Object.entries(s.Condition ?? {}).every(([op, entries]) => Object.entries(entries as object).every(([key, value]) => {
    const expected = many(value), actual = ctx[key];
    if (op === "StringLike") return actual !== undefined && expected.some((p) => glob(p, actual));
    if (op === "StringEquals" || op === "ArnEquals") return expected.includes(actual);
    if (op === "ArnNotEquals" || op === "StringNotEquals") return !expected.includes(actual);
    if (op === "Bool") return expected.includes(actual);
    if (op === "Null") return String(actual === undefined) === value;
    if (op === "StringNotEqualsIfExists") return actual === undefined || !expected.includes(actual);
    if (op === "NumericLessThan") return actual !== undefined && Number(actual) < Number(value);
    if (op === "ForAnyValue:StringEquals") return expected.includes(actual);
    throw new Error(`unsupported operator ${op}`);
  }));
}
/** Allowed iff no Deny matches in either policy and an Allow matches in one (same account). */
function allowed(identity: Statement[], bucket: Statement[], action: string, resource: string, ctx: Record<string, string>) {
  const all = [...identity.map((s) => ({ ...s, Principal: undefined })), ...bucket];
  const hit = all.filter((s) => matches(s, action, resource, ctx));
  return !hit.some((s) => s.Effect === "Deny") && hit.some((s) => s.Effect === "Allow");
}
const tls = { "aws:SecureTransport": "true", "s3:TlsVersion": "1.3" };
const sse = { "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": keyArn };
const bucketOn = transcriptBucketPolicy({ bucketArn, keyArn, eraserArn, accountId, bufferedGcRoleArn: gcRoleArn, uploadInbox: roles }) as Statement[];
const bucketOnAnalysis = transcriptBucketPolicy({ bucketArn, keyArn, eraserArn, accountId, bufferedGcRoleArn: gcRoleArn, analysisDeleterArn: pipelineRoleArn, uploadInbox: roles }) as Statement[];

describe("HISTORY_UPLOAD_DIRECT_INFRA", () => {
  it("is off unless exactly 'on', and refuses anything else", () => {
    expect(parseHistoryUploadDirectInfra(undefined)).toBe(false);
    expect(parseHistoryUploadDirectInfra("")).toBe(false);
    expect(parseHistoryUploadDirectInfra("off")).toBe(false);
    expect(parseHistoryUploadDirectInfra("on")).toBe(true);
    for (const bad of ["1", "true", "ON", " on", "yes"]) expect(() => parseHistoryUploadDirectInfra(bad)).toThrow(/HISTORY_UPLOAD_DIRECT_INFRA/);
  });

  it("the run file passes the input only when the flag is on", () => {
    const run = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
    expect(run).toContain("...(parseHistoryUploadDirectInfra(process.env.HISTORY_UPLOAD_DIRECT_INFRA) ? { historyUploadDirectInfra: true } : {})");
  });
});

describe("transcriptStore.uploadControlRoleArn input", () => {
  const prod = (extra: Record<string, unknown>) => ({ accountId: TRANSCRIPT_ACCOUNT_ID, region: TRANSCRIPT_REGION, partition: "aws",
    transcriptStore: { bufferedGcRoleArn: `arn:aws:iam::${TRANSCRIPT_ACCOUNT_ID}:role/synthetic-buffered-gc`, ...extra } as never });
  it("accepts a named role in any account and refuses anything else", () => {
    expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, prod({ uploadControlRoleArn: controlRoleArn }))).not.toThrow();
    for (const bad of ["*", "arn:aws:iam::222222222222:root", "arn:aws:iam::222222222222:user/x", "arn:aws-cn:iam::222222222222:role/x", "arn:aws:iam::2222:role/x", 7])
      expect(() => assertTranscriptStoreInputs(TRANSCRIPT_STAGE, prod({ uploadControlRoleArn: bad }))).toThrow(/uploadControlRoleArn/);
  });
});

describe("flag off renders the stack byte-identical", () => {
  const modulePath = fileURLToPath(new URL("../transcript-store.ts", import.meta.url));
  // Mock every provider the stage can request, render each scenario with the flag absent, explicitly
  // false, and absent with a control-role input present, and compare the full serialized output
  // (every declared resource's props, in declaration order, plus the stage's return value).
  const script = `import { mock } from 'bun:test'; import * as E from 'effect/Effect';
    const acct='${accountId}', region='${region}'; const seen=[];
    const out=(name,p)=>({userName:p.userName,userArn:'arn:aws:iam::'+acct+':user/96bulls/'+p.userName,roleName:p.roleName,roleArn:'arn:aws:iam::'+acct+':role'+(p.path??'/')+(p.roleName??name),keyArn:'arn:aws:kms:'+region+':'+acct+':key/synthetic-'+name,keyId:'synthetic-'+name,bucketName:p.bucketName,trailName:p.trailName,functionName:p.functionName??name,functionArn:'arn:aws:lambda:'+region+':'+acct+':function:'+(p.functionName??name),queueArn:'arn:aws:sqs:'+region+':'+acct+':'+(p.queueName??name),ruleArn:'arn:aws:events:'+region+':'+acct+':rule/'+name,secretArn:'arn:aws:secretsmanager:'+region+':'+acct+':secret:'+name,uuid:'uuid-'+name,functionUrl:'https://synthetic.invalid/'});
    const f=(type)=>(name,props)=>{seen.push({type,name,props});return E.succeed(out(name,props??{}))};
    const ns=(...t)=>Object.fromEntries(t.map(x=>[x,f(x)]));
    mock.module('@alchemy.run/aws',()=>({IAM:ns('User','Role','Policy'),KMS:ns('Key','Alias'),S3:ns('Bucket'),CloudTrail:ns('Trail'),Budgets:ns('Budget'),Lambda:ns('Function','EventSourceMapping','Permission','Url','FunctionUrl'),SQS:ns('Queue'),SecretsManager:ns('Secret'),EventBridge:ns('Rule'),CloudWatch:ns('Alarm'),Logs:ns('LogGroup')}));
    const {transcriptStoreStage}=await import(${JSON.stringify(modulePath)});
    const gc='arn:aws:iam::'+acct+':role/synthetic-buffered-gc';
    const base={stage:'${stage}',accountId:acct,region,budgetUsd:1,budgetEmail:'test@example.invalid',tags:(c)=>({component:c})};
    const scenarios={
      store:{bufferedGcRoleArn:gc},
      full:{bufferedGcRoleArn:gc,inboxAuthorityKeyRoot:'96bulls/authority-root',sessionPipeline:{searchIndex:true,analysisJobs:{enabled:true}},sessionReadApi:{completeSince:'epoch',analysisBucket:true}},
      noAnalysis:{bufferedGcRoleArn:gc,sessionPipeline:{searchIndex:true},sessionReadApi:{completeSince:'epoch'}},
    };
    const rep=(k,v)=>typeof v==='function'?'[fn]':v;
    const render=async(cfg)=>{seen.length=0;const ret=await E.runPromise(transcriptStoreStage(cfg));return JSON.stringify({resources:seen,ret},rep);};
    const res={};
    for(const [k,store] of Object.entries(scenarios)){
      res[k]={
        absent:await render({...base,transcriptStore:store}),
        explicitFalse:await render({...base,historyUploadDirectInfra:false,transcriptStore:store}),
        controlOnly:await render({...base,transcriptStore:{...store,uploadControlRoleArn:'${controlRoleArn}'}}),
      };
      if(store.sessionPipeline) res[k].on=await render({...base,historyUploadDirectInfra:true,transcriptStore:{...store,uploadControlRoleArn:'${controlRoleArn}'}});
    }
    console.log(JSON.stringify(res));`;
  const result = spawnSync(process.execPath, ["--eval", script], { encoding: "utf8", env: { PATH: process.env.PATH }, timeout: 120_000 });
  if (result.status !== 0) throw new Error(result.stderr);
  const renders = JSON.parse(result.stdout) as Record<string, { absent: string; explicitFalse: string; controlOnly: string; on?: string }>;
  const uploadSids = ["DenyUploadInbox", "UploadInboxOnlyItsRoles", "UploadInboxOnlyEraserOrConsumerDeletes", "DesktopUploadEncryptsInboxViaS3", "UploadAdmissionEncryptsAndReadsViaS3"];

  for (const scenario of ["store", "full", "noAnalysis"]) {
    it(`${scenario}: off, explicit false and an inert control-role input are identical`, () => {
      const r = renders[scenario]!;
      expect(r.explicitFalse).toBe(r.absent);
      expect(r.controlOnly).toBe(r.absent);
    });
    it(`${scenario}: off declares no upload resource, Sid, queue rule or output`, () => {
      const off = renders[scenario]!.absent;
      for (const token of [...UPLOAD_INBOX_RESOURCES, ...uploadSids, UPLOAD_INBOX_NOTIFICATION_ID, ".upload-inbox", "uploadInbox", "desktop-upload", "upload-admission"])
        expect(off).not.toContain(token);
    });
  }

  it("off: the session-pipeline notification carries only the manifests rule", () => {
    const off = JSON.parse(renders.full!.absent);
    expect(off.ret.sessionPipeline.notification.QueueConfigurations.map((q: any) => q.Id)).toEqual(["96bulls-session-pipeline-manifests"]);
  });

  it("on: declares exactly the upload resources, and the notification owns both Ids", () => {
    for (const scenario of ["full", "noAnalysis"]) {
      const on = JSON.parse(renders[scenario]!.on!);
      const off = JSON.parse(renders[scenario]!.absent);
      const added = on.resources.map((r: any) => r.name).filter((n: string) => !off.resources.some((o: any) => o.name === n));
      expect(added.sort()).toEqual([...UPLOAD_INBOX_RESOURCES].sort());
      expect(on.ret.sessionPipeline.notification.QueueConfigurations.map((q: any) => q.Id)).toEqual(["96bulls-session-pipeline-manifests", UPLOAD_INBOX_NOTIFICATION_ID]);
      expect(on.ret.sessionPipeline.notification).toEqual(bucketNotificationConfiguration(sessionPipelineNames(stage, accountId, region).queueArn, u.queueArn));
      expect(on.ret.uploadInbox).toEqual({ queueUrl: u.queueUrl, queueArn: u.queueArn, dlqArn: u.dlqArn, uploadRoleArn: u.uploadRoleArn, consumerRoleArn: u.consumerRoleArn });
      const byName = (n: string) => on.resources.find((r: any) => r.name === n).props;
      // Principals before the key: both roles are declared before transcripts-key.
      const order = on.resources.map((r: any) => r.name);
      expect(order.indexOf("desktop-upload-role")).toBeLessThan(order.indexOf("transcripts-key"));
      expect(order.indexOf("upload-admission-role")).toBeLessThan(order.indexOf("transcripts-key"));
      expect(byName("transcripts-key").policy.Statement.map((s: any) => s.Sid)).toContain("DesktopUploadEncryptsInboxViaS3");
      expect(byName("transcripts").policy.map((s: any) => s.Sid)).toContain("UploadInboxOnlyItsRoles");
      for (const user of ["transcript-writer", "transcript-reader", "transcript-srv-writer"])
        expect(byName(user).inlinePolicies.transcripts.Statement.at(-1).Sid).toBe("DenyUploadInbox");
      expect(byName("transcript-eraser").inlinePolicies.transcripts.Statement.map((s: any) => s.Sid)).not.toContain("DenyUploadInbox");
      const queue = byName("upload-admission-events");
      expect(queue.visibilityTimeout).toBe(`${6 * UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS} seconds`);
      expect(queue.redrivePolicy.maxReceiveCount).toBeGreaterThan(0);
      expect(queue.redrivePolicy.deadLetterTargetArn).toBe(u.dlqArn);
      expect(byName("upload-admission-dlq").messageRetentionPeriod).toBe("14 days");
      const dlqAlarm = byName("upload-admission-dlq-alarm");
      expect(dlqAlarm.Dimensions).toEqual([{ Name: "QueueName", Value: u.dlq }]);
      expect(dlqAlarm.MetricName).toBe("ApproximateNumberOfMessagesVisible");
      expect(byName("desktop-upload-role").assumeRolePolicyDocument).toEqual(desktopUploadTrustPolicy({ controlRoleArn }));
    }
  });

  it("on: refuses without a control role or without the session pipeline", () => {
    const refuse = (store: string) => spawnSync(process.execPath, ["--eval", script.replace(/const res=\{\};[\s\S]*$/, `await E.runPromise(transcriptStoreStage({...base,historyUploadDirectInfra:true,transcriptStore:${store}}));`)], { encoding: "utf8", env: { PATH: process.env.PATH }, timeout: 60_000 });
    const noControl = refuse("scenarios.full");
    expect(noControl.status).not.toBe(0);
    expect(noControl.stderr).toContain("uploadControlRoleArn is required");
    const noPipeline = refuse(`{...scenarios.store,uploadControlRoleArn:'${controlRoleArn}'}`);
    expect(noPipeline.status).not.toBe(0);
    expect(noPipeline.stderr).toContain("sessionPipeline is required");
  });
});

describe("bucket notification", () => {
  it("one rule off, both rules on, the inbox rule exact", () => {
    expect(bucketNotificationConfiguration("arn:q").QueueConfigurations.map((q) => q.Id)).toEqual(["96bulls-session-pipeline-manifests"]);
    const on = bucketNotificationConfiguration("arn:q", "arn:u");
    expect(on.QueueConfigurations[0]).toEqual(bucketNotificationConfiguration("arn:q").QueueConfigurations[0]!);
    expect(on.QueueConfigurations[1]).toEqual({
      Id: "96bulls-upload-inbox",
      QueueArn: "arn:u",
      Events: ["s3:ObjectCreated:*"],
      Filter: { Key: { FilterRules: [{ Name: "prefix", Value: "96bulls/.upload-inbox/" }, { Name: "suffix", Value: ".json.zst" }] } },
    });
  });
});

describe("desktop-upload role", () => {
  const policy = desktopUploadPolicy({ bucketArn, accountId, region, keyAlias }).Statement as Statement[];
  const ctx = { ...tls, ...sse, "aws:PrincipalArn": u.uploadRoleArn, "s3:if-none-match": "*" };

  it("trusts only the control role, and only with a source identity", () => {
    const trust = desktopUploadTrustPolicy({ controlRoleArn }).Statement;
    expect(trust).toHaveLength(1);
    expect(trust[0]!.Principal).toEqual({ AWS: controlRoleArn });
    expect([...trust[0]!.Action].sort()).toEqual(["sts:AssumeRole", "sts:SetSourceIdentity"]);
    expect(trust[0]!.Condition).toEqual({ Null: { "sts:SourceIdentity": "false" } });
  });

  it("holds only s3:PutObject (plus encrypt-only key use)", () => {
    const actions = policy.flatMap((s) => many(s.Action));
    expect(actions.filter((a) => a.startsWith("s3:"))).toEqual(["s3:PutObject"]);
    expect(actions.filter((a) => a.startsWith("kms:")).sort()).toEqual(["kms:Encrypt", "kms:GenerateDataKey"]);
    expect(policy.every((s) => s.Effect === "Allow")).toBe(true);
  });

  it("puts create-only, SSE-KMS with this key, under the inbox only", () => {
    expect(allowed(policy, bucketOn, "s3:PutObject", inboxKey, ctx)).toBe(true);
    expect(allowed(policy, bucketOn, "s3:PutObject", sessionKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:PutObject", srvKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:PutObject", inboxKey, { ...ctx, "s3:if-none-match": undefined as never })).toBe(false);
    expect(allowed(policy, bucketOn, "s3:PutObject", inboxKey, { ...ctx, "s3:x-amz-server-side-encryption": "AES256" })).toBe(false);
    expect(allowed(policy, bucketOn, "s3:PutObject", inboxKey, { ...ctx, "s3:x-amz-server-side-encryption-aws-kms-key-id": `arn:aws:kms:${region}:${accountId}:key/other` })).toBe(false);
    for (const action of ["s3:GetObject", "s3:ListBucket", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion"])
      expect(allowed(policy, bucketOn, action, inboxKey, ctx)).toBe(false);
  });

  it("the key policy lets it GenerateDataKey/Encrypt for inbox objects only", () => {
    const key = transcriptKeyPolicy({ accountId, region, bucketName, writerArn, srvWriterArn, readerArn, uploadInbox: roles }).Statement as Statement[];
    const mine = key.filter((s) => many(s.Principal?.AWS).includes(u.uploadRoleArn));
    expect(mine.flatMap((s) => many(s.Action)).sort()).toEqual(["kms:Encrypt", "kms:GenerateDataKey"]);
    const kctx = { "aws:PrincipalArn": u.uploadRoleArn, "kms:ViaService": `s3.${region}.amazonaws.com`, "kms:CallerAccount": accountId };
    expect(allowed([], key, "kms:GenerateDataKey", keyArn, { ...kctx, "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${bucketName}/${UPLOAD_INBOX_PREFIX}org-a/x.json.zst` })).toBe(true);
    expect(allowed([], key, "kms:GenerateDataKey", keyArn, { ...kctx, "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${bucketName}/96bulls/org-a/v1/x` })).toBe(false);
    expect(allowed([], key, "kms:Decrypt", keyArn, { ...kctx, "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${bucketName}/${UPLOAD_INBOX_PREFIX}org-a/x.json.zst` })).toBe(false);
  });
});

describe("the inbox wall", () => {
  const ctx = (principal: string) => ({ ...tls, ...sse, "aws:PrincipalArn": principal });
  const users = {
    writer: [writerArn, transcriptWriterPolicy(bucketArn, { denyUploadInbox: true }).Statement],
    reader: [readerArn, transcriptReaderPolicy(bucketArn, { denyUploadInbox: true }).Statement],
    srvWriter: [srvWriterArn, transcriptSrvWriterPolicy({ bucketArn, accountId, region, keyAlias, denyUploadInbox: true }).Statement],
  } as const;

  it("the writer, reader and srv-writer keep their session access but lose the inbox (identity deny alone)", () => {
    for (const [arn, policy] of Object.values(users)) {
      for (const action of ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]) expect(allowed(policy as Statement[], [], action, inboxKey, ctx(arn))).toBe(false);
      expect(policy.at(-1)).toEqual({ Sid: "DenyUploadInbox", Effect: "Deny", Action: ["s3:*"], Resource: [`${bucketArn}/96bulls/.upload-inbox/*`] });
    }
    expect(allowed(users.reader[1] as Statement[], bucketOn, "s3:GetObject", sessionKey, ctx(readerArn))).toBe(true);
    expect(allowed(users.writer[1] as Statement[], bucketOn, "s3:GetObject", sessionKey, ctx(writerArn))).toBe(true);
    expect(allowed(users.srvWriter[1] as Statement[], bucketOn, "s3:PutObject", srvKey, ctx(srvWriterArn))).toBe(true);
  });

  it("without the identity deny, the old wildcards would have reached inbox keys (why the wall exists)", () => {
    expect(allowed(transcriptReaderPolicy(bucketArn).Statement as Statement[], [], "s3:GetObject", inboxKey, ctx(readerArn))).toBe(true);
  });

  it("the bucket policy denies every other principal, including the pipeline, GC and the users", () => {
    const wideAllow = [{ Effect: "Allow", Action: ["s3:*"], Resource: [`${bucketArn}/*`] }];
    for (const p of [pipelineRoleArn, gcRoleArn, writerArn, readerArn, srvWriterArn, `arn:aws:iam::${accountId}:role/admin`])
      for (const action of ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion"])
        for (const bucket of [bucketOn, bucketOnAnalysis]) expect(allowed(wideAllow, bucket, action, inboxKey, ctx(p))).toBe(false);
    // Outside the inbox, the bucket policy changes nothing for the pipeline.
    expect(allowed(wideAllow, bucketOn, "s3:GetObject", sessionKey, ctx(pipelineRoleArn))).toBe(true);
  });

  it("only the consumer and the eraser delete inbox objects; elsewhere the eraser-only deny is unchanged", () => {
    const wideAllow = [{ Effect: "Allow", Action: ["s3:*"], Resource: [`${bucketArn}/*`] }];
    for (const bucket of [bucketOn, bucketOnAnalysis]) {
      expect(allowed(wideAllow, bucket, "s3:DeleteObject", inboxKey, ctx(u.consumerRoleArn))).toBe(true);
      expect(allowed(wideAllow, bucket, "s3:DeleteObject", inboxKey, ctx(eraserArn))).toBe(true);
      expect(allowed(wideAllow, bucket, "s3:DeleteObject", inboxKey, ctx(u.uploadRoleArn))).toBe(false);
      expect(allowed(wideAllow, bucket, "s3:DeleteObject", sessionKey, ctx(u.consumerRoleArn))).toBe(false);
      expect(allowed(wideAllow, bucket, "s3:DeleteObject", sessionKey, ctx(eraserArn))).toBe(true);
    }
    // Outside the inbox every principal's delete outcome equals the flag-off policy's.
    const offPolicy = transcriptBucketPolicy({ bucketArn, keyArn, eraserArn, accountId, bufferedGcRoleArn: gcRoleArn }) as Statement[];
    const bufferedKey = `${bucketArn}/96bulls/org-a/live-batches/b1/x.ndjson`;
    for (const p of [eraserArn, gcRoleArn, pipelineRoleArn, writerArn, u.consumerRoleArn, u.uploadRoleArn])
      for (const key of [sessionKey, srvKey, bufferedKey])
        for (const action of ["s3:DeleteObject", "s3:DeleteObjectVersion"])
          expect(allowed(wideAllow, bucketOn, action, key, ctx(p))).toBe(allowed(wideAllow, offPolicy, action, key, ctx(p)));
    // Analysis results stay deletable by the analysis worker with the inbox carve-out added.
    expect(allowed(wideAllow, bucketOnAnalysis, "s3:DeleteObject", `${bucketArn}/96bulls/org-a/analysis-results/v1/x.json`, ctx(pipelineRoleArn)))
      .toBe(allowed(wideAllow, transcriptBucketPolicy({ bucketArn, keyArn, eraserArn, accountId, bufferedGcRoleArn: gcRoleArn, analysisDeleterArn: pipelineRoleArn }) as Statement[], "s3:DeleteObject", `${bucketArn}/96bulls/org-a/analysis-results/v1/x.json`, ctx(pipelineRoleArn)));
  });
});

describe("upload-admission consumer role", () => {
  const policy = uploadAdmissionPolicy({ bucketArn, accountId, region, keyAlias, queueArn: u.queueArn, functionName: u.function, promotionObjects: srvWriterObjects(bucketArn) }).Statement as Statement[];
  const ctx = { ...tls, ...sse, "aws:PrincipalArn": u.consumerRoleArn };

  it("reads, tombstones and deletes inbox objects, and promotes through the srv-writer layout", () => {
    for (const action of ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]) expect(allowed(policy, bucketOn, action, inboxKey, ctx)).toBe(true);
    expect(allowed(policy, bucketOn, "s3:PutObject", admissionKey, ctx)).toBe(true);
    expect(allowed(policy, bucketOn, "s3:PutObject", srvKey, ctx)).toBe(true);
    expect(allowed(policy, bucketOn, "s3:GetObject", sessionKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:DeleteObject", srvKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:DeleteObjectVersion", inboxKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:GetObjectVersion", inboxKey, ctx)).toBe(false);
    expect(allowed(policy, bucketOn, "s3:PutObject", inboxKey, { ...ctx, "s3:x-amz-server-side-encryption": "AES256" })).toBe(false);
  });

  it("the queue takes S3 events from this bucket only, and the consumer cannot send", () => {
    const q = uploadAdmissionQueuePolicy({ queueArn: u.queueArn, bucketArn, accountId, consumerRoleArn: u.consumerRoleArn }).Statement as Statement[];
    const s3 = q.find((s) => s.Principal?.Service === "s3.amazonaws.com")!;
    expect(s3.Condition).toEqual({ ArnEquals: { "aws:SourceArn": bucketArn }, StringEquals: { "aws:SourceAccount": accountId } });
    expect(many(s3.Action)).toEqual(["sqs:SendMessage"]);
    const consumer = q.find((s) => s.Principal?.AWS === u.consumerRoleArn)!;
    expect(many(consumer.Action)).not.toContain("sqs:SendMessage");
    expect(UPLOAD_ADMISSION_VISIBILITY_TIMEOUT_SECONDS).toBe(6 * UPLOAD_ADMISSION_FUNCTION_TIMEOUT_SECONDS);
  });

  it("names carry no production identifiers", () => {
    expect(JSON.stringify(u)).not.toContain(TRANSCRIPT_ACCOUNT_ID);
    expect(uploadInboxNames(stage, accountId, region).queue).toBe(`96bulls-${stage}-upload-admission`);
  });
});
