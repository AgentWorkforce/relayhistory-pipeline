import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import * as AWS from "@alchemy.run/aws";
import { fileURLToPath } from "node:url";
import type { AwsInputs } from "../inputs.ts";
import {
  DELIVERY_DELAY_SECONDS,
  FUNCTION_TIMEOUT_SECONDS,
  SESSION_BRIEF_RESOURCES,
  SESSION_ANALYSIS_CACHE_RESOURCES,
  SESSION_ANALYSIS_JOB_RESOURCES,
  analysisCacheScheduleProps,
  analysisJobScheduleProps,
  analysisReconcileScheduleProps,
  SESSION_SEMANTIC_COPY_RESOURCES,
  semanticCopyScheduleProps,
  SESSION_BUFFERED_SEARCH_FEED_RESOURCES,
  BUFFERED_SEARCH_FEED_GATE_CONTRACT,
  bufferedSearchFeedScheduleProps,
  assertSessionPipelineInputs,
  SESSION_PIPELINE_RESOURCES,
  VISIBILITY_TIMEOUT_SECONDS,
  briefScheduleProps,
  bucketNotificationConfiguration,
  pipelineBucketStatements,
  pipelineKeyStatement,
  sessionPipelineEnv,
  sessionPipelineFunctionProps,
  sessionPipelineNames,
  sessionPipelinePermissions,
  sessionQueuePolicy,
  sessionSecretPolicy,
  type SessionPipelineInputs,
} from "../session-pipeline.ts";
import { assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames } from "../transcript-store.ts";
import { bufferedFeedConfig, bufferedFeedGateRefusal, BUFFERED_SEARCH_GATE_CONTRACT } from "../../../src/session-pipeline/buffered-search-feed.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const REGION = "us-east-1";
const ROLE = `arn:aws:iam::${ACCOUNT}:role/session-pipeline-role`;
const t = transcriptNames(STAGE, ACCOUNT);
const p = sessionPipelineNames(STAGE, ACCOUNT, REGION);
const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-sessions-prod.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;
example.transcriptStore!.bufferedGcRoleArn = `arn:aws:iam::${ACCOUNT}:role/test-buffered-gc`;

describe("session pipeline: grants", () => {
  it("the role may only decrypt via S3, for objects under 96bulls/ in this bucket", () => {
    const policy = transcriptKeyPolicy({ accountId: ACCOUNT, region: REGION, bucketName: t.bucket, writerArn: "w", srvWriterArn: "sw", readerArn: "r", pipelineRoleArn: ROLE });
    const stmt = policy.Statement.find((s) => s.Sid === "SessionPipelineDecryptsViaS3")!;
    expect(stmt).toEqual(pipelineKeyStatement({ accountId: ACCOUNT, region: REGION, bucketName: t.bucket, roleArn: ROLE }));
    expect(stmt.Action).toEqual(["kms:Decrypt"]);
    expect(stmt.Condition).toEqual({
      StringEquals: { "kms:ViaService": "s3.us-east-1.amazonaws.com", "kms:CallerAccount": ACCOUNT },
      StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${t.bucket}/96bulls/*` },
    });
    // Without the pipeline the key policy is unchanged (no stray statement).
    const without = transcriptKeyPolicy({ accountId: ACCOUNT, region: REGION, bucketName: t.bucket, writerArn: "w", srvWriterArn: "sw", readerArn: "r" }).Statement.map((s) => s.Sid);
    expect(without).not.toContain("SessionPipelineDecryptsViaS3");
    expect(policy.Statement.map((s) => s.Sid)).toEqual([...without, "SessionPipelineDecryptsViaS3"]);
  });

  it("the bucket grants are list + read of session objects only, and the store's denies still apply", () => {
    const grants = pipelineBucketStatements({ bucketArn: t.bucketArn, roleArn: ROLE });
    expect(grants.flatMap((g) => g.Action).sort()).toEqual(["s3:GetObject", "s3:ListBucket"]);
    expect(grants[1]!.Resource).toEqual([`${t.bucketArn}/96bulls/*/v1/*`]);
    const denies = transcriptBucketPolicy({ bucketArn: t.bucketArn, keyArn: "k", eraserArn: "e" });
    expect(denies.every((d) => d.Effect === "Deny")).toBe(true);
    expect(denies.find((d) => d.Sid === "OnlyTheEraserDeletes")!.Condition).toEqual({ ArnNotEquals: { "aws:PrincipalArn": "e" } });
  });

  it("the queue takes S3 events from this bucket and account only; the role consumes it; TLS only", () => {
    const policy = sessionQueuePolicy({ queueArn: p.queueArn, bucketArn: t.bucketArn, accountId: ACCOUNT, roleArn: ROLE });
    const s3 = policy.Statement.find((s) => s.Sid === "TranscriptBucketEvents")!;
    expect(s3.Condition).toEqual({ ArnEquals: { "aws:SourceArn": t.bucketArn }, StringEquals: { "aws:SourceAccount": ACCOUNT } });
    expect(s3.Action).toBe("sqs:SendMessage");
    const consume = policy.Statement.find((s) => s.Sid === "SessionPipelineConsumes")!;
    expect(consume.Principal).toEqual({ AWS: ROLE });
    expect(consume.Action).not.toContain("sqs:PurgeQueue");
    expect(policy.Statement[0]!.Effect).toBe("Deny");
  });

  it("only the role reads the secret", () => {
    expect(sessionSecretPolicy({ roleArn: ROLE }).Statement).toEqual([
      { Sid: "SessionPipelineReads", Effect: "Allow", Principal: { AWS: ROLE }, Action: "secretsmanager:GetSecretValue", Resource: "*" },
    ]);
  });
});

describe("session pipeline: wiring", () => {
  it("notifies manifest puts and deletes under 96bulls/ only (never chunks)", () => {
    const [config] = bucketNotificationConfiguration(p.queueArn).QueueConfigurations;
    expect(config!.Events).toEqual(["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]);
    expect(config!.Filter.Key.FilterRules).toEqual([
      { Name: "prefix", Value: "96bulls/" },
      { Name: "suffix", Value: "/manifest.json" },
    ]);
    expect(config!.QueueArn).toBe(`arn:aws:sqs:us-east-1:${ACCOUNT}:96bulls-aws-sessions-prod-session-events`);
  });

  it("visibility outlasts six function timeouts; delivery waits for a write burst", () => {
    expect(VISIBILITY_TIMEOUT_SECONDS).toBe(6 * FUNCTION_TIMEOUT_SECONDS);
    expect(DELIVERY_DELAY_SECONDS).toBeGreaterThan(0);
    expect(DELIVERY_DELAY_SECONDS).toBeLessThanOrEqual(900);
  });

  it("env carries ids and switches only; the search index and fan-out start off", () => {
    const env = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: {} });
    expect(env).toMatchObject({
      PIPELINE_RAW_S3_BUCKET: t.bucket,
      PIPELINE_KEY_PREFIX: "96bulls",
      PIPELINE_ORG_FROM_PREFIX: "true",
      SESSION_PIPELINE_SECRET_ARN: "96bulls/aws-sessions-prod/session-pipeline",
      SEARCH_INDEX_ENABLED: "0",
      SESSION_REPLAY_PAGES_ENABLED: "0",
      SESSION_ANALYSIS_CACHE_ENABLED: "0",
      SESSION_ANALYSIS_ENABLED: "false",
      SESSION_DESKTOP_CATALOG_ENABLED: "0",
      PIPELINE_FANOUT: "off",
      PIPELINE_RECONCILE_LOOKBACK_MINUTES: "60",
    });
    for (const value of Object.values(env)) expect(value).not.toMatch(/postgres(ql)?:\/\/|password|clickhouse\.cloud/i);
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { searchIndex: true } }).SEARCH_INDEX_ENABLED).toBe("1");
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { replayPages: true } }).SESSION_REPLAY_PAGES_ENABLED).toBe("1");
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { analysisJobs: { enabled: true } } }).SESSION_ANALYSIS_ENABLED).toBe("true");
  });

  it("the stack never switches on the large-read handoff or worker", () => {
    for (const p0 of [{}, { searchIndex: true, replayPages: true }]) {
      const env = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: p0 });
      expect(Object.keys(env).filter((k) => k.startsWith("SESSION_LARGE_READ_"))).toEqual([]);
    }
  });

  it("briefs start off: SESSION_BRIEF_ENABLED=false and the schedule DISABLED; enabled sets both, and the caps only when given", () => {
    const off = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: {} });
    expect(off.SESSION_BRIEF_ENABLED).toBe("false");
    expect(Object.keys(off).filter((k) => k.startsWith("SESSION_BRIEF_"))).toEqual(["SESSION_BRIEF_ENABLED"]);
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { briefs: { enabled: false, globalDailyCap: 5 } } }).SESSION_BRIEF_ENABLED).toBe("false");
    const on = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { briefs: { enabled: true, backgroundDailyBudget: 1000, globalDailyCap: 2000 } } });
    expect(on).toMatchObject({ SESSION_BRIEF_ENABLED: "true", SESSION_BRIEF_BACKGROUND_DAILY_BUDGET: "1000", SESSION_BRIEF_GLOBAL_DAILY_CAP: "2000" });
    // The sweep is emitted only when set; absent keeps the worker default (0, off).
    expect(Object.keys(on)).not.toContain("SESSION_BRIEF_SWEEP_SECONDS");
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { briefs: { enabled: true, sweepSeconds: 300 } } }).SESSION_BRIEF_SWEEP_SECONDS).toBe("300");
    // The model key is never in env: it is read from the pipeline secret.
    expect(Object.keys(on)).not.toContain("OPENAI_API_KEY");

    const FN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${p.function}`;
    const sched = (briefs?: { enabled: boolean }) => briefScheduleProps({ names: p, functionArn: FN, p: briefs ? { briefs } : {}, tags: {} });
    expect(sched().state).toBe("DISABLED");
    expect(sched({ enabled: false }).state).toBe("DISABLED");
    expect(sched({ enabled: true })).toEqual({
      name: "96bulls-aws-sessions-prod-session-brief",
      description: "96bulls session pipeline: one session-brief tick (due wakeups)",
      scheduleExpression: "rate(1 minute)",
      state: "ENABLED",
      targets: [{ Id: "session-pipeline", Arn: FN, Input: '{"brief":true}' }],
      tags: {},
    });
  });
});

describe("session pipeline: not reachable from the internet", () => {
  const stageInputs = { stage: STAGE, accountId: ACCOUNT, region: REGION, p: {}, tags: () => ({}), bucket: t.bucket };
  const RULE_ARN = `arn:aws:events:${REGION}:${ACCOUNT}:rule/${p.rule}`;

  it("the function has no Function URL (Alchemy's default is a public, unauthenticated one)", () => {
    const props = sessionPipelineFunctionProps(stageInputs);
    expect(props.functionUrl).toBe(false);
    // Alchemy's own normalizer decides whether a URL (and its public grant) is created.
    expect(AWS.Lambda.normalizeFunctionUrl(props.functionUrl)).toBeUndefined();
    // Omitting the setting is what this guards against: it means a public URL.
    expect(AWS.Lambda.normalizeFunctionUrl(undefined)).toEqual({ authType: "NONE", invokeMode: "BUFFERED" });
  });

  it("the function's resource policy grants InvokeFunction to EventBridge (this rule) and its own role only, never a public principal", () => {
    const BRIEF_RULE_ARN = `arn:aws:events:${REGION}:${ACCOUNT}:rule/${p.briefRule}`;
    const grants = sessionPipelinePermissions({ accountId: ACCOUNT, functionName: p.function, ruleArn: RULE_ARN, briefRuleArn: BRIEF_RULE_ARN, roleArn: ROLE });
    expect(grants.map((g) => g.id)).toEqual(["session-reconcile-permission", "session-reconcile-continue", "session-brief-permission"]);
    for (const { props } of grants) {
      expect(props.action).toBe("lambda:InvokeFunction");
      expect(props.principal).not.toBe("*");
      expect(JSON.stringify(props)).not.toMatch(/FunctionUrl|"\*"/);
    }
    expect(grants[0]!.props).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: "events.amazonaws.com", sourceArn: RULE_ARN, sourceAccount: ACCOUNT });
    expect(grants[1]!.props).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: ROLE });
    expect(grants[2]!.props).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: "events.amazonaws.com", sourceArn: BRIEF_RULE_ARN, sourceAccount: ACCOUNT });
  });

  it("the reconcile chain uses the Lambda Invoke API (SigV4), not a Function URL", () => {
    const src = readFileSync(fileURLToPath(new URL("../../../src/session-pipeline/aws-json.ts", import.meta.url)), "utf8");
    expect(src).toContain("https://lambda.${args.region}.amazonaws.com/2015-03-31/functions/");
    expect(src).toContain('"x-amz-invocation-type": "Event"');
    expect(src).not.toMatch(/lambda-url/);
  });

  it("every Lambda function and permission in the stacks is declared through these guarded props", () => {
    const root = fileURLToPath(new URL("../../", import.meta.url));
    const files = (readdirSync(root, { recursive: true }) as string[]).filter((f) => f.endsWith(".ts") && !f.includes("__tests__") && !f.includes("node_modules"));
    const calls: string[] = [];
    // readdirSync order is filesystem-dependent (ext4 on CI differs from btrfs/APFS locally): sort.
    for (const f of [...files].sort()) {
      const src = readFileSync(`${root}${f}`, "utf8");
      // URL-bearing hosts and raw URL configs are never used for the pipeline.
      expect(`${f}: ${/Lambda\.(WebFunction|HttpServer)\(|FunctionUrlConfig|InvokeFunctionUrl/.test(src)}`).toBe(`${f}: false`);
      for (const m of src.matchAll(/Lambda\.(Function|Permission)\(([^\n]*)/g)) calls.push(`${f}: ${m[1]}(${m[2]}`);
    }
    expect(calls).toEqual([
      'aws/session-pipeline.ts: Function("session-pipeline", sessionPipelineFunctionProps(i) as never);',
      "aws/session-pipeline.ts: Permission(id, props as never);",
      // The read API has a Function URL by design (application auth); its props are pinned in session-read-api.test.ts.
      'aws/session-read-api.ts: Function("session-read-api", sessionReadApiFunctionProps({ stage: i.stage, names, p: i.p }) as never);',
    ]);
  });
});

describe("session pipeline: inputs", () => {
  const withPipeline = (sessionPipeline: unknown) =>
    ({ ...example, transcriptStore: { ...example.transcriptStore, sessionPipeline } }) as unknown as AwsInputs & Record<string, unknown>;
  const validate = (i: AwsInputs & Record<string, unknown>) => assertTranscriptStoreInputs(STAGE, i);
  it("accepts the documented settings and refuses anything else", () => {
    expect(() => validate(withPipeline({ searchIndex: true, replayPages: false, maxConcurrency: 4, reconcileMinutes: 30 }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ queue: "x" }))).toThrow(/sessionPipeline.queue is not a known setting/);
    expect(() => validate(withPipeline({ maxConcurrency: 50 }))).toThrow(/maxConcurrency/);
    expect(() => validate(withPipeline({ reconcileMinutes: 1 }))).toThrow(/reconcileMinutes/);
    expect(() => validate(withPipeline([]))).toThrow(/plain object/);
  });

  it("replayPages: true is refused until the normalized-prefix write grant exists; false/absent keep working", () => {
    expect(() => validate(withPipeline({ replayPages: true }))).toThrow(
      "replayPages requires the reviewed normalized-prefix PutObject/KMS grant; not wired yet",
    );
    expect(() => validate(withPipeline({ replayPages: false }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({}))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ replayPages: "true" }))).toThrow(/replayPages must be a boolean/);
  });

  it("analysisCache: true is refused until the normalized-prefix write grant exists; false/absent keep working and the env stays 0", () => {
    expect(() => validate(withPipeline({ analysisCache: true }))).toThrow(
      "transcriptStore.sessionPipeline.analysisCache requires the reviewed normalized-prefix PutObject/KMS grant; not wired yet",
    );
    expect(() => validate(withPipeline({ analysisCache: false }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({}))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ analysisCache: "true" }))).toThrow(/analysisCache must be a boolean/);
    for (const pipeline of [{}, { analysisCache: false }])
      expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: pipeline }).SESSION_ANALYSIS_CACHE_ENABLED).toBe("0");
  });

  it("desktopCatalog: true is refused until migration 20261105031000 and its review; false/absent keep working", () => {
    expect(() => validate(withPipeline({ desktopCatalog: true }))).toThrow(
      "desktopCatalog requires the forget/reproject fix (migration 20261105031000) and its review; not wired yet",
    );
    expect(() => validate(withPipeline({ searchIndex: true, desktopCatalog: true }))).toThrow(/desktopCatalog requires the forget\/reproject fix/);
    expect(() => validate(withPipeline({ desktopCatalog: false }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ searchIndex: true, desktopCatalog: false }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({}))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ desktopCatalog: "true" }))).toThrow(/desktopCatalog must be a boolean/);
    expect(() => validate(withPipeline({ desktopCatalog: 1 }))).toThrow(/desktopCatalog must be a boolean/);
  });

  it("desktop catalog env: SESSION_DESKTOP_CATALOG_ENABLED is \"0\" by default and in every deployed or example env; searchIndex never turns it on", () => {
    const names = sessionPipelineNames(STAGE, ACCOUNT, REGION);
    for (const sp of [{}, { searchIndex: true }, { searchIndex: true, desktopCatalog: false }, { searchIndex: true, replayPages: false, maxConcurrency: 4, reconcileMinutes: 30, briefs: { enabled: true } }])
      expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names, p: sp }).SESSION_DESKTOP_CATALOG_ENABLED).toBe("0");
    // Every stage input present in the tree (local captures, which are not committed, and the
    // committed examples) that carries a session pipeline. docs/plans may be absent.
    const root = fileURLToPath(new URL("../../../", import.meta.url));
    const plansDir = `${root}docs/plans`;
    const planFiles = existsSync(plansDir) ? readdirSync(plansDir).map((f) => `${plansDir}/${f}`) : [];
    const files = [
      ...planFiles.filter((f) => f.endsWith(".inputs.sanitized.json")),
      ...readdirSync(`${root}stacks/aws/examples`).filter((f) => f.endsWith(".json")).map((f) => `${root}stacks/aws/examples/${f}`),
    ];
    let checked = 0;
    for (const file of files) {
      const inputs = JSON.parse(readFileSync(file, "utf8")) as { transcriptStore?: { sessionPipeline?: Record<string, unknown> } };
      const sp = inputs.transcriptStore?.sessionPipeline;
      if (sp === undefined) continue;
      checked++;
      expect(sp.desktopCatalog).not.toBe(true);
      for (const deployed of [sp, { ...sp, searchIndex: true }])
        expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names, p: deployed as SessionPipelineInputs }).SESSION_DESKTOP_CATALOG_ENABLED).toBe("0");
    }
    // No committed stage input carries a sessionPipeline today (captures are local; the examples
    // have none), so `checked` may be 0; the defaults above still cover the env.
    expect(checked).toBeLessThanOrEqual(files.length);
    // No file present (captures and examples) sets the switch to "1".
    const plans = [...planFiles, ...files.filter((f) => !planFiles.includes(f))].map((f) => readFileSync(f, "utf8"));
    for (const text of plans) expect(text).not.toMatch(/SESSION_DESKTOP_CATALOG_ENABLED["']?\s*[:=]\s*["']?1/);
  });

  it("briefs: enabled is a required boolean, the caps are bounded integers, nothing else", () => {
    expect(() => validate(withPipeline({ briefs: { enabled: true, backgroundDailyBudget: 5000, globalDailyCap: 20000 } }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ briefs: { enabled: false } }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ briefs: {} }))).toThrow(/briefs.enabled must be a boolean/);
    expect(() => validate(withPipeline({ briefs: { enabled: "true" } }))).toThrow(/briefs.enabled must be a boolean/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, model: "x" } }))).toThrow(/briefs.model is not a known setting/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, openai_api_key: "sk-x" } }))).toThrow(/briefs.openai_api_key is not a known setting/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, globalDailyCap: -1 } }))).toThrow(/globalDailyCap/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, backgroundDailyBudget: 1.5 } }))).toThrow(/backgroundDailyBudget/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, sweepSeconds: 300 } }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, sweepSeconds: 0 } }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, sweepSeconds: 86_401 } }))).toThrow(/sweepSeconds/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, sweepSeconds: 2.5 } }))).toThrow(/sweepSeconds/);
    expect(() => validate(withPipeline({ briefs: { enabled: true, sweepSeconds: "60" } }))).toThrow(/sweepSeconds/);
    expect(() => validate(withPipeline({ briefs: [] }))).toThrow(/briefs must be a plain object/);
    expect(() => validate(withPipeline({ briefs: null }))).toThrow(/briefs must be a plain object/);
  });
});

describe("session pipeline: deploy plan contract", () => {
  const store = [
    "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
    "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
    "transcripts-key", "transcripts-key-alias",
  ];
  const all = [...store, ...SESSION_PIPELINE_RESOURCES];
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const plan = (actions: Record<string, string>, resources = all) => {
    const counts = new Map<string, number>();
    for (const [r, a] of Object.entries(actions)) if (resources.includes(r)) counts.set(a, (counts.get(a) ?? 0) + 1);
    const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
    return `${summary}\n${resources.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  };
  const check = (compact: string) => {
    const full = compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n");
    return assertTranscriptStorePlanContract(compact, {
      detailedPlanText: full,
      provenanceText: JSON.stringify({
        schema: 1,
        stage: STAGE,
        outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) } },
        summary: compact.split("\n")[0],
      }),
    });
  };
  const creates = Object.fromEntries(SESSION_PIPELINE_RESOURCES.map((r) => [r, "create"]));

  it("rollout: every pipeline resource created, and only the key and bucket policies updated", () => {
    expect(check(plan({ ...creates, transcripts: "update", "transcripts-key": "update" }))).toBe("session pipeline rollout");
    // The rest after a partial apply.
    expect(check(plan({ "session-events-mapping": "create", "session-pipeline-errors-alarm": "create" }))).toBe("session pipeline rollout");
    expect(check(plan({}))).toBe("post-apply");
  });

  it("update: pipeline resources change in place while the store is untouched", () => {
    expect(check(plan({ "session-pipeline": "update" }))).toBe("session pipeline update");
    // A store grant may update only by the role-ARN cascade (the next suite), which this plan does not show.
    expect(() => check(plan({ "session-pipeline": "update", transcripts: "update" }))).toThrow(/transcripts updates without the role-ARN cascade/);
  });

  it("refuses any other store change, any replacement or delete, and a partial resource list", () => {
    expect(() => check(plan({ ...creates, trail: "update" }))).toThrow(/action set does not match/);
    expect(() => check(plan({ ...creates, "transcript-reader": "update" }))).toThrow(/action set does not match/);
    expect(() => check(plan({ ...creates, "transcripts-key-alias": "create" }))).toThrow(/action set does not match/);
    for (const action of ["replace", "delete"]) expect(() => check(plan({ "session-events": action }))).toThrow(/forbidden action/);
    const missing = all.filter((r) => r !== "session-events-dlq");
    expect(() => check(plan(creates, missing))).toThrow(/missing resource session-events-dlq/);
    // A pipeline-free plan keeps the original contract.
    expect(check(plan({}, store))).toBe("post-apply");
    expect(() => check(plan({ transcripts: "update" }, store))).toThrow(/initial, recovery/);
  });

  const withAnalysis=[...all,...SESSION_ANALYSIS_CACHE_RESOURCES];
  it("analysis schedule is dormant and its invoke permission is bound to the exact rule/account",()=>{const input={names:p,functionArn:"arn:lambda",p:{},tags:{}};expect(analysisCacheScheduleProps(input)).toMatchObject({state:"DISABLED",scheduleExpression:"rate(1 minute)",targets:[{Input:JSON.stringify({analysisCache:true})}]});expect(analysisCacheScheduleProps({...input,p:{analysisCache:true}}).state).toBe("ENABLED");expect(analysisReconcileScheduleProps(input)).toMatchObject({state:"DISABLED",scheduleExpression:"rate(30 minutes)",targets:[{Input:JSON.stringify({analysisCacheReconcile:true})}]});const grants=sessionPipelinePermissions({accountId:ACCOUNT,functionName:p.function,ruleArn:`arn:aws:events:${REGION}:${ACCOUNT}:rule/${p.rule}`,briefRuleArn:"brief",analysisRuleArn:"exact-analysis-rule",roleArn:ROLE});expect(grants.at(-1)!.props).toEqual({action:"lambda:InvokeFunction",functionName:p.function,principal:"events.amazonaws.com",sourceArn:"exact-analysis-rule",sourceAccount:ACCOUNT});const both=sessionPipelinePermissions({accountId:ACCOUNT,functionName:p.function,ruleArn:`arn:aws:events:${REGION}:${ACCOUNT}:rule/${p.rule}`,briefRuleArn:"brief",analysisRuleArn:"exact-analysis-rule",analysisReconcileRuleArn:"exact-reconcile-rule",roleArn:ROLE});for(const [id,sourceArn] of [["session-analysis-cache-permission","exact-analysis-rule"],["session-analysis-reconcile-permission","exact-reconcile-rule"]])expect(both.find(g=>g.id===id)!.props).toEqual({action:"lambda:InvokeFunction",functionName:p.function,principal:"events.amazonaws.com",sourceArn,sourceAccount:ACCOUNT})});
  it("analysis phase requires both resources and refuses permission widening, mixed schedules and unrelated writes",()=>{const creates=Object.fromEntries(SESSION_ANALYSIS_CACHE_RESOURCES.map(r=>[r,"create"]));expect(check(plan(creates,withAnalysis))).toBe("session analysis cache rollout");expect(check(plan({"session-analysis-cache-schedule":"update"},withAnalysis))).toBe("session analysis cache update");for(const permission of ["session-analysis-cache-permission","session-analysis-reconcile-permission"])expect(()=>check(plan({[permission]:"update"},withAnalysis))).toThrow(/unsafe action combination/);expect(()=>check(plan(creates,[...all,"session-analysis-cache-schedule"]))).toThrow(/missing resource session-analysis-cache-permission/);expect(()=>check(plan({...creates,trail:"update"},withAnalysis))).toThrow(/must be noop or a session pipeline update/);expect(()=>check(plan({...creates,"session-brief-schedule":"update"},[...withAnalysis,...SESSION_BRIEF_RESOURCES]))).toThrow(/separate reviewed phases/);for(const action of ["delete","replace"])expect(()=>check(plan({"session-analysis-cache-schedule":action},withAnalysis))).toThrow(/forbidden action/)});
  it("analysis phases refuse unproven role and read-api cascades",()=>{const creates=Object.fromEntries(SESSION_ANALYSIS_CACHE_RESOURCES.map(r=>[r,"create"]));expect(()=>check(plan({...creates,"session-pipeline":"update",transcripts:"update"},withAnalysis))).toThrow(/transcripts updates without the role-ARN cascade/);const withApi=[...withAnalysis,"session-read-api","session-read-api-secret","session-read-api-errors-alarm","session-read-api-throttles-alarm"];expect(()=>check(plan({...creates,"session-read-api":"update"},withApi))).toThrow(/session-read-api may update only with no declared property changes/);expect(()=>check(plan({...creates,"session-read-api-errors-alarm":"update"},withApi))).toThrow(/read api may not change/)});
  const withJobs=[...all,...SESSION_ANALYSIS_JOB_RESOURCES];
  it("analysis job schedule is dormant and its invoke permission is bound to the exact rule/account",()=>{const input={names:p,functionArn:"arn:lambda",p:{},tags:{}};expect(analysisJobScheduleProps(input)).toMatchObject({state:"DISABLED",scheduleExpression:"rate(1 minute)",targets:[{Input:JSON.stringify({sessionAnalysis:true})}]});expect(analysisJobScheduleProps({...input,p:{analysisJobs:{enabled:true}}}).state).toBe("ENABLED");const grants=sessionPipelinePermissions({accountId:ACCOUNT,functionName:p.function,ruleArn:`arn:aws:events:${REGION}:${ACCOUNT}:rule/${p.rule}`,briefRuleArn:"brief",analysisJobRuleArn:"exact-job-rule",roleArn:ROLE});expect(grants.find(g=>g.id==="session-analysis-job-permission")!.props).toEqual({action:"lambda:InvokeFunction",functionName:p.function,principal:"events.amazonaws.com",sourceArn:"exact-job-rule",sourceAccount:ACCOUNT})});
  it("analysis job phase requires both resources and refuses permission widening, mixed schedules and unrelated writes",()=>{const creates=Object.fromEntries(SESSION_ANALYSIS_JOB_RESOURCES.map(r=>[r,"create"]));expect(check(plan(creates,withJobs))).toBe("session analysis jobs rollout");expect(check(plan({"session-analysis-job-schedule":"update"},withJobs))).toBe("session analysis jobs update");expect(()=>check(plan({"session-analysis-job-permission":"update"},withJobs))).toThrow(/unsafe action combination/);expect(()=>check(plan(creates,[...all,"session-analysis-job-schedule"]))).toThrow(/missing resource session-analysis-job-permission/);expect(()=>check(plan({...creates,"session-events-dlq":"create"},withJobs))).toThrow(/must be noop or session pipeline update/);expect(()=>check(plan({...creates,"session-brief-schedule":"update"},[...withJobs,...SESSION_BRIEF_RESOURCES]))).toThrow(/separate reviewed phase/);for(const action of ["delete","replace"])expect(()=>check(plan({"session-analysis-job-schedule":action},withJobs))).toThrow(/forbidden action/)});
  it("analysis job phases refuse unproven role and read-api cascades",()=>{const creates=Object.fromEntries(SESSION_ANALYSIS_JOB_RESOURCES.map(r=>[r,"create"]));expect(()=>check(plan({...creates,"session-pipeline":"update","session-events":"update"},withJobs))).toThrow(/session-events updates without the role-ARN cascade/);const withApi=[...withJobs,"session-read-api","session-read-api-secret","session-read-api-errors-alarm","session-read-api-throttles-alarm"];expect(()=>check(plan({...creates,"session-read-api":"update"},withApi))).toThrow(/session-read-api may update only with no declared property changes/);expect(()=>check(plan({...creates,"session-read-api-errors-alarm":"update"},withApi))).toThrow(/read api may not change/)});
  const withBufferedFeed = [...all, ...SESSION_BUFFERED_SEARCH_FEED_RESOURCES];
  const armed = { searchIndex: true, bufferedSearchFeed: { enabled: true, gateDeclared: BUFFERED_SEARCH_FEED_GATE_CONTRACT } };
  it("buffered feed defaults disabled and sets no feed variable",()=>{
    expect(bufferedSearchFeedScheduleProps({names:p,functionArn:"fn",p:{},tags:{}})).toMatchObject({state:"DISABLED",scheduleExpression:"rate(1 minute)",targets:[{Input:JSON.stringify({bufferedSearchFeed:true})}]});
    const env=sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:{}});
    expect(env.INGEST_BUFFERED_SEARCH_FEED).toBeUndefined();
    expect(env.BUFFERED_SEARCH_FEED_GATE_DECLARED).toBeUndefined();
    expect(bufferedFeedConfig(env).enabled).toBe(false);
    const off={bufferedSearchFeed:{enabled:false}};
    expect(bufferedSearchFeedScheduleProps({names:p,functionArn:"fn",p:off,tags:{}}).state).toBe("DISABLED");
    expect(sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:off}).INGEST_BUFFERED_SEARCH_FEED).toBeUndefined();
  });
  it("buffered feed page/rate/budget knobs are passed through only when configured, within the feeder's bounds",()=>{
    const knobs={pageSize:100,stagesPerSecond:10,budgetMs:60000};
    const env=sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:{searchIndex:true,bufferedSearchFeed:{enabled:false,...knobs}}});
    expect(env).toMatchObject({INGEST_BUFFERED_SEARCH_FEED_PAGE:"100",INGEST_BUFFERED_SEARCH_FEED_RATE:"10",INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS:"60000"});
    expect(env.INGEST_BUFFERED_SEARCH_FEED).toBeUndefined();
    const refused=(v:unknown)=>{let why="";try{assertSessionPipelineInputs(v,(w:string)=>{throw new Error(w)})}catch(e){why=(e as Error).message}return why};
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:false,...knobs}})).toBe("");
    expect(refused({bufferedSearchFeed:{enabled:false,pageSize:0}})).toMatch(/pageSize must be an integer from 1 to 200/);
    expect(refused({bufferedSearchFeed:{enabled:false,pageSize:201}})).toMatch(/pageSize/);
    expect(refused({bufferedSearchFeed:{enabled:false,stagesPerSecond:51}})).toMatch(/stagesPerSecond must be an integer from 1 to 50/);
    expect(refused({bufferedSearchFeed:{enabled:false,budgetMs:4999}})).toMatch(/budgetMs must be an integer from 5000 to 240000/);
    expect(refused({bufferedSearchFeed:{enabled:false,budgetMs:1.5}})).toMatch(/budgetMs/);
  });
  it("buffered feed knobs are absent when not configured, so the feeder defaults apply",()=>{
    for(const cfg of [{},{searchIndex:true,bufferedSearchFeed:{enabled:true,gateDeclared:BUFFERED_SEARCH_FEED_GATE_CONTRACT}}]){
      const env=sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:cfg});
      for(const k of ["INGEST_BUFFERED_SEARCH_FEED_PAGE","INGEST_BUFFERED_SEARCH_FEED_RATE","INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS"])expect(Object.hasOwn(env,k)).toBe(false);
    }
    const c=bufferedFeedConfig(sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:{}}));
    expect([c.pageSize,c.stagesPerSecond,c.budgetMs]).toEqual([50,5,50000]);
  });
  it("buffered feed arms only with the gate's exact contract, and the feeder accepts that env",()=>{
    expect(BUFFERED_SEARCH_FEED_GATE_CONTRACT).toBe(BUFFERED_SEARCH_GATE_CONTRACT);
    expect(bufferedSearchFeedScheduleProps({names:p,functionArn:"fn",p:armed,tags:{}}).state).toBe("ENABLED");
    const env=sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:armed});
    expect(env).toMatchObject({INGEST_BUFFERED_SEARCH_FEED:"on",BUFFERED_SEARCH_FEED_GATE_DECLARED:BUFFERED_SEARCH_GATE_CONTRACT});
    expect(bufferedFeedConfig(env).enabled).toBe(true);
    expect(bufferedFeedGateRefusal(env)).toBeNull();
    const refused=(v:unknown)=>{let why="";try{assertSessionPipelineInputs(v,(w:string)=>{throw new Error(w)})}catch(e){why=(e as Error).message}return why};
    expect(refused(armed)).toBe("");
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:false}})).toBe("");
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:true}})).toMatch(/requires gateDeclared/);
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:true,gateDeclared:"buffered-search-gate/v2"}})).toMatch(/must be exactly/);
    expect(refused({bufferedSearchFeed:armed.bufferedSearchFeed})).toMatch(/requires searchIndex/);
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:"on"}})).toMatch(/must be a boolean/);
    expect(refused({searchIndex:true,bufferedSearchFeed:{enabled:false,extra:1}})).toMatch(/not a known setting/);
    expect(JSON.stringify(bufferedSearchFeedScheduleProps({names:p,functionArn:"fn",p:armed,tags:{}}))).not.toMatch(/database_url|ca_pem|password/);
    const permission=sessionPipelinePermissions({accountId:ACCOUNT,functionName:p.function,ruleArn:"old",briefRuleArn:"brief",bufferedSearchFeedRuleArn:"exact-feed-rule",roleArn:ROLE}).at(-1)!;
    expect(permission.id).toBe("session-buffered-search-feed-permission");
    expect(permission.props).toEqual({action:"lambda:InvokeFunction",functionName:p.function,principal:"events.amazonaws.com",sourceArn:"exact-feed-rule",sourceAccount:ACCOUNT});
  });
  it("buffered feed phase keeps permission create-only and refuses mixed schedules and unrelated changes",()=>{
    const creates=Object.fromEntries(SESSION_BUFFERED_SEARCH_FEED_RESOURCES.map(r=>[r,"create"]));
    expect(check(plan(creates,withBufferedFeed))).toBe("session buffered search feed rollout");
    expect(check(plan({"session-buffered-search-feed-schedule":"update"},withBufferedFeed))).toBe("session buffered search feed update");
    expect(()=>check(plan({"session-buffered-search-feed-permission":"update"},withBufferedFeed))).toThrow(/unsafe action combination/);
    expect(()=>check(plan(creates,[...all,"session-buffered-search-feed-schedule"]))).toThrow(/missing resource session-buffered-search-feed-permission/);
    for(const action of ["delete","replace"])expect(()=>check(plan({"session-buffered-search-feed-schedule":action},withBufferedFeed))).toThrow(/forbidden action/);
    expect(()=>check(plan({...creates,trail:"update"},withBufferedFeed))).toThrow(/separate reviewed phase/);
    expect(()=>check(plan({...creates,"session-semantic-copy-schedule":"update"},[...withBufferedFeed,...SESSION_SEMANTIC_COPY_RESOURCES]))).toThrow(/separate reviewed phase/);
    expect(()=>check(plan({...creates,"session-pipeline":"update",transcripts:"update"},withBufferedFeed))).toThrow(/without the role-ARN cascade/);
  });
  const withSemantic=[...all,...SESSION_SEMANTIC_COPY_RESOURCES];
  it("semantic copier defaults disabled with exact rule/account invoke permission",()=>{
    expect(semanticCopyScheduleProps({names:p,functionArn:"fn",p:{},tags:{}})).toMatchObject({state:"DISABLED",scheduleExpression:"rate(1 minute)",targets:[{Input:JSON.stringify({semanticCopy:true})}]});
    expect(sessionPipelineEnv({stage:STAGE,bucket:t.bucket,names:p,p:{}})).toMatchObject({SESSION_SEMANTIC_COPY_ENABLED:"0",SESSION_SEARCH_FEATURE_SOURCE_ID:""});
    const enabled={semanticCopy:{enabled:true,sourceId:"reviewed-source"}};
    expect(semanticCopyScheduleProps({names:p,functionArn:"fn",p:enabled,tags:{}}).state).toBe("ENABLED");
    const permission=sessionPipelinePermissions({accountId:ACCOUNT,functionName:p.function,ruleArn:"old",briefRuleArn:"brief",semanticRuleArn:"exact-semantic-rule",roleArn:ROLE}).at(-1)!;
    expect(permission.props).toEqual({action:"lambda:InvokeFunction",functionName:p.function,principal:"events.amazonaws.com",sourceArn:"exact-semantic-rule",sourceAccount:ACCOUNT});
  });
  it("semantic phase keeps permission create-only and refuses mixed schedules, unrelated changes and unproven cascades",()=>{
    const creates=Object.fromEntries(SESSION_SEMANTIC_COPY_RESOURCES.map(r=>[r,"create"]));expect(check(plan(creates,withSemantic))).toBe("session semantic copy rollout");
    expect(check(plan({"session-semantic-copy-schedule":"update"},withSemantic))).toBe("session semantic copy update");
    expect(()=>check(plan({"session-semantic-copy-permission":"update"},withSemantic))).toThrow(/unsafe action combination/);
    expect(()=>check(plan(creates,[...all,"session-semantic-copy-schedule"]))).toThrow(/missing resource session-semantic-copy-permission/);
    for(const action of ["delete","replace"])expect(()=>check(plan({"session-semantic-copy-schedule":action},withSemantic))).toThrow(/forbidden action/);
    expect(()=>check(plan({...creates,trail:"update"},withSemantic))).toThrow(/separate reviewed phase/);
    expect(()=>check(plan({...creates,"session-brief-schedule":"update"},[...withSemantic,...SESSION_BRIEF_RESOURCES]))).toThrow(/separate reviewed phase/);
    expect(()=>check(plan({...creates,"session-pipeline":"update",transcripts:"update"},withSemantic))).toThrow(/without the role-ARN cascade/);
    const withApi=[...withSemantic,"session-read-api","session-read-api-secret","session-read-api-errors-alarm","session-read-api-throttles-alarm"];
    expect(()=>check(plan({...creates,"session-read-api":"update"},withApi))).toThrow(/only with no declared property changes/);
  });

  const withBrief = [...all, ...SESSION_BRIEF_RESOURCES];
  const briefCreates = Object.fromEntries(SESSION_BRIEF_RESOURCES.map((r) => [r, "create"]));

  it("brief rollout: the schedule's resources created, alone or with exactly a session pipeline update", () => {
    expect(check(plan({ ...briefCreates, "session-pipeline": "update" }, withBrief))).toBe("session brief rollout");
    expect(check(plan(briefCreates, withBrief))).toBe("session brief rollout");
    // The rest after a partial apply.
    expect(check(plan({ "session-brief-permission": "create" }, withBrief))).toBe("session brief rollout");
    expect(check(plan({}, withBrief))).toBe("post-apply");
  });

  it("brief update: the rule changes in place (enabled flips), alone or with the function's env", () => {
    expect(check(plan({ "session-brief-schedule": "update", "session-pipeline": "update" }, withBrief))).toBe("session brief update");
    expect(check(plan({ "session-brief-schedule": "update" }, withBrief))).toBe("session brief update");
  });

  it("brief phases refuse store changes, pipeline creates, permission updates, mixed actions, missing resources and a read api change", () => {
    expect(() => check(plan({ ...briefCreates, trail: "update" }, withBrief))).toThrow(/must be noop or a session pipeline update/);
    expect(() => check(plan({ ...briefCreates, "transcript-reader": "update" }, withBrief))).toThrow(/must be noop or a session pipeline update/);
    expect(() => check(plan({ ...briefCreates, "session-events-dlq": "create" }, withBrief))).toThrow(/must be noop or a session pipeline update/);
    // A store grant may update only by the role-ARN cascade, which this plan does not show.
    expect(() => check(plan({ ...briefCreates, "session-pipeline": "update", transcripts: "update" }, withBrief))).toThrow(/transcripts updates without the role-ARN cascade/);
    expect(() => check(plan({ "session-brief-schedule": "create", "session-brief-permission": "update" }, withBrief))).toThrow(/unsafe action combination/);
    expect(() => check(plan({ "session-brief-permission": "update" }, withBrief))).toThrow(/unsafe action combination/);
    for (const action of ["replace", "delete"]) expect(() => check(plan({ "session-brief-schedule": action }, withBrief))).toThrow(/forbidden action/);
    expect(() => check(plan(briefCreates, [...all, "session-brief-schedule"]))).toThrow(/missing resource session-brief-permission/);
    // Brief resources never come without the pipeline's.
    expect(() => check(plan(briefCreates, [...store, ...SESSION_BRIEF_RESOURCES]))).toThrow(/missing resource session-pipeline/);
    const withApi = [...withBrief, "session-read-api", "session-read-api-secret", "session-read-api-errors-alarm", "session-read-api-throttles-alarm"];
    // A read api update is judged as its code-only cascade, which this compact plan does not show.
    expect(() => check(plan({ ...briefCreates, "session-read-api": "update" }, withApi))).toThrow(/session-read-api may update only with no declared property changes/);
    for (const changed of [{ "session-read-api": "create" }, { "session-read-api-errors-alarm": "update" }, { "session-read-api-secret": "update" }])
      expect(() => check(plan({ ...briefCreates, ...changed }, withApi))).toThrow(/the read api may not change in the same plan/);
    expect(check(plan(briefCreates, withApi))).toBe("session brief rollout");
  });
});

// The real capture of #141's code-only update (bin/96bulls plan aws-sessions-prod --capture at 84a962c), sanitized as the
// capture writes it. The function updates, and the five resources that name its role re-plan the role ARN as
// `(known after apply)`: Alchemy keeps the function's roleName stable, not its roleArn.
describe("session pipeline: update with the role-ARN cascade", () => {
  const LIVE_ROLE = `arn:aws:iam::${ACCOUNT}:role/96bulls-aws-session-pipeline-aws-sessions-prod-nsvhn3beskmuvvon`;
  const REAL = readFileSync(fileURLToPath(new URL("./fixtures/aws-sessions-prod.pipeline-update.detailed.txt", import.meta.url)), "utf8");
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const isResourceLine = (l: string) => /^\[[a-z0-9-]+\] \S+$/.test(l);
  /** Recount the summary from the resource lines, so a test changes only what it means to change. */
  const recount = (full: string) => {
    const counts = new Map<string, number>();
    for (const l of full.split("\n")) if (isResourceLine(l) && !l.endsWith(" noop")) counts.set(l.split(" ")[1]!, (counts.get(l.split(" ")[1]!) ?? 0) + 1);
    const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: no changes";
    return full.replace(/^Plan: .*$/m, summary);
  };
  const snapshot = (fn: object | null = { status: "created", attr: { roleArn: LIVE_ROLE } }) => JSON.stringify({
    schema: 1,
    objects: [
      { key: `96bulls/96bulls-aws/${STAGE}/transcripts-key.json`, text: JSON.stringify({ status: "created", attr: {} }) },
      ...(fn ? [{ key: `96bulls/96bulls-aws/${STAGE}/session-pipeline.json`, text: JSON.stringify(fn) }] : []),
    ],
  });
  /** `null`: the deploy's check, which has no state snapshot. */
  const check = (detailed: string, stateSnapshot: string | null = snapshot()) => {
    const full = recount(detailed);
    const compact = `${full.split("\n").filter((l) => l.startsWith("Plan:") || isResourceLine(l)).join("\n")}\n`;
    return assertTranscriptStorePlanContract(compact, {
      detailedPlanText: full,
      provenanceText: JSON.stringify({
        schema: 1,
        stage: STAGE,
        outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) } },
        summary: compact.split("\n")[0],
      }),
      stateSnapshotText: stateSnapshot ?? undefined,
    });
  };
  /** Rewrite the n-th line (0-based) of a resource's block that matches `find`. */
  const edit = (text: string, resource: string, find: RegExp | string, to: (line: string) => string | string[], nth = 0) => {
    const lines = text.split("\n");
    const start = lines.indexOf(`[${resource}] update`);
    if (start < 0) throw new Error(`fixture has no ${resource} update`);
    let seen = 0;
    for (let i = start + 1; i < lines.length && !isResourceLine(lines[i]!); i++) {
      const hit = typeof find === "string" ? lines[i] === find : find.test(lines[i]!);
      if (hit && seen++ === nth) {
        const out = to(lines[i]!);
        lines.splice(i, 1, ...(Array.isArray(out) ? out : [out]));
        return lines.join("\n");
      }
    }
    throw new Error(`fixture ${resource} has no match ${String(find)} #${nth}`);
  };
  const op = (text: string, resource: string, action: string) => text.replace(`[${resource}] noop`, `[${resource}] ${action}`);

  it("accepts the real cascade from the #141 capture as a session pipeline update", () => {
    const ops = REAL.split("\n").filter((l) => isResourceLine(l) && !l.endsWith(" noop"));
    expect(ops).toEqual(["[session-events] update", "[session-pipeline] update", "[session-pipeline-secret] update", "[session-reconcile-continue] update", "[transcripts] update", "[transcripts-key] update"]);
    expect(check(REAL)).toBe("session pipeline update");
    // The deploy has no snapshot; it is bound to the captured state by the version digest.
    expect(check(REAL, null)).toBe("session pipeline update");
    // A subset of the grants (the rest noop) is the same cascade.
    const keyOnly = REAL.replace(/\[transcripts-key\] update\n[\s\S]*?(?=\n\[transcripts-key-alias\])/, "[transcripts-key] noop\n");
    expect(check(keyOnly)).toBe("session pipeline update");
  });

  it("brief rollout on the real cascade: the schedule's creates plus the #141-shaped pipeline update", () => {
    const withBrief = (text: string, action = "create") => `${text.replace(/\n*$/, "")}\n[session-brief-schedule] ${action}\n[session-brief-permission] ${action}\n`;
    expect(check(withBrief(REAL))).toBe("session brief rollout");
    expect(check(withBrief(REAL), null)).toBe("session brief rollout");
    // The cascade is still checked line by line inside the brief phase.
    const other = edit(REAL, "session-pipeline-secret", "  +           AWS: (known after apply)", () => `  +           AWS: arn:aws:iam::${ACCOUNT}:role/someone-else`);
    expect(() => check(withBrief(other))).toThrow(/session-pipeline-secret changes .*someone-else.* not only the role-ARN cascade/);
    expect(() => check(withBrief(op(REAL, "trail", "update")))).toThrow(/must be noop or a session pipeline update/);
  });

  // The 53c60ed capture's shape (#166): shared code moved since the read API's last deploy, so
  // its function re-plans with no declared property changes and its secret re-plans its own role.
  const API_ROLE = `arn:aws:iam::${ACCOUNT}:role/96bulls-aws-session-read-api-aws-sessions-prod-q3tjk5c7wi43htnh`;
  const codeOnlyApiBlocks = (secretRole = API_ROLE, fn = ["  no declared property changes"]) => [
    "[session-read-api] update", ...fn, "",
    "[session-read-api-errors-alarm] noop", "",
    "[session-read-api-secret] update",
    "  properties:",
    "    resourcePolicy:",
    "  -     Statement:", "  -         Effect: Allow", "  -         Principal:", `  -           AWS: ${secretRole}`, '  -         Resource: "*"', "  -         Sid: SessionReadApiReads",
    "  +     Statement:", "  +         Effect: Allow", "  +         Principal:", "  +           AWS: (known after apply)", '  +         Resource: "*"', "  +         Sid: SessionReadApiReads",
    "      Version: 2012-10-17", "",
    "[session-read-api-throttles-alarm] noop",
  ].join("\n");
  const withBriefAndApi = (text: string, api = codeOnlyApiBlocks()) =>
    `${text.replace(/\n*$/, "")}\n[session-brief-schedule] create\n[session-brief-permission] create\n${api}\n`;
  const snapshotWithApi = (apiRole: string = API_ROLE) => JSON.stringify({
    schema: 1,
    objects: [
      ...(JSON.parse(snapshot()) as { objects: object[] }).objects,
      { key: `96bulls/96bulls-aws/${STAGE}/session-read-api.json`, text: JSON.stringify({ status: "updated", attr: { roleArn: apiRole } }) },
    ],
  });

  it("brief rollout with the read api's code-only cascade: function unchanged, secret re-plans only its own role", () => {
    expect(check(withBriefAndApi(REAL), snapshotWithApi())).toBe("session brief rollout");
    // The deploy has no snapshot; the ARN must still be one role in the account.
    expect(check(withBriefAndApi(REAL), null)).toBe("session brief rollout");
    // Without a secret change the function alone is the same shape.
    const fnOnly = codeOnlyApiBlocks().replace(/\[session-read-api-secret\] update[\s\S]*?(?=\n\[session-read-api-throttles-alarm\])/, "[session-read-api-secret] noop\n");
    expect(check(withBriefAndApi(REAL, fnOnly), snapshotWithApi())).toBe("session brief rollout");
  });

  it("brief rollout refuses a read api change beyond its code-only cascade", () => {
    // Any declared property change on the function (env, memory, url, ...).
    const declared = codeOnlyApiBlocks(API_ROLE, ["  properties:", "  -   memorySize: 1024", "  +   memorySize: 2048"]);
    expect(() => check(withBriefAndApi(REAL, declared), snapshotWithApi())).toThrow(/session-read-api may update only with no declared property changes/);
    // The secret naming another role than the read api's own, or the pipeline's.
    expect(() => check(withBriefAndApi(REAL, codeOnlyApiBlocks(`arn:aws:iam::${ACCOUNT}:role/someone-else`)), snapshotWithApi())).toThrow(/replaces .*someone-else.*, which is not the read api's role/);
    expect(() => check(withBriefAndApi(REAL, codeOnlyApiBlocks(LIVE_ROLE)), snapshotWithApi(LIVE_ROLE))).toThrow(/replaces the session pipeline's role/);
    expect(() => check(withBriefAndApi(REAL, codeOnlyApiBlocks(`arn:aws:iam::123456789012:role/x`)), null)).toThrow(/exactly one role ARN in 941433935742/);
    // The secret's policy gaining a statement.
    const widened = codeOnlyApiBlocks().replace('  +         Sid: SessionReadApiReads', '  +         Sid: SessionReadApiReads\n  +       - Action: secretsmanager:PutSecretValue');
    expect(() => check(withBriefAndApi(REAL, widened), snapshotWithApi())).toThrow(/adds or removes lines/);
    // An alarm change, or a create, is never the code-only cascade.
    expect(() => check(withBriefAndApi(REAL, codeOnlyApiBlocks().replace("[session-read-api-errors-alarm] noop", "[session-read-api-errors-alarm] update")), snapshotWithApi())).toThrow(/the read api may not change in the same plan/);
    // The pipeline cascade is still checked alongside.
    const other = edit(REAL, "session-pipeline-secret", "  +           AWS: (known after apply)", () => `  +           AWS: arn:aws:iam::${ACCOUNT}:role/someone-else`);
    expect(() => check(withBriefAndApi(other), snapshotWithApi())).toThrow(/session-pipeline-secret changes .*someone-else/);
  });

  it("refuses a grant diff that adds or removes a statement or line", () => {
    const addStatement = edit(REAL, "session-pipeline-secret", "  +         Sid: SessionPipelineReads", (l) => [l,
      "  +       - Action: secretsmanager:PutSecretValue", "  +         Effect: Allow", "  +         Principal:", "  +           AWS: (known after apply)", '  +         Resource: "*"']);
    expect(() => check(addStatement)).toThrow(/session-pipeline-secret adds or removes lines \(7 - \/ 12 \+\)/);
    const dropStatement = REAL.replace(/(  \+     - Action:\n  \+         - s3:GetObject\n(?:  \+ .*\n)*?  \+       Sid: SessionPipelineReads\n)/, "");
    expect(dropStatement).not.toBe(REAL);
    expect(() => check(dropStatement)).toThrow(/transcripts adds or removes lines/);
    const dropMinus = edit(REAL, "session-events", "  -         Sid: DenyInsecureTransport", () => []);
    expect(() => check(dropMinus)).toThrow(/session-events adds or removes lines/);
    // A whole new key (only + lines) is refused too.
    const newKey = edit(REAL, "session-reconcile-continue", /^  \+   principal:/, (l) => [l, "  +   sourceAccount: \"1\""]);
    expect(() => check(newKey)).toThrow(/session-reconcile-continue adds or removes lines/);
  });

  it("refuses a grant diff that changes an Action, Effect, Resource, Sid or condition", () => {
    for (const [resource, find, to, nth] of [
      ["session-pipeline-secret", "  +       - Action: secretsmanager:GetSecretValue", "  +       - Action: secretsmanager:*", 0],
      ["session-events", "  +         Effect: Allow", "  +         Effect: Deny", 1],
      ["session-events", "  +           - sqs:SendMessage", "  +           - sqs:PurgeQueue", 0],
      ["transcripts", "  +         - arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742/96bulls/*/v1/*", "  +         - arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742/*", 0],
      ["transcripts", "  +       Sid: OnlyTheEraserDeletes", "  +       Sid: AnyoneDeletes", 0],
      ["transcripts-key", "  +             kms:ViaService: s3.us-east-1.amazonaws.com", "  +             kms:ViaService: lambda.us-east-1.amazonaws.com", 3],
      ["session-reconcile-continue", "    action: lambda:InvokeFunction", "  -   action: lambda:InvokeFunction\n  +   action: lambda:*", 0],
    ] as const) {
      const mutated = edit(REAL, resource, find, () => to.split("\n"), nth);
      expect(() => check(mutated)).toThrow(new RegExp(`${resource} changes .* not only the role-ARN cascade`));
    }
  });

  it("refuses a cascade that swaps in, or replaces, a different ARN", () => {
    // The role ARN becomes another principal instead of (known after apply).
    const other = edit(REAL, "session-pipeline-secret", "  +           AWS: (known after apply)", () => `  +           AWS: arn:aws:iam::${ACCOUNT}:role/someone-else`);
    expect(() => check(other)).toThrow(/session-pipeline-secret changes .*someone-else.* not only the role-ARN cascade/);
    const star = edit(REAL, "transcripts", "  +         AWS: (known after apply)", () => '  +         AWS: "*"');
    expect(() => check(star)).toThrow(/transcripts changes .* not only the role-ARN cascade/);
    // Another principal (the writer user) re-planned as unknown: not the pipeline's role.
    const writer = edit(REAL, "transcripts-key", "  +           AWS: arn:aws:iam::941433935742:user/96bulls/96bulls-aws-sessions-prod-transcript-writer", () => "  +           AWS: (known after apply)");
    expect(() => check(writer)).toThrow(/transcripts-key replaces arn:aws:iam::941433935742:user\/96bulls\/96bulls-aws-sessions-prod-transcript-writer, which is not the session pipeline's role/);
    expect(() => check(writer, null)).toThrow(/must replace exactly one role ARN/);
    // The state's role differs from the ARN the plan replaces.
    expect(() => check(REAL, snapshot({ status: "created", attr: { roleArn: `arn:aws:iam::${ACCOUNT}:role/another-role` } }))).toThrow(/is not the session pipeline's role arn:aws:iam::941433935742:role\/another-role/);
    // Without a snapshot, two different role ARNs across the grants are refused.
    const twoRoles = REAL.replace(`  -           AWS: ${LIVE_ROLE}\n  -         Resource: "*"\n  -         Sid: SessionPipelineReads`, `  -           AWS: arn:aws:iam::${ACCOUNT}:role/another-role\n  -         Resource: "*"\n  -         Sid: SessionPipelineReads`);
    expect(twoRoles).not.toBe(REAL);
    expect(() => check(twoRoles, null)).toThrow(/must replace exactly one role ARN/);
    // The ARN is unknown under a different key than Principal.AWS / principal.
    const sourceArn = edit(edit(REAL, "session-events", "  -             aws:SourceArn: arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742", () => `  -             aws:SourceArn: ${LIVE_ROLE}`),
      "session-events", "  +             aws:SourceArn: arn:aws:s3:::96bulls-aws-sessions-prod-transcripts-941433935742", () => "  +             aws:SourceArn: (known after apply)");
    expect(() => check(sourceArn)).toThrow(/session-events changes .*aws:SourceArn.* not only the role-ARN cascade/);
  });

  it("refuses a grant update without the cascade, and a state snapshot without the function's role", () => {
    const noProps = REAL.replace(/\[session-pipeline-secret\] update\n[\s\S]*?(?=\n\[session-reconcile-continue\])/, "[session-pipeline-secret] update\n  no declared property changes\n");
    expect(() => check(noProps)).toThrow(/session-pipeline-secret updates without the role-ARN cascade/);
    expect(() => check(REAL, snapshot(null))).toThrow(/state snapshot has 0 session-pipeline records/);
    expect(() => check(REAL, snapshot({ status: "creating", attr: { roleArn: LIVE_ROLE } }))).toThrow(/session-pipeline is not created in state/);
    expect(() => check(REAL, snapshot({ status: "created", attr: {} }))).toThrow(/session-pipeline is not created in state/);
    expect(() => check(REAL, "not json")).toThrow(/state snapshot is not valid JSON/);
  });

  it("refuses the cascade together with any other resource change", () => {
    for (const r of ["trail", "transcript-reader", "transcripts-key-alias", "transcript-srv-writer"])
      expect(() => check(op(REAL, r, "update"))).toThrow(/action set does not match the session pipeline rollout, update or post-apply/);
    for (const r of ["session-events-dlq", "session-events-mapping", "session-reconcile-schedule", "session-reconcile-permission", "session-pipeline-errors-alarm"])
      expect(() => check(op(REAL, r, "update"))).toThrow(new RegExp(`role-ARN cascade comes with other changes: ${r}:update`));
    // A create alongside the update is neither rollout nor update.
    expect(() => check(op(REAL, "session-events-dlq-alarm", "create"))).toThrow(/action set does not match the session pipeline rollout, update or post-apply/);
    for (const action of ["replace", "delete"]) expect(() => check(op(REAL, "session-events-dlq", action))).toThrow(/forbidden action/);
    // The grants cascade only because the function updates.
    expect(() => check(REAL.replace("[session-pipeline] update\n  no declared property changes", "[session-pipeline] noop"))).toThrow(/update but session-pipeline does not/);
  });

  // The sessions-prod capture at 9e5c191 (#151): the read API's failed first apply left its errors
  // alarm in state, and main moved since the pipeline's last deploy (27f053e), so the one plan is the
  // real #141 cascade above (re-tagged as after #148) plus the remaining read API creates. The
  // compact action set is the capture's own; the read API create bodies are not compared by the
  // contract and are abbreviated here.
  const readApiBlocks = (actions: Record<string, string>) =>
    ["session-read-api", "session-read-api-errors-alarm", "session-read-api-secret", "session-read-api-throttles-alarm"]
      .map((r) => (actions[r] === "noop" ? `[${r}] noop\n` : `[${r}] ${actions[r]}\n  properties:\n    tags:\n      96bulls:component: session-read-api\n      96bulls:repo: AgentWorkforce/sessions-pipeline\n      96bulls:stack: aws-sessions-prod\n`))
      .join("\n");
  const REMAINING = { "session-read-api": "create", "session-read-api-errors-alarm": "noop", "session-read-api-secret": "create", "session-read-api-throttles-alarm": "create" };
  const withReadApi = (actions: Record<string, string> = REMAINING, base = REAL) => {
    const renamed = base.replaceAll("96bulls:repo: AgentWorkforce/96BullSessions", "96bulls:repo: AgentWorkforce/sessions-pipeline");
    const out = renamed.replace("[session-reconcile-continue] update", `${readApiBlocks(actions)}\n[session-reconcile-continue] update`);
    if (out === renamed) throw new Error("fixture has no session-reconcile-continue update");
    return out;
  };
  const CAPTURE_151 = "session-events:update, session-pipeline:update, session-pipeline-secret:update, session-read-api:create, session-read-api-secret:create, session-read-api-throttles-alarm:create, session-reconcile-continue:update, transcripts:update, transcripts-key:update";

  it("read api rollout with the pipeline cascade: accepts the 9e5c191 capture's action set", () => {
    const plan = withReadApi();
    const changes = plan.split("\n").filter((l) => isResourceLine(l) && !l.endsWith(" noop")).map((l) => l.slice(1).replace("] ", ":"));
    expect(changes.join(", ")).toBe(CAPTURE_151);
    expect(check(plan)).toBe("session read api rollout");
    expect(check(plan, null)).toBe("session read api rollout");
    // All four read api resources created (nothing applied yet) is the same rollout.
    expect(check(withReadApi({ ...REMAINING, "session-read-api-errors-alarm": "create" }))).toBe("session read api rollout");
  });

  it("read api rollout with the pipeline cascade: session-read-api must be created, never updated or noop", () => {
    expect(() => check(withReadApi({ ...REMAINING, "session-read-api": "update" }))).toThrow(/read api changes alone/);
    expect(() => check(withReadApi({ ...REMAINING, "session-read-api": "noop" }))).toThrow(/read api changes alone/);
    expect(() => check(withReadApi({ ...REMAINING, "session-read-api-errors-alarm": "update" }))).toThrow(/read api changes alone/);
    expect(() => check(withReadApi({ ...REMAINING, "session-read-api-secret": "update" }))).toThrow(/read api changes alone/);
  });

  it("read api rollout with the pipeline cascade: the pipeline may only carry a code-only update and the exact cascade", () => {
    // A declared property change on the function.
    const props = withReadApi().replace("[session-pipeline] update\n  no declared property changes", "[session-pipeline] update\n  properties:\n  -   memorySize: 1024\n  +   memorySize: 2048");
    expect(() => check(props)).toThrow(/session-pipeline may update only with no declared property changes/);
    // A grant that changes more than the role ARN (assertPipelineRoleCascadeDiff, unchanged).
    const action = edit(withReadApi(), "session-pipeline-secret", "  +       - Action: secretsmanager:GetSecretValue", () => "  +       - Action: secretsmanager:*");
    expect(() => check(action)).toThrow(/session-pipeline-secret changes .* not only the role-ARN cascade/);
    const swapped = edit(withReadApi(), "transcripts", "  +         AWS: (known after apply)", () => '  +         AWS: "*"');
    expect(() => check(swapped)).toThrow(/transcripts changes .* not only the role-ARN cascade/);
    expect(() => check(withReadApi(), snapshot({ status: "created", attr: { roleArn: `arn:aws:iam::${ACCOUNT}:role/another-role` } }))).toThrow(/is not the session pipeline's role/);
    const noCascade = withReadApi().replace(/\[session-pipeline-secret\] update\n[\s\S]*?(?=\n\[session-read-api\])/, "[session-pipeline-secret] update\n  no declared property changes\n");
    expect(() => check(noCascade)).toThrow(/session-pipeline-secret updates without the role-ARN cascade/);
    // Fewer than the five grants, or the grants without the function.
    const keyOnly = withReadApi().replace(/\[transcripts-key\] update\n[\s\S]*?(?=\n\[transcripts-key-alias\])/, "[transcripts-key] noop\n");
    expect(() => check(keyOnly)).toThrow(/read api changes alone/);
    expect(() => check(withReadApi().replace("[session-pipeline] update\n  no declared property changes", "[session-pipeline] noop"))).toThrow(/read api changes alone/);
  });

  it("read api rollout with the pipeline cascade: refuses any other change, and a tag change", () => {
    for (const r of ["session-events-mapping", "session-events-dlq", "session-pipeline-errors-alarm", "session-reconcile-schedule", "trail", "budget", "transcripts-key-alias"])
      expect(() => check(op(withReadApi(), r, "update"))).toThrow(/read api changes alone/);
    expect(() => check(op(withReadApi(), "session-events-dlq-alarm", "create"))).toThrow(/read api changes alone/);
    for (const action of ["replace", "delete"]) expect(() => check(op(withReadApi(), "trail", action))).toThrow(/forbidden action/);
    // A tag change on a grant is the repo tag rename's own phase, which refuses creates.
    const tag = edit(withReadApi(), "session-pipeline-secret", "      96bulls:repo: AgentWorkforce/sessions-pipeline", () => ["  -   96bulls:repo: AgentWorkforce/sessions-pipeline", "  +   96bulls:repo: AgentWorkforce/elsewhere"]);
    expect(() => check(tag)).toThrow(/repo tag rename/);
  });
});
