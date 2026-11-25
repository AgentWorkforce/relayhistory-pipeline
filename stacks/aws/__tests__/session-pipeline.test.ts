import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import * as AWS from "@alchemy.run/aws";
import { fileURLToPath } from "node:url";
import type { AwsInputs } from "../inputs.ts";
import {
  ALARM_DEFAULTS,
  DELIVERY_DELAY_SECONDS,
  POOLER_BUDGET,
  SESSION_SEARCH_QUEUE_RESOURCES,
  sessionDlqPolicy,
  sessionEventsMappingProps,
  sessionPoolDemand,
  sessionPoolSettings,
  sessionPoolTransitionDemand,
  sessionSearchAlarms,
  sessionSearchDlqProps,
  sessionSearchMappingProps,
  sessionSearchQueuePolicy,
  sessionSearchQueueProps,
  type SessionPipelineInputs,
  type SessionSearchQueueInputs,
  FUNCTION_TIMEOUT_SECONDS,
  SESSION_BRIEF_RESOURCES,
  SESSION_PIPELINE_RESOURCES,
  VISIBILITY_TIMEOUT_SECONDS,
  assertSessionPipelineInputs,
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
} from "../session-pipeline.ts";
import { assertTranscriptStoreInputs, assertTranscriptStorePlanContract, planProperties, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames } from "../transcript-store.ts";
import { PipelineLiveError, pipelineLiveFingerprint, readPipelineLive } from "../pipeline-live.ts";
import { SEARCH_SPLIT_REFUSED, SEARCH_SPLIT_ROLLBACK, SEARCH_SPLIT_ROLLOUT, STAGED_RENDER_PATH, renderSearchSplitStages } from "../../../scripts/search-split-staged-render.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const REGION = "us-east-1";
const ROLE = `arn:aws:iam::${ACCOUNT}:role/session-pipeline-role`;
const t = transcriptNames(STAGE, ACCOUNT);
const p = sessionPipelineNames(STAGE, ACCOUNT, REGION);
/** hoopdbPoolMax is required with any sessionPipeline: tests that are not about it carry the live 3. */
const withPool = (v: unknown) => (v !== null && typeof v === "object" && !Array.isArray(v) && !("hoopdbPoolMax" in v) ? { hoopdbPoolMax: 3, ...v } : v);
const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-sessions-prod.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;

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
      PIPELINE_FANOUT: "off",
      PIPELINE_RECONCILE_LOOKBACK_MINUTES: "60",
    });
    for (const value of Object.values(env)) expect(value).not.toMatch(/postgres(ql)?:\/\/|password|clickhouse\.cloud/i);
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { searchIndex: true } }).SEARCH_INDEX_ENABLED).toBe("1");
  });

  it("briefs start off: SESSION_BRIEF_ENABLED=false and the schedule DISABLED; enabled sets both, and the caps only when given", () => {
    const off = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: {} });
    expect(off.SESSION_BRIEF_ENABLED).toBe("false");
    expect(Object.keys(off).filter((k) => k.startsWith("SESSION_BRIEF_"))).toEqual(["SESSION_BRIEF_ENABLED"]);
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { briefs: { enabled: false, globalDailyCap: 5 } } }).SESSION_BRIEF_ENABLED).toBe("false");
    const on = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { briefs: { enabled: true, backgroundDailyBudget: 1000, globalDailyCap: 2000 } } });
    expect(on).toMatchObject({ SESSION_BRIEF_ENABLED: "true", SESSION_BRIEF_BACKGROUND_DAILY_BUDGET: "1000", SESSION_BRIEF_GLOBAL_DAILY_CAP: "2000" });
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
    ({ ...example, transcriptStore: { ...example.transcriptStore, sessionPipeline: withPool(sessionPipeline) } }) as unknown as AwsInputs & Record<string, unknown>;
  const validate = (i: AwsInputs & Record<string, unknown>) => assertTranscriptStoreInputs(STAGE, i);
  it("accepts the documented settings and refuses anything else", () => {
    expect(() => validate(withPipeline({ searchIndex: true, maxConcurrency: 4, reconcileMinutes: 30 }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ queue: "x" }))).toThrow(/sessionPipeline.queue is not a known setting/);
    expect(() => validate(withPipeline({ maxConcurrency: 50 }))).toThrow(/maxConcurrency/);
    expect(() => validate(withPipeline({ reconcileMinutes: 1 }))).toThrow(/reconcileMinutes/);
    expect(() => validate(withPipeline([]))).toThrow(/plain object/);
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

describe("session pipeline: streamed reconcile switch", () => {
  const refuse = (why: string): never => {
    throw new Error(why);
  };
  it("absent or disabled renders nothing new; enabled renders SEARCH_INDEX_STREAMING_CH=1 only", () => {
    const off = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { searchIndex: true } });
    expect(Object.keys(off)).not.toContain("SEARCH_INDEX_STREAMING_CH");
    expect(sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { searchIndex: true, streamReconcile: { enabled: false } } })).toEqual(off);
    const on = sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: { searchIndex: true, streamReconcile: { enabled: true } } });
    expect(on).toEqual({ ...off, SEARCH_INDEX_STREAMING_CH: "1" });
    expect(Object.keys(on).filter((k) => k.startsWith("SEARCH_INDEX_STREAM"))).toEqual(["SEARCH_INDEX_STREAMING_CH"]);
  });

  it("validates the shape and needs searchIndex: true when enabled", () => {
    const ok = (v: unknown) => assertSessionPipelineInputs(withPool(v), refuse);
    expect(() => ok({ searchIndex: true, streamReconcile: { enabled: true } })).not.toThrow();
    expect(() => ok({ streamReconcile: { enabled: false } })).not.toThrow();
    expect(() => ok({ streamReconcile: { enabled: true } })).toThrow(/requires searchIndex: true/);
    expect(() => ok({ searchIndex: false, streamReconcile: { enabled: true } })).toThrow(/requires searchIndex: true/);
    expect(() => ok({ searchIndex: true, streamReconcile: { enabled: "yes" } })).toThrow(/enabled must be a boolean/);
    expect(() => ok({ searchIndex: true, streamReconcile: { enabled: true, maxReadBytes: 1 } })).toThrow(/maxReadBytes is not a known setting/);
    expect(() => ok({ searchIndex: true, streamReconcile: true })).toThrow(/must be a plain object/);
  });

  it("the function's resources and props are the same with the switch on", () => {
    const i = { stage: STAGE, accountId: ACCOUNT, region: REGION, bucket: t.bucket, tags: () => ({}) };
    const off = sessionPipelineFunctionProps({ ...i, p: { searchIndex: true } });
    const on = sessionPipelineFunctionProps({ ...i, p: { searchIndex: true, streamReconcile: { enabled: true } } });
    expect({ ...on, env: undefined }).toEqual({ ...off, env: undefined });
    expect(on.env).toEqual({ ...off.env, SEARCH_INDEX_STREAMING_CH: "1" });
  });
});

describe("search-queue split: inputs, env and the pool budget", () => {
  const refuse = (why: string): never => {
    throw new Error(why);
  };
  const ok = (v: unknown) => assertSessionPipelineInputs(withPool(v), refuse);
  const env = (pi: SessionPipelineInputs) => sessionPipelineEnv({ stage: STAGE, bucket: t.bucket, names: p, p: pi });

  it("without hoopdbPoolMax or a handoff the live env is rendered unchanged (HOOPDB_POOL_MAX 4, no search URL)", () => {
    const live = env({ searchIndex: true, maxConcurrency: 20 });
    expect(live.HOOPDB_POOL_MAX).toBe("4");
    expect(Object.keys(live)).not.toContain("SESSION_PIPELINE_SEARCH_QUEUE_URL");
    for (const searchQueue of [{ enabled: false }, { enabled: true }, { enabled: true, mappingEnabled: true, maxConcurrency: 16 }])
      expect(env({ searchIndex: true, maxConcurrency: 20, searchQueue })).toEqual(live);
    // The one common input: the rendered HOOPDB_POOL_MAX and the budget check read the same value.
    expect(env({ searchIndex: true, maxConcurrency: 20, hoopdbPoolMax: 3 })).toEqual({ ...live, HOOPDB_POOL_MAX: "3" });
    expect(sessionPoolSettings({ hoopdbPoolMax: 3 }).pool).toBe(3);
    const on = env({ searchIndex: true, searchQueue: { enabled: true, mappingEnabled: true, handoff: true } });
    expect(on.SESSION_PIPELINE_SEARCH_QUEUE_URL).toBe(p.searchQueueUrl);
    expect(p.searchQueueUrl).toBe(`https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/96bulls-${STAGE}-session-search`);
    expect(p.searchQueueUrl).not.toBe(p.queueUrl);
  });

  it("hoopdbPoolMax is required with any sessionPipeline, so no render silently puts 4 over the live 3", () => {
    const raw = (v: unknown) => assertSessionPipelineInputs(v, refuse);
    for (const v of [{}, { searchIndex: true, maxConcurrency: 20 }, { searchIndex: true, searchQueue: { enabled: true } }])
      expect(() => raw(v)).toThrow(/hoopdbPoolMax is required: set the live HOOPDB_POOL_MAX \(3\), or the render resets the function to 4/);
    // A stage that declares no pipeline is untouched.
    expect(() => assertTranscriptStoreInputs(STAGE, example as unknown as Parameters<typeof assertTranscriptStoreInputs>[1])).not.toThrow(/hoopdbPoolMax/);
    // The production-shaped stage: inputs validated as the stage does, then the function's props rendered from them.
    const prod = { ...example, transcriptStore: { ...example.transcriptStore, sessionPipeline: { searchIndex: true, maxConcurrency: 20, hoopdbPoolMax: 3 } } } as unknown as AwsInputs & Record<string, unknown>;
    expect(() => assertTranscriptStoreInputs(STAGE, prod as never)).not.toThrow(/sessionPipeline/);
    const pipeline = (prod.transcriptStore as { sessionPipeline: SessionPipelineInputs }).sessionPipeline;
    const fn = sessionPipelineFunctionProps({ stage: STAGE, accountId: ACCOUNT, region: REGION, bucket: t.bucket, p: pipeline, tags: () => ({}) });
    expect(fn.env.HOOPDB_POOL_MAX).toBe("3");
    for (const s of [...SEARCH_SPLIT_ROLLOUT, ...SEARCH_SPLIT_ROLLBACK])
      expect([s.id, sessionPipelineFunctionProps({ stage: STAGE, accountId: ACCOUNT, region: REGION, bucket: t.bucket, p: s.p, tags: () => ({}) }).env.HOOPDB_POOL_MAX]).toEqual([s.id, "3"]);
    expect(() => raw({ maxConcurrency: 20 })).toThrow(/hoopdbPoolMax is required/);
  });

  it("validates the shape: handoff needs mappingEnabled and searchIndex; mappingEnabled needs enabled; concurrency 2-20; pool 1-4", () => {
    expect(() => ok({ searchIndex: true, maxConcurrency: 10, hoopdbPoolMax: 3, searchQueue: { enabled: true, mappingEnabled: true, handoff: true, maxConcurrency: 16 } })).not.toThrow();
    expect(() => ok({ searchQueue: { enabled: true } })).not.toThrow();
    expect(() => ok({ searchQueue: {} })).toThrow(/searchQueue.enabled must be a boolean/);
    expect(() => ok({ searchQueue: { enabled: "yes" } })).toThrow(/searchQueue.enabled must be a boolean/);
    expect(() => ok({ searchQueue: { enabled: true, handoff: 1 } })).toThrow(/searchQueue.handoff must be a boolean/);
    expect(() => ok({ searchIndex: true, searchQueue: { enabled: true, handoff: true } })).toThrow(/handoff requires mappingEnabled: true/);
    expect(() => ok({ searchQueue: { enabled: true, mappingEnabled: true, handoff: true } })).toThrow(/handoff requires searchIndex: true/);
    expect(() => ok({ searchQueue: { enabled: false, mappingEnabled: true } })).toThrow(/mappingEnabled requires enabled: true/);
    for (const maxConcurrency of [1, 21, 2.5, "4"]) expect(() => ok({ searchQueue: { enabled: true, maxConcurrency } })).toThrow(/searchQueue.maxConcurrency must be an integer from 2 to 20/);
    for (const maxConcurrency of [1, 21, 2.5, "10"]) expect(() => ok({ maxConcurrency })).toThrow(/sessionPipeline.maxConcurrency must be an integer from 2 to 20/);
    // The pool per invocation may be lowered, never raised past the live 4.
    for (const hoopdbPoolMax of [0, 5, 2.5, "3"]) expect(() => ok({ hoopdbPoolMax })).toThrow(/hoopdbPoolMax must be an integer from 1 to 4/);
    expect(() => ok({ searchQueue: { enabled: true, queueName: "x" } })).toThrow(/searchQueue.queueName is not a known setting/);
    expect(() => ok({ poolerBudget: 104 })).toThrow(/poolerBudget is not a known setting/);
    expect(() => ok({ searchQueue: true })).toThrow(/searchQueue must be a plain object/);
  });

  it("(pointer + polling search) x HOOPDB_POOL_MAX <= 80: exact production values 10 + 16 at 3 are 78; at 4 they are 104 and refused", () => {
    expect(POOLER_BUDGET).toBe(80);
    const split = (pool: number, pointer = 10) => ({ searchIndex: true, maxConcurrency: pointer, hoopdbPoolMax: pool, searchQueue: { enabled: true, mappingEnabled: true, handoff: true, maxConcurrency: 16 } });
    expect(sessionPoolDemand(split(3))).toBe(78);
    expect(() => ok(split(3))).not.toThrow();
    expect(sessionPoolDemand(split(4))).toBe(104);
    expect(() => ok(split(4))).toThrow(/\(maxConcurrency 10 \+ polling searchQueue.maxConcurrency 16\) x hoopdbPoolMax 4 is 104 pooler connections, over the budget of 80/);
    // Enabling search before lowering the pointer cap: 20 + 16 at 3 is 108.
    expect(() => ok(split(3, 20))).toThrow(/is 108 pooler connections/);
    // Live: the pointer mapping alone at 20 x 3 = 60; a created but disabled search mapping holds nothing.
    expect(sessionPoolDemand({ maxConcurrency: 20, hoopdbPoolMax: 3 })).toBe(60);
    expect(sessionPoolDemand({ maxConcurrency: 20, hoopdbPoolMax: 3, searchQueue: { enabled: true, maxConcurrency: 16 } })).toBe(60);
    expect(sessionPoolDemand({ maxConcurrency: 20, hoopdbPoolMax: 4 })).toBe(80);
    expect(() => ok({ maxConcurrency: 20, searchQueue: { enabled: true, maxConcurrency: 16 } })).not.toThrow();
    expect(sessionPoolDemand({})).toBe(16);
  });

  it("the queue, DLQ and alarms keep the events queue's conventions; the mapping is created disabled with its own concurrency", () => {
    const tags = { component: "session-pipeline" };
    const dlq = sessionSearchDlqProps({ names: p, tags });
    expect(dlq).toEqual({ queueName: p.searchDlq, messageRetentionPeriod: "14 days", sqsManagedSseEnabled: true, policy: sessionDlqPolicy({ dlqArn: p.searchDlqArn }), tags });
    const queue = sessionSearchQueueProps({ names: p, roleArn: ROLE, dlqArn: p.searchDlqArn, tags });
    expect(queue).toMatchObject({ queueName: p.searchQueue, visibilityTimeout: `${VISIBILITY_TIMEOUT_SECONDS} seconds`, messageRetentionPeriod: "4 days", receiveMessageWaitTime: "20 seconds", sqsManagedSseEnabled: true, redrivePolicy: { deadLetterTargetArn: p.searchDlqArn, maxReceiveCount: 8 } });
    expect("delay" in queue).toBe(false);
    const policy = sessionSearchQueuePolicy({ queueArn: p.searchQueueArn, roleArn: ROLE });
    expect(policy.Statement.map((s) => s.Sid)).toEqual(["DenyInsecureTransport", "SessionPipelineConsumes"]);
    expect(policy.Statement[1]!.Principal).toEqual({ AWS: ROLE });
    expect([...policy.Statement[1]!.Action].sort()).toEqual(["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ReceiveMessage", "sqs:SendMessage"]);
    const mapping = (q: SessionSearchQueueInputs) => sessionSearchMappingProps({ q, functionName: p.function, queueArn: p.searchQueueArn, tags });
    expect(mapping({ enabled: true })).toMatchObject({ eventSourceArn: p.searchQueueArn, functionResponseTypes: ["ReportBatchItemFailures"], scalingConfig: { MaximumConcurrency: 2 }, enabled: false });
    expect(mapping({ enabled: true, mappingEnabled: true, maxConcurrency: 16 })).toMatchObject({ scalingConfig: { MaximumConcurrency: 16 }, enabled: true });
    // The events mapping renders exactly as before the split.
    expect(sessionEventsMappingProps({ p: {}, functionName: p.function, queueArn: p.queueArn, tags })).toEqual({ functionName: p.function, eventSourceArn: p.queueArn, batchSize: 10, maximumBatchingWindow: "20 seconds", functionResponseTypes: ["ReportBatchItemFailures"], scalingConfig: { MaximumConcurrency: 4 }, enabled: true, tags });
    const alarms = sessionSearchAlarms(p);
    expect(alarms.map((a) => [a.id, a.props.MetricName, a.props.Dimensions[0].Value])).toEqual([
      ["session-search-dlq-alarm", "ApproximateNumberOfMessagesVisible", p.searchDlq],
      ["session-search-age-alarm", "ApproximateAgeOfOldestMessage", p.searchQueue],
    ]);
    expect(ALARM_DEFAULTS.ActionsEnabled).toBe(false);
  });

  it("every staged transition stays within the budget, and the refused shortcuts do not", () => {
    const stages = [...SEARCH_SPLIT_ROLLOUT, ...SEARCH_SPLIT_ROLLBACK];
    for (const s of stages) {
      expect(() => ok(s.p)).not.toThrow();
      expect([s.id, sessionPoolDemand(s.p) <= POOLER_BUDGET]).toEqual([s.id, true]);
    }
    const chain = [...SEARCH_SPLIT_ROLLOUT, ...SEARCH_SPLIT_ROLLBACK];
    const worst = chain.slice(1).map((s, n) => [`${chain[n]!.id}->${s.id}`, sessionPoolTransitionDemand(chain[n]!.p, s.p)]);
    expect(worst).toEqual([["live->A", 60], ["A->B", 60], ["B->C1", 78], ["C1->C2", 78], ["C2->R1", 78], ["R1->R2", 78], ["R2->R3", 60]]);
    // The pool is 3 in every stage: never raised back to 4.
    expect(chain.map((s) => s.p.hoopdbPoolMax)).toEqual(chain.map(() => 3));
    expect(sessionPoolDemand(SEARCH_SPLIT_ROLLOUT.at(-1)!.p)).toBe(78);
    expect(SEARCH_SPLIT_REFUSED.map((r) => sessionPoolTransitionDemand(r.from, r.to))).toEqual([108, 108, 104]);
    expect(() => ok(SEARCH_SPLIT_REFUSED[0]!.to)).toThrow(/108 pooler connections/);
    expect(() => ok(SEARCH_SPLIT_REFUSED[2]!.to)).toThrow(/104 pooler connections/);
  });

  it("the checked-in staged render is the script's output, and it carries placeholders only", () => {
    const file = readFileSync(fileURLToPath(new URL(`../../../${STAGED_RENDER_PATH}`, import.meta.url)), "utf8");
    expect(file).toBe(renderSearchSplitStages());
    expect(file).not.toMatch(/[0-9]{12}|arn:aws:iam::|us-east-1|aws-sessions-prod/);
  });
});

describe("search-queue split: the deploy plan contract", () => {
  const REAL = readFileSync(fileURLToPath(new URL("./fixtures/aws-sessions-prod.pipeline-update.detailed.txt", import.meta.url)), "utf8");
  /** The function's role as the captured cascade names it (read from the fixture, not restated here). */
  const LIVE_ROLE = /^  - \s*AWS: (arn:aws:iam::\S+)$/m.exec(REAL)![1]!;
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const isResourceLine = (l: string) => /^\[[a-z0-9-]+\] \S+$/.test(l);
  const snapshot = JSON.stringify({ schema: 1, objects: [{ key: `96bulls/96bulls-aws/${STAGE}/session-pipeline.json`, text: JSON.stringify({ status: "created", attr: { roleArn: LIVE_ROLE } }) }] });
  /** Block YAML as Alchemy prints declared props (sorted keys, quoted where YAML needs it). */
  const yaml = (v: unknown, indent: number): string[] => {
    const sp = " ".repeat(indent);
    const scalar = (x: unknown) => (typeof x === "string" ? (/^[0-9]+$|^\*$|^(true|false)$|: |^[{'"]/.test(x) ? JSON.stringify(x) : x) : String(x));
    if (Array.isArray(v))
      return v.flatMap((item) => {
        if (item !== null && typeof item === "object") {
          const inner = yaml(item, indent + 2);
          return [`${sp}- ${inner[0]!.trimStart()}`, ...inner.slice(1)];
        }
        return [`${sp}- ${scalar(item)}`];
      });
    return Object.keys(v as object)
      .sort()
      .flatMap((k) => {
        const x = (v as Record<string, unknown>)[k];
        if (x === undefined) return [];
        return x !== null && typeof x === "object" ? [`${sp}${k}:`, ...yaml(x, indent + 2)] : [`${sp}${k}: ${scalar(x)}`];
      });
  };
  const tags = { "96bulls:component": "session-pipeline", "96bulls:repo": "AgentWorkforce/sessions-pipeline", "96bulls:stack": STAGE };
  const KAA = "(known after apply)";
  /** The rendered create props of each search resource, as the stack declares them in stage A. */
  const createProps = (over: Record<string, Record<string, unknown>> = {}): Record<string, Record<string, unknown>> => ({
    "session-search-dlq": sessionSearchDlqProps({ names: p, tags }),
    "session-search": sessionSearchQueueProps({ names: p, roleArn: LIVE_ROLE, dlqArn: KAA, tags }),
    "session-search-mapping": sessionSearchMappingProps({ q: { enabled: true, maxConcurrency: 16 }, functionName: p.function, queueArn: KAA, tags }),
    ...Object.fromEntries(sessionSearchAlarms(p).map(({ id, props }) => [id, { ...ALARM_DEFAULTS, ...props, tags }])),
    ...over,
  });
  /** A detailed plan: REAL's resources (all noop unless `fromReal`), the search resources, and the given blocks. */
  const detailed = (blocks: Record<string, { action: string; body: string[] }>, fromReal = false) => {
    const base = fromReal ? REAL : REAL.replace(/^\[([a-z0-9-]+)\] update\n[\s\S]*?(?=\n\[)/gm, "[$1] noop\n");
    let text = base.trimEnd();
    for (const r of SESSION_SEARCH_QUEUE_RESOURCES) if (!blocks[r]) text += `\n\n[${r}] noop`;
    for (const [r, { action, body }] of Object.entries(blocks)) {
      const block = `[${r}] ${action}\n${body.join("\n")}`;
      text = text.includes(`[${r}] `) ? text.replace(new RegExp(`^\\[${r}\\] \\S+\\n(?:(?!\\[)[^\\n]*\\n?)*`, "m"), `${block}\n\n`) : `${text}\n\n${block}`;
    }
    const counts = new Map<string, number>();
    for (const l of text.split("\n")) if (isResourceLine(l) && !l.endsWith(" noop")) counts.set(l.split(" ")[1]!, (counts.get(l.split(" ")[1]!) ?? 0) + 1);
    const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: no changes";
    return `${text.replace(/^Plan: .*$/m, summary)}\n`;
  };
  /** Stage A's approved live baseline: events Enabled at 20, pool 3, no search mapping, no URL. */
  const liveA = (o: Parameters<typeof live>[0] = {}) => live({ pointer: 20, search: null, ...o });
  /** A stage-A capture (`state` null: the deploy's check, no state snapshot). */
  const check = (full: string, state: string | null = snapshot, liveText: string = liveA()) => capture(full, liveText, state);
  const creates = (props = createProps(), only: readonly string[] = SESSION_SEARCH_QUEUE_RESOURCES) =>
    Object.fromEntries(only.map((r) => [r, { action: "create", body: ["  properties:", ...yaml(props[r], 4)] }]));
  /** A `-`/`+` diff of one key line inside a resource's properties. */
  const change = (path: string[], key: string, from: string, to: string) => ["  properties:", ...path.map((k, n) => `${" ".repeat(4 + 2 * n)}${k}:`), `  -   ${" ".repeat(2 * path.length)}${key}: ${from}`, `  +   ${" ".repeat(2 * path.length)}${key}: ${to}`];
  /** The search queue's role-ARN cascade, as a pipeline update re-plans it (cf. session-events in REAL). */
  const searchCascade = () => {
    const live = yaml(sessionSearchQueuePolicy({ queueArn: p.searchQueueArn, roleArn: LIVE_ROLE }).Statement, 6);
    const planned = live.map((l) => l.replace(`AWS: ${LIVE_ROLE}`, `AWS: ${KAA}`));
    return { action: "update", body: ["  properties:", "    policy:", "  -     Statement:", ...live.map((l) => `  - ${l.slice(4)}`), "  +     Statement:", ...planned.map((l) => `  + ${l.slice(4)}`), "      Version: 2012-10-17"] };
  };
  const envChange = (lines: string[]) => ({ action: "update", body: ["  properties:", "    env:", ...lines] });

  it("stage A, provisioning only: the five creates pinned to their rendered props, with and without the state snapshot", () => {
    expect(check(detailed(creates()))).toBe("session search queue rollout");
    expect(check(detailed(creates()), null)).toBe("session search queue rollout");
    // The rest after a partial apply.
    expect(check(detailed(creates(createProps(), ["session-search-mapping", "session-search-age-alarm"])))).toBe("session search queue rollout");
    // All noop afterwards is post-apply.
    expect(check(detailed({}))).toBe("post-apply");
  });

  it("stage A control: only from the approved live baseline (events Enabled 20, pool 3, no search mapping, no URL), never without it", () => {
    const a = detailed(creates());
    expect(() => capture(a, undefined, null)).toThrow(/needs the live function and mapping readback \(the approved baseline\)/);
    expect(() => capture(a, undefined)).toThrow(/the approved baseline/);
    for (const [why, l] of [
      ["pointer 26", liveA({ pointer: 26 })],
      ["pointer 10", liveA({ pointer: 10 })],
      ["pool 2", liveA({ env: { ...(fnProps({ ...SPLIT, searchQueue: Q }).env as object), HOOPDB_POOL_MAX: "2" } })],
    ] as const)
      expect([why, refusal(() => check(a, snapshot, l))]).toEqual([why, expect.stringMatching(/the live baseline is not the approved one/)]);
    expect(() => check(a, snapshot, liveA({ search: "Enabled" }))).toThrow(/a live search mapping exists that is not this stage's/);
    expect(() => check(a, snapshot, liveA({ env: fnProps(C2_ON).env as Record<string, unknown> }))).toThrow(/already has SESSION_PIPELINE_SEARCH_QUEUE_URL/);
    // The function's role in state must be its live role.
    expect(() => check(a, snapshot, liveA({ role: ROLE }))).toThrow(/role in state is not its live role/);
    // After a partial apply: the stage's own search mapping, Disabled at 16, is accepted.
    expect(check(detailed(creates(createProps(), ["session-search-age-alarm"])), snapshot, liveA({ search: "Disabled" }))).toBe("session search queue rollout");
  });

  it("the props parser reads a real Alchemy create block (the brief schedule in the checked-in production plan)", () => {
    const real = readFileSync(fileURLToPath(new URL("../../../docs/plans/aws-sessions-prod.plan.detailed.txt", import.meta.url)), "utf8").split("\n");
    expect(planProperties(real, "session-brief-schedule", "create")).toEqual({
      description: "96bulls session pipeline: one session-brief tick (due wakeups)",
      name: `96bulls-${STAGE}-session-brief`,
      scheduleExpression: "rate(1 minute)",
      state: "DISABLED",
      tags: { "96bulls:component": "session-pipeline", "96bulls:repo": "AgentWorkforce/sessions-pipeline", "96bulls:stack": STAGE },
      targets: [{ Arn: `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${p.function}`, Id: "session-pipeline", Input: '{"brief":true}' }],
    });
    expect(planProperties(real, "session-brief-permission", "create")).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: "events.amazonaws.com", sourceAccount: ACCOUNT, sourceArn: "(known after apply)" });
  });

  it("stage A refuses any change beside the creates: a function, mapping, grant or schedule change", () => {
    for (const [r, block] of [
      ["session-pipeline", envChange(["  +     SESSION_PIPELINE_SEARCH_QUEUE_URL: x"])],
      ["session-events-mapping", { action: "update", body: change(["scalingConfig"], "MaximumConcurrency", "20", "10") }],
      ["session-events", { action: "update", body: ["  no declared property changes"] }],
      ["session-reconcile-schedule", { action: "update", body: ["  no declared property changes"] }],
    ] as const)
      expect(() => check(detailed({ ...creates(), [r]: block }))).toThrow(/session search queue rollout: provisioning only, every other resource must be noop/);
    expect(() => check(detailed({ ...creates(), "session-search": searchCascade() }))).toThrow(/may only be created or noop/);
  });

  it("stage A pins the queue policy, the mapping's target and settings, and the alarms", () => {
    const base = createProps();
    const queue = base["session-search"]!;
    const mapping = base["session-search-mapping"]!;
    const policy = (statements: unknown[]) => ({ ...queue, policy: { Version: "2012-10-17", Statement: statements } });
    const stmts = sessionSearchQueuePolicy({ queueArn: p.searchQueueArn, roleArn: LIVE_ROLE }).Statement as unknown as Record<string, unknown>[];
    const bad: [string, Record<string, Record<string, unknown>>, RegExp][] = [
      ["mapping created enabled", { "session-search-mapping": { ...mapping, enabled: true } }, /session-search-mapping is not exactly its rendered props \(enabled\)/],
      ["mapping on the events queue", { "session-search-mapping": { ...mapping, eventSourceArn: p.queueArn } }, /\(eventSourceArn\)/],
      ["mapping for another function", { "session-search-mapping": { ...mapping, functionName: "someone-else" } }, /\(functionName\)/],
      ["mapping concurrency 50", { "session-search-mapping": { ...mapping, scalingConfig: { MaximumConcurrency: 50 } } }, /\(scalingConfig.MaximumConcurrency\)/],
      ["mapping concurrency 8 (not the approved 16)", { "session-search-mapping": { ...mapping, scalingConfig: { MaximumConcurrency: 8 } } }, /\(scalingConfig.MaximumConcurrency\)/],
      ["foreign ownership tags", { "session-search": { ...queue, tags: { ...tags, owner: "someone-else" } } }, /tags.owner is not rendered/],
      ["another stage's tags", { "session-search-dlq": { ...base["session-search-dlq"], tags: { ...tags, "96bulls:stack": "aws-sessions-dev" } } }, /tags.96bulls:stack/],
      ["no tags", { "session-search-mapping": { ...mapping, tags: undefined } }, /tags is not a map/],
      ["mapping batch size", { "session-search-mapping": { ...mapping, batchSize: 100 } }, /\(batchSize\)/],
      ["mapping with a filter", { "session-search-mapping": { ...mapping, filterCriteria: { Filters: [] } } }, /filterCriteria is not rendered/],
      ["queue granting another role", { "session-search": policy([stmts[0], { ...stmts[1], Principal: { AWS: ROLE } }]) }, /policy.Statement\[1\].Principal.AWS/],
      ["queue granting everyone", { "session-search": policy([stmts[0], { ...stmts[1], Principal: { AWS: "*" } }]) }, /Principal.AWS/],
      ["queue with an S3 send statement", { "session-search": policy([...stmts, { Sid: "S3", Effect: "Allow", Principal: { Service: "s3.amazonaws.com" }, Action: "sqs:SendMessage", Resource: p.searchQueueArn }]) }, /policy.Statement length/],
      ["queue granting purge", { "session-search": policy([stmts[0], { ...stmts[1], Action: [...(stmts[1]!.Action as string[]), "sqs:PurgeQueue"] }]) }, /Action length/],
      ["queue without the TLS deny", { "session-search": policy([stmts[1]]) }, /policy.Statement length/],
      ["queue on another DLQ", { "session-search": { ...queue, redrivePolicy: { deadLetterTargetArn: p.dlqArn, maxReceiveCount: 8 } } }, /redrivePolicy.deadLetterTargetArn/],
      ["queue with a delay", { "session-search": { ...queue, delay: "30 seconds" } }, /delay is not rendered/],
      ["alarm with actions", { "session-search-dlq-alarm": { ...base["session-search-dlq-alarm"], ActionsEnabled: true } }, /\(ActionsEnabled\)/],
      ["alarm on another queue", { "session-search-age-alarm": { ...base["session-search-age-alarm"], Dimensions: [{ Name: "QueueName", Value: p.queue }] } }, /Dimensions\[0\].Value/],
      ["DLQ without its policy", { "session-search-dlq": { ...base["session-search-dlq"], policy: undefined } }, /policy is not a map/],
    ];
    for (const [why, over, error] of bad) {
      let message = "";
      try {
        check(detailed(creates(createProps(over))));
      } catch (e) {
        message = (e as Error).message;
      }
      expect([why, message]).toEqual([why, expect.stringMatching(error)]);
    }
    // At deploy (no snapshot) the principal is still exactly the function's live role: an unrelated
    // role in the same account, or another account's, is refused.
    for (const other of [ROLE, "arn:aws:iam::444455556666:role/x"])
      expect(() => check(detailed(creates(createProps({ "session-search": policy([stmts[0], { ...stmts[1], Principal: { AWS: other } }]) }))), null)).toThrow(/Principal.AWS/);
    expect(check(detailed(creates()), null)).toBe("session search queue rollout");
    // A create block with no properties is refused.
    expect(() => check(detailed({ ...creates(), "session-search": { action: "create", body: [] } }))).toThrow(/no properties for session-search/);
  });

  /** An Alchemy update block from whole old and new props: unchanged keys plain, a changed key's whole old value `  - `, then its new value `  + `. */
  const update = (old: Record<string, unknown>, now: Record<string, unknown>) => {
    const body = ["  properties:"];
    const mark = (m: string, ls: string[]) => ls.map((l) => `  ${m} ${l.slice(2)}`);
    const walk = (a: Record<string, unknown>, b: Record<string, unknown>, indent: number) => {
      for (const k of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) {
        const x = a[k], y = b[k];
        if (JSON.stringify(x) === JSON.stringify(y)) {
          if (x !== undefined) body.push(...yaml({ [k]: x }, indent));
        } else if (x && y && typeof x === "object" && typeof y === "object" && !Array.isArray(x) && !Array.isArray(y)) {
          body.push(`${" ".repeat(indent)}${k}:`);
          walk(x as Record<string, unknown>, y as Record<string, unknown>, indent + 2);
        } else {
          if (x !== undefined) body.push(...mark("-", yaml({ [k]: x }, indent)));
          if (y !== undefined) body.push(...mark("+", yaml({ [k]: y }, indent)));
        }
      }
    };
    walk(old, now, 4);
    return { action: "update", body };
  };
  const eventsMapping = (max: number, over: Record<string, unknown> = {}) => ({ ...sessionEventsMappingProps({ p: { maxConcurrency: max }, functionName: p.function, queueArn: p.queueArn, tags }), ...over });
  const searchMapping = (enabled: boolean, max = 16, over: Record<string, unknown> = {}) => ({ ...sessionSearchMappingProps({ q: { enabled: true, mappingEnabled: enabled, maxConcurrency: max }, functionName: p.function, queueArn: p.searchQueueArn, tags }), ...over });
  const fnProps = (pi: SessionPipelineInputs, env: Record<string, string> = {}, drop: string[] = []) => {
    const props = sessionPipelineFunctionProps({ stage: STAGE, accountId: ACCOUNT, region: REGION, bucket: t.bucket, p: pi, tags: () => tags });
    const e: Record<string, string> = { ...props.env, ...env };
    for (const k of drop) delete e[k];
    return { ...props, env: e } as Record<string, unknown>;
  };
  const SPLIT = { searchIndex: true, maxConcurrency: 10, hoopdbPoolMax: 3 } as const;
  const Q = { enabled: true, maxConcurrency: 16 } as const;
  const FN_ARN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${p.function}`;
  const U_EVENTS = "11111111-1111-4111-8111-111111111111";
  const U_SEARCH = "22222222-2222-4222-8222-222222222222";
  const U_EXTRA = "33333333-3333-4333-8333-333333333333";
  const mapping = (eventSourceArn: string, state: string, max: number | undefined, uuid: string, over: Record<string, unknown> = {}) => ({
    uuid,
    eventSourceArn,
    functionArn: FN_ARN,
    state,
    batchSize: 10,
    maximumBatchingWindowInSeconds: 20,
    functionResponseTypes: ["ReportBatchItemFailures"],
    ...(max === undefined ? {} : { maximumConcurrency: max }),
    ...over,
  });
  /** The live readback (pipeline-live.ts PipelineLive, schema 3): the function and every mapping on it. */
  function live(o: { env?: Record<string, unknown>; pointer?: number; search?: "Enabled" | "Disabled" | null; searchMax?: number; extra?: Record<string, unknown>[]; revision?: string; role?: string; pointerOver?: Record<string, unknown>; fn?: Record<string, unknown> } = {}) {
    return JSON.stringify({
      schema: 3,
      function: { name: p.function, functionArn: FN_ARN, role: o.role ?? LIVE_ROLE, revisionId: o.revision ?? "0a1b2c3d-0000-4000-8000-000000000001", state: "Active", lastUpdateStatus: "Successful", env: o.env ?? (fnProps({ ...SPLIT, searchQueue: Q }).env as Record<string, string>), ...o.fn },
      aliases: [],
      versions: ["$LATEST"],
      mappings: [
        mapping(p.queueArn, "Enabled", o.pointer ?? 10, U_EVENTS, o.pointerOver),
        ...(o.search === null ? [] : [mapping(p.searchQueueArn, o.search ?? "Disabled", o.searchMax ?? 16, U_SEARCH)]),
        ...(o.extra ?? []),
      ],
    });
  }
  /** `state` null: the deploy's check (no state snapshot). `recorded`: the live text the capture saw (default: the same). */
  function capture(full: string, liveText?: string, state: string | null = snapshot, recorded: string | undefined = liveText) {
    const compact = `${full.split("\n").filter((l) => l.startsWith("Plan:") || isResourceLine(l)).join("\n")}\n`;
    return assertTranscriptStorePlanContract(compact, {
      detailedPlanText: full,
      provenanceText: JSON.stringify({
        schema: 1,
        stage: STAGE,
        outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) } },
        summary: compact.split("\n")[0],
        ...(recorded ? { pipelineLive: pipelineLiveFingerprint(JSON.parse(recorded)) } : {}),
      }),
      stateSnapshotText: state ?? undefined,
      liveSnapshotText: liveText,
    });
  }
  const fnUpdate = (oldP: Record<string, unknown>, newP: Record<string, unknown>) => detailed({ "session-pipeline": update(oldP, newP), "session-search": searchCascade() }, true);
  const URL_KEY = "SESSION_PIPELINE_SEARCH_QUEUE_URL";
  const C1_BASE = { ...SPLIT, searchQueue: { ...Q, mappingEnabled: true } };
  const C2_ON = { ...SPLIT, searchQueue: { ...Q, mappingEnabled: true, handoff: true } };
  const refusal = (run: () => unknown) => {
    try {
      run();
      return "accepted";
    } catch (e) {
      return (e as Error).message;
    }
  };

  it("B and R3: exactly 20 -> 10 and 10 -> 20, from the approved live state", () => {
    expect(capture(detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)) }), live({ pointer: 20 }))).toBe("session pipeline update");
    expect(capture(detailed({ "session-events-mapping": update(eventsMapping(10), eventsMapping(20)) }), live({}))).toBe("session pipeline update");
    // Raising the pointer cap while search polls: (20 + 16) x 3 = 108, over the budget.
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(10), eventsMapping(20)) }), live({ search: "Enabled" }))).toThrow(/108 pooler connections/);
    // Other caps, even within the budget, are not approved stages.
    for (const [a, b] of [[20, 8], [20, 12], [10, 12], [12, 10]] as const)
      expect([a, b, refusal(() => capture(detailed({ "session-events-mapping": update(eventsMapping(a), eventsMapping(b)) }), live({ pointer: a })))]).toEqual([a, b, expect.stringMatching(/may move only 20 -> 10 \(B\) or 10 -> 20 \(R3\)/)]);
  });

  it("control 1: a stale B (plan 20 -> 10 while the live cap is 26) is refused", () => {
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)) }), live({ pointer: 26 }))).toThrow(/the plan's old events mapping MaximumConcurrency 20 is not the live 26 \(a stale plan\)/);
  });

  it("control 2: B from 26 is not the approved stage (B needs the settled Enabled/20 precondition); a planned 26 is never restored", () => {
    // The live 26 is read (within the account limit), but B is exactly 20 -> 10.
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(26), eventsMapping(10)) }), live({ pointer: 26 }))).toThrow(/may move only 20 -> 10 \(B\)/);
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(10), eventsMapping(26)) }), live({}))).toThrow(/MaximumConcurrency must be 2-20/);
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(1001), eventsMapping(10)) }), live({ pointer: 1001 }))).toThrow(/no MaximumConcurrency within 2-1000/);
    // B with the search mapping already polling, or the pool not 3, is not B either.
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)) }), live({ pointer: 20, env: { ...(fnProps({ ...SPLIT, searchQueue: Q }).env as object), HOOPDB_POOL_MAX: "2" } }))).toThrow(/B needs the live state events Enabled 20, search Disabled 16, HOOPDB_POOL_MAX 3/);
  });

  it("peak control: enabling search at 16 with the live pointer still 26 (pool 3) is 126 and refused", () => {
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ pointer: 26 }))).toThrow(/\(pointer 26 \+ search 16\) x pool 3 is 126 pooler connections/);
  });

  it("negative control (a): the pointer mapping retargeted to another function, or another queue, is refused", () => {
    const retarget = (over: Record<string, unknown>, max = 20) => detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(max, over)) });
    for (const [over, max] of [[{ functionName: "someone-else" }, 20], [{ functionName: "someone-else" }, 10], [{ eventSourceArn: p.searchQueueArn }, 10], [{ batchSize: 100 }, 10], [{ enabled: false }, 20]] as const)
      expect(() => capture(retarget(over, max), live({ pointer: 20 }))).toThrow(/session-events-mapping may change only its MaximumConcurrency/);
    const both = detailed({ "session-events-mapping": update(eventsMapping(20, { functionName: "someone-else" }), eventsMapping(10, { functionName: "someone-else" })) });
    expect(() => capture(both, live({ pointer: 20 }))).toThrow(/not its rendered props on the events queue \(functionName\)/);
    const plain = REAL.replace(/^\[([a-z0-9-]+)\] update\n[\s\S]*?(?=\n\[)/gm, "[$1] noop\n").replace("[session-events-mapping] noop", `[session-events-mapping] update\n${update(eventsMapping(4), eventsMapping(4, { functionName: "someone-else" })).body.join("\n")}`);
    expect(() => capture(plain.replace(/^Plan: .*$/m, "Plan: 1 to update"), undefined, null)).toThrow(/session-events-mapping may change only its MaximumConcurrency/);
  });

  it("C1 and R2: the search mapping's enabled flipped at exactly 16, from the approved live state", () => {
    expect(capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({}))).toBe("session pipeline update");
    expect(capture(detailed({ "session-search-mapping": update(searchMapping(true), searchMapping(false)) }), live({ search: "Enabled" }))).toBe("session pipeline update");
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ search: "Enabled" }))).toThrow(/is not the live one \(a stale plan\)/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ pointer: 20 }))).toThrow(/108 pooler connections/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ search: null }))).toThrow(/the live function has no search mapping/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true, 16, { eventSourceArn: p.queueArn })) }), live({}))).toThrow(/may change only enabled/);
  });

  it("C1 control: pointer 12 / search 8 (within budget) is not the approved C1", () => {
    // The live pointer is 12 (not the settled 10): refused even though (12 + 16) x 3 = 84 would also fail; at 8 it is the cap.
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false, 8), searchMapping(true, 8)) }), live({ pointer: 12, searchMax: 8 }))).toThrow(/MaximumConcurrency must be exactly 16/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ pointer: 8 }))).toThrow(/C1 needs the live state events Enabled 10, search Disabled 16/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ searchMax: 8 }))).toThrow(/C1 needs the live state/);
  });

  it("negative control (b): the search cap raised 16 -> 20 (90 at pointer 10 / pool 3), or lowered, is refused", () => {
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(true, 16), searchMapping(true, 20)) }), live({ search: "Enabled" }))).toThrow(/may change only enabled \(at 16\)/);
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(true, 16), searchMapping(true, 8)) }), live({ search: "Enabled" }))).toThrow(/may change only enabled \(at 16\)/);
  });

  it("control 4: R2 (disable search) is refused while the live env still has the handoff URL", () => {
    expect(() => capture(detailed({ "session-search-mapping": update(searchMapping(true), searchMapping(false)) }), live({ search: "Enabled", env: fnProps(C2_ON).env as Record<string, unknown> }))).toThrow(/disabling the search mapping needs R1 first/);
  });

  it("control 5: an extra mapping on the function (an unknown, enabled, cap 20) is refused, not left out of the budget", () => {
    const c1 = detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) });
    expect(() => capture(c1, live({ extra: [mapping(`arn:aws:sqs:${REGION}:${ACCOUNT}:someone-elses-queue`, "Enabled", 20, U_EXTRA)] }))).toThrow(new RegExp(`live mapping ${U_EXTRA} is not one this stack declares`));
    expect(() => capture(c1, live({ extra: [mapping(p.queueArn, "Enabled", 20, U_EXTRA)] }))).toThrow(/is not one this stack declares \(or a duplicate\)/);
    // A mapping on an alias or a version of the function is not on this function.
    expect(() => capture(c1, live({ extra: [mapping(`arn:aws:sqs:${REGION}:${ACCOUNT}:x`, "Enabled", 4, U_EXTRA, { functionArn: `${FN_ARN}:live` })] }))).toThrow(/does not target this function/);
  });

  it("control 6: no in-place change is accepted without the live readback, at capture or at deploy", () => {
    for (const b of [
      detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) }),
      detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)) }),
      fnUpdate(fnProps(C1_BASE), fnProps(C2_ON)),
    ]) {
      expect(() => capture(b, undefined, null)).toThrow(/needs the live function and mapping readback/);
      expect(() => capture(b, undefined)).toThrow(/needs the live function and mapping readback/);
    }
  });

  it("the verified source state: bound to the capture, this exact function Active/Successful, stable mappings, allow-listed fields", () => {
    const c1 = detailed({ "session-search-mapping": update(searchMapping(false), searchMapping(true)) });
    expect(() => capture(c1, live({ revision: "0a1b2c3d-0000-4000-8000-000000000002" }), null, live({}))).toThrow(/changed since the capture/);
    expect(() => capture(c1, live({ env: { ...(fnProps({ ...SPLIT, searchQueue: Q }).env as object), EXTRA: "1" } }), null, live({}))).toThrow(/changed since the capture/);
    expect(() => capture(c1, live({}), null, live({ pointerOver: { uuid: U_EXTRA } }))).toThrow(/changed since the capture/);
    expect(() => capture(c1, live({}), null, "")).toThrow(/did not record the live function/);
    expect(capture(c1, live({}), null)).toBe("session pipeline update");
    for (const state of ["Creating", "Updating", "Enabling", "Disabling", "Deleting"])
      expect(() => capture(c1, live({ pointerOver: { state } }))).toThrow(new RegExp(`live mapping ${U_EVENTS} is ${state}, not stable`));
    expect(() => capture(c1, live({ pointerOver: { functionArn: `${FN_ARN}-other` } }))).toThrow(/does not target this function/);
    expect(() => capture(c1, live({ pointerOver: { batchSize: 100 } }))).toThrow(/does not carry the rendered settings/);
    expect(() => capture(c1, live({ pointerOver: { maximumConcurrency: undefined } }))).toThrow(/no MaximumConcurrency/);
    expect(() => capture(c1, live({ pointerOver: { state: "Disabled" } }))).toThrow(/the live events mapping is not enabled/);
    for (const fn of [{ functionArn: `${FN_ARN}:1` }, { state: "Pending" }, { lastUpdateStatus: "Failed" }, { name: "someone-else" }])
      expect([fn, refusal(() => capture(c1, live({ fn })))]).toEqual([fn, expect.stringMatching(/not this stage's function, Active and Successful/)]);
    // A malformed state or UUID is never echoed: a fixed refusal.
    const canary = "Enabled-canary-state-55";
    const msg = refusal(() => capture(c1, live({ pointerOver: { state: canary } })));
    expect(msg).toMatch(/outside its allow-list/);
    expect(msg).not.toContain("canary");
    const msg2 = refusal(() => capture(c1, live({ pointerOver: { uuid: "canary-uuid-55" } })));
    expect(msg2).toMatch(/outside its allow-list/);
    expect(msg2).not.toContain("canary");
  });

  it("C2 and R1: the function env changes by exactly the search queue URL, against the plan and against the live env", () => {
    const off = fnProps(C1_BASE);
    const on = fnProps(C2_ON);
    expect((on.env as Record<string, string>)[URL_KEY]).toBe(p.searchQueueUrl);
    expect(capture(fnUpdate(off, on), live({ env: off.env as Record<string, unknown>, search: "Enabled" }))).toBe("session pipeline update");
    expect(capture(fnUpdate(on, off), live({ env: on.env as Record<string, unknown>, search: "Enabled" }))).toBe("session pipeline update");
    expect(() => capture(fnUpdate(off, { ...on, env: { ...(on.env as object), [URL_KEY]: p.queueUrl } }), live({ env: off.env as Record<string, unknown>, search: "Enabled" }))).toThrow(/may change only by adding or removing SESSION_PIPELINE_SEARCH_QUEUE_URL/);
    expect(() => capture(fnUpdate(off, { ...off, env: { ...(off.env as object), HOOPDB_POOL_MAX: "4" } }), live({ env: off.env as Record<string, unknown>, search: "Enabled" }))).toThrow(/HOOPDB_POOL_MAX changed/);
    expect(() => capture(fnUpdate(off, { ...on, memorySize: 10240 }), live({ env: off.env as Record<string, unknown>, search: "Enabled" }))).toThrow(/changes more than its env: memorySize/);
    // A code-only function update while the search resources exist is not a split stage.
    expect(() => capture(detailed({ "session-pipeline": { action: "update", body: ["  no declared property changes"] }, "session-search": searchCascade() }, true), live({ search: "Enabled" }))).toThrow(/must be C2 or R1/);
  });

  it("C2 control: pool 2 is not the approved C2 (pool exactly 3)", () => {
    const pool2 = (pi: SessionPipelineInputs) => fnProps({ ...pi, hoopdbPoolMax: 2 });
    const off = pool2(C1_BASE), on = pool2(C2_ON);
    expect(() => capture(fnUpdate(off, on), live({ env: off.env as Record<string, unknown>, search: "Enabled" }))).toThrow(/HOOPDB_POOL_MAX must be exactly 3/);
  });

  it("control 3: C2 is refused unless the search mapping exists, is enabled, and points at this queue and function", () => {
    const off = fnProps(C1_BASE), on = fnProps(C2_ON);
    const c2 = fnUpdate(off, on);
    expect(() => capture(c2, live({ env: off.env as Record<string, unknown>, search: null }))).toThrow(/needs the search mapping on this function and queue, enabled \(C1\) first/);
    expect(() => capture(c2, live({ env: off.env as Record<string, unknown>, search: "Disabled" }))).toThrow(/enabled \(C1\) first/);
  });

  it("control (retry var): a key in both plan sides but absent live would be re-introduced by C2, and is refused", () => {
    const retry = { SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" };
    const off = fnProps(C1_BASE, retry), on = fnProps(C2_ON, retry);
    const liveEnv = fnProps(C1_BASE).env as Record<string, unknown>;
    expect(() => capture(fnUpdate(off, on), live({ env: liveEnv, search: "Enabled" }))).toThrow(
      /after this apply the function env would differ from the live env by SESSION_PIPELINE_RETRY_VISIBILITY_S added, SESSION_PIPELINE_SEARCH_QUEUE_URL added; only SESSION_PIPELINE_SEARCH_QUEUE_URL may/,
    );
    expect(() => capture(fnUpdate(fnProps(C2_ON), fnProps(C1_BASE)), live({ env: { ...(fnProps(C2_ON).env as object), ...retry }, search: "Enabled" }))).toThrow(/SESSION_PIPELINE_RETRY_VISIBILITY_S removed/);
  });

  it("control (code-only update): the intended env is the env in state, compared in full with the live env", () => {
    // REAL: a function update with no declared property change (and its grants' cascade), no search resources.
    const codeOnly = REAL;
    const stateWith = (env: Record<string, string>) =>
      JSON.stringify({ schema: 1, objects: [{ key: `96bulls/96bulls-aws/${STAGE}/session-pipeline.json`, text: JSON.stringify({ status: "created", attr: { roleArn: LIVE_ROLE }, props: { env } }) }] });
    const liveEnv = fnProps(SPLIT).env as Record<string, string>;
    // The state holds SESSION_PIPELINE_RETRY_VISIBILITY_S=60, the live env lacks it: the apply would re-add it.
    expect(() => capture(codeOnly, live({ env: liveEnv }), stateWith({ ...liveEnv, SESSION_PIPELINE_RETRY_VISIBILITY_S: "60" }))).toThrow(/the update would add SESSION_PIPELINE_RETRY_VISIBILITY_S/);
    // The reverse: a live key the state lacks would be dropped.
    expect(() => capture(codeOnly, live({ env: { ...liveEnv, SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true" } }), stateWith(liveEnv))).toThrow(/does not render \(the update would drop it\)/);
    // Equal: accepted.
    expect(capture(codeOnly, live({ env: liveEnv }), stateWith(liveEnv))).toBe("session pipeline update");
    // At deploy (no state): only the very live function the capture compared.
    expect(capture(codeOnly, live({ env: liveEnv }), null)).toBe("session pipeline update");
    expect(() => capture(codeOnly, live({ env: liveEnv }), null, live({ env: { ...liveEnv, X: "1" } }))).toThrow(/is not the one the capture compared/);
  });

  it("negative control (c): the URL added while visibly removing SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED is refused; so is an invisible loss", () => {
    const flag = { SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED: "true" };
    const old = fnProps(C1_BASE, flag);
    const visible = fnProps(C2_ON);
    expect(() => capture(fnUpdate(old, visible), live({ env: old.env as Record<string, unknown>, search: "Enabled" }))).toThrow(
      /may change only by adding or removing SESSION_PIPELINE_SEARCH_QUEUE_URL .*found SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED removed, SESSION_PIPELINE_SEARCH_QUEUE_URL added/,
    );
    const plain = fnProps(C1_BASE);
    expect(() => capture(fnUpdate(plain, visible), live({ env: { ...(plain.env as object), ...flag }, search: "Enabled" }))).toThrow(/SEARCH_INDEX_SKIP_OVER_BOUND_ENABLED removed/);
    expect(() => capture(fnUpdate(plain, visible), live({ env: { ...(plain.env as object), HOOPDB_POOL_MAX: "2" }, search: "Enabled" }))).toThrow(/HOOPDB_POOL_MAX changed/);
  });

  it("all search resources noop: an unrelated read-api update is judged by its own phase; mixed changes stay refused", () => {
    const readApi = REAL.replace(/^\[([a-z0-9-]+)\] update\n[\s\S]*?(?=\n\[)/gm, "[$1] noop\n");
    const withReader = (text: string) =>
      `${text.trimEnd()}\n\n${["session-read-api", "session-read-api-errors-alarm", "session-read-api-secret", "session-read-api-throttles-alarm"].map((r) => (r === "session-read-api" ? "[session-read-api] update\n  no declared property changes" : `[${r}] noop`)).join("\n\n")}\n`.replace(/^Plan: .*$/m, "Plan: 1 to update");
    expect(capture(withReader(readApi), undefined, null)).toBe("session read api update");
    expect(capture(withReader(detailed({})), undefined, null)).toBe("session read api update");
    const mixed = withReader(detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)) })).replace(/^Plan: .*$/m, "Plan: 2 to update");
    expect(() => capture(mixed, live({ pointer: 20 }))).toThrow(/not one staged change/);
  });

  it("refuses a DLQ or alarm update, two staged changes at once, a delete or replace, and a partial resource list", () => {
    for (const r of ["session-search-dlq", "session-search-dlq-alarm", "session-search-age-alarm"])
      expect(() => capture(detailed({ [r]: { action: "update", body: ["  no declared property changes"] } }), live({}))).toThrow(/not one staged change/);
    expect(() => capture(detailed({ "session-events-mapping": update(eventsMapping(20), eventsMapping(10)), "session-search-mapping": update(searchMapping(false), searchMapping(true)) }), live({ pointer: 20 }))).toThrow(/not one staged change/);
    expect(() => capture(detailed({ "session-search": searchCascade() }), live({}))).toThrow(/not one staged change/);
    for (const action of ["delete", "replace"]) expect(() => capture(detailed({ "session-search": { action, body: [] } }), live({}))).toThrow(/forbidden action/);
    expect(() => capture(detailed({}).replace("[session-search-dlq] noop\n", ""), live({}))).toThrow(/missing resource session-search-dlq/);
  });

  it("each staged capture is judged as the staged render says, against the live state of the stage before it", () => {
    const fn = (pi: SessionPipelineInputs) => fnProps(pi);
    const liveOf = (pi: SessionPipelineInputs, first = false) => {
      const st = sessionPoolSettings(pi);
      return live({ env: fn(pi).env as Record<string, unknown>, pointer: st.pointer, search: first ? null : pi.searchQueue?.mappingEnabled ? "Enabled" : "Disabled", searchMax: pi.searchQueue?.maxConcurrency ?? 16 });
    };
    const blocks: Record<string, (prev: SessionPipelineInputs, next: SessionPipelineInputs) => string> = {
      A: () => detailed(creates()),
      B: (a, b) => detailed({ "session-events-mapping": update(eventsMapping(a.maxConcurrency!), eventsMapping(b.maxConcurrency!)) }),
      C1: (a, b) => detailed({ "session-search-mapping": update(searchMapping(!!a.searchQueue?.mappingEnabled), searchMapping(!!b.searchQueue?.mappingEnabled)) }),
      C2: (a, b) => fnUpdate(fn(a), fn(b)),
      R1: (a, b) => fnUpdate(fn(a), fn(b)),
      R2: (a, b) => detailed({ "session-search-mapping": update(searchMapping(!!a.searchQueue?.mappingEnabled), searchMapping(!!b.searchQueue?.mappingEnabled)) }),
      R3: (a, b) => detailed({ "session-events-mapping": update(eventsMapping(a.maxConcurrency!), eventsMapping(b.maxConcurrency!)) }),
    };
    const chain = [...SEARCH_SPLIT_ROLLOUT, ...SEARCH_SPLIT_ROLLBACK];
    for (let n = 1; n < chain.length; n++) {
      const st = chain[n]!, prev = chain[n - 1]!;
      expect([st.id, capture(blocks[st.id]!(prev.p, st.p), liveOf(prev.p, st.id === "A"))]).toEqual([st.id, st.phase]);
    }
  });
});

describe("search-queue split: the live readback", () => {
  const CANARY = "canary-env-value-7f3a";
  const REAL = readFileSync(fileURLToPath(new URL("./fixtures/aws-sessions-prod.pipeline-update.detailed.txt", import.meta.url)), "utf8");
  const LIVE_ROLE = /^  - \s*AWS: (arn:aws:iam::\S+)$/m.exec(REAL)![1]!;
  const FN_ARN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${p.function}`;
  const U1 = "11111111-1111-4111-8111-111111111111";
  const U2 = "22222222-2222-4222-8222-222222222222";
  const okConfig = { FunctionName: p.function, FunctionArn: FN_ARN, Role: LIVE_ROLE, RevisionId: "0a1b2c3d-0000-4000-8000-000000000001", State: "Active", LastUpdateStatus: "Successful", Environment: { Variables: { HOOPDB_POOL_MAX: "3", SECRETISH: CANARY } } };
  const esm = (uuid: string, over: Record<string, unknown> = {}) => ({ UUID: uuid, EventSourceArn: p.queueArn, FunctionArn: FN_ARN, State: "Enabled", BatchSize: 10, MaximumBatchingWindowInSeconds: 20, FunctionResponseTypes: ["ReportBatchItemFailures"], ScalingConfig: { MaximumConcurrency: 20 }, ...over });
  type Responses = { config?: unknown; aliases?: unknown[]; versions?: unknown[]; mappings?: Record<string, unknown[]> };
  /** A fake aws CLI. `mappings` by --function-name target, each a list of pages; `aliases`/`versions` lists of pages. */
  const fake = (r: Responses = {}) => {
    const calls: string[][] = [];
    const pages = (list: unknown[] | undefined, field: string, marker: string | undefined) => {
      const all = list ?? [{ [field]: [] }];
      const at = marker === undefined ? 0 : Number(marker.slice(1));
      return all[at];
    };
    const run = (_cmd: string, args: string[]) => {
      calls.push(args);
      const marker = args.includes("--marker") ? args[args.indexOf("--marker") + 1] : undefined;
      const out = (body: unknown) => {
        if (body && typeof body === "object" && "fail" in (body as object)) return { status: 255, stdout: CANARY, stderr: (body as { fail: string }).fail };
        return { status: 0, stdout: typeof body === "string" ? body : JSON.stringify(body), stderr: "" };
      };
      if (args[1] === "get-function-configuration") return out(r.config ?? okConfig);
      if (args[1] === "list-aliases") return out(pages(r.aliases, "Aliases", marker));
      if (args[1] === "list-versions-by-function") return out(pages(r.versions ?? [{ Versions: [{ Version: "$LATEST" }] }], "Versions", marker));
      const target = args[args.indexOf("--function-name") + 1]!;
      return out(pages((r.mappings ?? { [p.function]: [{ EventSourceMappings: [esm(U1)] }] })[target] ?? [{ EventSourceMappings: [] }], "EventSourceMappings", marker));
    };
    return { run, calls };
  };
  const codeOf = (run: Parameters<typeof readPipelineLive>[0]) => {
    try {
      readPipelineLive(run, STAGE, ACCOUNT, REGION);
      return "ok";
    } catch (e) {
      const err = e as PipelineLiveError;
      // No CLI output, no exception text, no env value in any message.
      expect([err.message, String(err.stack ?? ""), JSON.stringify(err)].join("\n")).not.toContain(CANARY);
      return err.code ?? err.message;
    }
  };

  it("reads the exact function (Active, Successful), its role, RevisionId and env, and the mappings of the function, every alias and every version", () => {
    const f = fake({
      aliases: [{ Aliases: [{ Name: "live" }], NextMarker: "m1" }, { Aliases: [{ Name: "canary" }] }],
      versions: [{ Versions: [{ Version: "$LATEST" }, { Version: "3" }] }],
      mappings: { [p.function]: [{ EventSourceMappings: [esm(U1)], NextMarker: "m1" }, { EventSourceMappings: [esm(U2, { EventSourceArn: p.searchQueueArn, State: "Disabled", ScalingConfig: { MaximumConcurrency: 16 } })] }], [`${FN_ARN}:live`]: [{ EventSourceMappings: [esm(U1)] }] },
    });
    const got = JSON.parse(readPipelineLive(f.run, STAGE, ACCOUNT, REGION)!);
    expect(got.schema).toBe(3);
    expect(got.function).toEqual({ name: p.function, functionArn: FN_ARN, role: LIVE_ROLE, revisionId: okConfig.RevisionId, state: "Active", lastUpdateStatus: "Successful", env: { HOOPDB_POOL_MAX: "3", SECRETISH: CANARY } });
    expect(got.aliases).toEqual(["live", "canary"]);
    expect(got.versions).toEqual(["$LATEST", "3"]);
    expect(got.mappings.map((m: { uuid: string }) => m.uuid)).toEqual([U1, U2]);
    // Every listing follows NextMarker to the end; mappings are listed per alias and version (not $LATEST twice).
    const targets = f.calls.filter((c) => c[1] === "list-event-source-mappings").map((c) => c[c.indexOf("--function-name") + 1]);
    expect([...new Set(targets)]).toEqual([p.function, `${FN_ARN}:live`, `${FN_ARN}:canary`, `${FN_ARN}:3`]);
    expect(f.calls.filter((c) => c[1] === "list-aliases").map((c) => c.includes("--marker"))).toEqual([false, true]);
    expect(f.calls.every((c) => c.includes("--no-paginate") || c[1] === "get-function-configuration")).toBe(true);
    // The fingerprint keeps RevisionId, role and mappings, and only a digest of the env.
    const fp = pipelineLiveFingerprint(got);
    expect(fp.revisionId).toBe(okConfig.RevisionId);
    expect(fp.role).toBe(LIVE_ROLE);
    expect(JSON.stringify(fp)).not.toContain(CANARY);
    expect(readPipelineLive(fake({ config: { fail: "An error occurred (ResourceNotFoundException) when calling GetFunctionConfiguration" } }).run, STAGE, ACCOUNT, REGION)).toBeUndefined();
  });

  it("refuses incomplete, redacted, unsettled or unreadable output with fixed codes, never echoing the canary env value", () => {
    const cfg = (over: Record<string, unknown>) => fake({ config: { ...okConfig, ...over } }).run;
    expect(codeOf(fake().run)).toBe("ok");
    expect(codeOf(fake({ config: "" }).run)).toBe("live_output_empty");
    expect(codeOf(fake({ config: `{"FunctionName":"${p.function}","Environment":{"Variables":{"X":"${CANARY}"` }).run)).toBe("live_parse_failed");
    expect(codeOf(fake({ config: { fail: `AccessDenied ${CANARY}` } }).run)).toBe("live_cli_failed");
    expect(codeOf(() => { throw new Error(`spawn failed ${CANARY}`); })).toBe("live_cli_failed");
    expect(codeOf(cfg({ Environment: { Error: { ErrorCode: "KMSAccessDeniedException", Message: `cannot decrypt ${CANARY}` } } }))).toBe("live_env_error");
    expect(codeOf(cfg({ Environment: undefined }))).toBe("live_incomplete");
    expect(codeOf(cfg({ Environment: { Variables: { X: 1 } } }))).toBe("live_incomplete");
    expect(codeOf(cfg({ Environment: { Variables: { [`bad key ${CANARY}`]: "1" } } }))).toBe("live_incomplete");
    expect(codeOf(cfg({ RevisionId: undefined }))).toBe("live_incomplete");
    expect(codeOf(cfg({ Role: `not-a-role-${CANARY}` }))).toBe("live_incomplete");
    expect(codeOf(cfg({ FunctionName: "someone-else" }))).toBe("live_wrong_function");
    expect(codeOf(cfg({ FunctionArn: `arn:aws:lambda:${REGION}:${ACCOUNT}:function:someone-else` }))).toBe("live_wrong_function");
    expect(codeOf(cfg({ FunctionArn: `${FN_ARN}:1` }))).toBe("live_wrong_function");
    for (const over of [{ State: "Pending" }, { State: "Failed" }, { LastUpdateStatus: "InProgress" }, { LastUpdateStatus: "Failed" }]) expect([over, codeOf(cfg(over))]).toEqual([over, "live_not_settled"]);
    expect(codeOf(fake({ mappings: { [p.function]: [{}] } }).run)).toBe("live_incomplete");
    expect(codeOf(fake({ mappings: { [p.function]: [`[${CANARY}`] } }).run)).toBe("live_parse_failed");
    expect(codeOf(fake({ mappings: { [p.function]: [{ EventSourceMappings: [esm(U1, { UUID: undefined })] }] } }).run)).toBe("live_incomplete");
    expect(codeOf(fake({ mappings: { [p.function]: [{ EventSourceMappings: [esm(U1, { State: `Enabled ${CANARY}` })] }] } }).run)).toBe("live_incomplete");
    expect(codeOf(fake({ mappings: { [p.function]: [{ EventSourceMappings: [esm(`uuid-${CANARY}`)] }] } }).run)).toBe("live_incomplete");
    expect(codeOf(fake({ mappings: { [p.function]: [{ fail: `throttled ${CANARY}` }] } }).run)).toBe("live_cli_failed");
    // Incomplete pagination: a NextMarker whose next page cannot be read, or that repeats, is refused, never a partial list.
    expect(codeOf(fake({ mappings: { [p.function]: [{ EventSourceMappings: [esm(U1)], NextMarker: "m1" }, { fail: "page 2 failed" }] } }).run)).toBe("live_cli_failed");
    expect(codeOf(fake({ mappings: { [p.function]: [{ EventSourceMappings: [esm(U1)], NextMarker: "m0" }] } }).run)).toBe("live_incomplete");
    expect(codeOf(fake({ aliases: [{ Aliases: [{ Name: `bad alias ${CANARY}` }] }] }).run)).toBe("live_incomplete");
    // The same UUID reported differently on two targets is refused.
    expect(codeOf(fake({ aliases: [{ Aliases: [{ Name: "live" }] }], mappings: { [p.function]: [{ EventSourceMappings: [esm(U1)] }], [`${FN_ARN}:live`]: [{ EventSourceMappings: [esm(U1, { State: "Disabled" })] }] } }).run)).toBe("live_incomplete");
  });

  it("guard refusals about the live env name keys, never values", () => {
    const text = readPipelineLive(fake().run, STAGE, ACCOUNT, REGION)!;
    expect(JSON.parse(text).function.env.SECRETISH).toBe(CANARY);
    // The captured #141 cascade: a code-only function update, the intended env from state.
    const plan = REAL;
    const compact = `${plan.split("\n").filter((l) => l.startsWith("Plan:") || /^\[[a-z0-9-]+\] \S+$/.test(l)).join("\n")}\n`;
    const h2 = (x: string) => createHash("sha256").update(x).digest("hex");
    let message = "";
    try {
      assertTranscriptStorePlanContract(compact, {
        detailedPlanText: plan,
        provenanceText: JSON.stringify({ schema: 1, stage: STAGE, outputs: { plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h2(compact) }, detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h2(plan) } }, summary: compact.split("\n")[0] }),
        stateSnapshotText: JSON.stringify({ schema: 1, objects: [{ key: `96bulls/96bulls-aws/${STAGE}/session-pipeline.json`, text: JSON.stringify({ status: "created", attr: { roleArn: LIVE_ROLE }, props: { env: { HOOPDB_POOL_MAX: "3" } } }) }] }),
        liveSnapshotText: text,
      });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/the live function has SECRETISH, which this plan does not render/);
    expect(message).not.toContain(CANARY);
  });
});
