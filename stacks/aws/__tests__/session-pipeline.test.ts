import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import * as AWS from "@alchemy.run/aws";
import { fileURLToPath } from "node:url";
import type { AwsInputs } from "../inputs.ts";
import {
  DELIVERY_DELAY_SECONDS,
  FUNCTION_TIMEOUT_SECONDS,
  SESSION_PIPELINE_RESOURCES,
  VISIBILITY_TIMEOUT_SECONDS,
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
import { assertTranscriptStoreInputs, assertTranscriptStorePlanContract, transcriptBucketPolicy, transcriptKeyPolicy, transcriptNames } from "../transcript-store.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const REGION = "us-east-1";
const ROLE = `arn:aws:iam::${ACCOUNT}:role/session-pipeline-role`;
const t = transcriptNames(STAGE, ACCOUNT);
const p = sessionPipelineNames(STAGE, ACCOUNT, REGION);
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
    const grants = sessionPipelinePermissions({ accountId: ACCOUNT, functionName: p.function, ruleArn: RULE_ARN, roleArn: ROLE });
    expect(grants.map((g) => g.id)).toEqual(["session-reconcile-permission", "session-reconcile-continue"]);
    for (const { props } of grants) {
      expect(props.action).toBe("lambda:InvokeFunction");
      expect(props.principal).not.toBe("*");
      expect(JSON.stringify(props)).not.toMatch(/FunctionUrl|"\*"/);
    }
    expect(grants[0]!.props).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: "events.amazonaws.com", sourceArn: RULE_ARN, sourceAccount: ACCOUNT });
    expect(grants[1]!.props).toEqual({ action: "lambda:InvokeFunction", functionName: p.function, principal: ROLE });
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
    expect(() => validate(withPipeline({ searchIndex: true, maxConcurrency: 4, reconcileMinutes: 30 }))).not.toThrow(/sessionPipeline/);
    expect(() => validate(withPipeline({ queue: "x" }))).toThrow(/sessionPipeline.queue is not a known setting/);
    expect(() => validate(withPipeline({ maxConcurrency: 50 }))).toThrow(/maxConcurrency/);
    expect(() => validate(withPipeline({ reconcileMinutes: 1 }))).toThrow(/reconcileMinutes/);
    expect(() => validate(withPipeline([]))).toThrow(/plain object/);
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
});
