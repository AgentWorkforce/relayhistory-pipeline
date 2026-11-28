import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as AWS from "@alchemy.run/aws";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import {
  OUTCOME_ALARM_DEFAULTS,
  REPLAY_REQUESTS_FILTER_PATTERN,
  REPLAY_UNAVAILABLE_FILTER_PATTERN,
  SESSION_READ_API_RESOURCES,
  sessionReadApiOutcomeAlarm,
  sessionReadApiOutcomeMetricFilters,
  assertSessionReadApiInputs,
  sessionReadApiAlarms,
  sessionReadApiEnv,
  sessionReadApiFunctionProps,
  sessionReadApiNames,
  sessionReadApiSecretPolicy,
} from "../session-read-api.ts";
import { assertTranscriptStorePlanContract } from "../transcript-store.ts";

const STAGE = "aws-sessions-prod";
const refuse = (why: string): never => {
  throw new Error(why);
};

describe("session read api: deploy plan contract", () => {
  const store = [
    "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
    "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
    "transcripts-key", "transcripts-key-alias",
  ];
  const withApi = [...store, ...SESSION_READ_API_RESOURCES];
  const withBoth = [...store, ...SESSION_PIPELINE_RESOURCES, ...SESSION_READ_API_RESOURCES];
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const plan = (actions: Record<string, string>, resources: string[]) => {
    const counts = new Map<string, number>();
    for (const [r, a] of Object.entries(actions)) if (resources.includes(r) && a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
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
  const creates = Object.fromEntries(SESSION_READ_API_RESOURCES.map((r) => [r, "create"]));

  it("rollout: the read api's resources are created and nothing else changes", () => {
    expect(check(plan(creates, withApi))).toBe("session read api rollout");
    expect(check(plan(creates, withBoth))).toBe("session read api rollout");
    // The rest after a partial apply.
    expect(check(plan({ "session-read-api-errors-alarm": "create" }, withApi))).toBe("session read api rollout");
  });

  // sessions-prod's first apply created the errors alarm, then failed on the function's reserved
  // concurrency (account quota 10, AWS keeps 10 unreserved). The re-plan with `reservedConcurrency:
  // "none"` creates the function, its secret and the throttles alarm; the errors alarm is noop.
  const partial = { "session-read-api": "create", "session-read-api-secret": "create", "session-read-api-throttles-alarm": "create" };

  it("rollout after the failed first apply: the remaining read api creates and the throttles alarm, alone", () => {
    expect(check(plan(partial, withApi))).toBe("session read api rollout");
    expect(check(plan(partial, withBoth))).toBe("session read api rollout");
    // The throttles alarm alone (everything else already applied).
    expect(check(plan({ "session-read-api-throttles-alarm": "create" }, withBoth))).toBe("session read api rollout");
  });

  // The outcome alarm and its two metric filters, added once everything else is applied: a rollout of their own.
  const outcome = { "session-read-api-outcome-alarm": "create", "session-read-api-replay-requests-metric": "create", "session-read-api-replay-unavailable-metric": "create" };

  it("rollout of the outcome alarm and its metric filters alone, and refused alongside anything else", () => {
    expect(check(plan(outcome, withApi))).toBe("session read api rollout");
    expect(check(plan(outcome, withBoth))).toBe("session read api rollout");
    expect(() => check(plan({ ...outcome, "session-read-api": "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...outcome, "session-pipeline": "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...outcome, transcripts: "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ "session-read-api-replay-requests-metric": "replace" }, withBoth))).toThrow(/forbidden action replace/);
    expect(() => check(plan(outcome, withBoth.filter((r) => r !== "session-read-api-outcome-alarm")))).toThrow(/missing resource session-read-api-outcome-alarm/);
  });

  it("refuses anything beyond that partial rollout shape", () => {
    // A pipeline resource created alongside (e.g. a pipeline alarm): the read api changes alone.
    expect(() => check(plan({ ...partial, "session-pipeline-errors-alarm": "create" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...partial, "session-pipeline": "update" }, withBoth))).toThrow(/read api changes alone/);
    // The existing errors alarm changing while the rest is created.
    expect(() => check(plan({ ...partial, "session-read-api-errors-alarm": "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...partial, budget: "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...partial, "session-read-api-errors-alarm": "replace" }, withBoth))).toThrow(/forbidden action replace/);
    // A plan from before the throttles alarm existed (three read api resources) is incomplete.
    const withoutThrottles = withBoth.filter((r) => r !== "session-read-api-throttles-alarm");
    expect(() => check(plan(partial, withoutThrottles))).toThrow(/missing resource session-read-api-throttles-alarm/);
  });

  it("update: in place (e.g. setting origin), alone", () => {
    expect(check(plan({ "session-read-api": "update" }, withApi))).toBe("session read api update");
    expect(check(plan({ "session-read-api": "update" }, withBoth))).toBe("session read api update");
  });

  it("refuses a read api change together with any store or pipeline change", () => {
    expect(() => check(plan({ ...creates, transcripts: "update" }, withApi))).toThrow(/read api changes alone/);
    expect(() => check(plan({ ...creates, "transcripts-key": "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ "session-read-api": "update", "session-pipeline": "update" }, withBoth))).toThrow(/read api changes alone/);
    expect(() => check(plan({ "session-read-api": "create", "session-read-api-secret": "update" }, withApi))).toThrow(/read api changes alone/);
  });

  it("refuses replacement, deletion and a partial resource list", () => {
    for (const action of ["replace", "delete"]) expect(() => check(plan({ "session-read-api": action }, withApi))).toThrow(/forbidden action/);
    const missing = withApi.filter((r) => r !== "session-read-api-secret");
    expect(() => check(plan(creates, missing))).toThrow(/missing resource session-read-api-secret/);
  });

  it("an all-noop read api leaves the other contracts exactly as they were", () => {
    expect(check(plan({}, withApi))).toBe("post-apply");
    expect(() => check(plan({ transcripts: "update" }, withApi))).toThrow(/initial, recovery/);
    const pipelineCreates = Object.fromEntries(SESSION_PIPELINE_RESOURCES.map((r) => [r, "create"]));
    expect(check(plan({ ...pipelineCreates, transcripts: "update", "transcripts-key": "update" }, withBoth))).toBe("session pipeline rollout");
  });
});

describe("session read api: inputs, grants and env", () => {
  it("accepts the documented shapes and refuses everything else", () => {
    assertSessionReadApiInputs({ completeSince: "2026-09-01T00:00:00Z" }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", origin: `https://${"a".repeat(32)}.lambda-url.us-east-1.on.aws`, reservedConcurrency: 4 }, refuse);
    for (const bad of [
      {},
      { completeSince: "yesterday" },
      { completeSince: "epoch", origin: "https://history.agentrelay.com" },
      { completeSince: "epoch", origin: `https://${"a".repeat(32)}.lambda-url.us-west-2.on.aws` },
      { completeSince: "epoch", origin: `https://${"a".repeat(32)}.lambda-url.us-east-1.on.aws/v1` },
      { completeSince: "epoch", reservedConcurrency: 500 },
      { completeSince: "epoch", reservedConcurrency: 1 },
      { completeSince: "epoch", reservedConcurrency: 51 },
      { completeSince: "epoch", reservedConcurrency: 2.5 },
      { completeSince: "epoch", reservedConcurrency: 0 },
      { completeSince: "epoch", reservedConcurrency: null },
      { completeSince: "epoch", vpc: true },
    ])
      expect(() => assertSessionReadApiInputs(bad, refuse)).toThrow();
  });

  it('reservedConcurrency "none" is accepted, and no other string', () => {
    assertSessionReadApiInputs({ completeSince: "epoch", reservedConcurrency: "none" }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", reservedConcurrency: 2 }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", reservedConcurrency: 50 }, refuse);
    for (const bad of ["None", "NONE", "none ", "", "0", "10", "unreserved", "unlimited", "default"])
      expect(() => assertSessionReadApiInputs({ completeSince: "epoch", reservedConcurrency: bad }, refuse)).toThrow(/reservedConcurrency must be an integer from 2 to 50, or "none"/);
  });

  it("only the function's own role reads the secret, and the env carries no credential", () => {
    const policy = sessionReadApiSecretPolicy({ roleArn: "arn:aws:iam::941433935742:role/read-api" });
    expect(policy.Statement).toEqual([
      { Sid: "SessionReadApiReads", Effect: "Allow", Principal: { AWS: "arn:aws:iam::941433935742:role/read-api" }, Action: "secretsmanager:GetSecretValue", Resource: "*" },
    ]);
    const env = sessionReadApiEnv({ stage: STAGE, names: sessionReadApiNames(STAGE), p: { completeSince: "epoch" } });
    expect(env.READ_API_ORIGIN).toBe("");
    expect(env.SESSION_READ_API_SECRET_ARN).toBe("96bulls/aws-sessions-prod/session-read-api");
    // RELAYAUTH_JWKS_URL is a public key-set location, not a credential; every other *URL stays banned.
    expect(Object.keys(env).filter((k) => k !== "RELAYAUTH_JWKS_URL").some((k) => /PASSWORD|KEY$|TOKEN|URL$/.test(k))).toBe(false);
  });

  it("summary reads: env present only when switched on, exactly \"true\"; inputs are booleans", () => {
    const names = sessionReadApiNames(STAGE);
    for (const p of [{ completeSince: "epoch" }, { completeSince: "epoch", briefSummary: false, listShortSummary: false }]) {
      const env = sessionReadApiEnv({ stage: STAGE, names, p }) as Record<string, string>;
      expect("READ_API_BRIEF_SUMMARY_ENABLED" in env).toBe(false);
      expect("READ_API_LIST_SHORT_SUMMARY_ENABLED" in env).toBe(false);
    }
    const on = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", briefSummary: true, listShortSummary: true } }) as Record<string, string>;
    expect(on.READ_API_BRIEF_SUMMARY_ENABLED).toBe("true");
    expect(on.READ_API_LIST_SHORT_SUMMARY_ENABLED).toBe("true");
    const one = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", listShortSummary: true } }) as Record<string, string>;
    expect("READ_API_BRIEF_SUMMARY_ENABLED" in one).toBe(false);
    expect(one.READ_API_LIST_SHORT_SUMMARY_ENABLED).toBe("true");
    const refuse = (why: string): never => { throw new Error(why); };
    assertSessionReadApiInputs({ completeSince: "epoch", briefSummary: true, listShortSummary: false }, refuse);
    for (const bad of ["true", 1, null])
      for (const k of ["briefSummary", "listShortSummary"])
        expect(() => assertSessionReadApiInputs({ completeSince: "epoch", [k]: bad }, refuse)).toThrow(new RegExp(`${k} must be a boolean`));
  });

  it("list readiness: env present only when switched on, exactly \"true\"; the input is a boolean; independent of the short summary", () => {
    const names = sessionReadApiNames(STAGE);
    for (const p of [{ completeSince: "epoch" }, { completeSince: "epoch", listReadiness: false }, { completeSince: "epoch", listShortSummary: true }]) {
      const env = sessionReadApiEnv({ stage: STAGE, names, p }) as Record<string, string>;
      expect("READ_API_LIST_READINESS_ENABLED" in env).toBe(false);
    }
    const off = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch" } }) as Record<string, string>;
    const on = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", listReadiness: true } }) as Record<string, string>;
    expect(on.READ_API_LIST_READINESS_ENABLED).toBe("true");
    expect("READ_API_LIST_SHORT_SUMMARY_ENABLED" in on).toBe(false);
    // Switching it on adds exactly one variable.
    const { READ_API_LIST_READINESS_ENABLED: _added, ...rest } = on;
    expect(rest).toEqual(off);
    const refuse = (why: string): never => { throw new Error(why); };
    assertSessionReadApiInputs({ completeSince: "epoch", listReadiness: true }, refuse);
    for (const bad of ["true", 1, null])
      expect(() => assertSessionReadApiInputs({ completeSince: "epoch", listReadiness: bad }, refuse)).toThrow(/listReadiness must be a boolean/);
  });

  it("relayfacts: env present only when switched on, exactly \"true\"; the input is a boolean", () => {
    const names = sessionReadApiNames(STAGE);
    const base = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch" } }) as Record<string, string>;
    expect("READ_API_RELAYFACTS_ENABLED" in base).toBe(false);
    const off = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", relayfacts: false } }) as Record<string, string>;
    expect(JSON.stringify(off)).toBe(JSON.stringify(base));
    const on = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", relayfacts: true } }) as Record<string, string>;
    expect(on).toEqual({ ...base, READ_API_RELAYFACTS_ENABLED: "true" });
    const summaries = sessionReadApiEnv({ stage: STAGE, names, p: { completeSince: "epoch", briefSummary: true } }) as Record<string, string>;
    expect("READ_API_RELAYFACTS_ENABLED" in summaries).toBe(false);
    const refuse = (why: string): never => { throw new Error(why); };
    assertSessionReadApiInputs({ completeSince: "epoch", relayfacts: true }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", relayfacts: false }, refuse);
    for (const bad of ["true", 1, null, "on"])
      expect(() => assertSessionReadApiInputs({ completeSince: "epoch", relayfacts: bad }, refuse)).toThrow(/relayfacts must be a boolean/);
  });
});

describe("session read api: reserved concurrency and alarms", () => {
  const props = (p: Record<string, unknown>) =>
    sessionReadApiFunctionProps({ stage: STAGE, names: sessionReadApiNames(STAGE), p: { completeSince: "epoch", ...p } }) as Record<string, unknown>;

  it("reserves 10 by default, the configured number when set, and nothing with \"none\"", () => {
    expect(props({}).reservedConcurrentExecutions).toBe(10);
    expect(props({ reservedConcurrency: 4 }).reservedConcurrentExecutions).toBe(4);
    const none = props({ reservedConcurrency: "none" });
    expect("reservedConcurrentExecutions" in none).toBe(false);
    // Nothing else differs.
    const { reservedConcurrentExecutions: _, ...rest } = props({});
    expect(none).toEqual(rest);
  });

  it("alarms on Errors (unchanged, already applied) and on Throttles: Sum > 0 over 5 minutes", () => {
    const fn = "96bulls-aws-sessions-prod-session-read-api";
    const lambda = { Namespace: "AWS/Lambda", Dimensions: [{ Name: "FunctionName", Value: fn }], Statistic: "Sum", Threshold: 0, ComparisonOperator: "GreaterThanThreshold" };
    const alarms = sessionReadApiAlarms(sessionReadApiNames(STAGE));
    expect(alarms.map(([id]) => id)).toEqual(["session-read-api-errors-alarm", "session-read-api-throttles-alarm"]);
    expect(alarms.map(([id]) => id).every((id) => (SESSION_READ_API_RESOURCES as readonly string[]).includes(id))).toBe(true);
    // The errors alarm exists in sessions-prod state: its props must stay exactly these, or the plan is not noop.
    expect(alarms[0][1]).toEqual({
      ActionsEnabled: false,
      TreatMissingData: "notBreaching",
      name: `${fn}-errors`,
      AlarmDescription: "Session read API invocations are failing (init, timeouts, crashes); Cloud falls back to relayhistory meanwhile.",
      ...lambda,
      MetricName: "Errors",
      Period: 300,
      EvaluationPeriods: 2,
    });
    expect(alarms[1][1]).toEqual({
      ActionsEnabled: false,
      TreatMissingData: "notBreaching",
      name: `${fn}-throttles`,
      AlarmDescription: "Session read API invocations are throttled (reserved concurrency or the account pool); Cloud falls back to relayhistory meanwhile.",
      ...lambda,
      MetricName: "Throttles",
      Period: 300,
      EvaluationPeriods: 1,
    });
  });
});

describe("session read api: outcome alarm (501/503 share of session replay requests)", () => {
  const names = sessionReadApiNames(STAGE);
  const fn = "96bulls-aws-sessions-prod-session-read-api";
  const ns = "96bulls/aws-sessions-prod/session-read-api";

  it("counts each session replay request line once, and the 501/503 ones, from the function's log group", () => {
    const filters = sessionReadApiOutcomeMetricFilters({ stage: STAGE, names, logGroupName: `/aws/lambda/${fn}` });
    expect(filters.map(([id]) => id)).toEqual(["session-read-api-replay-requests-metric", "session-read-api-replay-unavailable-metric"]);
    expect(filters.map(([id]) => id).every((id) => (SESSION_READ_API_RESOURCES as readonly string[]).includes(id))).toBe(true);
    expect(filters[0][1]).toEqual({
      logGroupName: `/aws/lambda/${fn}`,
      filterName: `${fn}-replay-requests`,
      filterPattern: '{ $.message = "session read" && $.route = "/v1/sessions/:sessionId/events" && $.duration_ms >= 0 }',
      metricTransformations: [{ metricName: "ReplayRequests", metricNamespace: ns, metricValue: "1", defaultValue: 0, unit: "Count" }],
    });
    expect(filters[1][1]).toEqual({
      logGroupName: `/aws/lambda/${fn}`,
      filterName: `${fn}-replay-unavailable`,
      filterPattern: '{ $.message = "session read" && $.route = "/v1/sessions/:sessionId/events" && $.duration_ms >= 0 && ($.status = 501 || $.status = 503) }',
      metricTransformations: [{ metricName: "ReplayUnavailable", metricNamespace: ns, metricValue: "1", defaultValue: 0, unit: "Count" }],
    });
    // The unavailable filter is the request filter narrowed, so the share can never exceed 100%.
    expect(REPLAY_UNAVAILABLE_FILTER_PATTERN.startsWith(REPLAY_REQUESTS_FILTER_PATTERN.slice(0, -2))).toBe(true);
    // The route is the one the app registers (the emitted lines themselves: session-read-api-outcome.test.ts).
    const app = readFileSync(fileURLToPath(new URL("../../../src/session-read-api/app.ts", import.meta.url)), "utf8");
    expect(app).toContain('app.get("/v1/sessions/:sessionId/events", async (c) => {');
  });

  it("defaults: >50% of replay requests 501/503 over 30 minutes, at least 20 of them, actionless like the other read api alarms", () => {
    expect(OUTCOME_ALARM_DEFAULTS).toEqual({ thresholdPercent: 50, windowMinutes: 30, minRequests: 20 });
    const [id, props] = sessionReadApiOutcomeAlarm({ stage: STAGE, names, p: { completeSince: "epoch" } });
    expect(id).toBe("session-read-api-outcome-alarm");
    expect((SESSION_READ_API_RESOURCES as readonly string[]).includes(id)).toBe(true);
    const [, errors] = sessionReadApiAlarms(names)[0];
    expect(props.ActionsEnabled).toBe(errors.ActionsEnabled);
    expect(props.TreatMissingData).toBe(errors.TreatMissingData);
    expect(props).toMatchObject({
      name: `${fn}-outcome`,
      Threshold: 50,
      ComparisonOperator: "GreaterThanThreshold",
      EvaluationPeriods: 1,
      DatapointsToAlarm: 1,
      Metrics: [
        { Id: "requests", MetricStat: { Metric: { Namespace: ns, MetricName: "ReplayRequests" }, Period: 1800, Stat: "Sum" }, ReturnData: false },
        { Id: "unavailable", MetricStat: { Metric: { Namespace: ns, MetricName: "ReplayUnavailable" }, Period: 1800, Stat: "Sum" }, ReturnData: false },
        { Id: "unavailable_pct", Expression: "IF(FILL(requests, 0) >= 20, 100 * FILL(unavailable, 0) / requests, 0)", ReturnData: true },
      ],
    });
    // No alarm action of any kind: ALARM state only, and the description says so.
    expect(props.ActionsEnabled).toBe(false);
    for (const k of ["AlarmActions", "OKActions", "InsufficientDataActions"]) expect(k in props).toBe(false);
    expect(props.AlarmDescription).toContain("No alarm action: ALARM state only.");
    // Exactly one series is returned (the alarm's), and it is the expression.
    expect(props.Metrics.filter((m) => m.ReturnData).map((m) => m.Id)).toEqual(["unavailable_pct"]);
    // A metric-math alarm sets no single-metric fields.
    for (const k of ["MetricName", "Namespace", "Statistic", "Period", "Dimensions"]) expect(k in props).toBe(false);
  });

  it("configurable: threshold, window and minimum volume, each independently", () => {
    const alarm = (outcomeAlarm: Record<string, number>) => sessionReadApiOutcomeAlarm({ stage: STAGE, names, p: { completeSince: "epoch", outcomeAlarm } })[1];
    const all = alarm({ thresholdPercent: 25, windowMinutes: 60, minRequests: 100 });
    expect(all.Threshold).toBe(25);
    expect(all.Metrics.slice(0, 2).map((m) => (m as { MetricStat: { Period: number } }).MetricStat.Period)).toEqual([3600, 3600]);
    expect(all.Metrics[2]).toMatchObject({ Expression: "IF(FILL(requests, 0) >= 100, 100 * FILL(unavailable, 0) / requests, 0)" });
    expect(all.AlarmDescription).toContain("more than 25% of session replay requests (GET /v1/sessions/:sessionId/events) answered 501/503 over 60 minutes (at least 100 requests)");
    const one = alarm({ windowMinutes: 15 });
    expect(one.Threshold).toBe(50);
    expect((one.Metrics[0] as { MetricStat: { Period: number } }).MetricStat.Period).toBe(900);
    expect(one.Metrics[2]).toMatchObject({ Expression: "IF(FILL(requests, 0) >= 20, 100 * FILL(unavailable, 0) / requests, 0)" });
    expect(alarm({})).toEqual(sessionReadApiOutcomeAlarm({ stage: STAGE, names, p: { completeSince: "epoch" } })[1]);
  });

  it("validates outcomeAlarm, and leaves the function's env and props untouched", () => {
    assertSessionReadApiInputs({ completeSince: "epoch", outcomeAlarm: {} }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", outcomeAlarm: { thresholdPercent: 1, windowMinutes: 5, minRequests: 1 } }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", outcomeAlarm: { thresholdPercent: 99, windowMinutes: 360, minRequests: 1_000_000 } }, refuse);
    assertSessionReadApiInputs({ completeSince: "epoch", outcomeAlarm: { thresholdPercent: 12.5 } }, refuse);
    for (const [bad, why] of [
      [null, /outcomeAlarm must be a plain object/],
      [[], /outcomeAlarm must be a plain object/],
      [50, /outcomeAlarm must be a plain object/],
      [{ threshold: 50 }, /outcomeAlarm\.threshold is not a known setting/],
      [{ thresholdPercent: 0 }, /thresholdPercent must be a number from 1 to 99/],
      [{ thresholdPercent: 100 }, /thresholdPercent must be a number from 1 to 99/],
      [{ thresholdPercent: "50" }, /thresholdPercent must be a number from 1 to 99/],
      [{ thresholdPercent: Number.NaN }, /thresholdPercent must be a number from 1 to 99/],
      [{ windowMinutes: 4 }, /windowMinutes must be an integer from 5 to 360/],
      [{ windowMinutes: 361 }, /windowMinutes must be an integer from 5 to 360/],
      [{ windowMinutes: 30.5 }, /windowMinutes must be an integer from 5 to 360/],
      [{ minRequests: 0 }, /minRequests must be an integer from 1 to 1000000/],
      [{ minRequests: 2.5 }, /minRequests must be an integer from 1 to 1000000/],
    ] as const)
      expect(() => assertSessionReadApiInputs({ completeSince: "epoch", outcomeAlarm: bad }, refuse)).toThrow(why);
    const base = sessionReadApiFunctionProps({ stage: STAGE, names, p: { completeSince: "epoch" } });
    const tuned = sessionReadApiFunctionProps({ stage: STAGE, names, p: { completeSince: "epoch", outcomeAlarm: { thresholdPercent: 10 } } });
    expect(JSON.stringify(tuned)).toBe(JSON.stringify(base));
  });
});

describe("session read api: Function URL (public by design, application auth)", () => {
  const props = sessionReadApiFunctionProps({ stage: STAGE, names: sessionReadApiNames(STAGE), p: { completeSince: "epoch" } });

  it("declares exactly a buffered NONE-auth URL with no CORS, and nothing Alchemy would default", () => {
    expect(props.functionUrl).toEqual({ authType: "NONE", invokeMode: "BUFFERED" });
    expect(AWS.Lambda.normalizeFunctionUrl(props.functionUrl)).toEqual({ authType: "NONE", cors: undefined, invokeMode: "BUFFERED" });
  });

  it("every /v1 route sits behind the application's bearer auth; only /health is open", () => {
    const src = readFileSync(fileURLToPath(new URL("../../../src/session-read-api/app.ts", import.meta.url)), "utf8");
    const routes = [...src.matchAll(/app\.(get|post|put|patch|delete|all)\("([^"]+)"/g)].map((m) => m[2]);
    expect(routes.sort()).toEqual([
      "/health",
      "/v1/coordination/advisories",
      "/v1/coordination/advisories/:advisoryId/ack",
      "/v1/coordination/overlaps",
      "/v1/delivery/records",
      "/v1/digest/daily",
      "/v1/events",
      "/v1/session-intel/workspace",
      "/v1/sessions",
      // Lane F1: GET and POST (rth:sync + the admission principal), both behind the bearer auth.
      "/v1/sessions/:sessionId/analysis",
      "/v1/sessions/:sessionId/analysis",
      "/v1/sessions/:sessionId/catalog",
      "/v1/sessions/:sessionId/events",
      "/v1/sessions/:sessionId/metadata",
      "/v1/sessions/:sessionId/sources",
      "/v1/sessions/:sessionId/summary",
      "/v1/sessions/:sessionId/timing",
      "/v1/sessions/:sessionId/turns",
      "/v1/sessions/activity",
      "/v1/sessions/output",
      "/v1/sessions/summaries/activity",
      "/v1/sessions/summaries/search",
      "/v1/sessions/titles",
      "/v1/sessions/users",
    ]);
    expect(src).toContain('app.use("/v1/*", async (c, next) => {');
    expect(src).toMatch(/if \(!m\) return c\.json\(\{ error: \{ code: "missing_authorization"/);
  });

  it("route modules registered from app.ts are pinned too, and register after the bearer auth", () => {
    const app = readFileSync(fileURLToPath(new URL("../../../src/session-read-api/app.ts", import.meta.url)), "utf8");
    const memory = readFileSync(fileURLToPath(new URL("../../../src/session-read-api/relayfacts.ts", import.meta.url)), "utf8");
    // Relayfacts (READ_API_RELAYFACTS_ENABLED) registers its routes outside app.ts.
    expect([...memory.matchAll(/app\.(get|post|put|patch|delete|all|on|route|use)\("([^"]+)"/g)].map((m) => m[2]).sort())
      .toEqual(["/v1/relayfacts", "/v1/relayfacts/:topic"]);
    const auth = app.search(/if \(!m\) return c\.json\(\{ error: \{ code: "missing_authorization"/);
    const bearer = app.lastIndexOf('app.use("/v1/*", async (c, next) => {', auth);
    const register = app.indexOf("registerRelayfactsRoutes(app");
    expect(auth).toBeGreaterThan(0);
    expect(bearer).toBeGreaterThan(0);
    expect(register).toBeGreaterThan(auth);
    expect(app.indexOf("registerRelayfactsRoutes(app", register + 1)).toBe(-1);
  });
});

import { sessionReadApiEnv as _envForJwks, sessionReadApiNames as _namesForJwks } from "../session-read-api.ts";
describe("read api RelayAuth endpoints", () => {
  // The verifier's fallback JWKS host (relayauth.dev) does not resolve; prod RelayAuth serves keys
  // at api.relayauth.dev. Without these the deployed API answered 401 to every Cloud request.
  it("sets the production JWKS URL and issuer", () => {
    const env = _envForJwks({ stage: "aws-sessions-prod", names: _namesForJwks("aws-sessions-prod"), p: { completeSince: "2026-10-17T13:48:56.633Z" } as never });
    expect(env.RELAYAUTH_JWKS_URL).toBe("https://api.relayauth.dev/.well-known/jwks.json");
    expect(env.RELAYAUTH_ISSUER).toBe("https://relayauth.dev");
  });
});
