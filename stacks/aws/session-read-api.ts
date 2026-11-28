/**
 * The sessions-prod session read API (src/session-read-api): one Lambda behind
 * its Function URL, serving relayhistory's /v1 event reads from ClickHouse
 * `transcript_events` with Supabase for erasure. Declared inside the
 * `aws-sessions-prod` transcript-store stage, opt-in with
 * `transcriptStore.sessionReadApi`.
 *
 *   Cloud Worker --HTTPS, Bearer RelayAuth assertion (aud = this origin)--> Function URL
 *       --> Lambda session-read-api --> ClickHouse Cloud (SELECT-only user)
 *                                   --> Supabase (bulls96_read_api role)
 *
 * The Function URL is `authType: NONE` because the caller is a Cloudflare
 * Worker with no AWS identity; every /v1 request is authenticated by the
 * application (RS256 RelayAuth assertion bound to this exact origin). Until
 * `origin` is set (a second apply, once the URL exists) every request is 401.
 *
 * The function reads no bucket, key or queue: its only grant is the secret's
 * resource policy (GetSecretValue for the function's own role). The role
 * Alchemy creates gets nothing else. The one exception is opt-in: with
 * `analysisBucket: true` (lane F1) the transcript bucket and key policies let
 * this role GetObject/Decrypt the session-analysis result objects only
 * (96bulls/<org>/v1/.../normalized/session-analysis-v2/*), and the function gets
 * READ_API_ANALYSIS_BUCKET. It is enabled in the same reviewed plan phase as
 * `sessionPipeline.analysisJobs` (transcript-store.ts sessionAnalysisJobPhase).
 */
import * as AWS from "@alchemy.run/aws";
import * as Output from "alchemy/Output";
import * as Effect from "effect/Effect";

export interface SessionReadApiInputs {
  /**
   * ISO timestamp from which ClickHouse holds complete history (the backfill
   * start), or "epoch" once backfill is proven complete. Older reads get 501
   * and the caller falls back.
   */
  completeSince: string;
  /** This function's URL origin (https://<id>.lambda-url.us-east-1.on.aws). Omit on the first apply. */
  origin?: string;
  /**
   * Reserved concurrency (an integer 2-50). Default 10.
   *
   * `"none"` reserves nothing: the function runs from the account's unreserved
   * pool. It exists only for an account whose Lambda concurrency quota is below
   * the default (a new account starts at 10, and AWS keeps 10 unreserved, so any
   * reservation is refused). It is never the default, and must be removed once
   * the quota is raised. Throttles are alarmed either way
   * (`session-read-api-throttles-alarm`).
   */
  reservedConcurrency?: number | "none";
  /** Lane F1: serve GET/POST /v1/sessions/:id/analysis from the transcript bucket. Default off. */
  analysisBucket?: boolean;
  /**
   * Serve the native session summary reads (GET /v1/sessions/:id/summary and the summary activity; the function
   * gets READ_API_BRIEF_SUMMARY_ENABLED=true). Default off: absent, the env is unchanged and those reads stay 501
   * `summary_not_configured`. Needs sessions.session_briefs and 20261105056000 applied, or those reads 501 anyway.
   */
  briefSummary?: boolean;
  /**
   * Serve GET /v1/sessions?includeShortSummary=recorded (READ_API_LIST_SHORT_SUMMARY_ENABLED=true). Default off:
   * absent, the param stays 501 `param_includeShortSummary` and the list shape is unchanged either way.
   */
  listShortSummary?: boolean;
  /**
   * Serve GET /v1/sessions?includeReadiness=true and ?readyOnly=true (READ_API_LIST_READINESS_ENABLED=true). Default
   * off: absent, the env is unchanged, either param stays 501 `param_includeReadiness` / `param_readyOnly`, and the
   * list is unchanged either way. Needs 20261105062200 applied, or those requests answer 501 `directory_not_migrated`.
   */
  listReadiness?: boolean;
  /**
   * Serve GET /v1/relayfacts and GET /v1/relayfacts/:topic (READ_API_RELAYFACTS_ENABLED=true; docs/relayfacts.md).
   * Default off: absent, the env is unchanged and both routes stay 501 `relayfacts_disabled`. Needs 062080 and
   * 062090 applied, and each workspace still needs the `relayfacts` canary.
   */
  relayfacts?: boolean;
  /**
   * The outcome alarm (`session-read-api-outcome-alarm`): the share of session replay requests (GET
   * /v1/sessions/:sessionId/events) answered 501 or 503 over one window. Actionless: it exposes ALARM state only.
   * Every field optional; defaults in {@link OUTCOME_ALARM_DEFAULTS}. The function never fails these requests, so the
   * Errors alarm stays quiet while Cloud falls back on every read; this alarm is the one that notices.
   */
  outcomeAlarm?: SessionReadApiOutcomeAlarmInputs;
}

export interface SessionReadApiOutcomeAlarmInputs {
  /** Alarm when more than this percent of replay requests in the window answered 501/503 (1-99). Default 50. */
  thresholdPercent?: number;
  /** The window, in minutes (an integer 5-360). Default 30. */
  windowMinutes?: number;
  /** Fewer replay requests than this in the window never alarm (an integer >= 1). Default 20. */
  minRequests?: number;
}

export const OUTCOME_ALARM_DEFAULTS = { thresholdPercent: 50, windowMinutes: 30, minRequests: 20 } as const;

export const SESSION_READ_API_RESOURCES = [
  "session-read-api",
  "session-read-api-secret",
  "session-read-api-errors-alarm",
  "session-read-api-throttles-alarm",
  "session-read-api-replay-requests-metric",
  "session-read-api-replay-unavailable-metric",
  "session-read-api-outcome-alarm",
] as const;

export const READ_API_TIMEOUT_SECONDS = 10;
export const READ_API_MEMORY_MB = 1024;

export interface SessionReadApiNames {
  function: string;
  secret: string;
}

export function sessionReadApiNames(stage: string): SessionReadApiNames {
  return { function: `96bulls-${stage}-session-read-api`, secret: `96bulls/${stage}/session-read-api` };
}

const ORIGIN = /^https:\/\/[a-z0-9]{32}\.lambda-url\.us-east-1\.on\.aws$/;

export function assertSessionReadApiInputs(p: unknown, refuse: (why: string) => never): asserts p is SessionReadApiInputs {
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.getPrototypeOf(p) !== Object.prototype)
    refuse("transcriptStore.sessionReadApi must be a plain object");
  const v = p as Record<string, unknown>;
  for (const k of Object.keys(v))
    if (k !== "completeSince" && k !== "origin" && k !== "reservedConcurrency" && k !== "analysisBucket" && k !== "briefSummary" && k !== "listShortSummary" && k !== "listReadiness" && k !== "relayfacts" && k !== "outcomeAlarm")
      refuse(`transcriptStore.sessionReadApi.${k} is not a known setting`);
  if (typeof v.completeSince !== "string" || (v.completeSince !== "epoch" && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(v.completeSince)))
    refuse('transcriptStore.sessionReadApi.completeSince must be an ISO UTC timestamp or "epoch"');
  if (v.origin !== undefined && (typeof v.origin !== "string" || !ORIGIN.test(v.origin)))
    refuse("transcriptStore.sessionReadApi.origin must be this function's us-east-1 Function URL origin (no path)");
  if (v.analysisBucket !== undefined && typeof v.analysisBucket !== "boolean")
    refuse("transcriptStore.sessionReadApi.analysisBucket must be a boolean");
  for (const k of ["briefSummary", "listShortSummary", "listReadiness", "relayfacts"] as const)
    if (v[k] !== undefined && typeof v[k] !== "boolean") refuse(`transcriptStore.sessionReadApi.${k} must be a boolean`);
  if (v.reservedConcurrency !== undefined && v.reservedConcurrency !== "none" && !(Number.isInteger(v.reservedConcurrency) && (v.reservedConcurrency as number) >= 2 && (v.reservedConcurrency as number) <= 50))
    refuse('transcriptStore.sessionReadApi.reservedConcurrency must be an integer from 2 to 50, or "none" (only below the default account quota)');
  if (v.outcomeAlarm !== undefined) {
    const a = v.outcomeAlarm;
    if (a === null || typeof a !== "object" || Array.isArray(a) || Object.getPrototypeOf(a) !== Object.prototype)
      refuse("transcriptStore.sessionReadApi.outcomeAlarm must be a plain object");
    const o = a as Record<string, unknown>;
    for (const k of Object.keys(o))
      if (k !== "thresholdPercent" && k !== "windowMinutes" && k !== "minRequests") refuse(`transcriptStore.sessionReadApi.outcomeAlarm.${k} is not a known setting`);
    const int = (x: unknown, lo: number, hi: number) => Number.isInteger(x) && (x as number) >= lo && (x as number) <= hi;
    if (o.thresholdPercent !== undefined && !(typeof o.thresholdPercent === "number" && Number.isFinite(o.thresholdPercent) && o.thresholdPercent >= 1 && o.thresholdPercent <= 99))
      refuse("transcriptStore.sessionReadApi.outcomeAlarm.thresholdPercent must be a number from 1 to 99");
    if (o.windowMinutes !== undefined && !int(o.windowMinutes, 5, 360))
      refuse("transcriptStore.sessionReadApi.outcomeAlarm.windowMinutes must be an integer from 5 to 360");
    if (o.minRequests !== undefined && !int(o.minRequests, 1, 1_000_000))
      refuse("transcriptStore.sessionReadApi.outcomeAlarm.minRequests must be an integer from 1 to 1000000");
  }
}

/** Only the read API's role reads its secret. */
export function sessionReadApiSecretPolicy(i: { roleArn: string }) {
  return {
    Version: "2012-10-17" as const,
    Statement: [
      { Sid: "SessionReadApiReads", Effect: "Allow" as const, Principal: { AWS: i.roleArn }, Action: "secretsmanager:GetSecretValue", Resource: "*" },
    ],
  };
}

/** Production RelayAuth endpoints the read API verifies Cloud's assertions against. */
export const RELAYAUTH_PROD_JWKS_URL = "https://api.relayauth.dev/.well-known/jwks.json";
export const RELAYAUTH_PROD_ISSUER = "https://relayauth.dev";

/** Lane F1: the result object root the read API may GetObject/Decrypt (session-pipeline.ts ANALYSIS_RESULT_OBJECT_GLOB). */
export function readApiAnalysisBucketStatement(i: { bucketArn: string; roleArn: string; glob: string }) {
  return { Sid: "SessionReadApiReadsAnalysisResults", Effect: "Allow" as const, Principal: { AWS: i.roleArn }, Action: ["s3:GetObject"], Resource: [`${i.bucketArn}/${i.glob}`] };
}
export function readApiAnalysisKeyStatement(i: { accountId: string; region: string; bucketName: string; roleArn: string; glob: string }) {
  return {
    Sid: "SessionReadApiDecryptsAnalysisResultsViaS3",
    Effect: "Allow" as const,
    Principal: { AWS: i.roleArn },
    Action: ["kms:Decrypt"],
    Resource: "*",
    Condition: {
      StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
      StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${i.bucketName}/${i.glob}` },
    },
  };
}

export function sessionReadApiEnv(i: { stage: string; names: SessionReadApiNames; p: SessionReadApiInputs; analysisBucketName?: string }) {
  return {
    DEPLOYMENT: i.stage,
    DEPLOYMENT_ID: i.stage,
    OTEL_SERVICE_NAME: "96bulls-session-read-api",
    SESSION_READ_API_SECRET_ARN: i.names.secret,
    READ_API_COMPLETE_SINCE: i.p.completeSince,
    // Empty until the second apply: the app then refuses every token.
    READ_API_ORIGIN: i.p.origin ?? "",
    // Production RelayAuth (the values relayhistory-cloud deploys with, repo vars RELAYAUTH_*).
    // The shared verifier's fallback JWKS, https://relayauth.dev/.well-known/jwks.json, does not
    // resolve, so without this every token failed the JWKS fetch and Cloud saw only 401s.
    RELAYAUTH_JWKS_URL: RELAYAUTH_PROD_JWKS_URL,
    RELAYAUTH_ISSUER: RELAYAUTH_PROD_ISSUER,
    // Lane F1: present only when analysisBucket is on, so existing deploys see no env change.
    ...(i.p.analysisBucket && i.analysisBucketName ? { READ_API_ANALYSIS_BUCKET: i.analysisBucketName } : {}),
    // Summary reads: present only when switched on (exactly "true", src/session-read-api/config.ts), so existing
    // deploys see no env change; switching either on is a "session read api update" phase on its own.
    ...(i.p.briefSummary === true ? { READ_API_BRIEF_SUMMARY_ENABLED: "true" } : {}),
    ...(i.p.listShortSummary === true ? { READ_API_LIST_SHORT_SUMMARY_ENABLED: "true" } : {}),
    ...(i.p.listReadiness === true ? { READ_API_LIST_READINESS_ENABLED: "true" } : {}),
    ...(i.p.relayfacts === true ? { READ_API_RELAYFACTS_ENABLED: "true" } : {}),
    NODE_OPTIONS: "--enable-source-maps",
  };
}

/**
 * The read API function's props. Unlike the session pipeline (`functionUrl:
 * false`), this function is reached through its Function URL by design
 * (docs/session-read-api-design.md section 1): the caller is a Cloudflare Worker
 * with no AWS identity, so `AWS_IAM` URL auth cannot be used, and every `/v1`
 * request is authenticated in the application (RS256 RelayAuth assertion bound
 * to this exact origin, src/session-read-api/auth.ts). Alchemy grants
 * the URL invoke action to `*` for an `authType: "NONE"` URL and
 * `lambda:InvokeFunction` to `*` only with `InvokedViaFunctionUrl`, so the
 * Invoke API stays closed. Explicit, buffered, and no CORS (no browser caller).
 */
export function sessionReadApiFunctionProps(i: { stage: string; names: SessionReadApiNames; p: SessionReadApiInputs; analysisBucketName?: string }) {
  return {
    functionName: i.names.function,
    main: "src/session-read-api/lambda.ts",
    handler: "handler",
    runtime: "nodejs24.x",
    architecture: "arm64",
    memorySize: READ_API_MEMORY_MB,
    timeout: `${READ_API_TIMEOUT_SECONDS} seconds`,
    // "none": the key is absent, so Alchemy sets no function-level reservation.
    ...(i.p.reservedConcurrency === "none" ? {} : { reservedConcurrentExecutions: i.p.reservedConcurrency ?? 10 }),
    env: sessionReadApiEnv(i),
    functionUrl: { authType: "NONE" as const, invokeMode: "BUFFERED" as const },
    logging: { retention: "30 days" },
    build: { tsconfig: "tsconfig.json" },
  };
}

/**
 * The read API's alarms (tags are added at declaration). Errors: invocations
 * failing. Throttles: invocations refused for concurrency, by the function's
 * reservation or, with `reservedConcurrency: "none"`, by the account pool it
 * then shares with the session pipeline.
 */
export function sessionReadApiAlarms(names: SessionReadApiNames) {
  const lambda = { Namespace: "AWS/Lambda", Dimensions: [{ Name: "FunctionName", Value: names.function }], Statistic: "Sum", Threshold: 0, ComparisonOperator: "GreaterThanThreshold" };
  return [
    ["session-read-api-errors-alarm", {
      ActionsEnabled: false,
      TreatMissingData: "notBreaching",
      name: `${names.function}-errors`,
      AlarmDescription: "Session read API invocations are failing (init, timeouts, crashes); Cloud falls back to relayhistory meanwhile.",
      ...lambda,
      MetricName: "Errors",
      Period: 300,
      EvaluationPeriods: 2,
    }],
    ["session-read-api-throttles-alarm", {
      ActionsEnabled: false,
      TreatMissingData: "notBreaching",
      name: `${names.function}-throttles`,
      AlarmDescription: "Session read API invocations are throttled (reserved concurrency or the account pool); Cloud falls back to relayhistory meanwhile.",
      ...lambda,
      MetricName: "Throttles",
      Period: 300,
      EvaluationPeriods: 1,
    }],
  ] as const;
}

/** Where the outcome metrics land: one namespace per stage (metric filters cannot set a static dimension). */
export function sessionReadApiMetricNamespace(stage: string): string {
  return `96bulls/${stage}/session-read-api`;
}

/**
 * The session replay route (the dashboard's session view; Cloud's worker reads GET /v1/sessions/:sessionId/events).
 * Only this route is counted: the rest of /v1 (directory, workspace search, flag-disabled routes answering 501 by
 * design) would dilute the share, e.g. 20 replays all 501 beside 100 healthy directory reads is 16.7%.
 */
export const READ_REPLAY_ROUTE = "/v1/sessions/:sessionId/events";

/**
 * The request line the app writes once per request, after the handler (src/session-read-api/app.ts, the "*"
 * middleware: route = Hono's routePath, status, duration_ms), raw JSON on stdout (src/shared/log.ts). The handler's own
 * reason lines ("session read" with a reason and no duration_ms) are excluded by `duration_ms >= 0`, so a request
 * counts once.
 */
const REPLAY_REQUEST_LINE = `$.message = "session read" && $.route = "${READ_REPLAY_ROUTE}" && $.duration_ms >= 0`;
export const REPLAY_REQUESTS_FILTER_PATTERN = `{ ${REPLAY_REQUEST_LINE} }`;
/** 501 (session_partial, history floor) and 503 (retryable session_partial, ClickHouse, unavailable). */
export const REPLAY_UNAVAILABLE_FILTER_PATTERN = `{ ${REPLAY_REQUEST_LINE} && ($.status = 501 || $.status = 503) }`;

/** Two counters from the function's log group: every replay request, and the ones answered 501/503. */
export function sessionReadApiOutcomeMetricFilters(i: { stage: string; names: SessionReadApiNames; logGroupName: string }) {
  const metricNamespace = sessionReadApiMetricNamespace(i.stage);
  const counter = (metricName: string) => [{ metricName, metricNamespace, metricValue: "1", defaultValue: 0, unit: "Count" as const }];
  return [
    ["session-read-api-replay-requests-metric", {
      logGroupName: i.logGroupName,
      filterName: `${i.names.function}-replay-requests`,
      filterPattern: REPLAY_REQUESTS_FILTER_PATTERN,
      metricTransformations: counter("ReplayRequests"),
    }],
    ["session-read-api-replay-unavailable-metric", {
      logGroupName: i.logGroupName,
      filterName: `${i.names.function}-replay-unavailable`,
      filterPattern: REPLAY_UNAVAILABLE_FILTER_PATTERN,
      metricTransformations: counter("ReplayUnavailable"),
    }],
  ] as const;
}

/**
 * The outcome alarm: more than `thresholdPercent` of replay requests in one `windowMinutes` window answered 501/503,
 * with at least `minRequests` of them in it (a quiet window, or no data, never alarms). Actionless, like the other read
 * API alarms: there is no approved alarm destination, so it exposes ALARM state only (console, DescribeAlarms) and
 * notifies nobody until one is wired.
 */
export function sessionReadApiOutcomeAlarm(i: { stage: string; names: SessionReadApiNames; p: SessionReadApiInputs }) {
  const { thresholdPercent, windowMinutes, minRequests } = { ...OUTCOME_ALARM_DEFAULTS, ...i.p.outcomeAlarm };
  const Namespace = sessionReadApiMetricNamespace(i.stage);
  const Period = windowMinutes * 60;
  const sum = (Id: string, MetricName: string) => ({ Id, MetricStat: { Metric: { Namespace, MetricName }, Period, Stat: "Sum" }, ReturnData: false });
  return ["session-read-api-outcome-alarm", {
    ActionsEnabled: false,
    TreatMissingData: "notBreaching",
    name: `${i.names.function}-outcome`,
    AlarmDescription: `Session read API: more than ${thresholdPercent}% of session replay requests (GET ${READ_REPLAY_ROUTE}) answered 501/503 over ${windowMinutes} minutes (at least ${minRequests} requests); the function is not failing, but Cloud falls back to relayhistory on those reads. Check the "session read" log lines' reason. No alarm action: ALARM state only.`,
    Metrics: [
      sum("requests", "ReplayRequests"),
      sum("unavailable", "ReplayUnavailable"),
      {
        Id: "unavailable_pct",
        Expression: `IF(FILL(requests, 0) >= ${minRequests}, 100 * FILL(unavailable, 0) / requests, 0)`,
        Label: "501/503 share of session replay requests (%)",
        ReturnData: true,
      },
    ],
    Threshold: thresholdPercent,
    ComparisonOperator: "GreaterThanThreshold",
    EvaluationPeriods: 1,
    DatapointsToAlarm: 1,
  }] as const;
}

export interface SessionReadApiStageInputs {
  stage: string;
  p: SessionReadApiInputs;
  /** The transcript bucket, for READ_API_ANALYSIS_BUCKET when analysisBucket is on. */
  analysisBucketName?: string;
  tags: (component: string) => Record<string, string>;
}

export const sessionReadApi = (i: SessionReadApiStageInputs) =>
  Effect.gen(function* () {
    const names = sessionReadApiNames(i.stage);
    const fn = yield* AWS.Lambda.Function("session-read-api", sessionReadApiFunctionProps({ stage: i.stage, names, p: i.p, ...(i.analysisBucketName ? { analysisBucketName: i.analysisBucketName } : {}) }) as never);
    const roleArn = (fn as unknown as { roleArn: string }).roleArn;
    // A container only: the lead sets the value from 1Password (runbook). Alchemy state never holds it.
    yield* AWS.SecretsManager.Secret("session-read-api-secret", {
      name: names.secret,
      description: "96bulls session read API: ClickHouse SELECT-only user and Supabase bulls96_read_api URL (JSON)",
      resourcePolicy: sessionReadApiSecretPolicy({ roleArn }),
      recoveryWindowInDays: 7,
      tags: i.tags("session-read-api"),
    } as never);
    for (const [id, props] of sessionReadApiAlarms(names)) yield* AWS.CloudWatch.Alarm(id, { ...props, tags: i.tags("session-read-api") } as never);
    // The function's own log group (Alchemy creates it with the function); mapped from the function's output so the
    // filters wait for it.
    const logGroupName = Output.map(fn as never, (f: { functionName: string }) => `/aws/lambda/${f.functionName}`) as unknown as string;
    for (const [id, props] of sessionReadApiOutcomeMetricFilters({ stage: i.stage, names, logGroupName })) yield* AWS.Logs.MetricFilter(id, props as never);
    const [outcomeId, outcomeProps] = sessionReadApiOutcomeAlarm({ stage: i.stage, names, p: i.p });
    yield* AWS.CloudWatch.Alarm(outcomeId, { ...outcomeProps, tags: i.tags("session-read-api") } as never);
    return {
      roleArn,
      functionName: names.function,
      functionUrl: (fn as unknown as { functionUrl: string | undefined }).functionUrl,
      secretName: names.secret,
    };
  });
