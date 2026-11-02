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
 * Alchemy creates gets nothing else.
 */
import * as AWS from "@alchemy.run/aws";
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
  /**
   * Workspace ids allowed to use the internal directory search route
   * (GET /v1/directory/search; 1 to 8 ids). Omitted: the route is closed for every
   * workspace and the function's environment is unchanged.
   */
  directorySearchWorkspaces?: string[];
}

export const SESSION_READ_API_RESOURCES = [
  "session-read-api",
  "session-read-api-secret",
  "session-read-api-errors-alarm",
  "session-read-api-throttles-alarm",
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

const DIRECTORY_WORKSPACE = /^[^\s,\p{Cc}]{1,128}$/u;
const ORIGIN = /^https:\/\/[a-z0-9]{32}\.lambda-url\.us-east-1\.on\.aws$/;

export function assertSessionReadApiInputs(p: unknown, refuse: (why: string) => never): asserts p is SessionReadApiInputs {
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.getPrototypeOf(p) !== Object.prototype)
    refuse("transcriptStore.sessionReadApi must be a plain object");
  const v = p as Record<string, unknown>;
  for (const k of Object.keys(v))
    if (k !== "completeSince" && k !== "origin" && k !== "reservedConcurrency" && k !== "directorySearchWorkspaces") refuse(`transcriptStore.sessionReadApi.${k} is not a known setting`);
  if (typeof v.completeSince !== "string" || (v.completeSince !== "epoch" && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(v.completeSince)))
    refuse('transcriptStore.sessionReadApi.completeSince must be an ISO UTC timestamp or "epoch"');
  if (v.origin !== undefined && (typeof v.origin !== "string" || !ORIGIN.test(v.origin)))
    refuse("transcriptStore.sessionReadApi.origin must be this function's us-east-1 Function URL origin (no path)");
  if (v.reservedConcurrency !== undefined && v.reservedConcurrency !== "none" && !(Number.isInteger(v.reservedConcurrency) && (v.reservedConcurrency as number) >= 2 && (v.reservedConcurrency as number) <= 50))
    refuse('transcriptStore.sessionReadApi.reservedConcurrency must be an integer from 2 to 50, or "none" (only below the default account quota)');
  if (v.directorySearchWorkspaces !== undefined) {
    const ws = v.directorySearchWorkspaces;
    if (!Array.isArray(ws) || ws.length < 1 || ws.length > 8 || new Set(ws).size !== ws.length || !ws.every((w) => typeof w === "string" && DIRECTORY_WORKSPACE.test(w)))
      refuse("transcriptStore.sessionReadApi.directorySearchWorkspaces must be 1 to 8 distinct workspace ids (1-128 characters, no whitespace or commas)");
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

export function sessionReadApiEnv(i: { stage: string; names: SessionReadApiNames; p: SessionReadApiInputs }) {
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
    NODE_OPTIONS: "--enable-source-maps",
    // Only when set, so a stage without directory search keeps its environment byte-identical.
    ...(i.p.directorySearchWorkspaces ? { READ_API_DIRECTORY_WORKSPACES: i.p.directorySearchWorkspaces.join(",") } : {}),
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
export function sessionReadApiFunctionProps(i: { stage: string; names: SessionReadApiNames; p: SessionReadApiInputs }) {
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

export interface SessionReadApiStageInputs {
  stage: string;
  p: SessionReadApiInputs;
  tags: (component: string) => Record<string, string>;
}

export const sessionReadApi = (i: SessionReadApiStageInputs) =>
  Effect.gen(function* () {
    const names = sessionReadApiNames(i.stage);
    const fn = yield* AWS.Lambda.Function("session-read-api", sessionReadApiFunctionProps({ stage: i.stage, names, p: i.p }) as never);
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
    return {
      functionName: names.function,
      functionUrl: (fn as unknown as { functionUrl: string | undefined }).functionUrl,
      secretName: names.secret,
    };
  });
