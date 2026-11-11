import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as AWS from "@alchemy.run/aws";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import {
  SESSION_READ_API_RESOURCES,
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
      "/v1/coordination/overlaps",
      "/v1/delivery/records",
      "/v1/digest/daily",
      "/v1/events",
      "/v1/sessions",
      // Lane F1: GET and POST (rth:sync + the admission principal), both behind the bearer auth.
      "/v1/sessions/:sessionId/analysis",
      "/v1/sessions/:sessionId/analysis",
      "/v1/sessions/:sessionId/catalog",
      "/v1/sessions/:sessionId/events",
      "/v1/sessions/:sessionId/metadata",
      "/v1/sessions/:sessionId/summary",
      "/v1/sessions/:sessionId/timing",
      "/v1/sessions/:sessionId/turns",
      "/v1/sessions/activity",
      "/v1/sessions/output",
      "/v1/sessions/summaries/activity",
      "/v1/sessions/titles",
      "/v1/sessions/users",
    ]);
    expect(src).toContain('app.use("/v1/*", async (c, next) => {');
    expect(src).toMatch(/if \(!m\) return c\.json\(\{ error: \{ code: "missing_authorization"/);
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
