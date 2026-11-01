import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import {
  SESSION_READ_API_RESOURCES,
  assertSessionReadApiInputs,
  sessionReadApiEnv,
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
    "transcript-eraser", "transcript-reader", "transcript-writer", "transcripts",
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
      { completeSince: "epoch", vpc: true },
    ])
      expect(() => assertSessionReadApiInputs(bad, refuse)).toThrow();
  });

  it("only the function's own role reads the secret, and the env carries no credential", () => {
    const policy = sessionReadApiSecretPolicy({ roleArn: "arn:aws:iam::941433935742:role/read-api" });
    expect(policy.Statement).toEqual([
      { Sid: "SessionReadApiReads", Effect: "Allow", Principal: { AWS: "arn:aws:iam::941433935742:role/read-api" }, Action: "secretsmanager:GetSecretValue", Resource: "*" },
    ]);
    const env = sessionReadApiEnv({ stage: STAGE, names: sessionReadApiNames(STAGE), p: { completeSince: "epoch" } });
    expect(env.READ_API_ORIGIN).toBe("");
    expect(env.SESSION_READ_API_SECRET_ARN).toBe("96bulls/aws-sessions-prod/session-read-api");
    expect(Object.keys(env).some((k) => /PASSWORD|KEY$|TOKEN|URL$/.test(k))).toBe(false);
  });
});
