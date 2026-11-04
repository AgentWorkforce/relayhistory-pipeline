/** Offline only: GitHub/Cloudflare/Alchemy are fake capability ports. */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import {
  assertIngestCiContext,
  assertHumanGate,
  assertCurrentHumanApproval,
  assertCaptureRun,
  runPrivateIngestCI,
  verifyIngestCiCapture,
  assertFreshIngestPlan,
  safePlan,
  boundedCiJson,
  INGEST_CI_ENVIRONMENT,
  INGEST_CI_WORKFLOW,
  type IngestCiIO,
} from "../../../scripts/private-ingest-ci.ts";
const digest = (text: string) =>
  createHash("sha256").update(text).digest("hex");
const source = "a".repeat(40),
  runId = "123",
  account = "a".repeat(32);
const plan =
  "[session-ingest-fanout-dlq] create\n[session-ingest-fanout] create\n[session-ingest] create\n[session-ingest-fanout-consumer] create\nPlan: 4 to create\n";
const environment = {
  id: 10,
  name: INGEST_CI_ENVIRONMENT,
  protection_rules: [
    {
      type: "required_reviewers",
      reviewers: [
        {
          type: "User",
          reviewer: { type: "User", login: "human-owner", id: 7 },
        },
      ],
    },
  ],
  deployment_branch_policy: {
    protected_branches: false,
    custom_branch_policies: true,
  },
};
const branches = {
  total_count: 1,
  branch_policies: [{ name: "main", type: "branch" }],
};
const history = [
  {
    state: "approved",
    user: { type: "User", login: "human-owner", id: 7 },
    environments: [{ id: 10, name: INGEST_CI_ENVIRONMENT }],
  },
];
function fixture() {
  const env = {
    GITHUB_ACTIONS: "true",
    GITHUB_EVENT_NAME: "workflow_dispatch",
    GITHUB_REF: "refs/heads/main",
    GITHUB_SHA: source,
    EXPECTED_SOURCE_SHA: source,
    GITHUB_RUN_ATTEMPT: "1",
    GITHUB_RUN_ID: runId,
    GITHUB_REPOSITORY: "AgentWorkforce/sessions-pipeline",
    SESSION_INGEST_APPROVER_LOGIN: "human-owner",
    INGEST_CI_PHASE: "capture",
    ACKNOWLEDGE_STATE_READY: "true",
    SESSION_INGEST_ACCOUNT_ID: account,
    SESSION_INGEST_HYPERDRIVE_ID: "b".repeat(32),
    SESSION_INGEST_SECRETS_STORE_ID: "c".repeat(32),
    SESSION_INGEST_KMS_KEY_ARN:
      "arn:aws:kms:us-east-1:941433935742:key/12345678-90ab-cdef-1234-567890abcdef",
    CLOUDFLARE_ACCOUNT_ID: account,
    CLOUDFLARE_API_TOKEN: "synthetic-token-never-printed",
  } as Record<string, string>;
  const files = new Map([
      [
        "node_modules/alchemy/package.json",
        JSON.stringify({ version: "2.0.0-beta.84" }),
      ],
    ]),
    commands: Array<{ command: string; args: string[]; cloudflare: boolean }> =
      [],
    github: string[] = [],
    cf: string[] = [];
  let sourceDirty = false,
    envResult: any = structuredClone(environment),
    branchResult: any = structuredClone(branches),
    approvalResult: any = structuredClone(history),
    planResult = plan,
    detailResult = plan;
  const captureRun = {
    id: 123,
    head_sha: source,
    head_branch: "main",
    run_attempt: 1,
    event: "workflow_dispatch",
    status: "completed",
    conclusion: "success",
    path: INGEST_CI_WORKFLOW,
    repository: { full_name: env.GITHUB_REPOSITORY },
  };
  const io: IngestCiIO = {
    env,
    read: (path) => {
      const value = files.get(path);
      if (value === undefined) throw Error("fixture file missing " + path);
      return value;
    },
    write: (path, text) => {
      files.set(path, text);
    },
    run: (command, args, cloudflare = false) => {
      commands.push({ command, args, cloudflare });
      if (command === "git")
        return args[0] === "rev-parse"
          ? source
          : sourceDirty
            ? " M source.ts"
            : "";
      if (args[0] === "--version")
        return command === "node" ? "v24.21.0" : "1.4.2";
      if (args[0] !== "node_modules/alchemy/bin/cli.js")
        throw Error("unexpected command");
      return args[1] === "deploy"
        ? ""
        : args.includes("--detailed")
          ? detailResult
          : planResult;
    },
    github: async (path) => {
      github.push(path);
      if (path.endsWith("/deployment-branch-policies")) return branchResult;
      if (path.endsWith("/approvals")) return approvalResult;
      if (path.includes("/environments/")) return envResult;
      if (path.endsWith("/actions/runs/123")) return captureRun;
      throw Error("unexpected GitHub route");
    },
    cloudflare: async (path) => {
      cf.push(path);
      return path.endsWith("/subdomain")
        ? { enabled: false, previews_enabled: false }
        : [];
    },
  };
  return {
    io,
    env,
    files,
    commands,
    github,
    cf,
    captureRun,
    setEnvironment: (v: any) => (envResult = v),
    setBranches: (v: any) => (branchResult = v),
    setHistory: (v: any) => (approvalResult = v),
    setDirty: () => (sourceDirty = true),
    setFresh: (p: string, d = p) => {
      planResult = p;
      detailResult = d;
    },
  };
}
async function prepared() {
  const x = fixture(),
    summary: any = await runPrivateIngestCI("capture", x.io);
  const base = "cf-sessions-prod.",
    from = "docs/plans/",
    to = ".96bulls/ci-capture/";
  const captures = new Map<string, string>();
  for (const [key, value] of x.files)
    if (key.startsWith(from)) captures.set(key.slice(from.length), value);
  for (const [key] of x.files)
    if (key.startsWith(from) || key.startsWith(".96bulls/"))
      x.files.delete(key);
  for (const [key, value] of captures) x.files.set(to + key, value);
  x.env.INGEST_CI_PHASE = "deploy";
  x.env.GITHUB_RUN_ID = "124";
  x.env.CAPTURE_RUN_ID = "123";
  x.env.CAPTURE_PROVENANCE_SHA256 = summary.capture.provenanceSha256;
  x.env.OPERATOR_APPROVAL_JSON = JSON.stringify({
    decision: "approved",
    by: "Khaliq Gant",
    date: new Date().toISOString().slice(0, 10),
    quote: "Approved this exact dark private-ingest capture.",
    capture: summary.capture,
  });
  x.commands.length = 0;
  x.github.length = 0;
  x.cf.length = 0;
  return {
    ...x,
    summary,
    captures,
    filesFor: () => ({
      provenanceText: x.files.get(to + base + "provenance.json")!,
      planText: x.files.get(to + base + "plan.txt")!,
      detailedText: x.files.get(to + base + "plan.detailed.txt")!,
      inputsText: x.files.get(to + base + "inputs.sanitized.json")!,
    }),
  };
}
test("absent/unprotected environment blocks before Alchemy or any Cloudflare call", async () => {
  for (const value of [
    undefined,
    { ...environment, protection_rules: [] },
    { ...environment, protection_rules: [{ type: "wait_timer" }] },
  ]) {
    const x = fixture();
    x.setEnvironment(value);
    await expect(runPrivateIngestCI("gate", x.io)).rejects.toThrow();
    expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
    expect(x.cf).toHaveLength(0);
  }
});
test.each([
  { GITHUB_REF: "refs/heads/feature" },
  { EXPECTED_SOURCE_SHA: "b".repeat(40) },
  { GITHUB_SHA: "b".repeat(40) },
  { GITHUB_EVENT_NAME: "push" },
  { GITHUB_RUN_ATTEMPT: "2" },
  { ACKNOWLEDGE_STATE_READY: "false" },
])(
  "main immutable dispatch/state prerequisite gate refuses %j before API or Alchemy",
  async (change) => {
    const x = fixture();
    Object.assign(x.env, change);
    await expect(runPrivateIngestCI("gate", x.io)).rejects.toThrow();
    expect(x.github).toHaveLength(0);
    expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
  },
);
test("dirty source refuses, including an uncommitted app change", async () => {
  const x = fixture();
  x.setDirty();
  await expect(runPrivateIngestCI("capture", x.io)).rejects.toMatchObject({
    code: "immutable_clean_source_required",
  });
  expect(x.github).toHaveLength(0);
});
test("a reviewer bot/team or non-main environment policy cannot release credentials", async () => {
  const context = assertIngestCiContext(fixture().env, source, false);
  for (const type of ["Bot", "Team"]) {
    const changed = structuredClone(environment);
    changed.protection_rules[0]!.reviewers[0]!.reviewer.type = type;
    expect(() => assertHumanGate(changed, branches, context)).toThrow();
  }
  expect(() =>
    assertHumanGate(
      environment,
      { ...branches, branch_policies: [{ name: "*", type: "branch" }] },
      context,
    ),
  ).toThrow();
  expect(() =>
    assertHumanGate(
      environment,
      { ...branches, branch_policies: [{ name: "main", type: "tag" }] },
      context,
    ),
  ).toThrow();
});
test("actual current run human approval is mandatory, not merely an environment name", async () => {
  const x = fixture();
  x.setHistory([]);
  await expect(runPrivateIngestCI("capture", x.io)).rejects.toMatchObject({
    code: "current_run_human_approval_required",
  });
  expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
  expect(x.files.size).toBe(1);
  const context = assertIngestCiContext(x.env, source, false),
    gate = assertHumanGate(environment, branches, context);
  expect(() =>
    assertCurrentHumanApproval(
      [{ ...history[0], state: "rejected" }],
      gate,
      context,
    ),
  ).toThrow();
  expect(() =>
    assertCurrentHumanApproval(
      [{ ...history[0], user: { ...history[0]!.user, type: "Bot" } }],
      gate,
      context,
    ),
  ).toThrow();
});
test("actual capture writes only bound sanitized inputs and two validated plans, never a secret", async () => {
  const x = fixture(),
    summary: any = await runPrivateIngestCI("capture", x.io);
  expect(
    x.commands.filter((c) => c.cloudflare).map((c) => c.args.slice(0, 2)),
  ).toEqual([
    ["node_modules/alchemy/bin/cli.js", "plan"],
    ["node_modules/alchemy/bin/cli.js", "plan"],
  ]);
  expect(summary.capture.sourceCommit).toBe(source);
  expect([...x.files.values()].join("")).not.toContain(
    x.env.CLOUDFLARE_API_TOKEN!,
  );
  const input = JSON.parse(
    x.files.get("docs/plans/cf-sessions-prod.inputs.sanitized.json")!,
  );
  expect(input.sessionIngest.replicaMode).toBe("off");
  expect(input.approval).toBeUndefined();
  expect(x.cf).toHaveLength(0);
});
test("capture refuses a credential-bearing detailed plan before artifact publication", async () => {
  const x = fixture();
  x.setFresh(plan, plan + "\n" + x.env.CLOUDFLARE_API_TOKEN);
  await expect(runPrivateIngestCI("capture", x.io)).rejects.toMatchObject({
    code: "unsafe_plan_output",
  });
  expect([...x.files.keys()].some((k) => k.startsWith("docs/plans"))).toBe(
    false,
  );
});
test("deploy uses the original capture, bound approval, fresh equal plans and exactly one existing Alchemy deploy", async () => {
  const x = await prepared();
  await runPrivateIngestCI("approved", x.io);
  expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
  const result = await runPrivateIngestCI("deploy", x.io);
  expect(x.commands.filter((c) => c.cloudflare).map((c) => c.args[1])).toEqual([
    "plan",
    "plan",
    "deploy",
  ]);
  expect(result.workersDevAndCustomDomainsVerified).toBe(true);
  expect(
    x.commands.filter((c) => c.cloudflare).every((c) => c.command === "node"),
  ).toBe(true);
  expect(x.cf[1]).toEndWith("/workers/domains?service=96bulls-session-ingest");
  const rendered = JSON.parse(
    x.files.get(".96bulls/cf-sessions-prod/cloudflare-inputs.json")!,
  );
  expect(rendered.approval.capture).toEqual(x.summary.capture);
  expect(x.commands.flatMap((c) => c.args)).not.toContain("wrangler");
});
test.each(["sha", "run", "workflow", "branch", "attempt", "conclusion"])(
  "a foreign capture %s never starts a fresh plan or deploy",
  async (mode) => {
    const x = await prepared();
    Object.assign(
      x.captureRun,
      mode === "sha"
        ? { head_sha: "b".repeat(40) }
        : mode === "run"
          ? { id: 999 }
          : mode === "workflow"
            ? { path: ".github/workflows/other.yml" }
            : mode === "branch"
              ? { head_branch: "feature" }
              : mode === "attempt"
                ? { run_attempt: 2 }
                : { conclusion: "failure" },
    );
    await expect(runPrivateIngestCI("approved", x.io)).rejects.toMatchObject({
      code: "capture_run_identity",
    });
    expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
  },
);
test.each(["provenance", "detailed", "inputs", "approval", "environment"])(
  "changed %s is refused before Alchemy",
  async (mode) => {
    const x = await prepared(),
      prefix = ".96bulls/ci-capture/cf-sessions-prod.";
    if (mode === "provenance")
      x.files.set(
        prefix + "provenance.json",
        x.files.get(prefix + "provenance.json") + " ",
      );
    if (mode === "detailed")
      x.files.set(prefix + "plan.detailed.txt", plan + "\nchanged");
    if (mode === "inputs") {
      const value = JSON.parse(x.files.get(prefix + "inputs.sanitized.json")!);
      value.sessionIngest.hyperdriveId = "e".repeat(32);
      x.files.set(prefix + "inputs.sanitized.json", JSON.stringify(value));
    }
    if (mode === "approval") {
      const value = JSON.parse(x.env.OPERATOR_APPROVAL_JSON!);
      value.capture.planSha256 = "b".repeat(64);
      x.env.OPERATOR_APPROVAL_JSON = JSON.stringify(value);
    }
    if (mode === "environment")
      x.env.SESSION_INGEST_HYPERDRIVE_ID = "e".repeat(32);
    await expect(runPrivateIngestCI("deploy", x.io)).rejects.toThrow();
    expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
  },
);
test("fresh live-plan drift stops before deploy even with a valid old approval", async () => {
  const x = await prepared();
  x.setFresh(plan.replaceAll("create", "update"));
  await expect(runPrivateIngestCI("deploy", x.io)).rejects.toMatchObject({
    code: "fresh_plan_drift",
  });
  expect(x.commands.filter((c) => c.cloudflare).map((c) => c.args[1])).toEqual([
    "plan",
    "plan",
  ]);
});
test("unsafe action set refuses before any deploy", async () => {
  const x = fixture();
  x.setFresh(
    plan.replace("[session-ingest] create", "[session-ingest] delete"),
  );
  await expect(runPrivateIngestCI("capture", x.io)).rejects.toMatchObject({
    code: "plan_contract",
  });
  expect([...x.files.keys()].some((k) => k.startsWith("docs/plans"))).toBe(
    false,
  );
});
test("byte-bounded API transport aborts without waiting for a stuck cancellation", async () => {
  const controller = new AbortController();
  let cancelled = false;
  const response = new Response(
    new ReadableStream({
      pull() {
        return new Promise(() => {});
      },
      cancel() {
        cancelled = true;
        return new Promise(() => {});
      },
    }),
  );
  const read = boundedCiJson(response, controller.signal);
  controller.abort();
  await expect(read).rejects.toMatchObject({ code: "api_deadline" });
  expect(cancelled).toBe(true);
  await expect(
    boundedCiJson(
      new Response("x".repeat(2 * 1024 * 1024 + 1)),
      new AbortController().signal,
    ),
  ).rejects.toMatchObject({ code: "api_body_bound" });
});
test("workflow never auto-creates the environment before protection checks and scopes CF credentials to Alchemy only", () => {
  const workflow: any = Bun.YAML.parse(
    readFileSync(
      new URL(
        "../../../.github/workflows/deploy-session-ingest-worker.yml",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  expect(workflow.on).toHaveProperty("workflow_dispatch");
  expect(Object.keys(workflow.on)).toEqual(["workflow_dispatch"]);
  expect(workflow.permissions).toEqual({ contents: "read", actions: "read" });
  expect(workflow.jobs.gate.environment).toBeUndefined();
  expect(workflow.jobs.release.environment).toBe(INGEST_CI_ENVIRONMENT);
  expect(workflow.jobs.release.needs).toBe("gate");
  for (const job of Object.values(workflow.jobs) as any[]) {
    expect(job.if).toBe("github.ref == 'refs/heads/main'");
    expect(job.env?.CLOUDFLARE_API_TOKEN).toBeUndefined();
    expect(
      job.steps.find((s: any) => s.uses === "actions/checkout@v4").with.ref,
    ).toBe("${{ github.sha }}");
  }
  const steps = workflow.jobs.release.steps,
    secret = steps.filter((s: any) => s.env?.CLOUDFLARE_API_TOKEN);
  expect(secret).toHaveLength(1);
  expect(secret[0].run).toBe(
    'bun scripts/private-ingest-ci.ts "$INGEST_CI_PHASE"',
  );
  expect(
    steps.findIndex(
      (s: any) => s.run === "bun scripts/private-ingest-ci.ts approved",
    ),
  ).toBeLessThan(steps.indexOf(secret[0]));
  expect(
    steps.find((s: any) => s.uses === "actions/download-artifact@v4").with[
      "run-id"
    ],
  ).toBe("${{ inputs.capture_run_id }}");
});

test.each(["reviewer", "environment", "node"])(
  "capture binds actual %s as well as source/plan/input digests",
  async (mode) => {
    const x = await prepared(),
      context = assertIngestCiContext(x.env, source, false),
      files = x.filesFor(),
      gate = { environmentId: 10, reviewerId: 7 },
      tools = { bun: "1.4.2", node: "v24.21.0", alchemy: "2.0.0-beta.84" };
    if (mode === "reviewer") gate.reviewerId = 999;
    if (mode === "environment") gate.environmentId = 999;
    if (mode === "node") tools.node = "v24.22.0";
    expect(() =>
      verifyIngestCiCapture(
        context,
        files,
        x.env.CAPTURE_PROVENANCE_SHA256!,
        "123",
        x.env.OPERATOR_APPROVAL_JSON!,
        { gate, tools },
      ),
    ).toThrow("capture_provenance");
  },
);

test("an earlier approval cannot hide a later rejection or ambiguous second review", async () => {
  const x = fixture();
  x.setHistory([...history, { ...history[0], state: "rejected" }]);
  await expect(runPrivateIngestCI("capture", x.io)).rejects.toMatchObject({
    code: "current_run_human_approval_required",
  });
  expect(x.commands.filter((c) => c.cloudflare)).toHaveLength(0);
});
