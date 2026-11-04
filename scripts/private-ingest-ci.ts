/** Main-only, human-gated CI adapter for the existing guarded Alchemy stage.
 * No credential is written to capture files or inherited by installation.
 * Importing this module never calls an API, Alchemy, or a deployment.
 */
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import {
  APPROVER,
  APPROVAL_MAX_AGE_DAYS,
  assertApprovalBinding,
  inputsDigest,
  parseCalendarDate,
} from "../stacks/aws/deploy-guard.ts";
import {
  assertCloudflareStage,
  type CloudflareStageInputs,
} from "../stacks/cloudflare/alchemy.cloudflare.run.ts";
import {
  assertSessionIngestPlanContract,
  assertPrivateWorker,
  assertNoSecretLiterals,
  sessionIngestWorkerProps,
  SESSION_INGEST_STAGE,
  SESSION_INGEST_WORKER,
  FANOUT_QUEUE,
} from "../stacks/cloudflare/session-ingest.ts";
import { sanitizePlan } from "../src/cli/plan-provenance.ts";
export const INGEST_CI_ENVIRONMENT = "session-ingest-worker";
export const INGEST_CI_WORKFLOW =
  ".github/workflows/deploy-session-ingest-worker.yml";
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const obj = (v: unknown): v is Record<string, any> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const sha = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{64}$/u.test(v);
const commit = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{40}$/u.test(v);
const positive = (v: unknown): v is number =>
  typeof v === "number" && Number.isSafeInteger(v) && v > 0;
export class IngestCiRefusal extends Error {
  constructor(readonly code: string) {
    super("private ingest CI refused: " + code);
  }
}
function refuse(code: string): never {
  throw new IngestCiRefusal(code);
}
export type IngestCiPhase = "capture" | "deploy";
type Env = Record<string, string | undefined>;
export interface IngestCiContext {
  phase: IngestCiPhase;
  sourceSha: string;
  runId: string;
  repository: string;
  approverLogin: string;
}
export function assertIngestCiContext(
  env: Env,
  actualSha: string,
  dirty: boolean,
): IngestCiContext {
  if (
    env.GITHUB_ACTIONS !== "true" ||
    env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    env.GITHUB_REF !== "refs/heads/main"
  )
    refuse("main_dispatch_required");
  if (
    !commit(env.EXPECTED_SOURCE_SHA) ||
    env.EXPECTED_SOURCE_SHA !== env.GITHUB_SHA ||
    actualSha !== env.EXPECTED_SOURCE_SHA ||
    dirty
  )
    refuse("immutable_clean_source_required");
  if (env.GITHUB_RUN_ATTEMPT !== "1") refuse("new_dispatch_required");
  if (
    !/^[1-9][0-9]{0,19}$/u.test(env.GITHUB_RUN_ID ?? "") ||
    env.GITHUB_REPOSITORY !== "AgentWorkforce/sessions-pipeline"
  )
    refuse("workflow_identity");
  if (
    !/^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,38})$/u.test(
      env.SESSION_INGEST_APPROVER_LOGIN ?? "",
    ) ||
    env.SESSION_INGEST_APPROVER_LOGIN?.endsWith("[bot]")
  )
    refuse("human_approver_configuration");
  if (env.INGEST_CI_PHASE !== "capture" && env.INGEST_CI_PHASE !== "deploy")
    refuse("phase");
  if (
    env.INGEST_CI_PHASE === "capture" &&
    env.ACKNOWLEDGE_STATE_READY !== "true"
  )
    refuse("state_prerequisite_acknowledgement");
  if (
    env.INGEST_CI_PHASE === "deploy" &&
    (!/^[1-9][0-9]{0,19}$/u.test(env.CAPTURE_RUN_ID ?? "") ||
      env.CAPTURE_RUN_ID === env.GITHUB_RUN_ID ||
      !sha(env.CAPTURE_PROVENANCE_SHA256) ||
      !env.OPERATOR_APPROVAL_JSON?.trim() ||
      env.OPERATOR_APPROVAL_JSON.length > 16384)
  )
    refuse("capture_dispatch_inputs");
  return {
    phase: env.INGEST_CI_PHASE,
    sourceSha: actualSha,
    runId: env.GITHUB_RUN_ID!,
    repository: env.GITHUB_REPOSITORY!,
    approverLogin: env.SESSION_INGEST_APPROVER_LOGIN!,
  };
}
/** An absent environment must fail here, before any job references/auto-creates it. */
export function assertHumanGate(
  environment: unknown,
  branches: unknown,
  context: IngestCiContext,
): { environmentId: number; reviewerId: number } {
  if (
    !obj(environment) ||
    environment.name !== INGEST_CI_ENVIRONMENT ||
    !positive(environment.id)
  )
    refuse("protected_environment_missing");
  const rules = environment.protection_rules;
  if (!Array.isArray(rules)) refuse("human_review_rule_missing");
  const required = rules.filter(
    (r: any) => obj(r) && r.type === "required_reviewers",
  );
  if (
    required.length !== 1 ||
    !Array.isArray(required[0].reviewers) ||
    required[0].reviewers.length !== 1
  )
    refuse("human_review_rule_missing");
  const reviewer = required[0].reviewers[0];
  if (
    !obj(reviewer) ||
    reviewer.type !== "User" ||
    !obj(reviewer.reviewer) ||
    reviewer.reviewer.type !== "User" ||
    reviewer.reviewer.login !== context.approverLogin ||
    !positive(reviewer.reviewer.id)
  )
    refuse("human_review_rule_missing");
  if (
    environment.deployment_branch_policy?.custom_branch_policies !== true ||
    environment.deployment_branch_policy?.protected_branches !== false ||
    !obj(branches) ||
    !Array.isArray(branches.branch_policies) ||
    branches.total_count !== 1 ||
    branches.branch_policies.length !== 1 ||
    branches.branch_policies[0].name !== "main" ||
    branches.branch_policies[0].type !== "branch"
  )
    refuse("main_environment_policy_required");
  return { environmentId: environment.id, reviewerId: reviewer.reviewer.id };
}
export function assertCurrentHumanApproval(
  history: unknown,
  gate: { environmentId: number; reviewerId: number },
  context: IngestCiContext,
): void {
  if (!Array.isArray(history)) refuse("current_run_human_approval_required");
  // One protected job/first attempt must have one unambiguous decision. An old
  // approval cannot mask a later rejection or another actor's bypass.
  const decisions = history.filter(
    (r: any) =>
      obj(r) &&
      Array.isArray(r.environments) &&
      r.environments.some(
        (e: any) =>
          e.id === gate.environmentId && e.name === INGEST_CI_ENVIRONMENT,
      ),
  );
  if (decisions.length !== 1) refuse("current_run_human_approval_required");
  const decision = decisions[0];
  if (
    decision.state !== "approved" ||
    !obj(decision.user) ||
    decision.user.type !== "User" ||
    decision.user.id !== gate.reviewerId ||
    decision.user.login !== context.approverLogin
  )
    refuse("current_run_human_approval_required");
}
export function assertCaptureRun(
  run: unknown,
  context: IngestCiContext,
  captureRunId: string,
): void {
  if (
    !/^[1-9][0-9]{0,19}$/u.test(captureRunId) ||
    captureRunId === context.runId ||
    !obj(run) ||
    String(run.id) !== captureRunId ||
    run.head_sha !== context.sourceSha ||
    run.head_branch !== "main" ||
    run.run_attempt !== 1 ||
    run.event !== "workflow_dispatch" ||
    run.status !== "completed" ||
    run.conclusion !== "success" ||
    ![INGEST_CI_WORKFLOW, INGEST_CI_WORKFLOW + "@refs/heads/main"].includes(
      run.path,
    ) ||
    run.repository?.full_name !== context.repository
  )
    refuse("capture_run_identity");
}
export function ingestCiInputs(env: Env): CloudflareStageInputs {
  if (
    [
      "SESSION_INGEST_ACCOUNT_ID",
      "SESSION_INGEST_HYPERDRIVE_ID",
      "SESSION_INGEST_SECRETS_STORE_ID",
    ].some((key) => /^0{32}$/u.test(env[key] ?? ""))
  )
    refuse("stage_inputs");
  const inputs = {
    sessionIngest: {
      accountId: env.SESSION_INGEST_ACCOUNT_ID,
      hyperdriveId: env.SESSION_INGEST_HYPERDRIVE_ID,
      secretsStoreId: env.SESSION_INGEST_SECRETS_STORE_ID,
      kmsKeyArn: env.SESSION_INGEST_KMS_KEY_ARN,
      replicaMode: "off",
    },
  };
  try {
    assertCloudflareStage(SESSION_INGEST_STAGE, "plan", inputs, {
      CLOUDFLARE_ACCOUNT_ID: env.SESSION_INGEST_ACCOUNT_ID,
    });
  } catch {
    refuse("stage_inputs");
  }
  const checked = inputs as CloudflareStageInputs;
  // Reuse the exact declarations/invariants; no alternate Wrangler resource path.
  const props = sessionIngestWorkerProps(checked.sessionIngest, {
    HYPERDRIVE: { id: checked.sessionIngest.hyperdriveId },
    INGEST_FANOUT: { queueName: FANOUT_QUEUE },
  });
  assertPrivateWorker(props as unknown as Record<string, unknown>, () =>
    refuse("private_worker"),
  );
  assertNoSecretLiterals(props.env, () => refuse("literal_credentials"));
  return checked;
}
export interface IngestCiCapture {
  schema: 1;
  kind: "session-ingest-ci-plan";
  phase: "capture";
  stage: string;
  generatedAt: string;
  source: { commit: string; clean: true };
  target: { accountId: string };
  inputs: { sha256: string };
  ci: {
    repository: string;
    workflow: string;
    runId: string;
    runAttempt: 1;
    environmentId: number;
    reviewerId: number;
  };
  outputs: { plan: { sha256: string }; detailed: { sha256: string } };
  tools: { bun: string; node: string; alchemy: string };
}
export function safePlan(
  text: string,
  secretValues: readonly string[] = [],
): string {
  // The established sanitizer removes ephemeral logger timestamps, not plan data.
  const sanitized =
    sanitizePlan(text.replace(/\x1b\[[0-9;]*m/gu, ""), []).trim() + "\n";
  if (
    !sanitized.trim() ||
    Buffer.byteLength(sanitized, "utf8") > 16 * 1024 * 1024 ||
    /secret_text|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY|postgres(?:ql)?:\/\/[^\s"]*:[^\s"@]*@/u.test(
      sanitized,
    ) ||
    secretValues.some((v) => v.length >= 8 && sanitized.includes(v))
  )
    refuse("unsafe_plan_output");
  return sanitized;
}
export function makeIngestCiCapture(
  context: IngestCiContext,
  gate: { environmentId: number; reviewerId: number },
  inputsText: string,
  planText: string,
  detailedText: string,
  tools: { bun: string; node: string; alchemy: string },
  now = new Date(),
): IngestCiCapture {
  try {
    assertSessionIngestPlanContract(planText, detailedText);
  } catch {
    refuse("plan_contract");
  }
  const inputs = JSON.parse(inputsText) as CloudflareStageInputs;
  return {
    schema: 1,
    kind: "session-ingest-ci-plan",
    phase: "capture",
    stage: SESSION_INGEST_STAGE,
    generatedAt: now.toISOString(),
    source: { commit: context.sourceSha, clean: true },
    target: { accountId: inputs.sessionIngest.accountId },
    inputs: { sha256: inputsDigest(inputsText) },
    ci: {
      repository: context.repository,
      workflow: INGEST_CI_WORKFLOW,
      runId: context.runId,
      runAttempt: 1,
      ...gate,
    },
    outputs: {
      plan: { sha256: hash(planText) },
      detailed: { sha256: hash(detailedText) },
    },
    tools,
  };
}
export function verifyIngestCiCapture(
  context: IngestCiContext,
  files: {
    provenanceText: string;
    planText: string;
    detailedText: string;
    inputsText: string;
  },
  expectedDigest: string,
  captureRunId: string,
  approvalText: string,
  observed: {
    gate: { environmentId: number; reviewerId: number };
    tools: { bun: string; node: string; alchemy: string };
  },
  now = new Date(),
): CloudflareStageInputs {
  if (!sha(expectedDigest) || hash(files.provenanceText) !== expectedDigest)
    refuse("capture_digest");
  let capture: any, approval: any, inputs: any;
  try {
    capture = JSON.parse(files.provenanceText);
    approval = JSON.parse(approvalText);
    inputs = JSON.parse(files.inputsText);
  } catch {
    refuse("capture_json");
  }
  if (
    !obj(capture) ||
    capture.schema !== 1 ||
    capture.kind !== "session-ingest-ci-plan" ||
    capture.phase !== "capture" ||
    capture.stage !== SESSION_INGEST_STAGE ||
    capture.source?.commit !== context.sourceSha ||
    capture.source?.clean !== true ||
    capture.ci?.repository !== context.repository ||
    capture.ci?.workflow !== INGEST_CI_WORKFLOW ||
    capture.ci?.runId !== captureRunId ||
    capture.ci?.runAttempt !== 1 ||
    capture.ci?.environmentId !== observed.gate.environmentId ||
    capture.ci?.reviewerId !== observed.gate.reviewerId ||
    capture.tools?.bun !== observed.tools.bun ||
    capture.tools?.node !== observed.tools.node ||
    capture.tools?.alchemy !== observed.tools.alchemy ||
    capture.target?.accountId !== inputs.sessionIngest?.accountId ||
    capture.inputs?.sha256 !== inputsDigest(files.inputsText) ||
    capture.outputs?.plan?.sha256 !== hash(files.planText) ||
    capture.outputs?.detailed?.sha256 !== hash(files.detailedText)
  )
    refuse("capture_provenance");
  const day = parseCalendarDate(approval?.date),
    today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  if (
    !obj(approval) ||
    approval.decision !== "approved" ||
    approval.by !== APPROVER ||
    typeof approval.quote !== "string" ||
    approval.quote.trim().length < 8 ||
    !day ||
    day.getTime() > today ||
    today - day.getTime() > APPROVAL_MAX_AGE_DAYS * 86400000
  )
    refuse("bound_operator_approval");
  inputs.approval = approval;
  const inputsText = JSON.stringify(inputs);
  try {
    assertCloudflareStage(SESSION_INGEST_STAGE, "deploy", inputs, {
      CLOUDFLARE_ACCOUNT_ID: inputs.sessionIngest.accountId,
    });
    assertApprovalBinding(approval.capture, { ...files, inputsText });
    assertSessionIngestPlanContract(files.planText, files.detailedText);
  } catch {
    refuse("bound_capture_contract");
  }
  return inputs;
}
export function assertFreshIngestPlan(
  reviewed: { planText: string; detailedText: string },
  fresh: { planText: string; detailedText: string },
): void {
  try {
    assertSessionIngestPlanContract(fresh.planText, fresh.detailedText);
  } catch {
    refuse("fresh_plan_contract");
  }
  if (
    hash(reviewed.planText) !== hash(fresh.planText) ||
    hash(reviewed.detailedText) !== hash(fresh.detailedText)
  )
    refuse("fresh_plan_drift");
}
export interface IngestCiIO {
  env: Env;
  read(path: string): string;
  write(path: string, text: string): void;
  run(command: string, args: string[], cloudflare?: boolean): string;
  github(path: string): Promise<unknown>;
  cloudflare(path: string): Promise<unknown>;
}
const paths = (directory: string) => ({
  provenance: join(directory, SESSION_INGEST_STAGE + ".provenance.json"),
  plan: join(directory, SESSION_INGEST_STAGE + ".plan.txt"),
  detailed: join(directory, SESSION_INGEST_STAGE + ".plan.detailed.txt"),
  inputs: join(directory, SESSION_INGEST_STAGE + ".inputs.sanitized.json"),
});
export async function runPrivateIngestCI(
  command: "gate" | "approved" | "capture" | "deploy",
  io: IngestCiIO,
): Promise<Record<string, unknown>> {
  const context = assertIngestCiContext(
    io.env,
    io.run("git", ["rev-parse", "HEAD"]).trim(),
    io.run("git", ["status", "--porcelain", "--untracked-files=all"]).trim() !==
      "",
  );
  const base = "repos/" + context.repository,
    endpoint = base + "/environments/" + INGEST_CI_ENVIRONMENT;
  const environment = await io.github(endpoint),
    branches = await io.github(endpoint + "/deployment-branch-policies");
  const gate = assertHumanGate(environment, branches, context);
  if (command === "gate")
    return {
      gate: "protected",
      sourceSha: context.sourceSha,
      environment: INGEST_CI_ENVIRONMENT,
    };
  if (command !== "approved" && command !== context.phase)
    refuse("phase_mismatch");
  const history = await io.github(
    base + "/actions/runs/" + context.runId + "/approvals",
  );
  assertCurrentHumanApproval(history, gate, context);
  const inputs = ingestCiInputs(io.env);
  if (command === "approved") {
    if (context.phase === "deploy") {
      const id = io.env.CAPTURE_RUN_ID!;
      assertCaptureRun(
        await io.github(base + "/actions/runs/" + id),
        context,
        id,
      );
    }
    return {
      gate: "human-approved",
      sourceSha: context.sourceSha,
      environment: INGEST_CI_ENVIRONMENT,
    };
  }
  const inputsPath = join(
    ".96bulls",
    SESSION_INGEST_STAGE,
    "cloudflare-inputs.json",
  );
  const plans = paths(join("docs", "plans")),
    artifact = paths(join(".96bulls", "ci-capture"));
  const child = (mode: "plan" | "deploy", detailed = false) => {
    const output = io.run(
      "node",
      [
        "node_modules/alchemy/bin/cli.js",
        mode,
        "--config",
        "stacks/cloudflare/alchemy.cloudflare.run.ts",
        "--stage",
        SESSION_INGEST_STAGE,
        ...(detailed ? ["--detailed"] : []),
      ],
      true,
    );
    return mode === "plan"
      ? safePlan(output, [
          io.env.CLOUDFLARE_API_TOKEN ?? "",
          io.env.GITHUB_TOKEN ?? "",
        ])
      : "";
  };
  const plan = () => ({
    planText: child("plan"),
    detailedText: child("plan", true),
  });
  if (command === "capture") {
    if (io.env.OPERATOR_APPROVAL_JSON?.trim()) refuse("capture_has_approval");
    const inputText = JSON.stringify(inputs, null, 2) + "\n";
    io.write(inputsPath, inputText);
    const fresh = plan(),
      capture = makeIngestCiCapture(
        context,
        gate,
        inputText,
        fresh.planText,
        fresh.detailedText,
        {
          bun: io.run("bun", ["--version"]).trim(),
          node: io.run("node", ["--version"]).trim(),
          alchemy: JSON.parse(io.read("node_modules/alchemy/package.json"))
            .version,
        },
      );
    const provenanceText = JSON.stringify(capture, null, 2) + "\n";
    io.write(plans.provenance, provenanceText);
    io.write(plans.plan, fresh.planText);
    io.write(plans.detailed, fresh.detailedText);
    io.write(plans.inputs, inputText);
    return {
      phase: "capture",
      sourceSha: context.sourceSha,
      captureRunId: context.runId,
      capture: {
        provenanceSha256: hash(provenanceText),
        planSha256: hash(fresh.planText),
        inputsSha256: inputsDigest(inputText),
        sourceCommit: context.sourceSha,
      },
    };
  }
  const captureRunId = io.env.CAPTURE_RUN_ID ?? "";
  assertCaptureRun(
    await io.github(base + "/actions/runs/" + captureRunId),
    context,
    captureRunId,
  );
  const files = {
    provenanceText: io.read(artifact.provenance),
    planText: io.read(artifact.plan),
    detailedText: io.read(artifact.detailed),
    inputsText: io.read(artifact.inputs),
  };
  const approved = verifyIngestCiCapture(
    context,
    files,
    io.env.CAPTURE_PROVENANCE_SHA256 ?? "",
    captureRunId,
    io.env.OPERATOR_APPROVAL_JSON ?? "",
    {
      gate,
      tools: {
        bun: io.run("bun", ["--version"]).trim(),
        node: io.run("node", ["--version"]).trim(),
        alchemy: JSON.parse(io.read("node_modules/alchemy/package.json"))
          .version,
      },
    },
  );
  if (
    inputsDigest(JSON.stringify(inputs)) !==
    inputsDigest(JSON.stringify(approved))
  )
    refuse("environment_inputs_drift");
  io.write(inputsPath, JSON.stringify(approved, null, 2) + "\n");
  // The existing stack consumes these exact reviewed files and approval again.
  io.write(plans.provenance, files.provenanceText);
  io.write(plans.plan, files.planText);
  io.write(plans.detailed, files.detailedText);
  const fresh = plan();
  assertFreshIngestPlan(files, fresh);
  child("deploy"); // no alternate deploy command, destroy, configuration patch or binding mutation.
  const account = approved.sessionIngest.accountId,
    script = SESSION_INGEST_WORKER;
  const surface: any = await io.cloudflare(
    "accounts/" + account + "/workers/scripts/" + script + "/subdomain",
  );
  const domains: any = await io.cloudflare(
    "accounts/" +
      account +
      "/workers/domains?service=" +
      encodeURIComponent(script),
  );
  if (
    surface?.enabled !== false ||
    surface?.previews_enabled !== false ||
    !Array.isArray(domains) ||
    domains.length !== 0
  )
    refuse("live_private_surface");
  return {
    phase: "deploy",
    sourceSha: context.sourceSha,
    captureRunId,
    worker: script,
    workersDevAndCustomDomainsVerified: true,
  };
}
export async function boundedCiJson(
  response: Response,
  signal: AbortSignal,
): Promise<any> {
  const reader = response.body?.getReader();
  if (!reader) refuse("api_body");
  let onAbort: () => void = () => {};
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => {
      reject(new IngestCiRefusal("api_deadline"));
      void reader!.cancel().catch(() => {});
    };
  });
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  const parts: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const part = await Promise.race([reader.read(), aborted]);
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > 2 * 1024 * 1024) refuse("api_body_bound");
      parts.push(part.value);
    }
    if (signal.aborted) refuse("api_deadline");
    const all = new Uint8Array(bytes);
    let at = 0;
    for (const part of parts) {
      all.set(part, at);
      at += part.length;
    }
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(all));
  } finally {
    signal.removeEventListener("abort", onAbort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
function realIO(): IngestCiIO {
  const env = process.env;
  const api = async (
    url: string,
    token: string | undefined,
    cloudflare = false,
  ) => {
    if (!token) refuse("missing_ci_token");
    const signal = AbortSignal.timeout(20000);
    const response = await fetch(url, {
      headers: {
        authorization: "Bearer " + token,
        accept: "application/json",
        ...(!cloudflare ? { "X-GitHub-Api-Version": "2022-11-28" } : {}),
      },
      signal,
    });
    if (!response.ok) {
      void response.body?.cancel().catch(() => {});
      refuse(cloudflare ? "cloudflare_readback" : "github_readback");
    }
    const body = await boundedCiJson(response, signal);
    if (cloudflare) {
      if (body.success !== true) refuse("cloudflare_readback");
      return body.result;
    }
    return body;
  };
  return {
    env,
    read: (path) => readFileSync(resolve(path), "utf8"),
    write: (path, text) => {
      mkdirSync(resolve(path, ".."), { recursive: true });
      writeFileSync(resolve(path), text, { mode: 0o600 });
    },
    run: (command, args, cloudflare = false) => {
      if (
        cloudflare &&
        (!env.CLOUDFLARE_API_TOKEN ||
          env.CLOUDFLARE_ACCOUNT_ID !== env.SESSION_INGEST_ACCOUNT_ID)
      )
        refuse("cloudflare_credential_target");
      const childEnv = {
        PATH: env.PATH,
        HOME: env.HOME,
        NO_COLOR: "1",
        FORCE_COLOR: "0",
        CI: "true",
        ...(cloudflare
          ? {
              CLOUDFLARE_API_TOKEN: env.CLOUDFLARE_API_TOKEN,
              CLOUDFLARE_ACCOUNT_ID: env.CLOUDFLARE_ACCOUNT_ID,
            }
          : {}),
      };
      const result = spawnSync(command, args, {
        encoding: "utf8",
        env: childEnv,
        maxBuffer: 32 * 1024 * 1024,
      });
      if (result.status !== 0)
        refuse(cloudflare ? "alchemy_command_failed" : "local_command_failed");
      return result.stdout + result.stderr;
    },
    github: (path) => api("https://api.github.com/" + path, env.GITHUB_TOKEN),
    cloudflare: (path) =>
      api(
        "https://api.cloudflare.com/client/v4/" + path,
        env.CLOUDFLARE_API_TOKEN,
        true,
      ),
  };
}
if (import.meta.main) {
  const command = process.argv[2];
  if (
    command !== "gate" &&
    command !== "approved" &&
    command !== "capture" &&
    command !== "deploy"
  ) {
    console.error("private ingest CI refused: command");
    process.exit(1);
  }
  runPrivateIngestCI(command, realIO())
    .then((summary) => console.log(JSON.stringify(summary)))
    .catch((error) => {
      console.error(
        "private ingest CI refused: " +
          (error instanceof IngestCiRefusal ? error.code : "internal"),
      );
      process.exitCode = 1;
    });
}
