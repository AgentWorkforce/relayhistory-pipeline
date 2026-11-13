/**
 * layered-sessions-pipeline: the host this folder defines, its user data, the
 * deploy workflows' shape, and the first deploy (no stack yet, so "full"),
 * through the module's own decide() and its CLI. No cloud calls: the CLI test
 * points the AWS SDK at a local fake CloudFormation.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { AGENT_CARD_FILE, EC2_USER_DATA_ENCODED_LIMIT, RELAY_SESSIONS_KIT_FILES, WORKER_KIT_FILES, decide, fileReader, hostBoundaryPolicy, hostRolePolicy, hostRoleProps, hostTemplate, networkParams, renderAgentCard, userDataBase64 } from "@agentworkforce/layered-agent";
import agent from "../agent.config.ts";

const AGENT_DIR = fileURLToPath(new URL("..", import.meta.url));
const read = fileReader(AGENT_DIR);
const source = (name: string) => readFileSync(new URL(`../${name}`, import.meta.url), "utf8");
const repoFile = (name: string) => readFileSync(new URL(`../../${name}`, import.meta.url), "utf8").replace(/\r\n/g, "\n");
/** Headroom under EC2's 25,600-byte cap, so the kit and prompt can grow. */
// EC2 caps user data at 25,600 bytes. Keep at least 2,000 of headroom, the
// same rule the layered-agents module tests (relay workers add ~8 KB).
const USER_DATA_BUDGET = 23_600;

test("names: the module's defaults for a new agent, except the platform prefix (pinned in layered-agents infra too)", () => {
  assert.deepEqual(agent.names, {
    computeStack: "layered-agent-sessions-pipeline-dev",
    role: "layered-agent-sessions-pipeline-dev",
    sessionDocument: "layered-agent-sessions-pipeline-dev-session",
    sessionLogGroup: "/layered-agents/dev/sessions-pipeline/ssm-sessions",
    telemetryEventsLogGroup: "/layered-agents/dev/sessions-pipeline/claude-code-events",
    telemetryMetricsLogGroup: "/layered-agents/dev/sessions-pipeline/claude-code-metrics",
    telemetryNamespace: "LayeredAgents/ClaudeCode",
    paramPrefix: "/sessions-pipeline/dev",
    platformPrefix: "/relaycast-agent-host/dev",
  });
  assert.deepEqual(agent.tags, { "layered-agent:name": "sessions-pipeline", "layered-agent:stage": "dev", "layered-agent:repo": "AgentWorkforce/sessions-pipeline" });
  const props = hostTemplate(agent, "X").Resources.AgentHost.Properties;
  assert.equal(props.InstanceType, "m7i.xlarge");
  assert.equal(props.BlockDeviceMappings[0]?.Ebs.VolumeSize, 100);
  assert.match(String(hostTemplate(agent, "X").Parameters.AmiId.Default), /\/amd64\//);
});

test("network: the existing agent-hosts VPC ids relaycast-cloud's agent-host stack publishes", () => {
  assert.deepEqual(networkParams(agent.names.platformPrefix), {
    subnetId: "/relaycast-agent-host/dev/network/private-subnet-id",
    securityGroupId: "/relaycast-agent-host/dev/network/host-security-group-id",
  });
});

test("secrets: the host reads exactly the kit's two shared parameters and its sessions MCP token, and writes none", () => {
  assert.deepEqual(agent.secrets.map((s) => s.param), [
    "/relaycast-agent-host/dev/anthropic-api-key",
    "/relaycast-agent-host/dev/nango-secret-key",
    "/relaycast-agent-host/dev/relay-sessions-token-sessions-pipeline",
  ]);
  const policy = hostRolePolicy(agent, "500268966557");
  const ssm = policy.Statement.filter((s) => s.Action.some((a) => a.startsWith("ssm:")));
  assert.deepEqual(ssm, [{
    Sid: "ReadHostSecrets",
    Effect: "Allow",
    Action: ["ssm:GetParameter"],
    Resource: [
      "arn:aws:ssm:us-west-2:500268966557:parameter/relaycast-agent-host/dev/anthropic-api-key",
      "arn:aws:ssm:us-west-2:500268966557:parameter/relaycast-agent-host/dev/nango-secret-key",
      "arn:aws:ssm:us-west-2:500268966557:parameter/relaycast-agent-host/dev/relay-sessions-token-sessions-pipeline",
    ],
  }]);
  // The token is read by name (load-secrets.sh -> kit/relay-sessions.sh -> ~/.claude.json), never exported to secrets.env.
  assert.match(agent.envFile, /^EXPORT_SECRETS=$/m);
  assert.match(agent.envFile, /^RELAY_SESSIONS_PARAM=\/relaycast-agent-host\/dev\/relay-sessions-token-sessions-pipeline$/m);
  // The boundary allows reading it too (built the same way in layered-agents infra/agents.ts).
  const boundaryReads = hostBoundaryPolicy(agent, "500268966557").Statement.filter((s) => s.Action.includes("ssm:GetParameter")).flatMap((s) => [s.Resource].flat());
  assert.ok(boundaryReads.includes("arn:aws:ssm:us-west-2:500268966557:parameter/relaycast-agent-host/dev/relay-sessions-token-sessions-pipeline"));
});

test("the host role carries the platform's permissions boundary (the deploy role may write it only under that boundary)", () => {
  const props = hostRoleProps(agent, "500268966557");
  assert.equal(props.permissionsBoundary, "arn:aws:iam::500268966557:policy/layered-agent-host-boundary-sessions-pipeline-dev");
  for (const s of hostBoundaryPolicy(agent, "500268966557").Statement) {
    for (const a of s.Action) assert.doesNotMatch(a, /^(iam|sts):|\*/, `${s.Sid}: ${a}`);
  }
});

test(`user data: under ${USER_DATA_BUDGET} bytes encoded (EC2 caps it at ${EC2_USER_DATA_ENCODED_LIMIT}), every kit and lane file, no secret values`, () => {
  const encoded = userDataBase64(agent, read);
  console.log(`user data: ${encoded.length} bytes base64 (limit ${EC2_USER_DATA_ENCODED_LIMIT}, budget ${USER_DATA_BUDGET})`);
  assert.ok(encoded.length < USER_DATA_BUDGET, `user data is ${encoded.length} bytes encoded`);
  const script = gunzipSync(Buffer.from(encoded, "base64")).toString("utf8");
  // The optional kit files (headless supervisor, delegation, workers, relay
  // sessions) are not in user data; they reach the host over SSM in the hot
  // update after first boot (kit 0.1.10; headless.py and delegation.md since
  // 0.1.12). The kit's OPTIONAL_KIT_FILES, which its own test/host.test.ts and
  // test/fresh-host.test.ts use, is not exported, so those two are listed here.
  const optional: readonly string[] = ["headless.py", "delegation.md", ...WORKER_KIT_FILES, ...RELAY_SESSIONS_KIT_FILES];
  // The generated card file (module 0.1.17) travels by hot update, never in user data.
  for (const f of agent.files) if (f.source !== "generated" && !optional.includes(f.name)) assert.ok(script.includes(`cat > /opt/agent-host/${f.name} <<'`), f.name);
  for (const f of optional) assert.ok(!script.includes(`/opt/agent-host/${f} <<'`), `${f} is not in user data`);
  assert.doesNotMatch(script, /sk-ant-|ghp_|ghs_|gho_|github_pat_|BEGIN [A-Z ]*PRIVATE KEY/);
});

test("the A2A card renders within the relay's limits, one skill per kind of work, and is generated, never in user data", () => {
  assert.ok(agent.input.relaySessions, "the lead publishes the card with the agent-relay MCP");
  const rendered = renderAgentCard(agent.input.card!);
  const card = JSON.parse(rendered) as { description: string; skills: { id: string; tags?: string[]; examples?: string[] }[]; documentation_url?: string; version: string };
  assert.match(card.description, /^layered-sessions-pipeline coordinates work on the Agent Relay sessions pipeline/);
  assert.deepEqual(card.skills.map((s) => s.id), ["track-work", "check-overlaps", "review-and-land", "consolidate-prs", "implement-change"]);
  for (const s of card.skills) {
    assert.ok((s.tags?.length ?? 0) > 0, `${s.id} has tags`);
    assert.ok((s.examples?.length ?? 0) >= 2 && (s.examples?.length ?? 0) <= 4, `${s.id} has 2 to 4 examples`);
  }
  assert.equal(card.documentation_url, "https://github.com/AgentWorkforce/relayhistory-pipeline");
  assert.match(card.version, /^[0-9a-f]{12}$/);
  assert.equal(agent.generated[AGENT_CARD_FILE], rendered);
  assert.deepEqual(agent.files.filter((f) => f.source === "generated"), [{ name: AGENT_CARD_FILE, source: "generated" }]);
  const script = gunzipSync(Buffer.from(userDataBase64(agent, read), "base64")).toString("utf8");
  assert.ok(!script.includes(`/opt/agent-host/${AGENT_CARD_FILE}`), "the card never rides in user data");
});

test("the lead runs on Fable: the rendered lead unit starts the kit's lead-run.sh, which runs the kit's headless supervisor with --model claude-fable-5-1", () => {
  // The model is the kit's (lead-run.sh, which the lead unit starts), not a
  // per-agent setting; implementation goes to sonnet or opus subagents per
  // the routing block. Since kit 0.1.12 the unit runs lead-run.sh in the
  // foreground (no tmux), and lead-run.sh execs the kit's headless.py
  // supervisor, which the unit requires on disk (it arrives over SSM).
  const script = gunzipSync(Buffer.from(userDataBase64(agent, read), "base64")).toString("utf8");
  const launch = script.split("\n").filter((l) => l.startsWith("ExecStart=/opt/agent-host/lead-run.sh "));
  assert.equal(launch.length, 1);
  assert.equal(launch[0], "ExecStart=/opt/agent-host/lead-run.sh /opt/agent-host/lead-prompt.md");
  assert.doesNotMatch(script, /ExecStart=.*tmux|ExecStop=.*tmux/);
  assert.match(script, /ConditionPathExists=\/opt\/agent-host\/headless.py/);
  assert.match(script, /--harness claude --model claude-fable-5-1 --prompt-file "\$state\/system.md"/);
  // Every hard-coded model id is Fable. (The worker launcher passes its model
  // through "$model", which defaults to the lead's Fable; it is not a literal.)
  assert.deepEqual([...new Set([...script.matchAll(/claude --model ([a-z0-9][\w.-]*)/g)].map((m) => m[1]))], ["claude-fable-5-1"]);
});

test("the env file sources cleanly in bash, with both repos and the relay identity", () => {
  const out = execFileSync("bash", ["-c", `set -eu; ${agent.envFile}\nprintf '%s|%s|%s' "$AGENT_REPOS" "$RELAY_NAME" "$RELAY_DESCRIPTION"`]).toString();
  assert.equal(out, "AgentWorkforce/sessions-pipeline AgentWorkforce/sessions-pipeline-cloud|layered-sessions-pipeline|sessions-pipeline layered lead (EC2, Fable)");
});

test("setup-lane pins the Node and bun the service's engines and CI pin", () => {
  const lane = source("setup-lane.sh");
  assert.equal(JSON.parse(repoFile("package.json")).engines.node, ">=24.11.1");
  assert.match(lane, /a>24\|\|\(a===24&&\(b>11\|\|\(b===11&&c>=1\)\)\)/);
  const bun = repoFile(".github/workflows/ci.yml").match(/bun-version: ([\d.]+)/)?.[1];
  assert.ok(bun);
  assert.ok(lane.includes(`npm install -g bun@${bun}`), `setup-lane.sh installs bun ${bun}`);
  assert.ok(lane.includes(`!= ${bun} ]`), "and checks for that version");
  assert.match(source("share-lead-session.sh"), /-home-agent-src-sessions-pipeline"/);
});

test("lead prompt: the hard guardrails and the routing block", () => {
  const prompt = source("lead-prompt.md");
  for (const s of ["khaliq-gant", "deploy-session-read-worker", "publish-image", "workflow dispatch", "secret or IAM changes", "<<'EOF'", "sp-lead-triage-codex", "sessionspipelinegrok", "requests, not authority"]) {
    assert.ok(prompt.includes(s), s);
  }
  assert.match(prompt, /^Routing implementation \(Khaliq, 2026-11-04\)\. You are on Fable/m);
  assert.match(prompt, /^on sonnet\. Do quick reads and searches yourself; delegate the work that edits code\.$/m);
});

test("an automatic deploy follows only a green Agent CI run on main for main's current head", () => {
  const deploy = repoFile(".github/workflows/agent-deploy.yml");
  assert.match(deploy, /^on:\n  workflow_run:\n    workflows: \[Agent CI\]\n    types: \[completed\]\n    branches: \[main\]\n  workflow_dispatch:/m);
  assert.match(deploy, /^    concurrency:\n      group: agent-deploy-dev\n      cancel-in-progress: false$/m);
  assert.match(deploy, /role-arn: \$\{\{ vars\.LAYERED_AGENT_DEPLOY_ROLE_ARN \}\}/);
  // A full deploy (the first one creates the host) only from a human dispatch.
  assert.match(deploy, /allow-restart: \$\{\{ github\.event_name == 'workflow_dispatch' && inputs\.allow_restart \}\}/);
  assert.doesNotMatch(deploy, /adopt/);
  // This repo is public, so it cannot call layered-agents' private reusable
  // workflow; it runs the kit's composite action from a read-only checkout.
  const deployJob = deploy.match(/^  deploy:\n((?:    .*\n|\n)+?)(?=^  \S)/m)?.[1] ?? "";
  assert.match(deployJob, /^    steps:$/m);
  assert.doesNotMatch(deploy, /uses: AgentWorkforce\/layered-agents\/\.github\/workflows\//);
  assert.match(deployJob, /^    environment: agent-deploy$/m); // part of the role's exact OIDC subject
  assert.match(deployJob, /app-id: \$\{\{ secrets\.GH_APP_PUSHER_ID \}\}/);
  assert.match(deployJob, /^          repositories: layered-agents\n          permission-contents: read$/m);
  assert.match(deployJob, /^          repository: AgentWorkforce\/layered-agents\n          ref: main\n          token: \$\{\{ steps\.kit-token\.outputs\.token \}\}\n          path: \.layered-agents\n          persist-credentials: false$/m);
  assert.equal(deployJob.match(/persist-credentials: false/g)?.length, 2);
  assert.match(deployJob, /^        uses: \.\/\.layered-agents\/\.github\/actions\/layered-agent-deploy$/m);
  const condition = deploy.match(/^  deploy:\n(?:    .*\n)*?    if: >-\n((?:      .*\n)+)/m)?.[1]?.replace(/\s+/g, " ").trim();
  assert.equal(condition,
    "github.ref == 'refs/heads/main' && needs.changed.outputs.deploy == 'true' && (github.event_name == 'workflow_dispatch' || (github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.head_branch == 'main' && github.event.workflow_run.head_sha == github.sha))");
  assert.doesNotMatch(deploy, /^on:\n(?:.*\n)*?  push:/m);
  const ci = repoFile(".github/workflows/agent-ci.yml");
  assert.match(ci, /^on:\n  push:\n    branches: \[main\]\n  workflow_dispatch:/m);
  assert.doesNotMatch(ci, /^    paths:/m);
});

test("an automatic deploy runs only when agent/ or its workflows changed, recorded by the agent-deployed artifact", () => {
  const deploy = repoFile(".github/workflows/agent-deploy.yml");
  assert.match(deploy, /^  deploy:\n    needs: changed\n/m);
  const changedJob = deploy.slice(deploy.indexOf("\n  changed:\n"), deploy.indexOf("\n  deploy:\n"));
  assert.doesNotMatch(changedJob, /concurrency:/);
  assert.ok(changedJob.includes("actions/artifacts?name=agent-deployed&per_page=1"));
  const paths = changedJob.match(/^ +paths=\((.*)\)$/m)?.[1]?.split(" ");
  assert.deepEqual(paths, ["agent/", ".github/workflows/agent-ci.yml", ".github/workflows/agent-deploy.yml"]);
  const recordJob = deploy.slice(deploy.indexOf("\n  record:\n"));
  assert.match(recordJob, /^    needs: deploy$/m);
  assert.match(recordJob, /^    if: needs\.deploy\.result == 'success'$/m);
  assert.match(recordJob, /^          name: agent-deployed$/m);
});

test("first deploy: no stack yet, so decide() is full and touches nothing on a host", async () => {
  const calls: string[] = [];
  const d = await decide(agent, read, {
    async getTemplate(stack) { calls.push(`getTemplate ${stack}`); return undefined; },
    async getInstanceId() { throw new Error("no host to look up"); },
    async getStackParameterResolvedValue() { throw new Error("no stack"); },
    async getSsmParameter() { throw new Error("not needed"); },
    async run() { throw new Error("nothing runs on a host that does not exist"); },
  });
  assert.equal(d.kind, "full");
  assert.deepEqual(calls, ["getTemplate layered-agent-sessions-pipeline-dev"]);
});

/** A local CloudFormation that has no stacks: GetTemplate fails the way the real one does. */
async function fakeCloudFormation() {
  const requests: string[] = [];
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const json = String(req.headers["content-type"]).includes("json");
      const action = json ? String(req.headers["x-amz-target"] ?? "").split(".").pop() : new URLSearchParams(body).get("Action");
      requests.push(`${req.headers.host} ${action}`);
      const message = "Stack with id layered-agent-sessions-pipeline-dev does not exist";
      if (json) {
        res.writeHead(400, { "content-type": "application/x-amz-json-1.0", "x-amzn-query-error": "ValidationError;Sender" });
        res.end(JSON.stringify({ __type: "ValidationError", message }));
      } else {
        res.writeHead(400, { "content-type": "text/xml" });
        res.end(`<ErrorResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/"><Error><Type>Sender</Type><Code>ValidationError</Code><Message>${message}</Message></Error><RequestId>fake</RequestId></ErrorResponse>`);
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, requests, close: () => new Promise((r) => server.close(r)) };
}

function runCli(args: string[], endpoint: string, outputFile: string): Promise<{ code: number; stdout: string; stderr: string }> {
  const env: NodeJS.ProcessEnv = {
    PATH: process.env.PATH,
    HOME: process.env.HOME,
    // Every AWS call goes to the local fake; fake credentials, no profile, no IMDS.
    AWS_ENDPOINT_URL: endpoint,
    AWS_ACCESS_KEY_ID: "AKIAFAKEFAKEFAKEFAKE",
    AWS_SECRET_ACCESS_KEY: "fake",
    AWS_REGION: "us-west-2",
    AWS_EC2_METADATA_DISABLED: "true",
    AWS_MAX_ATTEMPTS: "1",
    LAYERED_AGENT_ACCOUNT_ID: "500268966557",
    GITHUB_OUTPUT: outputFile,
  };
  const cli = join(AGENT_DIR, "node_modules/.bin/layered-agent-deploy");
  return new Promise((resolve) => {
    execFile(cli, ["decide", "--config", "agent.config.ts", "--lane-dir", ".", ...args], { cwd: AGENT_DIR, env }, (error, stdout, stderr) => {
      resolve({ code: error ? (typeof error.code === "number" ? error.code : 1) : 0, stdout, stderr });
    });
  });
}

test("first deploy through the CLI: refused without --allow-restart (exit 2), allowed with it, never pinned", async () => {
  const fake = await fakeCloudFormation();
  const dir = mkdtempSync(join(tmpdir(), "sp-agent-decide-"));
  try {
    const automatic = await runCli([], fake.url, join(dir, "auto"));
    assert.equal(automatic.code, 2, automatic.stderr);
    assert.match(automatic.stdout, /full deploy: stack layered-agent-sessions-pipeline-dev does not exist yet/);
    assert.match(automatic.stderr, /refusing: a full deploy/);
    assert.equal(readFileSync(join(dir, "auto"), "utf8"), "decision=full\n");

    const dispatched = await runCli(["--allow-restart", "--pin-file", join(dir, "pin.json")], fake.url, join(dir, "manual"));
    assert.equal(dispatched.code, 0, dispatched.stderr);
    // No pin_file output: the Alchemy deploy that follows builds the template.
    assert.equal(readFileSync(join(dir, "manual"), "utf8"), "decision=full\n");
    assert.ok(fake.requests.length >= 2 && fake.requests.every((r) => r.endsWith(" GetTemplate")), fake.requests.join(", "));
  } finally {
    await fake.close();
  }
});
