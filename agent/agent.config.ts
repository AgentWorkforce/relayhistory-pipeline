/**
 * layered-sessions-pipeline: the layered agent for this repo and
 * AgentWorkforce/sessions-pipeline-cloud (ledger, overlap calls, coordination
 * over Agent Relay). Built by @agentworkforce/layered-agent; lane files are
 * the rest of this folder.
 *
 * The first layered agent created fresh rather than adopted: its own host
 * role, instance profile and log groups under the module's default names,
 * tagged layered-agent:name=sessions-pipeline. The one override is
 * platformPrefix. sessions-dev has no layered-agents network; the host joins
 * the existing agent-hosts VPC, whose ids relaycast-cloud's agent-host stack
 * publishes under /relaycast-agent-host/<stage>/network/. The kit's shared
 * Anthropic and Nango parameters also live under that prefix. The names must
 * match this agent's entry in layered-agents infra/agents.ts, which owns its
 * deploy role; test/agent.test.ts pins them.
 */
import { defineLayeredAgent } from "@agentworkforce/layered-agent";

const stage = process.env.LAYERED_AGENT_STAGE ?? "dev";

export default defineLayeredAgent({
  name: "sessions-pipeline",
  repo: "AgentWorkforce/sessions-pipeline",
  stage,
  accountId: process.env.LAYERED_AGENT_ACCOUNT_ID,
  arch: "x64",
  // Pinned to the AMI the host runs: a new Canonical "current" AMI would
  // otherwise turn every deploy into a host replacement. Move it deliberately.
  ami: "ami-08cf17e3856f70154",
  // 4 vCPU / 16 GiB: one Fable lead plus a few subagents running bun install,
  // tsc and vitest (PGlite, in process) over two TypeScript repos. Nothing
  // here compiles native code or runs containers.
  instanceType: "m7i.xlarge",
  rootVolumeGiB: 100,
  laneFiles: ["lead-prompt.md", "setup-lane.sh", "share-lead-session.sh"],
  // Relay workers (module 0.1.7): long-running parallel work, e.g. one worker
  // per PR stack from the triage. Two at a time on this 4-vCPU / 16 GiB box
  // (the default is 3); in-process subagents stay the default for
  // implementation.
  workers: { maxConcurrent: 2 },
  // The first repo is the lead's working directory and the ledger's
  // repository (LEDGER_REPO=~/build/sessions-pipeline).
  repos: ["AgentWorkforce/sessions-pipeline", "AgentWorkforce/sessions-pipeline-cloud"],
  relayDescription: "sessions-pipeline layered lead (EC2, Fable)",
  names: { platformPrefix: `/relaycast-agent-host/${stage}` },
  // The Agent Relay sessions MCP (module 0.1.9): the lead can search and read
  // the team's shared sessions as agent-relay-sessions. The host role and its
  // boundary read exactly this parameter; the boundary comes from this
  // agent's entry in layered-agents infra/agents.ts, which must match.
  relaySessions: { tokenParam: `/relaycast-agent-host/${stage}/relay-sessions-token-sessions-pipeline` },
  // The A2A card the lead publishes on the relay (set_relay_agent_card,
  // module 0.1.17), so a peer can see what to send it before delegating.
  // Delivered by hot update, never in the user data.
  card: {
    description:
      "layered-sessions-pipeline coordinates work on the Agent Relay sessions pipeline (AgentWorkforce/relayhistory-pipeline, codename 96Bulls: ingest, scrub, store, index and summarize coding sessions) and AgentWorkforce/sessions-pipeline-cloud. It keeps a ledger of every agent's work in both repos, warns agents whose changes overlap, reviews and lands changes, and drives the consolidation of open pull requests. Tell it what you are working on in either repo, and send it changes to review.",
    skills: [
      {
        id: "track-work",
        name: "Record your work and flag overlaps",
        description:
          "Records what an agent is working on in either repo (task, branch, session, paths) in its ledger, checks it against every open entry and both repos' main, and messages both agents when their work overlaps. Send a one-line summary with your branch and paths, or a shared session id.",
        tags: ["ledger", "coordination"],
        examples: [
          "Working on the summary worker's retry backoff on fix/summary-retry, paths workers/summary,packages/queue",
          "In sessions-pipeline-cloud I'm on feat/gcs-lifecycle touching stacks/gcp; record it",
          "PR #312 merged; mark me done",
        ],
      },
      {
        id: "check-overlaps",
        name: "Who is working on what",
        description:
          "Answers who is changing a file, area or feature in either repo right now, from the ledger and open branches, and suggests who should go first when two changes collide.",
        tags: ["coordination", "ownership"],
        examples: [
          "Is anyone touching the ingest API's scrub path right now?",
          "Who owns the HoopDb migration work, and on which branch?",
        ],
      },
      {
        id: "review-and-land",
        name: "Review and land a change",
        description:
          "Reviews the exact final diff of a pushed branch or pull request (an adversarial and a contextual pass, with the CI flow) and sends back the findings. Lands it on main once the checks are green, both reviews are recorded and no blocker or major finding is open.",
        tags: ["code-review", "merge"],
        examples: [
          "Please review review/alice-ingest-limits: it caps manifest size at 10 MB",
          "PR #320 has green checks; review it and land it if it's clean",
          "I pushed fixes for the findings on review/alice-ingest-limits",
        ],
      },
      {
        id: "consolidate-prs",
        name: "Triage and consolidate open pull requests",
        description:
          "Keeps the plan for the open sessions-pipeline pull requests: the merge order for the critical path, which PRs fold into which stack or parent, and which are close candidates and what supersedes them. Closing a PR needs the owner's own OK.",
        tags: ["triage", "pull-requests"],
        examples: [
          "What's the merge order for the critical path?",
          "Which open PRs are close candidates, and what supersedes each?",
          "Which stack should PR #290 fold into?",
        ],
      },
      {
        id: "implement-change",
        name: "Make a code change",
        description:
          "Implements a change in either repo through a coding subagent in its own clone, runs that repo's CI checks, reviews the exact final diff (adversarial and contextual) and lands it by direct push to main here (pull requests are disabled on relayhistory-pipeline), by pull request in sessions-pipeline-cloud, with the evidence recorded in the ledger. Deploys, migrations, workflow dispatches, flag flips and secret or IAM changes need the owner's own OK first.",
        tags: ["implementation", "direct-push"],
        examples: [
          "Add a regression test for the courier's chunk retry and land it",
          "Fix the flaky pg-proof test on PostgreSQL 17 and land it on main",
        ],
      },
    ],
    tags: ["sessions-pipeline", "relayhistory", "coordination", "code-review", "ledger"],
    documentationUrl: "https://github.com/AgentWorkforce/relayhistory-pipeline",
  },
});
