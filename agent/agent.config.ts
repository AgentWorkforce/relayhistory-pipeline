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
  // 4 vCPU / 16 GiB: one Fable lead plus a few subagents running bun install,
  // tsc and vitest (PGlite, in process) over two TypeScript repos. Nothing
  // here compiles native code or runs containers.
  instanceType: "m7i.xlarge",
  rootVolumeGiB: 100,
  laneFiles: ["lead-prompt.md", "setup-lane.sh", "share-lead-session.sh"],
  // The first repo is the lead's working directory and the ledger's
  // repository (LEDGER_REPO=~/build/sessions-pipeline).
  repos: ["AgentWorkforce/sessions-pipeline", "AgentWorkforce/sessions-pipeline-cloud"],
  relayDescription: "sessions-pipeline layered lead (EC2, Fable)",
  names: { platformPrefix: `/relaycast-agent-host/${stage}` },
});
