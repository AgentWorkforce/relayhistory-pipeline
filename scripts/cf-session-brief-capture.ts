/**
 * Capture the session brief Worker's plan for review (read only: `alchemy plan` twice).
 *
 *   CLOUDFLARE_ACCOUNT_ID=<id> CLOUDFLARE_API_TOKEN=<token from 1Password, never argv> \
 *     bun scripts/cf-session-brief-capture.ts
 *
 * Needs a clean tree and `.96bulls/cf-sessions-prod/session-brief-inputs.json` (without
 * the approval's `capture`). Writes docs/plans/cf-sessions-prod.session-brief.{plan.txt,
 * plan.detailed.txt,provenance.json}, refuses a plan the contract does not accept, and
 * prints the `approval.capture` record to bind (stacks/aws/deploy-guard.ts
 * assertApprovalBinding). Commit the three files, then write the approval.
 */
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { inputsDigest } from "../stacks/aws/deploy-guard.ts";
import { sanitizePlan, sha256 } from "../src/cli/plan-provenance.ts";
import { SESSION_BRIEF_STAGE, assertSessionBriefPlanContract, type SessionBriefPlanPhase } from "../stacks/cloudflare/session-brief.ts";
import { sessionBriefCaptureFiles, sessionBriefInputsPath } from "../stacks/cloudflare/alchemy.session-brief.run.ts";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const CONFIG = "stacks/cloudflare/alchemy.session-brief.run.ts";

export interface SessionBriefProvenance {
  schema: 1;
  stage: string;
  stack: "96bulls-session-brief";
  generatedAt: string;
  target: { accountId: string };
  source: { commit: string; clean: true };
  inputs: { sha256: string };
  outputs: { plan: { path: string; sha256: string }; detailed: { path: string; sha256: string } };
  summary: string;
  phase: SessionBriefPlanPhase;
}

/** The provenance record and the approval.capture it implies. Pure. */
export function sessionBriefCapture(i: { stage: string; accountId: string; commit: string; inputsText: string; planText: string; detailedText: string; now: Date }) {
  const phase = assertSessionBriefPlanContract(i.planText, i.detailedText);
  const files = sessionBriefCaptureFiles(i.stage);
  const provenance: SessionBriefProvenance = {
    schema: 1,
    stage: i.stage,
    stack: "96bulls-session-brief",
    generatedAt: i.now.toISOString(),
    target: { accountId: i.accountId },
    source: { commit: i.commit, clean: true },
    inputs: { sha256: inputsDigest(i.inputsText) },
    outputs: { plan: { path: files.plan, sha256: sha256(i.planText) }, detailed: { path: files.detailed, sha256: sha256(i.detailedText) } },
    summary: i.planText.split(/\r?\n/).find((l) => l.startsWith("Plan:")) ?? "",
    phase,
  };
  const provenanceText = `${JSON.stringify(provenance, null, 2)}\n`;
  return {
    provenanceText,
    capture: { provenanceSha256: sha256(provenanceText), planSha256: sha256(i.planText), inputsSha256: provenance.inputs.sha256, sourceCommit: i.commit },
    phase,
  };
}

function run(cmd: string, args: string[]): string {
  const r = spawnSync(cmd, args, { cwd: ROOT, encoding: "utf8", env: process.env });
  if (r.status !== 0) throw new Error(`${cmd} ${args.join(" ")} exited ${r.status}`);
  return r.stdout;
}

if (import.meta.main) {
  const stage = SESSION_BRIEF_STAGE;
  if (run("git", ["status", "--porcelain"]).trim()) throw new Error("capture needs a clean tree");
  const commit = run("git", ["rev-parse", "HEAD"]).trim();
  const inputsText = readFileSync(join(ROOT, sessionBriefInputsPath(stage)), "utf8");
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID ?? "";
  const plan = (detailed: boolean) =>
    sanitizePlan(run("bunx", ["alchemy", "plan", "--config", CONFIG, "--stage", stage, ...(detailed ? ["--detailed"] : [])]), []);
  const planText = plan(false);
  const detailedText = plan(true);
  const out = sessionBriefCapture({ stage, accountId, commit, inputsText, planText, detailedText, now: new Date() });
  const files = sessionBriefCaptureFiles(stage);
  writeFileSync(join(ROOT, files.plan), planText);
  writeFileSync(join(ROOT, files.detailed), detailedText);
  writeFileSync(join(ROOT, files.provenance), out.provenanceText);
  console.log(`phase: ${out.phase}`);
  console.log(JSON.stringify({ capture: out.capture }, null, 2));
}
