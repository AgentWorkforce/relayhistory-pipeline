/**
 * Guarded 96bulls Cloudflare stage for the session brief Worker
 * (stacks/cloudflare/session-brief.ts). Its own Alchemy config and stack name, so the
 * local, AWS and session-ingest (#154) stacks never load it and its state never mixes
 * with theirs. Same guards as #154's alchemy.cloudflare.run.ts.
 *
 *   bunx alchemy plan   --config stacks/cloudflare/alchemy.session-brief.run.ts --stage cf-sessions-prod
 *   bunx alchemy deploy --config stacks/cloudflare/alchemy.session-brief.run.ts --stage cf-sessions-prod   (needs a bound approval)
 *   destroy             refused (a production stage)
 *
 * Nothing runs without `.96bulls/cf-sessions-prod/session-brief-inputs.json` (written by
 * hand, never committed). A deploy also needs the reviewed capture
 * docs/plans/cf-sessions-prod.session-brief.{plan.txt,plan.detailed.txt,provenance.json}
 * (scripts/cf-session-brief-capture.ts), Khaliq's approval bound to it, and a plan that
 * passes the session brief plan contract.
 *
 * State: the account's Cloudflare state store (`alchemy-state-store-96bulls`). If this
 * is the first Alchemy stage in the account, its first plan deploys that store Worker,
 * so the first plan is itself part of the rollout approval.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { APPROVER, assertApprovalBinding, findPlaceholders, type ApprovedCapture } from "../aws/deploy-guard.ts";
import {
  SESSION_BRIEF_STAGE,
  assertSessionBriefInputs,
  assertSessionBriefPlanContract,
  sessionBriefStage,
  type SessionBriefInputs,
} from "./session-brief.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));

export const SESSION_BRIEF_STACK = "96bulls-session-brief";
export const sessionBriefInputsPath = (stage: string) => join(".96bulls", stage, "session-brief-inputs.json");
export const sessionBriefCaptureFiles = (stage: string) => ({
  plan: `docs/plans/${stage}.session-brief.plan.txt`,
  detailed: `docs/plans/${stage}.session-brief.plan.detailed.txt`,
  provenance: `docs/plans/${stage}.session-brief.provenance.json`,
});

export interface SessionBriefStageInputs {
  approval?: { decision: "approved"; by: string; date: string; quote: string; capture?: ApprovedCapture };
  sessionBrief: SessionBriefInputs;
}

export type CloudflareMode = "plan" | "deploy" | "destroy";

export function cloudflareMode(argv: readonly string[]): CloudflareMode {
  const sub = argv.find((a) => a === "plan" || a === "deploy" || a === "destroy");
  return sub === "plan" ? "plan" : sub === "destroy" ? "destroy" : "deploy";
}

/** Everything checked before a resource is declared. Pure, so it is unit-tested. */
export function assertSessionBriefStage(
  stage: string,
  mode: CloudflareMode,
  inputs: unknown,
  env: Record<string, string | undefined>,
): asserts inputs is SessionBriefStageInputs {
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${stage}: ${why}. Nothing was changed.`);
  };
  if (stage !== SESSION_BRIEF_STAGE) refuse(`this config is for ${SESSION_BRIEF_STAGE} only`);
  if (mode === "destroy") refuse("a production stage is never destroyed from Alchemy");
  if (inputs === null || typeof inputs !== "object" || Array.isArray(inputs)) refuse("inputs must be a JSON object");
  const v = inputs as Record<string, unknown>;
  for (const k of Object.keys(v)) if (k !== "approval" && k !== "sessionBrief") refuse(`${k} is not a known setting`);
  const placeholders = findPlaceholders(v);
  if (placeholders.length) refuse(`inputs still hold placeholders at ${placeholders.join(", ")}`);
  assertSessionBriefInputs(v.sessionBrief, refuse);
  const brief = v.sessionBrief as SessionBriefInputs;
  // The Hyperdrive config and the Secrets Store are per account: plan against the one the inputs name.
  if (env.CLOUDFLARE_ACCOUNT_ID !== brief.accountId)
    refuse("CLOUDFLARE_ACCOUNT_ID must be exactly sessionBrief.accountId (the account holding its Hyperdrive config and Secrets Store)");
  if (mode === "deploy") {
    const a = v.approval as SessionBriefStageInputs["approval"];
    if (!a || a.decision !== "approved" || a.by !== APPROVER || typeof a.quote !== "string" || !a.quote.trim())
      refuse(`deploy needs ${APPROVER}'s approval record (decision, by, date, quote, capture)`);
  }
}

function readInputs(stage: string, mode: CloudflareMode): { inputs: SessionBriefStageInputs; inputsText: string } {
  const path = join(ROOT, sessionBriefInputsPath(stage));
  let inputsText: string;
  let inputs: unknown;
  try {
    inputsText = readFileSync(path, "utf8");
    inputs = JSON.parse(inputsText);
  } catch {
    throw new Error(`96bulls: the session brief stage ${stage} is not cleared: ${path} does not exist. Nothing was created.`);
  }
  assertSessionBriefStage(stage, mode, inputs, process.env);
  return { inputs, inputsText };
}

export default Alchemy.Stack(
  SESSION_BRIEF_STACK,
  {
    providers: Layer.mergeAll(Cloudflare.providers()),
    state: Cloudflare.state({ name: "96bulls" }),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    const mode = cloudflareMode(process.argv);
    const { inputs, inputsText } = readInputs(stage, mode);
    if (mode === "deploy") {
      const files = sessionBriefCaptureFiles(stage);
      let provenanceText: string, planText: string, detailedPlanText: string;
      try {
        provenanceText = readFileSync(join(ROOT, files.provenance), "utf8");
        planText = readFileSync(join(ROOT, files.plan), "utf8");
        detailedPlanText = readFileSync(join(ROOT, files.detailed), "utf8");
      } catch {
        throw new Error(`96bulls: deploy needs the reviewed capture ${files.plan}, ${files.detailed} and ${files.provenance}. Nothing was changed.`);
      }
      assertApprovalBinding(inputs.approval?.capture, { provenanceText, planText, inputsText });
      assertSessionBriefPlanContract(planText, detailedPlanText);
    }
    const brief = yield* sessionBriefStage(inputs.sessionBrief);
    return { stage, accountId: inputs.sessionBrief.accountId, sessionBrief: brief };
  }),
);
