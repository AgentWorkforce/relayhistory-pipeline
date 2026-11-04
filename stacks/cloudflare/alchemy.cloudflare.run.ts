/**
 * Guarded 96bulls Cloudflare stage: the session ingest Worker
 * (stacks/cloudflare/session-ingest.ts) in relayhistory's Cloudflare account.
 * Kept in its own Alchemy config so the local and AWS stacks never load it.
 *
 *   bunx alchemy plan   --config stacks/cloudflare/alchemy.cloudflare.run.ts --stage cf-sessions-prod
 *   bunx alchemy deploy --config stacks/cloudflare/alchemy.cloudflare.run.ts --stage cf-sessions-prod   (needs a bound approval)
 *   destroy             refused (a production stage)
 *
 * Nothing runs without `.96bulls/cf-sessions-prod/cloudflare-inputs.json`
 * (written by hand, never committed; no stage config in this repo turns the
 * ingest on). A deploy also needs the reviewed capture
 * docs/plans/cf-sessions-prod.{plan.txt,plan.detailed.txt,provenance.json},
 * Khaliq's approval bound to it, and a plan that passes the ingest plan contract.
 *
 * State: the account's Cloudflare state store (`alchemy-state-store-96bulls`).
 * Its first use deploys that store Worker, so the first plan is itself part of
 * the rollout approval.
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
  SESSION_INGEST_STAGE,
  assertSessionIngestInputs,
  assertSessionIngestPlanContract,
  sessionIngestStage,
  type SessionIngestInputs,
} from "./session-ingest.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));

export interface CloudflareStageInputs {
  approval?: { decision: "approved"; by: string; date: string; quote: string; capture?: ApprovedCapture };
  sessionIngest: SessionIngestInputs;
}

export type CloudflareMode = "plan" | "deploy" | "destroy";

export function cloudflareMode(argv: readonly string[]): CloudflareMode {
  const sub = argv.find((a) => a === "plan" || a === "deploy" || a === "destroy");
  return sub === "plan" ? "plan" : sub === "destroy" ? "destroy" : "deploy";
}

/** Everything checked before a resource is declared. Pure, so it is unit-tested. */
export function assertCloudflareStage(
  stage: string,
  mode: CloudflareMode,
  inputs: unknown,
  env: Record<string, string | undefined>,
): asserts inputs is CloudflareStageInputs {
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${stage}: ${why}. Nothing was changed.`);
  };
  if (stage !== SESSION_INGEST_STAGE) refuse(`this config is for ${SESSION_INGEST_STAGE} only`);
  if (mode === "destroy") refuse("a production stage is never destroyed from Alchemy");
  if (inputs === null || typeof inputs !== "object" || Array.isArray(inputs)) refuse("inputs must be a JSON object");
  const v = inputs as Record<string, unknown>;
  for (const k of Object.keys(v)) if (k !== "approval" && k !== "sessionIngest") refuse(`${k} is not a known setting`);
  const placeholders = findPlaceholders(v);
  if (placeholders.length) refuse(`inputs still hold placeholders at ${placeholders.join(", ")}`);
  assertSessionIngestInputs(v.sessionIngest, refuse);
  const ingest = v.sessionIngest as SessionIngestInputs;
  // A service binding only reaches a Worker in the same account: relayhistory's.
  if (env.CLOUDFLARE_ACCOUNT_ID !== ingest.accountId)
    refuse("CLOUDFLARE_ACCOUNT_ID must be exactly sessionIngest.accountId (relayhistory's account; a service binding cannot cross accounts)");
  if (mode === "deploy") {
    const a = v.approval as CloudflareStageInputs["approval"];
    if (!a || a.decision !== "approved" || a.by !== APPROVER || typeof a.quote !== "string" || !a.quote.trim())
      refuse(`deploy needs ${APPROVER}'s approval record (decision, by, date, quote, capture)`);
  }
}

function readInputs(stage: string, mode: CloudflareMode): { inputs: CloudflareStageInputs; inputsText: string } {
  const path = join(ROOT, ".96bulls", stage, "cloudflare-inputs.json");
  let inputsText: string;
  let inputs: unknown;
  try {
    inputsText = readFileSync(path, "utf8");
    inputs = JSON.parse(inputsText);
  } catch {
    throw new Error(`96bulls: the Cloudflare stage ${stage} is not cleared: ${path} does not exist. Nothing was created.`);
  }
  assertCloudflareStage(stage, mode, inputs, process.env);
  return { inputs, inputsText };
}

/** Must refuse buffered before any convenience state/bootstrap path is built. */
export function stockCloudflareState(load: typeof readInputs = readInputs) {
  return Layer.unwrap(
    Effect.gen(function* () {
      const stage = yield* Stage;
      const { inputs } = load(stage, cloudflareMode(process.argv));
      if (inputs.sessionIngest.acceptanceMode === "buffered")
        throw new Error(
          "stock_buffered_refused: use retained operator with paused Queue provider",
        );
      return Cloudflare.state({ name: "96bulls" });
    }),
  );
}

export default Alchemy.Stack(
  "96bulls-cloudflare",
  {
    providers: Layer.mergeAll(Cloudflare.providers()),
    state: stockCloudflareState(),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    const mode = cloudflareMode(process.argv);
    const { inputs, inputsText } = readInputs(stage, mode);
    if (inputs.sessionIngest.acceptanceMode === "buffered") throw new Error("stock_buffered_refused: use retained operator with paused Queue provider");
    if (mode === "deploy") {
      const plans = join(ROOT, "docs", "plans");
      let provenanceText: string, planText: string, detailedPlanText: string;
      try {
        provenanceText = readFileSync(join(plans, `${stage}.provenance.json`), "utf8");
        planText = readFileSync(join(plans, `${stage}.plan.txt`), "utf8");
        detailedPlanText = readFileSync(join(plans, `${stage}.plan.detailed.txt`), "utf8");
      } catch {
        throw new Error(`96bulls: deploy needs the reviewed capture docs/plans/${stage}.{provenance.json,plan.txt,plan.detailed.txt}. Nothing was changed.`);
      }
      assertApprovalBinding(inputs.approval?.capture, { provenanceText, planText, inputsText });
      assertSessionIngestPlanContract(planText, detailedPlanText);
    }
    const ingest = yield* sessionIngestStage(inputs.sessionIngest);
    return { stage, accountId: inputs.sessionIngest.accountId, sessionIngest: ingest };
  }),
);
