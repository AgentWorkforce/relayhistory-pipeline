/**
 * Regenerates the Stage 2 goldens from the fixture cohort and the fake reply (synthetic; no model is called):
 *   expected-stage2-input.json   the prompt's data object, pretty-printed (what the transport receives, compact)
 *   expected-synthesis-v2.json   the workspace-synthesis/2 document of the happy path
 * Run: bun src/session-intel/__tests__/fixtures/synthesis/make-stage2-golden.ts
 */
import { readFileSync, writeFileSync } from "node:fs";
import { analyzedSessions, synthesize } from "../../../synthesis/synthesize.ts";
import { buildStage2Prompt, memoryBudgetLedger, runStage2, synthesisV2Json } from "../../../synthesis/stage2.ts";

const dir = new URL("./", import.meta.url);
const cohort = JSON.parse(readFileSync(new URL("cohort.json", dir), "utf8"));
const reply = readFileSync(new URL("stage2-reply.json", dir), "utf8");
const options = { now: "2026-11-08T00:00:00.000Z" };
const v1 = synthesize(cohort, options);
const prompt = buildStage2Prompt(v1, analyzedSessions(cohort, options));
writeFileSync(new URL("expected-stage2-input.json", dir), `${JSON.stringify(JSON.parse(prompt.input), null, 2)}\n`);
const run = await runStage2({
  v1, cohort, options, orgId: "fx-org",
  deps: {
    enabled: true, trigger: "scheduled", ledger: memoryBudgetLedger(), clock: () => new Date("2026-11-24T10:00:00.000Z"),
    transport: { async complete(r) { return { outputText: JSON.stringify(JSON.parse(reply)), inputTokens: 4_000, outputTokens: 900, providerRequestId: "fx-req-1", model: r.model }; } },
  },
});
writeFileSync(new URL("expected-synthesis-v2.json", dir), synthesisV2Json(run.document));
