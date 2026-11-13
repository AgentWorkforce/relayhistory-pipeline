// pipeline-e2e: the full-pipeline e2e lane on its own (docs/pipeline-e2e.md).
//
//   flows/node_modules/.bin/flows run --local-agent flows/pipeline-e2e.flow.ts --input '{}'
//   flows/node_modules/.bin/flows run --local-agent flows/pipeline-e2e.flow.ts --input '{"target":"sandbox"}'
//   flows/node_modules/.bin/flows run --local-agent flows/pipeline-e2e.flow.ts --input '{"steps":["upload.courier","index.hoop","summary.extractive"]}'
//
// It runs e2e/pipeline/run.ts (the same script `bun run e2e:pipeline` runs without flows) through
// flows/lib/lane.sh, which holds it to CI's bun, on a fresh compose project here or in a throwaway Blaxel
// sandbox. ci.flow.ts runs the same lane beside the others when the classifier asks for it.
//
// Result: .relayflow/pipeline-e2e/report.md (under $RELAYFLOW_STATE_DIR/pipeline-e2e/ when set), a
// PASS/FAIL/SKIP line per step. The run ends step_failed on any FAIL.
import { flow } from "@relayflows/surface";
import { runFleet } from "./lib/fleet.ts";
import { lib, shellWord } from "./lib/shell.ts";

interface E2eInput {
  /** Where the stack runs. Default "local". */
  target?: "local" | "sandbox";
  /** Step ids (e2e/pipeline/steps.ts); default every step. */
  steps?: string[];
}

const STEP_ID = /^[a-z0-9._]+$/;

export default flow<E2eInput>(
  "sessions-pipeline.pipeline-e2e",
  { budget: { wallclock: "60m" } },
  async (f, input) => {
    const target = input?.target === "sandbox" ? "sandbox" : "local";
    const steps = input?.steps ?? [];
    if (steps.some((s) => !STEP_ID.test(s))) throw new Error("steps must be e2e/pipeline/steps.ts ids");
    // The state root holds every flow's directory; run.ts writes <root>/pipeline-e2e/.
    const root = (await f.run(`node ${lib("state-dir.mjs")}`)).trim();
    const dir = (await f.run(`node ${lib("state-dir.mjs")} pipeline-e2e`)).trim();
    const select = steps.length ? ` --steps ${shellWord(steps.join(","))}` : "";
    // One job in the fleet: its polls are short, so a lost root lease is followed (lib/fleet.ts).
    const [result] = await runFleet(f, `${dir}/fleet`, [{
      name: "e2e",
      steps: [{ name: "pipeline e2e", command: `RELAYFLOW_STATE_DIR=${shellWord(root)} bash ${lib("lane.sh")} e2e --target ${target}${select}`, timeoutMinutes: 50 }],
    }], 27, 2);
    const report = (await f.run(`cat ${shellWord(`${dir}/report.md`)} 2>/dev/null || true`)).trim();
    if (!result?.ok) return f.done("step_failed", { detail: `${report.split("\n")[0] || result?.output || "no report"}; see ${dir}/report.md` });
    f.done("success", { detail: report.split("\n")[0] ?? "green" });
  },
);
