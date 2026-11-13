// reviewer: one member of a review fleet, as its own run. The two review flows
// start one of these per lens at the same time (flows/lib/fleet.mjs): with
// relayflows 2.0.55, a body that imports helpers (as these do) ran its
// `f.agent` calls one after another when measured, even under Promise.all and
// in separate directories. Separate runs are not limited that way. Not meant to be run by
// hand; the input is written by flows/lib/review.ts.
import { flow } from "@relayflows/surface";
import type { ReviewerInput } from "./lib/review.ts";
import { lib, shellWord } from "./lib/shell.ts";

export default flow<ReviewerInput>(
  "sessions-pipeline.reviewer",
  { budget: { wallclock: "20m" } },
  async (f, input) => {
    await f
      .agent(`${input.lens}-reviewer`, {
        cli: input.cli,
        ...(input.model ? { model: input.model } : {}),
        cwd: input.cwd,
        task: input.task,
        timeout: input.timeout,
        permissions: { accessPreset: "readonly" },
      })
      .gate({
        type: "subprocess_gate",
        command: `node ${lib("check-findings.mjs")} ${shellWord(input.findings)} ${shellWord(input.files)}`,
      });
    f.done("success");
  },
);
