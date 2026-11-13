// What counts as part of a change in this checkout, shared by ci.flow.ts,
// the review flows and their tests. Adapted from relay-app's flows/lib/changes.ts
// (see flows/README.md for provenance); the lane selection itself lives in
// flows/lib/classify.mjs.
import { shellWord } from "./shell.ts";

/**
 * Machine-local state that lives in the checkout but is never part of a
 * change: the relay's workspace, the agent CLIs' settings, and this repo's
 * local secrets and Alchemy state (AGENTS.md: both stay gitignored).
 */
export const MACHINE_LOCAL = [".agentworkforce", ".claude", ".96bulls", ".alchemy"];
// .gitignore lists the same directories (flows/.claude too, which the CLIs
// create there); this list is what the flows enforce whatever is ignored.

/**
 * New files the change has not staged, anywhere in the tree (a new top-level
 * directory or root file included). Anything else local to one machine
 * belongs in .git/info/exclude.
 */
export const UNTRACKED =
  `git ls-files --others --exclude-standard -- . ${MACHINE_LOCAL.map((p) => `':!:${p}'`).join(" ")} | head -200`;

export const GITHUB_STEP = "Actions";

/** The classifier's verdict for the paths changed from the merge base to the working tree (flows/lib/classify.mjs). */
export const classifyCommand = (mergeBase: string, jev: boolean): string =>
  `node flows/lib/classify.mjs ${shellWord(mergeBase)}${jev ? " --jev" : ""}`;
