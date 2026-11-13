// Every deterministic step runs under /bin/sh; this is the one way a string
// becomes a shell argument in these flows.
import { fileURLToPath } from "node:url";

export const shellWord = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;

/**
 * Where the gate's own code is: this directory, absolute, wherever the flow
 * module was loaded from. Every gate script (classifier, lanes, fleet,
 * findings gate, review state, publish guard) is run from here, never from
 * `flows/lib/` of the tree under review. The lander runs main's flows against
 * the change's worktree (`flows run <main checkout>/flows/ci.flow.ts` with the
 * change as the working directory), so a change that edits a gate script
 * cannot vouch for itself: its edit runs only once it is on main
 * (docs/maintainer-workflow.md "Trust").
 */
export const LIB = fileURLToPath(new URL(".", import.meta.url)).replace(/\/$/, "");
/** The flows directory the gate was loaded from (its node_modules hold the flows CLI). */
export const FLOWS = LIB.replace(/\/lib$/, "");
/** A gate script under LIB, as one shell word. */
export const lib = (file: string): string => shellWord(`${LIB}/${file}`);
