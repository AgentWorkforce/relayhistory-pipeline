/**
 * scrubText for identifier predicates in modules that `pipeline/errors.ts` imports
 * (`pipeline/trace.ts` `isSafeIdentifier`). Those cannot import the seam statically:
 * errors.ts -> trace.ts -> seam -> errors.ts is an import cycle that leaves
 * `PipelineError` uninitialised when the seam's error classes extend it.
 *
 * The seam (index.ts) installs the hook when it is loaded. Until then the selection
 * is read the way the seam reads it: unset, "" or "ts" is scrub.ts `scrubText`,
 * exactly as before; anything else returns undefined, so the predicate fails and
 * the identifier is dropped (never the TypeScript engine under SCRUB_ENGINE=wasm).
 */
import { scrubText } from "../scrub.js";

type PredicateScrub = (value: string) => string | undefined;
let hook: PredicateScrub | undefined;

/** Called once by the seam at module evaluation. */
export function installPredicateScrub(fn: PredicateScrub): void {
  hook = fn;
}

/** The scrubbed text, or undefined when the selected engine cannot scrub it. */
export function predicateScrubText(value: string): string | undefined {
  if (hook) return hook(value);
  const selected = (globalThis as { process?: { env?: { SCRUB_ENGINE?: string } } }).process?.env?.SCRUB_ENGINE;
  return selected === undefined || selected === "" || selected === "ts" ? scrubText(value) : undefined;
}
