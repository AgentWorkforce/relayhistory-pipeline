/**
 * scrubText for identifier predicates in modules that `pipeline/errors.ts` imports
 * (`pipeline/trace.ts` `isSafeIdentifier`). Those cannot import the seam statically:
 * errors.ts -> trace.ts -> seam -> errors.ts is an import cycle that leaves
 * `PipelineError` uninitialised when the seam's error classes extend it.
 *
 * The seam (index.ts) installs the hook when it is loaded. Until then the selection
 * is read the way the seam resolves it: scrub.ts `scrubText` only in a process holding
 * a TS grant (ts-capability.ts) with SCRUB_ENGINE unset, "" or "ts" and no
 * SCRUB_ENGINE_REQUIRED=wasm-bindgen; anything else (including the ungranted default,
 * wasm-bindgen) returns undefined, so the predicate fails and the identifier is dropped.
 */
import { scrubText } from "../scrub.js";
import { scrubTsAllowed } from "./ts-capability.js";

type PredicateScrub = (value: string) => string | undefined;
let hook: PredicateScrub | undefined;

/** Called once by the seam at module evaluation. */
export function installPredicateScrub(fn: PredicateScrub): void {
  hook = fn;
}

/** The scrubbed text, or undefined when the selected engine cannot scrub it. */
export function predicateScrubText(value: string): string | undefined {
  if (hook) return hook(value);
  const env = (globalThis as { process?: { env?: { SCRUB_ENGINE?: string; SCRUB_ENGINE_REQUIRED?: string } } }).process?.env;
  const selected = env?.SCRUB_ENGINE;
  const ts = selected === "ts" || ((selected === undefined || selected === "") && env?.SCRUB_ENGINE_REQUIRED !== "wasm-bindgen");
  return ts && scrubTsAllowed() ? scrubText(value) : undefined;
}
