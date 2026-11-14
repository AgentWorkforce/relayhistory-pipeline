/**
 * scrubText for identifier predicates in modules that `pipeline/errors.ts` imports
 * (`pipeline/trace.ts` `isSafeIdentifier`). Those cannot import the seam statically:
 * errors.ts -> trace.ts -> seam -> errors.ts is an import cycle that leaves
 * `PipelineError` uninitialised when the seam's error classes extend it.
 *
 * The seam (index.ts) installs the hook when it is loaded; from then on every answer is the
 * seam's own (its selection, its SCRUB_ENGINE_REQUIRED check and its refusal latches, which
 * exist only inside the seam). Until then this fallback answers with scrub.ts `scrubText`
 * exactly where the seam's first selection would pick the TS engine without refusing:
 * the process holds a TS grant (ts-capability.ts) AND SCRUB_ENGINE is unset, "" or "ts" AND
 * SCRUB_ENGINE_REQUIRED is unset, "" or "ts". Everything else (the ungranted default, which
 * is wasm-bindgen; SCRUB_ENGINE=ts without a grant, which the seam refuses; any other
 * engine or requirement, which the seam either selects as a wasm engine or refuses)
 * returns undefined. The fallback never latches; the seam re-reads the environment when it
 * selects.
 *
 * Undefined means "cannot scrub": the predicate fails and the identifier is dropped, so the
 * caller answers 400 or omits the field. That is how an engine fault surfaces here (an
 * uninitialised or failed wasm-bindgen engine, before or after the seam loads), never as
 * unscrubbed output. Pre-existing behaviour; since the default flip it is what an
 * uninitialised self-host process sees (docs/scrub-engine.md, "Import cycle").
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
  const tsOrUnset = (v: string | undefined) => v === undefined || v === "" || v === "ts";
  return scrubTsAllowed() && tsOrUnset(env?.SCRUB_ENGINE) && tsOrUnset(env?.SCRUB_ENGINE_REQUIRED) ? scrubText(value) : undefined;
}
