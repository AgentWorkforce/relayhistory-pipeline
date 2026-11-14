/**
 * The code-level grant that lets a process run the TypeScript scrub engine
 * (docs/scrub-engine.md, "Default flip"). With no grant, SCRUB_ENGINE unset or ""
 * selects the pinned wasm-bindgen engine and SCRUB_ENGINE=ts is refused
 * (`ScrubEngineConfigError`, reason `scrub_engine_ts_not_allowed`).
 *
 * - "hosted-worker" / "hosted-lambda": declared only by the enumerated hosted runtime
 *   handlers (Cloudflare Worker `fetch` / `queue`, AWS Lambda `handler`), which ship
 *   no relayscrubber artifacts. Declared at handler entry, not at module load: the
 *   self-host Node services import those modules for helpers and must never gain TS.
 * - "test": declared only by the test preloads (root bunfig.toml preload, the vitest
 *   setupFiles), never by production code.
 *
 * Deliberately not environment-driven and not re-exported from the seam (index.ts) or
 * any package index; `scripts/scrub-default-scan.ts` (enforced in CI by its unit test,
 * src/session-ingest/__tests__/scrub-default-scan.test.ts) fails if a declaration
 * appears anywhere else. This module imports nothing from the seam, so
 * predicate-hook.ts can consult it before the seam loads.
 */
export type ScrubTsCapability = "hosted-worker" | "hosted-lambda" | "test";

const KINDS: ReadonlySet<string> = new Set<ScrubTsCapability>(["hosted-worker", "hosted-lambda", "test"]);
const granted = new Set<ScrubTsCapability>();

/** Grant this process the TypeScript engine. Idempotent. Unknown kinds throw. */
export function declareScrubTsCapability(kind: ScrubTsCapability): void {
  if (!KINDS.has(kind)) throw new TypeError("unknown scrub TS capability");
  granted.add(kind);
}

/** True when any grant is present. */
export function scrubTsAllowed(): boolean {
  return granted.size > 0;
}

/** The grants present, sorted (diagnostics and tests). */
export function scrubTsCapabilities(): ScrubTsCapability[] {
  return [...granted].sort();
}

/** Tests only (the seam's `resetScrubEngineForTests` calls it). */
export function clearScrubTsCapabilitiesForTests(): void {
  granted.clear();
}
