/**
 * Module-load TypeScript-engine grant for hosted AWS Lambda entries (docs/scrub-engine.md,
 * "Default flip"). A hosted entry imports this module FIRST, as a bare side-effect import, so the
 * grant exists before any other module in its graph evaluates: a logger or helper that reaches the
 * seam during module evaluation (or before the handler runs) then resolves the unset SCRUB_ENGINE
 * to "ts" instead of latching wasm-bindgen, which a hosted bundle cannot load. The handler-entry
 * declaration stays as an idempotent second call.
 *
 * Only the enumerated hosted entries may import this module, directly or transitively
 * (scripts/scrub-default-scan.ts rules "hosted-grant-reach" and "hosted-grant-first"); a
 * self-host main that reached it would silently gain the TypeScript engine.
 */
import { declareScrubTsCapability } from "./ts-capability.js";

declareScrubTsCapability("hosted-lambda");
