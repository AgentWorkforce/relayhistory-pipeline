/**
 * Root `bun test` preload (bunfig.toml `[test] preload`). TEST ONLY.
 *
 * Since the default flip (docs/scrub-engine.md "Default flip") an unset SCRUB_ENGINE is the
 * pinned wasm-bindgen engine, and TypeScript scrub needs a code-level grant
 * (packages/pipeline/src/lib/scrub-engine/ts-capability.ts). The ~100 test files that drive
 * scrubbing code without initialising an engine keep their TypeScript expectations through
 * this "test" grant, unchanged.
 *
 * It does not mask production behaviour: the default-flip, refusal, REQUIRED-override,
 * reachability and Node-path tests run in clean child processes without any preload and
 * assert no grant is present (src/session-ingest/__tests__/scrub-default-flip.test.ts).
 * No production module may import this file (scripts/scrub-default-scan.ts).
 *
 * `resetScrubEngineForTests()` clears every grant, so a file that resets sees production
 * semantics until the next hook below re-grants: before each test and after each test
 * (after the file's own afterEach), so a reset never leaks into the next file of a shard.
 */
import { afterEach, beforeEach } from "bun:test";
import { declareScrubTsCapability } from "../../packages/pipeline/src/lib/scrub-engine/ts-capability.ts";

/** Re-declare the "test" grant (after a `resetScrubEngineForTests()`). */
export function restoreScrubTsTestCapability(): void {
  declareScrubTsCapability("test");
}

restoreScrubTsTestCapability();
beforeEach(restoreScrubTsTestCapability);
afterEach(restoreScrubTsTestCapability);
