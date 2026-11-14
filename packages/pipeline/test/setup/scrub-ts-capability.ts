/**
 * packages/pipeline vitest setup file (vitest.config.ts `setupFiles`). TEST ONLY.
 *
 * Same role as the root bun preload scripts/test-preload/scrub-ts-capability.ts: since the
 * default flip (docs/scrub-engine.md "Default flip") TypeScript scrub needs a code-level
 * grant, and test files that never initialise an engine keep their TypeScript expectations
 * through this "test" grant. `resetScrubEngineForTests()` clears it; it is re-declared
 * before and after every test. Clean-process tests without this file assert the production
 * default. No production module may import this file (scripts/scrub-default-scan.ts).
 */
import { afterEach, beforeEach } from "vitest";
import { declareScrubTsCapability } from "../../src/lib/scrub-engine/ts-capability.js";

/** Re-declare the "test" grant (after a `resetScrubEngineForTests()`). */
export function restoreScrubTsTestCapability(): void {
  declareScrubTsCapability("test");
}

restoreScrubTsTestCapability();
beforeEach(restoreScrubTsTestCapability);
afterEach(restoreScrubTsTestCapability);
