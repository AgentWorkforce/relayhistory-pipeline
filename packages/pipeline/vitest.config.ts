import { defineConfig } from "vitest/config";

// Mirrors relayhistory-cloud packages/relayhistory/vitest.config.ts (minus the
// cloud-client alias, which nothing here imports).
export default defineConfig({
  test: {
    // PGlite boot can exceed Vitest's 10s default when DB-backed files start together.
    hookTimeout: 30_000,
    include: ["test/**/*.test.ts"],
    // TypeScript-scrub grant for tests ("test" kind only); see the file. Clean-process
    // tests run without it to assert the production default.
    setupFiles: ["test/setup/scrub-ts-capability.ts"],
  },
});
