/** Offline real workerd cache proof against the Node producer's immutable fixture. */
import { fileURLToPath } from "node:url";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";
const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
export default defineConfig({
  plugins: [
    cloudflareTest({
      miniflare: {
        compatibilityDate: "2026-07-01",
        compatibilityFlags: ["nodejs_compat"],
      },
    }),
  ],
  resolve: {
    alias: [
      {
        find: "@rhc-object-store",
        replacement: here("../object-store/src/index.ts"),
      },
      { find: /^@rhc\/(.*)$/, replacement: here("../pipeline/src/$1") },
    ],
  },
  test: {
    globalSetup: ["./test/artifact-capacity-setup.ts"],
    include: ["test/analysis-cache.proof.ts"],
    testTimeout: 30000,
  },
});
