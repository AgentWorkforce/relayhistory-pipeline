/** The canonical S3 provider has no CH/Hyperdrive dependency. Exercise its
 * real Worker runtime with synthetic object storage and no external services. */
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
    include: ["test/canonical.test.ts", "test/canonical-pages.test.ts"],
    testTimeout: 30000,
  },
});
