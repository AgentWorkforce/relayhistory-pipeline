/**
 * Every suite runs inside workerd (Miniflare) with the Worker's own wrangler
 * config, so the code under test is the code that deploys:
 *
 *   test/ported/*   #135's suites (src/session-read-api/__tests__), unchanged:
 *                   `bun:test` resolves to vitest (test/bun-test.ts).
 *   test/*.test.ts  the Worker's full request path: entry, RelayAuth JWKS,
 *                   ClickHouse as the SELECT-only user, Supabase as
 *                   bulls96_read_api through the Hyperdrive binding.
 *
 * The PGlite liveness suite stays on bun (PGlite reads its WASM from disk);
 * the same role and SQL run here against real Postgres through Hyperdrive.
 */
import { fileURLToPath } from "node:url";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url).href);

export default defineConfig({
  plugins: [
    cloudflareTest(({ inject }) => {
      // Wrangler resolves the Hyperdrive binding before the miniflare overrides
      // apply; this run's role URL, in memory only.
      process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_SUPABASE = inject("pgReadUrl");
      return {
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        hyperdrives: {
          SUPABASE: inject("pgReadUrl"),
          // Test-only: seeds pointer rows as the database owner.
          TEST_PG_ADMIN: inject("pgAdminUrl"),
          TEST_INGEST_PG: inject("pgIngestUrl"),
        },
        bindings: {
          CLICKHOUSE_URL: inject("chUrl"),
          CLICKHOUSE_USER: inject("chReadUser"),
          CLICKHOUSE_PASSWORD: inject("chReadPassword"),
          CLICKHOUSE_DATABASE: inject("chDatabase"),
          READ_API_ORIGIN: "https://read.test.example",
          READ_API_COMPLETE_SINCE: "epoch",
          RELAYAUTH_JWKS_URL: inject("jwksUrl"),
          RELAYAUTH_ISSUER: "https://relayauth.dev",
          TEST_CH_ADMIN_USER: inject("chAdminUser"),
          TEST_CH_ADMIN_PASSWORD: inject("chAdminPassword"),
          TEST_SIGNING_JWK: inject("signingJwk") as Record<string, string>,
        },
      },
      };
    }),
  ],
  resolve: {
    alias: [
      { find: "bun:test", replacement: here("./test/bun-test.ts") },
      { find: "@rhc-object-store", replacement: here("../object-store/src/index.ts") },
      { find: /^@rhc\/(.*)$/, replacement: here("../pipeline/src/$1") },
    ],
  },
  test: {
    globalSetup: ["./test/global-setup.ts"],
    setupFiles: ["./test/setup-env.ts"],
    include: ["test/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
