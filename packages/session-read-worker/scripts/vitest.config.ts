import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["scripts/check-hyperdrive-config.test.ts", "scripts/render-deploy-config.test.ts"],
  },
});
