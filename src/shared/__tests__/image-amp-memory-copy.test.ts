import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
test("Amp image copy preserves the signed manifest and confines credentials to memory", () => {
  const result = spawnSync("python3", ["src/shared/__tests__/image_amp_copy_test.py"], { cwd: new URL("../../../", import.meta.url).pathname, encoding: "utf8" });
  expect(result.status, result.stderr).toBe(0);
  expect(result.stdout + result.stderr).not.toContain("fixture-private-upload-capability");
});
