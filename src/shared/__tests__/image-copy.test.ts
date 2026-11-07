import { expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

test("reviewed image copy preserves digest and keeps credentials out of disk/argv", () => {
  const script = fileURLToPath(new URL("./image_copy_test.py", import.meta.url));
  const result = Bun.spawnSync(["python3", script], { stdout: "pipe", stderr: "pipe" });
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  expect(result.stdout.toString() + result.stderr.toString()).not.toContain("fixture-private-upload-capability");
});
