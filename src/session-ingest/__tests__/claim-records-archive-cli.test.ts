import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
test("the claim-records-archive.ts CLI builds its owner client from the pinned helper, never a connectionString", () => {
  const src = readFileSync(new URL("../../../scripts/ops/claim-records-archive.ts", import.meta.url), "utf8").replace(/^\s*\/\/.*$/gm, "");
  expect({ conn: /connectionString/.test(src), helper: /ownerClientConfig\(/.test(src) }).toEqual({ conn: false, helper: true });
});
