import { afterEach, expect, test } from "bun:test";
import { handler, parseSecret } from "../lambda.ts";
const before = process.env.SESSION_DERIVED_ENABLED;
afterEach(() => { if (before === undefined) delete process.env.SESSION_DERIVED_ENABLED; else process.env.SESSION_DERIVED_ENABLED = before; });
test("derived tick is off by default and returns before initialization", async () => {
  delete process.env.SESSION_DERIVED_ENABLED;
  expect(await handler({ derived: true })).toEqual({ ok: true });
});
test("armed derived tick refuses invocation without a runtime deadline", async () => {
  process.env.SESSION_DERIVED_ENABLED = "true";
  expect(await handler({ derived: true })).toEqual({ ok: false });
});
test("managed keyring validation refuses malformed input without exposing it", () => {
  expect(() => parseSecret(JSON.stringify({ hoopdb_url: "postgres://fixture", summary_master_keys: "bad-value" }))).toThrow("invalid id");
});
