import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
test("the claim-records-archive.ts CLI builds its owner client from the pinned helper, never a connectionString", () => {
  const src = readFileSync(new URL("../../../scripts/ops/claim-records-archive.ts", import.meta.url), "utf8").replace(/^\s*\/\/.*$/gm, "");
  expect({ conn: /connectionString/.test(src), helper: /ownerClientConfig\(/.test(src) }).toEqual({ conn: false, helper: true });
});
test("the CLI's failure reason is a closed redacted code, never a message that can carry a host", async () => {
  const { redactedReason } = await import("../../../scripts/ops/claim-records-archive-reason.ts");
  const { ClaimRecordsArchiveError } = await import("../claim-records-archive.ts");
  expect([
    redactedReason(new ClaimRecordsArchiveError("archive_readback")),
    redactedReason(Object.assign(new Error("connect to postgres://x@host/db failed"), { code: "42501" })),
    redactedReason(new Error("postgres://x@host/db")),
    redactedReason(new Error("temporary_aws_credentials_required")),
  ]).toEqual(["archive_readback", "sqlstate_42501", "internal_error", "temporary_aws_credentials_required"]);
});
