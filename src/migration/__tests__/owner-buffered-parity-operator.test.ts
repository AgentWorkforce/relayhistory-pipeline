import { describe, expect, test } from "bun:test";
import type { ParityCensusReceipt } from "../owner-buffered-parity.ts";
import {
  OwnerBufferedOperatorRefusal,
  parseOperatorArgs,
  publicCensusReceipt,
} from "../../../scripts/owner-buffered-parity-operator.ts";

const SHA = "a".repeat(40);
const GENERATION = "b".repeat(64);

describe("owner-buffered parity operator arguments", () => {
  test("pins census to source and target generation", () => {
    expect(parseOperatorArgs([
      "census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION,
    ])).toEqual({ mode: "census", expectedSourceSha: SHA, expectedTargetGeneration: GENERATION });
  });

  test("requires the exact explicit G2 confirmation", () => {
    expect(() => parseOperatorArgs([
      "write", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION,
    ])).toThrow(OwnerBufferedOperatorRefusal);
    expect(parseOperatorArgs([
      "write", "--confirm-write", "owner-buffered-parity/v1",
      "--expect-target-generation", GENERATION, "--expect-source-sha", SHA,
    ]).mode).toBe("write");
  });

  test("rejects duplicate, unknown, malformed, and census write flags", () => {
    for (const argv of [
      ["census", "--expect-source-sha", SHA, "--expect-source-sha", SHA, "--expect-target-generation", GENERATION],
      ["census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, "--other", "x"],
      ["census", "--expect-source-sha", "short", "--expect-target-generation", GENERATION],
      ["census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, "--confirm-write", "owner-buffered-parity/v1"],
    ]) expect(() => parseOperatorArgs(argv)).toThrow(OwnerBufferedOperatorRefusal);
  });
});

test("public receipt hashes and removes every tenant identifier", () => {
  const receipt: ParityCensusReceipt = {
    contract: "owner-buffered-parity-census/v1",
    tenant: { orgId: "private-org", workspaceId: "private-workspace", userId: "private-user" },
    equal: true,
    mismatches: [],
    proof: {
      contract: "owner-buffered-parity/v1",
      tenant: { orgId: "private-org", workspaceId: "private-workspace", userId: "private-user" },
      controlEpoch: "1".repeat(64),
      privacyBaselineSha256: "2".repeat(64),
      explicitNoHead: false,
      head: { epoch: "1".repeat(64), version: "7", digest: "3".repeat(64), kind: "selection" },
      deletionSetSha256: "4".repeat(64),
      resetSetSha256: "5".repeat(64),
      targetLineageDigest: "6".repeat(64),
      targetGeneration: GENERATION,
    },
  };
  const publicReceipt = publicCensusReceipt(receipt);
  const serialized = JSON.stringify(publicReceipt);
  expect(publicReceipt.tenantSha256).toMatch(/^[0-9a-f]{64}$/);
  expect(serialized).not.toContain("private-org");
  expect(serialized).not.toContain("private-workspace");
  expect(serialized).not.toContain("private-user");
  expect(serialized).not.toContain('"tenant"');
  expect(publicCensusReceipt(receipt)).toEqual(publicReceipt);
});

test("public mismatch receipt remains diagnostic without exposing the tenant", () => {
  const receipt: ParityCensusReceipt = {
    contract: "owner-buffered-parity-census/v1",
    tenant: { orgId: "private-org", workspaceId: "private-workspace", userId: "private-user" },
    equal: false,
    mismatches: ["deletion_set_mismatch", "reset_set_mismatch"],
    proof: null,
  };
  const serialized = JSON.stringify(publicCensusReceipt(receipt));
  expect(serialized).toContain("deletion_set_mismatch");
  expect(serialized).toContain("reset_set_mismatch");
  expect(serialized).not.toContain("private-");
});
