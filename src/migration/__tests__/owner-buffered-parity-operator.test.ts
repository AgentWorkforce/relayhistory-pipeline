import { describe, expect, test } from "bun:test";
import { ParityCensusRefusal, type ParityCensusReceipt, type ParityTenant } from "../owner-buffered-parity.ts";
import {
  BULK_BUFFERED_TENANTS_SQL,
  OwnerBufferedOperatorRefusal,
  parseOperatorArgs,
  publicCensusReceipt,
  runBulkParity,
} from "../../../scripts/owner-buffered-parity-operator.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { PGlite } from "@electric-sql/pglite";

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

  test("bulk requires its distinct confirmation and clamps the bounded batch", () => {
    expect(parseOperatorArgs([
      "bulk", "--confirm-write", "owner-buffered-parity-bulk/v1", "--batch-size", "100",
      "--expect-target-generation", GENERATION, "--expect-source-sha", SHA,
    ])).toEqual({ mode: "bulk", expectedSourceSha: SHA, expectedTargetGeneration: GENERATION, batchSize: 100 });
    for (const size of ["0", "101", "1.5", "NaN"]) expect(() => parseOperatorArgs([
      "bulk", "--confirm-write", "owner-buffered-parity-bulk/v1", "--batch-size", size,
      "--expect-target-generation", GENERATION, "--expect-source-sha", SHA,
    ])).toThrow(OwnerBufferedOperatorRefusal);
    expect(() => parseOperatorArgs([
      "bulk", "--confirm-write", "owner-buffered-parity/v1",
      "--expect-target-generation", GENERATION, "--expect-source-sha", SHA,
    ])).toThrow(OwnerBufferedOperatorRefusal);
  });

  test("rejects duplicate, unknown, malformed, and census write flags", () => {
    for (const argv of [
      ["census", "--expect-source-sha", SHA, "--expect-source-sha", SHA, "--expect-target-generation", GENERATION],
      ["census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, "--other", "x"],
      ["census", "--expect-source-sha", "short", "--expect-target-generation", GENERATION],
      ["census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, "--confirm-write", "owner-buffered-parity/v1"],
      ["write", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION,
        "--confirm-write", "owner-buffered-parity/v1", "--batch-size", "2"],
    ]) expect(() => parseOperatorArgs(argv)).toThrow(OwnerBufferedOperatorRefusal);
  });
});

const tenant = (n: number): ParityTenant => ({ orgId: `private-org-${n}`, workspaceId: `private-ws-${n}`, userId: `private-user-${n}` });
const row = (n: number) => ({ org_id: `private-org-${n}`, workspace_id: `private-ws-${n}`, user_id: `private-user-${n}` });
const equalReceipt = (t: ParityTenant): ParityCensusReceipt => ({
  contract: "owner-buffered-parity-census/v1", tenant: t, equal: true, mismatches: [],
  proof: {
    contract: "owner-buffered-parity/v1", tenant: t, controlEpoch: "1".repeat(64),
    privacyBaselineSha256: "2".repeat(64), explicitNoHead: true, head: null,
    deletionSetSha256: "3".repeat(64), resetSetSha256: "4".repeat(64),
    targetLineageDigest: "5".repeat(64), targetGeneration: GENERATION,
  },
});

function enumerator(pages: Array<Array<Record<string, unknown>>>, statements: string[]): Queryable {
  return { async query<T>(sql: string) {
    statements.push(sql);
    if (sql.startsWith("FETCH FORWARD")) return { rows: (pages.shift() ?? []) as T[] };
    return { rows: [] as T[] };
  } };
}

function targetOwner(): Queryable {
  return { async query<T>(sql: string) {
    if (sql.includes("__96bulls_role_validation")) return { rows: [{ generation: GENERATION }] as T[] };
    return { rows: [] as T[] };
  } };
}

describe("owner-buffered parity bulk runner", () => {
  test("refuses the whole run before enumeration when the generation pin is stale", async () => {
    const statements: string[] = [];
    const staleTarget: Queryable = { async query<T>() {
      return { rows: [{ generation: "9".repeat(64) }] as T[] };
    } };
    await expect(runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) }, enumerator([[row(1)]], statements),
      staleTarget, GENERATION, 1,
    )).rejects.toThrow("bulk_target_generation_mismatch");
    expect(statements).toEqual([]);
  });

  test("real PostgreSQL cursor enumerates each distinct live buffered tenant once", async () => {
    const db = new PGlite();
    try {
      await db.exec(`CREATE SCHEMA sessions;
        CREATE TABLE sessions.ingest_buffer_outbox(
          org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,state text NOT NULL);
        INSERT INTO sessions.ingest_buffer_outbox VALUES
          ('private-org-1','private-ws-1','private-user-1','buffered'),
          ('private-org-1','private-ws-1','private-user-1','buffered'),
          ('private-org-2','private-ws-2','private-user-2','buffered'),
          ('private-org-3','private-ws-3','private-user-3','erased');`);
      const queryable: Queryable = { query: async <T>(sql: string, params?: unknown[]) =>
        (params?.length ? db.query(sql, params as never) : db.query(sql)) as Promise<{ rows: T[] }> };
      const seen: string[] = [];
      const counts = await runBulkParity(
        { query: async <T>() => ({ rows: [] as T[] }) }, queryable, targetOwner(), GENERATION, 1,
        {
          readCensus: async (_n, _t, candidate) => { seen.push(candidate.userId); return equalReceipt(candidate); },
          writeProof: async (_n, _t, candidate) => ({
            census: equalReceipt(candidate), outcome: "insert", writtenAt: "now",
          }),
        },
      );
      expect(seen).toEqual(["private-user-1", "private-user-2"]);
      expect(counts).toMatchObject({ batches: 2, candidates: 2, inserted: 2 });
    } finally {
      await db.close();
    }
  });

  test("uses a stable read-only cursor, bounded pages, and counts-only results", async () => {
    const statements: string[] = [];
    const outcomes = new Map([["private-user-1", "insert"], ["private-user-2", "update"], ["private-user-3", "unchanged"]] as const);
    const counts = await runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) },
      enumerator([[row(1), row(2)], [row(3)], []], statements), targetOwner(), GENERATION, 2,
      {
        readCensus: async (_n, _t, candidate) => equalReceipt(candidate),
        writeProof: async (_n, _t, candidate) => ({
          census: equalReceipt(candidate), outcome: outcomes.get(candidate.userId)!, writtenAt: "now",
        }),
      },
    );
    expect(counts).toEqual({ batches: 2, candidates: 3, g1Equal: 3, g1Mismatch: 0, tenantRefused: 0,
      inserted: 1, updated: 1, unchanged: 1 });
    expect(statements[0]).toContain("REPEATABLE READ READ ONLY");
    expect(statements.some((sql) => sql.includes(BULK_BUFFERED_TENANTS_SQL))).toBeTrue();
    expect(statements.filter((sql) => sql === "FETCH FORWARD 2 FROM owner_buffered_parity_bulk_tenants")).toHaveLength(3);
    expect(statements.at(-1)).toBe("COMMIT");
    expect(JSON.stringify(counts)).not.toContain("private-");
  });

  test("a mismatch or tenant refusal skips only that tenant", async () => {
    const seen: string[] = [];
    const counts = await runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) }, enumerator([[row(1), row(2), row(3)], []], []),
      targetOwner(), GENERATION, 3,
      {
        readCensus: async (_n, _t, candidate) => candidate.userId.endsWith("1")
          ? { ...equalReceipt(candidate), equal: false, proof: null, mismatches: ["deletion_set_mismatch"] }
          : equalReceipt(candidate),
        writeProof: async (_n, _t, candidate) => {
          seen.push(candidate.userId);
          if (candidate.userId.endsWith("2")) throw new ParityCensusRefusal("insert_conflict");
          return { census: equalReceipt(candidate), outcome: "insert", writtenAt: "now" };
        },
      },
    );
    expect(seen).toEqual(["private-user-2", "private-user-3"]);
    expect(counts).toMatchObject({ candidates: 3, g1Equal: 2, g1Mismatch: 1, tenantRefused: 1, inserted: 1 });
  });

  test("generation drift aborts the run and rolls back enumeration", async () => {
    const statements: string[] = [];
    let writes = 0;
    await expect(runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) }, enumerator([[row(1), row(2)], []], statements),
      targetOwner(), GENERATION, 2,
      {
        readCensus: async (_n, _t, candidate) => candidate.userId.endsWith("2")
          ? { ...equalReceipt(candidate), proof: { ...equalReceipt(candidate).proof!, targetGeneration: "9".repeat(64) } }
          : equalReceipt(candidate),
        writeProof: async (_n, _t, candidate) => {
          writes++;
          return { census: equalReceipt(candidate), outcome: "insert", writtenAt: "now" };
        },
      },
    )).rejects.toThrow("bulk_target_generation_mismatch");
    expect(writes).toBe(1);
    expect(statements.at(-1)).toBe("ROLLBACK");
  });

  test("generation loss is global even when the current tenant census is malformed", async () => {
    const statements: string[] = [];
    let generationReads = 0;
    const movingTarget: Queryable = { async query<T>(sql: string) {
      if (sql.includes("__96bulls_role_validation")) {
        generationReads++;
        return { rows: (generationReads < 3 ? [{ generation: GENERATION }] : []) as T[] };
      }
      return { rows: [] as T[] };
    } };
    await expect(runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) }, enumerator([[row(1)], []], statements),
      movingTarget, GENERATION, 1,
      { readCensus: async (_n, _t, candidate) => ({
        ...equalReceipt(candidate), equal: false, proof: null, mismatches: ["invalid_shape"],
      }) },
    )).rejects.toThrow("bulk_target_generation_mismatch");
    expect(statements.at(-1)).toBe("ROLLBACK");
  });

  test("rerun is resumable through unchanged outcomes without a private cursor", async () => {
    let pass = 0;
    const run = () => runBulkParity(
      { query: async <T>() => ({ rows: [] as T[] }) }, enumerator([[row(1), row(2)], []], []),
      targetOwner(), GENERATION, 2,
      {
        readCensus: async (_n, _t, candidate) => equalReceipt(candidate),
        writeProof: async (_n, _t, candidate) => ({
          census: equalReceipt(candidate), outcome: pass === 0 ? "insert" : "unchanged", writtenAt: "now",
        }),
      },
    );
    expect(await run()).toMatchObject({ inserted: 2, unchanged: 0 });
    pass++;
    expect(await run()).toMatchObject({ inserted: 0, unchanged: 2 });
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
