import { expect, test } from "bun:test";
import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { open as openFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import {
  assertPublicationCanaryPlan,
  publicationCanaryPlanDigest,
  runPublicationCanary,
  type PublicationCanaryAuthority,
  type PublicationCanaryOutcome,
  type PublicationCanaryOutcomeStore,
  type PublicationCanaryPlan,
  type PublicationCanaryStage,
} from "../publication-canary.ts";
import { openPublicationCanaryOutcomeFile } from "../publication-canary-outcomes.ts";
import type {
  PublicationRpc,
  PublicationTicket,
} from "../publication-protocol.ts";

const IN = {
  stageToken: "11111111-1111-1111-1111-111111111111",
  tenant: { orgId: "org-a", workspaceId: "ws-a" },
  userId: "user-a",
  originId: "origin-a",
  sha256: "a".repeat(64),
  bytes: 128,
} satisfies PublicationCanaryStage;
const OUT = {
  stageToken: "22222222-2222-2222-2222-222222222222",
  tenant: { orgId: "org-b", workspaceId: "ws-b" },
  userId: "user-b",
  originId: "origin-b",
  sha256: "b".repeat(64),
  bytes: 256,
} satisfies PublicationCanaryStage;
const plan = (stages = [IN, OUT]): PublicationCanaryPlan => {
  const at = Date.now();
  return {
    version: 1,
    planId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    ownerId: "operator-a",
    createdAt: new Date(at - 1000).toISOString(),
    expiresAt: new Date(at + 60_000).toISOString(),
    tenant: IN.tenant,
    target: {
      bucket: "fixture-bucket",
      region: "us-east-1",
      keyRoot: "96bulls",
      generation: "c".repeat(64),
    },
    limits: {
      maxStages: 2,
      maxRecordsPerStage: 10,
      maxSessionsPerStage: 2,
      maxStageMs: 1000,
      maxPlanMs: 5000,
      maxReads: 10,
      maxReadBytes: 1024,
      maxPuts: 10,
      maxPutBytes: 1024,
    },
    stages,
  };
};
function ticket(stage = IN): PublicationTicket {
  const recordDigest = "e".repeat(64);
  return {
    schemaGeneration: "c".repeat(64),
    attemptToken: "33333333-3333-3333-3333-333333333333",
    leaseToken: "44444444-4444-4444-4444-444444444444",
    leaseUntil: new Date(Date.now() + 60_000).toISOString(),
    stageToken: stage.stageToken,
    stageVersion: "1",
    outboxVersion: "1",
    tenant: { ...stage.tenant, userId: stage.userId },
    originId: stage.originId,
    batchId: "batch-a",
    batchDigest: "d".repeat(64),
    claimToken: "55555555-5555-5555-5555-555555555555",
    stage: {
      key: `96bulls/${stage.tenant.orgId}/buffered-batches/v1/ws=ws-a/m=srv_11111111111111111111111111111111/b=22222222222222222222222222222222-3333333333333333-44444444444444444444444444444444.staged.ndjson.zst`,
      sha256: stage.sha256,
      bytes: stage.bytes,
      groupOrdinals: [[0]],
    },
    records: [{ ord: 0, source: "claude", session_id: "session-a", record_id: "record-a", revision_id: "revision-a", revision: 1, digest: recordDigest }],
    receiptSeats: {
      controlEpoch: "1".repeat(64),
      sourceIdentitySha256: "2".repeat(64),
      allocatedHighWater: "9007199254740993",
      lastAssignedSeat: "9007199254740994",
      records: [{ ord: 0, seat: "9007199254740994", bodyAccepted: false, candidateDigest: recordDigest, firstDigest: "f".repeat(64), resetEpoch: "0", fenceEpoch: "0" }],
    },
    acceptedOrdinals: [],
    heads: [],
  };
}
class MemoryOutcomes implements PublicationCanaryOutcomeStore {
  plan = "";
  rows = new Map<string, PublicationCanaryOutcome>();
  finished = 0;
  async begin(digest: string) {
    if (this.plan && this.plan !== digest) throw Error("plan conflict");
    this.plan = digest;
  }
  async existing() {
    return new Map(this.rows);
  }
  async record(_digest: string, outcome: PublicationCanaryOutcome) {
    const prior = this.rows.get(outcome.stageToken);
    if (prior && JSON.stringify(prior) !== JSON.stringify(outcome))
      throw Error("outcome conflict");
    this.rows.set(outcome.stageToken, outcome);
  }
  async finish() {
    this.finished++;
  }
}
function fixture(options: { mismatchTicket?: boolean; crossEligible?: boolean } = {}) {
  const calls: string[] = [];
  const outcomes = new MemoryOutcomes();
  const rpc: PublicationRpc = {
    candidates: async () => [],
    defer: async () => {},
    claim: async () => {
      throw Error("base claim must not run");
    },
    heartbeat: async () => {
      calls.push("heartbeat");
    },
    prepare: async () => {},
    register: async () => {},
    publish: async () => {
      calls.push("publish");
      return {
        published: true,
        replay: false,
        images: 0,
        records: 0,
        eligibleSessions: 0,
      };
    },
    abandon: async () => {
      calls.push("abandon");
    },
  };
  const authority: PublicationCanaryAuthority = {
    async inspect(input) {
      calls.push(`inspect:${input.stageToken}`);
      const stage = input.stageToken === IN.stageToken ? IN : OUT;
      return {
        stage,
        evidenceDigest: stage.sha256,
        decision:
          stage === OUT && !options.crossEligible ? "hold" : "eligible",
        ...(stage === OUT && !options.crossEligible
          ? { holdReason: "outside_canary_tenant" as const }
          : {}),
      };
    },
    async claim(input) {
      calls.push(`claim:${input.stageToken}`);
      return {
        binding: input,
        ticket: options.mismatchTicket
          ? ticket({ ...IN, tenant: OUT.tenant })
          : ticket(IN),
      };
    },
  };
  const raw = { backend: "fs" } as ObjectStore;
  return { calls, outcomes, rpc, authority, raw };
}

test("inspects the complete plan before claim, publishes only exact scope, and exposes cross-scope hold", async () => {
  const f = fixture();
  const p = plan();
  const result = await runPublicationCanary(p, {
    authority: f.authority,
    rpc: f.rpc,
    store: () => f.raw,
    outcomes: f.outcomes,
  });
  expect(result).toMatchObject({ planned: 2, published: 1, held: 1, refused: 0, complete: true });
  expect(f.calls.slice(0, 3)).toEqual([
    `inspect:${IN.stageToken}`,
    `inspect:${OUT.stageToken}`,
    `claim:${IN.stageToken}`,
  ]);
  expect(f.calls).not.toContain(`claim:${OUT.stageToken}`);
  expect(f.outcomes.rows.get(OUT.stageToken)).toMatchObject({
    status: "held",
    reason: "outside_canary_tenant",
  });
});

test("refuses an authority that marks cross-scope work eligible and never claims it", async () => {
  const f = fixture({ crossEligible: true });
  const result = await runPublicationCanary(plan(), {
    authority: f.authority,
    rpc: f.rpc,
    store: () => f.raw,
    outcomes: f.outcomes,
  });
  expect(result).toMatchObject({ published: 0, held: 0, refused: 2 });
  expect(f.calls).toEqual([
    `inspect:${IN.stageToken}`,
    `inspect:${OUT.stageToken}`,
  ]);
  expect(f.calls.some((call) => call.startsWith("claim:"))).toBe(false);
  expect(f.outcomes.rows.get(IN.stageToken)).toMatchObject({
    status: "refused",
    reason: "publication_canary_plan_refused",
  });
  expect(f.outcomes.rows.get(OUT.stageToken)).toMatchObject({
    status: "refused",
    reason: "publication_canary_cross_scope_eligible",
  });
});

test("a durable cross-scope refusal permanently vetoes claims after an exact crash and resume", async () => {
  const f = fixture({ crossEligible: true });
  const p = plan([OUT, IN]);
  const record = f.outcomes.record.bind(f.outcomes);
  f.outcomes.record = async (digest, outcome) => {
    await record(digest, outcome);
    if (outcome.reason === "publication_canary_cross_scope_eligible")
      throw Error("simulated crash after durable plan veto");
  };
  await expect(
    runPublicationCanary(p, {
      authority: f.authority,
      rpc: f.rpc,
      store: () => f.raw,
      outcomes: f.outcomes,
    }),
  ).rejects.toThrow("simulated crash");
  expect([...f.outcomes.rows]).toEqual([
    [
      OUT.stageToken,
      expect.objectContaining({
        reason: "publication_canary_cross_scope_eligible",
      }),
    ],
  ]);

  f.outcomes.record = record;
  f.calls.length = 0;
  const resumed = await runPublicationCanary(p, {
    authority: f.authority,
    rpc: f.rpc,
    store: () => f.raw,
    outcomes: f.outcomes,
  });
  expect(resumed).toMatchObject({
    published: 0,
    held: 0,
    refused: 2,
    complete: true,
  });
  expect(f.calls).toEqual([`inspect:${IN.stageToken}`]);
  expect(f.outcomes.rows.get(IN.stageToken)).toMatchObject({
    status: "refused",
    reason: "publication_canary_plan_refused",
  });
});

test("a persisted plan veto cannot coexist with a published outcome", async () => {
  const f = fixture();
  const p = plan([OUT, IN]);
  f.outcomes.plan = publicationCanaryPlanDigest(p);
  f.outcomes.rows.set(OUT.stageToken, {
    stageToken: OUT.stageToken,
    status: "refused",
    tenant: OUT.tenant,
    reason: "publication_canary_cross_scope_eligible",
  });
  f.outcomes.rows.set(IN.stageToken, {
    stageToken: IN.stageToken,
    status: "published",
    tenant: IN.tenant,
    records: 1,
    images: 1,
    eligibleSessions: 1,
    replay: false,
    io: { reads: 1, readBytes: 1, puts: 1, putBytes: 1 },
  });
  await expect(
    runPublicationCanary(p, {
      authority: f.authority,
      rpc: f.rpc,
      store: () => f.raw,
      outcomes: f.outcomes,
    }),
  ).rejects.toThrow("publication_canary_outcome_invalid");
  expect(f.calls).toEqual([]);
});

test("an unknown inspection decision is durably refused before claim", async () => {
  const f = fixture();
  f.authority.inspect = async (input) => {
    f.calls.push(`inspect:${input.stageToken}`);
    return { stage: IN, evidenceDigest: IN.sha256, decision: "unknown" } as never;
  };
  const result = await runPublicationCanary(plan([IN]), {
    authority: f.authority,
    rpc: f.rpc,
    store: () => f.raw,
    outcomes: f.outcomes,
  });
  expect(result).toMatchObject({ published: 0, refused: 1, complete: true });
  expect(f.calls).toEqual([`inspect:${IN.stageToken}`]);
  expect(f.outcomes.rows.get(IN.stageToken)).toMatchObject({
    status: "refused",
    reason: "publication_canary_inspection_invalid",
  });
});

test("abandons a mismatched trusted ticket before constructing a store or reading bytes", async () => {
  const f = fixture({ mismatchTicket: true });
  let stores = 0;
  const result = await runPublicationCanary(plan([IN]), {
    authority: f.authority,
    rpc: f.rpc,
    store: () => {
      stores++;
      return f.raw;
    },
    outcomes: f.outcomes,
  });
  expect(result.refused).toBe(1);
  expect(stores).toBe(0);
  expect(f.calls).toContain("abandon");
  expect(f.calls).not.toContain("publish");
});

test("refuses a trusted ticket with a different user or origin before object access", async () => {
  const valid = ticket(IN);
  for (const changed of [
    { ...valid, tenant: { ...valid.tenant, userId: "other-user" } },
    { ...valid, originId: "other-origin" },
  ]) {
    const f = fixture();
    f.authority.claim = async (input) => ({ binding: input, ticket: changed });
    let stores = 0;
    const result = await runPublicationCanary(plan([IN]), {
      authority: f.authority,
      rpc: f.rpc,
      store: () => {
        stores++;
        return f.raw;
      },
      outcomes: f.outcomes,
    });
    expect(result.refused).toBe(1);
    expect(stores).toBe(0);
    expect(f.calls).toContain("abandon");
  }
});

test("a durable prior outcome resumes without another inspect or claim", async () => {
  const f = fixture();
  const p = plan([OUT]);
  f.outcomes.plan = publicationCanaryPlanDigest(p);
  f.outcomes.rows.set(OUT.stageToken, {
    stageToken: OUT.stageToken,
    status: "held",
    reason: "outside_canary_tenant",
    tenant: OUT.tenant,
    evidenceDigest: OUT.sha256,
  });
  const result = await runPublicationCanary(p, {
    authority: f.authority,
    rpc: f.rpc,
    store: () => f.raw,
    outcomes: f.outcomes,
  });
  expect(result).toMatchObject({ held: 1, complete: true });
  expect(f.calls).toEqual([]);
});

test("plan bounds reject expiry, undersize and oversize before authority work", () => {
  const p = plan([IN]);
  expect(() => assertPublicationCanaryPlan({ ...p, expiresAt: new Date(0).toISOString() })).toThrow("publication_canary_plan_invalid");
  expect(() => assertPublicationCanaryPlan({ ...p, stages: [{ ...IN, bytes: 0 }] })).toThrow("publication_canary_plan_invalid");
  expect(() => assertPublicationCanaryPlan({ ...p, stages: [{ ...IN, bytes: 16 * 1024 * 1024 + 1 }] })).toThrow("publication_canary_plan_invalid");
});

test("fsync ledger resumes exact outcomes, locks concurrent runners and refuses a torn tail", async () => {
  const dir = mkdtempSync(join(tmpdir(), "publication-canary-"));
  const path = join(dir, "outcomes.ndjson");
  try {
    const p = plan([OUT]);
    const digest = publicationCanaryPlanDigest(p);
    const first = await openPublicationCanaryOutcomeFile(path);
    await first.begin(digest, p);
    const outcome: PublicationCanaryOutcome = {
      stageToken: OUT.stageToken,
      status: "held",
      reason: "outside_canary_tenant",
      tenant: OUT.tenant,
      evidenceDigest: OUT.sha256,
    };
    await first.record(digest, outcome);
    await expect(openPublicationCanaryOutcomeFile(path)).rejects.toThrow("publication_canary_outcome_locked");
    await first.close();
    const resumed = await openPublicationCanaryOutcomeFile(path);
    await resumed.begin(digest, p);
    expect((await resumed.existing(digest)).get(OUT.stageToken)).toEqual(outcome);
    await resumed.close();
    appendFileSync(path, "torn");
    await expect(openPublicationCanaryOutcomeFile(path)).rejects.toThrow("publication_canary_outcome_torn");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("fsync ledger completes short writes and never accepts a partial write followed by error", async () => {
  const dir = mkdtempSync(join(tmpdir(), "publication-canary-short-write-"));
  const prototypeHandle = await openFile(join(dir, "prototype"), "w+");
  const prototype = Object.getPrototypeOf(prototypeHandle) as {
    write: (...args: any[]) => Promise<{ bytesWritten: number; buffer: unknown }>;
  };
  await prototypeHandle.close();
  const originalWrite = prototype.write;
  const p = plan([OUT]);
  const digest = publicationCanaryPlanDigest(p);
  try {
    const completePath = join(dir, "complete.ndjson");
    const complete = await openPublicationCanaryOutcomeFile(completePath);
    prototype.write = async function (
      buffer: Buffer,
      offset = 0,
      length = buffer.byteLength - offset,
      position?: number | null,
    ) {
      return originalWrite.call(
        this,
        buffer,
        offset,
        Math.max(1, Math.floor(length / 2)),
        position,
      );
    };
    await complete.begin(digest, p);
    await complete.finish(digest, {
      planDigest: digest,
      planned: 1,
      published: 0,
      held: 0,
      refused: 1,
      complete: true,
    });
    prototype.write = originalWrite;
    await complete.close();
    expect(
      readFileSync(completePath, "utf8").split("\n").filter(Boolean),
    ).toHaveLength(2);
    const reopened = await openPublicationCanaryOutcomeFile(completePath);
    await reopened.begin(digest, p);
    await reopened.close();

    const failedPath = join(dir, "failed.ndjson");
    const failed = await openPublicationCanaryOutcomeFile(failedPath);
    let writes = 0;
    prototype.write = async function (
      buffer: Buffer,
      offset = 0,
      length = buffer.byteLength - offset,
      position?: number | null,
    ) {
      writes++;
      if (writes === 2) throw Error("simulated write failure");
      return originalWrite.call(
        this,
        buffer,
        offset,
        Math.max(1, Math.floor(length / 2)),
        position,
      );
    };
    await expect(failed.begin(digest, p)).rejects.toThrow("simulated write failure");
    await expect(
      failed.finish(digest, {
        planDigest: digest,
        planned: 1,
        published: 0,
        held: 0,
        refused: 1,
        complete: true,
      }),
    ).rejects.toThrow("publication_canary_outcome_plan_conflict");
    prototype.write = originalWrite;
    await failed.close();
    expect(readFileSync(failedPath, "utf8").endsWith("\n")).toBe(false);
    await expect(openPublicationCanaryOutcomeFile(failedPath)).rejects.toThrow(
      "publication_canary_outcome_torn",
    );
  } finally {
    prototype.write = originalWrite;
    rmSync(dir, { recursive: true, force: true });
  }
});
