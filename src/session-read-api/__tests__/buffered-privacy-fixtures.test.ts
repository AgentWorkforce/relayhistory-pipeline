/** Release-owner PR B fixture harness against the concrete owner-buffered implementation. */
import { describe, expect, test } from "bun:test";
import { createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  BufferedFenced,
  parseControlHeadResponse,
  type AuthorityHead,
  type ControlHeadGate,
  type OwnerBufferedDeps,
  type OwnerBufferedReads,
} from "../owner-buffered.ts";
import { memoryLiveness } from "./support.ts";
import {
  SESSION,
  SCOPE,
  bufferedRow,
  putStage,
  scratch,
  turn,
  unzip,
} from "./owner-buffered-support.ts";
import {
  admission,
  causal,
  consumerVisible,
  controlHeadBody,
  finalAuthority,
  firstAuthority,
  parity,
  parityVisible,
  sqlEligible,
  type CausalCase,
} from "./buffered-privacy-fixture-support.ts";

describe("release-owner fixture inventory", () => {
  test("loads all 26 causal and 16 parity cases without duplicate ids", () => {
    expect(causal.cases).toHaveLength(26);
    expect(parity.cases).toHaveLength(16);
    expect(new Set(causal.cases.map((c) => c.id)).size).toBe(26);
    expect(new Set(parity.cases.map((c) => c.id)).size).toBe(16);
  });
});

describe("frozen 12-key control head + immutable admission", () => {
  for (const c of causal.cases) {
    test(c.id, () => {
      expect(consumerVisible(c)).toBe(c.required.finalBufferedContentEmitted);
    });
  }

  test("strict parser refuses extras, malformed fields, and versions above int64", () => {
    const fixture = causal.cases.find((c) => c.id === "healthy-bounded-row-positive")!;
    const mutations: Array<(value: Record<string, any>) => void> = [
      (v) => { v.extra = true; },
      (v) => { v.controlEpoch = "A".repeat(64); },
      (v) => { v.privacyBaselineSha256 = "short"; },
      (v) => { v.sourceControlVersion = "9223372036854775808"; },
      (v) => { v.sourceControlDigest = null; },
      (v) => { v.sourceControlKind = null; },
      (v) => { delete v.tenant.userId; },
    ];
    for (const mutate of mutations) {
      const value = structuredClone(controlHeadBody(fixture));
      mutate(value);
      expect(() => parseControlHeadResponse(value, fixture.candidate.scope)).toThrow(BufferedFenced);
    }
  });
});

describe("release-owner G2 parity matrix pinned for 043700", () => {
  for (const c of parity.cases) {
    test(c.id, () => expect(parityVisible(c)).toBe(c.emitBuffered));
  }
  test("v5.3 privacy baseline mismatch is hidden", () => {
    expect(parityVisible({ id: "v5.3-baseline", emitBuffered: false,
      patch: { "proof.privacyBaselineSha256": "9".repeat(64) } })).toBe(false);
  });
  test("v5.3 head kind mismatch is hidden", () => {
    expect(parityVisible({ id: "v5.3-kind", emitBuffered: false,
      patch: { "proof.head.kind": "reset" } })).toBe(false);
  });
});

const ID: VerifiedIdentity = { ...SCOPE, scopes: ["rth:read"] };
const summaryKeys = [
  "eventCount", "firstTs", "kinds", "lastTs", "machineIds", "models", "projectId", "sessionId",
  "source", "summary", "summarySource", "taskRefs", "taskTitle", "title", "totalCacheCreate1hTokens",
  "totalCacheCreate5mTokens", "totalCacheCreateTokens", "totalCacheReadTokens", "totalCostUsdMicros",
  "totalInputTokens", "totalOutputTokens", "totalReasoningTokens", "userIds", "workState",
].sort();

async function request(c: CausalCase, path: string) {
  const scratchStore = scratch();
  const calls: string[] = [];
  try {
    const witness = admission(c);
    const stored = await putStage(scratchStore.store, 1, [turn(0)], {
      admission: { epoch: witness.admission_epoch, version: witness.admission_version },
    });
    const stage = { ...stored.row, ...witness };
    const reads: OwnerBufferedReads = {
      async list() {
        calls.push("rpc-list");
        return sqlEligible(c) ? [bufferedRow({ ...witness })] : [];
      },
      async stages() {
        calls.push("rpc-stages");
        return sqlEligible(c) ? [stage] : [];
      },
    };
    const answers: Array<AuthorityHead | Error> = [firstAuthority(c), finalAuthority(c)];
    let observations = 0;
    const gate: ControlHeadGate = {
      async observe() {
        calls.push(`gate-${observations + 1}`);
        const answer = answers[Math.min(observations++, answers.length - 1)]!;
        if (answer instanceof Error) throw answer;
        return answer;
      },
    };
    const observedStore = new Proxy(scratchStore.store as any, {
      get(target, property) {
        const value = Reflect.get(target, property, target);
        if (property === "getObject") return async (...args: unknown[]) => {
          calls.push("s3");
          return value.apply(target, args);
        };
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as OwnerBufferedDeps["store"];
    const ownerBuffered: OwnerBufferedDeps = {
      enabled: true,
      workspaces: new Set([SCOPE.workspaceId]),
      reads,
      gate,
      store: observedStore,
      decompress: unzip,
    };
    const app = createReadApi({
      verify: async () => ID,
      ch: { database: "fixture", async command() {}, async insert() {}, async select<T>() { return [] as T[]; } },
      liveness: memoryLiveness().liveness,
      completeSinceMs: 0,
      directoryWorkspaces: new Set([SCOPE.workspaceId]),
      directory: { async list() { return []; }, async activity() { return []; } },
      turns: { async harnesses() { return []; }, async source() { throw new Error("canonical source must not run"); } },
      ownerBuffered,
    });
    const response = await app.request(path, { headers: { authorization: "Bearer fixture" } });
    return { response, body: await response.json() as Record<string, any>, calls };
  } finally {
    scratchStore.done();
  }
}

function expectPostS3(calls: string[]) {
  expect(calls.indexOf("s3")).toBeGreaterThanOrEqual(0);
  expect(calls.indexOf("gate-2")).toBeGreaterThan(calls.indexOf("s3"));
}

describe("all 26 causal vectors through buffered list, turns, and metadata", () => {
  for (const c of causal.cases) {
    test(c.id, async () => {
      const visible = c.required.finalBufferedContentEmitted;
      let result = await request(c, `/v1/sessions?workspace=${SCOPE.workspaceId}`);
      expect(result.response.status).toBe(200);
      const listed = result.body.sessions.find((s: any) => s.sessionId === SESSION);
      expect(Boolean(listed)).toBe(visible);
      if (visible) {
        expect(Object.keys(listed).sort()).toEqual(summaryKeys);
        expect(listed).toMatchObject({ sessionId: SESSION, source: "claude", userIds: [SCOPE.userId] });
        expect(result.response.headers.get("x-relayhistory-buffered-ids")).toBe(`claude:${SESSION}`);
      }

      result = await request(c, `/v1/sessions/${SESSION}/turns?workspace=${SCOPE.workspaceId}&source=claude`);
      if (visible) {
        expect(result.response.status).toBe(200);
        expect(result.body.turns.map((row: any) => row.content)).toEqual(["buffered 0"]);
        expect(result.response.headers.get("x-relayhistory-buffered")).toBe("1");
      } else {
        expect([501, 503]).toContain(result.response.status);
        expect(JSON.stringify(result.body)).not.toContain("buffered 0");
      }
      if (sqlEligible(c)) expectPostS3(result.calls);

      result = await request(c, `/v1/sessions/${SESSION}/metadata?workspace=${SCOPE.workspaceId}&source=claude`);
      if (visible) {
        expect(result.response.status).toBe(200);
        expect(result.body).toMatchObject({ nativeCli: "claude", sessionOwner: SCOPE.userId });
        expect(result.response.headers.get("x-relayhistory-buffered")).toBe("1");
      } else {
        expect([404, 501, 503]).toContain(result.response.status);
        expect(JSON.stringify(result.body)).not.toContain("buffered 0");
      }
      if (sqlEligible(c)) expectPostS3(result.calls);
    });
  }
});
