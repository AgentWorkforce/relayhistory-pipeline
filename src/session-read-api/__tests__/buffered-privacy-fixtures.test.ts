/**
 * PR B release-owner fixture harness.
 *
 * This is intentionally red on the 043700 scaffold: the registry implementation
 * fills `buffered-privacy.ts` and wires its `BufferedReads` seam into createReadApi.
 * The fixture JSON is consumed in place from the release-owner commits, so an
 * implementation cannot silently weaken or hand-pick the cases.
 */
import { describe, expect, test } from "bun:test";
import { createReadApi, type VerifiedIdentity } from "../app.ts";
import {
  bufferedParityProofCurrent,
  bufferedVisibleAfterFetch,
  type BufferedReads,
} from "../buffered-privacy.ts";
import { causal, gateInput, parity, parityState, type CausalCase } from "./buffered-privacy-fixture-support.ts";

describe("release-owner fixture inventory", () => {
  test("loads all 26 causal and 16 parity cases without duplicate ids", () => {
    expect(causal.cases).toHaveLength(26);
    expect(parity.cases).toHaveLength(16);
    expect(new Set(causal.cases.map((c) => c.id)).size).toBe(26);
    expect(new Set(parity.cases.map((c) => c.id)).size).toBe(16);
  });
});
describe("control-head v5 causal gate", () => {
  for (const c of causal.cases) {
    test(c.id, () => {
      expect(bufferedVisibleAfterFetch(gateInput(c))).toBe(c.required.finalBufferedContentEmitted);
    });
  }
  test("the 200 body is exact: extras, malformed fields and versions above int64 are hidden", () => {
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
      const input = structuredClone(gateInput(fixture));
      const value = (input.controlHead as { ok: true; value: Record<string, any> }).value;
      mutate(value);
      expect(bufferedVisibleAfterFetch(input)).toBe(false);
    }
  });
});

describe("G2 parity proof gate", () => {
  for (const c of parity.cases) {
    test(c.id, () => {
      expect(bufferedParityProofCurrent(parityState(c))).toBe(c.emitBuffered);
    });
  }
  test("v5 privacy baseline mismatch is hidden", () => {
    const state = parityState(parity.cases[0]!);
    (state.proof as Record<string, unknown>).privacyBaselineSha256 = "f".repeat(64);
    expect(bufferedParityProofCurrent(state)).toBe(false);
  });
  test("v5 source control kind mismatch is hidden", () => {
    const state = parityState(parity.cases[0]!);
    ((state.proof as Record<string, any>).head as Record<string, unknown>).kind = "reset";
    expect(bufferedParityProofCurrent(state)).toBe(false);
  });
});

const clickhouse = {
  database: "fixture",
  async command() {},
  async insert() {},
  async select<T>() { return [] as T[]; },
};
const liveness = {
  async erased() { return new Set<string>(); },
  async livePointerSessions() { return new Set<string>(); },
  async sessionStart() { return { pointers: 0, live: 0, createdAtMs: null }; },
};

function bufferedDeps(c: CausalCase, calls: string[]): BufferedReads {
  const input = gateInput(c);
  const row = {
    source: c.candidate.scope.source,
    sessionId: c.candidate.scope.sessionId,
    firstTs: "2026-11-05T12:00:00.000Z",
    lastTs: "2026-11-05T12:00:01.000Z",
    stageCount: 1,
    bytes: "256",
    userId: c.candidate.scope.userId,
  };
  return {
    async list() { calls.push("list"); return [row]; },
    async stages() {
      calls.push("stages");
      return [{ batchId: "fixture-batch", key: "fixture-stage", sha256: "a".repeat(64), bytes: 256,
        groupIndexes: [0], keptOrdinals: [0], controlEpoch: input.admission.controlEpoch,
        sourceControlVersion: input.admission.sourceControlVersion, parity: input.parity, targetEligible: input.targetEligible }];
    },
    async readStage() {
      calls.push("s3");
      return { rows: [{
        id: "00000000-0000-4000-8000-000000000001", org_id: c.candidate.scope.orgId,
        workspace_id: c.candidate.scope.workspaceId, source: c.candidate.scope.source,
        session_id: c.candidate.scope.sessionId, turn_index: 0, session_owner: c.candidate.scope.userId,
        role: "user", content: "buffered fixture turn", actor_name: "Ada", actor_role: "owner",
        metadata: { nativeCli: c.candidate.scope.source }, ts: "2026-11-05T12:00:00.123Z",
      }] };
    },
    async controlHead() { calls.push("head"); return input.controlHead; },
    deadlineMs: 2000,
  };
}

function api(c: CausalCase, calls: string[]) {
  const identity: VerifiedIdentity = { ...c.candidate.scope, scopes: ["rth:read"] };
  return createReadApi({
    verify: async () => identity,
    ch: clickhouse,
    liveness,
    completeSinceMs: 0,
    directoryWorkspaces: "*",
    directory: { async list() { return []; }, async activity() { return []; } },
    turns: { async harnesses() { return []; }, async source() { throw new Error("canonical source must not run"); } },
    buffered: bufferedDeps(c, calls),
  });
}

/** A control-head check is useful only if it happens after the integrity-checked object read. */
function expectPostS3(calls: string[]) {
  expect(calls.indexOf("s3")).toBeGreaterThanOrEqual(0);
  expect(calls.indexOf("head")).toBeGreaterThan(calls.indexOf("s3"));
}

describe("every causal vector is enforced on buffered list, turns and metadata", () => {
  for (const c of causal.cases) {
    test(c.id, async () => {
      const visible = c.required.finalBufferedContentEmitted;
      const q = `workspace=${encodeURIComponent(c.candidate.scope.workspaceId)}`;

      let calls: string[] = [];
      let r = await api(c, calls).request(`/v1/sessions?${q}`, { headers: { authorization: "Bearer fixture" } });
      expect(r.status).toBe(200);
      let body = await r.json() as any;
      const listed = body.sessions.find((s: any) => s.sessionId === c.candidate.scope.sessionId);
      expect(Boolean(listed)).toBe(visible);
      if (visible) {
        expect(listed).toEqual({
          title: null, sessionId: c.candidate.scope.sessionId, userIds: [c.candidate.scope.userId], source: c.candidate.scope.source,
          projectId: null, machineIds: [], firstTs: "2026-11-05T12:00:00.000Z", lastTs: "2026-11-05T12:00:01.000Z",
          workState: null, eventCount: 1, kinds: [], models: [], taskTitle: null, summary: null, summarySource: null, taskRefs: [],
          totalCostUsdMicros: null, totalInputTokens: 0, totalOutputTokens: 0, totalReasoningTokens: 0,
          totalCacheReadTokens: 0, totalCacheCreateTokens: 0, totalCacheCreate5mTokens: null, totalCacheCreate1hTokens: null,
        });
        expect(r.headers.get("x-relayhistory-buffered-ids")).toContain(`${c.candidate.scope.source}:${c.candidate.scope.sessionId}`);
      }
      if (gateInput(c).targetEligible) expectPostS3(calls);

      calls = [];
      r = await api(c, calls).request(`/v1/sessions/${c.candidate.scope.sessionId}/turns?${q}`, { headers: { authorization: "Bearer fixture" } });
      body = await r.json() as any;
      expect(r.status).not.toBe(501); // 501 would make Cloud fall back to the authority we just refused.
      if (visible) {
        expect(r.status).toBe(200);
        expect(body.turns.map((t: any) => t.content)).toEqual(["buffered fixture turn"]);
        expect(r.headers.get("x-relayhistory-buffered")).toBe("1");
      } else expect(JSON.stringify(body)).not.toContain("buffered fixture turn");
      if (gateInput(c).targetEligible) expectPostS3(calls);

      calls = [];
      r = await api(c, calls).request(`/v1/sessions/${c.candidate.scope.sessionId}/metadata?${q}`, { headers: { authorization: "Bearer fixture" } });
      body = await r.json() as any;
      expect(r.status).not.toBe(501);
      if (visible) {
        expect(r.status).toBe(200);
        expect(body).toMatchObject({ nativeCli: c.candidate.scope.source, sessionOwner: c.candidate.scope.userId });
        expect(r.headers.get("x-relayhistory-buffered")).toBe("1");
      } else {
        expect([404, 503]).toContain(r.status);
        expect(JSON.stringify(body)).not.toContain("buffered fixture turn");
      }
      if (gateInput(c).targetEligible) expectPostS3(calls);
    });
  }
});
