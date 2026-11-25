/**
 * GET /v1/session-intel/synthesis?stage=2 (docs/session-intel-workspace-synthesis.md, "Serving v2"): its own flag
 * (stage2Enabled, READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED), the strict stored-v2 reader, the erase check over
 * considered + related + the kept sessions, and the v1 route unchanged.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { memoryLiveness } from "./support.ts";
import { SYNTHESIS_ALIAS_MAX_ROUNDS, storeWorkspaceSynthesis, type SessionAliasRow, type SessionAliases, type WorkspaceSynthesisSource } from "../workspace-synthesis.ts";
import { memoryArtifactStore, runWorkspaceSynthesisJob, workspaceSynthesisStage2Key } from "../../session-analysis/workspace-synthesis.ts";
import { memoryBudgetLedger, type Stage2ModelTransport } from "../../session-intel/synthesis/stage2.ts";
import { workspaceSynthesisDeps } from "../../../packages/session-read-worker/src/index.ts";
import { keyOf, type SessionKey } from "../page.ts";
import { withChild } from "../../session-analysis/__tests__/workspace-synthesis-fixtures.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohortText = readFileSync(new URL("cohort.json", FIXTURE), "utf8");
const REPLY = readFileSync(new URL("stage2-reply.json", FIXTURE), "utf8");
const golden = JSON.parse(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
const goldenV2 = JSON.parse(readFileSync(new URL("expected-synthesis-v2.json", FIXTURE), "utf8"));
const ID: VerifiedIdentity = { userId: "u", orgId: "fx-org", workspaceId: "fx-ws-synthesis", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const SINCE = "2026-11-01T00:00:00.000Z", UNTIL = "2026-11-08T00:00:00.000Z";
const WINDOW = { since: SINCE, until: UNTIL };
const PATH = `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=${SINCE}&until=${UNTIL}`;
const PATH2 = `${PATH}&stage=2`;

const transport: Stage2ModelTransport = {
  async complete(r) { return { outputText: REPLY, inputTokens: 4_000, outputTokens: 900, providerRequestId: "fx-req-1", model: r.model }; },
};

async function stored(opts: { stage2?: boolean; cohort?: unknown } = {}) {
  const store = memoryArtifactStore();
  const r = await runWorkspaceSynthesisJob({
    store, keyRoot: "fxroot", orgId: "fx-org", cohort: opts.cohort ?? JSON.parse(cohortText), options: { now: UNTIL },
    ...(opts.stage2 === false ? {} : { stage2: { flag: "true", transport, ledger: memoryBudgetLedger(), clock: () => new Date("2026-11-24T10:00:00.000Z"), trigger: "manual" as const } }),
  });
  const calls: string[] = [];
  const inner = storeWorkspaceSynthesis(store, "fxroot");
  const source: WorkspaceSynthesisSource = {
    async latest(...args) { calls.push("latest"); return inner.latest(...args); },
    async latestStage2(...args) { calls.push("latestStage2"); return inner.latestStage2!(...args); },
  };
  return { store, source, calls, job: r, v2Key: workspaceSynthesisStage2Key("fxroot", "fx-org", "fx-ws-synthesis", WINDOW, r.key) };
}

const api = (opts: { stage2Enabled?: boolean; source: WorkspaceSynthesisSource; erased?: SessionKey[] }) => {
  const live = memoryLiveness({ erased: opts.erased ?? [] });
  const app = createReadApi({
    verify, adapter: {} as never, liveness: live.liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
    workspaceSynthesis: { enabled: true, source: opts.source, ...(opts.stage2Enabled === undefined ? {} : { stage2Enabled: opts.stage2Enabled }) },
  });
  return { app, live };
};
const get = async (app: ReturnType<typeof api>["app"], path = PATH2) => {
  const res = await app.request(path, { headers: auth });
  return { status: res.status, text: await res.clone().text(), body: await res.json() as any };
};

describe("flag off", () => {
  test("stage=2 is 501 workspace_synthesis_v2_disabled and nothing is read", async () => {
    for (const stage2Enabled of [undefined, false]) {
      const s = await stored();
      const { app, live } = api({ stage2Enabled, source: s.source });
      const r = await get(app);
      expect(r.status).toBe(501);
      expect(r.body.error).toEqual({ code: "unsupported_by_backend", message: "not served by this backend: workspace_synthesis_v2_disabled", reason: "workspace_synthesis_v2_disabled" });
      expect([s.calls, live.calls.erased]).toEqual([[], []]);
    }
  });

  test("everything else is byte-identical to the v1 route (other stage values stay param_stage)", async () => {
    const s = await stored();
    const plain = api({ source: s.source });
    for (const path of [PATH, `${PATH}&stage=1`, `${PATH}&stage=2&stage=2`, `${PATH}&limit=1`]) {
      const off = await get(api({ stage2Enabled: false, source: s.source }).app, path);
      const base = await get(plain.app, path);
      expect([path, off.status, off.body.error ?? null]).toEqual([path, base.status, base.body.error ?? null]);
      if (base.status === 200) expect(off.text).toBe(base.text);
    }
    expect((await get(plain.app, `${PATH}&stage=1`)).body.error.reason).toBe("param_stage");
  });

  test("the v1 flag gates v2 too: v1 off is workspace_synthesis_disabled for stage=2", async () => {
    const s = await stored();
    const app = createReadApi({
      verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      workspaceSynthesis: { enabled: false, source: s.source, stage2Enabled: true },
    });
    expect((await get(app)).body.error.reason).toBe("workspace_synthesis_disabled");
  });
});

describe("flag on", () => {
  test("serves the stored v2 document after the erase check; no stage is still exactly v1", async () => {
    const s = await stored();
    const { app, live } = api({ stage2Enabled: true, source: s.source });
    const r = await get(app);
    expect(r.status).toBe(200);
    expect(r.body).toEqual(s.job.stage2 && s.job.stage2.outcome !== "failed" ? JSON.parse(JSON.stringify(s.job.stage2.document)) : null);
    expect(r.body.contract).toBe("workspace-synthesis/2");
    expect(r.body.stage2.initiatives.length).toBeGreaterThan(0);
    expect(r.body.orgId).toBeUndefined();
    expect(s.calls).toEqual(["latestStage2"]);
    expect(live.calls.erased.flat().length).toBe(golden.inputs.considered);
    const v1 = await get(app, PATH);
    expect([v1.status, v1.body]).toEqual([200, golden]);
  });

  test("bad stage values are 400", async () => {
    const s = await stored();
    const { app } = api({ stage2Enabled: true, source: s.source });
    for (const path of [`${PATH}&stage=1`, `${PATH}&stage=`, `${PATH}&stage=2&stage=2`]) expect((await get(app, path)).status).toBe(400);
    expect(s.calls).toEqual([]);
  });

  test("no v2 for the authoritative artifact is 404 stage2_not_found; no artifact at all is a plain 404", async () => {
    const s = await stored({ stage2: false });
    const r = await get(api({ stage2Enabled: true, source: s.source }).app);
    expect([r.status, r.body.error.code, r.body.error.reason]).toEqual([404, "not_found", "stage2_not_found"]);
    const none = await get(api({ stage2Enabled: true, source: s.source }).app, PATH2.replace(UNTIL, "2026-11-07T00:00:00.000Z"));
    expect([none.status, none.body.error.reason]).toEqual([404, undefined]);
    // A source without the v2 lookup has no v2.
    const bare: WorkspaceSynthesisSource = { latest: s.source.latest };
    expect((await get(api({ stage2Enabled: true, source: bare }).app)).body.error.reason).toBe("stage2_not_found");
  });

  test("an erased kept session is 404 synthesis_invalidated, and is never echoed", async () => {
    const s = await stored();
    const doc = s.job.stage2 && s.job.stage2.outcome !== "failed" ? s.job.stage2.document : null;
    const kept = doc!.stage2!.sessions[0]!;
    const r = await get(api({ stage2Enabled: true, source: s.source, erased: [kept] }).app);
    expect([r.status, r.body.error.reason]).toEqual([404, "synthesis_invalidated"]);
    expect(r.text).not.toContain(kept.sessionId);
  });

  test("the kept sessions are checked directly, not assumed covered by considered", async () => {
    const s = await stored();
    const found = await s.source.latestStage2!("fx-org", "fx-ws-synthesis", WINDOW);
    const outside = { source: "fx-claude", sessionId: "fx-kept-outside" };
    const v2 = structuredClone(found!.v2!);
    v2.synthesis.stage2!.sessions = [...v2.synthesis.stage2!.sessions, outside];
    const source: WorkspaceSynthesisSource = { latest: s.source.latest, async latestStage2() { return { v1: found!.v1, v2 }; } };
    expect((await get(api({ stage2Enabled: true, source }).app)).status).toBe(200);
    const r = await get(api({ stage2Enabled: true, source, erased: [outside] }).app);
    expect([r.status, r.body.error.reason]).toEqual([404, "synthesis_invalidated"]);
  });

  test("a stored v2 that fails the strict reader is a typed 500 with the reader's code; never served", async () => {
    const tamper = async (edit: (a: any) => void, code: string) => {
      const s = await stored();
      const a = JSON.parse(new TextDecoder().decode(s.store.objects.get(s.v2Key)!));
      edit(a);
      s.store.objects.set(s.v2Key, new TextEncoder().encode(`${JSON.stringify(a)}\n`));
      const r = await get(api({ stage2Enabled: true, source: s.source }).app);
      expect([code, r.status, r.body.error.code, r.body.error.reason]).toEqual([code, 500, "internal", code]);
      expect(r.text).not.toContain("initiative:");
    };
    await tamper((a) => { a.synthesis.stage2.initiatives[0].title = "altered"; }, "synthesis_stage2_digest");
    await tamper((a) => { a.v1Sha256 = "0".repeat(64); }, "synthesis_stage2_digest");
    await tamper((a) => { a.extra = 1; }, "synthesis_stage2_invalid");
    const s = await stored();
    s.store.objects.set(s.v2Key, new TextEncoder().encode("not json"));
    const r = await get(api({ stage2Enabled: true, source: s.source }).app);
    expect([r.status, r.body.error.reason]).toEqual([500, "synthesis_stage2_invalid"]);
    // Another org's v2 envelope is the tenant refusal.
    await (async () => {
      const t = await stored();
      const a = JSON.parse(new TextDecoder().decode(t.store.objects.get(t.v2Key)!));
      t.store.objects.set(t.v2Key, new TextEncoder().encode(`${JSON.stringify({ ...a, orgId: "fx-org-other" })}\n`));
      const x = await get(api({ stage2Enabled: true, source: t.source }).app);
      expect([x.status, x.body.error.code, x.body.error.reason]).toEqual([500, "internal", undefined]);
    })();
  });

  test("the reader's detail never reaches the response: the reason is the code only", async () => {
    const s = await stored();
    const a = JSON.parse(new TextDecoder().decode(s.store.objects.get(s.v2Key)!));
    s.store.objects.set(s.v2Key, new TextEncoder().encode(`${JSON.stringify({ ...a, workspace: "fx-ws-other" })}\n`));
    const r = await get(api({ stage2Enabled: true, source: s.source }).app);
    expect([r.status, r.body.error]).toEqual([500, { code: "internal", message: "the stored Stage 2 workspace synthesis failed verification", reason: "synthesis_stage2_invalid" }]);
    expect(r.text).not.toContain("envelope");
    expect(r.text).not.toContain("fx-ws-other");
  });

  test("a v2 envelope of another org (or workspace) handed back for the right key is the 500 tenant refusal, never served", async () => {
    const s = await stored();
    const found = await s.source.latestStage2!("fx-org", "fx-ws-synthesis", WINDOW);
    for (const forged of [{ ...found!.v2!, orgId: "fx-org-other" }, { ...found!.v2!, workspace: "fx-ws-other" }]) {
      const source: WorkspaceSynthesisSource = { latest: s.source.latest, async latestStage2() { return { v1: found!.v1, v2: forged }; } };
      const { app, live } = api({ stage2Enabled: true, source });
      const r = await get(app);
      expect([r.status, r.body.error.code, r.body.error.reason]).toEqual([500, "internal", undefined]);
      expect(r.text).not.toContain("initiative:");
      expect(live.calls.erased).toEqual([]);
    }
  });

  test("a failing erase check is a 503", async () => {
    const s = await stored();
    const app = createReadApi({
      verify, adapter: {} as never, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      liveness: { ...memoryLiveness().liveness, async erased() { throw new Error("tombstones unavailable"); } },
      workspaceSynthesis: { enabled: true, source: s.source, stage2Enabled: true },
    });
    expect((await get(app)).status).toBe(503);
  });
});

describe("Worker wiring", () => {
  test("stage2Enabled only for exactly \"true\"", () => {
    expect(workspaceSynthesisDeps({} as never)).toEqual({ enabled: true });
    for (const v of ["", "1", "TRUE", " true"]) expect(workspaceSynthesisDeps({ READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED: v } as never)).toEqual({ enabled: true });
    expect(workspaceSynthesisDeps({ READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED: "true" } as never)).toEqual({ enabled: true, stage2Enabled: true });
  });
});

/**
 * stage=2 with the operator erase fence bound (self-host SESSION_ERASE_FENCE_ENFORCED=1) as the Lambda binds it:
 * operatorErasedBatch beside operatorErased, and the alias reader. Ordinary liveness has no tombstone: an operator
 * erase is in progress.
 */
describe("stage=2: the operator erase fence", () => {
  const fencedApi = (opts: {
    source: WorkspaceSynthesisSource;
    fence?: SessionKey[];
    outage?: boolean;
    aliasRows?: Array<{ harness: string; session_id: string; canonical_session_id: string | null; org_id?: string; workspace_id?: string }>;
    aliases?: SessionAliases;
    noAliases?: boolean;
  }) => {
    const live = memoryLiveness();
    const fence = new Set((opts.fence ?? []).map(keyOf));
    const batches: SessionKey[][] = [];
    const aliases: SessionAliases = async (orgId, workspaceId, keys) => {
      const asked = new Set(keys.map(keyOf));
      // The bound tenant's rows only (the role's RLS); a row without a tenant is the caller's.
      return (opts.aliasRows ?? [])
        .map((r): SessionAliasRow => ({ ...r, org_id: r.org_id ?? orgId, workspace_id: r.workspace_id ?? workspaceId }))
        .filter((r) => r.org_id === orgId && r.workspace_id === workspaceId)
        .filter((r) => asked.has(keyOf({ source: r.harness, sessionId: r.session_id })) ||
          (r.canonical_session_id !== null && asked.has(keyOf({ source: r.harness, sessionId: r.canonical_session_id }))));
    };
    const app = createReadApi({
      verify, adapter: {} as never, liveness: live.liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      operatorErased: async (key) => fence.has(keyOf(key)),
      operatorErasedBatch: async (_o: string, _w: string, keys: SessionKey[]) => {
        batches.push(keys);
        if (opts.outage) throw new Error("operator_fence_unavailable fx-s01");
        return new Set(keys.map(keyOf).filter((k) => fence.has(k)));
      },
      workspaceSynthesis: { enabled: true, source: opts.source, stage2Enabled: true, ...(opts.noAliases ? {} : { aliases: opts.aliases ?? aliases }) },
    });
    return { app, live, batches };
  };
  const invalidated = (r: { status: number; text: string; body: any }, id: string) => {
    expect([id, r.status, r.body.error?.reason]).toEqual([id, 404, "synthesis_invalidated"]);
    expect(r.text).not.toContain(id);
    expect(r.text).not.toContain("initiative:");
  };
  /** The stored v2 with `edit` applied, served past the strict reader (a source that skipped its checks). */
  async function edited(edit: (stage2: any) => void) {
    const s = await stored();
    const found = await s.source.latestStage2!("fx-org", "fx-ws-synthesis", WINDOW);
    const v2 = structuredClone(found!.v2!);
    edit(v2.synthesis.stage2);
    const source: WorkspaceSynthesisSource = { latest: s.source.latest, async latestStage2() { return { v1: found!.v1, v2 }; } };
    return source;
  }

  test("nothing fenced still serves exactly the v2 golden; v1 still serves the v1 golden", async () => {
    const s = await stored();
    const f = fencedApi({ source: s.source });
    const r = await get(f.app);
    expect(r.status).toBe(200);
    expect(r.body).toEqual(goldenV2);
    expect(f.batches.flat().length).toBe(golden.inputs.considered);
    const v1 = await get(f.app, PATH);
    expect([v1.status, v1.body]).toEqual([200, golden]);
  });

  test("a fenced considered session (the lander's fx-s01) is never served by stage=2", async () => {
    for (const key of [{ source: "fx-claude", sessionId: "fx-s01" }, { source: "fx-claude", sessionId: "fx-skip-not-analyzed" }]) {
      const s = await stored();
      const f = fencedApi({ source: s.source, fence: [key] });
      invalidated(await get(f.app), key.sessionId);
      expect(f.live.calls.erased.flat()).toContainEqual(key);
    }
  });

  test("a fenced RELATED-only session (a child only the evidence belongs to) is never served by stage=2", async () => {
    for (const child of ["fx-s01-child", "fx-s01-spawned"]) {
      const s = await stored({ cohort: withChild(JSON.parse(cohortText)) });
      expect((await get(fencedApi({ source: s.source }).app)).status).toBe(200);
      invalidated(await get(fencedApi({ source: s.source, fence: [{ source: "fx-claude", sessionId: child }] }).app), child);
    }
  });

  test("a fenced kept-only session, initiative or theme session, or evidence-only session is never served", async () => {
    const outside = { source: "fx-claude", sessionId: "fx-kept-outside" };
    const cases: Array<[string, (stage2: any) => void]> = [
      ["kept", (st) => { st.sessions.push(outside); }],
      ["initiative session", (st) => { st.initiatives[0].sessions.push(outside); }],
      ["initiative evidence", (st) => { st.initiatives[0].evidence.push({ ...outside, titleRef: st.initiatives[0].evidence[0].titleRef }); }],
      ["theme session", (st) => { st.themes[0].sessions.push(outside); }],
      ["theme evidence", (st) => { st.themes[0].evidence.push({ ...outside, titleRef: st.themes[0].evidence[0].titleRef }); }],
    ];
    for (const [name, edit] of cases) {
      const source = await edited(edit);
      expect([name, (await get(fencedApi({ source }).app)).status]).toEqual([name, 200]);
      const f = fencedApi({ source, fence: [outside] });
      invalidated(await get(f.app), outside.sessionId);
      expect(f.batches.flat()).toContainEqual(outside);
    }
  });

  test("a fence under a canonical or physical alias of a recorded session is never served by stage=2", async () => {
    const s = await stored();
    invalidated(await get(fencedApi({
      source: s.source, aliasRows: [{ harness: "fx-claude", session_id: "fx-s06", canonical_session_id: "fx-canon-s06" }],
      fence: [{ source: "fx-claude", sessionId: "fx-canon-s06" }],
    }).app), "fx-s06");
    invalidated(await get(fencedApi({
      source: s.source, aliasRows: [{ harness: "fx-claude", session_id: "fx-phys-s06", canonical_session_id: "fx-s06" }],
      fence: [{ source: "fx-claude", sessionId: "fx-phys-s06" }],
    }).app), "fx-s06");
    // Per source: the alias under another source is another session.
    expect((await get(fencedApi({
      source: s.source, aliasRows: [{ harness: "fx-claude", session_id: "fx-s06", canonical_session_id: "fx-canon-s06" }],
      fence: [{ source: "fx-codex", sessionId: "fx-canon-s06" }],
    }).app)).status).toBe(200);
  });

  test("the alias closure: a sibling physical id sharing a recorded session's source id is checked, per source and tenant", async () => {
    const s = await stored();
    // Recorded physical A = fx-s06 (an initiative session, source id C); physical B = fx-sib-b shares C. Only B fenced.
    const A = { harness: "fx-claude", session_id: "fx-s06", canonical_session_id: "fx-canon-c" };
    const sib = { harness: "fx-claude", session_id: "fx-sib-b", canonical_session_id: "fx-canon-c" };
    const B = { source: "fx-claude", sessionId: "fx-sib-b" };
    const f = fencedApi({ source: s.source, aliasRows: [A, sib], fence: [B] });
    invalidated(await get(f.app), "fx-s06");
    expect(f.batches.flat()).toContainEqual(B);
    // A further hop: B's other pointer holds source id D, whose physical E is fenced.
    invalidated(await get(fencedApi({
      source: s.source, fence: [{ source: "fx-claude", sessionId: "fx-sib-e" }],
      aliasRows: [A, sib, { ...sib, canonical_session_id: "fx-canon-d" }, { harness: "fx-claude", session_id: "fx-sib-e", canonical_session_id: "fx-canon-d" }],
    }).app), "fx-s06");
    // The sibling under another SOURCE is another session: served, its form never checked.
    const os = fencedApi({ source: s.source, aliasRows: [A, { ...sib, harness: "fx-codex" }], fence: [{ source: "fx-codex", sessionId: "fx-sib-b" }] });
    const served = await get(os.app);
    expect([served.status, served.body]).toEqual([200, goldenV2]);
    expect(os.batches.flat()).not.toContainEqual({ source: "fx-codex", sessionId: "fx-sib-b" });
    // The sibling in another TENANT is invisible to the bound read: served, never checked.
    for (const tenant of [{ org_id: "fx-org-other" }, { workspace_id: "fx-ws-other" }]) {
      const ot = fencedApi({ source: s.source, aliasRows: [A, { ...sib, ...tenant }], fence: [B] });
      const r = await get(ot.app);
      expect([r.status, r.body]).toEqual([200, goldenV2]);
      expect(ot.batches.flat()).not.toContainEqual(B);
    }
    // A reader that hands back another tenant's sibling row is refused (500), never served.
    const leak = fencedApi({ source: s.source, fence: [B], aliases: async (orgId, workspaceId, keys) => keys.some((k) => k.sessionId === "fx-canon-c")
      ? [{ ...sib, org_id: "fx-org-other", workspace_id: workspaceId }]
      : keys.some((k) => k.sessionId === "fx-s06") ? [{ ...A, org_id: orgId, workspace_id: workspaceId }] : [] });
    const refused = await get(leak.app);
    expect([refused.status, refused.body.error.code]).toEqual([500, "internal"]);
    expect(refused.text).not.toContain("initiative:");
    // A closure still growing past the round bound is a 503.
    const chain = [{ ...A, canonical_session_id: "fx-chain-c0" }];
    for (let i = 1; i <= 2 * SYNTHESIS_ALIAS_MAX_ROUNDS; i++) {
      chain.push({ harness: "fx-claude", session_id: `fx-chain-p${i}`, canonical_session_id: `fx-chain-c${i - 1}` });
      chain.push({ harness: "fx-claude", session_id: `fx-chain-p${i}`, canonical_session_id: `fx-chain-c${i}` });
    }
    const over = await get(fencedApi({ source: s.source, aliasRows: chain }).app);
    expect([over.status, over.body.error.code]).toEqual([503, "backend_unavailable"]);
    expect(over.text).not.toContain("initiative:");
  });

  test("fails closed: the fence unavailable, or bound without an alias reader, is a 503 with nothing served", async () => {
    const s = await stored();
    for (const f of [fencedApi({ source: s.source, outage: true }), fencedApi({ source: s.source, noAliases: true })]) {
      const r = await get(f.app);
      expect([r.status, r.body.error.code]).toEqual([503, "backend_unavailable"]);
      expect(r.text).not.toContain("fx-s01");
      expect(r.text).not.toContain("initiative:");
    }
  });
});
