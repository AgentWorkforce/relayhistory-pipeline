/**
 * GET /v1/session-intel/synthesis?stage=2 (docs/session-intel-workspace-synthesis.md, "Serving v2"): its own flag
 * (stage2Enabled, READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED), the strict stored-v2 reader, the erase check over
 * considered + related + the kept sessions, and the v1 route unchanged.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { memoryLiveness } from "./support.ts";
import { storeWorkspaceSynthesis, type WorkspaceSynthesisSource } from "../workspace-synthesis.ts";
import { memoryArtifactStore, runWorkspaceSynthesisJob, workspaceSynthesisStage2Key } from "../../session-analysis/workspace-synthesis.ts";
import { memoryBudgetLedger, type Stage2ModelTransport } from "../../session-intel/synthesis/stage2.ts";
import { workspaceSynthesisDeps } from "../../../packages/session-read-worker/src/index.ts";
import type { SessionKey } from "../page.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohortText = readFileSync(new URL("cohort.json", FIXTURE), "utf8");
const REPLY = readFileSync(new URL("stage2-reply.json", FIXTURE), "utf8");
const golden = JSON.parse(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
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

async function stored(opts: { stage2?: boolean } = {}) {
  const store = memoryArtifactStore();
  const r = await runWorkspaceSynthesisJob({
    store, keyRoot: "fxroot", orgId: "fx-org", cohort: JSON.parse(cohortText), options: { now: UNTIL },
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
