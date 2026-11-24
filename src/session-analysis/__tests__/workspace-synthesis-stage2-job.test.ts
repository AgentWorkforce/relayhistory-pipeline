/** The offline job with Stage 2 behind WORKSPACE_SYNTHESIS_STAGE2_ENABLED (docs/session-intel-workspace-synthesis.md, "Stage 2"). */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  latestWorkspaceSynthesis, memoryArtifactStore, runWorkspaceSynthesisJob, SYNTHESIS_ARTIFACT_FORMAT_V2, workspaceSynthesisDirectory,
  workspaceSynthesisKey, workspaceSynthesisPrefix, workspaceSynthesisStage2Key, type Stage2JobOptions, type SynthesisArtifactV2,
} from "../workspace-synthesis.ts";
import { memoryBudgetLedger, stage1Of, synthesisV2Json, type Stage2ModelTransport } from "../../session-intel/synthesis/stage2.ts";
import { synthesisJson } from "../../session-intel/synthesis/synthesize.ts";
import { userErasePrefixes } from "../../session-erase/user-erase.ts";
import { ANALYSIS_RESULT_OBJECT_GLOB } from "../../../stacks/aws/session-pipeline.ts";
import { TENANT_KEY_BASE } from "../../../stacks/aws/tenants.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE), "utf8"));
const REPLY = readFileSync(new URL("stage2-reply.json", FIXTURE), "utf8");
const NOW = "2026-11-08T00:00:00.000Z";
const LOOKUP = { keyRoot: "fxroot", orgId: "fx-org", workspace: "fx-ws-synthesis", window: { since: "2026-11-01T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" } };

function fake(fail = false) {
  const t = {
    calls: 0,
    async complete(r: { model: string }) {
      t.calls++;
      if (fail) throw new Error("fx transport down");
      return { outputText: JSON.stringify(JSON.parse(REPLY)), inputTokens: 4_000, outputTokens: 900, providerRequestId: "fx-req-1", model: r.model };
    },
  };
  return t as Stage2ModelTransport & { calls: number };
}
const stage2 = (flag: string | undefined, transport = fake(), ledger = memoryBudgetLedger()): Stage2JobOptions =>
  ({ flag, transport, ledger, clock: () => new Date("2026-11-24T10:00:00.000Z"), trigger: "scheduled" });
const job = (store = memoryArtifactStore(), s2?: Stage2JobOptions) =>
  runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: NOW }, ...(s2 ? { stage2: s2 } : {}) });

describe("flag off", () => {
  test("keeps Stage 1 byte-identical: same key, same bytes, one object, no stage2 field, no call", async () => {
    const plain = memoryArtifactStore();
    const base = await job(plain);
    for (const flag of [undefined, "", "1", "TRUE", "true ", "false"]) {
      const store = memoryArtifactStore();
      const t = fake();
      const r = await job(store, stage2(flag, t));
      expect(r).toEqual(base);
      expect("stage2" in r).toBe(false);
      expect([...store.objects.keys()]).toEqual([...plain.objects.keys()]);
      expect(Buffer.from(store.objects.get(r.key)!).equals(Buffer.from(plain.objects.get(base.key)!))).toBe(true);
      expect(t.calls).toBe(0);
    }
  });
});

describe("flag on", () => {
  test("writes the unchanged v1 artifact and one v2 artifact beside it; the v1 lookup still serves v1", async () => {
    const plain = memoryArtifactStore();
    const base = await job(plain);
    const store = memoryArtifactStore();
    const t = fake();
    const r = await job(store, stage2("true", t));
    expect([r.key, r.sha256, r.outcome]).toEqual([base.key, base.sha256, "created"]);
    expect(Buffer.from(store.objects.get(r.key)!).equals(Buffer.from(plain.objects.get(base.key)!))).toBe(true);
    expect(r.stage2!.outcome).toBe("created");
    expect(r.stage2!.key).toBe(workspaceSynthesisStage2Key(r.key));
    expect(r.stage2!.usage).toEqual({ inputTokens: 4_000, outputTokens: 900 });
    expect(store.objects.size).toBe(2);
    const v2 = JSON.parse(new TextDecoder().decode(store.objects.get(r.stage2!.key)!)) as SynthesisArtifactV2;
    expect(v2.format).toBe(SYNTHESIS_ARTIFACT_FORMAT_V2);
    expect(synthesisV2Json(v2.synthesis)).toBe(readFileSync(new URL("expected-synthesis-v2.json", FIXTURE), "utf8"));
    expect(synthesisJson(stage1Of(v2.synthesis))).toBe(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
    // The read path's v1 lookup ignores the stage2 key and serves the v1 artifact.
    const served = await latestWorkspaceSynthesis(store, LOOKUP);
    expect(served!.synthesis.contract).toBe("workspace-synthesis/1");
    expect(synthesisJson(served!.synthesis)).toBe(synthesisJson(base.synthesis));
    // A rerun finds the v2 artifact and makes no second call.
    const again = await job(store, stage2("true", t));
    expect([again.stage2!.outcome, t.calls, store.objects.size]).toEqual(["exists", 1, 2]);
  });

  test("the v2 key is inside the analysis-result glob and the workspace's erase directory", async () => {
    const r = await job(memoryArtifactStore(), stage2("true"));
    const glob = new RegExp(`^${ANALYSIS_RESULT_OBJECT_GLOB.split("*").map((p) => p.replace(/[.?+^$()[\]{}|\\]/gu, "\\$&")).join(".*")}$`, "u");
    // Under the deployment's tenant base, as the v1 glob test does; `*` matches `/` too.
    const deployed = workspaceSynthesisStage2Key(workspaceSynthesisKey(workspaceSynthesisPrefix(TENANT_KEY_BASE, "fx-org", "fx-ws-synthesis", LOOKUP.window), NOW, "a".repeat(64)));
    expect(glob.test(deployed)).toBe(true);
    expect(r.stage2!.key.startsWith(workspaceSynthesisDirectory("fxroot", "fx-org", "fx-ws-synthesis"))).toBe(true);
    const prefixes = userErasePrefixes("fxroot", { requestId: "fx-req", orgId: "fx-org", workspaceId: "fx-ws-synthesis", userId: "fx-user" }, []);
    expect(prefixes.some((p) => r.stage2!.key.startsWith(p))).toBe(true);
  });

  test("transport failure: stage2 null with transport_failed, nothing extra written, v1 unchanged", async () => {
    const store = memoryArtifactStore();
    const t = fake(true);
    const r = await job(store, stage2("true", t));
    expect([r.stage2!.outcome, r.stage2!.document.stage2, r.stage2!.document.stage2Reason, t.calls, store.objects.size]).toEqual(["not_written", null, "transport_failed", 1, 1]);
  });

  test("budget exhausted: no call, nothing extra written", async () => {
    const ledger = memoryBudgetLedger();
    const t = fake();
    await job(memoryArtifactStore(), stage2("true", t, ledger));
    const store = memoryArtifactStore();
    const r = await job(store, stage2("true", t, ledger));
    expect([r.stage2!.document.stage2Reason, t.calls, store.objects.size]).toEqual(["budget_exhausted", 1, 1]);
  });
});
