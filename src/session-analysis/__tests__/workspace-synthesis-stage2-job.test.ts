/** The offline job with Stage 2 behind WORKSPACE_SYNTHESIS_STAGE2_ENABLED (docs/session-intel-workspace-synthesis.md, "Stage 2"). */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import {
  latestWorkspaceSynthesis, memoryArtifactStore, parseStage2Artifact, runWorkspaceSynthesisJob, SYNTHESIS_ARTIFACT_FORMAT_V2, SynthesisArtifactError,
  workspaceSynthesisDirectory, workspaceSynthesisKey, workspaceSynthesisPrefix, workspaceSynthesisStage2Key,
  type Stage2JobOptions, type Stage2JobResult, type SynthesisArtifact, type SynthesisArtifactV2,
} from "../workspace-synthesis.ts";
import { memoryBudgetLedger, stage1Of, synthesisV2Json, type Stage2ModelTransport } from "../../session-intel/synthesis/stage2.ts";
import { synthesisJson } from "../../session-intel/synthesis/synthesize.ts";
import { userErasePrefixes } from "../../session-erase/user-erase.ts";
import { ANALYSIS_RESULT_OBJECT_GLOB } from "../../../stacks/aws/session-pipeline.ts";
import { TENANT_KEY_BASE } from "../../../stacks/aws/tenants.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE), "utf8"));
const REPLY = readFileSync(new URL("stage2-reply.json", FIXTURE), "utf8");
const EMPTY_REPLY = JSON.stringify({ initiatives: [], themes: [], hypotheses: [] });
const NOW = "2026-11-08T00:00:00.000Z";
const WINDOW = { since: "2026-11-01T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" };
const LOOKUP = { keyRoot: "fxroot", orgId: "fx-org", workspace: "fx-ws-synthesis", window: WINDOW };
const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");

function fake(opts: { fail?: boolean; reply?: string; gate?: Promise<void> } = {}) {
  const t = {
    calls: 0,
    async complete(r: { model: string }) {
      t.calls++;
      if (opts.gate) await opts.gate;
      if (opts.fail) throw new Error("fx transport down");
      return { outputText: JSON.stringify(JSON.parse(opts.reply ?? REPLY)), inputTokens: 4_000, outputTokens: 900, providerRequestId: "fx-req-1", model: r.model };
    },
  };
  return t as Stage2ModelTransport & { calls: number };
}
const stage2 = (flag: string | undefined, transport = fake(), ledger = memoryBudgetLedger()): Stage2JobOptions =>
  ({ flag, transport, ledger, clock: () => new Date("2026-11-24T10:00:00.000Z"), trigger: "manual" });
const job = (store = memoryArtifactStore(), s2?: Stage2JobOptions) =>
  runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: NOW }, ...(s2 ? { stage2: s2 } : {}) });
function ok(r: Stage2JobResult | undefined) {
  if (!r || r.outcome === "failed") throw new Error(`expected a Stage 2 result, got ${JSON.stringify(r)}`);
  return r;
}
const stage2KeyOf = (v1Key: string) => workspaceSynthesisStage2Key("fxroot", "fx-org", "fx-ws-synthesis", WINDOW, v1Key);

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
  test("writes the unchanged v1 artifact and one v2 artifact at the fixed key; the v1 lookup still serves v1; a rerun makes no call", async () => {
    const plain = memoryArtifactStore();
    const base = await job(plain);
    const store = memoryArtifactStore();
    const t = fake();
    const r = await job(store, stage2("true", t));
    expect([r.key, r.sha256, r.outcome]).toEqual([base.key, base.sha256, "created"]);
    expect(Buffer.from(store.objects.get(r.key)!).equals(Buffer.from(plain.objects.get(base.key)!))).toBe(true);
    const s2 = ok(r.stage2);
    expect(s2.outcome).toBe("created");
    expect(s2.key).toBe(stage2KeyOf(r.key));
    expect(s2.key.endsWith(`/${r.key.slice(r.key.lastIndexOf("/") + 1, -5)}.workspace-synthesis-stage2-1.json`)).toBe(true);
    expect(s2.usage).toEqual({ inputTokens: 4_000, outputTokens: 900 });
    expect(store.objects.size).toBe(2);
    const v2 = JSON.parse(new TextDecoder().decode(store.objects.get(s2.key)!)) as SynthesisArtifactV2;
    expect(v2.format).toBe(SYNTHESIS_ARTIFACT_FORMAT_V2);
    expect(v2.v1Sha256).toBe(r.sha256);
    expect(v2.stage2Sha256).toBe(sha(synthesisV2Json(v2.synthesis)));
    expect(synthesisV2Json(v2.synthesis)).toBe(readFileSync(new URL("expected-synthesis-v2.json", FIXTURE), "utf8"));
    expect(synthesisJson(stage1Of(v2.synthesis))).toBe(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
    const served = await latestWorkspaceSynthesis(store, LOOKUP);
    expect(served!.synthesis.contract).toBe("workspace-synthesis/1");
    expect(synthesisJson(served!.synthesis)).toBe(synthesisJson(base.synthesis));
    const again = await job(store, stage2("true", t));
    expect([again.stage2!.outcome, t.calls, store.objects.size]).toEqual(["exists", 1, 2]);
    expect(ok(again.stage2).document).toEqual(s2.document);
  });

  test("the v2 key is outside the v1 window prefix, inside the analysis-result glob and the workspace's erase directory", async () => {
    const r = await job(memoryArtifactStore(), stage2("true"));
    const key = ok(r.stage2).key;
    expect(key.startsWith(workspaceSynthesisPrefix("fxroot", "fx-org", "fx-ws-synthesis", WINDOW))).toBe(false);
    const glob = new RegExp(`^${ANALYSIS_RESULT_OBJECT_GLOB.split("*").map((p) => p.replace(/[.?+^$()[\]{}|\\]/gu, "\\$&")).join(".*")}$`, "u");
    const v1 = workspaceSynthesisKey(workspaceSynthesisPrefix(TENANT_KEY_BASE, "fx-org", "fx-ws-synthesis", WINDOW), NOW, "a".repeat(64));
    expect(glob.test(workspaceSynthesisStage2Key(TENANT_KEY_BASE, "fx-org", "fx-ws-synthesis", WINDOW, v1))).toBe(true);
    expect(key.startsWith(workspaceSynthesisDirectory("fxroot", "fx-org", "fx-ws-synthesis"))).toBe(true);
    const prefixes = userErasePrefixes("fxroot", { requestId: "fx-req", orgId: "fx-org", workspaceId: "fx-ws-synthesis", userId: "fx-user" }, []);
    expect(prefixes.some((p) => key.startsWith(p))).toBe(true);
  });

  test("Stage 2 keys do not count toward the v1 lookup's 1,000-key list bound", async () => {
    const store = memoryArtifactStore();
    const r = await job(store, stage2("true"));
    const dir = ok(r.stage2).key.slice(0, ok(r.stage2).key.lastIndexOf("/") + 1);
    for (let i = 0; i < 1_001; i++) store.objects.set(`${dir}g=${String(i).padStart(13, "0")}-${"b".repeat(64)}.workspace-synthesis-stage2-1.json`, new Uint8Array([1]));
    expect((await latestWorkspaceSynthesis(store, LOOKUP))!.synthesis.contract).toBe("workspace-synthesis/1");
  });

  test("transport failure: stage2 null with transport_failed, nothing extra written", async () => {
    const store = memoryArtifactStore();
    const t = fake({ fail: true });
    const r = await job(store, stage2("true", t));
    const s2 = ok(r.stage2);
    expect([s2.outcome, s2.document.stage2, s2.document.stage2Reason, t.calls, store.objects.size]).toEqual(["not_written", null, "transport_failed", 1, 1]);
  });

  test("budget exhausted: no call, nothing extra written", async () => {
    const ledger = memoryBudgetLedger();
    const t = fake();
    for (let i = 0; i < 2; i++) await job(memoryArtifactStore(), stage2("true", t, ledger));
    const store = memoryArtifactStore();
    const r = await job(store, stage2("true", t, ledger));
    expect([ok(r.stage2).document.stage2Reason, t.calls, store.objects.size]).toEqual(["budget_exhausted", 2, 1]);
  });
});

describe("the job always returns v1", () => {
  test("a garbage object at the Stage 2 key: v1 plus failed, no call", async () => {
    const store = memoryArtifactStore();
    const base = await job(store);
    store.objects.set(stage2KeyOf(base.key), new TextEncoder().encode("{\"not\":\"a v2\"}"));
    const t = fake();
    const r = await job(store, stage2("true", t));
    expect([r.key, r.outcome]).toEqual([base.key, "exists"]);
    expect(r.stage2).toEqual({ outcome: "failed", key: stage2KeyOf(base.key), code: "synthesis_stage2_invalid", called: false });
    expect(t.calls).toBe(0);
  });

  test("a programming error inside Stage 2 is reported, not thrown", async () => {
    const r = await job(memoryArtifactStore(), { ...stage2("true"), model: "claude-opus-5-5" });
    expect([r.outcome, r.stage2!.outcome, (r.stage2 as { code: string }).code]).toEqual(["created", "failed", "synthesis_stage2_failed"]);
  });

  test("two concurrent runs: both return v1, one creates, the loser returns exists with the winner's document; one v2 object", async () => {
    const store = memoryArtifactStore();
    let open!: () => void;
    const gate = new Promise<void>((r) => { open = r; });
    const ta = fake({ gate }), tb = fake({ gate, reply: EMPTY_REPLY });
    const both = Promise.all([job(store, stage2("true", ta)), job(store, stage2("true", tb))]);
    // Both runs miss the key and call before either writes.
    while (ta.calls + tb.calls < 2) await new Promise((r) => setTimeout(r, 1));
    open();
    const [a, b] = await both;
    expect([a.key, b.key, a.outcome === "created" || b.outcome === "created"]).toEqual([a.key, a.key, true]);
    const outcomes = [a.stage2!.outcome, b.stage2!.outcome].sort();
    expect(outcomes).toEqual(["created", "exists"]);
    const winner = ok([a, b].find((x) => x.stage2!.outcome === "created")!.stage2), loser = ok([a, b].find((x) => x.stage2!.outcome === "exists")!.stage2);
    expect(loser.document).toEqual(winner.document);
    expect([...store.objects.keys()].filter((k) => k.includes("/stage2/"))).toEqual([winner.key]);
  });
});

describe("the strict stored-v2 reader", () => {
  async function stored() {
    const store = memoryArtifactStore();
    const r = await job(store, stage2("true"));
    const v1 = JSON.parse(new TextDecoder().decode(store.objects.get(r.key)!)) as SynthesisArtifact;
    const v2 = JSON.parse(new TextDecoder().decode(store.objects.get(ok(r.stage2).key)!)) as SynthesisArtifactV2;
    return { v1, v2, digest: r.sha256 };
  }
  const bytes = (a: unknown) => new TextEncoder().encode(`${JSON.stringify(a)}\n`);
  /** Re-seal stage2Sha256 after a mutation, so the check under test is the one that fails. */
  const reseal = (a: any) => ({ ...a, stage2Sha256: sha(synthesisV2Json(a.synthesis)) });
  const code = (f: () => unknown) => {
    try { f(); } catch (e) { expect(e).toBeInstanceOf(SynthesisArtifactError); return (e as SynthesisArtifactError).code; }
    return "accepted";
  };

  test("accepts the stored artifact", async () => {
    const { v1, v2, digest } = await stored();
    expect(code(() => parseStage2Artifact(bytes(v2), v1, digest))).toBe("accepted");
  });

  test("one rejection each, every one a typed SynthesisArtifactError", async () => {
    const { v1, v2, digest } = await stored();
    const mutate = (f: (a: any) => void, seal = true) => {
      const a = JSON.parse(JSON.stringify(v2));
      f(a);
      return code(() => parseStage2Artifact(bytes(seal ? reseal(a) : a), v1, digest));
    };
    expect(mutate((a) => { a.extra = 1; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.initiatives[0].priority = "high"; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.initiatives[0].title = "Two\nlines"; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.themes[0].title = "Zero​width"; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.initiatives[0].evidence[0].titleRef = "x1"; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.usage.inputTokens = 10 ** 12; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { delete a.synthesis.stage2.initiatives; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.themes[0] = null; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.sessions.pop(); })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2.initiatives[0].sessions[0].sessionId = "fx-not-kept"; })).toBe("synthesis_stage2_invalid");
    expect(mutate((a) => { a.synthesis.stage2 = null; })).toBe("synthesis_stage2_invalid");
    // Digest: the v2 bytes no longer match stage2Sha256, or the envelope names another v1.
    expect(mutate((a) => { a.synthesis.stage2.initiatives[0].goal = "Changed."; }, false)).toBe("synthesis_stage2_digest");
    expect(mutate((a) => { a.v1Sha256 = "0".repeat(64); })).toBe("synthesis_stage2_digest");
    // The Stage 1 part must be the v1 document byte for byte.
    expect(mutate((a) => { a.synthesis.inputs.notes = [...a.synthesis.inputs.notes, "An extra note."]; })).toBe("synthesis_stage2_stage1_mismatch");
    expect(mutate((a) => { a.synthesis.freshness.staleCount += 1; })).toBe("synthesis_stage2_stage1_mismatch");
    // Another tenant's envelope is loud.
    expect(mutate((a) => { a.orgId = "fx-other-org"; })).toBe("synthesis_cross_tenant");
    // Not JSON at all.
    expect(code(() => parseStage2Artifact(new Uint8Array([0xff, 0x00]), v1, digest))).toBe("synthesis_stage2_invalid");
  });
});
