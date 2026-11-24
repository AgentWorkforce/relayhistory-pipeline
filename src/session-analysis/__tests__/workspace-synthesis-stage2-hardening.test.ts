/**
 * Stage 2 job and strict stored-v2 reader hardening (src/session-analysis/workspace-synthesis.ts,
 * src/session-intel/synthesis/stage2.ts validateStoredStage2): `called` on a failed outcome, the envelope equality with
 * v1, the race loser's read-back, one resealed mutation per stored-v2 rule (asserted by SynthesisArtifactError.detail),
 * the format / contract / stage2Reason checks and strict UTF-8.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import {
  memoryArtifactStore, parseStage2Artifact, runWorkspaceSynthesisJob, SynthesisArtifactError,
  type Stage2JobOptions, type SynthesisArtifact, type SynthesisArtifactStore, type SynthesisArtifactV2,
} from "../workspace-synthesis.ts";
import { memoryBudgetLedger, synthesisV2Json, type Stage2ModelTransport } from "../../session-intel/synthesis/stage2.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE), "utf8"));
const REPLY = readFileSync(new URL("stage2-reply.json", FIXTURE), "utf8");
const NOW = "2026-11-08T00:00:00.000Z";
const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
const isStage2Key = (k: string) => k.includes("/stage2/");

function fake() {
  const t = {
    calls: 0,
    async complete(r: { model: string }) {
      t.calls++;
      return { outputText: REPLY, inputTokens: 4_000, outputTokens: 900, providerRequestId: "fx-req-1", model: r.model };
    },
  };
  return t as Stage2ModelTransport & { calls: number };
}
const stage2 = (transport = fake()): Stage2JobOptions =>
  ({ flag: "true", transport, ledger: memoryBudgetLedger(), clock: () => new Date("2026-11-24T10:00:00.000Z"), trigger: "manual" });
const job = (store: SynthesisArtifactStore, s2: Stage2JobOptions = stage2(), orgId = "fx-org") =>
  runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId, cohort: cohort(), options: { now: NOW }, stage2: s2 });

async function stored(orgId = "fx-org") {
  const store = memoryArtifactStore();
  const r = await job(store, stage2(), orgId);
  if (!r.stage2 || r.stage2.outcome === "failed") throw new Error("fx: no v2");
  const v1 = JSON.parse(new TextDecoder().decode(store.objects.get(r.key)!)) as SynthesisArtifact;
  const v2Bytes = store.objects.get(r.stage2.key)!;
  const v2 = JSON.parse(new TextDecoder().decode(v2Bytes)) as SynthesisArtifactV2;
  return { v1, v2, v2Bytes, digest: r.sha256, key: r.stage2.key };
}
const bytes = (a: unknown) => new TextEncoder().encode(`${JSON.stringify(a)}\n`);
/** Re-seal stage2Sha256 after a mutation, so only the rule under test can catch it. */
const reseal = (a: any) => ({ ...a, stage2Sha256: sha(synthesisV2Json(a.synthesis)) });
function outcome(f: () => unknown): [string, string | undefined] | ["accepted"] {
  try { f(); } catch (e) {
    expect(e).toBeInstanceOf(SynthesisArtifactError);
    return [(e as SynthesisArtifactError).code, (e as SynthesisArtifactError).detail];
  }
  return ["accepted"];
}

describe("`called` on a failed Stage 2 outcome", () => {
  test("a failure AFTER a paid call (the write and its read-back fail) reports called true", async () => {
    const inner = memoryArtifactStore();
    const store: SynthesisArtifactStore = {
      list: inner.list, get: inner.get,
      async putNew(key, b, d) { if (isStage2Key(key)) throw new Error("fx: store down"); return inner.putNew(key, b, d); },
    };
    const t = fake();
    const r = await job(store, stage2(t));
    expect(r.outcome).toBe("created");
    expect(t.calls).toBe(1);
    expect(r.stage2).toEqual({ outcome: "failed", key: expect.stringContaining("/stage2/"), code: "synthesis_write_verification_failed", called: true });
  });

  test("a failure BEFORE the call (the prior read fails) reports called false", async () => {
    const inner = memoryArtifactStore();
    const store: SynthesisArtifactStore = {
      list: inner.list, putNew: inner.putNew,
      async get(key, max) { if (isStage2Key(key)) throw new SynthesisArtifactError("synthesis_artifact_bound"); return inner.get(key, max); },
    };
    const t = fake();
    const r = await job(store, stage2(t));
    expect(t.calls).toBe(0);
    expect(r.stage2).toEqual({ outcome: "failed", key: expect.stringContaining("/stage2/"), code: "synthesis_artifact_bound", called: false });
  });
});

describe("envelope equality with the Stage 1 artifact", () => {
  test("one resealed mutation per field is rejected, by that field's check", async () => {
    const { v1, v2, digest } = await stored();
    expect(outcome(() => parseStage2Artifact(bytes(v2), v1, digest))).toEqual(["accepted"]);
    const cases: Array<[string, (a: any) => void]> = [
      ["workspace", (a) => { a.workspace = "fx-ws-other"; }],
      ["window", (a) => { a.window = { ...a.window, until: "2026-11-07T00:00:00.000Z" }; }],
      ["generatedAt", (a) => { a.generatedAt = "2026-11-07T00:00:00.000Z"; }],
      ["considered", (a) => { a.considered = a.considered.slice(1); }],
      ["related", (a) => { a.related = [...a.related, { source: "fx-claude", sessionId: "fx-zz-extra" }]; }],
    ];
    for (const [field, edit] of cases) {
      const a = structuredClone(v2);
      edit(a);
      expect([field, ...outcome(() => parseStage2Artifact(bytes(reseal(a)), v1, digest))]).toEqual([field, "synthesis_stage2_invalid", `envelope.${field}`]);
    }
  });

  test("format, contract and stage2Reason: one case each", async () => {
    const { v1, v2, digest } = await stored();
    const cases: Array<[string, (a: any) => void]> = [
      ["envelope.format", (a) => { a.format = "workspace-synthesis-artifact/1"; }],
      ["synthesis.contract", (a) => { a.synthesis.contract = "workspace-synthesis/1"; }],
      ["synthesis.stage2Reason", (a) => { a.synthesis.stage2Reason = "budget_exhausted"; }],
    ];
    for (const [detail, edit] of cases) {
      const a = structuredClone(v2);
      edit(a);
      expect(outcome(() => parseStage2Artifact(bytes(reseal(a)), v1, digest))).toEqual(["synthesis_stage2_invalid", detail]);
    }
  });

  test("invalid UTF-8 inside an otherwise valid JSON string is rejected, never decoded to U+FFFD", async () => {
    const { v1, v2, digest } = await stored();
    // Sealed as if the title held U+FFFD, then that character's encoding (EF BF BD) replaced by one bare 0xFF byte:
    // a lenient decoder would read back exactly the sealed document.
    const a = structuredClone(v2) as any;
    a.synthesis.stage2.initiatives[0].title = "Unblock the � lint configuration";
    const sealed = bytes(reseal(a));
    expect(outcome(() => parseStage2Artifact(sealed, v1, digest))).toEqual(["accepted"]);
    const at = sealed.findIndex((b, i) => b === 0xef && sealed[i + 1] === 0xbf && sealed[i + 2] === 0xbd);
    const broken = new Uint8Array([...sealed.slice(0, at), 0xff, ...sealed.slice(at + 3)]);
    expect(new TextDecoder().decode(broken)).toBe(new TextDecoder().decode(sealed));
    expect(outcome(() => parseStage2Artifact(broken, v1, digest))).toEqual(["synthesis_stage2_invalid", "bytes"]);
  });
});

describe("the envelope's analyzed list against a TRUNCATED drilldown", () => {
  test("a ref outside considered is rejected even where no drilldown or kept-session check could see it", async () => {
    // Drilldowns cut to 1 per category and the kept sessions cut by the input cap: some analyzed sessions are in
    // neither, so swapping one of them for an outside ref leaves only the `considered` check to catch it.
    const store = memoryArtifactStore();
    const options = { now: NOW, bounds: { maxDrilldown: 1 } };
    const r = await runWorkspaceSynthesisJob({
      store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options, stage2: { ...stage2(), caps: { maxInputTokensPerRun: 4_000 } },
    });
    if (!r.stage2 || r.stage2.outcome === "failed" || !r.stage2.document.stage2) throw new Error("fx: no v2");
    const v1 = JSON.parse(new TextDecoder().decode(store.objects.get(r.key)!)) as SynthesisArtifact;
    const v2 = JSON.parse(new TextDecoder().decode(store.objects.get(r.stage2.key)!)) as SynthesisArtifactV2;
    expect(v1.synthesis.inputs.caps.drilldown.truncated).toBe(true);
    expect(v2.synthesis.stage2!.truncatedSessions).toBeGreaterThan(0);
    expect(outcome(() => parseStage2Artifact(bytes(v2), v1, r.sha256))).toEqual(["accepted"]);
    const k = (x: { source: string; sessionId: string }) => `${x.source}\u0000${x.sessionId}`;
    const drilled = new Set(v1.synthesis.categories.flatMap((c) => c.sessions.map(k)));
    const kept = new Set(v2.synthesis.stage2!.sessions.map(k));
    const free = v2.analyzed.find((x) => !drilled.has(k(x)) && !kept.has(k(x)))!;
    expect(free).toBeDefined();
    const outside = { source: "fx-claude", sessionId: "fx-zz-not-considered" };
    expect(v1.considered.some((x) => k(x) === k(outside))).toBe(false);
    const a = structuredClone(v2);
    a.analyzed = [...a.analyzed.filter((x) => k(x) !== k(free)), outside].sort((x, y) => (k(x) < k(y) ? -1 : 1));
    expect(outcome(() => parseStage2Artifact(bytes(a), v1, r.sha256))).toEqual(["synthesis_stage2_invalid", "envelope.analyzed: not considered"]);
  });
});

describe("the detail stays out of the message", () => {
  test("SynthesisArtifactError.message is the code alone, with or without a detail", async () => {
    expect(new SynthesisArtifactError("synthesis_stage2_invalid", "envelope.window").message).toBe("synthesis_stage2_invalid");
    const { v1, v2, digest } = await stored();
    const a = structuredClone(v2) as any;
    a.workspace = "fx-ws-other";
    let error: SynthesisArtifactError | null = null;
    try { parseStage2Artifact(bytes(reseal(a)), v1, digest); } catch (e) { error = e as SynthesisArtifactError; }
    expect([error!.code, error!.detail, error!.message, String(error)]).toEqual([
      "synthesis_stage2_invalid", "envelope.workspace", "synthesis_stage2_invalid", "SynthesisArtifactError: synthesis_stage2_invalid",
    ]);
  });
});

describe("the race loser's read-back", () => {
  /** The store as the loser sees it: the "winner" writes `planted` at the Stage 2 key just before the loser's put. */
  const racing = (planted: Uint8Array) => {
    const inner = memoryArtifactStore();
    const store: SynthesisArtifactStore = {
      list: inner.list, get: inner.get,
      async putNew(key, b, d) {
        if (isStage2Key(key)) inner.objects.set(key, new Uint8Array(planted));
        return inner.putNew(key, b, d);
      },
    };
    return store;
  };

  test("garbage in place of the winner's object: failed, not exists", async () => {
    const t = fake();
    const r = await job(racing(new TextEncoder().encode("{\"not\":\"a v2\"}\n")), stage2(t));
    expect(t.calls).toBe(1);
    expect(r.stage2).toMatchObject({ outcome: "failed", code: "synthesis_stage2_invalid", called: true });
  });

  test("another tenant's valid envelope in place of the winner's object: failed (cross tenant), not exists", async () => {
    const other = await stored("fx-org-other");
    expect(other.v2.orgId).toBe("fx-org-other");
    const r = await job(racing(other.v2Bytes), stage2());
    expect(r.stage2).toMatchObject({ outcome: "failed", code: "synthesis_cross_tenant", called: true });
  });
});

describe("validateStoredStage2: one resealed mutation per rule", () => {
  test("each mutation trips only its rule, named in the detail", async () => {
    const { v1, v2, digest } = await stored();
    const s = v2.synthesis.stage2!;
    // The fixture: one initiative (fx-s06, fx-s11, fx-s16, each cited by b1), one theme, one hypothesis on both.
    expect([s.initiatives.length, s.themes.length, s.hypotheses.length, s.keptSessions, s.truncatedSessions]).toEqual([1, 1, 1, 40, 0]);
    expect(s.initiatives[0]!.sessions.map((x) => x.sessionId)).toEqual(["fx-s06", "fx-s11", "fx-s16"]);
    const ref = (sessionId: string) => ({ source: "fx-claude", sessionId });
    const cases: Array<[string, (x: any) => void]> = [
      ["model", (x) => { x.model = "claude-opus-5-5"; }],
      ["promptVersion", (x) => { x.promptVersion = "workspace-synthesis-stage2/0"; }],
      ["basis", (x) => { x.basis = "rule"; }],
      ["kept + truncated != analyzed", (x) => { x.truncatedSessions = 1; }],
      ["keptSessions below the minimum", (x) => {
        Object.assign(x, { keptSessions: 1, truncatedSessions: 39, sessions: x.sessions.slice(0, 1), initiatives: [], themes: [], hypotheses: [] });
      }],
      ["sessions: order", (x) => { [x.sessions[0], x.sessions[1]] = [x.sessions[1], x.sessions[0]]; }],
      ["initiatives[0].sessions: duplicate", (x) => { x.initiatives[0].sessions.push(ref("fx-s06")); }],
      ["initiatives[0].sessions: not kept", (x) => {
        x.initiatives[0].sessions.push(ref("fx-skip-not-analyzed"));
        x.initiatives[0].evidence.push({ ...ref("fx-skip-not-analyzed"), titleRef: "i1" });
      }],
      ["initiatives[0].evidence: session not named", (x) => { x.initiatives[0].evidence[2] = { ...ref("fx-s01"), titleRef: "b1" }; }],
      ["initiatives[0].evidence: duplicate", (x) => { x.initiatives[0].evidence.push({ ...x.initiatives[0].evidence[0] }); }],
      ["initiatives[0]: uncited session", (x) => { x.initiatives[0].evidence.pop(); }],
      ["initiatives[1].id", (x) => { x.initiatives.push(structuredClone(x.initiatives[0])); }],
      ["initiatives[0].id", (x) => { x.initiatives[0].id = "initiative:11"; x.hypotheses[0].basis[0] = "initiative:11"; }],
    ];
    for (const [rule, edit] of cases) {
      const a = structuredClone(v2) as any;
      edit(a.synthesis.stage2);
      expect([rule, ...outcome(() => parseStage2Artifact(bytes(reseal(a)), v1, digest))]).toEqual([rule, "synthesis_stage2_invalid", `stage2: ${rule}`]);
    }
  });
});
