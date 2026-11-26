import { describe, expect, test } from "bun:test";
import type { EventFacts } from "../cloud-harvest/features.ts";
import type { DirectoryRow, SessionFacts } from "../cloud-harvest/harvest.ts";
import type { Alert, Context } from "../overlap-bench/detectors.ts";
import { adjudicationSample, pairId, precisionOf, wilson } from "../overlap-bench/eval.ts";
import { buildModel, communicated } from "../overlap-bench/model.ts";
import { rank, type Components } from "../overlap-bench/ranker.ts";
import { budgetCut } from "../overlap-bench/score.ts";

const alert = (at: string, score: number, sides: [string, string] = ["root:a", "root:b"]): Alert => ({ detector: "t", sides, at, artifact: "x", score, owners: ["K", "K"] });

describe("eval metrics", () => {
  test("wilson brackets the rate and is empty without judgements", () => {
    expect(wilson(0, 0)).toBeNull();
    const w = wilson(8, 10)!;
    expect(w.low).toBeGreaterThan(0.44);
    expect(w.high).toBeLessThan(0.95);
  });
  test("precision counts only judged alerts and reports unclear and unjudged apart", () => {
    const v = new Map([[pairId(["root:a", "root:b"]), { pair: "", verdict: "overlap" as const }], [pairId(["root:c", "root:d"]), { pair: "", verdict: "unclear" as const }]]);
    const p = precisionOf([alert("2026-10-01T00:00:00Z", 1), alert("2026-10-01T00:00:00Z", 1, ["root:d", "root:c"]), alert("2026-10-01T00:00:00Z", 1, ["root:e", "root:f"])], v);
    expect([p.judged, p.overlap, p.unclear, p.unjudged, p.rate]).toEqual([1, 1, 1, 1, 1]);
  });
});

describe("weekly budget", () => {
  test("each week spends its own budget on its best alerts, so one busy week cannot take the window's", () => {
    const wk = (ts: string) => Math.floor(Date.parse(ts) / (7 * 86_400_000));
    const busy = Array.from({ length: 10 }, (_, i) => alert(`2026-09-0${(i % 3) + 7}T00:00:00Z`, i, [`root:x${i}`, "root:y"]));
    const quiet = [alert("2026-10-15T00:00:00Z", 0.1, ["root:q", "root:r"])];
    const kept = budgetCut([...busy, ...quiet], new Map([[wk("2026-09-07T00:00:00Z"), 40], [wk("2026-10-15T00:00:00Z"), 20]]), 5);
    expect(kept.map((a) => a.score).sort((a, b) => b - a)).toEqual([9, 8, 0.1]);
  });
});

describe("combined ranker", () => {
  const ctx: Context = { sessions: new Map(), prs: [], communicated: () => false };
  const c = (over: Partial<Components>): Components => ({ openPr: [], trigger: [], concurrent: [], ...over });
  const p = { wOpenPr: 1, wTrigger: 1, wRarity: 1, rarityCap: 3, wAgree: 1, wCrossOwner: 0, threshold: 2 };
  test("fires when corroborating evidence first reaches the threshold, never earlier", () => {
    const a = rank(ctx, c({ concurrent: [alert("2026-10-01T10:00:00Z", 0.5)], trigger: [alert("2026-10-01T12:00:00Z", 1)] }), p);
    // 0.5 rarity alone is below 2; the trigger at 12:00 adds 1 and the agreement bonus 1.
    expect(a.map((x) => [x.at, x.score, x.artifact])).toEqual([["2026-10-01T12:00:00Z", 2.5, "concurrent+trigger"]]);
  });
  test("one weak signal alone stays below the threshold", () => {
    expect(rank(ctx, c({ trigger: [alert("2026-10-01T12:00:00Z", 1)] }), p)).toEqual([]);
  });
});

describe("adjudication sample", () => {
  test("merges pairs across detectors, skips judged pairs and hides which detector raised them", () => {
    const rows: DirectoryRow[] = [{ sessionId: "s", source: "codex", lastTs: "2026-10-01T00:00:00Z", userIds: ["K"] }];
    const f: SessionFacts[] = [{ source: "codex", sessionId: "s", eventsRead: 0, censored: false, error: null, events: [] as EventFacts[] }];
    const sessions = buildModel(rows, f);
    const ctx: Context = { sessions, prs: [], communicated: communicated(sessions) };
    const byDetector = new Map([["d1", [alert("2026-10-01T00:00:00Z", 2), alert("2026-10-01T00:00:00Z", 1, ["root:c", "root:d"])]], ["d2", [alert("2026-10-01T01:00:00Z", 5, ["root:b", "root:a"])]]]);
    const s = adjudicationSample(ctx, byDetector, 5, "seed", new Set([pairId(["root:c", "root:d"])]));
    expect(s.map((x) => x.pair)).toEqual(["root:a | root:b"]);
    expect(Object.keys(s[0]!)).toEqual(["pair", "sides", "at"]);
  });
});

describe("stage 2", () => {
  const key = [{ pair: "a", truth: "incident:1" as const }, { pair: "b", truth: "hindsight-overlap" as const }, { pair: "c", truth: "fresh" as const }, { pair: "d", truth: "fresh" as const }];
  const ok = [
    { pair: "a", verdict: "overlap", p_overlap: 0.6 }, { pair: "b", verdict: "not_overlap", p_overlap: 0.2 },
    { pair: "c", verdict: "not_overlap", p_overlap: 0.25 }, { pair: "d", verdict: "overlap", p_overlap: 0.3 },
  ];
  test("scores kept positives and fresh alerts at the cutoff", async () => {
    const { scoreStage2 } = await import("../overlap-bench/stage2.ts");
    const s = scoreStage2(key, ok, 0.3);
    expect([s.positives, s.positivesKept, s.fresh, s.freshKept]).toEqual([2, 1, 2, 1]);
  });
  test("refuses missing, duplicated, foreign, out-of-range and unknown verdicts", async () => {
    const { scoreStage2 } = await import("../overlap-bench/stage2.ts");
    expect(() => scoreStage2(key, ok.slice(0, 3), 0.3)).toThrow("cover 3 of 4");
    expect(() => scoreStage2(key, [...ok, { pair: "d", verdict: "overlap", p_overlap: 2 }], 0.3)).toThrow("more than one");
    expect(() => scoreStage2(key, [...ok.slice(0, 3), { pair: "d", verdict: "overlap", p_overlap: 2 }], 0.3)).toThrow("probability");
    expect(() => scoreStage2(key, [...ok.slice(0, 3), { pair: "z", verdict: "overlap", p_overlap: 0.1 }], 0.3)).toThrow("not in the batch");
    expect(() => scoreStage2(key, [...ok.slice(0, 3), { pair: "d", verdict: "maybe", p_overlap: 0.1 }], 0.3)).toThrow("not overlap");
    expect(() => scoreStage2([], [], 0.3)).not.toThrow();
  });
  test("a batch changed after it was made is refused", async () => {
    const { verifiedBatch } = await import("../overlap-bench/stage2.ts");
    const { createHash } = await import("node:crypto");
    const { mkdtempSync, mkdirSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const sha = (t: string) => createHash("sha256").update(t).digest("hex");
    const make = (deleteBy?: string) => {
      const d = mkdtempSync(join(tmpdir(), "batch-")), P = { mode: 0o600 };
      mkdirSync(join(d, "judge"), { mode: 0o700 }); mkdirSync(join(d, "key"), { mode: 0o700 });
      const keyText = JSON.stringify(key), part = "{}\n", prompt = "p";
      writeFileSync(join(d, "key", "key.json"), keyText, P); writeFileSync(join(d, "judge", "part-00"), part, P); writeFileSync(join(d, "judge", "prompt-00.txt"), prompt, P);
      writeFileSync(join(d, "judge", "verdicts-00.json"), JSON.stringify(ok), P);
      const body = { format: "overlap-bench-stage2/3", split: "dev", terms: {}, cutoff: 0.3, promptHash: "x", seed: "s", judge: "j", labelsHash: "l", deleteBy: deleteBy ?? "2099-01-01T00:00:00.000Z", items: key.length, keyHash: sha(keyText), parts: { "part-00": sha(part) }, prompts: { "prompt-00.txt": sha(prompt) } };
      writeFileSync(join(d, "key", "stage2.json"), JSON.stringify({ ...body, manifestHash: sha(JSON.stringify(body)) }), P);
      return d;
    };
    expect(verifiedBatch(make()).verdicts).toHaveLength(4);
    const k = make(); writeFileSync(join(k, "key", "key.json"), JSON.stringify(key.slice(0, 1)), { mode: 0o600 });
    expect(() => verifiedBatch(k)).toThrow("key was changed");
    const m = make(); const man = JSON.parse(await Bun.file(join(m, "key", "stage2.json")).text()); man.cutoff = 0.01;
    writeFileSync(join(m, "key", "stage2.json"), JSON.stringify(man), { mode: 0o600 });
    expect(() => verifiedBatch(m)).toThrow("manifest was changed");
    const pt = make(); writeFileSync(join(pt, "judge", "part-00"), "{\"x\":1}\n", { mode: 0o600 });
    expect(() => verifiedBatch(pt)).toThrow("part-00 was changed");
    // Past its delete-by, a batch is refused before any part or verdict is opened.
    expect(() => verifiedBatch(make("2026-01-01T00:00:00.000Z"))).toThrow("past (or has no valid) delete-by");
    expect(() => verifiedBatch(make("not a time"))).toThrow("delete-by");
  });
});

describe("once-only held-out", () => {
  const setup = async () => {
    const { mkdtempSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    return { reg: join(mkdtempSync(join(tmpdir(), "reg-")), "heldout"), L: "a".repeat(64) };
  };
  const DEAD = 999_999_99;
  test("one evaluation per labels digest; an interrupted claim resumes only under the same terms", async () => {
    const { claimOnce, finishOnce, noteOnce } = await import("../overlap-bench/once.ts");
    const { reg, L } = await setup();
    claimOnce("stage2", L, { f: 1 }, reg, DEAD);
    // The owner died: the same terms resume (ownership moves here), other terms are refused.
    const rec = claimOnce("stage2", L, { f: 1 }, reg);
    expect(rec.owner).toBe(process.pid);
    expect(() => claimOnce("stage2", L, { f: 2 }, reg)).toThrow("under other terms");
    noteOnce(rec, "m1", reg);
    expect(() => noteOnce(rec, "m2", reg)).toThrow("already has a batch");
    finishOnce(rec, { ok: true }, reg);
    expect(() => claimOnce("stage2", L, { f: 1 }, reg)).toThrow("already evaluated");
    expect(() => finishOnce(rec, {}, reg)).toThrow("already evaluated");
    expect(() => claimOnce("stage1", "not-a-digest", {}, reg)).toThrow("sha256");
  });
  test("an active claim by a live process is never shared, and only its owner can change it", async () => {
    const { claimOnce, finishOnce } = await import("../overlap-bench/once.ts");
    const { reg, L } = await setup();
    const live = process.ppid; // a process that is certainly alive
    const theirs = claimOnce("stage1", L, { f: 1 }, reg, live);
    expect(() => claimOnce("stage1", L, { f: 1 }, reg)).toThrow(`being evaluated by process ${live}`);
    expect(() => finishOnce(theirs, {}, reg)).toThrow(`held by process ${live}`);
  });
  test("a transition while another process holds the record lock is refused", async () => {
    const { claimOnce } = await import("../overlap-bench/once.ts");
    const { writeFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const { reg, L } = await setup();
    claimOnce("stage1", L, { f: 1 }, reg, DEAD);
    writeFileSync(join(reg, `stage1-${L}.json.lock`), "1", { mode: 0o600 });
    expect(() => claimOnce("stage1", L, { f: 1 }, reg)).toThrow("busy in another process");
  });
  test("two processes racing for one claim: exactly one wins", async () => {
    const { join } = await import("node:path");
    const { reg, L } = await setup();
    const script = join(import.meta.dir, "..", "overlap-bench", "once.ts");
    const child = (n: number) => Bun.spawn([process.execPath, "-e", `const { claimOnce } = await import(${JSON.stringify(script)}); try { claimOnce("stage1", "${L}", { f: 1 }, ${JSON.stringify(reg)}); await Bun.sleep(300); console.log("won"); } catch (e) { console.log("lost"); }`], { stdout: "pipe" });
    const procs = [child(1), child(2), child(3)];
    const out = await Promise.all(procs.map(async (p) => (await new Response(p.stdout).text()).trim()));
    expect(out.filter((x) => x === "won")).toHaveLength(1);
  });
  test("labels identity ignores the file name: a renamed copy is the same held-out set", async () => {
    const { labelsIdentity } = await import("../overlap-bench/cli.ts");
    const { claimOnce, finishOnce } = await import("../overlap-bench/once.ts");
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const d = mkdtempSync(join(tmpdir(), "lab-"));
    writeFileSync(join(d, "labels.json"), '{"incidents":[]}', { mode: 0o600 });
    writeFileSync(join(d, "copy-with-another-name.json"), '{"incidents":[]}', { mode: 0o600 });
    const a = labelsIdentity(join(d, "labels.json")), b = labelsIdentity(join(d, "copy-with-another-name.json"));
    expect(a).toBe(b);
    const { reg } = await setup();
    finishOnce(claimOnce("stage1", a, { f: 1 }, reg), {}, reg);
    expect(() => claimOnce("stage1", b, { f: 1 }, reg)).toThrow("already evaluated");
  });
});
