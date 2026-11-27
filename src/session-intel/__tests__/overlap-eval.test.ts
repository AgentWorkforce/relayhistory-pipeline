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

describe("round 6", () => {
  test("an interrupted held-out score cannot be redone with other verdicts, in any directory", async () => {
    const { createHash } = await import("node:crypto");
    const { mkdtempSync, mkdirSync, writeFileSync, cpSync, readFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const sha = (t: string) => createHash("sha256").update(t).digest("hex");
    const home = mkdtempSync(join(tmpdir(), "home-")), L = "b".repeat(64), P = { mode: 0o600 };
    const batch = join(mkdtempSync(join(tmpdir(), "b-")), "batch");
    mkdirSync(join(batch, "judge"), { recursive: true, mode: 0o700 }); mkdirSync(join(batch, "key"), { mode: 0o700 });
    const key = [{ pair: "p1", truth: "incident:x" }], keyText = JSON.stringify(key), part = "{}\n", prompt = "p";
    const verdicts = (p: number) => JSON.stringify([{ pair: "p1", verdict: "overlap", p_overlap: p }]);
    writeFileSync(join(batch, "key", "key.json"), keyText, P); writeFileSync(join(batch, "judge", "part-00"), part, P); writeFileSync(join(batch, "judge", "prompt-00.txt"), prompt, P);
    writeFileSync(join(batch, "judge", "verdicts-00.json"), verdicts(0.9), P);
    const terms = { t: 1 };
    const body = { format: "overlap-bench-stage2/3", split: "heldout", terms, cutoff: 0.3, promptHash: "x", seed: "s", judge: "j", labelsHash: L, deleteBy: "2099-01-01T00:00:00.000Z", items: 1, keyHash: sha(keyText), parts: { "part-00": sha(part) }, prompts: { "prompt-00.txt": sha(prompt) } };
    const manifestHash = sha(JSON.stringify(body));
    writeFileSync(join(batch, "key", "stage2.json"), JSON.stringify({ ...body, manifestHash }), P);
    // The registry as an interrupted run left it: batch pinned, score pinned with the first verdicts, not finished.
    const reg = join(home, ".local", "state", "overlap-bench", "heldout");
    mkdirSync(reg, { recursive: true, mode: 0o700 });
    const vh = sha(JSON.stringify([["p1", "overlap", 0.9]]));
    const pinned = { positives: 1, positivesKept: 1, fresh: 0, freshKept: 0, cutoff: 0.3, marker: "PINNED" };
    writeFileSync(join(reg, `stage2-${L}.json`), JSON.stringify({ kind: "stage2", labelsHash: L, terms, state: "claimed", claimedAt: "x", owner: 999_999_99, batch: manifestHash, score: { verdictsHash: vh, result: pinned } }), P);
    const cli = join(import.meta.dir, "..", "overlap-bench", "cli.ts");
    const run = async (dir: string) => {
      const p = Bun.spawn([process.execPath, cli, "stage2-score", "--out", dir], { env: { ...process.env, HOME: home }, stdout: "pipe", stderr: "pipe" });
      await p.exited;
      return { code: p.exitCode, err: await new Response(p.stderr).text() };
    };
    // Changed verdicts in a copy elsewhere: refused, and the registry is untouched.
    const copy = join(mkdtempSync(join(tmpdir(), "c-")), "batch");
    cpSync(batch, copy, { recursive: true });
    writeFileSync(join(copy, "judge", "verdicts-00.json"), verdicts(0.1), P);
    const r1 = await run(copy);
    expect(r1.code).not.toBe(0);
    expect(r1.err).toContain("differ from the ones this held-out score was pinned with");
    // The original verdicts: the pinned result is published and the evaluation completes.
    const r2 = await run(batch);
    expect(r2.code).toBe(0);
    expect(JSON.parse(readFileSync(join(batch, "key", "score.json"), "utf8")).marker).toBe("PINNED");
    expect(JSON.parse(readFileSync(join(reg, `stage2-${L}.json`), "utf8")).state).toBe("done");
    expect((await run(batch)).code).not.toBe(0);
  });
  test("a reserved list request is sent once: a lost answer is not retried", async () => {
    const { harvestMcpFrame } = await import("../cloud-harvest/mcp-source.ts");
    const { CloudClient } = await import("../cloud-harvest/client.ts");
    const limits: number[] = [];
    const fetchImpl = (async (_u: string, init: RequestInit) => { limits.push(JSON.parse(String(init.body)).params.arguments.limit); throw new Error("connection reset"); }) as unknown as typeof fetch;
    const c = new CloudClient({ base: "https://cloud.example/cloud/api/v1", workspaceId: "00000000-0000-4000-8000-000000000001", maxRequests: 100, fetchImpl, sleep: async () => {} }, "cld_test_token_abcdefghijklmnop");
    let spent = 0;
    const budget = { reserve: (w: number) => { const g = Math.min(w, 3 - spent); spent += g; return g; }, settle: () => {} };
    await expect(harvestMcpFrame(c, { since: "2026-11-01T00:00:00Z", until: "2026-12-01T00:00:00Z" }, () => {}, budget)).rejects.toThrow("transport");
    expect(limits).toEqual([3]);
    expect(spent).toBe(3); // the reservation stays spent: the server may have served it
  });
  test("a batch's deadline is the earliest delete-by as an instant, whatever the format", async () => {
    const { earliestDeleteBy } = await import("../overlap-bench/cli.ts");
    // As strings, ".900Z" sorts before "Z"; as instants it is 900 ms later.
    expect(earliestDeleteBy([{ deleteBy: "2026-12-09T00:00:00.900Z" }, { deleteBy: "2026-12-09T00:00:00Z" }])).toBe("2026-12-09T00:00:00.000Z");
    expect(() => earliestDeleteBy([{ deleteBy: "2026-12-09" }])).toThrow("canonical UTC");
  });
});

describe("round 7", () => {
  test("admitted times must be real calendar instants", async () => {
    const { parseInstant, instantOf } = await import("../cloud-harvest/instant.ts");
    const { earliestDeleteBy } = await import("../overlap-bench/cli.ts");
    for (const ok of ["2028-02-29T00:00:00Z", "2026-12-09T00:00:00.900Z", "2026-12-31T23:59:59.123000000Z", "2000-02-29T12:00:00Z"]) expect(Number.isFinite(parseInstant(ok))).toBe(true);
    for (const bad of ["2099-02-30T00:00:00Z", "2027-02-29T00:00:00Z", "1900-02-29T00:00:00Z", "2026-04-31T00:00:00Z", "2026-13-01T00:00:00Z", "2026-00-10T00:00:00Z",
      "2026-12-09T24:00:00Z", "2026-12-09T23:60:00Z", "2026-12-09T23:59:60Z", "2026-12-09T00:00:00+00:00", "2026-12-09", "2026-12-09T00:00:00.Z", "2026-12-31T23:59:59.123456789Z"]) {
      expect(Number.isNaN(parseInstant(bad))).toBe(true);
    }
    expect(() => instantOf("2099-02-30T00:00:00Z", "deleteBy")).toThrow("real calendar date");
    expect(() => earliestDeleteBy([{ deleteBy: "2099-02-30T00:00:00Z" }])).toThrow("real calendar date");
  });
  test("a batch with an impossible deadline, or with non-private key or judge directories, is refused", async () => {
    const { verifiedBatch } = await import("../overlap-bench/stage2.ts");
    const { createHash } = await import("node:crypto");
    const { mkdtempSync, mkdirSync, writeFileSync, chmodSync, cpSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const sha = (t: string) => createHash("sha256").update(t).digest("hex");
    const make = (deleteBy: string) => {
      const d = mkdtempSync(join(tmpdir(), "b7-")), P = { mode: 0o600 };
      mkdirSync(join(d, "judge"), { mode: 0o700 }); mkdirSync(join(d, "key"), { mode: 0o700 });
      const keyText = JSON.stringify([{ pair: "a", truth: "fresh" }]), part = "{}\n", prompt = "p";
      writeFileSync(join(d, "key", "key.json"), keyText, P); writeFileSync(join(d, "judge", "part-00"), part, P); writeFileSync(join(d, "judge", "prompt-00.txt"), prompt, P);
      writeFileSync(join(d, "judge", "verdicts-00.json"), JSON.stringify([{ pair: "a", verdict: "not_overlap", p_overlap: 0.1 }]), P);
      const body = { format: "overlap-bench-stage2/3", split: "dev", terms: {}, cutoff: 0.3, promptHash: "x", seed: "s", judge: "j", labelsHash: "l", deleteBy, items: 1, keyHash: sha(keyText), parts: { "part-00": sha(part) }, prompts: { "prompt-00.txt": sha(prompt) } };
      writeFileSync(join(d, "key", "stage2.json"), JSON.stringify({ ...body, manifestHash: sha(JSON.stringify(body)) }), P);
      return d;
    };
    expect(() => verifiedBatch(make("2099-02-30T00:00:00Z"))).toThrow("delete-by");
    const ok = make("2099-01-01T00:00:00Z");
    expect(verifiedBatch(ok).verdicts).toHaveLength(1);
    // A copied batch whose subdirectories were opened up (0755) is refused before anything in them is read.
    const copy = join(mkdtempSync(join(tmpdir(), "b7c-")), "batch");
    cpSync(ok, copy, { recursive: true });
    chmodSync(copy, 0o700); chmodSync(join(copy, "key"), 0o755);
    expect(() => verifiedBatch(copy)).toThrow("open to group or others");
    chmodSync(join(copy, "key"), 0o700); chmodSync(join(copy, "judge"), 0o755);
    expect(() => verifiedBatch(copy)).toThrow("open to group or others");
  });
});

describe("round 8", () => {
  test("admitted bounds are millisecond-exact; sub-millisecond digits are refused, not dropped", async () => {
    const { parseInstant } = await import("../cloud-harvest/instant.ts");
    expect(parseInstant("2026-11-01T00:00:00.900000000Z")).toBe(Date.parse("2026-11-01T00:00:00.900Z"));
    expect(Number.isNaN(parseInstant("2026-11-01T00:00:00.000900000Z"))).toBe(true);
    expect(Number.isNaN(parseInstant("2026-11-01T00:00:00.0001Z"))).toBe(true);
  });
  test("event times are compared at full precision at both edges, through the collector", async () => {
    const { inWindow } = await import("../cloud-harvest/harvest.ts");
    const w = { since: "2026-11-01T00:00:00.001Z", until: "2026-11-02T00:00:00.000Z" };
    expect(inWindow("2026-11-01T00:00:00.000100000Z", w)).toBe(-1); // 0.9 ms before the start
    expect(inWindow("2026-11-01T00:00:00.001000000Z", w)).toBe(0);
    expect(inWindow("2026-11-02T00:00:00.000000000Z", w)).toBe(0);
    expect(inWindow("2026-11-02T00:00:00.000000001Z", w)).toBe(1); // 1 ns after the end
    expect(inWindow("2026-11-02T00:00:00.000900000Z", w)).toBe(1);
    expect(inWindow("2026-11-01T01:00:00.000+01:00", w)).toBe(-1); // = 00:00:00.000Z, before the start
  });
  test("a window bound with sub-millisecond digits is refused", async () => {
    const { inWindow } = await import("../cloud-harvest/harvest.ts");
    expect(() => inWindow("2026-11-02T00:00:00Z", { since: "2026-11-01T00:00:00.000900000Z", until: "2026-11-03T00:00:00Z" })).toThrow("millisecond precision");
  });
  test("the freeze's sources follow the import graph, and a change to the parser changes the identity", async () => {
    const { sourceFiles, sourceHashOf } = await import("../overlap-bench/cli.ts");
    const { readFileSync } = await import("node:fs");
    const files = sourceFiles();
    for (const f of ["cloud-harvest/instant.ts", "cloud-harvest/private-fs.ts", "overlap-bench/once.ts", "overlap-bench/stage2.ts", "packages/coordination-detect/src/index.ts"]) {
      expect(files.some((x) => x.endsWith(f))).toBe(true);
    }
    const parser = files.find((x) => x.endsWith("cloud-harvest/instant.ts"))!;
    const changed = (f: string) => (f === parser ? Buffer.from(readFileSync(f, "utf8").replace("1_000_000n", "1_000_001n")) : readFileSync(f));
    expect(sourceHashOf(files, changed)).not.toBe(sourceHashOf(files));
  });
});
