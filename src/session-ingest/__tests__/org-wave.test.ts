/** org-wave.ts: the wave's org list, bounded parallel per-org work with independent verdicts, and the pinned wave id. */
import { expect, test } from "bun:test";
import { WAVE_MAX_CONCURRENCY, WAVE_MAX_ORGS, codeOf, runWave, waveConcurrency, waveId, waveOrgList } from "../org-wave.ts";

const org = (i: number) => `${i.toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`;
const ORGS = Array.from({ length: WAVE_MAX_ORGS }, (_, i) => org(i + 1));
const code = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const acode = async (fn: () => Promise<unknown>) => { try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };

test("the wave's orgs: 1..64 lowercase org UUIDs, a duplicate refused (never deduplicated), sorted", () => {
  expect(waveOrgList([org(3), org(1)])).toEqual([org(1), org(3)]);
  expect(waveOrgList(ORGS)).toHaveLength(64);
  expect(code(() => waveOrgList([]))).toBe("wave_orgs_empty");
  expect(code(() => waveOrgList([...ORGS, org(65)]))).toBe("wave_orgs_oversized");
  expect(code(() => waveOrgList([org(1), org(1)]))).toBe("wave_org_duplicate");
  for (const bad of [["ABCDEF01-0000-4000-8000-000000000000"], ["org-a"], [1], "x", null, [` ${org(1)}`]]) expect(code(() => waveOrgList(bad))).toBe("wave_org_invalid");
  // The refusal never carries the value.
  try { waveOrgList([org(1), org(1)]); } catch (e) { expect(String((e as Error).message)).not.toContain(org(1)); }
});

test("runWave: at most `concurrency` in flight, every org settled on its own, input order, single org compatible", async () => {
  let inFlight = 0, peak = 0;
  const out = await runWave(ORGS, 8, async (o, i) => {
    inFlight++; peak = Math.max(peak, inFlight);
    await new Promise((r) => setTimeout(r, 1 + (i % 3)));
    inFlight--;
    if (i % 5 === 0) throw Object.assign(new Error(`boom ${o}`), { code: "known_refusal" });
    if (i % 7 === 0) throw new Error(`secret ${o}`);
    return i;
  }, { allowed: new Set(["known_refusal"]), fallback: "wave_org_failed" });
  expect(peak).toBe(8);
  expect(out.map((s) => s.org)).toEqual(ORGS);
  for (const [i, s] of out.entries()) {
    if (i % 5 === 0) expect(s).toEqual({ org: ORGS[i]!, ok: false, code: "known_refusal" });
    else if (i % 7 === 0) expect(s).toEqual({ org: ORGS[i]!, ok: false, code: "wave_org_failed" }); // a message never becomes a code
    else expect(s).toEqual({ org: ORGS[i]!, ok: true, value: i });
  }
  // One org: one worker, whatever was asked.
  let peak1 = 0, n = 0;
  expect(await runWave([org(9)], 16, async () => { n++; peak1 = Math.max(peak1, n); return "x"; })).toEqual([{ org: org(9), ok: true, value: "x" }]);
  expect(peak1).toBe(1);
  expect(await acode(() => runWave([], 4, async () => 1))).toBe("wave_orgs_empty");
  expect(await acode(() => runWave([org(1), org(1)], 4, async () => 1))).toBe("wave_org_duplicate");
  expect(await acode(() => runWave([...ORGS, org(99)], 4, async () => 1))).toBe("wave_orgs_oversized");
  for (const c of [0, -1, 1.5, WAVE_MAX_CONCURRENCY + 1]) expect(await acode(() => runWave([org(1)], c, async () => 1))).toBe("wave_concurrency_invalid");
  expect(waveConcurrency(16, 3)).toBe(3);
  expect(codeOf({ code: "Not_A_Code" }, new Set(["Not_A_Code"]), "fb")).toBe("fb");
});

test("waveId: pins the commit, the scope, every bound run and each org's epoch; names none of them", () => {
  const h = (c: string) => "h_" + c.repeat(32);
  const w = { sha: "1".repeat(40), scope: "orgs_" + "a".repeat(32), runs: { authority: "1", backfill: "2", full: "3", reshard: "4" },
    orgs: [{ orgHash: h("1"), epochHash: h("2") }, { orgHash: h("3"), epochHash: h("4") }] };
  const id = waveId(w);
  expect(id).toMatch(/^h_[0-9a-f]{32}$/u);
  expect(waveId({ ...w, orgs: [...w.orgs].reverse() })).toBe(id); // order-free
  for (const other of [{ ...w, sha: "2".repeat(40) }, { ...w, scope: "orgs_" + "b".repeat(32) }, { ...w, runs: { ...w.runs, full: "5" } },
    { ...w, orgs: [w.orgs[0]!] }, { ...w, orgs: [w.orgs[0]!, { orgHash: h("3"), epochHash: h("5") }] }])
    expect(waveId(other)).not.toBe(id);
  for (const bad of [{ ...w, scope: "all" }, { ...w, runs: {} }, { ...w, orgs: [] }, { ...w, orgs: [w.orgs[0]!, w.orgs[0]!] },
    { ...w, orgs: [{ orgHash: "x", epochHash: h("1") }] }, { ...w, runs: { full: "01" } }])
    expect(code(() => waveId(bad))).toBe("wave_identity_invalid");
});
