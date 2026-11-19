/**
 * M2-4 replay parity: the advisor detector with conflicts enabled runs the LIVE wiring (scanOnce with the conflict
 * rows, the replay store's claims reader standing in for 062150) over the conflict fixture. Its conflict rows must
 * give the same classes, urgencies and times as the offline conflict-rule detector, case by case. The fleet case
 * differs only in its pair: the live row goes to the orchestrator.
 */
import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import { isConflictKind } from "../../coordination-advisor/conflicts.ts";
import { ruleConflictDetector } from "../conflict/detector.ts";
import { alertClassOf } from "../replay/classes.ts";
import { advisorDetector, detectorHash } from "../replay/detector.ts";
import { replaySnapshot } from "../replay/replay.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { session, tmp, writeSnapshot } from "./helpers.ts";

const fixture = () => loadSnapshot(join(import.meta.dir, "fixtures", "conflict", "snapshot"), { now: new Date("2026-11-18T00:00:00Z") });
/** The fixture case of a pair: the shared session-id prefix (cv, ic, fl, ...). */
const caseOf = (pair: readonly string[]) => [...new Set(pair.map((p) => p.slice(p.indexOf(":") + 1).split("-")[0]!))].join("+");

describe("live wiring in replay", () => {
  test("advisorDetector({conflicts: true}) gives conflict-rule's classes, urgencies and times", async () => {
    const r = await replaySnapshot(await fixture(), { detectors: [advisorDetector({ conflicts: true }), ruleConflictDetector()] });
    expect(r.incomplete).toEqual([]);
    const key = (a: (typeof r.alerts)[number]) => `${caseOf(a.pair)} ${alertClassOf(a)} ${a.urgency} ${a.sentAt.slice(11, 16)}`;
    const live = r.alerts.filter((a) => a.detector === "coordination-advisor" && isConflictKind(a.kind)).map(key).sort();
    const offline = r.alerts.filter((a) => a.detector === "conflict-rule").map(key).sort();
    expect(live).toEqual(offline);
    expect(live).toEqual([
      "cat overlap low 09:32", "cv constraint_violation high 09:42", "fl intent_conflict low 09:52", "ic intent_conflict high 10:02",
      "id intent_conflict low 10:02", "lk constraint_violation high 11:52", "lk constraint_violation low 11:32",
    ]);
    // The fleet row is the orchestrator's; the workers get none.
    const fleet = r.alerts.filter((a) => a.detector === "coordination-advisor" && caseOf(a.pair) === "fl");
    expect(fleet.map((a) => a.pair)).toEqual([["claude:fl-2", "claude:fl-lead"]]);
    // HIGH conflicts are DMed on the existing path; LOW ones are pull only.
    for (const a of r.alerts.filter((x) => x.detector === "coordination-advisor" && isConflictKind(x.kind)))
      expect(a.channel).toBe(a.urgency === "high" ? "dm" : "pull");
  });

  test("conflicts off (the default) runs no conflict code; the option is hashed; the conflicts control gates it too", async () => {
    const off = await replaySnapshot(await fixture(), { detectors: [advisorDetector()] });
    expect(off.alerts.some((a) => isConflictKind(a.kind))).toBe(false);
    expect(off.detectors[0]!.diagnostics.conflicts).toBeUndefined();
    expect(detectorHash(advisorDetector({ conflicts: true }), { intervalMs: 120_000 })).not.toBe(off.detectors[0]!.hash);
    const ctlOff = await replaySnapshot(await fixture(), { detectors: [advisorDetector({ conflicts: true, controls: { conflicts: false } })] });
    expect(ctlOff.alerts.some((a) => isConflictKind(a.kind))).toBe(false);
  });

  test("a claim shared by both sessions (one recordId) is agreement: no conflict, live or offline", async () => {
    const t = (hm: string) => `2026-11-01T${hm}:00.000Z`;
    const constraint = (recordId: string) => ({ id: "k1", at: t("09:10"), kind: "constraint", scope: "project", recordId,
      text: "No new migrations under supabase/migrations until the 062 renumber lands.", confidence: 1, evidenceIds: ["e1"] });
    const mk = (recordB: string) => loadSnapshot(writeSnapshot(tmp(), [
      session("own", { usage: [{ ts: t("09:05"), eventId: "u1", tokens: 1, costUsdMicros: 1 }], claims: [constraint("rec-1")] }),
      session("act", { writes: [{ path: "supabase/migrations/062140_x.sql", ts: t("09:40"), eventId: "w1", kind: "tool_result", branch: null }],
        claims: [{ ...constraint(recordB), at: t("09:12") }] }),
    ]), { now: new Date("2026-11-18T00:00:00Z") });
    const conflicts = (r: Awaited<ReturnType<typeof replaySnapshot>>) => r.alerts.filter((a) => isConflictKind(a.kind)).map((a) => `${a.detector} ${a.kind}`);
    const shared = await replaySnapshot(await mk("rec-1"), { detectors: [advisorDetector({ conflicts: true }), ruleConflictDetector()] });
    expect(conflicts(shared)).toEqual([]);
    // Control: the same claims as two different records do conflict (the holder's constraint, the other's write).
    const apart = await replaySnapshot(await mk("rec-2"), { detectors: [advisorDetector({ conflicts: true }), ruleConflictDetector()] });
    expect(conflicts(apart)).toEqual(expect.arrayContaining(["conflict-rule constraint_violation", "coordination-advisor constraint_violation"]));
  });
});
