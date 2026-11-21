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

  const t = (hm: string) => `2026-11-01T${hm}:00.000Z`;
  const conflicts = (r: Awaited<ReturnType<typeof replaySnapshot>>) =>
    r.alerts.filter((a) => isConflictKind(a.kind)).map((a) => `${a.detector} ${a.pair.join("+")} ${a.kind} ${a.urgency} ${a.sentAt.slice(11, 16)}`).sort();
  const split = (r: Awaited<ReturnType<typeof replaySnapshot>>) => {
    const all = conflicts(r);
    const of = (d: string) => all.filter((x) => x.startsWith(`${d} `)).map((x) => x.slice(d.length + 1));
    return { live: of("coordination-advisor"), offline: of("conflict-rule") };
  };
  const both = (r: Awaited<ReturnType<typeof replaySnapshot>>) => {
    const all = conflicts(r);
    const of = (d: string) => all.filter((x) => x.startsWith(`${d} `)).map((x) => x.slice(d.length + 1));
    expect(of("coordination-advisor")).toEqual(of("conflict-rule"));
    return of("conflict-rule");
  };
  const run = async (sessions: unknown[]) => replaySnapshot(await loadSnapshot(writeSnapshot(tmp(), sessions), { now: new Date("2026-11-18T00:00:00Z") }),
    { detectors: [advisorDetector({ conflicts: true }), ruleConflictDetector()] });
  const usage = (id: string) => [{ ts: t("09:05"), eventId: `u-${id}`, tokens: 1, costUsdMicros: 1 }];

  test("a shared intent (one recordId) is agreement: no conflict, live or offline; a shared constraint is cited offline only", async () => {
    const intent = (recordId: string, hm: string) => ({ id: "i1", at: t(hm), kind: "intent", scope: "project", recordId,
      text: "Add pagination to the list route in src/catalog.", confidence: 1, evidenceIds: ["e1"] });
    const pair = (recB: string) => run([
      session("own", { usage: usage("own"), writes: [{ path: "src/catalog/list.ts", ts: t("09:20"), eventId: "w0", kind: "tool_result", branch: null }],
        claims: [intent("rec-1", "09:10")] }),
      session("act", { writes: [{ path: "src/catalog/page.ts", ts: t("09:40"), eventId: "w1", kind: "tool_result", branch: null }], claims: [intent(recB, "09:12")] }),
    ]);
    expect(both(await pair("rec-1"))).toEqual([]);
    expect(both(await pair("rec-2"))).toEqual(["claude:act+claude:own overlap low 09:14"]);
    // The constraint restated by the writer (one record), then broken by its later write. Offline (statement times)
    // the earlier holder's copy is cited: a violation. Live, claim times are ingest times, so the author cannot be
    // told from the restater and neither copy is cited: no row (conservative: a possible missed violation, never a
    // false one).
    const constraint = (hm: string) => ({ id: "k1", at: t(hm), kind: "constraint", scope: "project", recordId: "rec-k",
      text: "No new migrations under supabase/migrations until the 062 renumber lands.", confidence: 1, evidenceIds: ["e1"] });
    const r = await run([
      session("own", { usage: usage("own"), claims: [constraint("09:10")] }),
      session("act", { writes: [{ path: "supabase/migrations/062140_x.sql", ts: t("09:40"), eventId: "w1", kind: "tool_result", branch: null }], claims: [constraint("09:12")] }),
    ]);
    expect(split(r)).toEqual({ live: [], offline: ["claude:act+claude:own constraint_violation high 09:42"] });
    // H-2: the constraint's founder writes later, the restater never does: no conflict, live or offline.
    const founder = await run([
      session("own", { usage: usage("own"), writes: [{ path: "supabase/migrations/062140_x.sql", ts: t("09:40"), eventId: "w1", kind: "tool_result", branch: null }],
        claims: [constraint("09:10")] }),
      session("act", { usage: usage("act"), claims: [constraint("09:12")] }),
    ]);
    expect(both(founder)).toEqual([]);
  });

  test("three sessions, one record: a session that joined an older intent is timed by its own claim, live and offline", async () => {
    // fnd states the intent at 09:20; join wrote at 09:30 and joins the same record at 10:00. join's write came
    // before its intent, so (join, own) is a LOW intent_conflict, not a HIGH.
    const intent = (hm: string) => ({ id: "i1", at: t(hm), kind: "intent", scope: "project", recordId: "rec-i",
      text: "Return 404 when the flag is off in src/advisories routes.", confidence: 1, evidenceIds: [`e-${hm}`] });
    const r = await run([
      session("own", { usage: usage("own"), claims: [{ id: "d1", at: t("09:15"), kind: "decision", scope: "project",
        text: "Advisories return 501 when the flag is off in src/advisories.", confidence: 1, evidenceIds: ["e-d"] }] }),
      session("fnd", { usage: usage("fnd"), claims: [intent("09:20")] }),
      session("join", { writes: [{ path: "src/advisories/route.ts", ts: t("09:30"), eventId: "w1", kind: "tool_result", branch: null }], claims: [intent("10:00")] }),
    ]);
    expect(both(r)).toEqual(["claude:fnd+claude:own intent_conflict low 09:22", "claude:join+claude:own intent_conflict low 10:02"]);
  });
});
