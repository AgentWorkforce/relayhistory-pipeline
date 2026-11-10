import { describe, expect, test } from "bun:test";
import { sharedWrite60m, type LaneFacts } from "../src/index.ts";

const t = (minute: number) => new Date(Date.UTC(2026, 10, 1, 0, minute)).toISOString();
const lane = (sessionId: string, minute: number, over: Partial<LaneFacts> = {}): LaneFacts => ({
  sessionId, source: "codex", userId: "same-user", projectId: "p", branch: "main", taskRef: null,
  title: sessionId, parentSessionId: null, startedAt: t(minute), lastActionAt: t(minute + 10),
  writes: [{ path: "src/a.ts", ts: t(minute), eventId: `${sessionId}-write`, kind: "tool" }],
  commits: [], outcomes: [], usage: [{ ts: t(minute + 1), eventId: `${sessionId}-usage`, tokens: 10, costUsdMicros: 20 }],
  ...over,
});
const input = (lanes: LaneFacts[]) => ({ workspaceId: "w", window: { since: t(0), until: t(120) }, lanes });

describe("shared-write-60m", () => {
  test("is replay honest: alertAt is the second root's first qualifying write", () => {
    const a = lane("a", 0, { writes: [{ path: "src/a.ts", ts: t(0), eventId: "a0", kind: "tool" }, { path: "src/a.ts", ts: t(50), eventId: "a50", kind: "tool" }] });
    const b = lane("b", 30, { writes: [{ path: "src/a.ts", ts: t(30), eventId: "b30", kind: "tool" }, { path: "src/a.ts", ts: t(40), eventId: "b40", kind: "tool" }] });
    const result = sharedWrite60m.detect(input([a, b]));
    expect(result.findings).toHaveLength(1);
    expect(result.findings[0]!.alertAt).toBe(t(30));
    expect(result.findings[0]!.evidence.map((e) => e.eventId)).toEqual(["a0", "b30"]);
    const outside = lane("outside", 0, { writes: [{ path: "src/a.ts", ts: t(120), eventId: "at-until", kind: "tool" }] });
    expect(sharedWrite60m.detect(input([a, outside])).findings).toEqual([]); // half-open window
  });

  test("excludes a parent and its subagent root, but keeps same-user independent lanes", () => {
    expect(sharedWrite60m.detect(input([lane("parent", 0), lane("child", 5, { parentSessionId: "parent" })])).findings).toEqual([]);
    expect(sharedWrite60m.detect(input([
      lane("child-a", 0, { parentSessionId: "parent-outside-window" }),
      lane("child-b", 5, { parentSessionId: "parent-outside-window" }),
    ])).findings).toEqual([]);
    expect(sharedWrite60m.detect(input([lane("a", 0), lane("b", 5)])).findings).toHaveLength(1);
  });

  test("damps hot paths, including lockfiles and generated manifests", () => {
    const lanes = Array.from({ length: 9 }, (_, i) => lane(`s${i}`, i, { writes: [
      { path: "bun.lock", ts: t(i), eventId: `lock-${i}`, kind: "tool" },
      { path: "generated/manifest.json", ts: t(i), eventId: `manifest-${i}`, kind: "tool" },
    ] }));
    expect(sharedWrite60m.detect(input(lanes)).findings).toEqual([]);
    expect(sharedWrite60m.detect(input(lanes), { hotPathLaneLimit: 9 }).findings.length).toBeGreaterThan(0);
  });

  test("unknown usage is null, never zero", () => {
    const result = sharedWrite60m.detect(input([lane("a", 0), lane("b", 5, { usage: [{ ts: t(6), eventId: "u", tokens: null, costUsdMicros: null }] })]));
    expect(result.findings[0]!.estimatedDuplicateCost.usdMicros).toBeNull();
    expect(result.findings[0]!.estimatedDuplicateCost.tokens).toBeNull();
    expect(result.summary.estimatedDuplicateCostUsdMicros).toBeNull();
  });
});
