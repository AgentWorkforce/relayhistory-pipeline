import { describe, expect, test } from "bun:test";
import { detectCoordination, outcomeFacts, type LaneFacts } from "../src/index.ts";

const t = (hour: number) => new Date(Date.UTC(2026, 10, 1, hour)).toISOString();
const lane = (sessionId: string, over: Partial<LaneFacts> = {}): LaneFacts => ({
  sessionId, source: "codex", userId: "same-user", projectId: "project", repo: "AgentWorkforce/repo",
  branch: `feat/${sessionId}`, taskRef: null, title: sessionId, parentSessionId: null,
  startedAt: t(0), lastActionAt: t(12), writes: [], commits: [], outcomes: [],
  usage: [{ ts: t(3), eventId: `${sessionId}-usage`, tokens: 10, costUsdMicros: 20 }],
  ...over,
});
const write = (sessionId: string, path: string, hour: number, branch = `feat/${sessionId}`) =>
  ({ path, ts: t(hour), eventId: `${sessionId}-${hour}`, kind: "tool", branch });
const input = (lanes: LaneFacts[], since = t(0), until = t(24)) => ({ workspaceId: "workspace", window: { since, until }, lanes });

describe("outcome-facts", () => {
  test("ranks duplicate migration T1, cross-lane revert T2, then weak different-branch edit T3", () => {
    const lanes = [
      lane("migration-a", { writes: [write("migration-a", "db/migrations/0042_first.sql", 0)] }),
      lane("migration-b", { writes: [write("migration-b", "schema/migrations/0042_second.sql", 1)] }),
      lane("commit-a", { commits: [{ sha: "1234567abcdef", ts: t(0), eventId: "commit", reverted: null }] }),
      lane("revert-b", { outcomes: [{ key: "1234567abcdef", ts: t(2), eventId: "outcome", reverted: true }] }),
      lane("edit-a", { writes: [write("edit-a", "src/shared.ts", 0)] }),
      lane("edit-b", { writes: [write("edit-b", "src/shared.ts", 3)] }),
    ];
    const result = outcomeFacts.detect(input(lanes));
    expect(result.detector).toMatchObject({ name: "outcome-facts", version: "1" });
    expect(result.findings.map((finding) => [finding.kind, finding.confidence])).toEqual([
      ["duplicate_work", "high"], ["superseded_work", "high"], ["concurrent_edit", "low"],
    ]);
    expect(result.findings[0]!.artifacts[0]).toMatchObject({ type: "migration", value: "number:0042" });
    expect(result.findings[1]).toMatchObject({ alertAt: t(2), harmAt: t(2), leadTimeMs: 0 });
    expect(result.findings[2]!.alertAt).toBe(t(3));
  });

  test("demotes exact root siblings behind unsuppressed weak evidence", () => {
    const lanes = [
      lane("sibling-a", { parentSessionId: "orchestrator", writes: [write("sibling-a", "db/migrations/0099_a.sql", 0)] }),
      lane("sibling-b", { parentSessionId: "orchestrator", writes: [write("sibling-b", "db/migrations/0099_b.sql", 1)] }),
      lane("independent-a", { writes: [write("independent-a", "src/rare.ts", 0)] }),
      lane("independent-b", { writes: [write("independent-b", "src/rare.ts", 1)] }),
    ];
    const result = outcomeFacts.detect(input(lanes));
    expect(result.findings.map((finding) => finding.kind)).toEqual(["concurrent_edit", "duplicate_work"]);
    const suppressed = result.findings[1]!;
    expect(suppressed.confidence).toBe("low");
    expect(suppressed.explanation).toContain("same orchestrating session");
  });

  test("is replay honest and keeps a stable id across different windows", () => {
    const a = lane("a", { writes: [write("a", "src/rare.ts", 10), write("a", "src/later.ts", 20)] });
    const b = lane("b", { writes: [write("b", "src/rare.ts", 11), write("b", "src/later.ts", 21)] });
    const narrow = outcomeFacts.detect(input([a, b], t(9), t(15))).findings[0]!;
    const broad = outcomeFacts.detect(input([a, b], t(0), t(24))).findings[0]!;
    expect(narrow.alertAt).toBe(t(11));
    expect(broad.alertAt).toBe(t(11));
    expect(narrow.id).toBe(broad.id);
    expect(narrow.evidence.map((event) => event.eventId)).toEqual(["a-10", "b-11"]);
  });

  test("uses earlier lookback evidence but emits only alerts inside the response window", () => {
    const lanes = [
      lane("migration-a", { writes: [write("migration-a", "db/migrations/0042_a.sql", 8)] }),
      lane("migration-b", { writes: [write("migration-b", "db/migrations/0042_b.sql", 10)] }),
      lane("commit-a", { commits: [{ sha: "1234567abcdef", ts: t(8), eventId: "commit", reverted: null }] }),
      lane("revert-b", { outcomes: [{ key: "1234567abcdef", ts: t(10), eventId: "outcome", reverted: true }] }),
      lane("edit-a", { writes: [write("edit-a", "src/lookback.ts", 8)] }),
      lane("edit-b", { writes: [write("edit-b", "src/lookback.ts", 10)] }),
      lane("old-a", { writes: [write("old-a", "src/old.ts", 7)] }),
      lane("old-b", { writes: [write("old-b", "src/old.ts", 9)] }),
    ];
    const result = outcomeFacts.detect(input(lanes, t(10), t(15)));
    expect(result.findings.map((finding) => [finding.kind, finding.alertAt])).toEqual([
      ["duplicate_work", t(10)], ["superseded_work", t(10)], ["concurrent_edit", t(10)],
    ]);
    expect(result.findings.some((finding) => finding.artifacts[0]!.value === "src/old.ts")).toBe(false);
  });

  test("later roots do not retroactively make an earlier path hot", () => {
    const first = lane("first", { writes: [write("first", "src/rare.ts", 10)] });
    const second = lane("second", { writes: [write("second", "src/rare.ts", 11)] });
    const later = Array.from({ length: 8 }, (_, i) => lane(`later-${i}`, {
      writes: [write(`later-${i}`, "src/rare.ts", 12 + i)],
    }));
    const finding = outcomeFacts.detect(input([first, second, ...later], t(9), t(24))).findings
      .find((row) => row.lanes.some((value) => value.sessionId === "first"));
    expect(finding?.alertAt).toBe(t(11));
  });

  test("requires different branches and damps known and high-cardinality hot paths", () => {
    const sameBranch = [
      lane("same-a", { writes: [write("same-a", "src/a.ts", 0, "feat/shared")] }),
      lane("same-b", { writes: [write("same-b", "src/a.ts", 1, "feat/shared")] }),
    ];
    expect(outcomeFacts.detect(input(sameBranch)).findings).toEqual([]);
    const locks = [
      lane("lock-a", { writes: [write("lock-a", "bun.lock", 0)] }),
      lane("lock-b", { writes: [write("lock-b", "bun.lock", 1)] }),
    ];
    expect(outcomeFacts.detect(input(locks)).findings).toEqual([]);
    const hot = Array.from({ length: 9 }, (_, i) => lane(`hot-${i}`, {
      writes: [write(`hot-${i}`, "src/hot.ts", i)],
    }));
    const findings = outcomeFacts.detect(input(hot)).findings;
    expect(findings.length).toBeGreaterThan(0); // alerts emitted before the path became hot remain honest
    expect(findings.every((finding) => finding.alertAt < t(8))).toBe(true);
    expect(findings.some((finding) => finding.lanes.some((item) => item.sessionId === "hot-8"))).toBe(false);
  });

  test("keeps same-user independent lanes, reports unknown cost as null, and leaves the baseline selectable", () => {
    const a = lane("a", { writes: [write("a", "db/migrations/0123_a.sql", 0)] });
    const b = lane("b", {
      writes: [write("b", "db/migrations/0123_b.sql", 1)],
      usage: [{ ts: t(2), eventId: "unknown", tokens: null, costUsdMicros: null }],
    });
    const result = detectCoordination(input([a, b]));
    expect(result.findings).toHaveLength(1);
    expect(result.findings[0]!.estimatedDuplicateCost).toMatchObject({ usdMicros: null, tokens: null });
    expect(result.summary.estimatedDuplicateCostUsdMicros).toBeNull();
    const baseline = detectCoordination(input([a, b]), { detector: "shared-write-60m" });
    expect(baseline.detector.name).toBe("shared-write-60m");
  });
});
