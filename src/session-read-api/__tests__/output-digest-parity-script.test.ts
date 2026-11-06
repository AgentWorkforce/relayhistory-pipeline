import { describe, expect, test } from "bun:test";
import { compareDigest, compareOutput } from "../../../scripts/output-digest-parity.ts";

const output = { since: "a", until: "b", output: { agentMessages: 1, commits: 2, linesAdded: 3, linesRemoved: 4, commitsWithoutLineCounts: 0 } };

describe("output-digest-parity compare", () => {
  test("identical Output bodies are all equal; one differing field is named", () => {
    expect(compareOutput(output, output).every((l) => l.equal)).toBe(true);
    const diff = compareOutput(output, { ...output, output: { ...output.output, commits: 3 } }).filter((l) => !l.equal);
    expect(diff.map((l) => l.field)).toEqual(["output.commits"]);
  });
  test("digest: key order is irrelevant, reverted is excluded, the narrative is informational only", () => {
    const rhc = { date: "d", tz: "UTC", window: { since: "s", until: "u" }, scope: { projectId: null }, projects: [], epics: [], sessions: [],
      stats: { eventCount: 1, outcomes: { committed: 1, reverted: 2, unknown: 0 } }, narrative: { status: "ready", text: "x" } };
    const sp = { ...rhc, window: { until: "u", since: "s" }, stats: { outcomes: { unknown: 0, committed: 1 }, eventCount: 1 }, narrative: { status: "ready", text: "y" } };
    const lines = compareDigest(rhc, sp);
    expect(lines.filter((l) => l.required && !l.equal)).toEqual([]);
    expect(lines.find((l) => l.field === "narrative")!.equal).toBe(false);
    expect(compareDigest(rhc, { ...sp, stats: { ...sp.stats, eventCount: 2 } }).filter((l) => l.required && !l.equal).map((l) => l.field)).toEqual(["stats.eventCount"]);
  });
});
