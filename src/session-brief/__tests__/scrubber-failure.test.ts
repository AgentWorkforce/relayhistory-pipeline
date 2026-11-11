/** Infrastructure failures preserve stored summary state; content refusals still clear it. */
import { expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { ScrubberError, ScrubEngineUnavailable } from "@rhc/lib/scrub-engine/index.ts";
import { ShortSummaryRejected, writeShortSummary } from "../../hoopdb/session-directory-summary.ts";
import { scrubBrief } from "../store.ts";

const input = {
  orgId: "org-test", workspaceId: "workspace-test", harness: "claude", sessionId: "session-test",
  summary: "Updated summary", source: "session-brief/test", generatedAt: new Date("2026-11-10T00:00:00Z"),
};
function database() {
  const queries: string[] = [];
  const db: SqlExecutor = { async query(sql: string) { queries.push(sql); return { rows: [] }; } };
  return { db, queries };
}
const failures = [
  new ScrubEngineUnavailable("engine_failed"),
  new ScrubberError("scrubber_engine_failure", "instance_poison"),
  new ScrubberError("scrubber_instance_poisoned", "instance_poison"),
  new ScrubberError("scrub_admission_unavailable", "call_refusal", undefined, true),
];
for (const failure of failures) {
  const label = failure instanceof ScrubberError ? failure.code : failure.reason;
  for (const failAt of [1, 2]) {
    test(`${failure.name}/${label} on pass ${failAt} preserves the stored short summary`, async () => {
      const { db, queries } = database();
      let calls = 0;
      await expect(writeShortSummary(db, input, value => {
        if (++calls === failAt) throw failure;
        return value;
      })).rejects.toBe(failure);
      expect(calls).toBe(failAt);
      // Neither the old summary nor its searchable projection may be cleared or overwritten.
      expect(queries).toEqual([]);
    });
  }
  test(`${failure.name}/${label} propagates through brief scrubbing`, () => {
    let caught: unknown;
    try {
      scrubBrief({ title: "Updated title", description: "Updated description", state: "in-progress" }, () => { throw failure; });
    } catch (error) { caught = error; }
    expect(caught).toBe(failure);
  });
}
test("a deliberate nonretryable content refusal still clears an older summary", async () => {
  const { db, queries } = database();
  const refusal = new ScrubberError("text_too_large", "call_refusal");
  await expect(writeShortSummary(db, input, () => { throw refusal; })).rejects.toBeInstanceOf(ShortSummaryRejected);
  expect(queries).toHaveLength(1);
  expect(queries[0]).toContain("SET short_summary = NULL");
});
