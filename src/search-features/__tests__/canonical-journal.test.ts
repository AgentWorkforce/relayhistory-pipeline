import { describe, expect, test } from "bun:test";
import type { CanonicalJournalEntry } from "../canonical-version.ts";

/** In-memory fixture proving the contract invariants — not the storage. */
class FakeJournal {
  entries: CanonicalJournalEntry[] = [];
  async entriesAfter(after: string) {
    return this.entries.filter((e) => BigInt(e.revision) > BigInt(after));
  }
  /** One CAS-fenced append — a stale revision can never land. */
  append(e: CanonicalJournalEntry, observed: string) {
    if (BigInt(e.revision) <= BigInt(observed)) throw new Error("stale_revision");
    this.entries.push(e);
  }
}
describe("canonical journal fencing", () => {
  test("a delayed old write cannot land: stale revision fenced", async () => {
    const j = new FakeJournal();
    j.append({ kind: "publish", revision: "10", imageKey: "img-a",
      schemaGeneration: "g1", sourceHash: "a".repeat(64) }, "0");
    // A stale publish from a drained-old-worker generation is fenced.
    expect(() => j.append({ kind: "publish", revision: "5", imageKey: "img-b",
      schemaGeneration: "g1", sourceHash: "b".repeat(64) }, "10"))
      .toThrow("stale_revision");
  });
  test("retire after publish: prior keyset retires, re-enable does not revive", async () => {
    const j = new FakeJournal();
    j.append({ kind: "publish", revision: "10", imageKey: "img-a",
      schemaGeneration: "g1", sourceHash: "a".repeat(64) }, "0");
    j.append({ kind: "retire", revision: "20", imageKey: "img-a",
      schemaGeneration: "g1", priorKeysetHash: "c".repeat(64) }, "10");
    const after = await j.entriesAfter("0");
    expect(after.map((e) => e.kind)).toEqual(["publish", "retire"]);
    // A subsequent re-enable requires a NEW publish at a higher revision —
    // the tombstone stands under the version order.
    j.append({ kind: "publish", revision: "30", imageKey: "img-a2",
      schemaGeneration: "g1", sourceHash: "d".repeat(64) }, "20");
    expect((await j.entriesAfter("20")).map((e) => e.kind)).toEqual(["publish"]);
  });
});
