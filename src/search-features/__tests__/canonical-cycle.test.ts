import { describe, expect, test } from "bun:test";
import { copyCanonicalEntry } from "../canonical-cycle.ts";
import { featureVersion } from "../source.ts";

const HASH = "a".repeat(64);
const REV = "1000001";
const stamp = "2026-10-30T10:00:00.000000Z";

const eventRow = {
  orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
  machineId: "m-1", kind: "session_event", eventId: "e-1", ts: stamp,
  ingestedAt: stamp, userId: "u", actorName: null, actorRole: "assistant",
  projectId: null, projectFolded: null, taskTitle: "T", taskTitleFolded: "t",
  content: "c", contentFolded: "c", taskRefText: "{}", taskRefTextFolded: "{}",
  tags: [], embedding: null, embeddingModel: null, contentHash: null,
};
const briefRow = {
  orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
  title: "T", description: "D", titleFolded: "t", descriptionFolded: "d",
  briefCompletedAt: stamp, embedding: null, embeddingModel: null,
  contentHash: "h",
};
const images = (over: Partial<Record<string, unknown[] | null>> = {}) => ({
  events: async () => over.events !== undefined ? over.events : [eventRow],
  briefs: async () => over.briefs !== undefined ? over.briefs : [briefRow],
  priorEvents: async () => over.priorEvents ?? [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
    machineId: "m-1", kind: "session_event", eventId: "e-old", ts: stamp }],
  priorBriefs: async () => over.priorBriefs ?? [{
    orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }],
});
const fakeCH = () => {
  const inserts: Record<string, unknown>[][] = [];
  return { inserts, database: "event_feature_fixture_codex_builtin",
    insert: async (_t: string, rows: Record<string, unknown>[]) => {
      inserts.push(rows);
    }, select: async () => [] };
};
const journal = { entries: async () => [] };

describe("canonical copy cycle", () => {
  test("publish entry copies both pools' rows, checkpoint advances", async () => {
    const ch = fakeCH();
    const out = await copyCanonicalEntry(journal, images(), ch,
      { checkpoint: "0" },
      { kind: "publish", revision: REV, imageKey: "img-1",
        schemaGeneration: "g1", sourceHash: HASH });
    expect(out.kind).toBe("copied");
    expect(out.checkpoint).toBe(REV);
    const rows = ch.inserts[0]!;
    expect(rows.length).toBe(2); // one event + one brief
    expect(rows.every((r) => r.version === featureVersion(REV))).toBe(true);
    expect(rows.every((r) => r.source_revision === REV)).toBe(true);
    expect(rows.every((r) => r.deleted === 0)).toBe(true);
  });
  test("retire entry writes tombstones for exact prior keyset", async () => {
    const ch = fakeCH();
    const out = await copyCanonicalEntry(journal, images(), ch,
      { checkpoint: "10" },
      { kind: "retire", revision: "20", imageKey: "img-1",
        schemaGeneration: "g1", priorKeysetHash: HASH });
    expect(out.kind).toBe("copied");
    const rows = ch.inserts[0]!;
    expect(rows.every((r) => r.deleted === 1)).toBe(true);
    expect(rows.find((r) => r.event_id === "e-old")).toBeTruthy();
    expect(rows.find((r) => r.title === "")).toBeTruthy(); // brief tombstone
  });
  test("stale entry at/below checkpoint is refused — delayed writer fenced", async () => {
    const ch = fakeCH();
    const out = await copyCanonicalEntry(journal, images(), ch,
      { checkpoint: "10" },
      { kind: "publish", revision: "10", imageKey: "img-1",
        schemaGeneration: "g1", sourceHash: HASH });
    expect(out).toEqual({ kind: "refused", reason: "stale_revision" });
    expect(ch.inserts.length).toBe(0);
  });
  test("unavailable image refuses — never fabricates rows", async () => {
    const ch = fakeCH();
    const out = await copyCanonicalEntry(journal, images({ events: null }), ch,
      { checkpoint: "0" },
      { kind: "publish", revision: REV, imageKey: "img-1",
        schemaGeneration: "g1", sourceHash: HASH });
    expect(out).toEqual({ kind: "refused", reason: "image_unavailable" });
    expect(ch.inserts.length).toBe(0);
  });
});
