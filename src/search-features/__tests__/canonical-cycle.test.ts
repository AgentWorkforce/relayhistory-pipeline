import { describe, expect, test } from "bun:test";
import { copyCanonicalEntry } from "../canonical-cycle.ts";
import type {
  CanonicalJournalEntry,
  CanonicalPageAckPort,
} from "../canonical-version.ts";
import { featureVersion } from "../source.ts";

const HASH = "a".repeat(64);
const SCOPE = { orgId: "org", workspaceId: "ws", harness: "h", sessionId: "s-1" };
const AUTH = { epoch: "7", fingerprint: "f".repeat(64) };
const PTR = { hash: "p".repeat(64), count: 1 };
const REV = "1000001";
const stamp = "2026-10-30T10:00:00.000000Z";

const publish = (rev = REV): CanonicalJournalEntry => ({
  kind: "publish", revision: rev, imageKey: "img-1",
  schemaGeneration: "g1", sourceHash: HASH, scope: SCOPE,
  sourceAuthority: AUTH, pointerSet: PTR,
});
const retire = (rev = "1000002"): CanonicalJournalEntry => ({
  kind: "retire", revision: rev, imageKey: "img-1",
  schemaGeneration: "g1", priorKeysetHash: HASH, scope: SCOPE,
  priorSourceAuthority: AUTH, pointerSet: PTR,
});
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
const page = (rows: unknown[], cursor: string | null = null) => ({
  rows, cursor, source: "img", pointerSetHash: "p".repeat(64), pageSha256: "x",
});
const images = (over: Partial<Record<string, unknown>> = {}) => ({
  imagePages: async (_e: CanonicalJournalEntry, kind: "event" | "brief", c: string | null) =>
    c === null ? page(kind === "event" ? [eventRow] : [briefRow], "done")
               : page([], null),
  priorKeyPages: async (_e: CanonicalJournalEntry, kind: "event" | "brief", c: string | null) =>
    c === null ? page(kind === "event" ? [{
        orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1",
        machineId: "m-1", kind: "session_event", eventId: "e-old", ts: stamp }] : [{
        orgId: "org", workspaceId: "ws", source: "codex", sessionId: "s-1" }], "done")
               : page([], null),
  imageDescriptor: async () => ({ imageKey: "img-1", sourceHash: HASH,
    pointerSetHash: "p".repeat(64), complete: true }),
  ...over,
});
const fakeCH = () => {
  const inserts: Record<string, unknown>[][] = [];
  return { inserts, database: "event_feature_fixture_codex_builtin",
    insert: async (_t: string, rows: Record<string, unknown>[]) => {
      inserts.push(rows);
    }, select: async () => [] };
};
const fakeAcks = () => {
  const acked: string[] = [];
  const finished: unknown[] = [];
  const a: CanonicalPageAckPort = {
    ackPage: async (i) => { acked.push(`${i.kind}:${i.cursor}`); },
    finish: async (i) => { finished.push(i); },
  };
  return { a, acked, finished };
};
describe("canonical copy cycle (paged contract)", () => {
  test("publish drains all pages, acks each, finish CAS", async () => {
    const ch = fakeCH(), { a, acked, finished } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "0" }, publish());
    expect(out.kind).toBe("copied");
    expect(out.checkpoint).toBe(REV);
    const rows = ch.inserts.flat();
    expect(rows.every((r) => r.version === featureVersion(REV))).toBe(true);
    expect(acked.length).toBe(4);       // two pages per pool (content + terminal)
    expect(finished.length).toBe(1);
  });
  test("retire pages prior keysets into tombstones", async () => {
    const ch = fakeCH(), { a } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "10" }, retire());
    expect(out.kind).toBe("copied");
    const rows = ch.inserts.flat();
    expect(rows.every((r) => r.deleted === 1)).toBe(true);
    expect(rows.some((r) => r.event_id === "e-old")).toBe(true);
  });
  test("stale entry refuses before any write or ack", async () => {
    const ch = fakeCH(), { a, acked } = fakeAcks();
    const out = await copyCanonicalEntry(images(), a, ch,
      { checkpoint: "10" },
      { ...publish(), revision: "10" });
    expect(out).toEqual({ kind: "refused", reason: "stale_revision" });
    expect(ch.inserts.length).toBe(0);
    expect(acked.length).toBe(0);
  });
});
