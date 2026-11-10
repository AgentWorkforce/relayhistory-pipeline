import { describe, expect, test } from "bun:test";
import {
  documentFeatureRow,
  produceDocumentFeatures,
} from "../document-producer.ts";

const EV = {
  schemaGeneration: "g", imageKey: "k", revision: "7",
  sourceHash: "a".repeat(64),
};
const ROW = {
  orgId: "o", workspaceId: "w", source: "s", sessionId: "sid",
  title: "T", summary: "S", titleFolded: "t", summaryFolded: "s",
  projectId: "P", lastActivityAt: "2026-11-05T12:00:00.000000Z",
};

describe("document pool producer", () => {
  test("publish rows + tombstones produce feature rows", () => {
    const out = produceDocumentFeatures([ROW],
      [{ orgId: "o", workspaceId: "w", source: "s", sessionId: "old" }], EV);
    expect(out.kind).toBe("rows");
    const rows = (out as { rows: Record<string, unknown>[] }).rows;
    expect(rows.length).toBe(2);
    expect(rows[0]?.title).toBe("T");
    expect(rows[0]?.deleted).toBe(0);
    expect(rows[1]?.deleted).toBe(1);
    expect(rows[1]?.session_id).toBe("old");
  });
  test("malformed row refuses row_invalid", () => {
    const out = produceDocumentFeatures(
      [{ ...ROW, lastActivityAt: "not-a-stamp" } as never], [], EV);
    expect((out as { reason?: string }).reason).toBe("row_invalid");
  });
  test("tombstone with a payload refuses tombstone_invalid", () => {
    const snap = {
      reference: {
        kind: "document" as const,
        key: { orgId: "o", workspaceId: "w", source: "s", sessionId: "x" },
        revision: "7", sourceHash: "a".repeat(64), deleted: true,
      },
      row: { title: "t", summary: "s", titleFolded: "t",
        summaryFolded: "s", projectId: null,
        lastActivityAt: "2026-11-05T12:00:00.000000Z" },
    };
    expect(() => documentFeatureRow(snap)).toThrow();
  });
});
