import { describe, expect, test } from "bun:test";
import { writeFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backfillFromIndex } from "../canonical-backfill.ts";
import { manifestIndexImages } from "../canonical-backfill-index.ts";

const KEY = (org: string, ws: string, ses: string, date: string) =>
  `96bulls/${org}/v1/ws=${ws}/m=m/h=chatgpt/d=${date}/s=${ses}/manifest.json`;
const descriptor = async (key: string) => ({
  imageKey: key, sourceHash: "a".repeat(64), schemaGeneration: "g1",
  authorityId: "u-" + key.slice(-3), epoch: "1", fingerprint: "f".repeat(64),
  pointerSetHash: "p".repeat(64), pointerSetCount: "1",
});
const fakePublish = () => {
  const table: Record<string, unknown>[] = [];
  return { table, publishOnce: async (input: {
    scope: { orgId: string; sessionId: string };
    source: { authorityId: string; fingerprint: string };
    pointerSet: { hash: string; count: number };
  }) => {
    const rev = String(table.length + 101);
    table.push({ revision: rev, kind: "publish",
      scope: JSON.stringify(input.scope), org_id: input.scope.orgId,
      source_hash: input.source.fingerprint,
      pointer_set: JSON.stringify(input.pointerSet) });
    return rev;
  } };
};
describe("canonical backfill driver", () => {
  test("JSONL index → publish journal entries per image key", async () => {
    const dir = mkdtempSync(join(tmpdir(), "bf-"));
    const path = join(dir, "idx.jsonl");
    const lines = [
      { org_id: "o1", session_id: "s-1", revision: 1, error: false,
        keys: [KEY("o1", "w1", "s-1", "2026/10/27")] },
      { org_id: "o1", session_id: "s-2", revision: 1, error: false,
        keys: [KEY("o1", "w1", "s-2", "2026/10/28"),
              KEY("o1", "w1", "s-2b", "2026/10/28")] },
      { org_id: "o1", session_id: "s-err", revision: 1, error: true,
        keys: [KEY("o1", "w1", "s-err", "2026/10/28")] },
    ];
    writeFileSync(path, lines.map((l) => JSON.stringify(l)).join("\n"));
    const pub = fakePublish();
    const out = await backfillFromIndex(pub,
      manifestIndexImages(path, descriptor), "100");
    expect(out.kind).toBe("copied");
    expect(out.entries).toBe(3); // 2 keys in s-2, errored skipped
    expect(pub.table.filter((r) => r.kind === "publish").length).toBe(3);
    const revs = pub.table.filter((r) => r.kind === "publish")
      .map((r) => Number(r.revision)).sort();
    expect(revs).toEqual([101, 102, 103]);   // managed revs above seed
    expect(pub.table.every((r) => r.kind !== "publish" || r.scope.includes("o1")))
      .toBe(true);
  });
});
