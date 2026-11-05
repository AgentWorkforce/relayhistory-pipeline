import { describe, expect, test } from "bun:test";
import {
  CanonicalJournalStore,
  CanonicalVersionStore,
} from "../canonical-journal.ts";

const SCOPE = JSON.stringify({ orgId: "o", workspaceId: "w", harness: "h",
  sessionId: "s" });
const AUTH = JSON.stringify({ epoch: "1", fingerprint: "a".repeat(64) });
const PTR = JSON.stringify({ hash: "p".repeat(64), count: 1, pointers: ["k1"] });
const fakeDb = () => {
  const table: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("ORDER BY revision DESC"))
      return { rows: table.length
        ? [{ revision: String(Math.max(...table.map((r) => Number(r.revision)))) } as T]
        : [] as T[] };
    if (text.includes("COALESCE(MAX(revision)"))
      return { rows: [{ revision: String(
        table.length ? Math.max(...table.map((r) => Number(r.revision))) + 1 : 1) } as T] };
    if (text.includes("WHERE revision >"))
      return { rows: table
        .filter((r) => Number(r.revision) > Number(params[0]) &&
          r.kind !== "seed")
        .sort((a, b) => Number(a.revision) - Number(b.revision))
        .slice(0, Number(params[1])) as T[] };
    if (text.startsWith("INSERT")) {
      table.push({
        revision: params[0], kind: params[1], org_id: params[2],
        workspace_id: params[3], scope: params[4],
        schema_generation: params[5], image_key: params[6],
        source_hash: params[7], prior_keyset_hash: params[8] ?? null,
        source_authority: params[9], prior_source_authority: params[10],
        pointer_set: params[11],
      });
      return { rows: [] as T[] };
    }
    return { rows: [] as T[] };
  };
  return { table, query };
};
const pub = (rev: string, key = "i") => ({
  kind: "publish" as const, revision: rev, imageKey: key,
  schemaGeneration: "g", sourceHash: "a".repeat(64),
  scope: JSON.parse(SCOPE), sourceAuthority: JSON.parse(AUTH),
  pointerSet: JSON.parse(PTR),
});
const ret = (rev: string) => ({
  kind: "retire" as const, revision: rev, imageKey: "i",
  schemaGeneration: "g", priorKeysetHash: "c".repeat(64),
  scope: JSON.parse(SCOPE), priorSourceAuthority: JSON.parse(AUTH),
  pointerSet: JSON.parse(PTR),
});
describe("canonical journal store", () => {
  test("seed above legacy clock, never backwards", async () => {
    const db = fakeDb();
    const v = new CanonicalVersionStore({ query: db.query });
    await v.seed("100");
    expect(await v.last()).toBe("100");
    await expect(v.seed("100")).rejects.toThrow("seed_below_last");
  });
  test("next() allocates strictly above the last revision", async () => {
    const db = fakeDb();
    const v = new CanonicalVersionStore({ query: db.query });
    await v.seed("100");
    expect(await v.next()).toBe("101");
  });
  test("append writes publish/retire entries; stale fences", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    await j.append(pub("10"), "0");
    await expect(j.append(pub("5", "i2"), "10"))
      .rejects.toThrow("stale_revision");
    await j.append(ret("20"), "10");
    const out = await j.entries("0", 10);
    expect(out.entries.map((e) => e.kind)).toEqual(["publish", "retire"]);
    expect(out.hasMore).toBe(false);
  });
  test("entries paging: hasMore, nextRevision, limit != completeness", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    for (const r of ["10", "20", "30"]) await j.append(pub(r, "i" + r), "0");
    const first = await j.entries("10", 1);
    expect(first.entries.map((e) => e.revision)).toEqual(["20"]);
    expect(first.hasMore).toBe(true);
    const rest = await j.entries("10", 10);
    expect(rest.entries.map((e) => e.revision)).toEqual(["20", "30"]);
    expect(rest.hasMore).toBe(false);
    expect(rest.nextRevision).toBe("30");
  });
});
