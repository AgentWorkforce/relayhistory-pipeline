import { describe, expect, test } from "bun:test";
import {
  CanonicalJournalStore,
  CanonicalVersionStore,
} from "../canonical-journal.ts";

/** In-memory SqlQuery emulation of the 040610 table shape. */
const fakeDb = () => {
  const table: Record<string, unknown>[] = [];
  const query = async <T>(text: string, params: unknown[] = []) => {
    if (text.includes("SELECT revision FROM") && text.includes("DESC"))
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
        revision: params[0], kind: params[1], schema_generation: params[2],
        image_key: params[3], source_hash: params[4],
        prior_keyset_hash: params[5] ?? null,
      });
      return { rows: [] as T[] };
    }
    return { rows: [] as T[] };
  };
  return { table, query };
};
describe("canonical journal store", () => {
  test("seed above legacy clock, never backwards", async () => {
    const db = fakeDb();
    const v = new CanonicalVersionStore({ query: db.query });
    await v.seed("100");
    expect(await v.last()).toBe("100");
    await expect(v.seed("100")).rejects.toThrow("seed_below_last");
    await expect(v.seed("50")).rejects.toThrow("seed_below_last");
  });
  test("next() allocates strictly above the last revision", async () => {
    const db = fakeDb();
    const v = new CanonicalVersionStore({ query: db.query });
    await v.seed("100");
    expect(await v.next()).toBe("101");
    await v.next();
    expect(await v.last()).toBe("100"); // next() only reads, doesn't insert
  });
  test("append writes publish/retire entries; stale fences", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    await j.append({ kind: "publish", revision: "10", imageKey: "i",
      schemaGeneration: "g", sourceHash: "a".repeat(64) }, "0");
    await expect(j.append({ kind: "publish", revision: "5",
      imageKey: "i2", schemaGeneration: "g",
      sourceHash: "b".repeat(64) }, "10")).rejects.toThrow("stale_revision");
    await j.append({ kind: "retire", revision: "20", imageKey: "i",
      schemaGeneration: "g", priorKeysetHash: "c".repeat(64) }, "10");
    const entries = await j.entries("0", 10);
    expect(entries.map((e) => e.kind)).toEqual(["publish", "retire"]);
  });
  test("entries paging respects the after cursor + limit", async () => {
    const db = fakeDb();
    const j = new CanonicalJournalStore({ query: db.query });
    for (const r of ["10", "20", "30"])
      await j.append({ kind: "publish", revision: r, imageKey: "i" + r,
        schemaGeneration: "g", sourceHash: "a".repeat(64) }, "0");
    expect((await j.entries("10", 1)).map((e) => e.revision)).toEqual(["20"]);
    expect((await j.entries("10", 10)).map((e) => e.revision))
      .toEqual(["20", "30"]);
  });
});
