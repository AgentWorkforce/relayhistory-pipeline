import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openReadOnlySqlite } from "../src/sqlite-reader.ts";

test("read-only SQLite supports budget interruption and live WAL without changing evidence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "coordination-sqlite-"));
  const path = join(directory, "history.db");
  const writer = new Database(path);
  try {
    writer.exec(
      "PRAGMA journal_mode=WAL; CREATE TABLE evidence (id INTEGER); INSERT INTO evidence VALUES (1), (2)",
    );
    const before = await readFile(path);
    const walBefore = await readFile(`${path}-wal`);
    const reader = openReadOnlySqlite(path);
    try {
      reader.exec("PRAGMA query_only=ON; BEGIN");
      expect(reader.inTransaction).toBe(true);
      reader.function("within_budget", () => 1);
      expect(
        reader
          .query<{ id: number }, [number]>(
            "SELECT id FROM evidence WHERE id=? AND within_budget()=1",
          )
          .all(2),
      ).toEqual([{ id: 2 }]);
      const expired = new Error("local export: read budget exceeded");
      reader.function("expired_budget", () => {
        throw expired;
      });
      expect(() =>
        reader
          .query<{ id: number }, []>(
            "SELECT id FROM evidence WHERE expired_budget()=1",
          )
          .all(),
      ).toThrow("read budget exceeded");
      expect(() => reader.exec("INSERT INTO evidence VALUES (3)")).toThrow();
      reader.exec("ROLLBACK");
      expect(reader.inTransaction).toBe(false);
    } finally {
      if (reader.inTransaction) reader.exec("ROLLBACK");
      reader.close();
    }
    expect(await readFile(path)).toEqual(before);
    expect(await readFile(`${path}-wal`)).toEqual(walBefore);
    expect(writer.query("SELECT count(*) AS n FROM evidence").get()).toEqual({
      n: 2,
    });
  } finally {
    writer.close();
    await rm(directory, { recursive: true, force: true });
  }
});
