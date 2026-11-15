import { afterEach, describe, expect, test } from "bun:test";
import { openHoopDb } from "../db.ts";

const URL = "postgres://user:pw@localhost:1/db";
const saved = process.env.HOOPDB_POOL_MAX;

afterEach(() => {
  if (saved === undefined) delete process.env.HOOPDB_POOL_MAX;
  else process.env.HOOPDB_POOL_MAX = saved;
});

async function poolMax(): Promise<number | undefined> {
  const db = openHoopDb(URL);
  try {
    return (db.pool as unknown as { options: { max?: number } }).options.max;
  } finally {
    await db.end();
  }
}

describe("HOOPDB_POOL_MAX", () => {
  test("unset keeps the default of 8", async () => {
    delete process.env.HOOPDB_POOL_MAX;
    expect(await poolMax()).toBe(8);
  });

  test.each([["2", 2], ["64", 64], ["1", 1]])("%p sets the pool max to %p", async (raw, want) => {
    process.env.HOOPDB_POOL_MAX = raw;
    expect(await poolMax()).toBe(want);
  });

  test.each(["0", "08", "65", "1.5", "+2", " 2", "2\n", "abc", "", "999", "-1"])("rejects %p without echoing it", (raw) => {
    process.env.HOOPDB_POOL_MAX = raw;
    let message = "";
    try {
      openHoopDb(URL).end().catch(() => {});
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toContain("HOOPDB_POOL_MAX");
    expect(message).toContain("1 to 64");
    if (raw !== "") expect(message).not.toContain(raw);
  });
});
