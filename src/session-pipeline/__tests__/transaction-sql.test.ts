import { expect, test } from "bun:test";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { writerTransactionSql } from "../transaction-sql.ts";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createPostgresSearchIndex } from "@rhc/search-index/postgres.ts";

function recorded(name: string, calls: string[]): SqlExecutor {
  return { async query(text) { calls.push(`${name}:${text}`); return { rows: [] }; } };
}

test("public Postgres index writes roll back with later metadata failure in the bound SQL transaction", async () => {
  const db = new PGlite({ extensions: { pg_trgm } });
  const binding = writerTransactionSql(db);
  const index = createPostgresSearchIndex({ client: binding.sql });
  const tenant = { orgId: "org", workspaceId: "ws" };
  try {
    await db.exec("CREATE EXTENSION pg_trgm; CREATE TABLE fixture_metadata(complete boolean)");
    await index.ensureSchema(); // Explicit in-memory fixture owner, never service startup.
    const failure = new Error("synthetic later metadata failure");
    await expect(db.transaction(tx => binding.run(tx, async () => {
      await index.upsert([{
        ...tenant, tsMs: 1, sessionId: "s", source: "codex", machineId: "m", kind: "history", eventId: "e",
        type: "history", userId: "u", actorName: "", actorRole: "", subagentId: "", projectId: "", taskTitle: "", taskRef: "{}", taskRefId: "",
        tags: [], filesTouched: [], toolName: "", toolStatus: "", deliveryRecordId: "", content: "content", fingerprint: "f", version: 1,
      }]);
      await tx.query("INSERT INTO fixture_metadata VALUES(true)");
      throw failure;
    }))).rejects.toBe(failure);
    expect((await index.query({ tenant, order: "asc", limit: 1 })).hits).toEqual([]);
    expect((await db.query("SELECT * FROM fixture_metadata")).rows).toEqual([]);
  } finally { await db.close(); }
}, 30_000);

test("nested bindings restore the outer client after success and failure", async () => {
  const calls: string[] = [];
  const binding = writerTransactionSql(recorded("pool", calls));
  const a = recorded("a", calls), b = recorded("b", calls);
  const failure = new Error("synthetic callback failure");
  await binding.run(a, async () => {
    await binding.sql.query("outer");
    await binding.run(b, () => binding.sql.query("inner"));
    await expect(binding.run(b, async () => { throw failure; })).rejects.toBe(failure);
    await binding.sql.query("outer-restored");
  });
  await binding.sql.query("explicit-outside");
  expect(calls).toEqual(["a:outer", "b:inner", "a:outer-restored", "pool:explicit-outside"]);
});

for (const failed of [false, true]) {
  test(`escaped callbacks cannot reuse a released transaction after ${failed ? "failure" : "success"}`, async () => {
    const calls: string[] = [];
    const binding = writerTransactionSql(recorded("pool", calls));
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let escaped!: Promise<unknown>;
    const failure = new Error("synthetic callback failure");
    const transaction = binding.run(recorded("tx", calls), async () => {
      escaped = gate.then(() => binding.sql.query("late"));
      await binding.sql.query("owned");
      if (failed) throw failure;
    });
    if (failed) await expect(transaction).rejects.toBe(failure); else await transaction;
    release();
    await expect(escaped).rejects.toThrow("Writer transaction is closed");
    await binding.sql.query("outside");
    expect(calls).toEqual(["tx:owned", "pool:outside"]);
  });
}
