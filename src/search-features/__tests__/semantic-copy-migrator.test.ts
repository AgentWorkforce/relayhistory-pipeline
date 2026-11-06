import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { runMigrations } from "../../hoopdb/migrate.ts";
import { replayDatabase } from "../../session-pipeline/__tests__/replay-fixture.ts";

test("full authoritative migrator admits exact metadata functions and reverse rollback/reapply", async () => {
  const f = await replayDatabase();
  try {
    const sql = (path: string) => readFileSync(new URL(`../../../supabase/${path}`, import.meta.url), "utf8");
    const ledger = async () => (await f.pg.query<{ name: string; sha256: string }>(
      'SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name COLLATE "C"',
    )).rows;
    const before = await ledger();
    const claim = async () => (await f.pg.query<{ body: string }>(
      "SELECT prosrc body FROM pg_proc WHERE oid='sessions.ingest_buffer_gc_claim(jsonb)'::regprocedure",
    )).rows[0]!.body;
    const beforeClaim = await claim();
    expect((await f.pg.query<any>("SELECT to_regclass('sessions.search_feature_copy_state')::text v")).rows[0]!.v).toBe("sessions.search_feature_copy_state");
    // Out-of-order downs still refuse. Explicit-transaction downs require cleanup on error.
    await expect(f.pg.exec(sql("rollbacks/20261105035000_semantic_copy_state.down.sql"))).rejects.toMatchObject({ code: "55000" });
    await f.pg.exec("ROLLBACK");
    const target = "upstream:20261105035000_semantic_copy_state.sql";
    const suffix = before.filter(({ name }) => name.startsWith("upstream:") && name >= target);
    for (const { name } of [...suffix].reverse()) {
      await f.pg.exec(sql(`rollbacks/${name.slice("upstream:".length).replace(/\.sql$/, ".down.sql")}`));
    }
    expect((await f.pg.query<any>("SELECT to_regclass('sessions.search_feature_copy_state')::text v")).rows[0]!.v).toBeNull();
    expect((await ledger()).filter(({ name }) => suffix.some(row => row.name === name))).toEqual([]);
    await runMigrations(f.db, { profile: { mode: "local", shim: true, bootstrap: true, verify: true, runtimeRole: "postgres" } });
    expect((await f.pg.query<any>("SELECT to_regclass('sessions.search_feature_sources')::text v")).rows[0]!.v).toBe("sessions.search_feature_sources");
    expect((await f.pg.query<any>("SELECT to_regclass('sessions.canonical_feature_consumer_lease')::text v")).rows[0]!.v).toBe("sessions.canonical_feature_consumer_lease");
    expect((await f.pg.query<any>("SELECT to_regprocedure('sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)')::text v")).rows[0]!.v).toBe("sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)");
    expect(await ledger()).toEqual(before);
    expect(await claim()).toBe(beforeClaim);
  } finally {
    await f.pg.close();
  }
});
