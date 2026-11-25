import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { commitSearchScopeWatermarksCas, createSearchWatermarkStore } from "../search-incremental.ts";

const UP = readFileSync(join(import.meta.dir, "../../../supabase/proposals/incremental-search/search-batch-watermarks.up.sql"), "utf8");
const DOWN = readFileSync(join(import.meta.dir, "../../../supabase/proposals/incremental-search/search-batch-watermarks.down.sql"), "utf8");
const digest = (digit: string) => digit.repeat(64);
const scope = ["org-a", "workspace-a", "codex", "session-a"] as const;

function json(value: unknown): unknown {
  return typeof value === "string" ? JSON.parse(value) : value;
}

describe("M3 proposed watermark RPC boundary", () => {
  let db: PGlite;

  beforeEach(async () => {
    db = new PGlite();
    await db.exec(`CREATE SCHEMA sessions;
      CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;
      GRANT USAGE ON SCHEMA sessions TO service_role;
      CREATE TABLE sessions.session_directory(
        org_id text NOT NULL, workspace_id text NOT NULL, harness text NOT NULL, session_id text NOT NULL,
        deleted_at timestamptz, PRIMARY KEY(org_id,workspace_id,harness,session_id));
      INSERT INTO sessions.session_directory VALUES
        ('org-a','workspace-a','codex','session-a',NULL),
        ('org-b','workspace-a','codex','session-a',NULL);`);
    await db.exec(UP);
  });
  afterEach(async () => db.close());

  async function read(actualScope: readonly string[] = scope): Promise<any> {
    const { rows } = await db.query<{ snapshot: unknown }>(
      "SELECT sessions.read_search_batch_watermarks_v1($1,$2,$3,$4) AS snapshot",
      [...actualScope],
    );
    return json(rows[0]!.snapshot);
  }

  async function cas(expectedVersion: number, expected: unknown[], next: unknown[], actualScope: readonly string[] = scope): Promise<any> {
    const { rows } = await db.query<{ result: unknown }>(
      "SELECT sessions.cas_search_batch_watermarks_v1($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb) AS result",
      [...actualScope, expectedVersion, JSON.stringify(expected), JSON.stringify(next)],
    );
    return json(rows[0]!.result);
  }

  test("seeds, sorts and atomically replaces the complete scope snapshot", async () => {
    expect(await read()).toEqual({ version: 0, watermarks: [] });
    const next = [
      { manifestKey: "b/manifest.json", lastSeq: 3, chainDigest: digest("b") },
      { manifestKey: "a/manifest.json", lastSeq: 1, chainDigest: digest("a") },
    ];
    const committed = await cas(0, [], next);
    expect(committed).toEqual({ committed: true, version: 1, watermarks: [next[1], next[0]] });
    expect(await read()).toEqual({ version: 1, watermarks: [next[1], next[0]] });

    const replacement = [{ manifestKey: "a/manifest.json", lastSeq: 2, chainDigest: digest("c") }];
    expect(await cas(1, [next[0], next[1]], replacement)).toEqual({
      committed: true,
      version: 2,
      watermarks: replacement,
    });
    expect(await read()).toEqual({ version: 2, watermarks: replacement });
  });

  test("version plus exact-set comparison rejects stale and ABA writers", async () => {
    const a = [{ manifestKey: "m", lastSeq: 0, chainDigest: digest("a") }];
    const b = [{ manifestKey: "m", lastSeq: 1, chainDigest: digest("b") }];
    expect((await cas(0, [], a)).committed).toBe(true);       // version 1, A
    expect((await cas(1, a, b)).committed).toBe(true);        // version 2, B
    expect((await cas(2, b, a)).committed).toBe(true);        // version 3, A again
    expect(await cas(1, a, b)).toEqual({ committed: false, version: 3, watermarks: a });
    expect(await cas(3, b, b)).toEqual({ committed: false, version: 3, watermarks: a });
    expect(await read()).toEqual({ version: 3, watermarks: a });
  });

  test("invalid or duplicate evidence fails before changing state", async () => {
    const duplicate = [
      { manifestKey: "same", lastSeq: 0, chainDigest: digest("a") },
      { manifestKey: "same", lastSeq: 1, chainDigest: digest("b") },
    ];
    await expect(cas(0, [], duplicate)).rejects.toMatchObject({ code: "22023" });
    await expect(cas(0, [], [{ manifestKey: "m", lastSeq: 0, chainDigest: "BAD" }])).rejects.toMatchObject({ code: "22023" });
    await expect(cas(0, [], [{ manifestKey: "m", lastSeq: 0, chainDigest: digest("a"), ignored: true }])).rejects.toMatchObject({ code: "22023" });
    await expect(cas(0, [], [], ["unknown", "workspace-a", "codex", "session-a"])).rejects.toMatchObject({ code: "22023" });
    await expect(cas(0, [], Array.from({ length: 257 }, (_, i) => ({
      manifestKey: `m-${i}`, lastSeq: 0, chainDigest: digest("a"),
    })))).rejects.toMatchObject({ code: "22023" });
    expect(await read()).toEqual({ version: 0, watermarks: [] });
  });

  test("tenant scopes are independent and a surrounding failure rolls the CAS back", async () => {
    const row = [{ manifestKey: "m", lastSeq: 0, chainDigest: digest("a") }];
    const other = ["org-b", "workspace-a", "codex", "session-a"];
    expect((await cas(0, [], row, other)).committed).toBe(true);
    expect(await read()).toEqual({ version: 0, watermarks: [] });
    expect(await read(other)).toEqual({ version: 1, watermarks: row });

    await expect(db.transaction(async tx => {
      await tx.query(
        "SELECT sessions.cas_search_batch_watermarks_v1($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb)",
        [...scope, 0, "[]", JSON.stringify(row)],
      );
      throw new Error("synthetic search write failure");
    })).rejects.toThrow("synthetic search write failure");
    expect(await read()).toEqual({ version: 0, watermarks: [] });
  });

  test("runtime receives RPC execution but no table or helper authority", async () => {
    const { rows } = await db.query<{
      read_rpc: boolean; cas_rpc: boolean; helper: boolean; public_read: boolean; public_cas: boolean;
      versions_dml: boolean; watermarks_dml: boolean;
    }>(`SELECT
      has_function_privilege('service_role','sessions.read_search_batch_watermarks_v1(text,text,text,text)','EXECUTE') AS read_rpc,
      has_function_privilege('service_role','sessions.cas_search_batch_watermarks_v1(text,text,text,text,bigint,jsonb,jsonb)','EXECUTE') AS cas_rpc,
      has_function_privilege('service_role','sessions.canonical_search_watermarks_v1(jsonb)','EXECUTE') AS helper,
      has_function_privilege('anon','sessions.read_search_batch_watermarks_v1(text,text,text,text)','EXECUTE') AS public_read,
      has_function_privilege('anon','sessions.cas_search_batch_watermarks_v1(text,text,text,text,bigint,jsonb,jsonb)','EXECUTE') AS public_cas,
      has_table_privilege('service_role','sessions.search_scope_watermark_versions','INSERT,UPDATE,DELETE,TRUNCATE') AS versions_dml,
      has_table_privilege('service_role','sessions.search_batch_watermarks','INSERT,UPDATE,DELETE,TRUNCATE') AS watermarks_dml`);
    expect(rows[0]).toEqual({
      read_rpc: true, cas_rpc: true, helper: false, public_read: false, public_cas: false,
      versions_dml: false, watermarks_dml: false,
    });

    await db.transaction(async tx => {
      await tx.exec("SET LOCAL ROLE service_role");
      expect(json((await tx.query<{ snapshot: unknown }>(
        "SELECT sessions.read_search_batch_watermarks_v1($1,$2,$3,$4) AS snapshot", [...scope],
      )).rows[0]!.snapshot)).toEqual({ version: 0, watermarks: [] });
      await expect(tx.query("SELECT sessions.canonical_search_watermarks_v1('[]'::jsonb)")).rejects.toMatchObject({ code: "42501" });
    });
  });

  test("the TypeScript adapter carries the versioned CAS contract end to end", async () => {
    const store = createSearchWatermarkStore(db);
    const key = { orgId: scope[0], workspaceId: scope[1], harness: scope[2], sessionId: scope[3] };
    const initial = await store.read(key);
    expect(initial).toEqual({ version: 0, watermarks: [] });
    const next = [{ manifestKey: "m", lastSeq: 4, chainDigest: digest("d") }];
    expect(await commitSearchScopeWatermarksCas({
      expected: initial,
      next,
      compareAndSet: (expected, value) => store.compareAndSet(key, expected, value),
    })).toEqual({ status: "committed", snapshot: { version: 1, watermarks: next } });
    expect(await commitSearchScopeWatermarksCas({
      expected: initial,
      next,
      compareAndSet: (expected, value) => store.compareAndSet(key, expected, value),
    })).toEqual({ status: "retry" });
  });

  test("proposal rollback removes only the derived boundary", async () => {
    const row = [{ manifestKey: "m", lastSeq: 0, chainDigest: digest("a") }];
    expect((await cas(0, [], row)).committed).toBe(true);
    await db.exec(DOWN);
    const { rows } = await db.query<{ versions: unknown; watermarks: unknown; directory: unknown }>(
      `SELECT to_regclass('sessions.search_scope_watermark_versions') AS versions,
              to_regclass('sessions.search_batch_watermarks') AS watermarks,
              to_regclass('sessions.session_directory') AS directory`,
    );
    expect(rows[0]).toEqual({ versions: null, watermarks: null, directory: "sessions.session_directory" });
  });
});

