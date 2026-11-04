import { afterEach, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { packagedDerivedSchemaSource, derivedSchemaBuildPlugin } from "../derived-schema-build.ts";
import { derivedSchemaAssets } from "../../../src/session-pipeline/derived-schema.ts";
import { ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH, schemaVerdict, type Transact } from "../../../src/shared/db.ts";

import{ROLE_VERIFIERS}from"../../../src/hoopdb/role-verifier-contract.ts";

const originalMode = process.env.HOOPDB_DB_MODE;
afterEach(() => { if (originalMode === undefined) delete process.env.HOOPDB_DB_MODE; else process.env.HOOPDB_DB_MODE = originalMode; });

test("packaged contract carries the exact authoritative schema and every authoritative SQL verifier", async () => {
  delete process.env.HOOPDB_DB_MODE;
  const expected = await derivedSchemaAssets();
  const source = await packagedDerivedSchemaSource();
  const packaged = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
  const captured = await packaged.derivedSchemaAssets();
  expect(captured).toEqual(expected);
  expect(Object.isFrozen(captured.verifiers)).toBe(true);
  expect(expected.verifiers.searchWorker).toBe(await readFile(ROLE_VERIFY_SQL_PATH, "utf8"));
  for(const v of ROLE_VERIFIERS)expect(expected.verifiers[v.key]).toBe(await readFile(v.path,"utf8"));
  process.env.HOOPDB_DB_MODE = "supabase";
  expect((await packaged.derivedSchemaAssets()).expected).toEqual({ ...expected.expected, dbMode: "supabase" });
});

test("an incomplete packaged verifier closes the gate rather than skipping checks", async () => {
  const assets = await derivedSchemaAssets();
  const seen: string[] = [];
  const transact: Transact = work => work({ query: async (sql: string) => {
    seen.push(sql);
    return { rows: sql.includes("FROM sessions.__96bulls_migrations")
      ? assets.expected.names.map(name => ({ name }))
      : sql.includes("WHERE name = $1") ? [{ generation: assets.expected.generation, status: "pass" }] : [] } as never;
  } });
  for(const v of ROLE_VERIFIERS){
    seen.length=0;
    expect(await schemaVerdict(assets.expected, transact, { verifierSql: { ...assets.verifiers, [v.key]: "" } }))
      .toEqual({ ok: false, reason: "role_validation_failed", error_code: "gate_assets_missing" });
    for(const source of Object.values(assets.verifiers))expect(seen).not.toContain(source);
  }
});

test("build plugin only replaces its own schema module", async () => {
  expect(await derivedSchemaBuildPlugin().load.call({ addWatchFile() {} }, "/unrelated/source.ts")).toBeNull();
  const watched: string[] = [];
  await packagedDerivedSchemaSource(path => watched.push(path));
  expect(watched).toContain(ROLE_VERIFY_SQL_PATH);
  for(const v of ROLE_VERIFIERS)expect(watched).toContain(v.path);
  const assets = await derivedSchemaAssets();
  for (const name of assets.expected.names)
    expect(watched.some(path => path.endsWith(name.slice(name.indexOf(":") + 1)))).toBe(true);
});
