import { afterEach, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { packagedDerivedSchemaSource, derivedSchemaBuildPlugin } from "../derived-schema-build.ts";
import { derivedSchemaAssets } from "../../../src/session-pipeline/derived-schema.ts";
import { ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH, schemaVerdict, type Transact } from "../../../src/shared/db.ts";

const originalMode = process.env.HOOPDB_DB_MODE;
afterEach(() => { if (originalMode === undefined) delete process.env.HOOPDB_DB_MODE; else process.env.HOOPDB_DB_MODE = originalMode; });

test("packaged contract carries the exact authoritative schema and both SQL verifiers", async () => {
  delete process.env.HOOPDB_DB_MODE;
  const expected = await derivedSchemaAssets();
  const source = await packagedDerivedSchemaSource();
  const packaged = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
  const captured = await packaged.derivedSchemaAssets();
  expect(captured).toEqual(expected);
  expect(Object.isFrozen(captured.verifiers)).toBe(true);
  expect(expected.verifiers.searchWorker).toBe(await readFile(ROLE_VERIFY_SQL_PATH, "utf8"));
  expect(expected.verifiers.tokenVerifier).toBe(await readFile(TOKEN_VERIFIER_VERIFY_SQL_PATH, "utf8"));
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
  expect(await schemaVerdict(assets.expected, transact, { verifierSql: { ...assets.verifiers, tokenVerifier: "" } }))
    .toEqual({ ok: false, reason: "role_validation_failed", error_code: "gate_assets_missing" });
  expect(seen).not.toContain(assets.verifiers.searchWorker);
});

test("build plugin only replaces its own schema module", async () => {
  expect(await derivedSchemaBuildPlugin().load.call({ addWatchFile() {} }, "/unrelated/source.ts")).toBeNull();
  const watched: string[] = [];
  await packagedDerivedSchemaSource(path => watched.push(path));
  expect(watched).toContain(ROLE_VERIFY_SQL_PATH);
  expect(watched).toContain(TOKEN_VERIFIER_VERIFY_SQL_PATH);
  const assets = await derivedSchemaAssets();
  for (const name of assets.expected.names)
    expect(watched.some(path => path.endsWith(name.slice(name.indexOf(":") + 1)))).toBe(true);
});
