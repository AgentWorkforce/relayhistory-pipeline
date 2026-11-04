/** Source/container contract. The Lambda build plugin replaces this module with
 * a literal capture of the same migration generation and verifier files. */
import { readFile } from "node:fs/promises";
import { expectedSchema } from "../hoopdb/migrate.ts";
import type{ExpectedSchema,SchemaVerifierSql}from"../shared/db.ts";
import{ROLE_VERIFIERS}from"../hoopdb/role-verifier-contract.ts";

export interface DerivedSchemaAssets {
  expected: ExpectedSchema;
  verifiers: SchemaVerifierSql;
}

export async function derivedSchemaAssets(): Promise<DerivedSchemaAssets> {
  const expected = await expectedSchema();
  const verifiers=Object.fromEntries(await Promise.all(ROLE_VERIFIERS.map(async v=>[v.key,await readFile(v.path,"utf8")]))) as unknown as SchemaVerifierSql;
  return {expected,verifiers};
}
