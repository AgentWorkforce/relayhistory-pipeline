/** Source/container contract. The Lambda build plugin replaces this module with
 * a literal capture of the same migration generation and verifier files. */
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { expectedSchema } from "../hoopdb/migrate.ts";
import { ROLE_VERIFY_SQL_PATH, TOKEN_VERIFIER_VERIFY_SQL_PATH, INGEST_SEED_VERIFY_SQL_PATH, INGEST_ROLE_VERIFY_SQL_PATH, type ExpectedSchema, type SchemaVerifierSql } from "../shared/db.ts";

/** The portable storage admission verifier (also a migrator phase-2 step). */
export const PORTABLE_STORAGE_VERIFY_SQL_PATH = fileURLToPath(new URL("../../supabase/bootstrap/portable_search_storage_verify.sql", import.meta.url));
/** The production read-role verifier (also the operator check after read-role activation). */
export const READER_CONTRACT_VERIFY_SQL_PATH = fileURLToPath(new URL("../../supabase/bootstrap/session_read_api_role_verify.sql", import.meta.url));

export interface DerivedSchemaAssets {
  expected: ExpectedSchema;
  /** Exactly the four shared phase-2 verifiers that other consumers (ingest
   * publication, buffered GC) count and run as a set. Do not add keys here. */
  verifiers: SchemaVerifierSql;
  /** 20261105047000 storage admission, run only by the portable writer's assembly
   * admission with its own identity bound; deliberately outside `verifiers`. */
  portableStorageVerifier: string;
  /** supabase/bootstrap/session_read_api_role_verify.sql without psql meta-commands, run
   * only by the portable reader's admission; deliberately outside `verifiers`. */
  readerContractVerifier: string;
}

export async function derivedSchemaAssets(): Promise<DerivedSchemaAssets> {
  const expected = await expectedSchema();
  const [searchWorker, tokenVerifier,ingestSeeder,ingest,portableStorage,readerContract] = await Promise.all([
    readFile(ROLE_VERIFY_SQL_PATH, "utf8"), readFile(TOKEN_VERIFIER_VERIFY_SQL_PATH, "utf8"),readFile(INGEST_SEED_VERIFY_SQL_PATH,"utf8"),readFile(INGEST_ROLE_VERIFY_SQL_PATH,"utf8"),
    readFile(PORTABLE_STORAGE_VERIFY_SQL_PATH, "utf8"), readFile(READER_CONTRACT_VERIFY_SQL_PATH, "utf8"),
  ]);
  return { expected, verifiers: { searchWorker, tokenVerifier,ingestSeeder,ingest }, portableStorageVerifier: portableStorage,
    readerContractVerifier: readerContract.split("\n").filter(line => !line.startsWith("\\")).join("\n") };
}
