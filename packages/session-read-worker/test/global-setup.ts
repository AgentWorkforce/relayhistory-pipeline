/**
 * Node-side setup for the workerd suites (vitest.config.ts):
 *
 *   ClickHouse  a fresh database with the production schema, and a dedicated
 *               user with SELECT on the two tables the read path reads and
 *               nothing else, the production read user's grant.
 *   Postgres    a fresh database with the Supabase test shim, every HoopDb
 *               migration and the read role SQL exactly as the lead applies it,
 *               the role's password set to a random per-run value.
 *   RelayAuth   a JWKS server on loopback with a per-run RSA key; the private
 *               JWK goes to the tests so they mint real RS256 assertions.
 *
 * Needs READ_API_TEST_CLICKHOUSE_URL (and _PASSWORD) and
 * READ_WORKER_TEST_PG_URL (a superuser URL). Everything it creates is dropped
 * in teardown. Test-only credentials, generated here, never written to disk.
 */
import { createServer } from "node:http";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import pg from "pg";
import type { TestProject } from "vitest/node";
import { createClickHouseClient } from "../../../src/session-pipeline/vendor/search-index/clickhouse.ts";
import { schemaStatements } from "../../../src/session-pipeline/vendor/search-index/schema.ts";

declare module "vitest" {
  export interface ProvidedContext {
    chUrl: string;
    chAdminUser: string;
    chAdminPassword: string;
    chDatabase: string;
    chReadUser: string;
    chReadPassword: string;
    pgReadUrl: string;
    pgIngestUrl: string;
    pgAdminUrl: string;
    jwksUrl: string;
    signingJwk: JsonWebKey;
  }
}

const root = join(import.meta.dirname, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
const sql = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const id = () => randomBytes(8).toString("hex");

export default async function setup(project: TestProject) {
  const chUrl = process.env.READ_API_TEST_CLICKHOUSE_URL;
  const pgUrl = process.env.READ_WORKER_TEST_PG_URL;
  if (!chUrl || !pgUrl) throw new Error("READ_API_TEST_CLICKHOUSE_URL and READ_WORKER_TEST_PG_URL are required for the Worker suites");
  const chAdminUser = process.env.READ_API_TEST_CLICKHOUSE_USER ?? "default";
  const chAdminPassword = process.env.READ_API_TEST_CLICKHOUSE_PASSWORD ?? "";

  // ClickHouse: schema, then a SELECT-only user.
  const chDatabase = `read_worker_${id()}`;
  const chReadUser = `read_worker_${id()}`;
  const chReadPassword = id() + id();
  const admin = createClickHouseClient({ url: chUrl, user: chAdminUser, password: chAdminPassword, database: "default" });
  await admin.command(`CREATE DATABASE ${chDatabase}`);
  const ch = createClickHouseClient({ url: chUrl, user: chAdminUser, password: chAdminPassword, database: chDatabase });
  for (const statement of schemaStatements()) await ch.command(statement);
  await admin.command(`CREATE USER ${chReadUser} IDENTIFIED WITH sha256_password BY {p:String}`, { params: { p: chReadPassword } });
  await admin.command(`GRANT SELECT ON ${chDatabase}.transcript_events TO ${chReadUser}`);
  await admin.command(`GRANT SELECT ON ${chDatabase}.search_ingest_session_state TO ${chReadUser}`);

  // Postgres: a fresh database as Supabase looks after the HoopDb migrations.
  const pgDatabase = `read_worker_${id()}`;
  const pgReadPassword = id() + id();
  const pgIngestPassword = id() + id();
  const server = new pg.Client({ connectionString: pgUrl });
  await server.connect();
  await server.query(`CREATE DATABASE ${pgDatabase}`);
  const adminUrl = new URL(pgUrl);
  adminUrl.pathname = `/${pgDatabase}`;
  const db = new pg.Client({ connectionString: adminUrl.toString() });
  await db.connect();
  await db.query(read("supabase/tests/supabase_shim.sql"));
  await db.query(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await db.query(read("supabase/bootstrap/session_token_verifier.sql"));
  // The disposable database replays the ingest ledger too; provision its
  // restricted roles through the same privileged bootstrap used by migrate.
  await db.query(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) await db.query(read(`supabase/migrations/${file}`));
  await db.query(sql("supabase/bootstrap/session_read_api_role.sql"));
  // Roles are cluster-wide: the password is per run, so a stale one never logs in.
  await db.query(`ALTER ROLE bulls96_read_api PASSWORD '${pgReadPassword}'`);
  await db.query(`GRANT CONNECT ON DATABASE ${pgDatabase} TO bulls96_read_api`);
  await db.query(sql("supabase/bootstrap/session_read_api_role_verify.sql"));
  // The ingest entry is exercised in workerd against its real restricted role.
  await db.query(`ALTER ROLE bulls96_session_ingest LOGIN PASSWORD '${pgIngestPassword}'`);
  await db.query(`GRANT CONNECT ON DATABASE ${pgDatabase} TO bulls96_session_ingest`);
  await db.end();
  const readUrl = new URL(adminUrl);
  readUrl.username = "bulls96_read_api";
  readUrl.password = pgReadPassword;
  const ingestUrl = new URL(adminUrl);
  ingestUrl.username = "bulls96_session_ingest";
  ingestUrl.password = pgIngestPassword;

  // RelayAuth: a JWKS endpoint with one per-run key.
  const pair = await crypto.subtle.generateKey(
    { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
    true,
    ["sign", "verify"],
  );
  const kid = `test-${id()}`;
  const publicJwk = { ...(await crypto.subtle.exportKey("jwk", pair.publicKey)), kid, alg: "RS256", use: "sig" };
  const signingJwk: JsonWebKey & { kid: string } = { ...(await crypto.subtle.exportKey("jwk", pair.privateKey)), kid };
  const jwks = createServer((req, res) => {
    if (req.url === "/.well-known/jwks.json") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ keys: [publicJwk] }));
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise<void>((resolve) => jwks.listen(0, "127.0.0.1", resolve));
  const port = (jwks.address() as { port: number }).port;

  project.provide("chUrl", chUrl);
  project.provide("chAdminUser", chAdminUser);
  project.provide("chAdminPassword", chAdminPassword);
  project.provide("chDatabase", chDatabase);
  project.provide("chReadUser", chReadUser);
  project.provide("chReadPassword", chReadPassword);
  project.provide("pgReadUrl", readUrl.toString());
  project.provide("pgIngestUrl", ingestUrl.toString());
  project.provide("pgAdminUrl", adminUrl.toString());
  project.provide("jwksUrl", `http://127.0.0.1:${port}/.well-known/jwks.json`);
  project.provide("signingJwk", signingJwk);

  return async () => {
    await new Promise<void>((resolve) => jwks.close(() => resolve()));
    await admin.command(`DROP USER IF EXISTS ${chReadUser}`).catch(() => {});
    await admin.command(`DROP DATABASE IF EXISTS ${chDatabase}`).catch(() => {});
    await server.query(`DROP DATABASE IF EXISTS ${pgDatabase} WITH (FORCE)`).catch(() => {});
    await server.end();
  };
}
