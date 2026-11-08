import { afterEach, expect, test } from "bun:test";
import pg from "pg";
import { ownerClientConfig, OwnerUrlError } from "../owner-pg-config.ts";

const good = "postgresql://owner:s3cret@db.example.supabase.co:5432/postgres";
const saved = { ...process.env };
afterEach(() => { for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k]; Object.assign(process.env, saved); });

test("builds discrete pinned fields: no connectionString, verified TLS with the pinned CA, fixed name, non-empty options", () => {
  const c = ownerClientConfig(good, "96bulls-bookkeeping-cleanup", "CA");
  expect(c).toEqual({ host: "db.example.supabase.co", port: 5432, user: "owner", password: "s3cret", database: "postgres",
    application_name: "96bulls-bookkeeping-cleanup", ssl: { rejectUnauthorized: true, ca: "CA", servername: "db.example.supabase.co" },
    options: "-c idle_in_transaction_session_timeout=60000" });
  expect("connectionString" in c).toBe(false);
  expect(ownerClientConfig(good + "?sslmode=verify-full", "x", "CA")).toEqual({ ...c, application_name: "x" });
});
test("refuses every URL variant that would weaken TLS, rename the session, or fall back to the environment", () => {
  process.env.PGPASSWORD = "from-env"; process.env.PGUSER = "env-user"; process.env.PGSSLMODE = "disable";
  const cases: Array<[string, string]> = [
    [good + "?sslmode=disable", "query_params"], [good + "?sslmode=no-verify", "query_params"], [good + "?sslmode=prefer", "query_params"],
    [good + "?sslmode=require", "query_params"], [good + "?application_name=other", "query_params"],
    [good + "?sslmode=verify-full&application_name=other", "query_params"],
    ["postgresql://owner@db.example.supabase.co:5432/postgres", "password"], ["postgresql://:pw@db.example.supabase.co/postgres", "user"],
    ["postgresql://owner:pw@db.example.supabase.co/", "database"], ["https://owner:pw@db.example.supabase.co/postgres", "protocol"],
    [good + "#x", "fragment"], ["not a url", "url"],
  ];
  for (const [url, code] of cases) {
    let got: unknown;
    try { ownerClientConfig(url, "x", "CA"); } catch (e) { got = e; }
    expect({ url, err: got instanceof OwnerUrlError ? (got as OwnerUrlError).code : String(got) }).toEqual({ url, err: code });
  }
});
test("pg honours the pinned fields over PG* env (the config, not the environment, decides)", () => {
  process.env.PGPASSWORD = "from-env"; process.env.PGUSER = "env-user"; process.env.PGHOST = "evil.example"; process.env.PGSSLMODE = "disable";
  process.env.PGOPTIONS = "-c statement_timeout=0"; process.env.PGAPPNAME = "env-app";
  const client = new pg.Client(ownerClientConfig(good, "96bulls-bookkeeping-cleanup", "CA")) as unknown as Record<string, any>;
  expect([client.host, client.user, client.password, client.database, client.port]).toEqual(["db.example.supabase.co", "owner", "s3cret", "postgres", 5432]);
  expect(client.ssl).toEqual({ rejectUnauthorized: true, ca: "CA", servername: "db.example.supabase.co" });
  expect(client.connectionParameters.application_name).toBe("96bulls-bookkeeping-cleanup");
  expect(client.connectionParameters.options).toBe("-c idle_in_transaction_session_timeout=60000");
});
