import pg from "pg";
const c = new pg.Client({ connectionString: process.env.URL, application_name: "96bulls-pgss-install", statement_timeout: 30000, lock_timeout: 5000 });
await c.connect();
const q = async (sql) => (await c.query(sql)).rows;
const before = await q("SELECT name, default_version, installed_version FROM pg_available_extensions WHERE name = 'pg_stat_statements'");
console.log("before:", JSON.stringify(before));
if (before.length && !before[0].installed_version) {
  await q("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
}
const after = await q("SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_stat_statements'");
const works = await q("SELECT count(*)::int AS rows FROM pg_stat_statements");
console.log("after:", JSON.stringify(after), "view rows:", works[0].rows);
await c.end();
