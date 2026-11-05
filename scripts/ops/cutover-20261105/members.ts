import { execFileSync } from "node:child_process";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
const { parseCliLogin } = await import(WT + "/scripts/schema-release-owner-operator-plan.ts");
const { SUPABASE_PROD_CA_2021 } = await import(WT + "/src/session-pipeline/supabase-ca.ts");
const HOST = "aws-0-us-east-1.pooler.supabase.com", REF = "pyamkrhoyszgzirqqffq";
const login = parseCliLogin(execFileSync("/home/khaliqgant/.local/bin/supabase", ["db","dump","--linked","--project-ref",REF,"--dry-run"], { cwd: WT, encoding: "utf8", stdio: ["ignore","pipe","pipe"], timeout: 30000 }));
const c = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `cli_login_postgres.${REF}`, password: login.password, ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=15000", application_name: "96bulls-bulls-migrator-members-readonly" });
login.password = "";
await c.connect();
await c.query("BEGIN READ ONLY"); await c.query("SET LOCAL ROLE postgres");
const r = (await c.query(`SELECT r.rolname AS role, m.rolname AS member, g.rolname AS grantor, a.admin_option, a.inherit_option, a.set_option
  FROM pg_auth_members a JOIN pg_roles r ON r.oid=a.roleid JOIN pg_roles m ON m.oid=a.member JOIN pg_roles g ON g.oid=a.grantor
  WHERE r.rolname IN ('bulls96_session_ingest','bulls96_session_ingest_seeder') OR m.rolname IN ('bulls96_session_ingest','bulls96_session_ingest_seeder') ORDER BY 1,2`)).rows;
const v = (await c.query("SELECT current_setting('server_version') AS v")).rows[0];
await c.query("ROLLBACK"); await c.end();
console.log(JSON.stringify({ server: v.v, memberships: r }));
