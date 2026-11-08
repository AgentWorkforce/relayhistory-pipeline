/** Explicit isolated loopback PG17 proof; never targets a live/project database.
 * Opt-in (HISTORY_NATIVE_TEST=true) on a disposable cluster named released-history-fixture.
 * Ported from #396 onto MIGRATION_RENAME_ALIASES. */
import { expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import pg from "pg";
import { plan, runMigrations, expectedSchema, migrationLedgerSha256, type MigrationClient, type MigrationProfile } from "../migrate.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { MIGRATION_SENTINELS } from "../migration-sentinels.ts";
import { MIGRATION_RENAME_ALIASES, sessionsFunctionAclDrift } from "../migrate-guards.ts";
import { schemaVerdict, type Transact } from "../../shared/db.ts";
setDefaultTimeout(120_000);
const root = join(import.meta.dir, "../../..");
const fixtures = join(import.meta.dir, "fixtures/released-migration-history");
const rows = JSON.parse(readFileSync(join(fixtures,"ledger.json"),"utf8")) as {name:string;sha256:string}[];
const old: MigrationProfile = {mode:"local",shim:true,bootstrap:false,verify:false,runtimeRole:"hoop"};
const next: MigrationProfile = {mode:"selfhosted",shim:false,bootstrap:false,verify:true,runtimeRole:"sessions_pipeline_runtime",runtimeLogin:true,functionBoundary:"global-default-revoke"};
const sql = (c:pg.Client):MigrationClient => ({async query<R>(text:string,params?:unknown[]) { const result=await c.query(text,params);return {rows:result.rows as R[]}; }});
const strip = (text:string) => text.split("\n").filter(line=>!/^\s*\\/.test(line)).join("\n");
const rejected = async (p:Promise<unknown>) => p.then(()=>null,(e:unknown)=>e as {code?:string});
const connect = async (database:string,user:string) => {const c=new pg.Client({host:"127.0.0.1",port:5432,database,user,ssl:false});await c.connect();return c;};
const txn = (c:pg.Client):Transact => async work=>{await c.query("BEGIN");try{const v=await work(sql(c));await c.query("COMMIT");return v;}catch(e){await c.query("ROLLBACK");throw e;}};
const liveRows = async(c:pg.Client)=>(await c.query("SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name")).rows as {name:string;sha256:string}[];
const selected = async(c:pg.Client)=>(await c.query("SELECT * FROM sessions.session_index ORDER BY org_id,workspace_id,harness,session_id,machine_id")).rows;
const native = process.env.HISTORY_NATIVE_TEST === "true" ? test : test.skip;
native("native PG17: released image ledger upgrades in place as unchanged non-CREATEROLE owner, no history rewrite", async()=>{
 const admin=await connect("postgres","postgres");let op:pg.Client|undefined;let owner:pg.Client|undefined;let runtime:pg.Client|undefined;
 try {
  // This opt-in proof must not mutate an unrelated loopback PostgreSQL server.
  expect((await admin.query("SHOW cluster_name")).rows[0]?.cluster_name).toBe("released-history-fixture");
  expect((await admin.query("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")).rows.map(r=>r.datname)).toEqual(["postgres"]);
  expect((await admin.query("SELECT rolname FROM pg_roles WHERE rolname !~ '^pg_' AND rolname <> 'postgres'")).rowCount).toBe(0);
  expect(Number((await admin.query("SHOW server_version_num")).rows[0]?.server_version_num)).toBeGreaterThanOrEqual(170000);
  expect((await admin.query("SELECT 1 FROM pg_database WHERE datname='hoopdb'")).rowCount).toBe(0);
  await admin.query("CREATE ROLE hoop LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION");
  await admin.query("CREATE DATABASE hoopdb OWNER hoop");
  op=await connect("hoopdb","postgres");
  await op.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  await op.query(readFileSync(join(root,"supabase/tests/supabase_shim.sql"),"utf8"));
  for(const file of SESSION_BOOTSTRAP_FILES)await op.query(strip(readFileSync(join(root,file),"utf8")));
  await op.query("CREATE ROLE sessions_pipeline_runtime LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION; REVOKE TEMPORARY ON DATABASE hoopdb FROM PUBLIC; GRANT CONNECT ON DATABASE hoopdb TO sessions_pipeline_runtime");
  await op.query(strip(readFileSync(join(root,"supabase/bootstrap/hoopdb_function_default_revoke.sql"),"utf8")).replaceAll(":'migration_owner'","'hoop'"));
  owner=await connect("hoopdb","hoop");
  const aliases=new Map(MIGRATION_RENAME_ALIASES.map(a=>[a.migration as string,a]));const wanted=new Set(rows.map(r=>r.name));
  const oldSteps=(await plan(old)).map(step=>{const a=aliases.get(step.name);return a?{name:a.ledger_name,path:join(fixtures,a.ledger_name.slice("upstream:".length)),sentinel:MIGRATION_SENTINELS[a.migration]}:step;}).filter(s=>wanted.has(s.name));
  const lane=(name:string)=>name.startsWith("shim:")?0:name.startsWith("upstream:")?1:2;
  oldSteps.sort((a,b)=>lane(a.name)-lane(b.name)||a.name.localeCompare(b.name));
  await runMigrations(sql(owner),{profile:old,steps:oldSteps});
  expect(await liveRows(owner)).toEqual(rows);
  await owner.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,title,created_at,last_activity_at,storage_provider,bucket,manifest_key,trace_id) VALUES('org_native','ws_native','codex','historical-native','machine_native','user_native','keep native history',now(),now(),'s3','fixture','96bulls/org_native/v1/ws=ws_native/m=machine_native/h=codex/d=2026/11/08/s=historical-native/manifest.json','01984624-2222-7000-8000-111111111111')`);
  const preserved=await selected(owner);const ownerAttrs=(await op.query("SELECT rolsuper,rolcreaterole,rolcreatedb,rolbypassrls FROM pg_roles WHERE rolname='hoop'")).rows;
  const schemaOwner=(await owner.query("SELECT nspowner FROM pg_namespace WHERE nspname='sessions'")).rows;
  const relationOwners=(await owner.query("SELECT c.relname,c.relkind,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' ORDER BY c.relname")).rows;
  expect((await owner.query("SELECT has_function_privilege('bulls96_session_ingest_seeder','sessions.ingest_seed_records(jsonb)','EXECUTE') AS ok")).rows[0]?.ok).toBe(true);
  const rawBinding=migrationLedgerSha256(new Map((await liveRows(owner)).map(r=>[r.name,r.sha256])));
  const applied=await runMigrations(sql(owner),{profile:next,expectedLedgerSha256:rawBinding});
  expect(applied.applied).toBeGreaterThan(0);
  expect(await selected(owner)).toEqual(preserved);
  const physicalAfter=await liveRows(owner);
  for(const row of rows)expect(physicalAfter.find(r=>r.name===row.name)).toEqual(row);
  for(const a of MIGRATION_RENAME_ALIASES)expect(physicalAfter.some(r=>r.name===a.migration)).toBe(false);
  expect((await op.query("SELECT rolsuper,rolcreaterole,rolcreatedb,rolbypassrls FROM pg_roles WHERE rolname='hoop'")).rows).toEqual(ownerAttrs);
  expect((await owner.query("SELECT nspowner FROM pg_namespace WHERE nspname='sessions'")).rows).toEqual(schemaOwner);
  const afterOwners=(await owner.query("SELECT c.relname,c.relkind,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' ORDER BY c.relname")).rows;
  for(const relation of relationOwners)expect(afterOwners.find(r=>r.relname===relation.relname)).toEqual(relation);
  expect((await owner.query("SELECT has_function_privilege('bulls96_session_ingest_seeder','sessions.ingest_seed_records(jsonb)','EXECUTE') AS ok")).rows[0]?.ok).toBe(false);
  expect(await sessionsFunctionAclDrift(sql(owner),"sessions_pipeline_runtime")).toEqual([]);
  runtime=await connect("hoopdb","sessions_pipeline_runtime");
  expect(await schemaVerdict({...await expectedSchema(),dbMode:"selfhosted"},txn(runtime))).toEqual({ok:true});
  expect((await runMigrations(sql(owner),{profile:next})).applied).toBe(0);
  expect(await liveRows(owner)).toEqual(physicalAfter);
  // A canonical row cannot conceal an original/current collision, even if
  // the original is corrupt. All negative probes are read-only for history/marker.
  for(const alias of MIGRATION_RENAME_ALIASES){
   for(const hash of [alias.ledger_sha,"0".repeat(64)]){
    await owner.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2",[hash,alias.ledger_name]);
    await owner.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)",[alias.migration,alias.accepted_as_alias_of]);
    const ledgerBefore=await liveRows(owner), markerBeforeCollision=(await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows;
    expect(await schemaVerdict({...await expectedSchema(),dbMode:"selfhosted"},txn(runtime))).toEqual({ok:false,reason:"schema_pending",count:1});
    expect((await rejected(runMigrations(sql(owner),{profile:next})))?.code).toBe("ledger_unknown_step");
    expect(await liveRows(owner)).toEqual(ledgerBefore);
    expect((await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows).toEqual(markerBeforeCollision);
    expect(await selected(owner)).toEqual(preserved);
    await owner.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1",[alias.migration]);
    await owner.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2",[alias.ledger_sha,alias.ledger_name]);
    expect(await schemaVerdict({...await expectedSchema(),dbMode:"selfhosted"},txn(runtime))).toEqual({ok:true});
   }
  }
  // Original and current CLI versions are both a second authority. Every
  // refusal leaves the physical ledger, ready marker and seeded pointer intact.
  await owner.query("CREATE SCHEMA supabase_migrations; CREATE TABLE supabase_migrations.schema_migrations(version text PRIMARY KEY)");
  for(const alias of MIGRATION_RENAME_ALIASES){
   for(const name of [alias.ledger_name,alias.migration]){
    const version=name.slice("upstream:".length).split("_")[0]!;
    await owner.query("INSERT INTO supabase_migrations.schema_migrations(version) VALUES($1)",[version]);
    const ledgerBefore=await liveRows(owner),markerBeforeCli=(await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows;
    expect((await rejected(runMigrations(sql(owner),{profile:next})))?.code).toBe("mixed_migration_authority");
    expect(await liveRows(owner)).toEqual(ledgerBefore);
    expect((await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows).toEqual(markerBeforeCli);
    expect(await selected(owner)).toEqual(preserved);
    expect((await owner.query("SELECT version FROM supabase_migrations.schema_migrations")).rows).toEqual([{version}]);
    await owner.query("DELETE FROM supabase_migrations.schema_migrations WHERE version=$1",[version]);
   }
  }
  // A checksum forgery on this isolated fixture is not adopted: readiness stays
  // schema_pending and the migrator refuses, without marker/ledger side effects.
  const a=MIGRATION_RENAME_ALIASES[0];await owner.query("UPDATE sessions.__96bulls_migrations SET sha256=$1 WHERE name=$2",["0".repeat(64),a.ledger_name]);
  const markerBefore=(await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows;
  expect(await schemaVerdict({...await expectedSchema(),dbMode:"selfhosted"},txn(runtime))).toEqual({ok:false,reason:"schema_pending",count:1});
  expect((await rejected(runMigrations(sql(owner),{profile:next})))?.code).toBe("ledger_unknown_step");
  expect((await owner.query("SELECT * FROM sessions.__96bulls_role_validation")).rows).toEqual(markerBefore);
 } finally {await runtime?.end();await owner?.end();await op?.end();await admin.end();}
});
