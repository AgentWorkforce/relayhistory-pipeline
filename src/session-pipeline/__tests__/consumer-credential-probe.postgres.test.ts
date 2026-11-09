/** Opt-in, disposable PostgreSQL 17 cluster only; never targets a project DB. */
import { test, expect, setDefaultTimeout } from 'bun:test';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { SESSION_BOOTSTRAP_FILES } from '../../hoopdb/session-bootstrap-chain.ts';
import { runMigrations, expectedSchema } from '../../hoopdb/migrate.ts';
import { bootstrapCheckOwner, bootstrapLock, ensureServiceTables, runBootstrap, stripPsqlMeta } from '../consumer-bootstrap.ts';
import { writerComposition } from '../consumer-service.ts';
import { writerTransactionSql } from '../transaction-sql.ts';
import { composeSearchService } from '../../shared/search-composition.ts';
setDefaultTimeout(120_000);
const native=process.env.CONSUMER_CREDENTIAL_PROBE_NATIVE==='true'?test:test.skip;
const read=(p:string)=>readFileSync(new URL('../../../'+p,import.meta.url),'utf8');
const role=stripPsqlMeta(read('supabase/bootstrap/session_consumer_role.sql'));
const helper=read('supabase/bootstrap/session_consumer_credential_probe.sql');
const guard=role.slice(0,role.indexOf('-- Schema access'));
const asSql=(c:pg.Client)=>({async query<R>(q:string,p?:unknown[]){return {rows:(await c.query(q,p)).rows as R[]}}});
const connect=async(database:string,user:string)=>{const c=new pg.Client({host:'127.0.0.1',port:Number(process.env.CONSUMER_CREDENTIAL_PROBE_PG_PORT ?? 5432),database,user});await c.connect();return c};
const code=async(p:Promise<unknown>)=>p.then(()=>null,(e:{code?:string})=>e.code);
native('PG17: non-superuser owner bootstrap uses exact fixed-role probe and all refusals preserve the role',async()=>{
 const admin=await connect('postgres','postgres');let op:pg.Client|undefined,owner:pg.Client|undefined,other:pg.Client|undefined;
 try{
  expect((await admin.query('SHOW cluster_name')).rows[0]?.cluster_name).toBe('consumer-credential-proof');
  expect(Number((await admin.query('SHOW server_version_num')).rows[0]?.server_version_num)).toBeGreaterThanOrEqual(170000);
  expect((await admin.query("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")).rows.map(r=>r.datname)).toEqual(['postgres']);
  await admin.query('CREATE ROLE hoop LOGIN CREATEROLE NOINHERIT NOSUPERUSER NOBYPASSRLS; CREATE ROLE unrelated LOGIN CREATEROLE NOINHERIT NOSUPERUSER NOBYPASSRLS');
  await admin.query('CREATE DATABASE consumer_probe OWNER hoop');
  op=await connect('consumer_probe','postgres');owner=await connect('consumer_probe','hoop');other=await connect('consumer_probe','unrelated');
  await op.query('CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin');
  await op.query(read('supabase/tests/supabase_shim.sql'));
  for(const p of SESSION_BOOTSTRAP_FILES.filter(p=>!p.endsWith('session_consumer_roles.sql')))await op.query(stripPsqlMeta(read(p)));
  await owner.query(read('supabase/bootstrap/session_consumer_roles.sql'));
  await runMigrations(asSql(owner),{profile:{mode:'local',shim:false,bootstrap:false,verify:false,runtimeRole:'hoop'}});
  await owner.query('CREATE SCHEMA IF NOT EXISTS search_index');
  // The catalog view masks NULL too: it cannot prove credential absence.
  expect((await owner.query("SELECT rolpassword FROM pg_roles WHERE rolname='search_consumer_runtime'")).rows[0]?.rolpassword).toBe('********');
  expect((await op.query("SELECT rolpassword IS NULL AS absent FROM pg_authid WHERE rolname='search_consumer_runtime'")).rows[0]?.absent).toBe(true);
  expect(await code(owner.query('SELECT rolpassword FROM pg_authid'))).toBe('42501');
  const unchanged=async()=>expect((await op!.query("SELECT rolcanlogin,rolvaliduntil,(SELECT count(*) FROM pg_db_role_setting WHERE setrole=pg_roles.oid)::int AS settings FROM pg_roles WHERE rolname='search_consumer_runtime'")).rows[0]).toEqual({rolcanlogin:false,rolvaliduntil:null,settings:0});
  const refuse=async(sql:string,want:string)=>{await owner!.query('BEGIN');expect(await code(owner!.query(sql))).toBe(want);await owner!.query('ROLLBACK');await unchanged()};
  await refuse(guard,'55000'); // unavailable helper is not a masked-password guess
  expect(await code(owner.query(helper))).toBe('42501');await owner.query('ROLLBACK');
  await op.query('ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT EXECUTE ON FUNCTIONS TO unrelated');
  await op.query(helper);
  await refuse(guard,'55000'); // unexpected default ACLs cannot silently broaden the helper
  await op.query('REVOKE EXECUTE ON FUNCTION session_consumer_bootstrap_private.consumer_has_password() FROM unrelated; ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM unrelated');
  expect((await owner.query('SELECT session_consumer_bootstrap_private.consumer_has_password() AS present')).rows[0]?.present).toBe(false);
  expect(await code(other.query('SELECT session_consumer_bootstrap_private.consumer_has_password()'))).toBe('42501');
  await op.query('ALTER SCHEMA search_index OWNER TO unrelated');
  expect(await code(owner.query('SELECT session_consumer_bootstrap_private.consumer_has_password()'))).toBe('42501');
  await op.query('ALTER SCHEMA search_index OWNER TO hoop');
  // Even an accidental execute grant cannot bypass the caller/schema-owner pin.
  await op.query('GRANT USAGE ON SCHEMA session_consumer_bootstrap_private TO unrelated; GRANT EXECUTE ON FUNCTION session_consumer_bootstrap_private.consumer_has_password() TO unrelated');
  expect(await code(other.query('SELECT session_consumer_bootstrap_private.consumer_has_password()'))).toBe('42501');
  await refuse(guard,'55000'); // broadened helper ACL refuses before promotion
  await op.query('REVOKE ALL ON SCHEMA session_consumer_bootstrap_private FROM unrelated; REVOKE ALL ON FUNCTION session_consumer_bootstrap_private.consumer_has_password() FROM unrelated');
  // Generate an unrelated synthetic credential inside PG; never return/log it.
  await owner.query('BEGIN');
  await owner.query("DO $$ BEGIN EXECUTE format('ALTER ROLE search_consumer_runtime PASSWORD %L',md5(random()::text)); END $$");
  expect((await owner.query('SELECT session_consumer_bootstrap_private.consumer_has_password() AS present')).rows[0]?.present).toBe(true);
  expect(await code(owner.query(guard))).toBe('55000');await owner.query('ROLLBACK');await unchanged();
  await op.query('ALTER FUNCTION session_consumer_bootstrap_private.consumer_has_password() VOLATILE');
  await refuse(guard,'55000');
  await op.query('ALTER FUNCTION session_consumer_bootstrap_private.consumer_has_password() STABLE');
  await op.query(helper.slice(helper.indexOf('CREATE FUNCTION'),helper.indexOf('REVOKE ALL ON FUNCTION')).replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION').replace('RETURN present;','RETURN false;'));
  await refuse(guard,'55000');
  await op.query(helper.slice(helper.indexOf('CREATE FUNCTION'),helper.indexOf('REVOKE ALL ON FUNCTION')).replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION'));
  await op.query('BEGIN; SET LOCAL ROLE hoop');
  expect(await code(op.query('SELECT session_consumer_bootstrap_private.consumer_has_password()'))).toBe('42501');await op.query('ROLLBACK');
  await owner.query('BEGIN; ALTER ROLE search_consumer_runtime LOGIN');
  expect(await code(owner.query('SELECT session_consumer_bootstrap_private.consumer_has_password()'))).toBe('55000');await owner.query('ROLLBACK');
  await owner.query('BEGIN; ALTER ROLE search_consumer_runtime LOGIN');expect(await code(owner.query(guard))).toBe('55000');await owner.query('ROLLBACK');
  await owner.query("BEGIN; ALTER ROLE search_consumer_runtime VALID UNTIL '2099-01-01'");
  expect(await code(owner.query(guard))).toBe('55000');await owner.query('ROLLBACK');
  await unchanged();
  const sql=asSql(owner);const writerSql=writerTransactionSql(sql);
  const binding=composeSearchService(writerComposition('credential-proof',0),{env:{SEARCH_INDEX_BACKEND:'postgres',SEARCH_INDEX_POSTGRES_BACKEND_ID:'credential-proof'},sql:writerSql.sql},'writer');
  const bootstrap=async()=>{await owner!.query('BEGIN');try{await runBootstrap({owner:sql,checkOwner:()=>bootstrapCheckOwner(sql),lock:()=>bootstrapLock(sql),ensureIndexSchema:()=>binding.index.ensureSchema(),ensureProjectionState:o=>binding.state.ensureSchema(o),ensureServiceTables:o=>ensureServiceTables(o),ensureRole:o=>o.query(role).then(()=>{}),verifyRole:o=>o.query(stripPsqlMeta(read('supabase/bootstrap/session_consumer_role_verify.sql'))).then(()=>{}),generation:(await expectedSchema()).generation});await owner!.query('COMMIT')}catch(e){await owner!.query('ROLLBACK');throw e}};
  await bootstrap();await bootstrap();
  expect((await op.query("SELECT rolcanlogin,rolsuper,rolcreaterole,rolbypassrls,rolpassword IS NULL AS password_absent FROM pg_authid WHERE rolname='search_consumer_runtime'")).rows[0]).toEqual({rolcanlogin:true,rolsuper:false,rolcreaterole:false,rolbypassrls:false,password_absent:true});
  expect((await op.query("SELECT setdatabase,setconfig FROM pg_db_role_setting WHERE setrole='search_consumer_runtime'::regrole")).rows).toEqual([{setdatabase:0,setconfig:['statement_timeout=60s']}]);
  expect(await code(owner.query('SELECT rolpassword FROM pg_authid'))).toBe('42501');
 }finally{await other?.end();await owner?.end();await op?.end();await admin.end()}
});

test('operator helper body is byte-pinned by the promotion guard',()=>{
 const body=helper.split('AS $credential_probe$')[1]!.split('$credential_probe$;')[0]!;
 expect(body.length).toBeGreaterThan(500);
 expect(role.split('v_probe_source constant text := $expected_probe$')[1]!.split('$expected_probe$;')[0]).toBe(body);
 expect(helper).not.toMatch(/GRANT\s+SELECT[\s\S]*pg_authid/i);
 expect(helper).not.toMatch(/ALTER\s+ROLE[\s\S]*PASSWORD/i);
});
