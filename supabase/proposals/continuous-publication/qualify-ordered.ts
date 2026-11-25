// This entrypoint runs only inside qualify-ordered.sh's disposable tree/cluster.
import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import pg from "pg";
import assert from "node:assert/strict";
import { MIGRATION_SENTINELS } from "../../../src/hoopdb/migration-sentinels.ts";
import { plan, runMigrations } from "../../../src/hoopdb/migrate.ts";
import { publicationKernelSlot, publicationRecoverySlot, heldPublicationSentinels } from "./held-profile.ts";

const root=realpathSync(join(import.meta.dir,"../../.."));
const scratch=process.env.PUBLICATION_PROOF_SCRATCH;
if (!scratch || root !== realpathSync(join(scratch,"tree"))) throw Error("isolated proof tree required");
const source=join(root,"supabase/proposals/continuous-publication");
const discovery=join(root,"supabase/migrations");
const privateFile="20261105062270_fixture_private_read_profile.sql";
const searchFile="20261105062300_search_index_stage.sql";
const privateName="upstream:"+privateFile;
const searchName="upstream:"+searchFile;
const ordered=[privateName, publicationKernelSlot.name,publicationRecoverySlot.name,searchName];
const profile={mode:"local" as const,shim:true,bootstrap:true,verify:true,runtimeRole:"fixture_owner"};
const initial=await plan(profile);
assert.deepEqual(initial.map(s=>s.name).filter(n=>ordered.includes(n)),[searchName]);
// The private precondition is synthetic. Its filename deliberately cannot be
// confused with released capture SQL. Never compute a production generation.
writeFileSync(join(discovery,privateFile),readFileSync(join(source,"062270-private-read-profile.up.sql"),"utf8").replace(/^BEGIN;\n/m,"").replace(/\nCOMMIT;\s*$/,"\n"), {flag:"wx"});
for (const slot of [publicationKernelSlot,publicationRecoverySlot])
  writeFileSync(join(discovery,slot.name.slice("upstream:".length)),slot.up,{flag:"wx"});
const steps=await plan(profile);
assert.deepEqual(steps.map(s=>s.name).filter(n=>ordered.includes(n)),ordered);

const c=new pg.Client({host:scratch,user:"fixture_owner",port:5549,database:"postgres"});
await c.connect();
try {
  assert.equal((await c.query("SHOW server_encoding")).rows[0].server_encoding,"UTF8");
  await c.query("CREATE ROLE bulls96_publication_scoped NOLOGIN; CREATE ROLE bulls96_publication_recovery NOLOGIN; CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  await c.query("CREATE SCHEMA sessions; GRANT USAGE ON SCHEMA sessions TO bulls96_publication_scoped,bulls96_publication_recovery");
  // Exact on-disk discovery, SQL-byte ledger and preflight of the real migrator.
  // Its local marker describes only this disposable tree. It is never emitted
  // as a release generation or used for an enrolled receipt.
  // Exercise the refusal before supplying the proposed sentinel registrations
  // to this isolated process. Active tree registrations remain untouched.
  await assert.rejects(runMigrations(c,{profile}),{code:"sentinel_missing"});
  Object.assign(MIGRATION_SENTINELS,heldPublicationSentinels,{[privateName]:{
    evidence:"fixture private profile, revocation ledger and request validator",
    sql:"SELECT to_regclass('frontier_receipt_private.read_profiles') IS NOT NULL AND to_regclass('frontier_receipt_private.revoked_profiles') IS NOT NULL AND to_regprocedure('frontier_receipt_private.check_request(jsonb)') IS NOT NULL AS applied",
  }});
  const first=await runMigrations(c,{profile});
  assert.ok(first.applied>ordered.length);
  const rerun=await runMigrations(c,{profile});
  assert.equal(rerun.applied,0);
  const rows=await c.query("SELECT name FROM sessions.__96bulls_migrations WHERE name=ANY($1) ORDER BY name COLLATE \"C\"",[ordered]);
  assert.deepEqual(rows.rows.map(row=>row.name),ordered);
  assert.equal((await c.query("SELECT to_regprocedure('sessions.ingest_scoped_publication_receipt_capture_v1(jsonb)') IS NULL missing")).rows[0].missing,true);
  for(const s of Object.values(heldPublicationSentinels)) assert.equal((await c.query(s.sql)).rows[0].applied,true);
  await c.query("CREATE DATABASE publication_release_fixture TEMPLATE postgres");
  await assert.rejects(c.query(publicationRecoverySlot.down),{code:"55000"}); await c.query("ROLLBACK");
  const searchDown=readFileSync(join(root,"supabase/rollbacks",searchFile.replace(".sql",".down.sql")),"utf8");
  await c.query(searchDown);
  await c.query(publicationRecoverySlot.down);
  await c.query(publicationKernelSlot.down);
  await c.query(readFileSync(join(source,"062270-private-read-profile.down.sql"),"utf8"));
  assert.equal((await c.query("SELECT to_regclass('sessions.search_stage_archive') IS NOT NULL retained")).rows[0].retained,true);
  for(const s of Object.values(heldPublicationSentinels)) assert.equal((await c.query(s.sql)).rows[0].applied,false);
  console.log(JSON.stringify({server:(await c.query("SHOW server_version")).rows[0].server_version,encoding:"UTF8",discovered:ordered,rerunApplied:rerun.applied,rollback:"300/290/280/270",searchArchiveRetained:true,captureRpc:"absent"}));
} finally {await c.end();}
