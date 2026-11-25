// This entrypoint runs only inside qualify-ordered.sh's disposable tree/cluster.
import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import pg from "pg";
import assert from "node:assert/strict";
import { plan, sha256Text } from "../../../src/hoopdb/migrate.ts";
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
const profile={mode:"local" as const,shim:true,bootstrap:true,verify:false,runtimeRole:"bulls96_runtime"};
const initial=await plan(profile);
assert.deepEqual(initial.map(s=>s.name).filter(n=>ordered.includes(n)),[searchName]);
// The private precondition is synthetic. Its filename deliberately cannot be
// confused with released capture SQL. Never compute a production generation.
writeFileSync(join(discovery,privateFile),readFileSync(join(source,"062270-private-read-profile.up.sql")), {flag:"wx"});
for (const slot of [publicationKernelSlot,publicationRecoverySlot])
  writeFileSync(join(discovery,slot.name.slice("upstream:".length)),slot.up,{flag:"wx"});
const steps=await plan(profile);
assert.deepEqual(steps.map(s=>s.name).filter(n=>ordered.includes(n)),ordered);

const c=new pg.Client({host:scratch,user:"fixture_owner",port:5549,database:"postgres"});
await c.connect();
try {
  assert.equal((await c.query("SHOW server_encoding")).rows[0].server_encoding,"UTF8");
  await c.query("CREATE ROLE bulls96_publication_scoped NOLOGIN; CREATE ROLE bulls96_publication_recovery NOLOGIN; CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  for(const step of steps) {
    const raw=readFileSync(step.path,"utf8");
    const sql=raw.split("\n").filter(line=>!line.startsWith("\\")).join("\n");
    if(step.name===privateName){
      await c.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY,sha256 text NOT NULL)");
      await c.query("GRANT USAGE ON SCHEMA sessions TO bulls96_publication_scoped,bulls96_publication_recovery");
    }
    // The private component already manages its own transaction. Preserve its
    // exact source. Every proposed up after it uses one outer transaction.
    if(!ordered.includes(step.name) || step.name===privateName) {await c.query(sql);continue;}
    await c.query("BEGIN");
    try {
      await c.query(sql);
      await c.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)",[step.name,sha256Text(raw)]);
      await c.query("COMMIT");
    } catch(e){await c.query("ROLLBACK");throw e;}
  }
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
  console.log(JSON.stringify({server:(await c.query("SHOW server_version")).rows[0].server_version,encoding:"UTF8",discovered:ordered,rollback:"300/290/280/270",searchArchiveRetained:true,captureRpc:"absent"}));
} finally {await c.end();}
