/** Serial local mutation proof. Restores exact source in finally; never uses services. */
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {spawnSync} from 'node:child_process';
const root=resolve(import.meta.dir,'..'),out=resolve(process.argv[2]??'/tmp/canonical-replay-fence-proof');mkdirSync(out,{recursive:true});
const checks=[
 {name:'readback_sha',file:'src/session-pipeline/replay-pages.ts',replacements:[['if (!read || read.length !== bytes.length || artifactDigest(read) !== sha256) refuse("artifact_write_verification_failed");','if (false) refuse("artifact_write_verification_failed");']],test:'src/session-pipeline/__tests__/replay-pages.test.ts',pattern:'failed readback'},
 {name:'source_epoch_and_snapshot_cas',file:'supabase/migrations/20261103040000_session_replay_pages.sql',replacements:[[' AND a.epoch=p_epoch FOR UPDATE',' FOR UPDATE'],['live IS DISTINCT FROM p_snapshot OR fp IS DISTINCT FROM p_fingerprint OR ',''],[' AND epoch=p_epoch;',';']],test:'src/session-pipeline/__tests__/replay-publication.test.ts',pattern:'CAS refuses changed source user_id|ABA ownership'},
 {name:'directory_tombstone',file:'supabase/migrations/20261103040000_session_replay_pages.sql',replacements:[["coalesce(live->'directory'->>'deleted_at','')<>'' OR ",'']],test:'src/session-pipeline/__tests__/replay-publication.test.ts',pattern:'directory tombstone'},
 {name:'event_stage_wiring',file:'src/session-pipeline/consumer.ts',replacements:[['if (deps.replay) {','if (false && deps.replay) {']],test:'src/session-pipeline/__tests__/replay-pages.test.ts',pattern:'existing event consumer'},
];
let failures=0;
for(const check of checks){const path=join(root,check.file),original=readFileSync(path,'utf8');try{let changed=original;for(const[from,to]of check.replacements){if(!changed.includes(from))throw Error('mutation anchor missing '+check.name);changed=changed.replaceAll(from,to)}writeFileSync(path,changed);const run=spawnSync(process.execPath,['test',check.test,'--test-name-pattern',check.pattern],{cwd:root,encoding:'utf8'});writeFileSync(join(out,check.name+'.log'),run.stdout+run.stderr);if(run.status===0)throw Error('reverted fence did not fail '+check.name);if(!/\(fail\)/u.test(run.stdout+run.stderr))throw Error('mutation failed without an assertion '+check.name);console.log(JSON.stringify({name:check.name,reverted_assertions_fail:true,status:run.status}));}catch(error){failures++;console.error(String(error))}finally{writeFileSync(path,original)}}
if(failures)process.exitCode=1;
