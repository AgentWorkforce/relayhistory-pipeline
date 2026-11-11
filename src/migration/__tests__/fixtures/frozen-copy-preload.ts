// Synthetic transport only: no provider CLI, process spawn or network is permitted. All values are fake.
import pg from "pg";
import { writeSync } from "node:fs";
import { createRequire, syncBuiltinESMExports } from "node:module";
import { FROZEN_COPY_TEST_OWNER_PASSWORD } from "./frozen-copy-owner-url.ts";
import { PROGRESS_COLUMNS } from "../../frozen-source-copy-progress.ts";
const cp = createRequire(import.meta.url)("node:child_process");
let processCalls=0, neoCreated=0, targetCreated=0, targetClosed=0, identityChecks=0, ownershipLocks=0;
let closedWhileWorkersActive=false;
const clients: FakeClient[] = [], records: string[] = [], targetConfigs: any[] = [], txLog: string[] = [];
/** Committed checkpoints (the fake owner-only progress table): key org|dataset. */
const checkpoints = new Map<string, any>();
for (const k of ["execFileSync","execSync","spawnSync","execFile","exec","spawn"]) cp[k] = () => { processCalls++; throw Error("unexpected_process_refused"); };
syncBuiltinESMExports();
globalThis.fetch = async () => { throw Error("network_refused_in_copy_fixture"); };
const env = process.env;
const H = (c: string) => c.repeat(64);
export const FIXTURE_POLICY = { generation: H("c"), source_identity_sha256: H("d"), control_epoch: H("a"), privacy_baseline_sha256: H("b") };
class FakeClient {
  target: boolean; neoIndex=0; ended=false; cfg: any;
  tx: { records: string[]; checkpoints: Map<string, any> } | null = null;
  constructor(cfg: any) {
    this.cfg=cfg;this.target=!!cfg.host;
    if(this.target) { targetCreated++; targetConfigs.push({...cfg,password:cfg.password===FROZEN_COPY_TEST_OWNER_PASSWORD?"<synthetic>":"<other>"}); }
    else this.neoIndex=++neoCreated;
    clients.push(this);
  }
  on() { return this; }
  async connect() {
    await new Promise(r=>setTimeout(r,this.target?5:1));
    if(env.FROZEN_COPY_TEST_CONNECT===(this.target?"target":this.neoIndex===2?"source":"none")) throw Object.assign(Error("synthetic_connect_failure"),{code:"08006"});
    if(this.target && this.cfg.password!==FROZEN_COPY_TEST_OWNER_PASSWORD) throw Object.assign(Error("synthetic_password_refused"),{code:"28P01"});
  }
  async query(sql: string,args: any[]=[]) {
    if(this.ended) throw Error("query_after_end");
    if(/\bSET\b.*\bROLE\b|session_authorization/i.test(sql)) throw Error("role_switch_refused_in_fixture");
    if(!this.target) {
      if(/^(BEGIN|ROLLBACK|COMMIT)/.test(sql))return {rows:[]};
      if(sql.includes("delivery_cutover_status()"))return {rows:[{s:{phase:"active",epoch:FIXTURE_POLICY.control_epoch,privacyBaselineSha256:FIXTURE_POLICY.privacy_baseline_sha256}}]};
      if(sql.includes("FROM sessions.delivery_origins"))return {rows:[{org_id:"org-a"},{org_id:"org-b"}]};
      if(sql.includes("FROM sessions.deleted_upload_sessions")||sql.includes("FROM sessions.session_resets")||sql.includes("FROM sessions.delivery_cutover_control_heads")||sql.includes("FROM sessions.delivery_receipts"))return {rows:[]};
      if(sql.includes("FROM sessions.delivery_records r")) {
        await new Promise(r=>setTimeout(r,5));
        return {rows:args[1]!==""?[]:[{org_id:args[0],workspace_id:"ws",origin_id:"origin",record_id:args[0],revision_id:"rev",revision:1,digest:"digest",kind:"kind",source:"source",session_id:"session",operation:"upsert",user_id:"user",projection_accepted_seq:null,catalog_accepted_seq:null}]};
      }
      throw Error("unexpected_sql_refused");
    }
    if(sql==="BEGIN") { txLog.push("BEGIN"); this.tx={records:[],checkpoints:new Map()}; return {rows:[]}; }
    if(sql==="ROLLBACK") { txLog.push("ROLLBACK"); this.tx=null; return {rows:[]}; }
    if(sql==="COMMIT") {
      txLog.push("COMMIT");
      const tx=this.tx!; this.tx=null;
      records.push(...tx.records); for (const [k,v] of tx.checkpoints) checkpoints.set(k,v);
      // The COMMIT reached the server and applied, but the answer was lost (no SQLSTATE).
      if(tx.records.includes(env.FROZEN_COPY_TEST_LOST ?? "\0")) throw Error("Connection terminated unexpectedly");
      return {rows:[]};
    }
    if(sql.includes("FROM pg_roles r WHERE r.rolname = current_user")) {
      identityChecks++;
      // FROZEN_COPY_TEST_IDENTITY=cli: a temporary login that switched role (session_user differs).
      const session=env.FROZEN_COPY_TEST_IDENTITY==="cli"?"cli_login_postgres":"postgres";
      return {rows:[{me:"postgres",session,db:"postgres",rolcanlogin:true,rolsuper:false,rolcreaterole:true,rolcreatedb:true,rolbypassrls:true,rolreplication:true,schema_owner:"postgres"}]};
    }
    if(sql.includes("to_regclass('sessions.__96bulls_frozen_copy_progress')")) {
      if(env.FROZEN_COPY_TEST_PROGRESS==="missing") return {rows:[]};
      return {rows:[{rls:env.FROZEN_COPY_TEST_PROGRESS!=="unsafe",owned:true,foreign_grants:0,cols:[...PROGRESS_COLUMNS]}]};
    }
    if(sql.includes("pg_try_advisory_lock")) { ownershipLocks++; return {rows:[{ok:env.FROZEN_COPY_TEST_BUSY!=="1"}]}; }
    if(sql.includes("FROM sessions.ingest_lazy_policy")) return {rows:[FIXTURE_POLICY]};
    if(sql.includes("GROUP BY 1,2,3,4,5,6")) {
      // FROZEN_COPY_TEST_PRIOR=generation|operator: checkpoints left by another binding / another operator build.
      if(env.FROZEN_COPY_TEST_PRIOR==="generation") return {rows:[{...FIXTURE_POLICY,generation:H("e"),binding_sha256:H("f"),operator_sha256:H("9"),n:1}]};
      if(env.FROZEN_COPY_TEST_PRIOR==="operator") return {rows:[{...FIXTURE_POLICY,binding_sha256:H("f"),operator_sha256:H("9"),n:1}]};
      return {rows:[]};
    }
    if(sql.includes("SELECT cursor,page_seq")) {
      const c=checkpoints.get(`${args[0]}|${args[1]}`);
      return {rows:c?[{cursor:c.cursor,seq:String(c.page_seq),rows:String(c.rows),status:c.status,at:c.at}]:[]};
    }
    if(sql.includes("SELECT page_seq::text AS seq FROM sessions.__96bulls_frozen_copy_progress")) {
      const c=checkpoints.get(`${args[0]}|${args[1]}`); return {rows:c?[{seq:String(c.page_seq)}]:[]};
    }
    if(sql.includes("INSERT INTO sessions.__96bulls_frozen_copy_progress")) {
      if(!this.tx) throw Error("checkpoint_outside_transaction");
      const key=`${args[0]}|${args[1]}`, prev=this.tx.checkpoints.get(key)??checkpoints.get(key);
      if(prev && !(prev.page_seq===args[9]-1 && prev.binding===args[2] && prev.status==="in_progress")) return {rows:[]};
      const at=new Date(Date.UTC(2026,0,1)).toISOString();
      this.tx.checkpoints.set(key,{binding:args[2],cursor:JSON.parse(args[8]),page_seq:args[9],rows:args[10],status:args[11],at});
      return {rows:[{seq:String(args[9]),at}]};
    }
    if(sql.includes("INSERT INTO sessions.ingest_frozen_source_records")) {
      if(!this.tx) throw Error("page_outside_transaction");
      const org=args[0][0];
      // The signal arrives while this page is in flight (delivery is async: hold the page until the handler ran).
      if(env.FROZEN_COPY_TEST_SIGNAL===org) { process.kill(process.pid,"SIGTERM"); await new Promise(r=>setTimeout(r,100)); }
      if(env.FROZEN_COPY_TEST_FAIL===org) {
        if(env.FROZEN_COPY_TEST_UNDEFINED==="1") throw undefined;
        throw Object.assign(Error("synthetic_target_failure"),{code:"XX000"});
      }
      this.tx.records.push(org);return {rows:[]};
    }
    throw Error("unexpected_sql_refused");
  }
  async end() {
    if(this.target) {
      targetClosed++;
      closedWhileWorkersActive ||= clients.some(c=>!c.target&&c.neoIndex>1&&!c.ended);
    }
    this.ended=true;
  }
}
pg.Client=FakeClient as any;
// "exit" (not beforeExit) so the proof is also printed after an early process.exit(64).
process.on("exit",()=>writeSync(1,"\n"+JSON.stringify({syntheticCopyProof:{processCalls,identityChecks,ownershipLocks,targetCreated,targetClosed,closedWhileWorkersActive,records:records.sort(),
  checkpoints:Object.fromEntries([...checkpoints].sort().map(([k,v])=>[k,{page_seq:v.page_seq,rows:v.rows,status:v.status}])),txLog,
  allNeoClosed:clients.filter(c=>!c.target).every(c=>c.ended),targetConfigs}})+"\n"));
