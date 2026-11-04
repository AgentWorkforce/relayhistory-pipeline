/** Parity: old oracle SQL over raw tables vs session_output over the compact
 * publication, same seeded data. */
import { describe, expect, test, afterEach, setDefaultTimeout } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { join } from "node:path";
import { readFileSync, readdirSync } from "node:fs";
import { poolTenantRunner } from "../liveness.ts";
import { deriveCandidates } from "../aggregate-fold.ts";
import { publishAggregates } from "../aggregate-writer.ts";
import { sessionOutput } from "../session-output.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const open: PGlite[] = [];
afterEach(async () => Promise.all(open.splice(0).map(d => d.close())));

const TABLES = `
CREATE TABLE sessions.convergence_events (
  org_id text, workspace_id text, machine_id text, user_id text, source text,
  session_id text, event_id text, kind text, type text, ts timestamptz,
  actor_role text, content text, request_key text, delivery_record_id text,
  tool_name text, record jsonb, ingested_at timestamptz DEFAULT now(),
  PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id));
CREATE TABLE sessions.conversation_turns (
  id uuid, org_id text, workspace_id text, session_id text, source text,
  role text, content text, ts timestamptz);
CREATE TABLE sessions.session_commit_links (
  org_id text, workspace_id text, source text, session_id text, commit_sha text,
  numstat jsonb, linked_at timestamptz, received_at timestamptz);
CREATE TABLE sessions.session_outcomes (
  org_id text, workspace_id text, source text, session_id text, commit_sha text,
  numstat_json jsonb, shipped_at timestamptz, created_at timestamptz DEFAULT now());`;

const ORACLE = `
WITH events AS MATERIALIZED (
  SELECT * FROM sessions.convergence_events
   WHERE org_id='org-a' AND workspace_id='ws-a' AND ts >= $1::timestamptz AND ts <= $2::timestamptz
), messages AS (
  SELECT DISTINCT source, session_id,
    COALESCE(NULLIF(request_key,''), NULLIF(record->'payload'->>'message_id',''), delivery_record_id, machine_id||':'||event_id) AS message_id
  FROM events WHERE actor_role='assistant' AND btrim(COALESCE(content,''))<>''
    AND kind IN ('session_event','assistant','text')
    AND (record->'payload'->>'kind'='text' OR (record->'payload'->>'kind' IS NULL AND type IN ('assistant','text')))
), legacy_messages AS (
  SELECT t.id FROM sessions.conversation_turns t
   WHERE t.org_id='org-a' AND t.workspace_id='ws-a' AND t.ts >= $1::timestamptz AND t.ts <= $2::timestamptz
     AND t.role='assistant' AND btrim(t.content)<>''
     AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.source=t.source AND m.session_id=t.session_id)
), commit_candidates AS (
  SELECT lower(commit_sha) sha, COALESCE(linked_at, received_at) ts, numstat stats
    FROM sessions.session_commit_links WHERE org_id='org-a' AND workspace_id='ws-a'
  UNION ALL
  SELECT lower(commit_sha), COALESCE(shipped_at, created_at), numstat_json
    FROM sessions.session_outcomes WHERE org_id='org-a' AND workspace_id='ws-a'
  UNION ALL
  SELECT matched.parts[1], e.ts,
    jsonb_build_object('added', COALESCE(matched.parts[2],'0'), 'deleted', COALESCE(matched.parts[3],'0'))
   FROM events e CROSS JOIN LATERAL regexp_matches(e.content,
    '(?:^|\\n)\\[[^\\]\\n]+ ([0-9a-f]{7,40})\\][^\\n]*\\n[ \\t]*[0-9]+ files? changed(?:, ([0-9]+) insertions?\\(\\+\\))?(?:, ([0-9]+) deletions?\\(-\\))?','g') matched(parts)
   WHERE e.actor_role='tool_result' AND EXISTS (
    SELECT 1 FROM sessions.convergence_events call
     WHERE call.org_id=e.org_id AND call.workspace_id=e.workspace_id
       AND call.source=e.source AND call.session_id=e.session_id
       AND call.kind='tool_call' AND call.ts <= e.ts
       AND NULLIF(call.record->'payload'->>'tool_use_id','')=NULLIF(e.record->'payload'->>'tool_use_id','')
       AND (NULLIF(e.record->'payload'->>'parent_id','') IS NULL
         OR e.record->'payload'->>'parent_id'=call.record->'payload'->>'message_id'
         OR NULLIF(e.record->'payload'->>'message_id','')=NULLIF(call.record->'payload'->>'message_id',''))
       AND (NULLIF(e.record->'payload'->>'turn_id','') IS NULL OR EXISTS (
        SELECT 1 FROM sessions.convergence_events parent
         WHERE parent.org_id=call.org_id AND parent.workspace_id=call.workspace_id
           AND parent.source=call.source AND parent.session_id=call.session_id
           AND parent.kind='session_event'
           AND NULLIF(parent.record->'payload'->>'message_id','')=NULLIF(call.record->'payload'->>'message_id','')
           AND parent.record->'payload'->>'turn_id'=e.record->'payload'->>'turn_id'))
    HAVING count(DISTINCT jsonb_build_array(call.record->'payload'->>'message_id', call.tool_name,
      call.record->'payload'->>'args_json', CASE WHEN NULLIF(call.record->'payload'->>'message_id','') IS NULL THEN call.ts END))=1
     AND bool_and(COALESCE(call.tool_name IN ('exec_command','exec','Bash','bash','shell','run_shell_command')
       AND call.record->'payload'->>'args_json' ~ '\\mgit\\M[^\\n]*\\mcommit\\M', false))
   )
), commit_hashes AS MATERIALIZED (SELECT DISTINCT sha FROM commit_candidates),
short_lengths AS (SELECT DISTINCT length(sha) size FROM commit_hashes WHERE length(sha) BETWEEN 1 AND 39),
unique_prefixes AS MATERIALIZED (
  SELECT left(f.sha, s.size) prefix, min(f.sha) sha FROM commit_hashes f CROSS JOIN short_lengths s
   WHERE length(f.sha)=40 GROUP BY left(f.sha,s.size) HAVING count(*)=1),
canonical_commits AS (SELECT COALESCE(p.sha,c.sha) sha,c.ts,c.stats FROM commit_candidates c LEFT JOIN unique_prefixes p ON p.prefix=c.sha),
stat_entries AS (SELECT sha,ts,CASE
    WHEN jsonb_typeof(stats)='array' THEN stats
    WHEN jsonb_typeof(stats)='object' AND EXISTS (SELECT 1 FROM jsonb_each(stats) f WHERE f.key IN ('added','deleted','removed','additions','deletions') AND jsonb_typeof(f.value) NOT IN ('object','array')) THEN jsonb_build_array(stats)
    WHEN jsonb_typeof(stats)='object' THEN (SELECT jsonb_agg(value) FROM jsonb_each(stats)) END entries FROM canonical_commits),
normalized_stats AS (SELECT sha,ts,
  CASE WHEN entries IS NOT NULL THEN (SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'additions',value->>'added')~'^[0-9]{1,12}$' OR COALESCE(value->>'additions',value->>'added')='-' OR (jsonb_typeof(value->'path')='string' AND value->'additions'='null'::jsonb AND value->'deletions'='null'::jsonb),false)),true) THEN COALESCE(sum(CASE WHEN COALESCE(value->>'additions',value->>'added')~'^[0-9]{1,12}$' THEN COALESCE(value->>'additions',value->>'added')::bigint ELSE 0 END),0) END FROM jsonb_array_elements(entries)) END added,
  CASE WHEN entries IS NOT NULL THEN (SELECT CASE WHEN COALESCE(bool_and(COALESCE(COALESCE(value->>'deletions',value->>'removed',value->>'deleted')~'^[0-9]{1,12}$' OR COALESCE(value->>'deletions',value->>'removed',value->>'deleted')='-' OR (jsonb_typeof(value->'path')='string' AND value->'additions'='null'::jsonb AND value->'deletions'='null'::jsonb),false)),true) THEN COALESCE(sum(CASE WHEN COALESCE(value->>'deletions',value->>'removed',value->>'deleted')~'^[0-9]{1,12}$' THEN COALESCE(value->>'deletions',value->>'removed',value->>'deleted')::bigint ELSE 0 END),0) END FROM jsonb_array_elements(entries)) END removed
 FROM stat_entries),
commits AS (SELECT sha,min(ts) ts,max(added) added,max(removed) removed FROM normalized_stats GROUP BY sha)
SELECT ((SELECT count(*) FROM messages)+(SELECT count(*) FROM legacy_messages)) messages,
  count(*) commits, COALESCE(sum(added),0) added, COALESCE(sum(removed),0) removed,
  count(*) FILTER (WHERE added IS NULL OR removed IS NULL) missing FROM commits WHERE ts >= $1::timestamptz AND ts <= $2::timestamptz`;

async function db(): Promise<PGlite> {
  const d = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(d);
  await d.exec(read("supabase/tests/supabase_shim.sql"));
  await d.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  await d.exec(read("supabase/bootstrap/session_token_verifier.sql"));
  await d.exec(read("supabase/bootstrap/session_ingest_roles.sql"));
  for (const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())
    await d.exec(read(`supabase/migrations/${f}`));
  await d.exec(TABLES);
  return d;
}
const run = (d: PGlite) => poolTenantRunner({ connect: async () => ({ query: async (t,p) => d.query(t,p) as never, release(){} }) });
const S="2026-10-29T00:00:00Z", U="2026-11-02T00:00:00Z";
const ev=(day:string,session:string,ex:Record<string,string>={})=>`INSERT INTO sessions.convergence_events(org_id,workspace_id,machine_id,user_id,source,session_id,event_id,kind,type,ts,actor_role,content,request_key,record,tool_name) VALUES('org-a','ws-a','m-1','u-1','src-x','${session}','e${Math.random().toString(36).slice(2,9)}','${ex.kind??"session_event"}','${ex.type??"assistant"}','${day}T12:00:00Z','${ex.actor_role??"assistant"}','${(ex.content??"txt").replaceAll("'","''")}',${ex.request_key?`'${ex.request_key}'`:"NULL"},'${ex.record??"{}"}'::jsonb,${ex.tool_name?`'${ex.tool_name}'`:"NULL"})`;

async function parity(d: PGlite, events: ReturnType<typeof ev>[], turns:string, extra:string, since=S, until=U) {
  for (const e of events) await d.exec(e); if (turns) await d.exec(turns); if (extra) await d.exec(extra);
  const o = (await d.query<{messages:string;commits:string;added:string;removed:string;missing:string}>(ORACLE,[since,until])).rows[0]!;
  // fold the same rows into candidates and publish
  const rows=(await d.query<Record<string,unknown>>("SELECT * FROM sessions.convergence_events")).rows;
  const cands=deriveCandidates(rows.map(r=>({orgId:r.org_id,workspaceId:r.workspace_id,source:r.source,sessionId:r.session_id,kind:r.kind,type:r.type,ts:r.ts,actorRole:r.actor_role,content:r.content,requestKey:r.request_key,deliveryRecordId:r.delivery_record_id,record:r.record as never,toolName:r.tool_name,machineId:r.machine_id,eventId:r.event_id}) as never),
    (await d.query<Record<string,unknown>>("SELECT * FROM sessions.conversation_turns")).rows.map(r=>({id:r.id,orgId:r.org_id,workspaceId:r.workspace_id,source:r.source,sessionId:r.session_id,ts:r.ts}) as never),
    (await d.query<Record<string,unknown>>("SELECT * FROM sessions.session_commit_links")).rows.map(r=>({sha:r.commit_sha,orgId:r.org_id,workspaceId:r.workspace_id,source:r.source,sessionId:r.session_id,ts:r.linked_at??r.received_at,stats:r.numstat,provenance:"link"}) as never),
    (await d.query<Record<string,unknown>>("SELECT * FROM sessions.session_outcomes")).rows.map(r=>({sha:r.commit_sha,orgId:r.org_id,workspaceId:r.workspace_id,source:r.source,sessionId:r.session_id,ts:r.shipped_at??r.created_at,stats:r.numstat_json,provenance:"outcome"}) as never));
  await d.query("BEGIN"); try {
    await publishAggregates({query:(t,p)=>d.query(t,p) as never},{orgId:"org-a",workspaceId:"ws-a",sourceEpoch:new Date(),unionCas:"cas-1",candidates:cands});
    await d.query("COMMIT");
  } catch(e) { await d.query("ROLLBACK"); throw e; }
  await d.query("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://issuer.test','session-directory-search') ON CONFLICT DO NOTHING");
  const iat=Math.floor(Date.now()/1000);
  await d.query("SELECT set_config('request.jwt.claims',$1,false)",[JSON.stringify({role:"session_directory_search_worker",iss:"https://issuer.test",aud:"session-directory-search",iat,exp:iat+60,app_metadata:{org_id:"org-a",workspace_ids:["ws-a"]}})]);
  await d.exec("SET ROLE session_directory_search_worker");
  const n=await sessionOutput(run(d),"org-a","ws-a",new Date(since),new Date(until));
  return {o:{agentMessages:+o.messages,commits:+o.commits,linesAdded:+o.added,linesRemoved:+o.removed,commitsWithoutLineCounts:+o.missing},n};
}
describe("oracle parity",()=>{
  test("outside-window assistant + inside-window legacy turn counts the turn",async()=>{
    const d=await db();
    const {o,n}=await parity(d,[ev("2026-10-20","s-1")],`INSERT INTO sessions.conversation_turns VALUES('11111111-1111-1111-1111-111111111111','org-a','ws-a','s-1','src-x','assistant','legacy text','2026-10-30T12:00:00Z')`,"");
    expect(n).toEqual(o);
  });
  test("duplicate sha differing stats/types dedups once",async()=>{
    const d=await db();
    const {o,n}=await parity(d,[ev("2026-10-30","s-1")],"",`INSERT INTO sessions.session_commit_links VALUES('org-a','ws-a','src-x','s-1','abc1234','[]'::jsonb,NULL,'2026-10-30T01:00:00Z');INSERT INTO sessions.session_outcomes VALUES('org-a','ws-a','src-x','s-1','abc1234','[{"additions":"5","deletions":"2"}]'::jsonb,'2026-10-30T02:00:00Z','2026-10-30T02:00:00Z');`);
    expect(n).toEqual(o);
  });
  test("unready publication refuses 42501, never false zeros",async()=>{
    const d=await db();
    await expect(sessionOutput(run(d),"org-a","ws-a",new Date(S),new Date(U))).rejects.toThrow();
  });
});
